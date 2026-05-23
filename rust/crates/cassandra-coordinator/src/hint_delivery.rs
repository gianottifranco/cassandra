// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.
// SPDX-License-Identifier: Apache-2.0

//! Hint delivery service: sends stored hints to recovered replicas via messaging.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.hints.HintsDispatcher`
//! - `org.apache.cassandra.hints.HintsService`
//!
//! ## Architecture
//!
//! When a node recovers, `HintDeliveryService::deliver_hints()` drains
//! all stored hints for that target and sends them as `Verb::Hint`
//! messages via the `MessagingService`. Throttling is applied per the
//! configured delivery rate.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use tokio::sync::Notify;
use tracing::{debug, info, warn};

use cassandra_cluster_metadata::Endpoint;
use cassandra_messaging::{Message, MessagingService, Verb};

use crate::hints::{Hint, HintedHandoffManager};
use crate::verb_handlers::hint_handler::HintRequest;

// ─── Delivery Metrics ───────────────────────────────────────────

/// Metrics for hint delivery.
pub struct HintDeliveryMetrics {
    /// Number of hints successfully delivered.
    pub hints_delivered: AtomicU64,
    /// Number of hints that failed delivery.
    pub hints_delivery_failed: AtomicU64,
    /// Number of delivery runs completed.
    pub delivery_runs: AtomicU64,
}

impl HintDeliveryMetrics {
    pub fn new() -> Self {
        Self {
            hints_delivered: AtomicU64::new(0),
            hints_delivery_failed: AtomicU64::new(0),
            delivery_runs: AtomicU64::new(0),
        }
    }
}

impl Default for HintDeliveryMetrics {
    fn default() -> Self {
        Self::new()
    }
}

// ─── Hint Delivery Service ──────────────────────────────────────

/// Service that delivers stored hints to recovered replicas.
///
/// Wraps `HintedHandoffManager` for hint lifecycle and
/// `MessagingService` for actual delivery over the network.
pub struct HintDeliveryService {
    /// The hinted handoff manager (hint store + lifecycle).
    manager: Arc<HintedHandoffManager>,
    /// The messaging service for sending hints.
    messaging: Arc<MessagingService>,
    /// Delivery timeout per hint.
    delivery_timeout: Duration,
    /// Delivery metrics.
    pub metrics: HintDeliveryMetrics,
    /// Maximum number of delivery retries per hint.
    max_retries: usize,
    /// Shutdown signal for cancellation-safe delivery.
    shutdown: Arc<Notify>,
    /// Sticky shutdown flag so notifications cannot be missed.
    shutdown_requested: Arc<AtomicBool>,
}

impl HintDeliveryService {
    /// Create a new hint delivery service.
    pub fn new(manager: Arc<HintedHandoffManager>, messaging: Arc<MessagingService>) -> Self {
        Self {
            manager,
            messaging,
            delivery_timeout: Duration::from_secs(10),
            metrics: HintDeliveryMetrics::new(),
            max_retries: 3,
            shutdown: Arc::new(Notify::new()),
            shutdown_requested: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Set the delivery timeout per hint.
    pub fn with_delivery_timeout(mut self, timeout: Duration) -> Self {
        self.delivery_timeout = timeout;
        self
    }

    /// Set the maximum number of retries per hint.
    pub fn with_max_retries(mut self, retries: usize) -> Self {
        self.max_retries = retries;
        self
    }

    /// Access the underlying hint manager.
    pub fn manager(&self) -> &Arc<HintedHandoffManager> {
        &self.manager
    }

    /// Check if there are hints pending for the given target.
    pub fn has_hints_for(&self, target: &Endpoint) -> bool {
        self.manager.has_hints_for(target)
    }

    /// Pause hint delivery (delegates to HintStore).
    pub fn pause_delivery(&self) {
        self.manager.store().pause();
    }

    /// Resume hint delivery (delegates to HintStore).
    pub fn resume_delivery(&self) {
        self.manager.store().resume();
    }

    /// Signal shutdown for cancellation-safe delivery.
    pub fn shutdown(&self) {
        info!("Hint delivery service shutting down");
        self.shutdown_requested.store(true, Ordering::SeqCst);
        self.shutdown.notify_waiters();
    }

    /// Get a clone of the shutdown notify for external cancellation.
    pub fn shutdown_notify(&self) -> Arc<Notify> {
        self.shutdown.clone()
    }

    /// Run one scheduler pass over all endpoints that currently have pending
    /// in-memory hints.
    pub async fn deliver_pending_hints_once(&self) -> DeliveryResult {
        let targets: Vec<Endpoint> = self
            .manager
            .store()
            .stats_per_endpoint()
            .keys()
            .copied()
            .collect();

        let mut aggregate = DeliveryResult::default();
        for target in targets {
            let result = self.deliver_hints(target).await;
            aggregate.delivered += result.delivered;
            aggregate.failed += result.failed;
        }

        aggregate
    }

    /// Start periodic hint delivery scheduling.
    ///
    /// Each tick scans endpoints with pending hints and attempts delivery.
    /// Undelivered hints are requeued by `deliver_hints`, so a failed round is
    /// retried by the next tick.
    pub fn spawn_periodic_delivery(
        self: Arc<Self>,
        interval: Duration,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            loop {
                if self.is_shutdown_signaled() {
                    break;
                }
                tokio::select! {
                    () = self.shutdown.notified() => break,
                    _ = ticker.tick() => {
                        let _ = self.deliver_pending_hints_once().await;
                    }
                }
            }
        })
    }

    /// Deliver all pending hints for a target endpoint.
    ///
    /// Drains hints from the store and sends each as a `Verb::Hint`
    /// message. Retries failed hints up to `max_retries` times with
    /// exponential backoff (100ms, 200ms, 400ms). Respects pause state,
    /// delivery throttle, hint expiration, and shutdown signals.
    ///
    /// Returns the number of successfully delivered hints.
    pub async fn deliver_hints(&self, target: Endpoint) -> DeliveryResult {
        let hints = self.manager.store().drain_hints(&target);
        self.deliver_hint_batch(target, hints).await
    }

    /// Deliver hints for a node that just recovered, applying topology
    /// ownership filtering before dispatch.
    pub async fn deliver_hints_for_recovered_node(
        &self,
        target: Endpoint,
        snapshot: &cassandra_cluster_metadata::ClusterSnapshot,
        snitch: &dyn cassandra_cluster_metadata::Snitch,
        strategies: &HashMap<String, Box<dyn cassandra_cluster_metadata::ReplicationStrategy>>,
    ) -> DeliveryResult {
        let hints = self
            .manager
            .on_node_recovered(&target, snapshot, snitch, strategies);
        self.deliver_hint_batch(target, hints).await
    }

    async fn deliver_hint_batch(&self, target: Endpoint, hints: Vec<Hint>) -> DeliveryResult {
        if hints.is_empty() {
            return DeliveryResult {
                delivered: 0,
                failed: 0,
            };
        }

        info!(
            target = %target,
            count = hints.len(),
            "Delivering hints"
        );

        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        let window_ms = self.manager.store().config().max_hint_window.as_millis() as i64;
        let cutoff = now_ms - window_ms;

        let throttle_bps = self
            .manager
            .store()
            .config()
            .delivery_throttle_bytes_per_sec;

        let mut delivered = 0u64;
        let mut failed = 0u64;
        let mut requeue = Vec::new();

        // Track in-progress hints.
        let total_hints = hints.len() as u64;
        self.manager
            .store()
            .metrics
            .hints_in_progress
            .fetch_add(total_hints, Ordering::Relaxed);

        let mut pending: VecDeque<Hint> = hints.into();
        while let Some(hint) = pending.pop_front() {
            // Check shutdown signal.
            if self.is_shutdown_signaled() {
                debug!("Shutdown signaled, stopping hint delivery");
                requeue.push(hint);
                requeue.extend(pending.drain(..));
                self.manager
                    .store()
                    .metrics
                    .hints_in_progress
                    .fetch_sub(total_hints - delivered - failed, Ordering::Relaxed);
                break;
            }

            // Check pause state.
            if self.manager.store().is_paused() {
                debug!(target = %target, "Delivery paused, stopping");
                requeue.push(hint);
                requeue.extend(pending.drain(..));
                self.manager
                    .store()
                    .metrics
                    .hints_in_progress
                    .fetch_sub(total_hints - delivered - failed, Ordering::Relaxed);
                break;
            }

            // Filter expired hints.
            if hint.created_at < cutoff {
                self.manager
                    .store()
                    .metrics
                    .hints_expired
                    .fetch_add(1, Ordering::Relaxed);
                self.manager
                    .store()
                    .metrics
                    .hints_in_progress
                    .fetch_sub(1, Ordering::Relaxed);
                continue;
            }

            let request = HintRequest {
                mutation: hint.mutation.clone(),
                hint_id: hint.hint_id,
                created_at: hint.created_at,
            };

            let payload = match serde_json::to_vec(&request) {
                Ok(p) => p,
                Err(e) => {
                    warn!(error = %e, "Failed to serialize hint for delivery");
                    failed += 1;
                    self.manager
                        .store()
                        .metrics
                        .hints_in_progress
                        .fetch_sub(1, Ordering::Relaxed);
                    continue;
                }
            };

            let payload_len = payload.len() as u64;

            // Retry with exponential backoff.
            let mut success = false;
            for attempt in 0..=self.max_retries {
                if attempt > 0 {
                    let backoff = Duration::from_millis(100 * (1 << (attempt - 1)));
                    tokio::select! {
                        () = tokio::time::sleep(backoff) => {}
                        () = self.shutdown.notified() => {
                            debug!("Shutdown during backoff");
                            break;
                        }
                    }
                }

                let msg = Message::request(Verb::Hint, self.messaging.next_id(), payload.clone());

                let result = tokio::select! {
                    r = self.messaging.send_and_wait(target.0, msg, self.delivery_timeout) => r,
                    () = self.shutdown.notified() => {
                        debug!("Shutdown during send");
                        break;
                    }
                };

                match result {
                    Ok(response) if !response.is_failure() => {
                        success = true;
                        break;
                    }
                    Ok(_) => {
                        if attempt < self.max_retries {
                            debug!(target = %target, attempt = attempt + 1, "Hint delivery got failure, retrying");
                        }
                    }
                    Err(e) => {
                        if attempt < self.max_retries {
                            debug!(target = %target, attempt = attempt + 1, error = %e, "Hint delivery failed, retrying");
                        }
                    }
                }
            }

            if success {
                delivered += 1;
            } else {
                warn!(target = %target, "Hint delivery failed after retries");
                failed += 1;
                requeue.push(hint);
            }

            self.manager
                .store()
                .metrics
                .hints_in_progress
                .fetch_sub(1, Ordering::Relaxed);

            // Throttling: sleep proportional to payload size vs allowed throughput.
            if throttle_bps > 0 && payload_len > 0 {
                let sleep_us = (payload_len as u128 * 1_000_000) / throttle_bps as u128;
                if sleep_us > 0 {
                    tokio::time::sleep(Duration::from_micros(sleep_us as u64)).await;
                }
            }
        }

        let requeued = self.manager.store().requeue_hints(target, requeue);
        if requeued > 0 {
            debug!(target = %target, requeued, "Requeued undelivered hints");
        }

        self.metrics
            .hints_delivered
            .fetch_add(delivered, Ordering::Relaxed);
        self.metrics
            .hints_delivery_failed
            .fetch_add(failed, Ordering::Relaxed);
        self.metrics.delivery_runs.fetch_add(1, Ordering::Relaxed);

        // Update the hint store metrics as well.
        self.manager
            .store()
            .metrics
            .hints_replayed
            .fetch_add(delivered, Ordering::Relaxed);
        self.manager
            .store()
            .metrics
            .hints_failed
            .fetch_add(failed, Ordering::Relaxed);

        debug!(
            target = %target,
            delivered,
            failed,
            "Hint delivery complete"
        );

        DeliveryResult { delivered, failed }
    }

    /// Check if shutdown has been signaled (non-blocking poll).
    fn is_shutdown_signaled(&self) -> bool {
        self.shutdown_requested.load(Ordering::SeqCst)
    }
}

/// Result of a hint delivery run.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct DeliveryResult {
    /// Number of hints successfully delivered.
    pub delivered: u64,
    /// Number of hints that failed delivery.
    pub failed: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hints::{HintConfig, HintedHandoffManager};
    use crate::write::CoordinatedMutation;

    fn make_service() -> (HintDeliveryService, Endpoint) {
        let config = HintConfig::default();
        let manager = Arc::new(HintedHandoffManager::new(config));
        let messaging = Arc::new(MessagingService::new("127.0.0.1:0".parse().unwrap()));
        let target = Endpoint::new("127.0.0.2:7000".parse().unwrap());

        let svc = HintDeliveryService::new(manager, messaging);
        (svc, target)
    }

    #[test]
    fn construction() {
        let (svc, target) = make_service();
        assert!(!svc.has_hints_for(&target));
    }

    #[test]
    fn has_hints_after_store() {
        let (svc, target) = make_service();
        let mutation =
            CoordinatedMutation::simple("ks".into(), "tbl".into(), vec![1], vec![], 1000);
        svc.manager().store().store_hint(target, mutation);
        assert!(svc.has_hints_for(&target));
    }

    #[tokio::test]
    async fn deliver_empty() {
        let (svc, target) = make_service();
        let result = svc.deliver_hints(target).await;
        assert_eq!(result.delivered, 0);
        assert_eq!(result.failed, 0);
    }

    #[tokio::test]
    async fn deliver_with_hints_no_listener() {
        // Hints exist but no listener on target — delivery will fail.
        let (svc, target) = make_service();
        let mutation =
            CoordinatedMutation::simple("ks".into(), "tbl".into(), vec![1], vec![], 1000);
        svc.manager().store().store_hint(target, mutation);

        let result = svc.deliver_hints(target).await;
        // Delivery fails because no server is listening.
        assert_eq!(result.delivered, 0);
        assert_eq!(result.failed, 1);
        assert_eq!(svc.metrics.hints_delivery_failed.load(Ordering::Relaxed), 1);
        assert_eq!(svc.metrics.delivery_runs.load(Ordering::Relaxed), 1);
        assert!(svc.has_hints_for(&target));
    }

    // ── WU-11: Delivery enhancements tests ──────────────────────

    #[test]
    fn pause_and_resume_delivery() {
        let (svc, _target) = make_service();
        assert!(!svc.manager().store().is_paused());

        svc.pause_delivery();
        assert!(svc.manager().store().is_paused());

        svc.resume_delivery();
        assert!(!svc.manager().store().is_paused());
    }

    #[test]
    fn shutdown_notify_cloneable() {
        let (svc, _target) = make_service();
        let notify = svc.shutdown_notify();
        // Should be cloneable and callable.
        svc.shutdown();
        drop(notify);
    }

    #[test]
    fn with_max_retries_builder() {
        let config = HintConfig::default();
        let manager = Arc::new(HintedHandoffManager::new(config));
        let messaging = Arc::new(MessagingService::new("127.0.0.1:0".parse().unwrap()));
        let svc = HintDeliveryService::new(manager, messaging).with_max_retries(5);
        assert_eq!(svc.max_retries, 5);
    }

    #[tokio::test]
    async fn scheduler_pass_attempts_all_targets_and_requeues_failures() {
        let config = HintConfig {
            delivery_throttle_bytes_per_sec: 0,
            ..HintConfig::default()
        };
        let manager = Arc::new(HintedHandoffManager::new(config));
        let messaging = Arc::new(MessagingService::new("127.0.0.1:0".parse().unwrap()));
        let svc = HintDeliveryService::new(manager, messaging)
            .with_delivery_timeout(Duration::from_millis(1))
            .with_max_retries(0);
        let target_a = Endpoint::new("127.0.0.2:7000".parse().unwrap());
        let target_b = Endpoint::new("127.0.0.3:7000".parse().unwrap());
        let mutation =
            CoordinatedMutation::simple("ks".into(), "tbl".into(), vec![1], vec![], 1000);

        svc.manager().store().store_hint(target_a, mutation.clone());
        svc.manager().store().store_hint(target_b, mutation);

        let result = svc.deliver_pending_hints_once().await;

        assert_eq!(result.delivered, 0);
        assert_eq!(result.failed, 2);
        assert!(svc.has_hints_for(&target_a));
        assert!(svc.has_hints_for(&target_b));
        assert_eq!(svc.metrics.delivery_runs.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn periodic_scheduler_stops_on_shutdown() {
        let config = HintConfig::default();
        let manager = Arc::new(HintedHandoffManager::new(config));
        let messaging = Arc::new(MessagingService::new("127.0.0.1:0".parse().unwrap()));
        let svc = Arc::new(HintDeliveryService::new(manager, messaging));

        let handle = Arc::clone(&svc).spawn_periodic_delivery(Duration::from_millis(5));
        tokio::time::sleep(Duration::from_millis(10)).await;
        svc.shutdown();

        tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .expect("periodic scheduler should stop")
            .expect("periodic scheduler task should not panic");
    }

    #[tokio::test]
    async fn deliver_paused_returns_empty() {
        let (svc, target) = make_service();
        let mutation =
            CoordinatedMutation::simple("ks".into(), "tbl".into(), vec![1], vec![], 1000);
        svc.manager().store().store_hint(target, mutation);

        // Pause delivery.
        svc.pause_delivery();
        let result = svc.deliver_hints(target).await;
        // Drain returns empty when paused.
        assert_eq!(result.delivered, 0);
        assert_eq!(result.failed, 0);
    }

    #[tokio::test]
    async fn recovered_node_delivery_filters_invalid_topology_hints() {
        use cassandra_cluster_metadata::{
            ClusterMetadata, NodeId, NodeInfo, SimpleSnitch, SimpleStrategy,
        };
        use cassandra_common::Token;
        use std::collections::HashMap;

        let (svc, target) = make_service();
        let mutation =
            CoordinatedMutation::simple("ks".into(), "tbl".into(), vec![1], vec![], 1000);
        svc.manager().store().store_hint(target, mutation);
        assert!(svc.has_hints_for(&target));

        let node = NodeInfo::new(
            NodeId::random(),
            Endpoint::new("127.0.0.3:7000".parse().unwrap()),
            "dc1",
            "rack1",
            vec![Token::from_raw(0)],
        );
        let cm = ClusterMetadata::new(node);
        let snapshot = cm.snapshot();
        let snitch = SimpleSnitch;
        let mut strategies: HashMap<
            String,
            Box<dyn cassandra_cluster_metadata::ReplicationStrategy>,
        > = HashMap::new();
        strategies.insert("ks".to_string(), Box::new(SimpleStrategy::new(1)));

        let result = svc
            .deliver_hints_for_recovered_node(target, &snapshot, &snitch, &strategies)
            .await;

        assert_eq!(result.delivered, 0);
        assert_eq!(result.failed, 0);
        assert!(!svc.has_hints_for(&target));
    }

    // ── WU-13: In-progress metrics tests ────────────────────────

    #[tokio::test]
    async fn hints_in_progress_returns_to_zero() {
        let (svc, target) = make_service();
        let mutation =
            CoordinatedMutation::simple("ks".into(), "tbl".into(), vec![1], vec![], 1000);
        svc.manager().store().store_hint(target, mutation);

        let _result = svc.deliver_hints(target).await;

        // After delivery completes, in_progress should be 0.
        assert_eq!(
            svc.manager()
                .store()
                .metrics
                .hints_in_progress
                .load(Ordering::Relaxed),
            0
        );
    }
}
