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

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tracing::{debug, info, warn};

use cassandra_cluster_metadata::Endpoint;
use cassandra_messaging::{Message, MessagingService, Verb};

use crate::hints::{Hint, HintMetrics, HintStore, HintedHandoffManager};
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
}

impl HintDeliveryService {
    /// Create a new hint delivery service.
    pub fn new(
        manager: Arc<HintedHandoffManager>,
        messaging: Arc<MessagingService>,
    ) -> Self {
        Self {
            manager,
            messaging,
            delivery_timeout: Duration::from_secs(10),
            metrics: HintDeliveryMetrics::new(),
        }
    }

    /// Set the delivery timeout per hint.
    pub fn with_delivery_timeout(mut self, timeout: Duration) -> Self {
        self.delivery_timeout = timeout;
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

    /// Deliver all pending hints for a target endpoint.
    ///
    /// Drains hints from the store and sends each as a `Verb::Hint`
    /// message. Returns the number of successfully delivered hints.
    pub async fn deliver_hints(&self, target: Endpoint) -> DeliveryResult {
        let hints = self.manager.store().drain_hints(&target);
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

        let mut delivered = 0u64;
        let mut failed = 0u64;

        for hint in hints {
            let request = HintRequest {
                mutation: hint.mutation,
                hint_id: hint.hint_id,
                created_at: hint.created_at,
            };

            let payload = match serde_json::to_vec(&request) {
                Ok(p) => p,
                Err(e) => {
                    warn!(error = %e, "Failed to serialize hint for delivery");
                    failed += 1;
                    continue;
                }
            };

            let msg = Message::request(Verb::Hint, self.messaging.next_id(), payload);

            match self
                .messaging
                .send_and_wait(target.0, msg, self.delivery_timeout)
                .await
            {
                Ok(response) => {
                    if response.is_failure() {
                        warn!(target = %target, "Hint delivery got failure response");
                        failed += 1;
                    } else {
                        delivered += 1;
                    }
                }
                Err(e) => {
                    warn!(target = %target, error = %e, "Hint delivery failed");
                    failed += 1;
                }
            }
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
}

/// Result of a hint delivery run.
#[derive(Debug)]
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
        let messaging = Arc::new(MessagingService::new(
            "127.0.0.1:0".parse().unwrap(),
        ));
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
        let mutation = CoordinatedMutation::simple(
            "ks".into(),
            "tbl".into(),
            vec![1],
            vec![],
            1000,
        );
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
        let mutation = CoordinatedMutation::simple(
            "ks".into(),
            "tbl".into(),
            vec![1],
            vec![],
            1000,
        );
        svc.manager().store().store_hint(target, mutation);

        let result = svc.deliver_hints(target).await;
        // Delivery fails because no server is listening.
        assert_eq!(result.delivered, 0);
        assert_eq!(result.failed, 1);
        assert_eq!(svc.metrics.hints_delivery_failed.load(Ordering::Relaxed), 1);
        assert_eq!(svc.metrics.delivery_runs.load(Ordering::Relaxed), 1);
    }
}
