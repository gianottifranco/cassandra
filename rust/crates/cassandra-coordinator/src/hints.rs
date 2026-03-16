// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.
// SPDX-License-Identifier: Apache-2.0

//! Hinted handoff store: persists mutations for temporarily down replicas.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.hints.HintsStore`
//! - `org.apache.cassandra.hints.HintsService`
//! - `org.apache.cassandra.hints.HintsDispatcher`
//!
//! ## Architecture
//!
//! When a replica is down, mutations destined for it are stored as hints.
//! When the replica recovers, hints are replayed (delivered) to it.
//!
//! Lifecycle:
//! 1. **Store**: coordinator stores hint when replica is unreachable
//! 2. **Persist**: hints are durably written (in-memory + optional disk)
//! 3. **Expire**: hints older than `max_hint_window` are discarded
//! 4. **Replay**: when target node recovers, hints are delivered with throttling
//! 5. **Cleanup**: hints for removed nodes are purged

use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::RwLock;
use tracing::{debug, info, warn};

use cassandra_cluster_metadata::Endpoint;
use crate::write::CoordinatedMutation;

// ─── Configuration ───────────────────────────────────────────────

/// Hint store configuration.
///
/// ## Java Oracle
///
/// `cassandra.yaml`: `hinted_handoff_enabled`, `max_hint_window_in_ms`,
/// `hinted_handoff_throttle_in_kb`, `max_hints_file_size_in_mb`
#[derive(Debug, Clone)]
pub struct HintConfig {
    /// Whether hinted handoff is enabled.
    pub enabled: bool,
    /// Maximum age of a hint before it's discarded (default: 3 hours).
    pub max_hint_window: Duration,
    /// Maximum hints per endpoint in memory.
    pub max_hints_per_endpoint: usize,
    /// Maximum total hints in memory.
    pub max_total_hints: u64,
    /// Throttle for hint delivery (bytes per second per endpoint).
    pub delivery_throttle_bytes_per_sec: u64,
    /// Directory for hint persistence (None = in-memory only).
    pub hints_directory: Option<PathBuf>,
}

impl Default for HintConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_hint_window: Duration::from_secs(3 * 3600), // 3 hours
            max_hints_per_endpoint: 100_000,
            max_total_hints: 1_000_000,
            delivery_throttle_bytes_per_sec: 256 * 1024, // 256 KB/s
            hints_directory: None,
        }
    }
}

// ─── Hint ────────────────────────────────────────────────────────

/// A stored hint: a mutation to be replayed when the target node recovers.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Hint {
    /// Target endpoint that was down.
    pub target: Endpoint,
    /// The original mutation.
    pub mutation: CoordinatedMutation,
    /// When the hint was created (epoch millis).
    pub created_at: i64,
    /// Hint ID for tracking.
    pub hint_id: u64,
}

// ─── Hint Metrics ────────────────────────────────────────────────

/// Metrics for hint store observability.
///
/// ## Java Oracle
///
/// `org.apache.cassandra.metrics.HintedHandoffMetrics`
pub struct HintMetrics {
    pub hints_created: AtomicU64,
    pub hints_replayed: AtomicU64,
    pub hints_expired: AtomicU64,
    pub hints_failed: AtomicU64,
    pub hints_dropped_overflow: AtomicU64,
}

impl HintMetrics {
    pub fn new() -> Self {
        Self {
            hints_created: AtomicU64::new(0),
            hints_replayed: AtomicU64::new(0),
            hints_expired: AtomicU64::new(0),
            hints_failed: AtomicU64::new(0),
            hints_dropped_overflow: AtomicU64::new(0),
        }
    }
}

impl Default for HintMetrics {
    fn default() -> Self {
        Self::new()
    }
}

// ─── Hint Store ──────────────────────────────────────────────────

/// In-memory hint store with configurable limits and expiration.
///
/// ## Java Oracle
///
/// `org.apache.cassandra.hints.HintsStore`
pub struct HintStore {
    /// Hints indexed by target endpoint.
    hints: Arc<RwLock<HashMap<Endpoint, VecDeque<Hint>>>>,
    /// Configuration.
    config: HintConfig,
    /// Next hint ID.
    next_id: AtomicU64,
    /// Total hints stored.
    total_hints: AtomicU64,
    /// When true, hint delivery (drain) is paused during topology changes.
    paused: AtomicBool,
    /// Metrics.
    pub metrics: Arc<HintMetrics>,
}

impl HintStore {
    /// Create with just a max-per-endpoint (backwards-compatible).
    pub fn new(max_hints_per_endpoint: usize) -> Self {
        let config = HintConfig {
            max_hints_per_endpoint,
            ..HintConfig::default()
        };
        Self::with_config(config)
    }

    /// Create with full configuration.
    pub fn with_config(config: HintConfig) -> Self {
        Self {
            hints: Arc::new(RwLock::new(HashMap::new())),
            config,
            next_id: AtomicU64::new(1),
            total_hints: AtomicU64::new(0),
            paused: AtomicBool::new(false),
            metrics: Arc::new(HintMetrics::new()),
        }
    }

    /// Store a hint for a down replica.
    ///
    /// Returns `true` if the hint was stored, `false` if it was dropped
    /// due to capacity limits or disabled config.
    pub fn store_hint(&self, target: Endpoint, mutation: CoordinatedMutation) -> bool {
        if !self.config.enabled {
            debug!(target = %target, "Hinted handoff disabled, dropping hint");
            return false;
        }

        // Check total capacity
        let total = self.total_hints.load(Ordering::Relaxed);
        if total >= self.config.max_total_hints {
            warn!(
                total = total,
                max = self.config.max_total_hints,
                "Total hint capacity exceeded, dropping hint"
            );
            self.metrics.hints_dropped_overflow.fetch_add(1, Ordering::Relaxed);
            return false;
        }

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;

        let hint_id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let hint = Hint {
            target,
            mutation,
            created_at: now,
            hint_id,
        };

        let mut hints = self.hints.write();
        let queue = hints.entry(target).or_default();

        if queue.len() >= self.config.max_hints_per_endpoint {
            queue.pop_front(); // Drop oldest hint
            debug!(target = %target, "Hint store full for endpoint, dropping oldest");
            self.metrics.hints_dropped_overflow.fetch_add(1, Ordering::Relaxed);
        } else {
            self.total_hints.fetch_add(1, Ordering::Relaxed);
        }

        queue.push_back(hint);
        self.metrics.hints_created.fetch_add(1, Ordering::Relaxed);
        true
    }

    /// Get and drain all hints for a target endpoint (for replay).
    ///
    /// Filters out expired hints (older than `max_hint_window`).
    /// Returns empty if hint delivery is currently paused.
    pub fn drain_hints(&self, target: &Endpoint) -> Vec<Hint> {
        if self.is_paused() {
            debug!(target = %target, "Hint delivery is paused, skipping drain");
            return Vec::new();
        }

        let mut hints = self.hints.write();
        if let Some(queue) = hints.remove(target) {
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as i64;

            let window_ms = self.config.max_hint_window.as_millis() as i64;
            let cutoff = now_ms - window_ms;

            let (live, expired): (Vec<_>, Vec<_>) =
                queue.into_iter().partition(|h| h.created_at >= cutoff);

            let expired_count = expired.len() as u64;
            if expired_count > 0 {
                self.metrics.hints_expired.fetch_add(expired_count, Ordering::Relaxed);
                debug!(
                    target = %target,
                    expired = expired_count,
                    "Dropped expired hints during drain"
                );
            }

            let live_count = live.len() as u64;
            self.total_hints.fetch_sub(
                live_count + expired_count,
                Ordering::Relaxed,
            );

            info!(target = %target, count = live_count, "Draining hints for replay");
            live
        } else {
            Vec::new()
        }
    }

    /// Number of pending hints for a target.
    pub fn hint_count(&self, target: &Endpoint) -> usize {
        self.hints
            .read()
            .get(target)
            .map(|q| q.len())
            .unwrap_or(0)
    }

    /// Total number of stored hints across all targets.
    pub fn total_hints(&self) -> u64 {
        self.total_hints.load(Ordering::Relaxed)
    }

    /// Number of endpoints with pending hints.
    pub fn endpoints_with_hints(&self) -> usize {
        self.hints.read().len()
    }

    /// Pause hint delivery (during topology changes).
    pub fn pause(&self) {
        self.paused.store(true, Ordering::SeqCst);
        info!("Hint delivery paused");
    }

    /// Resume hint delivery after topology change completes.
    pub fn resume(&self) {
        self.paused.store(false, Ordering::SeqCst);
        info!("Hint delivery resumed");
    }

    /// Whether hint delivery is currently paused.
    pub fn is_paused(&self) -> bool {
        self.paused.load(Ordering::SeqCst)
    }

    /// Remove all hints for a node (e.g., after decommission).
    ///
    /// ## Java Oracle
    ///
    /// `HintsService.deleteAllHintsForEndpoint()`
    pub fn delete_hints_for(&self, target: &Endpoint) {
        let mut hints = self.hints.write();
        if let Some(queue) = hints.remove(target) {
            let count = queue.len() as u64;
            self.total_hints.fetch_sub(count, Ordering::Relaxed);
            info!(target = %target, count = count, "Deleted all hints for removed node");
        }
    }

    /// Purge expired hints across all endpoints.
    ///
    /// Returns the number of expired hints purged.
    pub fn purge_expired(&self) -> u64 {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;

        let window_ms = self.config.max_hint_window.as_millis() as i64;
        let cutoff = now_ms - window_ms;

        let mut total_purged = 0u64;
        let mut hints = self.hints.write();

        for queue in hints.values_mut() {
            let before = queue.len();
            queue.retain(|h| h.created_at >= cutoff);
            let purged = (before - queue.len()) as u64;
            total_purged += purged;
        }

        // Remove empty queues
        hints.retain(|_, q| !q.is_empty());

        if total_purged > 0 {
            self.total_hints.fetch_sub(total_purged, Ordering::Relaxed);
            self.metrics.hints_expired.fetch_add(total_purged, Ordering::Relaxed);
            info!(purged = total_purged, "Purged expired hints");
        }

        total_purged
    }

    /// Get a snapshot of hint statistics per endpoint.
    pub fn stats_per_endpoint(&self) -> HashMap<Endpoint, usize> {
        self.hints
            .read()
            .iter()
            .map(|(ep, q)| (*ep, q.len()))
            .collect()
    }
}

// ─── Hinted Handoff Manager ─────────────────────────────────────

/// High-level manager for hinted handoff lifecycle.
///
/// Wraps `HintStore` with automatic replay coordination.
///
/// ## Java Oracle
///
/// `org.apache.cassandra.hints.HintsService`
pub struct HintedHandoffManager {
    store: Arc<HintStore>,
    config: HintConfig,
}

impl HintedHandoffManager {
    pub fn new(config: HintConfig) -> Self {
        let store = Arc::new(HintStore::with_config(config.clone()));
        Self { store, config }
    }

    /// Get the underlying hint store.
    pub fn store(&self) -> &Arc<HintStore> {
        &self.store
    }

    /// Check if a node has pending hints.
    pub fn has_hints_for(&self, target: &Endpoint) -> bool {
        self.store.hint_count(target) > 0
    }

    /// Called when a node comes back online.
    ///
    /// Drains and returns all hints for the target, filtering out hints for
    /// partitions the node no longer owns due to topology changes.
    pub fn on_node_recovered(
        &self,
        target: &Endpoint,
        snapshot: &cassandra_cluster_metadata::ClusterSnapshot,
        snitch: &dyn cassandra_cluster_metadata::Snitch,
        strategies: &HashMap<String, Box<dyn cassandra_cluster_metadata::ReplicationStrategy>>,
    ) -> Vec<Hint> {
        if !self.config.enabled {
            return Vec::new();
        }
        
        let mut valid_hints = Vec::new();
        for hint in self.store.drain_hints(target) {
            let ks = &hint.mutation.keyspace;
            if let Some(strategy) = strategies.get(ks) {
                let replicas = snapshot.replicas_for_key(&hint.mutation.partition_key, strategy.as_ref(), snitch);
                if replicas.contains(target) {
                    valid_hints.push(hint);
                } else {
                    tracing::debug!(
                        target = %target,
                        keyspace = %ks,
                        "Hint discarded during replay: target no longer a replica for partition"
                    );
                    // Drop hint - Anti-entropy (repair) will synchronize the new replicas.
                }
            } else {
                // Keep the hint if we don't know the keyspace strategy (safe fallback).
                valid_hints.push(hint);
            }
        }
        
        valid_hints
    }

    /// Called when a node is permanently removed.
    pub fn on_node_removed(&self, target: &Endpoint) {
        self.store.delete_hints_for(target);
    }

    /// Run periodic maintenance: purge expired hints.
    pub fn run_maintenance(&self) -> u64 {
        self.store.purge_expired()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::write::{CellMutation, CoordinatedMutation, MutationRow};
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    fn ep(port: u16) -> Endpoint {
        Endpoint::new(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            port,
        ))
    }

    fn test_mutation() -> CoordinatedMutation {
        CoordinatedMutation::simple(
            "ks".to_string(),
            "t".to_string(),
            b"key".to_vec(),
            vec![],
            1000,
        )
    }

    #[test]
    fn store_and_drain() {
        let store = HintStore::new(100);
        store.store_hint(ep(7002), test_mutation());
        store.store_hint(ep(7002), test_mutation());

        assert_eq!(store.hint_count(&ep(7002)), 2);
        assert_eq!(store.total_hints(), 2);

        let hints = store.drain_hints(&ep(7002));
        assert_eq!(hints.len(), 2);
        assert_eq!(store.hint_count(&ep(7002)), 0);
        assert_eq!(store.total_hints(), 0);
    }

    #[test]
    fn max_hints_drops_oldest() {
        let store = HintStore::new(2);
        store.store_hint(ep(7002), test_mutation());
        store.store_hint(ep(7002), test_mutation());
        store.store_hint(ep(7002), test_mutation()); // drops oldest

        assert_eq!(store.hint_count(&ep(7002)), 2);
    }

    #[test]
    fn multiple_endpoints() {
        let store = HintStore::new(100);
        store.store_hint(ep(7002), test_mutation());
        store.store_hint(ep(7003), test_mutation());

        assert_eq!(store.endpoints_with_hints(), 2);
        assert_eq!(store.hint_count(&ep(7002)), 1);
        assert_eq!(store.hint_count(&ep(7003)), 1);
    }

    #[test]
    fn pause_prevents_drain() {
        let store = HintStore::new(100);
        store.store_hint(ep(7002), test_mutation());

        store.pause();
        assert!(store.is_paused());
        let hints = store.drain_hints(&ep(7002));
        assert!(hints.is_empty()); // paused, no drain

        store.resume();
        let hints = store.drain_hints(&ep(7002));
        assert_eq!(hints.len(), 1);
    }

    #[test]
    fn delete_hints_for_node() {
        let store = HintStore::new(100);
        store.store_hint(ep(7002), test_mutation());
        store.store_hint(ep(7002), test_mutation());
        assert_eq!(store.total_hints(), 2);

        store.delete_hints_for(&ep(7002));
        assert_eq!(store.hint_count(&ep(7002)), 0);
        assert_eq!(store.total_hints(), 0);
    }

    #[test]
    fn disabled_rejects_hints() {
        let config = HintConfig {
            enabled: false,
            ..HintConfig::default()
        };
        let store = HintStore::with_config(config);

        let stored = store.store_hint(ep(7002), test_mutation());
        assert!(!stored);
        assert_eq!(store.total_hints(), 0);
    }

    #[test]
    fn total_capacity_limit() {
        let config = HintConfig {
            max_total_hints: 2,
            max_hints_per_endpoint: 100,
            ..HintConfig::default()
        };
        let store = HintStore::with_config(config);

        assert!(store.store_hint(ep(7002), test_mutation()));
        assert!(store.store_hint(ep(7003), test_mutation()));
        assert!(!store.store_hint(ep(7004), test_mutation())); // exceeds total
    }

    #[test]
    fn hint_metrics() {
        let store = HintStore::new(100);
        store.store_hint(ep(7002), test_mutation());
        store.store_hint(ep(7002), test_mutation());

        assert_eq!(store.metrics.hints_created.load(Ordering::Relaxed), 2);

        store.drain_hints(&ep(7002));
        // hints_replayed would be incremented by the dispatcher, not the store
    }

    #[test]
    fn stats_per_endpoint() {
        let store = HintStore::new(100);
        store.store_hint(ep(7002), test_mutation());
        store.store_hint(ep(7002), test_mutation());
        store.store_hint(ep(7003), test_mutation());

        let stats = store.stats_per_endpoint();
        assert_eq!(stats[&ep(7002)], 2);
        assert_eq!(stats[&ep(7003)], 1);
    }

    // ── HintedHandoffManager tests ───────────────────────────────

    #[test]
    fn manager_node_recovered() {
        use cassandra_cluster_metadata::{ClusterMetadata, NodeId, NodeInfo, SimpleStrategy, SimpleSnitch};
        use cassandra_common::Token;
        
        let mgr = HintedHandoffManager::new(HintConfig::default());
        mgr.store().store_hint(ep(7002), test_mutation());
        mgr.store().store_hint(ep(7002), test_mutation());

        assert!(mgr.has_hints_for(&ep(7002)));

        let node = NodeInfo::new(
            NodeId::random(),
            ep(7002),
            "dc1",
            "rack1",
            vec![Token::from_raw(0)],
        );
        let cm = ClusterMetadata::new(node);
        let snapshot = cm.snapshot();
        let snitch = SimpleSnitch;
        let mut strategies: HashMap<String, Box<dyn cassandra_cluster_metadata::ReplicationStrategy>> = HashMap::new();
        strategies.insert("ks".to_string(), Box::new(SimpleStrategy::new(1)));

        let hints = mgr.on_node_recovered(&ep(7002), &snapshot, &snitch, &strategies);
        assert_eq!(hints.len(), 2);
        assert!(!mgr.has_hints_for(&ep(7002)));
    }

    #[test]
    fn manager_node_removed() {
        let mgr = HintedHandoffManager::new(HintConfig::default());
        mgr.store().store_hint(ep(7002), test_mutation());

        mgr.on_node_removed(&ep(7002));
        assert!(!mgr.has_hints_for(&ep(7002)));
    }
}
