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

use std::collections::{HashMap, HashSet, VecDeque};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::time::Duration;

use parking_lot::RwLock;
use tracing::{debug, info, warn};

use crate::hint_segment::{HintSegmentManager, HintSegmentReader, HintSegmentWriter};
use crate::write::CoordinatedMutation;
use cassandra_cluster_metadata::Endpoint;

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
    /// Number of hints currently being delivered.
    pub hints_in_progress: AtomicU64,
    /// Earliest hint creation time (epoch millis), or 0 if no hints.
    pub oldest_hint_timestamp: AtomicI64,
    /// Estimated total size of all stored hints in bytes.
    pub hint_store_size_bytes: AtomicU64,
}

impl HintMetrics {
    pub fn new() -> Self {
        Self {
            hints_created: AtomicU64::new(0),
            hints_replayed: AtomicU64::new(0),
            hints_expired: AtomicU64::new(0),
            hints_failed: AtomicU64::new(0),
            hints_dropped_overflow: AtomicU64::new(0),
            hints_in_progress: AtomicU64::new(0),
            oldest_hint_timestamp: AtomicI64::new(0),
            hint_store_size_bytes: AtomicU64::new(0),
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
    /// Optional segment manager for disk persistence.
    segment_manager: Option<Arc<HintSegmentManager>>,
    /// Active segment writers per target endpoint (by address string).
    active_writers: RwLock<HashMap<String, HintSegmentWriter>>,
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
        let segment_manager = config
            .hints_directory
            .as_ref()
            .map(|dir| Arc::new(HintSegmentManager::new(dir.clone())));
        Self {
            hints: Arc::new(RwLock::new(HashMap::new())),
            config,
            next_id: AtomicU64::new(1),
            total_hints: AtomicU64::new(0),
            paused: AtomicBool::new(false),
            metrics: Arc::new(HintMetrics::new()),
            segment_manager,
            active_writers: RwLock::new(HashMap::new()),
        }
    }

    /// Enable disk persistence with the given hints directory.
    pub fn with_persistence(mut self, hints_dir: PathBuf) -> Self {
        self.segment_manager = Some(Arc::new(HintSegmentManager::new(hints_dir)));
        self
    }

    /// Get the hint store configuration.
    pub fn config(&self) -> &HintConfig {
        &self.config
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
            self.metrics
                .hints_dropped_overflow
                .fetch_add(1, Ordering::Relaxed);
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

        {
            let mut hints = self.hints.write();
            let queue = hints.entry(target).or_default();

            if queue.len() >= self.config.max_hints_per_endpoint {
                queue.pop_front(); // Drop oldest hint
                debug!(target = %target, "Hint store full for endpoint, dropping oldest");
                self.metrics
                    .hints_dropped_overflow
                    .fetch_add(1, Ordering::Relaxed);
            } else {
                self.total_hints.fetch_add(1, Ordering::Relaxed);
            }

            // Estimate size for metrics.
            let estimated_size = serde_json::to_vec(&hint)
                .map(|v| v.len() as u64)
                .unwrap_or(256);
            self.metrics
                .hint_store_size_bytes
                .fetch_add(estimated_size, Ordering::Relaxed);

            queue.push_back(hint.clone());
            self.metrics.hints_created.fetch_add(1, Ordering::Relaxed);
            self.update_oldest_hint_timestamp_locked(&hints);
        }
        // hints write lock is dropped here.

        // Persist to disk if segment manager is configured.
        if let Some(ref mgr) = self.segment_manager {
            let target_id = format!("{}", target);
            let mut writers = self.active_writers.write();
            let written = {
                let writer = if !writers.contains_key(&target_id) {
                    match mgr.writer_for(&target_id) {
                        Ok(w) => {
                            writers.insert(target_id.clone(), w);
                            writers.get_mut(&target_id).unwrap()
                        }
                        Err(e) => {
                            warn!(error = %e, "Failed to create hint segment writer");
                            return true;
                        }
                    }
                } else {
                    writers.get_mut(&target_id).unwrap()
                };
                writer.append(&hint)
            };
            match written {
                Ok(false) => {
                    // Segment needs rotation.
                    drop(writers.remove(&target_id));
                    match mgr.writer_for(&target_id) {
                        Ok(mut new_writer) => {
                            let _ = new_writer.append(&hint);
                            writers.insert(target_id, new_writer);
                        }
                        Err(e) => {
                            warn!(error = %e, "Failed to rotate hint segment writer");
                        }
                    }
                }
                Err(e) => {
                    warn!(error = %e, "Failed to persist hint to segment");
                }
                Ok(true) => { /* success */ }
            }
        }

        true
    }

    /// Recalculate the oldest hint timestamp from all queues.
    /// IMPORTANT: caller must NOT hold self.hints lock when calling this.
    fn update_oldest_hint_timestamp(&self) {
        let hints = self.hints.read();
        let oldest = hints
            .values()
            .filter_map(|q| q.front().map(|h| h.created_at))
            .min()
            .unwrap_or(0);
        self.metrics
            .oldest_hint_timestamp
            .store(oldest, Ordering::Relaxed);
    }

    /// Compute and store the oldest hint timestamp from a locked guard.
    fn update_oldest_hint_timestamp_locked(&self, hints: &HashMap<Endpoint, VecDeque<Hint>>) {
        let oldest = hints
            .values()
            .filter_map(|q| q.front().map(|h| h.created_at))
            .min()
            .unwrap_or(0);
        self.metrics
            .oldest_hint_timestamp
            .store(oldest, Ordering::Relaxed);
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
                self.metrics
                    .hints_expired
                    .fetch_add(expired_count, Ordering::Relaxed);
                debug!(
                    target = %target,
                    expired = expired_count,
                    "Dropped expired hints during drain"
                );
            }

            let live_count = live.len() as u64;
            self.total_hints
                .fetch_sub(live_count + expired_count, Ordering::Relaxed);

            // Update metrics.
            let drained_size: u64 = live
                .iter()
                .map(|h| serde_json::to_vec(h).map(|v| v.len() as u64).unwrap_or(256))
                .sum();
            self.metrics.hint_store_size_bytes.fetch_sub(
                drained_size.min(self.metrics.hint_store_size_bytes.load(Ordering::Relaxed)),
                Ordering::Relaxed,
            );

            info!(target = %target, count = live_count, "Draining hints for replay");
            self.update_oldest_hint_timestamp_locked(&hints);
            live
        } else {
            // In-memory queue is empty; try reading from segment files.
            drop(hints);
            self.drain_from_segments(target)
        }
    }

    /// Requeue hints that were drained but not delivered.
    ///
    /// Requeued hints keep their original IDs and creation timestamps. They are
    /// placed ahead of newer hints for the same target so a transient delivery
    /// failure does not let older writes fall behind later ones.
    pub fn requeue_hints(&self, target: Endpoint, hints_to_requeue: Vec<Hint>) -> usize {
        if hints_to_requeue.is_empty() {
            return 0;
        }

        let mut requeued = 0usize;
        let mut hints = self.hints.write();
        let queue = hints.entry(target).or_default();

        for hint in hints_to_requeue.into_iter().rev() {
            if queue.len() >= self.config.max_hints_per_endpoint {
                self.metrics
                    .hints_dropped_overflow
                    .fetch_add(1, Ordering::Relaxed);
                continue;
            }

            let estimated_size = serde_json::to_vec(&hint)
                .map(|v| v.len() as u64)
                .unwrap_or(256);
            self.metrics
                .hint_store_size_bytes
                .fetch_add(estimated_size, Ordering::Relaxed);

            queue.push_front(hint);
            requeued += 1;
        }

        if requeued > 0 {
            self.total_hints
                .fetch_add(requeued as u64, Ordering::Relaxed);
            self.update_oldest_hint_timestamp_locked(&hints);
        }

        requeued
    }

    /// Read hints from on-disk segments for the given target when in-memory queue is empty.
    fn drain_from_segments(&self, target: &Endpoint) -> Vec<Hint> {
        let mgr = match self.segment_manager.as_ref() {
            Some(m) => m,
            None => return Vec::new(),
        };

        let target_id = format!("{}", target);

        // Close active writer for this target before reading.
        {
            let mut writers = self.active_writers.write();
            if let Some(mut writer) = writers.remove(&target_id) {
                let _ = writer.sync();
            }
        }

        let segments = match mgr.segments_for(&target_id) {
            Ok(s) => s,
            Err(e) => {
                warn!(error = %e, "Failed to list hint segments for drain");
                return Vec::new();
            }
        };

        if segments.is_empty() {
            return Vec::new();
        }

        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        let window_ms = self.config.max_hint_window.as_millis() as i64;
        let cutoff = now_ms - window_ms;

        let mut live = Vec::new();
        let mut expired_count = 0u64;

        for seg_path in &segments {
            match HintSegmentReader::open(seg_path) {
                Ok(mut reader) => match reader.read_all() {
                    Ok(hints) => {
                        for h in hints {
                            if h.created_at >= cutoff {
                                live.push(h);
                            } else {
                                expired_count += 1;
                            }
                        }
                    }
                    Err(e) => {
                        warn!(error = %e, path = %seg_path.display(), "Failed to read hint segment");
                    }
                },
                Err(e) => {
                    warn!(error = %e, path = %seg_path.display(), "Failed to open hint segment");
                }
            }
            // Delete segment after reading.
            let _ = mgr.delete_segment(seg_path);
        }

        if expired_count > 0 {
            self.metrics
                .hints_expired
                .fetch_add(expired_count, Ordering::Relaxed);
        }

        info!(target = %target, count = live.len(), "Drained hints from segments");
        live
    }

    /// Number of pending hints for a target.
    pub fn hint_count(&self, target: &Endpoint) -> usize {
        self.hints.read().get(target).map(|q| q.len()).unwrap_or(0)
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

            // Estimate size for metrics.
            let size: u64 = queue
                .iter()
                .map(|h| serde_json::to_vec(h).map(|v| v.len() as u64).unwrap_or(256))
                .sum();
            self.metrics.hint_store_size_bytes.fetch_sub(
                size.min(self.metrics.hint_store_size_bytes.load(Ordering::Relaxed)),
                Ordering::Relaxed,
            );

            info!(target = %target, count = count, "Deleted all hints for removed node");
        }
        drop(hints);

        // Also delete segment files for this target.
        if let Some(ref mgr) = self.segment_manager {
            let target_id = format!("{}", target);
            let mut writers = self.active_writers.write();
            writers.remove(&target_id);
            let _ = mgr.delete_all_for(&target_id);
        }

        self.update_oldest_hint_timestamp();
    }

    /// Clear all hints for all endpoints (nodetool truncatehints equivalent).
    ///
    /// ## Java Oracle
    ///
    /// `HintsService.truncateAllHints()`
    pub fn truncate_all_hints(&self) {
        let mut hints = self.hints.write();
        let total = self.total_hints.load(Ordering::Relaxed);
        hints.clear();
        self.total_hints.store(0, Ordering::Relaxed);
        self.metrics
            .hint_store_size_bytes
            .store(0, Ordering::Relaxed);
        self.metrics
            .oldest_hint_timestamp
            .store(0, Ordering::Relaxed);
        drop(hints);

        // Clear all segment writers and files.
        if let Some(ref mgr) = self.segment_manager {
            let mut writers = self.active_writers.write();
            writers.clear();
            // Delete all segment files in the hints directory.
            if let Ok(entries) = std::fs::read_dir(mgr.hints_dir()) {
                for entry in entries.flatten() {
                    if entry.path().extension().is_some_and(|e| e == "hints") {
                        let _ = std::fs::remove_file(entry.path());
                    }
                }
            }
        }

        info!(total = total, "Truncated all hints");
    }

    /// Purge hints for nodes no longer in the live set.
    ///
    /// Removes all in-memory hints and segment files for any endpoint
    /// not in the given set of current live endpoints.
    pub fn purge_hints_for_removed_nodes(&self, live_endpoints: &HashSet<Endpoint>) {
        let targets_to_remove: Vec<Endpoint> = {
            let hints = self.hints.read();
            hints
                .keys()
                .filter(|ep| !live_endpoints.contains(ep))
                .copied()
                .collect()
        };

        for target in &targets_to_remove {
            self.delete_hints_for(target);
            info!(target = %target, "Purged hints for removed node");
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
            self.metrics
                .hints_expired
                .fetch_add(total_purged, Ordering::Relaxed);
            info!(purged = total_purged, "Purged expired hints");
        }

        self.update_oldest_hint_timestamp_locked(&hints);
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
                let replicas = snapshot.replicas_for_key(
                    &hint.mutation.partition_key,
                    strategy.as_ref(),
                    snitch,
                );
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

    /// Called on topology changes: re-evaluates all pending hints across all targets.
    ///
    /// For each target, verifies hints are still for partitions the target owns.
    /// Drops hints where the target no longer owns the partition.
    ///
    /// ## Java Oracle
    ///
    /// `HintsService.onTopologyChange()`
    pub fn on_topology_change(
        &self,
        snapshot: &cassandra_cluster_metadata::ClusterSnapshot,
        snitch: &dyn cassandra_cluster_metadata::Snitch,
        strategies: &HashMap<String, Box<dyn cassandra_cluster_metadata::ReplicationStrategy>>,
    ) {
        self.store.pause();

        // Collect all targets with hints.
        let targets: Vec<Endpoint> = {
            let hints = self.store.hints.read();
            hints.keys().copied().collect()
        };

        let mut total_dropped = 0u64;

        for target in targets {
            let mut hints_guard = self.store.hints.write();
            if let Some(queue) = hints_guard.get_mut(&target) {
                let before = queue.len();
                queue.retain(|hint| {
                    let ks = &hint.mutation.keyspace;
                    if let Some(strategy) = strategies.get(ks) {
                        let replicas = snapshot.replicas_for_key(
                            &hint.mutation.partition_key,
                            strategy.as_ref(),
                            snitch,
                        );
                        replicas.contains(&target)
                    } else {
                        true // keep if strategy unknown
                    }
                });
                let dropped = before - queue.len();
                total_dropped += dropped as u64;
            }
            // Remove empty queues.
            if hints_guard.get(&target).is_some_and(|q| q.is_empty()) {
                hints_guard.remove(&target);
            }
        }

        if total_dropped > 0 {
            self.store
                .total_hints
                .fetch_sub(total_dropped, Ordering::Relaxed);
            info!(
                dropped = total_dropped,
                "Dropped hints during topology change"
            );
        }

        self.store.update_oldest_hint_timestamp();
        self.store.resume();
    }

    /// Purge all hints for endpoints not in the current live set.
    pub fn purge_hints_for_removed_nodes(&self, live_endpoints: &HashSet<Endpoint>) {
        self.store.purge_hints_for_removed_nodes(live_endpoints);
    }

    /// Run periodic maintenance: purge expired hints.
    pub fn run_maintenance(&self) -> u64 {
        self.store.purge_expired()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::write::CoordinatedMutation;
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

    #[test]
    fn requeue_hints_preserves_original_order_ahead_of_newer_hints() {
        let store = HintStore::new(100);
        let target = ep(7002);

        store.store_hint(target, test_mutation());
        store.store_hint(target, test_mutation());
        let drained = store.drain_hints(&target);
        assert_eq!(
            drained.iter().map(|hint| hint.hint_id).collect::<Vec<_>>(),
            vec![1, 2]
        );

        store.store_hint(target, test_mutation());
        assert_eq!(store.requeue_hints(target, drained), 2);
        assert_eq!(store.total_hints(), 3);

        let redrained = store.drain_hints(&target);
        assert_eq!(
            redrained
                .iter()
                .map(|hint| hint.hint_id)
                .collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
    }

    // ── HintedHandoffManager tests ───────────────────────────────

    #[test]
    fn manager_node_recovered() {
        use cassandra_cluster_metadata::{
            ClusterMetadata, NodeId, NodeInfo, SimpleSnitch, SimpleStrategy,
        };
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
        let mut strategies: HashMap<
            String,
            Box<dyn cassandra_cluster_metadata::ReplicationStrategy>,
        > = HashMap::new();
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

    // ── WU-10: Persistence wiring tests ─────────────────────────

    #[test]
    fn with_persistence_builder() {
        let dir = tempfile::tempdir().unwrap();
        let store = HintStore::new(100).with_persistence(dir.path().to_path_buf());
        assert!(store.segment_manager.is_some());
    }

    #[test]
    fn store_hint_persists_to_disk() {
        let dir = tempfile::tempdir().unwrap();
        let config = HintConfig {
            hints_directory: Some(dir.path().to_path_buf()),
            ..HintConfig::default()
        };
        let store = HintStore::with_config(config);

        store.store_hint(ep(7002), test_mutation());
        store.store_hint(ep(7002), test_mutation());

        // Verify segment files were created.
        let mgr = store.segment_manager.as_ref().unwrap();
        let target_id = format!("{}", ep(7002));
        // Sync the writer before checking.
        {
            let mut writers = store.active_writers.write();
            if let Some(w) = writers.get_mut(&target_id) {
                w.sync().unwrap();
            }
        }
        let segments = mgr.segments_for(&target_id).unwrap();
        assert!(!segments.is_empty());
    }

    #[test]
    fn drain_from_segments_when_memory_empty() {
        let dir = tempfile::tempdir().unwrap();
        let config = HintConfig {
            hints_directory: Some(dir.path().to_path_buf()),
            ..HintConfig::default()
        };
        let store = HintStore::with_config(config);

        // Write hints directly to a segment file, bypassing in-memory store.
        let target_id = format!("{}", ep(7002));
        let mgr = store.segment_manager.as_ref().unwrap();
        let mut writer = mgr.writer_for(&target_id).unwrap();
        let hint = Hint {
            target: ep(7002),
            mutation: test_mutation(),
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64,
            hint_id: 42,
        };
        writer.append(&hint).unwrap();
        writer.sync().unwrap();
        drop(writer);

        // In-memory is empty but segments exist.
        assert_eq!(store.hint_count(&ep(7002)), 0);
        let drained = store.drain_hints(&ep(7002));
        assert_eq!(drained.len(), 1);
        assert_eq!(drained[0].hint_id, 42);
    }

    // ── WU-12: Topology awareness tests ─────────────────────────

    #[test]
    fn on_topology_change_drops_invalid_hints() {
        use cassandra_cluster_metadata::{
            ClusterMetadata, NodeId, NodeInfo, SimpleSnitch, SimpleStrategy,
        };
        use cassandra_common::Token;

        let mgr = HintedHandoffManager::new(HintConfig::default());

        // Store a hint for ep(7002) with keyspace "ks"
        mgr.store().store_hint(ep(7002), test_mutation());
        assert_eq!(mgr.store().hint_count(&ep(7002)), 1);

        // Build a cluster where ep(7002) is NOT a replica for the partition.
        let node = NodeInfo::new(
            NodeId::random(),
            ep(7003), // only node 7003 in the ring
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

        mgr.on_topology_change(&snapshot, &snitch, &strategies);

        // Hint should have been dropped since ep(7002) is not a replica.
        assert_eq!(mgr.store().hint_count(&ep(7002)), 0);
    }

    #[test]
    fn purge_hints_for_removed_nodes() {
        let mgr = HintedHandoffManager::new(HintConfig::default());

        mgr.store().store_hint(ep(7002), test_mutation());
        mgr.store().store_hint(ep(7003), test_mutation());
        mgr.store().store_hint(ep(7004), test_mutation());

        // Only 7002 and 7003 are live.
        let mut live = HashSet::new();
        live.insert(ep(7002));
        live.insert(ep(7003));

        mgr.purge_hints_for_removed_nodes(&live);

        assert!(mgr.has_hints_for(&ep(7002)));
        assert!(mgr.has_hints_for(&ep(7003)));
        assert!(!mgr.has_hints_for(&ep(7004)));
    }

    // ── WU-13: Metrics and operational controls tests ───────────

    #[test]
    fn truncate_all_hints() {
        let store = HintStore::new(100);
        store.store_hint(ep(7002), test_mutation());
        store.store_hint(ep(7003), test_mutation());
        store.store_hint(ep(7004), test_mutation());
        assert_eq!(store.total_hints(), 3);

        store.truncate_all_hints();
        assert_eq!(store.total_hints(), 0);
        assert_eq!(store.endpoints_with_hints(), 0);
        assert_eq!(
            store.metrics.hint_store_size_bytes.load(Ordering::Relaxed),
            0
        );
        assert_eq!(
            store.metrics.oldest_hint_timestamp.load(Ordering::Relaxed),
            0
        );
    }

    #[test]
    fn hint_store_size_bytes_tracked() {
        let store = HintStore::new(100);
        assert_eq!(
            store.metrics.hint_store_size_bytes.load(Ordering::Relaxed),
            0
        );

        store.store_hint(ep(7002), test_mutation());
        let size_after_one = store.metrics.hint_store_size_bytes.load(Ordering::Relaxed);
        assert!(size_after_one > 0);

        store.store_hint(ep(7002), test_mutation());
        let size_after_two = store.metrics.hint_store_size_bytes.load(Ordering::Relaxed);
        assert!(size_after_two > size_after_one);

        store.drain_hints(&ep(7002));
        let size_after_drain = store.metrics.hint_store_size_bytes.load(Ordering::Relaxed);
        assert_eq!(size_after_drain, 0);
    }

    #[test]
    fn oldest_hint_timestamp_tracked() {
        let store = HintStore::new(100);
        assert_eq!(
            store.metrics.oldest_hint_timestamp.load(Ordering::Relaxed),
            0
        );

        store.store_hint(ep(7002), test_mutation());
        let ts = store.metrics.oldest_hint_timestamp.load(Ordering::Relaxed);
        assert!(ts > 0);

        store.drain_hints(&ep(7002));
        let ts_after = store.metrics.oldest_hint_timestamp.load(Ordering::Relaxed);
        assert_eq!(ts_after, 0);
    }

    #[test]
    fn config_accessor() {
        let config = HintConfig {
            max_hints_per_endpoint: 42,
            ..HintConfig::default()
        };
        let store = HintStore::with_config(config);
        assert_eq!(store.config().max_hints_per_endpoint, 42);
    }

    #[test]
    fn truncate_all_with_persistence() {
        let dir = tempfile::tempdir().unwrap();
        let config = HintConfig {
            hints_directory: Some(dir.path().to_path_buf()),
            ..HintConfig::default()
        };
        let store = HintStore::with_config(config);

        store.store_hint(ep(7002), test_mutation());
        store.store_hint(ep(7003), test_mutation());

        // Sync writers.
        {
            let mut writers = store.active_writers.write();
            for w in writers.values_mut() {
                w.sync().unwrap();
            }
        }

        // Verify files exist.
        let mgr = store.segment_manager.as_ref().unwrap();
        assert!(mgr.total_disk_usage().unwrap() > 0);

        store.truncate_all_hints();

        assert_eq!(store.total_hints(), 0);
        assert_eq!(mgr.total_disk_usage().unwrap(), 0);
    }
}
