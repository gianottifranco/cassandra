// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.
// SPDX-License-Identifier: Apache-2.0

//! Batch coordination: logged/unlogged/counter batches.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.batchlog.BatchlogManager`
//! - `org.apache.cassandra.batchlog.Batch`
//! - `org.apache.cassandra.service.StorageProxy.mutateWithTriggers()`
//!
//! ## Architecture
//!
//! Logged batches follow the batchlog protocol:
//! 1. Store batch entry to batchlog replicas (prefer non-local DC)
//! 2. Execute all mutations
//! 3. Remove batchlog entry after success
//!
//! Unlogged batches skip batchlog but group mutations for efficiency.
//! Counter batches are routed differently (counter leader logic).

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use parking_lot::RwLock;
use tracing::{debug, info, warn};
use uuid::Uuid;

use cassandra_cluster_metadata::{ReplicationStrategy, Snitch};

use crate::consistency::ConsistencyLevel;
use crate::write::{CoordinatedMutation, WriteCoordinator, WriteError, WriteResult};

// ─── Batch Types ─────────────────────────────────────────────────

/// Type of batch operation.
///
/// ## Java Oracle
///
/// `org.apache.cassandra.cql3.statements.BatchStatement.Type`
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum BatchType {
    /// Logged batch: batchlog ensures atomicity.
    Logged,
    /// Unlogged batch: no batchlog, mutations may partially apply.
    Unlogged,
    /// Counter batch: all mutations must be counter increments.
    Counter,
}

impl BatchType {
    /// Protocol byte for this batch type.
    pub fn protocol_code(&self) -> u8 {
        match self {
            Self::Logged => 0,
            Self::Unlogged => 1,
            Self::Counter => 2,
        }
    }

    /// Parse from protocol byte.
    pub fn from_protocol_code(code: u8) -> Option<Self> {
        match code {
            0 => Some(Self::Logged),
            1 => Some(Self::Unlogged),
            2 => Some(Self::Counter),
            _ => None,
        }
    }
}

impl std::fmt::Display for BatchType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Logged => write!(f, "LOGGED"),
            Self::Unlogged => write!(f, "UNLOGGED"),
            Self::Counter => write!(f, "COUNTER"),
        }
    }
}

// ─── Batch Entry ─────────────────────────────────────────────────

/// A batch log entry.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct BatchEntry {
    /// Unique batch ID.
    pub id: Uuid,
    /// Batch type.
    pub batch_type: BatchType,
    /// All mutations in this batch.
    pub mutations: Vec<CoordinatedMutation>,
    /// Creation timestamp (epoch millis).
    pub created_at: i64,
    /// Version for serialization compatibility.
    pub version: u32,
}

// ─── Batch Guardrails ────────────────────────────────────────────

/// Configurable guardrails for batch size.
///
/// ## Java Oracle
///
/// `cassandra.yaml`: `batch_size_warn_threshold_in_kb`,
/// `batch_size_fail_threshold_in_kb`
#[derive(Debug, Clone)]
pub struct BatchGuardrails {
    /// Warn when batch exceeds this size (bytes). Default: 5 KiB.
    pub warn_threshold: usize,
    /// Reject batch when it exceeds this size (bytes). Default: 50 KiB.
    pub fail_threshold: usize,
    /// Warn when batch contains mutations to this many partitions.
    pub warn_partition_count: usize,
    /// Maximum number of mutations in a single batch.
    pub max_mutations_per_batch: usize,
}

impl Default for BatchGuardrails {
    fn default() -> Self {
        Self {
            warn_threshold: 5 * 1024,
            fail_threshold: 50 * 1024,
            warn_partition_count: 128,
            max_mutations_per_batch: 65535,
        }
    }
}

// ─── Batch Log Manager ──────────────────────────────────────────

/// Batch log manager: stores pending batch entries.
///
/// In production, batchlog entries are stored on two replicas in a
/// non-local DC. This implementation uses in-memory storage with
/// the correct protocol semantics.
///
/// ## Java Oracle
///
/// `org.apache.cassandra.batchlog.BatchlogManager`
pub struct BatchLogManager {
    entries: Arc<RwLock<HashMap<Uuid, BatchEntry>>>,
    /// Batchlog replay interval.
    replay_interval: Duration,
    /// Metrics.
    pub metrics: BatchLogMetrics,
}

/// Batch log metrics.
pub struct BatchLogMetrics {
    pub batches_stored: AtomicU64,
    pub batches_removed: AtomicU64,
    pub batches_replayed: AtomicU64,
    pub replay_failures: AtomicU64,
}

impl BatchLogMetrics {
    pub fn new() -> Self {
        Self {
            batches_stored: AtomicU64::new(0),
            batches_removed: AtomicU64::new(0),
            batches_replayed: AtomicU64::new(0),
            replay_failures: AtomicU64::new(0),
        }
    }
}

impl Default for BatchLogMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl BatchLogManager {
    pub fn new() -> Self {
        Self {
            entries: Arc::new(RwLock::new(HashMap::new())),
            replay_interval: Duration::from_secs(60),
            metrics: BatchLogMetrics::new(),
        }
    }

    pub fn with_replay_interval(mut self, interval: Duration) -> Self {
        self.replay_interval = interval;
        self
    }

    /// Store a new batch entry before executing mutations.
    pub fn store(&self, batch_type: BatchType, mutations: Vec<CoordinatedMutation>) -> Uuid {
        let id = Uuid::new_v4();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;

        let entry = BatchEntry {
            id,
            batch_type,
            mutations,
            created_at: now,
            version: 1,
        };

        self.entries.write().insert(id, entry);
        self.metrics.batches_stored.fetch_add(1, Ordering::Relaxed);
        debug!(batch_id = %id, "Batch log entry stored");
        id
    }

    /// Remove a batch entry after all mutations succeed.
    pub fn remove(&self, id: &Uuid) -> Option<BatchEntry> {
        let entry = self.entries.write().remove(id);
        if entry.is_some() {
            self.metrics.batches_removed.fetch_add(1, Ordering::Relaxed);
            debug!(batch_id = %id, "Batch log entry removed");
        }
        entry
    }

    /// Get a pending batch entry.
    pub fn get(&self, id: &Uuid) -> Option<BatchEntry> {
        self.entries.read().get(id).cloned()
    }

    /// Get all pending batch entries (for replay on recovery).
    pub fn pending_entries(&self) -> Vec<BatchEntry> {
        self.entries.read().values().cloned().collect()
    }

    /// Number of pending batch entries.
    pub fn pending_count(&self) -> usize {
        self.entries.read().len()
    }

    /// Get entries older than the given age (candidates for replay).
    ///
    /// ## Java Oracle
    ///
    /// `BatchlogManager.getExpiredBatches()`
    pub fn expired_entries(&self, max_age: Duration) -> Vec<BatchEntry> {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        let cutoff = now_ms - max_age.as_millis() as i64;

        self.entries
            .read()
            .values()
            .filter(|e| e.created_at < cutoff)
            .cloned()
            .collect()
    }
}

impl Default for BatchLogManager {
    fn default() -> Self {
        Self::new()
    }
}

// ─── Batch Coordinator ──────────────────────────────────────────

/// Coordinates batch execution with batchlog protocol.
///
/// ## Protocol (logged batches)
///
/// 1. Validate batch type and size against guardrails
/// 2. Store batchlog entry to batchlog replicas
/// 3. Execute all individual mutations at the given CL
/// 4. Remove batchlog entry
/// 5. Return aggregate result
///
/// ## Java Oracle
///
/// `StorageProxy.mutateWithTriggers()` →
/// `StorageProxy.syncWriteBatchedMutations()`
pub struct BatchCoordinator {
    write_coordinator: Arc<WriteCoordinator>,
    batchlog: Arc<BatchLogManager>,
    guardrails: BatchGuardrails,
    pub metrics: BatchCoordinatorMetrics,
}

pub struct BatchCoordinatorMetrics {
    pub batches_executed: AtomicU64,
    pub batches_failed: AtomicU64,
}

impl BatchCoordinatorMetrics {
    pub fn new() -> Self {
        Self {
            batches_executed: AtomicU64::new(0),
            batches_failed: AtomicU64::new(0),
        }
    }
}

impl Default for BatchCoordinatorMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl BatchCoordinator {
    pub fn new(write_coordinator: Arc<WriteCoordinator>, batchlog: Arc<BatchLogManager>) -> Self {
        Self {
            write_coordinator,
            batchlog,
            guardrails: BatchGuardrails::default(),
            metrics: BatchCoordinatorMetrics::new(),
        }
    }

    pub fn with_guardrails(mut self, guardrails: BatchGuardrails) -> Self {
        self.guardrails = guardrails;
        self
    }

    /// Execute a batch of mutations.
    ///
    /// For logged batches, follows the batchlog protocol.
    /// For unlogged batches, executes directly.
    /// Counter batches are validated and delegated appropriately.
    pub fn execute_batch(
        &self,
        batch_type: BatchType,
        mutations: Vec<CoordinatedMutation>,
        cl: ConsistencyLevel,
        strategy: &dyn ReplicationStrategy,
        snitch: &dyn Snitch,
    ) -> Result<Vec<WriteResult>, WriteError> {
        // 1. Validate
        self.validate_batch(&mutations, batch_type)?;

        // 2. Group mutations by partition for efficiency
        let mutation_groups = self.group_mutations(&mutations);
        let partition_count = mutation_groups.len();

        if partition_count > self.guardrails.warn_partition_count {
            warn!(
                partitions = partition_count,
                threshold = self.guardrails.warn_partition_count,
                "Batch spans many partitions, consider splitting"
            );
        }

        // 3. For logged batches, store batchlog entry first
        let batch_id = if batch_type == BatchType::Logged {
            Some(self.batchlog.store(batch_type, mutations.clone()))
        } else {
            None
        };

        // 4. Execute all mutations
        let mut results = Vec::with_capacity(mutations.len());
        for mutation in &mutations {
            match self
                .write_coordinator
                .coordinate_write(mutation, cl, strategy, snitch)
            {
                Ok(result) => results.push(result),
                Err(e) => {
                    self.metrics.batches_failed.fetch_add(1, Ordering::Relaxed);

                    // For logged batches, the batchlog entry remains for replay
                    if let Some(ref id) = batch_id {
                        info!(
                            batch_id = %id,
                            error = %e,
                            "Batch mutation failed, batchlog entry preserved for replay"
                        );
                    }
                    return Err(e);
                }
            }
        }

        // 5. Remove batchlog entry after all succeed
        if let Some(id) = batch_id {
            self.batchlog.remove(&id);
        }

        self.metrics
            .batches_executed
            .fetch_add(1, Ordering::Relaxed);

        Ok(results)
    }

    /// Validate batch constraints.
    fn validate_batch(
        &self,
        mutations: &[CoordinatedMutation],
        batch_type: BatchType,
    ) -> Result<(), WriteError> {
        if mutations.is_empty() {
            return Err(WriteError::Internal("Empty batch".to_string()));
        }

        // Max mutations per batch
        if mutations.len() > self.guardrails.max_mutations_per_batch {
            return Err(WriteError::Internal(format!(
                "Batch contains {} mutations, exceeds limit of {}",
                mutations.len(),
                self.guardrails.max_mutations_per_batch
            )));
        }

        // Check total size
        let total_size: usize = mutations.iter().map(|m| m.estimated_size()).sum();

        if total_size > self.guardrails.fail_threshold {
            return Err(WriteError::Internal(format!(
                "Batch too large: {} bytes exceeds fail threshold of {} bytes",
                total_size, self.guardrails.fail_threshold
            )));
        }

        if total_size > self.guardrails.warn_threshold {
            warn!(
                size = total_size,
                threshold = self.guardrails.warn_threshold,
                "Batch exceeds warn threshold"
            );
        }

        // Cross-keyspace detection for unlogged batches
        // Java: "Unlogged batch covering N partitions in N keyspaces ..."
        if batch_type == BatchType::Unlogged {
            let keyspaces: std::collections::HashSet<&str> =
                mutations.iter().map(|m| m.keyspace.as_str()).collect();
            if keyspaces.len() > 1 {
                warn!(
                    keyspace_count = keyspaces.len(),
                    keyspaces = ?keyspaces,
                    "Unlogged batch spans multiple keyspaces; \
                     this is usually a mistake. Consider using a LOGGED batch."
                );
            }
        }

        // Counter batches must only contain counter mutations
        if batch_type == BatchType::Counter {
            let all_counters = mutations
                .iter()
                .all(|m| m.kind == crate::write::MutationKind::Counter);
            if !all_counters {
                return Err(WriteError::Internal(
                    "Counter batch contains non-counter mutations".to_string(),
                ));
            }
        }

        // Non-counter batches must not contain counter mutations
        if batch_type != BatchType::Counter {
            let has_counters = mutations
                .iter()
                .any(|m| m.kind == crate::write::MutationKind::Counter);
            if has_counters {
                return Err(WriteError::Internal(
                    "Non-counter batch contains counter mutations".to_string(),
                ));
            }
        }

        Ok(())
    }

    /// Group mutations by target endpoint for batched sending.
    fn group_mutations<'a>(
        &self,
        mutations: &'a [CoordinatedMutation],
    ) -> HashMap<Vec<u8>, Vec<&'a CoordinatedMutation>> {
        let mut groups: HashMap<Vec<u8>, Vec<&CoordinatedMutation>> = HashMap::new();
        for m in mutations {
            groups.entry(m.partition_key.clone()).or_default().push(m);
        }
        groups
    }

    /// Replay expired batchlog entries (crash recovery).
    ///
    /// Called periodically by a background task.
    ///
    /// ## Java Oracle
    ///
    /// `BatchlogManager.replayFailedBatches()`
    pub fn replay_batchlog(
        &self,
        max_age: Duration,
        strategy: &dyn ReplicationStrategy,
        snitch: &dyn Snitch,
    ) -> (usize, usize) {
        let expired = self.batchlog.expired_entries(max_age);
        let total = expired.len();
        let mut replayed = 0;

        for entry in expired {
            info!(
                batch_id = %entry.id,
                mutations = entry.mutations.len(),
                "Replaying expired batchlog entry"
            );

            let mut all_ok = true;
            for mutation in &entry.mutations {
                // Use CL=ANY for replay — best effort
                if let Err(e) = self.write_coordinator.coordinate_write(
                    mutation,
                    ConsistencyLevel::Any,
                    strategy,
                    snitch,
                ) {
                    warn!(
                        batch_id = %entry.id,
                        error = %e,
                        "Failed to replay batch mutation"
                    );
                    all_ok = false;
                    self.batchlog
                        .metrics
                        .replay_failures
                        .fetch_add(1, Ordering::Relaxed);
                }
            }

            if all_ok {
                self.batchlog.remove(&entry.id);
                replayed += 1;
                self.batchlog
                    .metrics
                    .batches_replayed
                    .fetch_add(1, Ordering::Relaxed);
            }
        }

        (total, replayed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hints::HintStore;
    use crate::write::{CellMutation, MutationKind, MutationRow};
    use cassandra_cluster_metadata::{
        ClusterMetadata, Endpoint, NodeId, NodeInfo, SimpleSnitch, SimpleStrategy,
    };
    use cassandra_common::Token;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    fn ep(port: u16) -> Endpoint {
        Endpoint::new(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            port,
        ))
    }

    fn node(port: u16, tokens: Vec<i64>) -> NodeInfo {
        NodeInfo::new(
            NodeId::random(),
            ep(port),
            "dc1",
            "rack1",
            tokens.into_iter().map(Token::from_raw).collect(),
        )
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

    fn setup() -> (
        Arc<WriteCoordinator>,
        Arc<BatchLogManager>,
        BatchCoordinator,
    ) {
        let cm = Arc::new(ClusterMetadata::new(node(7001, vec![-100])));
        cm.update_node(node(7002, vec![0]));
        cm.update_node(node(7003, vec![100]));

        let hint_store = Arc::new(HintStore::new(1000));
        let wc = Arc::new(WriteCoordinator::new(Arc::clone(&cm), ep(7001), hint_store));
        let blm = Arc::new(BatchLogManager::new());
        let bc = BatchCoordinator::new(Arc::clone(&wc), Arc::clone(&blm));
        (wc, blm, bc)
    }

    // ── BatchLogManager tests ────────────────────────────────────

    #[test]
    fn store_and_remove() {
        let blm = BatchLogManager::new();

        let id = blm.store(BatchType::Logged, vec![test_mutation()]);
        assert_eq!(blm.pending_count(), 1);

        let entry = blm.remove(&id).unwrap();
        assert_eq!(entry.id, id);
        assert_eq!(blm.pending_count(), 0);
    }

    #[test]
    fn get_entry() {
        let blm = BatchLogManager::new();
        let id = blm.store(BatchType::Logged, vec![test_mutation(), test_mutation()]);

        let entry = blm.get(&id).unwrap();
        assert_eq!(entry.mutations.len(), 2);
        assert_eq!(entry.batch_type, BatchType::Logged);
    }

    #[test]
    fn pending_entries() {
        let blm = BatchLogManager::new();
        blm.store(BatchType::Logged, vec![test_mutation()]);
        blm.store(BatchType::Unlogged, vec![test_mutation()]);

        assert_eq!(blm.pending_entries().len(), 2);
    }

    // ── BatchType tests ──────────────────────────────────────────

    #[test]
    fn batch_type_round_trip() {
        for (code, expected) in [
            (0u8, BatchType::Logged),
            (1, BatchType::Unlogged),
            (2, BatchType::Counter),
        ] {
            assert_eq!(BatchType::from_protocol_code(code), Some(expected));
            assert_eq!(expected.protocol_code(), code);
        }
        assert_eq!(BatchType::from_protocol_code(3), None);
    }

    // ── BatchCoordinator tests ───────────────────────────────────

    #[test]
    fn execute_logged_batch() {
        let (_wc, blm, bc) = setup();
        let strategy = SimpleStrategy::new(3);
        let snitch = SimpleSnitch;

        let results = bc
            .execute_batch(
                BatchType::Logged,
                vec![test_mutation(), test_mutation()],
                ConsistencyLevel::One,
                &strategy,
                &snitch,
            )
            .unwrap();

        assert_eq!(results.len(), 2);
        // Batchlog should be cleaned up
        assert_eq!(blm.pending_count(), 0);
    }

    #[test]
    fn execute_unlogged_batch() {
        let (_wc, blm, bc) = setup();
        let strategy = SimpleStrategy::new(3);
        let snitch = SimpleSnitch;

        let results = bc
            .execute_batch(
                BatchType::Unlogged,
                vec![test_mutation()],
                ConsistencyLevel::Quorum,
                &strategy,
                &snitch,
            )
            .unwrap();

        assert_eq!(results.len(), 1);
        // No batchlog for unlogged
        assert_eq!(blm.pending_count(), 0);
    }

    #[test]
    fn empty_batch_rejected() {
        let (_wc, _blm, bc) = setup();
        let strategy = SimpleStrategy::new(3);
        let snitch = SimpleSnitch;

        let result = bc.execute_batch(
            BatchType::Logged,
            vec![],
            ConsistencyLevel::One,
            &strategy,
            &snitch,
        );
        assert!(result.is_err());
    }

    #[test]
    fn batch_size_fail_threshold() {
        let (_wc, _blm, bc) = setup();
        let bc = bc.with_guardrails(BatchGuardrails {
            warn_threshold: 10,
            fail_threshold: 50,
            warn_partition_count: 10,
            max_mutations_per_batch: 65535,
        });
        let strategy = SimpleStrategy::new(3);
        let snitch = SimpleSnitch;

        // Create a mutation that exceeds the tiny threshold
        let mut big_mutation = test_mutation();
        big_mutation.rows.push(MutationRow {
            clustering_key: vec![0u8; 100],
            cells: vec![CellMutation {
                column: "col".to_string(),
                value: Some(vec![0u8; 100]),
                timestamp: 1000,
                ttl: 0,
                is_tombstone: false,
                collection_op: None,
            }],
            is_tombstone: false,
            range_tombstone: None,
        });

        let result = bc.execute_batch(
            BatchType::Logged,
            vec![big_mutation],
            ConsistencyLevel::One,
            &strategy,
            &snitch,
        );
        assert!(result.is_err());
    }

    #[test]
    fn counter_batch_rejects_non_counter_mutations() {
        let (_wc, _blm, bc) = setup();
        let strategy = SimpleStrategy::new(3);
        let snitch = SimpleSnitch;

        // Standard mutation in a counter batch should fail
        let result = bc.execute_batch(
            BatchType::Counter,
            vec![test_mutation()], // MutationKind::Standard
            ConsistencyLevel::One,
            &strategy,
            &snitch,
        );
        assert!(result.is_err());
    }

    #[test]
    fn batch_metrics_tracked() {
        let (_wc, _blm, bc) = setup();
        let strategy = SimpleStrategy::new(3);
        let snitch = SimpleSnitch;

        bc.execute_batch(
            BatchType::Unlogged,
            vec![test_mutation()],
            ConsistencyLevel::One,
            &strategy,
            &snitch,
        )
        .unwrap();

        assert_eq!(bc.metrics.batches_executed.load(Ordering::Relaxed), 1);
    }
}
