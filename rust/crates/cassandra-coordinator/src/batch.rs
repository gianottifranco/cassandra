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
use std::fs::{self, File};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use parking_lot::RwLock;
use tracing::{debug, info, warn};
use uuid::Uuid;

use cassandra_cluster_metadata::{ClusterSnapshot, Endpoint, ReplicationStrategy, Snitch};
use cassandra_messaging::{Message, MessagingService, Verb};

use crate::consistency::ConsistencyLevel;
use crate::verb_handlers::batch_handler::{
    BatchRemoveRequest, BatchRemoveResponse, BatchStoreRequest, BatchStoreResponse,
};
use crate::write::{CoordinatedMutation, MutationKind, WriteCoordinator, WriteError, WriteResult};

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
/// non-local DC. This implementation stores local entries in memory and can
/// mirror them to a durable system-batchlog directory for crash recovery.
///
/// ## Java Oracle
///
/// `org.apache.cassandra.batchlog.BatchlogManager`
pub struct BatchLogManager {
    entries: Arc<RwLock<HashMap<Uuid, BatchEntry>>>,
    persistence_dir: Option<PathBuf>,
    /// Batchlog replay interval.
    replay_interval: Duration,
    /// Distributed lease: prevents concurrent replays.
    pub replay_in_progress: AtomicBool,
    /// Max replays per interval for rate limiting.
    pub replay_rate_limit: AtomicUsize,
    /// Retry counts per batch entry for exponential backoff.
    retry_counts: Arc<RwLock<HashMap<Uuid, u32>>>,
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
            persistence_dir: None,
            replay_interval: Duration::from_secs(60),
            replay_in_progress: AtomicBool::new(false),
            replay_rate_limit: AtomicUsize::new(100),
            retry_counts: Arc::new(RwLock::new(HashMap::new())),
            metrics: BatchLogMetrics::new(),
        }
    }

    /// Create a batchlog manager backed by durable local storage.
    ///
    /// Existing batchlog entry files in `dir` are loaded immediately, matching
    /// `system.batches` recovery semantics after a node restart.
    pub fn with_persistence_dir<P: Into<PathBuf>>(mut self, dir: P) -> io::Result<Self> {
        let dir = dir.into();
        fs::create_dir_all(&dir)?;
        let entries = load_persisted_entries(&dir)?;
        {
            let mut guard = self.entries.write();
            for entry in entries {
                guard.insert(entry.id, entry);
            }
        }
        self.persistence_dir = Some(dir);
        Ok(self)
    }

    pub fn with_replay_interval(mut self, interval: Duration) -> Self {
        self.replay_interval = interval;
        self
    }

    pub fn with_replay_rate_limit(self, limit: usize) -> Self {
        self.replay_rate_limit.store(limit, Ordering::Relaxed);
        self
    }

    /// Get the retry count for a batch entry (for exponential backoff).
    pub fn retry_count(&self, id: &Uuid) -> u32 {
        self.retry_counts.read().get(id).copied().unwrap_or(0)
    }

    /// Increment and return the retry count for a batch entry.
    pub fn increment_retry(&self, id: &Uuid) -> u32 {
        let mut counts = self.retry_counts.write();
        let count = counts.entry(*id).or_insert(0);
        *count += 1;
        *count
    }

    /// Clear retry tracking for a batch entry.
    pub fn clear_retry(&self, id: &Uuid) {
        self.retry_counts.write().remove(id);
    }

    /// Store a new batch entry before executing mutations.
    pub fn store(&self, batch_type: BatchType, mutations: Vec<CoordinatedMutation>) -> Uuid {
        let id = Uuid::new_v4();
        let now = current_epoch_millis();

        let entry = BatchEntry {
            id,
            batch_type,
            mutations,
            created_at: now,
            version: 1,
        };

        self.persist_entry(&entry);
        self.entries.write().insert(id, entry);
        self.metrics.batches_stored.fetch_add(1, Ordering::Relaxed);
        debug!(batch_id = %id, "Batch log entry stored");
        id
    }

    /// Store a batch entry received from another coordinator.
    pub fn store_entry(&self, entry: BatchEntry) {
        let id = entry.id;
        self.persist_entry(&entry);
        self.entries.write().insert(id, entry);
        self.metrics.batches_stored.fetch_add(1, Ordering::Relaxed);
        debug!(batch_id = %id, "Batch log entry stored from replica request");
    }

    /// Remove a batch entry after all mutations succeed.
    pub fn remove(&self, id: &Uuid) -> Option<BatchEntry> {
        let entry = self.entries.write().remove(id);
        if entry.is_some() {
            self.remove_persisted_entry(id);
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

    fn persist_entry(&self, entry: &BatchEntry) {
        let Some(dir) = &self.persistence_dir else {
            return;
        };
        if let Err(err) = persist_batch_entry(dir, entry) {
            warn!(batch_id = %entry.id, error = %err, "Failed to persist batchlog entry");
        }
    }

    fn remove_persisted_entry(&self, id: &Uuid) {
        let Some(dir) = &self.persistence_dir else {
            return;
        };
        let path = batch_entry_path(dir, id);
        if let Err(err) = fs::remove_file(&path) {
            if err.kind() != io::ErrorKind::NotFound {
                warn!(batch_id = %id, path = %path.display(), error = %err, "Failed to remove persisted batchlog entry");
            }
        }
    }
}

impl Default for BatchLogManager {
    fn default() -> Self {
        Self::new()
    }
}

fn current_epoch_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

fn batch_entry_path(dir: &Path, id: &Uuid) -> PathBuf {
    dir.join(format!("{id}.batchlog"))
}

fn persist_batch_entry(dir: &Path, entry: &BatchEntry) -> io::Result<()> {
    fs::create_dir_all(dir)?;
    let path = batch_entry_path(dir, &entry.id);
    let tmp_path = dir.join(format!("{}.batchlog.tmp", entry.id));
    let data = serde_json::to_vec(entry).map_err(|err| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("serialize batchlog entry: {err}"),
        )
    })?;

    {
        let mut file = File::create(&tmp_path)?;
        file.write_all(&data)?;
        file.sync_all()?;
    }
    fs::rename(&tmp_path, &path)?;
    Ok(())
}

fn load_persisted_entries(dir: &Path) -> io::Result<Vec<BatchEntry>> {
    let mut entries = Vec::new();
    if !dir.exists() {
        return Ok(entries);
    }

    for item in fs::read_dir(dir)? {
        let path = item?.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("batchlog") {
            continue;
        }
        let bytes = match fs::read(&path) {
            Ok(bytes) => bytes,
            Err(err) => {
                warn!(path = %path.display(), error = %err, "Failed to read persisted batchlog entry");
                continue;
            }
        };
        match serde_json::from_slice::<BatchEntry>(&bytes) {
            Ok(entry) => entries.push(entry),
            Err(err) => {
                warn!(path = %path.display(), error = %err, "Failed to decode persisted batchlog entry");
            }
        }
    }
    Ok(entries)
}

// ─── Replay Result ──────────────────────────────────────────────

/// Result of a batchlog replay operation.
///
/// ## Java Oracle
///
/// `BatchlogManager.replayFailedBatches()` return semantics
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayResult {
    /// Total expired entries found.
    pub total_expired: usize,
    /// Successfully replayed entries.
    pub replayed: usize,
    /// Failed replay attempts.
    pub failed: usize,
    /// Entries skipped due to rate limiting.
    pub skipped_rate_limit: usize,
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
    messaging: Option<Arc<MessagingService>>,
    guardrails: BatchGuardrails,
    /// Local datacenter name for batchlog replica selection.
    local_dc: String,
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
            messaging: None,
            guardrails: BatchGuardrails::default(),
            local_dc: "dc1".to_string(),
            metrics: BatchCoordinatorMetrics::new(),
        }
    }

    pub fn with_guardrails(mut self, guardrails: BatchGuardrails) -> Self {
        self.guardrails = guardrails;
        self
    }

    pub fn with_local_dc(mut self, dc: String) -> Self {
        self.local_dc = dc;
        self
    }

    pub fn with_messaging(mut self, messaging: Arc<MessagingService>) -> Self {
        self.messaging = Some(messaging);
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

        // 3. For logged batches, store batchlog entry first.
        //    Skip batchlog for Unlogged and Counter batches:
        //    - Unlogged: by definition, no batchlog
        //    - Counter: counter mutations use a separate path through counter leader
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
            let all_counters = mutations.iter().all(|m| m.kind == MutationKind::Counter);
            if !all_counters {
                return Err(WriteError::Internal(
                    "Counter batch contains non-counter mutations".to_string(),
                ));
            }
        }

        // Non-counter batches must not contain counter mutations
        if batch_type != BatchType::Counter {
            let has_counters = mutations.iter().any(|m| m.kind == MutationKind::Counter);
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
    /// Called periodically by a background task. Uses distributed lease tracking
    /// to prevent concurrent replays, exponential backoff on failures, and
    /// rate limiting.
    ///
    /// ## Java Oracle
    ///
    /// `BatchlogManager.replayFailedBatches()`
    pub fn replay_batchlog(
        &self,
        max_age: Duration,
        strategy: &dyn ReplicationStrategy,
        snitch: &dyn Snitch,
    ) -> ReplayResult {
        // Acquire distributed lease — prevent concurrent replays
        if self
            .batchlog
            .replay_in_progress
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            debug!("Batchlog replay already in progress, skipping");
            return ReplayResult {
                total_expired: 0,
                replayed: 0,
                failed: 0,
                skipped_rate_limit: 0,
            };
        }

        let expired = self.batchlog.expired_entries(max_age);
        let total = expired.len();
        let mut replayed = 0;
        let mut failed = 0;
        let mut skipped_rate_limit = 0;
        let rate_limit = self.batchlog.replay_rate_limit.load(Ordering::Relaxed);

        for entry in expired {
            // Rate limiting: stop after reaching the max replays per interval
            if replayed + failed >= rate_limit {
                skipped_rate_limit += 1;
                continue;
            }

            // Exponential backoff: skip entries that have failed recently
            let retry_count = self.batchlog.retry_count(&entry.id);
            if retry_count > 0 {
                // Backoff: 2^retry_count seconds, capped at 5 min
                let backoff_ms = std::cmp::min((1u64 << retry_count.min(18)) * 1000, 300_000);
                let age_ms = {
                    let now_ms = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis() as i64;
                    (now_ms - entry.created_at) as u64
                };
                // Only retry if enough time has passed since creation relative to backoff
                if age_ms < backoff_ms * (retry_count as u64) {
                    skipped_rate_limit += 1;
                    continue;
                }
            }

            info!(
                batch_id = %entry.id,
                mutations = entry.mutations.len(),
                retry_count = retry_count,
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
                self.batchlog.clear_retry(&entry.id);
                replayed += 1;
                self.batchlog
                    .metrics
                    .batches_replayed
                    .fetch_add(1, Ordering::Relaxed);
            } else {
                self.batchlog.increment_retry(&entry.id);
                failed += 1;
            }
        }

        // Release the lease
        self.batchlog
            .replay_in_progress
            .store(false, Ordering::Release);

        ReplayResult {
            total_expired: total,
            replayed,
            failed,
            skipped_rate_limit,
        }
    }

    /// Select batchlog endpoints: prefer 2 nodes from non-local DCs.
    ///
    /// ## Java Oracle
    ///
    /// `BatchlogManager.getBatchlogEndpoints()` — prefers endpoints in
    /// a non-local datacenter; falls back to local DC if only one DC exists.
    pub fn select_batchlog_endpoints(
        &self,
        snapshot: &ClusterSnapshot,
        snitch: &dyn Snitch,
    ) -> Vec<Endpoint> {
        let live = snapshot.live_endpoints();
        let mut non_local: Vec<Endpoint> = Vec::new();
        let mut local: Vec<Endpoint> = Vec::new();

        for ep in &live {
            let dc = snitch.datacenter(ep);
            if dc != self.local_dc {
                non_local.push(*ep);
            } else {
                local.push(*ep);
            }
        }

        // Prefer non-local DC endpoints; fall back to local if only one DC
        let candidates = if non_local.len() >= 2 {
            non_local
        } else if !non_local.is_empty() {
            // Not enough non-local, supplement with local
            let mut combined = non_local;
            combined.extend(local.iter().take(2 - combined.len()));
            combined
        } else {
            local
        };

        // Pick up to 2 endpoints
        candidates.into_iter().take(2).collect()
    }

    /// Store a batch entry in the local batchlog.
    ///
    /// This is the synchronous local path used by tests and recovery code.
    /// Use [`Self::send_batchlog_store_to_replicas`] when internode messaging
    /// is available and the selected batchlog endpoints should be contacted.
    ///
    /// ## Java Oracle
    ///
    /// `StorageProxy.syncWriteBatchedMutations()` — batchlog store phase
    pub fn send_batchlog_store(
        &self,
        _endpoints: &[Endpoint],
        batch_type: BatchType,
        mutations: Vec<CoordinatedMutation>,
    ) -> Uuid {
        self.batchlog.store(batch_type, mutations)
    }

    /// Store a batch entry on the selected batchlog replicas with
    /// `Verb::BatchStore` and require every replica response to confirm it.
    pub async fn send_batchlog_store_to_replicas(
        &self,
        endpoints: &[Endpoint],
        batch_type: BatchType,
        mutations: Vec<CoordinatedMutation>,
    ) -> Result<Uuid, WriteError> {
        if endpoints.is_empty() {
            return Ok(self.batchlog.store(batch_type, mutations));
        }

        let messaging = self.messaging.as_ref().ok_or_else(|| {
            WriteError::Internal("Messaging service not configured for batchlog store".to_string())
        })?;

        let id = Uuid::new_v4();
        let created_at = current_epoch_millis();
        let request = BatchStoreRequest {
            id,
            batch_type,
            mutations,
            created_at,
        };
        let payload = serde_json::to_vec(&request)
            .map_err(|e| WriteError::Internal(format!("Serialize batchlog store request: {e}")))?;

        for endpoint in endpoints {
            let msg = Message::request(Verb::BatchStore, messaging.next_id(), payload.clone());
            let response = messaging
                .send_and_wait(endpoint.addr(), msg, Duration::from_secs(5))
                .await
                .map_err(|e| {
                    WriteError::Internal(format!(
                        "Batchlog store to {endpoint} failed before response: {e}"
                    ))
                })?;

            if response.header.verb != Verb::BatchStoreResponse {
                return Err(WriteError::Internal(format!(
                    "Batchlog store to {endpoint} returned unexpected verb {}",
                    response.header.verb
                )));
            }

            let body: BatchStoreResponse =
                serde_json::from_slice(&response.payload).map_err(|e| {
                    WriteError::Internal(format!(
                        "Batchlog store response from {endpoint} was invalid: {e}"
                    ))
                })?;
            if !body.success {
                return Err(WriteError::Internal(format!(
                    "Batchlog store to {endpoint} was rejected"
                )));
            }
        }

        Ok(id)
    }

    /// Remove a local batch entry after all mutations succeed.
    ///
    /// ## Java Oracle
    ///
    /// `StorageProxy.syncWriteBatchedMutations()` — batchlog remove phase
    pub fn send_batchlog_remove(&self, _endpoints: &[Endpoint], id: &Uuid) {
        self.batchlog.remove(id);
    }

    /// Remove a batch entry from selected batchlog replicas with
    /// `Verb::BatchRemove`.
    pub async fn send_batchlog_remove_from_replicas(
        &self,
        endpoints: &[Endpoint],
        id: &Uuid,
    ) -> Result<(), WriteError> {
        if endpoints.is_empty() {
            self.batchlog.remove(id);
            return Ok(());
        }

        let messaging = self.messaging.as_ref().ok_or_else(|| {
            WriteError::Internal("Messaging service not configured for batchlog remove".to_string())
        })?;
        let payload = serde_json::to_vec(&BatchRemoveRequest { id: *id })
            .map_err(|e| WriteError::Internal(format!("Serialize batchlog remove request: {e}")))?;

        for endpoint in endpoints {
            let msg = Message::request(Verb::BatchRemove, messaging.next_id(), payload.clone());
            let response = messaging
                .send_and_wait(endpoint.addr(), msg, Duration::from_secs(5))
                .await
                .map_err(|e| {
                    WriteError::Internal(format!(
                        "Batchlog remove from {endpoint} failed before response: {e}"
                    ))
                })?;

            // BatchRemove reuses BatchStoreResponse as the ack verb on this
            // Rust wire enum; the payload remains BatchRemoveResponse.
            if response.header.verb != Verb::BatchStoreResponse {
                return Err(WriteError::Internal(format!(
                    "Batchlog remove from {endpoint} returned unexpected verb {}",
                    response.header.verb
                )));
            }

            let body: BatchRemoveResponse =
                serde_json::from_slice(&response.payload).map_err(|e| {
                    WriteError::Internal(format!(
                        "Batchlog remove response from {endpoint} was invalid: {e}"
                    ))
                })?;
            if !body.removed {
                debug!(batch_id = %id, endpoint = %endpoint, "Batchlog replica did not have entry to remove");
            }
        }

        Ok(())
    }

    /// Merge counter mutations per-partition for counter batch optimization.
    ///
    /// Groups counter mutations by (keyspace, table, partition_key) and merges
    /// their cells, combining increments to the same column.
    ///
    /// ## Java Oracle
    ///
    /// `CounterMutation.mergeCounterValues()` — merges counter cells
    pub fn merge_counter_mutations(
        &self,
        mutations: Vec<CoordinatedMutation>,
    ) -> Vec<CoordinatedMutation> {
        // Group by (keyspace, table, partition_key)
        let mut groups: HashMap<(String, String, Vec<u8>), CoordinatedMutation> = HashMap::new();

        for mutation in mutations {
            let key = (
                mutation.keyspace.clone(),
                mutation.table.clone(),
                mutation.partition_key.clone(),
            );

            if let Some(existing) = groups.get_mut(&key) {
                // Merge rows: combine cells for same clustering key
                for new_row in mutation.rows {
                    if let Some(existing_row) = existing
                        .rows
                        .iter_mut()
                        .find(|r| r.clustering_key == new_row.clustering_key)
                    {
                        // Merge cells: for counter mutations, combine values for same column
                        for new_cell in new_row.cells {
                            if let Some(existing_cell) = existing_row
                                .cells
                                .iter_mut()
                                .find(|c| c.column == new_cell.column)
                            {
                                // Merge counter values by summing the byte-encoded i64 deltas
                                if let (Some(ev), Some(nv)) =
                                    (&existing_cell.value, &new_cell.value)
                                {
                                    if ev.len() == 8 && nv.len() == 8 {
                                        let e_val =
                                            i64::from_be_bytes(ev.as_slice().try_into().unwrap());
                                        let n_val =
                                            i64::from_be_bytes(nv.as_slice().try_into().unwrap());
                                        existing_cell.value =
                                            Some((e_val + n_val).to_be_bytes().to_vec());
                                    }
                                }
                                // Update timestamp to latest
                                existing_cell.timestamp =
                                    existing_cell.timestamp.max(new_cell.timestamp);
                            } else {
                                existing_row.cells.push(new_cell);
                            }
                        }
                    } else {
                        existing.rows.push(new_row);
                    }
                }
                // Update timestamp to latest
                existing.timestamp = existing.timestamp.max(mutation.timestamp);
            } else {
                groups.insert(key, mutation);
            }
        }

        groups.into_values().collect()
    }

    /// Start a periodic batchlog replay task.
    ///
    /// Spawns a tokio interval task that replays expired batchlog entries.
    /// Returns a `JoinHandle` that can be used to cancel the task.
    ///
    /// ## Java Oracle
    ///
    /// `BatchlogManager.startBatchlogReplay()`
    pub fn start_periodic_replay(
        self: &Arc<Self>,
        interval: Duration,
        max_age: Duration,
        strategy: Arc<dyn ReplicationStrategy>,
        snitch: Arc<dyn Snitch>,
    ) -> tokio::task::JoinHandle<()> {
        let coordinator = Arc::clone(self);
        tokio::spawn(async move {
            let mut timer = tokio::time::interval(interval);
            loop {
                timer.tick().await;
                let result =
                    coordinator.replay_batchlog(max_age, strategy.as_ref(), snitch.as_ref());
                if result.total_expired > 0 {
                    info!(
                        total = result.total_expired,
                        replayed = result.replayed,
                        failed = result.failed,
                        skipped = result.skipped_rate_limit,
                        "Periodic batchlog replay completed"
                    );
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hints::HintStore;
    use crate::verb_handlers::{BatchRemoveVerbHandler, BatchStoreVerbHandler};
    use crate::write::{CellMutation, MutationKind, MutationRow};
    use cassandra_cluster_metadata::{
        ClusterMetadata, Endpoint, NodeId, NodeInfo, SimpleSnitch, SimpleStrategy,
    };
    use cassandra_common::Token;
    use cassandra_messaging::MessagingService;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    fn ep(port: u16) -> Endpoint {
        Endpoint::new(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            port,
        ))
    }

    fn free_addr() -> SocketAddr {
        std::net::TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 0))
            .unwrap()
            .local_addr()
            .unwrap()
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
    fn store_entry_preserves_remote_batch_id() {
        let blm = BatchLogManager::new();
        let id = Uuid::new_v4();
        blm.store_entry(BatchEntry {
            id,
            batch_type: BatchType::Logged,
            mutations: vec![test_mutation()],
            created_at: 1_700_000_000_000,
            version: 1,
        });

        let stored = blm.get(&id).unwrap();
        assert_eq!(stored.id, id);
        assert_eq!(stored.batch_type, BatchType::Logged);
        assert_eq!(stored.mutations.len(), 1);
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

    #[test]
    fn persisted_batchlog_entries_survive_restart_and_remove_deletes_file() {
        let dir = tempfile::tempdir().unwrap();
        let blm = BatchLogManager::new()
            .with_persistence_dir(dir.path())
            .unwrap();

        let id = blm.store(BatchType::Logged, vec![test_mutation()]);
        assert!(dir.path().join(format!("{id}.batchlog")).exists());

        let reloaded = BatchLogManager::new()
            .with_persistence_dir(dir.path())
            .unwrap();
        let entry = reloaded.get(&id).unwrap();
        assert_eq!(entry.id, id);
        assert_eq!(entry.batch_type, BatchType::Logged);
        assert_eq!(entry.mutations.len(), 1);

        reloaded.remove(&id);
        assert!(reloaded.get(&id).is_none());
        assert!(!dir.path().join(format!("{id}.batchlog")).exists());
    }

    #[test]
    fn persisted_store_entry_preserves_replica_batch_id() {
        let dir = tempfile::tempdir().unwrap();
        let blm = BatchLogManager::new()
            .with_persistence_dir(dir.path())
            .unwrap();
        let id = Uuid::new_v4();
        blm.store_entry(BatchEntry {
            id,
            batch_type: BatchType::Logged,
            mutations: vec![test_mutation()],
            created_at: 1_700_000_000_000,
            version: 1,
        });

        let reloaded = BatchLogManager::new()
            .with_persistence_dir(dir.path())
            .unwrap();
        let stored = reloaded.get(&id).unwrap();
        assert_eq!(stored.id, id);
        assert_eq!(stored.created_at, 1_700_000_000_000);
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

    // ── WU-07: Batchlog endpoint selection tests ────────────────

    fn node_in_dc(port: u16, tokens: Vec<i64>, dc: &str, rack: &str) -> NodeInfo {
        NodeInfo::new(
            NodeId::random(),
            ep(port),
            dc,
            rack,
            tokens.into_iter().map(Token::from_raw).collect(),
        )
    }

    /// Snitch that uses a preconfigured DC map for multi-DC tests.
    struct MultiDcSnitch {
        dc_map: HashMap<Endpoint, String>,
    }

    impl Snitch for MultiDcSnitch {
        fn datacenter(&self, endpoint: &Endpoint) -> String {
            self.dc_map
                .get(endpoint)
                .cloned()
                .unwrap_or_else(|| "unknown".to_string())
        }
        fn rack(&self, _endpoint: &Endpoint) -> String {
            "rack1".to_string()
        }
    }

    #[test]
    fn select_batchlog_endpoints_prefers_non_local_dc() {
        let n1 = node_in_dc(8001, vec![-100], "dc1", "rack1");
        let n2 = node_in_dc(8002, vec![0], "dc2", "rack1");
        let n3 = node_in_dc(8003, vec![100], "dc2", "rack1");

        let cm = Arc::new(ClusterMetadata::new(n1));
        cm.update_node(n2);
        cm.update_node(n3);

        let hint_store = Arc::new(HintStore::new(1000));
        let wc = Arc::new(WriteCoordinator::new(Arc::clone(&cm), ep(8001), hint_store));
        let blm = Arc::new(BatchLogManager::new());
        let bc = BatchCoordinator::new(Arc::clone(&wc), Arc::clone(&blm))
            .with_local_dc("dc1".to_string());

        let mut dc_map = HashMap::new();
        dc_map.insert(ep(8001), "dc1".to_string());
        dc_map.insert(ep(8002), "dc2".to_string());
        dc_map.insert(ep(8003), "dc2".to_string());
        let snitch = MultiDcSnitch { dc_map };

        let snapshot = cm.snapshot();
        let endpoints = bc.select_batchlog_endpoints(&snapshot, &snitch);

        assert_eq!(endpoints.len(), 2);
        // Both should be from dc2 (non-local)
        for ep_val in &endpoints {
            assert_eq!(snitch.datacenter(ep_val), "dc2");
        }
    }

    #[test]
    fn select_batchlog_endpoints_falls_back_to_local_dc() {
        // Single DC setup
        let (_wc, _blm, bc) = setup();
        let bc = bc.with_local_dc("dc1".to_string());

        let cm = Arc::new(ClusterMetadata::new(node(9001, vec![-100])));
        cm.update_node(node(9002, vec![0]));
        cm.update_node(node(9003, vec![100]));

        let snapshot = cm.snapshot();
        let endpoints = bc.select_batchlog_endpoints(&snapshot, &SimpleSnitch);

        // Should fall back to local DC endpoints
        assert!(endpoints.len() <= 2);
        assert!(!endpoints.is_empty());
    }

    // ── WU-08: Replay robustness tests ──────────────────────────

    #[test]
    fn replay_batchlog_returns_replay_result() {
        let (_wc, blm, bc) = setup();
        let strategy = SimpleStrategy::new(3);
        let snitch = SimpleSnitch;

        // Store an entry with a very old timestamp to make it expired
        {
            let id = Uuid::new_v4();
            let entry = BatchEntry {
                id,
                batch_type: BatchType::Logged,
                mutations: vec![test_mutation()],
                created_at: 0, // epoch = very old
                version: 1,
            };
            blm.entries.write().insert(id, entry);
        }

        let result = bc.replay_batchlog(Duration::from_secs(1), &strategy, &snitch);
        assert_eq!(result.total_expired, 1);
        assert_eq!(result.replayed, 1);
        assert_eq!(result.failed, 0);
        assert_eq!(result.skipped_rate_limit, 0);
    }

    #[test]
    fn replay_batchlog_prevents_concurrent_replay() {
        let (_wc, blm, bc) = setup();
        let strategy = SimpleStrategy::new(3);
        let snitch = SimpleSnitch;

        // Simulate replay in progress
        blm.replay_in_progress.store(true, Ordering::Release);

        let result = bc.replay_batchlog(Duration::from_secs(1), &strategy, &snitch);
        // Should return immediately with empty result
        assert_eq!(result.total_expired, 0);
        assert_eq!(result.replayed, 0);
    }

    #[test]
    fn replay_batchlog_rate_limiting() {
        let (_wc, blm, bc) = setup();
        let strategy = SimpleStrategy::new(3);
        let snitch = SimpleSnitch;

        // Set rate limit to 1
        blm.replay_rate_limit.store(1, Ordering::Relaxed);

        // Store 3 old entries
        for _ in 0..3 {
            let id = Uuid::new_v4();
            let entry = BatchEntry {
                id,
                batch_type: BatchType::Logged,
                mutations: vec![test_mutation()],
                created_at: 0,
                version: 1,
            };
            blm.entries.write().insert(id, entry);
        }

        let result = bc.replay_batchlog(Duration::from_secs(1), &strategy, &snitch);
        assert_eq!(result.total_expired, 3);
        // Only 1 should be replayed due to rate limit
        assert_eq!(result.replayed, 1);
        assert!(result.skipped_rate_limit >= 1);
    }

    #[test]
    fn retry_count_tracking() {
        let blm = BatchLogManager::new();
        let id = Uuid::new_v4();

        assert_eq!(blm.retry_count(&id), 0);
        assert_eq!(blm.increment_retry(&id), 1);
        assert_eq!(blm.increment_retry(&id), 2);
        assert_eq!(blm.retry_count(&id), 2);

        blm.clear_retry(&id);
        assert_eq!(blm.retry_count(&id), 0);
    }

    // ── WU-09: Counter batch tests ──────────────────────────────

    fn counter_mutation(
        ks: &str,
        table: &str,
        pk: &[u8],
        col: &str,
        delta: i64,
    ) -> CoordinatedMutation {
        CoordinatedMutation {
            keyspace: ks.to_string(),
            table: table.to_string(),
            partition_key: pk.to_vec(),
            rows: vec![MutationRow {
                clustering_key: vec![],
                cells: vec![CellMutation {
                    column: col.to_string(),
                    value: Some(delta.to_be_bytes().to_vec()),
                    timestamp: 1000,
                    ttl: 0,
                    is_tombstone: false,
                    collection_op: None,
                }],
                is_tombstone: false,
                range_tombstone: None,
            }],
            timestamp: 1000,
            kind: MutationKind::Counter,
            static_cells: vec![],
            partition_tombstone: None,
        }
    }

    #[test]
    fn merge_counter_mutations_same_partition() {
        let (_wc, _blm, bc) = setup();

        let m1 = counter_mutation("ks", "t", b"pk1", "counter_col", 5);
        let m2 = counter_mutation("ks", "t", b"pk1", "counter_col", 10);

        let merged = bc.merge_counter_mutations(vec![m1, m2]);
        assert_eq!(merged.len(), 1);

        let m = &merged[0];
        assert_eq!(m.keyspace, "ks");
        assert_eq!(m.partition_key, b"pk1");
        assert_eq!(m.rows.len(), 1);
        assert_eq!(m.rows[0].cells.len(), 1);

        // Counter values should be summed: 5 + 10 = 15
        let val = i64::from_be_bytes(
            m.rows[0].cells[0]
                .value
                .as_ref()
                .unwrap()
                .as_slice()
                .try_into()
                .unwrap(),
        );
        assert_eq!(val, 15);
    }

    #[test]
    fn merge_counter_mutations_different_partitions() {
        let (_wc, _blm, bc) = setup();

        let m1 = counter_mutation("ks", "t", b"pk1", "counter_col", 5);
        let m2 = counter_mutation("ks", "t", b"pk2", "counter_col", 10);

        let merged = bc.merge_counter_mutations(vec![m1, m2]);
        assert_eq!(merged.len(), 2);
    }

    #[test]
    fn counter_batch_skips_batchlog() {
        let (_wc, blm, bc) = setup();
        let strategy = SimpleStrategy::new(3);
        let snitch = SimpleSnitch;

        let m = counter_mutation("ks", "t", b"pk1", "counter_col", 5);

        let results = bc
            .execute_batch(
                BatchType::Counter,
                vec![m],
                ConsistencyLevel::One,
                &strategy,
                &snitch,
            )
            .unwrap();

        assert_eq!(results.len(), 1);
        // No batchlog for counter batches
        assert_eq!(blm.pending_count(), 0);
    }

    #[test]
    fn send_batchlog_store_and_remove_local_entry() {
        let (_wc, blm, bc) = setup();
        let endpoints = vec![ep(7001), ep(7002)];

        let id = bc.send_batchlog_store(&endpoints, BatchType::Logged, vec![test_mutation()]);
        assert_eq!(blm.pending_count(), 1);

        bc.send_batchlog_remove(&endpoints, &id);
        assert_eq!(blm.pending_count(), 0);
    }

    #[tokio::test]
    async fn send_batchlog_store_and_remove_replicas_via_messaging() {
        let (_wc, _blm, bc) = setup();
        let remote_batchlog = Arc::new(BatchLogManager::new());
        let remote_addr = free_addr();
        let remote_service = Arc::new(MessagingService::new(remote_addr));

        let store_batchlog = Arc::clone(&remote_batchlog);
        remote_service.register_handler(
            Verb::BatchStore,
            Arc::new(move |msg| BatchStoreVerbHandler::handle_with_batchlog(msg, &store_batchlog)),
        );
        let remove_batchlog = Arc::clone(&remote_batchlog);
        remote_service.register_handler(
            Verb::BatchRemove,
            Arc::new(move |msg| {
                BatchRemoveVerbHandler::handle_with_batchlog(msg, &remove_batchlog)
            }),
        );
        Arc::clone(&remote_service).start_listener().await.unwrap();

        let coordinator_service = Arc::new(MessagingService::new(free_addr()));
        let bc = bc.with_messaging(coordinator_service);
        let endpoint = Endpoint::new(remote_addr);

        let id = bc
            .send_batchlog_store_to_replicas(&[endpoint], BatchType::Logged, vec![test_mutation()])
            .await
            .unwrap();
        assert!(remote_batchlog.get(&id).is_some());

        bc.send_batchlog_remove_from_replicas(&[endpoint], &id)
            .await
            .unwrap();
        assert!(remote_batchlog.get(&id).is_none());
    }
}
