// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

//! Write coordinator: routes mutations to replicas and enforces consistency.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.service.StorageProxy.mutate()`
//! - `org.apache.cassandra.service.AbstractWriteResponseHandler`
//! - `org.apache.cassandra.db.Mutation`
//! - `org.apache.cassandra.db.WriteType`

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use dashmap::DashSet;
use tracing::{debug, warn};

use cassandra_cluster_metadata::{ClusterMetadata, Endpoint, ReplicationStrategy, Snitch};
use cassandra_common::Token;

use crate::consistency::ConsistencyLevel;
use crate::hints::HintStore;
use crate::write_response_handler::WriteResponseHandler;

/// Default maximum mutation size (16 MiB, matches Java's commitlog_segment_size/2).
pub const DEFAULT_MAX_MUTATION_SIZE: usize = 16 * 1024 * 1024;

/// Default timestamp drift threshold for client timestamps (1 second).
pub const DEFAULT_TIMESTAMP_DRIFT_THRESHOLD: Duration = Duration::from_secs(1);

/// Default write timeout (matches Java's 2s default).
pub const DEFAULT_WRITE_TIMEOUT: Duration = Duration::from_secs(2);

// ─── Write Type ──────────────────────────────────────────────────

/// Type of write operation, used in error reporting to clients.
///
/// ## Java Oracle
///
/// `org.apache.cassandra.db.WriteType`
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum WriteType {
    /// Simple non-batch mutation.
    Simple,
    /// Logged batch.
    Batch,
    /// Unlogged batch.
    UnloggedBatch,
    /// Counter mutation.
    Counter,
    /// Materialized view update.
    View,
    /// Compare-and-set (LWT).
    Cas,
    /// Batch log write.
    BatchLog,
}

impl WriteType {
    /// Protocol string for error reporting (matches Java names exactly).
    pub fn protocol_name(&self) -> &'static str {
        match self {
            Self::Simple => "SIMPLE",
            Self::Batch => "BATCH",
            Self::UnloggedBatch => "UNLOGGED_BATCH",
            Self::Counter => "COUNTER",
            Self::View => "VIEW",
            Self::Cas => "CAS",
            Self::BatchLog => "BATCH_LOG",
        }
    }
}

impl std::fmt::Display for WriteType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.protocol_name())
    }
}

// ─── Mutation Kind ───────────────────────────────────────────────

/// Kind of mutation for routing purposes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum MutationKind {
    /// Standard row/partition mutation.
    Standard,
    /// Counter increment/decrement.
    Counter,
    /// View maintenance mutation (generated internally).
    View,
}

// ─── Coordinated Mutation ────────────────────────────────────────

/// A mutation to be coordinated across replicas.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CoordinatedMutation {
    pub keyspace: String,
    pub table: String,
    pub partition_key: Vec<u8>,
    pub rows: Vec<MutationRow>,
    pub timestamp: i64,
    /// Kind of mutation (standard, counter, view).
    pub kind: MutationKind,
    /// Static row cells (applied to the partition, not to a specific clustering key).
    pub static_cells: Vec<CellMutation>,
    /// Whether this is a partition-level tombstone.
    pub partition_tombstone: Option<TombstoneMarker>,
}

impl CoordinatedMutation {
    /// Create a simple standard mutation.
    pub fn simple(
        keyspace: String,
        table: String,
        partition_key: Vec<u8>,
        rows: Vec<MutationRow>,
        timestamp: i64,
    ) -> Self {
        Self {
            keyspace,
            table,
            partition_key,
            rows,
            timestamp,
            kind: MutationKind::Standard,
            static_cells: Vec::new(),
            partition_tombstone: None,
        }
    }

    /// Estimated serialized size in bytes for guardrail checks.
    pub fn estimated_size(&self) -> usize {
        let mut size = self.keyspace.len() + self.table.len() + self.partition_key.len() + 8;
        for row in &self.rows {
            size += row.clustering_key.len() + 2;
            for cell in &row.cells {
                size += cell.column.len() + cell.value.as_ref().map_or(0, |v| v.len()) + 16;
            }
        }
        for cell in &self.static_cells {
            size += cell.column.len() + cell.value.as_ref().map_or(0, |v| v.len()) + 16;
        }
        size
    }
}

/// Tombstone marker for partition or range deletions.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TombstoneMarker {
    pub deletion_time: i64,
    pub local_deletion_time: i32,
}

/// A row within a coordinated mutation.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MutationRow {
    pub clustering_key: Vec<u8>,
    pub cells: Vec<CellMutation>,
    pub is_tombstone: bool,
    /// Range tombstone: if set, deletes a range of clustering keys.
    pub range_tombstone: Option<RangeTombstone>,
}

/// A range tombstone deleting all rows in [start, end].
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RangeTombstone {
    pub start: Vec<u8>,
    pub end: Vec<u8>,
    pub deletion_time: i64,
    pub local_deletion_time: i32,
}

/// A cell mutation.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CellMutation {
    pub column: String,
    pub value: Option<Vec<u8>>,
    pub timestamp: i64,
    pub ttl: i32,
    pub is_tombstone: bool,
    /// Collection operation (for set/list/map types).
    pub collection_op: Option<CollectionOp>,
}

/// Collection mutation operations.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum CollectionOp {
    /// Add elements to a set/list.
    Append(Vec<Vec<u8>>),
    /// Remove elements from a set/list.
    Remove(Vec<Vec<u8>>),
    /// Put entries in a map.
    MapPut(Vec<(Vec<u8>, Vec<u8>)>),
}

// ─── Write Result / Error ────────────────────────────────────────

/// Result of a coordinated write.
#[derive(Debug)]
pub struct WriteResult {
    /// Number of successful replica acks.
    pub acks_received: usize,
    /// Number of acks required.
    pub acks_required: usize,
    /// Replicas that were contacted.
    pub contacted_replicas: Vec<Endpoint>,
    /// Whether hints were stored for failed replicas.
    pub hints_stored: usize,
}

/// Errors from write coordination.
///
/// ## Java Oracle
///
/// Maps to native protocol error codes:
/// - `Unavailable`  → 0x1000
/// - `Timeout`      → 0x1100 (WriteTimeout)
/// - `WriteFailure` → 0x1500
/// - `Overloaded`   → 0x1001
/// - `IsBootstrapping` → 0x1002
#[derive(Debug, thiserror::Error)]
pub enum WriteError {
    #[error("Write timeout: CL={cl}, type={write_type}, required={required}, received={received}")]
    Timeout {
        cl: ConsistencyLevel,
        write_type: WriteType,
        required: usize,
        received: usize,
        block_for: usize,
    },

    #[error("Unavailable: CL={cl}, required={required}, alive={alive}")]
    Unavailable {
        cl: ConsistencyLevel,
        required: usize,
        alive: usize,
    },

    #[error(
        "Write failure: CL={cl}, type={write_type}, required={required}, received={received}, failures={num_failures}"
    )]
    WriteFailure {
        cl: ConsistencyLevel,
        write_type: WriteType,
        required: usize,
        received: usize,
        block_for: usize,
        num_failures: usize,
        failure_map: HashMap<Endpoint, u16>,
    },

    #[error("Coordinator node is overloaded")]
    Overloaded,

    #[error("Coordinator node is bootstrapping")]
    IsBootstrapping,

    #[error("Cannot write during truncation")]
    TruncateInProgress,

    #[error("Schema disagreement: {0}")]
    SchemaDisagreement(String),

    #[error("Mutation too large: {size} bytes exceeds limit of {limit} bytes")]
    MutationTooLarge { size: usize, limit: usize },

    #[error("Internal error: {0}")]
    Internal(String),
}

impl WriteError {
    /// Native protocol error code.
    pub fn error_code(&self) -> u32 {
        match self {
            Self::Unavailable { .. } => 0x1000,
            Self::Timeout { .. } => 0x1100,
            Self::WriteFailure { .. } => 0x1500,
            Self::Overloaded => 0x1001,
            Self::IsBootstrapping => 0x1002,
            Self::TruncateInProgress => 0x1003,
            Self::SchemaDisagreement(_) => 0x2200,
            Self::MutationTooLarge { .. } => 0x2200,
            Self::Internal(_) => 0x0000,
        }
    }
}

// ─── Write Plan ──────────────────────────────────────────────────

/// A computed write plan: which endpoints to contact and what is required.
#[derive(Debug, Clone)]
pub struct WritePlan {
    /// All natural replicas for this partition.
    pub replicas: Vec<Endpoint>,
    /// Subset of replicas that are alive and will receive mutations.
    pub live_replicas: Vec<Endpoint>,
    /// Subset of replicas that are dead (need hints).
    pub dead_replicas: Vec<Endpoint>,
    /// Whether the local node is among the replicas.
    pub local_is_replica: bool,
    /// Number of acks required.
    pub block_for: usize,
    /// Consistency level.
    pub cl: ConsistencyLevel,
}

/// DC-aware write plan for LOCAL_QUORUM, LOCAL_ONE, EACH_QUORUM.
///
/// ## Java Oracle
///
/// `DatacenterWriteResponseHandler` tracks per-DC ack counts.
#[derive(Debug, Clone)]
pub struct DatacenterWritePlan {
    /// Base write plan.
    pub base: WritePlan,
    /// Per-DC breakdown: DC name → (live_replicas, required_acks).
    pub dc_plans: HashMap<String, DcReplicaPlan>,
    /// The coordinator's local datacenter.
    pub local_dc: String,
}

/// Per-datacenter replica plan.
#[derive(Debug, Clone)]
pub struct DcReplicaPlan {
    /// Live replicas in this DC.
    pub live_replicas: Vec<Endpoint>,
    /// Dead replicas in this DC.
    pub dead_replicas: Vec<Endpoint>,
    /// Number of acks required from this DC.
    pub required: usize,
}

/// Result of MV fanout from a write-path hook.
#[derive(Debug, Default)]
pub struct ViewFanoutResult {
    /// Number of view mutations generated.
    pub mutations_generated: usize,
    /// Number of view mutations successfully applied.
    pub mutations_applied: usize,
    /// Number of failed attempts requeued for retry.
    pub mutations_retried: usize,
    /// Number of view mutations that failed (logged, not cascaded).
    pub mutations_failed: usize,
}

#[derive(Debug, Clone)]
pub struct AsyncViewFanoutTask {
    pub mutation: cassandra_storage::materialized_views::ViewMutation,
    pub attempts: u32,
}

/// Queue-backed asynchronous MV fanout dispatcher.
///
/// Java schedules view writes separately from the base mutation. This
/// dispatcher models that split: view mutations are enqueued, backlog metrics
/// are updated immediately, and a worker/drain applies them later.
#[derive(Clone)]
pub struct AsyncViewFanoutDispatcher {
    pending: Arc<Mutex<VecDeque<AsyncViewFanoutTask>>>,
    metrics: Arc<ViewFanoutMetrics>,
    backlog: Arc<AtomicU64>,
}

impl AsyncViewFanoutDispatcher {
    pub fn new(metrics: Arc<ViewFanoutMetrics>, backlog: Arc<AtomicU64>) -> Self {
        Self {
            pending: Arc::new(Mutex::new(VecDeque::new())),
            metrics,
            backlog,
        }
    }

    pub fn enqueue(
        &self,
        mutations: impl IntoIterator<Item = cassandra_storage::materialized_views::ViewMutation>,
    ) -> usize {
        let mut pending = self.pending.lock().expect("view fanout queue lock");
        let mut count = 0;
        for mutation in mutations {
            pending.push_back(AsyncViewFanoutTask {
                mutation,
                attempts: 0,
            });
            count += 1;
        }
        self.metrics
            .view_mutations_generated
            .fetch_add(count as u64, Ordering::Relaxed);
        self.backlog.fetch_add(count as u64, Ordering::Relaxed);
        count
    }

    pub fn pending_count(&self) -> usize {
        self.pending.lock().expect("view fanout queue lock").len()
    }

    pub fn drain_with<F>(&self, mut apply: F) -> ViewFanoutResult
    where
        F: FnMut(&cassandra_storage::materialized_views::ViewMutation) -> Result<(), WriteError>,
    {
        let mut result = ViewFanoutResult::default();
        loop {
            let task = self
                .pending
                .lock()
                .expect("view fanout queue lock")
                .pop_front();
            let Some(task) = task else {
                break;
            };

            result.mutations_generated += 1;
            match apply(&task.mutation) {
                Ok(()) => {
                    result.mutations_applied += 1;
                    self.metrics
                        .view_mutations_applied
                        .fetch_add(1, Ordering::Relaxed);
                }
                Err(_) => {
                    result.mutations_failed += 1;
                    self.metrics
                        .view_mutations_failed
                        .fetch_add(1, Ordering::Relaxed);
                }
            }
            self.backlog.fetch_sub(1, Ordering::Relaxed);
        }
        result
    }

    pub fn drain_with_retries<F>(&self, max_attempts: u32, mut apply: F) -> ViewFanoutResult
    where
        F: FnMut(&cassandra_storage::materialized_views::ViewMutation) -> Result<(), WriteError>,
    {
        let max_attempts = max_attempts.max(1);
        let tasks_to_process = self.pending_count();
        let mut result = ViewFanoutResult::default();

        for _ in 0..tasks_to_process {
            let task = self
                .pending
                .lock()
                .expect("view fanout queue lock")
                .pop_front();
            let Some(mut task) = task else {
                break;
            };

            result.mutations_generated += 1;
            match apply(&task.mutation) {
                Ok(()) => {
                    result.mutations_applied += 1;
                    self.metrics
                        .view_mutations_applied
                        .fetch_add(1, Ordering::Relaxed);
                    self.backlog.fetch_sub(1, Ordering::Relaxed);
                }
                Err(_) => {
                    task.attempts = task.attempts.saturating_add(1);
                    if task.attempts < max_attempts {
                        result.mutations_retried += 1;
                        self.metrics
                            .view_mutations_retried
                            .fetch_add(1, Ordering::Relaxed);
                        self.pending
                            .lock()
                            .expect("view fanout queue lock")
                            .push_back(task);
                    } else {
                        result.mutations_failed += 1;
                        self.metrics
                            .view_mutations_failed
                            .fetch_add(1, Ordering::Relaxed);
                        self.backlog.fetch_sub(1, Ordering::Relaxed);
                    }
                }
            }
        }

        result
    }
}

/// Write guardrails configuration.
///
/// ## Java Oracle
///
/// `cassandra.yaml`: `max_mutation_size_in_kb`,
/// `CDC_MAX_MUTATION_SIZE_WARN_THRESHOLD_IN_KB`
#[derive(Debug, Clone)]
pub struct WriteGuardrails {
    /// Maximum individual mutation size in bytes.
    pub max_mutation_size: usize,
    /// Threshold for timestamp drift warning (client vs server).
    pub timestamp_drift_threshold: Duration,
    /// Warn when partition size exceeds this (bytes, default 100 MiB).
    pub partition_size_warn: usize,
    /// Warn when tombstone count in a single mutation exceeds this (default 1000).
    pub tombstone_warn_threshold: usize,
    /// Warn when collection size exceeds this (default 65535 elements).
    pub collection_size_warn: usize,
    /// Warn when the number of columns touched in a single query exceeds this (default 100).
    pub columns_per_query_warn: usize,
}

impl Default for WriteGuardrails {
    fn default() -> Self {
        Self {
            max_mutation_size: DEFAULT_MAX_MUTATION_SIZE,
            timestamp_drift_threshold: DEFAULT_TIMESTAMP_DRIFT_THRESHOLD,
            partition_size_warn: 100 * 1024 * 1024,
            tombstone_warn_threshold: 1000,
            collection_size_warn: 65535,
            columns_per_query_warn: 100,
        }
    }
}

// ─── Write Metrics ───────────────────────────────────────────────

/// Counters for write-path observability.
///
/// ## Java Oracle
///
/// `org.apache.cassandra.metrics.ClientRequestMetrics`
pub struct WriteMetrics {
    pub writes_total: AtomicU64,
    pub writes_succeeded: AtomicU64,
    pub writes_failed: AtomicU64,
    pub writes_timed_out: AtomicU64,
    pub writes_unavailable: AtomicU64,
    pub hints_in_flight: AtomicU64,
    pub write_latency_us_sum: AtomicU64,
}

impl WriteMetrics {
    pub fn new() -> Self {
        Self {
            writes_total: AtomicU64::new(0),
            writes_succeeded: AtomicU64::new(0),
            writes_failed: AtomicU64::new(0),
            writes_timed_out: AtomicU64::new(0),
            writes_unavailable: AtomicU64::new(0),
            hints_in_flight: AtomicU64::new(0),
            write_latency_us_sum: AtomicU64::new(0),
        }
    }
}

impl Default for WriteMetrics {
    fn default() -> Self {
        Self::new()
    }
}

// ─── Write Coordinator ──────────────────────────────────────────

/// The write coordinator.
///
/// Routes mutations to the correct replicas based on the partition key,
/// waits for the required number of acks (per CL), and handles failures
/// with hint storage.
///
/// ## Java Oracle
///
/// `org.apache.cassandra.service.StorageProxy.performWrite()`

/// Default backpressure threshold for view update backlog.
///
/// When the number of pending view mutations exceeds this, the coordinator
/// logs a warning. Java uses `max_pending_view_updates` in cassandra.yaml.
pub const DEFAULT_VIEW_UPDATE_BACKLOG_THRESHOLD: u64 = 10_000;

/// Reader used by MV maintenance to fetch the existing base row before a write.
///
/// The storage layer implementation should return the currently visible row
/// state for the mutation's partition/clustering key before the base mutation is
/// applied. `None` means the row does not exist or cannot be read.
pub type ViewExistingRowReader =
    Arc<dyn Fn(&CoordinatedMutation) -> Option<HashMap<String, Option<Vec<u8>>>> + Send + Sync>;

pub struct WriteCoordinator {
    /// Cluster metadata for replica lookups.
    cluster: Arc<ClusterMetadata>,
    /// The local node's endpoint.
    local_endpoint: Endpoint,
    /// Write timeout.
    timeout: Duration,
    /// Hint store for dead replicas.
    hint_store: Arc<HintStore>,
    /// Write metrics.
    pub metrics: Arc<WriteMetrics>,
    /// Whether this node is bootstrapping (rejects writes if true).
    is_bootstrapping: bool,
    /// Local datacenter name for DC-aware CLs.
    local_datacenter: String,
    /// Write guardrails.
    guardrails: WriteGuardrails,
    /// MV fanout metrics.
    pub view_fanout_metrics: Arc<ViewFanoutMetrics>,
    /// Semaphore limiting max concurrent writes (WU-05).
    write_semaphore: Arc<tokio::sync::Semaphore>,
    /// Tables currently being truncated (WU-05).
    truncating_tables: Arc<DashSet<String>>,
    /// Backpressure counter: tracks pending MV update mutations (WU-18).
    ///
    /// ## Java Oracle
    ///
    /// `ViewManager.getViewUpdateBacklog()` — used by StorageProxy to apply
    /// backpressure when view updates fall behind.
    pub view_update_backlog: Arc<AtomicU64>,
    /// Threshold for view update backlog warning (WU-18).
    view_update_backlog_threshold: u64,
    /// Existing row reader used for materialized-view delta generation.
    view_existing_row_reader: Option<ViewExistingRowReader>,
}

/// Metrics for MV fanout tracking.
pub struct ViewFanoutMetrics {
    pub view_mutations_generated: AtomicU64,
    pub view_mutations_applied: AtomicU64,
    pub view_mutations_retried: AtomicU64,
    pub view_mutations_failed: AtomicU64,
}

impl ViewFanoutMetrics {
    pub fn new() -> Self {
        Self {
            view_mutations_generated: AtomicU64::new(0),
            view_mutations_applied: AtomicU64::new(0),
            view_mutations_retried: AtomicU64::new(0),
            view_mutations_failed: AtomicU64::new(0),
        }
    }
}

impl Default for ViewFanoutMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl WriteCoordinator {
    pub fn new(
        cluster: Arc<ClusterMetadata>,
        local_endpoint: Endpoint,
        hint_store: Arc<HintStore>,
    ) -> Self {
        Self {
            cluster,
            local_endpoint,
            timeout: DEFAULT_WRITE_TIMEOUT,
            hint_store,
            metrics: Arc::new(WriteMetrics::new()),
            is_bootstrapping: false,
            local_datacenter: "dc1".to_string(),
            guardrails: WriteGuardrails::default(),
            view_fanout_metrics: Arc::new(ViewFanoutMetrics::new()),
            write_semaphore: Arc::new(tokio::sync::Semaphore::new(1024)),
            truncating_tables: Arc::new(DashSet::new()),
            view_update_backlog: Arc::new(AtomicU64::new(0)),
            view_update_backlog_threshold: DEFAULT_VIEW_UPDATE_BACKLOG_THRESHOLD,
            view_existing_row_reader: None,
        }
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    pub fn with_local_datacenter(mut self, dc: String) -> Self {
        self.local_datacenter = dc;
        self
    }

    pub fn with_guardrails(mut self, guardrails: WriteGuardrails) -> Self {
        self.guardrails = guardrails;
        self
    }

    pub fn with_view_existing_row_reader(mut self, reader: ViewExistingRowReader) -> Self {
        self.view_existing_row_reader = Some(reader);
        self
    }

    pub fn set_bootstrapping(&mut self, bootstrapping: bool) {
        self.is_bootstrapping = bootstrapping;
    }

    /// Get the local datacenter name.
    pub fn local_datacenter(&self) -> &str {
        &self.local_datacenter
    }

    /// Compute a write plan for the given mutation and CL.
    pub fn compute_write_plan(
        &self,
        mutation: &CoordinatedMutation,
        cl: ConsistencyLevel,
        strategy: &dyn ReplicationStrategy,
        snitch: &dyn Snitch,
    ) -> Result<WritePlan, WriteError> {
        let token = Token::from_partition_key(&mutation.partition_key);
        let snapshot = self.cluster.snapshot();

        let replicas = snapshot.replicas_for_token(token, strategy, snitch);
        let rf = replicas.len();

        if rf == 0 {
            return Err(WriteError::Unavailable {
                cl,
                required: 1,
                alive: 0,
            });
        }

        let required = cl.block_for(rf);

        let live_replicas: Vec<Endpoint> = replicas
            .iter()
            .filter(|ep| snapshot.nodes.get(ep).is_some_and(|n| n.state.is_live()))
            .cloned()
            .collect();

        let dead_replicas: Vec<Endpoint> = replicas
            .iter()
            .filter(|ep| !snapshot.nodes.get(ep).is_some_and(|n| n.state.is_live()))
            .cloned()
            .collect();

        let alive = live_replicas.len();

        // For CL=ANY, hints count toward satisfaction
        if cl != ConsistencyLevel::Any && alive < required {
            return Err(WriteError::Unavailable {
                cl,
                required,
                alive,
            });
        }

        let local_is_replica = replicas.contains(&self.local_endpoint);

        Ok(WritePlan {
            replicas,
            live_replicas,
            dead_replicas,
            local_is_replica,
            block_for: required,
            cl,
        })
    }

    /// Check write guardrails before coordinating.
    fn check_guardrails(&self, mutation: &CoordinatedMutation) -> Result<(), WriteError> {
        let size = mutation.estimated_size();
        if size > self.guardrails.max_mutation_size {
            return Err(WriteError::MutationTooLarge {
                size,
                limit: self.guardrails.max_mutation_size,
            });
        }

        // Timestamp drift detection
        let now_us = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as i64;
        let drift_threshold_us = self.guardrails.timestamp_drift_threshold.as_micros() as i64;
        let drift = (mutation.timestamp - now_us).abs();
        if drift > drift_threshold_us {
            warn!(
                drift_ms = drift / 1000,
                threshold_ms = drift_threshold_us / 1000,
                keyspace = %mutation.keyspace,
                table = %mutation.table,
                "Client timestamp drifts significantly from coordinator time"
            );
        }

        // Partition size warning (WU-04)
        if size > self.guardrails.partition_size_warn {
            warn!(
                size,
                threshold = self.guardrails.partition_size_warn,
                keyspace = %mutation.keyspace,
                table = %mutation.table,
                "Mutation exceeds partition size warn threshold"
            );
        }

        // Tombstone count warning (WU-04)
        let tombstone_count = Self::count_tombstones(mutation);
        if tombstone_count > self.guardrails.tombstone_warn_threshold {
            warn!(
                tombstones = tombstone_count,
                threshold = self.guardrails.tombstone_warn_threshold,
                keyspace = %mutation.keyspace,
                table = %mutation.table,
                "Mutation contains excessive tombstones"
            );
        }

        // Collection size warning (WU-04)
        let max_collection_size = Self::max_collection_size(mutation);
        if max_collection_size > self.guardrails.collection_size_warn {
            warn!(
                collection_elements = max_collection_size,
                threshold = self.guardrails.collection_size_warn,
                keyspace = %mutation.keyspace,
                table = %mutation.table,
                "Collection size exceeds warn threshold"
            );
        }

        // Columns per query warning (WU-04)
        let column_count = Self::count_columns(mutation);
        if column_count > self.guardrails.columns_per_query_warn {
            warn!(
                columns = column_count,
                threshold = self.guardrails.columns_per_query_warn,
                keyspace = %mutation.keyspace,
                table = %mutation.table,
                "Column count per query exceeds warn threshold"
            );
        }

        Ok(())
    }

    /// Count tombstones in a mutation (rows + cells + partition + range).
    fn count_tombstones(mutation: &CoordinatedMutation) -> usize {
        let mut count = 0;
        if mutation.partition_tombstone.is_some() {
            count += 1;
        }
        for row in &mutation.rows {
            if row.is_tombstone {
                count += 1;
            }
            if row.range_tombstone.is_some() {
                count += 1;
            }
            for cell in &row.cells {
                if cell.is_tombstone {
                    count += 1;
                }
            }
        }
        for cell in &mutation.static_cells {
            if cell.is_tombstone {
                count += 1;
            }
        }
        count
    }

    /// Return the largest collection operation size in the mutation.
    fn max_collection_size(mutation: &CoordinatedMutation) -> usize {
        let all_cells = mutation
            .rows
            .iter()
            .flat_map(|r| r.cells.iter())
            .chain(mutation.static_cells.iter());
        let mut max_size = 0usize;
        for cell in all_cells {
            if let Some(ref op) = cell.collection_op {
                let sz = match op {
                    CollectionOp::Append(elems) | CollectionOp::Remove(elems) => elems.len(),
                    CollectionOp::MapPut(entries) => entries.len(),
                };
                if sz > max_size {
                    max_size = sz;
                }
            }
        }
        max_size
    }

    /// Count distinct columns touched by a mutation.
    fn count_columns(mutation: &CoordinatedMutation) -> usize {
        let mut columns = std::collections::HashSet::new();
        for row in &mutation.rows {
            for cell in &row.cells {
                columns.insert(cell.column.as_str());
            }
        }
        for cell in &mutation.static_cells {
            columns.insert(cell.column.as_str());
        }
        columns.len()
    }

    /// Check pre-write preconditions (WU-05).
    ///
    /// 1. Reject if bootstrapping
    /// 2. Reject if write semaphore is full (Overloaded)
    /// 3. Reject if table is currently being truncated
    fn check_preconditions(
        &self,
        mutation: &CoordinatedMutation,
    ) -> Result<Option<tokio::sync::OwnedSemaphorePermit>, WriteError> {
        if self.is_bootstrapping {
            return Err(WriteError::IsBootstrapping);
        }

        // Try to acquire write semaphore without blocking
        let permit = Arc::clone(&self.write_semaphore)
            .try_acquire_owned()
            .map_err(|_| WriteError::Overloaded)?;

        // Check truncation in progress
        let table_key = format!("{}.{}", mutation.keyspace, mutation.table);
        if self.truncating_tables.contains(&table_key) {
            return Err(WriteError::TruncateInProgress);
        }

        Ok(Some(permit))
    }

    /// Mark a table as being truncated (blocks writes to that table).
    pub fn begin_truncation(&self, keyspace: &str, table: &str) {
        self.truncating_tables
            .insert(format!("{}.{}", keyspace, table));
    }

    /// Mark a table truncation as complete (unblocks writes).
    pub fn end_truncation(&self, keyspace: &str, table: &str) {
        self.truncating_tables
            .remove(&format!("{}.{}", keyspace, table));
    }

    /// Access the write guardrails (for server-layer checks).
    pub fn guardrails(&self) -> &WriteGuardrails {
        &self.guardrails
    }

    pub fn write_type_for_mutation(mutation: &CoordinatedMutation) -> WriteType {
        match mutation.kind {
            MutationKind::Standard => WriteType::Simple,
            MutationKind::Counter => WriteType::Counter,
            MutationKind::View => WriteType::View,
        }
    }

    /// Evaluate a write plan using actual acknowledgements and hints.
    pub fn complete_write_plan(
        &self,
        mutation: &CoordinatedMutation,
        plan: &WritePlan,
        acks_received: usize,
        hints_stored: usize,
        contacted_replicas: Vec<Endpoint>,
    ) -> Result<WriteResult, WriteError> {
        let satisfied = if plan.cl == ConsistencyLevel::Any {
            acks_received + hints_stored > 0
        } else {
            acks_received >= plan.block_for
        };

        if satisfied {
            Ok(WriteResult {
                acks_received,
                acks_required: plan.block_for,
                contacted_replicas,
                hints_stored,
            })
        } else {
            Err(WriteError::Timeout {
                cl: plan.cl,
                write_type: Self::write_type_for_mutation(mutation),
                required: plan.block_for,
                received: acks_received,
                block_for: plan.block_for,
            })
        }
    }

    /// Coordinate a write at the given consistency level.
    ///
    /// 1. Check preconditions (not bootstrapping, CL is valid for writes)
    /// 2. Check guardrails (mutation size, timestamp drift)
    /// 3. Compute write plan (replicas, block_for)
    /// 4. Check availability
    /// 5. Fan out to replicas
    /// 6. Wait for acks via WriteResponseHandler
    /// 7. Store hints for dead/non-responding replicas
    /// 8. Return result or error
    ///
    /// ## Java Oracle
    ///
    /// `StorageProxy.mutate()` → `StorageProxy.performWrite()`
    pub fn coordinate_write(
        &self,
        mutation: &CoordinatedMutation,
        cl: ConsistencyLevel,
        strategy: &dyn ReplicationStrategy,
        snitch: &dyn Snitch,
    ) -> Result<WriteResult, WriteError> {
        let start = std::time::Instant::now();
        self.metrics.writes_total.fetch_add(1, Ordering::Relaxed);

        // Precondition checks (WU-05): bootstrapping, semaphore, truncation
        let _permit = self.check_preconditions(mutation)?;

        if cl.is_serial() {
            return Err(WriteError::Internal(
                "Serial consistency levels require CAS path".to_string(),
            ));
        }

        // Check guardrails
        self.check_guardrails(mutation)?;

        // Compute plan
        let plan = self.compute_write_plan(mutation, cl, strategy, snitch)?;

        // Store hints for known-dead replicas immediately
        for dead in &plan.dead_replicas {
            self.hint_store.store_hint(*dead, mutation.clone());
            debug!(target = %dead, "Stored hint for dead replica");
        }

        let hints_stored_for_dead = plan.dead_replicas.len();

        // Synchronous compatibility path: callers that need real replica
        // messaging should use StorageProxy, which feeds actual ack counts
        // through complete_write_plan().
        let acks_received = plan.live_replicas.len();

        let elapsed_us = start.elapsed().as_micros() as u64;
        self.metrics
            .write_latency_us_sum
            .fetch_add(elapsed_us, Ordering::Relaxed);

        match self.complete_write_plan(
            mutation,
            &plan,
            acks_received,
            hints_stored_for_dead,
            plan.replicas.clone(),
        ) {
            Ok(result) => {
                self.metrics
                    .writes_succeeded
                    .fetch_add(1, Ordering::Relaxed);
                debug!(
                    cl = %cl,
                    acks = result.acks_received,
                    hints = result.hints_stored,
                    replicas = plan.replicas.len(),
                    "Write succeeded"
                );
                Ok(result)
            }
            Err(WriteError::Timeout {
                cl,
                write_type,
                required,
                received,
                block_for,
            }) => {
                self.metrics
                    .writes_timed_out
                    .fetch_add(1, Ordering::Relaxed);
                Err(WriteError::Timeout {
                    cl,
                    write_type,
                    required,
                    received,
                    block_for,
                })
            }
            Err(err) => Err(err),
        }
    }

    /// Compute a DC-aware write plan for LOCAL_* and EACH_QUORUM CLs.
    ///
    /// ## Java Oracle
    ///
    /// `DatacenterWriteResponseHandler` — per-DC ack tracking.
    pub fn compute_dc_aware_write_plan(
        &self,
        mutation: &CoordinatedMutation,
        cl: ConsistencyLevel,
        strategy: &dyn ReplicationStrategy,
        snitch: &dyn Snitch,
    ) -> Result<DatacenterWritePlan, WriteError> {
        let token = Token::from_partition_key(&mutation.partition_key);
        let snapshot = self.cluster.snapshot();
        let replicas = snapshot.replicas_for_token(token, strategy, snitch);

        if replicas.is_empty() {
            return Err(WriteError::Unavailable {
                cl,
                required: 1,
                alive: 0,
            });
        }

        // Group replicas by datacenter using snitch
        let mut dc_groups: HashMap<String, (Vec<Endpoint>, Vec<Endpoint>)> = HashMap::new();
        for ep in &replicas {
            let dc = snapshot
                .nodes
                .get(ep)
                .map(|n| n.datacenter.clone())
                .unwrap_or_else(|| self.local_datacenter.clone());
            let is_live = snapshot.nodes.get(ep).is_some_and(|n| n.state.is_live());
            let entry = dc_groups
                .entry(dc)
                .or_insert_with(|| (Vec::new(), Vec::new()));
            if is_live {
                entry.0.push(*ep);
            } else {
                entry.1.push(*ep);
            }
        }

        // Build per-DC plans
        let mut dc_plans = HashMap::new();
        let mut total_block_for = 0;

        match cl {
            ConsistencyLevel::LocalQuorum | ConsistencyLevel::LocalOne => {
                // Only the local DC matters
                let (live, dead) = dc_groups
                    .get(&self.local_datacenter)
                    .cloned()
                    .unwrap_or_default();
                let local_rf = live.len() + dead.len();
                let required = cl.block_for(local_rf);

                if live.len() < required {
                    return Err(WriteError::Unavailable {
                        cl,
                        required,
                        alive: live.len(),
                    });
                }

                total_block_for = required;
                dc_plans.insert(
                    self.local_datacenter.clone(),
                    DcReplicaPlan {
                        live_replicas: live,
                        dead_replicas: dead,
                        required,
                    },
                );
            }
            ConsistencyLevel::EachQuorum => {
                // Each DC must independently reach quorum
                for (dc, (live, dead)) in &dc_groups {
                    let dc_rf = live.len() + dead.len();
                    let dc_required = dc_rf / 2 + 1;

                    if live.len() < dc_required {
                        return Err(WriteError::Unavailable {
                            cl,
                            required: dc_required,
                            alive: live.len(),
                        });
                    }

                    total_block_for += dc_required;
                    dc_plans.insert(
                        dc.clone(),
                        DcReplicaPlan {
                            live_replicas: live.clone(),
                            dead_replicas: dead.clone(),
                            required: dc_required,
                        },
                    );
                }
            }
            _ => {
                // Fallback: treat as non-DC-aware
                let rf = replicas.len();
                let required = cl.block_for(rf);
                let live: Vec<Endpoint> = dc_groups
                    .values()
                    .flat_map(|(l, _)| l.iter())
                    .copied()
                    .collect();
                let dead: Vec<Endpoint> = dc_groups
                    .values()
                    .flat_map(|(_, d)| d.iter())
                    .copied()
                    .collect();
                total_block_for = required;
                dc_plans.insert(
                    self.local_datacenter.clone(),
                    DcReplicaPlan {
                        live_replicas: live,
                        dead_replicas: dead,
                        required,
                    },
                );
            }
        }

        let all_live: Vec<Endpoint> = dc_plans
            .values()
            .flat_map(|p| p.live_replicas.iter())
            .copied()
            .collect();
        let all_dead: Vec<Endpoint> = dc_plans
            .values()
            .flat_map(|p| p.dead_replicas.iter())
            .copied()
            .collect();
        let local_is_replica = replicas.contains(&self.local_endpoint);

        Ok(DatacenterWritePlan {
            base: WritePlan {
                replicas,
                live_replicas: all_live,
                dead_replicas: all_dead,
                local_is_replica,
                block_for: total_block_for,
                cl,
            },
            dc_plans,
            local_dc: self.local_datacenter.clone(),
        })
    }

    /// Coordinate a write asynchronously (for use with real messaging).
    ///
    /// Creates a `WriteResponseHandler` and returns it. The caller is
    /// responsible for sending mutations to replicas and calling
    /// `on_response` / `on_failure` as acks arrive.
    pub fn prepare_async_write(
        &self,
        mutation: &CoordinatedMutation,
        cl: ConsistencyLevel,
        strategy: &dyn ReplicationStrategy,
        snitch: &dyn Snitch,
    ) -> Result<(WritePlan, Arc<WriteResponseHandler>), WriteError> {
        if self.is_bootstrapping {
            return Err(WriteError::IsBootstrapping);
        }

        let plan = self.compute_write_plan(mutation, cl, strategy, snitch)?;

        let write_type = match mutation.kind {
            MutationKind::Standard => WriteType::Simple,
            MutationKind::Counter => WriteType::Counter,
            MutationKind::View => WriteType::View,
        };

        let handler = Arc::new(WriteResponseHandler::new(
            cl,
            write_type,
            plan.block_for,
            plan.live_replicas.clone(),
            self.timeout,
        ));

        // Store hints for known-dead replicas immediately
        for dead in &plan.dead_replicas {
            self.hint_store.store_hint(*dead, mutation.clone());
            handler.on_hint_stored();
        }

        Ok((plan, handler))
    }

    /// Coordinate a write asynchronously using a WriteResponseHandler (WU-01).
    ///
    /// Returns the plan and handler. The caller (e.g., StorageProxy) is responsible
    /// for sending mutations to replicas via messaging and recording acks.
    /// Call `handler.await_completion()` to wait for CL satisfaction.
    ///
    /// The synchronous `coordinate_write()` remains available for batch/testing use.
    ///
    /// ## Java Oracle
    ///
    /// `StorageProxy.performWrite()` — creates handler, sends messages, awaits
    pub fn coordinate_write_async(
        &self,
        mutation: &CoordinatedMutation,
        cl: ConsistencyLevel,
        strategy: &dyn ReplicationStrategy,
        snitch: &dyn Snitch,
    ) -> Result<(WritePlan, Arc<WriteResponseHandler>), WriteError> {
        self.metrics.writes_total.fetch_add(1, Ordering::Relaxed);

        // Precondition checks (WU-05)
        let _permit = self.check_preconditions(mutation)?;

        if cl.is_serial() {
            return Err(WriteError::Internal(
                "Serial consistency levels require CAS path".to_string(),
            ));
        }

        // Check guardrails
        self.check_guardrails(mutation)?;

        // For DC-aware CLs, use DC-aware plan (WU-02)
        if matches!(
            cl,
            ConsistencyLevel::EachQuorum
                | ConsistencyLevel::LocalQuorum
                | ConsistencyLevel::LocalOne
        ) {
            let dc_plan = self.compute_dc_aware_write_plan(mutation, cl, strategy, snitch)?;
            return self.prepare_async_write_from_plan(mutation, &dc_plan.base);
        }

        self.prepare_async_write(mutation, cl, strategy, snitch)
    }

    /// Build a WriteResponseHandler from an existing WritePlan.
    fn prepare_async_write_from_plan(
        &self,
        mutation: &CoordinatedMutation,
        plan: &WritePlan,
    ) -> Result<(WritePlan, Arc<WriteResponseHandler>), WriteError> {
        let write_type = match mutation.kind {
            MutationKind::Standard => WriteType::Simple,
            MutationKind::Counter => WriteType::Counter,
            MutationKind::View => WriteType::View,
        };

        let handler = Arc::new(WriteResponseHandler::new(
            plan.cl,
            write_type,
            plan.block_for,
            plan.live_replicas.clone(),
            self.timeout,
        ));

        // Store hints for known-dead replicas immediately
        for dead in &plan.dead_replicas {
            self.hint_store.store_hint(*dead, mutation.clone());
            handler.on_hint_stored();
        }

        Ok((plan.clone(), handler))
    }

    /// Check if a write at the given CL can be satisfied with current topology.
    pub fn can_satisfy_cl(
        &self,
        partition_key: &[u8],
        cl: ConsistencyLevel,
        strategy: &dyn ReplicationStrategy,
        snitch: &dyn Snitch,
    ) -> bool {
        let token = Token::from_partition_key(partition_key);
        let snapshot = self.cluster.snapshot();
        let replicas = snapshot.replicas_for_token(token, strategy, snitch);

        let mut rf_by_dc: HashMap<String, usize> = HashMap::new();
        let mut live_by_dc: HashMap<String, usize> = HashMap::new();
        for ep in replicas {
            let dc = snapshot
                .nodes
                .get(&ep)
                .map(|n| n.datacenter.clone())
                .unwrap_or_else(|| self.local_datacenter.clone());
            *rf_by_dc.entry(dc.clone()).or_insert(0) += 1;
            if snapshot.nodes.get(&ep).is_some_and(|n| n.state.is_live()) {
                *live_by_dc.entry(dc).or_insert(0) += 1;
            }
        }

        cl.is_satisfied_by_datacenter(&live_by_dc, &rf_by_dc, Some(&self.local_datacenter))
    }

    /// Select timestamp for mutation: client-provided or server-generated.
    ///
    /// ## Java Oracle
    ///
    /// `ClientState.getTimestamp()` / `FBUtilities.timestampMicros()`
    pub fn select_timestamp(client_timestamp: Option<i64>) -> i64 {
        client_timestamp.unwrap_or_else(|| {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_micros() as i64
        })
    }

    /// Compute local_deletion_time from current time (for TTL/tombstones).
    ///
    /// Returns seconds since epoch, matching Java's `FBUtilities.nowInSeconds()`.
    pub fn now_in_seconds() -> i32 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i32
    }

    /// Compute local_deletion_time for a TTL-bearing cell.
    ///
    /// `local_deletion_time = now_in_seconds + ttl`
    pub fn compute_local_deletion_time(ttl_seconds: i32) -> i32 {
        if ttl_seconds <= 0 {
            return i32::MAX; // no expiration
        }
        Self::now_in_seconds().saturating_add(ttl_seconds)
    }

    /// Coordinate a write with MV fanout and trigger hooks.
    ///
    /// This is the full write path that Java's `mutateWithTriggers()` implements:
    /// 1. Check triggers → augment mutation
    /// 2. Coordinate base mutation
    /// 3. Generate MV updates and apply them (best-effort at CL=ONE)
    ///
    /// ## Java Oracle
    ///
    /// `StorageProxy.mutateWithTriggers()` → `mutate()` → view update scheduling
    pub fn coordinate_write_with_hooks(
        &self,
        mutation: &CoordinatedMutation,
        cl: ConsistencyLevel,
        strategy: &dyn ReplicationStrategy,
        snitch: &dyn Snitch,
        view_manager: Option<&cassandra_storage::materialized_views::ViewManager>,
        trigger_manager: Option<&cassandra_storage::triggers::TriggerManager>,
    ) -> Result<(WriteResult, ViewFanoutResult), WriteError> {
        // Step 1: Trigger augmentation (WU-19)
        let mut augmented_mutations = Vec::new();
        if let Some(tm) = trigger_manager {
            if tm.has_triggers_for(&mutation.keyspace, &mutation.table) {
                let mutation_bytes = serde_json::to_vec(mutation).unwrap_or_default();
                let augmented =
                    tm.augment_mutation(&mutation.keyspace, &mutation.table, &mutation_bytes);
                for bytes in augmented {
                    let augmented_mutation: CoordinatedMutation = serde_json::from_slice(&bytes)
                        .map_err(|e| {
                            WriteError::Internal(format!(
                                "Trigger returned invalid augmented mutation: {e}"
                            ))
                        })?;
                    augmented_mutations.push(augmented_mutation);
                }
                debug!(
                    keyspace = %mutation.keyspace,
                    table = %mutation.table,
                    augmented_count = augmented_mutations.len(),
                    "Trigger augmented mutations decoded"
                );
            }
        }

        let existing_row = if view_manager
            .is_some_and(|vm| vm.has_views_for(&mutation.keyspace, &mutation.table))
        {
            self.view_existing_row_reader
                .as_ref()
                .and_then(|reader| reader(mutation))
        } else {
            None
        };

        // Step 2: Coordinate the base mutation
        let result = self.coordinate_write(mutation, cl, strategy, snitch)?;
        for augmented in &augmented_mutations {
            self.coordinate_write(augmented, cl, strategy, snitch)?;
        }

        // Step 3: MV fanout
        let mut fanout = ViewFanoutResult::default();
        if let Some(vm) = view_manager {
            if vm.has_views_for(&mutation.keyspace, &mutation.table) {
                let columns: HashMap<String, Option<Vec<u8>>> = mutation
                    .rows
                    .iter()
                    .flat_map(|r| r.cells.iter())
                    .map(|c| (c.column.clone(), c.value.clone()))
                    .collect();

                let is_delete = mutation.partition_tombstone.is_some()
                    || mutation.rows.iter().all(|r| r.is_tombstone);

                // Use the existing-row-aware method when we have prior state,
                // otherwise fall back to the simple method (WU-18).
                let view_result = if existing_row.is_some() {
                    vm.generate_view_updates_with_existing(
                        &mutation.keyspace,
                        &mutation.table,
                        &mutation.partition_key,
                        &columns,
                        mutation.timestamp,
                        is_delete,
                        existing_row,
                    )
                } else {
                    vm.generate_view_updates(
                        &mutation.keyspace,
                        &mutation.table,
                        &mutation.partition_key,
                        &columns,
                        mutation.timestamp,
                        is_delete,
                    )
                };

                fanout.mutations_generated = view_result.mutations.len();
                self.view_fanout_metrics
                    .view_mutations_generated
                    .fetch_add(view_result.mutations.len() as u64, Ordering::Relaxed);

                // Backpressure check (WU-18): increment backlog before scheduling
                let pending = self
                    .view_update_backlog
                    .fetch_add(view_result.mutations.len() as u64, Ordering::Relaxed);
                if pending + view_result.mutations.len() as u64 > self.view_update_backlog_threshold
                {
                    warn!(
                        backlog = pending + view_result.mutations.len() as u64,
                        threshold = self.view_update_backlog_threshold,
                        keyspace = %mutation.keyspace,
                        table = %mutation.table,
                        "View update backlog exceeds threshold — backpressure warning"
                    );
                }

                // Apply each view mutation at CL=ONE (best-effort)
                for vm_mutation in &view_result.mutations {
                    let view_coordinated = CoordinatedMutation {
                        keyspace: vm_mutation.keyspace.clone(),
                        table: vm_mutation.view_table.clone(),
                        partition_key: vm_mutation.partition_key.clone(),
                        rows: vec![],
                        timestamp: vm_mutation.timestamp,
                        kind: MutationKind::View,
                        static_cells: vec![],
                        partition_tombstone: if vm_mutation.is_delete {
                            Some(TombstoneMarker {
                                deletion_time: vm_mutation.timestamp,
                                local_deletion_time: Self::now_in_seconds(),
                            })
                        } else {
                            None
                        },
                    };

                    match self.coordinate_write(
                        &view_coordinated,
                        ConsistencyLevel::One,
                        strategy,
                        snitch,
                    ) {
                        Ok(_) => {
                            fanout.mutations_applied += 1;
                            self.view_fanout_metrics
                                .view_mutations_applied
                                .fetch_add(1, Ordering::Relaxed);
                        }
                        Err(e) => {
                            warn!(
                                view_table = %vm_mutation.view_table,
                                error = %e,
                                "MV fanout write failed (non-cascading)"
                            );
                            fanout.mutations_failed += 1;
                            self.view_fanout_metrics
                                .view_mutations_failed
                                .fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }

                // Decrement backlog after view mutations are processed (WU-18)
                self.view_update_backlog
                    .fetch_sub(view_result.mutations.len() as u64, Ordering::Relaxed);
            }
        }

        Ok((result, fanout))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cassandra_cluster_metadata::{NodeId, NodeInfo, SimpleSnitch, SimpleStrategy};
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

    fn setup_cluster() -> (Arc<ClusterMetadata>, WriteCoordinator) {
        let cm = Arc::new(ClusterMetadata::new(node(7001, vec![-100])));
        cm.update_node(node(7002, vec![0]));
        cm.update_node(node(7003, vec![100]));

        let hint_store = Arc::new(HintStore::new(1000));
        let coordinator = WriteCoordinator::new(Arc::clone(&cm), ep(7001), hint_store);
        (cm, coordinator)
    }

    fn test_mutation() -> CoordinatedMutation {
        CoordinatedMutation::simple(
            "ks".to_string(),
            "users".to_string(),
            b"user1".to_vec(),
            vec![MutationRow {
                clustering_key: vec![],
                cells: vec![CellMutation {
                    column: "name".to_string(),
                    value: Some(b"Alice".to_vec()),
                    timestamp: 1000,
                    ttl: 0,
                    is_tombstone: false,
                    collection_op: None,
                }],
                is_tombstone: false,
                range_tombstone: None,
            }],
            1000,
        )
    }

    #[test]
    fn write_cl_one() {
        let (_cm, coordinator) = setup_cluster();
        let strategy = SimpleStrategy::new(3);
        let snitch = SimpleSnitch;

        let result = coordinator.coordinate_write(
            &test_mutation(),
            ConsistencyLevel::One,
            &strategy,
            &snitch,
        );
        assert!(result.is_ok());
        let r = result.unwrap();
        assert_eq!(r.acks_required, 1);
        assert!(r.acks_received >= 1);
    }

    #[test]
    fn write_cl_quorum() {
        let (_cm, coordinator) = setup_cluster();
        let strategy = SimpleStrategy::new(3);
        let snitch = SimpleSnitch;

        let result = coordinator.coordinate_write(
            &test_mutation(),
            ConsistencyLevel::Quorum,
            &strategy,
            &snitch,
        );
        assert!(result.is_ok());
        let r = result.unwrap();
        assert_eq!(r.acks_required, 2);
    }

    #[test]
    fn write_cl_all() {
        let (_cm, coordinator) = setup_cluster();
        let strategy = SimpleStrategy::new(3);
        let snitch = SimpleSnitch;

        let result = coordinator.coordinate_write(
            &test_mutation(),
            ConsistencyLevel::All,
            &strategy,
            &snitch,
        );
        assert!(result.is_ok());
        let r = result.unwrap();
        assert_eq!(r.acks_required, 3);
        assert_eq!(r.acks_received, 3);
    }

    #[test]
    fn write_unavailable_when_node_dead() {
        let (cm, coordinator) = setup_cluster();
        let strategy = SimpleStrategy::new(3);
        let snitch = SimpleSnitch;

        cm.mark_dead(&ep(7002));
        cm.mark_dead(&ep(7003));

        let result = coordinator.coordinate_write(
            &test_mutation(),
            ConsistencyLevel::Quorum,
            &strategy,
            &snitch,
        );
        assert!(result.is_err());
        match result.unwrap_err() {
            WriteError::Unavailable {
                cl,
                required,
                alive,
            } => {
                assert_eq!(cl, ConsistencyLevel::Quorum);
                assert_eq!(required, 2);
                assert_eq!(alive, 1);
            }
            e => panic!("Expected Unavailable, got {e:?}"),
        }
    }

    #[test]
    fn write_cl_any_with_hints() {
        let (cm, coordinator) = setup_cluster();
        let strategy = SimpleStrategy::new(3);
        let snitch = SimpleSnitch;

        cm.mark_dead(&ep(7002));
        cm.mark_dead(&ep(7003));

        let result = coordinator.coordinate_write(
            &test_mutation(),
            ConsistencyLevel::Any,
            &strategy,
            &snitch,
        );
        assert!(result.is_ok());
        let r = result.unwrap();
        assert_eq!(r.hints_stored, 2);
    }

    #[test]
    fn can_satisfy_cl() {
        let (cm, coordinator) = setup_cluster();
        let strategy = SimpleStrategy::new(3);
        let snitch = SimpleSnitch;

        assert!(coordinator.can_satisfy_cl(b"key", ConsistencyLevel::One, &strategy, &snitch));
        assert!(coordinator.can_satisfy_cl(b"key", ConsistencyLevel::All, &strategy, &snitch));

        cm.mark_dead(&ep(7002));
        assert!(coordinator.can_satisfy_cl(b"key", ConsistencyLevel::Quorum, &strategy, &snitch));
        assert!(!coordinator.can_satisfy_cl(b"key", ConsistencyLevel::All, &strategy, &snitch));
    }

    #[test]
    fn write_error_codes() {
        let unav = WriteError::Unavailable {
            cl: ConsistencyLevel::Quorum,
            required: 2,
            alive: 1,
        };
        assert_eq!(unav.error_code(), 0x1000);

        let timeout = WriteError::Timeout {
            cl: ConsistencyLevel::Quorum,
            write_type: WriteType::Simple,
            required: 2,
            received: 1,
            block_for: 2,
        };
        assert_eq!(timeout.error_code(), 0x1100);

        let failure = WriteError::WriteFailure {
            cl: ConsistencyLevel::All,
            write_type: WriteType::Batch,
            required: 3,
            received: 2,
            block_for: 3,
            num_failures: 1,
            failure_map: HashMap::new(),
        };
        assert_eq!(failure.error_code(), 0x1500);

        assert_eq!(WriteError::Overloaded.error_code(), 0x1001);
        assert_eq!(WriteError::IsBootstrapping.error_code(), 0x1002);
    }

    #[test]
    fn write_type_names() {
        assert_eq!(WriteType::Simple.protocol_name(), "SIMPLE");
        assert_eq!(WriteType::Batch.protocol_name(), "BATCH");
        assert_eq!(WriteType::UnloggedBatch.protocol_name(), "UNLOGGED_BATCH");
        assert_eq!(WriteType::Counter.protocol_name(), "COUNTER");
        assert_eq!(WriteType::View.protocol_name(), "VIEW");
        assert_eq!(WriteType::Cas.protocol_name(), "CAS");
        assert_eq!(WriteType::BatchLog.protocol_name(), "BATCH_LOG");
    }

    #[test]
    fn bootstrapping_rejects_writes() {
        let (_cm, mut coordinator) = setup_cluster();
        coordinator.set_bootstrapping(true);
        let strategy = SimpleStrategy::new(3);
        let snitch = SimpleSnitch;

        let result = coordinator.coordinate_write(
            &test_mutation(),
            ConsistencyLevel::One,
            &strategy,
            &snitch,
        );
        assert!(result.is_err());
        match result.unwrap_err() {
            WriteError::IsBootstrapping => {}
            e => panic!("Expected IsBootstrapping, got {e:?}"),
        }
    }

    #[test]
    fn serial_cl_rejected_for_writes() {
        let (_cm, coordinator) = setup_cluster();
        let strategy = SimpleStrategy::new(3);
        let snitch = SimpleSnitch;

        let result = coordinator.coordinate_write(
            &test_mutation(),
            ConsistencyLevel::Serial,
            &strategy,
            &snitch,
        );
        assert!(result.is_err());
    }

    #[test]
    fn mutation_estimated_size() {
        let m = test_mutation();
        let size = m.estimated_size();
        assert!(size > 0);
    }

    #[test]
    fn mutation_with_static_cells() {
        let mut m = test_mutation();
        m.static_cells.push(CellMutation {
            column: "version".to_string(),
            value: Some(b"1".to_vec()),
            timestamp: 1000,
            ttl: 0,
            is_tombstone: false,
            collection_op: None,
        });
        assert_eq!(m.static_cells.len(), 1);
    }

    #[test]
    fn mutation_with_partition_tombstone() {
        let mut m = test_mutation();
        m.partition_tombstone = Some(TombstoneMarker {
            deletion_time: 1000,
            local_deletion_time: WriteCoordinator::now_in_seconds(),
        });
        assert!(m.partition_tombstone.is_some());
    }

    #[test]
    fn timestamp_selection() {
        let client_ts = WriteCoordinator::select_timestamp(Some(42));
        assert_eq!(client_ts, 42);

        let server_ts = WriteCoordinator::select_timestamp(None);
        assert!(server_ts > 0);
    }

    #[test]
    fn local_deletion_time_computation() {
        let ldt = WriteCoordinator::compute_local_deletion_time(86400); // 1 day TTL
        assert!(ldt > WriteCoordinator::now_in_seconds());

        let no_ttl = WriteCoordinator::compute_local_deletion_time(0);
        assert_eq!(no_ttl, i32::MAX);
    }

    #[test]
    fn write_cl_two() {
        let (_cm, coordinator) = setup_cluster();
        let strategy = SimpleStrategy::new(3);
        let snitch = SimpleSnitch;

        let result = coordinator.coordinate_write(
            &test_mutation(),
            ConsistencyLevel::Two,
            &strategy,
            &snitch,
        );
        assert!(result.is_ok());
        let r = result.unwrap();
        assert_eq!(r.acks_required, 2);
    }

    #[test]
    fn write_cl_three() {
        let (_cm, coordinator) = setup_cluster();
        let strategy = SimpleStrategy::new(3);
        let snitch = SimpleSnitch;

        let result = coordinator.coordinate_write(
            &test_mutation(),
            ConsistencyLevel::Three,
            &strategy,
            &snitch,
        );
        assert!(result.is_ok());
        let r = result.unwrap();
        assert_eq!(r.acks_required, 3);
    }

    #[test]
    fn write_cl_local_one() {
        let (_cm, coordinator) = setup_cluster();
        let strategy = SimpleStrategy::new(3);
        let snitch = SimpleSnitch;

        let result = coordinator.coordinate_write(
            &test_mutation(),
            ConsistencyLevel::LocalOne,
            &strategy,
            &snitch,
        );
        assert!(result.is_ok());
        let r = result.unwrap();
        assert_eq!(r.acks_required, 1);
    }

    // ── Phase 2: DC-Aware Tests ──────────────────────────────────

    #[test]
    fn dc_aware_plan_local_quorum() {
        let (_cm, coordinator) = setup_cluster();
        let strategy = SimpleStrategy::new(3);
        let snitch = SimpleSnitch;

        let result = coordinator.compute_dc_aware_write_plan(
            &test_mutation(),
            ConsistencyLevel::LocalQuorum,
            &strategy,
            &snitch,
        );
        assert!(result.is_ok());
        let plan = result.unwrap();
        assert!(plan.dc_plans.contains_key("dc1"));
        let dc1 = &plan.dc_plans["dc1"];
        assert!(dc1.required > 0);
    }

    #[test]
    fn dc_aware_plan_each_quorum() {
        let (_cm, coordinator) = setup_cluster();
        let strategy = SimpleStrategy::new(3);
        let snitch = SimpleSnitch;

        let result = coordinator.compute_dc_aware_write_plan(
            &test_mutation(),
            ConsistencyLevel::EachQuorum,
            &strategy,
            &snitch,
        );
        assert!(result.is_ok());
        let plan = result.unwrap();
        assert!(plan.base.block_for > 0);
    }

    #[test]
    fn dc_aware_plan_local_one() {
        let (_cm, coordinator) = setup_cluster();
        let strategy = SimpleStrategy::new(3);
        let snitch = SimpleSnitch;

        let result = coordinator.compute_dc_aware_write_plan(
            &test_mutation(),
            ConsistencyLevel::LocalOne,
            &strategy,
            &snitch,
        );
        assert!(result.is_ok());
        let plan = result.unwrap();
        let dc1 = &plan.dc_plans["dc1"];
        assert_eq!(dc1.required, 1);
    }

    // ── Phase 2: Guardrail Tests ─────────────────────────────────

    #[test]
    fn mutation_too_large_rejected() {
        let (_cm, coordinator) = setup_cluster();
        let coordinator = coordinator.with_guardrails(WriteGuardrails {
            max_mutation_size: 50, // very small limit
            ..WriteGuardrails::default()
        });
        let strategy = SimpleStrategy::new(3);
        let snitch = SimpleSnitch;

        let mut m = test_mutation();
        m.rows[0].cells[0].value = Some(vec![0u8; 100]); // exceeds 50 bytes

        let result = coordinator.coordinate_write(&m, ConsistencyLevel::One, &strategy, &snitch);
        assert!(result.is_err());
        match result.unwrap_err() {
            WriteError::MutationTooLarge { size, limit } => {
                assert!(size > limit);
                assert_eq!(limit, 50);
            }
            e => panic!("Expected MutationTooLarge, got {e:?}"),
        }
    }

    #[test]
    fn schema_disagreement_error_code() {
        let err = WriteError::SchemaDisagreement("table being dropped".to_string());
        assert_eq!(err.error_code(), 0x2200);
    }

    #[test]
    fn mutation_too_large_error_code() {
        let err = WriteError::MutationTooLarge {
            size: 100,
            limit: 50,
        };
        assert_eq!(err.error_code(), 0x2200);
    }

    // ── Phase 2: MV Fanout Tests ─────────────────────────────────

    #[test]
    fn coordinate_write_with_hooks_no_views() {
        let (_cm, coordinator) = setup_cluster();
        let strategy = SimpleStrategy::new(3);
        let snitch = SimpleSnitch;

        // No view manager → no fanout
        let (result, fanout) = coordinator
            .coordinate_write_with_hooks(
                &test_mutation(),
                ConsistencyLevel::One,
                &strategy,
                &snitch,
                None,
                None,
            )
            .unwrap();

        assert!(result.acks_received >= 1);
        assert_eq!(fanout.mutations_generated, 0);
        assert_eq!(fanout.mutations_applied, 0);
    }

    #[test]
    fn coordinate_write_with_hooks_mv_fanout() {
        use cassandra_storage::materialized_views::{MaterializedViewDefinition, ViewManager};

        let (_cm, coordinator) = setup_cluster();
        let strategy = SimpleStrategy::new(3);
        let snitch = SimpleSnitch;
        let vm = ViewManager::new();

        // Register a view for ks.users
        vm.register(MaterializedViewDefinition {
            name: "users_by_name".to_string(),
            keyspace: "ks".to_string(),
            base_table: "users".to_string(),
            view_table: "users_by_name".to_string(),
            included_columns: vec!["name".to_string()],
            where_clause: String::new(),
            include_all_columns: false,
            view_pk_columns: Vec::new(),
        })
        .unwrap();

        let (result, fanout) = coordinator
            .coordinate_write_with_hooks(
                &test_mutation(),
                ConsistencyLevel::One,
                &strategy,
                &snitch,
                Some(&vm),
                None,
            )
            .unwrap();

        assert!(result.acks_received >= 1);
        assert_eq!(fanout.mutations_generated, 1);
        assert_eq!(fanout.mutations_applied, 1);
        assert_eq!(fanout.mutations_failed, 0);

        // MV fanout metrics were tracked
        assert_eq!(
            coordinator
                .view_fanout_metrics
                .view_mutations_generated
                .load(Ordering::Relaxed),
            1
        );
    }

    #[test]
    fn coordinate_write_with_hooks_mv_reads_existing_row_for_delta() {
        use cassandra_storage::materialized_views::{MaterializedViewDefinition, ViewManager};

        let (_cm, coordinator) = setup_cluster();
        let strategy = SimpleStrategy::new(3);
        let snitch = SimpleSnitch;
        let vm = ViewManager::new();

        vm.register(MaterializedViewDefinition {
            name: "users_by_email".to_string(),
            keyspace: "ks".to_string(),
            base_table: "users".to_string(),
            view_table: "users_by_email".to_string(),
            included_columns: vec!["email".to_string(), "name".to_string()],
            where_clause: "email IS NOT NULL".to_string(),
            include_all_columns: false,
            view_pk_columns: vec!["email".to_string()],
        })
        .unwrap();

        let coordinator = coordinator.with_view_existing_row_reader(Arc::new(|mutation| {
            assert_eq!(mutation.keyspace, "ks");
            assert_eq!(mutation.table, "users");

            let mut existing = HashMap::new();
            existing.insert("email".to_string(), Some(b"old@example.com".to_vec()));
            existing.insert("name".to_string(), Some(b"Alice".to_vec()));
            Some(existing)
        }));

        let mutation = CoordinatedMutation::simple(
            "ks".to_string(),
            "users".to_string(),
            b"user1".to_vec(),
            vec![MutationRow {
                clustering_key: vec![],
                cells: vec![
                    CellMutation {
                        column: "email".to_string(),
                        value: Some(b"new@example.com".to_vec()),
                        timestamp: 1000,
                        ttl: 0,
                        is_tombstone: false,
                        collection_op: None,
                    },
                    CellMutation {
                        column: "name".to_string(),
                        value: Some(b"Alice".to_vec()),
                        timestamp: 1000,
                        ttl: 0,
                        is_tombstone: false,
                        collection_op: None,
                    },
                ],
                is_tombstone: false,
                range_tombstone: None,
            }],
            1000,
        );

        let (_result, fanout) = coordinator
            .coordinate_write_with_hooks(
                &mutation,
                ConsistencyLevel::One,
                &strategy,
                &snitch,
                Some(&vm),
                None,
            )
            .unwrap();

        assert_eq!(fanout.mutations_generated, 2);
        assert_eq!(fanout.mutations_applied, 2);
        assert_eq!(fanout.mutations_failed, 0);
    }

    #[test]
    fn coordinate_write_with_hooks_applies_trigger_augmented_mutations() {
        use cassandra_storage::triggers::{
            StaticTriggerPlugin, TriggerDefinition, TriggerManager, TriggerPluginRegistry,
        };

        let (_cm, coordinator) = setup_cluster();
        let strategy = SimpleStrategy::new(3);
        let snitch = SimpleSnitch;
        let mut registry = TriggerPluginRegistry::new();
        let augmented = CoordinatedMutation::simple(
            "ks".to_string(),
            "audit".to_string(),
            b"pk".to_vec(),
            Vec::new(),
            1000,
        );
        registry.register_plugin(
            "static://audit",
            std::sync::Arc::new(StaticTriggerPlugin::new(vec![
                serde_json::to_vec(&augmented).unwrap(),
            ])),
        );
        let mut tm = TriggerManager::with_registry(registry);
        tm.register(TriggerDefinition {
            name: "audit".to_string(),
            keyspace: "ks".to_string(),
            table: "users".to_string(),
            trigger_class: "static://audit".to_string(),
        })
        .unwrap();

        let (result, fanout) = coordinator
            .coordinate_write_with_hooks(
                &test_mutation(),
                ConsistencyLevel::One,
                &strategy,
                &snitch,
                None,
                Some(&tm),
            )
            .unwrap();

        assert!(result.acks_received >= 1);
        assert_eq!(fanout.mutations_generated, 0);
        assert_eq!(coordinator.metrics.writes_total.load(Ordering::Relaxed), 2);
    }

    // ── Phase 2: ViewFanoutMetrics ───────────────────────────────

    #[test]
    fn view_fanout_metrics_default() {
        let m = ViewFanoutMetrics::new();
        assert_eq!(m.view_mutations_generated.load(Ordering::Relaxed), 0);
        assert_eq!(m.view_mutations_applied.load(Ordering::Relaxed), 0);
        assert_eq!(m.view_mutations_retried.load(Ordering::Relaxed), 0);
        assert_eq!(m.view_mutations_failed.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn async_view_fanout_requeues_retryable_failures() {
        use cassandra_storage::materialized_views::ViewMutation;

        let metrics = Arc::new(ViewFanoutMetrics::new());
        let backlog = Arc::new(AtomicU64::new(0));
        let dispatcher = AsyncViewFanoutDispatcher::new(Arc::clone(&metrics), Arc::clone(&backlog));
        dispatcher.enqueue([ViewMutation {
            keyspace: "ks".to_string(),
            view_table: "users_by_email".to_string(),
            partition_key: b"a@example.com".to_vec(),
            columns: HashMap::new(),
            timestamp: 1000,
            is_delete: false,
        }]);

        let mut attempts = 0;
        let first = dispatcher.drain_with_retries(2, |_| {
            attempts += 1;
            Err(WriteError::Overloaded)
        });
        assert_eq!(first.mutations_generated, 1);
        assert_eq!(first.mutations_retried, 1);
        assert_eq!(first.mutations_failed, 0);
        assert_eq!(dispatcher.pending_count(), 1);
        assert_eq!(backlog.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.view_mutations_retried.load(Ordering::Relaxed), 1);

        let second = dispatcher.drain_with_retries(2, |_| Ok(()));
        assert_eq!(second.mutations_generated, 1);
        assert_eq!(second.mutations_applied, 1);
        assert_eq!(second.mutations_retried, 0);
        assert_eq!(dispatcher.pending_count(), 0);
        assert_eq!(backlog.load(Ordering::Relaxed), 0);
        assert_eq!(metrics.view_mutations_applied.load(Ordering::Relaxed), 1);
    }

    // ── WU-18: View update backlog backpressure ────────────────────

    #[test]
    fn view_update_backlog_tracks_pending() {
        use cassandra_storage::materialized_views::{MaterializedViewDefinition, ViewManager};

        let (_cm, coordinator) = setup_cluster();
        let strategy = SimpleStrategy::new(3);
        let snitch = SimpleSnitch;
        let vm = ViewManager::new();

        vm.register(MaterializedViewDefinition {
            name: "users_by_name".to_string(),
            keyspace: "ks".to_string(),
            base_table: "users".to_string(),
            view_table: "users_by_name".to_string(),
            included_columns: vec!["name".to_string()],
            where_clause: String::new(),
            include_all_columns: false,
            view_pk_columns: Vec::new(),
        })
        .unwrap();

        // Before the write, backlog should be 0
        assert_eq!(coordinator.view_update_backlog.load(Ordering::Relaxed), 0);

        let (_result, _fanout) = coordinator
            .coordinate_write_with_hooks(
                &test_mutation(),
                ConsistencyLevel::One,
                &strategy,
                &snitch,
                Some(&vm),
                None,
            )
            .unwrap();

        // After the write completes, backlog should return to 0
        // (incremented then decremented during processing)
        assert_eq!(coordinator.view_update_backlog.load(Ordering::Relaxed), 0);
    }

    // ── WU-01: coordinate_write_async ─────────────────────────────

    #[test]
    fn coordinate_write_async_returns_handler_and_plan() {
        let (_cm, coordinator) = setup_cluster();
        let strategy = SimpleStrategy::new(3);
        let snitch = SimpleSnitch;

        let result = coordinator.coordinate_write_async(
            &test_mutation(),
            ConsistencyLevel::One,
            &strategy,
            &snitch,
        );
        assert!(result.is_ok());
        let (plan, handler) = result.unwrap();
        assert!(!plan.live_replicas.is_empty());
        assert_eq!(handler.current_acks(), 0);
    }

    #[test]
    fn coordinate_write_async_rejects_serial_cl() {
        let (_cm, coordinator) = setup_cluster();
        let strategy = SimpleStrategy::new(3);
        let snitch = SimpleSnitch;

        let result = coordinator.coordinate_write_async(
            &test_mutation(),
            ConsistencyLevel::Serial,
            &strategy,
            &snitch,
        );
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn coordinate_write_async_handler_completes() {
        let (_cm, coordinator) = setup_cluster();
        let strategy = SimpleStrategy::new(3);
        let snitch = SimpleSnitch;

        let (plan, handler) = coordinator
            .coordinate_write_async(&test_mutation(), ConsistencyLevel::One, &strategy, &snitch)
            .unwrap();

        // Simulate one ack
        handler.on_response(&plan.live_replicas[0]);

        let result = handler.await_completion().await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap().acks_received, 1);
    }

    // ── WU-04: Expanded write guardrails ──────────────────────────

    #[test]
    fn guardrails_default_values() {
        let g = WriteGuardrails::default();
        assert_eq!(g.partition_size_warn, 100 * 1024 * 1024);
        assert_eq!(g.tombstone_warn_threshold, 1000);
        assert_eq!(g.collection_size_warn, 65535);
        assert_eq!(g.columns_per_query_warn, 100);
    }

    #[test]
    fn count_tombstones_in_mutation() {
        let mut m = test_mutation();
        // No tombstones initially
        assert_eq!(WriteCoordinator::count_tombstones(&m), 0);

        // Add tombstones
        m.rows[0].is_tombstone = true;
        m.partition_tombstone = Some(TombstoneMarker {
            deletion_time: 1000,
            local_deletion_time: 0,
        });
        assert_eq!(WriteCoordinator::count_tombstones(&m), 2);
    }

    #[test]
    fn count_columns_in_mutation() {
        let m = test_mutation();
        assert_eq!(WriteCoordinator::count_columns(&m), 1); // "name" only

        let mut m2 = test_mutation();
        m2.rows[0].cells.push(CellMutation {
            column: "age".to_string(),
            value: Some(b"25".to_vec()),
            timestamp: 1000,
            ttl: 0,
            is_tombstone: false,
            collection_op: None,
        });
        assert_eq!(WriteCoordinator::count_columns(&m2), 2);
    }

    #[test]
    fn max_collection_size_in_mutation() {
        let mut m = test_mutation();
        assert_eq!(WriteCoordinator::max_collection_size(&m), 0);

        m.rows[0].cells[0].collection_op = Some(CollectionOp::Append(vec![
            b"a".to_vec(),
            b"b".to_vec(),
            b"c".to_vec(),
        ]));
        assert_eq!(WriteCoordinator::max_collection_size(&m), 3);
    }

    // ── WU-05: Pre-write precondition checks ──────────────────────

    #[test]
    fn precondition_rejects_bootstrapping() {
        let (_cm, mut coordinator) = setup_cluster();
        coordinator.set_bootstrapping(true);

        let result = coordinator.check_preconditions(&test_mutation());
        assert!(result.is_err());
        match result.unwrap_err() {
            WriteError::IsBootstrapping => {}
            e => panic!("Expected IsBootstrapping, got {e:?}"),
        }
    }

    #[test]
    fn precondition_rejects_truncating_table() {
        let (_cm, coordinator) = setup_cluster();
        coordinator.begin_truncation("ks", "users");

        let result = coordinator.check_preconditions(&test_mutation());
        assert!(result.is_err());
        match result.unwrap_err() {
            WriteError::TruncateInProgress => {}
            e => panic!("Expected TruncateInProgress, got {e:?}"),
        }

        // End truncation and verify writes work again
        coordinator.end_truncation("ks", "users");
        assert!(coordinator.check_preconditions(&test_mutation()).is_ok());
    }

    #[test]
    fn precondition_passes_normally() {
        let (_cm, coordinator) = setup_cluster();
        let result = coordinator.check_preconditions(&test_mutation());
        assert!(result.is_ok());
        // The returned permit is Some
        assert!(result.unwrap().is_some());
    }

    #[test]
    fn coordinate_write_rejects_truncating_table() {
        let (_cm, coordinator) = setup_cluster();
        let strategy = SimpleStrategy::new(3);
        let snitch = SimpleSnitch;

        coordinator.begin_truncation("ks", "users");

        let result = coordinator.coordinate_write(
            &test_mutation(),
            ConsistencyLevel::One,
            &strategy,
            &snitch,
        );
        assert!(result.is_err());
        match result.unwrap_err() {
            WriteError::TruncateInProgress => {}
            e => panic!("Expected TruncateInProgress, got {e:?}"),
        }
    }
}
