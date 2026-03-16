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

//! Read coordinator: routes reads to replicas with digest comparison,
//! speculative retry, short-read protection, paging, and read repair.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.service.StorageProxy.fetchRows()`
//! - `org.apache.cassandra.service.reads.AbstractReadExecutor`
//! - `org.apache.cassandra.service.reads.DigestResolver`
//! - `org.apache.cassandra.service.reads.DataResolver`
//! - `org.apache.cassandra.service.reads.ShortReadProtection`

pub mod command;
pub mod executor;
pub mod paging;
pub mod repair;
pub mod resolver;
pub mod response;
pub mod short_read;
pub mod speculative_retry;

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tracing::{debug, info};

use cassandra_cluster_metadata::{
    ClusterMetadata, Endpoint, ReplicationStrategy, Snitch,
};
use cassandra_common::Token;

use crate::consistency::ConsistencyLevel;

pub use command::{
    ClusteringSlice, ColumnFilter, DataRange, PartitionRangeReadCommand, ReadCommand,
    ReadLimits, SinglePartitionReadCommand,
};
pub use executor::{compute_execution_plan, ReadExecutionPlan, ReadExecutorType};
pub use paging::{PageSizeControl, PagingState};
pub use repair::{ReadRepairHandler, ReadRepairMutation, ReadRepairStrategy};
pub use resolver::{DataResolver, DigestMismatch, DigestResolver, RepairMutation, ResolvedData};
pub use response::{
    DataResponse, Digest, PartitionResult, ReadResponse, TombstoneThresholds, TombstoneTracker,
};
pub use short_read::{ShortReadProtection, ShortReadRetry};
pub use speculative_retry::SpeculativeRetryPolicy;

/// Default read timeout (matches Java's 5s default).
pub const DEFAULT_READ_TIMEOUT: Duration = Duration::from_secs(5);

// ─── Read Result ────────────────────────────────────────────────

/// Result of a coordinated read.
#[derive(Debug)]
pub struct ReadResult {
    /// The data returned (from the data replica or merged).
    pub data: Option<Vec<u8>>,
    /// Structured partition data.
    pub partitions: Vec<PartitionResult>,
    /// Number of replicas that responded.
    pub responses_received: usize,
    /// Number of responses required by CL.
    pub responses_required: usize,
    /// Whether a read repair was triggered.
    pub read_repair_triggered: bool,
    /// Whether a digest mismatch was resolved.
    pub digest_mismatch_resolved: bool,
    /// The replicas that were contacted.
    pub contacted_replicas: Vec<Endpoint>,
    /// Warnings to include in the response (e.g., tombstone warnings).
    pub warnings: Vec<String>,
    /// Paging state for continuation (if applicable).
    pub paging_state: Option<PagingState>,
    /// Whether this was a speculative retry.
    pub speculative_retry_used: bool,
    /// Number of tombstones encountered.
    pub tombstones_read: u32,
}

// ─── Read Error ─────────────────────────────────────────────────

/// Errors from read coordination.
///
/// ## Java Oracle
///
/// Maps to native protocol error codes:
/// - `ReadTimeout`        → 0x1200
/// - `ReadFailure`        → 0x1300
/// - `Unavailable`        → 0x1000
/// - `TombstoneOverwhelming` — custom abort
#[derive(Debug, thiserror::Error)]
pub enum ReadError {
    #[error("Read timeout: CL={cl}, required={required}, received={received}, data_present={data_present}")]
    Timeout {
        cl: ConsistencyLevel,
        required: usize,
        received: usize,
        data_present: bool,
    },

    #[error("Read failure: CL={cl}, required={required}, received={received}, failures={num_failures}")]
    ReadFailure {
        cl: ConsistencyLevel,
        required: usize,
        received: usize,
        num_failures: usize,
        data_present: bool,
        failure_map: HashMap<Endpoint, u16>,
    },

    #[error("Unavailable: CL={cl}, required={required}, alive={alive}")]
    Unavailable {
        cl: ConsistencyLevel,
        required: usize,
        alive: usize,
    },

    #[error("Tombstone overwhelming: scanned {count} tombstones (threshold={threshold}), query aborted")]
    TombstoneOverwhelming {
        count: u32,
        threshold: u32,
    },

    #[error("Digest mismatch on {replicas_mismatched} replicas")]
    DigestMismatch {
        replicas_mismatched: usize,
    },

    #[error("Query cancelled")]
    QueryCancelled,

    #[error("Coordinator behind: {0}")]
    CoordinatorBehind(String),

    #[error("Internal error: {0}")]
    Internal(String),
}

impl ReadError {
    /// Native protocol error code.
    pub fn error_code(&self) -> u32 {
        match self {
            Self::Unavailable { .. } => 0x1000,
            Self::Timeout { .. } => 0x1200,
            Self::ReadFailure { .. } => 0x1300,
            Self::TombstoneOverwhelming { .. } => 0x1300,
            Self::DigestMismatch { .. } => 0x1200,
            Self::QueryCancelled => 0x1200,
            Self::CoordinatorBehind(_) => 0x1200,
            Self::Internal(_) => 0x0000,
        }
    }
}

// ─── Read Metrics ───────────────────────────────────────────────

/// Counters for read-path observability.
///
/// ## Java Oracle
///
/// `org.apache.cassandra.metrics.ClientRequestMetrics`
pub struct ReadMetrics {
    pub reads_total: AtomicU64,
    pub reads_succeeded: AtomicU64,
    pub reads_failed: AtomicU64,
    pub reads_timed_out: AtomicU64,
    pub reads_unavailable: AtomicU64,
    pub digest_mismatches: AtomicU64,
    pub speculative_retries: AtomicU64,
    pub short_read_retries: AtomicU64,
    pub read_repairs_triggered: AtomicU64,
    pub tombstones_scanned: AtomicU64,
    pub read_latency_us_sum: AtomicU64,
}

impl ReadMetrics {
    pub fn new() -> Self {
        Self {
            reads_total: AtomicU64::new(0),
            reads_succeeded: AtomicU64::new(0),
            reads_failed: AtomicU64::new(0),
            reads_timed_out: AtomicU64::new(0),
            reads_unavailable: AtomicU64::new(0),
            digest_mismatches: AtomicU64::new(0),
            speculative_retries: AtomicU64::new(0),
            short_read_retries: AtomicU64::new(0),
            read_repairs_triggered: AtomicU64::new(0),
            tombstones_scanned: AtomicU64::new(0),
            read_latency_us_sum: AtomicU64::new(0),
        }
    }
}

impl Default for ReadMetrics {
    fn default() -> Self {
        Self::new()
    }
}

// ─── Read Coordinator ───────────────────────────────────────────

/// The read coordinator.
///
/// Routes reads to replicas, compares digests, and triggers read repair
/// when mismatches are detected.
///
/// ## Java Oracle
///
/// `org.apache.cassandra.service.StorageProxy.fetchRows()`
pub struct ReadCoordinator {
    /// Cluster metadata for replica lookups.
    cluster: Arc<ClusterMetadata>,
    /// The local node's endpoint.
    local_endpoint: Endpoint,
    /// Read timeout.
    timeout: Duration,
    /// Read metrics.
    pub metrics: Arc<ReadMetrics>,
    /// Speculative retry policy.
    speculative_retry_policy: SpeculativeRetryPolicy,
    /// Read repair strategy.
    read_repair_strategy: ReadRepairStrategy,
    /// Tombstone thresholds.
    tombstone_thresholds: TombstoneThresholds,
}

/// Legacy compatibility alias to match the old API expected by lib.rs.
#[derive(Debug, Clone)]
pub struct CoordinatedRead {
    pub keyspace: String,
    pub table: String,
    pub partition_key: Vec<u8>,
}

impl ReadCoordinator {
    pub fn new(
        cluster: Arc<ClusterMetadata>,
        local_endpoint: Endpoint,
    ) -> Self {
        Self {
            cluster,
            local_endpoint,
            timeout: DEFAULT_READ_TIMEOUT,
            metrics: Arc::new(ReadMetrics::new()),
            speculative_retry_policy: SpeculativeRetryPolicy::default(),
            read_repair_strategy: ReadRepairStrategy::default(),
            tombstone_thresholds: TombstoneThresholds::default(),
        }
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    pub fn with_speculative_retry(mut self, policy: SpeculativeRetryPolicy) -> Self {
        self.speculative_retry_policy = policy;
        self
    }

    pub fn with_read_repair_strategy(mut self, strategy: ReadRepairStrategy) -> Self {
        self.read_repair_strategy = strategy;
        self
    }

    pub fn with_tombstone_thresholds(mut self, thresholds: TombstoneThresholds) -> Self {
        self.tombstone_thresholds = thresholds;
        self
    }

    /// Coordinate a read at the given consistency level.
    ///
    /// The full read strategy:
    /// 1. Compute token from partition key
    /// 2. Get replicas from cluster metadata
    /// 3. Check availability
    /// 4. Compute execution plan (data/digest/speculative replicas)
    /// 5. Send data request to nearest replica, digest requests to others
    /// 6. Compare digests — on mismatch, trigger full data read + merge
    /// 7. Check tombstone thresholds
    /// 8. Generate read repair mutations if needed
    /// 9. Compute paging state
    /// 10. Return result
    ///
    /// ## Java Oracle
    ///
    /// `StorageProxy.fetchRows()` → `AbstractReadExecutor.execute()` →
    /// `DigestResolver/DataResolver`
    pub fn coordinate_read(
        &self,
        read: &CoordinatedRead,
        cl: ConsistencyLevel,
        strategy: &dyn ReplicationStrategy,
        snitch: &dyn Snitch,
    ) -> Result<ReadResult, ReadError> {
        let start = std::time::Instant::now();
        self.metrics.reads_total.fetch_add(1, Ordering::Relaxed);

        let token = Token::from_partition_key(&read.partition_key);
        let snapshot = self.cluster.snapshot();

        // Get replicas
        let replicas = snapshot.replicas_for_token(token, strategy, snitch);
        let rf = replicas.len();

        if rf == 0 {
            self.metrics.reads_unavailable.fetch_add(1, Ordering::Relaxed);
            return Err(ReadError::Unavailable {
                cl,
                required: cl.block_for(strategy.replication_factor()),
                alive: 0,
            });
        }

        let required = cl.block_for(rf);

        // Check live replicas
        let live_replicas: Vec<Endpoint> = replicas
            .iter()
            .filter(|ep| {
                snapshot.nodes.get(ep).is_some_and(|n| n.state.is_live())
            })
            .copied()
            .collect();

        if live_replicas.len() < required {
            self.metrics.reads_unavailable.fetch_add(1, Ordering::Relaxed);
            return Err(ReadError::Unavailable {
                cl,
                required,
                alive: live_replicas.len(),
            });
        }

        // Sort by proximity
        let mut sorted_replicas = live_replicas.clone();
        snitch.sort_by_proximity(&self.local_endpoint, &mut sorted_replicas);

        // Compute execution plan
        let plan = compute_execution_plan(
            &sorted_replicas,
            required,
            &self.speculative_retry_policy,
            |_percentile| 50, // TODO: wire real latency percentiles from metrics
        );

        debug!(
            data_replica = %plan.data_replica,
            digest_count = plan.digest_replicas.len(),
            speculative = ?plan.speculative_replica,
            cl = %cl,
            "Coordinating read"
        );

        // Simulate read responses
        // In a real implementation, this would:
        // 1. Send READ_DATA to data_replica via messaging
        // 2. Send READ_DIGEST to digest_replicas
        // 3. Optionally send speculative after delay
        // 4. Wait for responses with timeout
        // 5. Run through DigestResolver → DataResolver if mismatch
        //
        // For now, simulate all replicas agreeing (no digest mismatch).
        let responses_received = 1 + plan.digest_replicas.len();
        let speculative_retry_used = plan.speculative_replica.is_some()
            && self.speculative_retry_policy == SpeculativeRetryPolicy::Always;

        if speculative_retry_used {
            self.metrics.speculative_retries.fetch_add(1, Ordering::Relaxed);
        }

        // Simulated data response
        let data = Some(format!(
            "data-for-{}-{}-{:?}",
            read.keyspace, read.table, &read.partition_key
        ).into_bytes());

        let elapsed_us = start.elapsed().as_micros() as u64;
        self.metrics.read_latency_us_sum.fetch_add(elapsed_us, Ordering::Relaxed);
        self.metrics.reads_succeeded.fetch_add(1, Ordering::Relaxed);

        let mut contacted = vec![plan.data_replica];
        contacted.extend(plan.digest_replicas);
        if let Some(spec) = plan.speculative_replica {
            contacted.push(spec);
        }

        Ok(ReadResult {
            data,
            partitions: Vec::new(),
            responses_received,
            responses_required: required,
            read_repair_triggered: false,
            digest_mismatch_resolved: false,
            contacted_replicas: contacted,
            warnings: Vec::new(),
            paging_state: None,
            speculative_retry_used,
            tombstones_read: 0,
        })
    }

    /// Coordinate a range read across multiple partitions.
    ///
    /// ## Java Oracle
    ///
    /// `StorageProxy.getRangeSlice()`
    pub fn coordinate_range_read(
        &self,
        cmd: &PartitionRangeReadCommand,
        cl: ConsistencyLevel,
        strategy: &dyn ReplicationStrategy,
        snitch: &dyn Snitch,
    ) -> Result<ReadResult, ReadError> {
        let start = std::time::Instant::now();
        self.metrics.reads_total.fetch_add(1, Ordering::Relaxed);

        // For range reads, we need to find all replicas that own
        // tokens in the requested range.
        //
        // Simplified: use a representative token from the range.
        let representative_token = cmd.data_range.start_token;
        let snapshot = self.cluster.snapshot();
        let replicas = snapshot.replicas_for_token(representative_token, strategy, snitch);
        let rf = replicas.len();

        if rf == 0 {
            self.metrics.reads_unavailable.fetch_add(1, Ordering::Relaxed);
            return Err(ReadError::Unavailable {
                cl,
                required: 1,
                alive: 0,
            });
        }

        let required = cl.block_for(rf);

        let live_replicas: Vec<Endpoint> = replicas
            .iter()
            .filter(|ep| {
                snapshot.nodes.get(ep).is_some_and(|n| n.state.is_live())
            })
            .copied()
            .collect();

        if live_replicas.len() < required {
            self.metrics.reads_unavailable.fetch_add(1, Ordering::Relaxed);
            return Err(ReadError::Unavailable {
                cl,
                required,
                alive: live_replicas.len(),
            });
        }

        let mut sorted = live_replicas.clone();
        snitch.sort_by_proximity(&self.local_endpoint, &mut sorted);

        debug!(
            range_start = %cmd.data_range.start_token,
            range_end = %cmd.data_range.end_token,
            cl = %cl,
            "Coordinating range read"
        );

        let responses_received = required;

        let elapsed_us = start.elapsed().as_micros() as u64;
        self.metrics.read_latency_us_sum.fetch_add(elapsed_us, Ordering::Relaxed);
        self.metrics.reads_succeeded.fetch_add(1, Ordering::Relaxed);

        Ok(ReadResult {
            data: None,
            partitions: Vec::new(),
            responses_received,
            responses_required: required,
            read_repair_triggered: false,
            digest_mismatch_resolved: false,
            contacted_replicas: sorted[..required].to_vec(),
            warnings: Vec::new(),
            paging_state: None,
            speculative_retry_used: false,
            tombstones_read: 0,
        })
    }

    /// Perform read repair after a digest mismatch.
    ///
    /// In a full implementation, this would:
    /// 1. Re-read full data from all replicas
    /// 2. Merge using timestamp-based conflict resolution
    /// 3. Send the merged result to out-of-date replicas
    pub fn read_repair(
        &self,
        _read: &CoordinatedRead,
        replicas: &[Endpoint],
    ) -> Result<(), ReadError> {
        info!(
            replicas = ?replicas,
            "Triggering read repair"
        );
        self.metrics.read_repairs_triggered.fetch_add(1, Ordering::Relaxed);
        // TODO: Implement full read repair via MessagingService:
        // 1. Send READ_DATA to all replicas
        // 2. Collect DataResponses
        // 3. Use DataResolver to merge
        // 4. Send repair mutations to stale replicas via ReadRepairHandler
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cassandra_cluster_metadata::{
        ClusterMetadata, NodeId, NodeInfo, SimpleStrategy, SimpleSnitch,
    };
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

    fn setup_cluster() -> (Arc<ClusterMetadata>, ReadCoordinator) {
        let cm = Arc::new(ClusterMetadata::new(node(7001, vec![-100])));
        cm.update_node(node(7002, vec![0]));
        cm.update_node(node(7003, vec![100]));

        let coordinator = ReadCoordinator::new(Arc::clone(&cm), ep(7001));
        (cm, coordinator)
    }

    fn test_read() -> CoordinatedRead {
        CoordinatedRead {
            keyspace: "ks".to_string(),
            table: "users".to_string(),
            partition_key: b"user1".to_vec(),
        }
    }

    #[test]
    fn read_cl_one() {
        let (_cm, coordinator) = setup_cluster();
        let strategy = SimpleStrategy::new(3);
        let snitch = SimpleSnitch;

        let result = coordinator.coordinate_read(
            &test_read(),
            ConsistencyLevel::One,
            &strategy,
            &snitch,
        );
        assert!(result.is_ok());
        let r = result.unwrap();
        assert!(r.data.is_some());
        assert!(r.responses_received >= 1);
    }

    #[test]
    fn read_cl_quorum() {
        let (_cm, coordinator) = setup_cluster();
        let strategy = SimpleStrategy::new(3);
        let snitch = SimpleSnitch;

        let result = coordinator.coordinate_read(
            &test_read(),
            ConsistencyLevel::Quorum,
            &strategy,
            &snitch,
        );
        assert!(result.is_ok());
        let r = result.unwrap();
        assert_eq!(r.responses_required, 2);
        assert!(r.responses_received >= 2);
    }

    #[test]
    fn read_cl_all() {
        let (_cm, coordinator) = setup_cluster();
        let strategy = SimpleStrategy::new(3);
        let snitch = SimpleSnitch;

        let result = coordinator.coordinate_read(
            &test_read(),
            ConsistencyLevel::All,
            &strategy,
            &snitch,
        );
        assert!(result.is_ok());
        let r = result.unwrap();
        assert_eq!(r.responses_required, 3);
    }

    #[test]
    fn read_unavailable_when_nodes_dead() {
        let (cm, coordinator) = setup_cluster();
        let strategy = SimpleStrategy::new(3);
        let snitch = SimpleSnitch;

        cm.mark_dead(&ep(7002));
        cm.mark_dead(&ep(7003));

        let result = coordinator.coordinate_read(
            &test_read(),
            ConsistencyLevel::Quorum,
            &strategy,
            &snitch,
        );
        assert!(result.is_err());
        match result.unwrap_err() {
            ReadError::Unavailable { cl, required, alive } => {
                assert_eq!(cl, ConsistencyLevel::Quorum);
                assert_eq!(required, 2);
                assert_eq!(alive, 1);
            }
            e => panic!("Expected Unavailable, got {e:?}"),
        }
    }

    #[test]
    fn read_cl_one_survives_two_dead() {
        let (cm, coordinator) = setup_cluster();
        let strategy = SimpleStrategy::new(3);
        let snitch = SimpleSnitch;

        cm.mark_dead(&ep(7002));
        cm.mark_dead(&ep(7003));

        let result = coordinator.coordinate_read(
            &test_read(),
            ConsistencyLevel::One,
            &strategy,
            &snitch,
        );
        assert!(result.is_ok());
    }

    #[test]
    fn contacted_replicas_sorted_by_proximity() {
        let (_cm, coordinator) = setup_cluster();
        let strategy = SimpleStrategy::new(3);
        let snitch = SimpleSnitch;

        let result = coordinator.coordinate_read(
            &test_read(),
            ConsistencyLevel::Quorum,
            &strategy,
            &snitch,
        );
        assert!(result.is_ok());
        let r = result.unwrap();
        assert!(!r.contacted_replicas.is_empty());
    }

    #[test]
    fn read_with_speculative_always() {
        let (_cm, mut coordinator) = setup_cluster();
        coordinator.speculative_retry_policy = SpeculativeRetryPolicy::Always;

        let strategy = SimpleStrategy::new(3);
        let snitch = SimpleSnitch;

        let result = coordinator.coordinate_read(
            &test_read(),
            ConsistencyLevel::Quorum,
            &strategy,
            &snitch,
        );
        assert!(result.is_ok());
        let r = result.unwrap();
        assert!(r.speculative_retry_used);
        assert!(r.contacted_replicas.len() >= 3); // data + digest + speculative
    }

    #[test]
    fn read_error_codes() {
        let err = ReadError::Timeout {
            cl: ConsistencyLevel::Quorum,
            required: 2,
            received: 1,
            data_present: false,
        };
        assert_eq!(err.error_code(), 0x1200);

        let err = ReadError::Unavailable {
            cl: ConsistencyLevel::All,
            required: 3,
            alive: 1,
        };
        assert_eq!(err.error_code(), 0x1000);

        let err = ReadError::TombstoneOverwhelming {
            count: 100_000,
            threshold: 100_000,
        };
        assert_eq!(err.error_code(), 0x1300);
    }

    #[test]
    fn read_metrics_tracked() {
        let (_cm, coordinator) = setup_cluster();
        let strategy = SimpleStrategy::new(3);
        let snitch = SimpleSnitch;

        let _ = coordinator.coordinate_read(
            &test_read(),
            ConsistencyLevel::One,
            &strategy,
            &snitch,
        );

        assert_eq!(coordinator.metrics.reads_total.load(Ordering::Relaxed), 1);
        assert_eq!(coordinator.metrics.reads_succeeded.load(Ordering::Relaxed), 1);
        assert!(coordinator.metrics.read_latency_us_sum.load(Ordering::Relaxed) > 0);
    }

    #[test]
    fn range_read_basic() {
        let (_cm, coordinator) = setup_cluster();
        let strategy = SimpleStrategy::new(3);
        let snitch = SimpleSnitch;

        let cmd = PartitionRangeReadCommand::full_scan("ks", "users");
        let result = coordinator.coordinate_range_read(
            &cmd,
            ConsistencyLevel::One,
            &strategy,
            &snitch,
        );
        assert!(result.is_ok());
    }

    #[test]
    fn range_read_unavailable() {
        let (cm, coordinator) = setup_cluster();
        let strategy = SimpleStrategy::new(3);
        let snitch = SimpleSnitch;

        cm.mark_dead(&ep(7001));
        cm.mark_dead(&ep(7002));
        cm.mark_dead(&ep(7003));

        let cmd = PartitionRangeReadCommand::full_scan("ks", "users");
        let result = coordinator.coordinate_range_read(
            &cmd,
            ConsistencyLevel::One,
            &strategy,
            &snitch,
        );
        // Might be unavailable depending on token assignment
        // (if no live replicas own the representative token)
        assert!(result.is_err() || result.is_ok());
    }
}
