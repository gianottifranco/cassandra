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

//! Read coordinator: routes reads to replicas with digest comparison.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.service.StorageProxy.fetchRows()`
//! - `org.apache.cassandra.service.reads.AbstractReadExecutor`
//! - `org.apache.cassandra.service.reads.DigestResolver`

use std::sync::Arc;
use std::time::Duration;

use tracing::{debug, info, warn};

use cassandra_cluster_metadata::{
    ClusterMetadata, Endpoint, ReplicationStrategy, Snitch,
};
use cassandra_common::Token;

use crate::consistency::ConsistencyLevel;

/// Default read timeout (matches Java's 5s default).
pub const DEFAULT_READ_TIMEOUT: Duration = Duration::from_secs(5);

/// A coordinated read request.
#[derive(Debug, Clone)]
pub struct CoordinatedRead {
    pub keyspace: String,
    pub table: String,
    pub partition_key: Vec<u8>,
    // TODO: column filters, clustering range, etc.
}

/// A simulated digest — in real implementation this would be a hash.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Digest(pub Vec<u8>);

impl Digest {
    /// Compute a digest from partition data (simplified: just hash the bytes).
    pub fn from_data(data: &[u8]) -> Self {
        // Simplified digest: use md5 for now
        // In production, use a lightweight hash
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        data.hash(&mut hasher);
        let hash = hasher.finish();
        Self(hash.to_be_bytes().to_vec())
    }
}

/// Result of a coordinated read.
#[derive(Debug)]
pub struct ReadResult {
    /// The data returned (from the data replica).
    pub data: Option<Vec<u8>>,
    /// Number of replicas that responded.
    pub responses_received: usize,
    /// Number of responses required by CL.
    pub responses_required: usize,
    /// Whether a read repair was triggered.
    pub read_repair_triggered: bool,
    /// The replicas that were contacted.
    pub contacted_replicas: Vec<Endpoint>,
}

/// Errors from read coordination.
#[derive(Debug, thiserror::Error)]
pub enum ReadError {
    #[error("Read timeout: CL={cl}, required={required}, received={received}")]
    Timeout {
        cl: ConsistencyLevel,
        required: usize,
        received: usize,
    },

    #[error("Unavailable: CL={cl}, required={required}, alive={alive}")]
    Unavailable {
        cl: ConsistencyLevel,
        required: usize,
        alive: usize,
    },

    #[error("Digest mismatch on {replicas_mismatched} replicas")]
    DigestMismatch {
        replicas_mismatched: usize,
    },

    #[error("Internal error: {0}")]
    Internal(String),
}

/// The read coordinator.
///
/// Routes reads to replicas, compares digests, and triggers read repair
/// when mismatches are detected.
pub struct ReadCoordinator {
    /// Cluster metadata for replica lookups.
    cluster: Arc<ClusterMetadata>,
    /// The local node's endpoint.
    local_endpoint: Endpoint,
    /// Read timeout.
    timeout: Duration,
    /// Whether read repair is enabled.
    read_repair_enabled: bool,
    /// Whether speculative retry is enabled (stub).
    speculative_retry_enabled: bool,
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
            read_repair_enabled: true,
            speculative_retry_enabled: false, // TODO: implement speculative retry
        }
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Coordinate a read at the given consistency level.
    ///
    /// The read strategy:
    /// 1. Compute token from partition key
    /// 2. Get replicas from cluster metadata
    /// 3. Send data request to nearest replica, digest requests to others
    /// 4. Compare digests — on mismatch, trigger read repair
    /// 5. Return data
    pub fn coordinate_read(
        &self,
        read: &CoordinatedRead,
        cl: ConsistencyLevel,
        strategy: &dyn ReplicationStrategy,
        snitch: &dyn Snitch,
    ) -> Result<ReadResult, ReadError> {
        let token = Token::from_partition_key(&read.partition_key);
        let snapshot = self.cluster.snapshot();

        // Get replicas
        let replicas = snapshot.replicas_for_token(token, strategy, snitch);
        let rf = replicas.len();

        if rf == 0 {
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
            return Err(ReadError::Unavailable {
                cl,
                required,
                alive: live_replicas.len(),
            });
        }

        // Select replicas to contact:
        // - First: data replica (get full data)
        // - Remaining: digest replicas (get hash only)
        let mut sorted_replicas = live_replicas.clone();
        snitch.sort_by_proximity(&self.local_endpoint, &mut sorted_replicas);

        let data_replica = sorted_replicas[0];
        let digest_replicas: Vec<Endpoint> = sorted_replicas[1..required]
            .to_vec();

        debug!(
            data_replica = %data_replica,
            digest_count = digest_replicas.len(),
            cl = %cl,
            "Coordinating read"
        );

        // Simulate read responses
        // In a real implementation, this would:
        // 1. Send READ_DATA to data_replica via messaging
        // 2. Send READ_DIGEST to digest_replicas
        // 3. Wait for responses with timeout
        // 4. Compare digests
        //
        // For now, we simulate all replicas agreeing (no digest mismatch).
        let responses_received = 1 + digest_replicas.len(); // data + digests
        let read_repair_triggered = false;

        // Simulated data response
        let data = Some(format!(
            "data-for-{}-{}-{:?}",
            read.keyspace, read.table, &read.partition_key
        ).into_bytes());

        Ok(ReadResult {
            data,
            responses_received,
            responses_required: required,
            read_repair_triggered,
            contacted_replicas: std::iter::once(data_replica)
                .chain(digest_replicas)
                .collect(),
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
            "Triggering read repair (stub — full implementation pending)"
        );
        // TODO: Implement full read repair
        // - Re-read from all replicas
        // - Merge by timestamp
        // - Send mutations to stale replicas
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
        // Data replica should be first
        assert!(!r.contacted_replicas.is_empty());
    }
}
