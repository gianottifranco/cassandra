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

use std::sync::Arc;
use std::time::Duration;

use tracing::{debug, info, warn};

use cassandra_cluster_metadata::{
    ClusterMetadata, ClusterSnapshot, Endpoint, ReplicationStrategy, Snitch,
};
use cassandra_common::Token;

use crate::consistency::ConsistencyLevel;

/// Default write timeout (matches Java's 2s default).
pub const DEFAULT_WRITE_TIMEOUT: Duration = Duration::from_secs(2);

/// A mutation to be coordinated across replicas.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CoordinatedMutation {
    pub keyspace: String,
    pub table: String,
    pub partition_key: Vec<u8>,
    pub rows: Vec<MutationRow>,
    pub timestamp: i64,
}

/// A row within a coordinated mutation.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MutationRow {
    pub clustering_key: Vec<u8>,
    pub cells: Vec<CellMutation>,
    pub is_tombstone: bool,
}

/// A cell mutation.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CellMutation {
    pub column: String,
    pub value: Option<Vec<u8>>,
    pub timestamp: i64,
    pub ttl: i32,
    pub is_tombstone: bool,
}

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
#[derive(Debug, thiserror::Error)]
pub enum WriteError {
    #[error("Write timeout: CL={cl}, required={required}, received={received}")]
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

    #[error("Internal error: {0}")]
    Internal(String),
}

/// The write coordinator.
///
/// Routes mutations to the correct replicas based on the partition key,
/// waits for the required number of acks (per CL), and handles failures.
pub struct WriteCoordinator {
    /// Cluster metadata for replica lookups.
    cluster: Arc<ClusterMetadata>,
    /// The local node's endpoint.
    local_endpoint: Endpoint,
    /// Write timeout.
    timeout: Duration,
}

impl WriteCoordinator {
    pub fn new(
        cluster: Arc<ClusterMetadata>,
        local_endpoint: Endpoint,
    ) -> Self {
        Self {
            cluster,
            local_endpoint,
            timeout: DEFAULT_WRITE_TIMEOUT,
        }
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Coordinate a write at the given consistency level.
    ///
    /// 1. Compute token from partition key
    /// 2. Get natural replicas from cluster metadata
    /// 3. Check if enough replicas are alive (else Unavailable)
    /// 4. Send mutation to replicas (local + remote)
    /// 5. Wait for block_for acks
    /// 6. Return success or timeout
    pub fn coordinate_write(
        &self,
        mutation: &CoordinatedMutation,
        cl: ConsistencyLevel,
        strategy: &dyn ReplicationStrategy,
        snitch: &dyn Snitch,
    ) -> Result<WriteResult, WriteError> {
        let token = Token::from_partition_key(&mutation.partition_key);
        let snapshot = self.cluster.snapshot();

        // Get replicas
        let replicas = snapshot.replicas_for_token(token, strategy, snitch);
        let rf = replicas.len();

        if rf == 0 {
            return Err(WriteError::Unavailable {
                cl,
                required: cl.block_for(strategy.replication_factor()),
                alive: 0,
            });
        }

        let required = cl.block_for(rf);

        // Check live replicas
        let live_replicas: Vec<&Endpoint> = replicas
            .iter()
            .filter(|ep| {
                snapshot.nodes.get(ep).is_some_and(|n| n.state.is_live())
            })
            .collect();

        let alive = live_replicas.len();

        // For CL=ANY, we can accept hints for dead replicas
        if cl != ConsistencyLevel::Any && alive < required {
            return Err(WriteError::Unavailable {
                cl,
                required,
                alive,
            });
        }

        // Simulate writing to replicas
        // In a real implementation, this would:
        // 1. Apply locally if self is a replica
        // 2. Send MUTATION messages to remote replicas via messaging service
        // 3. Wait for acks with timeout
        //
        // For now, we simulate immediate acks from all live replicas.
        let acks_received = alive;
        let hints_stored = replicas.len() - alive; // hints for dead replicas

        if cl.is_satisfied(acks_received, rf) || (cl == ConsistencyLevel::Any && acks_received + hints_stored > 0) {
            debug!(
                cl = %cl,
                acks = acks_received,
                hints = hints_stored,
                replicas = rf,
                "Write succeeded"
            );

            Ok(WriteResult {
                acks_received,
                acks_required: required,
                contacted_replicas: replicas.clone(),
                hints_stored,
            })
        } else {
            Err(WriteError::Timeout {
                cl,
                required,
                received: acks_received,
            })
        }
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
        let rf = replicas.len();

        let live_count = replicas
            .iter()
            .filter(|ep| {
                snapshot.nodes.get(ep).is_some_and(|n| n.state.is_live())
            })
            .count();

        cl.is_satisfied(live_count, rf)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cassandra_cluster_metadata::{NodeId, NodeInfo, NodeState, SimpleStrategy, SimpleSnitch};
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

        let coordinator = WriteCoordinator::new(Arc::clone(&cm), ep(7001));
        (cm, coordinator)
    }

    fn test_mutation() -> CoordinatedMutation {
        CoordinatedMutation {
            keyspace: "ks".to_string(),
            table: "users".to_string(),
            partition_key: b"user1".to_vec(),
            rows: vec![MutationRow {
                clustering_key: vec![],
                cells: vec![CellMutation {
                    column: "name".to_string(),
                    value: Some(b"Alice".to_vec()),
                    timestamp: 1000,
                    ttl: 0,
                    is_tombstone: false,
                }],
                is_tombstone: false,
            }],
            timestamp: 1000,
        }
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

        // Kill two nodes
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
            WriteError::Unavailable { cl, required, alive } => {
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

        // Kill two nodes — CL=ANY should still succeed with hints
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
}
