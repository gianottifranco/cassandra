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

//! Counter read-before-write coordination.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.service.StorageProxy.mutateCounter()`
//! - `org.apache.cassandra.db.CounterMutation`
//! - `org.apache.cassandra.service.AbstractWriteResponseHandler`
//!
//! ## Architecture
//!
//! Counter updates require a read-before-write to fetch the current
//! CounterContext CRDT, apply the delta, and then replicate the fully
//! merged context to all replicas.
//!
//! The process is:
//! 1. Coordinator receives `CounterMutation`.
//! 2. Coordinator computes a `WritePlan`.
//! 3. Coordinator selects one replica to act as the "leader" for this operation.
//! 4. Leader reads its local `CounterContext`, applies the delta, and returns the merged context.
//! 5. Coordinator sends the merged context to all replicas in the `WritePlan`.
//! 6. Coordinator waits for acks according to the ConsistencyLevel.

use std::sync::Arc;

use dashmap::DashMap;
use tracing::debug;
use uuid::Uuid;

use cassandra_cluster_metadata::{Endpoint, ReplicationStrategy, Snitch};

use crate::consistency::ConsistencyLevel;
use crate::write::{CoordinatedMutation, WriteCoordinator, WriteError, WriteResult};

/// Simulated remote node State for testing counter leadership
pub struct CounterReplica {
    pub endpoint: Endpoint,
    pub node_id: Uuid,
    /// partition_key -> serialized CounterContext
    pub storage: DashMap<Vec<u8>, Vec<u8>>,
}

impl CounterReplica {
    pub fn new(endpoint: Endpoint, node_id: Uuid) -> Self {
        Self {
            endpoint,
            node_id,
            storage: DashMap::new(),
        }
    }

    /// The read-before-write phase on the leader.
    /// Reads the current context (or creates empty), applies delta, and returns the serialized result.
    pub fn read_before_write(&self, partition_key: &[u8], delta: i64) -> Vec<u8> {
        let mut ctx = match self.storage.get(partition_key) {
            Some(data) => {
                cassandra_storage::counter::CounterContext::deserialize(&data).unwrap_or_default()
            }
            None => cassandra_storage::counter::CounterContext::new(),
        };

        // Apply local delta
        ctx.apply_local(self.node_id, delta);

        let serialized = ctx.serialize();
        // Leader persists it immediately before returning
        self.storage
            .insert(partition_key.to_vec(), serialized.clone());

        serialized
    }

    /// The standard write/merge phase.
    pub fn apply_merged_context(&self, partition_key: &[u8], merged_context: &[u8]) {
        let incoming = cassandra_storage::counter::CounterContext::deserialize(merged_context)
            .unwrap_or_default();

        let mut current = match self.storage.get(partition_key) {
            Some(data) => {
                cassandra_storage::counter::CounterContext::deserialize(&data).unwrap_or_default()
            }
            None => cassandra_storage::counter::CounterContext::new(),
        };

        current.merge(&incoming);
        self.storage
            .insert(partition_key.to_vec(), current.serialize());
    }
}

pub struct CounterCoordinator {
    write_coordinator: Arc<WriteCoordinator>,
    replicas: Arc<DashMap<Endpoint, CounterReplica>>,
}

impl CounterCoordinator {
    pub fn new(
        write_coordinator: Arc<WriteCoordinator>,
        replicas: Arc<DashMap<Endpoint, CounterReplica>>,
    ) -> Self {
        Self {
            write_coordinator,
            replicas,
        }
    }

    /// Coordinate a counter mutation.
    pub fn coordinate_counter(
        &self,
        mutation: &CoordinatedMutation,
        delta: i64,
        cl: ConsistencyLevel,
        strategy: &dyn ReplicationStrategy,
        snitch: &dyn Snitch,
    ) -> Result<WriteResult, WriteError> {
        // 1. Compute Write Plan
        let plan = self
            .write_coordinator
            .compute_write_plan(mutation, cl, strategy, snitch)?;

        if plan.live_replicas.is_empty() {
            return Err(WriteError::Unavailable {
                cl,
                required: plan.block_for,
                alive: 0,
            });
        }

        // 2. Select Leader (we just pick the first live replica for simplicity here)
        let leader_endpoint = plan.live_replicas[0];
        let leader = self.replicas.get(&leader_endpoint).ok_or_else(|| {
            WriteError::Internal("Leader replica not found in registry".to_string())
        })?;

        // 3. Read Before Write
        debug!(leader = %leader_endpoint, "Sending ReadBeforeWrite to leader");
        let merged_context = leader.read_before_write(&mutation.partition_key, delta);

        // 4. Send merged context to all replicas
        let mut acks = 0;
        for ep in &plan.live_replicas {
            if let Some(replica) = self.replicas.get(ep) {
                replica.apply_merged_context(&mutation.partition_key, &merged_context);
                acks += 1;
            }
        }

        // Handle hints for dead replicas (omitted here, handled by write_coordinator in full path)
        let satisfied = if cl == ConsistencyLevel::Any {
            acks + plan.dead_replicas.len() > 0
        } else {
            acks >= plan.block_for
        };

        if satisfied {
            self.write_coordinator
                .metrics
                .writes_succeeded
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Ok(WriteResult {
                acks_received: acks,
                acks_required: plan.block_for,
                contacted_replicas: plan.replicas,
                hints_stored: plan.dead_replicas.len(),
            })
        } else {
            self.write_coordinator
                .metrics
                .writes_timed_out
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Err(WriteError::Timeout {
                cl,
                write_type: crate::write::WriteType::Counter,
                required: plan.block_for,
                received: acks,
                block_for: plan.block_for,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hints::HintStore;
    use cassandra_cluster_metadata::{ClusterMetadata, NodeId, NodeInfo, SimpleStrategy};
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

    fn setup_env() -> (
        Arc<WriteCoordinator>,
        Arc<DashMap<Endpoint, CounterReplica>>,
    ) {
        let replicas_map = Arc::new(DashMap::new());

        // Use NodeInfo array to set up cluster
        let n1 = node(7001, vec![100]);
        let n2 = node(7002, vec![200]);
        let n3 = node(7003, vec![300]);

        replicas_map.insert(ep(7001), CounterReplica::new(ep(7001), Uuid::new_v4()));
        replicas_map.insert(ep(7002), CounterReplica::new(ep(7002), Uuid::new_v4()));
        replicas_map.insert(ep(7003), CounterReplica::new(ep(7003), Uuid::new_v4()));

        let cm = Arc::new(ClusterMetadata::new(n1));
        cm.update_node(n2);
        cm.update_node(n3);

        let coord = Arc::new(WriteCoordinator::new(
            cm,
            ep(7001),
            Arc::new(HintStore::new(10000)),
        ));

        (coord, replicas_map)
    }

    #[test]
    fn test_counter_read_before_write_flow() {
        let (write_coord, replicas) = setup_env();
        let counter_coord = CounterCoordinator::new(write_coord, replicas.clone());

        let mutation = CoordinatedMutation {
            keyspace: "ks".to_string(),
            table: "tbl".to_string(),
            partition_key: b"pk1".to_vec(),
            rows: vec![],
            timestamp: 1000,
            kind: crate::write::MutationKind::Counter,
            static_cells: vec![],
            partition_tombstone: None,
        };

        struct DummySnitch;
        impl Snitch for DummySnitch {
            fn datacenter(&self, _endpoint: &Endpoint) -> String {
                "dc1".to_string()
            }
            fn rack(&self, _endpoint: &Endpoint) -> String {
                "rack1".to_string()
            }
        }
        let snitch = DummySnitch;
        let strategy = SimpleStrategy {
            replication_factor: 3,
        };

        // 1. Initial increment
        let res = counter_coord.coordinate_counter(
            &mutation,
            10,
            ConsistencyLevel::Quorum,
            &strategy,
            &snitch,
        );
        assert!(res.is_ok());

        // Verify across replicas
        for rep in replicas.iter() {
            let data = rep.storage.get(b"pk1".as_slice()).unwrap();
            let ctx = cassandra_storage::counter::CounterContext::deserialize(&data).unwrap();
            assert_eq!(ctx.total(), 10);
        }

        // 2. Second increment
        let res2 = counter_coord.coordinate_counter(
            &mutation,
            5,
            ConsistencyLevel::Quorum,
            &strategy,
            &snitch,
        );
        assert!(res2.is_ok());

        for rep in replicas.iter() {
            let data = rep.storage.get(b"pk1".as_slice()).unwrap();
            let ctx = cassandra_storage::counter::CounterContext::deserialize(&data).unwrap();
            assert_eq!(ctx.total(), 15);
        }
    }
}
