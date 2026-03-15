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

//! Immutable cluster metadata snapshot.
//!
//! `ClusterMetadata` is the read-side view of the cluster: who are the nodes,
//! what tokens do they own, and which replication strategies apply. It is
//! designed to be Arc-swapped so readers never block.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.locator.TokenMetadata`
//! - `org.apache.cassandra.config.DatabaseDescriptor` (cluster-level view)

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;
use uuid::Uuid;

use cassandra_common::Token;

use crate::node::{Endpoint, NodeInfo, NodeState};
use crate::replication::{create_strategy, ReplicationStrategy};
use crate::ring::TokenRing;
use crate::snitch::Snitch;

/// Immutable snapshot of the cluster's topology.
///
/// Created and replaced atomically. Readers always see a consistent view.
#[derive(Debug, Clone)]
pub struct ClusterSnapshot {
    /// Token ring (token → endpoint mapping).
    pub ring: TokenRing,
    /// Node information indexed by endpoint.
    pub nodes: HashMap<Endpoint, NodeInfo>,
    /// Schema version for agreement checks.
    pub schema_version: Option<Uuid>,
}

impl ClusterSnapshot {
    /// Get all live endpoints (state == Normal).
    pub fn live_endpoints(&self) -> Vec<Endpoint> {
        self.nodes
            .values()
            .filter(|n| n.state.is_live())
            .map(|n| n.endpoint)
            .collect()
    }

    /// Get replicas for a partition key using the given strategy.
    pub fn replicas_for_token(
        &self,
        token: Token,
        strategy: &dyn ReplicationStrategy,
        snitch: &dyn Snitch,
    ) -> Vec<Endpoint> {
        strategy.calculate_natural_endpoints(token, &self.ring, snitch)
    }

    /// Get replicas for a partition key (raw bytes).
    pub fn replicas_for_key(
        &self,
        partition_key: &[u8],
        strategy: &dyn ReplicationStrategy,
        snitch: &dyn Snitch,
    ) -> Vec<Endpoint> {
        let token = Token::from_partition_key(partition_key);
        self.replicas_for_token(token, strategy, snitch)
    }

    /// Number of known nodes (all states).
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }
}

/// Mutable cluster metadata manager.
///
/// Holds an `Arc<ClusterSnapshot>` that can be atomically swapped.
/// All writes go through this struct; readers clone the Arc for a
/// consistent snapshot.
pub struct ClusterMetadata {
    /// Current snapshot (Arc-swapped for lock-free reads).
    current: Arc<RwLock<Arc<ClusterSnapshot>>>,
    /// Local node endpoint.
    local_endpoint: Endpoint,
}

impl ClusterMetadata {
    /// Create new cluster metadata with the local node only.
    pub fn new(local_node: NodeInfo) -> Self {
        let endpoint = local_node.endpoint;
        let mut ring = TokenRing::new();
        ring.add_node(endpoint, &local_node.tokens);

        let mut nodes = HashMap::new();
        nodes.insert(endpoint, local_node);

        let snapshot = ClusterSnapshot {
            ring,
            nodes,
            schema_version: None,
        };

        Self {
            current: Arc::new(RwLock::new(Arc::new(snapshot))),
            local_endpoint: endpoint,
        }
    }

    /// Get the current cluster snapshot (lock-free read).
    pub fn snapshot(&self) -> Arc<ClusterSnapshot> {
        Arc::clone(&self.current.read())
    }

    /// Add or update a node in the cluster.
    pub fn update_node(&self, node: NodeInfo) {
        let mut guard = self.current.write();
        let old = &**guard;

        let mut ring = old.ring.clone();
        let mut nodes = old.nodes.clone();

        // Remove old tokens if the node existed
        ring.remove_node(&node.endpoint);

        // Add new tokens
        if node.state.owns_tokens() {
            ring.add_node(node.endpoint, &node.tokens);
        }

        nodes.insert(node.endpoint, node);

        *guard = Arc::new(ClusterSnapshot {
            ring,
            nodes,
            schema_version: old.schema_version,
        });
    }

    /// Remove a node from the cluster.
    pub fn remove_node(&self, endpoint: &Endpoint) {
        let mut guard = self.current.write();
        let old = &**guard;

        let mut ring = old.ring.clone();
        let mut nodes = old.nodes.clone();

        ring.remove_node(endpoint);
        nodes.remove(endpoint);

        *guard = Arc::new(ClusterSnapshot {
            ring,
            nodes,
            schema_version: old.schema_version,
        });
    }

    /// Mark a node as dead (but keep it in the node list for hints).
    pub fn mark_dead(&self, endpoint: &Endpoint) {
        let mut guard = self.current.write();
        let old = &**guard;

        let mut nodes = old.nodes.clone();
        if let Some(node) = nodes.get_mut(endpoint) {
            node.state = NodeState::Dead;
        }

        *guard = Arc::new(ClusterSnapshot {
            ring: old.ring.clone(),
            nodes,
            schema_version: old.schema_version,
        });
    }

    /// Mark a node as alive/normal.
    pub fn mark_alive(&self, endpoint: &Endpoint) {
        let mut guard = self.current.write();
        let old = &**guard;

        let mut nodes = old.nodes.clone();
        let mut ring = old.ring.clone();

        if let Some(node) = nodes.get_mut(endpoint) {
            node.state = NodeState::Normal;
            ring.add_node(*endpoint, &node.tokens);
        }

        *guard = Arc::new(ClusterSnapshot {
            ring,
            nodes,
            schema_version: old.schema_version,
        });
    }

    /// Update the cluster schema version.
    pub fn set_schema_version(&self, version: Uuid) {
        let mut guard = self.current.write();
        let old = &**guard;

        *guard = Arc::new(ClusterSnapshot {
            ring: old.ring.clone(),
            nodes: old.nodes.clone(),
            schema_version: Some(version),
        });
    }

    /// Get the local endpoint.
    pub fn local_endpoint(&self) -> Endpoint {
        self.local_endpoint
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::NodeId;
    use crate::replication::SimpleStrategy;
    use crate::snitch::SimpleSnitch;
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

    #[test]
    fn single_node_cluster() {
        let cm = ClusterMetadata::new(node(7001, vec![0]));
        let snap = cm.snapshot();

        assert_eq!(snap.node_count(), 1);
        assert_eq!(snap.ring.token_count(), 1);
        assert_eq!(snap.live_endpoints().len(), 1);
    }

    #[test]
    fn add_node() {
        let cm = ClusterMetadata::new(node(7001, vec![-100]));
        cm.update_node(node(7002, vec![0]));
        cm.update_node(node(7003, vec![100]));

        let snap = cm.snapshot();
        assert_eq!(snap.node_count(), 3);
        assert_eq!(snap.ring.token_count(), 3);
    }

    #[test]
    fn remove_node() {
        let cm = ClusterMetadata::new(node(7001, vec![0]));
        cm.update_node(node(7002, vec![100]));

        cm.remove_node(&ep(7002));
        let snap = cm.snapshot();
        assert_eq!(snap.node_count(), 1);
    }

    #[test]
    fn mark_dead_alive() {
        let cm = ClusterMetadata::new(node(7001, vec![0]));
        cm.update_node(node(7002, vec![100]));

        cm.mark_dead(&ep(7002));
        let snap = cm.snapshot();
        assert_eq!(snap.live_endpoints().len(), 1); // only 7001

        cm.mark_alive(&ep(7002));
        let snap = cm.snapshot();
        assert_eq!(snap.live_endpoints().len(), 2);
    }

    #[test]
    fn replicas_for_key() {
        let cm = ClusterMetadata::new(node(7001, vec![-100]));
        cm.update_node(node(7002, vec![0]));
        cm.update_node(node(7003, vec![100]));

        let snap = cm.snapshot();
        let strategy = SimpleStrategy::new(3);
        let snitch = SimpleSnitch;

        let replicas = snap.replicas_for_key(b"user123", &strategy, &snitch);
        assert_eq!(replicas.len(), 3);
    }

    #[test]
    fn concurrent_reads() {
        use std::thread;

        let cm = Arc::new(ClusterMetadata::new(node(7001, vec![0])));

        let cm2 = Arc::clone(&cm);
        let handle = thread::spawn(move || {
            for _ in 0..100 {
                let snap = cm2.snapshot();
                assert!(snap.node_count() > 0);
            }
        });

        for i in 0..10 {
            cm.update_node(node(7002 + i, vec![i as i64 * 100]));
        }

        handle.join().unwrap();
    }
}
