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

//! Token ownership, replica placement, and data placement tracking.
//!
//! Provides structures for managing which nodes own which token ranges,
//! how replicas are distributed across the cluster for different replication
//! strategies, and how placement changes are tracked during topology
//! transitions.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.tcm.ownership.DataPlacements`
//! - `org.apache.cassandra.tcm.ownership.DataPlacement`
//! - `org.apache.cassandra.tcm.ownership.PlacementDeltas`
//! - `org.apache.cassandra.tcm.ownership.PlacementProvider`
//! - `org.apache.cassandra.tcm.ownership.VersionedEndpoints`

use std::collections::{BTreeMap, HashMap};
use std::hash::{Hash, Hasher};

use serde::{Deserialize, Serialize};

use cassandra_common::token::{Token, TokenRange};

use crate::node::{Endpoint, NodeId};
use crate::tcm::Epoch;

// ─────────────────────────────────────────────────────────────────────────────
// ReplicationParams
// ─────────────────────────────────────────────────────────────────────────────

/// Parameters that identify a replication strategy and its configuration.
///
/// Each keyspace has a `ReplicationParams` that determines how data is
/// replicated (e.g., `SimpleStrategy` with RF=3, or `NetworkTopologyStrategy`
/// with per-DC replication factors).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicationParams {
    /// Fully qualified strategy class name.
    pub strategy_class: String,
    /// Strategy-specific options (e.g., `"replication_factor" -> "3"`).
    pub options: HashMap<String, String>,
}

impl Hash for ReplicationParams {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.strategy_class.hash(state);
        // Sort keys for deterministic hashing of the options map.
        let mut pairs: Vec<_> = self.options.iter().collect();
        pairs.sort_by_key(|(k, _)| *k);
        for (k, v) in pairs {
            k.hash(state);
            v.hash(state);
        }
    }
}

impl ReplicationParams {
    pub fn new(strategy_class: impl Into<String>, options: HashMap<String, String>) -> Self {
        Self {
            strategy_class: strategy_class.into(),
            options,
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// ReplicaGroup
// ─────────────────────────────────────────────────────────────────────────────

/// A set of replica endpoints for a token range.
///
/// Supports transient replication: `full` replicas hold full copies of the
/// data, while `transient_replicas` only participate in writes for consistency
/// but may not retain the data permanently.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicaGroup {
    /// All endpoints (union of full + transient).
    pub endpoints: Vec<Endpoint>,
    /// Endpoints that hold full replicas.
    pub full: Vec<Endpoint>,
    /// Endpoints that hold transient replicas.
    pub transient_replicas: Vec<Endpoint>,
}

impl ReplicaGroup {
    /// Create a replica group where all endpoints are full replicas.
    pub fn new(endpoints: Vec<Endpoint>) -> Self {
        Self {
            full: endpoints.clone(),
            transient_replicas: Vec::new(),
            endpoints,
        }
    }

    /// Create a replica group with explicit full and transient replicas.
    pub fn with_transient(full: Vec<Endpoint>, transient: Vec<Endpoint>) -> Self {
        let mut endpoints = full.clone();
        endpoints.extend_from_slice(&transient);
        Self {
            endpoints,
            full,
            transient_replicas: transient,
        }
    }

    /// All endpoints in this replica group.
    pub fn all(&self) -> &[Endpoint] {
        &self.endpoints
    }

    /// Total number of replicas (full + transient).
    pub fn size(&self) -> usize {
        self.endpoints.len()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// DataPlacement
// ─────────────────────────────────────────────────────────────────────────────

/// Read and write replica placements for a single replication strategy.
///
/// During topology transitions, reads and writes may be directed to different
/// replica sets to maintain consistency guarantees.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataPlacement {
    /// Token range → replica group for reads.
    pub reads: BTreeMap<TokenRange, ReplicaGroup>,
    /// Token range → replica group for writes.
    pub writes: BTreeMap<TokenRange, ReplicaGroup>,
}

impl DataPlacement {
    pub fn new() -> Self {
        Self {
            reads: BTreeMap::new(),
            writes: BTreeMap::new(),
        }
    }

    /// Add a read placement for a token range.
    pub fn add_read(&mut self, range: TokenRange, group: ReplicaGroup) {
        self.reads.insert(range, group);
    }

    /// Add a write placement for a token range.
    pub fn add_write(&mut self, range: TokenRange, group: ReplicaGroup) {
        self.writes.insert(range, group);
    }

    /// Look up the read replica group for a token range.
    pub fn read_replicas(&self, range: &TokenRange) -> Option<&ReplicaGroup> {
        self.reads.get(range)
    }

    /// Look up the write replica group for a token range.
    pub fn write_replicas(&self, range: &TokenRange) -> Option<&ReplicaGroup> {
        self.writes.get(range)
    }
}

impl Default for DataPlacement {
    fn default() -> Self {
        Self::new()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// DataPlacements
// ─────────────────────────────────────────────────────────────────────────────

/// Aggregate data placements across all replication strategies in the cluster.
///
/// Maps each keyspace's `ReplicationParams` to its `DataPlacement`, allowing
/// the coordinator to look up read/write endpoints for any token range and
/// any keyspace.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataPlacements {
    placements: HashMap<ReplicationParams, DataPlacement>,
}

impl DataPlacements {
    pub fn new() -> Self {
        Self {
            placements: HashMap::new(),
        }
    }

    /// Get the data placement for a replication strategy.
    pub fn get(&self, params: &ReplicationParams) -> Option<&DataPlacement> {
        self.placements.get(params)
    }

    /// Set the data placement for a replication strategy.
    pub fn set(&mut self, params: ReplicationParams, placement: DataPlacement) {
        self.placements.insert(params, placement);
    }

    /// Iterate over all replication strategies that have placements.
    pub fn keys(&self) -> impl Iterator<Item = &ReplicationParams> {
        self.placements.keys()
    }
}

impl Default for DataPlacements {
    fn default() -> Self {
        Self::new()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// PlacementDelta
// ─────────────────────────────────────────────────────────────────────────────

/// Describes changes to a data placement during a topology transition.
///
/// Additions are new replica assignments, removals are replica assignments
/// that should be dropped once the transition completes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlacementDelta {
    /// New replica groups being added.
    pub additions: BTreeMap<TokenRange, ReplicaGroup>,
    /// Replica groups being removed.
    pub removals: BTreeMap<TokenRange, ReplicaGroup>,
}

impl PlacementDelta {
    pub fn new() -> Self {
        Self {
            additions: BTreeMap::new(),
            removals: BTreeMap::new(),
        }
    }

    /// Returns `true` if there are no additions or removals.
    pub fn is_empty(&self) -> bool {
        self.additions.is_empty() && self.removals.is_empty()
    }

    /// Record a new replica assignment being added.
    pub fn add_addition(&mut self, range: TokenRange, group: ReplicaGroup) {
        self.additions.insert(range, group);
    }

    /// Record a replica assignment being removed.
    pub fn add_removal(&mut self, range: TokenRange, group: ReplicaGroup) {
        self.removals.insert(range, group);
    }
}

impl Default for PlacementDelta {
    fn default() -> Self {
        Self::new()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// PlacementProvider
// ─────────────────────────────────────────────────────────────────────────────

/// Trait for computing replica placements from the token ring.
///
/// Implementations encapsulate the logic of a specific replication strategy
/// (e.g., `SimpleStrategy`, `NetworkTopologyStrategy`) and produce
/// `DataPlacement` results from the current ring state.
pub trait PlacementProvider {
    /// Compute the replica group for a specific token range.
    fn placement_for_range(
        &self,
        token_range: &TokenRange,
        params: &ReplicationParams,
    ) -> ReplicaGroup;

    /// Recalculate the full data placement for a replication strategy.
    fn recalculate(&self, params: &ReplicationParams) -> DataPlacement;
}

// ─────────────────────────────────────────────────────────────────────────────
// VersionedEndpoints
// ─────────────────────────────────────────────────────────────────────────────

/// A set of endpoints tagged with the epoch at which they were computed.
///
/// Used to detect stale placement information: if the current cluster epoch
/// has advanced beyond the versioned endpoints' epoch, the placements may
/// need recalculation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VersionedEndpoints {
    /// The epoch at which these endpoints were computed.
    pub epoch: Epoch,
    /// The endpoint list.
    pub endpoints: Vec<Endpoint>,
}

impl VersionedEndpoints {
    pub fn new(epoch: Epoch, endpoints: Vec<Endpoint>) -> Self {
        Self { epoch, endpoints }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// TokenMap
// ─────────────────────────────────────────────────────────────────────────────

/// Maps tokens to the nodes that own them on the ring.
///
/// The token map is the fundamental data structure for routing: given a
/// partition key's token, the map identifies which node is the primary
/// owner. Replica sets are then computed by walking the ring from that
/// token.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenMap {
    ring: BTreeMap<Token, NodeId>,
}

impl TokenMap {
    pub fn new() -> Self {
        Self {
            ring: BTreeMap::new(),
        }
    }

    /// Assign a token to a node.
    pub fn add(&mut self, token: Token, node_id: NodeId) {
        self.ring.insert(token, node_id);
    }

    /// Remove a token from the ring.
    pub fn remove(&mut self, token: &Token) -> Option<NodeId> {
        self.ring.remove(token)
    }

    /// Look up the owner of a specific token.
    pub fn owner(&self, token: &Token) -> Option<&NodeId> {
        self.ring.get(token)
    }

    /// Iterate over all (token, node_id) pairs in ring order.
    pub fn all_tokens(&self) -> impl Iterator<Item = (&Token, &NodeId)> {
        self.ring.iter()
    }

    /// Total number of tokens on the ring.
    pub fn token_count(&self) -> usize {
        self.ring.len()
    }
}

impl Default for TokenMap {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use uuid::Uuid;

    fn ep(port: u16) -> Endpoint {
        Endpoint::new(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            port,
        ))
    }

    fn node_id(n: u128) -> NodeId {
        NodeId::from_uuid(Uuid::from_u128(n))
    }

    fn simple_params(rf: u32) -> ReplicationParams {
        let mut opts = HashMap::new();
        opts.insert("replication_factor".to_string(), rf.to_string());
        ReplicationParams::new("SimpleStrategy", opts)
    }

    fn nts_params(dc1_rf: u32, dc2_rf: u32) -> ReplicationParams {
        let mut opts = HashMap::new();
        opts.insert("dc1".to_string(), dc1_rf.to_string());
        opts.insert("dc2".to_string(), dc2_rf.to_string());
        ReplicationParams::new("NetworkTopologyStrategy", opts)
    }

    // ── ReplicaGroup ─────────────────────────────────────────────────────

    #[test]
    fn replica_group_new_all_full() {
        let group = ReplicaGroup::new(vec![ep(7001), ep(7002), ep(7003)]);
        assert_eq!(group.size(), 3);
        assert_eq!(group.all().len(), 3);
        assert_eq!(group.full.len(), 3);
        assert!(group.transient_replicas.is_empty());
    }

    #[test]
    fn replica_group_with_transient() {
        let group = ReplicaGroup::with_transient(
            vec![ep(7001), ep(7002)],
            vec![ep(7003)],
        );
        assert_eq!(group.size(), 3);
        assert_eq!(group.full.len(), 2);
        assert_eq!(group.transient_replicas.len(), 1);
        assert!(group.all().contains(&ep(7003)));
    }

    // ── DataPlacement read/write lookups ─────────────────────────────────

    #[test]
    fn data_placement_read_write_lookups() {
        let mut dp = DataPlacement::new();

        let range_a = TokenRange::new(Token::from_raw(0), Token::from_raw(100));
        let range_b = TokenRange::new(Token::from_raw(100), Token::from_raw(200));

        dp.add_read(range_a, ReplicaGroup::new(vec![ep(7001), ep(7002)]));
        dp.add_write(range_a, ReplicaGroup::new(vec![ep(7001), ep(7002), ep(7003)]));

        dp.add_read(range_b, ReplicaGroup::new(vec![ep(7002), ep(7003)]));
        dp.add_write(range_b, ReplicaGroup::new(vec![ep(7002), ep(7003)]));

        // Read replicas for range_a
        let read_group = dp.read_replicas(&range_a).unwrap();
        assert_eq!(read_group.size(), 2);

        // Write replicas for range_a (includes pending replica)
        let write_group = dp.write_replicas(&range_a).unwrap();
        assert_eq!(write_group.size(), 3);

        // Read replicas for range_b
        let read_group = dp.read_replicas(&range_b).unwrap();
        assert_eq!(read_group.size(), 2);

        // Missing range returns None
        let missing = TokenRange::new(Token::from_raw(200), Token::from_raw(300));
        assert!(dp.read_replicas(&missing).is_none());
        assert!(dp.write_replicas(&missing).is_none());
    }

    // ── DataPlacements multi-strategy ────────────────────────────────────

    #[test]
    fn data_placements_multi_strategy() {
        let mut placements = DataPlacements::new();

        let simple = simple_params(3);
        let nts = nts_params(3, 2);

        let range = TokenRange::new(Token::from_raw(0), Token::from_raw(100));

        // Simple strategy placement
        let mut dp_simple = DataPlacement::new();
        dp_simple.add_read(range, ReplicaGroup::new(vec![ep(7001), ep(7002), ep(7003)]));
        placements.set(simple.clone(), dp_simple);

        // NTS placement
        let mut dp_nts = DataPlacement::new();
        dp_nts.add_read(range, ReplicaGroup::new(vec![ep(8001), ep(8002)]));
        placements.set(nts.clone(), dp_nts);

        // Verify lookups
        let simple_placement = placements.get(&simple).unwrap();
        assert_eq!(simple_placement.read_replicas(&range).unwrap().size(), 3);

        let nts_placement = placements.get(&nts).unwrap();
        assert_eq!(nts_placement.read_replicas(&range).unwrap().size(), 2);

        // Keys iterator
        let keys: Vec<_> = placements.keys().collect();
        assert_eq!(keys.len(), 2);
    }

    // ── PlacementDelta ───────────────────────────────────────────────────

    #[test]
    fn placement_delta_empty() {
        let delta = PlacementDelta::new();
        assert!(delta.is_empty());
    }

    #[test]
    fn placement_delta_additions_and_removals() {
        let mut delta = PlacementDelta::new();

        let range_a = TokenRange::new(Token::from_raw(0), Token::from_raw(100));
        let range_b = TokenRange::new(Token::from_raw(100), Token::from_raw(200));

        delta.add_addition(range_a, ReplicaGroup::new(vec![ep(7003)]));
        delta.add_removal(range_b, ReplicaGroup::new(vec![ep(7001)]));

        assert!(!delta.is_empty());
        assert_eq!(delta.additions.len(), 1);
        assert_eq!(delta.removals.len(), 1);

        assert!(delta.additions.contains_key(&range_a));
        assert!(delta.removals.contains_key(&range_b));
    }

    // ── VersionedEndpoints ───────────────────────────────────────────────

    #[test]
    fn versioned_endpoints_creation() {
        let ve = VersionedEndpoints::new(Epoch::FIRST, vec![ep(7001), ep(7002)]);
        assert_eq!(ve.epoch, Epoch::FIRST);
        assert_eq!(ve.endpoints.len(), 2);
    }

    // ── TokenMap ─────────────────────────────────────────────────────────

    #[test]
    fn token_map_add_and_lookup() {
        let mut tm = TokenMap::new();
        let n1 = node_id(1);
        let n2 = node_id(2);

        tm.add(Token::from_raw(0), n1);
        tm.add(Token::from_raw(100), n2);

        assert_eq!(tm.token_count(), 2);
        assert_eq!(*tm.owner(&Token::from_raw(0)).unwrap(), n1);
        assert_eq!(*tm.owner(&Token::from_raw(100)).unwrap(), n2);
        assert!(tm.owner(&Token::from_raw(50)).is_none());
    }

    #[test]
    fn token_map_remove() {
        let mut tm = TokenMap::new();
        let n1 = node_id(1);

        tm.add(Token::from_raw(0), n1);
        assert_eq!(tm.token_count(), 1);

        let removed = tm.remove(&Token::from_raw(0));
        assert_eq!(removed, Some(n1));
        assert_eq!(tm.token_count(), 0);
        assert!(tm.owner(&Token::from_raw(0)).is_none());
    }

    #[test]
    fn token_map_remove_nonexistent() {
        let mut tm = TokenMap::new();
        assert!(tm.remove(&Token::from_raw(42)).is_none());
    }

    #[test]
    fn token_map_all_tokens_ordering() {
        let mut tm = TokenMap::new();
        let n1 = node_id(1);
        let n2 = node_id(2);
        let n3 = node_id(3);

        tm.add(Token::from_raw(200), n3);
        tm.add(Token::from_raw(0), n1);
        tm.add(Token::from_raw(100), n2);

        let tokens: Vec<_> = tm.all_tokens().map(|(t, _)| t.value()).collect();
        assert_eq!(tokens, vec![0, 100, 200]);
    }

    #[test]
    fn token_map_overwrite_owner() {
        let mut tm = TokenMap::new();
        let n1 = node_id(1);
        let n2 = node_id(2);

        tm.add(Token::from_raw(0), n1);
        tm.add(Token::from_raw(0), n2);

        assert_eq!(tm.token_count(), 1);
        assert_eq!(*tm.owner(&Token::from_raw(0)).unwrap(), n2);
    }

    // ── ReplicationParams ────────────────────────────────────────────────

    #[test]
    fn replication_params_equality() {
        let a = simple_params(3);
        let b = simple_params(3);
        let c = simple_params(2);

        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn replication_params_hash_key() {
        let mut map: HashMap<ReplicationParams, u32> = HashMap::new();
        let params = simple_params(3);
        map.insert(params.clone(), 42);
        assert_eq!(*map.get(&params).unwrap(), 42);
    }
}
