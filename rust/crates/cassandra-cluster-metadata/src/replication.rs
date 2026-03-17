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

//! Replication strategies: determine which nodes store replicas of a partition.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.locator.AbstractReplicationStrategy`
//! - `org.apache.cassandra.locator.SimpleStrategy`
//! - `org.apache.cassandra.locator.NetworkTopologyStrategy`

use std::collections::{BTreeMap, HashMap, HashSet};

use cassandra_common::Token;

use crate::node::Endpoint;
use crate::ring::TokenRing;
use crate::snitch::Snitch;

/// A replica holds a copy of data for a token range.
/// It can be a full replica or a transient replica (only receives writes).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Replica {
    pub endpoint: Endpoint,
    pub is_transient: bool,
}

impl Replica {
    pub fn full(endpoint: Endpoint) -> Self {
        Self {
            endpoint,
            is_transient: false,
        }
    }

    pub fn transient(endpoint: Endpoint) -> Self {
        Self {
            endpoint,
            is_transient: true,
        }
    }

    pub fn is_full(&self) -> bool {
        !self.is_transient
    }

    /// Promote a transient replica to a full replica.
    pub fn promote(&mut self) {
        self.is_transient = false;
    }
}

/// A replication strategy determines which endpoints store replicas.
pub trait ReplicationStrategy: Send + Sync {
    /// Calculate the set of endpoints that should hold replicas for the given token.
    ///
    /// The returned list is ordered: first element is the primary replica.
    fn calculate_natural_endpoints(
        &self,
        token: Token,
        ring: &TokenRing,
        snitch: &dyn Snitch,
    ) -> Vec<Endpoint>;

    /// Calculate the set of replicas for the given token.
    /// Distinguishes between full and transient replicas.
    fn calculate_natural_replicas(
        &self,
        token: Token,
        ring: &TokenRing,
        snitch: &dyn Snitch,
    ) -> Vec<Replica> {
        self.calculate_natural_endpoints(token, ring, snitch)
            .into_iter()
            .map(Replica::full)
            .collect()
    }

    /// The replication factor (total number of replicas).
    fn replication_factor(&self) -> usize;

    /// Human-readable strategy name.
    fn name(&self) -> &str;
}

/// SimpleStrategy: places `rf` replicas on consecutive unique nodes walking
/// clockwise from the token's position on the ring.
///
/// Ignores datacenter and rack topology. Suitable for single-DC clusters.
#[derive(Debug, Clone)]
pub struct SimpleStrategy {
    pub replication_factor: usize,
}

impl SimpleStrategy {
    pub fn new(replication_factor: usize) -> Self {
        Self { replication_factor }
    }
}

impl ReplicationStrategy for SimpleStrategy {
    fn calculate_natural_endpoints(
        &self,
        token: Token,
        ring: &TokenRing,
        _snitch: &dyn Snitch,
    ) -> Vec<Endpoint> {
        ring.natural_endpoints(token, self.replication_factor)
    }

    fn replication_factor(&self) -> usize {
        self.replication_factor
    }

    fn name(&self) -> &str {
        "SimpleStrategy"
    }
}

/// NetworkTopologyStrategy: places replicas across datacenters and racks.
///
/// For each DC with RF=N, places N replicas on distinct racks when possible.
/// Walks the ring clockwise from the token position.
///
/// ## Java Oracle
///
/// `org.apache.cassandra.locator.NetworkTopologyStrategy.calculateNaturalEndpoints()`
#[derive(Debug, Clone)]
pub struct NetworkTopologyStrategy {
    /// Per-datacenter replication factors.
    pub dc_replication: BTreeMap<String, usize>,
}

impl NetworkTopologyStrategy {
    pub fn new(dc_replication: BTreeMap<String, usize>) -> Self {
        Self { dc_replication }
    }

    /// Total replication factor across all DCs.
    fn total_rf(&self) -> usize {
        self.dc_replication.values().sum()
    }
}

impl ReplicationStrategy for NetworkTopologyStrategy {
    fn calculate_natural_endpoints(
        &self,
        token: Token,
        ring: &TokenRing,
        snitch: &dyn Snitch,
    ) -> Vec<Endpoint> {
        let total_rf = self.total_rf();
        if ring.is_empty() || total_rf == 0 {
            return Vec::new();
        }

        // Track per-DC state
        let mut dc_replicas: HashMap<String, Vec<Endpoint>> = HashMap::new();
        let mut dc_racks_used: HashMap<String, HashSet<String>> = HashMap::new();
        let mut seen_endpoints: HashSet<Endpoint> = HashSet::new();
        let mut result = Vec::with_capacity(total_rf);

        // Collect all ring entries in clockwise order from token
        let all_entries: Vec<(Token, Endpoint)> = ring.iter().map(|(t, e)| (*t, *e)).collect();

        if all_entries.is_empty() {
            return Vec::new();
        }

        // Find start position (first token >= query token)
        let start_idx = all_entries
            .binary_search_by_key(&token, |(t, _)| *t)
            .unwrap_or_else(|i| i);

        // Walk the ring: start_idx..end, then 0..start_idx
        let n = all_entries.len();
        for offset in 0..n {
            let idx = (start_idx + offset) % n;
            let (_t, ep) = all_entries[idx];

            if seen_endpoints.contains(&ep) {
                continue;
            }

            let dc = snitch.datacenter(&ep);
            let rack = snitch.rack(&ep);

            let dc_rf = match self.dc_replication.get(&dc) {
                Some(&rf) => rf,
                None => continue, // DC not in strategy
            };

            let dc_reps = dc_replicas.entry(dc.clone()).or_default();

            if dc_reps.len() >= dc_rf {
                continue; // This DC is already fully replicated
            }

            // Prefer different racks
            let racks_used = dc_racks_used.entry(dc.clone()).or_default();
            let dc_nodes_in_ring: usize = all_entries
                .iter()
                .filter(|(_, e)| !seen_endpoints.contains(e) && snitch.datacenter(e) == dc)
                .map(|(_, e)| *e)
                .collect::<HashSet<_>>()
                .len();

            // Accept if: new rack, or we've used all available racks
            if racks_used.contains(&rack) && dc_reps.len() + dc_nodes_in_ring > dc_rf {
                // There are still other racks available, skip for now
                // But we need to be careful: this endpoint will be revisited
                continue;
            }

            racks_used.insert(rack);
            dc_reps.push(ep);
            seen_endpoints.insert(ep);
            result.push(ep);

            if result.len() >= total_rf {
                break;
            }
        }

        // Second pass: if any DC didn't fill its RF (ran out of racks),
        // accept same-rack nodes
        if result.len() < total_rf {
            let start_idx2 = start_idx;
            for offset in 0..n {
                let idx = (start_idx2 + offset) % n;
                let (_t, ep) = all_entries[idx];

                if seen_endpoints.contains(&ep) {
                    continue;
                }

                let dc = snitch.datacenter(&ep);
                let dc_rf = match self.dc_replication.get(&dc) {
                    Some(&rf) => rf,
                    None => continue,
                };

                let dc_reps = dc_replicas.entry(dc).or_default();
                if dc_reps.len() >= dc_rf {
                    continue;
                }

                dc_reps.push(ep);
                seen_endpoints.insert(ep);
                result.push(ep);

                if result.len() >= total_rf {
                    break;
                }
            }
        }

        result
    }

    fn replication_factor(&self) -> usize {
        self.total_rf()
    }

    fn name(&self) -> &str {
        "NetworkTopologyStrategy"
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// OldNetworkTopologyStrategy
// ─────────────────────────────────────────────────────────────────────────────

/// OldNetworkTopologyStrategy: simple ring-walk placement without rack awareness.
///
/// This is the predecessor to `NetworkTopologyStrategy`. It walks the ring
/// clockwise and places replicas on distinct nodes, but does not consider
/// rack diversity. Maintained for backward compatibility only.
///
/// ## Java Oracle
///
/// `org.apache.cassandra.locator.OldNetworkTopologyStrategy`
#[derive(Debug, Clone)]
pub struct OldNetworkTopologyStrategy {
    pub replication_factor: usize,
}

impl OldNetworkTopologyStrategy {
    pub fn new(replication_factor: usize) -> Self {
        Self { replication_factor }
    }
}

impl ReplicationStrategy for OldNetworkTopologyStrategy {
    fn calculate_natural_endpoints(
        &self,
        token: Token,
        ring: &TokenRing,
        _snitch: &dyn Snitch,
    ) -> Vec<Endpoint> {
        // Simple ring walk: same as SimpleStrategy (no rack awareness)
        ring.natural_endpoints(token, self.replication_factor)
    }

    fn replication_factor(&self) -> usize {
        self.replication_factor
    }

    fn name(&self) -> &str {
        "OldNetworkTopologyStrategy"
    }
}

/// Create a replication strategy from schema replication params.
///
/// Matches Java's `AbstractReplicationStrategy.createReplicationStrategy()`.
/// Supports transient replication format "3/1" (3 total, 1 transient).
pub fn create_strategy(
    strategy_class: &str,
    options: &BTreeMap<String, String>,
) -> Box<dyn ReplicationStrategy> {
    if strategy_class.contains("SimpleStrategy") {
        let rf: usize = options
            .get("replication_factor")
            .and_then(|v| v.parse().ok())
            .unwrap_or(1);
        Box::new(SimpleStrategy::new(rf))
    } else if strategy_class.contains("OldNetworkTopologyStrategy") {
        let rf: usize = options
            .get("replication_factor")
            .and_then(|v| v.parse().ok())
            .unwrap_or(1);
        Box::new(OldNetworkTopologyStrategy::new(rf))
    } else if strategy_class.contains("NetworkTopologyStrategy") {
        // Check if any DC value has transient format "3/1"
        let has_transient = options
            .iter()
            .any(|(k, v)| k != "class" && k != "replication_factor" && v.contains('/'));

        if has_transient {
            let mut dc_replication = BTreeMap::new();
            let mut dc_transient = BTreeMap::new();
            for (k, v) in options {
                if k == "class" || k == "replication_factor" {
                    continue;
                }
                if let Some((total_str, trans_str)) = v.split_once('/') {
                    if let (Ok(total), Ok(trans)) =
                        (total_str.trim().parse::<usize>(), trans_str.trim().parse::<usize>())
                    {
                        dc_replication.insert(k.clone(), total);
                        if trans > 0 {
                            dc_transient.insert(k.clone(), trans);
                        }
                    }
                } else if let Ok(rf) = v.parse::<usize>() {
                    dc_replication.insert(k.clone(), rf);
                }
            }
            Box::new(TransientReplicationStrategy::new(dc_replication, dc_transient))
        } else {
            let dc_replication: BTreeMap<String, usize> = options
                .iter()
                .filter_map(|(k, v)| {
                    if k == "class" || k == "replication_factor" {
                        None
                    } else {
                        v.parse().ok().map(|rf| (k.clone(), rf))
                    }
                })
                .collect();
            Box::new(NetworkTopologyStrategy::new(dc_replication))
        }
    } else if strategy_class.contains("LocalStrategy") {
        Box::new(LocalStrategy)
    } else if strategy_class.contains("EverywhereStrategy") {
        Box::new(EverywhereStrategy)
    } else {
        // Unknown — treat as RF=1
        Box::new(SimpleStrategy::new(1))
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// LocalStrategy
// ─────────────────────────────────────────────────────────────────────────────

/// LocalStrategy: places a single replica on the local node only.
///
/// Used by system keyspaces (`system`, `system_schema`) that must exist
/// on every node but are not replicated across the cluster.
///
/// ## Java Oracle
///
/// `org.apache.cassandra.locator.LocalStrategy`
#[derive(Debug, Clone)]
pub struct LocalStrategy;

impl ReplicationStrategy for LocalStrategy {
    fn calculate_natural_endpoints(
        &self,
        token: Token,
        ring: &TokenRing,
        _snitch: &dyn Snitch,
    ) -> Vec<Endpoint> {
        // Return only the primary (local) endpoint for this token
        ring.primary_endpoint(token).into_iter().collect()
    }

    fn replication_factor(&self) -> usize {
        1
    }

    fn name(&self) -> &str {
        "LocalStrategy"
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// EverywhereStrategy
// ─────────────────────────────────────────────────────────────────────────────

/// EverywhereStrategy: places a replica on every node in the cluster.
///
/// Used by `system_traces` and similar keyspaces where data should
/// exist on all nodes.
///
/// ## Java Oracle
///
/// `org.apache.cassandra.locator.EverywhereStrategy`
#[derive(Debug, Clone)]
pub struct EverywhereStrategy;

impl ReplicationStrategy for EverywhereStrategy {
    fn calculate_natural_endpoints(
        &self,
        _token: Token,
        ring: &TokenRing,
        _snitch: &dyn Snitch,
    ) -> Vec<Endpoint> {
        ring.all_endpoints()
    }

    fn replication_factor(&self) -> usize {
        // RF = total number of nodes; the caller should use ring.endpoint_count()
        // for the actual number. This returns a sentinel.
        usize::MAX
    }

    fn name(&self) -> &str {
        "EverywhereStrategy"
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// TransientReplicationStrategy (stub)
// ─────────────────────────────────────────────────────────────────────────────

/// TransientReplicationStrategy: experimental support for transient replicas.
///
/// **Feature-flagged / stub** — transient replication is an experimental
/// feature in the Java baseline. This struct tracks the configuration
/// but currently delegates to NTS for placement.
///
/// GAP(gap_guard_gossip_wire_compat): Full implementation of transient replica — tracked in gap_guards.rs
/// placement, read routing (only full replicas serve reads), and
/// anti-compaction awareness.
///
/// ## Java Oracle
///
/// `org.apache.cassandra.locator.NetworkTopologyStrategy` with
/// `replication_factor` format `'3/1'` (3 total, 1 transient).
#[derive(Debug, Clone)]
pub struct TransientReplicationStrategy {
    /// Underlying NTS for placement.
    inner: NetworkTopologyStrategy,
    /// Per-DC transient counts (DC → number of transient replicas).
    pub dc_transient: BTreeMap<String, usize>,
}

impl TransientReplicationStrategy {
    pub fn new(
        dc_replication: BTreeMap<String, usize>,
        dc_transient: BTreeMap<String, usize>,
    ) -> Self {
        Self {
            inner: NetworkTopologyStrategy::new(dc_replication),
            dc_transient,
        }
    }

    /// Full (non-transient) replication factor for a DC.
    pub fn full_rf_for_dc(&self, dc: &str) -> usize {
        let total = self.inner.dc_replication.get(dc).copied().unwrap_or(0);
        let trans = self.dc_transient.get(dc).copied().unwrap_or(0);
        total.saturating_sub(trans)
    }

    /// Transient replication factor for a DC.
    pub fn transient_rf_for_dc(&self, dc: &str) -> usize {
        self.dc_transient.get(dc).copied().unwrap_or(0)
    }

    /// Calculate read endpoints: returns only full replicas (transient
    /// replicas do not serve reads).
    pub fn calculate_read_endpoints(
        &self,
        token: Token,
        ring: &TokenRing,
        snitch: &dyn Snitch,
    ) -> Vec<Endpoint> {
        self.calculate_natural_replicas(token, ring, snitch)
            .into_iter()
            .filter(|r| r.is_full())
            .map(|r| r.endpoint)
            .collect()
    }
}

impl ReplicationStrategy for TransientReplicationStrategy {
    fn calculate_natural_endpoints(
        &self,
        token: Token,
        ring: &TokenRing,
        snitch: &dyn Snitch,
    ) -> Vec<Endpoint> {
        self.inner.calculate_natural_endpoints(token, ring, snitch)
    }

    fn calculate_natural_replicas(
        &self,
        token: Token,
        ring: &TokenRing,
        snitch: &dyn Snitch,
    ) -> Vec<Replica> {
        let endpoints = self.inner.calculate_natural_endpoints(token, ring, snitch);

        // Count total replicas per DC
        let mut dc_totals: HashMap<String, usize> = HashMap::new();
        for ep in &endpoints {
            *dc_totals.entry(snitch.datacenter(ep)).or_insert(0) += 1;
        }

        // First pass: assign full/transient status
        let mut dc_seen: HashMap<String, usize> = HashMap::new();
        let mut replicas = Vec::with_capacity(endpoints.len());

        for ep in &endpoints {
            let dc = snitch.datacenter(ep);
            let seen = dc_seen.entry(dc.clone()).or_insert(0);
            *seen += 1;

            let total = *dc_totals.get(&dc).unwrap();
            let transient_count = self.dc_transient.get(&dc).copied().unwrap_or(0);

            // The last `transient_count` replicas for a DC are transient.
            let remaining = total - *seen + 1;
            if remaining <= transient_count {
                replicas.push(Replica::transient(*ep));
            } else {
                replicas.push(Replica::full(*ep));
            }
        }

        // Ensure full replicas come before transient replicas (stable sort)
        replicas.sort_by_key(|r| r.is_transient as u8);

        replicas
    }

    fn replication_factor(&self) -> usize {
        self.inner.replication_factor()
    }

    fn name(&self) -> &str {
        "TransientReplicationStrategy"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::snitch::{PropertyFileSnitch, SimpleSnitch};
    use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

    fn ep(port: u16) -> Endpoint {
        Endpoint::new(SocketAddr::V4(SocketAddrV4::new(
            Ipv4Addr::new(127, 0, 0, 1),
            port,
        )))
    }

    #[test]
    fn simple_strategy_rf1() {
        let strategy = SimpleStrategy::new(1);
        let snitch = SimpleSnitch;
        let mut ring = TokenRing::new();
        ring.add_token(Token::from_raw(-100), ep(7001));
        ring.add_token(Token::from_raw(0), ep(7002));
        ring.add_token(Token::from_raw(100), ep(7003));

        let replicas = strategy.calculate_natural_endpoints(Token::from_raw(-50), &ring, &snitch);
        assert_eq!(replicas.len(), 1);
        assert_eq!(replicas[0], ep(7002));
    }

    #[test]
    fn simple_strategy_rf3() {
        let strategy = SimpleStrategy::new(3);
        let snitch = SimpleSnitch;
        let mut ring = TokenRing::new();
        ring.add_token(Token::from_raw(-100), ep(7001));
        ring.add_token(Token::from_raw(0), ep(7002));
        ring.add_token(Token::from_raw(100), ep(7003));

        let replicas = strategy.calculate_natural_endpoints(Token::from_raw(-50), &ring, &snitch);
        assert_eq!(replicas.len(), 3);
    }

    #[test]
    fn simple_strategy_rf_exceeds() {
        let strategy = SimpleStrategy::new(5);
        let snitch = SimpleSnitch;
        let mut ring = TokenRing::new();
        ring.add_token(Token::from_raw(0), ep(7001));
        ring.add_token(Token::from_raw(100), ep(7002));

        let replicas = strategy.calculate_natural_endpoints(Token::from_raw(-50), &ring, &snitch);
        assert_eq!(replicas.len(), 2); // can't exceed node count
    }

    #[test]
    fn nts_basic() {
        let mut dc_rf = BTreeMap::new();
        dc_rf.insert("dc1".to_string(), 2);
        dc_rf.insert("dc2".to_string(), 1);
        let strategy = NetworkTopologyStrategy::new(dc_rf);

        assert_eq!(strategy.replication_factor(), 3);

        let mut topology = HashMap::new();
        topology.insert(ep(7001), ("dc1".to_string(), "rack1".to_string()));
        topology.insert(ep(7002), ("dc1".to_string(), "rack2".to_string()));
        topology.insert(ep(7003), ("dc2".to_string(), "rack1".to_string()));
        let snitch = PropertyFileSnitch::new(topology, "dc1", "rack1");

        let mut ring = TokenRing::new();
        ring.add_token(Token::from_raw(-100), ep(7001));
        ring.add_token(Token::from_raw(0), ep(7002));
        ring.add_token(Token::from_raw(100), ep(7003));

        let replicas = strategy.calculate_natural_endpoints(Token::from_raw(-50), &ring, &snitch);
        assert_eq!(replicas.len(), 3);

        // Verify DC distribution
        let dc1_count = replicas
            .iter()
            .filter(|e| **e == ep(7001) || **e == ep(7002))
            .count();
        let dc2_count = replicas.iter().filter(|e| **e == ep(7003)).count();
        assert_eq!(dc1_count, 2);
        assert_eq!(dc2_count, 1);
    }

    #[test]
    fn create_strategy_simple() {
        let mut options = BTreeMap::new();
        options.insert("replication_factor".to_string(), "3".to_string());
        let s = create_strategy("org.apache.cassandra.locator.SimpleStrategy", &options);
        assert_eq!(s.replication_factor(), 3);
        assert_eq!(s.name(), "SimpleStrategy");
    }

    #[test]
    fn create_strategy_nts() {
        let mut options = BTreeMap::new();
        options.insert("dc1".to_string(), "3".to_string());
        options.insert("dc2".to_string(), "2".to_string());
        let s = create_strategy(
            "org.apache.cassandra.locator.NetworkTopologyStrategy",
            &options,
        );
        assert_eq!(s.replication_factor(), 5);
        assert_eq!(s.name(), "NetworkTopologyStrategy");
    }

    #[test]
    fn local_strategy_single_replica() {
        let strategy = LocalStrategy;
        let snitch = SimpleSnitch;
        let mut ring = TokenRing::new();
        ring.add_token(Token::from_raw(-100), ep(7001));
        ring.add_token(Token::from_raw(0), ep(7002));
        ring.add_token(Token::from_raw(100), ep(7003));

        let replicas = strategy.calculate_natural_endpoints(Token::from_raw(-50), &ring, &snitch);
        assert_eq!(replicas.len(), 1);
        assert_eq!(replicas[0], ep(7002)); // primary for token -50
    }

    #[test]
    fn everywhere_strategy_all_nodes() {
        let strategy = EverywhereStrategy;
        let snitch = SimpleSnitch;
        let mut ring = TokenRing::new();
        ring.add_token(Token::from_raw(-100), ep(7001));
        ring.add_token(Token::from_raw(0), ep(7002));
        ring.add_token(Token::from_raw(100), ep(7003));

        let replicas = strategy.calculate_natural_endpoints(Token::from_raw(-50), &ring, &snitch);
        assert_eq!(replicas.len(), 3); // all nodes
    }

    #[test]
    fn create_strategy_local() {
        let s = create_strategy(
            "org.apache.cassandra.locator.LocalStrategy",
            &BTreeMap::new(),
        );
        assert_eq!(s.name(), "LocalStrategy");
        assert_eq!(s.replication_factor(), 1);
    }

    #[test]
    fn create_strategy_everywhere() {
        let s = create_strategy(
            "org.apache.cassandra.locator.EverywhereStrategy",
            &BTreeMap::new(),
        );
        assert_eq!(s.name(), "EverywhereStrategy");
    }

    #[test]
    fn transient_replication_basic() {
        let mut dc_rf = BTreeMap::new();
        dc_rf.insert("dc1".to_string(), 3);
        let mut dc_trans = BTreeMap::new();
        dc_trans.insert("dc1".to_string(), 1);

        let strategy = TransientReplicationStrategy::new(dc_rf, dc_trans);
        assert_eq!(strategy.name(), "TransientReplicationStrategy");
        assert_eq!(strategy.replication_factor(), 3);
        assert_eq!(strategy.full_rf_for_dc("dc1"), 2);
        assert_eq!(strategy.transient_rf_for_dc("dc1"), 1);
    }

    #[test]
    fn transient_replication_full_before_transient() {
        let mut dc_rf = BTreeMap::new();
        dc_rf.insert("datacenter1".to_string(), 3);
        let mut dc_trans = BTreeMap::new();
        dc_trans.insert("datacenter1".to_string(), 1);

        let strategy = TransientReplicationStrategy::new(dc_rf, dc_trans);
        let snitch = SimpleSnitch;
        let mut ring = TokenRing::new();
        ring.add_token(Token::from_raw(-100), ep(7001));
        ring.add_token(Token::from_raw(0), ep(7002));
        ring.add_token(Token::from_raw(100), ep(7003));

        let replicas =
            strategy.calculate_natural_replicas(Token::from_raw(-50), &ring, &snitch);
        assert_eq!(replicas.len(), 3);

        // Full replicas should come before transient
        let full_count = replicas.iter().filter(|r| r.is_full()).count();
        let transient_count = replicas.iter().filter(|r| r.is_transient).count();
        assert_eq!(full_count, 2);
        assert_eq!(transient_count, 1);

        // Full replicas first, then transient
        assert!(replicas[0].is_full());
        assert!(replicas[1].is_full());
        assert!(replicas[2].is_transient);
    }

    #[test]
    fn transient_replication_read_endpoints() {
        let mut dc_rf = BTreeMap::new();
        dc_rf.insert("datacenter1".to_string(), 3);
        let mut dc_trans = BTreeMap::new();
        dc_trans.insert("datacenter1".to_string(), 1);

        let strategy = TransientReplicationStrategy::new(dc_rf, dc_trans);
        let snitch = SimpleSnitch;
        let mut ring = TokenRing::new();
        ring.add_token(Token::from_raw(-100), ep(7001));
        ring.add_token(Token::from_raw(0), ep(7002));
        ring.add_token(Token::from_raw(100), ep(7003));

        let read_eps =
            strategy.calculate_read_endpoints(Token::from_raw(-50), &ring, &snitch);
        // Only full replicas serve reads
        assert_eq!(read_eps.len(), 2);
    }

    #[test]
    fn old_nts_basic() {
        let strategy = OldNetworkTopologyStrategy::new(2);
        assert_eq!(strategy.name(), "OldNetworkTopologyStrategy");
        assert_eq!(strategy.replication_factor(), 2);

        let snitch = SimpleSnitch;
        let mut ring = TokenRing::new();
        ring.add_token(Token::from_raw(-100), ep(7001));
        ring.add_token(Token::from_raw(0), ep(7002));
        ring.add_token(Token::from_raw(100), ep(7003));

        let replicas =
            strategy.calculate_natural_endpoints(Token::from_raw(-50), &ring, &snitch);
        assert_eq!(replicas.len(), 2);
    }

    #[test]
    fn create_strategy_old_nts() {
        let mut options = BTreeMap::new();
        options.insert("replication_factor".to_string(), "2".to_string());
        let s = create_strategy(
            "org.apache.cassandra.locator.OldNetworkTopologyStrategy",
            &options,
        );
        assert_eq!(s.name(), "OldNetworkTopologyStrategy");
        assert_eq!(s.replication_factor(), 2);
    }

    #[test]
    fn create_strategy_nts_transient() {
        let mut options = BTreeMap::new();
        options.insert("dc1".to_string(), "3/1".to_string());
        options.insert("dc2".to_string(), "2".to_string());
        let s = create_strategy(
            "org.apache.cassandra.locator.NetworkTopologyStrategy",
            &options,
        );
        assert_eq!(s.name(), "TransientReplicationStrategy");
        assert_eq!(s.replication_factor(), 5);
    }

    #[test]
    fn replica_promote() {
        let mut r = Replica::transient(ep(7001));
        assert!(r.is_transient);
        r.promote();
        assert!(r.is_full());
    }
}
