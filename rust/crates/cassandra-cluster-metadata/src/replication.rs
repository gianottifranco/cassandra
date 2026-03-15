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
        let all_entries: Vec<(Token, Endpoint)> = ring
            .iter()
            .map(|(t, e)| (*t, *e))
            .collect();

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
                .filter(|(_, e)| {
                    !seen_endpoints.contains(e) && snitch.datacenter(e) == dc
                })
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

/// Create a replication strategy from schema replication params.
///
/// Matches Java's `AbstractReplicationStrategy.createReplicationStrategy()`.
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
    } else if strategy_class.contains("NetworkTopologyStrategy") {
        let dc_replication: BTreeMap<String, usize> = options
            .iter()
            .filter_map(|(k, v)| {
                // Skip "class" and other non-DC keys
                if k == "class" || k == "replication_factor" {
                    None
                } else {
                    v.parse().ok().map(|rf| (k.clone(), rf))
                }
            })
            .collect();
        Box::new(NetworkTopologyStrategy::new(dc_replication))
    } else {
        // LocalStrategy or unknown — treat as RF=1
        Box::new(SimpleStrategy::new(1))
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

        let replicas =
            strategy.calculate_natural_endpoints(Token::from_raw(-50), &ring, &snitch);
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

        let replicas =
            strategy.calculate_natural_endpoints(Token::from_raw(-50), &ring, &snitch);
        assert_eq!(replicas.len(), 3);
    }

    #[test]
    fn simple_strategy_rf_exceeds() {
        let strategy = SimpleStrategy::new(5);
        let snitch = SimpleSnitch;
        let mut ring = TokenRing::new();
        ring.add_token(Token::from_raw(0), ep(7001));
        ring.add_token(Token::from_raw(100), ep(7002));

        let replicas =
            strategy.calculate_natural_endpoints(Token::from_raw(-50), &ring, &snitch);
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

        let replicas =
            strategy.calculate_natural_endpoints(Token::from_raw(-50), &ring, &snitch);
        assert_eq!(replicas.len(), 3);

        // Verify DC distribution
        let dc1_count = replicas.iter().filter(|e| **e == ep(7001) || **e == ep(7002)).count();
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
}
