// Licensed under Apache License, Version 2.0.

//! Asymmetric repair difference tracking and sync planning.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.repair.asymmetric.DifferenceHolder`
//! - `org.apache.cassandra.repair.asymmetric.RangeMap`

use std::collections::{HashMap, HashSet};

use cassandra_cluster_metadata::Endpoint;
use cassandra_common::token::TokenRange;
use serde::{Deserialize, Serialize};

use crate::job::{DiffResult, SyncTaskDescriptor};

/// Unordered pair of replicas that disagree for one or more token ranges.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EndpointPair {
    pub left: Endpoint,
    pub right: Endpoint,
}

impl EndpointPair {
    pub fn new(a: Endpoint, b: Endpoint) -> Self {
        if endpoint_key(a) <= endpoint_key(b) {
            Self { left: a, right: b }
        } else {
            Self { left: b, right: a }
        }
    }

    pub fn contains(&self, endpoint: Endpoint) -> bool {
        self.left == endpoint || self.right == endpoint
    }
}

/// Range-keyed view of replica differences.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RangeMap {
    entries: Vec<RangeDifference>,
}

impl RangeMap {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_difference(&mut self, range: TokenRange, a: Endpoint, b: Endpoint) {
        if let Some(entry) = self.entries.iter_mut().find(|entry| entry.range == range) {
            entry.endpoints.insert(a);
            entry.endpoints.insert(b);
            entry.pairs.insert(EndpointPair::new(a, b));
            return;
        }

        let mut endpoints = HashSet::new();
        endpoints.insert(a);
        endpoints.insert(b);
        let mut pairs = HashSet::new();
        pairs.insert(EndpointPair::new(a, b));
        self.entries.push(RangeDifference {
            range,
            endpoints,
            pairs,
        });
    }

    pub fn differences(&self) -> &[RangeDifference] {
        &self.entries
    }

    pub fn endpoints_for_range(&self, range: TokenRange) -> Option<Vec<Endpoint>> {
        self.entries
            .iter()
            .find(|entry| entry.range == range)
            .map(|entry| sorted_endpoints(entry.endpoints.iter().copied()))
    }

    pub fn ranges_for_endpoint(&self, endpoint: Endpoint) -> Vec<TokenRange> {
        self.entries
            .iter()
            .filter(|entry| entry.endpoints.contains(&endpoint))
            .map(|entry| entry.range)
            .collect()
    }
}

/// Difference metadata for a single token range.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RangeDifference {
    pub range: TokenRange,
    pub endpoints: HashSet<Endpoint>,
    pub pairs: HashSet<EndpointPair>,
}

/// Stores pairwise Merkle differences and derives optimized sync work.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DifferenceHolder {
    pair_ranges: Vec<PairDifference>,
    range_map: RangeMap,
}

/// Mismatched ranges for an unordered endpoint pair.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PairDifference {
    pub pair: EndpointPair,
    pub ranges: Vec<TokenRange>,
}

impl DifferenceHolder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_diffs(diffs: &[DiffResult]) -> Self {
        let mut holder = Self::new();
        for diff in diffs {
            holder.add_diff(
                diff.endpoint_a,
                diff.endpoint_b,
                diff.mismatched_ranges
                    .iter()
                    .map(|(start, end)| TokenRange::new(*start, *end)),
            );
        }
        holder
    }

    pub fn add_diff<I>(&mut self, a: Endpoint, b: Endpoint, ranges: I)
    where
        I: IntoIterator<Item = TokenRange>,
    {
        let pair = EndpointPair::new(a, b);
        let pair_index =
            if let Some(index) = self.pair_ranges.iter().position(|entry| entry.pair == pair) {
                index
            } else {
                self.pair_ranges.push(PairDifference {
                    pair,
                    ranges: Vec::new(),
                });
                self.pair_ranges.len() - 1
            };
        let pair_ranges = &mut self.pair_ranges[pair_index].ranges;
        for range in ranges {
            if !pair_ranges.contains(&range) {
                pair_ranges.push(range);
            }
            self.range_map.add_difference(range, a, b);
        }
    }

    pub fn is_empty(&self) -> bool {
        self.pair_ranges.is_empty()
    }

    pub fn pair_ranges(&self, pair: EndpointPair) -> Option<&[TokenRange]> {
        self.pair_ranges
            .iter()
            .find(|entry| entry.pair == pair)
            .map(|entry| entry.ranges.as_slice())
    }

    pub fn range_map(&self) -> &RangeMap {
        &self.range_map
    }

    /// Build an asymmetric sync plan.
    ///
    /// For each mismatched range, the source is the replica with the fewest
    /// pairwise disagreements for that range. Replicas with more disagreements
    /// become targets. If all replicas are tied, the plan falls back to
    /// bidirectional sync among the disagreeing pair set because no majority
    /// source can be inferred from Merkle diffs alone.
    pub fn create_asymmetric_sync_plan(
        &self,
        replicas: &[Endpoint],
        keyspace: &str,
        table: &str,
    ) -> Vec<SyncTaskDescriptor> {
        let mut tasks = Vec::new();
        for difference in self.range_map.differences() {
            let counts = self.disagreement_counts(difference);
            let candidates = sorted_endpoints(replicas.iter().copied());
            let Some(min_count) = candidates
                .iter()
                .map(|endpoint| *counts.get(endpoint).unwrap_or(&0))
                .min()
            else {
                continue;
            };

            let source = candidates
                .iter()
                .copied()
                .find(|endpoint| *counts.get(endpoint).unwrap_or(&0) == min_count)
                .unwrap();
            let mut targets: Vec<_> = candidates
                .iter()
                .copied()
                .filter(|endpoint| *counts.get(endpoint).unwrap_or(&0) > min_count)
                .collect();

            if targets.is_empty() {
                for pair in sorted_pairs(difference.pairs.iter().copied()) {
                    tasks.push(sync_task(
                        pair.left,
                        pair.right,
                        difference.range,
                        keyspace,
                        table,
                    ));
                    tasks.push(sync_task(
                        pair.right,
                        pair.left,
                        difference.range,
                        keyspace,
                        table,
                    ));
                }
                continue;
            }

            targets.sort_by_key(|endpoint| endpoint_key(*endpoint));
            for target in targets {
                tasks.push(sync_task(source, target, difference.range, keyspace, table));
            }
        }
        tasks
    }

    fn disagreement_counts(&self, difference: &RangeDifference) -> HashMap<Endpoint, usize> {
        let mut counts = HashMap::new();
        for pair in &difference.pairs {
            *counts.entry(pair.left).or_insert(0) += 1;
            *counts.entry(pair.right).or_insert(0) += 1;
        }
        counts
    }
}

fn sync_task(
    source: Endpoint,
    target: Endpoint,
    range: TokenRange,
    keyspace: &str,
    table: &str,
) -> SyncTaskDescriptor {
    SyncTaskDescriptor {
        source,
        target,
        ranges: vec![(range.start, range.end)],
        keyspace: keyspace.to_string(),
        table: table.to_string(),
    }
}

fn sorted_endpoints<I>(endpoints: I) -> Vec<Endpoint>
where
    I: IntoIterator<Item = Endpoint>,
{
    let mut endpoints: Vec<_> = endpoints.into_iter().collect();
    endpoints.sort_by_key(|endpoint| endpoint_key(*endpoint));
    endpoints.dedup();
    endpoints
}

fn sorted_pairs<I>(pairs: I) -> Vec<EndpointPair>
where
    I: IntoIterator<Item = EndpointPair>,
{
    let mut pairs: Vec<_> = pairs.into_iter().collect();
    pairs.sort_by_key(|pair| (endpoint_key(pair.left), endpoint_key(pair.right)));
    pairs
}

fn endpoint_key(endpoint: Endpoint) -> String {
    endpoint.addr().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use cassandra_common::Token;

    fn tok(value: i64) -> Token {
        Token::from_raw(value)
    }

    fn range(start: i64, end: i64) -> TokenRange {
        TokenRange::new(tok(start), tok(end))
    }

    fn ep(port: u16) -> Endpoint {
        use std::net::{IpAddr, Ipv4Addr, SocketAddr};
        Endpoint::new(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            port,
        ))
    }

    #[test]
    fn endpoint_pair_is_order_independent() {
        assert_eq!(
            EndpointPair::new(ep(7002), ep(7001)),
            EndpointPair::new(ep(7001), ep(7002))
        );
    }

    #[test]
    fn range_map_groups_endpoints_and_ranges() {
        let mut ranges = RangeMap::new();
        ranges.add_difference(range(0, 100), ep(7001), ep(7002));
        ranges.add_difference(range(0, 100), ep(7002), ep(7003));
        ranges.add_difference(range(100, 200), ep(7001), ep(7003));

        assert_eq!(
            ranges.endpoints_for_range(range(0, 100)).unwrap(),
            vec![ep(7001), ep(7002), ep(7003)]
        );
        assert_eq!(
            ranges.ranges_for_endpoint(ep(7001)),
            vec![range(0, 100), range(100, 200)]
        );
    }

    #[test]
    fn holder_builds_from_pairwise_diffs() {
        let diffs = vec![DiffResult {
            endpoint_a: ep(7001),
            endpoint_b: ep(7002),
            mismatched_ranges: vec![(tok(0), tok(100))],
        }];
        let holder = DifferenceHolder::from_diffs(&diffs);
        let pair = EndpointPair::new(ep(7002), ep(7001));

        assert_eq!(holder.pair_ranges(pair).unwrap(), &[range(0, 100)]);
        assert_eq!(
            holder
                .range_map()
                .endpoints_for_range(range(0, 100))
                .unwrap(),
            vec![ep(7001), ep(7002)]
        );
    }

    #[test]
    fn majority_replica_streams_to_divergent_replica() {
        let mut holder = DifferenceHolder::new();
        holder.add_diff(ep(7001), ep(7002), [range(0, 100)]);
        holder.add_diff(ep(7002), ep(7003), [range(0, 100)]);

        let tasks =
            holder.create_asymmetric_sync_plan(&[ep(7001), ep(7002), ep(7003)], "ks", "tbl");

        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].source, ep(7001));
        assert_eq!(tasks[0].target, ep(7002));
        assert_eq!(tasks[0].ranges, vec![(tok(0), tok(100))]);
    }

    #[test]
    fn two_replica_tie_falls_back_to_bidirectional_sync() {
        let mut holder = DifferenceHolder::new();
        holder.add_diff(ep(7001), ep(7002), [range(0, 100)]);

        let tasks = holder.create_asymmetric_sync_plan(&[ep(7001), ep(7002)], "ks", "tbl");

        assert_eq!(tasks.len(), 2);
        assert!(
            tasks
                .iter()
                .any(|task| task.source == ep(7001) && task.target == ep(7002))
        );
        assert!(
            tasks
                .iter()
                .any(|task| task.source == ep(7002) && task.target == ep(7001))
        );
    }

    #[test]
    fn holder_serializes() {
        let mut holder = DifferenceHolder::new();
        holder.add_diff(ep(7001), ep(7002), [range(0, 100)]);

        let json = serde_json::to_string(&holder).unwrap();
        let decoded: DifferenceHolder = serde_json::from_str(&json).unwrap();

        assert_eq!(
            decoded
                .range_map()
                .endpoints_for_range(range(0, 100))
                .unwrap(),
            vec![ep(7001), ep(7002)]
        );
    }
}
