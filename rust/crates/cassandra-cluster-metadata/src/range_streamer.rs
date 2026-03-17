// Licensed under Apache License, Version 2.0.

//! Range streaming orchestration for bootstrap and rebuild operations.
//!
//! Determines which ranges need to be fetched from which source nodes
//! when a new node joins the cluster or a node rebuilds its data.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.dht.RangeStreamer`
//! - `org.apache.cassandra.dht.RangeStreamer.SourceFilter`

use std::collections::HashMap;

use cassandra_common::Token;
use cassandra_common::token::TokenRange;

use crate::node::Endpoint;
use crate::replication::ReplicationStrategy;
use crate::ring::TokenRing;
use crate::snitch::Snitch;
use crate::topology::StreamPlanDescriptor;
use crate::topology::{StreamRangeRequest, TopologyOperation};

/// A pairing of source endpoint and token range to fetch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchReplica {
    /// The source endpoint to stream from.
    pub source: Endpoint,
    /// The range to fetch from this source.
    pub range: TokenRange,
}

/// Filters for excluding source endpoints during range streaming.
///
/// ## Java Oracle
///
/// `org.apache.cassandra.dht.RangeStreamer.SourceFilter`
pub trait SourceFilter: Send + Sync {
    /// Returns true if this endpoint should be excluded as a source.
    fn should_exclude(&self, endpoint: &Endpoint) -> bool;
}

/// Excludes dead nodes from streaming sources.
pub struct ExcludeDeadFilter {
    dead_endpoints: Vec<Endpoint>,
}

impl ExcludeDeadFilter {
    pub fn new(dead_endpoints: Vec<Endpoint>) -> Self {
        Self { dead_endpoints }
    }
}

impl SourceFilter for ExcludeDeadFilter {
    fn should_exclude(&self, endpoint: &Endpoint) -> bool {
        self.dead_endpoints.contains(endpoint)
    }
}

/// Excludes bootstrapping (joining) nodes from streaming sources.
pub struct ExcludeBootstrappingFilter {
    bootstrapping: Vec<Endpoint>,
}

impl ExcludeBootstrappingFilter {
    pub fn new(bootstrapping: Vec<Endpoint>) -> Self {
        Self { bootstrapping }
    }
}

impl SourceFilter for ExcludeBootstrappingFilter {
    fn should_exclude(&self, endpoint: &Endpoint) -> bool {
        self.bootstrapping.contains(endpoint)
    }
}

/// Excludes nodes not in the specified datacenter.
pub struct SameDcFilter<'a> {
    target_dc: String,
    snitch: &'a dyn Snitch,
}

impl<'a> SameDcFilter<'a> {
    pub fn new(target_dc: String, snitch: &'a dyn Snitch) -> Self {
        Self { target_dc, snitch }
    }
}

impl SourceFilter for SameDcFilter<'_> {
    fn should_exclude(&self, endpoint: &Endpoint) -> bool {
        self.snitch.datacenter(endpoint) != self.target_dc
    }
}

/// Orchestrates which ranges to fetch from which sources during bootstrap.
///
/// ## Java Oracle
///
/// `org.apache.cassandra.dht.RangeStreamer`
pub struct RangeStreamer<'a> {
    /// The endpoint that is streaming data in (bootstrapping node).
    local_endpoint: Endpoint,
    /// The current token ring.
    ring: &'a TokenRing,
    /// The replication strategy for the keyspace.
    strategy: &'a dyn ReplicationStrategy,
    /// The snitch for topology decisions.
    snitch: &'a dyn Snitch,
    /// Source filters to exclude unsuitable nodes.
    filters: Vec<Box<dyn SourceFilter + 'a>>,
    /// Ranges to fetch, keyed by keyspace.
    ranges_to_fetch: Vec<FetchReplica>,
}

impl<'a> RangeStreamer<'a> {
    pub fn new(
        local_endpoint: Endpoint,
        ring: &'a TokenRing,
        strategy: &'a dyn ReplicationStrategy,
        snitch: &'a dyn Snitch,
    ) -> Self {
        Self {
            local_endpoint,
            ring,
            strategy,
            snitch,
            filters: Vec::new(),
            ranges_to_fetch: Vec::new(),
        }
    }

    /// Add a source filter.
    pub fn add_filter(&mut self, filter: Box<dyn SourceFilter + 'a>) {
        self.filters.push(filter);
    }

    /// Whether a source endpoint passes all filters.
    fn is_valid_source(&self, endpoint: &Endpoint) -> bool {
        if *endpoint == self.local_endpoint {
            return false;
        }
        !self.filters.iter().any(|f| f.should_exclude(endpoint))
    }

    /// Add ranges that the local endpoint needs to own.
    ///
    /// For each range, finds the best source endpoint that currently
    /// holds the data and can serve it.
    pub fn add_ranges(&mut self, ranges: &[TokenRange]) {
        for range in ranges {
            // Find which endpoints currently hold this range
            let token = range.end; // Use end token for lookup
            let current_replicas =
                self.strategy
                    .calculate_natural_endpoints(token, self.ring, self.snitch);

            // Pick the best valid source (prefer same DC, same rack)
            let mut candidates: Vec<Endpoint> = current_replicas
                .into_iter()
                .filter(|ep| self.is_valid_source(ep))
                .collect();

            self.snitch
                .sort_by_proximity(&self.local_endpoint, &mut candidates);

            if let Some(source) = candidates.first() {
                self.ranges_to_fetch.push(FetchReplica {
                    source: *source,
                    range: *range,
                });
            }
        }
    }

    /// Get all fetch replicas.
    pub fn fetch_replicas(&self) -> &[FetchReplica] {
        &self.ranges_to_fetch
    }

    /// Convert to a stream plan descriptor.
    ///
    /// Groups ranges by source endpoint for efficient streaming.
    pub fn to_stream_plan(&self) -> StreamPlanDescriptor {
        // Group by source
        let mut by_source: HashMap<Endpoint, Vec<(Token, Token)>> = HashMap::new();
        for fetch in &self.ranges_to_fetch {
            by_source
                .entry(fetch.source)
                .or_default()
                .push((fetch.range.start, fetch.range.end));
        }

        let requests = by_source
            .into_iter()
            .map(|(source, ranges)| StreamRangeRequest {
                source,
                destination: self.local_endpoint,
                ranges,
            })
            .collect();

        StreamPlanDescriptor {
            operation: TopologyOperation::Bootstrap,
            requests,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replication::SimpleStrategy;
    use crate::snitch::SimpleSnitch;
    use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

    fn ep(port: u16) -> Endpoint {
        Endpoint::new(SocketAddr::V4(SocketAddrV4::new(
            Ipv4Addr::new(127, 0, 0, 1),
            port,
        )))
    }

    #[test]
    fn basic_range_streaming() {
        let mut ring = TokenRing::new();
        ring.add_token(Token::from_raw(-100), ep(7001));
        ring.add_token(Token::from_raw(0), ep(7002));
        ring.add_token(Token::from_raw(100), ep(7003));

        let strategy = SimpleStrategy::new(2);
        let snitch = SimpleSnitch;

        let mut streamer = RangeStreamer::new(ep(7004), &ring, &strategy, &snitch);

        let ranges = vec![
            TokenRange::new(Token::from_raw(-100), Token::from_raw(0)),
            TokenRange::new(Token::from_raw(0), Token::from_raw(100)),
        ];
        streamer.add_ranges(&ranges);

        let fetches = streamer.fetch_replicas();
        assert_eq!(fetches.len(), 2);

        // Each fetch should have a valid source
        for fetch in fetches {
            assert_ne!(fetch.source, ep(7004));
        }
    }

    #[test]
    fn excludes_dead_nodes() {
        let mut ring = TokenRing::new();
        ring.add_token(Token::from_raw(0), ep(7001));
        ring.add_token(Token::from_raw(100), ep(7002));

        let strategy = SimpleStrategy::new(2);
        let snitch = SimpleSnitch;

        let mut streamer = RangeStreamer::new(ep(7003), &ring, &strategy, &snitch);
        streamer.add_filter(Box::new(ExcludeDeadFilter::new(vec![ep(7001)])));

        let ranges = vec![TokenRange::new(Token::from_raw(0), Token::from_raw(100))];
        streamer.add_ranges(&ranges);

        // Should only use ep(7002) as source
        for fetch in streamer.fetch_replicas() {
            assert_ne!(fetch.source, ep(7001));
        }
    }

    #[test]
    fn stream_plan_groups_by_source() {
        let mut ring = TokenRing::new();
        ring.add_token(Token::from_raw(-100), ep(7001));
        ring.add_token(Token::from_raw(0), ep(7001)); // same endpoint, two tokens
        ring.add_token(Token::from_raw(100), ep(7002));

        let strategy = SimpleStrategy::new(1);
        let snitch = SimpleSnitch;

        let mut streamer = RangeStreamer::new(ep(7003), &ring, &strategy, &snitch);
        let ranges = vec![
            TokenRange::new(Token::from_raw(-100), Token::from_raw(0)),
            TokenRange::new(Token::from_raw(0), Token::from_raw(100)),
        ];
        streamer.add_ranges(&ranges);

        let plan = streamer.to_stream_plan();
        assert_eq!(plan.operation, TopologyOperation::Bootstrap);
        // Requests should be grouped by source
        assert!(!plan.requests.is_empty());
    }

    #[test]
    fn excludes_self_as_source() {
        let mut ring = TokenRing::new();
        ring.add_token(Token::from_raw(0), ep(7001));
        ring.add_token(Token::from_raw(100), ep(7002));

        let strategy = SimpleStrategy::new(2);
        let snitch = SimpleSnitch;

        let mut streamer = RangeStreamer::new(ep(7001), &ring, &strategy, &snitch);
        let ranges = vec![TokenRange::new(Token::from_raw(0), Token::from_raw(100))];
        streamer.add_ranges(&ranges);

        // Should not use self as source
        for fetch in streamer.fetch_replicas() {
            assert_ne!(fetch.source, ep(7001));
        }
    }
}
