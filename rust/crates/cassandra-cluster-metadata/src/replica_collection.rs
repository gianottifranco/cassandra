// Licensed under Apache License, Version 2.0.

//! Typed replication factor and endpoint collection types.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.locator.ReplicationFactor`
//! - `org.apache.cassandra.locator.EndpointsForToken`
//! - `org.apache.cassandra.locator.EndpointsForRange`
//! - `org.apache.cassandra.locator.RangesAtEndpoint`

use std::fmt;

use cassandra_common::token::TokenRange;

use crate::node::Endpoint;
use crate::replication::Replica;

// ─────────────────────────────────────────────────────────────────────────────
// ReplicationFactor
// ─────────────────────────────────────────────────────────────────────────────

/// Typed replication factor supporting transient replication.
///
/// Format: `"3"` means 3 full replicas, `"3/1"` means 3 total (2 full + 1
/// transient).
///
/// ## Java Oracle
///
/// `org.apache.cassandra.locator.ReplicationFactor`
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ReplicationFactor {
    /// Total number of replicas (full + transient).
    all_replicas: usize,
    /// Number of full (non-transient) replicas.
    full_replicas: usize,
}

impl ReplicationFactor {
    /// Create a ReplicationFactor with only full replicas.
    pub fn full(rf: usize) -> Self {
        Self {
            all_replicas: rf,
            full_replicas: rf,
        }
    }

    /// Create a ReplicationFactor with transient replicas.
    pub fn with_transient(all_replicas: usize, transient_replicas: usize) -> Self {
        assert!(
            transient_replicas < all_replicas,
            "transient count must be less than total"
        );
        Self {
            all_replicas,
            full_replicas: all_replicas - transient_replicas,
        }
    }

    /// Parse from string format: "3" or "3/1".
    pub fn parse(s: &str) -> Option<Self> {
        if let Some((total_str, trans_str)) = s.split_once('/') {
            let total: usize = total_str.trim().parse().ok()?;
            let transient: usize = trans_str.trim().parse().ok()?;
            if transient >= total {
                return None;
            }
            Some(Self::with_transient(total, transient))
        } else {
            let total: usize = s.trim().parse().ok()?;
            Some(Self::full(total))
        }
    }

    /// Total replicas (full + transient).
    pub fn all_replicas(&self) -> usize {
        self.all_replicas
    }

    /// Number of full replicas.
    pub fn full_replicas(&self) -> usize {
        self.full_replicas
    }

    /// Number of transient replicas.
    pub fn transient_replicas(&self) -> usize {
        self.all_replicas - self.full_replicas
    }

    /// Whether this RF includes transient replicas.
    pub fn has_transient(&self) -> bool {
        self.all_replicas > self.full_replicas
    }
}

impl fmt::Display for ReplicationFactor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.has_transient() {
            write!(f, "{}/{}", self.all_replicas, self.transient_replicas())
        } else {
            write!(f, "{}", self.all_replicas)
        }
    }
}

impl From<usize> for ReplicationFactor {
    fn from(rf: usize) -> Self {
        Self::full(rf)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// EndpointsForToken
// ─────────────────────────────────────────────────────────────────────────────

/// Collection of replicas for a specific token.
///
/// ## Java Oracle
///
/// `org.apache.cassandra.locator.EndpointsForToken`
#[derive(Debug, Clone)]
pub struct EndpointsForToken {
    replicas: Vec<Replica>,
}

impl EndpointsForToken {
    pub fn new(replicas: Vec<Replica>) -> Self {
        Self { replicas }
    }

    pub fn empty() -> Self {
        Self {
            replicas: Vec::new(),
        }
    }

    /// All replicas (full + transient).
    pub fn replicas(&self) -> &[Replica] {
        &self.replicas
    }

    /// Only full replicas.
    pub fn full(&self) -> Vec<&Replica> {
        self.replicas.iter().filter(|r| r.is_full()).collect()
    }

    /// Only transient replicas.
    pub fn transient(&self) -> Vec<&Replica> {
        self.replicas.iter().filter(|r| r.is_transient).collect()
    }

    /// All endpoints (regardless of full/transient).
    pub fn endpoints(&self) -> Vec<Endpoint> {
        self.replicas.iter().map(|r| r.endpoint).collect()
    }

    /// Filter replicas by datacenter using a snitch.
    pub fn filter_by_dc(&self, dc: &str, snitch: &dyn crate::snitch::Snitch) -> Self {
        Self {
            replicas: self
                .replicas
                .iter()
                .filter(|r| snitch.datacenter(&r.endpoint) == dc)
                .cloned()
                .collect(),
        }
    }

    pub fn len(&self) -> usize {
        self.replicas.len()
    }

    pub fn is_empty(&self) -> bool {
        self.replicas.is_empty()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// EndpointsForRange
// ─────────────────────────────────────────────────────────────────────────────

/// Collection of replicas for a specific token range.
///
/// ## Java Oracle
///
/// `org.apache.cassandra.locator.EndpointsForRange`
#[derive(Debug, Clone)]
pub struct EndpointsForRange {
    range: TokenRange,
    replicas: Vec<Replica>,
}

impl EndpointsForRange {
    pub fn new(range: TokenRange, replicas: Vec<Replica>) -> Self {
        Self { range, replicas }
    }

    pub fn range(&self) -> &TokenRange {
        &self.range
    }

    pub fn replicas(&self) -> &[Replica] {
        &self.replicas
    }

    pub fn full(&self) -> Vec<&Replica> {
        self.replicas.iter().filter(|r| r.is_full()).collect()
    }

    pub fn transient(&self) -> Vec<&Replica> {
        self.replicas.iter().filter(|r| r.is_transient).collect()
    }

    pub fn endpoints(&self) -> Vec<Endpoint> {
        self.replicas.iter().map(|r| r.endpoint).collect()
    }

    pub fn filter_by_dc(&self, dc: &str, snitch: &dyn crate::snitch::Snitch) -> Self {
        Self {
            range: self.range,
            replicas: self
                .replicas
                .iter()
                .filter(|r| snitch.datacenter(&r.endpoint) == dc)
                .cloned()
                .collect(),
        }
    }

    pub fn len(&self) -> usize {
        self.replicas.len()
    }

    pub fn is_empty(&self) -> bool {
        self.replicas.is_empty()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// RangesAtEndpoint
// ─────────────────────────────────────────────────────────────────────────────

/// All token ranges held by a single endpoint.
///
/// ## Java Oracle
///
/// `org.apache.cassandra.locator.RangesAtEndpoint`
#[derive(Debug, Clone)]
pub struct RangesAtEndpoint {
    endpoint: Endpoint,
    full_ranges: Vec<TokenRange>,
    transient_ranges: Vec<TokenRange>,
}

impl RangesAtEndpoint {
    pub fn new(endpoint: Endpoint) -> Self {
        Self {
            endpoint,
            full_ranges: Vec::new(),
            transient_ranges: Vec::new(),
        }
    }

    pub fn endpoint(&self) -> Endpoint {
        self.endpoint
    }

    pub fn add_full_range(&mut self, range: TokenRange) {
        self.full_ranges.push(range);
    }

    pub fn add_transient_range(&mut self, range: TokenRange) {
        self.transient_ranges.push(range);
    }

    /// All ranges (full + transient).
    pub fn all_ranges(&self) -> Vec<&TokenRange> {
        self.full_ranges
            .iter()
            .chain(self.transient_ranges.iter())
            .collect()
    }

    pub fn full_ranges(&self) -> &[TokenRange] {
        &self.full_ranges
    }

    pub fn transient_ranges(&self) -> &[TokenRange] {
        &self.transient_ranges
    }

    pub fn is_empty(&self) -> bool {
        self.full_ranges.is_empty() && self.transient_ranges.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cassandra_common::Token;
    use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

    fn ep(port: u16) -> Endpoint {
        Endpoint::new(SocketAddr::V4(SocketAddrV4::new(
            Ipv4Addr::new(127, 0, 0, 1),
            port,
        )))
    }

    #[test]
    fn replication_factor_full() {
        let rf = ReplicationFactor::full(3);
        assert_eq!(rf.all_replicas(), 3);
        assert_eq!(rf.full_replicas(), 3);
        assert_eq!(rf.transient_replicas(), 0);
        assert!(!rf.has_transient());
        assert_eq!(rf.to_string(), "3");
    }

    #[test]
    fn replication_factor_transient() {
        let rf = ReplicationFactor::with_transient(3, 1);
        assert_eq!(rf.all_replicas(), 3);
        assert_eq!(rf.full_replicas(), 2);
        assert_eq!(rf.transient_replicas(), 1);
        assert!(rf.has_transient());
        assert_eq!(rf.to_string(), "3/1");
    }

    #[test]
    fn replication_factor_parse() {
        assert_eq!(ReplicationFactor::parse("3"), Some(ReplicationFactor::full(3)));
        assert_eq!(
            ReplicationFactor::parse("3/1"),
            Some(ReplicationFactor::with_transient(3, 1))
        );
        assert_eq!(ReplicationFactor::parse("invalid"), None);
        assert_eq!(ReplicationFactor::parse("3/3"), None); // transient >= total
    }

    #[test]
    fn replication_factor_from_usize() {
        let rf: ReplicationFactor = 3.into();
        assert_eq!(rf, ReplicationFactor::full(3));
    }

    #[test]
    fn endpoints_for_token_basics() {
        let replicas = vec![
            Replica::full(ep(7001)),
            Replica::full(ep(7002)),
            Replica::transient(ep(7003)),
        ];
        let eft = EndpointsForToken::new(replicas);
        assert_eq!(eft.len(), 3);
        assert_eq!(eft.full().len(), 2);
        assert_eq!(eft.transient().len(), 1);
        assert_eq!(eft.endpoints().len(), 3);
    }

    #[test]
    fn endpoints_for_range_basics() {
        let range = TokenRange::new(Token::from_raw(0), Token::from_raw(100));
        let replicas = vec![Replica::full(ep(7001)), Replica::full(ep(7002))];
        let efr = EndpointsForRange::new(range, replicas);
        assert_eq!(*efr.range(), range);
        assert_eq!(efr.len(), 2);
    }

    #[test]
    fn ranges_at_endpoint() {
        let mut rae = RangesAtEndpoint::new(ep(7001));
        assert!(rae.is_empty());
        rae.add_full_range(TokenRange::new(Token::from_raw(0), Token::from_raw(100)));
        rae.add_transient_range(TokenRange::new(Token::from_raw(100), Token::from_raw(200)));
        assert!(!rae.is_empty());
        assert_eq!(rae.full_ranges().len(), 1);
        assert_eq!(rae.transient_ranges().len(), 1);
        assert_eq!(rae.all_ranges().len(), 2);
        assert_eq!(rae.endpoint(), ep(7001));
    }
}
