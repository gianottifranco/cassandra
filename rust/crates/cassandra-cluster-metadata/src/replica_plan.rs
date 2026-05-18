// Licensed under Apache License, Version 2.0.

//! Replica plans for read and write operations.
//!
//! A replica plan captures the set of replicas to contact for a given
//! operation, including consistency level requirements.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.locator.ReplicaPlan`
//! - `org.apache.cassandra.locator.ReplicaPlan.ForRead`
//! - `org.apache.cassandra.locator.ReplicaPlan.ForWrite`
//! - `org.apache.cassandra.locator.ReplicaPlan.ForRangeRead`

use cassandra_common::Token;
use cassandra_common::token::TokenRange;

use crate::node::Endpoint;
use crate::replica_collection::EndpointsForToken;
use crate::replication::Replica;

// ─────────────────────────────────────────────────────────────────────────────
// ReplicaPlanForRead
// ─────────────────────────────────────────────────────────────────────────────

/// Plan for a read operation at a single token.
///
/// Contains the full set of replicas and the subset to contact.
#[derive(Debug, Clone)]
pub struct ReplicaPlanForRead {
    /// The token being read.
    token: Token,
    /// All replicas for this token.
    replicas: EndpointsForToken,
    /// Endpoints to contact for this read.
    contacts: Vec<Endpoint>,
    /// Number of responses needed for consistency.
    block_for: usize,
}

impl ReplicaPlanForRead {
    pub fn new(
        token: Token,
        replicas: EndpointsForToken,
        contacts: Vec<Endpoint>,
        block_for: usize,
    ) -> Self {
        Self {
            token,
            replicas,
            contacts,
            block_for,
        }
    }

    pub fn token(&self) -> Token {
        self.token
    }

    pub fn replicas(&self) -> &EndpointsForToken {
        &self.replicas
    }

    pub fn contacts(&self) -> &[Endpoint] {
        &self.contacts
    }

    pub fn block_for(&self) -> usize {
        self.block_for
    }

    /// Whether we have enough contacts to satisfy the consistency level.
    pub fn is_sufficient(&self) -> bool {
        self.contacts.len() >= self.block_for
    }

    /// Number of additional responses needed.
    pub fn shortfall(&self) -> usize {
        self.block_for.saturating_sub(self.contacts.len())
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// ReplicaPlanForWrite
// ─────────────────────────────────────────────────────────────────────────────

/// Plan for a write operation at a single token.
///
/// Includes both live and pending endpoints to ensure consistency
/// during topology changes.
#[derive(Debug, Clone)]
pub struct ReplicaPlanForWrite {
    /// The token being written.
    token: Token,
    /// All replicas for this token (natural replicas).
    replicas: EndpointsForToken,
    /// Currently live endpoints that can accept writes.
    live_endpoints: Vec<Endpoint>,
    /// Pending endpoints (during topology changes).
    pending_endpoints: Vec<Endpoint>,
    /// Number of responses needed for consistency.
    block_for: usize,
}

impl ReplicaPlanForWrite {
    pub fn new(
        token: Token,
        replicas: EndpointsForToken,
        live_endpoints: Vec<Endpoint>,
        pending_endpoints: Vec<Endpoint>,
        block_for: usize,
    ) -> Self {
        Self {
            token,
            replicas,
            live_endpoints,
            pending_endpoints,
            block_for,
        }
    }

    pub fn token(&self) -> Token {
        self.token
    }

    pub fn replicas(&self) -> &EndpointsForToken {
        &self.replicas
    }

    pub fn live_endpoints(&self) -> &[Endpoint] {
        &self.live_endpoints
    }

    pub fn pending_endpoints(&self) -> &[Endpoint] {
        &self.pending_endpoints
    }

    pub fn block_for(&self) -> usize {
        self.block_for
    }

    /// All endpoints that should receive this write (live + pending).
    pub fn all_write_endpoints(&self) -> Vec<Endpoint> {
        let mut all = self.live_endpoints.clone();
        for ep in &self.pending_endpoints {
            if !all.contains(ep) {
                all.push(*ep);
            }
        }
        all
    }

    /// Whether we have enough live endpoints for the consistency level.
    pub fn is_sufficient(&self) -> bool {
        self.live_endpoints.len() >= self.block_for
    }

    /// Number of additional responses needed.
    pub fn shortfall(&self) -> usize {
        self.block_for.saturating_sub(self.live_endpoints.len())
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// ReplicaPlanForRangeRead
// ─────────────────────────────────────────────────────────────────────────────

/// Plan for a range read (scan) operation across multiple token ranges.
///
/// Each sub-range has its own set of contacts.
#[derive(Debug, Clone)]
pub struct ReplicaPlanForRangeRead {
    /// The overall range being scanned.
    range: TokenRange,
    /// Per-range replica plans.
    range_plans: Vec<RangeReadPlan>,
    /// Number of responses needed per range.
    block_for: usize,
}

/// A single range within a range read plan.
#[derive(Debug, Clone)]
pub struct RangeReadPlan {
    pub range: TokenRange,
    pub replicas: Vec<Replica>,
    pub contacts: Vec<Endpoint>,
}

impl ReplicaPlanForRangeRead {
    pub fn new(range: TokenRange, range_plans: Vec<RangeReadPlan>, block_for: usize) -> Self {
        Self {
            range,
            range_plans,
            block_for,
        }
    }

    pub fn range(&self) -> &TokenRange {
        &self.range
    }

    pub fn range_plans(&self) -> &[RangeReadPlan] {
        &self.range_plans
    }

    pub fn block_for(&self) -> usize {
        self.block_for
    }

    /// Whether all sub-ranges have sufficient contacts.
    pub fn is_sufficient(&self) -> bool {
        self.range_plans
            .iter()
            .all(|p| p.contacts.len() >= self.block_for)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replication::Replica;
    use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

    fn ep(port: u16) -> Endpoint {
        Endpoint::new(SocketAddr::V4(SocketAddrV4::new(
            Ipv4Addr::new(127, 0, 0, 1),
            port,
        )))
    }

    #[test]
    fn read_plan_sufficient() {
        let replicas = EndpointsForToken::new(vec![
            Replica::full(ep(7001)),
            Replica::full(ep(7002)),
            Replica::full(ep(7003)),
        ]);
        let plan =
            ReplicaPlanForRead::new(Token::from_raw(42), replicas, vec![ep(7001), ep(7002)], 2);
        assert!(plan.is_sufficient());
        assert_eq!(plan.shortfall(), 0);
    }

    #[test]
    fn read_plan_insufficient() {
        let replicas = EndpointsForToken::new(vec![Replica::full(ep(7001))]);
        let plan = ReplicaPlanForRead::new(Token::from_raw(42), replicas, vec![ep(7001)], 2);
        assert!(!plan.is_sufficient());
        assert_eq!(plan.shortfall(), 1);
    }

    #[test]
    fn write_plan_all_endpoints() {
        let replicas =
            EndpointsForToken::new(vec![Replica::full(ep(7001)), Replica::full(ep(7002))]);
        let plan = ReplicaPlanForWrite::new(
            Token::from_raw(42),
            replicas,
            vec![ep(7001), ep(7002)],
            vec![ep(7003)],
            2,
        );
        let all = plan.all_write_endpoints();
        assert_eq!(all.len(), 3);
        assert!(all.contains(&ep(7003)));
    }

    #[test]
    fn write_plan_no_duplicate_endpoints() {
        let replicas = EndpointsForToken::new(vec![Replica::full(ep(7001))]);
        let plan = ReplicaPlanForWrite::new(
            Token::from_raw(42),
            replicas,
            vec![ep(7001)],
            vec![ep(7001)], // duplicate
            1,
        );
        let all = plan.all_write_endpoints();
        assert_eq!(all.len(), 1);
    }

    #[test]
    fn range_read_plan_sufficient() {
        let plans = vec![
            RangeReadPlan {
                range: TokenRange::new(Token::from_raw(0), Token::from_raw(100)),
                replicas: vec![Replica::full(ep(7001))],
                contacts: vec![ep(7001)],
            },
            RangeReadPlan {
                range: TokenRange::new(Token::from_raw(100), Token::from_raw(200)),
                replicas: vec![Replica::full(ep(7002))],
                contacts: vec![ep(7002)],
            },
        ];
        let plan = ReplicaPlanForRangeRead::new(
            TokenRange::new(Token::from_raw(0), Token::from_raw(200)),
            plans,
            1,
        );
        assert!(plan.is_sufficient());
    }
}
