// Licensed under Apache License, Version 2.0.

//! # Topology Operations Integration
//!
//! High-level wrappers connecting streaming to topology operations
//! (bootstrap, decommission, rebuild, repair).
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.dht.BootStrapper`
//! - `org.apache.cassandra.streaming.StreamPlan`

use std::net::SocketAddr;
use std::sync::Arc;

use cassandra_cluster_metadata::Endpoint;
use cassandra_common::token::Token;

use crate::coordinator::{CoordinatorResult, StreamCoordinator};
use crate::plan::{StreamOperation, StreamPlan};
use crate::transport::StreamTransport;

/// Errors from topology streaming operations.
#[derive(Debug, thiserror::Error)]
pub enum StreamTopologyError {
    #[error("no sources provided")]
    NoSources,

    #[error("no targets provided")]
    NoTargets,

    #[error("coordinator error: {0}")]
    Coordinator(#[from] crate::coordinator::CoordinatorError),

    #[error("streaming failed: {completed} completed, {failed} failed")]
    PartialFailure { completed: u32, failed: u32 },
}

/// Bootstrap stream: fetch ranges from existing nodes.
pub struct BootstrapStream;

impl BootstrapStream {
    /// Execute bootstrap streaming from multiple sources.
    ///
    /// Each source is `(address, keyspace, ranges)`.
    pub async fn execute(
        coordinator: &StreamCoordinator,
        transport: Arc<StreamTransport>,
        sources: Vec<(SocketAddr, String, Vec<(i64, i64)>)>,
    ) -> Result<CoordinatorResult, StreamTopologyError> {
        if sources.is_empty() {
            return Err(StreamTopologyError::NoSources);
        }

        let mut plan = StreamPlan::new(StreamOperation::Bootstrap);
        for (addr, keyspace, ranges) in sources {
            let token_ranges: Vec<(Token, Token)> = ranges
                .into_iter()
                .map(|(s, e)| (Token(s), Token(e)))
                .collect();
            plan = plan.request_ranges(
                Endpoint::new(addr),
                keyspace,
                vec![],
                token_ranges,
            );
        }

        let mut futures = coordinator.execute_plan(plan, transport).await?;
        let result = StreamCoordinator::await_all(&mut futures).await;

        if result.sessions_failed > 0 {
            return Err(StreamTopologyError::PartialFailure {
                completed: result.sessions_completed,
                failed: result.sessions_failed,
            });
        }

        Ok(result)
    }
}

/// Decommission stream: transfer data to remaining nodes.
pub struct DecommissionStream;

impl DecommissionStream {
    /// Execute decommission by transferring ranges to targets.
    pub async fn execute(
        coordinator: &StreamCoordinator,
        transport: Arc<StreamTransport>,
        targets: Vec<(SocketAddr, String, Vec<(i64, i64)>)>,
    ) -> Result<CoordinatorResult, StreamTopologyError> {
        if targets.is_empty() {
            return Err(StreamTopologyError::NoTargets);
        }

        let mut plan = StreamPlan::new(StreamOperation::Decommission);
        for (addr, keyspace, ranges) in targets {
            let token_ranges: Vec<(Token, Token)> = ranges
                .into_iter()
                .map(|(s, e)| (Token(s), Token(e)))
                .collect();
            plan = plan.transfer_ranges(
                Endpoint::new(addr),
                keyspace,
                vec![],
                token_ranges,
            );
        }

        let mut futures = coordinator.execute_plan(plan, transport).await?;
        let result = StreamCoordinator::await_all(&mut futures).await;

        if result.sessions_failed > 0 {
            return Err(StreamTopologyError::PartialFailure {
                completed: result.sessions_completed,
                failed: result.sessions_failed,
            });
        }

        Ok(result)
    }
}

/// Rebuild stream: fetch data from a specific datacenter.
pub struct RebuildStream;

impl RebuildStream {
    /// Execute rebuild by requesting keyspaces from a source DC peer.
    pub async fn execute(
        coordinator: &StreamCoordinator,
        transport: Arc<StreamTransport>,
        source_addr: SocketAddr,
        keyspaces: Vec<String>,
        ranges: Vec<(i64, i64)>,
    ) -> Result<CoordinatorResult, StreamTopologyError> {
        if keyspaces.is_empty() {
            return Err(StreamTopologyError::NoSources);
        }

        let token_ranges: Vec<(Token, Token)> = ranges
            .into_iter()
            .map(|(s, e)| (Token(s), Token(e)))
            .collect();

        let mut plan = StreamPlan::new(StreamOperation::Rebuild);
        for ks in keyspaces {
            plan = plan.request_ranges(
                Endpoint::new(source_addr),
                ks,
                vec![],
                token_ranges.clone(),
            );
        }

        let mut futures = coordinator.execute_plan(plan, transport).await?;
        let result = StreamCoordinator::await_all(&mut futures).await;

        if result.sessions_failed > 0 {
            return Err(StreamTopologyError::PartialFailure {
                completed: result.sessions_completed,
                failed: result.sessions_failed,
            });
        }

        Ok(result)
    }
}

/// Repair stream: exchange ranges with a peer during repair.
pub struct RepairStream;

impl RepairStream {
    /// Execute repair-driven streaming with a peer.
    pub async fn execute(
        coordinator: &StreamCoordinator,
        transport: Arc<StreamTransport>,
        peer_addr: SocketAddr,
        keyspace: String,
        ranges: Vec<(i64, i64)>,
    ) -> Result<CoordinatorResult, StreamTopologyError> {
        let token_ranges: Vec<(Token, Token)> = ranges
            .into_iter()
            .map(|(s, e)| (Token(s), Token(e)))
            .collect();

        let plan = StreamPlan::new(StreamOperation::Repair).request_ranges(
            Endpoint::new(peer_addr),
            keyspace,
            vec![],
            token_ranges,
        );

        let mut futures = coordinator.execute_plan(plan, transport).await?;
        let result = StreamCoordinator::await_all(&mut futures).await;

        if result.sessions_failed > 0 {
            return Err(StreamTopologyError::PartialFailure {
                completed: result.sessions_completed,
                failed: result.sessions_failed,
            });
        }

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bootstrap_plan_construction() {
        let sources: Vec<(SocketAddr, String, Vec<(i64, i64)>)> = vec![
            (
                "127.0.0.1:7000".parse().unwrap(),
                "ks1".into(),
                vec![(0, 1000), (2000, 3000)],
            ),
            (
                "127.0.0.2:7000".parse().unwrap(),
                "ks2".into(),
                vec![(1000, 2000)],
            ),
        ];

        let mut plan = StreamPlan::new(StreamOperation::Bootstrap);
        for (addr, keyspace, ranges) in &sources {
            let token_ranges: Vec<(Token, Token)> = ranges
                .iter()
                .map(|(s, e)| (Token(*s), Token(*e)))
                .collect();
            plan = plan.request_ranges(
                Endpoint::new(*addr),
                keyspace.clone(),
                vec![],
                token_ranges,
            );
        }

        assert_eq!(plan.peer_count(), 2);
        let sessions = plan.build();
        assert_eq!(sessions.len(), 2);
    }

    #[test]
    fn decommission_plan_construction() {
        let mut plan = StreamPlan::new(StreamOperation::Decommission);
        plan = plan.transfer_ranges(
            Endpoint::new("127.0.0.1:7000".parse().unwrap()),
            "ks1",
            vec![],
            vec![(Token(0), Token(1000))],
        );
        plan = plan.transfer_ranges(
            Endpoint::new("127.0.0.2:7000".parse().unwrap()),
            "ks1",
            vec![],
            vec![(Token(1000), Token(2000))],
        );

        assert_eq!(plan.peer_count(), 2);
        let sessions = plan.build();
        assert_eq!(sessions.len(), 2);
        for s in &sessions {
            assert!(!s.outgoing.is_empty());
        }
    }

    #[test]
    fn repair_plan_construction() {
        let plan = StreamPlan::new(StreamOperation::Repair).request_ranges(
            Endpoint::new("127.0.0.1:7000".parse().unwrap()),
            "ks1",
            vec![],
            vec![(Token(0), Token(1000))],
        );

        assert_eq!(plan.peer_count(), 1);
        let sessions = plan.build();
        assert_eq!(sessions.len(), 1);
    }

    #[test]
    fn rebuild_multiple_keyspaces_same_peer() {
        let peer = Endpoint::new("127.0.0.1:7000".parse().unwrap());
        let ranges = vec![(Token(0), Token(1000))];

        let mut plan = StreamPlan::new(StreamOperation::Rebuild);
        for ks in &["ks1", "ks2", "ks3"] {
            plan = plan.request_ranges(peer.clone(), *ks, vec![], ranges.clone());
        }

        // Same peer should produce a single session
        assert_eq!(plan.peer_count(), 1);
        let sessions = plan.build();
        assert_eq!(sessions.len(), 1);
        // Should have 3 incoming transfers
        assert_eq!(sessions[0].incoming.len(), 3);
    }
}
