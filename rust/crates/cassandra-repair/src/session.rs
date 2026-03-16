// Licensed under Apache License, Version 2.0.

//! Repair session: manages repair between coordinator and replicas for a range.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.repair.RepairSession`

use std::fmt;
use std::time::Instant;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use cassandra_cluster_metadata::Endpoint;
use cassandra_common::Token;
use crate::coordinator::RepairType;
use crate::merkle::MerkleTree;

/// Unique repair session identifier.
pub type RepairSessionId = Uuid;

/// State of a repair session.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RepairSessionState {
    /// Session created, not started.
    Initialized,
    /// Building Merkle trees.
    BuildingTrees,
    /// Exchanging and diffing trees.
    ExchangingTrees,
    /// Streaming mismatched data.
    Streaming,
    /// Repair completed.
    Complete,
    /// Repair failed.
    Failed,
    /// Repair cancelled.
    Cancelled,
}

impl fmt::Display for RepairSessionState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Initialized => write!(f, "INITIALIZED"),
            Self::BuildingTrees => write!(f, "BUILDING_TREES"),
            Self::ExchangingTrees => write!(f, "EXCHANGING_TREES"),
            Self::Streaming => write!(f, "STREAMING"),
            Self::Complete => write!(f, "COMPLETE"),
            Self::Failed => write!(f, "FAILED"),
            Self::Cancelled => write!(f, "CANCELLED"),
        }
    }
}

/// A repair session between the coordinator and replicas for a specific range.
#[derive(Debug)]
pub struct RepairSession {
    /// Unique session identifier.
    pub id: RepairSessionId,
    /// Parent repair identifier
    pub parent_id: Uuid,
    /// Type of repair.
    pub repair_type: RepairType,
    /// Keyspace being repaired.
    pub keyspace: String,
    /// Table being repaired (empty = all tables).
    pub table: String,
    /// Token range being repaired.
    pub range: (Token, Token),
    /// Participating replicas.
    pub participants: Vec<Endpoint>,
    /// Current state.
    pub state: RepairSessionState,
    /// When the session started.
    pub started_at: Instant,
    /// Error message if failed.
    pub error: Option<String>,
    /// Number of mismatched ranges found.
    pub mismatched_ranges: usize,
    /// Bytes streamed for repair.
    pub bytes_streamed: u64,
}

impl RepairSession {
    /// Create a new repair session.
    pub fn new(
        parent_id: Uuid,
        repair_type: RepairType,
        keyspace: impl Into<String>,
        table: impl Into<String>,
        range: (Token, Token),
        participants: Vec<Endpoint>,
    ) -> Self {
        Self {
            id: Uuid::new_v4(),
            parent_id,
            repair_type,
            keyspace: keyspace.into(),
            table: table.into(),
            range,
            participants,
            state: RepairSessionState::Initialized,
            started_at: Instant::now(),
            error: None,
            mismatched_ranges: 0,
            bytes_streamed: 0,
        }
    }

    /// Transition to BuildingTrees.
    pub fn start_building_trees(&mut self) -> Result<(), RepairSessionError> {
        if self.state != RepairSessionState::Initialized {
            return Err(RepairSessionError::InvalidTransition {
                from: self.state,
                to: RepairSessionState::BuildingTrees,
            });
        }
        self.state = RepairSessionState::BuildingTrees;
        Ok(())
    }

    /// Transition to ExchangingTrees.
    pub fn start_exchanging_trees(&mut self) -> Result<(), RepairSessionError> {
        if self.state != RepairSessionState::BuildingTrees {
            return Err(RepairSessionError::InvalidTransition {
                from: self.state,
                to: RepairSessionState::ExchangingTrees,
            });
        }
        self.state = RepairSessionState::ExchangingTrees;
        Ok(())
    }

    /// Transition to Streaming.
    pub fn start_streaming(&mut self, mismatched: usize) -> Result<(), RepairSessionError> {
        if self.state != RepairSessionState::ExchangingTrees {
            return Err(RepairSessionError::InvalidTransition {
                from: self.state,
                to: RepairSessionState::Streaming,
            });
        }
        self.mismatched_ranges = mismatched;
        self.state = RepairSessionState::Streaming;
        Ok(())
    }

    /// Complete the session.
    pub fn complete(&mut self) -> Result<(), RepairSessionError> {
        if self.state != RepairSessionState::Streaming
            && self.state != RepairSessionState::ExchangingTrees
        {
            return Err(RepairSessionError::InvalidTransition {
                from: self.state,
                to: RepairSessionState::Complete,
            });
        }
        self.state = RepairSessionState::Complete;
        Ok(())
    }

    /// Mark as failed.
    pub fn fail(&mut self, error: impl Into<String>) {
        self.state = RepairSessionState::Failed;
        self.error = Some(error.into());
    }

    /// Cancel the session.
    pub fn cancel(&mut self) {
        self.state = RepairSessionState::Cancelled;
    }

    /// Duration since session start.
    pub fn elapsed(&self) -> std::time::Duration {
        self.started_at.elapsed()
    }
}

/// Errors from repair session state machine.
#[derive(Debug, thiserror::Error)]
pub enum RepairSessionError {
    #[error("Invalid state transition from {from} to {to}")]
    InvalidTransition {
        from: RepairSessionState,
        to: RepairSessionState,
    },
}

/// Request to a replica to validate a token range (build a Merkle tree).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationRequest {
    pub session_id: RepairSessionId,
    pub keyspace: String,
    pub table: String,
    pub range: (Token, Token),
    pub repair_type: RepairType,
    pub gc_grace_seconds: i32,
    pub now_seconds: i32,
}

/// Response from a replica containing the built Merkle tree.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationResponse {
    pub session_id: RepairSessionId,
    pub tree: MerkleTree,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    fn ep(port: u16) -> Endpoint {
        Endpoint::new(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            port,
        ))
    }

    #[test]
    fn session_happy_path() {
        let mut s = RepairSession::new(
            Uuid::new_v4(),
            RepairType::Full,
            "ks", "t1",
            (Token::from_raw(0), Token::from_raw(100)),
            vec![ep(7001), ep(7002)],
        );

        assert_eq!(s.state, RepairSessionState::Initialized);
        s.start_building_trees().unwrap();
        s.start_exchanging_trees().unwrap();
        s.start_streaming(3).unwrap();
        assert_eq!(s.mismatched_ranges, 3);
        s.complete().unwrap();
        assert_eq!(s.state, RepairSessionState::Complete);
    }

    #[test]
    fn session_no_diffs_complete_from_exchange() {
        let mut s = RepairSession::new(
            Uuid::new_v4(),
            RepairType::Full,
            "ks", "t1",
            (Token::from_raw(0), Token::from_raw(100)),
            vec![ep(7001)],
        );

        s.start_building_trees().unwrap();
        s.start_exchanging_trees().unwrap();
        // No diffs found - complete directly from exchange
        s.complete().unwrap();
        assert_eq!(s.state, RepairSessionState::Complete);
    }

    #[test]
    fn session_failure() {
        let mut s = RepairSession::new(
            Uuid::new_v4(),
            RepairType::Full,
            "ks", "t1",
            (Token::from_raw(0), Token::from_raw(100)),
            vec![ep(7001)],
        );

        s.fail("connection lost");
        assert_eq!(s.state, RepairSessionState::Failed);
        assert_eq!(s.error.as_deref(), Some("connection lost"));
    }

    #[test]
    fn session_invalid_transition() {
        let mut s = RepairSession::new(
            Uuid::new_v4(),
            RepairType::Full,
            "ks", "t1",
            (Token::from_raw(0), Token::from_raw(100)),
            vec![],
        );

        // Can't jump to streaming from initialized
        let result = s.start_streaming(0);
        assert!(result.is_err());
    }
}
