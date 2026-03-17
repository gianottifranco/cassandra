// Licensed under Apache License, Version 2.0.

//! Repair messages: wire-format types for all repair protocol verbs.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.repair.messages.RepairMessage`
//! - `org.apache.cassandra.repair.messages.PrepareConsistentRequest`
//! - `org.apache.cassandra.repair.messages.PrepareConsistentResponse`
//! - `org.apache.cassandra.repair.messages.FinalizePropose`
//! - `org.apache.cassandra.repair.messages.FinalizeCommit`
//! - `org.apache.cassandra.repair.messages.StatusRequest`
//! - `org.apache.cassandra.repair.messages.StatusResponse`
//! - `org.apache.cassandra.repair.messages.SyncRequest`
//! - `org.apache.cassandra.repair.messages.SyncComplete`

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use cassandra_cluster_metadata::Endpoint;
use cassandra_common::Token;

/// State of a consistent repair session (coordinator or participant side).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConsistentSessionState {
    Preparing,
    Prepared,
    Repairing,
    FinalizeProposing,
    FinalizePromised,
    Committed,
    Failed,
}

impl std::fmt::Display for ConsistentSessionState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Preparing => write!(f, "PREPARING"),
            Self::Prepared => write!(f, "PREPARED"),
            Self::Repairing => write!(f, "REPAIRING"),
            Self::FinalizeProposing => write!(f, "FINALIZE_PROPOSING"),
            Self::FinalizePromised => write!(f, "FINALIZE_PROMISED"),
            Self::Committed => write!(f, "COMMITTED"),
            Self::Failed => write!(f, "FAILED"),
        }
    }
}

// ── Consistent repair messages ──────────────────────────────

/// Coordinator → participant: prepare for consistent repair.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrepareConsistentRequest {
    pub parent_id: Uuid,
    pub keyspace: String,
    pub tables: Vec<String>,
    pub ranges: Vec<(Token, Token)>,
    pub participants: Vec<Endpoint>,
    pub is_forced: bool,
}

/// Participant → coordinator: prepare response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrepareConsistentResponse {
    pub parent_id: Uuid,
    pub endpoint: Endpoint,
    pub success: bool,
}

/// Coordinator → participant: propose finalization.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FinalizePropose {
    pub parent_id: Uuid,
}

/// Participant → coordinator: promise to finalize.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FinalizePromise {
    pub parent_id: Uuid,
    pub endpoint: Endpoint,
    pub success: bool,
}

/// Coordinator → participant: commit the repair.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FinalizeCommit {
    pub parent_id: Uuid,
}

/// Coordinator → participant: fail the session.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FailSessionMessage {
    pub parent_id: Uuid,
}

/// Status inquiry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatusRequest {
    pub parent_id: Uuid,
}

/// Status response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatusResponse {
    pub parent_id: Uuid,
    pub state: ConsistentSessionState,
}

// ── Sync messages ───────────────────────────────────────────

/// Coordinator → participant: request streaming between two nodes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncRequest {
    pub parent_id: Uuid,
    pub session_id: Uuid,
    pub source: Endpoint,
    pub target: Endpoint,
    pub ranges: Vec<(Token, Token)>,
    pub keyspace: String,
    pub tables: Vec<String>,
}

/// Participant → coordinator: sync completed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncResponse {
    pub parent_id: Uuid,
    pub session_id: Uuid,
    pub success: bool,
    pub error: Option<String>,
}

// ── Envelope ────────────────────────────────────────────────

/// Union of all repair message types.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RepairMessage {
    PrepareConsistentRequest(PrepareConsistentRequest),
    PrepareConsistentResponse(PrepareConsistentResponse),
    FinalizePropose(FinalizePropose),
    FinalizePromise(FinalizePromise),
    FinalizeCommit(FinalizeCommit),
    FailSession(FailSessionMessage),
    StatusRequest(StatusRequest),
    StatusResponse(StatusResponse),
    SyncRequest(SyncRequest),
    SyncResponse(SyncResponse),
}

impl RepairMessage {
    /// Serialize this message to JSON bytes.
    pub fn to_payload(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("RepairMessage serialization should not fail")
    }

    /// Deserialize a message from JSON bytes.
    pub fn from_payload(payload: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(payload)
    }
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

    fn tok(v: i64) -> Token {
        Token::from_raw(v)
    }

    #[test]
    fn prepare_consistent_request_serde() {
        let msg = PrepareConsistentRequest {
            parent_id: Uuid::new_v4(),
            keyspace: "ks".into(),
            tables: vec!["t1".into()],
            ranges: vec![(tok(0), tok(100))],
            participants: vec![ep(7001), ep(7002)],
            is_forced: false,
        };
        let json = serde_json::to_string(&msg).unwrap();
        let deser: PrepareConsistentRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(deser.keyspace, "ks");
        assert_eq!(deser.participants.len(), 2);
    }

    #[test]
    fn prepare_consistent_response_serde() {
        let msg = PrepareConsistentResponse {
            parent_id: Uuid::new_v4(),
            endpoint: ep(7001),
            success: true,
        };
        let json = serde_json::to_string(&msg).unwrap();
        let deser: PrepareConsistentResponse = serde_json::from_str(&json).unwrap();
        assert!(deser.success);
    }

    #[test]
    fn finalize_propose_serde() {
        let msg = FinalizePropose {
            parent_id: Uuid::new_v4(),
        };
        let json = serde_json::to_string(&msg).unwrap();
        let _: FinalizePropose = serde_json::from_str(&json).unwrap();
    }

    #[test]
    fn finalize_promise_serde() {
        let msg = FinalizePromise {
            parent_id: Uuid::new_v4(),
            endpoint: ep(7001),
            success: true,
        };
        let json = serde_json::to_string(&msg).unwrap();
        let deser: FinalizePromise = serde_json::from_str(&json).unwrap();
        assert!(deser.success);
    }

    #[test]
    fn finalize_commit_serde() {
        let msg = FinalizeCommit {
            parent_id: Uuid::new_v4(),
        };
        let json = serde_json::to_string(&msg).unwrap();
        let _: FinalizeCommit = serde_json::from_str(&json).unwrap();
    }

    #[test]
    fn fail_session_serde() {
        let msg = FailSessionMessage {
            parent_id: Uuid::new_v4(),
        };
        let json = serde_json::to_string(&msg).unwrap();
        let _: FailSessionMessage = serde_json::from_str(&json).unwrap();
    }

    #[test]
    fn status_request_response_serde() {
        let req = StatusRequest {
            parent_id: Uuid::new_v4(),
        };
        let json = serde_json::to_string(&req).unwrap();
        let _: StatusRequest = serde_json::from_str(&json).unwrap();

        let resp = StatusResponse {
            parent_id: Uuid::new_v4(),
            state: ConsistentSessionState::Committed,
        };
        let json = serde_json::to_string(&resp).unwrap();
        let deser: StatusResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(deser.state, ConsistentSessionState::Committed);
    }

    #[test]
    fn sync_request_serde() {
        let msg = SyncRequest {
            parent_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            source: ep(7001),
            target: ep(7002),
            ranges: vec![(tok(0), tok(100))],
            keyspace: "ks".into(),
            tables: vec!["t1".into()],
        };
        let json = serde_json::to_string(&msg).unwrap();
        let deser: SyncRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(deser.keyspace, "ks");
    }

    #[test]
    fn sync_response_serde() {
        let msg = SyncResponse {
            parent_id: Uuid::new_v4(),
            session_id: Uuid::new_v4(),
            success: false,
            error: Some("timeout".into()),
        };
        let json = serde_json::to_string(&msg).unwrap();
        let deser: SyncResponse = serde_json::from_str(&json).unwrap();
        assert!(!deser.success);
        assert_eq!(deser.error.as_deref(), Some("timeout"));
    }

    #[test]
    fn repair_message_envelope_round_trip() {
        let msg = RepairMessage::FinalizeCommit(FinalizeCommit {
            parent_id: Uuid::new_v4(),
        });
        let payload = msg.to_payload();
        let deser = RepairMessage::from_payload(&payload).unwrap();
        assert!(matches!(deser, RepairMessage::FinalizeCommit(_)));
    }

    #[test]
    fn consistent_session_state_display() {
        assert_eq!(ConsistentSessionState::Preparing.to_string(), "PREPARING");
        assert_eq!(ConsistentSessionState::Committed.to_string(), "COMMITTED");
        assert_eq!(ConsistentSessionState::Failed.to_string(), "FAILED");
    }

    #[test]
    fn consistent_session_state_serde() {
        let state = ConsistentSessionState::FinalizePromised;
        let json = serde_json::to_string(&state).unwrap();
        let deser: ConsistentSessionState = serde_json::from_str(&json).unwrap();
        assert_eq!(deser, ConsistentSessionState::FinalizePromised);
    }
}
