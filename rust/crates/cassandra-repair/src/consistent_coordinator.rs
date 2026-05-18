// Licensed under Apache License, Version 2.0.

//! Coordinator-side state machine for consistent (incremental) repair.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.repair.consistent.CoordinatorSession`
//!
//! ## Design
//!
//! The coordinator drives the consistent repair protocol through a two-phase
//! commit:
//!
//! 1. **Prepare** — ask all participants to create a local session
//! 2. **Repair** — standard Merkle tree exchange + streaming
//! 3. **Propose** — ask participants to finalize (anti-compact)
//! 4. **Commit** — mark data as repaired on all participants

use std::collections::HashSet;

use uuid::Uuid;

use cassandra_cluster_metadata::Endpoint;
use cassandra_common::Token;

use crate::messages::{
    ConsistentSessionState, FailSessionMessage, FinalizeCommit, FinalizePromise, FinalizePropose,
    PrepareConsistentRequest, PrepareConsistentResponse,
};

/// Action the coordinator should take after processing a response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CoordinatorAction {
    /// Still waiting for more responses.
    WaitForMore,
    /// All prepares received — proceed to repair phase.
    ProceedToRepair,
    /// All repairs done — proceed to finalize proposal.
    ProceedToPropose,
    /// All promises received — commit.
    Commit,
    /// Something failed — abort.
    Fail(String),
}

/// Coordinator-side session for consistent repair.
#[derive(Debug)]
pub struct CoordinatorSession {
    /// Unique parent session identifier.
    pub parent_id: Uuid,
    /// Current state.
    pub state: ConsistentSessionState,
    /// All participants.
    pub participants: Vec<Endpoint>,
    /// Keyspace being repaired.
    pub keyspace: String,
    /// Tables being repaired.
    pub tables: Vec<String>,
    /// Token ranges being repaired.
    pub ranges: Vec<(Token, Token)>,
    /// Participants that have responded to prepare.
    prepare_responses: HashSet<Endpoint>,
    /// Participants that have promised to finalize.
    promise_responses: HashSet<Endpoint>,
}

impl CoordinatorSession {
    pub fn new(
        parent_id: Uuid,
        participants: Vec<Endpoint>,
        keyspace: String,
        tables: Vec<String>,
        ranges: Vec<(Token, Token)>,
    ) -> Self {
        Self {
            parent_id,
            state: ConsistentSessionState::Preparing,
            participants,
            keyspace,
            tables,
            ranges,
            prepare_responses: HashSet::new(),
            promise_responses: HashSet::new(),
        }
    }

    /// Generate prepare messages for all participants.
    pub fn prepare(&self) -> Vec<(Endpoint, PrepareConsistentRequest)> {
        self.participants
            .iter()
            .map(|ep| {
                (
                    *ep,
                    PrepareConsistentRequest {
                        parent_id: self.parent_id,
                        keyspace: self.keyspace.clone(),
                        tables: self.tables.clone(),
                        ranges: self.ranges.clone(),
                        participants: self.participants.clone(),
                        is_forced: false,
                    },
                )
            })
            .collect()
    }

    /// Handle a prepare response from a participant.
    pub fn handle_prepare_response(
        &mut self,
        resp: PrepareConsistentResponse,
    ) -> CoordinatorAction {
        if self.state != ConsistentSessionState::Preparing {
            return CoordinatorAction::Fail(format!(
                "Unexpected prepare response in state {}",
                self.state
            ));
        }

        if !resp.success {
            self.state = ConsistentSessionState::Failed;
            return CoordinatorAction::Fail(format!(
                "Participant {} failed to prepare",
                resp.endpoint
            ));
        }

        self.prepare_responses.insert(resp.endpoint);

        if self.prepare_responses.len() == self.participants.len() {
            self.state = ConsistentSessionState::Prepared;
            CoordinatorAction::ProceedToRepair
        } else {
            CoordinatorAction::WaitForMore
        }
    }

    /// Mark the repair phase as complete and move to finalize.
    pub fn repair_complete(&mut self) {
        if self.state == ConsistentSessionState::Prepared {
            self.state = ConsistentSessionState::Repairing;
        }
        self.state = ConsistentSessionState::FinalizeProposing;
    }

    /// Generate finalize-propose messages for all participants.
    pub fn propose_finalize(&self) -> Vec<(Endpoint, FinalizePropose)> {
        self.participants
            .iter()
            .map(|ep| {
                (
                    *ep,
                    FinalizePropose {
                        parent_id: self.parent_id,
                    },
                )
            })
            .collect()
    }

    /// Handle a finalize promise from a participant.
    pub fn handle_promise(&mut self, resp: FinalizePromise) -> CoordinatorAction {
        if self.state != ConsistentSessionState::FinalizeProposing {
            return CoordinatorAction::Fail(format!("Unexpected promise in state {}", self.state));
        }

        if !resp.success {
            self.state = ConsistentSessionState::Failed;
            return CoordinatorAction::Fail(format!(
                "Participant {} failed to promise",
                resp.endpoint
            ));
        }

        self.promise_responses.insert(resp.endpoint);

        if self.promise_responses.len() == self.participants.len() {
            self.state = ConsistentSessionState::FinalizePromised;
            CoordinatorAction::Commit
        } else {
            CoordinatorAction::WaitForMore
        }
    }

    /// Generate commit messages for all participants.
    pub fn commit(&mut self) -> Vec<(Endpoint, FinalizeCommit)> {
        self.state = ConsistentSessionState::Committed;
        self.participants
            .iter()
            .map(|ep| {
                (
                    *ep,
                    FinalizeCommit {
                        parent_id: self.parent_id,
                    },
                )
            })
            .collect()
    }

    /// Generate fail messages for all participants.
    pub fn fail(&mut self) -> Vec<(Endpoint, FailSessionMessage)> {
        self.state = ConsistentSessionState::Failed;
        self.participants
            .iter()
            .map(|ep| {
                (
                    *ep,
                    FailSessionMessage {
                        parent_id: self.parent_id,
                    },
                )
            })
            .collect()
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

    fn make_session(participants: Vec<Endpoint>) -> CoordinatorSession {
        CoordinatorSession::new(
            Uuid::new_v4(),
            participants,
            "ks".into(),
            vec!["t1".into()],
            vec![(tok(0), tok(100))],
        )
    }

    #[test]
    fn full_happy_path() {
        let eps = vec![ep(7001), ep(7002)];
        let mut session = make_session(eps.clone());

        // Prepare
        let msgs = session.prepare();
        assert_eq!(msgs.len(), 2);

        let action = session.handle_prepare_response(PrepareConsistentResponse {
            parent_id: session.parent_id,
            endpoint: ep(7001),
            success: true,
        });
        assert_eq!(action, CoordinatorAction::WaitForMore);

        let action = session.handle_prepare_response(PrepareConsistentResponse {
            parent_id: session.parent_id,
            endpoint: ep(7002),
            success: true,
        });
        assert_eq!(action, CoordinatorAction::ProceedToRepair);
        assert_eq!(session.state, ConsistentSessionState::Prepared);

        // Repair complete
        session.repair_complete();
        assert_eq!(session.state, ConsistentSessionState::FinalizeProposing);

        // Propose
        let msgs = session.propose_finalize();
        assert_eq!(msgs.len(), 2);

        let action = session.handle_promise(FinalizePromise {
            parent_id: session.parent_id,
            endpoint: ep(7001),
            success: true,
        });
        assert_eq!(action, CoordinatorAction::WaitForMore);

        let action = session.handle_promise(FinalizePromise {
            parent_id: session.parent_id,
            endpoint: ep(7002),
            success: true,
        });
        assert_eq!(action, CoordinatorAction::Commit);

        // Commit
        let msgs = session.commit();
        assert_eq!(msgs.len(), 2);
        assert_eq!(session.state, ConsistentSessionState::Committed);
    }

    #[test]
    fn failure_at_prepare() {
        let mut session = make_session(vec![ep(7001), ep(7002)]);
        session.prepare();

        let action = session.handle_prepare_response(PrepareConsistentResponse {
            parent_id: session.parent_id,
            endpoint: ep(7001),
            success: false,
        });

        assert!(matches!(action, CoordinatorAction::Fail(_)));
        assert_eq!(session.state, ConsistentSessionState::Failed);
    }

    #[test]
    fn failure_at_promise() {
        let mut session = make_session(vec![ep(7001)]);
        session.prepare();

        session.handle_prepare_response(PrepareConsistentResponse {
            parent_id: session.parent_id,
            endpoint: ep(7001),
            success: true,
        });
        session.repair_complete();

        let action = session.handle_promise(FinalizePromise {
            parent_id: session.parent_id,
            endpoint: ep(7001),
            success: false,
        });

        assert!(matches!(action, CoordinatorAction::Fail(_)));
        assert_eq!(session.state, ConsistentSessionState::Failed);
    }

    #[test]
    fn single_participant() {
        let mut session = make_session(vec![ep(7001)]);
        session.prepare();

        let action = session.handle_prepare_response(PrepareConsistentResponse {
            parent_id: session.parent_id,
            endpoint: ep(7001),
            success: true,
        });
        assert_eq!(action, CoordinatorAction::ProceedToRepair);

        session.repair_complete();

        let action = session.handle_promise(FinalizePromise {
            parent_id: session.parent_id,
            endpoint: ep(7001),
            success: true,
        });
        assert_eq!(action, CoordinatorAction::Commit);

        session.commit();
        assert_eq!(session.state, ConsistentSessionState::Committed);
    }

    #[test]
    fn fail_sends_messages_to_all() {
        let mut session = make_session(vec![ep(7001), ep(7002), ep(7003)]);
        let msgs = session.fail();
        assert_eq!(msgs.len(), 3);
        assert_eq!(session.state, ConsistentSessionState::Failed);
    }
}
