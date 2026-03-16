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

//! Per-partition Paxos state machine.
//!
//! ## Java Oracle
//!
//! `org.apache.cassandra.service.paxos.PaxosState`
//!
//! Each partition key maintains independent Paxos state.  The state tracks
//! the highest ballot promised, the highest ballot accepted with its
//! proposal, and the most recent committed ballot+value.
//!
//! ## Invariants
//!
//! 1. `promised >= accepted >= committed` by ballot order.
//! 2. A node must not promise a ballot lower than one already promised.
//! 3. A node must not accept a proposal with a ballot lower than promised.
//! 4. Once committed, the committed value is the durable truth.

use super::ballot::Ballot;

use serde::{Deserialize, Serialize};

/// A Paxos proposal — the value (mutation) being proposed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Proposal {
    /// The ballot under which this proposal was made.
    pub ballot: Ballot,
    /// Serialized mutation bytes — the CAS write to be applied.
    pub mutation: Vec<u8>,
}

/// Per-partition Paxos state.
///
/// Tracks the three phases of a Paxos round:
/// - **Promised**: highest ballot we've promised not to accept anything lower.
/// - **Accepted**: highest ballot+proposal we've accepted.
/// - **Committed**: highest ballot+proposal that was committed (durable).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaxosState {
    /// Highest ballot we have promised.
    pub promised: Ballot,
    /// Most recently accepted proposal (may be `None` if nothing accepted).
    pub accepted: Option<Proposal>,
    /// Most recently committed proposal (the durable truth).
    pub committed: Option<Proposal>,
}

/// Errors from Paxos state transitions.
#[derive(Debug, thiserror::Error)]
pub enum PaxosError {
    #[error("Ballot {proposed} rejected: already promised {promised}")]
    BallotTooOld { proposed: Ballot, promised: Ballot },

    #[error("Cannot accept ballot {proposed}: promised {promised}")]
    PromiseViolation { proposed: Ballot, promised: Ballot },
}

/// The response to a Prepare request.
#[derive(Debug, Clone)]
pub struct PrepareResponse {
    /// Whether the promise was granted.
    pub promised: bool,
    /// The current promised ballot (either the new one or existing higher one).
    pub ballot: Ballot,
    /// Any in-progress accepted proposal (must be adopted by new proposer).
    pub accepted: Option<Proposal>,
    /// The most recently committed proposal.
    pub committed: Option<Proposal>,
}

/// The response to a Propose request.
#[derive(Debug, Clone)]
pub struct ProposeResponse {
    /// Whether the proposal was accepted.
    pub accepted: bool,
    /// The current promised ballot.
    pub ballot: Ballot,
}

impl PaxosState {
    /// Create a fresh Paxos state with no history.
    pub fn new() -> Self {
        Self {
            promised: Ballot::none(),
            accepted: None,
            committed: None,
        }
    }

    /// **Phase 1b**: Handle a Prepare request.
    ///
    /// If `ballot >= self.promised`, update the promise and return success
    /// with any in-progress accepted value.
    ///
    /// If `ballot < self.promised`, reject.
    pub fn prepare(&mut self, ballot: Ballot) -> PrepareResponse {
        if ballot > self.promised {
            self.promised = ballot;
            PrepareResponse {
                promised: true,
                ballot,
                accepted: self.accepted.clone(),
                committed: self.committed.clone(),
            }
        } else if ballot == self.promised {
            // Idempotent re-prepare
            PrepareResponse {
                promised: true,
                ballot,
                accepted: self.accepted.clone(),
                committed: self.committed.clone(),
            }
        } else {
            PrepareResponse {
                promised: false,
                ballot: self.promised,
                accepted: self.accepted.clone(),
                committed: self.committed.clone(),
            }
        }
    }

    /// **Phase 2b**: Handle a Propose request.
    ///
    /// If `proposal.ballot >= self.promised`, accept the proposal.
    /// Otherwise reject.
    pub fn propose(&mut self, proposal: Proposal) -> ProposeResponse {
        if proposal.ballot >= self.promised {
            self.promised = proposal.ballot;
            self.accepted = Some(proposal);
            ProposeResponse {
                accepted: true,
                ballot: self.promised,
            }
        } else {
            ProposeResponse {
                accepted: false,
                ballot: self.promised,
            }
        }
    }

    /// **Phase 3**: Commit a decided value.
    ///
    /// The proposal is now durable.  Clear in-progress state.
    pub fn commit(&mut self, proposal: Proposal) {
        // Only advance — never go backwards
        if self
            .committed
            .as_ref()
            .is_none_or(|c| proposal.ballot > c.ballot)
        {
            self.committed = Some(proposal);
        }
        // Clear in-progress accepted state for this round
        if let Some(ref acc) = self.accepted {
            if acc.ballot <= self.committed.as_ref().map_or(Ballot::none(), |c| c.ballot) {
                self.accepted = None;
            }
        }
    }

    /// Check if there's an in-progress (accepted but not committed) round.
    pub fn has_in_progress(&self) -> bool {
        if let Some(ref accepted) = self.accepted {
            match &self.committed {
                Some(committed) => accepted.ballot > committed.ballot,
                None => true,
            }
        } else {
            false
        }
    }

    /// Check if this state needs recovery — an accepted-but-not-committed
    /// proposal exists that a new proposer must finish before starting its
    /// own round.
    ///
    /// ## Java Oracle
    ///
    /// In `StorageProxy.cas()`, the coordinator checks promise responses for
    /// in-progress proposals and finishes them.
    pub fn needs_recovery(&self) -> bool {
        self.has_in_progress()
    }

    /// Get the in-progress proposal that needs recovery, if any.
    pub fn in_progress_proposal(&self) -> Option<&Proposal> {
        if self.has_in_progress() {
            self.accepted.as_ref()
        } else {
            None
        }
    }

    /// Get the most recent committed value.
    pub fn committed_value(&self) -> Option<&Proposal> {
        self.committed.as_ref()
    }

    /// Get the highest ballot currently promised.
    pub fn promised_ballot(&self) -> Ballot {
        self.promised
    }
}

impl Default for PaxosState {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    fn node_a() -> Uuid {
        Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap()
    }

    fn node_b() -> Uuid {
        Uuid::parse_str("00000000-0000-0000-0000-000000000002").unwrap()
    }

    fn node_c() -> Uuid {
        Uuid::parse_str("00000000-0000-0000-0000-000000000003").unwrap()
    }

    fn make_proposal(ts: i64, node: Uuid, data: &[u8]) -> Proposal {
        Proposal {
            ballot: Ballot::with_timestamp(ts, node),
            mutation: data.to_vec(),
        }
    }

    #[test]
    fn fresh_state_accepts_any_prepare() {
        let mut state = PaxosState::new();
        let ballot = Ballot::with_timestamp(100, node_a());
        let resp = state.prepare(ballot);
        assert!(resp.promised);
        assert_eq!(resp.ballot, ballot);
        assert!(resp.accepted.is_none());
    }

    #[test]
    fn prepare_rejects_old_ballot() {
        let mut state = PaxosState::new();
        let b1 = Ballot::with_timestamp(200, node_a());
        let b2 = Ballot::with_timestamp(100, node_a());

        state.prepare(b1);
        let resp = state.prepare(b2);
        assert!(!resp.promised);
        assert_eq!(resp.ballot, b1); // returns the higher promised ballot
    }

    #[test]
    fn prepare_returns_accepted_proposal() {
        let mut state = PaxosState::new();
        let b1 = Ballot::with_timestamp(100, node_a());
        state.prepare(b1);
        state.propose(make_proposal(100, node_a(), b"value1"));

        let b2 = Ballot::with_timestamp(200, node_b());
        let resp = state.prepare(b2);
        assert!(resp.promised);
        assert!(resp.accepted.is_some());
        assert_eq!(resp.accepted.unwrap().mutation, b"value1");
    }

    #[test]
    fn propose_accepted_when_ballot_matches() {
        let mut state = PaxosState::new();
        let ballot = Ballot::with_timestamp(100, node_a());
        state.prepare(ballot);

        let resp = state.propose(make_proposal(100, node_a(), b"value"));
        assert!(resp.accepted);
    }

    #[test]
    fn propose_rejected_when_higher_promise_exists() {
        let mut state = PaxosState::new();
        let b1 = Ballot::with_timestamp(200, node_b());
        state.prepare(b1);

        let resp = state.propose(make_proposal(100, node_a(), b"value"));
        assert!(!resp.accepted);
        assert_eq!(resp.ballot, b1);
    }

    #[test]
    fn commit_clears_accepted() {
        let mut state = PaxosState::new();
        let ballot = Ballot::with_timestamp(100, node_a());
        state.prepare(ballot);
        state.propose(make_proposal(100, node_a(), b"value"));
        assert!(state.has_in_progress());

        state.commit(make_proposal(100, node_a(), b"value"));
        assert!(!state.has_in_progress());
        assert_eq!(state.committed_value().unwrap().mutation, b"value");
    }

    #[test]
    fn commit_does_not_go_backwards() {
        let mut state = PaxosState::new();
        state.commit(make_proposal(200, node_a(), b"new"));
        state.commit(make_proposal(100, node_a(), b"old"));
        assert_eq!(state.committed_value().unwrap().mutation, b"new");
    }

    #[test]
    fn full_paxos_round() {
        let mut state = PaxosState::new();
        let ballot = Ballot::with_timestamp(100, node_a());

        // Phase 1
        let prep = state.prepare(ballot);
        assert!(prep.promised);

        // Phase 2
        let prop = state.propose(make_proposal(100, node_a(), b"INSERT x"));
        assert!(prop.accepted);

        // Phase 3
        state.commit(make_proposal(100, node_a(), b"INSERT x"));
        assert_eq!(state.committed_value().unwrap().mutation, b"INSERT x");
    }

    #[test]
    fn contention_higher_ballot_wins() {
        let mut state = PaxosState::new();

        // Node A starts
        let b_a = Ballot::with_timestamp(100, node_a());
        state.prepare(b_a);
        state.propose(make_proposal(100, node_a(), b"A's value"));

        // Node B comes in with higher ballot
        let b_b = Ballot::with_timestamp(200, node_b());
        let prep = state.prepare(b_b);
        assert!(prep.promised);
        // B sees A's in-progress value
        assert!(prep.accepted.is_some());
        assert_eq!(prep.accepted.unwrap().mutation, b"A's value");

        // Node A's late propose is rejected
        let prop_a = state.propose(make_proposal(100, node_a(), b"A's value"));
        assert!(!prop_a.accepted);

        // Node B can propose (must adopt A's value per Paxos protocol)
        let prop_b = state.propose(make_proposal(200, node_b(), b"A's value"));
        assert!(prop_b.accepted);

        state.commit(make_proposal(200, node_b(), b"A's value"));
        assert_eq!(state.committed_value().unwrap().mutation, b"A's value");
    }

    // ─── Additional edge-case tests ───────────────────────────────────────

    #[test]
    fn needs_recovery_when_accepted_not_committed() {
        let mut state = PaxosState::new();
        assert!(!state.needs_recovery());

        let ballot = Ballot::with_timestamp(100, node_a());
        state.prepare(ballot);
        state.propose(make_proposal(100, node_a(), b"inflight"));

        assert!(state.needs_recovery());
        assert!(state.in_progress_proposal().is_some());
        assert_eq!(state.in_progress_proposal().unwrap().mutation, b"inflight");
    }

    #[test]
    fn no_recovery_needed_after_commit() {
        let mut state = PaxosState::new();
        let ballot = Ballot::with_timestamp(100, node_a());
        state.prepare(ballot);
        state.propose(make_proposal(100, node_a(), b"value"));
        state.commit(make_proposal(100, node_a(), b"value"));

        assert!(!state.needs_recovery());
        assert!(state.in_progress_proposal().is_none());
    }

    #[test]
    fn re_prepare_same_ballot_is_idempotent() {
        let mut state = PaxosState::new();
        let ballot = Ballot::with_timestamp(100, node_a());

        let r1 = state.prepare(ballot);
        let r2 = state.prepare(ballot);
        assert!(r1.promised);
        assert!(r2.promised);
        assert_eq!(r1.ballot, r2.ballot);
    }

    #[test]
    fn propose_after_commit_starts_new_round() {
        let mut state = PaxosState::new();
        // First round
        let b1 = Ballot::with_timestamp(100, node_a());
        state.prepare(b1);
        state.propose(make_proposal(100, node_a(), b"v1"));
        state.commit(make_proposal(100, node_a(), b"v1"));

        // Second round with higher ballot
        let b2 = Ballot::with_timestamp(200, node_a());
        let prep = state.prepare(b2);
        assert!(prep.promised);
        assert!(prep.accepted.is_none()); // no in-progress after commit
        assert_eq!(prep.committed.unwrap().mutation, b"v1");

        state.propose(make_proposal(200, node_a(), b"v2"));
        state.commit(make_proposal(200, node_a(), b"v2"));
        assert_eq!(state.committed_value().unwrap().mutation, b"v2");
    }

    #[test]
    fn three_node_contention_cascade() {
        let mut state = PaxosState::new();

        // Node A starts, prepares and proposes
        let b_a = Ballot::with_timestamp(100, node_a());
        state.prepare(b_a);
        state.propose(make_proposal(100, node_a(), b"A"));
        assert!(state.needs_recovery());

        // Node B supersedes, sees A's proposal
        let b_b = Ballot::with_timestamp(200, node_b());
        let prep_b = state.prepare(b_b);
        assert!(prep_b.promised);
        assert_eq!(prep_b.accepted.unwrap().mutation, b"A");

        // Node B proposes A's value (Paxos obligation), then proposes its own
        state.propose(make_proposal(200, node_b(), b"A"));

        // Node C supersedes, sees A's value accepted under B's ballot
        let b_c = Ballot::with_timestamp(300, node_c());
        let prep_c = state.prepare(b_c);
        assert!(prep_c.promised);
        assert_eq!(prep_c.accepted.unwrap().mutation, b"A");

        // C finishes the round with A's value
        state.propose(make_proposal(300, node_c(), b"A"));
        state.commit(make_proposal(300, node_c(), b"A"));
        assert_eq!(state.committed_value().unwrap().mutation, b"A");
        assert!(!state.needs_recovery());
    }

    #[test]
    fn promised_ballot_accessor() {
        let mut state = PaxosState::new();
        assert!(state.promised_ballot().is_none());

        let ballot = Ballot::with_timestamp(42, node_a());
        state.prepare(ballot);
        assert_eq!(state.promised_ballot(), ballot);
    }
}
