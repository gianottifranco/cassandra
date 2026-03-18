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

//! TCM migration: gossip-to-CMS migration coordinator.
//!
//! Handles the transition from gossip-based cluster metadata to the
//! Cluster Metadata Service (CMS). Includes leader election, gossip
//! event conversion, and migration plan execution.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.tcm.migration.Election`
//! - `org.apache.cassandra.tcm.migration.GossipCMSListener`
//! - `org.apache.cassandra.tcm.migration.GossipProcessor`

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use cassandra_common::token::Token;

use crate::node::{Endpoint, NodeId, NodeState};
use crate::tcm::{Epoch, Transformation};

// ─────────────────────────────────────────────────────────────────────────────
// MigrationState
// ─────────────────────────────────────────────────────────────────────────────

/// Current state of the gossip-to-CMS migration process.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MigrationState {
    /// Migration has not been initiated.
    NotStarted,
    /// A leader election is currently in progress.
    ElectionInProgress,
    /// Migration is actively running under the elected leader.
    Migrating {
        /// The node elected to coordinate the migration.
        leader: NodeId,
        /// Progress as a fraction in `[0.0, 1.0]`.
        progress: f64,
    },
    /// Migration completed successfully.
    Complete,
    /// Migration failed with the given reason.
    Failed(String),
}

// ─────────────────────────────────────────────────────────────────────────────
// MigrationError
// ─────────────────────────────────────────────────────────────────────────────

/// Errors that can occur during the migration process.
#[derive(Debug, thiserror::Error)]
pub enum MigrationError {
    #[error("Election failed: {0}")]
    ElectionFailed(String),

    #[error("No leader has been elected")]
    NoLeader,

    #[error("Migration is already in progress")]
    AlreadyMigrating,

    #[error("Invalid state: {0}")]
    InvalidState(String),

    #[error("Transformation failed: {0}")]
    TransformationFailed(String),
}

// ─────────────────────────────────────────────────────────────────────────────
// Election
// ─────────────────────────────────────────────────────────────────────────────

/// Leader election for choosing the migration coordinator.
///
/// Candidates register, nodes cast votes, and the candidate with a
/// strict majority wins.
#[derive(Debug, Clone)]
pub struct Election {
    /// The set of candidate nodes.
    candidates: Vec<NodeId>,
    /// The elected winner, if any.
    winner: Option<NodeId>,
    /// Map of voter → candidate they voted for.
    votes: HashMap<NodeId, NodeId>,
}

impl Election {
    /// Create a new election with the given candidates.
    pub fn new(candidates: Vec<NodeId>) -> Self {
        Self {
            candidates,
            winner: None,
            votes: HashMap::new(),
        }
    }

    /// Cast a vote. The candidate must be in the candidate list.
    pub fn vote(&mut self, voter: NodeId, candidate: NodeId) -> Result<(), MigrationError> {
        if !self.candidates.contains(&candidate) {
            return Err(MigrationError::ElectionFailed(format!(
                "candidate {candidate} is not in the candidate list"
            )));
        }
        self.votes.insert(voter, candidate);
        Ok(())
    }

    /// Tally the votes. A candidate needs a strict majority to win.
    /// Sets and returns the winner if one exists.
    pub fn tally(&mut self) -> Option<NodeId> {
        let total = self.votes.len();
        let majority = total / 2 + 1;

        let mut counts: HashMap<NodeId, usize> = HashMap::new();
        for candidate in self.votes.values() {
            *counts.entry(*candidate).or_insert(0) += 1;
        }

        let winner = counts
            .into_iter()
            .find(|(_, count)| *count >= majority)
            .map(|(node, _)| node);

        self.winner = winner;
        winner
    }

    /// Returns the elected winner, if any.
    pub fn winner(&self) -> Option<NodeId> {
        self.winner
    }

    /// Returns `true` if the election has been decided.
    pub fn is_decided(&self) -> bool {
        self.winner.is_some()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// GossipEvent
// ─────────────────────────────────────────────────────────────────────────────

/// A gossip-observed node state, used during migration to convert
/// gossip information into CMS transformations.
#[derive(Debug, Clone)]
pub struct GossipEvent {
    /// The endpoint that sent the gossip.
    pub source: Endpoint,
    /// The node's unique identifier.
    pub node_id: NodeId,
    /// Datacenter reported via gossip.
    pub dc: String,
    /// Rack reported via gossip.
    pub rack: String,
    /// Tokens reported via gossip.
    pub tokens: Vec<Token>,
    /// Node lifecycle state reported via gossip.
    pub state: NodeState,
}

// ─────────────────────────────────────────────────────────────────────────────
// GossipCmsListener
// ─────────────────────────────────────────────────────────────────────────────

/// Listens for gossip events and converts them into CMS transformations
/// during the migration period.
#[derive(Debug)]
pub struct GossipCmsListener {
    /// Transformations accumulated from gossip events.
    pending_transforms: Vec<Transformation>,
}

impl GossipCmsListener {
    /// Create a new listener with no pending transformations.
    pub fn new() -> Self {
        Self {
            pending_transforms: Vec::new(),
        }
    }

    /// Process a gossip event and optionally produce a transformation.
    ///
    /// Converts the gossip event into the appropriate `Transformation`
    /// variant based on the node's reported state:
    /// - `Joining` → `Register`
    /// - `Normal` with tokens → `AssignTokens`
    /// - Other states → `UpdateNodeState`
    pub fn on_gossip_event(&mut self, event: GossipEvent) -> Option<Transformation> {
        let transform = match event.state {
            NodeState::Joining => Transformation::Register {
                node_id: event.node_id,
                endpoint: event.source,
                dc: event.dc,
                rack: event.rack,
            },
            NodeState::Normal if !event.tokens.is_empty() => Transformation::AssignTokens {
                node_id: event.node_id,
                tokens: event.tokens,
            },
            _ => Transformation::UpdateNodeState {
                node_id: event.node_id,
                state: event.state,
            },
        };

        self.pending_transforms.push(transform.clone());
        Some(transform)
    }

    /// Drain all pending transformations, leaving the internal buffer empty.
    pub fn drain_pending(&mut self) -> Vec<Transformation> {
        std::mem::take(&mut self.pending_transforms)
    }
}

impl Default for GossipCmsListener {
    fn default() -> Self {
        Self::new()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// MigrationPlan
// ─────────────────────────────────────────────────────────────────────────────

/// A plan describing the set of transformations needed to migrate
/// from one epoch to another.
#[derive(Debug, Clone)]
pub struct MigrationPlan {
    /// The epoch we are migrating from.
    pub source_epoch: Epoch,
    /// The target epoch after migration completes.
    pub target_epoch: Epoch,
    /// Ordered list of transformations to apply.
    pub transformations: Vec<Transformation>,
    /// Estimated total number of entries (for progress tracking).
    pub estimated_entries: usize,
}

impl MigrationPlan {
    /// Create a new empty migration plan.
    pub fn new(source_epoch: Epoch, target_epoch: Epoch) -> Self {
        Self {
            source_epoch,
            target_epoch,
            transformations: Vec::new(),
            estimated_entries: 0,
        }
    }

    /// Add a transformation to the plan.
    pub fn add_transformation(&mut self, t: Transformation) {
        self.transformations.push(t);
    }

    /// Current progress as a fraction in `[0.0, 1.0]`.
    ///
    /// Returns 0.0 if `estimated_entries` is zero.
    pub fn progress(&self) -> f64 {
        if self.estimated_entries == 0 {
            return 0.0;
        }
        self.transformations.len() as f64 / self.estimated_entries as f64
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// MigrationCoordinator
// ─────────────────────────────────────────────────────────────────────────────

/// Coordinates the full gossip-to-CMS migration lifecycle.
///
/// Manages the state machine from `NotStarted` through leader election,
/// active migration, and final completion or failure.
#[derive(Debug)]
pub struct MigrationCoordinator {
    /// Current migration state.
    state: MigrationState,
    /// The local node's identity.
    local_node: NodeId,
    /// Active election, if any.
    election: Option<Election>,
    /// Active migration plan, if any.
    plan: Option<MigrationPlan>,
}

impl MigrationCoordinator {
    /// Create a new coordinator for the given local node.
    pub fn new(local_node: NodeId) -> Self {
        Self {
            state: MigrationState::NotStarted,
            local_node,
            election: None,
            plan: None,
        }
    }

    /// Start a leader election with the given candidates.
    ///
    /// The coordinator must be in the `NotStarted` state.
    pub fn start_election(&mut self, candidates: Vec<NodeId>) -> Result<(), MigrationError> {
        if !matches!(self.state, MigrationState::NotStarted) {
            return Err(MigrationError::InvalidState(
                "election can only start from NotStarted state".into(),
            ));
        }
        self.election = Some(Election::new(candidates));
        self.state = MigrationState::ElectionInProgress;
        Ok(())
    }

    /// Cast a vote in the current election.
    pub fn cast_vote(&mut self, voter: NodeId, candidate: NodeId) -> Result<(), MigrationError> {
        let election = self
            .election
            .as_mut()
            .ok_or_else(|| MigrationError::InvalidState("no election in progress".into()))?;
        election.vote(voter, candidate)
    }

    /// Finalize the election by tallying votes.
    ///
    /// If a winner is found and it is the local node, the state transitions
    /// to `Migrating`. Returns the winner's `NodeId`.
    pub fn finalize_election(&mut self) -> Result<NodeId, MigrationError> {
        let election = self
            .election
            .as_mut()
            .ok_or_else(|| MigrationError::InvalidState("no election in progress".into()))?;

        let winner = election
            .tally()
            .ok_or_else(|| MigrationError::ElectionFailed("no majority reached".into()))?;

        if winner == self.local_node {
            self.state = MigrationState::Migrating {
                leader: winner,
                progress: 0.0,
            };
        } else {
            self.state = MigrationState::Migrating {
                leader: winner,
                progress: 0.0,
            };
        }

        Ok(winner)
    }

    /// Returns a reference to the current migration state.
    pub fn state(&self) -> &MigrationState {
        &self.state
    }

    /// Mark the migration as complete.
    pub fn complete(&mut self) {
        self.state = MigrationState::Complete;
    }

    /// Mark the migration as failed with the given reason.
    pub fn fail(&mut self, reason: String) {
        self.state = MigrationState::Failed(reason);
    }

    /// Returns a reference to the current migration plan, if any.
    pub fn plan(&self) -> Option<&MigrationPlan> {
        self.plan.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    fn node_id(n: u128) -> NodeId {
        NodeId::from_uuid(Uuid::from_u128(n))
    }

    fn ep(port: u16) -> Endpoint {
        Endpoint::new(std::net::SocketAddr::new(
            std::net::IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1)),
            port,
        ))
    }

    // ── Election Tests ──────────────────────────────────────────────────

    #[test]
    fn election_majority_vote() {
        let c1 = node_id(1);
        let c2 = node_id(2);
        let c3 = node_id(3);

        let mut election = Election::new(vec![c1, c2, c3]);

        // Three voters, two vote for c1 → c1 wins with majority
        election.vote(node_id(10), c1).unwrap();
        election.vote(node_id(11), c1).unwrap();
        election.vote(node_id(12), c2).unwrap();

        let winner = election.tally();
        assert_eq!(winner, Some(c1));
        assert!(election.is_decided());
        assert_eq!(election.winner(), Some(c1));
    }

    #[test]
    fn election_invalid_candidate() {
        let mut election = Election::new(vec![node_id(1), node_id(2)]);
        let result = election.vote(node_id(10), node_id(99));
        assert!(result.is_err());
    }

    #[test]
    fn election_no_majority() {
        let c1 = node_id(1);
        let c2 = node_id(2);
        let c3 = node_id(3);

        let mut election = Election::new(vec![c1, c2, c3]);

        // Three voters, one vote each → no majority
        election.vote(node_id(10), c1).unwrap();
        election.vote(node_id(11), c2).unwrap();
        election.vote(node_id(12), c3).unwrap();

        let winner = election.tally();
        assert_eq!(winner, None);
        assert!(!election.is_decided());
    }

    // ── GossipCmsListener Tests ─────────────────────────────────────────

    #[test]
    fn gossip_event_joining_produces_register() {
        let mut listener = GossipCmsListener::new();

        let event = GossipEvent {
            source: ep(7001),
            node_id: node_id(1),
            dc: "dc1".into(),
            rack: "rack1".into(),
            tokens: vec![Token::from_raw(42)],
            state: NodeState::Joining,
        };

        let transform = listener.on_gossip_event(event).unwrap();
        match transform {
            Transformation::Register {
                node_id: nid,
                endpoint,
                dc,
                rack,
            } => {
                assert_eq!(nid, node_id(1));
                assert_eq!(endpoint, ep(7001));
                assert_eq!(dc, "dc1");
                assert_eq!(rack, "rack1");
            }
            _ => panic!("expected Register transformation"),
        }

        let pending = listener.drain_pending();
        assert_eq!(pending.len(), 1);
    }

    #[test]
    fn gossip_event_normal_with_tokens_produces_assign() {
        let mut listener = GossipCmsListener::new();

        let event = GossipEvent {
            source: ep(7001),
            node_id: node_id(1),
            dc: "dc1".into(),
            rack: "rack1".into(),
            tokens: vec![Token::from_raw(100), Token::from_raw(200)],
            state: NodeState::Normal,
        };

        let transform = listener.on_gossip_event(event).unwrap();
        match transform {
            Transformation::AssignTokens {
                node_id: nid,
                tokens,
            } => {
                assert_eq!(nid, node_id(1));
                assert_eq!(tokens.len(), 2);
            }
            _ => panic!("expected AssignTokens transformation"),
        }
    }

    #[test]
    fn gossip_event_leaving_produces_update_state() {
        let mut listener = GossipCmsListener::new();

        let event = GossipEvent {
            source: ep(7001),
            node_id: node_id(1),
            dc: "dc1".into(),
            rack: "rack1".into(),
            tokens: vec![],
            state: NodeState::Leaving,
        };

        let transform = listener.on_gossip_event(event).unwrap();
        match transform {
            Transformation::UpdateNodeState {
                node_id: nid,
                state,
            } => {
                assert_eq!(nid, node_id(1));
                assert_eq!(state, NodeState::Leaving);
            }
            _ => panic!("expected UpdateNodeState transformation"),
        }
    }

    // ── MigrationPlan Tests ─────────────────────────────────────────────

    #[test]
    fn migration_plan_progress() {
        let mut plan = MigrationPlan::new(Epoch(0), Epoch(10));
        plan.estimated_entries = 4;

        assert_eq!(plan.progress(), 0.0);

        plan.add_transformation(Transformation::ForceSnapshot);
        plan.add_transformation(Transformation::ForceSnapshot);

        assert!((plan.progress() - 0.5).abs() < f64::EPSILON);
    }

    #[test]
    fn migration_plan_progress_zero_estimated() {
        let plan = MigrationPlan::new(Epoch(0), Epoch(1));
        assert_eq!(plan.progress(), 0.0);
    }

    // ── MigrationCoordinator State Machine Tests ────────────────────────

    #[test]
    fn coordinator_full_lifecycle() {
        let local = node_id(1);
        let mut coord = MigrationCoordinator::new(local);

        // Starts in NotStarted
        assert!(matches!(coord.state(), MigrationState::NotStarted));

        // Start election
        coord
            .start_election(vec![local, node_id(2), node_id(3)])
            .unwrap();
        assert!(matches!(coord.state(), MigrationState::ElectionInProgress));

        // Cast votes: local node wins majority
        coord.cast_vote(node_id(10), local).unwrap();
        coord.cast_vote(node_id(11), local).unwrap();
        coord.cast_vote(node_id(12), node_id(2)).unwrap();

        // Finalize election
        let winner = coord.finalize_election().unwrap();
        assert_eq!(winner, local);
        assert!(matches!(
            coord.state(),
            MigrationState::Migrating { leader, .. } if *leader == local
        ));

        // Complete migration
        coord.complete();
        assert!(matches!(coord.state(), MigrationState::Complete));
    }

    #[test]
    fn coordinator_fail_transition() {
        let local = node_id(1);
        let mut coord = MigrationCoordinator::new(local);

        coord.start_election(vec![local]).unwrap();
        coord.cast_vote(local, local).unwrap();
        coord.finalize_election().unwrap();

        coord.fail("disk full".into());
        match coord.state() {
            MigrationState::Failed(reason) => assert_eq!(reason, "disk full"),
            _ => panic!("expected Failed state"),
        }
    }

    #[test]
    fn coordinator_start_election_wrong_state() {
        let local = node_id(1);
        let mut coord = MigrationCoordinator::new(local);

        coord.start_election(vec![local]).unwrap();

        // Starting another election while one is in progress should fail
        let result = coord.start_election(vec![local]);
        assert!(result.is_err());
    }

    #[test]
    fn coordinator_finalize_no_election() {
        let local = node_id(1);
        let mut coord = MigrationCoordinator::new(local);

        let result = coord.finalize_election();
        assert!(result.is_err());
    }
}
