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

//! Multi-step topology operation sequences.
//!
//! Each topology change (bootstrap, decommission, move, replace) is modelled
//! as a deterministic sequence of [`Transformation`] steps that advance one
//! epoch at a time through the metadata log.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.tcm.sequences.MultiStepOperation`
//! - `org.apache.cassandra.tcm.sequences.BootstrapAndJoin`
//! - `org.apache.cassandra.tcm.sequences.UnbootstrapAndLeave`
//! - `org.apache.cassandra.tcm.sequences.Move`
//! - `org.apache.cassandra.tcm.sequences.InProgressSequences`

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use cassandra_common::token::{Token, TokenRange};

use crate::node::{Endpoint, NodeId, NodeState};
use crate::tcm::{Epoch, SequenceType, Transformation};

// ─────────────────────────────────────────────────────────────────────────────
// MultiStepOperation trait
// ─────────────────────────────────────────────────────────────────────────────

/// A topology operation that spans multiple metadata epochs.
///
/// Each call to [`advance`](MultiStepOperation::advance) produces the next
/// [`Transformation`] to commit, or `None` when the sequence is complete.
pub trait MultiStepOperation {
    /// Produce the next transformation for this operation, advancing the
    /// internal step counter. Returns `None` when the sequence is finished.
    fn advance(&mut self, epoch: Epoch) -> Option<Transformation>;

    /// Whether all steps have been emitted.
    fn is_complete(&self) -> bool;

    /// The kind of topology operation this sequence represents.
    fn kind(&self) -> SequenceType;

    /// The primary node involved in this operation.
    fn node_id(&self) -> NodeId;

    /// Token ranges affected by this operation (used for range locking).
    fn affected_ranges(&self) -> Vec<TokenRange>;
}

// ─────────────────────────────────────────────────────────────────────────────
// BootstrapAndJoin
// ─────────────────────────────────────────────────────────────────────────────

/// Bootstrap a new node into the cluster.
///
/// Steps:
/// 0. `Register` — add the node to the directory.
/// 1. `AssignTokens` — assign token ownership.
/// 2. `UpdateNodeState(Normal)` — mark the node as fully joined.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BootstrapAndJoin {
    pub node_id: NodeId,
    pub endpoint: Endpoint,
    pub tokens: Vec<Token>,
    pub dc: String,
    pub rack: String,
    pub step: u8,
}

impl BootstrapAndJoin {
    pub fn new(
        node_id: NodeId,
        endpoint: Endpoint,
        tokens: Vec<Token>,
        dc: impl Into<String>,
        rack: impl Into<String>,
    ) -> Self {
        Self {
            node_id,
            endpoint,
            tokens,
            dc: dc.into(),
            rack: rack.into(),
            step: 0,
        }
    }
}

impl MultiStepOperation for BootstrapAndJoin {
    fn advance(&mut self, _epoch: Epoch) -> Option<Transformation> {
        let current = self.step;
        if current >= 3 {
            return None;
        }
        self.step += 1;
        match current {
            0 => Some(Transformation::Register {
                node_id: self.node_id,
                endpoint: self.endpoint,
                dc: self.dc.clone(),
                rack: self.rack.clone(),
            }),
            1 => Some(Transformation::AssignTokens {
                node_id: self.node_id,
                tokens: self.tokens.clone(),
            }),
            2 => Some(Transformation::UpdateNodeState {
                node_id: self.node_id,
                state: NodeState::Normal,
            }),
            _ => None,
        }
    }

    fn is_complete(&self) -> bool {
        self.step >= 3
    }

    fn kind(&self) -> SequenceType {
        SequenceType::Bootstrap
    }

    fn node_id(&self) -> NodeId {
        self.node_id
    }

    fn affected_ranges(&self) -> Vec<TokenRange> {
        // Each token defines the end of a range whose start is the previous
        // token on the ring. For simplicity, we report point ranges.
        self.tokens
            .iter()
            .map(|t| TokenRange::new(Token::from_raw(t.value() - 1), *t))
            .collect()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// BootstrapAndReplace
// ─────────────────────────────────────────────────────────────────────────────

/// Replace a dead node with a new one.
///
/// Steps:
/// 0. `Register` — add the replacement node.
/// 1. `AssignTokens` — give the old node's tokens to the new node.
/// 2. `Unregister` — remove the old (replaced) node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BootstrapAndReplace {
    pub replacing_node: NodeId,
    pub new_node: NodeId,
    pub new_endpoint: Endpoint,
    pub tokens: Vec<Token>,
    pub dc: String,
    pub rack: String,
    pub step: u8,
}

impl BootstrapAndReplace {
    pub fn new(
        replacing_node: NodeId,
        new_node: NodeId,
        new_endpoint: Endpoint,
        tokens: Vec<Token>,
        dc: impl Into<String>,
        rack: impl Into<String>,
    ) -> Self {
        Self {
            replacing_node,
            new_node,
            new_endpoint,
            tokens,
            dc: dc.into(),
            rack: rack.into(),
            step: 0,
        }
    }
}

impl MultiStepOperation for BootstrapAndReplace {
    fn advance(&mut self, _epoch: Epoch) -> Option<Transformation> {
        let current = self.step;
        if current >= 3 {
            return None;
        }
        self.step += 1;
        match current {
            0 => Some(Transformation::Register {
                node_id: self.new_node,
                endpoint: self.new_endpoint,
                dc: self.dc.clone(),
                rack: self.rack.clone(),
            }),
            1 => Some(Transformation::AssignTokens {
                node_id: self.new_node,
                tokens: self.tokens.clone(),
            }),
            2 => Some(Transformation::Unregister {
                node_id: self.replacing_node,
            }),
            _ => None,
        }
    }

    fn is_complete(&self) -> bool {
        self.step >= 3
    }

    fn kind(&self) -> SequenceType {
        SequenceType::Replace
    }

    fn node_id(&self) -> NodeId {
        self.new_node
    }

    fn affected_ranges(&self) -> Vec<TokenRange> {
        self.tokens
            .iter()
            .map(|t| TokenRange::new(Token::from_raw(t.value() - 1), *t))
            .collect()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// UnbootstrapAndLeave
// ─────────────────────────────────────────────────────────────────────────────

/// Decommission a node from the cluster.
///
/// Steps:
/// 0. `UpdateNodeState(Leaving)` — mark the node as leaving.
/// 1. `Unregister` — remove the node from the directory.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnbootstrapAndLeave {
    pub node_id: NodeId,
    pub step: u8,
}

impl UnbootstrapAndLeave {
    pub fn new(node_id: NodeId) -> Self {
        Self { node_id, step: 0 }
    }
}

impl MultiStepOperation for UnbootstrapAndLeave {
    fn advance(&mut self, _epoch: Epoch) -> Option<Transformation> {
        let current = self.step;
        if current >= 2 {
            return None;
        }
        self.step += 1;
        match current {
            0 => Some(Transformation::UpdateNodeState {
                node_id: self.node_id,
                state: NodeState::Leaving,
            }),
            1 => Some(Transformation::Unregister {
                node_id: self.node_id,
            }),
            _ => None,
        }
    }

    fn is_complete(&self) -> bool {
        self.step >= 2
    }

    fn kind(&self) -> SequenceType {
        SequenceType::Decommission
    }

    fn node_id(&self) -> NodeId {
        self.node_id
    }

    fn affected_ranges(&self) -> Vec<TokenRange> {
        // Affected ranges depend on the node's current tokens, which are not
        // stored in this struct. The caller should compute them from the ring.
        Vec::new()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Move
// ─────────────────────────────────────────────────────────────────────────────

/// Move a node to new token positions on the ring.
///
/// Steps:
/// 0. `UpdateNodeState(Moving)` — mark the node as moving.
/// 1. `AssignTokens` — assign the new token positions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Move {
    pub node_id: NodeId,
    pub new_tokens: Vec<Token>,
    pub step: u8,
}

impl Move {
    pub fn new(node_id: NodeId, new_tokens: Vec<Token>) -> Self {
        Self {
            node_id,
            new_tokens,
            step: 0,
        }
    }
}

impl MultiStepOperation for Move {
    fn advance(&mut self, _epoch: Epoch) -> Option<Transformation> {
        let current = self.step;
        if current >= 2 {
            return None;
        }
        self.step += 1;
        match current {
            0 => Some(Transformation::UpdateNodeState {
                node_id: self.node_id,
                state: NodeState::Moving,
            }),
            1 => Some(Transformation::AssignTokens {
                node_id: self.node_id,
                tokens: self.new_tokens.clone(),
            }),
            _ => None,
        }
    }

    fn is_complete(&self) -> bool {
        self.step >= 2
    }

    fn kind(&self) -> SequenceType {
        SequenceType::Move
    }

    fn node_id(&self) -> NodeId {
        self.node_id
    }

    fn affected_ranges(&self) -> Vec<TokenRange> {
        self.new_tokens
            .iter()
            .map(|t| TokenRange::new(Token::from_raw(t.value() - 1), *t))
            .collect()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// SequenceState
// ─────────────────────────────────────────────────────────────────────────────

/// Lifecycle state of a tracked sequence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SequenceState {
    /// Registered but not yet advanced.
    NotStarted,
    /// In progress at the given step number.
    InProgress(u8),
    /// All steps have been executed.
    Complete,
}

// ─────────────────────────────────────────────────────────────────────────────
// SequenceEntry
// ─────────────────────────────────────────────────────────────────────────────

/// A lightweight summary of a registered multi-step operation, stored
/// without the full operation object so that `InProgressSequences`
/// remains `Send + Sync` without trait-object complexity.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SequenceEntry {
    /// The primary node involved in this operation.
    pub node_id: NodeId,
    /// The type of topology operation.
    pub kind: SequenceType,
    /// Current lifecycle state.
    pub state: SequenceState,
    /// Token ranges affected by this operation.
    pub affected_ranges: Vec<TokenRange>,
}

// ─────────────────────────────────────────────────────────────────────────────
// InProgressSequences
// ─────────────────────────────────────────────────────────────────────────────

/// Registry of all in-progress topology sequences.
///
/// Tracks `(Uuid, SequenceEntry)` pairs so that the commit protocol can
/// query which operations are active and what ranges are affected.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct InProgressSequences {
    sequences: HashMap<Uuid, SequenceEntry>,
}

/// Errors from sequence operations.
#[derive(Debug, thiserror::Error)]
pub enum SequenceError {
    #[error("Sequence {0} already registered")]
    AlreadyRegistered(Uuid),

    #[error("Sequence {0} not found")]
    NotFound(Uuid),

    #[error("Sequence {0} is already complete")]
    AlreadyComplete(Uuid),
}

impl InProgressSequences {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a new in-progress operation.
    pub fn register(
        &mut self,
        id: Uuid,
        node_id: NodeId,
        kind: SequenceType,
        affected_ranges: Vec<TokenRange>,
    ) -> Result<(), SequenceError> {
        if self.sequences.contains_key(&id) {
            return Err(SequenceError::AlreadyRegistered(id));
        }
        self.sequences.insert(
            id,
            SequenceEntry {
                node_id,
                kind,
                state: SequenceState::NotStarted,
                affected_ranges,
            },
        );
        Ok(())
    }

    /// Advance the sequence to the given step. Returns the new state.
    pub fn advance(&mut self, id: Uuid, step: u8) -> Result<SequenceState, SequenceError> {
        let entry = self
            .sequences
            .get_mut(&id)
            .ok_or(SequenceError::NotFound(id))?;
        if entry.state == SequenceState::Complete {
            return Err(SequenceError::AlreadyComplete(id));
        }
        entry.state = SequenceState::InProgress(step);
        Ok(entry.state.clone())
    }

    /// Mark a sequence as complete.
    pub fn complete(&mut self, id: Uuid) -> Result<(), SequenceError> {
        let entry = self
            .sequences
            .get_mut(&id)
            .ok_or(SequenceError::NotFound(id))?;
        entry.state = SequenceState::Complete;
        Ok(())
    }

    /// Get the current state of a sequence.
    pub fn get_state(&self, id: &Uuid) -> Option<&SequenceState> {
        self.sequences.get(id).map(|e| &e.state)
    }

    /// Get the full entry for a sequence.
    pub fn get(&self, id: &Uuid) -> Option<&SequenceEntry> {
        self.sequences.get(id)
    }

    /// Number of sequences that are not yet complete.
    pub fn active_count(&self) -> usize {
        self.sequences
            .values()
            .filter(|e| e.state != SequenceState::Complete)
            .count()
    }

    /// Whether there are no registered sequences at all.
    pub fn is_empty(&self) -> bool {
        self.sequences.is_empty()
    }

    /// Remove completed sequences from the registry.
    pub fn purge_completed(&mut self) {
        self.sequences
            .retain(|_, e| e.state != SequenceState::Complete);
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// AffectedRanges
// ─────────────────────────────────────────────────────────────────────────────

/// Per-keyspace collection of token ranges affected by in-progress operations.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AffectedRanges {
    ranges: HashMap<String, Vec<TokenRange>>,
}

impl AffectedRanges {
    /// Create an empty collection.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add affected ranges for a keyspace.
    pub fn add(&mut self, keyspace: impl Into<String>, ranges: Vec<TokenRange>) {
        self.ranges
            .entry(keyspace.into())
            .or_default()
            .extend(ranges);
    }

    /// Get affected ranges for a keyspace.
    pub fn get(&self, keyspace: &str) -> Option<&Vec<TokenRange>> {
        self.ranges.get(keyspace)
    }

    /// Check whether any affected range for the given keyspace intersects
    /// with the provided range.
    pub fn intersects(&self, keyspace: &str, range: &TokenRange) -> bool {
        if let Some(ranges) = self.ranges.get(keyspace) {
            for r in ranges {
                if ranges_overlap(r, range) {
                    return true;
                }
            }
        }
        false
    }

    /// Iterate over all keyspaces that have affected ranges.
    pub fn all_keyspaces(&self) -> impl Iterator<Item = &str> {
        self.ranges.keys().map(|s| s.as_str())
    }
}

/// Check if two non-wrapping token ranges overlap.
fn ranges_overlap(a: &TokenRange, b: &TokenRange) -> bool {
    if a.wraps_around() || b.wraps_around() {
        // Conservative: assume overlap for wrapping ranges.
        return true;
    }
    a.start < b.end && b.start < a.end
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

    fn node_id(n: u128) -> NodeId {
        NodeId::from_uuid(Uuid::from_u128(n))
    }

    fn ep(port: u16) -> Endpoint {
        Endpoint::new(SocketAddr::V4(SocketAddrV4::new(
            Ipv4Addr::new(127, 0, 0, 1),
            port,
        )))
    }

    // ── BootstrapAndJoin ────────────────────────────────────────────────

    #[test]
    fn bootstrap_and_join_full_sequence() {
        let tokens = vec![Token::from_raw(100), Token::from_raw(200)];
        let mut op = BootstrapAndJoin::new(node_id(1), ep(7001), tokens, "dc1", "rack1");

        assert!(!op.is_complete());
        assert_eq!(op.kind(), SequenceType::Bootstrap);
        assert_eq!(op.node_id(), node_id(1));

        // Step 0 -> Register
        let t0 = op.advance(Epoch::FIRST).unwrap();
        assert!(matches!(t0, Transformation::Register { .. }));

        // Step 1 -> AssignTokens
        let t1 = op.advance(Epoch(2)).unwrap();
        assert!(matches!(t1, Transformation::AssignTokens { .. }));

        // Step 2 -> UpdateNodeState(Normal)
        let t2 = op.advance(Epoch(3)).unwrap();
        assert!(matches!(
            t2,
            Transformation::UpdateNodeState {
                state: NodeState::Normal,
                ..
            }
        ));

        // Step 3+ -> None, complete
        assert!(op.advance(Epoch(4)).is_none());
        assert!(op.is_complete());
    }

    #[test]
    fn bootstrap_and_join_affected_ranges() {
        let tokens = vec![Token::from_raw(100), Token::from_raw(200)];
        let op = BootstrapAndJoin::new(node_id(1), ep(7001), tokens, "dc1", "rack1");

        let ranges = op.affected_ranges();
        assert_eq!(ranges.len(), 2);
        assert_eq!(ranges[0].end, Token::from_raw(100));
        assert_eq!(ranges[1].end, Token::from_raw(200));
    }

    // ── BootstrapAndReplace ─────────────────────────────────────────────

    #[test]
    fn bootstrap_and_replace_full_sequence() {
        let tokens = vec![Token::from_raw(50)];
        let mut op =
            BootstrapAndReplace::new(node_id(1), node_id(2), ep(7002), tokens, "dc1", "rack1");

        assert_eq!(op.kind(), SequenceType::Replace);
        assert_eq!(op.node_id(), node_id(2));

        // Step 0 -> Register new node
        let t0 = op.advance(Epoch::FIRST).unwrap();
        let expected_new = node_id(2);
        let expected_old = node_id(1);
        assert!(matches!(t0, Transformation::Register { node_id: nid, .. } if nid == expected_new));

        // Step 1 -> AssignTokens to new node
        let t1 = op.advance(Epoch(2)).unwrap();
        assert!(
            matches!(t1, Transformation::AssignTokens { node_id: nid, .. } if nid == expected_new)
        );

        // Step 2 -> Unregister old node
        let t2 = op.advance(Epoch(3)).unwrap();
        assert!(matches!(t2, Transformation::Unregister { node_id: nid } if nid == expected_old));

        assert!(op.advance(Epoch(4)).is_none());
        assert!(op.is_complete());
    }

    // ── UnbootstrapAndLeave ─────────────────────────────────────────────

    #[test]
    fn unbootstrap_and_leave_full_sequence() {
        let mut op = UnbootstrapAndLeave::new(node_id(1));

        assert_eq!(op.kind(), SequenceType::Decommission);
        assert!(!op.is_complete());

        // Step 0 -> Leaving
        let t0 = op.advance(Epoch::FIRST).unwrap();
        assert!(matches!(
            t0,
            Transformation::UpdateNodeState {
                state: NodeState::Leaving,
                ..
            }
        ));

        // Step 1 -> Unregister
        let t1 = op.advance(Epoch(2)).unwrap();
        assert!(matches!(t1, Transformation::Unregister { .. }));

        assert!(op.advance(Epoch(3)).is_none());
        assert!(op.is_complete());
    }

    // ── Move ────────────────────────────────────────────────────────────

    #[test]
    fn move_full_sequence() {
        let new_tokens = vec![Token::from_raw(500)];
        let mut op = Move::new(node_id(1), new_tokens);

        assert_eq!(op.kind(), SequenceType::Move);
        assert!(!op.is_complete());

        // Step 0 -> Moving
        let t0 = op.advance(Epoch::FIRST).unwrap();
        assert!(matches!(
            t0,
            Transformation::UpdateNodeState {
                state: NodeState::Moving,
                ..
            }
        ));

        // Step 1 -> AssignTokens(new)
        let t1 = op.advance(Epoch(2)).unwrap();
        match t1 {
            Transformation::AssignTokens { tokens, .. } => {
                assert_eq!(tokens, vec![Token::from_raw(500)]);
            }
            other => panic!("expected AssignTokens, got {other:?}"),
        }

        assert!(op.advance(Epoch(3)).is_none());
        assert!(op.is_complete());
    }

    // ── InProgressSequences ─────────────────────────────────────────────

    #[test]
    fn register_advance_complete() {
        let mut seqs = InProgressSequences::new();
        let id = Uuid::from_u128(42);
        let ranges = vec![TokenRange::new(Token::from_raw(0), Token::from_raw(100))];

        seqs.register(id, node_id(1), SequenceType::Bootstrap, ranges)
            .unwrap();

        assert_eq!(seqs.active_count(), 1);
        assert!(!seqs.is_empty());
        assert_eq!(seqs.get_state(&id), Some(&SequenceState::NotStarted));

        // Advance to step 1
        let state = seqs.advance(id, 1).unwrap();
        assert_eq!(state, SequenceState::InProgress(1));

        // Complete
        seqs.complete(id).unwrap();
        assert_eq!(seqs.get_state(&id), Some(&SequenceState::Complete));
        assert_eq!(seqs.active_count(), 0);
    }

    #[test]
    fn register_duplicate_fails() {
        let mut seqs = InProgressSequences::new();
        let id = Uuid::from_u128(1);

        seqs.register(id, node_id(1), SequenceType::Decommission, vec![])
            .unwrap();
        let result = seqs.register(id, node_id(1), SequenceType::Decommission, vec![]);
        assert!(result.is_err());
    }

    #[test]
    fn advance_unknown_fails() {
        let mut seqs = InProgressSequences::new();
        let result = seqs.advance(Uuid::from_u128(99), 1);
        assert!(result.is_err());
    }

    #[test]
    fn purge_completed_removes_done_sequences() {
        let mut seqs = InProgressSequences::new();
        let id1 = Uuid::from_u128(1);
        let id2 = Uuid::from_u128(2);

        seqs.register(id1, node_id(1), SequenceType::Bootstrap, vec![])
            .unwrap();
        seqs.register(id2, node_id(2), SequenceType::Move, vec![])
            .unwrap();

        seqs.complete(id1).unwrap();
        seqs.purge_completed();

        assert!(seqs.get(&id1).is_none());
        assert!(seqs.get(&id2).is_some());
    }

    // ── AffectedRanges ──────────────────────────────────────────────────

    #[test]
    fn affected_ranges_add_and_get() {
        let mut ar = AffectedRanges::new();
        let r = vec![TokenRange::new(Token::from_raw(0), Token::from_raw(100))];

        ar.add("ks1", r.clone());

        assert_eq!(ar.get("ks1").unwrap().len(), 1);
        assert!(ar.get("ks2").is_none());
    }

    #[test]
    fn affected_ranges_intersects() {
        let mut ar = AffectedRanges::new();
        ar.add(
            "ks1",
            vec![TokenRange::new(Token::from_raw(10), Token::from_raw(50))],
        );

        // Overlapping range
        let overlap = TokenRange::new(Token::from_raw(30), Token::from_raw(60));
        assert!(ar.intersects("ks1", &overlap));

        // Non-overlapping range
        let disjoint = TokenRange::new(Token::from_raw(60), Token::from_raw(80));
        assert!(!ar.intersects("ks1", &disjoint));

        // Unknown keyspace never intersects
        assert!(!ar.intersects("ks2", &overlap));
    }

    #[test]
    fn affected_ranges_all_keyspaces() {
        let mut ar = AffectedRanges::new();
        ar.add("ks1", vec![]);
        ar.add("ks2", vec![]);

        let mut ks: Vec<&str> = ar.all_keyspaces().collect();
        ks.sort();
        assert_eq!(ks, vec!["ks1", "ks2"]);
    }
}
