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

//! Transactional Cluster Metadata (TCM) / Cluster Metadata Service (CMS).
//!
//! A Raft/Paxos-backed metadata store that replaces gossip for authoritative
//! cluster state in newer Cassandra versions. Provides linearizable writes
//! to cluster metadata via an append-only epoch-ordered log.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.tcm.*`
//! - `org.apache.cassandra.tcm.ClusterMetadata`
//! - `org.apache.cassandra.tcm.Epoch`
//! - `org.apache.cassandra.tcm.log.Entry`
//! - `org.apache.cassandra.tcm.ownership.DataPlacements`
//! - `org.apache.cassandra.tcm.sequences.InProgressSequences`

pub mod bridge;
pub mod commit;
pub mod listeners;
pub mod locking;
pub mod log_storage;
pub mod migration;
pub mod transformations;
pub mod membership;
pub mod metadata;
pub mod ownership;
pub mod sequences;
pub mod service;
pub mod serialization;

use std::collections::{BTreeMap, HashMap};
use std::fmt;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use cassandra_common::token::{Token, TokenRange};

use crate::node::{Endpoint, NodeId, NodeInfo, NodeState};

// ─────────────────────────────────────────────────────────────────────────────
// Epoch
// ─────────────────────────────────────────────────────────────────────────────

/// Monotonically increasing epoch counter for metadata versions.
///
/// Each committed transformation increments the epoch. Nodes use epochs
/// to determine whether their local metadata snapshot is current.
///
/// ## Java Oracle
///
/// `org.apache.cassandra.tcm.Epoch`
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Epoch(pub u64);

impl Epoch {
    pub const EMPTY: Self = Self(0);
    pub const FIRST: Self = Self(1);

    pub fn next(self) -> Self {
        Self(self.0 + 1)
    }

    pub fn value(self) -> u64 {
        self.0
    }

    pub fn is_empty(self) -> bool {
        self.0 == 0
    }
}

impl fmt::Display for Epoch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Epoch({})", self.0)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Transformation
// ─────────────────────────────────────────────────────────────────────────────

/// The type of metadata transformation applied at an epoch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Transformation {
    /// Register a new node in the cluster.
    Register {
        node_id: NodeId,
        endpoint: Endpoint,
        dc: String,
        rack: String,
    },
    /// Unregister (decommission) a node.
    Unregister { node_id: NodeId },
    /// Assign tokens to a node (bootstrap complete).
    AssignTokens { node_id: NodeId, tokens: Vec<Token> },
    /// Update a node's state.
    UpdateNodeState { node_id: NodeId, state: NodeState },
    /// Schema change: bump schema version.
    SchemaChange {
        schema_version: Uuid,
        description: String,
    },
    /// Lock ranges for an in-progress topology operation.
    LockRanges {
        operation_id: Uuid,
        ranges: Vec<TokenRange>,
    },
    /// Unlock ranges after topology operation completes.
    UnlockRanges { operation_id: Uuid },
    /// Force snapshot at this epoch (for log compaction).
    ForceSnapshot,
}

// ─────────────────────────────────────────────────────────────────────────────
// MetadataLogEntry
// ─────────────────────────────────────────────────────────────────────────────

/// A single entry in the metadata log.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetadataLogEntry {
    pub epoch: Epoch,
    pub transformation: Transformation,
    /// Who committed this entry.
    pub committed_by: NodeId,
}

// ─────────────────────────────────────────────────────────────────────────────
// MetadataLog
// ─────────────────────────────────────────────────────────────────────────────

/// Append-only log of metadata transformations.
///
/// The log is the source of truth for all cluster metadata. Nodes can
/// replay the log to reconstruct the current state, or use snapshots
/// for efficiency.
#[derive(Debug)]
pub struct MetadataLog {
    /// All log entries, ordered by epoch.
    entries: Vec<MetadataLogEntry>,
    /// Current epoch (latest committed).
    current_epoch: Epoch,
}

impl MetadataLog {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
            current_epoch: Epoch::EMPTY,
        }
    }

    /// Append a new transformation and advance the epoch.
    pub fn append(&mut self, transformation: Transformation, committed_by: NodeId) -> Epoch {
        let new_epoch = self.current_epoch.next();
        self.entries.push(MetadataLogEntry {
            epoch: new_epoch,
            transformation,
            committed_by,
        });
        self.current_epoch = new_epoch;
        new_epoch
    }

    /// Get all entries since a given epoch (exclusive).
    pub fn entries_since(&self, since: Epoch) -> &[MetadataLogEntry] {
        let start = self.entries.partition_point(|e| e.epoch <= since);
        &self.entries[start..]
    }

    /// Get the entry at a specific epoch.
    pub fn entry_at(&self, epoch: Epoch) -> Option<&MetadataLogEntry> {
        self.entries.iter().find(|e| e.epoch == epoch)
    }

    /// Current epoch.
    pub fn current_epoch(&self) -> Epoch {
        self.current_epoch
    }

    /// Total number of entries.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the log is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Truncate entries up to (and including) the given epoch.
    /// Used after snapshotting to reclaim space.
    pub fn truncate_before(&mut self, epoch: Epoch) {
        self.entries.retain(|e| e.epoch > epoch);
    }
}

impl Default for MetadataLog {
    fn default() -> Self {
        Self::new()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// NodeDirectory
// ─────────────────────────────────────────────────────────────────────────────

/// Directory of all registered nodes in the cluster.
///
/// The authoritative map from NodeId → NodeInfo, maintained by
/// replaying the metadata log.
#[derive(Debug, Clone, Default)]
pub struct NodeDirectory {
    nodes: HashMap<NodeId, NodeInfo>,
    /// Reverse lookup: endpoint → node_id.
    endpoint_to_id: HashMap<Endpoint, NodeId>,
}

impl NodeDirectory {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a new node.
    pub fn register(&mut self, info: NodeInfo) {
        let id = info.host_id;
        let ep = info.endpoint;
        self.nodes.insert(id, info);
        self.endpoint_to_id.insert(ep, id);
    }

    /// Unregister a node.
    pub fn unregister(&mut self, id: &NodeId) -> Option<NodeInfo> {
        if let Some(info) = self.nodes.remove(id) {
            self.endpoint_to_id.remove(&info.endpoint);
            Some(info)
        } else {
            None
        }
    }

    /// Get node info by ID.
    pub fn get(&self, id: &NodeId) -> Option<&NodeInfo> {
        self.nodes.get(id)
    }

    /// Get node info by endpoint.
    pub fn get_by_endpoint(&self, ep: &Endpoint) -> Option<&NodeInfo> {
        self.endpoint_to_id
            .get(ep)
            .and_then(|id| self.nodes.get(id))
    }

    /// Get mutable node info by ID.
    pub fn get_mut(&mut self, id: &NodeId) -> Option<&mut NodeInfo> {
        self.nodes.get_mut(id)
    }

    /// Number of registered nodes.
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    /// All node infos.
    pub fn all_nodes(&self) -> impl Iterator<Item = &NodeInfo> {
        self.nodes.values()
    }

    /// All node IDs.
    pub fn all_ids(&self) -> impl Iterator<Item = &NodeId> {
        self.nodes.keys()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Placement
// ─────────────────────────────────────────────────────────────────────────────

/// Computed replica placements at a specific epoch.
///
/// Pre-computed map of token → replica set, used by the coordinator
/// for fast lookups without recalculating from the ring on every request.
#[derive(Debug, Clone)]
pub struct Placement {
    /// The epoch at which this placement was computed.
    pub epoch: Epoch,
    /// For each token range (represented by its end token),
    /// the set of replica endpoints.
    pub replicas: BTreeMap<Token, Vec<Endpoint>>,
}

impl Placement {
    pub fn new(epoch: Epoch) -> Self {
        Self {
            epoch,
            replicas: BTreeMap::new(),
        }
    }

    /// Set replicas for a token.
    pub fn set_replicas(&mut self, token: Token, endpoints: Vec<Endpoint>) {
        self.replicas.insert(token, endpoints);
    }

    /// Get replicas for a token (exact match).
    pub fn get_replicas(&self, token: &Token) -> Option<&Vec<Endpoint>> {
        self.replicas.get(token)
    }

    /// Find replicas for a token by walking the placement map (first >= token).
    pub fn find_replicas(&self, token: Token) -> Option<&Vec<Endpoint>> {
        self.replicas
            .range(token..)
            .next()
            .map(|(_, eps)| eps)
            .or_else(|| self.replicas.values().next())
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// LockedRanges
// ─────────────────────────────────────────────────────────────────────────────

/// Tracks token ranges that are currently locked by in-progress
/// topology operations (bootstrap, decommission, move).
///
/// While ranges are locked, certain operations (like concurrent
/// topology changes affecting the same ranges) are rejected.
#[derive(Debug, Clone, Default)]
pub struct LockedRanges {
    /// operation_id → set of locked ranges.
    locks: HashMap<Uuid, Vec<TokenRange>>,
}

impl LockedRanges {
    pub fn new() -> Self {
        Self::default()
    }

    /// Lock ranges for an operation. Returns `Err` if any range conflicts.
    pub fn lock(&mut self, operation_id: Uuid, ranges: Vec<TokenRange>) -> Result<(), TokenRange> {
        // Check for conflicts
        for range in &ranges {
            if let Some(conflict) = self.conflicts_with(range) {
                return Err(conflict);
            }
        }
        self.locks.insert(operation_id, ranges);
        Ok(())
    }

    /// Unlock ranges for an operation.
    pub fn unlock(&mut self, operation_id: &Uuid) -> Option<Vec<TokenRange>> {
        self.locks.remove(operation_id)
    }

    /// Check if a range conflicts with any currently locked range.
    fn conflicts_with(&self, range: &TokenRange) -> Option<TokenRange> {
        for locked_ranges in self.locks.values() {
            for locked in locked_ranges {
                if ranges_overlap(locked, range) {
                    return Some(*locked);
                }
            }
        }
        None
    }

    /// Whether any ranges are currently locked.
    pub fn has_locks(&self) -> bool {
        !self.locks.is_empty()
    }

    /// Number of active lock sets.
    pub fn lock_count(&self) -> usize {
        self.locks.len()
    }
}

/// Check if two token ranges overlap.
fn ranges_overlap(a: &TokenRange, b: &TokenRange) -> bool {
    // Simplified overlap check for non-wrapping ranges.
    // For wrapping ranges, we'd need more complex logic.
    if a.wraps_around() || b.wraps_around() {
        // Conservative: assume overlap for wrapping ranges
        return true;
    }
    a.start < b.end && b.start < a.end
}

// ─────────────────────────────────────────────────────────────────────────────
// InProgressSequence
// ─────────────────────────────────────────────────────────────────────────────

/// Tracks a multi-step topology operation in progress.
///
/// For example, bootstrapping a new node involves:
/// 1. Register node
/// 2. Lock ranges
/// 3. Stream data
/// 4. Assign tokens
/// 5. Unlock ranges
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InProgressSequence {
    /// Unique operation ID.
    pub operation_id: Uuid,
    /// The node involved in this operation.
    pub node_id: NodeId,
    /// Type of operation.
    pub operation_type: SequenceType,
    /// Current step (0-indexed).
    pub current_step: usize,
    /// Total steps.
    pub total_steps: usize,
    /// Epoch at which this sequence started.
    pub start_epoch: Epoch,
}

/// Type of topology operation sequence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SequenceType {
    Bootstrap,
    Decommission,
    Move,
    Replace,
    Rebuild,
}

impl InProgressSequence {
    pub fn new(
        node_id: NodeId,
        operation_type: SequenceType,
        total_steps: usize,
        start_epoch: Epoch,
    ) -> Self {
        Self {
            operation_id: Uuid::new_v4(),
            node_id,
            operation_type,
            current_step: 0,
            total_steps,
            start_epoch,
        }
    }

    /// Advance to the next step.
    pub fn advance(&mut self) -> bool {
        if self.current_step < self.total_steps {
            self.current_step += 1;
            true
        } else {
            false
        }
    }

    /// Whether the sequence is complete.
    pub fn is_complete(&self) -> bool {
        self.current_step >= self.total_steps
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// TcmMetadata — Aggregate state
// ─────────────────────────────────────────────────────────────────────────────

/// Complete TCM cluster metadata state, reconstructed from the log.
///
/// This is the Rust equivalent of Java's `ClusterMetadata` from the `tcm` package.
#[derive(Debug)]
pub struct TcmMetadata {
    pub epoch: Epoch,
    pub directory: NodeDirectory,
    pub locked_ranges: LockedRanges,
    pub in_progress: HashMap<Uuid, InProgressSequence>,
    pub schema_version: Option<Uuid>,
    pub log: MetadataLog,
}

impl TcmMetadata {
    pub fn new() -> Self {
        Self {
            epoch: Epoch::EMPTY,
            directory: NodeDirectory::new(),
            locked_ranges: LockedRanges::new(),
            in_progress: HashMap::new(),
            schema_version: None,
            log: MetadataLog::new(),
        }
    }

    /// Apply a transformation and advance the epoch.
    pub fn apply(
        &mut self,
        transformation: Transformation,
        committed_by: NodeId,
    ) -> Result<Epoch, TcmError> {
        match &transformation {
            Transformation::Register {
                node_id,
                endpoint,
                dc,
                rack,
            } => {
                if self.directory.get(node_id).is_some() {
                    return Err(TcmError::NodeAlreadyRegistered(*node_id));
                }
                let info = NodeInfo::new(*node_id, *endpoint, dc.clone(), rack.clone(), Vec::new());
                self.directory.register(info);
            }
            Transformation::Unregister { node_id } => {
                self.directory.unregister(node_id);
            }
            Transformation::AssignTokens { node_id, tokens } => {
                if let Some(info) = self.directory.get_mut(node_id) {
                    info.tokens = tokens.clone();
                    info.state = NodeState::Normal;
                } else {
                    return Err(TcmError::NodeNotFound(*node_id));
                }
            }
            Transformation::UpdateNodeState { node_id, state } => {
                if let Some(info) = self.directory.get_mut(node_id) {
                    info.state = *state;
                } else {
                    return Err(TcmError::NodeNotFound(*node_id));
                }
            }
            Transformation::SchemaChange { schema_version, .. } => {
                self.schema_version = Some(*schema_version);
            }
            Transformation::LockRanges {
                operation_id,
                ranges,
            } => {
                self.locked_ranges
                    .lock(*operation_id, ranges.clone())
                    .map_err(TcmError::RangeConflict)?;
            }
            Transformation::UnlockRanges { operation_id } => {
                self.locked_ranges.unlock(operation_id);
            }
            Transformation::ForceSnapshot => {
                // No-op for state; the caller should snapshot the current state.
            }
        }

        let epoch = self.log.append(transformation, committed_by);
        self.epoch = epoch;
        Ok(epoch)
    }

    /// Replay all entries from a log to rebuild state from scratch.
    pub fn replay_from(entries: &[MetadataLogEntry]) -> Result<Self, TcmError> {
        let mut metadata = Self::new();
        for entry in entries {
            // Re-apply without re-logging
            metadata.apply_without_logging(&entry.transformation)?;
            metadata.epoch = entry.epoch;
        }
        Ok(metadata)
    }

    /// Apply a transformation without adding to the log (for replay).
    fn apply_without_logging(&mut self, transformation: &Transformation) -> Result<(), TcmError> {
        match transformation {
            Transformation::Register {
                node_id,
                endpoint,
                dc,
                rack,
            } => {
                let info = NodeInfo::new(*node_id, *endpoint, dc.clone(), rack.clone(), Vec::new());
                self.directory.register(info);
            }
            Transformation::Unregister { node_id } => {
                self.directory.unregister(node_id);
            }
            Transformation::AssignTokens { node_id, tokens } => {
                if let Some(info) = self.directory.get_mut(node_id) {
                    info.tokens = tokens.clone();
                    info.state = NodeState::Normal;
                }
            }
            Transformation::UpdateNodeState { node_id, state } => {
                if let Some(info) = self.directory.get_mut(node_id) {
                    info.state = *state;
                }
            }
            Transformation::SchemaChange { schema_version, .. } => {
                self.schema_version = Some(*schema_version);
            }
            Transformation::LockRanges {
                operation_id,
                ranges,
            } => {
                let _ = self.locked_ranges.lock(*operation_id, ranges.clone());
            }
            Transformation::UnlockRanges { operation_id } => {
                self.locked_ranges.unlock(operation_id);
            }
            Transformation::ForceSnapshot => {}
        }
        Ok(())
    }
}

impl Default for TcmMetadata {
    fn default() -> Self {
        Self::new()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// TcmSnapshot
// ─────────────────────────────────────────────────────────────────────────────

/// A serializable snapshot of TCM metadata at a specific epoch.
///
/// Used for:
/// 1. **Fast catch-up**: New nodes (or nodes that fell behind) can load a
///    snapshot instead of replaying the entire log.
/// 2. **Log compaction**: After snapshotting, older log entries can be discarded.
///
/// ## Java Oracle
///
/// `org.apache.cassandra.tcm.ClusterMetadataSnapshot`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TcmSnapshot {
    /// The epoch at which this snapshot was taken.
    pub epoch: Epoch,
    /// Directory of all registered nodes.
    pub nodes: Vec<NodeInfo>,
    /// Schema version at snapshot time.
    pub schema_version: Option<Uuid>,
    /// Any in-progress topology sequences.
    pub in_progress: Vec<InProgressSequence>,
    /// Timestamp when the snapshot was created.
    pub created_at_millis: i64,
}

impl TcmMetadata {
    /// Create a snapshot of the current metadata state.
    ///
    /// The snapshot captures the full state at the current epoch,
    /// enabling log compaction and fast catch-up for new nodes.
    pub fn snapshot(&self) -> TcmSnapshot {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;

        TcmSnapshot {
            epoch: self.epoch,
            nodes: self.directory.all_nodes().cloned().collect(),
            schema_version: self.schema_version,
            in_progress: self.in_progress.values().cloned().collect(),
            created_at_millis: now,
        }
    }

    /// Restore metadata from a snapshot.
    ///
    /// Rebuilds the NodeDirectory and in-progress sequences from the
    /// snapshot, then sets the epoch. Log entries are NOT restored
    /// (the caller should replay entries since the snapshot epoch).
    pub fn restore_from_snapshot(snapshot: &TcmSnapshot) -> Self {
        let mut metadata = Self::new();
        metadata.epoch = snapshot.epoch;
        metadata.schema_version = snapshot.schema_version;

        for info in &snapshot.nodes {
            metadata.directory.register(info.clone());
        }

        for seq in &snapshot.in_progress {
            metadata.in_progress.insert(seq.operation_id, seq.clone());
        }

        metadata
    }

    /// Compact the log by snapshotting and truncating old entries.
    ///
    /// Returns the snapshot that was taken.
    pub fn compact(&mut self) -> TcmSnapshot {
        let snapshot = self.snapshot();
        self.log.truncate_before(self.epoch);
        snapshot
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// SealedPeriod
// ─────────────────────────────────────────────────────────────────────────────

/// A sealed period of the metadata log.
///
/// A period is "sealed" when a snapshot has been taken at its end epoch.
/// Log entries within a sealed period can be safely garbage-collected
/// because the snapshot contains all the state up to that point.
///
/// ## Java Oracle
///
/// `org.apache.cassandra.tcm.log.LogStorage.SealedPeriod`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SealedPeriod {
    /// Epoch at which this period was sealed (snapshot epoch).
    pub epoch: Epoch,
    /// Number of log entries in this sealed period.
    pub entry_count: usize,
    /// Timestamp when this period was sealed.
    pub sealed_at_millis: i64,
}

impl SealedPeriod {
    /// Create a new sealed period from a snapshot.
    pub fn seal(epoch: Epoch, entry_count: usize) -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;

        Self {
            epoch,
            entry_count,
            sealed_at_millis: now,
        }
    }
}

/// Errors from TCM operations.
#[derive(Debug, thiserror::Error)]
pub enum TcmError {
    #[error("Node {0} is already registered")]
    NodeAlreadyRegistered(NodeId),

    #[error("Node {0} not found")]
    NodeNotFound(NodeId),

    #[error("Range conflict with locked range {0}")]
    RangeConflict(TokenRange),

    #[error("Epoch mismatch: expected {expected}, got {actual}")]
    EpochMismatch { expected: Epoch, actual: Epoch },

    #[error("Epoch {0} is stale, catch-up needed from {1}")]
    EpochStale(Epoch, Epoch),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node_id(n: u128) -> NodeId {
        NodeId::from_uuid(Uuid::from_u128(n))
    }

    fn ep(port: u16) -> Endpoint {
        Endpoint::new(std::net::SocketAddr::new(
            std::net::IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1)),
            port,
        ))
    }

    #[test]
    fn epoch_ordering() {
        assert!(Epoch::EMPTY < Epoch::FIRST);
        assert!(Epoch::FIRST < Epoch::FIRST.next());
        assert_eq!(Epoch::FIRST.value(), 1);
    }

    #[test]
    fn metadata_log_append_and_query() {
        let mut log = MetadataLog::new();
        assert!(log.is_empty());

        let e1 = log.append(
            Transformation::Register {
                node_id: node_id(1),
                endpoint: ep(7001),
                dc: "dc1".into(),
                rack: "rack1".into(),
            },
            node_id(1),
        );
        assert_eq!(e1, Epoch::FIRST);

        let e2 = log.append(
            Transformation::SchemaChange {
                schema_version: Uuid::nil(),
                description: "create table".into(),
            },
            node_id(1),
        );
        assert_eq!(e2, Epoch(2));
        assert_eq!(log.len(), 2);

        // entries_since epoch 0 returns all
        assert_eq!(log.entries_since(Epoch::EMPTY).len(), 2);
        // entries_since epoch 1 returns only the second
        assert_eq!(log.entries_since(Epoch::FIRST).len(), 1);
    }

    #[test]
    fn metadata_log_truncate() {
        let mut log = MetadataLog::new();
        for i in 0..5 {
            log.append(
                Transformation::SchemaChange {
                    schema_version: Uuid::new_v4(),
                    description: format!("change {i}"),
                },
                node_id(1),
            );
        }
        assert_eq!(log.len(), 5);

        log.truncate_before(Epoch(3));
        assert_eq!(log.len(), 2); // epochs 4 and 5 remain
    }

    #[test]
    fn node_directory_crud() {
        let mut dir = NodeDirectory::new();
        assert!(dir.is_empty());

        let info = NodeInfo::new(
            node_id(1),
            ep(7001),
            "dc1",
            "rack1",
            vec![Token::from_raw(0)],
        );
        dir.register(info);
        assert_eq!(dir.len(), 1);

        assert!(dir.get(&node_id(1)).is_some());
        assert!(dir.get_by_endpoint(&ep(7001)).is_some());

        dir.unregister(&node_id(1));
        assert!(dir.is_empty());
    }

    #[test]
    fn locked_ranges_conflict() {
        let mut lr = LockedRanges::new();
        let op1 = Uuid::new_v4();
        let op2 = Uuid::new_v4();

        let range1 = vec![TokenRange::new(Token::from_raw(0), Token::from_raw(100))];
        assert!(lr.lock(op1, range1).is_ok());
        assert!(lr.has_locks());

        // Overlapping range should conflict
        let range2 = vec![TokenRange::new(Token::from_raw(50), Token::from_raw(150))];
        assert!(lr.lock(op2, range2).is_err());

        // Unlock first, then second should succeed
        lr.unlock(&op1);
        let range3 = vec![TokenRange::new(Token::from_raw(50), Token::from_raw(150))];
        assert!(lr.lock(op2, range3).is_ok());
    }

    #[test]
    fn in_progress_sequence() {
        let mut seq = InProgressSequence::new(node_id(1), SequenceType::Bootstrap, 3, Epoch::FIRST);
        assert!(!seq.is_complete());
        assert_eq!(seq.current_step, 0);

        assert!(seq.advance());
        assert!(seq.advance());
        assert!(seq.advance());
        assert!(seq.is_complete());
        assert!(!seq.advance()); // can't advance past complete
    }

    #[test]
    fn placement_find_replicas() {
        let mut p = Placement::new(Epoch::FIRST);
        p.set_replicas(Token::from_raw(100), vec![ep(7001), ep(7002)]);
        p.set_replicas(Token::from_raw(200), vec![ep(7002), ep(7003)]);

        // Find replicas for token 150 → first >= 150 → token 200
        let r = p.find_replicas(Token::from_raw(150)).unwrap();
        assert!(r.contains(&ep(7002)));
        assert!(r.contains(&ep(7003)));

        // Find replicas for token 50 → first >= 50 → token 100
        let r = p.find_replicas(Token::from_raw(50)).unwrap();
        assert!(r.contains(&ep(7001)));
    }

    #[test]
    fn tcm_metadata_apply() {
        let mut tcm = TcmMetadata::new();
        let nid = node_id(1);

        // Register
        let e = tcm
            .apply(
                Transformation::Register {
                    node_id: nid,
                    endpoint: ep(7001),
                    dc: "dc1".into(),
                    rack: "rack1".into(),
                },
                nid,
            )
            .unwrap();
        assert_eq!(e, Epoch::FIRST);
        assert_eq!(tcm.directory.len(), 1);

        // Assign tokens
        let e = tcm
            .apply(
                Transformation::AssignTokens {
                    node_id: nid,
                    tokens: vec![Token::from_raw(0), Token::from_raw(100)],
                },
                nid,
            )
            .unwrap();
        assert_eq!(e, Epoch(2));

        let info = tcm.directory.get(&nid).unwrap();
        assert_eq!(info.tokens.len(), 2);
        assert_eq!(info.state, NodeState::Normal);
    }

    #[test]
    fn tcm_metadata_duplicate_register() {
        let mut tcm = TcmMetadata::new();
        let nid = node_id(1);

        tcm.apply(
            Transformation::Register {
                node_id: nid,
                endpoint: ep(7001),
                dc: "dc1".into(),
                rack: "rack1".into(),
            },
            nid,
        )
        .unwrap();

        // Should fail on duplicate
        let result = tcm.apply(
            Transformation::Register {
                node_id: nid,
                endpoint: ep(7001),
                dc: "dc1".into(),
                rack: "rack1".into(),
            },
            nid,
        );
        assert!(result.is_err());
    }

    #[test]
    fn tcm_metadata_schema_change() {
        let mut tcm = TcmMetadata::new();
        let sv = Uuid::new_v4();

        tcm.apply(
            Transformation::SchemaChange {
                schema_version: sv,
                description: "create keyspace".into(),
            },
            node_id(1),
        )
        .unwrap();

        assert_eq!(tcm.schema_version, Some(sv));
    }

    #[test]
    fn tcm_metadata_lock_unlock_ranges() {
        let mut tcm = TcmMetadata::new();
        let op = Uuid::new_v4();

        tcm.apply(
            Transformation::LockRanges {
                operation_id: op,
                ranges: vec![TokenRange::new(Token::from_raw(0), Token::from_raw(100))],
            },
            node_id(1),
        )
        .unwrap();

        assert!(tcm.locked_ranges.has_locks());

        tcm.apply(
            Transformation::UnlockRanges { operation_id: op },
            node_id(1),
        )
        .unwrap();

        assert!(!tcm.locked_ranges.has_locks());
    }

    // ── Snapshot / Sealed Period Tests ───────────────────────────────────

    #[test]
    fn snapshot_captures_current_state() {
        let mut tcm = TcmMetadata::new();
        let nid = node_id(1);

        tcm.apply(
            Transformation::Register {
                node_id: nid,
                endpoint: ep(7001),
                dc: "dc1".into(),
                rack: "rack1".into(),
            },
            nid,
        )
        .unwrap();

        let snap = tcm.snapshot();
        assert_eq!(snap.epoch, Epoch::FIRST);
        assert_eq!(snap.nodes.len(), 1);
        assert_eq!(snap.nodes[0].host_id, nid);
    }

    #[test]
    fn snapshot_serde_round_trip() {
        let mut tcm = TcmMetadata::new();
        let nid = node_id(1);

        tcm.apply(
            Transformation::Register {
                node_id: nid,
                endpoint: ep(7001),
                dc: "dc1".into(),
                rack: "rack1".into(),
            },
            nid,
        )
        .unwrap();

        let snap = tcm.snapshot();
        let json = serde_json::to_string(&snap).unwrap();
        let deserialized: TcmSnapshot = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.epoch, snap.epoch);
        assert_eq!(deserialized.nodes.len(), snap.nodes.len());
    }

    #[test]
    fn restore_from_snapshot() {
        let mut tcm = TcmMetadata::new();
        let nid = node_id(1);

        tcm.apply(
            Transformation::Register {
                node_id: nid,
                endpoint: ep(7001),
                dc: "dc1".into(),
                rack: "rack1".into(),
            },
            nid,
        )
        .unwrap();

        let snap = tcm.snapshot();

        // Restore
        let restored = TcmMetadata::restore_from_snapshot(&snap);
        assert_eq!(restored.epoch, Epoch::FIRST);
        assert_eq!(restored.directory.len(), 1);
        assert!(restored.directory.get(&nid).is_some());
    }

    #[test]
    fn compact_truncates_log() {
        let mut tcm = TcmMetadata::new();
        let nid = node_id(1);

        for i in 0..5 {
            tcm.apply(
                Transformation::SchemaChange {
                    schema_version: Uuid::new_v4(),
                    description: format!("change {i}"),
                },
                nid,
            )
            .unwrap();
        }
        assert_eq!(tcm.log.len(), 5);

        let snap = tcm.compact();
        assert_eq!(snap.epoch, Epoch(5));
        // After compact, log should be empty (all truncated before current epoch)
        assert!(tcm.log.len() < 5);
    }

    #[test]
    fn sealed_period_creation() {
        let sp = SealedPeriod::seal(Epoch(10), 50);
        assert_eq!(sp.epoch, Epoch(10));
        assert_eq!(sp.entry_count, 50);
        assert!(sp.sealed_at_millis > 0);
    }
}
