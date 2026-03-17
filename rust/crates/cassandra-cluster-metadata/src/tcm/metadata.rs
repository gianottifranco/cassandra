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

//! Immutable full-cluster metadata snapshot and diff computation.
//!
//! [`FullClusterMetadata`] is an immutable point-in-time snapshot of the entire
//! cluster state. It is built via [`Builder`] and produces new instances through
//! [`FullClusterMetadata::apply`]. Two snapshots can be compared with
//! [`FullClusterMetadata::diff`] to obtain a [`MetadataDiff`].
//!
//! ## Java Oracle
//!
//! `org.apache.cassandra.tcm.ClusterMetadata`

use std::collections::{HashMap, HashSet};

use uuid::Uuid;

use crate::node::{NodeId, NodeState};
use crate::tcm::{
    Epoch, InProgressSequence, LockedRanges, NodeDirectory, Placement, Transformation,
};

// ─────────────────────────────────────────────────────────────────────────────
// MetadataDiff
// ─────────────────────────────────────────────────────────────────────────────

/// Describes the differences between two [`FullClusterMetadata`] instances.
#[derive(Debug, Clone)]
pub struct MetadataDiff {
    /// Epoch of the *before* snapshot.
    pub epoch_before: Epoch,
    /// Epoch of the *after* snapshot.
    pub epoch_after: Epoch,
    /// Nodes present in *after* but not in *before*.
    pub nodes_added: Vec<NodeId>,
    /// Nodes present in *before* but not in *after*.
    pub nodes_removed: Vec<NodeId>,
    /// Whether the schema version changed between the two snapshots.
    pub schema_changed: bool,
    /// Nodes whose token assignment changed.
    pub tokens_changed: Vec<NodeId>,
    /// State transitions: `(node, old_state, new_state)`.
    pub state_changes: Vec<(NodeId, NodeState, NodeState)>,
}

impl MetadataDiff {
    /// Create a new diff with the given epoch boundaries and empty change sets.
    pub fn new(before: Epoch, after: Epoch) -> Self {
        Self {
            epoch_before: before,
            epoch_after: after,
            nodes_added: Vec::new(),
            nodes_removed: Vec::new(),
            schema_changed: false,
            tokens_changed: Vec::new(),
            state_changes: Vec::new(),
        }
    }

    /// Returns `true` when there are no differences between the two snapshots.
    pub fn is_empty(&self) -> bool {
        self.nodes_added.is_empty()
            && self.nodes_removed.is_empty()
            && !self.schema_changed
            && self.tokens_changed.is_empty()
            && self.state_changes.is_empty()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// FullClusterMetadata
// ─────────────────────────────────────────────────────────────────────────────

/// An immutable, point-in-time snapshot of the full cluster metadata.
///
/// Constructed via [`Builder`] and never mutated in place.
/// [`apply`](FullClusterMetadata::apply) returns a *new* instance with the
/// transformation applied, leaving the original untouched.
#[derive(Debug, Clone)]
pub struct FullClusterMetadata {
    epoch: Epoch,
    directory: NodeDirectory,
    locked_ranges: LockedRanges,
    in_progress: HashMap<Uuid, InProgressSequence>,
    schema_version: Option<Uuid>,
    placements: HashMap<String, Placement>,
    partitioner: String,
    cluster_name: String,
}

impl FullClusterMetadata {
    /// Current epoch of this metadata snapshot.
    pub fn epoch(&self) -> Epoch {
        self.epoch
    }

    /// The node directory.
    pub fn directory(&self) -> &NodeDirectory {
        &self.directory
    }

    /// Currently locked token ranges.
    pub fn locked_ranges(&self) -> &LockedRanges {
        &self.locked_ranges
    }

    /// Schema version at this epoch, if any.
    pub fn schema_version(&self) -> Option<Uuid> {
        self.schema_version
    }

    /// Lookup the [`Placement`] for a keyspace.
    pub fn placement_for(&self, keyspace: &str) -> Option<&Placement> {
        self.placements.get(keyspace)
    }

    /// Cluster name.
    pub fn cluster_name(&self) -> &str {
        &self.cluster_name
    }

    /// Partitioner class name.
    pub fn partitioner(&self) -> &str {
        &self.partitioner
    }

    /// In-progress topology sequences.
    pub fn in_progress(&self) -> &HashMap<Uuid, InProgressSequence> {
        &self.in_progress
    }

    /// Apply a [`Transformation`] and return a **new** metadata instance with
    /// the epoch incremented by one.
    ///
    /// The original instance is not modified.
    pub fn apply(&self, transformation: &Transformation) -> Result<FullClusterMetadata, String> {
        let mut next = self.clone();
        next.epoch = self.epoch.next();

        match transformation {
            Transformation::Register {
                node_id,
                endpoint,
                dc,
                rack,
            } => {
                if next.directory.get(node_id).is_some() {
                    return Err(format!("node {} is already registered", node_id));
                }
                let info = crate::node::NodeInfo::new(
                    *node_id,
                    *endpoint,
                    dc.clone(),
                    rack.clone(),
                    Vec::new(),
                );
                next.directory.register(info);
            }
            Transformation::Unregister { node_id } => {
                next.directory.unregister(node_id);
            }
            Transformation::AssignTokens { node_id, tokens } => {
                if let Some(info) = next.directory.get_mut(node_id) {
                    info.tokens = tokens.clone();
                    info.state = NodeState::Normal;
                } else {
                    return Err(format!("node {} not found", node_id));
                }
            }
            Transformation::UpdateNodeState { node_id, state } => {
                if let Some(info) = next.directory.get_mut(node_id) {
                    info.state = *state;
                } else {
                    return Err(format!("node {} not found", node_id));
                }
            }
            Transformation::SchemaChange { schema_version, .. } => {
                next.schema_version = Some(*schema_version);
            }
            Transformation::LockRanges {
                operation_id,
                ranges,
            } => {
                next.locked_ranges
                    .lock(*operation_id, ranges.clone())
                    .map_err(|r| format!("range conflict with {r}"))?;
            }
            Transformation::UnlockRanges { operation_id } => {
                next.locked_ranges.unlock(operation_id);
            }
            Transformation::ForceSnapshot => {
                // No-op for state; epoch still advances.
            }
        }

        Ok(next)
    }

    /// Compute the diff between `self` (before) and `other` (after).
    pub fn diff(&self, other: &FullClusterMetadata) -> MetadataDiff {
        let mut d = MetadataDiff::new(self.epoch, other.epoch());

        let self_ids: HashSet<NodeId> = self.directory.all_ids().copied().collect();
        let other_ids: HashSet<NodeId> = other.directory.all_ids().copied().collect();

        // Nodes in other but not self → added.
        for id in &other_ids {
            if !self_ids.contains(id) {
                d.nodes_added.push(*id);
            }
        }

        // Nodes in self but not other → removed.
        for id in &self_ids {
            if !other_ids.contains(id) {
                d.nodes_removed.push(*id);
            }
        }

        // Schema comparison.
        d.schema_changed = self.schema_version != other.schema_version;

        // For nodes present in both, compare tokens and state.
        for id in self_ids.intersection(&other_ids) {
            let before = self.directory.get(id).unwrap();
            let after = other.directory.get(id).unwrap();

            if before.tokens != after.tokens {
                d.tokens_changed.push(*id);
            }
            if before.state != after.state {
                d.state_changes.push((*id, before.state, after.state));
            }
        }

        d
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Builder
// ─────────────────────────────────────────────────────────────────────────────

/// Fluent builder for [`FullClusterMetadata`].
///
/// The only **required** field is `epoch`; all others fall back to sensible
/// defaults (empty directory, no locks, `Murmur3Partitioner`, etc.).
#[derive(Debug, Clone, Default)]
pub struct Builder {
    epoch: Option<Epoch>,
    directory: Option<NodeDirectory>,
    locked_ranges: Option<LockedRanges>,
    in_progress: Option<HashMap<Uuid, InProgressSequence>>,
    schema_version: Option<Uuid>,
    placements: Option<HashMap<String, Placement>>,
    partitioner: Option<String>,
    cluster_name: Option<String>,
}

impl Builder {
    /// Create a new, empty builder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the epoch.
    pub fn epoch(mut self, e: Epoch) -> Self {
        self.epoch = Some(e);
        self
    }

    /// Set the node directory.
    pub fn directory(mut self, d: NodeDirectory) -> Self {
        self.directory = Some(d);
        self
    }

    /// Set the locked ranges.
    pub fn locked_ranges(mut self, lr: LockedRanges) -> Self {
        self.locked_ranges = Some(lr);
        self
    }

    /// Set the schema version.
    pub fn schema_version(mut self, sv: Uuid) -> Self {
        self.schema_version = Some(sv);
        self
    }

    /// Add a placement for a keyspace.
    pub fn add_placement(mut self, keyspace: String, p: Placement) -> Self {
        self.placements
            .get_or_insert_with(HashMap::new)
            .insert(keyspace, p);
        self
    }

    /// Set the partitioner class name.
    pub fn partitioner(mut self, p: String) -> Self {
        self.partitioner = Some(p);
        self
    }

    /// Set the cluster name.
    pub fn cluster_name(mut self, name: String) -> Self {
        self.cluster_name = Some(name);
        self
    }

    /// Consume the builder and produce a [`FullClusterMetadata`].
    ///
    /// Fails if `epoch` was not set.
    pub fn build(self) -> Result<FullClusterMetadata, String> {
        let epoch = self.epoch.ok_or("epoch is required")?;
        Ok(FullClusterMetadata {
            epoch,
            directory: self.directory.unwrap_or_default(),
            locked_ranges: self.locked_ranges.unwrap_or_default(),
            in_progress: self.in_progress.unwrap_or_default(),
            schema_version: self.schema_version,
            placements: self.placements.unwrap_or_default(),
            partitioner: self
                .partitioner
                .unwrap_or_else(|| "org.apache.cassandra.dht.Murmur3Partitioner".to_string()),
            cluster_name: self
                .cluster_name
                .unwrap_or_else(|| "Test Cluster".to_string()),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::{Endpoint, NodeId, NodeInfo, NodeState};
    use crate::tcm::{Epoch, NodeDirectory, Placement, Transformation};
    use cassandra_common::token::Token;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use uuid::Uuid;

    fn node_id(n: u128) -> NodeId {
        NodeId::from_uuid(Uuid::from_u128(n))
    }

    fn ep(port: u16) -> Endpoint {
        Endpoint::new(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            port,
        ))
    }

    fn dir_with_node(id: NodeId, endpoint: Endpoint) -> NodeDirectory {
        let mut d = NodeDirectory::new();
        d.register(NodeInfo::new(id, endpoint, "dc1", "rack1", Vec::new()));
        d
    }

    // ── Builder tests ────────────────────────────────────────────────────

    #[test]
    fn builder_requires_epoch() {
        let result = Builder::new().build();
        assert!(result.is_err());
    }

    #[test]
    fn builder_defaults() {
        let meta = Builder::new().epoch(Epoch::FIRST).build().unwrap();
        assert_eq!(meta.epoch(), Epoch::FIRST);
        assert!(meta.directory().is_empty());
        assert!(!meta.locked_ranges().has_locks());
        assert_eq!(meta.schema_version(), None);
        assert_eq!(
            meta.partitioner(),
            "org.apache.cassandra.dht.Murmur3Partitioner"
        );
        assert_eq!(meta.cluster_name(), "Test Cluster");
    }

    #[test]
    fn builder_with_directory_and_placement() {
        let nid = node_id(1);
        let dir = dir_with_node(nid, ep(7001));
        let placement = Placement::new(Epoch::FIRST);

        let meta = Builder::new()
            .epoch(Epoch::FIRST)
            .directory(dir)
            .add_placement("ks1".to_string(), placement)
            .cluster_name("MyCluster".to_string())
            .partitioner("RandomPartitioner".to_string())
            .build()
            .unwrap();

        assert_eq!(meta.directory().len(), 1);
        assert!(meta.placement_for("ks1").is_some());
        assert!(meta.placement_for("ks2").is_none());
        assert_eq!(meta.cluster_name(), "MyCluster");
        assert_eq!(meta.partitioner(), "RandomPartitioner");
    }

    // ── Apply tests ──────────────────────────────────────────────────────

    #[test]
    fn apply_register_returns_new_instance() {
        let meta = Builder::new().epoch(Epoch::FIRST).build().unwrap();
        let nid = node_id(1);

        let next = meta
            .apply(&Transformation::Register {
                node_id: nid,
                endpoint: ep(7001),
                dc: "dc1".into(),
                rack: "rack1".into(),
            })
            .unwrap();

        // Original unchanged.
        assert!(meta.directory().is_empty());
        assert_eq!(meta.epoch(), Epoch::FIRST);

        // New instance updated.
        assert_eq!(next.epoch(), Epoch(2));
        assert_eq!(next.directory().len(), 1);
    }

    #[test]
    fn apply_duplicate_register_fails() {
        let nid = node_id(1);
        let dir = dir_with_node(nid, ep(7001));
        let meta = Builder::new()
            .epoch(Epoch::FIRST)
            .directory(dir)
            .build()
            .unwrap();

        let result = meta.apply(&Transformation::Register {
            node_id: nid,
            endpoint: ep(7001),
            dc: "dc1".into(),
            rack: "rack1".into(),
        });
        assert!(result.is_err());
    }

    #[test]
    fn apply_schema_change_updates_version() {
        let meta = Builder::new().epoch(Epoch::FIRST).build().unwrap();
        let sv = Uuid::from_u128(42);

        let next = meta
            .apply(&Transformation::SchemaChange {
                schema_version: sv,
                description: "create ks".into(),
            })
            .unwrap();

        assert_eq!(next.schema_version(), Some(sv));
        assert_eq!(meta.schema_version(), None); // original untouched
    }

    #[test]
    fn apply_assign_tokens() {
        let nid = node_id(1);
        let dir = dir_with_node(nid, ep(7001));
        let meta = Builder::new()
            .epoch(Epoch::FIRST)
            .directory(dir)
            .build()
            .unwrap();

        let next = meta
            .apply(&Transformation::AssignTokens {
                node_id: nid,
                tokens: vec![Token::from_raw(100), Token::from_raw(200)],
            })
            .unwrap();

        let info = next.directory().get(&nid).unwrap();
        assert_eq!(info.tokens.len(), 2);
        assert_eq!(info.state, NodeState::Normal);
    }

    #[test]
    fn apply_update_node_state() {
        let nid = node_id(1);
        let dir = dir_with_node(nid, ep(7001));
        let meta = Builder::new()
            .epoch(Epoch::FIRST)
            .directory(dir)
            .build()
            .unwrap();

        let next = meta
            .apply(&Transformation::UpdateNodeState {
                node_id: nid,
                state: NodeState::Leaving,
            })
            .unwrap();

        let info = next.directory().get(&nid).unwrap();
        assert_eq!(info.state, NodeState::Leaving);
    }

    // ── Diff tests ───────────────────────────────────────────────────────

    #[test]
    fn diff_identical_is_empty() {
        let meta = Builder::new().epoch(Epoch::FIRST).build().unwrap();
        let d = meta.diff(&meta);
        assert!(d.is_empty());
    }

    #[test]
    fn diff_detects_added_and_removed_nodes() {
        let nid_a = node_id(1);
        let nid_b = node_id(2);

        let mut dir_before = NodeDirectory::new();
        dir_before.register(NodeInfo::new(nid_a, ep(7001), "dc1", "rack1", Vec::new()));

        let mut dir_after = NodeDirectory::new();
        dir_after.register(NodeInfo::new(nid_b, ep(7002), "dc1", "rack1", Vec::new()));

        let before = Builder::new()
            .epoch(Epoch::FIRST)
            .directory(dir_before)
            .build()
            .unwrap();

        let after = Builder::new()
            .epoch(Epoch(2))
            .directory(dir_after)
            .build()
            .unwrap();

        let d = before.diff(&after);
        assert!(!d.is_empty());
        assert_eq!(d.nodes_added, vec![nid_b]);
        assert_eq!(d.nodes_removed, vec![nid_a]);
    }

    #[test]
    fn diff_detects_schema_change() {
        let sv = Uuid::from_u128(99);
        let before = Builder::new().epoch(Epoch::FIRST).build().unwrap();
        let after = Builder::new()
            .epoch(Epoch(2))
            .schema_version(sv)
            .build()
            .unwrap();

        let d = before.diff(&after);
        assert!(d.schema_changed);
    }

    #[test]
    fn diff_detects_token_and_state_changes() {
        let nid = node_id(1);

        let mut dir_before = NodeDirectory::new();
        dir_before.register(NodeInfo::new(
            nid,
            ep(7001),
            "dc1",
            "rack1",
            vec![Token::from_raw(0)],
        ));

        let mut dir_after = NodeDirectory::new();
        let mut info = NodeInfo::new(
            nid,
            ep(7001),
            "dc1",
            "rack1",
            vec![Token::from_raw(0), Token::from_raw(100)],
        );
        info.state = NodeState::Leaving;
        dir_after.register(info);

        let before = Builder::new()
            .epoch(Epoch::FIRST)
            .directory(dir_before)
            .build()
            .unwrap();

        let after = Builder::new()
            .epoch(Epoch(2))
            .directory(dir_after)
            .build()
            .unwrap();

        let d = before.diff(&after);
        assert_eq!(d.tokens_changed, vec![nid]);
        assert_eq!(d.state_changes.len(), 1);
        assert_eq!(d.state_changes[0], (nid, NodeState::Normal, NodeState::Leaving));
    }

    #[test]
    fn metadata_diff_new_is_empty() {
        let d = MetadataDiff::new(Epoch::FIRST, Epoch(2));
        assert!(d.is_empty());
        assert_eq!(d.epoch_before, Epoch::FIRST);
        assert_eq!(d.epoch_after, Epoch(2));
    }
}
