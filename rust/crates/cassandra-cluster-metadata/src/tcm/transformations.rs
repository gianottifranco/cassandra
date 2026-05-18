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

//! Extended transformation types for the TCM subsystem.
//!
//! While [`Transformation`] covers the core metadata-log primitives,
//! `ExtendedTransformation` models the full set of cluster operations
//! including multi-step topology sequences (join/leave/move/replace),
//! CMS lifecycle, snapshot management, and CMS reconfiguration.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.tcm.Transformation` (base interface)
//! - `org.apache.cassandra.tcm.transformations/` directory
//!   (PrepareJoin, StartJoin, MidJoin, FinishJoin, etc.)

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use cassandra_common::token::{Token, TokenRange};

use crate::node::{Endpoint, NodeId, NodeState};
use crate::tcm::{Epoch, Transformation};

// ─────────────────────────────────────────────────────────────────────────────
// TransformationKind
// ─────────────────────────────────────────────────────────────────────────────

/// High-level classification of a transformation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransformationKind {
    /// CMS lifecycle (init, seal, force-snapshot).
    CmsInit,
    /// Topology changes (join, leave, move, replace, register, unregister, state).
    TopologyChange,
    /// Schema DDL changes.
    SchemaChange,
    /// Snapshot operations (take, restore).
    SnapshotOp,
    /// CMS membership reconfiguration.
    CmsReconfig,
    /// Token assignment/unassignment.
    TokenOp,
    /// In-progress sequence bookkeeping.
    SequenceOp,
}

// ─────────────────────────────────────────────────────────────────────────────
// ExtendedTransformation
// ─────────────────────────────────────────────────────────────────────────────

/// The full set of cluster metadata transformations.
///
/// Each variant maps 1:1 to a concrete Java `Transformation` subclass
/// found in the `org.apache.cassandra.tcm.transformations` package.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ExtendedTransformation {
    // ── CMS lifecycle ────────────────────────────────────────────────────
    /// Bootstrap the CMS subsystem.
    InitializeCms,

    /// Seal the current log period at the given epoch.
    SealPeriod { epoch: Epoch },

    /// Request a snapshot at this point in the log.
    ForceSnapshot,

    // ── Topology: 4-step join ────────────────────────────────────────────
    /// Reserve tokens and announce intent to join.
    PrepareJoin {
        node_id: NodeId,
        endpoint: Endpoint,
        dc: String,
        rack: String,
        tokens: Vec<Token>,
    },
    /// Begin streaming data for join.
    StartJoin { node_id: NodeId },
    /// Mid-join: ranges are being transferred.
    MidJoin { node_id: NodeId },
    /// Join complete: node is now NORMAL.
    FinishJoin { node_id: NodeId },

    // ── Topology: 4-step leave ───────────────────────────────────────────
    /// Announce intent to leave the ring.
    PrepareLeave { node_id: NodeId },
    /// Begin streaming data away.
    StartLeave { node_id: NodeId },
    /// Mid-leave: ranges are being transferred.
    MidLeave { node_id: NodeId },
    /// Leave complete: node is removed from the ring.
    FinishLeave { node_id: NodeId },

    // ── Topology: 4-step move ────────────────────────────────────────────
    /// Announce intent to move to new tokens.
    PrepareMove {
        node_id: NodeId,
        new_tokens: Vec<Token>,
    },
    /// Begin streaming data for move.
    StartMove { node_id: NodeId },
    /// Mid-move: ranges are being transferred.
    MidMove { node_id: NodeId },
    /// Move complete: node owns new tokens.
    FinishMove { node_id: NodeId },

    // ── Topology: 4-step replace ─────────────────────────────────────────
    /// Announce intent to replace a dead node.
    PrepareReplace {
        old_node: NodeId,
        new_node: NodeId,
        new_endpoint: Endpoint,
        dc: String,
        rack: String,
    },
    /// Begin streaming data for replacement.
    StartReplace { new_node: NodeId },
    /// Mid-replace: ranges are being transferred.
    MidReplace { new_node: NodeId },
    /// Replace complete: old node removed, new node is NORMAL.
    FinishReplace { old_node: NodeId, new_node: NodeId },

    // ── Schema ───────────────────────────────────────────────────────────
    /// Apply a schema DDL change.
    AlterSchema {
        schema_version: Uuid,
        description: String,
    },

    // ── Snapshots ────────────────────────────────────────────────────────
    /// Take a snapshot of the current metadata state.
    TakeSnapshot,
    /// Restore metadata from a snapshot at the given epoch.
    RestoreSnapshot { epoch: Epoch },

    // ── CMS reconfiguration ──────────────────────────────────────────────
    /// Add a node to the CMS voter set.
    AddCmsMember { node_id: NodeId },
    /// Remove a node from the CMS voter set.
    RemoveCmsMember { node_id: NodeId },

    // ── Token operations ─────────────────────────────────────────────────
    /// Assign tokens to a node.
    AssignTokens { node_id: NodeId, tokens: Vec<Token> },
    /// Remove all token assignments from a node.
    UnassignTokens { node_id: NodeId },
    /// Lock token ranges for an in-progress topology operation.
    LockRanges {
        operation_id: Uuid,
        ranges: Vec<TokenRange>,
    },
    /// Unlock token ranges for a completed topology operation.
    UnlockRanges { operation_id: Uuid },

    // ── Register / Unregister ────────────────────────────────────────────
    /// Register a new node in the directory.
    RegisterNode {
        node_id: NodeId,
        endpoint: Endpoint,
        dc: String,
        rack: String,
    },
    /// Remove a node from the directory.
    UnregisterNode { node_id: NodeId },

    // ── State ────────────────────────────────────────────────────────────
    /// Update a node's lifecycle state.
    UpdateNodeState { node_id: NodeId, state: NodeState },
}

impl ExtendedTransformation {
    /// Classify this transformation into a high-level kind.
    pub fn kind(&self) -> TransformationKind {
        match self {
            Self::InitializeCms | Self::SealPeriod { .. } | Self::ForceSnapshot => {
                TransformationKind::CmsInit
            }

            Self::PrepareJoin { .. }
            | Self::StartJoin { .. }
            | Self::MidJoin { .. }
            | Self::FinishJoin { .. }
            | Self::PrepareLeave { .. }
            | Self::StartLeave { .. }
            | Self::MidLeave { .. }
            | Self::FinishLeave { .. }
            | Self::PrepareMove { .. }
            | Self::StartMove { .. }
            | Self::MidMove { .. }
            | Self::FinishMove { .. }
            | Self::PrepareReplace { .. }
            | Self::StartReplace { .. }
            | Self::MidReplace { .. }
            | Self::FinishReplace { .. }
            | Self::RegisterNode { .. }
            | Self::UnregisterNode { .. }
            | Self::UpdateNodeState { .. } => TransformationKind::TopologyChange,

            Self::AlterSchema { .. } => TransformationKind::SchemaChange,

            Self::TakeSnapshot | Self::RestoreSnapshot { .. } => TransformationKind::SnapshotOp,

            Self::AddCmsMember { .. } | Self::RemoveCmsMember { .. } => {
                TransformationKind::CmsReconfig
            }

            Self::AssignTokens { .. } | Self::UnassignTokens { .. } => TransformationKind::TokenOp,

            Self::LockRanges { .. } | Self::UnlockRanges { .. } => TransformationKind::SequenceOp,
        }
    }

    /// Convert to a base [`Transformation`] where a direct mapping exists.
    ///
    /// Returns `None` for extended-only variants (multi-step sequences,
    /// CMS lifecycle, CMS reconfiguration, etc.).
    pub fn to_base(&self) -> Option<Transformation> {
        match self {
            Self::RegisterNode {
                node_id,
                endpoint,
                dc,
                rack,
            } => Some(Transformation::Register {
                node_id: *node_id,
                endpoint: *endpoint,
                dc: dc.clone(),
                rack: rack.clone(),
            }),

            Self::UnregisterNode { node_id } => {
                Some(Transformation::Unregister { node_id: *node_id })
            }

            Self::AssignTokens { node_id, tokens } => Some(Transformation::AssignTokens {
                node_id: *node_id,
                tokens: tokens.clone(),
            }),

            Self::LockRanges {
                operation_id,
                ranges,
            } => Some(Transformation::LockRanges {
                operation_id: *operation_id,
                ranges: ranges.clone(),
            }),

            Self::UnlockRanges { operation_id } => Some(Transformation::UnlockRanges {
                operation_id: *operation_id,
            }),

            Self::UpdateNodeState { node_id, state } => Some(Transformation::UpdateNodeState {
                node_id: *node_id,
                state: *state,
            }),

            Self::AlterSchema {
                schema_version,
                description,
            } => Some(Transformation::SchemaChange {
                schema_version: *schema_version,
                description: description.clone(),
            }),

            Self::ForceSnapshot => Some(Transformation::ForceSnapshot),

            Self::TakeSnapshot => Some(Transformation::ForceSnapshot),

            _ => None,
        }
    }

    /// Construct an `ExtendedTransformation` from a base [`Transformation`].
    ///
    /// Every base variant has a corresponding extended variant, so this
    /// conversion is total.
    pub fn from_base(t: &Transformation) -> Self {
        match t {
            Transformation::Register {
                node_id,
                endpoint,
                dc,
                rack,
            } => Self::RegisterNode {
                node_id: *node_id,
                endpoint: *endpoint,
                dc: dc.clone(),
                rack: rack.clone(),
            },

            Transformation::Unregister { node_id } => Self::UnregisterNode { node_id: *node_id },

            Transformation::AssignTokens { node_id, tokens } => Self::AssignTokens {
                node_id: *node_id,
                tokens: tokens.clone(),
            },

            Transformation::UpdateNodeState { node_id, state } => Self::UpdateNodeState {
                node_id: *node_id,
                state: *state,
            },

            Transformation::SchemaChange {
                schema_version,
                description,
            } => Self::AlterSchema {
                schema_version: *schema_version,
                description: description.clone(),
            },

            Transformation::LockRanges {
                operation_id,
                ranges,
            } => Self::LockRanges {
                operation_id: *operation_id,
                ranges: ranges.clone(),
            },

            Transformation::UnlockRanges { operation_id } => Self::UnlockRanges {
                operation_id: *operation_id,
            },

            Transformation::ForceSnapshot => Self::ForceSnapshot,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

    fn nid(n: u128) -> NodeId {
        NodeId::from_uuid(Uuid::from_u128(n))
    }

    fn ep(port: u16) -> Endpoint {
        Endpoint::new(SocketAddr::V4(SocketAddrV4::new(
            Ipv4Addr::new(127, 0, 0, 1),
            port,
        )))
    }

    // ── Construction ─────────────────────────────────────────────────────

    #[test]
    fn construct_cms_lifecycle_variants() {
        let _ = ExtendedTransformation::InitializeCms;
        let _ = ExtendedTransformation::SealPeriod {
            epoch: Epoch::FIRST,
        };
        let _ = ExtendedTransformation::ForceSnapshot;
    }

    #[test]
    fn construct_join_variants() {
        let _ = ExtendedTransformation::PrepareJoin {
            node_id: nid(1),
            endpoint: ep(7001),
            dc: "dc1".into(),
            rack: "rack1".into(),
            tokens: vec![Token::from_raw(100)],
        };
        let _ = ExtendedTransformation::StartJoin { node_id: nid(1) };
        let _ = ExtendedTransformation::MidJoin { node_id: nid(1) };
        let _ = ExtendedTransformation::FinishJoin { node_id: nid(1) };
    }

    #[test]
    fn construct_leave_variants() {
        let _ = ExtendedTransformation::PrepareLeave { node_id: nid(1) };
        let _ = ExtendedTransformation::StartLeave { node_id: nid(1) };
        let _ = ExtendedTransformation::MidLeave { node_id: nid(1) };
        let _ = ExtendedTransformation::FinishLeave { node_id: nid(1) };
    }

    #[test]
    fn construct_move_variants() {
        let _ = ExtendedTransformation::PrepareMove {
            node_id: nid(1),
            new_tokens: vec![Token::from_raw(200)],
        };
        let _ = ExtendedTransformation::StartMove { node_id: nid(1) };
        let _ = ExtendedTransformation::MidMove { node_id: nid(1) };
        let _ = ExtendedTransformation::FinishMove { node_id: nid(1) };
    }

    #[test]
    fn construct_replace_variants() {
        let _ = ExtendedTransformation::PrepareReplace {
            old_node: nid(1),
            new_node: nid(2),
            new_endpoint: ep(7002),
            dc: "dc1".into(),
            rack: "rack1".into(),
        };
        let _ = ExtendedTransformation::StartReplace { new_node: nid(2) };
        let _ = ExtendedTransformation::MidReplace { new_node: nid(2) };
        let _ = ExtendedTransformation::FinishReplace {
            old_node: nid(1),
            new_node: nid(2),
        };
    }

    #[test]
    fn construct_schema_snapshot_cms_token_register_state() {
        let _ = ExtendedTransformation::AlterSchema {
            schema_version: Uuid::nil(),
            description: "create table".into(),
        };
        let _ = ExtendedTransformation::TakeSnapshot;
        let _ = ExtendedTransformation::RestoreSnapshot { epoch: Epoch(10) };
        let _ = ExtendedTransformation::AddCmsMember { node_id: nid(1) };
        let _ = ExtendedTransformation::RemoveCmsMember { node_id: nid(1) };
        let _ = ExtendedTransformation::AssignTokens {
            node_id: nid(1),
            tokens: vec![Token::from_raw(0)],
        };
        let _ = ExtendedTransformation::UnassignTokens { node_id: nid(1) };
        let _ = ExtendedTransformation::LockRanges {
            operation_id: Uuid::nil(),
            ranges: vec![TokenRange::new(Token::from_raw(0), Token::from_raw(100))],
        };
        let _ = ExtendedTransformation::UnlockRanges {
            operation_id: Uuid::nil(),
        };
        let _ = ExtendedTransformation::RegisterNode {
            node_id: nid(1),
            endpoint: ep(7001),
            dc: "dc1".into(),
            rack: "rack1".into(),
        };
        let _ = ExtendedTransformation::UnregisterNode { node_id: nid(1) };
        let _ = ExtendedTransformation::UpdateNodeState {
            node_id: nid(1),
            state: NodeState::Normal,
        };
    }

    // ── Kind classification ──────────────────────────────────────────────

    #[test]
    fn kind_cms_init() {
        assert_eq!(
            ExtendedTransformation::InitializeCms.kind(),
            TransformationKind::CmsInit,
        );
        assert_eq!(
            ExtendedTransformation::SealPeriod {
                epoch: Epoch::FIRST
            }
            .kind(),
            TransformationKind::CmsInit,
        );
        assert_eq!(
            ExtendedTransformation::ForceSnapshot.kind(),
            TransformationKind::CmsInit,
        );
    }

    #[test]
    fn kind_topology_change() {
        let topology_variants: Vec<ExtendedTransformation> = vec![
            ExtendedTransformation::PrepareJoin {
                node_id: nid(1),
                endpoint: ep(7001),
                dc: "dc1".into(),
                rack: "rack1".into(),
                tokens: vec![],
            },
            ExtendedTransformation::StartJoin { node_id: nid(1) },
            ExtendedTransformation::MidJoin { node_id: nid(1) },
            ExtendedTransformation::FinishJoin { node_id: nid(1) },
            ExtendedTransformation::PrepareLeave { node_id: nid(1) },
            ExtendedTransformation::StartLeave { node_id: nid(1) },
            ExtendedTransformation::MidLeave { node_id: nid(1) },
            ExtendedTransformation::FinishLeave { node_id: nid(1) },
            ExtendedTransformation::PrepareMove {
                node_id: nid(1),
                new_tokens: vec![],
            },
            ExtendedTransformation::StartMove { node_id: nid(1) },
            ExtendedTransformation::MidMove { node_id: nid(1) },
            ExtendedTransformation::FinishMove { node_id: nid(1) },
            ExtendedTransformation::PrepareReplace {
                old_node: nid(1),
                new_node: nid(2),
                new_endpoint: ep(7002),
                dc: "dc1".into(),
                rack: "rack1".into(),
            },
            ExtendedTransformation::StartReplace { new_node: nid(2) },
            ExtendedTransformation::MidReplace { new_node: nid(2) },
            ExtendedTransformation::FinishReplace {
                old_node: nid(1),
                new_node: nid(2),
            },
            ExtendedTransformation::RegisterNode {
                node_id: nid(1),
                endpoint: ep(7001),
                dc: "dc1".into(),
                rack: "rack1".into(),
            },
            ExtendedTransformation::UnregisterNode { node_id: nid(1) },
            ExtendedTransformation::UpdateNodeState {
                node_id: nid(1),
                state: NodeState::Normal,
            },
        ];
        for t in &topology_variants {
            assert_eq!(t.kind(), TransformationKind::TopologyChange, "{t:?}");
        }
    }

    #[test]
    fn kind_schema_change() {
        let t = ExtendedTransformation::AlterSchema {
            schema_version: Uuid::nil(),
            description: "test".into(),
        };
        assert_eq!(t.kind(), TransformationKind::SchemaChange);
    }

    #[test]
    fn kind_snapshot_op() {
        assert_eq!(
            ExtendedTransformation::TakeSnapshot.kind(),
            TransformationKind::SnapshotOp,
        );
        assert_eq!(
            ExtendedTransformation::RestoreSnapshot {
                epoch: Epoch::FIRST
            }
            .kind(),
            TransformationKind::SnapshotOp,
        );
    }

    #[test]
    fn kind_cms_reconfig() {
        assert_eq!(
            ExtendedTransformation::AddCmsMember { node_id: nid(1) }.kind(),
            TransformationKind::CmsReconfig,
        );
        assert_eq!(
            ExtendedTransformation::RemoveCmsMember { node_id: nid(1) }.kind(),
            TransformationKind::CmsReconfig,
        );
    }

    #[test]
    fn kind_token_op() {
        assert_eq!(
            ExtendedTransformation::AssignTokens {
                node_id: nid(1),
                tokens: vec![],
            }
            .kind(),
            TransformationKind::TokenOp,
        );
        assert_eq!(
            ExtendedTransformation::UnassignTokens { node_id: nid(1) }.kind(),
            TransformationKind::TokenOp,
        );
    }

    #[test]
    fn kind_sequence_op() {
        assert_eq!(
            ExtendedTransformation::LockRanges {
                operation_id: Uuid::nil(),
                ranges: vec![],
            }
            .kind(),
            TransformationKind::SequenceOp,
        );
        assert_eq!(
            ExtendedTransformation::UnlockRanges {
                operation_id: Uuid::nil()
            }
            .kind(),
            TransformationKind::SequenceOp,
        );
    }

    // ── Base conversion round-trip ───────────────────────────────────────

    #[test]
    fn to_base_register_node() {
        let ext = ExtendedTransformation::RegisterNode {
            node_id: nid(1),
            endpoint: ep(7001),
            dc: "dc1".into(),
            rack: "rack1".into(),
        };
        let base = ext.to_base().unwrap();
        assert!(matches!(base, Transformation::Register { .. }));
    }

    #[test]
    fn to_base_unregister_node() {
        let ext = ExtendedTransformation::UnregisterNode { node_id: nid(1) };
        let base = ext.to_base().unwrap();
        assert!(matches!(base, Transformation::Unregister { .. }));
    }

    #[test]
    fn to_base_assign_tokens() {
        let ext = ExtendedTransformation::AssignTokens {
            node_id: nid(1),
            tokens: vec![Token::from_raw(42)],
        };
        let base = ext.to_base().unwrap();
        assert!(matches!(base, Transformation::AssignTokens { .. }));
    }

    #[test]
    fn to_base_lock_ranges() {
        let op = Uuid::new_v4();
        let ext = ExtendedTransformation::LockRanges {
            operation_id: op,
            ranges: vec![TokenRange::new(Token::from_raw(10), Token::from_raw(20))],
        };
        let base = ext.to_base().unwrap();
        match base {
            Transformation::LockRanges {
                operation_id,
                ranges,
            } => {
                assert_eq!(operation_id, op);
                assert_eq!(ranges.len(), 1);
            }
            _ => panic!("expected LockRanges"),
        }
    }

    #[test]
    fn to_base_unlock_ranges() {
        let op = Uuid::new_v4();
        let ext = ExtendedTransformation::UnlockRanges { operation_id: op };
        assert!(matches!(
            ext.to_base(),
            Some(Transformation::UnlockRanges { operation_id }) if operation_id == op
        ));
    }

    #[test]
    fn to_base_update_node_state() {
        let ext = ExtendedTransformation::UpdateNodeState {
            node_id: nid(1),
            state: NodeState::Leaving,
        };
        let base = ext.to_base().unwrap();
        assert!(matches!(base, Transformation::UpdateNodeState { .. }));
    }

    #[test]
    fn to_base_alter_schema() {
        let sv = Uuid::new_v4();
        let ext = ExtendedTransformation::AlterSchema {
            schema_version: sv,
            description: "add column".into(),
        };
        let base = ext.to_base().unwrap();
        match base {
            Transformation::SchemaChange {
                schema_version,
                description,
            } => {
                assert_eq!(schema_version, sv);
                assert_eq!(description, "add column");
            }
            _ => panic!("expected SchemaChange"),
        }
    }

    #[test]
    fn to_base_force_snapshot() {
        let ext = ExtendedTransformation::ForceSnapshot;
        assert!(matches!(ext.to_base(), Some(Transformation::ForceSnapshot)));
    }

    #[test]
    fn to_base_take_snapshot() {
        let ext = ExtendedTransformation::TakeSnapshot;
        assert!(matches!(ext.to_base(), Some(Transformation::ForceSnapshot)));
    }

    #[test]
    fn to_base_returns_none_for_extended_only() {
        let none_variants: Vec<ExtendedTransformation> = vec![
            ExtendedTransformation::InitializeCms,
            ExtendedTransformation::SealPeriod {
                epoch: Epoch::FIRST,
            },
            ExtendedTransformation::PrepareJoin {
                node_id: nid(1),
                endpoint: ep(7001),
                dc: "dc1".into(),
                rack: "rack1".into(),
                tokens: vec![],
            },
            ExtendedTransformation::StartJoin { node_id: nid(1) },
            ExtendedTransformation::MidJoin { node_id: nid(1) },
            ExtendedTransformation::FinishJoin { node_id: nid(1) },
            ExtendedTransformation::PrepareLeave { node_id: nid(1) },
            ExtendedTransformation::AddCmsMember { node_id: nid(1) },
            ExtendedTransformation::RestoreSnapshot {
                epoch: Epoch::FIRST,
            },
            ExtendedTransformation::UnassignTokens { node_id: nid(1) },
        ];
        for t in &none_variants {
            assert!(t.to_base().is_none(), "expected None for {t:?}");
        }
    }

    // ── from_base for all base variants ──────────────────────────────────

    #[test]
    fn from_base_register() {
        let base = Transformation::Register {
            node_id: nid(1),
            endpoint: ep(7001),
            dc: "dc1".into(),
            rack: "rack1".into(),
        };
        let ext = ExtendedTransformation::from_base(&base);
        assert!(matches!(ext, ExtendedTransformation::RegisterNode { .. }));
        assert_eq!(ext.kind(), TransformationKind::TopologyChange);
    }

    #[test]
    fn from_base_unregister() {
        let base = Transformation::Unregister { node_id: nid(1) };
        let ext = ExtendedTransformation::from_base(&base);
        assert!(matches!(ext, ExtendedTransformation::UnregisterNode { .. }));
    }

    #[test]
    fn from_base_assign_tokens() {
        let base = Transformation::AssignTokens {
            node_id: nid(1),
            tokens: vec![Token::from_raw(42)],
        };
        let ext = ExtendedTransformation::from_base(&base);
        assert!(matches!(ext, ExtendedTransformation::AssignTokens { .. }));
    }

    #[test]
    fn from_base_update_node_state() {
        let base = Transformation::UpdateNodeState {
            node_id: nid(1),
            state: NodeState::Normal,
        };
        let ext = ExtendedTransformation::from_base(&base);
        assert!(matches!(
            ext,
            ExtendedTransformation::UpdateNodeState { .. }
        ));
    }

    #[test]
    fn from_base_schema_change() {
        let sv = Uuid::new_v4();
        let base = Transformation::SchemaChange {
            schema_version: sv,
            description: "drop table".into(),
        };
        let ext = ExtendedTransformation::from_base(&base);
        match ext {
            ExtendedTransformation::AlterSchema {
                schema_version,
                description,
            } => {
                assert_eq!(schema_version, sv);
                assert_eq!(description, "drop table");
            }
            _ => panic!("expected AlterSchema"),
        }
    }

    #[test]
    fn from_base_force_snapshot() {
        let base = Transformation::ForceSnapshot;
        let ext = ExtendedTransformation::from_base(&base);
        assert!(matches!(ext, ExtendedTransformation::ForceSnapshot));
    }

    #[test]
    fn from_base_lock_ranges() {
        let op = Uuid::new_v4();
        let base = Transformation::LockRanges {
            operation_id: op,
            ranges: vec![TokenRange::new(Token::from_raw(0), Token::from_raw(1))],
        };
        let ext = ExtendedTransformation::from_base(&base);
        match ext {
            ExtendedTransformation::LockRanges {
                operation_id,
                ranges,
            } => {
                assert_eq!(operation_id, op);
                assert_eq!(ranges.len(), 1);
            }
            _ => panic!("expected LockRanges"),
        }
    }

    #[test]
    fn from_base_unlock_ranges() {
        let op = Uuid::new_v4();
        let base = Transformation::UnlockRanges { operation_id: op };
        let ext = ExtendedTransformation::from_base(&base);
        assert!(matches!(
            ext,
            ExtendedTransformation::UnlockRanges { operation_id } if operation_id == op
        ));
    }

    // ── Round-trip: from_base(to_base(x)) == x for convertible variants ─

    #[test]
    fn round_trip_register() {
        let ext = ExtendedTransformation::RegisterNode {
            node_id: nid(1),
            endpoint: ep(7001),
            dc: "dc1".into(),
            rack: "rack1".into(),
        };
        let base = ext.to_base().unwrap();
        let back = ExtendedTransformation::from_base(&base);
        assert!(matches!(back, ExtendedTransformation::RegisterNode { .. }));
        // Verify fields preserved
        if let ExtendedTransformation::RegisterNode {
            node_id,
            endpoint,
            dc,
            rack,
        } = back
        {
            assert_eq!(node_id, nid(1));
            assert_eq!(endpoint, ep(7001));
            assert_eq!(dc, "dc1");
            assert_eq!(rack, "rack1");
        }
    }

    #[test]
    fn round_trip_unregister() {
        let ext = ExtendedTransformation::UnregisterNode { node_id: nid(5) };
        let base = ext.to_base().unwrap();
        let back = ExtendedTransformation::from_base(&base);
        if let ExtendedTransformation::UnregisterNode { node_id } = back {
            assert_eq!(node_id, nid(5));
        } else {
            panic!("expected UnregisterNode");
        }
    }

    #[test]
    fn round_trip_assign_tokens() {
        let tokens = vec![Token::from_raw(10), Token::from_raw(20)];
        let ext = ExtendedTransformation::AssignTokens {
            node_id: nid(3),
            tokens: tokens.clone(),
        };
        let base = ext.to_base().unwrap();
        let back = ExtendedTransformation::from_base(&base);
        if let ExtendedTransformation::AssignTokens { node_id, tokens: t } = back {
            assert_eq!(node_id, nid(3));
            assert_eq!(t, tokens);
        } else {
            panic!("expected AssignTokens");
        }
    }

    #[test]
    fn round_trip_update_node_state() {
        let ext = ExtendedTransformation::UpdateNodeState {
            node_id: nid(4),
            state: NodeState::Leaving,
        };
        let base = ext.to_base().unwrap();
        let back = ExtendedTransformation::from_base(&base);
        if let ExtendedTransformation::UpdateNodeState { node_id, state } = back {
            assert_eq!(node_id, nid(4));
            assert_eq!(state, NodeState::Leaving);
        } else {
            panic!("expected UpdateNodeState");
        }
    }

    #[test]
    fn round_trip_alter_schema() {
        let sv = Uuid::new_v4();
        let ext = ExtendedTransformation::AlterSchema {
            schema_version: sv,
            description: "alter table".into(),
        };
        let base = ext.to_base().unwrap();
        let back = ExtendedTransformation::from_base(&base);
        if let ExtendedTransformation::AlterSchema {
            schema_version,
            description,
        } = back
        {
            assert_eq!(schema_version, sv);
            assert_eq!(description, "alter table");
        } else {
            panic!("expected AlterSchema");
        }
    }

    #[test]
    fn round_trip_force_snapshot() {
        let ext = ExtendedTransformation::ForceSnapshot;
        let base = ext.to_base().unwrap();
        let back = ExtendedTransformation::from_base(&base);
        assert!(matches!(back, ExtendedTransformation::ForceSnapshot));
    }
}
