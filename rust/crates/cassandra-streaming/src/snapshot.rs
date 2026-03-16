// Licensed under Apache License, Version 2.0.

//! Snapshot-based streaming: links streaming transfers to storage snapshots.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.db.streaming.CassandraOutgoingFile`
//! - `org.apache.cassandra.db.streaming.CassandraStreamWriter`
//!
//! When streaming outgoing data, Cassandra takes a snapshot of the relevant
//! SSTables to ensure consistency during transfer. This module manages
//! the lifecycle of those references.

use std::fmt;
use std::time::Instant;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Reference to a snapshot taken for streaming.
///
/// Associates a streaming session/transfer with a storage snapshot
/// so that SSTables remain pinned (not compacted away) during transfer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotTransferRef {
    /// Unique identifier for this snapshot reference.
    pub id: Uuid,
    /// Streaming session this snapshot supports.
    pub session_id: Uuid,
    /// Snapshot tag (used by storage engine to identify the snapshot).
    pub snapshot_tag: String,
    /// Keyspace covered.
    pub keyspace: String,
    /// Table covered.
    pub table: String,
    /// Whether the snapshot has been released.
    pub released: bool,
    /// Size of the snapshot in bytes (estimated).
    pub size_bytes: u64,
    /// When the snapshot was created.
    #[serde(skip)]
    pub created_at: Option<Instant>,
}

impl SnapshotTransferRef {
    /// Create a new snapshot reference for streaming.
    pub fn new(session_id: Uuid, keyspace: impl Into<String>, table: impl Into<String>) -> Self {
        let tag = format!("stream-{session_id}-{}", Uuid::new_v4().as_simple());
        Self {
            id: Uuid::new_v4(),
            session_id,
            snapshot_tag: tag,
            keyspace: keyspace.into(),
            table: table.into(),
            released: false,
            size_bytes: 0,
            created_at: Some(Instant::now()),
        }
    }

    /// Mark this snapshot as released (storage engine can clean up).
    pub fn release(&mut self) {
        self.released = true;
    }

    /// Whether this snapshot is still active (not released).
    pub fn is_active(&self) -> bool {
        !self.released
    }

    /// Duration since the snapshot was taken.
    pub fn age(&self) -> std::time::Duration {
        self.created_at.map(|t| t.elapsed()).unwrap_or_default()
    }
}

impl fmt::Display for SnapshotTransferRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Snapshot[{}:{}, tag={}, session={}, {}]",
            self.keyspace,
            self.table,
            self.snapshot_tag,
            self.session_id,
            if self.released { "released" } else { "active" }
        )
    }
}

/// Manager for streaming snapshot references.
///
/// Tracks all active snapshots and provides cleanup facilities.
#[derive(Debug, Default)]
pub struct SnapshotManager {
    refs: parking_lot::RwLock<Vec<SnapshotTransferRef>>,
}

impl SnapshotManager {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a new snapshot reference.
    pub fn register(&self, snap_ref: SnapshotTransferRef) {
        self.refs.write().push(snap_ref);
    }

    /// Release all snapshots for a given session.
    pub fn release_for_session(&self, session_id: &Uuid) -> usize {
        let mut refs = self.refs.write();
        let mut count = 0;
        for r in refs.iter_mut() {
            if r.session_id == *session_id && !r.released {
                r.release();
                count += 1;
            }
        }
        count
    }

    /// Get all active snapshot tags (for storage engine cleanup).
    pub fn active_tags(&self) -> Vec<String> {
        self.refs
            .read()
            .iter()
            .filter(|r| r.is_active())
            .map(|r| r.snapshot_tag.clone())
            .collect()
    }

    /// Remove released snapshot references.
    pub fn gc_released(&self) -> usize {
        let mut refs = self.refs.write();
        let before = refs.len();
        refs.retain(|r| r.is_active());
        before - refs.len()
    }

    /// Number of active snapshot references.
    pub fn active_count(&self) -> usize {
        self.refs.read().iter().filter(|r| r.is_active()).count()
    }

    /// Total size of active snapshots.
    pub fn active_size_bytes(&self) -> u64 {
        self.refs
            .read()
            .iter()
            .filter(|r| r.is_active())
            .map(|r| r.size_bytes)
            .sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_ref_lifecycle() {
        let session_id = Uuid::new_v4();
        let mut snap = SnapshotTransferRef::new(session_id, "ks", "t1");

        assert!(snap.is_active());
        assert!(!snap.released);
        assert!(snap.snapshot_tag.starts_with("stream-"));

        snap.release();
        assert!(!snap.is_active());
        assert!(snap.released);
    }

    #[test]
    fn snapshot_ref_display() {
        let snap = SnapshotTransferRef::new(Uuid::new_v4(), "my_ks", "my_table");
        let display = format!("{snap}");
        assert!(display.contains("my_ks"));
        assert!(display.contains("my_table"));
        assert!(display.contains("active"));
    }

    #[test]
    fn snapshot_manager_register_and_release() {
        let mgr = SnapshotManager::new();
        let sid = Uuid::new_v4();

        let s1 = SnapshotTransferRef::new(sid, "ks", "t1");
        let s2 = SnapshotTransferRef::new(sid, "ks", "t2");
        let other_sid = Uuid::new_v4();
        let s3 = SnapshotTransferRef::new(other_sid, "ks", "t3");

        mgr.register(s1);
        mgr.register(s2);
        mgr.register(s3);

        assert_eq!(mgr.active_count(), 3);

        let released = mgr.release_for_session(&sid);
        assert_eq!(released, 2);
        assert_eq!(mgr.active_count(), 1);
    }

    #[test]
    fn snapshot_manager_gc() {
        let mgr = SnapshotManager::new();
        let sid = Uuid::new_v4();

        mgr.register(SnapshotTransferRef::new(sid, "ks", "t1"));
        mgr.register(SnapshotTransferRef::new(sid, "ks", "t2"));

        mgr.release_for_session(&sid);
        let removed = mgr.gc_released();
        assert_eq!(removed, 2);
        assert_eq!(mgr.active_count(), 0);
    }

    #[test]
    fn snapshot_manager_active_tags() {
        let mgr = SnapshotManager::new();
        let sid = Uuid::new_v4();

        let s1 = SnapshotTransferRef::new(sid, "ks", "t1");
        let tag1 = s1.snapshot_tag.clone();
        mgr.register(s1);

        let tags = mgr.active_tags();
        assert_eq!(tags.len(), 1);
        assert_eq!(tags[0], tag1);
    }

    #[test]
    fn snapshot_manager_size_tracking() {
        let mgr = SnapshotManager::new();
        let sid = Uuid::new_v4();

        let mut s1 = SnapshotTransferRef::new(sid, "ks", "t1");
        s1.size_bytes = 1_000_000;
        let mut s2 = SnapshotTransferRef::new(sid, "ks", "t2");
        s2.size_bytes = 2_000_000;

        mgr.register(s1);
        mgr.register(s2);

        assert_eq!(mgr.active_size_bytes(), 3_000_000);
    }
}
