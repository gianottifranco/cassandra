// Licensed under Apache License, Version 2.0.

//! Active compactions tracker with cancellation support.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.db.compaction.CompactionManager` (active compactions tracking)
//! - `org.apache.cassandra.db.compaction.CompactionInfo`

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use parking_lot::RwLock;
use uuid::Uuid;

use crate::compaction::errors::CompactionType;
use crate::sstable::format::SSTableId;

// ─── CancellationToken ──────────────────────────────────────────────────────

/// A cooperative cancellation token backed by an `Arc<AtomicBool>`.
///
/// Cloning produces a handle to the **same** underlying flag, so any holder
/// can observe or trigger cancellation.
#[derive(Debug, Clone)]
pub struct CancellationToken {
    cancelled: Arc<AtomicBool>,
}

impl CancellationToken {
    /// Create a new token in the non-cancelled state.
    pub fn new() -> Self {
        Self {
            cancelled: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Signal cancellation.
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::SeqCst);
    }

    /// Returns `true` if cancellation has been requested.
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::SeqCst)
    }
}

impl Default for CancellationToken {
    fn default() -> Self {
        Self::new()
    }
}

// ─── CompactionInfo ─────────────────────────────────────────────────────────

/// Descriptor for a single in-flight compaction.
#[derive(Debug, Clone)]
pub struct CompactionInfo {
    pub id: Uuid,
    pub compaction_type: CompactionType,
    pub sstable_ids: Vec<SSTableId>,
    pub started_at_ms: u64,
    pub cancel_token: CancellationToken,
}

// ─── ActiveCompactions ──────────────────────────────────────────────────────

/// Thread-safe registry of currently running compactions.
///
/// Ensures that no two concurrent compactions operate on the same SSTable.
pub struct ActiveCompactions {
    inner: RwLock<HashMap<Uuid, CompactionInfo>>,
}

impl ActiveCompactions {
    /// Create an empty tracker.
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(HashMap::new()),
        }
    }

    /// Register a new compaction.
    ///
    /// Returns `Err` if any of the SSTables listed in `info` are already
    /// participating in another active compaction.
    pub fn register(&self, info: CompactionInfo) -> Result<(), String> {
        let mut guard = self.inner.write();

        // Check for SSTable conflicts.
        for existing in guard.values() {
            for id in &info.sstable_ids {
                if existing.sstable_ids.contains(id) {
                    return Err(format!(
                        "SSTable {} is already in active compaction {}",
                        id, existing.id
                    ));
                }
            }
        }

        guard.insert(info.id, info);
        Ok(())
    }

    /// Remove a compaction from the active set, returning its info if found.
    pub fn unregister(&self, id: &Uuid) -> Option<CompactionInfo> {
        self.inner.write().remove(id)
    }

    /// Cancel a specific compaction by setting its cancellation token.
    ///
    /// Returns `true` if the compaction was found (and cancelled), `false`
    /// otherwise.
    pub fn cancel(&self, id: &Uuid) -> bool {
        let guard = self.inner.read();
        if let Some(info) = guard.get(id) {
            info.cancel_token.cancel();
            true
        } else {
            false
        }
    }

    /// Cancel every active compaction.
    pub fn cancel_all(&self) {
        let guard = self.inner.read();
        for info in guard.values() {
            info.cancel_token.cancel();
        }
    }

    /// Check whether any of the given SSTable IDs are currently in use.
    pub fn has_conflict(&self, sstable_ids: &[SSTableId]) -> bool {
        let guard = self.inner.read();
        for existing in guard.values() {
            for id in sstable_ids {
                if existing.sstable_ids.contains(id) {
                    return true;
                }
            }
        }
        false
    }

    /// Return a snapshot (clone) of all active compaction descriptors.
    pub fn snapshot(&self) -> Vec<CompactionInfo> {
        self.inner.read().values().cloned().collect()
    }

    /// Number of currently active compactions.
    pub fn count(&self) -> usize {
        self.inner.read().len()
    }
}

impl Default for ActiveCompactions {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_info(sstable_ids: Vec<SSTableId>) -> CompactionInfo {
        CompactionInfo {
            id: Uuid::new_v4(),
            compaction_type: CompactionType::Compaction,
            sstable_ids,
            started_at_ms: 0,
            cancel_token: CancellationToken::new(),
        }
    }

    #[test]
    fn register_and_unregister() {
        let tracker = ActiveCompactions::new();
        let info = make_info(vec![1, 2, 3]);
        let id = info.id;

        assert!(tracker.register(info).is_ok());
        assert_eq!(tracker.count(), 1);

        let removed = tracker.unregister(&id);
        assert!(removed.is_some());
        assert_eq!(tracker.count(), 0);
    }

    #[test]
    fn conflict_detection_overlapping_sstables() {
        let tracker = ActiveCompactions::new();

        let info1 = make_info(vec![1, 2, 3]);
        assert!(tracker.register(info1).is_ok());

        // Overlapping SSTable 2 should fail.
        let info2 = make_info(vec![2, 4, 5]);
        let result = tracker.register(info2);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("SSTable 2"));
    }

    #[test]
    fn no_conflict_disjoint_sstables() {
        let tracker = ActiveCompactions::new();

        let info1 = make_info(vec![1, 2]);
        assert!(tracker.register(info1).is_ok());

        let info2 = make_info(vec![3, 4]);
        assert!(tracker.register(info2).is_ok());
        assert_eq!(tracker.count(), 2);
    }

    #[test]
    fn has_conflict_check() {
        let tracker = ActiveCompactions::new();
        let info = make_info(vec![10, 20]);
        tracker.register(info).unwrap();

        assert!(tracker.has_conflict(&[20]));
        assert!(!tracker.has_conflict(&[30]));
    }

    #[test]
    fn cancel_sets_token() {
        let tracker = ActiveCompactions::new();
        let info = make_info(vec![1]);
        let id = info.id;
        let token = info.cancel_token.clone();

        tracker.register(info).unwrap();

        assert!(!token.is_cancelled());
        assert!(tracker.cancel(&id));
        assert!(token.is_cancelled());
    }

    #[test]
    fn cancel_returns_false_for_unknown_id() {
        let tracker = ActiveCompactions::new();
        assert!(!tracker.cancel(&Uuid::new_v4()));
    }

    #[test]
    fn cancel_all_cancels_everything() {
        let tracker = ActiveCompactions::new();

        let info1 = make_info(vec![1]);
        let token1 = info1.cancel_token.clone();
        tracker.register(info1).unwrap();

        let info2 = make_info(vec![2]);
        let token2 = info2.cancel_token.clone();
        tracker.register(info2).unwrap();

        tracker.cancel_all();
        assert!(token1.is_cancelled());
        assert!(token2.is_cancelled());
    }

    #[test]
    fn snapshot_returns_current_state() {
        let tracker = ActiveCompactions::new();

        let info1 = make_info(vec![1]);
        let info2 = make_info(vec![2]);
        let id1 = info1.id;
        let id2 = info2.id;

        tracker.register(info1).unwrap();
        tracker.register(info2).unwrap();

        let snap = tracker.snapshot();
        assert_eq!(snap.len(), 2);

        let ids: Vec<Uuid> = snap.iter().map(|i| i.id).collect();
        assert!(ids.contains(&id1));
        assert!(ids.contains(&id2));
    }
}
