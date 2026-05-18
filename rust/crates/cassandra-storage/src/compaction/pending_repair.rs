// Licensed under Apache License, Version 2.0.

//! Pending repair session tracking for anticompaction.
//!
//! Tracks which SSTables belong to in-progress repair sessions so that
//! compaction can avoid merging them until the repair is finalized or failed.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.service.PendingRepairManager`
//! - `org.apache.cassandra.repair.consistent.LocalSession`

use std::collections::{HashMap, HashSet};

use parking_lot::RwLock;
use uuid::Uuid;

use crate::sstable::format::SSTableId;

// ─── RepairState ────────────────────────────────────────────────────────────

/// State of a repair session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RepairState {
    Pending,
    Finalized,
    Failed,
}

// ─── RepairSession ──────────────────────────────────────────────────────────

/// A single repair session with its associated SSTables.
#[derive(Debug, Clone)]
pub struct RepairSession {
    pub id: Uuid,
    pub state: RepairState,
    pub sstable_ids: HashSet<SSTableId>,
    pub started_at_ms: u64,
}

// ─── PendingRepairManager ───────────────────────────────────────────────────

/// Manages repair sessions and their associated SSTables.
pub struct PendingRepairManager {
    sessions: RwLock<HashMap<Uuid, RepairSession>>,
}

impl PendingRepairManager {
    /// Create a new empty manager.
    pub fn new() -> Self {
        Self {
            sessions: RwLock::new(HashMap::new()),
        }
    }

    /// Register a new pending repair session.
    pub fn register_pending(&self, session_id: Uuid) -> Result<(), String> {
        let mut sessions = self.sessions.write();
        if sessions.contains_key(&session_id) {
            return Err(format!("repair session {} already exists", session_id));
        }
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        sessions.insert(
            session_id,
            RepairSession {
                id: session_id,
                state: RepairState::Pending,
                sstable_ids: HashSet::new(),
                started_at_ms: now_ms,
            },
        );
        Ok(())
    }

    /// Add an SSTable to a repair session.
    pub fn add_sstable(&self, session_id: &Uuid, sstable_id: SSTableId) -> Result<(), String> {
        let mut sessions = self.sessions.write();
        let session = sessions
            .get_mut(session_id)
            .ok_or_else(|| format!("repair session {} not found", session_id))?;
        session.sstable_ids.insert(sstable_id);
        Ok(())
    }

    /// Remove an SSTable from a repair session.
    pub fn remove_sstable(&self, session_id: &Uuid, sstable_id: &SSTableId) -> Result<(), String> {
        let mut sessions = self.sessions.write();
        let session = sessions
            .get_mut(session_id)
            .ok_or_else(|| format!("repair session {} not found", session_id))?;
        if !session.sstable_ids.remove(sstable_id) {
            return Err(format!(
                "SSTable {} not found in repair session {}",
                sstable_id, session_id
            ));
        }
        Ok(())
    }

    /// Finalize a repair session. Returns the set of SSTables that were in the session.
    pub fn finalize(&self, session_id: &Uuid) -> Result<HashSet<SSTableId>, String> {
        let mut sessions = self.sessions.write();
        let session = sessions
            .get_mut(session_id)
            .ok_or_else(|| format!("repair session {} not found", session_id))?;
        if session.state != RepairState::Pending {
            return Err(format!(
                "repair session {} is {:?}, expected Pending",
                session_id, session.state
            ));
        }
        session.state = RepairState::Finalized;
        Ok(session.sstable_ids.clone())
    }

    /// Mark a repair session as failed.
    pub fn fail(&self, session_id: &Uuid) -> Result<(), String> {
        let mut sessions = self.sessions.write();
        let session = sessions
            .get_mut(session_id)
            .ok_or_else(|| format!("repair session {} not found", session_id))?;
        if session.state != RepairState::Pending {
            return Err(format!(
                "repair session {} is {:?}, expected Pending",
                session_id, session.state
            ));
        }
        session.state = RepairState::Failed;
        Ok(())
    }

    /// Returns all SSTable IDs across all sessions that are still in `Pending` state.
    pub fn get_pending_sstables(&self) -> HashSet<SSTableId> {
        let sessions = self.sessions.read();
        sessions
            .values()
            .filter(|s| s.state == RepairState::Pending)
            .flat_map(|s| s.sstable_ids.iter().copied())
            .collect()
    }

    /// Returns `true` if the given SSTable is in any pending repair session.
    pub fn is_pending(&self, sstable_id: &SSTableId) -> bool {
        let sessions = self.sessions.read();
        sessions
            .values()
            .any(|s| s.state == RepairState::Pending && s.sstable_ids.contains(sstable_id))
    }

    /// Number of tracked repair sessions (all states).
    pub fn session_count(&self) -> usize {
        self.sessions.read().len()
    }
}

impl Default for PendingRepairManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn register_add_and_finalize() {
        let mgr = PendingRepairManager::new();
        let sid = Uuid::new_v4();

        mgr.register_pending(sid).unwrap();
        mgr.add_sstable(&sid, 1).unwrap();
        mgr.add_sstable(&sid, 2).unwrap();
        mgr.add_sstable(&sid, 3).unwrap();

        assert!(mgr.is_pending(&1));
        assert!(mgr.is_pending(&2));

        let finalized = mgr.finalize(&sid).unwrap();
        assert_eq!(finalized.len(), 3);
        assert!(finalized.contains(&1));
        assert!(finalized.contains(&2));
        assert!(finalized.contains(&3));

        // After finalization, SSTables are no longer pending.
        assert!(!mgr.is_pending(&1));
    }

    #[test]
    fn fail_sets_state() {
        let mgr = PendingRepairManager::new();
        let sid = Uuid::new_v4();

        mgr.register_pending(sid).unwrap();
        mgr.add_sstable(&sid, 10).unwrap();

        assert!(mgr.is_pending(&10));
        mgr.fail(&sid).unwrap();
        assert!(!mgr.is_pending(&10));
    }

    #[test]
    fn duplicate_register_fails() {
        let mgr = PendingRepairManager::new();
        let sid = Uuid::new_v4();

        mgr.register_pending(sid).unwrap();
        let err = mgr.register_pending(sid).unwrap_err();
        assert!(err.contains("already exists"));
    }

    #[test]
    fn get_pending_sstables_aggregates_across_sessions() {
        let mgr = PendingRepairManager::new();
        let sid1 = Uuid::new_v4();
        let sid2 = Uuid::new_v4();

        mgr.register_pending(sid1).unwrap();
        mgr.register_pending(sid2).unwrap();

        mgr.add_sstable(&sid1, 1).unwrap();
        mgr.add_sstable(&sid1, 2).unwrap();
        mgr.add_sstable(&sid2, 3).unwrap();
        mgr.add_sstable(&sid2, 4).unwrap();

        let pending = mgr.get_pending_sstables();
        assert_eq!(pending.len(), 4);
        assert!(pending.contains(&1));
        assert!(pending.contains(&2));
        assert!(pending.contains(&3));
        assert!(pending.contains(&4));
    }

    #[test]
    fn concurrent_sessions_work_independently() {
        let mgr = PendingRepairManager::new();
        let sid1 = Uuid::new_v4();
        let sid2 = Uuid::new_v4();

        mgr.register_pending(sid1).unwrap();
        mgr.register_pending(sid2).unwrap();

        mgr.add_sstable(&sid1, 1).unwrap();
        mgr.add_sstable(&sid2, 2).unwrap();

        // Finalize session 1.
        let finalized = mgr.finalize(&sid1).unwrap();
        assert_eq!(finalized.len(), 1);
        assert!(finalized.contains(&1));

        // Session 2 still pending.
        assert!(mgr.is_pending(&2));
        assert!(!mgr.is_pending(&1));

        // Fail session 2.
        mgr.fail(&sid2).unwrap();
        assert!(!mgr.is_pending(&2));

        assert_eq!(mgr.session_count(), 2);
    }

    #[test]
    fn remove_sstable_works() {
        let mgr = PendingRepairManager::new();
        let sid = Uuid::new_v4();

        mgr.register_pending(sid).unwrap();
        mgr.add_sstable(&sid, 1).unwrap();
        mgr.add_sstable(&sid, 2).unwrap();

        assert!(mgr.is_pending(&1));
        mgr.remove_sstable(&sid, &1).unwrap();
        assert!(!mgr.is_pending(&1));
        assert!(mgr.is_pending(&2));
    }

    #[test]
    fn remove_nonexistent_sstable_fails() {
        let mgr = PendingRepairManager::new();
        let sid = Uuid::new_v4();

        mgr.register_pending(sid).unwrap();
        let err = mgr.remove_sstable(&sid, &99).unwrap_err();
        assert!(err.contains("not found"));
    }

    #[test]
    fn add_to_nonexistent_session_fails() {
        let mgr = PendingRepairManager::new();
        let sid = Uuid::new_v4();

        let err = mgr.add_sstable(&sid, 1).unwrap_err();
        assert!(err.contains("not found"));
    }
}
