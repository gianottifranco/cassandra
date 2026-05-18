// Licensed under Apache License, Version 2.0.

//! Paxos to Accord migration state tracking.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.service.consensus.ConsensusKeyMigrationState`
//! - `org.apache.cassandra.service.accord.TableMigrationState`

use std::collections::HashMap;
use uuid::Uuid;

/// Per-key migration state between Paxos and Accord.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyMigrationState {
    /// Key is managed by Paxos.
    Paxos,
    /// Key is in migration -- both protocols may be active.
    Migrating,
    /// Key is managed by Accord.
    Accord,
}

/// Per-table migration tracking.
///
/// Tracks which partition keys have been migrated from Paxos to Accord.
pub struct TableMigrationState {
    /// Table identifier.
    table_id: Uuid,
    /// Per-key migration state. Keys not present default to Paxos.
    key_states: HashMap<Vec<u8>, KeyMigrationState>,
    /// Overall table migration progress (0.0 = all Paxos, 1.0 = all Accord).
    progress: f64,
}

impl TableMigrationState {
    pub fn new(table_id: Uuid) -> Self {
        Self {
            table_id,
            key_states: HashMap::new(),
            progress: 0.0,
        }
    }

    /// Get the migration state for a specific key.
    pub fn get_key_state(&self, key: &[u8]) -> KeyMigrationState {
        self.key_states
            .get(key)
            .copied()
            .unwrap_or(KeyMigrationState::Paxos)
    }

    /// Set the migration state for a key.
    pub fn set_key_state(&mut self, key: Vec<u8>, state: KeyMigrationState) {
        self.key_states.insert(key, state);
        self.recalculate_progress();
    }

    /// Mark a key as migrated to Accord.
    pub fn mark_migrated(&mut self, key: Vec<u8>) {
        self.set_key_state(key, KeyMigrationState::Accord);
    }

    /// Mark a key as currently migrating.
    pub fn mark_migrating(&mut self, key: Vec<u8>) {
        self.set_key_state(key, KeyMigrationState::Migrating);
    }

    /// Overall migration progress (0.0 to 1.0).
    pub fn progress(&self) -> f64 {
        self.progress
    }

    /// Table identifier.
    pub fn table_id(&self) -> Uuid {
        self.table_id
    }

    /// Check if migration is complete for all tracked keys.
    pub fn is_complete(&self) -> bool {
        !self.key_states.is_empty()
            && self
                .key_states
                .values()
                .all(|s| *s == KeyMigrationState::Accord)
    }

    /// Number of tracked keys.
    pub fn tracked_keys(&self) -> usize {
        self.key_states.len()
    }

    fn recalculate_progress(&mut self) {
        if self.key_states.is_empty() {
            self.progress = 0.0;
            return;
        }
        let migrated = self
            .key_states
            .values()
            .filter(|s| **s == KeyMigrationState::Accord)
            .count();
        self.progress = migrated as f64 / self.key_states.len() as f64;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_state_is_paxos() {
        let state = TableMigrationState::new(Uuid::new_v4());
        assert_eq!(state.get_key_state(b"any_key"), KeyMigrationState::Paxos);
    }

    #[test]
    fn migration_lifecycle() {
        let mut state = TableMigrationState::new(Uuid::new_v4());

        state.mark_migrating(b"key1".to_vec());
        assert_eq!(state.get_key_state(b"key1"), KeyMigrationState::Migrating);
        assert!(!state.is_complete());

        state.mark_migrated(b"key1".to_vec());
        assert_eq!(state.get_key_state(b"key1"), KeyMigrationState::Accord);
        assert!(state.is_complete());
    }

    #[test]
    fn progress_tracking() {
        let mut state = TableMigrationState::new(Uuid::new_v4());

        state.set_key_state(b"k1".to_vec(), KeyMigrationState::Paxos);
        state.set_key_state(b"k2".to_vec(), KeyMigrationState::Paxos);
        assert_eq!(state.progress(), 0.0);

        state.mark_migrated(b"k1".to_vec());
        assert!((state.progress() - 0.5).abs() < f64::EPSILON);

        state.mark_migrated(b"k2".to_vec());
        assert!((state.progress() - 1.0).abs() < f64::EPSILON);
        assert!(state.is_complete());
    }

    #[test]
    fn tracked_keys_count() {
        let mut state = TableMigrationState::new(Uuid::new_v4());
        assert_eq!(state.tracked_keys(), 0);
        state.mark_migrating(b"k1".to_vec());
        assert_eq!(state.tracked_keys(), 1);
    }
}
