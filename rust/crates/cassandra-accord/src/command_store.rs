// Licensed under Apache License, Version 2.0.

//! In-memory command store tracking transaction status through Accord phases.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.service.accord.AccordCommandStore`

use dashmap::DashMap;
use tracing::{debug, warn};

use crate::error::{AccordError, AccordResult};
use crate::types::{CommandStatus, Timestamp, Txn, TxnId};

/// A command entry in the store.
#[derive(Debug, Clone)]
pub struct CommandEntry {
    pub txn_id: TxnId,
    pub status: CommandStatus,
    pub execute_at: Timestamp,
    pub txn: Txn,
}

/// In-memory state machine for Accord command tracking.
///
/// Thread-safe via DashMap for concurrent access.
pub struct CommandStore {
    commands: DashMap<TxnId, CommandEntry>,
}

impl CommandStore {
    pub fn new() -> Self {
        Self {
            commands: DashMap::new(),
        }
    }

    /// Register a new transaction at PreAccepted status.
    pub fn pre_accept(&self, txn_id: TxnId, txn: Txn, execute_at: Timestamp) -> AccordResult<()> {
        let entry = CommandEntry {
            txn_id,
            status: CommandStatus::PreAccepted,
            execute_at,
            txn,
        };
        self.commands.insert(txn_id, entry);
        debug!(%txn_id, "Command pre-accepted");
        Ok(())
    }

    /// Transition a command to Accepted status.
    pub fn accept(&self, txn_id: TxnId, execute_at: Timestamp) -> AccordResult<()> {
        let mut entry = self.commands.get_mut(&txn_id)
            .ok_or_else(|| AccordError::Internal(format!("Unknown txn: {txn_id}")))?;

        if entry.status != CommandStatus::PreAccepted {
            return Err(AccordError::InvalidTransition {
                from: entry.status,
                to: CommandStatus::Accepted,
            });
        }

        entry.status = CommandStatus::Accepted;
        entry.execute_at = execute_at;
        debug!(%txn_id, "Command accepted");
        Ok(())
    }

    /// Transition a command to Committed status.
    pub fn commit(&self, txn_id: TxnId, execute_at: Timestamp) -> AccordResult<()> {
        let mut entry = self.commands.get_mut(&txn_id)
            .ok_or_else(|| AccordError::Internal(format!("Unknown txn: {txn_id}")))?;

        if !matches!(entry.status, CommandStatus::PreAccepted | CommandStatus::Accepted) {
            return Err(AccordError::InvalidTransition {
                from: entry.status,
                to: CommandStatus::Committed,
            });
        }

        entry.status = CommandStatus::Committed;
        entry.execute_at = execute_at;
        debug!(%txn_id, "Command committed");
        Ok(())
    }

    /// Transition a command to Applied status.
    pub fn apply(&self, txn_id: TxnId) -> AccordResult<()> {
        let mut entry = self.commands.get_mut(&txn_id)
            .ok_or_else(|| AccordError::Internal(format!("Unknown txn: {txn_id}")))?;

        if entry.status != CommandStatus::Committed {
            return Err(AccordError::InvalidTransition {
                from: entry.status,
                to: CommandStatus::Applied,
            });
        }

        entry.status = CommandStatus::Applied;
        debug!(%txn_id, "Command applied");
        Ok(())
    }

    /// Invalidate a transaction.
    pub fn invalidate(&self, txn_id: TxnId) -> AccordResult<()> {
        let mut entry = self.commands.get_mut(&txn_id)
            .ok_or_else(|| AccordError::Internal(format!("Unknown txn: {txn_id}")))?;

        if entry.status.is_terminal() {
            warn!(%txn_id, status = %entry.status, "Cannot invalidate terminal command");
            return Err(AccordError::InvalidTransition {
                from: entry.status,
                to: CommandStatus::Invalidated,
            });
        }

        entry.status = CommandStatus::Invalidated;
        debug!(%txn_id, "Command invalidated");
        Ok(())
    }

    /// Get the current status of a transaction.
    pub fn get_status(&self, txn_id: &TxnId) -> Option<CommandStatus> {
        self.commands.get(txn_id).map(|e| e.status)
    }

    /// Get a full command entry.
    pub fn get(&self, txn_id: &TxnId) -> Option<CommandEntry> {
        self.commands.get(txn_id).map(|e| e.clone())
    }

    /// Number of tracked commands.
    pub fn len(&self) -> usize {
        self.commands.len()
    }

    pub fn is_empty(&self) -> bool {
        self.commands.is_empty()
    }
}

impl Default for CommandStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Keys;

    fn test_txn_id() -> TxnId {
        TxnId::with_timestamp(100, uuid::Uuid::nil(), 0)
    }

    fn test_txn() -> Txn {
        Txn {
            keys: Keys::single(b"key1".to_vec()),
            mutation: b"INSERT data".to_vec(),
            keyspace: "ks".to_string(),
        }
    }

    #[test]
    fn full_lifecycle() {
        let store = CommandStore::new();
        let txn_id = test_txn_id();
        let ts = Timestamp(100);

        store.pre_accept(txn_id, test_txn(), ts).unwrap();
        assert_eq!(store.get_status(&txn_id), Some(CommandStatus::PreAccepted));

        store.accept(txn_id, ts).unwrap();
        assert_eq!(store.get_status(&txn_id), Some(CommandStatus::Accepted));

        store.commit(txn_id, ts).unwrap();
        assert_eq!(store.get_status(&txn_id), Some(CommandStatus::Committed));

        store.apply(txn_id).unwrap();
        assert_eq!(store.get_status(&txn_id), Some(CommandStatus::Applied));
    }

    #[test]
    fn invalid_transition_rejected() {
        let store = CommandStore::new();
        let txn_id = test_txn_id();
        let ts = Timestamp(100);

        store.pre_accept(txn_id, test_txn(), ts).unwrap();
        // Can't apply from PreAccepted (must commit first)
        let result = store.apply(txn_id);
        assert!(result.is_err());
    }

    #[test]
    fn invalidate_non_terminal() {
        let store = CommandStore::new();
        let txn_id = test_txn_id();
        let ts = Timestamp(100);

        store.pre_accept(txn_id, test_txn(), ts).unwrap();
        store.invalidate(txn_id).unwrap();
        assert_eq!(store.get_status(&txn_id), Some(CommandStatus::Invalidated));
    }

    #[test]
    fn cannot_invalidate_applied() {
        let store = CommandStore::new();
        let txn_id = test_txn_id();
        let ts = Timestamp(100);

        store.pre_accept(txn_id, test_txn(), ts).unwrap();
        store.accept(txn_id, ts).unwrap();
        store.commit(txn_id, ts).unwrap();
        store.apply(txn_id).unwrap();

        let result = store.invalidate(txn_id);
        assert!(result.is_err());
    }

    #[test]
    fn unknown_txn_returns_none() {
        let store = CommandStore::new();
        let txn_id = test_txn_id();
        assert_eq!(store.get_status(&txn_id), None);
    }

    #[test]
    fn len_tracks_commands() {
        let store = CommandStore::new();
        assert!(store.is_empty());
        store.pre_accept(test_txn_id(), test_txn(), Timestamp(100)).unwrap();
        assert_eq!(store.len(), 1);
    }
}
