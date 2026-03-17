// Licensed under Apache License, Version 2.0.

//! Durable transaction journal for Accord.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.service.accord.AccordJournal`

use std::collections::VecDeque;
use std::sync::Mutex;

use serde::{Deserialize, Serialize};
use tracing::{debug, info};

use crate::error::AccordResult;
use crate::types::{CommandStatus, Timestamp, TxnId};

/// A journal entry recording a status transition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JournalEntry {
    pub txn_id: TxnId,
    pub status: CommandStatus,
    pub execute_at: Timestamp,
    pub mutation: Vec<u8>,
    /// Monotonic sequence number for ordering within the journal.
    pub sequence: u64,
}

/// Durable transaction journal.
///
/// Records all status transitions for recovery. In production, this would
/// write to a commit log or dedicated journal file. Currently in-memory
/// with replay support.
pub struct AccordJournal {
    entries: Mutex<VecDeque<JournalEntry>>,
    next_sequence: Mutex<u64>,
    enabled: bool,
}

impl AccordJournal {
    pub fn new(enabled: bool) -> Self {
        Self {
            entries: Mutex::new(VecDeque::new()),
            next_sequence: Mutex::new(0),
            enabled,
        }
    }

    /// Append a journal entry.
    pub fn write(
        &self,
        txn_id: TxnId,
        status: CommandStatus,
        execute_at: Timestamp,
        mutation: Vec<u8>,
    ) -> AccordResult<u64> {
        if !self.enabled {
            return Ok(0);
        }

        let mut seq = self.next_sequence.lock().unwrap();
        let sequence = *seq;
        *seq += 1;
        drop(seq);

        let entry = JournalEntry {
            txn_id,
            status,
            execute_at,
            mutation,
            sequence,
        };

        debug!(%txn_id, %status, sequence, "Journal write");

        self.entries.lock().unwrap().push_back(entry);
        Ok(sequence)
    }

    /// Read all journal entries (for recovery).
    pub fn read_all(&self) -> Vec<JournalEntry> {
        self.entries.lock().unwrap().iter().cloned().collect()
    }

    /// Replay journal entries, returning them grouped by TxnId.
    ///
    /// Returns entries in journal order, which can be used to reconstruct
    /// the CommandStore state on startup.
    pub fn replay(&self) -> Vec<JournalEntry> {
        let entries = self.read_all();
        info!(count = entries.len(), "Journal replay");
        entries
    }

    /// Truncate entries older than the given sequence number.
    pub fn truncate_before(&self, sequence: u64) -> usize {
        let mut entries = self.entries.lock().unwrap();
        let before = entries.len();
        entries.retain(|e| e.sequence >= sequence);
        let removed = before - entries.len();
        if removed > 0 {
            debug!(removed, "Journal truncated");
        }
        removed
    }

    /// Number of entries in the journal.
    pub fn len(&self) -> usize {
        self.entries.lock().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.lock().unwrap().is_empty()
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_txn_id(ts: i64) -> TxnId {
        TxnId::with_timestamp(ts, uuid::Uuid::nil(), 0)
    }

    #[test]
    fn write_and_read() {
        let journal = AccordJournal::new(true);
        journal.write(
            test_txn_id(100),
            CommandStatus::PreAccepted,
            Timestamp(100),
            b"data".to_vec(),
        ).unwrap();

        let entries = journal.read_all();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].txn_id.timestamp, 100);
    }

    #[test]
    fn sequence_monotonic() {
        let journal = AccordJournal::new(true);
        let s1 = journal.write(test_txn_id(1), CommandStatus::PreAccepted, Timestamp(1), vec![]).unwrap();
        let s2 = journal.write(test_txn_id(2), CommandStatus::PreAccepted, Timestamp(2), vec![]).unwrap();
        assert!(s2 > s1);
    }

    #[test]
    fn disabled_journal_noop() {
        let journal = AccordJournal::new(false);
        journal.write(test_txn_id(1), CommandStatus::PreAccepted, Timestamp(1), vec![]).unwrap();
        assert!(journal.is_empty());
    }

    #[test]
    fn truncate_removes_old_entries() {
        let journal = AccordJournal::new(true);
        for i in 0..5 {
            journal.write(test_txn_id(i), CommandStatus::PreAccepted, Timestamp(i), vec![]).unwrap();
        }
        assert_eq!(journal.len(), 5);

        let removed = journal.truncate_before(3);
        assert_eq!(removed, 3);
        assert_eq!(journal.len(), 2);
    }

    #[test]
    fn replay_returns_all() {
        let journal = AccordJournal::new(true);
        journal.write(test_txn_id(1), CommandStatus::PreAccepted, Timestamp(1), vec![]).unwrap();
        journal.write(test_txn_id(1), CommandStatus::Committed, Timestamp(1), vec![]).unwrap();

        let entries = journal.replay();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].status, CommandStatus::PreAccepted);
        assert_eq!(entries[1].status, CommandStatus::Committed);
    }
}
