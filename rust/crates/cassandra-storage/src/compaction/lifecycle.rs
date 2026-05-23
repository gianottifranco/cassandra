// Licensed under Apache License, Version 2.0.

//! Crash-safe SSTable replacement via lifecycle transactions.
//!
//! A `LifecycleTransaction` records staging (new SSTables) and obsoleting
//! (old SSTables) in a write-ahead log so that incomplete operations can be
//! detected and rolled back after a crash.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.db.lifecycle.LifecycleTransaction`
//! - `org.apache.cassandra.db.lifecycle.LogFile`
//! - `org.apache.cassandra.db.lifecycle.LogRecord`

use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::sstable::format::SSTableId;

use super::errors::CompactionError;

type Result<T> = std::result::Result<T, CompactionError>;

// ─── Transaction State ───────────────────────────────────────────────────────

/// Current state of a lifecycle transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionState {
    Active,
    Committed,
    Aborted,
}

// ─── Log Entries ─────────────────────────────────────────────────────────────

/// A single action recorded in the transaction log.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TransactionAction {
    Stage { sstable_id: SSTableId },
    Obsolete { sstable_id: SSTableId },
    Commit,
    Abort,
}

/// A timestamped log entry written to the transaction log file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionLogEntry {
    pub timestamp_ms: u64,
    pub action: TransactionAction,
}

// ─── LifecycleTransaction ────────────────────────────────────────────────────

/// Crash-safe transaction that tracks SSTable staging and obsoleting.
///
/// Writes a JSON-lines log to `txnlog-{uuid}.log` so that incomplete
/// transactions can be recovered after a crash.
#[derive(Debug)]
pub struct LifecycleTransaction {
    pub id: Uuid,
    state: TransactionState,
    staged: Vec<SSTableId>,
    obsoleted: Vec<SSTableId>,
    log_path: PathBuf,
    log_entries: Vec<TransactionLogEntry>,
}

impl LifecycleTransaction {
    /// Create a new transaction, writing the log file in `log_dir`.
    pub fn new(log_dir: &Path) -> Result<Self> {
        let id = Uuid::new_v4();
        let log_path = log_dir.join(format!("txnlog-{}.log", id));
        // Create the log file (touch it so it exists for later appends).
        File::create(&log_path)?;
        Ok(Self {
            id,
            state: TransactionState::Active,
            staged: Vec::new(),
            obsoleted: Vec::new(),
            log_path,
            log_entries: Vec::new(),
        })
    }

    /// Stage a new SSTable (one that is being added by compaction).
    pub fn stage(&mut self, id: SSTableId) -> Result<()> {
        self.ensure_active()?;
        self.staged.push(id);
        self.write_entry(TransactionAction::Stage { sstable_id: id })
    }

    /// Mark an SSTable as obsoleted (one that will be removed after commit).
    pub fn obsolete(&mut self, id: SSTableId) -> Result<()> {
        self.ensure_active()?;
        self.obsoleted.push(id);
        self.write_entry(TransactionAction::Obsolete { sstable_id: id })
    }

    /// Commit the transaction — all staged SSTables become live, all obsoleted
    /// ones may be removed.
    pub fn commit(&mut self) -> Result<()> {
        self.ensure_active()?;
        self.write_entry(TransactionAction::Commit)?;
        self.state = TransactionState::Committed;
        Ok(())
    }

    /// Abort the transaction — staged SSTables should be cleaned up by the caller.
    pub fn abort(&mut self) -> Result<()> {
        self.ensure_active()?;
        self.write_entry(TransactionAction::Abort)?;
        self.state = TransactionState::Aborted;
        self.staged.clear();
        Ok(())
    }

    /// IDs of SSTables staged (added) in this transaction.
    pub fn staged_ids(&self) -> &[SSTableId] {
        &self.staged
    }

    /// IDs of SSTables obsoleted (removed) in this transaction.
    pub fn obsoleted_ids(&self) -> &[SSTableId] {
        &self.obsoleted
    }

    /// Current transaction state.
    pub fn state(&self) -> TransactionState {
        self.state
    }

    /// Path to the transaction log file.
    pub fn log_path(&self) -> &Path {
        &self.log_path
    }

    // ── internal helpers ─────────────────────────────────────────────────

    fn ensure_active(&self) -> Result<()> {
        if self.state != TransactionState::Active {
            return Err(CompactionError::InvalidState(format!(
                "transaction {} is {:?}, expected Active",
                self.id, self.state
            )));
        }
        Ok(())
    }

    fn now_ms() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
    }

    fn write_entry(&mut self, action: TransactionAction) -> Result<()> {
        let entry = TransactionLogEntry {
            timestamp_ms: Self::now_ms(),
            action,
        };
        let mut file = OpenOptions::new().append(true).open(&self.log_path)?;
        let line = serde_json::to_string(&entry).map_err(|e| {
            CompactionError::InvalidState(format!("failed to serialize log entry: {e}"))
        })?;
        writeln!(file, "{}", line)?;
        self.log_entries.push(entry);
        Ok(())
    }
}

impl Drop for LifecycleTransaction {
    fn drop(&mut self) {
        if self.state == TransactionState::Active {
            tracing::warn!(
                txn_id = %self.id,
                "lifecycle transaction dropped while still active — auto-aborting"
            );
            if let Err(e) = self.abort() {
                tracing::warn!(
                    txn_id = %self.id,
                    error = %e,
                    "failed to auto-abort lifecycle transaction on drop"
                );
            }
        }
    }
}

// ─── Recovery ────────────────────────────────────────────────────────────────

/// Scan `log_dir` for incomplete transaction logs and replay them.
///
/// Returns transactions that were neither committed nor aborted so the caller
/// can decide how to clean them up (typically by aborting them).
pub fn recover_incomplete_transactions(log_dir: &Path) -> Result<Vec<LifecycleTransaction>> {
    let mut incomplete = Vec::new();

    let entries = fs::read_dir(log_dir)?;
    for entry in entries {
        let entry = entry?;
        let file_name = entry.file_name();
        let name = file_name.to_string_lossy();
        if !name.starts_with("txnlog-") || !name.ends_with(".log") {
            continue;
        }

        // Extract UUID from filename: txnlog-{uuid}.log
        let uuid_str = name
            .strip_prefix("txnlog-")
            .and_then(|s| s.strip_suffix(".log"))
            .unwrap_or("");
        let id = match Uuid::parse_str(uuid_str) {
            Ok(id) => id,
            Err(_) => continue,
        };

        let file = File::open(entry.path())?;
        let reader = BufReader::new(file);

        let mut staged = Vec::new();
        let mut obsoleted = Vec::new();
        let mut log_entries = Vec::new();
        let mut final_state = TransactionState::Active;

        for line in reader.lines() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }
            let log_entry: TransactionLogEntry = serde_json::from_str(&line).map_err(|e| {
                CompactionError::InvalidState(format!(
                    "failed to deserialize log entry in {}: {e}",
                    name
                ))
            })?;
            match &log_entry.action {
                TransactionAction::Stage { sstable_id } => staged.push(*sstable_id),
                TransactionAction::Obsolete { sstable_id } => obsoleted.push(*sstable_id),
                TransactionAction::Commit => final_state = TransactionState::Committed,
                TransactionAction::Abort => {
                    final_state = TransactionState::Aborted;
                    staged.clear();
                }
            }
            log_entries.push(log_entry);
        }

        // Only return transactions that are still active (uncommitted, not aborted).
        if final_state == TransactionState::Active {
            incomplete.push(LifecycleTransaction {
                id,
                state: final_state,
                staged,
                obsoleted,
                log_path: entry.path(),
                log_entries,
            });
        }
    }

    Ok(incomplete)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stage_and_commit_writes_log() {
        let dir = tempfile::tempdir().unwrap();
        let mut txn = LifecycleTransaction::new(dir.path()).unwrap();

        txn.stage(1).unwrap();
        txn.stage(2).unwrap();
        txn.obsolete(10).unwrap();
        txn.commit().unwrap();

        assert_eq!(txn.state(), TransactionState::Committed);
        assert_eq!(txn.staged_ids(), &[1, 2]);
        assert_eq!(txn.obsoleted_ids(), &[10]);

        // Verify the log file has 4 entries (2 stage + 1 obsolete + 1 commit).
        let contents = fs::read_to_string(txn.log_path()).unwrap();
        let lines: Vec<&str> = contents.lines().collect();
        assert_eq!(lines.len(), 4);

        // Verify entries deserialise correctly.
        let first: TransactionLogEntry = serde_json::from_str(lines[0]).unwrap();
        assert!(matches!(
            first.action,
            TransactionAction::Stage { sstable_id: 1 }
        ));
        let last: TransactionLogEntry = serde_json::from_str(lines[3]).unwrap();
        assert!(matches!(last.action, TransactionAction::Commit));
    }

    #[test]
    fn stage_and_abort_clears_staged() {
        let dir = tempfile::tempdir().unwrap();
        let mut txn = LifecycleTransaction::new(dir.path()).unwrap();

        txn.stage(5).unwrap();
        txn.stage(6).unwrap();
        txn.abort().unwrap();

        assert_eq!(txn.state(), TransactionState::Aborted);
        assert!(txn.staged_ids().is_empty());
        // Obsoleted is untouched (nothing was obsoleted).
        assert!(txn.obsoleted_ids().is_empty());
    }

    #[test]
    fn drop_auto_aborts_active_transaction() {
        let dir = tempfile::tempdir().unwrap();
        let log_path;
        {
            let mut txn = LifecycleTransaction::new(dir.path()).unwrap();
            txn.stage(99).unwrap();
            log_path = txn.log_path().to_path_buf();
            // txn dropped here while still Active
        }

        // The log should contain an Abort entry written by Drop.
        let contents = fs::read_to_string(&log_path).unwrap();
        let lines: Vec<&str> = contents.lines().collect();
        assert_eq!(lines.len(), 2); // Stage + Abort
        let last: TransactionLogEntry = serde_json::from_str(lines[1]).unwrap();
        assert!(matches!(last.action, TransactionAction::Abort));
    }

    #[test]
    fn recover_incomplete_transactions_finds_uncommitted() {
        use std::mem::ManuallyDrop;

        let dir = tempfile::tempdir().unwrap();

        // Create a committed transaction — should NOT appear in recovery.
        {
            let mut txn = LifecycleTransaction::new(dir.path()).unwrap();
            txn.stage(1).unwrap();
            txn.commit().unwrap();
        }

        // Create an active (incomplete) transaction — SHOULD appear.
        let incomplete_id;
        let incomplete_path;
        {
            let mut txn = ManuallyDrop::new(LifecycleTransaction::new(dir.path()).unwrap());
            txn.stage(42).unwrap();
            txn.obsolete(7).unwrap();
            incomplete_id = txn.id;
            incomplete_path = txn.log_path().to_path_buf();
            // Simulate a process crash: no Rust destructors run, so no terminal
            // Commit/Abort entry is appended to the transaction log.
        }
        // Leave the log without a commit/abort line, matching crash recovery input.
        // The file already has Stage(42) + Obsolete(7) — no Commit/Abort,
        // because the manually-dropped transaction did not run Drop.
        let contents = fs::read_to_string(&incomplete_path).unwrap();
        let lines: Vec<&str> = contents.lines().collect();
        assert_eq!(lines.len(), 2); // Stage + Obsolete only

        let recovered = recover_incomplete_transactions(dir.path()).unwrap();
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].id, incomplete_id);
        assert_eq!(recovered[0].staged_ids(), &[42]);
        assert_eq!(recovered[0].obsoleted_ids(), &[7]);
        assert_eq!(recovered[0].state(), TransactionState::Active);
    }

    #[test]
    fn commit_then_stage_errors() {
        let dir = tempfile::tempdir().unwrap();
        let mut txn = LifecycleTransaction::new(dir.path()).unwrap();
        txn.commit().unwrap();

        let err = txn.stage(1).unwrap_err();
        assert!(matches!(err, CompactionError::InvalidState(_)));
    }

    #[test]
    fn abort_then_commit_errors() {
        let dir = tempfile::tempdir().unwrap();
        let mut txn = LifecycleTransaction::new(dir.path()).unwrap();
        txn.abort().unwrap();

        let err = txn.commit().unwrap_err();
        assert!(matches!(err, CompactionError::InvalidState(_)));
    }
}
