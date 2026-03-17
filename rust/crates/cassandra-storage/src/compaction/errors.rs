// Licensed under Apache License, Version 2.0.

//! Compaction error types, operation enums, and progress tracking.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.db.compaction.CompactionManager`
//! - `org.apache.cassandra.db.compaction.OperationType`
//! - `org.apache.cassandra.db.compaction.CompactionInfo`

use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::sstable::format::SSTableId;

// ─── Errors ──────────────────────────────────────────────────────────────────

/// Errors that can occur during compaction operations.
#[derive(Debug, thiserror::Error)]
pub enum CompactionError {
    #[error("I/O error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("corrupted SSTable {id}: {reason}")]
    CorruptedSSTable { id: SSTableId, reason: String },

    #[error("transaction {txn_id} failed: {reason}")]
    TransactionFailed { txn_id: Uuid, reason: String },

    #[error("compaction cancelled")]
    Cancelled,

    #[error("concurrent modification of SSTables {sstable_ids:?}")]
    ConcurrentModification { sstable_ids: Vec<SSTableId> },

    #[error("invalid state: {0}")]
    InvalidState(String),

    #[error("rate limited")]
    RateLimited,
}

// ─── Compaction Type ─────────────────────────────────────────────────────────

/// The kind of compaction operation being performed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompactionType {
    Compaction,
    Cleanup,
    Scrub,
    Upgrade,
    AntiCompaction,
    Tombstone,
}

// ─── Compaction Reason ───────────────────────────────────────────────────────

/// Why a compaction was triggered.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompactionReason {
    Normal,
    UserDefined,
    Repair,
    Reshape,
    Upgrade,
}

// ─── Operation Progress ──────────────────────────────────────────────────────

/// Tracks byte-level and partition-level progress of a compaction operation.
#[derive(Debug)]
pub struct OperationProgress {
    pub total_bytes: AtomicU64,
    pub bytes_processed: AtomicU64,
    pub total_partitions: AtomicU64,
    pub partitions_processed: AtomicU64,
    pub start_time_ms: u64,
}

impl OperationProgress {
    /// Create a new progress tracker with the given totals.
    pub fn new(total_bytes: u64, total_partitions: u64) -> Self {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        Self {
            total_bytes: AtomicU64::new(total_bytes),
            bytes_processed: AtomicU64::new(0),
            total_partitions: AtomicU64::new(total_partitions),
            partitions_processed: AtomicU64::new(0),
            start_time_ms: now_ms,
        }
    }

    /// Returns the completion percentage (0.0 – 100.0) based on bytes processed.
    pub fn progress_pct(&self) -> f64 {
        let total = self.total_bytes.load(Ordering::Relaxed);
        if total == 0 {
            return 100.0;
        }
        let processed = self.bytes_processed.load(Ordering::Relaxed);
        (processed as f64 / total as f64) * 100.0
    }

    /// Returns `true` when all bytes have been processed.
    pub fn is_complete(&self) -> bool {
        let total = self.total_bytes.load(Ordering::Relaxed);
        let processed = self.bytes_processed.load(Ordering::Relaxed);
        processed >= total
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_display_io() {
        let err = CompactionError::IoError(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "file missing",
        ));
        assert!(err.to_string().contains("I/O error"));
        assert!(err.to_string().contains("file missing"));
    }

    #[test]
    fn error_display_corrupted() {
        let err = CompactionError::CorruptedSSTable {
            id: 42,
            reason: "bad checksum".into(),
        };
        let msg = err.to_string();
        assert!(msg.contains("42"));
        assert!(msg.contains("bad checksum"));
    }

    #[test]
    fn error_display_transaction_failed() {
        let id = Uuid::new_v4();
        let err = CompactionError::TransactionFailed {
            txn_id: id,
            reason: "disk full".into(),
        };
        let msg = err.to_string();
        assert!(msg.contains(&id.to_string()));
        assert!(msg.contains("disk full"));
    }

    #[test]
    fn error_display_cancelled() {
        let err = CompactionError::Cancelled;
        assert_eq!(err.to_string(), "compaction cancelled");
    }

    #[test]
    fn error_display_concurrent_modification() {
        let err = CompactionError::ConcurrentModification {
            sstable_ids: vec![1, 2, 3],
        };
        let msg = err.to_string();
        assert!(msg.contains("[1, 2, 3]"));
    }

    #[test]
    fn error_display_invalid_state() {
        let err = CompactionError::InvalidState("wrong phase".into());
        assert!(err.to_string().contains("wrong phase"));
    }

    #[test]
    fn error_display_rate_limited() {
        let err = CompactionError::RateLimited;
        assert_eq!(err.to_string(), "rate limited");
    }

    #[test]
    fn compaction_type_serde_roundtrip() {
        let variants = [
            CompactionType::Compaction,
            CompactionType::Cleanup,
            CompactionType::Scrub,
            CompactionType::Upgrade,
            CompactionType::AntiCompaction,
            CompactionType::Tombstone,
        ];
        for variant in &variants {
            let json = serde_json::to_string(variant).unwrap();
            let back: CompactionType = serde_json::from_str(&json).unwrap();
            assert_eq!(*variant, back);
        }
    }

    #[test]
    fn compaction_reason_serde_roundtrip() {
        let variants = [
            CompactionReason::Normal,
            CompactionReason::UserDefined,
            CompactionReason::Repair,
            CompactionReason::Reshape,
            CompactionReason::Upgrade,
        ];
        for variant in &variants {
            let json = serde_json::to_string(variant).unwrap();
            let back: CompactionReason = serde_json::from_str(&json).unwrap();
            assert_eq!(*variant, back);
        }
    }

    #[test]
    fn progress_new_and_pct() {
        let progress = OperationProgress::new(1000, 50);
        assert_eq!(progress.progress_pct(), 0.0);
        assert!(!progress.is_complete());

        progress
            .bytes_processed
            .store(500, std::sync::atomic::Ordering::Relaxed);
        assert!((progress.progress_pct() - 50.0).abs() < f64::EPSILON);
        assert!(!progress.is_complete());

        progress
            .bytes_processed
            .store(1000, std::sync::atomic::Ordering::Relaxed);
        assert!((progress.progress_pct() - 100.0).abs() < f64::EPSILON);
        assert!(progress.is_complete());
    }

    #[test]
    fn progress_zero_total() {
        let progress = OperationProgress::new(0, 0);
        assert_eq!(progress.progress_pct(), 100.0);
        assert!(progress.is_complete());
    }
}
