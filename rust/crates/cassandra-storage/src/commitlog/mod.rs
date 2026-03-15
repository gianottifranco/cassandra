// Licensed under Apache License, Version 2.0.

//! Commit Log (WAL) for Cassandra Rust.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.db.commitlog.CommitLog`
//! - `org.apache.cassandra.db.commitlog.CommitLogSegment`
//!
//! ## Architecture
//!
//! The commit log is an append-only WAL that guarantees durability for
//! mutations before they are applied to memtables. Each mutation is
//! serialized as `[len: u32][crc32c: u32][payload: [u8]]`.
//!
//! Segments are rotated when they exceed a configurable size threshold
//! (default 32 MiB). Replay iterates segments in order, verifying CRCs,
//! and yields deserialized mutations.

pub mod segment;

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::Mutex;
use thiserror::Error;
use tracing::{debug, info, warn};

use segment::Segment;

// ─── Errors ────────────────────────────────────────────────────────────────

#[derive(Debug, Error)]
pub enum CommitLogError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("CRC mismatch at segment {segment_id} offset {offset}: expected {expected:#010x}, got {actual:#010x}")]
    CrcMismatch {
        segment_id: u64,
        offset: u64,
        expected: u32,
        actual: u32,
    },
    #[error("Corrupt segment header in {path}")]
    CorruptHeader { path: PathBuf },
    #[error("Serialization error: {0}")]
    Serialization(String),
}

pub type Result<T> = std::result::Result<T, CommitLogError>;

// ─── Sync policy ───────────────────────────────────────────────────────────

/// Durability sync policy, mirroring Java's `CommitLogSync`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncPolicy {
    /// Sync after every write (safest, slowest).
    Batch,
    /// Sync periodically on a background interval.
    Periodic { interval_ms: u64 },
}

impl Default for SyncPolicy {
    fn default() -> Self {
        SyncPolicy::Periodic { interval_ms: 10_000 }
    }
}

// ─── Configuration ─────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct CommitLogConfig {
    /// Maximum segment size before rotation (bytes). Default: 32 MiB.
    pub max_segment_size: u64,
    /// Sync policy.
    pub sync_policy: SyncPolicy,
    /// Directory for commit log segments.
    pub directory: PathBuf,
}

impl Default for CommitLogConfig {
    fn default() -> Self {
        Self {
            max_segment_size: 32 * 1024 * 1024,
            sync_policy: SyncPolicy::default(),
            directory: PathBuf::from("data/commitlog"),
        }
    }
}

// ─── Mutation ──────────────────────────────────────────────────────────────

/// A serializable mutation that gets written to the commit log.
/// This is the unit of durability — matches Java's `Mutation`.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Mutation {
    pub keyspace: String,
    pub table: String,
    pub partition_key: Vec<u8>,
    pub rows: Vec<MutationRow>,
    pub timestamp: i64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MutationRow {
    pub clustering_key: Vec<u8>,
    pub cells: Vec<CellMutation>,
    /// If true, this is a row-level tombstone.
    pub is_tombstone: bool,
    /// Local deletion time for tombstones (seconds since epoch).
    pub local_deletion_time: Option<i32>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CellMutation {
    pub column: String,
    pub value: Option<Vec<u8>>,
    pub timestamp: i64,
    /// Time-to-live in seconds; 0 = no TTL.
    pub ttl: i32,
    /// Local deletion time for TTL (seconds since epoch).
    pub local_deletion_time: Option<i32>,
    /// If true, this cell is a tombstone (column delete).
    pub is_tombstone: bool,
}

// ─── CommitLog ─────────────────────────────────────────────────────────────

/// The commit log manages a sequence of append-only WAL segments.
pub struct CommitLog {
    config: CommitLogConfig,
    current_segment: Mutex<Segment>,
    next_segment_id: AtomicU64,
}

impl CommitLog {
    /// Open or create a commit log in the configured directory.
    pub fn open(config: CommitLogConfig) -> Result<Self> {
        fs::create_dir_all(&config.directory)?;

        // Find the highest existing segment ID.
        let mut max_id: u64 = 0;
        for entry in fs::read_dir(&config.directory)? {
            let entry = entry?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if let Some(id) = parse_segment_filename(&name) {
                max_id = max_id.max(id);
            }
        }

        let next_id = max_id + 1;
        let segment = Segment::create(&config.directory, next_id)?;

        info!(
            directory = %config.directory.display(),
            segment_id = next_id,
            "Commit log opened"
        );

        Ok(Self {
            config,
            current_segment: Mutex::new(segment),
            next_segment_id: AtomicU64::new(next_id + 1),
        })
    }

    /// Append a mutation. Returns the segment-id and offset where it was written.
    pub fn append(&self, mutation: &Mutation) -> Result<(u64, u64)> {
        let payload = serde_json::to_vec(mutation)
            .map_err(|e| CommitLogError::Serialization(e.to_string()))?;

        let mut seg = self.current_segment.lock();

        // Rotate if this write would exceed the max segment size.
        if seg.size() + payload.len() as u64 + 8 > self.config.max_segment_size {
            let old_id = seg.id();
            seg.sync()?;

            let new_id = self.next_segment_id.fetch_add(1, Ordering::SeqCst);
            let new_seg = Segment::create(&self.config.directory, new_id)?;
            *seg = new_seg;

            debug!(old_id, new_id, "Segment rotated");
        }

        let offset = seg.append_entry(&payload)?;
        let seg_id = seg.id();

        if self.config.sync_policy == SyncPolicy::Batch {
            seg.sync()?;
        }

        Ok((seg_id, offset))
    }

    /// Force sync the current segment.
    pub fn sync(&self) -> Result<()> {
        self.current_segment.lock().sync()
    }

    /// Replay all segments in order, yielding mutations.
    /// Returns the number of mutations replayed and any segments that were corrupt.
    pub fn replay(&self) -> Result<ReplayResult> {
        let mut segments = list_segment_files(&self.config.directory)?;
        segments.sort(); // sort by filename = sort by segment ID

        let mut result = ReplayResult {
            mutations: Vec::new(),
            replayed_segments: 0,
            corrupt_entries: 0,
        };

        for path in &segments {
            let name = path.file_name().unwrap().to_string_lossy();
            let seg_id = parse_segment_filename(&name).unwrap_or(0);

            info!(segment_id = seg_id, path = %path.display(), "Replaying segment");

            match Segment::open_for_read(path) {
                Ok(seg) => {
                    let entries = seg.read_all_entries();
                    for entry_result in entries {
                        match entry_result {
                            Ok(payload) => {
                                match serde_json::from_slice::<Mutation>(&payload) {
                                    Ok(mutation) => result.mutations.push(mutation),
                                    Err(e) => {
                                        warn!(
                                            segment_id = seg_id,
                                            error = %e,
                                            "Failed to deserialize mutation, skipping"
                                        );
                                        result.corrupt_entries += 1;
                                    }
                                }
                            }
                            Err(e) => {
                                warn!(
                                    segment_id = seg_id,
                                    error = %e,
                                    "Corrupt entry in segment, skipping rest"
                                );
                                result.corrupt_entries += 1;
                                break;
                            }
                        }
                    }
                    result.replayed_segments += 1;
                }
                Err(e) => {
                    warn!(
                        path = %path.display(),
                        error = %e,
                        "Failed to open segment for replay"
                    );
                }
            }
        }

        info!(
            mutations = result.mutations.len(),
            segments = result.replayed_segments,
            corrupt = result.corrupt_entries,
            "Commit log replay complete"
        );

        Ok(result)
    }

    /// Discard segments with IDs <= the given segment ID.
    /// Called after memtable flush completes.
    pub fn discard_completed_segments(&self, up_to_segment_id: u64) -> Result<()> {
        let segments = list_segment_files(&self.config.directory)?;
        for path in segments {
            let name = path.file_name().unwrap().to_string_lossy();
            if let Some(id) = parse_segment_filename(&name) {
                if id <= up_to_segment_id {
                    // Don't delete current segment
                    let current_id = self.current_segment.lock().id();
                    if id != current_id {
                        debug!(segment_id = id, "Discarding completed segment");
                        fs::remove_file(&path)?;
                    }
                }
            }
        }
        Ok(())
    }

    /// Returns the current segment ID.
    pub fn current_segment_id(&self) -> u64 {
        self.current_segment.lock().id()
    }
}

// ─── ReplayResult ──────────────────────────────────────────────────────────

#[derive(Debug)]
pub struct ReplayResult {
    pub mutations: Vec<Mutation>,
    pub replayed_segments: u64,
    pub corrupt_entries: u64,
}

// ─── Helpers ───────────────────────────────────────────────────────────────

/// Segment filename format: `CommitLog-{id}.log`
pub fn segment_filename(id: u64) -> String {
    format!("CommitLog-{id}.log")
}

fn parse_segment_filename(name: &str) -> Option<u64> {
    let name = name.strip_prefix("CommitLog-")?;
    let name = name.strip_suffix(".log")?;
    name.parse().ok()
}

fn list_segment_files(dir: &Path) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    if dir.exists() {
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if name.starts_with("CommitLog-") && name.ends_with(".log") {
                files.push(entry.path());
            }
        }
    }
    Ok(files)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn test_config(dir: &Path) -> CommitLogConfig {
        CommitLogConfig {
            max_segment_size: 4096, // small for testing rotation
            sync_policy: SyncPolicy::Batch,
            directory: dir.to_path_buf(),
        }
    }

    fn test_mutation(ks: &str, tbl: &str, pk: &[u8]) -> Mutation {
        Mutation {
            keyspace: ks.to_string(),
            table: tbl.to_string(),
            partition_key: pk.to_vec(),
            rows: vec![MutationRow {
                clustering_key: vec![],
                cells: vec![CellMutation {
                    column: "name".to_string(),
                    value: Some(b"test_value".to_vec()),
                    timestamp: 1000,
                    ttl: 0,
                    local_deletion_time: None,
                    is_tombstone: false,
                }],
                is_tombstone: false,
                local_deletion_time: None,
            }],
            timestamp: 1000,
        }
    }

    #[test]
    fn open_creates_directory() {
        let dir = TempDir::new().unwrap();
        let subdir = dir.path().join("cl");
        let config = CommitLogConfig {
            directory: subdir.clone(),
            ..test_config(dir.path())
        };
        let _cl = CommitLog::open(config).unwrap();
        assert!(subdir.exists());
    }

    #[test]
    fn append_and_replay() {
        let dir = TempDir::new().unwrap();
        let config = test_config(dir.path());
        let cl = CommitLog::open(config.clone()).unwrap();

        let m1 = test_mutation("ks", "t1", b"pk1");
        let m2 = test_mutation("ks", "t2", b"pk2");

        cl.append(&m1).unwrap();
        cl.append(&m2).unwrap();
        cl.sync().unwrap();

        // Replay
        let result = cl.replay().unwrap();
        assert_eq!(result.mutations.len(), 2);
        assert_eq!(result.mutations[0].table, "t1");
        assert_eq!(result.mutations[1].table, "t2");
        assert_eq!(result.corrupt_entries, 0);
    }

    #[test]
    fn segment_rotation() {
        let dir = TempDir::new().unwrap();
        let config = CommitLogConfig {
            max_segment_size: 256, // very small → force rotation
            ..test_config(dir.path())
        };
        let cl = CommitLog::open(config).unwrap();

        let first_seg = cl.current_segment_id();

        // Write enough to trigger rotation
        for i in 0..10 {
            let m = test_mutation("ks", &format!("t{i}"), &[i as u8]);
            cl.append(&m).unwrap();
        }

        let final_seg = cl.current_segment_id();
        assert!(final_seg > first_seg, "Expected segments to rotate");
    }

    #[test]
    fn replay_after_rotation() {
        let dir = TempDir::new().unwrap();
        let config = CommitLogConfig {
            max_segment_size: 256,
            ..test_config(dir.path())
        };
        let cl = CommitLog::open(config).unwrap();

        let count = 20;
        for i in 0..count {
            let m = test_mutation("ks", &format!("t{i}"), &[i as u8]);
            cl.append(&m).unwrap();
        }
        cl.sync().unwrap();

        let result = cl.replay().unwrap();
        assert_eq!(result.mutations.len(), count);
        assert!(result.replayed_segments > 1, "Expected multiple segments");
    }

    #[test]
    fn discard_completed_segments() {
        let dir = TempDir::new().unwrap();
        let config = CommitLogConfig {
            max_segment_size: 256,
            ..test_config(dir.path())
        };
        let cl = CommitLog::open(config.clone()).unwrap();

        // Force several rotations
        for i in 0..20 {
            let m = test_mutation("ks", &format!("t{i}"), &[i as u8]);
            cl.append(&m).unwrap();
        }

        let current = cl.current_segment_id();
        // Discard all but current
        cl.discard_completed_segments(current - 1).unwrap();

        // Count remaining segment files
        let remaining = list_segment_files(&config.directory).unwrap();
        // Should have at least the current segment
        assert!(!remaining.is_empty());
    }
}
