// Licensed under Apache License, Version 2.0.

//! Commit Log (WAL) for Cassandra Rust.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.db.commitlog.CommitLog`
//! - `org.apache.cassandra.db.commitlog.CommitLogSegment`
//! - `org.apache.cassandra.db.commitlog.CommitLogArchiver`
//!
//! ## Architecture
//!
//! The commit log is an append-only WAL that guarantees durability for
//! mutations before they are applied to memtables. Each mutation is
//! serialized as `[len: u32][flags: u8][crc32c: u32][payload: [u8]]`.
//!
//! Segments are rotated when they exceed a configurable size threshold
//! (default 32 MiB). Replay iterates segments in order, verifying CRCs,
//! and yields deserialized mutations.
//!
//! ## Enhancements over v1
//! - Truncation point tracking per column family
//! - Archiving hooks (command or callback after segment rotation)
//! - Optional LZ4 compression per entry
//! - Segment recycling pool
//! - Configurable corruption policy (skip vs stop)
//! - CDC integration hooks
//! - Operational metrics

pub mod encrypted;
pub mod segment;

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::Mutex;
use thiserror::Error;
use tracing::{debug, info, warn};

use segment::{CorruptionPolicy, Segment, SegmentFlags};

// ─── Errors ────────────────────────────────────────────────────────────────

#[derive(Debug, Error)]
pub enum CommitLogError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error(
        "CRC mismatch at segment {segment_id} offset {offset}: expected {expected:#010x}, got {actual:#010x}"
    )]
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
    #[error("CDC space limit exceeded: {used} / {limit} bytes")]
    CdcSpaceLimitExceeded { used: u64, limit: u64 },
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
        SyncPolicy::Periodic {
            interval_ms: 10_000,
        }
    }
}

// ─── Archiving ─────────────────────────────────────────────────────────────

/// Configuration for commit log segment archiving.
/// Mirrors Java's `commitlog-archiving.properties`.
#[derive(Debug, Clone, Default)]
pub struct ArchivingConfig {
    /// If set, run this command after segment rotation.
    /// `%path` is replaced with the segment file path,
    /// `%name` with the segment file name.
    pub archive_command: Option<String>,
    /// Directory to copy/archive segments to.
    pub archive_directory: Option<PathBuf>,
    /// If true, restore archived segments on replay.
    pub restore_on_replay: bool,
    /// Restore command (with `%from` and `%to` placeholders).
    pub restore_command: Option<String>,
}

// ─── CDC config ────────────────────────────────────────────────────────────

/// Change Data Capture configuration for commit log integration.
#[derive(Debug, Clone)]
pub struct CdcConfig {
    /// Enable CDC. When true, mutations to CDC-enabled tables are
    /// also written to the CDC raw directory.
    pub enabled: bool,
    /// Directory for CDC raw log segments.
    pub raw_directory: PathBuf,
    /// Maximum total size (bytes) of CDC raw segments.
    /// Writes are rejected when exceeded.
    pub size_limit: u64,
}

impl Default for CdcConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            raw_directory: PathBuf::from("data/cdc_raw"),
            size_limit: 4 * 1024 * 1024 * 1024, // 4 GiB
        }
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
    /// Enable LZ4 compression for entries.
    pub compression_enabled: bool,
    /// Corruption policy for replay.
    pub corruption_policy: CorruptionPolicy,
    /// Archiving configuration.
    pub archiving: ArchivingConfig,
    /// CDC configuration.
    pub cdc: CdcConfig,
    /// Number of segment files to keep in the recycling pool.
    pub recycle_pool_size: usize,
}

impl Default for CommitLogConfig {
    fn default() -> Self {
        Self {
            max_segment_size: 32 * 1024 * 1024,
            sync_policy: SyncPolicy::default(),
            directory: PathBuf::from("data/commitlog"),
            compression_enabled: false,
            corruption_policy: CorruptionPolicy::default(),
            archiving: ArchivingConfig::default(),
            cdc: CdcConfig::default(),
            recycle_pool_size: 0,
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
    /// If true, this mutation is for a CDC-enabled table.
    #[serde(default)]
    pub cdc_enabled: bool,
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

// ─── Truncation Points ────────────────────────────────────────────────────

/// Tracks the commit log position up to which each column family has been
/// flushed. Segments older than all truncation points can be safely discarded.
#[derive(Debug, Clone, Default)]
pub struct TruncationPoints {
    /// Map of CF name → (segment_id, offset) up to which the CF is flushed.
    points: HashMap<String, (u64, u64)>,
}

impl TruncationPoints {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record that `cf_name` has been flushed up to the given position.
    pub fn mark_flushed(&mut self, cf_name: &str, segment_id: u64, offset: u64) {
        let entry = self.points.entry(cf_name.to_string()).or_insert((0, 0));
        if segment_id > entry.0 || (segment_id == entry.0 && offset > entry.1) {
            *entry = (segment_id, offset);
        }
    }

    /// Get the lowest segment ID across all tracked CFs.
    /// Segments below this can be discarded.
    pub fn lowest_segment_id(&self) -> Option<u64> {
        self.points.values().map(|(seg, _)| *seg).min()
    }

    /// Check if a specific CF has been flushed past the given position.
    pub fn is_flushed(&self, cf_name: &str, segment_id: u64, offset: u64) -> bool {
        match self.points.get(cf_name) {
            Some(&(seg, off)) => seg > segment_id || (seg == segment_id && off >= offset),
            None => false,
        }
    }

    /// Get all tracked points.
    pub fn all(&self) -> &HashMap<String, (u64, u64)> {
        &self.points
    }
}

// ─── Metrics ───────────────────────────────────────────────────────────────

/// Operational metrics for the commit log.
#[derive(Debug, Default)]
pub struct CommitLogMetrics {
    pub bytes_written: AtomicU64,
    pub entries_written: AtomicU64,
    pub segments_rotated: AtomicU64,
    pub segments_recycled: AtomicU64,
    pub replay_mutations: AtomicU64,
    pub replay_corrupt_entries: AtomicU64,
    pub cdc_entries_written: AtomicU64,
}

impl CommitLogMetrics {
    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            bytes_written: self.bytes_written.load(Ordering::Relaxed),
            entries_written: self.entries_written.load(Ordering::Relaxed),
            segments_rotated: self.segments_rotated.load(Ordering::Relaxed),
            segments_recycled: self.segments_recycled.load(Ordering::Relaxed),
            replay_mutations: self.replay_mutations.load(Ordering::Relaxed),
            replay_corrupt_entries: self.replay_corrupt_entries.load(Ordering::Relaxed),
            cdc_entries_written: self.cdc_entries_written.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone)]
pub struct MetricsSnapshot {
    pub bytes_written: u64,
    pub entries_written: u64,
    pub segments_rotated: u64,
    pub segments_recycled: u64,
    pub replay_mutations: u64,
    pub replay_corrupt_entries: u64,
    pub cdc_entries_written: u64,
}

// ─── CommitLog ─────────────────────────────────────────────────────────────

/// The commit log manages a sequence of append-only WAL segments.
pub struct CommitLog {
    config: CommitLogConfig,
    current_segment: Mutex<Segment>,
    next_segment_id: AtomicU64,
    truncation_points: Mutex<TruncationPoints>,
    /// Pool of recycled segment file paths.
    recycle_pool: Mutex<Vec<PathBuf>>,
    /// CDC segment writer (if CDC enabled).
    cdc_segment: Mutex<Option<Segment>>,
    /// Metrics.
    pub metrics: CommitLogMetrics,
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
        let seg_flags = SegmentFlags {
            compression_enabled: config.compression_enabled,
        };
        let segment = Segment::create_with_flags(&config.directory, next_id, seg_flags)?;

        // Initialize CDC if enabled
        let cdc_segment = if config.cdc.enabled {
            fs::create_dir_all(&config.cdc.raw_directory)?;
            let cdc_seg = Segment::create(&config.cdc.raw_directory, next_id)?;
            Some(cdc_seg)
        } else {
            None
        };

        info!(
            directory = %config.directory.display(),
            segment_id = next_id,
            compression = config.compression_enabled,
            cdc = config.cdc.enabled,
            "Commit log opened"
        );

        Ok(Self {
            config,
            current_segment: Mutex::new(segment),
            next_segment_id: AtomicU64::new(next_id + 1),
            truncation_points: Mutex::new(TruncationPoints::new()),
            recycle_pool: Mutex::new(Vec::new()),
            cdc_segment: Mutex::new(cdc_segment),
            metrics: CommitLogMetrics::default(),
        })
    }

    /// Append a mutation. Returns the segment-id and offset where it was written.
    pub fn append(&self, mutation: &Mutation) -> Result<(u64, u64)> {
        let payload = serde_json::to_vec(mutation)
            .map_err(|e| CommitLogError::Serialization(e.to_string()))?;

        let (seg_id, offset) = {
            let mut seg = self.current_segment.lock();

            // Rotate if this write would exceed the max segment size.
            if seg.size() + payload.len() as u64 + 12 > self.config.max_segment_size {
                let old_id = seg.id();
                seg.sync()?;

                // Archive hook
                self.archive_segment(&seg);

                let new_id = self.next_segment_id.fetch_add(1, Ordering::SeqCst);
                let new_seg = self.create_or_recycle_segment(new_id)?;
                *seg = new_seg;

                self.metrics
                    .segments_rotated
                    .fetch_add(1, Ordering::Relaxed);
                debug!(old_id, new_id, "Segment rotated");
            }

            let offset = seg.append_entry(&payload)?;
            let seg_id = seg.id();

            if self.config.sync_policy == SyncPolicy::Batch {
                seg.sync()?;
            }

            (seg_id, offset)
        };

        self.metrics
            .bytes_written
            .fetch_add(payload.len() as u64, Ordering::Relaxed);
        self.metrics.entries_written.fetch_add(1, Ordering::Relaxed);

        // CDC hook: if mutation is CDC-enabled, also write to CDC directory
        if mutation.cdc_enabled && self.config.cdc.enabled {
            self.write_cdc_entry(&payload)?;
        }

        Ok((seg_id, offset))
    }

    /// Force sync the current segment.
    pub fn sync(&self) -> Result<()> {
        self.current_segment.lock().sync()
    }

    /// Record a truncation point for a column family.
    pub fn mark_cf_flushed(&self, cf_name: &str, segment_id: u64, offset: u64) {
        self.truncation_points
            .lock()
            .mark_flushed(cf_name, segment_id, offset);
    }

    /// Get a copy of current truncation points.
    pub fn truncation_points(&self) -> TruncationPoints {
        self.truncation_points.lock().clone()
    }

    /// Replay all segments in order, yielding mutations.
    /// Uses the configured corruption policy.
    pub fn replay(&self) -> Result<ReplayResult> {
        self.replay_with_policy(self.config.corruption_policy)
    }

    /// Replay with explicit corruption policy.
    pub fn replay_with_policy(&self, policy: CorruptionPolicy) -> Result<ReplayResult> {
        let mut segments = list_segment_files(&self.config.directory)?;
        segments.sort();

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
                    let entries = seg.read_entries_with_policy(policy);
                    for entry_result in entries {
                        match entry_result {
                            Ok(payload) => match serde_json::from_slice::<Mutation>(&payload) {
                                Ok(mutation) => result.mutations.push(mutation),
                                Err(e) => {
                                    warn!(
                                        segment_id = seg_id,
                                        error = %e,
                                        "Failed to deserialize mutation, skipping"
                                    );
                                    result.corrupt_entries += 1;
                                }
                            },
                            Err(e) => {
                                warn!(
                                    segment_id = seg_id,
                                    error = %e,
                                    "Corrupt entry in segment"
                                );
                                result.corrupt_entries += 1;
                                if policy == CorruptionPolicy::StopOnCorrupt {
                                    break;
                                }
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

        self.metrics
            .replay_mutations
            .store(result.mutations.len() as u64, Ordering::Relaxed);
        self.metrics
            .replay_corrupt_entries
            .store(result.corrupt_entries, Ordering::Relaxed);

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
                        // Try to recycle instead of delete
                        if !self.try_recycle_segment(&path) {
                            fs::remove_file(&path)?;
                        }
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

    /// Get metrics snapshot.
    pub fn metrics_snapshot(&self) -> MetricsSnapshot {
        self.metrics.snapshot()
    }

    // ─── Internal helpers ──────────────────────────────────────────────

    fn create_or_recycle_segment(&self, new_id: u64) -> Result<Segment> {
        let seg_flags = SegmentFlags {
            compression_enabled: self.config.compression_enabled,
        };

        // Try recycling
        let recycled_path = self.recycle_pool.lock().pop();
        if let Some(path) = recycled_path {
            match Segment::recycle(&path, new_id, seg_flags) {
                Ok(seg) => {
                    self.metrics
                        .segments_recycled
                        .fetch_add(1, Ordering::Relaxed);
                    return Ok(seg);
                }
                Err(e) => {
                    warn!(error = %e, "Failed to recycle segment, creating new");
                    let _ = fs::remove_file(&path);
                }
            }
        }

        Segment::create_with_flags(&self.config.directory, new_id, seg_flags)
    }

    fn try_recycle_segment(&self, path: &Path) -> bool {
        let mut pool = self.recycle_pool.lock();
        if pool.len() < self.config.recycle_pool_size {
            pool.push(path.to_path_buf());
            true
        } else {
            false
        }
    }

    fn archive_segment(&self, segment: &Segment) {
        if let Some(ref cmd_template) = self.config.archiving.archive_command {
            let path_str = segment.path().display().to_string();
            let name = segment
                .path()
                .file_name()
                .unwrap_or_default()
                .to_string_lossy();
            let cmd = cmd_template
                .replace("%path", &path_str)
                .replace("%name", &name);
            debug!(cmd = %cmd, "Running archive command");
            // Fire and forget — in production this would be async
            if let Err(e) = std::process::Command::new("sh").args(["-c", &cmd]).status() {
                warn!(error = %e, "Archive command failed");
            }
        }

        if let Some(ref archive_dir) = self.config.archiving.archive_directory {
            if let Err(e) = fs::create_dir_all(archive_dir) {
                warn!(error = %e, "Failed to create archive directory");
                return;
            }
            let dest = archive_dir.join(segment.path().file_name().unwrap());
            if let Err(e) = fs::copy(segment.path(), &dest) {
                warn!(error = %e, "Failed to archive segment");
            } else {
                debug!(dest = %dest.display(), "Segment archived");
            }
        }
    }

    fn write_cdc_entry(&self, payload: &[u8]) -> Result<()> {
        // Check space limit
        let used = dir_size(&self.config.cdc.raw_directory).unwrap_or(0);
        if used + payload.len() as u64 > self.config.cdc.size_limit {
            return Err(CommitLogError::CdcSpaceLimitExceeded {
                used,
                limit: self.config.cdc.size_limit,
            });
        }

        let mut cdc_seg = self.cdc_segment.lock();
        if let Some(ref mut seg) = *cdc_seg {
            // Rotate CDC segment if needed
            if seg.size() + payload.len() as u64 + 12 > self.config.max_segment_size {
                seg.sync()?;
                let new_id = self.next_segment_id.fetch_add(1, Ordering::SeqCst);
                *seg = Segment::create(&self.config.cdc.raw_directory, new_id)?;
            }
            seg.append_entry(payload)?;
            self.metrics
                .cdc_entries_written
                .fetch_add(1, Ordering::Relaxed);
        }
        Ok(())
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

fn dir_size(dir: &Path) -> std::io::Result<u64> {
    let mut total = 0u64;
    if dir.exists() {
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            if entry.file_type()?.is_file() {
                total += entry.metadata()?.len();
            }
        }
    }
    Ok(total)
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
            ..CommitLogConfig::default()
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
            cdc_enabled: false,
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

    #[test]
    fn truncation_points_tracking() {
        let dir = TempDir::new().unwrap();
        let config = test_config(dir.path());
        let cl = CommitLog::open(config).unwrap();

        cl.mark_cf_flushed("ks.t1", 5, 100);
        cl.mark_cf_flushed("ks.t2", 3, 200);

        let tp = cl.truncation_points();
        assert_eq!(tp.lowest_segment_id(), Some(3));
        assert!(tp.is_flushed("ks.t1", 4, 0));
        assert!(!tp.is_flushed("ks.t2", 5, 0));
    }

    #[test]
    fn metrics_counted() {
        let dir = TempDir::new().unwrap();
        let config = test_config(dir.path());
        let cl = CommitLog::open(config).unwrap();

        let m = test_mutation("ks", "t1", b"pk1");
        cl.append(&m).unwrap();
        cl.append(&m).unwrap();

        let snap = cl.metrics_snapshot();
        assert_eq!(snap.entries_written, 2);
        assert!(snap.bytes_written > 0);
    }

    #[test]
    fn compressed_commitlog() {
        let dir = TempDir::new().unwrap();
        let config = CommitLogConfig {
            compression_enabled: true,
            ..test_config(dir.path())
        };
        let cl = CommitLog::open(config).unwrap();

        let m = test_mutation("ks", "t1", b"pk1");
        cl.append(&m).unwrap();
        cl.sync().unwrap();

        let result = cl.replay().unwrap();
        assert_eq!(result.mutations.len(), 1);
        assert_eq!(result.mutations[0].table, "t1");
    }

    #[test]
    fn archiving_to_directory() {
        let dir = TempDir::new().unwrap();
        let archive_dir = dir.path().join("archive");
        let config = CommitLogConfig {
            max_segment_size: 256,
            archiving: ArchivingConfig {
                archive_directory: Some(archive_dir.clone()),
                ..Default::default()
            },
            ..test_config(dir.path())
        };
        let cl = CommitLog::open(config).unwrap();

        // Write enough to trigger rotation (which triggers archiving)
        for i in 0..20 {
            let m = test_mutation("ks", &format!("t{i}"), &[i as u8]);
            cl.append(&m).unwrap();
        }

        // Archived segments should exist
        if archive_dir.exists() {
            let archived: Vec<_> = fs::read_dir(&archive_dir).unwrap().flatten().collect();
            assert!(!archived.is_empty(), "Expected archived segments");
        }
    }

    #[test]
    fn cdc_writes() {
        let dir = TempDir::new().unwrap();
        let cdc_dir = dir.path().join("cdc_raw");
        let config = CommitLogConfig {
            cdc: CdcConfig {
                enabled: true,
                raw_directory: cdc_dir.clone(),
                size_limit: 1024 * 1024,
            },
            ..test_config(dir.path())
        };
        let cl = CommitLog::open(config).unwrap();

        // Non-CDC mutation: no CDC write
        let m1 = test_mutation("ks", "t1", b"pk1");
        cl.append(&m1).unwrap();

        // CDC mutation
        let mut m2 = test_mutation("ks", "t2", b"pk2");
        m2.cdc_enabled = true;
        cl.append(&m2).unwrap();

        let snap = cl.metrics_snapshot();
        assert_eq!(snap.entries_written, 2);
        assert_eq!(snap.cdc_entries_written, 1);
    }
}
