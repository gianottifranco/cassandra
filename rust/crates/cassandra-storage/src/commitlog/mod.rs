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
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::Mutex;
use thiserror::Error;
use tracing::{debug, info, warn};

use encrypted::{CommitLogEncryptor, EncryptedSegmentWriter, EncryptingSegmentWriter};
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
    /// Restore command; `%from` and `%to` are expanded before execution.
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
    /// Optional commitlog entry encryptor.
    ///
    /// When set and enabled, segment entries are encrypted on write and
    /// transparently decrypted during replay.
    pub encryptor: Option<Arc<dyn CommitLogEncryptor>>,
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
            encryptor: None,
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
    /// Static-column cells (one value per partition, independent of clustering key).
    #[serde(default)]
    pub static_cells: Vec<CellMutation>,
    /// Partition-level tombstone: deletes the entire partition.
    #[serde(default)]
    pub partition_tombstone: Option<TombstoneMarker>,
    /// Range tombstones: delete rows within a clustering key range.
    #[serde(default)]
    pub range_tombstones: Vec<RangeTombstoneMarker>,
}

/// A tombstone marker with timestamp and local deletion time.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TombstoneMarker {
    pub timestamp: i64,
    pub local_deletion_time: i32,
}

/// A range tombstone that deletes all rows with clustering keys in [start, end].
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RangeTombstoneMarker {
    pub start: Vec<u8>,
    pub end: Vec<u8>,
    pub timestamp: i64,
    pub local_deletion_time: i32,
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
    /// Whether the current segment has been mirrored into `cdc_raw`.
    cdc_current_segment_mirrored: Mutex<bool>,
    /// Metrics.
    pub metrics: CommitLogMetrics,
}

impl CommitLog {
    fn encryption_enabled(config: &CommitLogConfig) -> bool {
        config
            .encryptor
            .as_ref()
            .is_some_and(|encryptor| encryptor.is_enabled())
    }

    fn append_payload(seg: &mut Segment, payload: &[u8], config: &CommitLogConfig) -> Result<u64> {
        match config.encryptor.as_ref() {
            Some(encryptor) if encryptor.is_enabled() => {
                let codec = EncryptingSegmentWriter::new(encryptor.clone());
                seg.append_entry_with_codec(payload, &codec)
            }
            _ => seg.append_entry(payload),
        }
    }

    /// Compute the exact encoded entry size as it will be written into the
    /// commitlog entry stream: `[len][flags][crc][payload]`.
    ///
    /// This mirrors `Segment::append_entry_with_codec` transformations so CDC
    /// budget checks can happen before the append is persisted.
    fn projected_entry_size(&self, payload: &[u8], segment_flags: SegmentFlags) -> Result<u64> {
        let (mut actual_payload, _) = if segment_flags.compression_enabled && payload.len() > 64 {
            let compressed = lz4_flex::compress_prepend_size(payload);
            if compressed.len() < payload.len() {
                (compressed, 1u8)
            } else {
                (payload.to_vec(), 0u8)
            }
        } else {
            (payload.to_vec(), 0u8)
        };

        if let Some(encryptor) = self.config.encryptor.as_ref().filter(|e| e.is_enabled()) {
            let codec = EncryptingSegmentWriter::new(encryptor.clone());
            actual_payload = codec
                .encode_block(&actual_payload)
                .map_err(CommitLogError::Serialization)?;
        }

        Ok(4 + 1 + 4 + actual_payload.len() as u64)
    }

    fn read_segment_payloads(
        seg: &Segment,
        policy: CorruptionPolicy,
        config: &CommitLogConfig,
    ) -> Vec<Result<Vec<u8>>> {
        match config.encryptor.as_ref() {
            Some(encryptor) if encryptor.is_enabled() => {
                let codec = EncryptingSegmentWriter::new(encryptor.clone());
                seg.read_entries_with_codec(policy, &codec)
            }
            _ => seg.read_entries_with_policy(policy),
        }
    }

    /// Open or create a commit log in the configured directory.
    pub fn open(config: CommitLogConfig) -> Result<Self> {
        fs::create_dir_all(&config.directory)?;
        restore_archived_segments(&config)?;

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
            encryption_enabled: Self::encryption_enabled(&config),
            ..SegmentFlags::default()
        };
        let segment = Segment::create_with_flags(&config.directory, next_id, seg_flags)?;

        // Initialize CDC raw directory if enabled.
        if config.cdc.enabled {
            fs::create_dir_all(&config.cdc.raw_directory)?;
        }

        info!(
            directory = %config.directory.display(),
            segment_id = next_id,
            compression = config.compression_enabled,
            encryption = Self::encryption_enabled(&config),
            cdc = config.cdc.enabled,
            "Commit log opened"
        );

        Ok(Self {
            config,
            current_segment: Mutex::new(segment),
            next_segment_id: AtomicU64::new(next_id + 1),
            truncation_points: Mutex::new(TruncationPoints::new()),
            recycle_pool: Mutex::new(Vec::new()),
            cdc_current_segment_mirrored: Mutex::new(false),
            metrics: CommitLogMetrics::default(),
        })
    }

    /// Append a mutation. Returns the segment-id and offset where it was written.
    pub fn append(&self, mutation: &Mutation) -> Result<(u64, u64)> {
        let payload = serde_json::to_vec(mutation)
            .map_err(|e| CommitLogError::Serialization(e.to_string()))?;

        let should_track_cdc = mutation.cdc_enabled && self.config.cdc.enabled;
        let (seg_id, offset) = {
            let mut seg = self.current_segment.lock();
            let mut cdc_mirrored = self.cdc_current_segment_mirrored.lock();

            // Rotate if this write would exceed the max segment size.
            if seg.size() + payload.len() as u64 + 12 > self.config.max_segment_size {
                let old_id = seg.id();
                seg.sync()?;

                if *cdc_mirrored {
                    self.update_cdc_sidecar(&seg, true)?;
                }

                // Archive hook
                self.archive_segment(&seg);

                let new_id = self.next_segment_id.fetch_add(1, Ordering::SeqCst);
                let new_seg = self.create_or_recycle_segment(new_id)?;
                *seg = new_seg;
                *cdc_mirrored = false;

                self.metrics
                    .segments_rotated
                    .fetch_add(1, Ordering::Relaxed);
                debug!(old_id, new_id, "Segment rotated");
            }

            if should_track_cdc {
                let projected_entry_size = self.projected_entry_size(&payload, seg.flags())?;
                self.ensure_cdc_space_budget(&seg, projected_entry_size, *cdc_mirrored)?;
            }

            let offset = Self::append_payload(&mut seg, &payload, &self.config)?;
            let seg_id = seg.id();

            if self.config.sync_policy == SyncPolicy::Batch {
                seg.sync()?;
            }

            if should_track_cdc {
                self.update_cdc_sidecar(&seg, false)?;
                *cdc_mirrored = true;
            }

            (seg_id, offset)
        };

        self.metrics
            .bytes_written
            .fetch_add(payload.len() as u64, Ordering::Relaxed);
        self.metrics.entries_written.fetch_add(1, Ordering::Relaxed);

        if should_track_cdc {
            self.metrics
                .cdc_entries_written
                .fetch_add(1, Ordering::Relaxed);
        }

        Ok((seg_id, offset))
    }

    /// Force sync the current segment.
    pub fn sync(&self) -> Result<()> {
        let mut seg = self.current_segment.lock();
        seg.sync()?;

        if self.config.cdc.enabled && *self.cdc_current_segment_mirrored.lock() {
            self.update_cdc_sidecar(&seg, false)?;
        }
        Ok(())
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
        restore_archived_segments(&self.config)?;

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
                    let entries = Self::read_segment_payloads(&seg, policy, &self.config);
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
            encryption_enabled: Self::encryption_enabled(&self.config),
            ..SegmentFlags::default()
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

    fn ensure_cdc_space_budget(
        &self,
        current_segment: &Segment,
        projected_entry_size: u64,
        already_mirrored: bool,
    ) -> Result<()> {
        let used = dir_size(&self.config.cdc.raw_directory).unwrap_or(0);
        let projected = if already_mirrored {
            used + projected_entry_size
        } else {
            used + current_segment.size() + projected_entry_size
        };

        if projected > self.config.cdc.size_limit {
            return Err(CommitLogError::CdcSpaceLimitExceeded {
                used,
                limit: self.config.cdc.size_limit,
            });
        }
        Ok(())
    }

    fn update_cdc_sidecar(&self, segment: &Segment, completed: bool) -> Result<()> {
        fs::create_dir_all(&self.config.cdc.raw_directory)?;
        let source = segment.path();
        let destination = self
            .config
            .cdc
            .raw_directory
            .join(source.file_name().unwrap_or_default());

        if !destination.exists() {
            fs::hard_link(source, &destination)?;
            debug!(
                source = %source.display(),
                destination = %destination.display(),
                "Mirrored commitlog segment into cdc_raw via hard-link"
            );
        }

        crate::cdc::write_cdc_index(&destination, segment.size(), completed)?;
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

fn restore_archived_segments(config: &CommitLogConfig) -> Result<()> {
    if !config.archiving.restore_on_replay {
        return Ok(());
    }

    let Some(archive_dir) = config.archiving.archive_directory.as_ref() else {
        return Ok(());
    };
    if !archive_dir.exists() {
        return Ok(());
    }

    fs::create_dir_all(&config.directory)?;

    let mut archived = list_segment_files(archive_dir)?;
    archived.sort();
    for archived_path in archived {
        let file_name = archived_path.file_name().unwrap_or_default();
        let destination = config.directory.join(file_name);
        if destination.exists() {
            continue;
        }

        if let Some(command_template) = config.archiving.restore_command.as_ref() {
            let from = archived_path.display().to_string();
            let to = destination.display().to_string();
            let name = file_name.to_string_lossy();
            let command = command_template
                .replace("%from", &from)
                .replace("%to", &to)
                .replace("%name", &name);
            let status = std::process::Command::new("sh")
                .args(["-c", &command])
                .status()?;
            if !status.success() {
                return Err(CommitLogError::Io(std::io::Error::other(format!(
                    "commitlog restore command failed for {}",
                    archived_path.display()
                ))));
            }
        } else {
            fs::copy(&archived_path, &destination)?;
        }

        debug!(
            from = %archived_path.display(),
            to = %destination.display(),
            "Restored archived commitlog segment"
        );
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::encrypted::CommitLogEncryptor;
    use super::*;
    use std::sync::Arc;
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
            static_cells: Vec::new(),
            partition_tombstone: None,
            range_tombstones: Vec::new(),
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

    #[derive(Debug)]
    struct XorEncryptor(u8);

    impl CommitLogEncryptor for XorEncryptor {
        fn encrypt_segment(&self, data: &[u8]) -> std::result::Result<Vec<u8>, String> {
            Ok(data.iter().map(|byte| byte ^ self.0).collect())
        }

        fn decrypt_segment(&self, data: &[u8]) -> std::result::Result<Vec<u8>, String> {
            self.encrypt_segment(data)
        }

        fn is_enabled(&self) -> bool {
            true
        }
    }

    #[test]
    fn encrypted_commitlog_round_trips_with_matching_encryptor() {
        let dir = TempDir::new().unwrap();
        let config = CommitLogConfig {
            encryptor: Some(Arc::new(XorEncryptor(0x5a))),
            ..test_config(dir.path())
        };
        let cl = CommitLog::open(config.clone()).unwrap();

        let mutation = test_mutation("ks", "secure_tbl", b"secure_pk");
        cl.append(&mutation).unwrap();
        cl.sync().unwrap();

        // Ciphertext should not deserialize as JSON if replay is attempted
        // without the configured decryptor.
        let mut plain_config = config.clone();
        plain_config.encryptor = None;
        let plain_replay = CommitLog::open(plain_config).unwrap().replay().unwrap();
        assert_eq!(plain_replay.mutations.len(), 0);
        assert!(plain_replay.corrupt_entries > 0);

        // Replay with the same encryptor must recover the mutation.
        let replay = cl.replay().unwrap();
        assert_eq!(replay.mutations.len(), 1);
        assert_eq!(replay.mutations[0].table, "secure_tbl");
        assert_eq!(replay.mutations[0].partition_key, b"secure_pk");
    }

    #[test]
    fn encrypted_commitlog_sets_segment_header_flag() {
        let dir = TempDir::new().unwrap();
        let config = CommitLogConfig {
            encryptor: Some(Arc::new(XorEncryptor(0x33))),
            ..test_config(dir.path())
        };
        let cl = CommitLog::open(config.clone()).unwrap();

        cl.append(&test_mutation("ks", "t1", b"pk1")).unwrap();
        cl.sync().unwrap();

        let segments = list_segment_files(&config.directory).unwrap();
        assert!(!segments.is_empty());
        let read_segment = Segment::open_for_read(&segments[0]).unwrap();
        assert!(read_segment.flags().encryption_enabled);
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
    fn restore_on_replay_loads_archived_segments() {
        let dir = TempDir::new().unwrap();
        let archive_dir = dir.path().join("archive");
        let restore_dir = dir.path().join("restore");
        fs::create_dir_all(&archive_dir).unwrap();

        let archived_mutation = test_mutation("ks", "archived_table", b"archived_pk");
        let archived_payload = serde_json::to_vec(&archived_mutation).unwrap();
        let mut archived_segment = Segment::create(&archive_dir, 7).unwrap();
        archived_segment.append_entry(&archived_payload).unwrap();
        archived_segment.sync().unwrap();

        let config = CommitLogConfig {
            directory: restore_dir.clone(),
            archiving: ArchivingConfig {
                archive_directory: Some(archive_dir.clone()),
                restore_on_replay: true,
                ..Default::default()
            },
            ..test_config(&restore_dir)
        };
        let cl = CommitLog::open(config).unwrap();

        assert!(restore_dir.join(segment_filename(7)).exists());
        assert!(cl.current_segment_id() > 7);

        let result = cl.replay().unwrap();
        assert!(
            result
                .mutations
                .iter()
                .any(|mutation| mutation.table == "archived_table"
                    && mutation.partition_key == b"archived_pk")
        );
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
        let commitlog_dir = config.directory.clone();
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

        let infos = crate::cdc::list_cdc_segment_infos(&cdc_dir).unwrap();
        assert_eq!(infos.len(), 1);
        assert_eq!(infos[0].durable_offset, Some(infos[0].size_bytes));
        assert!(!infos[0].completed);
        let segment_name = infos[0].path.file_name().unwrap();
        assert!(commitlog_dir.join(segment_name).exists());

        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            let nlink = fs::metadata(&infos[0].path).unwrap().nlink();
            assert!(
                nlink >= 2,
                "CDC segment should be mirrored via hard-link (nlink={nlink})"
            );
        }

        let mutations = crate::cdc::read_cdc_segment_mutations(
            &infos[0].path,
            segment::CorruptionPolicy::StopOnCorrupt,
        )
        .unwrap();
        // CDC now mirrors commitlog segments, so non-CDC mutations that share the
        // segment can appear alongside CDC-enabled ones.
        assert!(mutations.iter().any(|mutation| mutation.table == "t2"));
        assert!(mutations.iter().any(|mutation| mutation.cdc_enabled));
    }

    #[test]
    fn cdc_space_limit_rejects_before_commit() {
        let dir = TempDir::new().unwrap();
        let cdc_dir = dir.path().join("cdc_raw");
        let config = CommitLogConfig {
            cdc: CdcConfig {
                enabled: true,
                raw_directory: cdc_dir.clone(),
                size_limit: 1,
            },
            ..test_config(dir.path())
        };
        let cl = CommitLog::open(config).unwrap();

        let mut mutation = test_mutation("ks", "too_big_for_cdc", b"pk");
        mutation.cdc_enabled = true;

        let err = cl.append(&mutation).unwrap_err();
        assert!(matches!(err, CommitLogError::CdcSpaceLimitExceeded { .. }));

        let snap = cl.metrics_snapshot();
        assert_eq!(snap.entries_written, 0);
        assert_eq!(snap.cdc_entries_written, 0);

        cl.sync().unwrap();
        let replay = cl.replay().unwrap();
        assert!(
            replay.mutations.is_empty(),
            "CDC budget failure must not persist the rejected mutation"
        );

        let infos = crate::cdc::list_cdc_segment_infos(&cdc_dir).unwrap();
        assert!(infos.is_empty());
    }

    #[test]
    fn cdc_writes_with_compression_round_trip() {
        let dir = TempDir::new().unwrap();
        let cdc_dir = dir.path().join("cdc_raw");
        let config = CommitLogConfig {
            compression_enabled: true,
            cdc: CdcConfig {
                enabled: true,
                raw_directory: cdc_dir.clone(),
                size_limit: 1024 * 1024,
            },
            ..test_config(dir.path())
        };
        let cl = CommitLog::open(config).unwrap();

        let mut mutation = test_mutation("ks", "t_comp", b"pk_comp");
        mutation.cdc_enabled = true;
        cl.append(&mutation).unwrap();

        let infos = crate::cdc::list_cdc_segment_infos(&cdc_dir).unwrap();
        assert_eq!(infos.len(), 1);

        let read_segment = Segment::open_for_read(&infos[0].path).unwrap();
        assert!(read_segment.flags().compression_enabled);
        assert!(!read_segment.flags().encryption_enabled);

        let mutations = crate::cdc::read_cdc_segment_mutations(
            &infos[0].path,
            segment::CorruptionPolicy::StopOnCorrupt,
        )
        .unwrap();
        assert_eq!(mutations.len(), 1);
        assert_eq!(mutations[0].table, "t_comp");
    }

    #[test]
    fn cdc_segment_inherits_encryption_and_compression_flags() {
        let dir = TempDir::new().unwrap();
        let cdc_dir = dir.path().join("cdc_raw");
        let config = CommitLogConfig {
            compression_enabled: true,
            encryptor: Some(Arc::new(XorEncryptor(0x11))),
            cdc: CdcConfig {
                enabled: true,
                raw_directory: cdc_dir.clone(),
                size_limit: 1024 * 1024,
            },
            ..test_config(dir.path())
        };
        let cl = CommitLog::open(config).unwrap();

        let mut mutation = test_mutation("ks", "t_enc", b"pk_enc");
        mutation.cdc_enabled = true;
        cl.append(&mutation).unwrap();

        let infos = crate::cdc::list_cdc_segment_infos(&cdc_dir).unwrap();
        assert_eq!(infos.len(), 1);

        let read_segment = Segment::open_for_read(&infos[0].path).unwrap();
        assert!(read_segment.flags().compression_enabled);
        assert!(read_segment.flags().encryption_enabled);
    }

    #[test]
    fn cdc_marks_rotated_segments_completed() {
        let dir = TempDir::new().unwrap();
        let cdc_dir = dir.path().join("cdc_raw");
        let config = CommitLogConfig {
            max_segment_size: 256,
            cdc: CdcConfig {
                enabled: true,
                raw_directory: cdc_dir.clone(),
                size_limit: 8 * 1024 * 1024,
            },
            ..test_config(dir.path())
        };
        let cl = CommitLog::open(config).unwrap();

        for i in 0..20 {
            let mut mutation = test_mutation("ks", &format!("tbl_{i}"), &[i as u8]);
            mutation.cdc_enabled = true;
            cl.append(&mutation).unwrap();
        }

        let infos = crate::cdc::list_cdc_segment_infos(&cdc_dir).unwrap();
        assert!(!infos.is_empty());
        assert!(
            infos.iter().any(|info| info.completed),
            "Expected at least one completed CDC segment after rotation"
        );
    }
}
