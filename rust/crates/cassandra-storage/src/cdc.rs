// Licensed under Apache License, Version 2.0.

//! Change Data Capture (CDC) module.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.db.commitlog.CommitLogSegment` (CDC mode)
//! - `org.apache.cassandra.db.commitlog.CommitLog.handleCDC()`
//!
//! ## Architecture
//!
//! When CDC is enabled for a table, mutations are also written to
//! a dedicated CDC raw directory as commit log segments. A consumer
//! process reads and processes these segments asynchronously.
//!
//! CDC segments use the same format as commit log segments.
//! Space is tracked to prevent unconstrained growth; writes are
//! rejected when the CDC raw directory exceeds the configured limit.
//!
//! CDC is controlled by the `CommitLogConfig.cdc` settings and the
//! per-mutation `cdc_enabled` flag.

use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use tracing::{debug, info};

use crate::commitlog::{
    CommitLogError, Mutation, Result as CommitLogResult,
    encrypted::{EncryptedSegmentWriter, PlainSegmentWriter},
    segment::{CorruptionPolicy, Segment},
};

const CDC_INDEX_SUFFIX: &str = "_cdc.idx";
const CDC_COMPLETED_MARKER: &str = "COMPLETED";

/// Status of the CDC subsystem.
#[derive(Debug, Clone)]
pub struct CdcStatus {
    /// Whether CDC is enabled.
    pub enabled: bool,
    /// CDC raw directory.
    pub directory: PathBuf,
    /// Current total size of CDC raw segments.
    pub used_bytes: u64,
    /// Configured size limit.
    pub limit_bytes: u64,
    /// Number of CDC segment files.
    pub segment_count: u64,
}

impl CdcStatus {
    /// Check if CDC can accept more writes.
    pub fn has_space(&self) -> bool {
        self.used_bytes < self.limit_bytes
    }

    /// Percentage of CDC space used.
    pub fn usage_percent(&self) -> f64 {
        if self.limit_bytes == 0 {
            return 100.0;
        }
        (self.used_bytes as f64 / self.limit_bytes as f64) * 100.0
    }
}

/// Java-style metadata for a CDC raw segment and its `_cdc.idx` sidecar.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CdcSegmentInfo {
    pub id: u64,
    pub path: PathBuf,
    pub index_path: PathBuf,
    pub size_bytes: u64,
    pub durable_offset: Option<u64>,
    pub completed: bool,
}

/// Cursor for incremental CDC reads across completed raw segments.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CdcReadCursor {
    /// Segment ID where the next read should resume.
    pub segment_id: u64,
    /// Mutation index inside the segment where the next read should resume.
    pub mutation_index: usize,
}

/// Batch result for incremental CDC consumption.
#[derive(Debug, Clone)]
pub struct CdcMutationBatch {
    /// Mutations returned for this batch.
    pub mutations: Vec<Mutation>,
    /// Resume cursor for subsequent reads.
    ///
    /// `None` means all currently completed segments were exhausted.
    pub next_cursor: Option<CdcReadCursor>,
}

impl CdcMutationBatch {
    /// True when there is no continuation cursor.
    pub fn is_exhausted(&self) -> bool {
        self.next_cursor.is_none()
    }
}

/// Follow-mode step result for incremental CDC consumers.
#[derive(Debug, Clone)]
pub struct CdcFollowBatch {
    /// Mutations returned for this step.
    pub mutations: Vec<Mutation>,
    /// Resume cursor for subsequent steps.
    ///
    /// In follow-mode this cursor is sticky at the terminal completed position
    /// to avoid rewinding when polls are exhausted.
    pub next_cursor: Option<CdcReadCursor>,
    /// True when no newer completed mutations were available for this step.
    pub exhausted: bool,
}

/// Configuration for reusable CDC follow consumers.
#[derive(Debug, Clone)]
pub struct CdcFollowerConfig {
    /// Maximum mutations to read per follow step.
    pub batch_size: usize,
    /// Optional threshold for consecutive non-empty steps that should trigger
    /// backpressure handling by the caller.
    pub max_consecutive_nonempty_batches: Option<usize>,
}

/// Mutable follow consumer state.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CdcFollowerState {
    /// Resume cursor for next follow step.
    pub cursor: Option<CdcReadCursor>,
    /// Number of consecutive non-empty follow steps observed.
    pub consecutive_nonempty_batches: usize,
}

/// Output of one follow-consumer step.
#[derive(Debug, Clone)]
pub struct CdcFollowerTick {
    /// Mutations and cursor progress for this step.
    pub batch: CdcFollowBatch,
    /// Updated consumer state.
    pub next_state: CdcFollowerState,
    /// Whether caller should apply backpressure before next step.
    pub backpressure_recommended: bool,
}

/// List CDC segment files in the raw directory.
pub fn list_cdc_segments(dir: &Path) -> io::Result<Vec<PathBuf>> {
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
        files.sort_by_key(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .and_then(parse_cdc_segment_id)
                .unwrap_or(u64::MAX)
        });
    }
    Ok(files)
}

/// List CDC segment files with their Java-compatible `_cdc.idx` sidecar state.
pub fn list_cdc_segment_infos(dir: &Path) -> io::Result<Vec<CdcSegmentInfo>> {
    let mut infos = Vec::new();
    for path in list_cdc_segments(dir)? {
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        let Some(id) = parse_cdc_segment_id(name) else {
            continue;
        };
        let index_path = cdc_index_path(&path);
        let (durable_offset, completed) = read_cdc_index(&index_path)?;
        infos.push(CdcSegmentInfo {
            id,
            size_bytes: fs::metadata(&path)?.len(),
            path,
            index_path,
            durable_offset,
            completed,
        });
    }
    infos.sort_by_key(|info| info.id);
    Ok(infos)
}

/// Get CDC subsystem status.
pub fn cdc_status(dir: &Path, limit: u64) -> io::Result<CdcStatus> {
    let segments = list_cdc_segments(dir)?;
    let mut used = 0u64;
    for seg_path in &segments {
        used += fs::metadata(seg_path)?.len();
    }

    Ok(CdcStatus {
        enabled: true,
        directory: dir.to_path_buf(),
        used_bytes: used,
        limit_bytes: limit,
        segment_count: segments.len() as u64,
    })
}

/// Write the Java-compatible `_cdc.idx` sidecar for a raw CDC segment.
pub fn write_cdc_index(
    segment_path: &Path,
    durable_offset: u64,
    completed: bool,
) -> io::Result<PathBuf> {
    let index_path = cdc_index_path(segment_path);
    let mut file = fs::File::create(&index_path)?;
    writeln!(file, "{durable_offset}")?;
    if completed {
        writeln!(file, "{CDC_COMPLETED_MARKER}")?;
    }
    file.sync_all()?;
    Ok(index_path)
}

/// Decode all mutations from a CDC raw segment.
pub fn read_cdc_segment_mutations(
    path: &Path,
    policy: CorruptionPolicy,
) -> CommitLogResult<Vec<Mutation>> {
    read_cdc_segment_mutations_with_codec(path, policy, &PlainSegmentWriter)
}

/// Decode all mutations from a CDC raw segment with an explicit codec.
///
/// Use this for encrypted CDC segments by passing the same codec used when
/// writing commitlog entries.
pub fn read_cdc_segment_mutations_with_codec(
    path: &Path,
    policy: CorruptionPolicy,
    codec: &dyn EncryptedSegmentWriter,
) -> CommitLogResult<Vec<Mutation>> {
    let segment = Segment::open_for_read(path)?;
    let (durable_offset, _) = read_cdc_index(&cdc_index_path(path))?;
    let entries = match durable_offset {
        Some(max_offset) => segment.read_entries_with_codec_up_to(policy, codec, max_offset),
        None => segment.read_entries_with_codec(policy, codec),
    };
    let mut mutations = Vec::new();
    for entry in entries {
        let payload = match entry {
            Ok(payload) => payload,
            Err(err) => match policy {
                CorruptionPolicy::StopOnCorrupt => return Err(err),
                CorruptionPolicy::SkipAndContinue => continue,
            },
        };
        let mutation = serde_json::from_slice::<Mutation>(&payload)
            .map_err(|err| CommitLogError::Serialization(err.to_string()))?;
        mutations.push(mutation);
    }
    Ok(mutations)
}

/// Decode mutations only from CDC segments whose sidecar contains `COMPLETED`.
pub fn read_completed_cdc_mutations(
    dir: &Path,
    policy: CorruptionPolicy,
) -> CommitLogResult<Vec<Mutation>> {
    read_completed_cdc_mutations_with_codec(dir, policy, &PlainSegmentWriter)
}

/// Decode mutations only from completed CDC segments with an explicit codec.
pub fn read_completed_cdc_mutations_with_codec(
    dir: &Path,
    policy: CorruptionPolicy,
    codec: &dyn EncryptedSegmentWriter,
) -> CommitLogResult<Vec<Mutation>> {
    let mut mutations = Vec::new();
    for info in list_cdc_segment_infos(dir)? {
        if info.completed {
            mutations.extend(read_cdc_segment_mutations_with_codec(
                &info.path, policy, codec,
            )?);
        }
    }
    Ok(mutations)
}

/// Incrementally decode mutations from completed CDC segments.
///
/// The returned cursor can be passed back to resume from the previous batch.
pub fn read_completed_cdc_mutations_batch(
    dir: &Path,
    policy: CorruptionPolicy,
    limit: usize,
    cursor: Option<&CdcReadCursor>,
) -> CommitLogResult<CdcMutationBatch> {
    read_completed_cdc_mutations_batch_with_codec(dir, policy, limit, cursor, &PlainSegmentWriter)
}

/// Incrementally decode mutations from completed CDC segments with a codec.
pub fn read_completed_cdc_mutations_batch_with_codec(
    dir: &Path,
    policy: CorruptionPolicy,
    limit: usize,
    cursor: Option<&CdcReadCursor>,
    codec: &dyn EncryptedSegmentWriter,
) -> CommitLogResult<CdcMutationBatch> {
    if limit == 0 {
        return Err(CommitLogError::Serialization(
            "CDC batch limit must be greater than zero".to_string(),
        ));
    }

    let infos = list_cdc_segment_infos(dir)?;
    let mut out = Vec::with_capacity(limit);
    let mut cursor_pending = cursor.cloned();

    for info in infos {
        if !info.completed {
            continue;
        }

        let start_index = match cursor_pending.as_ref() {
            Some(cur) if info.id < cur.segment_id => continue,
            Some(cur) if info.id == cur.segment_id => cur.mutation_index,
            Some(_) => 0,
            None => 0,
        };

        if cursor_pending.is_some() {
            cursor_pending = None;
        }

        let segment_mutations = read_cdc_segment_mutations_with_codec(&info.path, policy, codec)?;
        if start_index >= segment_mutations.len() {
            continue;
        }

        let remaining = limit - out.len();
        let to_take = remaining.min(segment_mutations.len() - start_index);
        out.extend(
            segment_mutations[start_index..start_index + to_take]
                .iter()
                .cloned(),
        );

        if out.len() == limit {
            return Ok(CdcMutationBatch {
                mutations: out,
                next_cursor: Some(CdcReadCursor {
                    segment_id: info.id,
                    mutation_index: start_index + to_take,
                }),
            });
        }
    }

    Ok(CdcMutationBatch {
        mutations: out,
        next_cursor: None,
    })
}

/// Incremental CDC follow step for completed segments.
///
/// Unlike `read_completed_cdc_mutations_batch`, this preserves a terminal
/// cursor when no newer completed entries exist, preventing consumer rewind.
pub fn read_completed_cdc_mutations_follow_step(
    dir: &Path,
    policy: CorruptionPolicy,
    limit: usize,
    cursor: Option<&CdcReadCursor>,
) -> CommitLogResult<CdcFollowBatch> {
    read_completed_cdc_mutations_follow_step_with_codec(
        dir,
        policy,
        limit,
        cursor,
        &PlainSegmentWriter,
    )
}

/// Incremental CDC follow step for completed segments with an explicit codec.
pub fn read_completed_cdc_mutations_follow_step_with_codec(
    dir: &Path,
    policy: CorruptionPolicy,
    limit: usize,
    cursor: Option<&CdcReadCursor>,
    codec: &dyn EncryptedSegmentWriter,
) -> CommitLogResult<CdcFollowBatch> {
    let batch = read_completed_cdc_mutations_batch_with_codec(dir, policy, limit, cursor, codec)?;
    if let Some(next) = batch.next_cursor {
        return Ok(CdcFollowBatch {
            mutations: batch.mutations,
            next_cursor: Some(next),
            exhausted: false,
        });
    }

    let sticky =
        terminal_completed_cursor_with_codec(dir, policy, codec)?.or_else(|| cursor.cloned());
    Ok(CdcFollowBatch {
        mutations: batch.mutations,
        next_cursor: sticky,
        exhausted: true,
    })
}

/// Execute one step of a reusable CDC follow consumer.
pub fn cdc_follower_tick(
    dir: &Path,
    policy: CorruptionPolicy,
    state: &CdcFollowerState,
    config: &CdcFollowerConfig,
) -> CommitLogResult<CdcFollowerTick> {
    cdc_follower_tick_with_codec(dir, policy, state, config, &PlainSegmentWriter)
}

/// Execute one step of a reusable CDC follow consumer with an explicit codec.
pub fn cdc_follower_tick_with_codec(
    dir: &Path,
    policy: CorruptionPolicy,
    state: &CdcFollowerState,
    config: &CdcFollowerConfig,
    codec: &dyn EncryptedSegmentWriter,
) -> CommitLogResult<CdcFollowerTick> {
    if config.batch_size == 0 {
        return Err(CommitLogError::Serialization(
            "CDC follower batch_size must be greater than zero".to_string(),
        ));
    }
    if config.max_consecutive_nonempty_batches == Some(0) {
        return Err(CommitLogError::Serialization(
            "CDC follower max_consecutive_nonempty_batches must be greater than zero".to_string(),
        ));
    }

    let batch = read_completed_cdc_mutations_follow_step_with_codec(
        dir,
        policy,
        config.batch_size,
        state.cursor.as_ref(),
        codec,
    )?;

    let mut next_state = CdcFollowerState {
        cursor: batch.next_cursor.clone(),
        consecutive_nonempty_batches: if batch.mutations.is_empty() {
            0
        } else {
            state.consecutive_nonempty_batches + 1
        },
    };

    let backpressure_recommended = config
        .max_consecutive_nonempty_batches
        .is_some_and(|limit| next_state.consecutive_nonempty_batches >= limit);
    if backpressure_recommended {
        // Start a new burst window after caller applies pacing.
        next_state.consecutive_nonempty_batches = 0;
    }

    Ok(CdcFollowerTick {
        batch,
        next_state,
        backpressure_recommended,
    })
}

fn terminal_completed_cursor_with_codec(
    dir: &Path,
    policy: CorruptionPolicy,
    codec: &dyn EncryptedSegmentWriter,
) -> CommitLogResult<Option<CdcReadCursor>> {
    let Some(last_completed) = list_cdc_segment_infos(dir)?
        .into_iter()
        .filter(|info| info.completed)
        .last()
    else {
        return Ok(None);
    };

    let count = read_cdc_segment_mutations_with_codec(&last_completed.path, policy, codec)?.len();
    Ok(Some(CdcReadCursor {
        segment_id: last_completed.id,
        mutation_index: count,
    }))
}

/// Discard CDC segments that have been consumed (by ID threshold).
pub fn discard_consumed_segments(dir: &Path, up_to_id: u64) -> io::Result<u64> {
    let segments = list_cdc_segments(dir)?;
    let mut discarded = 0u64;
    for path in segments {
        let name = path.file_name().unwrap().to_string_lossy();
        if let Some(id) = parse_cdc_segment_id(&name) {
            if id <= up_to_id {
                debug!(segment = %name, "Discarding consumed CDC segment");
                let index_path = cdc_index_path(&path);
                fs::remove_file(&path)?;
                if index_path.exists() {
                    fs::remove_file(index_path)?;
                }
                discarded += 1;
            }
        }
    }
    if discarded > 0 {
        info!(discarded, "Discarded consumed CDC segments");
    }
    Ok(discarded)
}

fn cdc_index_path(segment_path: &Path) -> PathBuf {
    let file_name = segment_path
        .file_name()
        .unwrap_or_default()
        .to_string_lossy();
    segment_path.with_file_name(format!("{file_name}{CDC_INDEX_SUFFIX}"))
}

fn read_cdc_index(path: &Path) -> io::Result<(Option<u64>, bool)> {
    if !path.exists() {
        return Ok((None, false));
    }
    let contents = fs::read_to_string(path)?;
    let mut durable_offset = None;
    let mut completed = false;
    for line in contents
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
    {
        if line == CDC_COMPLETED_MARKER {
            completed = true;
        } else if durable_offset.is_none() {
            durable_offset = line.parse::<u64>().ok();
        }
    }
    Ok((durable_offset, completed))
}

fn parse_cdc_segment_id(name: &str) -> Option<u64> {
    let name = name.strip_prefix("CommitLog-")?;
    let name = name.strip_suffix(".log")?;
    name.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commitlog::encrypted::{CommitLogEncryptor, EncryptingSegmentWriter};
    use crate::commitlog::segment::SegmentFlags;
    use std::sync::Arc;
    use tempfile::TempDir;

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
    fn cdc_status_empty() {
        let dir = TempDir::new().unwrap();
        let status = cdc_status(dir.path(), 1024).unwrap();
        assert_eq!(status.segment_count, 0);
        assert_eq!(status.used_bytes, 0);
        assert!(status.has_space());
    }

    #[test]
    fn cdc_status_with_segments() {
        let dir = TempDir::new().unwrap();
        fs::write(dir.path().join("CommitLog-1.log"), "data1").unwrap();
        fs::write(dir.path().join("CommitLog-2.log"), "data2").unwrap();

        let status = cdc_status(dir.path(), 1024).unwrap();
        assert_eq!(status.segment_count, 2);
        assert!(status.used_bytes > 0);
    }

    #[test]
    fn list_cdc_segments_sorts_by_numeric_id() {
        let dir = TempDir::new().unwrap();
        fs::write(dir.path().join("CommitLog-10.log"), "ten").unwrap();
        fs::write(dir.path().join("CommitLog-2.log"), "two").unwrap();
        fs::write(dir.path().join("CommitLog-1.log"), "one").unwrap();

        let segments = list_cdc_segments(dir.path()).unwrap();
        let names = segments
            .iter()
            .map(|path| path.file_name().unwrap().to_string_lossy().to_string())
            .collect::<Vec<_>>();
        assert_eq!(
            names,
            vec!["CommitLog-1.log", "CommitLog-2.log", "CommitLog-10.log"]
        );
    }

    #[test]
    fn discard_cdc_segments() {
        let dir = TempDir::new().unwrap();
        fs::write(dir.path().join("CommitLog-1.log"), "data1").unwrap();
        fs::write(dir.path().join("CommitLog-2.log"), "data2").unwrap();
        fs::write(dir.path().join("CommitLog-3.log"), "data3").unwrap();
        write_cdc_index(&dir.path().join("CommitLog-2.log"), 128, true).unwrap();

        let discarded = discard_consumed_segments(dir.path(), 2).unwrap();
        assert_eq!(discarded, 2);
        assert!(!dir.path().join("CommitLog-2.log_cdc.idx").exists());

        let remaining = list_cdc_segments(dir.path()).unwrap();
        assert_eq!(remaining.len(), 1);
    }

    #[test]
    fn cdc_usage_percent() {
        let status = CdcStatus {
            enabled: true,
            directory: PathBuf::from("/tmp"),
            used_bytes: 500,
            limit_bytes: 1000,
            segment_count: 1,
        };
        assert!((status.usage_percent() - 50.0).abs() < 0.01);
    }

    #[test]
    fn cdc_segment_infos_read_sidecar_state() {
        let dir = TempDir::new().unwrap();
        fs::write(dir.path().join("CommitLog-10.log"), b"ten").unwrap();
        fs::write(dir.path().join("CommitLog-2.log"), b"two").unwrap();
        write_cdc_index(&dir.path().join("CommitLog-10.log"), 512, true).unwrap();

        let infos = list_cdc_segment_infos(dir.path()).unwrap();
        assert_eq!(
            infos.iter().map(|info| info.id).collect::<Vec<_>>(),
            vec![2, 10]
        );
        assert_eq!(infos[0].durable_offset, None);
        assert!(!infos[0].completed);
        assert_eq!(infos[1].durable_offset, Some(512));
        assert!(infos[1].completed);
    }

    #[test]
    fn read_cdc_segment_mutations_respects_durable_offset() {
        let dir = TempDir::new().unwrap();
        let mut segment = Segment::create(dir.path(), 33).unwrap();

        let m1 = Mutation {
            keyspace: "ks".to_string(),
            table: "t1".to_string(),
            partition_key: b"pk1".to_vec(),
            rows: vec![],
            timestamp: 1,
            cdc_enabled: true,
            static_cells: vec![],
            partition_tombstone: None,
            range_tombstones: vec![],
        };
        let m2 = Mutation {
            keyspace: "ks".to_string(),
            table: "t2".to_string(),
            partition_key: b"pk2".to_vec(),
            rows: vec![],
            timestamp: 2,
            cdc_enabled: true,
            static_cells: vec![],
            partition_tombstone: None,
            range_tombstones: vec![],
        };
        let payload1 = serde_json::to_vec(&m1).unwrap();
        let payload2 = serde_json::to_vec(&m2).unwrap();
        segment.append_entry(&payload1).unwrap();
        let second_offset = segment.append_entry(&payload2).unwrap();
        segment.sync().unwrap();

        let path = dir.path().join("CommitLog-33.log");
        write_cdc_index(&path, second_offset, false).unwrap();

        let mutations = read_cdc_segment_mutations(&path, CorruptionPolicy::StopOnCorrupt).unwrap();
        assert_eq!(mutations.len(), 1);
        assert_eq!(mutations[0].table, "t1");
    }

    #[test]
    fn read_cdc_segment_mutations_with_codec_handles_encrypted_entries() {
        let dir = TempDir::new().unwrap();
        let flags = SegmentFlags {
            compression_enabled: true,
            encryption_enabled: true,
        };
        let codec = EncryptingSegmentWriter::new(Arc::new(XorEncryptor(0x2f)));
        let mut segment = Segment::create_with_flags(dir.path(), 44, flags).unwrap();

        let m1 = Mutation {
            keyspace: "ks".to_string(),
            table: "enc_t1".to_string(),
            partition_key: b"pk1".to_vec(),
            rows: vec![],
            timestamp: 1,
            cdc_enabled: true,
            static_cells: vec![],
            partition_tombstone: None,
            range_tombstones: vec![],
        };
        let m2 = Mutation {
            keyspace: "ks".to_string(),
            table: "enc_t2".to_string(),
            partition_key: b"pk2".to_vec(),
            rows: vec![],
            timestamp: 2,
            cdc_enabled: true,
            static_cells: vec![],
            partition_tombstone: None,
            range_tombstones: vec![],
        };
        let payload1 = serde_json::to_vec(&m1).unwrap();
        let payload2 = serde_json::to_vec(&m2).unwrap();
        segment.append_entry_with_codec(&payload1, &codec).unwrap();
        let second_offset = segment.append_entry_with_codec(&payload2, &codec).unwrap();
        segment.sync().unwrap();

        let path = dir.path().join("CommitLog-44.log");
        write_cdc_index(&path, second_offset, true).unwrap();

        let plain = read_cdc_segment_mutations(&path, CorruptionPolicy::StopOnCorrupt);
        assert!(
            plain.is_err(),
            "plaintext reader should fail on encrypted entries"
        );

        let mutations =
            read_cdc_segment_mutations_with_codec(&path, CorruptionPolicy::StopOnCorrupt, &codec)
                .unwrap();
        assert_eq!(mutations.len(), 1);
        assert_eq!(mutations[0].table, "enc_t1");
    }

    #[test]
    fn read_completed_cdc_mutations_batch_paginates_with_cursor() {
        let dir = TempDir::new().unwrap();

        let mut seg1 = Segment::create(dir.path(), 61).unwrap();
        for table in ["t1", "t2"] {
            let mutation = Mutation {
                keyspace: "ks".to_string(),
                table: table.to_string(),
                partition_key: b"pk".to_vec(),
                rows: vec![],
                timestamp: 1,
                cdc_enabled: true,
                static_cells: vec![],
                partition_tombstone: None,
                range_tombstones: vec![],
            };
            seg1.append_entry(&serde_json::to_vec(&mutation).unwrap())
                .unwrap();
        }
        seg1.sync().unwrap();
        write_cdc_index(seg1.path(), seg1.size(), true).unwrap();

        let mut seg2 = Segment::create(dir.path(), 62).unwrap();
        let mutation = Mutation {
            keyspace: "ks".to_string(),
            table: "t3".to_string(),
            partition_key: b"pk".to_vec(),
            rows: vec![],
            timestamp: 2,
            cdc_enabled: true,
            static_cells: vec![],
            partition_tombstone: None,
            range_tombstones: vec![],
        };
        seg2.append_entry(&serde_json::to_vec(&mutation).unwrap())
            .unwrap();
        seg2.sync().unwrap();
        write_cdc_index(seg2.path(), seg2.size(), true).unwrap();

        let batch1 = read_completed_cdc_mutations_batch(
            dir.path(),
            CorruptionPolicy::StopOnCorrupt,
            2,
            None,
        )
        .unwrap();
        assert_eq!(
            batch1
                .mutations
                .iter()
                .map(|m| m.table.as_str())
                .collect::<Vec<_>>(),
            vec!["t1", "t2"]
        );
        assert_eq!(
            batch1.next_cursor,
            Some(CdcReadCursor {
                segment_id: 61,
                mutation_index: 2,
            })
        );

        let batch2 = read_completed_cdc_mutations_batch(
            dir.path(),
            CorruptionPolicy::StopOnCorrupt,
            2,
            batch1.next_cursor.as_ref(),
        )
        .unwrap();
        assert_eq!(
            batch2
                .mutations
                .iter()
                .map(|m| m.table.as_str())
                .collect::<Vec<_>>(),
            vec!["t3"]
        );
        assert!(batch2.next_cursor.is_none());
        assert!(batch2.is_exhausted());
    }

    #[test]
    fn read_completed_cdc_mutations_batch_skips_incomplete_segments() {
        let dir = TempDir::new().unwrap();

        let mut incomplete = Segment::create(dir.path(), 70).unwrap();
        let m1 = Mutation {
            keyspace: "ks".to_string(),
            table: "incomplete".to_string(),
            partition_key: b"pk".to_vec(),
            rows: vec![],
            timestamp: 1,
            cdc_enabled: true,
            static_cells: vec![],
            partition_tombstone: None,
            range_tombstones: vec![],
        };
        incomplete
            .append_entry(&serde_json::to_vec(&m1).unwrap())
            .unwrap();
        incomplete.sync().unwrap();
        write_cdc_index(incomplete.path(), incomplete.size(), false).unwrap();

        let mut complete = Segment::create(dir.path(), 71).unwrap();
        let m2 = Mutation {
            keyspace: "ks".to_string(),
            table: "complete".to_string(),
            partition_key: b"pk".to_vec(),
            rows: vec![],
            timestamp: 2,
            cdc_enabled: true,
            static_cells: vec![],
            partition_tombstone: None,
            range_tombstones: vec![],
        };
        complete
            .append_entry(&serde_json::to_vec(&m2).unwrap())
            .unwrap();
        complete.sync().unwrap();
        write_cdc_index(complete.path(), complete.size(), true).unwrap();

        let batch = read_completed_cdc_mutations_batch(
            dir.path(),
            CorruptionPolicy::StopOnCorrupt,
            10,
            None,
        )
        .unwrap();
        assert_eq!(batch.mutations.len(), 1);
        assert_eq!(batch.mutations[0].table, "complete");
    }

    #[test]
    fn read_completed_cdc_mutations_batch_with_codec_resumes_encrypted_segments() {
        let dir = TempDir::new().unwrap();
        let flags = SegmentFlags {
            compression_enabled: true,
            encryption_enabled: true,
        };
        let codec = EncryptingSegmentWriter::new(Arc::new(XorEncryptor(0x3a)));
        let mut segment = Segment::create_with_flags(dir.path(), 81, flags).unwrap();

        for table in ["enc1", "enc2"] {
            let mutation = Mutation {
                keyspace: "ks".to_string(),
                table: table.to_string(),
                partition_key: b"pk".to_vec(),
                rows: vec![],
                timestamp: 3,
                cdc_enabled: true,
                static_cells: vec![],
                partition_tombstone: None,
                range_tombstones: vec![],
            };
            segment
                .append_entry_with_codec(&serde_json::to_vec(&mutation).unwrap(), &codec)
                .unwrap();
        }
        segment.sync().unwrap();
        write_cdc_index(segment.path(), segment.size(), true).unwrap();

        let batch1 = read_completed_cdc_mutations_batch_with_codec(
            dir.path(),
            CorruptionPolicy::StopOnCorrupt,
            1,
            None,
            &codec,
        )
        .unwrap();
        assert_eq!(batch1.mutations.len(), 1);
        assert_eq!(batch1.mutations[0].table, "enc1");
        assert!(batch1.next_cursor.is_some());

        let batch2 = read_completed_cdc_mutations_batch_with_codec(
            dir.path(),
            CorruptionPolicy::StopOnCorrupt,
            1,
            batch1.next_cursor.as_ref(),
            &codec,
        )
        .unwrap();
        assert_eq!(batch2.mutations.len(), 1);
        assert_eq!(batch2.mutations[0].table, "enc2");
    }

    #[test]
    fn read_completed_cdc_mutations_follow_step_keeps_terminal_cursor() {
        let dir = TempDir::new().unwrap();
        let mut segment = Segment::create(dir.path(), 91).unwrap();
        let mutation = Mutation {
            keyspace: "ks".to_string(),
            table: "tbl".to_string(),
            partition_key: b"pk".to_vec(),
            rows: vec![],
            timestamp: 10,
            cdc_enabled: true,
            static_cells: vec![],
            partition_tombstone: None,
            range_tombstones: vec![],
        };
        segment
            .append_entry(&serde_json::to_vec(&mutation).unwrap())
            .unwrap();
        segment.sync().unwrap();
        write_cdc_index(segment.path(), segment.size(), true).unwrap();

        let step1 = read_completed_cdc_mutations_follow_step(
            dir.path(),
            CorruptionPolicy::StopOnCorrupt,
            1,
            None,
        )
        .unwrap();
        assert_eq!(step1.mutations.len(), 1);
        assert!(!step1.exhausted);
        let cursor = step1.next_cursor.clone().unwrap();
        assert_eq!(cursor.segment_id, 91);
        assert_eq!(cursor.mutation_index, 1);

        let step2 = read_completed_cdc_mutations_follow_step(
            dir.path(),
            CorruptionPolicy::StopOnCorrupt,
            1,
            Some(&cursor),
        )
        .unwrap();
        assert!(step2.mutations.is_empty());
        assert!(step2.exhausted);
        assert_eq!(
            step2.next_cursor,
            Some(CdcReadCursor {
                segment_id: 91,
                mutation_index: 1
            })
        );
    }

    #[test]
    fn cdc_follower_tick_recommends_backpressure_on_threshold() {
        let dir = TempDir::new().unwrap();
        let mut segment = Segment::create(dir.path(), 92).unwrap();
        for table in ["t1", "t2", "t3"] {
            let mutation = Mutation {
                keyspace: "ks".to_string(),
                table: table.to_string(),
                partition_key: b"pk".to_vec(),
                rows: vec![],
                timestamp: 1,
                cdc_enabled: true,
                static_cells: vec![],
                partition_tombstone: None,
                range_tombstones: vec![],
            };
            segment
                .append_entry(&serde_json::to_vec(&mutation).unwrap())
                .unwrap();
        }
        segment.sync().unwrap();
        write_cdc_index(segment.path(), segment.size(), true).unwrap();

        let config = CdcFollowerConfig {
            batch_size: 1,
            max_consecutive_nonempty_batches: Some(2),
        };
        let state0 = CdcFollowerState::default();
        let tick1 = cdc_follower_tick(
            dir.path(),
            CorruptionPolicy::StopOnCorrupt,
            &state0,
            &config,
        )
        .unwrap();
        assert_eq!(tick1.batch.mutations.len(), 1);
        assert!(!tick1.backpressure_recommended);
        assert_eq!(tick1.next_state.consecutive_nonempty_batches, 1);

        let tick2 = cdc_follower_tick(
            dir.path(),
            CorruptionPolicy::StopOnCorrupt,
            &tick1.next_state,
            &config,
        )
        .unwrap();
        assert_eq!(tick2.batch.mutations.len(), 1);
        assert!(tick2.backpressure_recommended);
        assert_eq!(tick2.next_state.consecutive_nonempty_batches, 0);
    }

    #[test]
    fn cdc_follower_tick_keeps_cursor_on_exhaustion() {
        let dir = TempDir::new().unwrap();
        let mut segment = Segment::create(dir.path(), 93).unwrap();
        let mutation = Mutation {
            keyspace: "ks".to_string(),
            table: "t1".to_string(),
            partition_key: b"pk".to_vec(),
            rows: vec![],
            timestamp: 1,
            cdc_enabled: true,
            static_cells: vec![],
            partition_tombstone: None,
            range_tombstones: vec![],
        };
        segment
            .append_entry(&serde_json::to_vec(&mutation).unwrap())
            .unwrap();
        segment.sync().unwrap();
        write_cdc_index(segment.path(), segment.size(), true).unwrap();

        let config = CdcFollowerConfig {
            batch_size: 1,
            max_consecutive_nonempty_batches: None,
        };
        let tick1 = cdc_follower_tick(
            dir.path(),
            CorruptionPolicy::StopOnCorrupt,
            &CdcFollowerState::default(),
            &config,
        )
        .unwrap();
        let tick2 = cdc_follower_tick(
            dir.path(),
            CorruptionPolicy::StopOnCorrupt,
            &tick1.next_state,
            &config,
        )
        .unwrap();

        assert!(tick2.batch.mutations.is_empty());
        assert!(tick2.batch.exhausted);
        assert_eq!(tick2.next_state.cursor, tick1.next_state.cursor);
        assert_eq!(tick2.next_state.consecutive_nonempty_batches, 0);
    }

    #[test]
    fn cdc_follower_tick_rejects_invalid_config() {
        let dir = TempDir::new().unwrap();
        let state = CdcFollowerState::default();

        let err = cdc_follower_tick(
            dir.path(),
            CorruptionPolicy::StopOnCorrupt,
            &state,
            &CdcFollowerConfig {
                batch_size: 0,
                max_consecutive_nonempty_batches: None,
            },
        )
        .unwrap_err();
        assert!(
            err.to_string()
                .contains("CDC follower batch_size must be greater than zero")
        );

        let err = cdc_follower_tick(
            dir.path(),
            CorruptionPolicy::StopOnCorrupt,
            &state,
            &CdcFollowerConfig {
                batch_size: 1,
                max_consecutive_nonempty_batches: Some(0),
            },
        )
        .unwrap_err();
        assert!(
            err.to_string().contains(
                "CDC follower max_consecutive_nonempty_batches must be greater than zero"
            )
        );
    }

    #[test]
    fn read_completed_cdc_mutations_batch_rejects_zero_limit() {
        let dir = TempDir::new().unwrap();
        let err = read_completed_cdc_mutations_batch(
            dir.path(),
            CorruptionPolicy::StopOnCorrupt,
            0,
            None,
        )
        .unwrap_err();
        assert!(
            err.to_string()
                .contains("CDC batch limit must be greater than zero")
        );
    }
}
