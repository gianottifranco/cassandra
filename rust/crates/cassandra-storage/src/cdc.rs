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
    let segment = Segment::open_for_read(path)?;
    let mut mutations = Vec::new();
    for entry in segment.read_entries_with_policy(policy) {
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
    let mut mutations = Vec::new();
    for info in list_cdc_segment_infos(dir)? {
        if info.completed {
            mutations.extend(read_cdc_segment_mutations(&info.path, policy)?);
        }
    }
    Ok(mutations)
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
    use tempfile::TempDir;

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
}
