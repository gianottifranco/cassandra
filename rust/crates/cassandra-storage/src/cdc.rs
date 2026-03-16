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
use std::io;
use std::path::{Path, PathBuf};

use tracing::{debug, info};

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
        files.sort();
    }
    Ok(files)
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

/// Discard CDC segments that have been consumed (by ID threshold).
pub fn discard_consumed_segments(dir: &Path, up_to_id: u64) -> io::Result<u64> {
    let segments = list_cdc_segments(dir)?;
    let mut discarded = 0u64;
    for path in segments {
        let name = path.file_name().unwrap().to_string_lossy();
        if let Some(id) = parse_cdc_segment_id(&name) {
            if id <= up_to_id {
                debug!(segment = %name, "Discarding consumed CDC segment");
                fs::remove_file(&path)?;
                discarded += 1;
            }
        }
    }
    if discarded > 0 {
        info!(discarded, "Discarded consumed CDC segments");
    }
    Ok(discarded)
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
    fn discard_cdc_segments() {
        let dir = TempDir::new().unwrap();
        fs::write(dir.path().join("CommitLog-1.log"), "data1").unwrap();
        fs::write(dir.path().join("CommitLog-2.log"), "data2").unwrap();
        fs::write(dir.path().join("CommitLog-3.log"), "data3").unwrap();

        let discarded = discard_consumed_segments(dir.path(), 2).unwrap();
        assert_eq!(discarded, 2);

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
}
