// Licensed under Apache License, Version 2.0.

//! CDC continuity verification across migration boundary.
//!
//! ## Java Oracle
//! - `o.a.c.db.commitlog.CommitLog` (CDC mode)
//! - `o.a.c.db.commitlog.CommitLogSegment` (segment IDs)

use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::fs;
use std::io;
use std::path::Path;

use tracing::{debug, info, warn};

/// CDC segment metadata.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct CdcSegmentInfo {
    pub id: u64,
    pub size: u64,
    pub consumed: bool,
}

/// CDC state at a point in time.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CdcState {
    pub source: String,
    pub segment_ids: BTreeSet<u64>,
    pub last_consumed_id: Option<u64>,
    pub last_written_id: Option<u64>,
    pub total_segments: usize,
}

/// CDC continuity check result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CdcContinuityReport {
    pub continuous: bool,
    pub source_state: CdcState,
    pub target_state: CdcState,
    pub missing_segments: Vec<u64>,
    pub new_segments: Vec<u64>,
    pub gaps: Vec<(u64, u64)>,
    pub handoff: Option<CdcHandoff>,
}

/// Handoff point between Java and Rust CDC.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CdcHandoff {
    pub last_java_segment: u64,
    pub first_rust_segment: u64,
    pub contiguous: bool,
}

/// Scan a CDC directory and build state.
pub fn scan_cdc_directory(dir: &Path, source: &str) -> io::Result<CdcState> {
    let mut ids = BTreeSet::new();
    if dir.exists() {
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().to_string();
            if let Some(id) = parse_segment_id(&name) {
                ids.insert(id);
            }
        }
    }
    let last = ids.iter().last().copied();
    let total = ids.len();
    Ok(CdcState {
        source: source.into(),
        segment_ids: ids,
        last_consumed_id: None,
        last_written_id: last,
        total_segments: total,
    })
}

fn parse_segment_id(name: &str) -> Option<u64> {
    if let Some(rest) = name.strip_prefix("CommitLog-") {
        return rest.strip_suffix(".log")?.parse().ok();
    }
    if let Some(rest) = name.strip_prefix("cdc-") {
        return rest.strip_suffix(".log")?.parse().ok();
    }
    None
}

/// Check CDC continuity between source and target states.
pub fn check_continuity(source: &CdcState, target: &CdcState) -> CdcContinuityReport {
    let missing: Vec<u64> = source
        .segment_ids
        .difference(&target.segment_ids)
        .copied()
        .collect();
    let new_segs: Vec<u64> = target
        .segment_ids
        .difference(&source.segment_ids)
        .copied()
        .collect();
    let all: BTreeSet<u64> = source
        .segment_ids
        .union(&target.segment_ids)
        .copied()
        .collect();
    let gaps = find_gaps(&all);

    let handoff = match (source.last_written_id, target.segment_ids.iter().next()) {
        (Some(last), Some(&first)) if first > last => Some(CdcHandoff {
            last_java_segment: last,
            first_rust_segment: first,
            contiguous: first == last + 1,
        }),
        (Some(last), Some(&first)) => Some(CdcHandoff {
            last_java_segment: last,
            first_rust_segment: first,
            contiguous: false,
        }),
        _ => None,
    };

    let continuous = missing.is_empty() && gaps.is_empty();
    if !continuous {
        warn!(
            missing = missing.len(),
            gaps = gaps.len(),
            "CDC continuity issue"
        );
    } else {
        info!("CDC continuity check passed");
    }

    CdcContinuityReport {
        continuous,
        source_state: source.clone(),
        target_state: target.clone(),
        missing_segments: missing,
        new_segments: new_segs,
        gaps,
        handoff,
    }
}

fn find_gaps(ids: &BTreeSet<u64>) -> Vec<(u64, u64)> {
    let v: Vec<u64> = ids.iter().copied().collect();
    let mut gaps = Vec::new();
    for w in v.windows(2) {
        if w[1] > w[0] + 1 {
            gaps.push((w[0] + 1, w[1] - 1));
        }
    }
    gaps
}

/// Write CDC checkpoint for migration handoff.
pub fn write_checkpoint(state: &CdcState, path: &Path) -> io::Result<()> {
    let json =
        serde_json::to_vec_pretty(state).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
    fs::write(path, json)?;
    debug!(path = %path.display(), "CDC checkpoint written");
    Ok(())
}

/// Read CDC checkpoint.
pub fn read_checkpoint(path: &Path) -> io::Result<CdcState> {
    let data = fs::read_to_string(path)?;
    serde_json::from_str(&data).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn make_cdc_dir(dir: &Path, ids: &[u64]) {
        fs::create_dir_all(dir).unwrap();
        for id in ids {
            fs::write(dir.join(format!("CommitLog-{}.log", id)), "data").unwrap();
        }
    }

    #[test]
    fn scan_segments() {
        let dir = TempDir::new().unwrap();
        make_cdc_dir(dir.path(), &[1, 2, 3, 5]);
        let state = scan_cdc_directory(dir.path(), "java").unwrap();
        assert_eq!(state.total_segments, 4);
        assert_eq!(state.last_written_id, Some(5));
    }

    #[test]
    fn continuity_ok() {
        let src = CdcState {
            source: "java".into(),
            segment_ids: [1, 2, 3].into(),
            last_consumed_id: Some(3),
            last_written_id: Some(3),
            total_segments: 3,
        };
        let tgt = CdcState {
            source: "rust".into(),
            segment_ids: [1, 2, 3, 4].into(),
            last_consumed_id: None,
            last_written_id: Some(4),
            total_segments: 4,
        };
        let report = check_continuity(&src, &tgt);
        assert!(report.continuous);
        assert!(report.missing_segments.is_empty());
    }

    #[test]
    fn continuity_with_gaps() {
        let src = CdcState {
            source: "java".into(),
            segment_ids: [1, 2, 3].into(),
            last_consumed_id: None,
            last_written_id: Some(3),
            total_segments: 3,
        };
        let tgt = CdcState {
            source: "rust".into(),
            segment_ids: [1, 3, 5].into(),
            last_consumed_id: None,
            last_written_id: Some(5),
            total_segments: 3,
        };
        let report = check_continuity(&src, &tgt);
        assert!(!report.continuous);
        assert!(report.missing_segments.contains(&2));
    }

    #[test]
    fn handoff_contiguous() {
        let src = CdcState {
            source: "java".into(),
            segment_ids: [1, 2, 3].into(),
            last_consumed_id: Some(3),
            last_written_id: Some(3),
            total_segments: 3,
        };
        let tgt = CdcState {
            source: "rust".into(),
            segment_ids: [4, 5, 6].into(),
            last_consumed_id: None,
            last_written_id: Some(6),
            total_segments: 3,
        };
        let report = check_continuity(&src, &tgt);
        let h = report.handoff.unwrap();
        assert_eq!(h.last_java_segment, 3);
        assert_eq!(h.first_rust_segment, 4);
        assert!(h.contiguous);
    }

    #[test]
    fn checkpoint_roundtrip() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("cdc-ckpt.json");
        let state = CdcState {
            source: "java".into(),
            segment_ids: [10, 11, 12].into(),
            last_consumed_id: Some(9),
            last_written_id: Some(12),
            total_segments: 3,
        };
        write_checkpoint(&state, &path).unwrap();
        let loaded = read_checkpoint(&path).unwrap();
        assert_eq!(loaded.segment_ids, state.segment_ids);
    }

    #[test]
    fn find_gaps_works() {
        let ids: BTreeSet<u64> = [1, 2, 5, 10].into();
        assert_eq!(find_gaps(&ids), vec![(3, 4), (6, 9)]);
    }

    #[test]
    fn empty_dir() {
        let dir = TempDir::new().unwrap();
        let state = scan_cdc_directory(dir.path(), "java").unwrap();
        assert_eq!(state.total_segments, 0);
    }
}
