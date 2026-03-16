// Licensed under Apache License, Version 2.0.

//! FQL (Full Query Log) format conversion.
//!
//! Converts Java FQL logs (Chronicle-Queue binary format) to JSON
//! for the shadow traffic replay tool in `cassandra-diff-tests`.
//!
//! ## Java Oracle
//! - `o.a.c.fql.FullQueryLogger`
//! - `o.a.c.tools.fqltool.FQLQueryIterator`
//! - Chronicle-Queue binary format (opaque)
//!
//! ## Supported Formats
//! - **Input**: Chronicle-Queue binary (header parse) or pre-converted JSON
//! - **Output**: JSON array of `FqlEntry` (compatible with shadow_traffic.rs)

use serde::{Deserialize, Serialize};
use std::fs;
use std::io;
use std::path::Path;

use tracing::{debug, info, warn};

/// A single FQL entry (matches the shadow_traffic::FqlEntry format).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FqlEntry {
    pub timestamp: i64,
    pub query: String,
    pub keyspace: Option<String>,
    pub consistency: String,
    pub expected_result: Option<serde_json::Value>,
}

/// Summary of an FQL conversion.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConversionReport {
    pub source_path: String,
    pub target_path: String,
    pub source_format: FqlFormat,
    pub entries_read: usize,
    pub entries_written: usize,
    pub errors: usize,
    pub error_samples: Vec<String>,
}

/// FQL format type.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum FqlFormat {
    /// Chronicle-Queue binary format (Java native).
    ChronicleQueue,
    /// JSON array of FqlEntry objects.
    Json,
    /// Unknown format.
    Unknown,
}

/// Detect the format of an FQL file.
pub fn detect_format(path: &Path) -> io::Result<FqlFormat> {
    let data = fs::read(path)?;
    if data.is_empty() {
        return Ok(FqlFormat::Unknown);
    }

    // JSON starts with '[' or '{' (optionally with BOM/whitespace)
    let first_non_ws = data.iter().find(|b| !b.is_ascii_whitespace());
    match first_non_ws {
        Some(b'[') | Some(b'{') => Ok(FqlFormat::Json),
        // Chronicle-Queue magic bytes (simplified check)
        Some(b) if *b == 0x00 || *b > 0x7F => Ok(FqlFormat::ChronicleQueue),
        _ => {
            // Try parsing as JSON
            if serde_json::from_slice::<Vec<FqlEntry>>(&data).is_ok() {
                Ok(FqlFormat::Json)
            } else {
                Ok(FqlFormat::Unknown)
            }
        }
    }
}

/// Convert an FQL file from any supported format to JSON.
pub fn convert_to_json(source: &Path, target: &Path) -> io::Result<ConversionReport> {
    let format = detect_format(source)?;
    info!(format = ?format, source = %source.display(), "Converting FQL");

    match format {
        FqlFormat::Json => copy_json(source, target),
        FqlFormat::ChronicleQueue => convert_chronicle(source, target),
        FqlFormat::Unknown => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Unknown FQL format; cannot convert",
        )),
    }
}

/// Copy and validate a JSON FQL file.
fn copy_json(source: &Path, target: &Path) -> io::Result<ConversionReport> {
    let data = fs::read_to_string(source)?;
    let entries: Vec<FqlEntry> =
        serde_json::from_str(&data).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

    let count = entries.len();
    let json_out = serde_json::to_vec_pretty(&entries).map_err(io::Error::other)?;
    fs::write(target, &json_out)?;

    Ok(ConversionReport {
        source_path: source.display().to_string(),
        target_path: target.display().to_string(),
        source_format: FqlFormat::Json,
        entries_read: count,
        entries_written: count,
        errors: 0,
        error_samples: vec![],
    })
}

/// Convert Chronicle-Queue binary to JSON.
///
/// NOTE: Full Chronicle-Queue parsing requires understanding the Java
/// serialization format. This is a framework that extracts what it can
/// and logs unparseable sections. For production use, run the Java
/// `fqltool dump` command first and pipe JSON output to this converter.
fn convert_chronicle(source: &Path, target: &Path) -> io::Result<ConversionReport> {
    let data = fs::read(source)?;
    let mut entries = Vec::new();
    let mut errors = 0usize;
    let mut error_samples = Vec::new();

    // Simplified binary scan: look for CQL-like strings in the binary data.
    // Real implementation would use the Chronicle-Queue wire format spec.
    let mut offset = 0;
    while offset < data.len() {
        if let Some(entry) = try_extract_entry(&data, &mut offset) {
            entries.push(entry);
        } else {
            offset += 1;
            if offset % 4096 == 0 {
                errors += 1;
                if error_samples.len() < 5 {
                    error_samples.push(format!("Unparseable at offset {}", offset));
                }
            }
        }
    }

    if entries.is_empty() {
        warn!(source = %source.display(), "No entries extracted from Chronicle-Queue file");
        warn!("Consider using Java `fqltool dump` to pre-convert to JSON");
    }

    let json = serde_json::to_vec_pretty(&entries).map_err(io::Error::other)?;
    fs::write(target, &json)?;

    let written = entries.len();
    Ok(ConversionReport {
        source_path: source.display().to_string(),
        target_path: target.display().to_string(),
        source_format: FqlFormat::ChronicleQueue,
        entries_read: written + errors,
        entries_written: written,
        errors,
        error_samples,
    })
}

/// Try to extract an FQL entry from binary data at the given offset.
fn try_extract_entry(data: &[u8], offset: &mut usize) -> Option<FqlEntry> {
    // Look for CQL statement markers in the binary stream.
    let markers: &[&[u8]] = &[
        b"SELECT", b"INSERT", b"UPDATE", b"DELETE", b"CREATE", b"ALTER", b"DROP",
    ];

    for marker in markers {
        let mlen = marker.len();
        if *offset + mlen <= data.len() && &data[*offset..*offset + mlen] == *marker {
            // Try to find the end of the query (null terminator or newline)
            let start = *offset;
            let mut end = start;
            while end < data.len() && data[end] != 0 && data[end] != b'\n' {
                end += 1;
            }
            if end > start + 3 {
                if let Ok(query) = std::str::from_utf8(&data[start..end]) {
                    let query = query.trim().to_string();
                    *offset = end + 1;
                    return Some(FqlEntry {
                        timestamp: 0,
                        query,
                        keyspace: None,
                        consistency: "ONE".into(),
                        expected_result: None,
                    });
                }
            }
        }
    }
    None
}

/// Merge multiple FQL JSON files into a single file, sorted by timestamp.
pub fn merge_fql_files(sources: &[&Path], target: &Path) -> io::Result<usize> {
    let mut all_entries: Vec<FqlEntry> = Vec::new();

    for src in sources {
        let data = fs::read_to_string(src)?;
        let entries: Vec<FqlEntry> = serde_json::from_str(&data).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("{}: {}", src.display(), e),
            )
        })?;
        all_entries.extend(entries);
    }

    all_entries.sort_by_key(|e| e.timestamp);
    let count = all_entries.len();

    let json = serde_json::to_vec_pretty(&all_entries).map_err(io::Error::other)?;
    fs::write(target, &json)?;

    debug!(sources = sources.len(), entries = count, "FQL files merged");
    Ok(count)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn detect_json_format() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("fql.json");
        fs::write(
            &path,
            r#"[{"timestamp":1,"query":"SELECT 1","consistency":"ONE"}]"#,
        )
        .unwrap();
        assert_eq!(detect_format(&path).unwrap(), FqlFormat::Json);
    }

    #[test]
    fn detect_binary_format() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("fql.bin");
        fs::write(&path, vec![0x00, 0x01, 0xFF, 0xFE]).unwrap();
        assert_eq!(detect_format(&path).unwrap(), FqlFormat::ChronicleQueue);
    }

    #[test]
    fn convert_json_to_json() {
        let dir = TempDir::new().unwrap();
        let src = dir.path().join("src.json");
        let tgt = dir.path().join("tgt.json");

        let entries = vec![FqlEntry {
            timestamp: 100,
            query: "SELECT * FROM ks.t1".into(),
            keyspace: Some("ks".into()),
            consistency: "QUORUM".into(),
            expected_result: None,
        }];
        fs::write(&src, serde_json::to_string(&entries).unwrap()).unwrap();

        let report = convert_to_json(&src, &tgt).unwrap();
        assert_eq!(report.entries_read, 1);
        assert_eq!(report.entries_written, 1);
        assert_eq!(report.errors, 0);
        assert!(tgt.exists());
    }

    #[test]
    fn merge_multiple_files() {
        let dir = TempDir::new().unwrap();
        let f1 = dir.path().join("f1.json");
        let f2 = dir.path().join("f2.json");
        let out = dir.path().join("merged.json");

        let e1 = vec![FqlEntry {
            timestamp: 2,
            query: "Q2".into(),
            keyspace: None,
            consistency: "ONE".into(),
            expected_result: None,
        }];
        let e2 = vec![FqlEntry {
            timestamp: 1,
            query: "Q1".into(),
            keyspace: None,
            consistency: "ONE".into(),
            expected_result: None,
        }];

        fs::write(&f1, serde_json::to_string(&e1).unwrap()).unwrap();
        fs::write(&f2, serde_json::to_string(&e2).unwrap()).unwrap();

        let count = merge_fql_files(&[f1.as_path(), f2.as_path()], &out).unwrap();
        assert_eq!(count, 2);

        let merged: Vec<FqlEntry> =
            serde_json::from_str(&fs::read_to_string(&out).unwrap()).unwrap();
        assert_eq!(merged[0].timestamp, 1);
        assert_eq!(merged[1].timestamp, 2);
    }

    #[test]
    fn entry_roundtrip() {
        let e = FqlEntry {
            timestamp: 999,
            query: "INSERT INTO ks.t (id) VALUES (1)".into(),
            keyspace: Some("ks".into()),
            consistency: "LOCAL_QUORUM".into(),
            expected_result: Some(serde_json::json!({"kind": "void"})),
        };
        let json = serde_json::to_string(&e).unwrap();
        let parsed: FqlEntry = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.query, e.query);
        assert_eq!(parsed.timestamp, e.timestamp);
    }
}
