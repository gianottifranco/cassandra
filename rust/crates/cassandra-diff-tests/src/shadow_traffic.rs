// Licensed under Apache License, Version 2.0.

//! Shadow traffic replay tool.
//!
//! Reads Full Query Log (FQL) entries from a JSON-formatted log file,
//! replays them against the Rust storage engine, and compares the results
//! against expected (golden) responses from the Java oracle.
//!
//! ## FQL Entry Format (JSON)
//!
//! ```json
//! {
//!   "timestamp": 1710500000000,
//!   "query": "INSERT INTO ks.users (id, name) VALUES (1, 'Alice')",
//!   "keyspace": "ks",
//!   "consistency": "ONE",
//!   "expected_result": { "kind": "void" }
//! }
//! ```
//!
//! ## Architecture
//!
//! ```text
//! ┌──────────┐    FQL JSON    ┌──────────────┐   replay    ┌──────────────┐
//! │ Java FQL │───────────────▶│ ShadowReplay │────────────▶│ Rust Engine  │
//! │ (file)   │                │              │             └──────────────┘
//! └──────────┘                │              │   compare
//!                             │              │────────────▶ DiffReport
//!                             └──────────────┘
//! ```

use serde::{Deserialize, Serialize};
use std::path::Path;

/// A single FQL entry representing a captured query from the Java cluster.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FqlEntry {
    /// Microsecond timestamp of the original query.
    pub timestamp: i64,
    /// The CQL query string.
    pub query: String,
    /// Target keyspace (if set).
    pub keyspace: Option<String>,
    /// Consistency level used.
    pub consistency: String,
    /// Expected result from Java (for comparison).
    pub expected_result: Option<ExpectedResult>,
}

/// Expected result from the Java oracle.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum ExpectedResult {
    /// Void result (successful mutation with no return value).
    #[serde(rename = "void")]
    Void,
    /// Row result with expected rows.
    #[serde(rename = "rows")]
    Rows {
        columns: Vec<String>,
        rows: Vec<Vec<serde_json::Value>>,
    },
    /// Error result.
    #[serde(rename = "error")]
    Error { code: i32, message: String },
}

/// Result of comparing a single FQL entry replay.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayResult {
    pub entry_index: usize,
    pub query: String,
    pub status: ReplayStatus,
    pub latency_us: u64,
}

/// Status of a single replay comparison.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ReplayStatus {
    /// Rust result matched Java expected result.
    Match,
    /// Rust result diverged from Java expected result.
    Divergence { detail: String },
    /// Replay failed with an error.
    ReplayError { error: String },
    /// No expected result to compare against.
    NoBaseline,
}

/// Aggregate report from a shadow traffic replay session.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShadowReport {
    pub total_entries: usize,
    pub matched: usize,
    pub diverged: usize,
    pub errors: usize,
    pub no_baseline: usize,
    pub avg_latency_us: u64,
    pub p99_latency_us: u64,
    pub divergences: Vec<ReplayResult>,
}

/// Load FQL entries from a JSON file.
///
/// The file should contain a JSON array of `FqlEntry` objects.
pub fn load_fql_entries(path: &Path) -> Result<Vec<FqlEntry>, String> {
    let content = std::fs::read_to_string(path)
        .map_err(|e| format!("Failed to read FQL file {}: {}", path.display(), e))?;
    serde_json::from_str(&content).map_err(|e| format!("Failed to parse FQL JSON: {}", e))
}

/// Run a shadow traffic replay session.
///
/// Currently a framework that validates the replay pipeline without
/// requiring a live Rust server. The actual engine integration happens
/// when the query executor is wired to the protocol layer.
pub fn replay_session(entries: &[FqlEntry]) -> ShadowReport {
    let mut results = Vec::with_capacity(entries.len());
    let mut latencies = Vec::with_capacity(entries.len());

    for (idx, entry) in entries.iter().enumerate() {
        let start = std::time::Instant::now();

        // TODO: Wire to QueryExecutor when protocol layer is connected.
        // For now, we validate the replay framework itself.
        let status = match &entry.expected_result {
            None => ReplayStatus::NoBaseline,
            Some(_expected) => {
                // Stub: in a real replay, we'd execute the query against the
                // Rust engine and compare the result.
                ReplayStatus::NoBaseline
            }
        };

        let latency_us = start.elapsed().as_micros() as u64;
        latencies.push(latency_us);

        results.push(ReplayResult {
            entry_index: idx,
            query: entry.query.clone(),
            status,
            latency_us,
        });
    }

    // Compute statistics
    latencies.sort_unstable();
    let avg_latency_us = if latencies.is_empty() {
        0
    } else {
        latencies.iter().sum::<u64>() / latencies.len() as u64
    };
    let p99_latency_us = if latencies.is_empty() {
        0
    } else {
        let idx = (latencies.len() as f64 * 0.99).ceil() as usize;
        latencies[idx.min(latencies.len() - 1)]
    };

    let matched = results
        .iter()
        .filter(|r| r.status == ReplayStatus::Match)
        .count();
    let diverged = results
        .iter()
        .filter(|r| matches!(r.status, ReplayStatus::Divergence { .. }))
        .count();
    let errors = results
        .iter()
        .filter(|r| matches!(r.status, ReplayStatus::ReplayError { .. }))
        .count();
    let no_baseline = results
        .iter()
        .filter(|r| r.status == ReplayStatus::NoBaseline)
        .count();

    let divergences: Vec<ReplayResult> = results
        .iter()
        .filter(|r| matches!(r.status, ReplayStatus::Divergence { .. }))
        .cloned()
        .collect();

    ShadowReport {
        total_entries: entries.len(),
        matched,
        diverged,
        errors,
        no_baseline,
        avg_latency_us,
        p99_latency_us,
        divergences,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn parse_fql_entries() {
        let json = r#"[
            {
                "timestamp": 1710500000000,
                "query": "INSERT INTO ks.users (id, name) VALUES (1, 'Alice')",
                "keyspace": "ks",
                "consistency": "ONE",
                "expected_result": {"kind": "void"}
            },
            {
                "timestamp": 1710500001000,
                "query": "SELECT * FROM ks.users WHERE id = 1",
                "keyspace": "ks",
                "consistency": "ONE",
                "expected_result": {
                    "kind": "rows",
                    "columns": ["id", "name"],
                    "rows": [[1, "Alice"]]
                }
            }
        ]"#;

        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("fql.json");
        let mut f = std::fs::File::create(&path).unwrap();
        f.write_all(json.as_bytes()).unwrap();

        let entries = load_fql_entries(&path).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].consistency, "ONE");
        assert!(entries[0].expected_result.is_some());
    }

    #[test]
    fn replay_empty_session() {
        let report = replay_session(&[]);
        assert_eq!(report.total_entries, 0);
        assert_eq!(report.matched, 0);
    }

    #[test]
    fn replay_with_entries() {
        let entries = vec![
            FqlEntry {
                timestamp: 1,
                query: "INSERT INTO ks.t (id) VALUES (1)".to_string(),
                keyspace: Some("ks".to_string()),
                consistency: "ONE".to_string(),
                expected_result: Some(ExpectedResult::Void),
            },
            FqlEntry {
                timestamp: 2,
                query: "SELECT * FROM ks.t".to_string(),
                keyspace: Some("ks".to_string()),
                consistency: "QUORUM".to_string(),
                expected_result: None,
            },
        ];

        let report = replay_session(&entries);
        assert_eq!(report.total_entries, 2);
        assert_eq!(report.no_baseline, 2); // Stub implementation
        assert_eq!(report.diverged, 0);
    }

    #[test]
    fn fql_entry_serialization_roundtrip() {
        let entry = FqlEntry {
            timestamp: 1710500000000,
            query: "SELECT count(*) FROM system.local".to_string(),
            keyspace: None,
            consistency: "LOCAL_ONE".to_string(),
            expected_result: Some(ExpectedResult::Error {
                code: 0x2200,
                message: "Invalid query".to_string(),
            }),
        };

        let json = serde_json::to_string(&entry).unwrap();
        let parsed: FqlEntry = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.query, entry.query);
        assert_eq!(parsed.timestamp, entry.timestamp);
    }

    #[test]
    fn shadow_report_has_latency_stats() {
        let entries: Vec<FqlEntry> = (0..100)
            .map(|i| FqlEntry {
                timestamp: i,
                query: format!("SELECT {i}"),
                keyspace: None,
                consistency: "ONE".to_string(),
                expected_result: None,
            })
            .collect();

        let report = replay_session(&entries);
        assert_eq!(report.total_entries, 100);
        // p99 should be defined (not zero, unless all queries are instant)
        // The stub runs so fast p99 could be 0, but the field should be populated
        assert!(report.p99_latency_us <= 1_000_000); // sanity bound
    }
}
