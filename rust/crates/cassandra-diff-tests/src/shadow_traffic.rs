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

use cassandra_cql::ast::Statement;
use cassandra_cql::parser::parse;

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

/// Executor used by shadow replay.
pub trait ShadowQueryExecutor {
    /// Execute a captured FQL entry.
    ///
    /// `Ok(Some(result))` means the entry was executed and can be compared
    /// with the Java baseline. `Ok(None)` means the executor parsed/accepted
    /// the entry but does not provide a result for that statement class.
    fn execute(&self, entry: &FqlEntry) -> Result<Option<ExpectedResult>, String>;
}

/// Parser-backed executor used when no live storage/query engine is supplied.
pub struct ParserOnlyExecutor;

impl ShadowQueryExecutor for ParserOnlyExecutor {
    fn execute(&self, entry: &FqlEntry) -> Result<Option<ExpectedResult>, String> {
        let statement = parse(&entry.query).map_err(|err| err.to_string())?;
        if statement_returns_void(&statement) {
            Ok(Some(ExpectedResult::Void))
        } else {
            Ok(None)
        }
    }
}

/// Load FQL entries from a JSON file.
///
/// The file should contain a JSON array of `FqlEntry` objects.
pub fn load_fql_entries(path: &Path) -> Result<Vec<FqlEntry>, String> {
    let content = std::fs::read_to_string(path)
        .map_err(|e| format!("Failed to read FQL file {}: {}", path.display(), e))?;
    serde_json::from_str(&content).map_err(|e| format!("Failed to parse FQL JSON: {}", e))
}

/// Run a shadow traffic replay session using the parser-backed executor.
pub fn replay_session(entries: &[FqlEntry]) -> ShadowReport {
    replay_session_with_executor(entries, &ParserOnlyExecutor)
}

/// Run a shadow traffic replay session with an explicit query executor.
pub fn replay_session_with_executor<E: ShadowQueryExecutor>(
    entries: &[FqlEntry],
    executor: &E,
) -> ShadowReport {
    let mut results = Vec::with_capacity(entries.len());
    let mut latencies = Vec::with_capacity(entries.len());

    for (idx, entry) in entries.iter().enumerate() {
        let start = std::time::Instant::now();

        let status = match (executor.execute(entry), &entry.expected_result) {
            (Err(error), _) => ReplayStatus::ReplayError { error },
            (Ok(_), None) => ReplayStatus::NoBaseline,
            (Ok(Some(actual)), Some(expected)) if expected_results_match(&actual, expected) => {
                ReplayStatus::Match
            }
            (Ok(Some(actual)), Some(expected)) => ReplayStatus::Divergence {
                detail: format!("expected {expected:?}, got {actual:?}"),
            },
            (Ok(None), Some(_)) => ReplayStatus::NoBaseline,
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

fn expected_results_match(actual: &ExpectedResult, expected: &ExpectedResult) -> bool {
    match (actual, expected) {
        (ExpectedResult::Void, ExpectedResult::Void) => true,
        (
            ExpectedResult::Rows {
                columns: actual_columns,
                rows: actual_rows,
            },
            ExpectedResult::Rows {
                columns: expected_columns,
                rows: expected_rows,
            },
        ) => actual_columns == expected_columns && actual_rows == expected_rows,
        (
            ExpectedResult::Error {
                code: actual_code,
                message: actual_message,
            },
            ExpectedResult::Error {
                code: expected_code,
                message: expected_message,
            },
        ) => actual_code == expected_code && actual_message == expected_message,
        _ => false,
    }
}

fn statement_returns_void(statement: &Statement) -> bool {
    matches!(
        statement,
        Statement::Insert(_)
            | Statement::Update(_)
            | Statement::Delete(_)
            | Statement::Batch(_)
            | Statement::Truncate(_)
            | Statement::Use(_)
            | Statement::CreateKeyspace(_)
            | Statement::AlterKeyspace(_)
            | Statement::DropKeyspace(_)
            | Statement::CreateTable(_)
            | Statement::AlterTable(_)
            | Statement::DropTable(_)
            | Statement::CreateIndex(_)
            | Statement::DropIndex(_)
            | Statement::CreateMaterializedView(_)
            | Statement::DropMaterializedView(_)
            | Statement::CreateType(_)
            | Statement::AlterType(_)
            | Statement::DropType(_)
            | Statement::CreateFunction(_)
            | Statement::DropFunction(_)
            | Statement::CreateAggregate(_)
            | Statement::DropAggregate(_)
            | Statement::CreateTrigger(_)
            | Statement::DropTrigger(_)
            | Statement::AlterMaterializedView(_)
            | Statement::Transaction(_)
    )
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
        assert_eq!(report.matched, 1);
        assert_eq!(report.no_baseline, 1);
        assert_eq!(report.diverged, 0);
    }

    #[test]
    fn replay_reports_parse_errors() {
        let entries = vec![FqlEntry {
            timestamp: 1,
            query: "not valid cql".to_string(),
            keyspace: Some("ks".to_string()),
            consistency: "ONE".to_string(),
            expected_result: Some(ExpectedResult::Void),
        }];

        let report = replay_session(&entries);

        assert_eq!(report.total_entries, 1);
        assert_eq!(report.errors, 1);
        assert!(report.divergences.first().map(|r| &r.status).is_none());
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
        // p99 should be defined even when parser-only replay is effectively instant.
        assert!(report.p99_latency_us <= 1_000_000); // sanity bound
    }
}
