// Licensed under Apache License, Version 2.0.

//! Data validation for migration correctness.
//!
//! Provides row-count comparison, partition-level checksum validation,
//! and sample query comparison between Java and Rust clusters.
//!
//! ## Java Oracle
//! - `nodetool tablestats` (row counts, SSTable sizes)
//! - `SELECT count(*) FROM ...` (row counts)
//! - `org.apache.cassandra.utils.MurmurHash` (partition hashing)

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

// ─── Validation Configuration ─────────────────────────────────────────────

/// Configuration for a migration data validation run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationConfig {
    /// Tables to validate (keyspace.table format).
    pub tables: Vec<String>,
    /// Sample CQL queries to execute on both clusters and compare results.
    pub sample_queries: Vec<SampleQuery>,
    /// Whether to validate row counts.
    pub validate_row_counts: bool,
    /// Whether to validate partition checksums.
    pub validate_checksums: bool,
    /// Maximum number of partitions to checksum per table.
    pub max_partitions_per_table: usize,
    /// Tolerance for row count differences (0.0 = exact match required).
    pub row_count_tolerance: f64,
}

impl Default for ValidationConfig {
    fn default() -> Self {
        Self {
            tables: Vec::new(),
            sample_queries: Vec::new(),
            validate_row_counts: true,
            validate_checksums: true,
            max_partitions_per_table: 10_000,
            row_count_tolerance: 0.0,
        }
    }
}

/// A sample CQL query to run against both clusters.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SampleQuery {
    pub name: String,
    pub cql: String,
    pub expected_columns: Vec<String>,
}

// ─── Validation Report ────────────────────────────────────────────────────

/// Complete validation report for a migration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationReport {
    /// Overall result.
    pub passed: bool,
    /// Per-table row count comparison.
    pub row_counts: Vec<RowCountCheck>,
    /// Per-table checksum comparison.
    pub checksums: Vec<ChecksumCheck>,
    /// Sample query comparison results.
    pub query_checks: Vec<QueryCheck>,
    /// Summary statistics.
    pub summary: ValidationSummary,
}

/// Row count comparison for a single table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RowCountCheck {
    pub table: String,
    pub source_count: u64,
    pub target_count: u64,
    pub difference: i64,
    pub passed: bool,
}

impl RowCountCheck {
    /// Create a row count check comparing source and target counts.
    pub fn new(table: String, source: u64, target: u64, tolerance: f64) -> Self {
        let diff = target as i64 - source as i64;
        let passed = if source == 0 && target == 0 {
            true
        } else if source == 0 {
            false
        } else {
            (diff.unsigned_abs() as f64 / source as f64) <= tolerance
        };

        Self {
            table,
            source_count: source,
            target_count: target,
            difference: diff,
            passed,
        }
    }
}

/// Checksum comparison for a single table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChecksumCheck {
    pub table: String,
    pub partitions_checked: usize,
    pub matching: usize,
    pub mismatched: usize,
    pub passed: bool,
    /// First few mismatched partition keys (for debugging).
    pub sample_mismatches: Vec<String>,
}

/// Comparison of a sample query between clusters.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryCheck {
    pub name: String,
    pub query: String,
    pub source_rows: usize,
    pub target_rows: usize,
    pub matching_rows: usize,
    pub passed: bool,
    pub detail: String,
}

/// Summary statistics for the validation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationSummary {
    pub tables_checked: usize,
    pub tables_passed: usize,
    pub queries_checked: usize,
    pub queries_passed: usize,
    pub total_source_rows: u64,
    pub total_target_rows: u64,
}

// ─── Validation Engine ────────────────────────────────────────────────────

/// Run a full validation against provided data snapshots.
///
/// In a real deployment, this would connect to both Java and Rust clusters
/// via CQL. For now it accepts pre-collected data for offline validation.
pub fn validate_migration(
    source_data: &MigrationData,
    target_data: &MigrationData,
    config: &ValidationConfig,
) -> ValidationReport {
    let mut row_counts = Vec::new();
    let mut checksums = Vec::new();
    let mut query_checks = Vec::new();

    // Row count validation
    if config.validate_row_counts {
        for table in &config.tables {
            let src_count = source_data.row_counts.get(table).copied().unwrap_or(0);
            let tgt_count = target_data.row_counts.get(table).copied().unwrap_or(0);
            row_counts.push(RowCountCheck::new(
                table.clone(),
                src_count,
                tgt_count,
                config.row_count_tolerance,
            ));
        }
    }

    // Checksum validation
    if config.validate_checksums {
        for table in &config.tables {
            let src_checksums = source_data.partition_checksums.get(table);
            let tgt_checksums = target_data.partition_checksums.get(table);

            let check = match (src_checksums, tgt_checksums) {
                (Some(src), Some(tgt)) => compare_checksums(table, src, tgt),
                (Some(src), None) => ChecksumCheck {
                    table: table.clone(),
                    partitions_checked: src.len(),
                    matching: 0,
                    mismatched: src.len(),
                    passed: false,
                    sample_mismatches: vec!["Target has no checksum data".into()],
                },
                _ => ChecksumCheck {
                    table: table.clone(),
                    partitions_checked: 0,
                    matching: 0,
                    mismatched: 0,
                    passed: true,
                    sample_mismatches: vec![],
                },
            };

            checksums.push(check);
        }
    }

    // Query comparison
    for sq in &config.sample_queries {
        let src_result = source_data.query_results.get(&sq.name);
        let tgt_result = target_data.query_results.get(&sq.name);

        let check = match (src_result, tgt_result) {
            (Some(src_rows), Some(tgt_rows)) => {
                let matching = src_rows.iter().filter(|r| tgt_rows.contains(r)).count();
                QueryCheck {
                    name: sq.name.clone(),
                    query: sq.cql.clone(),
                    source_rows: src_rows.len(),
                    target_rows: tgt_rows.len(),
                    matching_rows: matching,
                    passed: matching == src_rows.len() && src_rows.len() == tgt_rows.len(),
                    detail: if matching == src_rows.len() && src_rows.len() == tgt_rows.len() {
                        "All rows match".into()
                    } else {
                        format!(
                            "{}/{} rows matched ({} source, {} target)",
                            matching,
                            src_rows.len(),
                            src_rows.len(),
                            tgt_rows.len()
                        )
                    },
                }
            }
            _ => QueryCheck {
                name: sq.name.clone(),
                query: sq.cql.clone(),
                source_rows: 0,
                target_rows: 0,
                matching_rows: 0,
                passed: false,
                detail: "Missing query results from one or both clusters".into(),
            },
        };

        query_checks.push(check);
    }

    // Summary
    let tables_passed = row_counts.iter().filter(|c| c.passed).count()
        + checksums.iter().filter(|c| c.passed).count();
    let tables_checked = row_counts.len() + checksums.len();
    let queries_checked = query_checks.len();
    let queries_passed = query_checks.iter().filter(|c| c.passed).count();

    let overall_passed = row_counts.iter().all(|c| c.passed)
        && checksums.iter().all(|c| c.passed)
        && query_checks.iter().all(|c| c.passed);

    let total_source_rows: u64 = row_counts.iter().map(|c| c.source_count).sum();
    let total_target_rows: u64 = row_counts.iter().map(|c| c.target_count).sum();

    ValidationReport {
        passed: overall_passed,
        row_counts,
        checksums,
        query_checks,
        summary: ValidationSummary {
            tables_checked,
            tables_passed,
            queries_checked,
            queries_passed,
            total_source_rows,
            total_target_rows,
        },
    }
}

fn compare_checksums(
    table: &str,
    source: &BTreeMap<String, u64>,
    target: &BTreeMap<String, u64>,
) -> ChecksumCheck {
    let mut matching = 0;
    let mut mismatched = 0;
    let mut sample_mismatches = Vec::new();

    for (pk, src_checksum) in source {
        match target.get(pk) {
            Some(tgt_checksum) if tgt_checksum == src_checksum => matching += 1,
            _ => {
                mismatched += 1;
                if sample_mismatches.len() < 10 {
                    sample_mismatches.push(pk.clone());
                }
            }
        }
    }

    // Count target-only partitions as mismatches
    for pk in target.keys() {
        if !source.contains_key(pk) {
            mismatched += 1;
            if sample_mismatches.len() < 10 {
                sample_mismatches.push(format!("{} (target-only)", pk));
            }
        }
    }

    ChecksumCheck {
        table: table.to_string(),
        partitions_checked: matching + mismatched,
        matching,
        mismatched,
        passed: mismatched == 0,
        sample_mismatches,
    }
}

// ─── Pre-collected Data ───────────────────────────────────────────────────

/// Pre-collected data from a cluster for offline validation.
///
/// In production, a collector tool would connect to each cluster via CQL,
/// gather row counts, compute checksums, and run sample queries.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationData {
    /// Cluster identifier.
    pub cluster: String,
    /// Row counts per table (keyspace.table → count).
    pub row_counts: BTreeMap<String, u64>,
    /// Per-partition checksums per table.
    pub partition_checksums: BTreeMap<String, BTreeMap<String, u64>>,
    /// Results of sample queries (query name → rows as JSON strings).
    pub query_results: BTreeMap<String, Vec<String>>,
}

impl MigrationData {
    pub fn new(cluster: &str) -> Self {
        Self {
            cluster: cluster.to_string(),
            row_counts: BTreeMap::new(),
            partition_checksums: BTreeMap::new(),
            query_results: BTreeMap::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identical_data_passes() {
        let mut src = MigrationData::new("java");
        src.row_counts.insert("ks.users".into(), 1000);
        src.row_counts.insert("ks.orders".into(), 5000);

        let tgt = src.clone();
        let mut tgt = tgt;
        tgt.cluster = "rust".to_string();

        let config = ValidationConfig {
            tables: vec!["ks.users".into(), "ks.orders".into()],
            validate_row_counts: true,
            validate_checksums: false,
            ..Default::default()
        };

        let report = validate_migration(&src, &tgt, &config);
        assert!(report.passed);
        assert_eq!(report.row_counts.len(), 2);
        for rc in &report.row_counts {
            assert!(rc.passed);
            assert_eq!(rc.difference, 0);
        }
    }

    #[test]
    fn row_count_mismatch_fails() {
        let mut src = MigrationData::new("java");
        src.row_counts.insert("ks.t1".into(), 1000);

        let mut tgt = MigrationData::new("rust");
        tgt.row_counts.insert("ks.t1".into(), 999);

        let config = ValidationConfig {
            tables: vec!["ks.t1".into()],
            validate_row_counts: true,
            validate_checksums: false,
            row_count_tolerance: 0.0,
            ..Default::default()
        };

        let report = validate_migration(&src, &tgt, &config);
        assert!(!report.passed);
    }

    #[test]
    fn row_count_within_tolerance_passes() {
        let mut src = MigrationData::new("java");
        src.row_counts.insert("ks.t1".into(), 1000);

        let mut tgt = MigrationData::new("rust");
        tgt.row_counts.insert("ks.t1".into(), 999);

        let config = ValidationConfig {
            tables: vec!["ks.t1".into()],
            validate_row_counts: true,
            validate_checksums: false,
            row_count_tolerance: 0.01,
            ..Default::default()
        };

        let report = validate_migration(&src, &tgt, &config);
        assert!(report.passed);
    }

    #[test]
    fn checksum_mismatch() {
        let mut src = MigrationData::new("java");
        let mut src_ck = BTreeMap::new();
        src_ck.insert("pk1".into(), 12345u64);
        src_ck.insert("pk2".into(), 67890u64);
        src.partition_checksums.insert("ks.t1".into(), src_ck);

        let mut tgt = MigrationData::new("rust");
        let mut tgt_ck = BTreeMap::new();
        tgt_ck.insert("pk1".into(), 12345u64);
        tgt_ck.insert("pk2".into(), 99999u64); // mismatch
        tgt.partition_checksums.insert("ks.t1".into(), tgt_ck);

        let config = ValidationConfig {
            tables: vec!["ks.t1".into()],
            validate_row_counts: false,
            validate_checksums: true,
            ..Default::default()
        };

        let report = validate_migration(&src, &tgt, &config);
        assert!(!report.passed);
        assert_eq!(report.checksums[0].mismatched, 1);
        assert!(!report.checksums[0].sample_mismatches.is_empty());
    }

    #[test]
    fn query_check_match() {
        let mut src = MigrationData::new("java");
        src.query_results
            .insert("q1".into(), vec!["row1".into(), "row2".into()]);

        let mut tgt = MigrationData::new("rust");
        tgt.query_results
            .insert("q1".into(), vec!["row1".into(), "row2".into()]);

        let config = ValidationConfig {
            tables: vec![],
            sample_queries: vec![SampleQuery {
                name: "q1".into(),
                cql: "SELECT * FROM ks.t1 LIMIT 10".into(),
                expected_columns: vec![],
            }],
            validate_row_counts: false,
            validate_checksums: false,
            ..Default::default()
        };

        let report = validate_migration(&src, &tgt, &config);
        assert!(report.query_checks[0].passed);
    }

    #[test]
    fn report_serializable() {
        let report = ValidationReport {
            passed: true,
            row_counts: vec![],
            checksums: vec![],
            query_checks: vec![],
            summary: ValidationSummary {
                tables_checked: 0,
                tables_passed: 0,
                queries_checked: 0,
                queries_passed: 0,
                total_source_rows: 0,
                total_target_rows: 0,
            },
        };
        let json = serde_json::to_string(&report).unwrap();
        assert!(json.contains("passed"));
    }
}
