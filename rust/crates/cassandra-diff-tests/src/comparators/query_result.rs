// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

//! Query result comparator.
//!
//! Compares CQL query result sets between Java and Rust implementations,
//! including column metadata, row values, and row counts.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.cql3.ResultSet`
//! - `org.apache.cassandra.transport.messages.ResultMessage`

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// A serialized query result for comparison.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResult {
    pub query: String,
    pub columns: Vec<String>,
    pub row_count: usize,
    pub rows: Vec<HashMap<String, serde_json::Value>>,
}

/// Result of comparing two query results.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryComparisonResult {
    pub columns_match: bool,
    pub row_count_matches: bool,
    pub rows_match: bool,
    pub details: Vec<String>,
}

impl QueryComparisonResult {
    pub fn is_match(&self) -> bool {
        self.columns_match && self.row_count_matches && self.rows_match
    }
}

/// Compare two query results.
///
/// Columns must match in name and order. Rows are compared value-by-value.
/// For unordered results, use [`compare_results_unordered`] instead.
pub fn compare_results(java: &QueryResult, rust: &QueryResult) -> QueryComparisonResult {
    let mut details = Vec::new();

    // Compare columns
    let columns_match = java.columns == rust.columns;
    if !columns_match {
        details.push(format!(
            "Column mismatch:\n  java: {:?}\n  rust: {:?}",
            java.columns, rust.columns
        ));
    }

    // Compare row count
    let row_count_matches = java.row_count == rust.row_count;
    if !row_count_matches {
        details.push(format!(
            "Row count mismatch: java={} rust={}",
            java.row_count, rust.row_count
        ));
    }

    // Compare rows (ordered, position-by-position)
    let mut rows_match = row_count_matches;
    if row_count_matches {
        for (i, (java_row, rust_row)) in java.rows.iter().zip(rust.rows.iter()).enumerate() {
            if java_row != rust_row {
                rows_match = false;
                details.push(format!(
                    "Row {} differs:\n  java: {:?}\n  rust: {:?}",
                    i, java_row, rust_row
                ));
                // Only report first 5 differences to avoid overwhelming output
                if details.len() > 10 {
                    details.push("... (truncated additional row diffs)".into());
                    break;
                }
            }
        }
    }

    QueryComparisonResult {
        columns_match,
        row_count_matches,
        rows_match,
        details,
    }
}

/// Compare two query results without regard to row order.
///
/// Useful for queries without ORDER BY where Cassandra doesn't guarantee
/// a specific row ordering.
pub fn compare_results_unordered(java: &QueryResult, rust: &QueryResult) -> QueryComparisonResult {
    let mut details = Vec::new();

    let columns_match = java.columns == rust.columns;
    if !columns_match {
        details.push(format!(
            "Column mismatch:\n  java: {:?}\n  rust: {:?}",
            java.columns, rust.columns
        ));
    }

    let row_count_matches = java.row_count == rust.row_count;
    if !row_count_matches {
        details.push(format!(
            "Row count mismatch: java={} rust={}",
            java.row_count, rust.row_count
        ));
    }

    // For unordered comparison, check that every Java row exists in Rust
    // and vice versa. We use BTreeMap for deterministic key ordering in
    // JSON serialization (HashMap iteration order is non-deterministic).
    let mut rows_match = row_count_matches;
    if row_count_matches {
        use std::collections::BTreeMap;

        let canonicalize = |rows: &[HashMap<String, serde_json::Value>]| -> Vec<String> {
            rows.iter()
                .map(|r| {
                    let ordered: BTreeMap<_, _> = r.iter().collect();
                    serde_json::to_string(&ordered).unwrap_or_default()
                })
                .collect()
        };

        let mut java_sorted = canonicalize(&java.rows);
        java_sorted.sort();
        let mut rust_sorted = canonicalize(&rust.rows);
        rust_sorted.sort();

        if java_sorted != rust_sorted {
            rows_match = false;
            // Find missing rows
            for (i, row) in java_sorted.iter().enumerate() {
                if i >= rust_sorted.len() || row != &rust_sorted[i] {
                    details.push(format!("Row set mismatch near position {}", i));
                    break;
                }
            }
        }
    }

    QueryComparisonResult {
        columns_match,
        row_count_matches,
        rows_match,
        details,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_result(rows: Vec<HashMap<String, serde_json::Value>>) -> QueryResult {
        QueryResult {
            query: "SELECT * FROM test".into(),
            columns: vec!["id".into(), "name".into()],
            row_count: rows.len(),
            rows,
        }
    }

    fn make_row(id: i64, name: &str) -> HashMap<String, serde_json::Value> {
        let mut m = HashMap::new();
        m.insert("id".into(), serde_json::json!(id));
        m.insert("name".into(), serde_json::json!(name));
        m
    }

    #[test]
    fn compare_identical_results() {
        let result = make_result(vec![make_row(1, "alice"), make_row(2, "bob")]);
        let cmp = compare_results(&result, &result);
        assert!(cmp.is_match());
    }

    #[test]
    fn compare_different_row_counts() {
        let java = make_result(vec![make_row(1, "alice")]);
        let rust = make_result(vec![make_row(1, "alice"), make_row(2, "bob")]);
        let cmp = compare_results(&java, &rust);
        assert!(!cmp.row_count_matches);
    }

    #[test]
    fn compare_different_columns() {
        let java = QueryResult {
            query: "SELECT * FROM test".into(),
            columns: vec!["id".into(), "name".into()],
            row_count: 0,
            rows: vec![],
        };
        let rust = QueryResult {
            query: "SELECT * FROM test".into(),
            columns: vec!["id".into(), "fullname".into()],
            row_count: 0,
            rows: vec![],
        };
        let cmp = compare_results(&java, &rust);
        assert!(!cmp.columns_match);
    }

    #[test]
    fn compare_unordered_same_rows() {
        let java = make_result(vec![make_row(1, "alice"), make_row(2, "bob")]);
        let rust = make_result(vec![make_row(2, "bob"), make_row(1, "alice")]);
        let cmp = compare_results_unordered(&java, &rust);
        assert!(cmp.is_match());
    }

    #[test]
    fn compare_ordered_different_order_fails() {
        let java = make_result(vec![make_row(1, "alice"), make_row(2, "bob")]);
        let rust = make_result(vec![make_row(2, "bob"), make_row(1, "alice")]);
        let cmp = compare_results(&java, &rust);
        assert!(!cmp.rows_match);
    }
}
