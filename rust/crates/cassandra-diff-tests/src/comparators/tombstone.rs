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

//! Tombstone and TTL comparator.
//!
//! Compares deletion and expiration semantics between Java and Rust
//! implementations, including tombstone generation, TTL tracking,
//! and gc_grace_seconds behavior.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.db.DeletionTime`
//! - `org.apache.cassandra.db.LivenessInfo`
//! - `org.apache.cassandra.db.rows.Cell` (TTL handling)

use serde::{Deserialize, Serialize};

/// Tombstone comparison entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TombstoneCheck {
    /// The query used to check tombstone behavior.
    pub query: String,
    /// Whether the row should be visible (false = deleted/tombstoned).
    pub row_visible: bool,
    /// Expected row count after deletion.
    pub expected_row_count: usize,
    /// Description of the scenario.
    pub description: String,
}

/// TTL comparison entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TtlCheck {
    /// The query used to check TTL.
    pub query: String,
    /// Whether TTL should be present on the row.
    pub has_ttl: bool,
    /// Expected TTL value (approximate, within tolerance).
    pub expected_ttl_approx: Option<u32>,
    /// Tolerance in seconds for TTL comparison.
    pub ttl_tolerance_seconds: u32,
}

/// Compare TTL values with tolerance for timing differences.
///
/// TTL values decrease over time, so we can't compare exact values.
/// Instead, we check that both implementations agree on:
/// 1. Whether a TTL is present at all.
/// 2. The approximate remaining TTL (within tolerance).
pub fn compare_ttl(
    java_ttl: Option<u32>,
    rust_ttl: Option<u32>,
    tolerance_seconds: u32,
) -> (bool, Option<String>) {
    match (java_ttl, rust_ttl) {
        (None, None) => (true, None),
        (Some(_), None) => (false, Some("Java has TTL but Rust does not".into())),
        (None, Some(_)) => (false, Some("Rust has TTL but Java does not".into())),
        (Some(j), Some(r)) => {
            let diff = (j as i64 - r as i64).unsigned_abs() as u32;
            if diff <= tolerance_seconds {
                (true, None)
            } else {
                (
                    false,
                    Some(format!(
                        "TTL mismatch: java={} rust={} diff={} tolerance={}",
                        j, r, diff, tolerance_seconds
                    )),
                )
            }
        }
    }
}

/// Standard tombstone test scenarios.
pub fn standard_tombstone_scenarios() -> Vec<TombstoneCheck> {
    vec![
        TombstoneCheck {
            query: "SELECT * FROM diff_test.simple_kv WHERE key = 'k3'".into(),
            row_visible: false,
            expected_row_count: 0,
            description: "Row deleted via DELETE statement should not be visible".into(),
        },
        TombstoneCheck {
            query: "SELECT * FROM diff_test.simple_kv".into(),
            row_visible: true,
            expected_row_count: 2,
            description: "Non-deleted rows should still be visible (k1, k2 remain after k3 delete)"
                .into(),
        },
    ]
}

/// Standard TTL test scenarios.
pub fn standard_ttl_scenarios() -> Vec<TtlCheck> {
    vec![
        TtlCheck {
            query: "SELECT pk, value, TTL(value) FROM diff_test.ttl_test WHERE pk = 1".into(),
            has_ttl: true,
            expected_ttl_approx: Some(86400),
            ttl_tolerance_seconds: 60,
        },
        TtlCheck {
            query: "SELECT pk, value, TTL(value) FROM diff_test.ttl_test WHERE pk = 2".into(),
            has_ttl: false,
            expected_ttl_approx: None,
            ttl_tolerance_seconds: 0,
        },
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compare_ttl_both_none() {
        let (ok, detail) = compare_ttl(None, None, 10);
        assert!(ok);
        assert!(detail.is_none());
    }

    #[test]
    fn compare_ttl_java_only() {
        let (ok, _) = compare_ttl(Some(100), None, 10);
        assert!(!ok);
    }

    #[test]
    fn compare_ttl_within_tolerance() {
        let (ok, _) = compare_ttl(Some(86400), Some(86395), 10);
        assert!(ok);
    }

    #[test]
    fn compare_ttl_outside_tolerance() {
        let (ok, _) = compare_ttl(Some(86400), Some(86000), 10);
        assert!(!ok);
    }

    #[test]
    fn standard_scenarios_defined() {
        assert_eq!(standard_tombstone_scenarios().len(), 2);
        assert_eq!(standard_ttl_scenarios().len(), 2);
    }
}
