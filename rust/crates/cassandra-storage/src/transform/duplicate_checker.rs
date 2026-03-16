// Licensed under Apache License, Version 2.0.

//! Duplicate row checker: detects duplicate clustering keys in merge iteration.
//!
//! ## Java Oracle
//! `org.apache.cassandra.db.rows.UnfilteredRowIterators.DuplicateRowChecker`

use std::collections::HashSet;

use crate::rows::unfiltered::RowData;
use super::transformation::Transformation;

/// Detects duplicate clustering keys during merge iteration.
///
/// This should not happen in correct operation but can occur after
/// corruption or bugs. Logs a warning on duplicates.
pub struct DuplicateRowChecker {
    seen: HashSet<Vec<u8>>,
    partition_key: Vec<u8>,
    duplicates_found: usize,
}

impl DuplicateRowChecker {
    pub fn new(partition_key: Vec<u8>) -> Self {
        Self {
            seen: HashSet::new(),
            partition_key,
            duplicates_found: 0,
        }
    }

    /// Number of duplicates detected so far.
    pub fn duplicates_found(&self) -> usize {
        self.duplicates_found
    }
}

impl Transformation for DuplicateRowChecker {
    fn apply_to_row(&mut self, row: RowData) -> Option<RowData> {
        if !self.seen.insert(row.clustering_key.clone()) {
            self.duplicates_found += 1;
            tracing::warn!(
                partition_key = ?self.partition_key,
                clustering_key = ?row.clustering_key,
                "Duplicate clustering key detected in partition"
            );
        }
        Some(row) // Always pass through; this is a checker, not a filter
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rows::liveness::LivenessInfo;

    fn row(ck: &[u8]) -> RowData {
        let mut r = RowData::new(ck.to_vec());
        r.liveness_info = LivenessInfo::create(100);
        r
    }

    #[test]
    fn no_duplicates() {
        let mut checker = DuplicateRowChecker::new(b"pk".to_vec());
        checker.apply_to_row(row(b"ck1"));
        checker.apply_to_row(row(b"ck2"));
        assert_eq!(checker.duplicates_found(), 0);
    }

    #[test]
    fn detects_duplicates() {
        let mut checker = DuplicateRowChecker::new(b"pk".to_vec());
        checker.apply_to_row(row(b"ck1"));
        checker.apply_to_row(row(b"ck1")); // duplicate
        assert_eq!(checker.duplicates_found(), 1);
    }

    #[test]
    fn passes_through() {
        let mut checker = DuplicateRowChecker::new(b"pk".to_vec());
        let result = checker.apply_to_row(row(b"ck1"));
        assert!(result.is_some());
        let result = checker.apply_to_row(row(b"ck1"));
        assert!(result.is_some()); // still passes through even on duplicate
    }
}
