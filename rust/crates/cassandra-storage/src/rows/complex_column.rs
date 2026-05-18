// Licensed under Apache License, Version 2.0.

//! Complex column data (collections: maps, sets, lists).
//!
//! ## Java Oracle
//! `org.apache.cassandra.db.rows.ComplexColumnData`

use std::collections::BTreeMap;

use cassandra_common::tombstone::DeletionTime;

use super::cell::{CellData, CellPath};

/// Data for a multi-cell (non-frozen) complex column.
///
/// A complex column has a top-level deletion time (the "complex deletion")
/// and a map of cells keyed by their `CellPath`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ComplexColumnData {
    /// Column name.
    pub column: String,
    /// Complex deletion: deletes all cells with timestamp <= this.
    pub complex_deletion: DeletionTime,
    /// Individual cells keyed by path.
    pub cells: BTreeMap<CellPath, CellData>,
}

impl ComplexColumnData {
    /// Create an empty complex column.
    pub fn new(column: String) -> Self {
        Self {
            column,
            complex_deletion: DeletionTime::LIVE,
            cells: BTreeMap::new(),
        }
    }

    /// Create with a deletion.
    pub fn with_deletion(column: String, deletion: DeletionTime) -> Self {
        Self {
            column,
            complex_deletion: deletion,
            cells: BTreeMap::new(),
        }
    }

    /// Add a cell. If a cell at the same path exists, merge (higher timestamp wins).
    pub fn add_cell(&mut self, cell: CellData) {
        let path = cell
            .path
            .clone()
            .expect("complex column cells must have a path");
        self.cells
            .entry(path)
            .and_modify(|existing| {
                *existing = existing.merge_with(&cell);
            })
            .or_insert(cell);
    }

    /// Returns cells that are live at the given time, considering complex deletion.
    pub fn live_cells(&self, now_in_seconds: i32) -> Vec<&CellData> {
        self.cells
            .values()
            .filter(|cell| {
                // Cell must be live
                if !cell.is_live(now_in_seconds) {
                    return false;
                }
                // Cell must not be superseded by complex deletion
                if !self.complex_deletion.is_live()
                    && cell.timestamp <= self.complex_deletion.marked_for_delete_at
                {
                    return false;
                }
                true
            })
            .collect()
    }

    /// Returns `true` if no live cells remain.
    pub fn is_empty_at(&self, now_in_seconds: i32) -> bool {
        self.live_cells(now_in_seconds).is_empty()
    }

    /// Merge another complex column into this one.
    pub fn merge_with(&mut self, other: &ComplexColumnData) {
        // Merge complex deletion
        if other.complex_deletion.supersedes(&self.complex_deletion) {
            self.complex_deletion = other.complex_deletion;
        }
        // Merge cells
        for (path, cell) in &other.cells {
            self.cells
                .entry(path.clone())
                .and_modify(|existing| {
                    *existing = existing.merge_with(cell);
                })
                .or_insert_with(|| cell.clone());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cassandra_common::ttl::NO_TTL;

    fn collection_cell(col: &str, path: Vec<u8>, val: &[u8], ts: i64) -> CellData {
        CellData {
            column: col.to_string(),
            value: Some(val.to_vec()),
            timestamp: ts,
            ttl: NO_TTL,
            local_deletion_time: i32::MAX,
            path: Some(CellPath(path)),
        }
    }

    #[test]
    fn add_and_read_cells() {
        let mut ccd = ComplexColumnData::new("tags".to_string());
        ccd.add_cell(collection_cell("tags", vec![1], b"a", 100));
        ccd.add_cell(collection_cell("tags", vec![2], b"b", 100));
        assert_eq!(ccd.live_cells(0).len(), 2);
    }

    #[test]
    fn complex_deletion_hides_old_cells() {
        let mut ccd = ComplexColumnData::new("tags".to_string());
        ccd.add_cell(collection_cell("tags", vec![1], b"a", 100));
        ccd.complex_deletion = DeletionTime::new(200, 200);
        assert!(ccd.is_empty_at(0));
    }

    #[test]
    fn cell_newer_than_complex_deletion_survives() {
        let mut ccd = ComplexColumnData::new("tags".to_string());
        ccd.add_cell(collection_cell("tags", vec![1], b"a", 300));
        ccd.complex_deletion = DeletionTime::new(200, 200);
        assert_eq!(ccd.live_cells(0).len(), 1);
    }

    #[test]
    fn merge_complex_columns() {
        let mut a = ComplexColumnData::new("tags".to_string());
        a.add_cell(collection_cell("tags", vec![1], b"a", 100));

        let mut b = ComplexColumnData::new("tags".to_string());
        b.add_cell(collection_cell("tags", vec![1], b"b", 200));
        b.add_cell(collection_cell("tags", vec![2], b"c", 100));

        a.merge_with(&b);
        assert_eq!(a.cells.len(), 2);
        let cell1 = a.cells.get(&CellPath(vec![1])).unwrap();
        assert_eq!(cell1.value.as_deref(), Some(b"b".as_slice()));
    }

    #[test]
    fn merge_complex_deletion() {
        let mut a =
            ComplexColumnData::with_deletion("tags".to_string(), DeletionTime::new(100, 100));
        let b = ComplexColumnData::with_deletion("tags".to_string(), DeletionTime::new(200, 200));
        a.merge_with(&b);
        assert_eq!(a.complex_deletion.marked_for_delete_at, 200);
    }
}
