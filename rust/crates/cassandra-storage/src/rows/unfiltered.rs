// Licensed under Apache License, Version 2.0.

//! Unfiltered row iterator model: rows and range tombstone markers.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.db.rows.Unfiltered`
//! - `org.apache.cassandra.db.rows.Row`
//! - `org.apache.cassandra.db.rows.RangeTombstoneMarker`
//! - `org.apache.cassandra.db.rows.RangeTombstoneBoundMarker`

use std::collections::BTreeMap;

use cassandra_common::tombstone::DeletionTime;

use super::cell::CellData;
use super::complex_column::ComplexColumnData;
use super::liveness::LivenessInfo;
use crate::memtable::partition::Row;

/// Data for a single simple or complex column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColumnData {
    /// A simple (single-cell) column.
    Simple(CellData),
    /// A complex (multi-cell) column: map, set, list, or non-frozen UDT.
    Complex(ComplexColumnData),
}

/// Bound kind for clustering range operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ClusteringBoundKind {
    /// Inclusive start bound.
    InclusiveStart,
    /// Exclusive start bound.
    ExclusiveStart,
    /// Inclusive end bound.
    InclusiveEnd,
    /// Exclusive end bound.
    ExclusiveEnd,
}

impl ClusteringBoundKind {
    /// Returns `true` if this is a start bound.
    pub fn is_start(&self) -> bool {
        matches!(
            self,
            ClusteringBoundKind::InclusiveStart | ClusteringBoundKind::ExclusiveStart
        )
    }

    /// Returns `true` if this is an end bound.
    pub fn is_end(&self) -> bool {
        !self.is_start()
    }

    /// Returns `true` if this bound is inclusive.
    pub fn is_inclusive(&self) -> bool {
        matches!(
            self,
            ClusteringBoundKind::InclusiveStart | ClusteringBoundKind::InclusiveEnd
        )
    }
}

/// A clustering bound: kind + clustering prefix bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusteringBound {
    pub kind: ClusteringBoundKind,
    pub values: Vec<u8>,
}

/// Range tombstone marker: open, close, or boundary.
///
/// Markers appear in the `Unfiltered` stream interleaved with rows to
/// indicate range tombstone boundaries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RangeTombstoneMarker {
    /// Opens a range tombstone.
    Open {
        bound: ClusteringBound,
        deletion: DeletionTime,
    },
    /// Closes a range tombstone.
    Close {
        bound: ClusteringBound,
        deletion: DeletionTime,
    },
    /// Boundary between two adjacent range tombstones (close one, open next).
    Boundary {
        bound: ClusteringBound,
        close_deletion: DeletionTime,
        open_deletion: DeletionTime,
    },
}

impl RangeTombstoneMarker {
    /// The clustering bound for ordering in the unfiltered stream.
    pub fn clustering_bound(&self) -> &ClusteringBound {
        match self {
            Self::Open { bound, .. } | Self::Close { bound, .. } | Self::Boundary { bound, .. } => {
                bound
            }
        }
    }

    /// Returns `true` if this marker opens a range tombstone.
    pub fn is_open(&self) -> bool {
        matches!(self, Self::Open { .. } | Self::Boundary { .. })
    }

    /// Returns `true` if this marker closes a range tombstone.
    pub fn is_close(&self) -> bool {
        matches!(self, Self::Close { .. } | Self::Boundary { .. })
    }
}

/// A rich row with proper Cassandra semantics.
///
/// Unlike `memtable::partition::Row`, this has liveness info, row deletion,
/// supports both simple and complex columns, and has a static row flag.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowData {
    /// Clustering key bytes.
    pub clustering_key: Vec<u8>,
    /// Primary-key liveness info.
    pub liveness_info: LivenessInfo,
    /// Row-level deletion.
    pub deletion: DeletionTime,
    /// Column data keyed by column name.
    pub columns: BTreeMap<String, ColumnData>,
    /// Whether this is the static row for the partition.
    pub is_static: bool,
}

impl RowData {
    /// Create a new empty row.
    pub fn new(clustering_key: Vec<u8>) -> Self {
        Self {
            clustering_key,
            liveness_info: LivenessInfo::EMPTY,
            deletion: DeletionTime::LIVE,
            columns: BTreeMap::new(),
            is_static: false,
        }
    }

    /// Create an empty static row.
    pub fn new_static() -> Self {
        Self {
            clustering_key: Vec::new(),
            liveness_info: LivenessInfo::EMPTY,
            deletion: DeletionTime::LIVE,
            columns: BTreeMap::new(),
            is_static: true,
        }
    }

    /// Returns `true` if this row has any live data at the given time.
    pub fn has_live_data(&self, now_in_seconds: i32) -> bool {
        self.has_live_data_after(now_in_seconds, DeletionTime::LIVE)
    }

    /// Returns `true` if this row has any live data after applying an enclosing deletion.
    pub fn has_live_data_after(
        &self,
        now_in_seconds: i32,
        enclosing_deletion: DeletionTime,
    ) -> bool {
        let active_deletion = if enclosing_deletion.supersedes(&self.deletion) {
            enclosing_deletion
        } else {
            self.deletion
        };

        if self.liveness_info.is_live(now_in_seconds)
            && row_timestamp_survives(self.liveness_info.timestamp, active_deletion)
        {
            return true;
        }
        self.columns.values().any(|cd| match cd {
            ColumnData::Simple(cell) => {
                cell.is_live(now_in_seconds)
                    && row_timestamp_survives(cell.timestamp, active_deletion)
            }
            ColumnData::Complex(ccd) => !ccd.is_empty_after(now_in_seconds, active_deletion),
        })
    }

    /// Returns `true` if the supplied deletion shadows all currently live row content.
    pub fn is_shadowed_by(&self, now_in_seconds: i32, deletion: DeletionTime) -> bool {
        !self.has_live_data_after(now_in_seconds, deletion)
    }

    /// Apply a row-level deletion if it supersedes the existing row deletion.
    pub fn apply_deletion(&mut self, deletion: DeletionTime) {
        if deletion.supersedes(&self.deletion) {
            self.deletion = deletion;
        }
    }

    /// Return a copy containing only data live after applying an enclosing deletion.
    pub fn live_data_after(
        &self,
        now_in_seconds: i32,
        enclosing_deletion: DeletionTime,
    ) -> Option<Self> {
        let active_deletion = if enclosing_deletion.supersedes(&self.deletion) {
            enclosing_deletion
        } else {
            self.deletion
        };

        let mut row = self.clone();
        if !row_timestamp_survives(row.liveness_info.timestamp, active_deletion) {
            row.liveness_info = LivenessInfo::EMPTY;
        }

        row.columns.retain(|_, column| match column {
            ColumnData::Simple(cell) => {
                cell.is_live(now_in_seconds)
                    && row_timestamp_survives(cell.timestamp, active_deletion)
            }
            ColumnData::Complex(complex) => {
                complex.retain_live_cells_after(now_in_seconds, active_deletion);
                !complex.cells.is_empty()
            }
        });

        if row.has_live_data_after(now_in_seconds, active_deletion) {
            Some(row)
        } else {
            None
        }
    }

    /// Add a simple column cell.
    pub fn add_cell(&mut self, cell: CellData) {
        let name = cell.column.clone();
        self.columns
            .entry(name)
            .and_modify(|existing| {
                if let ColumnData::Simple(existing_cell) = existing {
                    *existing_cell = existing_cell.merge_with(&cell);
                }
            })
            .or_insert(ColumnData::Simple(cell));
    }

    /// Add or merge complex column data.
    pub fn add_complex_column(&mut self, ccd: ComplexColumnData) {
        let name = ccd.column.clone();
        self.columns
            .entry(name)
            .and_modify(|existing| {
                if let ColumnData::Complex(existing_ccd) = existing {
                    existing_ccd.merge_with(&ccd);
                }
            })
            .or_insert(ColumnData::Complex(ccd));
    }

    /// Merge another row into this one.
    pub fn merge_with(&mut self, other: &RowData) {
        self.liveness_info = LivenessInfo::merge(self.liveness_info, other.liveness_info);
        if other.deletion.supersedes(&self.deletion) {
            self.deletion = other.deletion;
        }
        for (name, col_data) in &other.columns {
            match col_data {
                ColumnData::Simple(cell) => {
                    self.columns
                        .entry(name.clone())
                        .and_modify(|existing| {
                            if let ColumnData::Simple(existing_cell) = existing {
                                *existing_cell = existing_cell.merge_with(cell);
                            }
                        })
                        .or_insert_with(|| col_data.clone());
                }
                ColumnData::Complex(ccd) => {
                    self.columns
                        .entry(name.clone())
                        .and_modify(|existing| {
                            if let ColumnData::Complex(existing_ccd) = existing {
                                existing_ccd.merge_with(ccd);
                            }
                        })
                        .or_insert_with(|| col_data.clone());
                }
            }
        }
    }
}

fn row_timestamp_survives(timestamp: i64, deletion: DeletionTime) -> bool {
    deletion.is_live() || timestamp > deletion.marked_for_delete_at
}

/// Convert from the simplified `memtable::partition::Row`.
impl From<&Row> for RowData {
    fn from(row: &Row) -> Self {
        let mut data = RowData::new(row.clustering_key.clone());
        if row.is_tombstone {
            let ldt = row.local_deletion_time.unwrap_or(i32::MAX);
            // Use the max cell timestamp as the deletion timestamp
            let max_ts = row.cells.iter().map(|c| c.timestamp).max().unwrap_or(0);
            data.deletion = DeletionTime::new(max_ts, ldt);
        }
        for cell in &row.cells {
            data.add_cell(CellData::from(cell));
        }
        data
    }
}

/// The `Unfiltered` enum: either a row or a range tombstone marker.
///
/// This is the fundamental unit of the storage engine's row iteration.
/// An `UnfilteredRowIterator` yields a stream of these in clustering order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Unfiltered {
    /// A data row.
    Row(RowData),
    /// A range tombstone boundary marker.
    Marker(RangeTombstoneMarker),
}

impl Unfiltered {
    /// The clustering key bytes for ordering.
    pub fn clustering_key(&self) -> &[u8] {
        match self {
            Self::Row(row) => &row.clustering_key,
            Self::Marker(marker) => &marker.clustering_bound().values,
        }
    }

    /// Returns `true` if this is a row.
    pub fn is_row(&self) -> bool {
        matches!(self, Self::Row(_))
    }

    /// Returns `true` if this is a range tombstone marker.
    pub fn is_marker(&self) -> bool {
        matches!(self, Self::Marker(_))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rows::cell::CellData;
    use cassandra_common::ttl::NO_TTL;

    fn simple_cell(col: &str, val: &[u8], ts: i64) -> CellData {
        CellData {
            column: col.to_string(),
            value: Some(val.to_vec()),
            timestamp: ts,
            ttl: NO_TTL,
            local_deletion_time: i32::MAX,
            path: None,
        }
    }

    #[test]
    fn row_has_live_data() {
        let mut row = RowData::new(b"ck1".to_vec());
        row.add_cell(simple_cell("name", b"alice", 100));
        assert!(row.has_live_data(0));
    }

    #[test]
    fn row_with_liveness_info() {
        let mut row = RowData::new(b"ck1".to_vec());
        row.liveness_info = LivenessInfo::create(100);
        assert!(row.has_live_data(0));
    }

    #[test]
    fn empty_row_not_live() {
        let row = RowData::new(b"ck1".to_vec());
        assert!(!row.has_live_data(0));
    }

    #[test]
    fn row_merge() {
        let mut r1 = RowData::new(b"ck".to_vec());
        r1.add_cell(simple_cell("a", b"1", 100));

        let mut r2 = RowData::new(b"ck".to_vec());
        r2.add_cell(simple_cell("a", b"2", 200));
        r2.add_cell(simple_cell("b", b"3", 200));

        r1.merge_with(&r2);
        assert_eq!(r1.columns.len(), 2);
        if let ColumnData::Simple(cell) = &r1.columns["a"] {
            assert_eq!(cell.value.as_deref(), Some(b"2".as_slice()));
        }
    }

    #[test]
    fn row_deletion_shadows_older_cells_and_liveness() {
        let mut row = RowData::new(b"ck".to_vec());
        row.liveness_info = LivenessInfo::create(100);
        row.add_cell(simple_cell("a", b"old", 100));
        row.deletion = DeletionTime::new(200, 200);

        assert!(!row.has_live_data(0));
    }

    #[test]
    fn newer_cell_survives_row_deletion() {
        let mut row = RowData::new(b"ck".to_vec());
        row.add_cell(simple_cell("a", b"old", 100));
        row.add_cell(simple_cell("b", b"new", 300));
        row.deletion = DeletionTime::new(200, 200);

        assert!(row.has_live_data(0));
    }

    #[test]
    fn enclosing_range_deletion_shadows_old_row_content() {
        let mut row = RowData::new(b"ck".to_vec());
        row.liveness_info = LivenessInfo::create(100);
        row.add_cell(simple_cell("a", b"old", 100));

        assert!(row.is_shadowed_by(0, DeletionTime::new(200, 200)));
    }

    #[test]
    fn live_data_after_prunes_shadowed_cells() {
        let mut row = RowData::new(b"ck".to_vec());
        row.liveness_info = LivenessInfo::create(100);
        row.add_cell(simple_cell("old", b"old", 100));
        row.add_cell(simple_cell("new", b"new", 300));

        let live = row.live_data_after(0, DeletionTime::new(200, 200)).unwrap();
        assert!(live.liveness_info.is_empty());
        assert!(!live.columns.contains_key("old"));
        assert!(live.columns.contains_key("new"));
    }

    #[test]
    fn from_simple_row() {
        let row = Row {
            clustering_key: b"ck".to_vec(),
            cells: vec![crate::memtable::partition::Cell {
                column: "x".to_string(),
                value: Some(b"v".to_vec()),
                timestamp: 100,
                ttl: 0,
                local_deletion_time: None,
                is_tombstone: false,
            }],
            is_tombstone: false,
            local_deletion_time: None,
        };
        let rich = RowData::from(&row);
        assert_eq!(rich.clustering_key, b"ck");
        assert_eq!(rich.columns.len(), 1);
        assert!(rich.deletion.is_live());
    }

    #[test]
    fn unfiltered_ordering() {
        let row = Unfiltered::Row(RowData::new(b"ck1".to_vec()));
        let marker = Unfiltered::Marker(RangeTombstoneMarker::Open {
            bound: ClusteringBound {
                kind: ClusteringBoundKind::InclusiveStart,
                values: b"ck0".to_vec(),
            },
            deletion: DeletionTime::new(100, 100),
        });
        assert!(marker.clustering_key() < row.clustering_key());
    }

    #[test]
    fn range_tombstone_marker_properties() {
        let open = RangeTombstoneMarker::Open {
            bound: ClusteringBound {
                kind: ClusteringBoundKind::InclusiveStart,
                values: vec![1],
            },
            deletion: DeletionTime::new(100, 100),
        };
        assert!(open.is_open());
        assert!(!open.is_close());

        let close = RangeTombstoneMarker::Close {
            bound: ClusteringBound {
                kind: ClusteringBoundKind::InclusiveEnd,
                values: vec![5],
            },
            deletion: DeletionTime::new(100, 100),
        };
        assert!(!close.is_open());
        assert!(close.is_close());

        let boundary = RangeTombstoneMarker::Boundary {
            bound: ClusteringBound {
                kind: ClusteringBoundKind::InclusiveEnd,
                values: vec![3],
            },
            close_deletion: DeletionTime::new(100, 100),
            open_deletion: DeletionTime::new(200, 200),
        };
        assert!(boundary.is_open());
        assert!(boundary.is_close());
    }

    #[test]
    fn static_row() {
        let row = RowData::new_static();
        assert!(row.is_static);
        assert!(row.clustering_key.is_empty());
    }
}
