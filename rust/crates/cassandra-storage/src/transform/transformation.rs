// Licensed under Apache License, Version 2.0.

//! Transformation trait and built-in transforms.
//!
//! ## Java Oracle
//! `org.apache.cassandra.db.transform.Transformation`

use cassandra_common::tombstone::DeletionTime;

use crate::filter::data_limits::DataLimits;
use crate::rows::unfiltered::{RangeTombstoneMarker, RowData};

/// A transformation applied to rows and/or markers during iteration.
pub trait Transformation: Send {
    /// Transform a row. Return `None` to skip it.
    fn apply_to_row(&mut self, row: RowData) -> Option<RowData> {
        Some(row)
    }

    /// Transform a range tombstone marker. Return `None` to skip it.
    fn apply_to_marker(&mut self, marker: RangeTombstoneMarker) -> Option<RangeTombstoneMarker> {
        Some(marker)
    }

    /// Called when a new partition starts. Return `false` to skip the partition.
    fn apply_to_partition(&mut self, _partition_key: &[u8], _deletion: DeletionTime) -> bool {
        true
    }
}

/// Purge transform: removes purgeable tombstones.
///
/// A tombstone is purgeable if its local deletion time is before `gc_before`.
pub struct PurgeTransform {
    pub gc_before: i32,
    pub now_in_seconds: i32,
}

impl PurgeTransform {
    pub fn new(gc_before: i32, now_in_seconds: i32) -> Self {
        Self {
            gc_before,
            now_in_seconds,
        }
    }
}

impl Transformation for PurgeTransform {
    fn apply_to_row(&mut self, row: RowData) -> Option<RowData> {
        // If row deletion is purgeable, clear it
        let mut row = row;
        if row.deletion.is_purgeable(self.gc_before) {
            row.deletion = DeletionTime::LIVE;
        }
        // If row has no live data and no non-purgeable deletion, skip it
        if !row.has_live_data(self.now_in_seconds) && row.deletion.is_live() {
            return None;
        }
        Some(row)
    }

    fn apply_to_marker(&mut self, marker: RangeTombstoneMarker) -> Option<RangeTombstoneMarker> {
        // Check if the marker's deletion is purgeable
        let is_purgeable = match &marker {
            RangeTombstoneMarker::Open { deletion, .. } => deletion.is_purgeable(self.gc_before),
            RangeTombstoneMarker::Close { deletion, .. } => deletion.is_purgeable(self.gc_before),
            RangeTombstoneMarker::Boundary {
                close_deletion,
                open_deletion,
                ..
            } => {
                close_deletion.is_purgeable(self.gc_before)
                    && open_deletion.is_purgeable(self.gc_before)
            }
        };
        if is_purgeable { None } else { Some(marker) }
    }
}

/// Limits transform: enforces `DataLimits` by counting rows.
pub struct LimitsTransform {
    pub limits: DataLimits,
    pub rows_counted: u32,
    pub per_partition_counted: u32,
    pub now_in_seconds: i32,
}

impl LimitsTransform {
    pub fn new(limits: DataLimits, now_in_seconds: i32) -> Self {
        Self {
            limits,
            rows_counted: 0,
            per_partition_counted: 0,
            now_in_seconds,
        }
    }

    /// Returns `true` if limits have been exhausted.
    pub fn is_exhausted(&self) -> bool {
        self.limits.is_exhausted(self.rows_counted)
    }
}

impl Transformation for LimitsTransform {
    fn apply_to_row(&mut self, row: RowData) -> Option<RowData> {
        if self.is_exhausted() {
            return None;
        }
        if self.per_partition_counted >= self.limits.per_partition_limit() {
            return None;
        }
        // Only count live rows
        if row.has_live_data(self.now_in_seconds) {
            self.rows_counted += 1;
            self.per_partition_counted += 1;
        }
        Some(row)
    }

    fn apply_to_partition(&mut self, _partition_key: &[u8], _deletion: DeletionTime) -> bool {
        self.per_partition_counted = 0;
        !self.is_exhausted()
    }
}

/// Applies active range tombstone state to rows in an unfiltered stream.
///
/// Open/close/boundary markers are passed through unchanged, while rows are
/// pruned or skipped according to the newest active partition/range deletion.
pub struct RangeTombstoneLivenessTransform {
    now_in_seconds: i32,
    partition_deletion: DeletionTime,
    open_deletions: Vec<DeletionTime>,
}

impl RangeTombstoneLivenessTransform {
    pub fn new(now_in_seconds: i32) -> Self {
        Self {
            now_in_seconds,
            partition_deletion: DeletionTime::LIVE,
            open_deletions: Vec::new(),
        }
    }

    fn active_deletion(&self) -> DeletionTime {
        self.open_deletions
            .iter()
            .copied()
            .fold(self.partition_deletion, |active, deletion| {
                if deletion.supersedes(&active) {
                    deletion
                } else {
                    active
                }
            })
    }

    fn close_deletion(&mut self, deletion: DeletionTime) {
        if let Some(pos) = self
            .open_deletions
            .iter()
            .rposition(|open| *open == deletion)
        {
            self.open_deletions.remove(pos);
        }
    }
}

impl Transformation for RangeTombstoneLivenessTransform {
    fn apply_to_row(&mut self, row: RowData) -> Option<RowData> {
        row.live_data_after(self.now_in_seconds, self.active_deletion())
    }

    fn apply_to_marker(&mut self, marker: RangeTombstoneMarker) -> Option<RangeTombstoneMarker> {
        match &marker {
            RangeTombstoneMarker::Open { deletion, .. } => {
                self.open_deletions.push(*deletion);
            }
            RangeTombstoneMarker::Close { deletion, .. } => {
                self.close_deletion(*deletion);
            }
            RangeTombstoneMarker::Boundary {
                close_deletion,
                open_deletion,
                ..
            } => {
                self.close_deletion(*close_deletion);
                self.open_deletions.push(*open_deletion);
            }
        }
        Some(marker)
    }

    fn apply_to_partition(&mut self, _partition_key: &[u8], deletion: DeletionTime) -> bool {
        self.partition_deletion = deletion;
        self.open_deletions.clear();
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rows::cell::CellData;
    use crate::rows::liveness::LivenessInfo;
    use crate::rows::unfiltered::{ClusteringBound, ClusteringBoundKind};
    use cassandra_common::ttl::NO_TTL;

    fn live_row(ck: &[u8], ts: i64) -> RowData {
        let mut row = RowData::new(ck.to_vec());
        row.liveness_info = LivenessInfo::create(ts);
        row.add_cell(CellData {
            column: "x".to_string(),
            value: Some(b"v".to_vec()),
            timestamp: ts,
            ttl: NO_TTL,
            local_deletion_time: i32::MAX,
            path: None,
        });
        row
    }

    fn tombstone_row(ck: &[u8], ts: i64, ldt: i32) -> RowData {
        let mut row = RowData::new(ck.to_vec());
        row.deletion = DeletionTime::new(ts, ldt);
        row
    }

    #[test]
    fn purge_purgeable_tombstone() {
        let mut purge = PurgeTransform::new(200, 300);
        let row = tombstone_row(b"ck", 100, 100);
        // ldt=100 < gc_before=200, so purgeable
        let result = purge.apply_to_row(row);
        assert!(result.is_none()); // purged
    }

    #[test]
    fn purge_keeps_non_purgeable() {
        let mut purge = PurgeTransform::new(50, 300);
        let row = tombstone_row(b"ck", 100, 100);
        // ldt=100 >= gc_before=50, not purgeable
        let result = purge.apply_to_row(row);
        assert!(result.is_some());
    }

    #[test]
    fn purge_keeps_live_rows() {
        let mut purge = PurgeTransform::new(200, 300);
        let row = live_row(b"ck", 100);
        let result = purge.apply_to_row(row);
        assert!(result.is_some());
    }

    #[test]
    fn purge_marker() {
        let mut purge = PurgeTransform::new(200, 300);
        let marker = RangeTombstoneMarker::Open {
            bound: ClusteringBound {
                kind: ClusteringBoundKind::InclusiveStart,
                values: vec![1],
            },
            deletion: DeletionTime::new(50, 50),
        };
        assert!(purge.apply_to_marker(marker).is_none()); // purgeable
    }

    #[test]
    fn limits_counting() {
        let limits = DataLimits::CqlLimit {
            rows_limit: 2,
            per_partition_limit: u32::MAX,
        };
        let mut lt = LimitsTransform::new(limits, 0);

        assert!(lt.apply_to_row(live_row(b"ck1", 100)).is_some());
        assert!(lt.apply_to_row(live_row(b"ck2", 100)).is_some());
        assert!(lt.apply_to_row(live_row(b"ck3", 100)).is_none()); // exhausted
        assert!(lt.is_exhausted());
    }

    #[test]
    fn limits_per_partition() {
        let limits = DataLimits::CqlLimit {
            rows_limit: u32::MAX,
            per_partition_limit: 1,
        };
        let mut lt = LimitsTransform::new(limits, 0);

        lt.apply_to_partition(b"pk1", DeletionTime::LIVE);
        assert!(lt.apply_to_row(live_row(b"ck1", 100)).is_some());
        assert!(lt.apply_to_row(live_row(b"ck2", 100)).is_none());

        // New partition resets per-partition count
        lt.apply_to_partition(b"pk2", DeletionTime::LIVE);
        assert!(lt.apply_to_row(live_row(b"ck1", 100)).is_some());
    }

    #[test]
    fn range_tombstone_liveness_filters_shadowed_rows() {
        let mut transform = RangeTombstoneLivenessTransform::new(0);

        transform.apply_to_marker(RangeTombstoneMarker::Open {
            bound: ClusteringBound {
                kind: ClusteringBoundKind::InclusiveStart,
                values: b"ck1".to_vec(),
            },
            deletion: DeletionTime::new(200, 200),
        });

        assert!(transform.apply_to_row(live_row(b"ck1", 100)).is_none());
        assert!(transform.apply_to_row(live_row(b"ck2", 300)).is_some());

        transform.apply_to_marker(RangeTombstoneMarker::Close {
            bound: ClusteringBound {
                kind: ClusteringBoundKind::InclusiveEnd,
                values: b"ck3".to_vec(),
            },
            deletion: DeletionTime::new(200, 200),
        });
        assert!(transform.apply_to_row(live_row(b"ck4", 100)).is_some());
    }

    #[test]
    fn range_tombstone_liveness_handles_boundary_markers() {
        let mut transform = RangeTombstoneLivenessTransform::new(0);

        transform.apply_to_marker(RangeTombstoneMarker::Open {
            bound: ClusteringBound {
                kind: ClusteringBoundKind::InclusiveStart,
                values: b"ck1".to_vec(),
            },
            deletion: DeletionTime::new(100, 100),
        });
        assert!(transform.apply_to_row(live_row(b"ck1", 150)).is_some());

        transform.apply_to_marker(RangeTombstoneMarker::Boundary {
            bound: ClusteringBound {
                kind: ClusteringBoundKind::InclusiveEnd,
                values: b"ck2".to_vec(),
            },
            close_deletion: DeletionTime::new(100, 100),
            open_deletion: DeletionTime::new(300, 300),
        });

        assert!(transform.apply_to_row(live_row(b"ck2", 200)).is_none());
        assert!(transform.apply_to_row(live_row(b"ck3", 400)).is_some());
    }

    #[test]
    fn range_tombstone_liveness_prunes_shadowed_cells() {
        let mut transform = RangeTombstoneLivenessTransform::new(0);
        transform.apply_to_partition(b"pk", DeletionTime::new(200, 200));

        let mut row = RowData::new(b"ck".to_vec());
        row.add_cell(CellData {
            column: "old".to_string(),
            value: Some(b"old".to_vec()),
            timestamp: 100,
            ttl: NO_TTL,
            local_deletion_time: i32::MAX,
            path: None,
        });
        row.add_cell(CellData {
            column: "new".to_string(),
            value: Some(b"new".to_vec()),
            timestamp: 300,
            ttl: NO_TTL,
            local_deletion_time: i32::MAX,
            path: None,
        });

        let row = transform.apply_to_row(row).unwrap();
        assert!(!row.columns.contains_key("old"));
        assert!(row.columns.contains_key("new"));
    }
}
