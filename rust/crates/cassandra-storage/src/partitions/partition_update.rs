// Licensed under Apache License, Version 2.0.

//! Mutable partition builder for the write path.
//!
//! ## Java Oracle
//! `org.apache.cassandra.db.partitions.PartitionUpdate`

use std::collections::BTreeMap;

use cassandra_common::tombstone::{DeletionTime, RangeTombstone};

use super::decorated_key::DecoratedKey;
use crate::memtable::partition::PartitionData;
use crate::rows::deletion::MutableDeletionInfo;
use crate::rows::unfiltered::RowData;

/// A mutable partition for building writes.
///
/// Collects rows, tombstones, and deletion info before being applied
/// to a memtable or serialized to an SSTable.
#[derive(Debug, Clone)]
pub struct PartitionUpdate {
    /// Decorated partition key.
    pub key: DecoratedKey,
    /// Deletion info (partition deletion + range tombstones).
    pub deletion_info: MutableDeletionInfo,
    /// Rows keyed by clustering key.
    pub rows: BTreeMap<Vec<u8>, RowData>,
    /// Static row (one per partition, empty clustering key).
    pub static_row: Option<RowData>,
}

impl PartitionUpdate {
    /// Create an empty partition update.
    pub fn new(key: DecoratedKey) -> Self {
        Self {
            key,
            deletion_info: MutableDeletionInfo::live(),
            rows: BTreeMap::new(),
            static_row: None,
        }
    }

    /// Add or merge a row.
    pub fn add_row(&mut self, row: RowData) {
        if row.is_static {
            match &mut self.static_row {
                Some(existing) => existing.merge_with(&row),
                None => self.static_row = Some(row),
            }
        } else {
            let ck = row.clustering_key.clone();
            self.rows
                .entry(ck)
                .and_modify(|existing| existing.merge_with(&row))
                .or_insert(row);
        }
    }

    /// Add a range tombstone.
    pub fn add_range_tombstone(&mut self, rt: RangeTombstone) {
        self.deletion_info.add_range_tombstone(rt);
    }

    /// Set or supersede the partition deletion.
    pub fn set_partition_deletion(&mut self, deletion: DeletionTime) {
        self.deletion_info.set_partition_deletion(deletion);
    }

    /// Merge another partition update into this one.
    pub fn merge(&mut self, other: &PartitionUpdate) {
        self.deletion_info.merge(&other.deletion_info);
        for row in other.rows.values() {
            self.add_row(row.clone());
        }
        if let Some(ref sr) = other.static_row {
            self.add_row(sr.clone());
        }
    }

    /// Number of rows (excluding static row).
    pub fn row_count(&self) -> usize {
        self.rows.len()
    }

    /// Returns `true` if this update has no data.
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty() && self.static_row.is_none() && self.deletion_info.is_live()
    }

    /// Returns rows with live content after applying partition and range tombstones.
    pub fn live_rows(&self, now_in_seconds: i32) -> Vec<&RowData> {
        self.rows
            .values()
            .filter(|row| {
                let deletion = self.deletion_info.active_deletion_for(&row.clustering_key);
                row.has_live_data_after(now_in_seconds, deletion)
            })
            .collect()
    }

    /// Returns the static row when it has live content after partition deletion.
    pub fn live_static_row(&self, now_in_seconds: i32) -> Option<&RowData> {
        self.static_row.as_ref().filter(|row| {
            row.has_live_data_after(now_in_seconds, self.deletion_info.partition_deletion)
        })
    }
}

/// Convert from simplified `PartitionData`.
impl PartitionUpdate {
    pub fn from_partition_data(partition_key: Vec<u8>, pd: &PartitionData) -> Self {
        let dk = DecoratedKey::new(partition_key);
        let mut pu = PartitionUpdate::new(dk);

        // Convert partition-level tombstone
        if let (Some(ts), Some(ldt)) = (pd.tombstone_timestamp, pd.tombstone_local_deletion_time) {
            pu.set_partition_deletion(DeletionTime::new(ts, ldt));
        }

        // Convert rows
        for row in pd.rows.values() {
            pu.add_row(RowData::from(row));
        }

        pu
    }
}

/// Convert back to simplified `PartitionData`.
impl From<&PartitionUpdate> for PartitionData {
    fn from(pu: &PartitionUpdate) -> Self {
        let mut pd = PartitionData::new();

        if !pu.deletion_info.partition_deletion.is_live() {
            pd.set_tombstone(
                pu.deletion_info.partition_deletion.marked_for_delete_at,
                pu.deletion_info.partition_deletion.local_deletion_time,
            );
        }

        for (ck, row_data) in &pu.rows {
            use crate::memtable::partition::{Cell, Row};
            use crate::rows::unfiltered::ColumnData;

            let mut cells = Vec::new();
            for col_data in row_data.columns.values() {
                match col_data {
                    ColumnData::Simple(cell) => cells.push(Cell::from(cell)),
                    ColumnData::Complex(ccd) => {
                        for cell in ccd.cells.values() {
                            cells.push(Cell::from(cell));
                        }
                    }
                }
            }

            let row = Row {
                clustering_key: ck.clone(),
                cells,
                is_tombstone: !row_data.deletion.is_live(),
                local_deletion_time: if row_data.deletion.is_live() {
                    None
                } else {
                    Some(row_data.deletion.local_deletion_time)
                },
            };
            pd.apply_row(row);
        }

        pd
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rows::cell::CellData;
    use crate::rows::liveness::LivenessInfo;
    use cassandra_common::token::Token;
    use cassandra_common::ttl::NO_TTL;

    fn make_row(ck: &[u8], col: &str, val: &[u8], ts: i64) -> RowData {
        let mut row = RowData::new(ck.to_vec());
        row.liveness_info = LivenessInfo::create(ts);
        row.add_cell(CellData {
            column: col.to_string(),
            value: Some(val.to_vec()),
            timestamp: ts,
            ttl: NO_TTL,
            local_deletion_time: i32::MAX,
            path: None,
        });
        row
    }

    #[test]
    fn add_rows() {
        let dk = DecoratedKey::with_token(b"pk".to_vec(), Token::from_raw(1));
        let mut pu = PartitionUpdate::new(dk);
        pu.add_row(make_row(b"ck1", "a", b"1", 100));
        pu.add_row(make_row(b"ck2", "a", b"2", 100));
        assert_eq!(pu.row_count(), 2);
    }

    #[test]
    fn merge_rows_same_ck() {
        let dk = DecoratedKey::with_token(b"pk".to_vec(), Token::from_raw(1));
        let mut pu = PartitionUpdate::new(dk);
        pu.add_row(make_row(b"ck1", "a", b"old", 100));
        pu.add_row(make_row(b"ck1", "a", b"new", 200));
        assert_eq!(pu.row_count(), 1);
    }

    #[test]
    fn static_row() {
        let dk = DecoratedKey::with_token(b"pk".to_vec(), Token::from_raw(1));
        let mut pu = PartitionUpdate::new(dk);
        let mut sr = RowData::new_static();
        sr.add_cell(CellData {
            column: "s".to_string(),
            value: Some(b"val".to_vec()),
            timestamp: 100,
            ttl: NO_TTL,
            local_deletion_time: i32::MAX,
            path: None,
        });
        pu.add_row(sr);
        assert!(pu.static_row.is_some());
        assert_eq!(pu.row_count(), 0);
    }

    #[test]
    fn partition_deletion() {
        let dk = DecoratedKey::with_token(b"pk".to_vec(), Token::from_raw(1));
        let mut pu = PartitionUpdate::new(dk);
        pu.set_partition_deletion(DeletionTime::new(100, 100));
        assert!(!pu.deletion_info.is_live());
    }

    #[test]
    fn merge_updates() {
        let dk = DecoratedKey::with_token(b"pk".to_vec(), Token::from_raw(1));
        let mut a = PartitionUpdate::new(dk.clone());
        a.add_row(make_row(b"ck1", "a", b"1", 100));

        let mut b = PartitionUpdate::new(dk);
        b.add_row(make_row(b"ck2", "a", b"2", 100));
        b.set_partition_deletion(DeletionTime::new(50, 50));

        a.merge(&b);
        assert_eq!(a.row_count(), 2);
        assert!(!a.deletion_info.partition_deletion.is_live());
    }

    #[test]
    fn roundtrip_partition_data() {
        let dk = DecoratedKey::with_token(b"pk".to_vec(), Token::from_raw(1));
        let mut pu = PartitionUpdate::new(dk);
        pu.add_row(make_row(b"ck1", "name", b"alice", 100));
        pu.set_partition_deletion(DeletionTime::new(50, 50));

        let pd = PartitionData::from(&pu);
        assert_eq!(pd.rows.len(), 1);
        assert_eq!(pd.tombstone_timestamp, Some(50));

        let pu2 = PartitionUpdate::from_partition_data(b"pk".to_vec(), &pd);
        assert_eq!(pu2.row_count(), 1);
    }

    #[test]
    fn live_rows_applies_range_tombstones_by_timestamp() {
        let dk = DecoratedKey::with_token(b"pk".to_vec(), Token::from_raw(1));
        let mut pu = PartitionUpdate::new(dk);
        pu.add_row(make_row(b"ck1", "name", b"old", 100));
        pu.add_row(make_row(b"ck2", "name", b"new", 300));
        pu.add_range_tombstone(RangeTombstone::new(
            b"ck1".to_vec(),
            b"ck2".to_vec(),
            DeletionTime::new(200, 200),
        ));

        let live = pu.live_rows(0);
        assert_eq!(live.len(), 1);
        assert_eq!(live[0].clustering_key, b"ck2");
    }

    #[test]
    fn live_static_row_applies_partition_deletion() {
        let dk = DecoratedKey::with_token(b"pk".to_vec(), Token::from_raw(1));
        let mut pu = PartitionUpdate::new(dk);
        let mut static_row = RowData::new_static();
        static_row.liveness_info = LivenessInfo::create(100);
        static_row.add_cell(CellData {
            column: "s".to_string(),
            value: Some(b"old".to_vec()),
            timestamp: 100,
            ttl: NO_TTL,
            local_deletion_time: i32::MAX,
            path: None,
        });
        pu.add_row(static_row);
        pu.set_partition_deletion(DeletionTime::new(200, 200));

        assert!(pu.live_static_row(0).is_none());
    }
}
