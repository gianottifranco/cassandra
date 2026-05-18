// Licensed under Apache License, Version 2.0.

//! Immutable partition snapshot constructed by materializing an iterator.
//!
//! ## Java Oracle
//! `org.apache.cassandra.db.partitions.FilteredPartition`
//! `org.apache.cassandra.db.partitions.ImmutableBTreePartition`

use cassandra_common::tombstone::DeletionTime;

use super::iterators::UnfilteredRowIterator;
use crate::rows::unfiltered::{RowData, Unfiltered};

/// An immutable, fully-materialized partition.
///
/// Created by consuming an `UnfilteredRowIterator` and storing all items.
#[derive(Debug, Clone)]
pub struct FilteredPartition {
    /// Partition key bytes.
    pub partition_key: Vec<u8>,
    /// Partition-level deletion.
    pub partition_deletion: DeletionTime,
    /// Static row, if any.
    pub static_row: Option<RowData>,
    /// All unfiltered items (rows + markers) in clustering order.
    pub items: Vec<Unfiltered>,
}

impl FilteredPartition {
    /// Materialize from an `UnfilteredRowIterator`.
    pub fn create(iter: &mut dyn UnfilteredRowIterator) -> Self {
        let partition_key = iter.partition_key().to_vec();
        let partition_deletion = iter.partition_deletion();
        let static_row = iter.static_row().cloned();

        let mut items = Vec::new();
        while let Some(item) = iter.next() {
            items.push(item);
        }

        Self {
            partition_key,
            partition_deletion,
            static_row,
            items,
        }
    }

    /// Number of row items (excluding markers).
    pub fn row_count(&self) -> usize {
        self.items.iter().filter(|i| i.is_row()).count()
    }

    /// Returns `true` if this partition has no rows or static data.
    pub fn is_empty(&self) -> bool {
        self.items.is_empty() && self.static_row.is_none()
    }

    /// Get all rows (excluding range tombstone markers).
    pub fn rows(&self) -> Vec<&RowData> {
        self.items
            .iter()
            .filter_map(|item| match item {
                Unfiltered::Row(row) => Some(row),
                _ => None,
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memtable::partition::PartitionData;
    use crate::memtable::partition::{Cell, Row};
    use crate::partitions::iterators::InMemoryRowIterator;

    #[test]
    fn materialize_partition() {
        let mut pd = PartitionData::new();
        pd.apply_row(Row {
            clustering_key: b"ck1".to_vec(),
            cells: vec![Cell {
                column: "x".to_string(),
                value: Some(b"1".to_vec()),
                timestamp: 100,
                ttl: 0,
                local_deletion_time: None,
                is_tombstone: false,
            }],
            is_tombstone: false,
            local_deletion_time: None,
        });

        let mut iter = InMemoryRowIterator::from_partition_data(b"pk".to_vec(), &pd);
        let fp = FilteredPartition::create(&mut iter);

        assert_eq!(fp.partition_key, b"pk");
        assert_eq!(fp.row_count(), 1);
        assert!(!fp.is_empty());
    }

    #[test]
    fn empty_partition() {
        let pd = PartitionData::new();
        let mut iter = InMemoryRowIterator::from_partition_data(b"pk".to_vec(), &pd);
        let fp = FilteredPartition::create(&mut iter);
        assert!(fp.is_empty());
        assert_eq!(fp.row_count(), 0);
    }
}
