// Licensed under Apache License, Version 2.0.

//! Partition and row iterator abstractions.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.db.rows.UnfilteredRowIterator`
//! - `org.apache.cassandra.db.partitions.UnfilteredPartitionIterator`

use cassandra_common::tombstone::DeletionTime;

use crate::memtable::partition::PartitionData;
use crate::rows::unfiltered::{RowData, Unfiltered};

/// Iterator over `Unfiltered` items (rows and range tombstone markers)
/// within a single partition, in clustering order.
pub trait UnfilteredRowIterator {
    /// The raw partition key bytes.
    fn partition_key(&self) -> &[u8];

    /// The partition-level deletion.
    fn partition_deletion(&self) -> DeletionTime;

    /// The static row, if any.
    fn static_row(&self) -> Option<&RowData>;

    /// Whether iteration is in reverse clustering order.
    fn is_reversed(&self) -> bool;

    /// Get the next unfiltered item.
    fn next(&mut self) -> Option<Unfiltered>;
}

/// Iterator over partition iterators (for range reads).
pub trait UnfilteredPartitionIterator {
    /// Get the next partition's row iterator.
    fn next_partition(&mut self) -> Option<Box<dyn UnfilteredRowIterator>>;
}

/// In-memory row iterator that adapts `PartitionData` to yield `Unfiltered` items.
pub struct InMemoryRowIterator {
    partition_key: Vec<u8>,
    partition_deletion: DeletionTime,
    items: Vec<Unfiltered>,
    pos: usize,
    reversed: bool,
}

impl InMemoryRowIterator {
    /// Create from a `PartitionData`.
    pub fn from_partition_data(partition_key: Vec<u8>, pd: &PartitionData) -> Self {
        let partition_deletion = match (pd.tombstone_timestamp, pd.tombstone_local_deletion_time) {
            (Some(ts), Some(ldt)) => DeletionTime::new(ts, ldt),
            _ => DeletionTime::LIVE,
        };

        let items: Vec<Unfiltered> = pd
            .rows
            .values()
            .map(|row| Unfiltered::Row(RowData::from(row)))
            .collect();

        Self {
            partition_key,
            partition_deletion,
            items,
            pos: 0,
            reversed: false,
        }
    }

    /// Create from a list of `Unfiltered` items.
    pub fn new(
        partition_key: Vec<u8>,
        partition_deletion: DeletionTime,
        items: Vec<Unfiltered>,
        reversed: bool,
    ) -> Self {
        Self {
            partition_key,
            partition_deletion,
            items,
            pos: 0,
            reversed,
        }
    }

    /// Set reverse iteration.
    pub fn set_reversed(&mut self, reversed: bool) {
        self.reversed = reversed;
        if reversed {
            self.items.reverse();
        }
    }
}

impl UnfilteredRowIterator for InMemoryRowIterator {
    fn partition_key(&self) -> &[u8] {
        &self.partition_key
    }

    fn partition_deletion(&self) -> DeletionTime {
        self.partition_deletion
    }

    fn static_row(&self) -> Option<&RowData> {
        None // PartitionData doesn't have static rows
    }

    fn is_reversed(&self) -> bool {
        self.reversed
    }

    fn next(&mut self) -> Option<Unfiltered> {
        if self.pos < self.items.len() {
            let item = self.items[self.pos].clone();
            self.pos += 1;
            Some(item)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memtable::partition::{Cell, Row};

    fn test_partition() -> PartitionData {
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
        pd.apply_row(Row {
            clustering_key: b"ck2".to_vec(),
            cells: vec![Cell {
                column: "x".to_string(),
                value: Some(b"2".to_vec()),
                timestamp: 100,
                ttl: 0,
                local_deletion_time: None,
                is_tombstone: false,
            }],
            is_tombstone: false,
            local_deletion_time: None,
        });
        pd
    }

    #[test]
    fn iterate_partition_data() {
        let pd = test_partition();
        let mut iter = InMemoryRowIterator::from_partition_data(b"pk1".to_vec(), &pd);

        assert_eq!(iter.partition_key(), b"pk1");
        assert!(iter.partition_deletion().is_live());
        assert!(!iter.is_reversed());

        let item1 = iter.next().unwrap();
        assert!(item1.is_row());
        assert_eq!(item1.clustering_key(), b"ck1");

        let item2 = iter.next().unwrap();
        assert_eq!(item2.clustering_key(), b"ck2");

        assert!(iter.next().is_none());
    }

    #[test]
    fn iterate_with_partition_deletion() {
        let mut pd = test_partition();
        pd.set_tombstone(500, 500);

        let iter = InMemoryRowIterator::from_partition_data(b"pk1".to_vec(), &pd);
        assert!(!iter.partition_deletion().is_live());
        assert_eq!(iter.partition_deletion().marked_for_delete_at, 500);
    }

    #[test]
    fn reversed_iteration() {
        let pd = test_partition();
        let mut iter = InMemoryRowIterator::from_partition_data(b"pk1".to_vec(), &pd);
        iter.set_reversed(true);

        assert!(iter.is_reversed());
        let item1 = iter.next().unwrap();
        assert_eq!(item1.clustering_key(), b"ck2"); // reversed order
    }
}
