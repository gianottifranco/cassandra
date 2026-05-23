// Licensed under Apache License, Version 2.0.

//! Partition and row iterator abstractions.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.db.rows.UnfilteredRowIterator`
//! - `org.apache.cassandra.db.partitions.UnfilteredPartitionIterator`

use std::cmp::Ordering;
use std::collections::BinaryHeap;

use cassandra_common::tombstone::DeletionTime;

use crate::memtable::partition::PartitionData;
use crate::rows::unfiltered::{ClusteringBoundKind, RangeTombstoneMarker, RowData, Unfiltered};

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

/// Error returned when row iterators cannot be merged.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IteratorMergeError {
    /// All row iterators in a merged partition must refer to the same partition key.
    PartitionKeyMismatch { expected: Vec<u8>, actual: Vec<u8> },
    /// Reverse and forward row iterators cannot be merged together.
    ReversedMismatch { expected: bool, actual: bool },
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

/// In-memory partition iterator over pre-built row iterators.
pub struct InMemoryPartitionIterator {
    partitions: std::collections::VecDeque<Box<dyn UnfilteredRowIterator>>,
}

impl InMemoryPartitionIterator {
    /// Create a partition iterator from row iterators sorted by partition key.
    pub fn new(partitions: Vec<Box<dyn UnfilteredRowIterator>>) -> Self {
        Self {
            partitions: partitions.into(),
        }
    }

    /// Create from simplified partition data sorted by key.
    pub fn from_partition_data(partitions: Vec<(Vec<u8>, PartitionData)>) -> Self {
        let partitions = partitions
            .into_iter()
            .map(|(key, data)| {
                Box::new(InMemoryRowIterator::from_partition_data(key, &data))
                    as Box<dyn UnfilteredRowIterator>
            })
            .collect();
        Self::new(partitions)
    }
}

impl UnfilteredPartitionIterator for InMemoryPartitionIterator {
    fn next_partition(&mut self) -> Option<Box<dyn UnfilteredRowIterator>> {
        self.partitions.pop_front()
    }
}

/// Row iterator that merges multiple sorted unfiltered row streams for one partition.
///
/// The iterator materializes the sources into a single clustering-ordered stream, merging
/// rows with the same clustering key via the row last-write-wins rules and preserving range
/// tombstone markers in stream order.
pub struct MergedRowIterator {
    partition_key: Vec<u8>,
    partition_deletion: DeletionTime,
    static_row: Option<RowData>,
    items: Vec<Unfiltered>,
    pos: usize,
    reversed: bool,
}

impl MergedRowIterator {
    /// Merge row iterators for the same partition.
    pub fn new(
        mut sources: Vec<Box<dyn UnfilteredRowIterator>>,
    ) -> Result<Self, IteratorMergeError> {
        let partition_key = sources
            .first()
            .map(|source| source.partition_key().to_vec())
            .unwrap_or_default();
        let reversed = sources
            .first()
            .map(|source| source.is_reversed())
            .unwrap_or(false);

        let mut partition_deletion = DeletionTime::LIVE;
        let mut static_row: Option<RowData> = None;
        let mut rows = std::collections::BTreeMap::<Vec<u8>, RowData>::new();
        let mut markers = Vec::<RangeTombstoneMarker>::new();

        for source in &mut sources {
            if source.partition_key() != partition_key.as_slice() {
                return Err(IteratorMergeError::PartitionKeyMismatch {
                    expected: partition_key,
                    actual: source.partition_key().to_vec(),
                });
            }
            if source.is_reversed() != reversed {
                return Err(IteratorMergeError::ReversedMismatch {
                    expected: reversed,
                    actual: source.is_reversed(),
                });
            }

            let source_deletion = source.partition_deletion();
            if source_deletion.supersedes(&partition_deletion) {
                partition_deletion = source_deletion;
            }

            if let Some(source_static) = source.static_row() {
                match &mut static_row {
                    Some(existing) => existing.merge_with(source_static),
                    None => static_row = Some(source_static.clone()),
                }
            }

            while let Some(item) = source.next() {
                match item {
                    Unfiltered::Row(row) => {
                        rows.entry(row.clustering_key.clone())
                            .and_modify(|existing| existing.merge_with(&row))
                            .or_insert(row);
                    }
                    Unfiltered::Marker(marker) => markers.push(marker),
                }
            }
        }

        let mut items: Vec<Unfiltered> = rows.into_values().map(Unfiltered::Row).collect();
        items.extend(markers.into_iter().map(Unfiltered::Marker));
        items.sort_by(compare_unfiltered);
        if reversed {
            items.reverse();
        }

        Ok(Self {
            partition_key,
            partition_deletion,
            static_row,
            items,
            pos: 0,
            reversed,
        })
    }
}

impl UnfilteredRowIterator for MergedRowIterator {
    fn partition_key(&self) -> &[u8] {
        &self.partition_key
    }

    fn partition_deletion(&self) -> DeletionTime {
        self.partition_deletion
    }

    fn static_row(&self) -> Option<&RowData> {
        self.static_row.as_ref()
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

struct PartitionHeapEntry {
    key: Vec<u8>,
    source_idx: usize,
    partition: Box<dyn UnfilteredRowIterator>,
}

impl PartialEq for PartitionHeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.source_idx == other.source_idx
    }
}

impl Eq for PartitionHeapEntry {}

impl PartialOrd for PartitionHeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PartitionHeapEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .key
            .cmp(&self.key)
            .then_with(|| other.source_idx.cmp(&self.source_idx))
    }
}

/// Merge multiple partition iterators by partition key.
pub struct MergedPartitionIterator {
    sources: Vec<Box<dyn UnfilteredPartitionIterator>>,
    heap: BinaryHeap<PartitionHeapEntry>,
}

impl MergedPartitionIterator {
    /// Create a merged partition iterator. Each source must be sorted by partition key.
    pub fn new(mut sources: Vec<Box<dyn UnfilteredPartitionIterator>>) -> Self {
        let mut heap = BinaryHeap::new();
        for (source_idx, source) in sources.iter_mut().enumerate() {
            if let Some(partition) = source.next_partition() {
                heap.push(PartitionHeapEntry {
                    key: partition.partition_key().to_vec(),
                    source_idx,
                    partition,
                });
            }
        }
        Self { sources, heap }
    }
}

impl UnfilteredPartitionIterator for MergedPartitionIterator {
    fn next_partition(&mut self) -> Option<Box<dyn UnfilteredRowIterator>> {
        let first = self.heap.pop()?;
        let current_key = first.key.clone();
        let mut partitions = vec![first.partition];

        if let Some(next) = self.sources[first.source_idx].next_partition() {
            self.heap.push(PartitionHeapEntry {
                key: next.partition_key().to_vec(),
                source_idx: first.source_idx,
                partition: next,
            });
        }

        while self
            .heap
            .peek()
            .is_some_and(|entry| entry.key == current_key)
        {
            let same = self.heap.pop().expect("heap peeked Some");
            partitions.push(same.partition);

            if let Some(next) = self.sources[same.source_idx].next_partition() {
                self.heap.push(PartitionHeapEntry {
                    key: next.partition_key().to_vec(),
                    source_idx: same.source_idx,
                    partition: next,
                });
            }
        }

        if partitions.len() == 1 {
            partitions.pop()
        } else {
            Some(Box::new(MergedRowIterator::new(partitions).expect(
                "partition heap groups only matching partition keys",
            )))
        }
    }
}

fn compare_unfiltered(left: &Unfiltered, right: &Unfiltered) -> Ordering {
    left.clustering_key()
        .cmp(right.clustering_key())
        .then_with(|| unfiltered_rank(left).cmp(&unfiltered_rank(right)))
}

fn unfiltered_rank(item: &Unfiltered) -> u8 {
    match item {
        Unfiltered::Row(_) => 1,
        Unfiltered::Marker(marker) => match marker.clustering_bound().kind {
            ClusteringBoundKind::InclusiveStart | ClusteringBoundKind::ExclusiveEnd => 0,
            ClusteringBoundKind::ExclusiveStart | ClusteringBoundKind::InclusiveEnd => 2,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memtable::partition::{Cell, Row};
    use crate::rows::cell::CellData;
    use crate::rows::liveness::LivenessInfo;
    use cassandra_common::ttl::NO_TTL;

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

    fn simple_row(ck: &[u8], col: &str, val: &[u8], ts: i64) -> Row {
        Row {
            clustering_key: ck.to_vec(),
            cells: vec![Cell {
                column: col.to_string(),
                value: Some(val.to_vec()),
                timestamp: ts,
                ttl: 0,
                local_deletion_time: None,
                is_tombstone: false,
            }],
            is_tombstone: false,
            local_deletion_time: None,
        }
    }

    fn rich_cell(col: &str, val: &[u8], ts: i64) -> CellData {
        CellData {
            column: col.to_string(),
            value: Some(val.to_vec()),
            timestamp: ts,
            ttl: NO_TTL,
            local_deletion_time: i32::MAX,
            path: None,
        }
    }

    fn rich_row(ck: &[u8], col: &str, val: &[u8], ts: i64) -> RowData {
        let mut row = RowData::new(ck.to_vec());
        row.liveness_info = LivenessInfo::create(ts);
        row.add_cell(rich_cell(col, val, ts));
        row
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

    #[test]
    fn in_memory_partition_iterator_yields_partitions_in_order() {
        let mut first = test_partition();
        first.apply_row(Row {
            clustering_key: b"ck3".to_vec(),
            cells: vec![Cell {
                column: "x".to_string(),
                value: Some(b"3".to_vec()),
                timestamp: 100,
                ttl: 0,
                local_deletion_time: None,
                is_tombstone: false,
            }],
            is_tombstone: false,
            local_deletion_time: None,
        });
        let second = test_partition();

        let mut iter = InMemoryPartitionIterator::from_partition_data(vec![
            (b"pk1".to_vec(), first),
            (b"pk2".to_vec(), second),
        ]);

        assert_eq!(iter.next_partition().unwrap().partition_key(), b"pk1");
        assert_eq!(iter.next_partition().unwrap().partition_key(), b"pk2");
        assert!(iter.next_partition().is_none());
    }

    #[test]
    fn merged_row_iterator_merges_duplicate_rows_and_partition_metadata() {
        let left = InMemoryRowIterator::new(
            b"pk".to_vec(),
            DeletionTime::new(100, 100),
            vec![
                Unfiltered::Row(rich_row(b"ck1", "v", b"old", 100)),
                Unfiltered::Row(rich_row(b"ck2", "v", b"left", 100)),
            ],
            false,
        );
        let right = InMemoryRowIterator::new(
            b"pk".to_vec(),
            DeletionTime::new(200, 200),
            vec![
                Unfiltered::Row(rich_row(b"ck1", "v", b"new", 300)),
                Unfiltered::Row(rich_row(b"ck3", "v", b"right", 100)),
            ],
            false,
        );

        let mut merged = MergedRowIterator::new(vec![Box::new(left), Box::new(right)]).unwrap();

        assert_eq!(merged.partition_key(), b"pk");
        assert_eq!(merged.partition_deletion().marked_for_delete_at, 200);

        let first = merged.next().unwrap();
        let Unfiltered::Row(row) = first else {
            panic!("expected row");
        };
        assert_eq!(row.clustering_key, b"ck1");
        let crate::rows::unfiltered::ColumnData::Simple(cell) = &row.columns["v"] else {
            panic!("expected simple cell");
        };
        assert_eq!(cell.value.as_deref(), Some(b"new".as_slice()));

        assert_eq!(merged.next().unwrap().clustering_key(), b"ck2");
        assert_eq!(merged.next().unwrap().clustering_key(), b"ck3");
        assert!(merged.next().is_none());
    }

    #[test]
    fn merged_row_iterator_preserves_marker_order_around_rows() {
        let source = InMemoryRowIterator::new(
            b"pk".to_vec(),
            DeletionTime::LIVE,
            vec![
                Unfiltered::Marker(RangeTombstoneMarker::Open {
                    bound: crate::rows::unfiltered::ClusteringBound {
                        kind: ClusteringBoundKind::InclusiveStart,
                        values: b"ck1".to_vec(),
                    },
                    deletion: DeletionTime::new(100, 100),
                }),
                Unfiltered::Row(rich_row(b"ck1", "v", b"value", 200)),
                Unfiltered::Marker(RangeTombstoneMarker::Close {
                    bound: crate::rows::unfiltered::ClusteringBound {
                        kind: ClusteringBoundKind::InclusiveEnd,
                        values: b"ck1".to_vec(),
                    },
                    deletion: DeletionTime::new(100, 100),
                }),
            ],
            false,
        );

        let mut merged = MergedRowIterator::new(vec![Box::new(source)]).unwrap();
        assert!(merged.next().unwrap().is_marker());
        assert!(merged.next().unwrap().is_row());
        assert!(merged.next().unwrap().is_marker());
    }

    #[test]
    fn merged_partition_iterator_groups_matching_partition_keys() {
        let mut left_pk1 = PartitionData::new();
        left_pk1.apply_row(simple_row(b"ck1", "v", b"left", 100));
        let mut left_pk3 = PartitionData::new();
        left_pk3.apply_row(simple_row(b"ck1", "v", b"later", 100));

        let mut right_pk1 = PartitionData::new();
        right_pk1.apply_row(simple_row(b"ck1", "v", b"right", 200));
        let mut right_pk2 = PartitionData::new();
        right_pk2.apply_row(simple_row(b"ck1", "v", b"middle", 100));

        let left = InMemoryPartitionIterator::from_partition_data(vec![
            (b"pk1".to_vec(), left_pk1),
            (b"pk3".to_vec(), left_pk3),
        ]);
        let right = InMemoryPartitionIterator::from_partition_data(vec![
            (b"pk1".to_vec(), right_pk1),
            (b"pk2".to_vec(), right_pk2),
        ]);

        let mut merged = MergedPartitionIterator::new(vec![Box::new(left), Box::new(right)]);

        let mut pk1 = merged.next_partition().unwrap();
        assert_eq!(pk1.partition_key(), b"pk1");
        let Unfiltered::Row(row) = pk1.next().unwrap() else {
            panic!("expected merged row");
        };
        let crate::rows::unfiltered::ColumnData::Simple(cell) = &row.columns["v"] else {
            panic!("expected simple cell");
        };
        assert_eq!(cell.value.as_deref(), Some(b"right".as_slice()));
        assert!(pk1.next().is_none());

        assert_eq!(merged.next_partition().unwrap().partition_key(), b"pk2");
        assert_eq!(merged.next_partition().unwrap().partition_key(), b"pk3");
        assert!(merged.next_partition().is_none());
    }

    #[test]
    fn merged_row_iterator_rejects_mismatched_partition_keys() {
        let left = InMemoryRowIterator::new(b"pk1".to_vec(), DeletionTime::LIVE, vec![], false);
        let right = InMemoryRowIterator::new(b"pk2".to_vec(), DeletionTime::LIVE, vec![], false);

        let err = match MergedRowIterator::new(vec![Box::new(left), Box::new(right)]) {
            Ok(_) => panic!("expected partition-key mismatch"),
            Err(err) => err,
        };
        assert!(matches!(
            err,
            IteratorMergeError::PartitionKeyMismatch { .. }
        ));
    }
}
