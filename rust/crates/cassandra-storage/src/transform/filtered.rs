// Licensed under Apache License, Version 2.0.

//! Filtered rows/partitions: wraps iterators with transformation chains.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.db.transform.FilteredRows`
//! - `org.apache.cassandra.db.transform.FilteredPartitions`

use cassandra_common::tombstone::DeletionTime;

use super::transformation::Transformation;
use crate::partitions::iterators::{UnfilteredPartitionIterator, UnfilteredRowIterator};
use crate::rows::unfiltered::{RowData, Unfiltered};

/// Wraps an `UnfilteredRowIterator` with a chain of transformations.
pub struct FilteredRows {
    source: Box<dyn UnfilteredRowIterator>,
    transforms: Vec<Box<dyn Transformation>>,
}

impl FilteredRows {
    pub fn new(source: Box<dyn UnfilteredRowIterator>) -> Self {
        Self {
            source,
            transforms: Vec::new(),
        }
    }

    /// Add a transformation to the chain.
    pub fn add_transform(mut self, transform: Box<dyn Transformation>) -> Self {
        self.transforms.push(transform);
        self
    }
}

impl UnfilteredRowIterator for FilteredRows {
    fn partition_key(&self) -> &[u8] {
        self.source.partition_key()
    }

    fn partition_deletion(&self) -> DeletionTime {
        self.source.partition_deletion()
    }

    fn static_row(&self) -> Option<&RowData> {
        self.source.static_row()
    }

    fn is_reversed(&self) -> bool {
        self.source.is_reversed()
    }

    fn next(&mut self) -> Option<Unfiltered> {
        loop {
            let item = self.source.next()?;
            let result = match item {
                Unfiltered::Row(row) => {
                    let mut current = Some(row);
                    for t in &mut self.transforms {
                        if let Some(r) = current {
                            current = t.apply_to_row(r);
                        } else {
                            break;
                        }
                    }
                    current.map(Unfiltered::Row)
                }
                Unfiltered::Marker(marker) => {
                    let mut current = Some(marker);
                    for t in &mut self.transforms {
                        if let Some(m) = current {
                            current = t.apply_to_marker(m);
                        } else {
                            break;
                        }
                    }
                    current.map(Unfiltered::Marker)
                }
            };
            if result.is_some() {
                return result;
            }
            // Item was filtered out, try next
        }
    }
}

/// Wraps an `UnfilteredPartitionIterator` with transformations.
pub struct FilteredPartitions {
    source: Box<dyn UnfilteredPartitionIterator>,
    transforms: Vec<Box<dyn Transformation>>,
}

impl FilteredPartitions {
    pub fn new(source: Box<dyn UnfilteredPartitionIterator>) -> Self {
        Self {
            source,
            transforms: Vec::new(),
        }
    }

    /// Add a transformation to the chain.
    pub fn add_transform(mut self, transform: Box<dyn Transformation>) -> Self {
        self.transforms.push(transform);
        self
    }
}

impl UnfilteredPartitionIterator for FilteredPartitions {
    fn next_partition(&mut self) -> Option<Box<dyn UnfilteredRowIterator>> {
        loop {
            let partition_iter = self.source.next_partition()?;
            let pk = partition_iter.partition_key().to_vec();
            let deletion = partition_iter.partition_deletion();

            // Check if any transform wants to skip this partition
            let mut skip = false;
            for t in &mut self.transforms {
                if !t.apply_to_partition(&pk, deletion) {
                    skip = true;
                    break;
                }
            }
            if skip {
                continue;
            }

            return Some(partition_iter);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::partitions::iterators::InMemoryRowIterator;
    use crate::rows::cell::CellData;
    use crate::rows::liveness::LivenessInfo;
    use crate::transform::transformation::PurgeTransform;
    use cassandra_common::ttl::NO_TTL;

    fn live_row(ck: &[u8], ts: i64) -> Unfiltered {
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
        Unfiltered::Row(row)
    }

    fn tombstone_row(ck: &[u8], ts: i64, ldt: i32) -> Unfiltered {
        let mut row = RowData::new(ck.to_vec());
        row.deletion = DeletionTime::new(ts, ldt);
        Unfiltered::Row(row)
    }

    #[test]
    fn filtered_rows_purge() {
        let items = vec![
            live_row(b"ck1", 100),
            tombstone_row(b"ck2", 50, 50), // purgeable
            live_row(b"ck3", 100),
        ];
        let source = InMemoryRowIterator::new(b"pk".to_vec(), DeletionTime::LIVE, items, false);

        let purge = PurgeTransform::new(100, 200);
        let mut filtered = FilteredRows::new(Box::new(source)).add_transform(Box::new(purge));

        let item1 = filtered.next().unwrap();
        assert_eq!(item1.clustering_key(), b"ck1");

        let item2 = filtered.next().unwrap();
        assert_eq!(item2.clustering_key(), b"ck3"); // ck2 was purged

        assert!(filtered.next().is_none());
    }
}
