// Licensed under Apache License, Version 2.0.

//! Mutable deletion info: partition deletion + range tombstones.
//!
//! ## Java Oracle
//! `org.apache.cassandra.db.MutableDeletionInfo`

use cassandra_common::tombstone::{DeletionTime, RangeTombstone};

/// Mutable deletion info for a partition.
///
/// Tracks the partition-level deletion and a list of range tombstones.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MutableDeletionInfo {
    /// Partition-level deletion.
    pub partition_deletion: DeletionTime,
    /// Range tombstones sorted by start bound.
    pub range_tombstones: Vec<RangeTombstone>,
}

impl MutableDeletionInfo {
    /// Create with no deletion.
    pub fn live() -> Self {
        Self {
            partition_deletion: DeletionTime::LIVE,
            range_tombstones: Vec::new(),
        }
    }

    /// Create with a partition deletion.
    pub fn with_partition_deletion(deletion: DeletionTime) -> Self {
        Self {
            partition_deletion: deletion,
            range_tombstones: Vec::new(),
        }
    }

    /// Returns `true` if there is no deletion info (live partition, no range tombstones).
    pub fn is_live(&self) -> bool {
        self.partition_deletion.is_live() && self.range_tombstones.is_empty()
    }

    /// Add a range tombstone.
    pub fn add_range_tombstone(&mut self, rt: RangeTombstone) {
        self.range_tombstones.push(rt);
        // Keep sorted by start bound for efficient lookup
        self.range_tombstones.sort_by(|a, b| a.start.cmp(&b.start));
    }

    /// Set or supersede the partition deletion.
    pub fn set_partition_deletion(&mut self, deletion: DeletionTime) {
        if deletion.supersedes(&self.partition_deletion) {
            self.partition_deletion = deletion;
        }
    }

    /// Merge another deletion info into this one.
    pub fn merge(&mut self, other: &MutableDeletionInfo) {
        self.set_partition_deletion(other.partition_deletion);
        for rt in &other.range_tombstones {
            self.add_range_tombstone(rt.clone());
        }
    }

    /// Find the most relevant range tombstone covering the given clustering key.
    pub fn range_tombstone_covering(&self, clustering_key: &[u8]) -> Option<&RangeTombstone> {
        self.range_tombstones
            .iter()
            .find(|rt| clustering_key >= rt.start.as_slice() && clustering_key <= rt.end.as_slice())
    }
}

impl Default for MutableDeletionInfo {
    fn default() -> Self {
        Self::live()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn live_by_default() {
        let di = MutableDeletionInfo::default();
        assert!(di.is_live());
    }

    #[test]
    fn partition_deletion() {
        let di = MutableDeletionInfo::with_partition_deletion(DeletionTime::new(100, 100));
        assert!(!di.is_live());
    }

    #[test]
    fn set_partition_deletion_supersedes() {
        let mut di = MutableDeletionInfo::with_partition_deletion(DeletionTime::new(100, 100));
        di.set_partition_deletion(DeletionTime::new(200, 200));
        assert_eq!(di.partition_deletion.marked_for_delete_at, 200);
        // Lower timestamp doesn't supersede
        di.set_partition_deletion(DeletionTime::new(50, 50));
        assert_eq!(di.partition_deletion.marked_for_delete_at, 200);
    }

    #[test]
    fn range_tombstone_coverage() {
        let mut di = MutableDeletionInfo::live();
        di.add_range_tombstone(RangeTombstone::new(
            vec![10],
            vec![20],
            DeletionTime::new(100, 100),
        ));
        assert!(di.range_tombstone_covering(&[15]).is_some());
        assert!(di.range_tombstone_covering(&[5]).is_none());
        assert!(di.range_tombstone_covering(&[25]).is_none());
    }

    #[test]
    fn merge_deletion_info() {
        let mut a = MutableDeletionInfo::with_partition_deletion(DeletionTime::new(100, 100));
        let mut b = MutableDeletionInfo::with_partition_deletion(DeletionTime::new(200, 200));
        b.add_range_tombstone(RangeTombstone::new(
            vec![1],
            vec![5],
            DeletionTime::new(150, 150),
        ));
        a.merge(&b);
        assert_eq!(a.partition_deletion.marked_for_delete_at, 200);
        assert_eq!(a.range_tombstones.len(), 1);
    }
}
