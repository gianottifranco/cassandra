// Licensed under Apache License, Version 2.0.

//! Sharded memtable: divides token space into N shards.
//!
//! ## Java Oracle
//! `org.apache.cassandra.db.Memtable` (sharding logic)
//! `org.apache.cassandra.db.TrieMemtable` (CPU-sharded variant)

use cassandra_common::token::Token;

use super::MemtableBackend;
use super::partition::{PartitionData, Row};

/// Boundaries for sharding the token space.
#[derive(Debug, Clone)]
pub struct ShardBoundaries {
    /// Number of shards.
    pub shard_count: usize,
    /// Boundary tokens (shard_count - 1 values).
    /// Shard i covers (boundaries[i-1], boundaries[i]].
    boundaries: Vec<i64>,
}

impl ShardBoundaries {
    /// Create N evenly-spaced shards across the full token range.
    pub fn new(shard_count: usize) -> Self {
        assert!(shard_count > 0, "shard_count must be positive");
        let mut boundaries = Vec::with_capacity(shard_count - 1);
        let range = i64::MAX as i128 - i64::MIN as i128;
        for i in 1..shard_count {
            let boundary = i64::MIN as i128 + (range * i as i128) / shard_count as i128;
            boundaries.push(boundary as i64);
        }
        Self {
            shard_count,
            boundaries,
        }
    }

    /// Determine which shard a token belongs to.
    pub fn shard_for_token(&self, token: Token) -> usize {
        let val = token.value();
        for (i, &boundary) in self.boundaries.iter().enumerate() {
            if val <= boundary {
                return i;
            }
        }
        self.shard_count - 1
    }
}

/// A sharded memtable that distributes writes across N backends.
///
/// Each shard has its own lock, eliminating the single coarse `RwLock`
/// bottleneck of non-sharded backends.
pub struct ShardedMemtable {
    shards: Vec<Box<dyn MemtableBackend>>,
    boundaries: ShardBoundaries,
}

impl ShardedMemtable {
    /// Create with the given backends (one per shard).
    pub fn new(shards: Vec<Box<dyn MemtableBackend>>) -> Self {
        let shard_count = shards.len();
        Self {
            shards,
            boundaries: ShardBoundaries::new(shard_count),
        }
    }

    fn shard_index(&self, partition_key: &[u8]) -> usize {
        let token = Token::from_partition_key(partition_key);
        self.boundaries.shard_for_token(token)
    }
}

impl MemtableBackend for ShardedMemtable {
    fn apply(&self, partition_key: Vec<u8>, row: Row) {
        let idx = self.shard_index(&partition_key);
        self.shards[idx].apply(partition_key, row);
    }

    fn set_partition_tombstone(
        &self,
        partition_key: Vec<u8>,
        timestamp: i64,
        local_deletion_time: i32,
    ) {
        let idx = self.shard_index(&partition_key);
        self.shards[idx].set_partition_tombstone(partition_key, timestamp, local_deletion_time);
    }

    fn get_partition(&self, partition_key: &[u8]) -> Option<PartitionData> {
        let idx = self.shard_index(partition_key);
        self.shards[idx].get_partition(partition_key)
    }

    fn iter_partitions(&self) -> Vec<(Vec<u8>, PartitionData)> {
        let mut all: Vec<(Vec<u8>, PartitionData)> = Vec::new();
        for shard in &self.shards {
            all.extend(shard.iter_partitions());
        }
        all.sort_by(|(a, _), (b, _)| a.cmp(b));
        all
    }

    fn memory_usage(&self) -> usize {
        self.shards.iter().map(|s| s.memory_usage()).sum()
    }

    fn partition_count(&self) -> usize {
        self.shards.iter().map(|s| s.partition_count()).sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memtable::SkipListMemtable;
    use crate::memtable::partition::Cell;

    fn test_row(ck: &[u8], col: &str, val: &[u8], ts: i64) -> Row {
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

    fn create_sharded(n: usize) -> ShardedMemtable {
        let shards: Vec<Box<dyn MemtableBackend>> = (0..n)
            .map(|_| Box::new(SkipListMemtable::new()) as Box<dyn MemtableBackend>)
            .collect();
        ShardedMemtable::new(shards)
    }

    #[test]
    fn shard_boundaries_even() {
        let sb = ShardBoundaries::new(4);
        assert_eq!(sb.shard_count, 4);
        assert_eq!(sb.boundaries.len(), 3);
        // Token::MINIMUM should go to shard 0
        assert_eq!(sb.shard_for_token(Token::from_raw(i64::MIN)), 0);
        // Token::MAXIMUM should go to last shard
        assert_eq!(sb.shard_for_token(Token::from_raw(i64::MAX)), 3);
    }

    #[test]
    fn single_shard() {
        let sb = ShardBoundaries::new(1);
        assert_eq!(sb.shard_for_token(Token::from_raw(0)), 0);
        assert_eq!(sb.shard_for_token(Token::from_raw(i64::MAX)), 0);
    }

    #[test]
    fn sharded_basic_operations() {
        let mt = create_sharded(4);
        mt.apply(b"pk1".to_vec(), test_row(b"ck1", "name", b"alice", 100));
        mt.apply(b"pk2".to_vec(), test_row(b"ck1", "name", b"bob", 100));

        assert_eq!(mt.partition_count(), 2);
        assert!(mt.get_partition(b"pk1").is_some());
        assert!(mt.get_partition(b"pk2").is_some());
        assert!(mt.get_partition(b"pk3").is_none());
    }

    #[test]
    fn sharded_merge() {
        let mt = create_sharded(4);
        mt.apply(b"pk1".to_vec(), test_row(b"ck1", "name", b"old", 100));
        mt.apply(b"pk1".to_vec(), test_row(b"ck1", "name", b"new", 200));

        let pd = mt.get_partition(b"pk1").unwrap();
        assert_eq!(pd.rows.len(), 1);
        let row = &pd.rows[&b"ck1".to_vec()];
        assert_eq!(row.cells[0].value.as_deref(), Some(b"new".as_slice()));
    }

    #[test]
    fn sharded_iter_sorted() {
        let mt = create_sharded(4);
        // Insert many keys to spread across shards
        for i in 0..20u32 {
            let key = format!("pk{i:04}").into_bytes();
            mt.apply(key, test_row(b"ck", "x", &i.to_be_bytes(), 100));
        }

        let partitions = mt.iter_partitions();
        assert_eq!(partitions.len(), 20);
        // Verify sorted by key
        for i in 1..partitions.len() {
            assert!(partitions[i - 1].0 <= partitions[i].0);
        }
    }

    #[test]
    fn sharded_memory_usage() {
        let mt = create_sharded(4);
        mt.apply(b"pk1".to_vec(), test_row(b"ck1", "x", b"v", 100));
        assert!(mt.memory_usage() > 0);
    }
}
