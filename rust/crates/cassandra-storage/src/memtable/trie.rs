// Licensed under Apache License, Version 2.0.

//! Trie-based memtable implementation.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.db.TrieMemtable`
//!
//! ## Architecture
//!
//! Uses a prefix trie over partition key bytes for memory-efficient
//! storage and sorted iteration. Each leaf node holds a `PartitionData`.
//! Selected via `MemtableType::Trie` in configuration.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use parking_lot::RwLock;

use super::MemtableBackend;
use super::partition::{PartitionData, Row};

/// A node in the partition-key trie.
#[derive(Debug)]
struct TrieNode {
    children: BTreeMap<u8, TrieNode>,
    /// Partition data stored at this node (if this is a leaf).
    data: Option<PartitionData>,
}

impl TrieNode {
    fn new() -> Self {
        Self {
            children: BTreeMap::new(),
            data: None,
        }
    }

    fn insert(&mut self, key: &[u8], row: Row) {
        if key.is_empty() {
            let pd = self.data.get_or_insert_with(PartitionData::new);
            pd.apply_row(row);
        } else {
            let child = self.children.entry(key[0]).or_insert_with(TrieNode::new);
            child.insert(&key[1..], row);
        }
    }

    fn set_tombstone(&mut self, key: &[u8], timestamp: i64, local_deletion_time: i32) {
        if key.is_empty() {
            let pd = self.data.get_or_insert_with(PartitionData::new);
            pd.set_tombstone(timestamp, local_deletion_time);
        } else {
            let child = self.children.entry(key[0]).or_insert_with(TrieNode::new);
            child.set_tombstone(&key[1..], timestamp, local_deletion_time);
        }
    }

    fn get(&self, key: &[u8]) -> Option<&PartitionData> {
        if key.is_empty() {
            self.data.as_ref()
        } else {
            self.children.get(&key[0])?.get(&key[1..])
        }
    }

    /// DFS iteration in sorted order (BTreeMap keys are sorted).
    fn iter_all(&self, prefix: &mut Vec<u8>, out: &mut Vec<(Vec<u8>, PartitionData)>) {
        if let Some(ref pd) = self.data {
            out.push((prefix.clone(), pd.clone()));
        }
        for (&byte, child) in &self.children {
            prefix.push(byte);
            child.iter_all(prefix, out);
            prefix.pop();
        }
    }

    fn count_partitions(&self) -> usize {
        let mut count = if self.data.is_some() { 1 } else { 0 };
        for child in self.children.values() {
            count += child.count_partitions();
        }
        count
    }

    #[allow(dead_code)]
    fn estimate_memory(&self) -> usize {
        let mut size = std::mem::size_of::<TrieNode>();
        if let Some(ref pd) = self.data {
            size += estimate_partition_size(pd);
        }
        for child in self.children.values() {
            size += 1 + child.estimate_memory(); // 1 byte for the key
        }
        size
    }
}

#[allow(dead_code)]
fn estimate_partition_size(pd: &PartitionData) -> usize {
    let mut size = 64; // base overhead
    for (ck, row) in &pd.rows {
        size += ck.len() + 32;
        for cell in &row.cells {
            size += cell.column.len() + cell.value.as_ref().map_or(0, |v| v.len()) + 24;
        }
    }
    size
}

/// Trie-based memtable backend.
///
/// Provides memory-efficient storage for partition keys with common prefixes,
/// and naturally sorted iteration via DFS traversal.
pub struct TrieMemtable {
    root: RwLock<TrieNode>,
    approx_size: AtomicUsize,
    op_count: AtomicU64,
}

impl TrieMemtable {
    pub fn new() -> Self {
        Self {
            root: RwLock::new(TrieNode::new()),
            approx_size: AtomicUsize::new(0),
            op_count: AtomicU64::new(0),
        }
    }

    pub fn operation_count(&self) -> u64 {
        self.op_count.load(Ordering::Relaxed)
    }
}

impl Default for TrieMemtable {
    fn default() -> Self {
        Self::new()
    }
}

impl MemtableBackend for TrieMemtable {
    fn apply(&self, partition_key: Vec<u8>, row: Row) {
        let row_size = super::estimate_row_size(&row);
        let mut root = self.root.write();
        root.insert(&partition_key, row);
        self.approx_size.fetch_add(row_size, Ordering::Relaxed);
        self.op_count.fetch_add(1, Ordering::Relaxed);
    }

    fn set_partition_tombstone(
        &self,
        partition_key: Vec<u8>,
        timestamp: i64,
        local_deletion_time: i32,
    ) {
        let mut root = self.root.write();
        root.set_tombstone(&partition_key, timestamp, local_deletion_time);
        self.op_count.fetch_add(1, Ordering::Relaxed);
    }

    fn get_partition(&self, partition_key: &[u8]) -> Option<PartitionData> {
        let root = self.root.read();
        root.get(partition_key).cloned()
    }

    fn iter_partitions(&self) -> Vec<(Vec<u8>, PartitionData)> {
        let root = self.root.read();
        let mut result = Vec::new();
        let mut prefix = Vec::new();
        root.iter_all(&mut prefix, &mut result);
        result
    }

    fn memory_usage(&self) -> usize {
        self.approx_size.load(Ordering::Relaxed)
    }

    fn partition_count(&self) -> usize {
        self.root.read().count_partitions()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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

    #[test]
    fn trie_basic_operations() {
        let mt = TrieMemtable::new();
        mt.apply(b"pk1".to_vec(), test_row(b"ck1", "name", b"alice", 1000));

        assert_eq!(mt.partition_count(), 1);
        assert!(mt.memory_usage() > 0);

        let partition = mt.get_partition(b"pk1").unwrap();
        assert_eq!(partition.rows.len(), 1);
    }

    #[test]
    fn trie_merge_rows() {
        let mt = TrieMemtable::new();
        mt.apply(b"pk1".to_vec(), test_row(b"ck1", "name", b"old", 1000));
        mt.apply(b"pk1".to_vec(), test_row(b"ck1", "name", b"new", 2000));

        let partition = mt.get_partition(b"pk1").unwrap();
        assert_eq!(partition.rows.len(), 1);
        let cells = &partition.rows[&b"ck1".to_vec()].cells;
        assert_eq!(cells[0].value.as_deref(), Some(b"new".as_slice()));
        assert_eq!(cells[0].timestamp, 2000);
    }

    #[test]
    fn trie_sorted_iteration() {
        let mt = TrieMemtable::new();
        mt.apply(b"c".to_vec(), test_row(b"ck", "x", b"3", 100));
        mt.apply(b"a".to_vec(), test_row(b"ck", "x", b"1", 100));
        mt.apply(b"b".to_vec(), test_row(b"ck", "x", b"2", 100));

        let partitions = mt.iter_partitions();
        let keys: Vec<_> = partitions.iter().map(|(k, _)| k.clone()).collect();
        assert_eq!(keys, vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
    }

    #[test]
    fn trie_shared_prefix_efficiency() {
        let mt = TrieMemtable::new();
        // Keys with shared prefix
        mt.apply(b"users:1".to_vec(), test_row(b"ck", "n", b"a", 100));
        mt.apply(b"users:2".to_vec(), test_row(b"ck", "n", b"b", 100));
        mt.apply(b"users:3".to_vec(), test_row(b"ck", "n", b"c", 100));

        assert_eq!(mt.partition_count(), 3);

        let partitions = mt.iter_partitions();
        assert_eq!(partitions.len(), 3);
        assert_eq!(partitions[0].0, b"users:1");
        assert_eq!(partitions[1].0, b"users:2");
        assert_eq!(partitions[2].0, b"users:3");
    }

    #[test]
    fn trie_missing_partition_returns_none() {
        let mt = TrieMemtable::new();
        mt.apply(b"exists".to_vec(), test_row(b"ck", "n", b"v", 100));
        assert!(mt.get_partition(b"missing").is_none());
    }

    #[test]
    fn trie_operation_count() {
        let mt = TrieMemtable::new();
        assert_eq!(mt.operation_count(), 0);
        mt.apply(b"pk".to_vec(), test_row(b"ck", "n", b"v", 100));
        assert_eq!(mt.operation_count(), 1);
        mt.apply(b"pk".to_vec(), test_row(b"ck2", "n", b"v2", 200));
        assert_eq!(mt.operation_count(), 2);
    }
}
