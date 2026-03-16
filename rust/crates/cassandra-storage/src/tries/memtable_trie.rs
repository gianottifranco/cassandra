// Licensed under Apache License, Version 2.0.

//! Trie-based memtable backend using `InMemoryTrie<PartitionData>`.
//!
//! ## Java Oracle
//! `org.apache.cassandra.db.TrieMemtable`

use std::sync::atomic::{AtomicUsize, Ordering};

use parking_lot::RwLock;

use super::in_memory::InMemoryTrie;
use super::trie::Trie;
use crate::memtable::partition::{PartitionData, Row};
use crate::memtable::{MemtableBackend, estimate_row_size};

/// Memtable backend using a generic `InMemoryTrie`.
///
/// Unlike the existing `TrieMemtable` in `memtable/trie.rs` which uses
/// a custom `TrieNode`, this uses the generic `InMemoryTrie<T>` with
/// cursor-based iteration.
pub struct MemtableTrie {
    data: RwLock<InMemoryTrie<PartitionData>>,
    approx_size: AtomicUsize,
}

impl MemtableTrie {
    pub fn new() -> Self {
        Self {
            data: RwLock::new(InMemoryTrie::new()),
            approx_size: AtomicUsize::new(0),
        }
    }
}

impl Default for MemtableTrie {
    fn default() -> Self {
        Self::new()
    }
}

impl MemtableBackend for MemtableTrie {
    fn apply(&self, partition_key: Vec<u8>, row: Row) {
        let row_size = estimate_row_size(&row);
        let mut trie = self.data.write();
        trie.apply(&partition_key, |existing| {
            let mut pd = existing.cloned().unwrap_or_default();
            pd.apply_row(row.clone());
            pd
        });
        self.approx_size.fetch_add(row_size, Ordering::Relaxed);
    }

    fn get_partition(&self, partition_key: &[u8]) -> Option<PartitionData> {
        let trie = self.data.read();
        trie.get(partition_key).cloned()
    }

    fn iter_partitions(&self) -> Vec<(Vec<u8>, PartitionData)> {
        let trie = self.data.read();
        trie.iter()
            .into_iter()
            .map(|(k, v)| (k, v.clone()))
            .collect()
    }

    fn memory_usage(&self) -> usize {
        self.approx_size.load(Ordering::Relaxed)
    }

    fn partition_count(&self) -> usize {
        self.data.read().entry_count()
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
    fn basic_operations() {
        let mt = MemtableTrie::new();
        mt.apply(b"pk1".to_vec(), test_row(b"ck1", "name", b"alice", 100));

        assert_eq!(mt.partition_count(), 1);
        assert!(mt.memory_usage() > 0);

        let pd = mt.get_partition(b"pk1").unwrap();
        assert_eq!(pd.rows.len(), 1);
    }

    #[test]
    fn merge_rows() {
        let mt = MemtableTrie::new();
        mt.apply(b"pk1".to_vec(), test_row(b"ck1", "name", b"old", 100));
        mt.apply(b"pk1".to_vec(), test_row(b"ck1", "name", b"new", 200));

        let pd = mt.get_partition(b"pk1").unwrap();
        let row = &pd.rows[&b"ck1".to_vec()];
        assert_eq!(row.cells[0].value.as_deref(), Some(b"new".as_slice()));
    }

    #[test]
    fn sorted_iteration() {
        let mt = MemtableTrie::new();
        mt.apply(b"c".to_vec(), test_row(b"ck", "x", b"3", 100));
        mt.apply(b"a".to_vec(), test_row(b"ck", "x", b"1", 100));
        mt.apply(b"b".to_vec(), test_row(b"ck", "x", b"2", 100));

        let partitions = mt.iter_partitions();
        let keys: Vec<_> = partitions.iter().map(|(k, _)| k.clone()).collect();
        assert_eq!(keys, vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
    }

    #[test]
    fn missing_partition() {
        let mt = MemtableTrie::new();
        assert!(mt.get_partition(b"missing").is_none());
    }
}
