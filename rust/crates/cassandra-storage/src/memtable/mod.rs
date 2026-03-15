// Licensed under Apache License, Version 2.0.

//! Memtable: in-memory mutable storage for recent writes.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.db.Memtable`
//! - `org.apache.cassandra.db.SkipListMemtable` (default impl)
//! - `org.apache.cassandra.db.TrieMemtable` (newer, optional)
//!
//! ## Architecture
//!
//! The memtable stores partitions keyed by partition key bytes.
//! Within each partition, rows are sorted by clustering key bytes.
//! A `MemtableEntry` trait allows swapping implementations (skiplist → trie).
//!
//! The `MemtableManager` manages the lifecycle of memtables:
//! active → flushing → flushed. It enforces backpressure based on
//! memory thresholds.

pub mod partition;

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

use parking_lot::RwLock;

use partition::{PartitionData, Row};

// ─── Trait for pluggable memtable implementations ──────────────────────────

/// Trait for memtable backends. Enables swapping skiplist for trie in the future.
pub trait MemtableBackend: Send + Sync {
    /// Insert or merge a row into the given partition.
    fn apply(&self, partition_key: Vec<u8>, row: Row);

    /// Read a partition by key. Returns None if absent.
    fn get_partition(&self, partition_key: &[u8]) -> Option<PartitionData>;

    /// Iterate all partitions in sorted order (for flush).
    fn iter_partitions(&self) -> Vec<(Vec<u8>, PartitionData)>;

    /// Approximate memory usage in bytes.
    fn memory_usage(&self) -> usize;

    /// Number of partitions.
    fn partition_count(&self) -> usize;
}

// ─── SkipListMemtable (default implementation) ─────────────────────────────

/// Default memtable backed by a BTreeMap protected by RwLock.
///
/// This provides correct concurrent behavior. For higher concurrency,
/// a sharded or lock-free skiplist can replace this.
///
/// ## Future: TrieMemtable
/// When ready, implement `MemtableBackend` for a trie-based structure
/// and swap via feature flag or config.
pub struct SkipListMemtable {
    data: RwLock<BTreeMap<Vec<u8>, PartitionData>>,
    approx_size: AtomicUsize,
    op_count: AtomicU64,
}

impl SkipListMemtable {
    pub fn new() -> Self {
        Self {
            data: RwLock::new(BTreeMap::new()),
            approx_size: AtomicUsize::new(0),
            op_count: AtomicU64::new(0),
        }
    }

    pub fn operation_count(&self) -> u64 {
        self.op_count.load(Ordering::Relaxed)
    }
}

impl Default for SkipListMemtable {
    fn default() -> Self {
        Self::new()
    }
}

impl MemtableBackend for SkipListMemtable {
    fn apply(&self, partition_key: Vec<u8>, row: Row) {
        let row_size = estimate_row_size(&row);
        let mut data = self.data.write();
        let partition = data.entry(partition_key).or_insert_with(PartitionData::new);
        partition.apply_row(row);
        self.approx_size.fetch_add(row_size, Ordering::Relaxed);
        self.op_count.fetch_add(1, Ordering::Relaxed);
    }

    fn get_partition(&self, partition_key: &[u8]) -> Option<PartitionData> {
        let data = self.data.read();
        data.get(partition_key).cloned()
    }

    fn iter_partitions(&self) -> Vec<(Vec<u8>, PartitionData)> {
        let data = self.data.read();
        data.iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    fn memory_usage(&self) -> usize {
        self.approx_size.load(Ordering::Relaxed)
    }

    fn partition_count(&self) -> usize {
        self.data.read().len()
    }
}

fn estimate_row_size(row: &Row) -> usize {
    let mut size = row.clustering_key.len() + 32; // overhead
    for cell in &row.cells {
        size += cell.column.len() + cell.value.as_ref().map_or(0, |v| v.len()) + 24;
    }
    size
}

// ─── Memtable wrapper with metadata ───────────────────────────────────────

/// A memtable instance with unique ID and commit log position tracking.
pub struct Memtable {
    pub id: u64,
    backend: Box<dyn MemtableBackend>,
    /// The commit log segment ID when this memtable was created.
    pub commitlog_lower_bound: u64,
    /// The highest commit log segment ID seen by this memtable.
    pub commitlog_upper_bound: AtomicU64,
}

impl Memtable {
    pub fn new(id: u64, commitlog_segment_id: u64) -> Self {
        Self {
            id,
            backend: Box::new(SkipListMemtable::new()),
            commitlog_lower_bound: commitlog_segment_id,
            commitlog_upper_bound: AtomicU64::new(commitlog_segment_id),
        }
    }

    pub fn apply(&self, partition_key: Vec<u8>, row: Row) {
        self.backend.apply(partition_key, row);
    }

    pub fn get_partition(&self, partition_key: &[u8]) -> Option<PartitionData> {
        self.backend.get_partition(partition_key)
    }

    pub fn iter_partitions(&self) -> Vec<(Vec<u8>, PartitionData)> {
        self.backend.iter_partitions()
    }

    pub fn memory_usage(&self) -> usize {
        self.backend.memory_usage()
    }

    pub fn partition_count(&self) -> usize {
        self.backend.partition_count()
    }

    pub fn update_commitlog_upper_bound(&self, segment_id: u64) {
        self.commitlog_upper_bound.fetch_max(segment_id, Ordering::Relaxed);
    }
}

// ─── MemtableManager ──────────────────────────────────────────────────────

/// Manages memtable lifecycle: active → flushing transitions.
pub struct MemtableManager {
    /// The currently active memtable per column family (keyspace.table).
    active: RwLock<BTreeMap<String, Arc<Memtable>>>,
    /// Memtables currently being flushed.
    flushing: RwLock<Vec<Arc<Memtable>>>,
    /// Next memtable ID.
    next_id: AtomicU64,
    /// Memory threshold that triggers flush (bytes).
    pub flush_threshold: usize,
}

impl MemtableManager {
    pub fn new(flush_threshold: usize) -> Self {
        Self {
            active: RwLock::new(BTreeMap::new()),
            flushing: RwLock::new(Vec::new()),
            next_id: AtomicU64::new(1),
            flush_threshold,
        }
    }

    /// Get or create the active memtable for a column family.
    pub fn get_or_create(&self, cf_name: &str, commitlog_segment_id: u64) -> Arc<Memtable> {
        // Fast path: read lock
        {
            let active = self.active.read();
            if let Some(mt) = active.get(cf_name) {
                return Arc::clone(mt);
            }
        }

        // Slow path: create new memtable
        let mut active = self.active.write();
        active
            .entry(cf_name.to_string())
            .or_insert_with(|| {
                let id = self.next_id.fetch_add(1, Ordering::SeqCst);
                Arc::new(Memtable::new(id, commitlog_segment_id))
            })
            .clone()
    }

    /// Check if total memory across all active memtables exceeds threshold.
    pub fn should_flush(&self) -> bool {
        let active = self.active.read();
        let total: usize = active.values().map(|mt| mt.memory_usage()).sum();
        total >= self.flush_threshold
    }

    /// Total memory usage across all active memtables.
    pub fn total_memory_usage(&self) -> usize {
        let active = self.active.read();
        active.values().map(|mt| mt.memory_usage()).sum()
    }

    /// Swap the active memtable for a CF with a fresh one, returning
    /// the old one for flushing.
    pub fn switch_memtable(
        &self,
        cf_name: &str,
        commitlog_segment_id: u64,
    ) -> Option<Arc<Memtable>> {
        let mut active = self.active.write();
        let new_id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let new_mt = Arc::new(Memtable::new(new_id, commitlog_segment_id));

        let old = active.insert(cf_name.to_string(), new_mt);

        if let Some(ref old_mt) = old {
            self.flushing.write().push(Arc::clone(old_mt));
        }

        old
    }

    /// Mark a flushing memtable as complete.
    pub fn flush_complete(&self, memtable_id: u64) {
        let mut flushing = self.flushing.write();
        flushing.retain(|mt| mt.id != memtable_id);
    }

    /// Get the lowest commitlog segment bound across all active + flushing memtables.
    /// Everything below this can be discarded from the commitlog.
    pub fn lowest_commitlog_bound(&self) -> Option<u64> {
        let active = self.active.read();
        let flushing = self.flushing.read();

        let active_min = active.values().map(|mt| mt.commitlog_lower_bound).min();
        let flushing_min = flushing.iter().map(|mt| mt.commitlog_lower_bound).min();

        match (active_min, flushing_min) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (Some(a), None) => Some(a),
            (None, Some(b)) => Some(b),
            (None, None) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use partition::Cell;

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
    fn skiplist_basic_operations() {
        let mt = SkipListMemtable::new();
        let row = test_row(b"ck1", "name", b"alice", 1000);
        mt.apply(b"pk1".to_vec(), row);

        assert_eq!(mt.partition_count(), 1);
        assert!(mt.memory_usage() > 0);

        let partition = mt.get_partition(b"pk1").unwrap();
        assert_eq!(partition.rows.len(), 1);
        assert_eq!(partition.rows[&b"ck1".to_vec()].cells[0].column, "name");
    }

    #[test]
    fn skiplist_merge_rows() {
        let mt = SkipListMemtable::new();

        // First write
        let row1 = test_row(b"ck1", "name", b"alice", 1000);
        mt.apply(b"pk1".to_vec(), row1);

        // Second write to same partition, same clustering key, newer timestamp
        let row2 = test_row(b"ck1", "name", b"bob", 2000);
        mt.apply(b"pk1".to_vec(), row2);

        let partition = mt.get_partition(b"pk1").unwrap();
        assert_eq!(partition.rows.len(), 1);
        // Should have the newer value
        let cells = &partition.rows[&b"ck1".to_vec()].cells;
        assert_eq!(cells.len(), 1);
        assert_eq!(cells[0].value.as_deref(), Some(b"bob".as_slice()));
        assert_eq!(cells[0].timestamp, 2000);
    }

    #[test]
    fn skiplist_multiple_clustering_keys() {
        let mt = SkipListMemtable::new();
        mt.apply(b"pk1".to_vec(), test_row(b"ck1", "a", b"1", 100));
        mt.apply(b"pk1".to_vec(), test_row(b"ck2", "a", b"2", 100));
        mt.apply(b"pk1".to_vec(), test_row(b"ck3", "a", b"3", 100));

        let partition = mt.get_partition(b"pk1").unwrap();
        assert_eq!(partition.rows.len(), 3);
    }

    #[test]
    fn memtable_manager_lifecycle() {
        let mgr = MemtableManager::new(1024);
        let mt1 = mgr.get_or_create("ks.table1", 1);
        assert_eq!(mt1.id, 1);

        // Getting again returns the same one
        let mt1b = mgr.get_or_create("ks.table1", 1);
        assert_eq!(mt1.id, mt1b.id);

        // Different CF gets different memtable
        let mt2 = mgr.get_or_create("ks.table2", 1);
        assert_ne!(mt1.id, mt2.id);
    }

    #[test]
    fn memtable_manager_switch_and_flush() {
        let mgr = MemtableManager::new(1024);
        let mt1 = mgr.get_or_create("ks.t1", 1);
        mt1.apply(b"pk".to_vec(), test_row(b"ck", "c", b"v", 100));

        // Switch memtable
        let old = mgr.switch_memtable("ks.t1", 2).unwrap();
        assert_eq!(old.id, mt1.id);
        assert_eq!(old.partition_count(), 1);

        // New memtable is empty
        let new = mgr.get_or_create("ks.t1", 2);
        assert_eq!(new.partition_count(), 0);
        assert_ne!(new.id, old.id);

        // Complete flush
        mgr.flush_complete(old.id);
    }

    #[test]
    fn backpressure_detection() {
        let mgr = MemtableManager::new(100); // very small threshold
        let mt = mgr.get_or_create("ks.t1", 1);

        // Should not trigger initially
        assert!(!mgr.should_flush());

        // Write enough data
        for i in 0..20 {
            mt.apply(
                format!("pk{i}").into_bytes(),
                test_row(b"ck", "col", format!("value_{i}").as_bytes(), 100),
            );
        }

        assert!(mgr.should_flush());
    }

    #[test]
    fn commitlog_bound_tracking() {
        let mgr = MemtableManager::new(1024);
        let _mt1 = mgr.get_or_create("ks.t1", 5);
        let _mt2 = mgr.get_or_create("ks.t2", 10);

        assert_eq!(mgr.lowest_commitlog_bound(), Some(5));

        // Switch t1, now flushing mt with bound 5
        mgr.switch_memtable("ks.t1", 15);
        // Still 5 because it's flushing
        assert_eq!(mgr.lowest_commitlog_bound(), Some(5));
    }

    #[test]
    fn iter_partitions_sorted() {
        let mt = SkipListMemtable::new();
        mt.apply(b"c".to_vec(), test_row(b"ck", "x", b"3", 100));
        mt.apply(b"a".to_vec(), test_row(b"ck", "x", b"1", 100));
        mt.apply(b"b".to_vec(), test_row(b"ck", "x", b"2", 100));

        let partitions = mt.iter_partitions();
        let keys: Vec<_> = partitions.iter().map(|(k, _)| k.clone()).collect();
        assert_eq!(keys, vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
    }
}
