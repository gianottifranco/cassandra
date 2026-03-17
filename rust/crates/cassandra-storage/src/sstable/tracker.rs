// Licensed under Apache License, Version 2.0.

//! Reference-counted SSTable set with snapshot isolation.
//!
//! `SSTableSet` wraps an `Arc<BTreeSet<SSTableId>>` so that views (snapshots)
//! are cheap to clone and remain valid even after the tracker is mutated.
//! Mutations create a new `BTreeSet` and atomically swap the `Arc`, leaving
//! existing views pointing at the old, immutable set.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.db.lifecycle.Tracker`
//! - `org.apache.cassandra.db.lifecycle.View`

use std::collections::BTreeSet;
use std::sync::Arc;

use parking_lot::RwLock;

use crate::sstable::format::SSTableId;

// ─── SSTableSet ──────────────────────────────────────────────────────────────

/// A cheaply-cloneable, immutable snapshot of the current SSTable set.
#[derive(Debug, Clone)]
pub struct SSTableSet {
    inner: Arc<BTreeSet<SSTableId>>,
}

impl SSTableSet {
    /// Creates an empty SSTable set.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(BTreeSet::new()),
        }
    }

    /// Returns `true` if the set contains the given SSTable id.
    pub fn contains(&self, id: &SSTableId) -> bool {
        self.inner.contains(id)
    }

    /// Returns an iterator over the SSTable ids in sorted order.
    pub fn iter(&self) -> impl Iterator<Item = &SSTableId> {
        self.inner.iter()
    }

    /// Returns the number of SSTables in the set.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Returns `true` if the set is empty.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Returns a sorted `Vec` of all SSTable ids.
    pub fn to_vec(&self) -> Vec<SSTableId> {
        self.inner.iter().copied().collect()
    }
}

impl Default for SSTableSet {
    fn default() -> Self {
        Self::new()
    }
}

// ─── SSTableTracker ──────────────────────────────────────────────────────────

/// Thread-safe tracker for the current set of live SSTables.
///
/// Readers obtain a snapshot via [`view()`](SSTableTracker::view) that is
/// unaffected by concurrent mutations.  Writers create a new `BTreeSet` and
/// atomically swap the inner `Arc`.
#[derive(Debug)]
pub struct SSTableTracker {
    current: RwLock<SSTableSet>,
}

impl SSTableTracker {
    /// Creates a tracker with an empty SSTable set.
    pub fn new() -> Self {
        Self {
            current: RwLock::new(SSTableSet::new()),
        }
    }

    /// Creates a tracker pre-populated with the given ids.
    pub fn from_ids(ids: BTreeSet<SSTableId>) -> Self {
        Self {
            current: RwLock::new(SSTableSet {
                inner: Arc::new(ids),
            }),
        }
    }

    /// Returns a snapshot of the current SSTable set.
    ///
    /// The returned [`SSTableSet`] holds an `Arc` to the underlying data and
    /// will not be affected by subsequent mutations on the tracker.
    pub fn view(&self) -> SSTableSet {
        self.current.read().clone()
    }

    /// Adds an SSTable id to the tracked set.
    pub fn add(&self, id: SSTableId) {
        let mut guard = self.current.write();
        let mut new_set = (*guard.inner).clone();
        new_set.insert(id);
        guard.inner = Arc::new(new_set);
    }

    /// Removes an SSTable id from the tracked set.
    ///
    /// Returns `true` if the id was present.
    pub fn remove(&self, id: &SSTableId) -> bool {
        let mut guard = self.current.write();
        let mut new_set = (*guard.inner).clone();
        let removed = new_set.remove(id);
        guard.inner = Arc::new(new_set);
        removed
    }

    /// Atomically removes a set of old SSTables and adds new ones.
    ///
    /// This is the primary operation used after compaction: the old input
    /// SSTables are removed and the new compacted SSTable(s) are added in a
    /// single write-lock acquisition.
    pub fn apply_replacement(&self, remove: &[SSTableId], add: &[SSTableId]) {
        let mut guard = self.current.write();
        let mut new_set = (*guard.inner).clone();
        for id in remove {
            new_set.remove(id);
        }
        for id in add {
            new_set.insert(*id);
        }
        guard.inner = Arc::new(new_set);
    }

    /// Returns the number of SSTables currently tracked.
    pub fn count(&self) -> usize {
        self.current.read().len()
    }
}

impl Default for SSTableTracker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn add_and_remove_basic() {
        let tracker = SSTableTracker::new();
        assert_eq!(tracker.count(), 0);

        tracker.add(1);
        tracker.add(2);
        tracker.add(3);
        assert_eq!(tracker.count(), 3);

        assert!(tracker.remove(&2));
        assert_eq!(tracker.count(), 2);

        assert!(!tracker.remove(&42));
        assert_eq!(tracker.count(), 2);

        let view = tracker.view();
        assert!(view.contains(&1));
        assert!(!view.contains(&2));
        assert!(view.contains(&3));
    }

    #[test]
    fn from_ids_initializes_correctly() {
        let mut ids = BTreeSet::new();
        ids.insert(10);
        ids.insert(20);
        ids.insert(30);

        let tracker = SSTableTracker::from_ids(ids);
        assert_eq!(tracker.count(), 3);

        let view = tracker.view();
        assert_eq!(view.to_vec(), vec![10, 20, 30]);
    }

    #[test]
    fn view_returns_snapshot_isolation() {
        let tracker = SSTableTracker::new();
        tracker.add(1);
        tracker.add(2);

        // Take a snapshot.
        let snapshot = tracker.view();
        assert_eq!(snapshot.len(), 2);
        assert!(snapshot.contains(&1));
        assert!(snapshot.contains(&2));

        // Mutate the tracker after the snapshot.
        tracker.add(3);
        tracker.remove(&1);

        // Snapshot is unchanged.
        assert_eq!(snapshot.len(), 2);
        assert!(snapshot.contains(&1));
        assert!(!snapshot.contains(&3));

        // A new view reflects the mutations.
        let new_snapshot = tracker.view();
        assert_eq!(new_snapshot.len(), 2);
        assert!(!new_snapshot.contains(&1));
        assert!(new_snapshot.contains(&2));
        assert!(new_snapshot.contains(&3));
    }

    #[test]
    fn apply_replacement_atomically_swaps() {
        let tracker = SSTableTracker::new();
        tracker.add(1);
        tracker.add(2);
        tracker.add(3);

        // Simulate compaction: merge SSTables 1 and 2 into SSTable 100.
        tracker.apply_replacement(&[1, 2], &[100]);

        let view = tracker.view();
        assert_eq!(view.len(), 2);
        assert!(!view.contains(&1));
        assert!(!view.contains(&2));
        assert!(view.contains(&3));
        assert!(view.contains(&100));
    }

    #[test]
    fn sstable_set_iter_and_to_vec() {
        let tracker = SSTableTracker::new();
        tracker.add(3);
        tracker.add(1);
        tracker.add(2);

        let view = tracker.view();
        let via_iter: Vec<SSTableId> = view.iter().copied().collect();
        let via_to_vec = view.to_vec();

        assert_eq!(via_iter, vec![1, 2, 3]);
        assert_eq!(via_to_vec, vec![1, 2, 3]);
    }

    #[test]
    fn sstable_set_empty() {
        let set = SSTableSet::new();
        assert!(set.is_empty());
        assert_eq!(set.len(), 0);
        assert!(!set.contains(&1));
        assert!(set.to_vec().is_empty());
    }

    #[test]
    fn concurrent_access() {
        use std::sync::Arc as StdArc;
        use std::thread;

        let tracker = StdArc::new(SSTableTracker::new());
        let mut handles = Vec::new();

        // Spawn 10 threads, each adding 100 unique ids.
        for t in 0..10u64 {
            let tracker = StdArc::clone(&tracker);
            handles.push(thread::spawn(move || {
                for i in 0..100u64 {
                    tracker.add(t * 1000 + i);
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(tracker.count(), 1000);
    }
}
