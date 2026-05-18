// Licensed under Apache License, Version 2.0.

//! Merge trie: lazily merges multiple tries.
//!
//! ## Java Oracle
//! `org.apache.cassandra.db.tries.MergeTrie`

use super::in_memory::InMemoryTrie;
use super::trie::Trie;

/// Merges multiple `InMemoryTrie`s into a single view.
///
/// This is used during flush when merging memtable segments, or when
/// combining multiple SSTables' trie indexes.
pub struct MergeTrie<T> {
    /// Source tries to merge.
    tries: Vec<InMemoryTrie<T>>,
}

impl<T: Clone> MergeTrie<T> {
    pub fn new(tries: Vec<InMemoryTrie<T>>) -> Self {
        Self { tries }
    }

    /// Materialize the merge into a single `InMemoryTrie`.
    ///
    /// The merge function is called when the same key exists in multiple tries.
    pub fn materialize<F>(self, merge_fn: F) -> InMemoryTrie<T>
    where
        F: Fn(&T, &T) -> T + Copy,
    {
        let mut result = InMemoryTrie::new();

        for source in &self.tries {
            for (key, value) in source.iter() {
                result.apply(&key, |existing| match existing {
                    Some(existing) => merge_fn(existing, value),
                    None => value.clone(),
                });
            }
        }

        result
    }

    /// Number of source tries.
    pub fn source_count(&self) -> usize {
        self.tries.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merge_disjoint() {
        let mut t1 = InMemoryTrie::new();
        t1.apply(b"a", |_| 1);
        t1.apply(b"b", |_| 2);

        let mut t2 = InMemoryTrie::new();
        t2.apply(b"c", |_| 3);
        t2.apply(b"d", |_| 4);

        let merged = MergeTrie::new(vec![t1, t2]).materialize(|a, b| a + b);
        assert_eq!(merged.entry_count(), 4);
        assert_eq!(merged.get(b"a"), Some(&1));
        assert_eq!(merged.get(b"d"), Some(&4));
    }

    #[test]
    fn merge_overlapping() {
        let mut t1 = InMemoryTrie::new();
        t1.apply(b"key", |_| 10);

        let mut t2 = InMemoryTrie::new();
        t2.apply(b"key", |_| 20);

        let merged = MergeTrie::new(vec![t1, t2]).materialize(|a, b| a + b);
        assert_eq!(merged.get(b"key"), Some(&30));
        assert_eq!(merged.entry_count(), 1);
    }

    #[test]
    fn merge_empty() {
        let merged: InMemoryTrie<i32> = MergeTrie::new(vec![]).materialize(|a, _b| *a);
        assert!(merged.is_empty());
    }

    #[test]
    fn merge_single() {
        let mut t1 = InMemoryTrie::new();
        t1.apply(b"x", |_| 42);

        let merged = MergeTrie::new(vec![t1]).materialize(|a, _b| *a);
        assert_eq!(merged.get(b"x"), Some(&42));
    }

    #[test]
    fn merge_preserves_order() {
        let mut t1 = InMemoryTrie::new();
        t1.apply(b"c", |_| 3);

        let mut t2 = InMemoryTrie::new();
        t2.apply(b"a", |_| 1);

        let mut t3 = InMemoryTrie::new();
        t3.apply(b"b", |_| 2);

        let merged = MergeTrie::new(vec![t1, t2, t3]).materialize(|a, _b| *a);
        let entries: Vec<_> = merged.iter();
        let keys: Vec<_> = entries.iter().map(|(k, _)| k.clone()).collect();
        assert_eq!(keys, vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
    }
}
