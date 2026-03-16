// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

//! Legacy key-based secondary index.
//!
//! ## Java Oracle
//!
//! `org.apache.cassandra.index.internal.keys.KeysIndex`
//!
//! ## Design
//!
//! The legacy secondary index maintains an in-memory inverted index:
//! `indexed_value → Set<(partition_key, clustering_key)>`.
//!
//! In production Cassandra, this is stored as a hidden table.  Here we
//! use an in-memory BTreeMap for the initial implementation, which is
//! sufficient for correctness testing and small-scale validation.
//!
//! ## Limitations
//!
//! - In-memory only (not persisted to SSTable index segments).
//! - No bloom filter integration.
//! - Single-term exact-match only (no range queries).
//! - TODO: Persist index entries alongside SSTables for durability.

use parking_lot::RwLock;
use std::collections::{BTreeMap, BTreeSet};

use super::{IndexDefinition, IndexEntry, IndexError, IndexType, SecondaryIndex};

/// A legacy key-based secondary index (inverted index in memory).
#[derive(Debug)]
pub struct LegacyIndex {
    definition: IndexDefinition,
    /// term → set of (partition_key, clustering_key)
    entries: RwLock<BTreeMap<Vec<u8>, BTreeSet<(Vec<u8>, Vec<u8>)>>>,
}

impl LegacyIndex {
    /// Create a new legacy index from a definition.
    pub fn new(definition: IndexDefinition) -> Self {
        assert_eq!(
            definition.index_type,
            IndexType::Legacy,
            "LegacyIndex requires IndexType::Legacy"
        );
        Self {
            definition,
            entries: RwLock::new(BTreeMap::new()),
        }
    }

    /// Create a legacy index with a simplified definition.
    pub fn create(name: &str, keyspace: &str, table: &str, column: &str) -> Self {
        Self::new(IndexDefinition {
            name: name.to_string(),
            keyspace: keyspace.to_string(),
            table: table.to_string(),
            column: column.to_string(),
            index_type: IndexType::Legacy,
            options: Default::default(),
        })
    }

    /// Number of distinct indexed terms.
    pub fn term_count(&self) -> usize {
        self.entries.read().len()
    }

    /// Total number of index entries across all terms.
    pub fn entry_count(&self) -> usize {
        self.entries.read().values().map(|s| s.len()).sum()
    }
}

impl SecondaryIndex for LegacyIndex {
    fn insert(&self, entry: &IndexEntry) -> Result<(), IndexError> {
        let mut map = self.entries.write();
        map.entry(entry.term.clone())
            .or_default()
            .insert((entry.partition_key.clone(), entry.clustering_key.clone()));
        Ok(())
    }

    fn delete(&self, entry: &IndexEntry) -> Result<(), IndexError> {
        let mut map = self.entries.write();
        if let Some(set) = map.get_mut(&entry.term) {
            set.remove(&(entry.partition_key.clone(), entry.clustering_key.clone()));
            if set.is_empty() {
                map.remove(&entry.term);
            }
        }
        Ok(())
    }

    fn search(&self, term: &[u8]) -> Result<Vec<IndexEntry>, IndexError> {
        let map = self.entries.read();
        let results = map
            .get(term)
            .map(|set| {
                set.iter()
                    .map(|(pk, ck)| IndexEntry {
                        term: term.to_vec(),
                        partition_key: pk.clone(),
                        clustering_key: ck.clone(),
                    })
                    .collect()
            })
            .unwrap_or_default();
        Ok(results)
    }

    fn range_search(
        &self,
        _start: Option<&[u8]>,
        _end: Option<&[u8]>,
    ) -> Result<Vec<IndexEntry>, IndexError> {
        // Legacy indexes don't support range queries efficiently
        Err(IndexError::ReadFailed(
            "Legacy index does not support range queries".to_string(),
        ))
    }

    fn truncate(&self) -> Result<(), IndexError> {
        self.entries.write().clear();
        Ok(())
    }

    fn definition(&self) -> &IndexDefinition {
        &self.definition
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_index() -> LegacyIndex {
        LegacyIndex::create("idx_email", "ks", "users", "email")
    }

    fn entry(term: &[u8], pk: &[u8], ck: &[u8]) -> IndexEntry {
        IndexEntry {
            term: term.to_vec(),
            partition_key: pk.to_vec(),
            clustering_key: ck.to_vec(),
        }
    }

    #[test]
    fn insert_and_search() {
        let idx = test_index();
        idx.insert(&entry(b"alice@example.com", b"user1", b""))
            .unwrap();
        idx.insert(&entry(b"bob@example.com", b"user2", b""))
            .unwrap();

        let results = idx.search(b"alice@example.com").unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].partition_key, b"user1");

        let all_bobs = idx.search(b"bob@example.com").unwrap();
        assert_eq!(all_bobs.len(), 1);
    }

    #[test]
    fn search_missing_term() {
        let idx = test_index();
        let results = idx.search(b"nobody@example.com").unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn insert_duplicate_same_key() {
        let idx = test_index();
        idx.insert(&entry(b"email", b"pk1", b"ck1")).unwrap();
        idx.insert(&entry(b"email", b"pk1", b"ck1")).unwrap(); // duplicate
        assert_eq!(idx.entry_count(), 1); // BTreeSet deduplicates
    }

    #[test]
    fn multiple_rows_same_term() {
        let idx = test_index();
        idx.insert(&entry(b"common@email.com", b"pk1", b""))
            .unwrap();
        idx.insert(&entry(b"common@email.com", b"pk2", b""))
            .unwrap();
        idx.insert(&entry(b"common@email.com", b"pk3", b""))
            .unwrap();

        let results = idx.search(b"common@email.com").unwrap();
        assert_eq!(results.len(), 3);
    }

    #[test]
    fn delete_entry() {
        let idx = test_index();
        idx.insert(&entry(b"email", b"pk1", b"")).unwrap();
        idx.insert(&entry(b"email", b"pk2", b"")).unwrap();

        idx.delete(&entry(b"email", b"pk1", b"")).unwrap();

        let results = idx.search(b"email").unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].partition_key, b"pk2");
    }

    #[test]
    fn delete_last_entry_removes_term() {
        let idx = test_index();
        idx.insert(&entry(b"email", b"pk1", b"")).unwrap();
        idx.delete(&entry(b"email", b"pk1", b"")).unwrap();

        assert_eq!(idx.term_count(), 0);
        assert_eq!(idx.entry_count(), 0);
    }

    #[test]
    fn truncate() {
        let idx = test_index();
        idx.insert(&entry(b"a", b"pk1", b"")).unwrap();
        idx.insert(&entry(b"b", b"pk2", b"")).unwrap();
        idx.truncate().unwrap();
        assert_eq!(idx.term_count(), 0);
    }

    #[test]
    fn range_search_not_supported() {
        let idx = test_index();
        let result = idx.range_search(Some(b"a"), Some(b"z"));
        assert!(result.is_err());
    }
}
