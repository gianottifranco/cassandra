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

//! # Storage Attached Indexing (SAI)
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.index.sai` — the SAI package
//! - `org.apache.cassandra.index.sai.StorageAttachedIndex`
//! - `org.apache.cassandra.index.sai.disk.v1` — on-disk format
//!
//! ## Design
//!
//! SAI indexes maintain per-SSTable index segments that co-locate with the
//! data.  Unlike legacy indexes which maintain a separate hidden table,
//! SAI segments are:
//! - Built during memtable flush (when SSTable is written)
//! - Merged during compaction (when SSTables are merged)
//! - Streamed alongside SSTables during bootstrap/repair
//!
//! This module provides:
//! - [`SaiIndex`] — implements `SecondaryIndex` with in-memory posting lists
//! - [`PostingList`] — sorted list of row locations for a given term
//! - [`SaiSegment`] — per-SSTable index segment
//! - [`SaiBuilder`] — builds SAI segments from SSTable data
//!
//! ## Current Limitations
//!
//! - Posting lists are in-memory only (not persisted to disk segments yet)
//! - No trie-based term dictionary (using BTreeMap instead)
//! - No bloom filter for term existence check
//! - No integration with compaction lifecycle yet
//! - Vector index support is partial (brute-force kNN, no HNSW/IVF)
//!
//! ## TODO
//!
//! - [ ] Persist posting lists to disk alongside SSTables
//! - [ ] Implement trie-based term dictionary for memory efficiency
//! - [ ] Add bloom filter for fast non-match elimination
//! - [ ] Integrate with compaction: rebuild segments on SSTable merge
//! - [ ] Integrate with streaming: include SAI segments in stream plan
//! - [ ] Implement approximate nearest neighbor (HNSW or IVF) for vectors

pub mod bloom;
pub mod builder;
pub mod hnsw;
pub mod posting;
pub mod query;
pub mod segment_format;
pub mod vector_index;

use parking_lot::RwLock;
use std::collections::BTreeMap;

use super::{IndexDefinition, IndexEntry, IndexError, IndexType, SecondaryIndex};
use posting::PostingList;

/// A Storage Attached Index.
///
/// Maintains an in-memory index of term → posting list.  In production,
/// each SSTable would have its own on-disk SAI segment; here we maintain
/// a merged in-memory view for correctness validation.
#[derive(Debug)]
pub struct SaiIndex {
    definition: IndexDefinition,
    /// term → posting list (sorted row locations)
    terms: RwLock<BTreeMap<Vec<u8>, PostingList>>,
    /// Vector search index, instantiated if this is a vector column
    vector_index: Option<std::sync::Arc<crate::index::sai::vector_index::VectorIndex>>,
}

impl SaiIndex {
    pub fn new(definition: IndexDefinition) -> Self {
        assert_eq!(
            definition.index_type,
            IndexType::Sai,
            "SaiIndex requires IndexType::Sai"
        );
        let vector_index = if let Some(dims_str) = definition.options.get("vector_dimensions") {
            let dims = dims_str.parse().unwrap_or(0);
            let metric_str = definition
                .options
                .get("vector_similarity_metric")
                .map(|s| s.as_str())
                .unwrap_or("cosine");
            let metric = match metric_str.to_lowercase().as_str() {
                "euclidean" => cassandra_types::vector::SimilarityMetric::Euclidean,
                "dot_product" => cassandra_types::vector::SimilarityMetric::DotProduct,
                _ => cassandra_types::vector::SimilarityMetric::Cosine,
            };
            Some(std::sync::Arc::new(
                crate::index::sai::vector_index::VectorIndex::new(dims, metric),
            ))
        } else {
            None
        };

        Self {
            definition,
            terms: RwLock::new(BTreeMap::new()),
            vector_index,
        }
    }

    /// Create a SAI index with a simplified definition.
    pub fn create(name: &str, keyspace: &str, table: &str, column: &str) -> Self {
        Self::new(IndexDefinition {
            name: name.to_string(),
            keyspace: keyspace.to_string(),
            table: table.to_string(),
            column: column.to_string(),
            index_type: IndexType::Sai,
            options: Default::default(),
        })
    }

    /// Number of distinct indexed terms.
    pub fn term_count(&self) -> usize {
        self.terms.read().len()
    }

    /// Total number of postings across all terms.
    pub fn posting_count(&self) -> usize {
        self.terms.read().values().map(|p| p.len()).sum()
    }

    /// Perform a range search on sorted terms.
    pub fn range_search_inner(&self, start: Option<&[u8]>, end: Option<&[u8]>) -> Vec<IndexEntry> {
        let terms = self.terms.read();
        let mut results = Vec::new();

        let iter: Box<dyn Iterator<Item = (&Vec<u8>, &PostingList)>> = match (start, end) {
            (Some(s), Some(e)) => Box::new(terms.range(s.to_vec()..=e.to_vec())),
            (Some(s), None) => Box::new(terms.range(s.to_vec()..)),
            (None, Some(e)) => Box::new(terms.range(..=e.to_vec())),
            (None, None) => Box::new(terms.iter()),
        };

        for (term, posting_list) in iter {
            for location in posting_list.locations() {
                results.push(IndexEntry {
                    term: term.clone(),
                    partition_key: location.partition_key.clone(),
                    clustering_key: location.clustering_key.clone(),
                });
            }
        }

        results
    }
}

impl SecondaryIndex for SaiIndex {
    fn insert(&self, entry: &IndexEntry) -> Result<(), IndexError> {
        if let Some(ref vi) = self.vector_index {
            if let Ok(vec_val) =
                cassandra_types::vector::VectorValue::deserialize(&entry.term, vi.dimensions())
            {
                let _ = vi.insert(
                    vec_val,
                    entry.partition_key.clone(),
                    entry.clustering_key.clone(),
                );
            }
        }

        let mut terms = self.terms.write();
        let posting_list = terms.entry(entry.term.clone()).or_default();
        posting_list.add(entry.partition_key.clone(), entry.clustering_key.clone());
        Ok(())
    }

    fn delete(&self, entry: &IndexEntry) -> Result<(), IndexError> {
        let mut terms = self.terms.write();
        if let Some(posting_list) = terms.get_mut(&entry.term) {
            posting_list.remove(&entry.partition_key, &entry.clustering_key);
            if posting_list.is_empty() {
                terms.remove(&entry.term);
            }
        }
        Ok(())
    }

    fn search(&self, term: &[u8]) -> Result<Vec<IndexEntry>, IndexError> {
        let terms = self.terms.read();
        let results = terms
            .get(term)
            .map(|pl| {
                pl.locations()
                    .iter()
                    .map(|loc| IndexEntry {
                        term: term.to_vec(),
                        partition_key: loc.partition_key.clone(),
                        clustering_key: loc.clustering_key.clone(),
                    })
                    .collect()
            })
            .unwrap_or_default();
        Ok(results)
    }

    fn range_search(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
    ) -> Result<Vec<IndexEntry>, IndexError> {
        Ok(self.range_search_inner(start, end))
    }

    fn search_vector(
        &self,
        vector_bytes: &[u8],
        top_k: usize,
    ) -> Result<Vec<(IndexEntry, f32)>, IndexError> {
        if let Some(ref vi) = self.vector_index {
            let vec_val =
                cassandra_types::vector::VectorValue::deserialize(vector_bytes, vi.dimensions())
                    .map_err(|e| IndexError::ReadFailed(e.to_string()))?;
            let results = vi.knn_search(&vec_val, top_k);
            let entries = results
                .into_iter()
                .map(|res| {
                    (
                        IndexEntry {
                            term: vector_bytes.to_vec(),
                            partition_key: res.location.partition_key,
                            clustering_key: res.location.clustering_key,
                        },
                        res.score,
                    )
                })
                .collect();
            Ok(entries)
        } else {
            Err(IndexError::ReadFailed("Not a vector index".into()))
        }
    }

    fn truncate(&self) -> Result<(), IndexError> {
        self.terms.write().clear();
        Ok(())
    }

    fn definition(&self) -> &IndexDefinition {
        &self.definition
    }

    fn add_sai_segment(
        &self,
        segment: crate::index::sai::builder::SaiSegment,
    ) -> Result<(), IndexError> {
        let mut terms = self.terms.write();
        for (term, posting_list) in segment.terms {
            if let Some(ref vi) = self.vector_index {
                if let Ok(vec_val) =
                    cassandra_types::vector::VectorValue::deserialize(&term, vi.dimensions())
                {
                    for loc in posting_list.locations() {
                        let _ = vi.insert(
                            vec_val.clone(),
                            loc.partition_key.clone(),
                            loc.clustering_key.clone(),
                        );
                    }
                }
            }
            let existing_list = terms.entry(term).or_default();
            existing_list.merge(&posting_list);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_sai() -> SaiIndex {
        SaiIndex::create("sai_age", "ks", "users", "age")
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
        let idx = test_sai();
        idx.insert(&entry(b"\x00\x00\x00\x19", b"user1", b""))
            .unwrap(); // age=25
        idx.insert(&entry(b"\x00\x00\x00\x1e", b"user2", b""))
            .unwrap(); // age=30

        let results = idx.search(b"\x00\x00\x00\x19").unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].partition_key, b"user1");
    }

    #[test]
    fn range_search() {
        let idx = test_sai();
        // Insert ages 20, 25, 30, 35
        idx.insert(&entry(b"\x14", b"pk20", b"")).unwrap();
        idx.insert(&entry(b"\x19", b"pk25", b"")).unwrap();
        idx.insert(&entry(b"\x1e", b"pk30", b"")).unwrap();
        idx.insert(&entry(b"\x23", b"pk35", b"")).unwrap();

        // Range [25, 30]
        let results = idx.range_search(Some(b"\x19"), Some(b"\x1e")).unwrap();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn delete_posting() {
        let idx = test_sai();
        idx.insert(&entry(b"term", b"pk1", b"")).unwrap();
        idx.insert(&entry(b"term", b"pk2", b"")).unwrap();

        idx.delete(&entry(b"term", b"pk1", b"")).unwrap();

        let results = idx.search(b"term").unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].partition_key, b"pk2");
    }

    #[test]
    fn truncate_clears_all() {
        let idx = test_sai();
        idx.insert(&entry(b"a", b"pk1", b"")).unwrap();
        idx.insert(&entry(b"b", b"pk2", b"")).unwrap();
        idx.truncate().unwrap();
        assert_eq!(idx.term_count(), 0);
        assert_eq!(idx.posting_count(), 0);
    }

    #[test]
    fn open_range_search() {
        let idx = test_sai();
        idx.insert(&entry(b"a", b"pk1", b"")).unwrap();
        idx.insert(&entry(b"b", b"pk2", b"")).unwrap();
        idx.insert(&entry(b"c", b"pk3", b"")).unwrap();

        // >= "b"
        let results = idx.range_search(Some(b"b"), None).unwrap();
        assert_eq!(results.len(), 2); // b,c
    }
}
