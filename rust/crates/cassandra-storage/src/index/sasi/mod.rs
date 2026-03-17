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

//! SASI (SSTable Attached Secondary Index)
//!
//! ## Java Oracle
//!
//! `org.apache.cassandra.index.sasi.SASIIndex`
//!
//! ## Architecture
//!
//! SASI is an experimental index format in Cassandra. It provides advanced
//! querying capabilities over standard secondary indexes, including prefix,
//! contains, and sparse/dense token indexing.
//!
//! Due to its experimental status in the Java baseline, this module is
//! guarded by the `sasi` feature flag and typically disabled by default.

pub mod analyzer;

use std::collections::BTreeMap;

use parking_lot::RwLock;

use super::sai::posting::PostingList;
use super::{IndexDefinition, IndexEntry, IndexError, IndexType, SecondaryIndex};
use analyzer::{Analyzer, AnalyzerType};

/// An experimental SSTable Attached Secondary Index.
///
/// Uses an in-memory BTreeMap of analyzed terms to posting lists, with
/// pluggable analyzer support for tokenization and normalization.
#[derive(Debug)]
pub struct SasiIndex {
    definition: IndexDefinition,
    terms: RwLock<BTreeMap<Vec<u8>, PostingList>>,
    analyzer_type: AnalyzerType,
    analyzer: Box<dyn Analyzer>,
}

impl SasiIndex {
    /// Create a new SASI index from a definition.
    pub fn new(definition: IndexDefinition) -> Self {
        assert_eq!(
            definition.index_type,
            IndexType::Sasi,
            "SasiIndex requires IndexType::Sasi"
        );

        let analyzer_type = definition
            .options
            .get("analyzer_class")
            .and_then(|v| match v.as_str() {
                "standard" => Some(AnalyzerType::Standard),
                "non_tokenizing" => Some(AnalyzerType::NonTokenizing),
                _ => None,
            })
            .unwrap_or_default();

        let analyzer = analyzer_type.create();

        Self {
            definition,
            terms: RwLock::new(BTreeMap::new()),
            analyzer_type,
            analyzer,
        }
    }

    /// Number of distinct indexed terms.
    pub fn term_count(&self) -> usize {
        self.terms.read().len()
    }

    /// The analyzer type in use.
    pub fn analyzer_type(&self) -> AnalyzerType {
        self.analyzer_type
    }
}

impl SecondaryIndex for SasiIndex {
    fn insert(&self, entry: &IndexEntry) -> Result<(), IndexError> {
        // Try to interpret the term as UTF-8 text for analysis
        let analyzed_terms = if let Ok(text) = std::str::from_utf8(&entry.term) {
            let tokens = self.analyzer.analyze(text);
            tokens
                .into_iter()
                .map(|t| t.into_bytes())
                .collect::<Vec<_>>()
        } else {
            // Binary data: index as-is
            vec![entry.term.clone()]
        };

        let mut terms = self.terms.write();
        for term in analyzed_terms {
            let pl = terms.entry(term).or_default();
            pl.add(entry.partition_key.clone(), entry.clustering_key.clone());
        }
        Ok(())
    }

    fn delete(&self, entry: &IndexEntry) -> Result<(), IndexError> {
        let analyzed_terms = if let Ok(text) = std::str::from_utf8(&entry.term) {
            let tokens = self.analyzer.analyze(text);
            tokens
                .into_iter()
                .map(|t| t.into_bytes())
                .collect::<Vec<_>>()
        } else {
            vec![entry.term.clone()]
        };

        let mut terms = self.terms.write();
        for term in analyzed_terms {
            if let Some(pl) = terms.get_mut(&term) {
                pl.remove(&entry.partition_key, &entry.clustering_key);
                if pl.is_empty() {
                    terms.remove(&term);
                }
            }
        }
        Ok(())
    }

    fn search(&self, term: &[u8]) -> Result<Vec<IndexEntry>, IndexError> {
        // Analyze the search term the same way
        let search_terms = if let Ok(text) = std::str::from_utf8(term) {
            let tokens = self.analyzer.analyze(text);
            tokens
                .into_iter()
                .map(|t| t.into_bytes())
                .collect::<Vec<_>>()
        } else {
            vec![term.to_vec()]
        };

        let terms = self.terms.read();
        let mut results = Vec::new();

        for search_term in &search_terms {
            if let Some(pl) = terms.get(search_term) {
                for loc in pl.locations() {
                    results.push(IndexEntry {
                        term: search_term.clone(),
                        partition_key: loc.partition_key.clone(),
                        clustering_key: loc.clustering_key.clone(),
                    });
                }
            }
        }

        Ok(results)
    }

    fn range_search(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
    ) -> Result<Vec<IndexEntry>, IndexError> {
        let terms = self.terms.read();
        let mut results = Vec::new();

        let iter: Box<dyn Iterator<Item = (&Vec<u8>, &PostingList)>> = match (start, end) {
            (Some(s), Some(e)) => Box::new(terms.range(s.to_vec()..=e.to_vec())),
            (Some(s), None) => Box::new(terms.range(s.to_vec()..)),
            (None, Some(e)) => Box::new(terms.range(..=e.to_vec())),
            (None, None) => Box::new(terms.iter()),
        };

        for (term, pl) in iter {
            for loc in pl.locations() {
                results.push(IndexEntry {
                    term: term.clone(),
                    partition_key: loc.partition_key.clone(),
                    clustering_key: loc.clustering_key.clone(),
                });
            }
        }

        Ok(results)
    }

    fn truncate(&self) -> Result<(), IndexError> {
        self.terms.write().clear();
        Ok(())
    }

    fn definition(&self) -> &IndexDefinition {
        &self.definition
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn make_sasi_def(name: &str, column: &str) -> IndexDefinition {
        IndexDefinition {
            name: name.into(),
            keyspace: "ks".into(),
            table: "tbl".into(),
            column: column.into(),
            index_type: IndexType::Sasi,
            options: HashMap::new(),
        }
    }

    fn entry(term: &[u8], pk: &[u8], ck: &[u8]) -> IndexEntry {
        IndexEntry {
            term: term.to_vec(),
            partition_key: pk.to_vec(),
            clustering_key: ck.to_vec(),
        }
    }

    #[test]
    fn insert_and_search_exact() {
        let idx = SasiIndex::new(make_sasi_def("idx", "col"));
        idx.insert(&entry(b"hello", b"pk1", b"")).unwrap();

        let results = idx.search(b"hello").unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].partition_key, b"pk1");
    }

    #[test]
    fn standard_analyzer_tokenizes() {
        let idx = SasiIndex::new(make_sasi_def("idx", "col"));
        // Insert "Hello World" — standard analyzer splits into "hello", "world"
        idx.insert(&entry(b"Hello World", b"pk1", b"")).unwrap();

        // Searching for "hello" should find it
        let results = idx.search(b"hello").unwrap();
        assert_eq!(results.len(), 1);

        // Searching for "world" should find it too
        let results = idx.search(b"world").unwrap();
        assert_eq!(results.len(), 1);

        // Searching for the original exact term should also find it
        // (because the search term is also analyzed)
        let results = idx.search(b"Hello World").unwrap();
        assert_eq!(results.len(), 2); // "hello" and "world" both match
    }

    #[test]
    fn non_tokenizing_analyzer() {
        let mut opts = HashMap::new();
        opts.insert("analyzer_class".into(), "non_tokenizing".into());
        let def = IndexDefinition {
            name: "idx".into(),
            keyspace: "ks".into(),
            table: "tbl".into(),
            column: "col".into(),
            index_type: IndexType::Sasi,
            options: opts,
        };
        let idx = SasiIndex::new(def);
        idx.insert(&entry(b"Hello World", b"pk1", b"")).unwrap();

        // Non-tokenizing: stored as "hello world" (lowercase)
        let results = idx.search(b"hello world").unwrap();
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn delete_removes_entry() {
        let idx = SasiIndex::new(make_sasi_def("idx", "col"));
        idx.insert(&entry(b"term", b"pk1", b"")).unwrap();
        idx.insert(&entry(b"term", b"pk2", b"")).unwrap();

        idx.delete(&entry(b"term", b"pk1", b"")).unwrap();

        let results = idx.search(b"term").unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].partition_key, b"pk2");
    }

    #[test]
    fn range_search_works() {
        let idx = SasiIndex::new(make_sasi_def("idx", "col"));
        idx.insert(&entry(b"aaa", b"pk1", b"")).unwrap();
        idx.insert(&entry(b"bbb", b"pk2", b"")).unwrap();
        idx.insert(&entry(b"ccc", b"pk3", b"")).unwrap();

        let results = idx.range_search(Some(b"aaa"), Some(b"bbb")).unwrap();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn truncate_clears_all() {
        let idx = SasiIndex::new(make_sasi_def("idx", "col"));
        idx.insert(&entry(b"a", b"pk1", b"")).unwrap();
        idx.truncate().unwrap();
        assert_eq!(idx.term_count(), 0);
    }
}
