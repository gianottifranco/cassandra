// Licensed under Apache License, Version 2.0.

//! SAI query execution: filtering, intersection, and result assembly.
//!
//! ## Java Oracle
//!
//! `org.apache.cassandra.index.sai.plan.QueryController`
//! `org.apache.cassandra.index.sai.plan.Expression`
//!
//! ## Design
//!
//! A SAI query evaluates one or more predicates against the index,
//! combines results via intersection (AND) or union (OR), and returns
//! base-table row locations for the coordinator to fetch.

use super::SaiIndex;
use super::posting::PostingList;
use crate::index::{IndexEntry, IndexError, SecondaryIndex};

/// A predicate on an indexed column.
#[derive(Debug, Clone)]
pub enum SaiPredicate {
    /// Exact equality: `column = value`.
    Eq(Vec<u8>),
    /// Range: `column >= start AND column <= end`.
    Range {
        start: Option<Vec<u8>>,
        end: Option<Vec<u8>>,
    },
}

/// A composed SAI query.
#[derive(Debug)]
pub struct SaiQuery {
    pub predicates: Vec<SaiPredicate>,
    /// How to combine predicates (AND vs OR).
    pub conjunction: Conjunction,
}

/// How to combine multiple predicates.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Conjunction {
    And,
    Or,
}

impl SaiQuery {
    pub fn eq(term: Vec<u8>) -> Self {
        Self {
            predicates: vec![SaiPredicate::Eq(term)],
            conjunction: Conjunction::And,
        }
    }

    pub fn range(start: Option<Vec<u8>>, end: Option<Vec<u8>>) -> Self {
        Self {
            predicates: vec![SaiPredicate::Range { start, end }],
            conjunction: Conjunction::And,
        }
    }
}

/// Execute a SAI query against an index.
pub fn execute_query(index: &SaiIndex, query: &SaiQuery) -> Result<Vec<IndexEntry>, IndexError> {
    let mut posting_lists: Vec<PostingList> = Vec::new();

    for predicate in &query.predicates {
        let entries = match predicate {
            SaiPredicate::Eq(term) => index.search(term)?,
            SaiPredicate::Range { start, end } => {
                index.range_search(start.as_deref(), end.as_deref())?
            }
        };

        // Convert entries to a posting list
        let mut pl = PostingList::new();
        for entry in &entries {
            pl.add(entry.partition_key.clone(), entry.clustering_key.clone());
        }
        posting_lists.push(pl);
    }

    if posting_lists.is_empty() {
        return Ok(Vec::new());
    }

    // Combine posting lists
    let combined = match query.conjunction {
        Conjunction::And => {
            let mut result = posting_lists.remove(0);
            for pl in &posting_lists {
                result = result.intersect(pl);
            }
            result
        }
        Conjunction::Or => {
            let mut result = posting_lists.remove(0);
            for pl in &posting_lists {
                result.merge(pl);
            }
            result
        }
    };

    // Convert back to IndexEntry
    let results = combined
        .locations()
        .iter()
        .map(|loc| IndexEntry {
            term: Vec::new(), // Combined results don't have a single term
            partition_key: loc.partition_key.clone(),
            clustering_key: loc.clustering_key.clone(),
        })
        .collect();

    Ok(results)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn setup_index() -> SaiIndex {
        let idx = SaiIndex::create("sai_test", "ks", "t", "col");
        // Insert entries: terms a, b, c with various PKs
        let _ = idx.insert(&IndexEntry {
            term: b"a".to_vec(),
            partition_key: b"pk1".to_vec(),
            clustering_key: b"".to_vec(),
        });
        let _ = idx.insert(&IndexEntry {
            term: b"b".to_vec(),
            partition_key: b"pk1".to_vec(),
            clustering_key: b"".to_vec(),
        });
        let _ = idx.insert(&IndexEntry {
            term: b"b".to_vec(),
            partition_key: b"pk2".to_vec(),
            clustering_key: b"".to_vec(),
        });
        let _ = idx.insert(&IndexEntry {
            term: b"c".to_vec(),
            partition_key: b"pk2".to_vec(),
            clustering_key: b"".to_vec(),
        });
        idx
    }

    #[test]
    fn eq_query() {
        let idx = setup_index();
        let query = SaiQuery::eq(b"b".to_vec());
        let results = execute_query(&idx, &query).unwrap();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn range_query() {
        let idx = setup_index();
        let query = SaiQuery::range(Some(b"a".to_vec()), Some(b"b".to_vec()));
        let results = execute_query(&idx, &query).unwrap();
        // a→pk1, b→pk1, b→pk2 = 3 entries, but pk1 appears in both, union gives pk1, pk2
        assert!(results.len() >= 2);
    }

    #[test]
    fn empty_query() {
        let idx = setup_index();
        let query = SaiQuery::eq(b"nonexistent".to_vec());
        let results = execute_query(&idx, &query).unwrap();
        assert!(results.is_empty());
    }
}
