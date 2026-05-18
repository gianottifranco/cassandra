// Licensed under Apache License, Version 2.0.

//! Differential checks for secondary index backends.

use super::{IndexEntry, IndexError, SecondaryIndex};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IndexDifferentialOp {
    Insert(IndexEntry),
    Delete(IndexEntry),
    Search(Vec<u8>),
    RangeSearch {
        start: Option<Vec<u8>>,
        end: Option<Vec<u8>>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexDifferentialObservation {
    pub op_index: usize,
    pub term: Vec<u8>,
    pub left_results: Vec<IndexEntry>,
    pub right_results: Vec<IndexEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExpectedIndexObservation {
    pub op_index: usize,
    pub term: Vec<u8>,
    pub results: Vec<IndexEntry>,
}

pub fn compare_index_backends(
    left: &dyn SecondaryIndex,
    right: &dyn SecondaryIndex,
    ops: &[IndexDifferentialOp],
) -> Result<Vec<IndexDifferentialObservation>, IndexError> {
    let mut observations = Vec::new();
    for (op_index, op) in ops.iter().enumerate() {
        match op {
            IndexDifferentialOp::Insert(entry) => {
                left.insert(entry)?;
                right.insert(entry)?;
            }
            IndexDifferentialOp::Delete(entry) => {
                left.delete(entry)?;
                right.delete(entry)?;
            }
            IndexDifferentialOp::Search(term) => {
                let left_results = canonical_results(left.search(term)?);
                let right_results = canonical_results(right.search(term)?);
                observations.push(IndexDifferentialObservation {
                    op_index,
                    term: term.clone(),
                    left_results,
                    right_results,
                });
            }
            IndexDifferentialOp::RangeSearch { start, end } => {
                let left_results =
                    canonical_results(left.range_search(start.as_deref(), end.as_deref())?);
                let right_results =
                    canonical_results(right.range_search(start.as_deref(), end.as_deref())?);
                observations.push(IndexDifferentialObservation {
                    op_index,
                    term: range_observation_key(start.as_deref(), end.as_deref()),
                    left_results,
                    right_results,
                });
            }
        }
    }
    Ok(observations)
}

pub fn observations_match(observations: &[IndexDifferentialObservation]) -> bool {
    observations
        .iter()
        .all(|obs| obs.left_results == obs.right_results)
}

pub fn observations_match_expected(
    observations: &[IndexDifferentialObservation],
    expected: &[ExpectedIndexObservation],
) -> bool {
    if observations.len() != expected.len() {
        return false;
    }

    observations.iter().zip(expected.iter()).all(|(obs, exp)| {
        let expected_results = canonical_results(exp.results.clone());
        obs.op_index == exp.op_index
            && obs.term == exp.term
            && obs.left_results == expected_results
            && obs.right_results == expected_results
    })
}

fn canonical_results(results: Vec<IndexEntry>) -> Vec<IndexEntry> {
    let mut results = results;
    results.sort_by(|a, b| {
        (&a.term, &a.partition_key, &a.clustering_key).cmp(&(
            &b.term,
            &b.partition_key,
            &b.clustering_key,
        ))
    });
    results
}

pub fn range_observation_key(start: Option<&[u8]>, end: Option<&[u8]>) -> Vec<u8> {
    let mut key = Vec::new();
    if let Some(start) = start {
        key.extend_from_slice(start);
    }
    key.push(0xff);
    if let Some(end) = end {
        key.extend_from_slice(end);
    }
    key
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::legacy::LegacyIndex;
    use crate::index::sai::SaiIndex;

    fn entry(term: &[u8], pk: &[u8]) -> IndexEntry {
        IndexEntry {
            term: term.to_vec(),
            partition_key: pk.to_vec(),
            clustering_key: Vec::new(),
        }
    }

    #[test]
    fn compares_legacy_and_sai_exact_searches() {
        let legacy = LegacyIndex::create("idx_legacy", "ks", "users", "email");
        let sai = SaiIndex::create("idx_sai", "ks", "users", "email");
        let observations = compare_index_backends(
            &legacy,
            &sai,
            &[
                IndexDifferentialOp::Insert(entry(b"a@example.com", b"pk1")),
                IndexDifferentialOp::Insert(entry(b"b@example.com", b"pk2")),
                IndexDifferentialOp::Insert(entry(b"c@example.com", b"pk3")),
                IndexDifferentialOp::Search(b"a@example.com".to_vec()),
                IndexDifferentialOp::RangeSearch {
                    start: Some(b"b@example.com".to_vec()),
                    end: Some(b"c@example.com".to_vec()),
                },
                IndexDifferentialOp::Delete(entry(b"a@example.com", b"pk1")),
                IndexDifferentialOp::Search(b"a@example.com".to_vec()),
            ],
        )
        .unwrap();

        assert!(observations_match(&observations));
        assert_eq!(observations[0].left_results.len(), 1);
        assert_eq!(observations[1].left_results.len(), 2);
        assert!(observations[2].left_results.is_empty());
        assert!(observations_match_expected(
            &observations,
            &[
                ExpectedIndexObservation {
                    op_index: 3,
                    term: b"a@example.com".to_vec(),
                    results: vec![entry(b"a@example.com", b"pk1")],
                },
                ExpectedIndexObservation {
                    op_index: 4,
                    term: range_observation_key(
                        Some(b"b@example.com".as_slice()),
                        Some(b"c@example.com".as_slice()),
                    ),
                    results: vec![
                        entry(b"b@example.com", b"pk2"),
                        entry(b"c@example.com", b"pk3"),
                    ],
                },
                ExpectedIndexObservation {
                    op_index: 6,
                    term: b"a@example.com".to_vec(),
                    results: vec![],
                },
            ],
        ));
    }
}
