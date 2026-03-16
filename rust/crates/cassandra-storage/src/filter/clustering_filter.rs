// Licensed under Apache License, Version 2.0.

//! Clustering index filters: slice and names.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.db.filter.ClusteringIndexSliceFilter`
//! - `org.apache.cassandra.db.filter.ClusteringIndexNamesFilter`

use std::collections::BTreeSet;

/// A clustering bound for slice operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusteringBound {
    /// Clustering prefix bytes.
    pub values: Vec<u8>,
    /// Whether this bound is inclusive.
    pub inclusive: bool,
}

impl ClusteringBound {
    /// An unbounded start (beginning of partition).
    pub fn start_unbounded() -> Self {
        Self {
            values: Vec::new(),
            inclusive: true,
        }
    }

    /// An unbounded end (end of partition).
    pub fn end_unbounded() -> Self {
        Self {
            values: Vec::new(),
            inclusive: true,
        }
    }
}

/// A single slice: [start, end] with inclusivity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Slice {
    pub start: ClusteringBound,
    pub end: ClusteringBound,
}

impl Slice {
    /// Returns `true` if the given clustering key is selected by this slice.
    pub fn selects(&self, clustering_key: &[u8]) -> bool {
        let after_start = if self.start.values.is_empty() {
            true
        } else if self.start.inclusive {
            clustering_key >= self.start.values.as_slice()
        } else {
            clustering_key > self.start.values.as_slice()
        };

        let before_end = if self.end.values.is_empty() {
            true
        } else if self.end.inclusive {
            clustering_key <= self.end.values.as_slice()
        } else {
            clustering_key < self.end.values.as_slice()
        };

        after_start && before_end
    }
}

/// A collection of slices.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Slices {
    pub slices: Vec<Slice>,
    pub is_reversed: bool,
}

impl Slices {
    /// A single unbounded slice (select everything).
    pub fn all() -> Self {
        Self {
            slices: vec![Slice {
                start: ClusteringBound::start_unbounded(),
                end: ClusteringBound::end_unbounded(),
            }],
            is_reversed: false,
        }
    }

    /// Returns `true` if the given clustering key is selected by any slice.
    pub fn selects(&self, clustering_key: &[u8]) -> bool {
        self.slices.iter().any(|s| s.selects(clustering_key))
    }
}

/// Clustering index filter: determines which rows within a partition to read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClusteringIndexFilter {
    /// Select rows within one or more clustering key ranges.
    Slice(Slices),
    /// Select specific rows by clustering key.
    Names(BTreeSet<Vec<u8>>),
}

impl ClusteringIndexFilter {
    /// Returns `true` if the given clustering key is selected.
    pub fn selects(&self, clustering_key: &[u8]) -> bool {
        match self {
            Self::Slice(slices) => slices.selects(clustering_key),
            Self::Names(names) => names.contains(clustering_key),
        }
    }

    /// Returns `true` if iteration should be reversed.
    pub fn is_reversed(&self) -> bool {
        match self {
            Self::Slice(slices) => slices.is_reversed,
            Self::Names(_) => false,
        }
    }

    /// Get the slices (for Slice variant, or synthesize from Names).
    pub fn get_slices(&self) -> Slices {
        match self {
            Self::Slice(slices) => slices.clone(),
            Self::Names(names) => {
                let slices = names
                    .iter()
                    .map(|ck| Slice {
                        start: ClusteringBound {
                            values: ck.clone(),
                            inclusive: true,
                        },
                        end: ClusteringBound {
                            values: ck.clone(),
                            inclusive: true,
                        },
                    })
                    .collect();
                Slices {
                    slices,
                    is_reversed: false,
                }
            }
        }
    }

    /// Select all rows in a partition.
    pub fn all() -> Self {
        Self::Slice(Slices::all())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slice_unbounded_selects_all() {
        let filter = ClusteringIndexFilter::all();
        assert!(filter.selects(b"anything"));
        assert!(filter.selects(b""));
    }

    #[test]
    fn slice_bounded() {
        let filter = ClusteringIndexFilter::Slice(Slices {
            slices: vec![Slice {
                start: ClusteringBound {
                    values: b"b".to_vec(),
                    inclusive: true,
                },
                end: ClusteringBound {
                    values: b"d".to_vec(),
                    inclusive: false,
                },
            }],
            is_reversed: false,
        });
        assert!(!filter.selects(b"a"));
        assert!(filter.selects(b"b")); // inclusive start
        assert!(filter.selects(b"c"));
        assert!(!filter.selects(b"d")); // exclusive end
        assert!(!filter.selects(b"e"));
    }

    #[test]
    fn names_filter() {
        let mut names = BTreeSet::new();
        names.insert(b"ck1".to_vec());
        names.insert(b"ck3".to_vec());
        let filter = ClusteringIndexFilter::Names(names);

        assert!(filter.selects(b"ck1"));
        assert!(!filter.selects(b"ck2"));
        assert!(filter.selects(b"ck3"));
        assert!(!filter.is_reversed());
    }

    #[test]
    fn reversed_slice() {
        let filter = ClusteringIndexFilter::Slice(Slices {
            slices: vec![Slice {
                start: ClusteringBound::start_unbounded(),
                end: ClusteringBound::end_unbounded(),
            }],
            is_reversed: true,
        });
        assert!(filter.is_reversed());
    }

    #[test]
    fn multiple_slices() {
        let filter = ClusteringIndexFilter::Slice(Slices {
            slices: vec![
                Slice {
                    start: ClusteringBound {
                        values: b"a".to_vec(),
                        inclusive: true,
                    },
                    end: ClusteringBound {
                        values: b"b".to_vec(),
                        inclusive: true,
                    },
                },
                Slice {
                    start: ClusteringBound {
                        values: b"d".to_vec(),
                        inclusive: true,
                    },
                    end: ClusteringBound {
                        values: b"e".to_vec(),
                        inclusive: true,
                    },
                },
            ],
            is_reversed: false,
        });
        assert!(filter.selects(b"a"));
        assert!(filter.selects(b"b"));
        assert!(!filter.selects(b"c"));
        assert!(filter.selects(b"d"));
    }

    #[test]
    fn names_get_slices() {
        let mut names = BTreeSet::new();
        names.insert(b"ck1".to_vec());
        let filter = ClusteringIndexFilter::Names(names);
        let slices = filter.get_slices();
        assert_eq!(slices.slices.len(), 1);
        assert!(slices.slices[0].selects(b"ck1"));
        assert!(!slices.slices[0].selects(b"ck2"));
    }
}
