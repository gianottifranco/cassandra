// Licensed under Apache License, Version 2.0.

//! Clustering key representation and bounds.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.db.ClusteringPrefix`
//! - `org.apache.cassandra.db.ClusteringBound`

use std::cmp::Ordering;
use std::fmt;
use serde::{Deserialize, Serialize};

/// The kind of clustering bound (for range queries and tombstones).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ClusteringBoundKind {
    /// Inclusive start: `>=`
    InclusiveStart,
    /// Exclusive start: `>`
    ExclusiveStart,
    /// Inclusive end: `<=`
    InclusiveEnd,
    /// Exclusive end: `<`
    ExclusiveEnd,
}

impl ClusteringBoundKind {
    /// Returns `true` if this is a start bound.
    pub fn is_start(&self) -> bool {
        matches!(self, Self::InclusiveStart | Self::ExclusiveStart)
    }
    /// Returns `true` if this is an end bound.
    pub fn is_end(&self) -> bool {
        matches!(self, Self::InclusiveEnd | Self::ExclusiveEnd)
    }
    /// Returns `true` if this bound is inclusive.
    pub fn is_inclusive(&self) -> bool {
        matches!(self, Self::InclusiveStart | Self::InclusiveEnd)
    }
}

/// A clustering key composed of one or more column values.
///
/// Each value is stored as serialized bytes. The number of values
/// may be fewer than the total clustering columns (prefix query).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ClusteringKey {
    /// Serialized values for each clustering column (in order).
    pub values: Vec<Vec<u8>>,
}

impl ClusteringKey {
    /// Create a new clustering key from serialized column values.
    pub fn new(values: Vec<Vec<u8>>) -> Self {
        Self { values }
    }

    /// Number of clustering columns present in this key.
    pub fn size(&self) -> usize {
        self.values.len()
    }

    /// Returns `true` if this is an empty (minimum) clustering.
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Static "bottom" clustering key (sorts before everything).
    pub fn bottom() -> Self {
        Self { values: vec![] }
    }
}

impl fmt::Display for ClusteringKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CK[")?;
        for (i, v) in self.values.iter().enumerate() {
            if i > 0 { write!(f, ":")?; }
            write!(f, "{} bytes", v.len())?;
        }
        write!(f, "]")
    }
}

/// A clustering bound used for range scans and tombstones.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ClusteringBound {
    pub kind: ClusteringBoundKind,
    pub key: ClusteringKey,
}

impl ClusteringBound {
    pub fn inclusive_start(key: ClusteringKey) -> Self {
        Self { kind: ClusteringBoundKind::InclusiveStart, key }
    }
    pub fn exclusive_start(key: ClusteringKey) -> Self {
        Self { kind: ClusteringBoundKind::ExclusiveStart, key }
    }
    pub fn inclusive_end(key: ClusteringKey) -> Self {
        Self { kind: ClusteringBoundKind::InclusiveEnd, key }
    }
    pub fn exclusive_end(key: ClusteringKey) -> Self {
        Self { kind: ClusteringBoundKind::ExclusiveEnd, key }
    }
}

/// Compare two clustering keys using the given comparator functions.
///
/// `comparators` is a slice of comparison functions, one per clustering column.
pub fn compare_clustering_keys(
    left: &ClusteringKey,
    right: &ClusteringKey,
    comparators: &[fn(&[u8], &[u8]) -> Ordering],
) -> Ordering {
    let len = left.values.len().min(right.values.len()).min(comparators.len());
    for i in 0..len {
        match comparators[i](&left.values[i], &right.values[i]) {
            Ordering::Equal => continue,
            other => return other,
        }
    }
    left.values.len().cmp(&right.values.len())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clustering_key_size() {
        let ck = ClusteringKey::new(vec![vec![1], vec![2, 3]]);
        assert_eq!(ck.size(), 2);
    }

    #[test]
    fn bottom_is_empty() {
        assert!(ClusteringKey::bottom().is_empty());
    }

    #[test]
    fn bound_kinds() {
        let b = ClusteringBound::inclusive_start(ClusteringKey::bottom());
        assert!(b.kind.is_start());
        assert!(b.kind.is_inclusive());
    }

    #[test]
    fn compare_clustering() {
        let a = ClusteringKey::new(vec![vec![0, 1]]);
        let b = ClusteringKey::new(vec![vec![0, 2]]);
        let comps: Vec<fn(&[u8], &[u8]) -> Ordering> = vec![|a, b| a.cmp(b)];
        assert_eq!(compare_clustering_keys(&a, &b, &comps), Ordering::Less);
    }

    #[test]
    fn prefix_comparison() {
        let short = ClusteringKey::new(vec![vec![0, 1]]);
        let long = ClusteringKey::new(vec![vec![0, 1], vec![0, 2]]);
        let comps: Vec<fn(&[u8], &[u8]) -> Ordering> = vec![|a, b| a.cmp(b), |a, b| a.cmp(b)];
        assert_eq!(compare_clustering_keys(&short, &long, &comps), Ordering::Less);
    }
}
