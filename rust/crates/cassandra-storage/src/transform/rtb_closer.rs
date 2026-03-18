// Licensed under Apache License, Version 2.0.

//! Range tombstone bound closer: ensures open/close markers are paired.
//!
//! ## Java Oracle
//! `org.apache.cassandra.db.rows.UnfilteredRowIterators.RTBoundCloser`

use cassandra_common::tombstone::DeletionTime;

use super::transformation::Transformation;
use crate::rows::unfiltered::{ClusteringBound, ClusteringBoundKind, RangeTombstoneMarker};

/// Ensures range tombstone markers are properly paired.
///
/// When iteration is truncated (e.g., by limits or paging), an open
/// range tombstone marker might not have a matching close. This transform
/// tracks open/close state and can emit a closing marker when needed.
pub struct RTBoundCloser {
    /// The deletion of the currently open range tombstone, if any.
    open_deletion: Option<DeletionTime>,
    /// The last marker's bound values (for synthesizing close).
    last_bound_values: Vec<u8>,
}

impl RTBoundCloser {
    pub fn new() -> Self {
        Self {
            open_deletion: None,
            last_bound_values: Vec::new(),
        }
    }

    /// Returns `true` if there's an unclosed range tombstone.
    pub fn has_open_marker(&self) -> bool {
        self.open_deletion.is_some()
    }

    /// Generate a close marker for the currently open range tombstone.
    pub fn close_marker(&self) -> Option<RangeTombstoneMarker> {
        self.open_deletion
            .map(|deletion| RangeTombstoneMarker::Close {
                bound: ClusteringBound {
                    kind: ClusteringBoundKind::InclusiveEnd,
                    values: self.last_bound_values.clone(),
                },
                deletion,
            })
    }
}

impl Default for RTBoundCloser {
    fn default() -> Self {
        Self::new()
    }
}

impl Transformation for RTBoundCloser {
    fn apply_to_marker(&mut self, marker: RangeTombstoneMarker) -> Option<RangeTombstoneMarker> {
        match &marker {
            RangeTombstoneMarker::Open {
                deletion, bound, ..
            } => {
                self.open_deletion = Some(*deletion);
                self.last_bound_values = bound.values.clone();
            }
            RangeTombstoneMarker::Close { .. } => {
                self.open_deletion = None;
            }
            RangeTombstoneMarker::Boundary {
                open_deletion,
                bound,
                ..
            } => {
                self.open_deletion = Some(*open_deletion);
                self.last_bound_values = bound.values.clone();
            }
        }
        Some(marker)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn open_marker(values: Vec<u8>, ts: i64) -> RangeTombstoneMarker {
        RangeTombstoneMarker::Open {
            bound: ClusteringBound {
                kind: ClusteringBoundKind::InclusiveStart,
                values,
            },
            deletion: DeletionTime::new(ts, ts as i32),
        }
    }

    fn close_marker(values: Vec<u8>, ts: i64) -> RangeTombstoneMarker {
        RangeTombstoneMarker::Close {
            bound: ClusteringBound {
                kind: ClusteringBoundKind::InclusiveEnd,
                values,
            },
            deletion: DeletionTime::new(ts, ts as i32),
        }
    }

    #[test]
    fn no_open_marker_initially() {
        let closer = RTBoundCloser::new();
        assert!(!closer.has_open_marker());
        assert!(closer.close_marker().is_none());
    }

    #[test]
    fn tracks_open_marker() {
        let mut closer = RTBoundCloser::new();
        closer.apply_to_marker(open_marker(vec![1], 100));
        assert!(closer.has_open_marker());
    }

    #[test]
    fn clears_on_close() {
        let mut closer = RTBoundCloser::new();
        closer.apply_to_marker(open_marker(vec![1], 100));
        closer.apply_to_marker(close_marker(vec![5], 100));
        assert!(!closer.has_open_marker());
    }

    #[test]
    fn generates_close_marker() {
        let mut closer = RTBoundCloser::new();
        closer.apply_to_marker(open_marker(vec![1, 2, 3], 100));
        let close = closer.close_marker().unwrap();
        if let RangeTombstoneMarker::Close { bound, deletion } = close {
            assert_eq!(bound.values, vec![1, 2, 3]);
            assert_eq!(deletion.marked_for_delete_at, 100);
        } else {
            panic!("expected close marker");
        }
    }

    #[test]
    fn boundary_keeps_open() {
        let mut closer = RTBoundCloser::new();
        let boundary = RangeTombstoneMarker::Boundary {
            bound: ClusteringBound {
                kind: ClusteringBoundKind::InclusiveEnd,
                values: vec![3],
            },
            close_deletion: DeletionTime::new(100, 100),
            open_deletion: DeletionTime::new(200, 200),
        };
        closer.apply_to_marker(boundary);
        assert!(closer.has_open_marker());
    }
}
