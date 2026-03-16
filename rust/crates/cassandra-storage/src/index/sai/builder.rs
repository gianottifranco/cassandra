// Licensed under Apache License, Version 2.0.

//! SAI index builder: constructs index segments from SSTable data.
//!
//! ## Java Oracle
//!
//! `org.apache.cassandra.index.sai.disk.v1.SSTableIndexWriter`
//!
//! ## Design
//!
//! The builder accumulates (term, partition_key, clustering_key) entries
//! during SSTable flush or compaction, then produces a `SaiSegment` that
//! can be associated with the resulting SSTable.
//!
//! ## Current Limitations
//!
//! - Produces in-memory segments only (no on-disk persistence yet)
//! - No columnar encoding or compression
//! - No vector-specific segment format (HNSW graph)

use serde::{Deserialize, Serialize};

use super::posting::PostingList;
use std::collections::BTreeMap;

/// A built SAI segment — the index data associated with one SSTable.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SaiSegment {
    /// The SSTable generation this segment belongs to.
    pub sstable_generation: u64,
    /// Index name.
    pub index_name: String,
    /// Column name indexed.
    pub column: String,
    /// term → posting list
    pub terms: BTreeMap<Vec<u8>, PostingList>,
    /// Total number of indexed rows.
    pub row_count: u64,
}

/// Builder for accumulating index entries during flush/compaction.
#[derive(Debug)]
pub struct SaiSegmentBuilder {
    sstable_generation: u64,
    index_name: String,
    column: String,
    terms: BTreeMap<Vec<u8>, PostingList>,
    row_count: u64,
}

impl SaiSegmentBuilder {
    pub fn new(sstable_generation: u64, index_name: &str, column: &str) -> Self {
        Self {
            sstable_generation,
            index_name: index_name.to_string(),
            column: column.to_string(),
            terms: BTreeMap::new(),
            row_count: 0,
        }
    }

    /// Add an entry to the builder.
    pub fn add(&mut self, term: Vec<u8>, partition_key: Vec<u8>, clustering_key: Vec<u8>) {
        let pl = self.terms.entry(term).or_default();
        pl.add(partition_key, clustering_key);
        self.row_count += 1;
    }

    /// Build the final segment.
    pub fn build(self) -> SaiSegment {
        SaiSegment {
            sstable_generation: self.sstable_generation,
            index_name: self.index_name,
            column: self.column,
            terms: self.terms,
            row_count: self.row_count,
        }
    }

    /// Number of entries added so far.
    pub fn entry_count(&self) -> u64 {
        self.row_count
    }

    /// Number of distinct terms.
    pub fn term_count(&self) -> usize {
        self.terms.len()
    }
}

/// Merge multiple SAI segments (e.g., during compaction).
pub fn merge_segments(segments: &[SaiSegment], new_generation: u64) -> SaiSegment {
    let index_name = segments
        .first()
        .map(|s| s.index_name.clone())
        .unwrap_or_default();
    let column = segments
        .first()
        .map(|s| s.column.clone())
        .unwrap_or_default();

    let mut merged_terms: BTreeMap<Vec<u8>, PostingList> = BTreeMap::new();
    let mut total_rows = 0u64;

    for segment in segments {
        for (term, pl) in &segment.terms {
            let merged_pl = merged_terms.entry(term.clone()).or_default();
            merged_pl.merge(pl);
        }
        total_rows += segment.row_count;
    }

    SaiSegment {
        sstable_generation: new_generation,
        index_name,
        column,
        terms: merged_terms,
        row_count: total_rows,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_segment() {
        let mut builder = SaiSegmentBuilder::new(1, "idx_age", "age");
        builder.add(b"\x19".to_vec(), b"pk1".to_vec(), b"".to_vec());
        builder.add(b"\x1e".to_vec(), b"pk2".to_vec(), b"".to_vec());
        builder.add(b"\x19".to_vec(), b"pk3".to_vec(), b"".to_vec());

        let segment = builder.build();
        assert_eq!(segment.sstable_generation, 1);
        assert_eq!(segment.terms.len(), 2); // two distinct terms
        assert_eq!(segment.row_count, 3);
        assert_eq!(segment.terms[b"\x19".as_slice()].len(), 2); // pk1, pk3
    }

    #[test]
    fn merge_segments_union() {
        let mut b1 = SaiSegmentBuilder::new(1, "idx", "col");
        b1.add(b"a".to_vec(), b"pk1".to_vec(), b"".to_vec());

        let mut b2 = SaiSegmentBuilder::new(2, "idx", "col");
        b2.add(b"a".to_vec(), b"pk2".to_vec(), b"".to_vec());
        b2.add(b"b".to_vec(), b"pk3".to_vec(), b"".to_vec());

        let s1 = b1.build();
        let s2 = b2.build();

        let merged = merge_segments(&[s1, s2], 3);
        assert_eq!(merged.sstable_generation, 3);
        assert_eq!(merged.terms.len(), 2); // a, b
        assert_eq!(merged.terms[b"a".as_slice()].len(), 2); // pk1, pk2
        assert_eq!(merged.terms[b"b".as_slice()].len(), 1); // pk3
    }

    #[test]
    fn builder_counts() {
        let mut builder = SaiSegmentBuilder::new(1, "idx", "col");
        assert_eq!(builder.entry_count(), 0);
        assert_eq!(builder.term_count(), 0);

        builder.add(b"term".to_vec(), b"pk".to_vec(), b"ck".to_vec());
        assert_eq!(builder.entry_count(), 1);
        assert_eq!(builder.term_count(), 1);
    }
}
