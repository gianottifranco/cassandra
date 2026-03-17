// Licensed under Apache License, Version 2.0.

//! Index summary for fast partition lookup in Big-format SSTables.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.io.sstable.IndexSummary`
//! - `org.apache.cassandra.io.sstable.IndexSummaryBuilder`
//!
//! The summary is a sampled subset of the partition index. When looking up a
//! partition key, the summary narrows the search window in Index.db from the
//! full index down to a ~128-entry span, avoiding a full binary search.
//!
//! ## Summary.db Format
//!
//! ```text
//! [count: u32]
//! For each entry:
//!   [pk_len: u32][partition_key: bytes][index_offset: u64]
//! ```

use std::fs::File;
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::Path;

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

/// One sampled entry in the index summary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SummaryEntry {
    pub partition_key: Vec<u8>,
    pub index_offset: u64,
}

/// Result of a summary search: the index byte range to scan in Index.db.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SummarySearchResult {
    /// Start position (entry index, inclusive) in the summary.
    pub index_start: usize,
    /// End position (entry index, exclusive) in the summary.
    pub index_end: usize,
}

/// In-memory index summary loaded from Summary.db.
#[derive(Debug, Clone)]
pub struct IndexSummary {
    entries: Vec<SummaryEntry>,
}

impl IndexSummary {
    /// Create an empty summary.
    pub fn empty() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    /// Create from a pre-built list of entries.
    pub fn from_entries(entries: Vec<SummaryEntry>) -> Self {
        Self { entries }
    }

    /// Load a summary from a Summary.db file on disk.
    ///
    /// The file format matches what [`SSTableWriter`] writes:
    /// `[count: u32]` followed by per-entry `[pk_len: u32][pk][offset: u64]`.
    pub fn load(path: &Path) -> io::Result<Self> {
        let file = File::open(path)?;
        let mut reader = BufReader::new(file);

        let count = reader.read_u32::<BigEndian>()? as usize;
        let mut entries = Vec::with_capacity(count);

        for _ in 0..count {
            let pk_len = reader.read_u32::<BigEndian>()? as usize;
            let mut partition_key = vec![0u8; pk_len];
            reader.read_exact(&mut partition_key)?;
            let index_offset = reader.read_u64::<BigEndian>()?;
            entries.push(SummaryEntry {
                partition_key,
                index_offset,
            });
        }

        Ok(Self { entries })
    }

    /// Serialize entries to a writer in Summary.db format.
    pub fn serialize<W: Write>(entries: &[SummaryEntry], w: &mut W) -> io::Result<()> {
        let mut bw = BufWriter::new(w);
        bw.write_u32::<BigEndian>(entries.len() as u32)?;
        for entry in entries {
            bw.write_u32::<BigEndian>(entry.partition_key.len() as u32)?;
            bw.write_all(&entry.partition_key)?;
            bw.write_u64::<BigEndian>(entry.index_offset)?;
        }
        bw.flush()?;
        Ok(())
    }

    /// Search the summary for the span that may contain `partition_key`.
    ///
    /// Returns a [`SummarySearchResult`] whose `index_start` and `index_end`
    /// identify the two bounding summary entries. The caller should then
    /// binary-search Index.db between those offsets.
    ///
    /// For an empty summary the result covers the full range `[0, 0)`.
    pub fn search(&self, partition_key: &[u8]) -> SummarySearchResult {
        if self.entries.is_empty() {
            return SummarySearchResult {
                index_start: 0,
                index_end: 0,
            };
        }

        // Binary search: find the rightmost entry whose key <= partition_key.
        let pos = self
            .entries
            .partition_point(|e| e.partition_key.as_slice() <= partition_key);

        // `pos` is the first entry whose key > partition_key.
        // The window is [pos-1, pos], clamped to valid bounds.
        let start = if pos == 0 { 0 } else { pos - 1 };
        let end = pos.min(self.entries.len());

        SummarySearchResult {
            index_start: start,
            index_end: end,
        }
    }

    /// Number of entries in the summary.
    pub fn entry_count(&self) -> usize {
        self.entries.len()
    }

    /// The smallest partition key in the summary, if any.
    pub fn min_key(&self) -> Option<&[u8]> {
        self.entries.first().map(|e| e.partition_key.as_slice())
    }

    /// The largest partition key in the summary, if any.
    pub fn max_key(&self) -> Option<&[u8]> {
        self.entries.last().map(|e| e.partition_key.as_slice())
    }

    /// Access the underlying entries slice.
    pub fn entries(&self) -> &[SummaryEntry] {
        &self.entries
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    /// Build a summary, write it, reload it, and verify contents.
    #[test]
    fn write_and_load_round_trip() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("Summary.db");

        let entries = vec![
            SummaryEntry {
                partition_key: vec![0x01],
                index_offset: 0,
            },
            SummaryEntry {
                partition_key: vec![0x10],
                index_offset: 100,
            },
            SummaryEntry {
                partition_key: vec![0x20],
                index_offset: 200,
            },
        ];

        // Write
        let mut file = File::create(&path).unwrap();
        IndexSummary::serialize(&entries, &mut file).unwrap();

        // Load
        let summary = IndexSummary::load(&path).unwrap();
        assert_eq!(summary.entry_count(), 3);
        assert_eq!(summary.entries(), &entries);
    }

    #[test]
    fn search_narrows_range() {
        let entries = vec![
            SummaryEntry {
                partition_key: vec![0x00],
                index_offset: 0,
            },
            SummaryEntry {
                partition_key: vec![0x10],
                index_offset: 100,
            },
            SummaryEntry {
                partition_key: vec![0x20],
                index_offset: 200,
            },
            SummaryEntry {
                partition_key: vec![0x30],
                index_offset: 300,
            },
        ];
        let summary = IndexSummary::from_entries(entries);

        // Key between 0x10 and 0x20 should land in window [1, 2].
        let result = summary.search(&[0x15]);
        assert_eq!(result.index_start, 1);
        assert_eq!(result.index_end, 2);

        // Key exactly at 0x10 should land in window [1, 2]
        // (partition_point finds first entry > key, so pos=2, start=1).
        let result = summary.search(&[0x10]);
        assert_eq!(result.index_start, 1);
        assert_eq!(result.index_end, 2);
    }

    #[test]
    fn search_before_first_key() {
        let entries = vec![
            SummaryEntry {
                partition_key: vec![0x10],
                index_offset: 100,
            },
            SummaryEntry {
                partition_key: vec![0x20],
                index_offset: 200,
            },
        ];
        let summary = IndexSummary::from_entries(entries);

        let result = summary.search(&[0x05]);
        assert_eq!(result.index_start, 0);
        assert_eq!(result.index_end, 0);
    }

    #[test]
    fn search_after_last_key() {
        let entries = vec![
            SummaryEntry {
                partition_key: vec![0x10],
                index_offset: 100,
            },
            SummaryEntry {
                partition_key: vec![0x20],
                index_offset: 200,
            },
        ];
        let summary = IndexSummary::from_entries(entries);

        let result = summary.search(&[0xFF]);
        assert_eq!(result.index_start, 1);
        assert_eq!(result.index_end, 2);
    }

    #[test]
    fn empty_summary() {
        let summary = IndexSummary::empty();
        assert_eq!(summary.entry_count(), 0);
        assert!(summary.min_key().is_none());
        assert!(summary.max_key().is_none());

        let result = summary.search(&[0x01]);
        assert_eq!(result.index_start, 0);
        assert_eq!(result.index_end, 0);
    }

    #[test]
    fn min_max_keys() {
        let entries = vec![
            SummaryEntry {
                partition_key: vec![0x01, 0x02],
                index_offset: 0,
            },
            SummaryEntry {
                partition_key: vec![0xFF, 0xFE],
                index_offset: 999,
            },
        ];
        let summary = IndexSummary::from_entries(entries);
        assert_eq!(summary.min_key(), Some(&[0x01, 0x02][..]));
        assert_eq!(summary.max_key(), Some(&[0xFF, 0xFE][..]));
    }

    #[test]
    fn load_from_writer_format() {
        // Manually build a Summary.db in the exact format SSTableWriter uses.
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("Summary.db");

        let mut file = File::create(&path).unwrap();
        // count = 2
        file.write_u32::<BigEndian>(2).unwrap();
        // entry 1: pk = [0xAA], offset = 42
        file.write_u32::<BigEndian>(1).unwrap();
        file.write_all(&[0xAA]).unwrap();
        file.write_u64::<BigEndian>(42).unwrap();
        // entry 2: pk = [0xBB, 0xCC], offset = 128
        file.write_u32::<BigEndian>(2).unwrap();
        file.write_all(&[0xBB, 0xCC]).unwrap();
        file.write_u64::<BigEndian>(128).unwrap();
        drop(file);

        let summary = IndexSummary::load(&path).unwrap();
        assert_eq!(summary.entry_count(), 2);
        assert_eq!(summary.entries()[0].partition_key, vec![0xAA]);
        assert_eq!(summary.entries()[0].index_offset, 42);
        assert_eq!(summary.entries()[1].partition_key, vec![0xBB, 0xCC]);
        assert_eq!(summary.entries()[1].index_offset, 128);
    }
}
