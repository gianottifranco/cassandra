// Licensed under Apache License, Version 2.0.

//! Reverse scanner: reads partitions in descending key order.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.io.sstable.format.big.BigTableScanner`
//! - `org.apache.cassandra.db.columniterator.SSTableReversedIterator`
//!
//! Strategy: loads the full index from Index.db, iterates entries in reverse,
//! and seeks Data.db to each partition offset to read it.

use std::fs::File;
use std::io::{self, BufReader, Read, Seek, SeekFrom};
use std::ops::Bound;

use byteorder::{BigEndian, ReadBytesExt};

use super::format::*;
use super::reader::read_row_from_reader;
use super::tombstone_serializer::{
    PARTITION_DELETION_MARKER, RANGE_TOMBSTONE_BOUND_MARKER, read_partition_deletion,
    read_range_tombstone_marker,
};
use crate::memtable::partition::PartitionData;

/// A partition returned by the reverse scanner.
#[derive(Debug)]
pub struct ReverseScannedPartition {
    pub key: Vec<u8>,
    pub data: PartitionData,
}

/// Trait for iterating partitions in reverse order.
pub trait ReverseScanIterator {
    /// Returns the next partition in reverse (descending key) order.
    fn next_partition(&mut self) -> io::Result<Option<ReverseScannedPartition>>;
}

/// Index entry loaded from Index.db for reverse scanning.
#[derive(Debug, Clone)]
struct IndexEntry {
    partition_key: Vec<u8>,
    data_offset: u64,
}

/// Scans an SSTable in reverse partition-key order with optional range bounds.
pub struct ReverseScanner {
    descriptor: SSTableDescriptor,
    /// Index entries to iterate, already filtered and reversed.
    entries: Vec<IndexEntry>,
    /// Current position within `entries`.
    position: usize,
}

impl ReverseScanner {
    /// Open a reverse scanner over the entire SSTable.
    pub fn open(descriptor: SSTableDescriptor) -> io::Result<Self> {
        let entries = Self::load_index(&descriptor)?;
        let mut reversed = entries;
        reversed.reverse();
        Ok(Self {
            descriptor,
            entries: reversed,
            position: 0,
        })
    }

    /// Open a reverse scanner limited to keys within [start, end] (inclusive).
    pub fn open_range(
        descriptor: SSTableDescriptor,
        start: Bound<Vec<u8>>,
        end: Bound<Vec<u8>>,
    ) -> io::Result<Self> {
        let all_entries = Self::load_index(&descriptor)?;
        let filtered: Vec<IndexEntry> = all_entries
            .into_iter()
            .filter(|e| {
                let after_start = match &start {
                    Bound::Included(s) => e.partition_key.as_slice() >= s.as_slice(),
                    Bound::Excluded(s) => e.partition_key.as_slice() > s.as_slice(),
                    Bound::Unbounded => true,
                };
                let before_end = match &end {
                    Bound::Included(e_bound) => e.partition_key.as_slice() <= e_bound.as_slice(),
                    Bound::Excluded(e_bound) => e.partition_key.as_slice() < e_bound.as_slice(),
                    Bound::Unbounded => true,
                };
                after_start && before_end
            })
            .collect();

        let mut reversed = filtered;
        reversed.reverse();
        Ok(Self {
            descriptor,
            entries: reversed,
            position: 0,
        })
    }

    fn load_index(desc: &SSTableDescriptor) -> io::Result<Vec<IndexEntry>> {
        let path = desc.component_path(Component::Index);
        let mut reader = BufReader::new(File::open(&path)?);

        let mut magic = [0u8; 4];
        reader.read_exact(&mut magic)?;
        if magic != INDEX_MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid Index.db magic",
            ));
        }

        let mut entries = Vec::new();
        loop {
            let pk_len = match reader.read_u32::<BigEndian>() {
                Ok(l) => l,
                Err(ref e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            };
            let mut pk = vec![0u8; pk_len as usize];
            reader.read_exact(&mut pk)?;
            let offset = reader.read_u64::<BigEndian>()?;
            entries.push(IndexEntry {
                partition_key: pk,
                data_offset: offset,
            });
        }

        Ok(entries)
    }

    fn read_partition_at_offset(&self, offset: u64) -> io::Result<PartitionData> {
        let data_path = self.descriptor.component_path(Component::Data);
        let mut reader = BufReader::new(File::open(&data_path)?);
        reader.seek(SeekFrom::Start(offset))?;

        // Read partition key (skip it, we already know it from the index)
        let pk_len = reader.read_u32::<BigEndian>()? as usize;
        let mut _pk = vec![0u8; pk_len];
        reader.read_exact(&mut _pk)?;

        // Read rows until end-of-partition marker
        let mut partition = PartitionData::new();
        loop {
            let marker = reader.read_u8()?;
            if marker == END_OF_PARTITION {
                break;
            }
            if marker == PARTITION_DELETION_MARKER {
                let deletion = read_partition_deletion(&mut reader)?;
                partition
                    .set_tombstone(deletion.marked_for_delete_at, deletion.local_deletion_time);
                continue;
            }
            if marker == RANGE_TOMBSTONE_BOUND_MARKER {
                let _ = read_range_tombstone_marker(&mut reader)?;
                continue;
            }
            if marker != ROW_MARKER {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("unexpected marker byte: {marker:#x}"),
                ));
            }
            let row = read_row_from_reader(&mut reader)?;
            partition.apply_row(row);
        }

        Ok(partition)
    }
}

impl ReverseScanIterator for ReverseScanner {
    fn next_partition(&mut self) -> io::Result<Option<ReverseScannedPartition>> {
        if self.position >= self.entries.len() {
            return Ok(None);
        }

        let entry = &self.entries[self.position];
        let key = entry.partition_key.clone();
        let data = self.read_partition_at_offset(entry.data_offset)?;
        self.position += 1;

        Ok(Some(ReverseScannedPartition { key, data }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memtable::partition::{Cell, Row};
    use crate::sstable::writer::SSTableWriter;
    use tempfile::TempDir;

    fn sample_partitions() -> Vec<(Vec<u8>, PartitionData)> {
        let mut partitions = Vec::new();
        for i in 0..5u8 {
            let mut pd = PartitionData::new();
            for j in 0..2u8 {
                pd.apply_row(Row {
                    clustering_key: vec![j],
                    cells: vec![Cell {
                        column: "name".to_string(),
                        value: Some(format!("v_{i}_{j}").into_bytes()),
                        timestamp: 1000 + i as i64,
                        ttl: 0,
                        local_deletion_time: None,
                        is_tombstone: false,
                    }],
                    is_tombstone: false,
                    local_deletion_time: None,
                });
            }
            partitions.push((vec![i], pd));
        }
        partitions
    }

    fn write_sstable(dir: &TempDir) -> SSTableDescriptor {
        let desc = SSTableDescriptor::new(dir.path(), "ks", "t1", 1);
        let writer = SSTableWriter::new(desc.clone());
        writer.write(&sample_partitions()).unwrap();
        desc
    }

    #[test]
    fn reverse_full_scan() {
        let dir = TempDir::new().unwrap();
        let desc = write_sstable(&dir);

        let mut scanner = ReverseScanner::open(desc).unwrap();
        let mut keys = Vec::new();
        while let Some(p) = scanner.next_partition().unwrap() {
            keys.push(p.key.clone());
            assert_eq!(p.data.rows.len(), 2);
        }

        assert_eq!(keys, vec![vec![4], vec![3], vec![2], vec![1], vec![0]]);
    }

    #[test]
    fn reverse_with_range_bounds() {
        let dir = TempDir::new().unwrap();
        let desc = write_sstable(&dir);

        let mut scanner =
            ReverseScanner::open_range(desc, Bound::Included(vec![1]), Bound::Included(vec![3]))
                .unwrap();

        let mut keys = Vec::new();
        while let Some(p) = scanner.next_partition().unwrap() {
            keys.push(p.key.clone());
        }

        assert_eq!(keys, vec![vec![3], vec![2], vec![1]]);
    }

    #[test]
    fn reverse_single_partition() {
        let dir = TempDir::new().unwrap();
        let desc = write_sstable(&dir);

        let mut scanner =
            ReverseScanner::open_range(desc, Bound::Included(vec![2]), Bound::Included(vec![2]))
                .unwrap();

        let p = scanner.next_partition().unwrap().unwrap();
        assert_eq!(p.key, vec![2]);
        assert_eq!(p.data.rows.len(), 2);

        assert!(scanner.next_partition().unwrap().is_none());
    }

    #[test]
    fn comparison_with_forward_scan() {
        let dir = TempDir::new().unwrap();
        let desc = write_sstable(&dir);

        // Forward scan via reader
        let reader = crate::sstable::reader::SSTableReader::open(desc.clone()).unwrap();
        let forward = reader.iter_partitions().unwrap();

        // Reverse scan
        let mut scanner = ReverseScanner::open(desc).unwrap();
        let mut reversed = Vec::new();
        while let Some(p) = scanner.next_partition().unwrap() {
            reversed.push((p.key, p.data));
        }
        reversed.reverse();

        assert_eq!(forward.len(), reversed.len());
        for (f, r) in forward.iter().zip(reversed.iter()) {
            assert_eq!(f.0, r.0);
            assert_eq!(f.1.rows.len(), r.1.rows.len());
        }
    }

    #[test]
    fn reverse_scan_preserves_partition_tombstone() {
        let dir = TempDir::new().unwrap();
        let desc = SSTableDescriptor::new(dir.path(), "ks", "t1", 1);

        let mut pd = PartitionData::new();
        pd.set_tombstone(123, 456);
        SSTableWriter::new(desc.clone())
            .write(&[(b"pk".to_vec(), pd)])
            .unwrap();

        let mut scanner = ReverseScanner::open(desc).unwrap();
        let partition = scanner.next_partition().unwrap().unwrap();
        assert_eq!(partition.data.tombstone_timestamp, Some(123));
        assert_eq!(partition.data.tombstone_local_deletion_time, Some(456));
    }
}
