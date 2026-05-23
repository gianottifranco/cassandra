// Licensed under Apache License, Version 2.0.

//! SSTable scanner: sequential partition iteration over Data.db.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.io.sstable.format.SSTableScanner`
//! - `org.apache.cassandra.io.sstable.format.big.BigTableScanner`
//!
//! `ForwardScanner` reads partitions from a Data.db file in key order,
//! optionally restricted to a `[start_key, end_key]` range via index lookup.

use std::fs::File;
use std::io::{self, BufReader, Read, Seek, SeekFrom};

use byteorder::{BigEndian, ReadBytesExt};

use super::format::{Component, END_OF_PARTITION, INDEX_MAGIC, ROW_MARKER, SSTableDescriptor};
use super::reader::read_row_from_reader;
use super::tombstone_serializer::{
    PARTITION_DELETION_MARKER, RANGE_TOMBSTONE_BOUND_MARKER, read_partition_deletion,
    read_range_tombstone_marker,
};
use crate::memtable::partition::PartitionData;

/// A single scanned partition with its key and data.
#[derive(Debug)]
pub struct ScannedPartition {
    pub key: Vec<u8>,
    pub data: PartitionData,
}

/// Trait for iterating over SSTable partitions.
pub trait SSTableScanner {
    /// Advance to the next partition. Returns `None` at end of data.
    fn next_partition(&mut self) -> io::Result<Option<ScannedPartition>>;
}

/// Forward-only sequential scanner over Data.db.
pub struct ForwardScanner {
    reader: BufReader<File>,
    file_size: u64,
    end_key: Option<Vec<u8>>,
    done: bool,
}

impl ForwardScanner {
    /// Open a scanner that reads all partitions from the Data.db file.
    pub fn new(descriptor: &SSTableDescriptor) -> io::Result<Self> {
        let data_path = descriptor.component_path(Component::Data);
        let file_size = std::fs::metadata(&data_path)?.len();
        let mut reader = BufReader::new(File::open(&data_path)?);

        // Skip magic (4 bytes) + version (1 byte)
        reader.seek(SeekFrom::Start(5))?;

        Ok(Self {
            reader,
            file_size,
            end_key: None,
            done: false,
        })
    }

    /// Open a scanner restricted to `[start_key, end_key]` inclusive.
    ///
    /// Uses the Index.db to binary-search for the start position,
    /// then stops when partition key exceeds `end_key`.
    pub fn with_range(
        descriptor: &SSTableDescriptor,
        start_key: Option<Vec<u8>>,
        end_key: Option<Vec<u8>>,
    ) -> io::Result<Self> {
        let data_path = descriptor.component_path(Component::Data);
        let file_size = std::fs::metadata(&data_path)?.len();
        let mut reader = BufReader::new(File::open(&data_path)?);

        // Determine start offset via index
        let start_offset = if let Some(ref sk) = start_key {
            let index = load_index(descriptor)?;
            match index.binary_search_by(|e| e.0.as_slice().cmp(sk.as_slice())) {
                Ok(pos) => index[pos].1,
                Err(pos) => {
                    if pos < index.len() {
                        index[pos].1
                    } else {
                        // Start key is beyond all partitions
                        file_size
                    }
                }
            }
        } else {
            5 // after header
        };

        reader.seek(SeekFrom::Start(start_offset))?;

        Ok(Self {
            reader,
            file_size,
            end_key,
            done: start_offset >= file_size,
        })
    }
}

impl SSTableScanner for ForwardScanner {
    fn next_partition(&mut self) -> io::Result<Option<ScannedPartition>> {
        if self.done {
            return Ok(None);
        }

        let pos = self.reader.stream_position()?;
        // Stop before trailing CRC (last 4 bytes)
        if pos + 4 >= self.file_size {
            self.done = true;
            return Ok(None);
        }

        // Read partition key (length-prefixed)
        let pk_len = match self.reader.read_u32::<BigEndian>() {
            Ok(0) => {
                self.done = true;
                return Ok(None);
            }
            Ok(l) => l,
            Err(ref e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                self.done = true;
                return Ok(None);
            }
            Err(e) => return Err(e),
        };

        let mut pk = vec![0u8; pk_len as usize];
        self.reader.read_exact(&mut pk)?;

        // Check end_key bound
        if let Some(ref ek) = self.end_key {
            if pk.as_slice() > ek.as_slice() {
                self.done = true;
                return Ok(None);
            }
        }

        // Read rows until end-of-partition
        let mut partition = PartitionData::new();
        loop {
            let marker = self.reader.read_u8()?;
            if marker == END_OF_PARTITION {
                break;
            }
            if marker == PARTITION_DELETION_MARKER {
                let deletion = read_partition_deletion(&mut self.reader)?;
                partition
                    .set_tombstone(deletion.marked_for_delete_at, deletion.local_deletion_time);
                continue;
            }
            if marker == RANGE_TOMBSTONE_BOUND_MARKER {
                let _ = read_range_tombstone_marker(&mut self.reader)?;
                continue;
            }
            if marker != ROW_MARKER {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("unexpected marker byte: {marker:#x}"),
                ));
            }
            let row = read_row_from_reader(&mut self.reader)?;
            partition.apply_row(row);
        }

        Ok(Some(ScannedPartition {
            key: pk,
            data: partition,
        }))
    }
}

/// Load index entries as `(partition_key, data_offset)` pairs.
fn load_index(descriptor: &SSTableDescriptor) -> io::Result<Vec<(Vec<u8>, u64)>> {
    let path = descriptor.component_path(Component::Index);
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
        entries.push((pk, offset));
    }

    Ok(entries)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memtable::partition::{Cell, Row};
    use crate::sstable::writer::SSTableWriter;
    use tempfile::TempDir;

    fn sample_partitions() -> Vec<(Vec<u8>, PartitionData)> {
        let mut partitions = Vec::new();
        for i in 0..10u8 {
            let mut pd = PartitionData::new();
            for j in 0..2u8 {
                pd.apply_row(Row {
                    clustering_key: vec![j],
                    cells: vec![Cell {
                        column: "col".to_string(),
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
        let partitions = sample_partitions();
        SSTableWriter::new(desc.clone()).write(&partitions).unwrap();
        desc
    }

    #[test]
    fn scan_all_partitions() {
        let dir = TempDir::new().unwrap();
        let desc = write_sstable(&dir);

        let mut scanner = ForwardScanner::new(&desc).unwrap();
        let mut count = 0;
        let mut keys = Vec::new();

        while let Some(sp) = scanner.next_partition().unwrap() {
            keys.push(sp.key.clone());
            assert_eq!(sp.data.rows.len(), 2);
            count += 1;
        }

        assert_eq!(count, 10);
        // Verify ordering
        for i in 0..10u8 {
            assert_eq!(keys[i as usize], vec![i]);
        }
    }

    #[test]
    fn scan_with_start_end_range() {
        let dir = TempDir::new().unwrap();
        let desc = write_sstable(&dir);

        let mut scanner = ForwardScanner::with_range(&desc, Some(vec![3]), Some(vec![6])).unwrap();

        let mut keys = Vec::new();
        while let Some(sp) = scanner.next_partition().unwrap() {
            keys.push(sp.key.clone());
        }

        assert_eq!(keys, vec![vec![3], vec![4], vec![5], vec![6]]);
    }

    #[test]
    fn scan_empty_sstable() {
        let dir = TempDir::new().unwrap();
        let desc = SSTableDescriptor::new(dir.path(), "ks", "t1", 99);
        SSTableWriter::new(desc.clone()).write(&[]).unwrap();

        let mut scanner = ForwardScanner::new(&desc).unwrap();
        assert!(scanner.next_partition().unwrap().is_none());
    }

    #[test]
    fn scan_single_partition_match() {
        let dir = TempDir::new().unwrap();
        let desc = write_sstable(&dir);

        let mut scanner = ForwardScanner::with_range(&desc, Some(vec![5]), Some(vec![5])).unwrap();

        let sp = scanner.next_partition().unwrap().unwrap();
        assert_eq!(sp.key, vec![5]);
        assert_eq!(sp.data.rows.len(), 2);

        // No more partitions
        assert!(scanner.next_partition().unwrap().is_none());
    }

    #[test]
    fn scan_range_beyond_data() {
        let dir = TempDir::new().unwrap();
        let desc = write_sstable(&dir);

        let mut scanner =
            ForwardScanner::with_range(&desc, Some(vec![100]), Some(vec![200])).unwrap();

        assert!(scanner.next_partition().unwrap().is_none());
    }

    #[test]
    fn scan_with_only_end_key() {
        let dir = TempDir::new().unwrap();
        let desc = write_sstable(&dir);

        let mut scanner = ForwardScanner::with_range(&desc, None, Some(vec![2])).unwrap();

        let mut keys = Vec::new();
        while let Some(sp) = scanner.next_partition().unwrap() {
            keys.push(sp.key.clone());
        }

        assert_eq!(keys, vec![vec![0], vec![1], vec![2]]);
    }

    #[test]
    fn scanner_preserves_partition_tombstone() {
        let dir = TempDir::new().unwrap();
        let desc = SSTableDescriptor::new(dir.path(), "ks", "t1", 1);

        let mut pd = PartitionData::new();
        pd.set_tombstone(123, 456);
        let partitions = vec![(b"pk1".to_vec(), pd)];
        SSTableWriter::new(desc.clone()).write(&partitions).unwrap();

        let mut scanner = ForwardScanner::new(&desc).unwrap();
        let scanned = scanner.next_partition().unwrap().unwrap();
        assert_eq!(scanned.key, b"pk1");
        assert_eq!(scanned.data.tombstone_timestamp, Some(123));
        assert_eq!(scanned.data.tombstone_local_deletion_time, Some(456));
        assert!(scanner.next_partition().unwrap().is_none());
    }
}
