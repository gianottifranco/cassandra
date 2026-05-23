// Licensed under Apache License, Version 2.0.

//! SSTable reader: reads partitions from on-disk SSTable components.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.io.sstable.format.big.BigTableReader`
//! - `org.apache.cassandra.io.sstable.format.SSTableReader`
//!
//! ## Read Path
//!
//! 1. Check bloom filter (fast reject)
//! 2. Binary search on index for offset
//! 3. Seek data file to offset and scan partition

use std::fs::File;
use std::io::{self, BufReader, Read, Seek, SeekFrom};
use std::sync::Arc;

use byteorder::{BigEndian, ReadBytesExt};

use super::bloom::BloomFilter;
use super::format::*;
use super::key_cache::KeyCache;
use super::metadata::MetadataSerializer;
use super::tombstone_serializer::{
    PARTITION_DELETION_MARKER, RANGE_TOMBSTONE_BOUND_MARKER, read_partition_deletion,
    read_range_tombstone_marker,
};
use crate::memtable::partition::{Cell, PartitionData, Row};
use crate::partitions::filtered_partition::FilteredPartition;
use crate::rows::unfiltered::{RowData, Unfiltered};

/// Index entry loaded from Index.db.
#[derive(Debug, Clone)]
struct LoadedIndexEntry {
    partition_key: Vec<u8>,
    data_offset: u64,
}

/// An opened SSTable for reading.
pub struct SSTableReader {
    descriptor: SSTableDescriptor,
    bloom: BloomFilter,
    index: Vec<LoadedIndexEntry>,
    stats: Option<SSTableStats>,
    key_cache: Option<Arc<KeyCache>>,
}

/// Stats deserialized from Statistics.db.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct SSTableStats {
    pub partition_count: u64,
    pub row_count: u64,
    pub cell_count: u64,
    pub min_timestamp: i64,
    pub max_timestamp: i64,
    pub data_size: u64,
    pub index_size: u64,
}

impl SSTableReader {
    /// Open an SSTable for reading.
    pub fn open(descriptor: SSTableDescriptor) -> io::Result<Self> {
        let bloom = Self::load_bloom_filter(&descriptor)?;
        let index = Self::load_index(&descriptor)?;
        let stats = Self::load_stats(&descriptor).ok();

        Ok(Self {
            descriptor,
            bloom,
            index,
            stats,
            key_cache: None,
        })
    }

    /// The SSTable's descriptor.
    pub fn descriptor(&self) -> &SSTableDescriptor {
        &self.descriptor
    }

    /// Statistics, if available.
    pub fn stats(&self) -> Option<&SSTableStats> {
        self.stats.as_ref()
    }

    /// The generation / SSTable ID.
    pub fn generation(&self) -> SSTableId {
        self.descriptor.generation
    }

    /// Attach a shared key cache (builder pattern).
    pub fn with_key_cache(mut self, cache: Arc<KeyCache>) -> Self {
        self.key_cache = Some(cache);
        self
    }

    /// Check bloom filter: does this SSTable potentially contain the key?
    pub fn might_contain_key(&self, partition_key: &[u8]) -> bool {
        self.bloom.might_contain(partition_key)
    }

    /// Read a single partition by key. Returns None if not found.
    pub fn get_partition(&self, partition_key: &[u8]) -> io::Result<Option<PartitionData>> {
        // 1. Bloom filter fast path
        if !self.bloom.might_contain(partition_key) {
            return Ok(None);
        }

        // 2. Try key cache for a direct offset (skip binary search on hit)
        let sst_id = self.generation();
        let cached_offset = self
            .key_cache
            .as_ref()
            .and_then(|c| c.get(sst_id, partition_key));

        let offset = if let Some(off) = cached_offset {
            off
        } else {
            // Binary search on index
            match self
                .index
                .binary_search_by(|e| e.partition_key.as_slice().cmp(partition_key))
            {
                Ok(pos) => self.index[pos].data_offset,
                Err(_) => return Ok(None), // Not in index
            }
        };

        // 3. Read from Data.db
        let data_path = self.descriptor.component_path(Component::Data);
        let mut reader = BufReader::new(File::open(&data_path)?);
        reader.seek(SeekFrom::Start(offset))?;

        // Read partition key at offset and verify
        let pk = read_bytes(&mut reader)?;
        if pk != partition_key {
            return Ok(None); // Bloom filter false positive or index mismatch
        }

        // Read rows until end-of-partition
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

            let row = read_row(&mut reader)?;
            partition.apply_row(row);
        }

        // Populate key cache on successful read (miss path)
        if cached_offset.is_none() {
            if let Some(cache) = &self.key_cache {
                cache.put(sst_id, partition_key.to_vec(), offset);
            }
        }

        Ok(Some(partition))
    }

    /// Read a single partition by key as a materialized unfiltered stream.
    ///
    /// Unlike [`Self::get_partition`], this preserves range-tombstone markers
    /// instead of requiring the simplified `PartitionData` representation.
    pub fn get_filtered_partition(
        &self,
        partition_key: &[u8],
    ) -> io::Result<Option<FilteredPartition>> {
        if !self.bloom.might_contain(partition_key) {
            return Ok(None);
        }

        let offset = match self
            .index
            .binary_search_by(|entry| entry.partition_key.as_slice().cmp(partition_key))
        {
            Ok(pos) => self.index[pos].data_offset,
            Err(_) => return Ok(None),
        };

        let data_path = self.descriptor.component_path(Component::Data);
        let mut reader = BufReader::new(File::open(&data_path)?);
        reader.seek(SeekFrom::Start(offset))?;

        let pk = read_bytes(&mut reader)?;
        if pk != partition_key {
            return Ok(None);
        }

        read_filtered_partition_body(pk, &mut reader).map(Some)
    }

    /// Iterate all partitions in this SSTable (for compaction / merge).
    pub fn iter_partitions(&self) -> io::Result<Vec<(Vec<u8>, PartitionData)>> {
        let data_path = self.descriptor.component_path(Component::Data);
        let mut reader = BufReader::new(File::open(&data_path)?);

        // Skip magic + version
        reader.seek(SeekFrom::Start(5))?;

        let mut result = Vec::new();
        let file_size = std::fs::metadata(&data_path)?.len();

        loop {
            let pos = reader.stream_position()?;
            // Check if we're at the trailing CRC (last 4 bytes)
            if pos + 4 >= file_size {
                break;
            }

            // Try to read partition key length
            let pk_len = match reader.read_u32::<BigEndian>() {
                Ok(l) => l,
                Err(ref e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            };

            if pk_len == 0 {
                break;
            }

            let mut pk = vec![0u8; pk_len as usize];
            reader.read_exact(&mut pk)?;

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
                        format!("unexpected marker: {marker:#x}"),
                    ));
                }
                let row = read_row(&mut reader)?;
                partition.apply_row(row);
            }

            result.push((pk, partition));
        }

        Ok(result)
    }

    /// Iterate all partitions as materialized unfiltered streams.
    pub fn iter_filtered_partitions(&self) -> io::Result<Vec<FilteredPartition>> {
        let data_path = self.descriptor.component_path(Component::Data);
        let mut reader = BufReader::new(File::open(&data_path)?);
        reader.seek(SeekFrom::Start(5))?;

        let mut result = Vec::new();
        let file_size = std::fs::metadata(&data_path)?.len();

        loop {
            let pos = reader.stream_position()?;
            if pos + 4 >= file_size {
                break;
            }

            let pk_len = match reader.read_u32::<BigEndian>() {
                Ok(l) => l,
                Err(ref e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            };
            if pk_len == 0 {
                break;
            }

            let mut pk = vec![0u8; pk_len as usize];
            reader.read_exact(&mut pk)?;
            result.push(read_filtered_partition_body(pk, &mut reader)?);
        }

        Ok(result)
    }

    // ─── Private loading helpers ───────────────────────────────────────

    fn load_bloom_filter(desc: &SSTableDescriptor) -> io::Result<BloomFilter> {
        let path = desc.component_path(Component::Filter);
        let mut reader = BufReader::new(File::open(&path)?);

        // Skip magic
        let mut magic = [0u8; 4];
        reader.read_exact(&mut magic)?;
        if magic != FILTER_MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid Filter.db magic",
            ));
        }

        BloomFilter::deserialize(&mut reader)
    }

    fn load_index(desc: &SSTableDescriptor) -> io::Result<Vec<LoadedIndexEntry>> {
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
            entries.push(LoadedIndexEntry {
                partition_key: pk,
                data_offset: offset,
            });
        }

        Ok(entries)
    }

    fn load_stats(desc: &SSTableDescriptor) -> io::Result<SSTableStats> {
        let path = desc.component_path(Component::Statistics);
        let data = std::fs::read(&path)?;
        let metadata = MetadataSerializer::deserialize_auto(&data)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        Ok(SSTableStats {
            partition_count: metadata.partition_count,
            row_count: metadata.row_count,
            cell_count: metadata.cell_count,
            min_timestamp: metadata.min_timestamp,
            max_timestamp: metadata.max_timestamp,
            data_size: metadata.data_size,
            index_size: metadata.index_size,
        })
    }
}

// ─── Data.db parsing helpers ───────────────────────────────────────────────

fn read_bytes<R: Read>(reader: &mut R) -> io::Result<Vec<u8>> {
    let len = reader.read_u32::<BigEndian>()? as usize;
    let mut buf = vec![0u8; len];
    reader.read_exact(&mut buf)?;
    Ok(buf)
}

/// Public helper: read a row from a Data.db reader (used by both Big and BTI readers).
pub fn read_row_from_reader<R: Read>(reader: &mut R) -> io::Result<Row> {
    read_row(reader)
}

pub(crate) fn read_filtered_partition_body<R: Read>(
    partition_key: Vec<u8>,
    reader: &mut R,
) -> io::Result<FilteredPartition> {
    let mut partition_deletion = cassandra_common::tombstone::DeletionTime::LIVE;
    let mut static_row = None;
    let mut items = Vec::new();

    loop {
        let marker = reader.read_u8()?;
        if marker == END_OF_PARTITION {
            break;
        }
        if marker == PARTITION_DELETION_MARKER {
            partition_deletion = read_partition_deletion(reader)?;
            continue;
        }
        if marker == RANGE_TOMBSTONE_BOUND_MARKER {
            items.push(Unfiltered::Marker(read_range_tombstone_marker(reader)?));
            continue;
        }
        if marker != ROW_MARKER {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unexpected marker: {marker:#x}"),
            ));
        }

        let row = read_row(reader)?;
        let mut row_data = RowData::from(&row);
        if row_data.clustering_key.is_empty() {
            row_data.is_static = true;
            static_row = Some(row_data);
        } else {
            items.push(Unfiltered::Row(row_data));
        }
    }

    Ok(FilteredPartition {
        partition_key,
        partition_deletion,
        static_row,
        items,
    })
}

fn read_row<R: Read>(reader: &mut R) -> io::Result<Row> {
    // Clustering key
    let ck = read_bytes(reader)?;

    // Row flags
    let flags = reader.read_u8()?;
    let is_tombstone = (flags & 0x01) != 0;
    let has_ldt = (flags & 0x02) != 0;

    let local_deletion_time = if has_ldt {
        Some(reader.read_i32::<BigEndian>()?)
    } else {
        None
    };

    // Cell count
    let cell_count = reader.read_u32::<BigEndian>()?;

    let mut cells = Vec::with_capacity(cell_count as usize);
    for _ in 0..cell_count {
        cells.push(read_cell(reader)?);
    }

    Ok(Row {
        clustering_key: ck,
        cells,
        is_tombstone,
        local_deletion_time,
    })
}

fn read_cell<R: Read>(reader: &mut R) -> io::Result<Cell> {
    // Column name (u16 prefix)
    let name_len = reader.read_u16::<BigEndian>()? as usize;
    let mut name_buf = vec![0u8; name_len];
    reader.read_exact(&mut name_buf)?;
    let column =
        String::from_utf8(name_buf).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

    // Timestamp
    let timestamp = reader.read_i64::<BigEndian>()?;

    // TTL
    let ttl = reader.read_i32::<BigEndian>()?;

    // Cell flags
    let flags = reader.read_u8()?;
    let is_tombstone = (flags & 0x01) != 0;
    let has_value = (flags & 0x02) != 0;
    let has_ldt = (flags & 0x04) != 0;

    let local_deletion_time = if has_ldt {
        Some(reader.read_i32::<BigEndian>()?)
    } else {
        None
    };

    let value = if has_value {
        Some(read_bytes(reader)?)
    } else {
        None
    };

    Ok(Cell {
        column,
        value,
        timestamp,
        ttl,
        local_deletion_time,
        is_tombstone,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rows::unfiltered::{ClusteringBound, ClusteringBoundKind, RangeTombstoneMarker};
    use crate::sstable::tombstone_serializer::write_range_tombstone_marker;
    use crate::sstable::writer::SSTableWriter;
    use cassandra_common::tombstone::DeletionTime;
    use crc32fast::Hasher;
    use std::fs;
    use tempfile::TempDir;

    fn sample_partitions() -> Vec<(Vec<u8>, PartitionData)> {
        let mut partitions = Vec::new();
        for i in 0..10u8 {
            let mut pd = PartitionData::new();
            for j in 0..3u8 {
                pd.apply_row(Row {
                    clustering_key: vec![j],
                    cells: vec![Cell {
                        column: "name".to_string(),
                        value: Some(format!("value_{i}_{j}").into_bytes()),
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

    fn inject_range_tombstone_marker(desc: &SSTableDescriptor, marker: &RangeTombstoneMarker) {
        let data_path = desc.component_path(Component::Data);
        let data = fs::read(&data_path).unwrap();
        assert!(data.len() > 5);
        let crc_offset = data.len() - 4;
        let end_marker_offset = crc_offset - 1;
        assert_eq!(data[end_marker_offset], END_OF_PARTITION);

        let mut marker_bytes = Vec::new();
        let mut marker_crc = Hasher::new();
        write_range_tombstone_marker(&mut marker_bytes, marker, &mut marker_crc).unwrap();

        let mut rewritten = Vec::new();
        rewritten.extend_from_slice(&data[..end_marker_offset]);
        rewritten.extend_from_slice(&marker_bytes);
        rewritten.push(END_OF_PARTITION);

        let mut crc = Hasher::new();
        crc.update(&rewritten);
        rewritten.extend_from_slice(&crc.finalize().to_be_bytes());
        fs::write(data_path, rewritten).unwrap();
    }

    #[test]
    fn write_and_read_roundtrip() {
        let dir = TempDir::new().unwrap();
        let desc = SSTableDescriptor::new(dir.path(), "ks", "t1", 1);

        let partitions = sample_partitions();
        let writer = SSTableWriter::new(desc.clone());
        writer.write(&partitions).unwrap();

        let reader = SSTableReader::open(desc).unwrap();

        // Test specific partition lookup
        let p5 = reader.get_partition(&[5]).unwrap().unwrap();
        assert_eq!(p5.rows.len(), 3);
        let row = p5.rows.get(&vec![0u8]).unwrap();
        assert_eq!(row.cells[0].column, "name");
        assert_eq!(row.cells[0].value.as_deref(), Some(b"value_5_0".as_slice()));
    }

    #[test]
    fn bloom_filter_rejects_missing_key() {
        let dir = TempDir::new().unwrap();
        let desc = SSTableDescriptor::new(dir.path(), "ks", "t1", 1);

        let partitions = sample_partitions();
        SSTableWriter::new(desc.clone()).write(&partitions).unwrap();

        let reader = SSTableReader::open(desc).unwrap();
        assert!(!reader.might_contain_key(&[255])); // not in SSTable
    }

    #[test]
    fn iter_all_partitions() {
        let dir = TempDir::new().unwrap();
        let desc = SSTableDescriptor::new(dir.path(), "ks", "t1", 1);

        let partitions = sample_partitions();
        SSTableWriter::new(desc.clone()).write(&partitions).unwrap();

        let reader = SSTableReader::open(desc).unwrap();
        let all = reader.iter_partitions().unwrap();
        assert_eq!(all.len(), 10);

        // Verify ordering
        for i in 0..10u8 {
            assert_eq!(all[i as usize].0, vec![i]);
        }
    }

    #[test]
    fn missing_partition_returns_none() {
        let dir = TempDir::new().unwrap();
        let desc = SSTableDescriptor::new(dir.path(), "ks", "t1", 1);

        let partitions = sample_partitions();
        SSTableWriter::new(desc.clone()).write(&partitions).unwrap();

        let reader = SSTableReader::open(desc).unwrap();
        let result = reader.get_partition(&[200]).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn stats_loaded_correctly() {
        let dir = TempDir::new().unwrap();
        let desc = SSTableDescriptor::new(dir.path(), "ks", "t1", 1);

        let partitions = sample_partitions();
        SSTableWriter::new(desc.clone()).write(&partitions).unwrap();

        let reader = SSTableReader::open(desc).unwrap();
        let stats = reader.stats().unwrap();
        assert_eq!(stats.partition_count, 10);
        assert_eq!(stats.row_count, 30);
        assert_eq!(stats.cell_count, 30);
    }

    #[test]
    fn tombstone_roundtrip() {
        let dir = TempDir::new().unwrap();
        let desc = SSTableDescriptor::new(dir.path(), "ks", "t1", 1);

        let mut pd = PartitionData::new();
        pd.apply_row(Row {
            clustering_key: b"ck1".to_vec(),
            cells: vec![Cell {
                column: "x".to_string(),
                value: None,
                timestamp: 200,
                ttl: 0,
                local_deletion_time: Some(200),
                is_tombstone: true,
            }],
            is_tombstone: false,
            local_deletion_time: None,
        });

        let partitions = vec![(b"pk1".to_vec(), pd)];
        SSTableWriter::new(desc.clone()).write(&partitions).unwrap();

        let reader = SSTableReader::open(desc).unwrap();
        let p = reader.get_partition(b"pk1").unwrap().unwrap();
        let row = p.rows.get(&b"ck1".to_vec()).unwrap();
        assert!(row.cells[0].is_tombstone);
        assert_eq!(row.cells[0].local_deletion_time, Some(200));
    }

    #[test]
    fn partition_tombstone_roundtrip() {
        let dir = TempDir::new().unwrap();
        let desc = SSTableDescriptor::new(dir.path(), "ks", "t1", 1);

        let mut pd = PartitionData::new();
        pd.set_tombstone(123, 456);
        pd.apply_row(Row {
            clustering_key: b"ck1".to_vec(),
            cells: vec![Cell {
                column: "x".to_string(),
                value: Some(b"v".to_vec()),
                timestamp: 200,
                ttl: 0,
                local_deletion_time: None,
                is_tombstone: false,
            }],
            is_tombstone: false,
            local_deletion_time: None,
        });

        let partitions = vec![(b"pk1".to_vec(), pd)];
        SSTableWriter::new(desc.clone()).write(&partitions).unwrap();

        let reader = SSTableReader::open(desc).unwrap();
        let p = reader.get_partition(b"pk1").unwrap().unwrap();
        assert_eq!(p.tombstone_timestamp, Some(123));
        assert_eq!(p.tombstone_local_deletion_time, Some(456));
    }

    #[test]
    fn iter_partitions_preserves_partition_tombstone() {
        let dir = TempDir::new().unwrap();
        let desc = SSTableDescriptor::new(dir.path(), "ks", "t1", 1);

        let mut pd = PartitionData::new();
        pd.set_tombstone(321, 654);
        let partitions = vec![(b"pk1".to_vec(), pd)];
        SSTableWriter::new(desc.clone()).write(&partitions).unwrap();

        let reader = SSTableReader::open(desc).unwrap();
        let all = reader.iter_partitions().unwrap();
        assert_eq!(all[0].1.tombstone_timestamp, Some(321));
        assert_eq!(all[0].1.tombstone_local_deletion_time, Some(654));
    }

    #[test]
    fn filtered_partition_preserves_range_tombstone_marker() {
        let dir = TempDir::new().unwrap();
        let desc = SSTableDescriptor::new(dir.path(), "ks", "t1", 1);

        let mut pd = PartitionData::new();
        pd.set_tombstone(111, 222);
        pd.apply_row(Row {
            clustering_key: b"ck1".to_vec(),
            cells: vec![Cell {
                column: "x".to_string(),
                value: Some(b"v".to_vec()),
                timestamp: 300,
                ttl: 0,
                local_deletion_time: None,
                is_tombstone: false,
            }],
            is_tombstone: false,
            local_deletion_time: None,
        });
        SSTableWriter::new(desc.clone())
            .write(&[(b"pk1".to_vec(), pd)])
            .unwrap();

        let marker = RangeTombstoneMarker::Boundary {
            bound: ClusteringBound {
                kind: ClusteringBoundKind::InclusiveEnd,
                values: b"ck2".to_vec(),
            },
            close_deletion: DeletionTime::new(400, 444),
            open_deletion: DeletionTime::new(500, 555),
        };
        inject_range_tombstone_marker(&desc, &marker);

        let reader = SSTableReader::open(desc).unwrap();
        let filtered = reader.get_filtered_partition(b"pk1").unwrap().unwrap();
        assert_eq!(filtered.partition_key, b"pk1");
        assert_eq!(filtered.partition_deletion, DeletionTime::new(111, 222));
        assert_eq!(filtered.row_count(), 1);
        assert!(filtered.items.iter().any(|item| matches!(
            item,
            Unfiltered::Marker(decoded) if decoded == &marker
        )));

        let simplified = reader.get_partition(b"pk1").unwrap().unwrap();
        assert_eq!(simplified.rows.len(), 1);
    }

    #[test]
    fn iter_filtered_partitions_preserves_range_tombstone_marker() {
        let dir = TempDir::new().unwrap();
        let desc = SSTableDescriptor::new(dir.path(), "ks", "t1", 1);

        let mut pd = PartitionData::new();
        pd.apply_row(Row {
            clustering_key: b"ck1".to_vec(),
            cells: vec![Cell {
                column: "x".to_string(),
                value: Some(b"v".to_vec()),
                timestamp: 300,
                ttl: 0,
                local_deletion_time: None,
                is_tombstone: false,
            }],
            is_tombstone: false,
            local_deletion_time: None,
        });
        SSTableWriter::new(desc.clone())
            .write(&[(b"pk1".to_vec(), pd)])
            .unwrap();

        let marker = RangeTombstoneMarker::Open {
            bound: ClusteringBound {
                kind: ClusteringBoundKind::InclusiveStart,
                values: b"ck1".to_vec(),
            },
            deletion: DeletionTime::new(400, 444),
        };
        inject_range_tombstone_marker(&desc, &marker);

        let reader = SSTableReader::open(desc).unwrap();
        let partitions = reader.iter_filtered_partitions().unwrap();
        assert_eq!(partitions.len(), 1);
        assert!(partitions[0].items.iter().any(|item| matches!(
            item,
            Unfiltered::Marker(decoded) if decoded == &marker
        )));
    }
}
