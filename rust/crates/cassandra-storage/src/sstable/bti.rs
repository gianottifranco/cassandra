// Licensed under Apache License, Version 2.0.

//! BTI (Block-based Trie Index) SSTable format.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.io.sstable.format.bti.BtiFormat`
//! - `org.apache.cassandra.io.sstable.format.bti.PartitionIndex`
//! - `org.apache.cassandra.io.sstable.format.bti.RowIndexReader`
//!
//! ## Architecture
//!
//! BTI format replaces the traditional partition index with a trie-based
//! index for O(key_length) lookups instead of O(log(n)) binary search.
//!
//! Components (different from Big format):
//! - `*-Data.db`       – same serialized partitions as Big format
//! - `*-Partitions.db` – trie-encoded partition index
//! - `*-Rows.db`       – row-level trie index (per partition)
//! - `*-Filter.db`     – Bloom filter (same as Big)
//! - `*-Statistics.db`  – stats (same as Big)
//! - `*-TOC.txt`       – component listing
//!
//! The trie index uses a byte-trie where each path from root to leaf
//! corresponds to a partition key, and the leaf stores the data offset.
//!
//! ## Feature Flag
//!
//! This module is always compiled but BTI format selection is gated
//! by `SSTableFormat::Bti` in the descriptor.

use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

use super::bloom::BloomFilter;
use super::format::*;
use crate::memtable::partition::{Cell, PartitionData, Row};

// ─── BTI constants ─────────────────────────────────────────────────────────

const BTI_TRIE_MAGIC: [u8; 4] = *b"BTRI";
const BTI_TRIE_VERSION: u8 = 1;

// Trie node types
const TRIE_NODE_PAYLOAD: u8 = 0x01;
const TRIE_NODE_BRANCH: u8 = 0x02;
const TRIE_NODE_LEAF: u8 = 0x03;

// ─── Trie Index Builder ────────────────────────────────────────────────────

/// Entry in the trie index: key → data offset.
#[derive(Debug, Clone)]
struct TrieEntry {
    key: Vec<u8>,
    data_offset: u64,
}

/// Serialized trie node for on-disk format.
#[derive(Debug)]
struct TrieNode {
    children: BTreeMap<u8, TrieNode>,
    data_offset: Option<u64>,
}

impl TrieNode {
    fn new() -> Self {
        Self {
            children: BTreeMap::new(),
            data_offset: None,
        }
    }

    fn insert(&mut self, key: &[u8], offset: u64) {
        if key.is_empty() {
            self.data_offset = Some(offset);
        } else {
            let child = self.children.entry(key[0]).or_insert_with(TrieNode::new);
            child.insert(&key[1..], offset);
        }
    }

    fn lookup(&self, key: &[u8]) -> Option<u64> {
        if key.is_empty() {
            self.data_offset
        } else {
            self.children.get(&key[0])?.lookup(&key[1..])
        }
    }

    /// Serialize the trie to a writer (DFS, pre-order).
    fn serialize<W: Write>(&self, w: &mut W) -> io::Result<()> {
        // Write this node
        if let Some(offset) = self.data_offset {
            if self.children.is_empty() {
                // Leaf node
                w.write_u8(TRIE_NODE_LEAF)?;
                w.write_u64::<BigEndian>(offset)?;
            } else {
                // Payload + branches
                w.write_u8(TRIE_NODE_PAYLOAD)?;
                w.write_u64::<BigEndian>(offset)?;
                Self::write_children(w, &self.children)?;
            }
        } else if !self.children.is_empty() {
            // Branch only
            w.write_u8(TRIE_NODE_BRANCH)?;
            Self::write_children(w, &self.children)?;
        }
        Ok(())
    }

    fn write_children<W: Write>(w: &mut W, children: &BTreeMap<u8, TrieNode>) -> io::Result<()> {
        w.write_u16::<BigEndian>(children.len() as u16)?;
        for (&byte, child) in children {
            w.write_u8(byte)?;
            child.serialize(w)?;
        }
        Ok(())
    }

    /// Deserialize a trie from a reader.
    fn deserialize<R: Read>(r: &mut R) -> io::Result<Self> {
        let node_type = r.read_u8()?;
        match node_type {
            TRIE_NODE_LEAF => {
                let offset = r.read_u64::<BigEndian>()?;
                Ok(Self {
                    children: BTreeMap::new(),
                    data_offset: Some(offset),
                })
            }
            TRIE_NODE_PAYLOAD => {
                let offset = r.read_u64::<BigEndian>()?;
                let children = Self::read_children(r)?;
                Ok(Self {
                    children,
                    data_offset: Some(offset),
                })
            }
            TRIE_NODE_BRANCH => {
                let children = Self::read_children(r)?;
                Ok(Self {
                    children,
                    data_offset: None,
                })
            }
            other => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid trie node type: {other:#x}"),
            )),
        }
    }

    fn read_children<R: Read>(r: &mut R) -> io::Result<BTreeMap<u8, TrieNode>> {
        let count = r.read_u16::<BigEndian>()?;
        let mut children = BTreeMap::new();
        for _ in 0..count {
            let byte = r.read_u8()?;
            let child = Self::deserialize(r)?;
            children.insert(byte, child);
        }
        Ok(children)
    }
}

// ─── BTI Writer ────────────────────────────────────────────────────────────

/// Writer for the BTI SSTable format.
pub struct BtiWriter {
    descriptor: SSTableDescriptor,
}

impl BtiWriter {
    pub fn new(descriptor: SSTableDescriptor) -> Self {
        Self { descriptor }
    }

    /// Write partitions to BTI-format SSTable.
    /// The Data.db format is the same as Big; the index is trie-based.
    pub fn write(
        &self,
        partitions: &[(Vec<u8>, PartitionData)],
    ) -> io::Result<super::writer::SSTableStats> {
        fs::create_dir_all(&self.descriptor.directory)?;

        // First write Data.db (same format as Big)
        let (stats, entries) = self.write_data(partitions)?;

        // Write trie-based Partitions.db
        self.write_partition_index(&entries)?;

        // Write Bloom filter (same as Big)
        self.write_bloom_filter(partitions)?;

        // Write Statistics.db
        self.write_statistics(&stats)?;

        // Write TOC.txt
        self.write_toc()?;

        Ok(stats)
    }

    fn write_data(
        &self,
        partitions: &[(Vec<u8>, PartitionData)],
    ) -> io::Result<(super::writer::SSTableStats, Vec<TrieEntry>)> {
        let data_path = self.descriptor.component_path(Component::Data);
        let mut data_file = BufWriter::new(File::create(&data_path)?);
        let mut crc = crc32fast::Hasher::new();
        let mut data_offset: u64 = 0;

        let mut stats = super::writer::SSTableStats {
            partition_count: 0,
            row_count: 0,
            cell_count: 0,
            min_timestamp: i64::MAX,
            max_timestamp: i64::MIN,
            data_size: 0,
            index_size: 0,
        };

        let mut entries = Vec::new();

        // Header
        data_file.write_all(&DATA_MAGIC)?;
        crc.update(&DATA_MAGIC);
        data_file.write_all(&[DATA_VERSION])?;
        crc.update(&[DATA_VERSION]);
        data_offset += 5;

        for (pk, partition) in partitions {
            let partition_offset = data_offset;

            // Partition key
            let len_bytes = (pk.len() as u32).to_be_bytes();
            data_file.write_all(&len_bytes)?;
            crc.update(&len_bytes);
            data_file.write_all(pk)?;
            crc.update(pk);
            data_offset += 4 + pk.len() as u64;

            entries.push(TrieEntry {
                key: pk.clone(),
                data_offset: partition_offset,
            });

            stats.partition_count += 1;

            // Rows
            for (ck, row) in &partition.rows {
                data_offset += self.write_row(&mut data_file, ck, row, &mut crc, &mut stats)?;
            }

            // End of partition marker
            data_file.write_all(&[END_OF_PARTITION])?;
            crc.update(&[END_OF_PARTITION]);
            data_offset += 1;
        }

        let crc_val = crc.finalize();
        data_file.write_u32::<BigEndian>(crc_val)?;
        data_offset += 4;
        data_file.flush()?;
        stats.data_size = data_offset;

        Ok((stats, entries))
    }

    fn write_row<W: Write>(
        &self,
        w: &mut W,
        ck: &[u8],
        row: &Row,
        crc: &mut crc32fast::Hasher,
        stats: &mut super::writer::SSTableStats,
    ) -> io::Result<u64> {
        let mut written: u64 = 0;

        w.write_all(&[ROW_MARKER])?;
        crc.update(&[ROW_MARKER]);
        written += 1;

        let ck_len = (ck.len() as u32).to_be_bytes();
        w.write_all(&ck_len)?;
        crc.update(&ck_len);
        w.write_all(ck)?;
        crc.update(ck);
        written += 4 + ck.len() as u64;

        let mut flags: u8 = 0;
        if row.is_tombstone {
            flags |= 0x01;
        }
        if row.local_deletion_time.is_some() {
            flags |= 0x02;
        }
        w.write_all(&[flags])?;
        crc.update(&[flags]);
        written += 1;

        if let Some(ldt) = row.local_deletion_time {
            let ldt_bytes = ldt.to_be_bytes();
            w.write_all(&ldt_bytes)?;
            crc.update(&ldt_bytes);
            written += 4;
        }

        let cell_count = (row.cells.len() as u32).to_be_bytes();
        w.write_all(&cell_count)?;
        crc.update(&cell_count);
        written += 4;

        for cell in &row.cells {
            written += self.write_cell(w, cell, crc, stats)?;
        }

        stats.row_count += 1;
        Ok(written)
    }

    fn write_cell<W: Write>(
        &self,
        w: &mut W,
        cell: &Cell,
        crc: &mut crc32fast::Hasher,
        stats: &mut super::writer::SSTableStats,
    ) -> io::Result<u64> {
        let mut written: u64 = 0;

        let name = cell.column.as_bytes();
        let name_len = (name.len() as u16).to_be_bytes();
        w.write_all(&name_len)?;
        crc.update(&name_len);
        w.write_all(name)?;
        crc.update(name);
        written += 2 + name.len() as u64;

        let ts = cell.timestamp.to_be_bytes();
        w.write_all(&ts)?;
        crc.update(&ts);
        written += 8;

        let ttl = cell.ttl.to_be_bytes();
        w.write_all(&ttl)?;
        crc.update(&ttl);
        written += 4;

        let mut flags: u8 = 0;
        if cell.is_tombstone {
            flags |= 0x01;
        }
        if cell.value.is_some() {
            flags |= 0x02;
        }
        if cell.local_deletion_time.is_some() {
            flags |= 0x04;
        }
        w.write_all(&[flags])?;
        crc.update(&[flags]);
        written += 1;

        if let Some(ldt) = cell.local_deletion_time {
            let ldt_bytes = ldt.to_be_bytes();
            w.write_all(&ldt_bytes)?;
            crc.update(&ldt_bytes);
            written += 4;
        }

        if let Some(ref val) = cell.value {
            let val_len = (val.len() as u32).to_be_bytes();
            w.write_all(&val_len)?;
            crc.update(&val_len);
            w.write_all(val)?;
            crc.update(val);
            written += 4 + val.len() as u64;
        }

        stats.cell_count += 1;
        stats.min_timestamp = stats.min_timestamp.min(cell.timestamp);
        stats.max_timestamp = stats.max_timestamp.max(cell.timestamp);

        Ok(written)
    }

    fn write_partition_index(&self, entries: &[TrieEntry]) -> io::Result<()> {
        let path = self.descriptor.component_path(Component::Partitions);
        let mut file = BufWriter::new(File::create(&path)?);

        file.write_all(&BTI_TRIE_MAGIC)?;
        file.write_u8(BTI_TRIE_VERSION)?;

        // Build trie
        let mut root = TrieNode::new();
        for entry in entries {
            root.insert(&entry.key, entry.data_offset);
        }

        root.serialize(&mut file)?;
        file.flush()?;
        Ok(())
    }

    fn write_bloom_filter(&self, partitions: &[(Vec<u8>, PartitionData)]) -> io::Result<()> {
        let filter_path = self.descriptor.component_path(Component::Filter);
        let mut filter_file = BufWriter::new(File::create(&filter_path)?);
        filter_file.write_all(&FILTER_MAGIC)?;

        let mut bloom = BloomFilter::new(partitions.len().max(1), 0.01);
        for (pk, _) in partitions {
            bloom.add(pk);
        }
        bloom.serialize(&mut filter_file)?;
        filter_file.flush()?;
        Ok(())
    }

    fn write_statistics(&self, stats: &super::writer::SSTableStats) -> io::Result<()> {
        let path = self.descriptor.component_path(Component::Statistics);
        let json = serde_json::to_vec_pretty(stats)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        fs::write(&path, json)?;
        Ok(())
    }

    fn write_toc(&self) -> io::Result<()> {
        let toc_path = self.descriptor.component_path(Component::Toc);
        let components = [
            Component::Data,
            Component::Partitions,
            Component::Filter,
            Component::Statistics,
            Component::Toc,
        ];
        let content: String = components
            .iter()
            .map(|c| format!("{}-{}", self.descriptor.file_prefix(), c.extension()))
            .collect::<Vec<_>>()
            .join("\n");
        fs::write(&toc_path, content)?;
        Ok(())
    }
}

// ─── BTI Reader ────────────────────────────────────────────────────────────

/// Reader for the BTI SSTable format.
pub struct BtiReader {
    descriptor: SSTableDescriptor,
    trie: TrieNode,
    bloom: BloomFilter,
    stats: Option<super::reader::SSTableStats>,
}

impl BtiReader {
    /// Open a BTI-format SSTable.
    pub fn open(descriptor: SSTableDescriptor) -> io::Result<Self> {
        let trie = Self::load_trie_index(&descriptor)?;
        let bloom = Self::load_bloom_filter(&descriptor)?;
        let stats = Self::load_stats(&descriptor).ok();

        Ok(Self {
            descriptor,
            trie,
            bloom,
            stats,
        })
    }

    pub fn descriptor(&self) -> &SSTableDescriptor {
        &self.descriptor
    }

    pub fn stats(&self) -> Option<&super::reader::SSTableStats> {
        self.stats.as_ref()
    }

    pub fn generation(&self) -> SSTableId {
        self.descriptor.generation
    }

    pub fn might_contain_key(&self, partition_key: &[u8]) -> bool {
        self.bloom.might_contain(partition_key)
    }

    /// Read a single partition by key using trie index lookup.
    pub fn get_partition(&self, partition_key: &[u8]) -> io::Result<Option<PartitionData>> {
        if !self.bloom.might_contain(partition_key) {
            return Ok(None);
        }

        let offset = match self.trie.lookup(partition_key) {
            Some(o) => o,
            None => return Ok(None),
        };

        let data_path = self.descriptor.component_path(Component::Data);
        let mut reader = BufReader::new(File::open(&data_path)?);
        reader.seek(SeekFrom::Start(offset))?;

        // Read and verify partition key
        let pk_len = reader.read_u32::<BigEndian>()? as usize;
        let mut pk = vec![0u8; pk_len];
        reader.read_exact(&mut pk)?;
        if pk != partition_key {
            return Ok(None);
        }

        // Read rows
        let mut partition = PartitionData::new();
        loop {
            let marker = reader.read_u8()?;
            if marker == END_OF_PARTITION {
                break;
            }
            if marker != ROW_MARKER {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("unexpected marker: {marker:#x}"),
                ));
            }
            let row = super::reader::read_row_from_reader(&mut reader)?;
            partition.apply_row(row);
        }

        Ok(Some(partition))
    }

    /// Iterate all partitions.
    pub fn iter_partitions(&self) -> io::Result<Vec<(Vec<u8>, PartitionData)>> {
        let data_path = self.descriptor.component_path(Component::Data);
        let mut reader = BufReader::new(File::open(&data_path)?);
        reader.seek(SeekFrom::Start(5))?; // skip magic + version

        let mut result = Vec::new();
        let file_size = fs::metadata(&data_path)?.len();

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

            let mut partition = PartitionData::new();
            loop {
                let marker = reader.read_u8()?;
                if marker == END_OF_PARTITION {
                    break;
                }
                if marker != ROW_MARKER {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("unexpected marker: {marker:#x}"),
                    ));
                }
                let row = super::reader::read_row_from_reader(&mut reader)?;
                partition.apply_row(row);
            }

            result.push((pk, partition));
        }

        Ok(result)
    }

    fn load_trie_index(desc: &SSTableDescriptor) -> io::Result<TrieNode> {
        let path = desc.component_path(Component::Partitions);
        let mut reader = BufReader::new(File::open(&path)?);

        let mut magic = [0u8; 4];
        reader.read_exact(&mut magic)?;
        if magic != BTI_TRIE_MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid Partitions.db magic",
            ));
        }

        let version = reader.read_u8()?;
        if version != BTI_TRIE_VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unsupported trie version: {version}"),
            ));
        }

        TrieNode::deserialize(&mut reader)
    }

    fn load_bloom_filter(desc: &SSTableDescriptor) -> io::Result<BloomFilter> {
        let path = desc.component_path(Component::Filter);
        let mut reader = BufReader::new(File::open(&path)?);
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

    fn load_stats(desc: &SSTableDescriptor) -> io::Result<super::reader::SSTableStats> {
        let path = desc.component_path(Component::Statistics);
        let data = fs::read_to_string(&path)?;
        serde_json::from_str(&data).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
                        value: Some(format!("val_{i}_{j}").into_bytes()),
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

    #[test]
    fn bti_write_and_read_roundtrip() {
        let dir = TempDir::new().unwrap();
        let mut desc = SSTableDescriptor::new(dir.path(), "ks", "t1", 1);
        desc.format = SSTableFormat::Bti;

        let partitions = sample_partitions();
        let writer = BtiWriter::new(desc.clone());
        let stats = writer.write(&partitions).unwrap();
        assert_eq!(stats.partition_count, 10);
        assert_eq!(stats.row_count, 30);

        let reader = BtiReader::open(desc).unwrap();

        // Test specific partition
        let p5 = reader.get_partition(&[5]).unwrap().unwrap();
        assert_eq!(p5.rows.len(), 3);
        let row = p5.rows.get(&vec![0u8]).unwrap();
        assert_eq!(row.cells[0].value.as_deref(), Some(b"val_5_0".as_slice()));
    }

    #[test]
    fn bti_bloom_filter_rejects() {
        let dir = TempDir::new().unwrap();
        let mut desc = SSTableDescriptor::new(dir.path(), "ks", "t1", 1);
        desc.format = SSTableFormat::Bti;

        let partitions = sample_partitions();
        BtiWriter::new(desc.clone()).write(&partitions).unwrap();

        let reader = BtiReader::open(desc).unwrap();
        assert!(!reader.might_contain_key(&[255]));
    }

    #[test]
    fn bti_iter_all_partitions() {
        let dir = TempDir::new().unwrap();
        let mut desc = SSTableDescriptor::new(dir.path(), "ks", "t1", 1);
        desc.format = SSTableFormat::Bti;

        let partitions = sample_partitions();
        BtiWriter::new(desc.clone()).write(&partitions).unwrap();

        let reader = BtiReader::open(desc).unwrap();
        let all = reader.iter_partitions().unwrap();
        assert_eq!(all.len(), 10);
        for i in 0..10u8 {
            assert_eq!(all[i as usize].0, vec![i]);
        }
    }

    #[test]
    fn bti_missing_partition_returns_none() {
        let dir = TempDir::new().unwrap();
        let mut desc = SSTableDescriptor::new(dir.path(), "ks", "t1", 1);
        desc.format = SSTableFormat::Bti;

        BtiWriter::new(desc.clone())
            .write(&sample_partitions())
            .unwrap();

        let reader = BtiReader::open(desc).unwrap();
        assert!(reader.get_partition(&[200]).unwrap().is_none());
    }

    #[test]
    fn trie_serialize_roundtrip() {
        let mut root = TrieNode::new();
        root.insert(b"alpha", 100);
        root.insert(b"beta", 200);
        root.insert(b"alphabet", 300);

        let mut buf = Vec::new();
        root.serialize(&mut buf).unwrap();

        let deserialized = TrieNode::deserialize(&mut buf.as_slice()).unwrap();
        assert_eq!(deserialized.lookup(b"alpha"), Some(100));
        assert_eq!(deserialized.lookup(b"beta"), Some(200));
        assert_eq!(deserialized.lookup(b"alphabet"), Some(300));
        assert_eq!(deserialized.lookup(b"gamma"), None);
    }
}
