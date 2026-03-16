// Licensed under Apache License, Version 2.0.

//! SSTable writer: serializes memtable partitions to on-disk SSTable components.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.io.sstable.format.big.BigTableWriter`
//!
//! ## Data.db Format (simplified big-compatible)
//!
//! ```text
//! [magic: 4B][version: 1B]
//! For each partition:
//!   [pk_len: u32][partition_key: bytes]
//!   For each row:
//!     [ROW_MARKER: 1B][ck_len: u32][clustering_key: bytes]
//!     [cell_count: u32]
//!     For each cell:
//!       [col_name_len: u16][col_name: bytes]
//!       [timestamp: i64][ttl: i32][flags: u8]
//!       [value_len: u32][value: bytes]  (if not tombstone)
//!   [END_OF_PARTITION: 1B]
//! [CRC32: u32] (over all data)
//! ```

use std::fs::{self, File};
use std::io::{self, BufWriter, Write};

use byteorder::{BigEndian, WriteBytesExt};
use crc32fast::Hasher;

use super::bloom::BloomFilter;
use super::format::*;
use crate::memtable::partition::{Cell, PartitionData, Row};

/// Index entry: partition key → offset in Data.db.
#[derive(Debug, Clone)]
struct IndexEntry {
    partition_key: Vec<u8>,
    data_offset: u64,
}

/// Statistics collected during SSTable write.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SSTableStats {
    pub partition_count: u64,
    pub row_count: u64,
    pub cell_count: u64,
    pub min_timestamp: i64,
    pub max_timestamp: i64,
    pub data_size: u64,
    pub index_size: u64,
}

/// Writes a set of sorted partitions to SSTable component files.
pub struct SSTableWriter {
    descriptor: SSTableDescriptor,
}

impl SSTableWriter {
    pub fn new(descriptor: SSTableDescriptor) -> Self {
        Self { descriptor }
    }

    /// Write all partitions to disk. Partitions MUST be sorted by key.
    /// Returns statistics about the written SSTable.
    pub fn write(&self, partitions: &[(Vec<u8>, PartitionData)]) -> io::Result<SSTableStats> {
        fs::create_dir_all(&self.descriptor.directory)?;

        let mut stats = SSTableStats {
            partition_count: 0,
            row_count: 0,
            cell_count: 0,
            min_timestamp: i64::MAX,
            max_timestamp: i64::MIN,
            data_size: 0,
            index_size: 0,
        };

        // Create bloom filter
        let mut bloom = BloomFilter::new(partitions.len().max(1), 0.01);
        let mut index_entries = Vec::new();
        let mut summary_entries = Vec::new();

        // ── Write Data.db ─────────────────────────────────────────
        let data_path = self.descriptor.component_path(Component::Data);
        let mut data_file = BufWriter::new(File::create(&data_path)?);
        let mut data_crc = Hasher::new();
        let mut data_offset: u64 = 0;

        // Header
        write_and_hash(&mut data_file, &DATA_MAGIC, &mut data_crc)?;
        write_and_hash(&mut data_file, &[DATA_VERSION], &mut data_crc)?;
        data_offset += 5;

        for (i, (pk, partition)) in partitions.iter().enumerate() {
            let partition_offset = data_offset;

            // Partition key
            data_offset += write_bytes_and_hash(&mut data_file, pk, &mut data_crc)?;

            bloom.add(pk);
            index_entries.push(IndexEntry {
                partition_key: pk.clone(),
                data_offset: partition_offset,
            });

            // Summary: sample every 128th partition
            if i % 128 == 0 {
                summary_entries.push(IndexEntry {
                    partition_key: pk.clone(),
                    data_offset: partition_offset,
                });
            }

            stats.partition_count += 1;

            // Rows
            for (ck, row) in &partition.rows {
                data_offset += write_row(&mut data_file, ck, row, &mut data_crc, &mut stats)?;
            }

            // End of partition marker
            write_and_hash(&mut data_file, &[END_OF_PARTITION], &mut data_crc)?;
            data_offset += 1;
        }

        // Write CRC at end of data file
        let crc = data_crc.finalize();
        data_file.write_u32::<BigEndian>(crc)?;
        data_offset += 4;
        data_file.flush()?;
        stats.data_size = data_offset;

        // ── Write Index.db ────────────────────────────────────────
        let index_path = self.descriptor.component_path(Component::Index);
        let mut index_file = BufWriter::new(File::create(&index_path)?);
        index_file.write_all(&INDEX_MAGIC)?;
        let mut idx_size: u64 = 4;

        for entry in &index_entries {
            index_file.write_u32::<BigEndian>(entry.partition_key.len() as u32)?;
            index_file.write_all(&entry.partition_key)?;
            index_file.write_u64::<BigEndian>(entry.data_offset)?;
            idx_size += 4 + entry.partition_key.len() as u64 + 8;
        }
        index_file.flush()?;
        stats.index_size = idx_size;

        // ── Write Filter.db ───────────────────────────────────────
        let filter_path = self.descriptor.component_path(Component::Filter);
        let mut filter_file = BufWriter::new(File::create(&filter_path)?);
        filter_file.write_all(&FILTER_MAGIC)?;
        bloom.serialize(&mut filter_file)?;
        filter_file.flush()?;

        // ── Write Summary.db ──────────────────────────────────────
        let summary_path = self.descriptor.component_path(Component::Summary);
        let mut summary_file = BufWriter::new(File::create(&summary_path)?);
        summary_file.write_u32::<BigEndian>(summary_entries.len() as u32)?;
        for entry in &summary_entries {
            summary_file.write_u32::<BigEndian>(entry.partition_key.len() as u32)?;
            summary_file.write_all(&entry.partition_key)?;
            summary_file.write_u64::<BigEndian>(entry.data_offset)?;
        }
        summary_file.flush()?;

        // ── Write Statistics.db ───────────────────────────────────
        let stats_path = self.descriptor.component_path(Component::Statistics);
        let stats_json = serde_json::to_vec_pretty(&stats)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        fs::write(&stats_path, stats_json)?;

        // ── Write TOC.txt ─────────────────────────────────────────
        let toc_path = self.descriptor.component_path(Component::Toc);
        let toc_content: String = Component::all()
            .iter()
            .map(|c| format!("{}-{}", self.descriptor.file_prefix(), c.extension()))
            .collect::<Vec<_>>()
            .join("\n");
        fs::write(&toc_path, toc_content)?;

        Ok(stats)
    }
}

// ─── Helper write functions ────────────────────────────────────────────────

fn write_and_hash<W: Write>(w: &mut W, data: &[u8], crc: &mut Hasher) -> io::Result<()> {
    w.write_all(data)?;
    crc.update(data);
    Ok(())
}

fn write_bytes_and_hash<W: Write>(w: &mut W, data: &[u8], crc: &mut Hasher) -> io::Result<u64> {
    let len_bytes = (data.len() as u32).to_be_bytes();
    w.write_all(&len_bytes)?;
    crc.update(&len_bytes);
    w.write_all(data)?;
    crc.update(data);
    Ok(4 + data.len() as u64)
}

fn write_row<W: Write>(
    w: &mut W,
    ck: &[u8],
    row: &Row,
    crc: &mut Hasher,
    stats: &mut SSTableStats,
) -> io::Result<u64> {
    let mut written: u64 = 0;

    // Row marker
    write_and_hash(w, &[ROW_MARKER], crc)?;
    written += 1;

    // Clustering key
    written += write_bytes_and_hash(w, ck, crc)?;

    // Row flags: bit 0 = is_tombstone, bit 1 = has_local_deletion_time
    let mut flags: u8 = 0;
    if row.is_tombstone {
        flags |= 0x01;
    }
    if row.local_deletion_time.is_some() {
        flags |= 0x02;
    }
    write_and_hash(w, &[flags], crc)?;
    written += 1;

    if let Some(ldt) = row.local_deletion_time {
        let ldt_bytes = ldt.to_be_bytes();
        write_and_hash(w, &ldt_bytes, crc)?;
        written += 4;
    }

    // Cell count
    let cell_count = (row.cells.len() as u32).to_be_bytes();
    write_and_hash(w, &cell_count, crc)?;
    written += 4;

    // Cells
    for cell in &row.cells {
        written += write_cell(w, cell, crc, stats)?;
    }

    stats.row_count += 1;
    Ok(written)
}

fn write_cell<W: Write>(
    w: &mut W,
    cell: &Cell,
    crc: &mut Hasher,
    stats: &mut SSTableStats,
) -> io::Result<u64> {
    let mut written: u64 = 0;

    // Column name (u16 length prefix)
    let name_bytes = cell.column.as_bytes();
    let name_len = (name_bytes.len() as u16).to_be_bytes();
    write_and_hash(w, &name_len, crc)?;
    write_and_hash(w, name_bytes, crc)?;
    written += 2 + name_bytes.len() as u64;

    // Timestamp
    let ts_bytes = cell.timestamp.to_be_bytes();
    write_and_hash(w, &ts_bytes, crc)?;
    written += 8;

    // TTL
    let ttl_bytes = cell.ttl.to_be_bytes();
    write_and_hash(w, &ttl_bytes, crc)?;
    written += 4;

    // Cell flags: bit 0 = is_tombstone, bit 1 = has_value, bit 2 = has_ldt
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
    write_and_hash(w, &[flags], crc)?;
    written += 1;

    if let Some(ldt) = cell.local_deletion_time {
        let ldt_bytes = ldt.to_be_bytes();
        write_and_hash(w, &ldt_bytes, crc)?;
        written += 4;
    }

    // Value
    if let Some(ref val) = cell.value {
        written += write_bytes_and_hash(w, val, crc)?;
    }

    // Update stats
    stats.cell_count += 1;
    stats.min_timestamp = stats.min_timestamp.min(cell.timestamp);
    stats.max_timestamp = stats.max_timestamp.max(cell.timestamp);

    Ok(written)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn sample_partitions() -> Vec<(Vec<u8>, PartitionData)> {
        let mut partitions = Vec::new();

        for i in 0..5u8 {
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
    fn write_creates_all_components() {
        let dir = TempDir::new().unwrap();
        let desc = SSTableDescriptor::new(dir.path(), "ks", "t1", 1);
        let writer = SSTableWriter::new(desc.clone());

        let partitions = sample_partitions();
        let stats = writer.write(&partitions).unwrap();

        assert!(desc.is_complete());
        assert_eq!(stats.partition_count, 5);
        assert_eq!(stats.row_count, 15);
        assert_eq!(stats.cell_count, 15);
        assert!(stats.data_size > 0);
        assert!(stats.index_size > 0);
    }

    #[test]
    fn write_empty_sstable() {
        let dir = TempDir::new().unwrap();
        let desc = SSTableDescriptor::new(dir.path(), "ks", "t1", 0);
        let writer = SSTableWriter::new(desc);

        let stats = writer.write(&[]).unwrap();
        assert_eq!(stats.partition_count, 0);
        assert_eq!(stats.row_count, 0);
    }

    #[test]
    fn statistics_json_valid() {
        let dir = TempDir::new().unwrap();
        let desc = SSTableDescriptor::new(dir.path(), "ks", "t1", 1);
        let writer = SSTableWriter::new(desc.clone());

        let partitions = sample_partitions();
        writer.write(&partitions).unwrap();

        let stats_path = desc.component_path(Component::Statistics);
        let stats_json = fs::read_to_string(stats_path).unwrap();
        let stats: SSTableStats = serde_json::from_str(&stats_json).unwrap();
        assert_eq!(stats.partition_count, 5);
    }
}
