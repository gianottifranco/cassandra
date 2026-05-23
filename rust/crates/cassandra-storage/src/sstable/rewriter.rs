// Licensed under Apache License, Version 2.0.

//! SSTable rewriter: reads partitions from an input and writes to new SSTables.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.io.sstable.SSTableRewriter`
//! - `org.apache.cassandra.db.compaction.writers.CompactionAwareWriter`
//!
//! Supports size-based splitting: when `max_sstable_size` is set, the rewriter
//! starts a new SSTable when the cumulative estimated size exceeds the threshold.

use std::io;

use super::format::SSTableDescriptor;
use super::writer::{SSTableStats, SSTableWriter};
use crate::memtable::partition::PartitionData;

/// Configuration for the rewriter.
#[derive(Debug, Clone, Default)]
pub struct RewriterConfig {
    /// Maximum estimated size per output SSTable in bytes.
    /// `None` means write everything into a single SSTable.
    pub max_sstable_size: Option<u64>,
}

/// Result of a rewrite operation.
#[derive(Debug)]
pub struct RewriteResult {
    /// Descriptors of all output SSTables.
    pub output_descriptors: Vec<SSTableDescriptor>,
    /// Stats for each output SSTable.
    pub stats: Vec<SSTableStats>,
}

/// Estimate the byte size of a partition for split-size accounting.
fn estimate_partition_size(key: &[u8], data: &PartitionData) -> u64 {
    let mut size = key.len() as u64;
    for row in data.rows.values() {
        // Clustering key + overhead per row (marker, flags, cell count).
        size += row.clustering_key.len() as u64 + 10;
        for cell in &row.cells {
            // Column name + timestamp + ttl + flags + value.
            size += cell.column.len() as u64 + 16;
            if let Some(ref v) = cell.value {
                size += v.len() as u64 + 4; // value + length prefix
            }
        }
    }
    size
}

/// Generate a new descriptor with incremented generation.
fn next_descriptor(base: &SSTableDescriptor, generation_offset: u64) -> SSTableDescriptor {
    SSTableDescriptor {
        directory: base.directory.clone(),
        keyspace: base.keyspace.clone(),
        table: base.table.clone(),
        generation: base.generation + generation_offset,
        format: base.format,
    }
}

/// Rewrite partitions from an input iterator to one or more output SSTables.
pub fn rewrite(
    input: impl Iterator<Item = (Vec<u8>, PartitionData)>,
    output_descriptor: &SSTableDescriptor,
    config: &RewriterConfig,
) -> io::Result<RewriteResult> {
    let mut result = RewriteResult {
        output_descriptors: Vec::new(),
        stats: Vec::new(),
    };

    let mut current_batch: Vec<(Vec<u8>, PartitionData)> = Vec::new();
    let mut current_size: u64 = 0;
    let mut generation_offset: u64 = 0;

    for (key, data) in input {
        let partition_size = estimate_partition_size(&key, &data);

        // Check if adding this partition would exceed the limit.
        if let Some(max_size) = config.max_sstable_size {
            if !current_batch.is_empty() && current_size + partition_size > max_size {
                // Flush current batch.
                let desc = next_descriptor(output_descriptor, generation_offset);
                let stats = flush_batch(&desc, &current_batch)?;
                result.output_descriptors.push(desc);
                result.stats.push(stats);
                current_batch.clear();
                current_size = 0;
                generation_offset += 1;
            }
        }

        current_size += partition_size;
        current_batch.push((key, data));
    }

    // Flush remaining partitions.
    if !current_batch.is_empty() {
        let desc = next_descriptor(output_descriptor, generation_offset);
        let stats = flush_batch(&desc, &current_batch)?;
        result.output_descriptors.push(desc);
        result.stats.push(stats);
    }

    Ok(result)
}

fn flush_batch(
    desc: &SSTableDescriptor,
    partitions: &[(Vec<u8>, PartitionData)],
) -> io::Result<SSTableStats> {
    let writer = SSTableWriter::new(desc.clone());
    writer.write(partitions)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memtable::partition::{Cell, Row};
    use crate::sstable::reader::SSTableReader;
    use tempfile::TempDir;

    fn make_partitions(count: u8) -> Vec<(Vec<u8>, PartitionData)> {
        let mut partitions = Vec::new();
        for i in 0..count {
            let mut pd = PartitionData::new();
            pd.apply_row(Row {
                clustering_key: vec![0],
                cells: vec![Cell {
                    column: "name".to_string(),
                    value: Some(format!("value_{i}").into_bytes()),
                    timestamp: 1000 + i as i64,
                    ttl: 0,
                    local_deletion_time: None,
                    is_tombstone: false,
                }],
                is_tombstone: false,
                local_deletion_time: None,
            });
            partitions.push((vec![i], pd));
        }
        partitions
    }

    #[test]
    fn rewrite_preserves_all_data() {
        let dir = TempDir::new().unwrap();
        let desc = SSTableDescriptor::new(dir.path(), "ks", "t1", 1);

        let partitions = make_partitions(5);
        let config = RewriterConfig::default();

        let result = rewrite(partitions.clone().into_iter(), &desc, &config).unwrap();
        assert_eq!(result.output_descriptors.len(), 1);
        assert_eq!(result.stats.len(), 1);
        assert_eq!(result.stats[0].partition_count, 5);
        assert_eq!(result.stats[0].row_count, 5);

        // Verify data by reading back.
        let reader = SSTableReader::open(result.output_descriptors[0].clone()).unwrap();
        let read_back = reader.iter_partitions().unwrap();
        assert_eq!(read_back.len(), 5);
        for (i, (key, _)) in read_back.iter().enumerate() {
            assert_eq!(*key, vec![i as u8]);
        }
    }

    #[test]
    fn size_based_splitting_creates_multiple_outputs() {
        let dir = TempDir::new().unwrap();
        let desc = SSTableDescriptor::new(dir.path(), "ks", "t1", 100);

        let partitions = make_partitions(10);

        // Set a very small max size to force splitting.
        let config = RewriterConfig {
            max_sstable_size: Some(50),
        };

        let result = rewrite(partitions.into_iter(), &desc, &config).unwrap();

        // With such a small limit, we should get multiple output SSTables.
        assert!(
            result.output_descriptors.len() > 1,
            "expected multiple outputs, got {}",
            result.output_descriptors.len()
        );

        // Verify generations are sequential.
        for (i, d) in result.output_descriptors.iter().enumerate() {
            assert_eq!(d.generation, 100 + i as u64);
        }

        // Verify total partition count across all outputs.
        let total_partitions: u64 = result.stats.iter().map(|s| s.partition_count).sum();
        assert_eq!(total_partitions, 10);

        // Verify each output is readable.
        for d in &result.output_descriptors {
            let reader = SSTableReader::open(d.clone()).unwrap();
            let parts = reader.iter_partitions().unwrap();
            assert!(!parts.is_empty());
        }
    }

    #[test]
    fn rewrite_empty_input() {
        let dir = TempDir::new().unwrap();
        let desc = SSTableDescriptor::new(dir.path(), "ks", "t1", 1);
        let config = RewriterConfig::default();

        let result = rewrite(std::iter::empty(), &desc, &config).unwrap();
        assert!(result.output_descriptors.is_empty());
        assert!(result.stats.is_empty());
    }

    #[test]
    fn rewrite_single_partition() {
        let dir = TempDir::new().unwrap();
        let desc = SSTableDescriptor::new(dir.path(), "ks", "t1", 1);
        let config = RewriterConfig::default();

        let partitions = make_partitions(1);
        let result = rewrite(partitions.into_iter(), &desc, &config).unwrap();

        assert_eq!(result.output_descriptors.len(), 1);
        assert_eq!(result.stats[0].partition_count, 1);
        assert_eq!(result.stats[0].row_count, 1);
        assert_eq!(result.stats[0].cell_count, 1);
    }
}
