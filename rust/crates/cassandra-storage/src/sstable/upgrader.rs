// Licensed under Apache License, Version 2.0.

//! SSTable upgrader: rewrites an SSTable to the current format.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.io.sstable.format.SSTableWriter`
//! - `org.apache.cassandra.db.compaction.Upgrader`

use std::io;

use super::format::{Component, SSTableDescriptor, SSTableFormat};
use super::reader::SSTableReader;
use super::writer::SSTableWriter;

/// Result of an upgrade operation.
#[derive(Debug)]
pub struct UpgradeResult {
    pub partitions_written: u64,
    pub input_data_size: u64,
    pub output_data_size: u64,
    pub format: SSTableFormat,
}

/// Upgrades an SSTable by reading and rewriting it in the current format.
pub struct SSTableUpgrader;

impl SSTableUpgrader {
    /// Upgrade an SSTable: read all partitions from input, write them to output.
    ///
    /// Returns an error if input and output share the same generation and
    /// directory (would overwrite the source).
    pub fn upgrade(
        input: &SSTableDescriptor,
        output: &SSTableDescriptor,
    ) -> io::Result<UpgradeResult> {
        // Guard against overwriting the source
        if input.generation == output.generation && input.directory == output.directory {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "input and output descriptors must differ in generation or \
                 directory to avoid overwriting the source SSTable",
            ));
        }

        let input_data_size = std::fs::metadata(input.component_path(Component::Data))
            .map(|m| m.len())
            .unwrap_or(0);

        let reader = SSTableReader::open(input.clone())?;
        let partitions = reader.iter_partitions()?;

        let writer = SSTableWriter::new(output.clone());
        let stats = writer.write(&partitions)?;

        Ok(UpgradeResult {
            partitions_written: stats.partition_count,
            input_data_size,
            output_data_size: stats.data_size,
            format: output.format,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memtable::partition::{Cell, PartitionData, Row};
    use crate::sstable::reader::SSTableReader;
    use crate::sstable::writer::SSTableWriter;
    use tempfile::TempDir;

    fn sample_partitions() -> Vec<(Vec<u8>, PartitionData)> {
        let mut partitions = Vec::new();
        for i in 0..4u8 {
            let mut pd = PartitionData::new();
            pd.apply_row(Row {
                clustering_key: vec![i],
                cells: vec![Cell {
                    column: "c".to_string(),
                    value: Some(vec![i, i + 1]),
                    timestamp: 2000 + i as i64,
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
    fn upgrade_preserves_all_data() {
        let dir = TempDir::new().unwrap();
        let input_desc = SSTableDescriptor::new(dir.path(), "ks", "t1", 1);
        let output_desc = SSTableDescriptor::new(dir.path(), "ks", "t1", 2);

        let original = sample_partitions();
        SSTableWriter::new(input_desc.clone())
            .write(&original)
            .unwrap();

        let result = SSTableUpgrader::upgrade(&input_desc, &output_desc).unwrap();
        assert_eq!(result.partitions_written, 4);
        assert!(result.input_data_size > 0);
        assert!(result.output_data_size > 0);

        // Verify roundtrip: read from output and compare
        let reader = SSTableReader::open(output_desc).unwrap();
        let upgraded = reader.iter_partitions().unwrap();
        assert_eq!(upgraded.len(), original.len());
        for (i, (pk, pd)) in upgraded.iter().enumerate() {
            assert_eq!(pk, &original[i].0);
            assert_eq!(pd.rows.len(), original[i].1.rows.len());
        }
    }

    #[test]
    fn upgrade_empty_sstable() {
        let dir = TempDir::new().unwrap();
        let input_desc = SSTableDescriptor::new(dir.path(), "ks", "t1", 1);
        let output_desc = SSTableDescriptor::new(dir.path(), "ks", "t1", 2);

        SSTableWriter::new(input_desc.clone()).write(&[]).unwrap();

        let result = SSTableUpgrader::upgrade(&input_desc, &output_desc).unwrap();
        assert_eq!(result.partitions_written, 0);
    }

    #[test]
    fn upgrade_rejects_same_generation_and_directory() {
        let dir = TempDir::new().unwrap();
        let input_desc = SSTableDescriptor::new(dir.path(), "ks", "t1", 1);
        let output_desc = SSTableDescriptor::new(dir.path(), "ks", "t1", 1);

        SSTableWriter::new(input_desc.clone())
            .write(&sample_partitions())
            .unwrap();

        let err = SSTableUpgrader::upgrade(&input_desc, &output_desc).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        assert!(err.to_string().contains("generation"));
    }
}
