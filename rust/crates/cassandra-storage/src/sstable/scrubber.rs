// Licensed under Apache License, Version 2.0.

//! SSTable scrubber: recovers readable partitions from a potentially corrupt SSTable.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.db.compaction.Scrubber`

use std::fs::{self, File};
use std::io::{self, BufReader, Read, Seek, SeekFrom};

use byteorder::{BigEndian, ReadBytesExt};

use super::format::*;
use super::writer::SSTableWriter;
use crate::memtable::partition::PartitionData;

/// Result of a scrub operation.
#[derive(Debug)]
pub struct ScrubResult {
    pub partitions_recovered: u64,
    pub partitions_skipped: u64,
    pub bytes_read: u64,
    pub bytes_written: u64,
}

/// Scrubs an SSTable, recovering readable partitions and skipping corrupt ones.
pub struct SSTableScrubber;

impl SSTableScrubber {
    /// Scrub an SSTable: read partition by partition, skip corrupt ones,
    /// write recoverable partitions to a new SSTable.
    pub fn scrub(
        input: &SSTableDescriptor,
        output: &SSTableDescriptor,
    ) -> io::Result<ScrubResult> {
        let data_path = input.component_path(Component::Data);
        let file_size = fs::metadata(&data_path)?.len();
        let mut reader = BufReader::new(File::open(&data_path)?);

        // Skip 5-byte header (magic + version)
        reader.seek(SeekFrom::Start(5))?;

        let mut recovered: Vec<(Vec<u8>, PartitionData)> = Vec::new();
        let mut partitions_skipped: u64 = 0;

        loop {
            let pos = reader.stream_position()?;
            // Stop if we are at or past the trailing CRC (last 4 bytes)
            if pos + 4 >= file_size {
                break;
            }

            match Self::read_partition(&mut reader) {
                Ok(Some((pk, pd))) => {
                    recovered.push((pk, pd));
                }
                Ok(None) => break,
                Err(e) => {
                    eprintln!(
                        "scrub: skipping corrupt partition at offset {pos}: {e}"
                    );
                    partitions_skipped += 1;
                    // Try to find the next valid partition by scanning forward.
                    // Since we cannot reliably find the next partition boundary
                    // after corruption, break out of the loop.
                    break;
                }
            }
        }

        let bytes_read = file_size;
        let partitions_recovered = recovered.len() as u64;

        let writer = SSTableWriter::new(output.clone());
        let stats = writer.write(&recovered)?;

        Ok(ScrubResult {
            partitions_recovered,
            partitions_skipped,
            bytes_read,
            bytes_written: stats.data_size,
        })
    }

    /// Attempt to read one partition from the data file.
    /// Returns Ok(None) when there are no more partitions.
    fn read_partition<R: Read>(
        reader: &mut R,
    ) -> io::Result<Option<(Vec<u8>, PartitionData)>> {
        let pk_len = match reader.read_u32::<BigEndian>() {
            Ok(l) => l,
            Err(ref e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                return Ok(None);
            }
            Err(e) => return Err(e),
        };

        if pk_len == 0 {
            return Ok(None);
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
            let row = super::reader::read_row_from_reader(reader)?;
            partition.apply_row(row);
        }

        Ok(Some((pk, partition)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memtable::partition::{Cell, Row};
    use crate::sstable::reader::SSTableReader;
    use crate::sstable::writer::SSTableWriter;
    use tempfile::TempDir;

    fn sample_partitions() -> Vec<(Vec<u8>, PartitionData)> {
        let mut partitions = Vec::new();
        for i in 0..5u8 {
            let mut pd = PartitionData::new();
            pd.apply_row(Row {
                clustering_key: vec![i],
                cells: vec![Cell {
                    column: "c".to_string(),
                    value: Some(vec![i]),
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
    fn scrub_clean_sstable_all_recovered() {
        let dir = TempDir::new().unwrap();
        let input = SSTableDescriptor::new(dir.path(), "ks", "t1", 1);
        let output = SSTableDescriptor::new(dir.path(), "ks", "t1", 2);

        SSTableWriter::new(input.clone())
            .write(&sample_partitions())
            .unwrap();

        let result = SSTableScrubber::scrub(&input, &output).unwrap();
        assert_eq!(result.partitions_recovered, 5);
        assert_eq!(result.partitions_skipped, 0);
        assert!(result.bytes_written > 0);

        // Verify output is readable
        let reader = SSTableReader::open(output).unwrap();
        let all = reader.iter_partitions().unwrap();
        assert_eq!(all.len(), 5);
    }

    #[test]
    fn scrub_truncated_last_partition() {
        let dir = TempDir::new().unwrap();
        let input = SSTableDescriptor::new(dir.path(), "ks", "t1", 1);
        let output = SSTableDescriptor::new(dir.path(), "ks", "t1", 2);

        SSTableWriter::new(input.clone())
            .write(&sample_partitions())
            .unwrap();

        // Truncate Data.db: remove some bytes from the end to corrupt
        // the last partition. We keep enough to have valid earlier partitions.
        let data_path = input.component_path(Component::Data);
        let data = fs::read(&data_path).unwrap();
        // Truncate off the CRC and some partition data
        let truncated_len = data.len() - 20;
        fs::write(&data_path, &data[..truncated_len]).unwrap();

        let result = SSTableScrubber::scrub(&input, &output).unwrap();
        // Some partitions should be recovered, at least one skipped
        assert!(result.partitions_recovered > 0);
        assert!(
            result.partitions_recovered < 5
                || result.partitions_skipped > 0
        );
    }

    #[test]
    fn scrub_empty_sstable() {
        let dir = TempDir::new().unwrap();
        let input = SSTableDescriptor::new(dir.path(), "ks", "t1", 1);
        let output = SSTableDescriptor::new(dir.path(), "ks", "t1", 2);

        SSTableWriter::new(input.clone()).write(&[]).unwrap();

        let result = SSTableScrubber::scrub(&input, &output).unwrap();
        assert_eq!(result.partitions_recovered, 0);
        assert_eq!(result.partitions_skipped, 0);
    }
}
