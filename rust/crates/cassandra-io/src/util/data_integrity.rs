// Licensed under Apache License, Version 2.0.

//! Checksum metadata for data integrity verification of chunked files.
//!
//! Stores one CRC32 checksum per fixed-size chunk. The metadata can be
//! serialized to a binary format (big-endian) and read back for verification
//! during reads.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.io.util.DataIntegrityMetadata`

use std::io::{self, Read, Write};

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

// ---------------------------------------------------------------------------
// DataIntegrityMetadata
// ---------------------------------------------------------------------------

/// Per-chunk CRC32 checksum metadata for a file.
///
/// Binary format (all values big-endian):
/// ```text
/// [chunk_size: u32] [count: u32] [checksum_0: u32] ... [checksum_N-1: u32]
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataIntegrityMetadata {
    /// Size in bytes of each chunk (the last chunk may be shorter).
    chunk_size: u32,
    /// One CRC32 checksum per chunk, in order.
    checksums: Vec<u32>,
}

impl DataIntegrityMetadata {
    /// Creates new metadata with the given chunk size and checksums.
    pub fn new(chunk_size: u32, checksums: Vec<u32>) -> Self {
        Self {
            chunk_size,
            checksums,
        }
    }

    /// Serializes the metadata to `writer` in big-endian binary format.
    pub fn write_to(&self, writer: &mut impl Write) -> io::Result<()> {
        writer.write_u32::<BigEndian>(self.chunk_size)?;
        writer.write_u32::<BigEndian>(self.checksums.len() as u32)?;
        for &cs in &self.checksums {
            writer.write_u32::<BigEndian>(cs)?;
        }
        Ok(())
    }

    /// Deserializes metadata from `reader` (inverse of [`write_to`](Self::write_to)).
    pub fn read_from(reader: &mut impl Read) -> io::Result<Self> {
        let chunk_size = reader.read_u32::<BigEndian>()?;
        let count = reader.read_u32::<BigEndian>()? as usize;
        let mut checksums = Vec::with_capacity(count);
        for _ in 0..count {
            checksums.push(reader.read_u32::<BigEndian>()?);
        }
        Ok(Self {
            chunk_size,
            checksums,
        })
    }

    /// Returns the stored checksum for the given chunk index, or `None` if
    /// the index is out of range.
    pub fn checksum_for_chunk(&self, chunk_index: usize) -> Option<u32> {
        self.checksums.get(chunk_index).copied()
    }

    /// Returns the number of chunks (i.e. stored checksums).
    pub fn chunk_count(&self) -> usize {
        self.checksums.len()
    }

    /// Returns the chunk size in bytes.
    pub fn chunk_size(&self) -> u32 {
        self.chunk_size
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_round_trip_serialization() {
        let checksums = vec![0xAABBCCDD, 0x11223344, 0xDEADBEEF];
        let meta = DataIntegrityMetadata::new(4096, checksums.clone());

        let mut buf = Vec::new();
        meta.write_to(&mut buf).unwrap();

        let mut cursor = Cursor::new(&buf);
        let restored = DataIntegrityMetadata::read_from(&mut cursor).unwrap();

        assert_eq!(meta, restored);
        assert_eq!(restored.chunk_size(), 4096);
        assert_eq!(restored.chunk_count(), 3);
    }

    #[test]
    fn test_empty_checksums_round_trip() {
        let meta = DataIntegrityMetadata::new(1024, vec![]);

        let mut buf = Vec::new();
        meta.write_to(&mut buf).unwrap();

        let mut cursor = Cursor::new(&buf);
        let restored = DataIntegrityMetadata::read_from(&mut cursor).unwrap();

        assert_eq!(restored.chunk_count(), 0);
        assert_eq!(restored.chunk_size(), 1024);
    }

    #[test]
    fn test_checksum_for_chunk_valid_index() {
        let meta = DataIntegrityMetadata::new(512, vec![100, 200, 300]);
        assert_eq!(meta.checksum_for_chunk(0), Some(100));
        assert_eq!(meta.checksum_for_chunk(1), Some(200));
        assert_eq!(meta.checksum_for_chunk(2), Some(300));
    }

    #[test]
    fn test_checksum_for_chunk_out_of_range() {
        let meta = DataIntegrityMetadata::new(512, vec![100]);
        assert_eq!(meta.checksum_for_chunk(1), None);
        assert_eq!(meta.checksum_for_chunk(999), None);
    }

    #[test]
    fn test_binary_format_layout() {
        let meta = DataIntegrityMetadata::new(256, vec![0x01020304]);

        let mut buf = Vec::new();
        meta.write_to(&mut buf).unwrap();

        // chunk_size(4) + count(4) + 1 checksum(4) = 12 bytes
        assert_eq!(buf.len(), 12);

        // chunk_size = 256 = 0x00000100
        assert_eq!(&buf[0..4], &[0x00, 0x00, 0x01, 0x00]);
        // count = 1
        assert_eq!(&buf[4..8], &[0x00, 0x00, 0x00, 0x01]);
        // checksum = 0x01020304
        assert_eq!(&buf[8..12], &[0x01, 0x02, 0x03, 0x04]);
    }
}
