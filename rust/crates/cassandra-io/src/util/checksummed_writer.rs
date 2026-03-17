// Licensed under Apache License, Version 2.0.

//! Sequential writer that computes per-chunk CRC32 checksums.
//!
//! Wraps a [`SequentialWriter`] and maintains a running CRC32 hash for every
//! fixed-size chunk of data. On [`finish`](ChecksummedSequentialWriter::finish)
//! the accumulated checksums are persisted as a
//! [`DataIntegrityMetadata`](crate::util::data_integrity::DataIntegrityMetadata)
//! file alongside the data.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.io.util.ChecksummedSequentialWriter`

use std::fs::File;
use std::io::{self, Write};
use std::path::Path;

use crate::util::data_integrity::DataIntegrityMetadata;
use crate::util::sequential_writer::SequentialWriter;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const DEFAULT_CHUNK_SIZE: u32 = 65_536;

// ---------------------------------------------------------------------------
// ChecksummedSequentialWriter
// ---------------------------------------------------------------------------

/// A sequential writer that accumulates one CRC32 checksum per fixed-size
/// chunk of written data.
pub struct ChecksummedSequentialWriter {
    inner: SequentialWriter,
    hasher: crc32fast::Hasher,
    chunk_size: u32,
    current_chunk_pos: u32,
    checksums: Vec<u32>,
}

impl ChecksummedSequentialWriter {
    /// Wraps an existing [`SequentialWriter`] with CRC32 checksum tracking.
    ///
    /// `chunk_size` controls how many bytes belong to each checksum. Pass `0`
    /// to use the default of 64 KiB.
    pub fn new(writer: SequentialWriter, chunk_size: u32) -> Self {
        let cs = if chunk_size == 0 {
            DEFAULT_CHUNK_SIZE
        } else {
            chunk_size
        };
        Self {
            inner: writer,
            hasher: crc32fast::Hasher::new(),
            chunk_size: cs,
            current_chunk_pos: 0,
            checksums: Vec::new(),
        }
    }

    /// Total bytes written (delegated to inner writer).
    pub fn position(&self) -> u64 {
        self.inner.position()
    }

    /// Finalizes checksums and writes them to `checksum_path`, then finishes
    /// the inner writer.
    pub fn finish(mut self, checksum_path: impl AsRef<Path>) -> io::Result<()> {
        self.flush()?;

        // Finalize partial last chunk.
        if self.current_chunk_pos > 0 {
            let hash = self.hasher.finalize();
            self.checksums.push(hash);
        }

        // Finish the data file first.
        self.inner.finish()?;

        // Write checksum metadata.
        let meta = DataIntegrityMetadata::new(self.chunk_size, self.checksums);
        let mut file = File::create(checksum_path)?;
        meta.write_to(&mut file)?;
        file.sync_all()?;

        Ok(())
    }

    // -----------------------------------------------------------------------
    // Internal
    // -----------------------------------------------------------------------

    /// Feeds `data` to the hasher, emitting completed chunk checksums along the
    /// way.
    fn update_checksums(&mut self, data: &[u8]) {
        let mut offset = 0;
        while offset < data.len() {
            let remaining_in_chunk = self.chunk_size - self.current_chunk_pos;
            let to_hash = remaining_in_chunk.min((data.len() - offset) as u32) as usize;
            self.hasher.update(&data[offset..offset + to_hash]);
            self.current_chunk_pos += to_hash as u32;
            offset += to_hash;

            if self.current_chunk_pos >= self.chunk_size {
                let hash = self.hasher.clone().finalize();
                self.checksums.push(hash);
                self.hasher.reset();
                self.current_chunk_pos = 0;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// std::io::Write
// ---------------------------------------------------------------------------

impl Write for ChecksummedSequentialWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let n = self.inner.write(buf)?;
        self.update_checksums(&buf[..n]);
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Cursor, Read};

    /// Helper: compute CRC32 of a byte slice.
    fn crc32(data: &[u8]) -> u32 {
        let mut h = crc32fast::Hasher::new();
        h.update(data);
        h.finalize()
    }

    #[test]
    fn test_single_chunk_checksum() {
        let dir = tempfile::tempdir().unwrap();
        let data_path = dir.path().join("data.bin");
        let crc_path = dir.path().join("data.crc");

        let data = vec![0xABu8; 100];
        let writer = SequentialWriter::new(&data_path).unwrap();
        let mut cw = ChecksummedSequentialWriter::new(writer, 65536);
        cw.write_all(&data).unwrap();
        cw.finish(&crc_path).unwrap();

        // The entire write fits in a single (partial) chunk.
        let mut crc_bytes = Vec::new();
        File::open(&crc_path)
            .unwrap()
            .read_to_end(&mut crc_bytes)
            .unwrap();
        let meta = DataIntegrityMetadata::read_from(&mut Cursor::new(&crc_bytes)).unwrap();

        assert_eq!(meta.chunk_count(), 1);
        assert_eq!(meta.chunk_size(), 65536);
        assert_eq!(meta.checksum_for_chunk(0), Some(crc32(&data)));
    }

    #[test]
    fn test_multiple_chunks_with_partial_final() {
        let dir = tempfile::tempdir().unwrap();
        let data_path = dir.path().join("multi.bin");
        let crc_path = dir.path().join("multi.crc");

        // chunk_size = 32, write 80 bytes => 2 full + 1 partial (16 bytes).
        let data: Vec<u8> = (0u8..80).collect();
        let writer = SequentialWriter::new(&data_path).unwrap();
        let mut cw = ChecksummedSequentialWriter::new(writer, 32);
        cw.write_all(&data).unwrap();
        cw.finish(&crc_path).unwrap();

        let mut crc_bytes = Vec::new();
        File::open(&crc_path)
            .unwrap()
            .read_to_end(&mut crc_bytes)
            .unwrap();
        let meta = DataIntegrityMetadata::read_from(&mut Cursor::new(&crc_bytes)).unwrap();

        assert_eq!(meta.chunk_count(), 3); // 32 + 32 + 16
        assert_eq!(meta.checksum_for_chunk(0), Some(crc32(&data[0..32])));
        assert_eq!(meta.checksum_for_chunk(1), Some(crc32(&data[32..64])));
        assert_eq!(meta.checksum_for_chunk(2), Some(crc32(&data[64..80])));
    }

    #[test]
    fn test_exact_chunk_boundary() {
        let dir = tempfile::tempdir().unwrap();
        let data_path = dir.path().join("exact.bin");
        let crc_path = dir.path().join("exact.crc");

        // Write exactly 2 chunks of 16 bytes each.
        let data = vec![0xFFu8; 32];
        let writer = SequentialWriter::new(&data_path).unwrap();
        let mut cw = ChecksummedSequentialWriter::new(writer, 16);
        cw.write_all(&data).unwrap();
        cw.finish(&crc_path).unwrap();

        let mut crc_bytes = Vec::new();
        File::open(&crc_path)
            .unwrap()
            .read_to_end(&mut crc_bytes)
            .unwrap();
        let meta = DataIntegrityMetadata::read_from(&mut Cursor::new(&crc_bytes)).unwrap();

        assert_eq!(meta.chunk_count(), 2);
        assert_eq!(meta.checksum_for_chunk(0), Some(crc32(&data[0..16])));
        assert_eq!(meta.checksum_for_chunk(1), Some(crc32(&data[16..32])));
    }

    #[test]
    fn test_checksums_match_manual_crc32() {
        let dir = tempfile::tempdir().unwrap();
        let data_path = dir.path().join("manual.bin");
        let crc_path = dir.path().join("manual.crc");

        let chunk_size = 10u32;
        let data: Vec<u8> = (0u8..25).collect();

        let writer = SequentialWriter::new(&data_path).unwrap();
        let mut cw = ChecksummedSequentialWriter::new(writer, chunk_size);
        cw.write_all(&data).unwrap();
        cw.finish(&crc_path).unwrap();

        let mut crc_bytes = Vec::new();
        File::open(&crc_path)
            .unwrap()
            .read_to_end(&mut crc_bytes)
            .unwrap();
        let meta = DataIntegrityMetadata::read_from(&mut Cursor::new(&crc_bytes)).unwrap();

        // 25 bytes / 10 chunk => 2 full + 1 partial (5 bytes).
        assert_eq!(meta.chunk_count(), 3);
        for i in 0..3 {
            let start = i * chunk_size as usize;
            let end = ((i + 1) * chunk_size as usize).min(data.len());
            let expected = crc32(&data[start..end]);
            assert_eq!(
                meta.checksum_for_chunk(i),
                Some(expected),
                "mismatch at chunk {i}"
            );
        }
    }

    #[test]
    fn test_position_delegates_to_inner() {
        let dir = tempfile::tempdir().unwrap();
        let data_path = dir.path().join("pos.bin");
        let crc_path = dir.path().join("pos.crc");

        let writer = SequentialWriter::new(&data_path).unwrap();
        let mut cw = ChecksummedSequentialWriter::new(writer, 64);
        assert_eq!(cw.position(), 0);

        cw.write_all(&[0u8; 42]).unwrap();
        assert_eq!(cw.position(), 42);

        cw.write_all(&[0u8; 100]).unwrap();
        assert_eq!(cw.position(), 142);

        cw.finish(&crc_path).unwrap();
    }
}
