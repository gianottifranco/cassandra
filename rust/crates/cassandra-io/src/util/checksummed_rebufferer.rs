// Licensed under Apache License, Version 2.0.

//! A rebufferer decorator that verifies CRC32 checksums on every chunk read.
//!
//! Wraps any [`Rebufferer`] and transparently validates each returned buffer
//! against the checksums stored in [`DataIntegrityMetadata`]. A mismatch
//! produces an [`IoError::ChecksumMismatch`] error.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.io.util.ChecksummedRebufferer`

use crate::error::IoError;
use crate::util::data_integrity::DataIntegrityMetadata;
use crate::util::rebufferer::{BufferHolder, Rebufferer};

// ---------------------------------------------------------------------------
// ChecksummedRebufferer
// ---------------------------------------------------------------------------

/// A [`Rebufferer`] decorator that validates CRC32 checksums on each chunk.
pub struct ChecksummedRebufferer {
    inner: Box<dyn Rebufferer>,
    metadata: DataIntegrityMetadata,
}

impl ChecksummedRebufferer {
    /// Wraps `inner` with checksum verification using `metadata`.
    pub fn new(inner: Box<dyn Rebufferer>, metadata: DataIntegrityMetadata) -> Self {
        Self { inner, metadata }
    }

    /// Computes the chunk index for the given byte position.
    fn chunk_index(&self, position: u64) -> usize {
        (position / self.metadata.chunk_size() as u64) as usize
    }
}

impl Rebufferer for ChecksummedRebufferer {
    fn rebuffer(&self, position: u64) -> Result<BufferHolder, IoError> {
        let holder = self.inner.rebuffer(position)?;
        let chunk_idx = self.chunk_index(position);

        if let Some(expected) = self.metadata.checksum_for_chunk(chunk_idx) {
            let actual = crc32fast::hash(holder.buffer());
            if actual != expected {
                return Err(IoError::ChecksumMismatch { expected, actual });
            }
        }

        Ok(holder)
    }

    fn file_length(&self) -> u64 {
        self.inner.file_length()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Seek, SeekFrom};
    use tempfile::NamedTempFile;

    // -----------------------------------------------------------------------
    // Minimal test rebufferer that reads fixed-size chunks from a file.
    // -----------------------------------------------------------------------

    struct FileChunkRebufferer {
        file: std::fs::File,
        chunk_size: usize,
        file_length: u64,
    }

    impl FileChunkRebufferer {
        fn new(path: &std::path::Path, chunk_size: usize) -> Self {
            let file = std::fs::File::open(path).unwrap();
            let file_length = file.metadata().unwrap().len();
            Self {
                file,
                chunk_size,
                file_length,
            }
        }
    }

    impl Rebufferer for FileChunkRebufferer {
        fn rebuffer(&self, position: u64) -> Result<BufferHolder, IoError> {
            use std::os::unix::fs::FileExt;
            let chunk_start = (position / self.chunk_size as u64) * self.chunk_size as u64;

            let remaining = (self.file_length - chunk_start) as usize;
            let to_read = remaining.min(self.chunk_size);
            let mut buf = vec![0u8; to_read];
            self.file.read_exact_at(&mut buf, chunk_start)?;

            Ok(BufferHolder::owned(buf, chunk_start))
        }

        fn file_length(&self) -> u64 {
            self.file_length
        }
    }

    /// Write test data, compute per-chunk CRC32s, return (path, metadata).
    fn write_test_file(data: &[u8], chunk_size: usize) -> (NamedTempFile, DataIntegrityMetadata) {
        use std::io::Write;

        let mut tmp = NamedTempFile::new().unwrap();
        tmp.write_all(data).unwrap();
        tmp.flush().unwrap();

        let checksums: Vec<u32> = data
            .chunks(chunk_size)
            .map(|c| crc32fast::hash(c))
            .collect();

        let meta = DataIntegrityMetadata::new(chunk_size as u32, checksums);
        (tmp, meta)
    }

    #[test]
    fn test_valid_data_passes_checksum() {
        let data = b"Hello, checksummed world! This is chunk-aligned data.";
        let chunk_size = 16;
        let (tmp, meta) = write_test_file(data, chunk_size);

        let inner = Box::new(FileChunkRebufferer::new(tmp.path(), chunk_size));
        let rebuf = ChecksummedRebufferer::new(inner, meta);

        // Read each chunk — all should succeed.
        let num_chunks = (data.len() + chunk_size - 1) / chunk_size;
        for i in 0..num_chunks {
            let holder = rebuf.rebuffer(i as u64 * chunk_size as u64).unwrap();
            assert!(!holder.data().is_empty());
        }
    }

    #[test]
    fn test_corrupt_data_detected() {
        let data = b"AAAAAAAAAAAAAAAA"; // exactly one 16-byte chunk
        let chunk_size = 16;
        let (tmp, meta) = write_test_file(data, chunk_size);

        // Corrupt a byte on disk.
        {
            use std::io::Write;
            let mut f = std::fs::OpenOptions::new()
                .write(true)
                .open(tmp.path())
                .unwrap();
            f.seek(SeekFrom::Start(0)).unwrap();
            f.write_all(b"B").unwrap();
            f.flush().unwrap();
        }

        let inner = Box::new(FileChunkRebufferer::new(tmp.path(), chunk_size));
        let rebuf = ChecksummedRebufferer::new(inner, meta);

        let result = rebuf.rebuffer(0);
        assert!(result.is_err());
        match result.unwrap_err() {
            IoError::ChecksumMismatch { .. } => {}
            other => panic!("expected ChecksumMismatch, got {:?}", other),
        }
    }

    #[test]
    fn test_file_length_delegates() {
        let data = b"some test data";
        let chunk_size = 8;
        let (tmp, meta) = write_test_file(data, chunk_size);

        let inner = Box::new(FileChunkRebufferer::new(tmp.path(), chunk_size));
        let rebuf = ChecksummedRebufferer::new(inner, meta);

        assert_eq!(rebuf.file_length(), data.len() as u64);
    }
}
