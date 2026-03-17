// Licensed under Apache License, Version 2.0.

//! Simple chunk reader that serves file content via positioned I/O.
//!
//! [`SimpleChunkReader`] implements the [`Rebufferer`] trait by reading
//! chunk-aligned blocks from a file using `pread` (`read_at` on Unix).
//! Each call to [`rebuffer`](Rebufferer::rebuffer) allocates a fresh buffer
//! and performs a single positioned read -- no caching is done.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.io.util.SimpleChunkReader`

use std::fs::{self, File};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[cfg(unix)]
use std::os::unix::fs::FileExt;

use crate::error::IoError;

use super::rebufferer::{BufferHolder, Rebufferer};

/// Default chunk size: 64 KiB.
pub const DEFAULT_CHUNK_SIZE: usize = 65_536;

/// A [`Rebufferer`] that performs positioned reads of fixed-size chunks.
///
/// The underlying file handle is wrapped in an [`Arc`] so the reader is
/// cheaply cloneable and safe to share across threads.
pub struct SimpleChunkReader {
    file: Arc<File>,
    file_length: u64,
    chunk_size: usize,
    #[allow(dead_code)]
    path: PathBuf,
}

impl SimpleChunkReader {
    /// Opens `path` and creates a new chunk reader.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be opened or its metadata cannot be
    /// read.
    pub fn new(path: impl AsRef<Path>, chunk_size: usize) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = File::open(&path)?;
        let file_length = fs::metadata(&path)?.len();
        Ok(Self {
            file: Arc::new(file),
            file_length,
            chunk_size,
            path,
        })
    }

    /// Creates a reader with the default chunk size of 64 KiB.
    pub fn with_defaults(path: impl AsRef<Path>) -> io::Result<Self> {
        Self::new(path, DEFAULT_CHUNK_SIZE)
    }

    /// Returns the chunk size used by this reader.
    pub fn chunk_size(&self) -> usize {
        self.chunk_size
    }
}

impl Rebufferer for SimpleChunkReader {
    fn rebuffer(&self, position: u64) -> Result<BufferHolder, IoError> {
        if position >= self.file_length {
            return Err(IoError::Io(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "position {} is beyond file length {}",
                    position, self.file_length
                ),
            )));
        }

        // Align to chunk boundary.
        let chunk_start = (position / self.chunk_size as u64) * self.chunk_size as u64;
        let remaining = self.file_length - chunk_start;
        let to_read = (self.chunk_size as u64).min(remaining) as usize;

        let mut buf = vec![0u8; to_read];
        let mut total_read = 0usize;

        // Loop to handle partial reads.
        while total_read < to_read {
            let n = self
                .file
                .read_at(&mut buf[total_read..], chunk_start + total_read as u64)?;
            if n == 0 {
                // EOF before expected -- truncate.
                buf.truncate(total_read);
                break;
            }
            total_read += n;
        }

        buf.truncate(total_read);
        Ok(BufferHolder::owned(buf, chunk_start))
    }

    fn file_length(&self) -> u64 {
        self.file_length
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    /// Helper: write `data` to a temp file and return the path.
    fn temp_file_with(data: &[u8]) -> NamedTempFile {
        use std::io::Write;
        let mut f = NamedTempFile::new().unwrap();
        f.write_all(data).unwrap();
        f.flush().unwrap();
        f
    }

    #[test]
    fn test_sequential_rebuffering() {
        let data: Vec<u8> = (0..200u8).collect();
        let f = temp_file_with(&data);
        let reader = SimpleChunkReader::new(f.path(), 64).unwrap();

        // First chunk: bytes 0..64
        let holder = reader.rebuffer(0).unwrap();
        assert_eq!(holder.offset(), 0);
        assert_eq!(holder.limit(), 64);
        assert_eq!(holder.buffer(), &data[0..64]);

        // Second chunk: bytes 64..128
        let holder = reader.rebuffer(64).unwrap();
        assert_eq!(holder.offset(), 64);
        assert_eq!(holder.limit(), 64);
        assert_eq!(holder.buffer(), &data[64..128]);

        // Third chunk: bytes 128..192
        let holder = reader.rebuffer(128).unwrap();
        assert_eq!(holder.offset(), 128);
        assert_eq!(holder.limit(), 64);
        assert_eq!(holder.buffer(), &data[128..192]);

        // Last chunk: bytes 192..200 (partial)
        let holder = reader.rebuffer(192).unwrap();
        assert_eq!(holder.offset(), 192);
        assert_eq!(holder.limit(), 8);
        assert_eq!(holder.buffer(), &data[192..200]);
    }

    #[test]
    fn test_random_access() {
        let data: Vec<u8> = (0..=255u8).collect();
        let f = temp_file_with(&data);
        let reader = SimpleChunkReader::new(f.path(), 64).unwrap();

        // Access position in third chunk -- should align to chunk boundary.
        let holder = reader.rebuffer(150).unwrap();
        assert_eq!(holder.offset(), 128); // aligned to 64*2
        assert_eq!(holder.limit(), 64);
        assert_eq!(holder.buffer(), &data[128..192]);

        // Access first byte.
        let holder = reader.rebuffer(0).unwrap();
        assert_eq!(holder.offset(), 0);
        assert_eq!(holder.buffer(), &data[0..64]);
    }

    #[test]
    fn test_file_smaller_than_chunk() {
        let data = b"tiny file";
        let f = temp_file_with(data);
        let reader = SimpleChunkReader::new(f.path(), DEFAULT_CHUNK_SIZE).unwrap();

        let holder = reader.rebuffer(0).unwrap();
        assert_eq!(holder.offset(), 0);
        assert_eq!(holder.limit(), data.len());
        assert_eq!(holder.buffer(), data);
    }

    #[test]
    fn test_boundary_exact_chunk_size() {
        // File is exactly 2 chunks.
        let data = vec![0xABu8; 128];
        let f = temp_file_with(&data);
        let reader = SimpleChunkReader::new(f.path(), 64).unwrap();

        let h1 = reader.rebuffer(0).unwrap();
        assert_eq!(h1.limit(), 64);
        let h2 = reader.rebuffer(64).unwrap();
        assert_eq!(h2.limit(), 64);
    }

    #[test]
    fn test_position_beyond_file_length() {
        let f = temp_file_with(b"short");
        let reader = SimpleChunkReader::new(f.path(), 64).unwrap();
        let result = reader.rebuffer(100);
        assert!(result.is_err());
    }

    #[test]
    fn test_file_length_reported() {
        let data = vec![0u8; 1000];
        let f = temp_file_with(&data);
        let reader = SimpleChunkReader::new(f.path(), 64).unwrap();
        assert_eq!(reader.file_length(), 1000);
    }

    #[test]
    fn test_default_chunk_size() {
        let f = temp_file_with(b"x");
        let reader = SimpleChunkReader::with_defaults(f.path()).unwrap();
        assert_eq!(reader.chunk_size(), DEFAULT_CHUNK_SIZE);
    }

    #[test]
    fn test_position_within_chunk_returns_aligned() {
        let data = vec![0u8; 256];
        let f = temp_file_with(&data);
        let reader = SimpleChunkReader::new(f.path(), 64).unwrap();

        // Position 10 should still return chunk starting at 0.
        let holder = reader.rebuffer(10).unwrap();
        assert_eq!(holder.offset(), 0);
        assert_eq!(holder.limit(), 64);

        // Position 65 should return chunk starting at 64.
        let holder = reader.rebuffer(65).unwrap();
        assert_eq!(holder.offset(), 64);
    }
}
