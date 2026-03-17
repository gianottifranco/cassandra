// Licensed under Apache License, Version 2.0.

//! Memory-mapped rebufferer providing zero-copy file access.
//!
//! [`MmapRebufferer`] maps the entire file into memory on construction and
//! returns lightweight [`BufferHolder`] views that reference the mapping.
//! No data is copied on [`rebuffer`](Rebufferer::rebuffer) calls.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.io.util.MmapRebufferer`

use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use memmap2::Mmap;

use crate::error::IoError;

use super::rebufferer::{BufferHolder, Rebufferer};

/// A [`Rebufferer`] backed by a memory-mapped file.
///
/// The mapping is established once on construction and shared via [`Arc`],
/// making the rebufferer cheaply cloneable and safe for concurrent access.
pub struct MmapRebufferer {
    mmap: Arc<Mmap>,
    file_length: u64,
    chunk_size: usize,
    #[allow(dead_code)]
    path: PathBuf,
}

impl MmapRebufferer {
    /// Maps the file at `path` and creates a new rebufferer.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be opened or mapped.
    ///
    /// # Safety
    ///
    /// Memory-mapping a file is inherently unsafe if the file is modified
    /// externally while mapped. Callers must ensure the file is not truncated
    /// or overwritten during the lifetime of this rebufferer.
    pub fn new(path: impl AsRef<Path>, chunk_size: usize) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = fs::File::open(&path)?;
        let metadata = file.metadata()?;
        let file_length = metadata.len();

        if file_length == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "cannot memory-map an empty file",
            ));
        }

        let mmap = unsafe { Mmap::map(&file)? };

        Ok(Self {
            mmap: Arc::new(mmap),
            file_length,
            chunk_size,
            path,
        })
    }

    /// Creates a rebufferer with the default chunk size of 64 KiB.
    pub fn with_defaults(path: impl AsRef<Path>) -> io::Result<Self> {
        Self::new(path, super::chunk_reader::DEFAULT_CHUNK_SIZE)
    }

    /// Returns the chunk size used by this rebufferer.
    pub fn chunk_size(&self) -> usize {
        self.chunk_size
    }
}

impl Rebufferer for MmapRebufferer {
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

        let chunk_start = (position / self.chunk_size as u64) * self.chunk_size as u64;
        let remaining = self.file_length - chunk_start;
        let len = (self.chunk_size as u64).min(remaining) as usize;

        Ok(BufferHolder::mapped(
            Arc::clone(&self.mmap),
            chunk_start,
            chunk_start as usize,
            len,
        ))
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
    use crate::util::chunk_reader::SimpleChunkReader;
    use crate::util::rebufferer::Rebufferer;
    use tempfile::NamedTempFile;

    fn temp_file_with(data: &[u8]) -> NamedTempFile {
        use std::io::Write;
        let mut f = NamedTempFile::new().unwrap();
        f.write_all(data).unwrap();
        f.flush().unwrap();
        f
    }

    #[test]
    fn test_roundtrip_read() {
        let data: Vec<u8> = (0..200u8).collect();
        let f = temp_file_with(&data);
        let rebuf = MmapRebufferer::new(f.path(), 64).unwrap();

        let mut reconstructed = Vec::new();
        let mut pos = 0u64;
        while pos < rebuf.file_length() {
            let holder = rebuf.rebuffer(pos).unwrap();
            reconstructed.extend_from_slice(holder.buffer());
            pos = holder.offset() + holder.limit() as u64;
        }
        assert_eq!(reconstructed, data);
    }

    #[test]
    fn test_concurrent_access() {
        let data: Vec<u8> = (0..=255u8).collect();
        let f = temp_file_with(&data);
        let rebuf = Arc::new(MmapRebufferer::new(f.path(), 64).unwrap());

        let handles: Vec<_> = (0..4)
            .map(|i| {
                let r = Arc::clone(&rebuf);
                let expected = data.clone();
                std::thread::spawn(move || {
                    let pos = (i * 64) as u64;
                    let holder = r.rebuffer(pos).unwrap();
                    let end = (pos as usize + holder.limit()).min(expected.len());
                    assert_eq!(holder.buffer(), &expected[pos as usize..end]);
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }
    }

    #[test]
    fn test_matches_chunk_reader_output() {
        let data: Vec<u8> = (0..500).map(|i| (i % 256) as u8).collect();
        let f = temp_file_with(&data);
        let chunk_reader = SimpleChunkReader::new(f.path(), 64).unwrap();
        let mmap_reader = MmapRebufferer::new(f.path(), 64).unwrap();

        assert_eq!(chunk_reader.file_length(), mmap_reader.file_length());

        let mut pos = 0u64;
        while pos < mmap_reader.file_length() {
            let ch = chunk_reader.rebuffer(pos).unwrap();
            let mm = mmap_reader.rebuffer(pos).unwrap();
            assert_eq!(ch.offset(), mm.offset(), "offset mismatch at pos {pos}");
            assert_eq!(ch.limit(), mm.limit(), "limit mismatch at pos {pos}");
            assert_eq!(ch.buffer(), mm.buffer(), "data mismatch at pos {pos}");
            pos = ch.offset() + ch.limit() as u64;
        }
    }

    #[test]
    fn test_position_beyond_file_length() {
        let f = temp_file_with(b"short");
        let rebuf = MmapRebufferer::new(f.path(), 64).unwrap();
        assert!(rebuf.rebuffer(100).is_err());
    }

    #[test]
    fn test_empty_file_rejected() {
        let f = temp_file_with(b"");
        let result = MmapRebufferer::new(f.path(), 64);
        assert!(result.is_err());
    }

    #[test]
    fn test_file_smaller_than_chunk() {
        let data = b"small";
        let f = temp_file_with(data);
        let rebuf = MmapRebufferer::new(f.path(), 65536).unwrap();

        let holder = rebuf.rebuffer(0).unwrap();
        assert_eq!(holder.offset(), 0);
        assert_eq!(holder.limit(), data.len());
        assert_eq!(holder.buffer(), data);
    }

    #[test]
    fn test_file_length_reported() {
        let data = vec![0u8; 1234];
        let f = temp_file_with(&data);
        let rebuf = MmapRebufferer::new(f.path(), 64).unwrap();
        assert_eq!(rebuf.file_length(), 1234);
    }

    #[test]
    fn test_chunk_alignment() {
        let data = vec![0u8; 256];
        let f = temp_file_with(&data);
        let rebuf = MmapRebufferer::new(f.path(), 64).unwrap();

        // Position 10 aligns to chunk 0.
        let holder = rebuf.rebuffer(10).unwrap();
        assert_eq!(holder.offset(), 0);

        // Position 65 aligns to chunk 64.
        let holder = rebuf.rebuffer(65).unwrap();
        assert_eq!(holder.offset(), 64);
    }
}
