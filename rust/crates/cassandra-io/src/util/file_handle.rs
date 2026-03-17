// Licensed under Apache License, Version 2.0.

//! File access lifecycle manager with builder pattern.
//!
//! `FileHandle` owns the metadata needed to open readers over a single file,
//! supporting plain buffered I/O, memory-mapped I/O, compressed reads, and
//! optional checksum verification.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.io.util.FileHandle`

use std::io;
use std::path::{Path, PathBuf};

use crate::compress::compressed_reader::CompressedChunkReader;
use crate::compress::metadata::CompressionMetadata;
use crate::util::checksummed_rebufferer::ChecksummedRebufferer;
use crate::util::chunk_reader::SimpleChunkReader;
use crate::util::data_integrity::DataIntegrityMetadata;
use crate::util::mmap_rebufferer::MmapRebufferer;
use crate::util::random_access_reader::RandomAccessReader;
use crate::util::rebufferer::Rebufferer;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Default read buffer size (64 KiB).
const DEFAULT_BUFFER_SIZE: usize = 64 * 1024;

// ---------------------------------------------------------------------------
// FileHandle
// ---------------------------------------------------------------------------

/// Manages the lifecycle of file access for a single on-disk file.
///
/// A `FileHandle` is cheap to clone and can produce multiple independent
/// [`RandomAccessReader`]s over the same underlying file.
#[derive(Debug, Clone)]
pub struct FileHandle {
    /// Absolute or relative path to the backing file.
    path: PathBuf,
    /// Buffer size used by non-mmap readers.
    buffer_size: usize,
    /// Whether to prefer memory-mapped I/O.
    use_mmap: bool,
    /// Optional compression metadata for compressed SSTables.
    compression_metadata: Option<CompressionMetadata>,
    /// Optional data-integrity metadata for checksummed reads.
    integrity_metadata: Option<DataIntegrityMetadata>,
}

impl FileHandle {
    /// Returns a builder pre-configured with defaults for the given path.
    pub fn builder(path: impl AsRef<Path>) -> FileHandleBuilder {
        FileHandleBuilder {
            path: path.as_ref().to_path_buf(),
            buffer_size: DEFAULT_BUFFER_SIZE,
            use_mmap: false,
            compression_metadata: None,
            integrity_metadata: None,
        }
    }

    /// Returns the path to the backing file.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Returns the file length in bytes by querying file-system metadata.
    pub fn file_length(&self) -> io::Result<u64> {
        let meta = std::fs::metadata(&self.path)?;
        Ok(meta.len())
    }

    /// Creates a new [`RandomAccessReader`] over this file.
    ///
    /// The concrete rebufferer strategy is selected based on the handle
    /// configuration:
    ///
    /// 1. If compression metadata is present a [`CompressedChunkReader`] is
    ///    used.
    /// 2. Otherwise, if `use_mmap` is set, a [`MmapRebufferer`] is created.
    /// 3. Otherwise a [`SimpleChunkReader`] with the configured buffer size is
    ///    used.
    /// 4. When integrity metadata is present the rebufferer is wrapped in a
    ///    [`ChecksummedRebufferer`].
    pub fn create_reader(&self) -> io::Result<RandomAccessReader> {
        // Step 1-3: select the base rebufferer.
        let rebufferer: Box<dyn Rebufferer> = if let Some(ref cm) = self.compression_metadata {
            Box::new(CompressedChunkReader::new(&self.path, cm.clone())?)
        } else if self.use_mmap {
            Box::new(MmapRebufferer::with_defaults(&self.path)?)
        } else {
            Box::new(SimpleChunkReader::new(&self.path, self.buffer_size)?)
        };

        // Step 4: optionally wrap with checksum verification.
        let rebufferer: Box<dyn Rebufferer> = if let Some(ref im) = self.integrity_metadata {
            Box::new(ChecksummedRebufferer::new(rebufferer, im.clone()))
        } else {
            rebufferer
        };

        Ok(RandomAccessReader::new(rebufferer))
    }
}

// ---------------------------------------------------------------------------
// FileHandleBuilder
// ---------------------------------------------------------------------------

/// Builder for constructing a [`FileHandle`] with optional features.
#[derive(Debug)]
pub struct FileHandleBuilder {
    path: PathBuf,
    buffer_size: usize,
    use_mmap: bool,
    compression_metadata: Option<CompressionMetadata>,
    integrity_metadata: Option<DataIntegrityMetadata>,
}

impl FileHandleBuilder {
    /// Sets the read buffer size (ignored when mmap or compression is used).
    pub fn with_buffer_size(mut self, n: usize) -> Self {
        self.buffer_size = n;
        self
    }

    /// Enables or disables memory-mapped I/O.
    pub fn with_mmap(mut self, use_mmap: bool) -> Self {
        self.use_mmap = use_mmap;
        self
    }

    /// Attaches compression metadata so readers decompress on the fly.
    pub fn with_compression(mut self, metadata: CompressionMetadata) -> Self {
        self.compression_metadata = Some(metadata);
        self
    }

    /// Attaches data-integrity metadata for checksum verification.
    pub fn with_checksums(mut self, integrity: DataIntegrityMetadata) -> Self {
        self.integrity_metadata = Some(integrity);
        self
    }

    /// Builds the [`FileHandle`], validating that the path exists.
    pub fn build(self) -> io::Result<FileHandle> {
        if !self.path.exists() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("file not found: {}", self.path.display()),
            ));
        }

        Ok(FileHandle {
            path: self.path,
            buffer_size: self.buffer_size,
            use_mmap: self.use_mmap,
            compression_metadata: self.compression_metadata,
            integrity_metadata: self.integrity_metadata,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    /// Helper: create a temp file with known content.
    fn temp_file_with(content: &[u8]) -> tempfile::NamedTempFile {
        let mut f = tempfile::NamedTempFile::new().expect("create temp file");
        f.write_all(content).expect("write content");
        f.flush().expect("flush");
        f
    }

    #[test]
    fn test_build_defaults_and_path() {
        let f = temp_file_with(b"hello world");
        let handle = FileHandle::builder(f.path()).build().unwrap();

        assert_eq!(handle.path(), f.path());
        assert_eq!(handle.file_length().unwrap(), 11);
    }

    #[test]
    fn test_build_nonexistent_path_errors() {
        let result = FileHandle::builder("/tmp/nonexistent_cassandra_test_xyz.dat").build();
        assert!(result.is_err());
    }

    #[test]
    fn test_builder_with_buffer_size() {
        let f = temp_file_with(b"data");
        let handle = FileHandle::builder(f.path())
            .with_buffer_size(8192)
            .build()
            .unwrap();

        assert_eq!(handle.buffer_size, 8192);
    }

    #[test]
    fn test_builder_with_mmap() {
        let f = temp_file_with(b"mmap data");
        let handle = FileHandle::builder(f.path())
            .with_mmap(true)
            .build()
            .unwrap();

        assert!(handle.use_mmap);
    }

    #[test]
    fn test_builder_all_options() {
        let f = temp_file_with(b"full options");
        let handle = FileHandle::builder(f.path())
            .with_buffer_size(4096)
            .with_mmap(false)
            .build()
            .unwrap();

        assert_eq!(handle.buffer_size, 4096);
        assert!(!handle.use_mmap);
        assert!(handle.compression_metadata.is_none());
        assert!(handle.integrity_metadata.is_none());
    }

    #[test]
    fn test_file_length() {
        let content = b"0123456789ABCDEF";
        let f = temp_file_with(content);
        let handle = FileHandle::builder(f.path()).build().unwrap();

        assert_eq!(handle.file_length().unwrap(), content.len() as u64);
    }

    #[test]
    fn test_multiple_handles_same_file() {
        let f = temp_file_with(b"shared file");
        let h1 = FileHandle::builder(f.path()).build().unwrap();
        let h2 = FileHandle::builder(f.path()).with_mmap(true).build().unwrap();

        assert_eq!(h1.path(), h2.path());
        assert!(!h1.use_mmap);
        assert!(h2.use_mmap);
    }
}
