// Licensed under Apache License, Version 2.0.

//! Rebufferer trait and buffer holders for demand-paged file access.
//!
//! A [`Rebufferer`] provides chunk-aligned slices of a file's contents on
//! demand. Two concrete implementations exist:
//! - [`SimpleChunkReader`](super::chunk_reader::SimpleChunkReader) reads via
//!   positioned I/O.
//! - [`MmapRebufferer`](super::mmap_rebufferer::MmapRebufferer) returns
//!   zero-copy views into a memory-mapped file.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.io.util.Rebufferer`
//! - `org.apache.cassandra.io.util.Rebufferer.BufferHolder`

use std::sync::Arc;

use memmap2::Mmap;

use crate::error::IoError;

// ---------------------------------------------------------------------------
// Buffer data storage
// ---------------------------------------------------------------------------

/// Internal storage for buffer data -- either an owned heap allocation or a
/// zero-copy view into a memory-mapped region.
enum BufferData {
    /// Heap-allocated buffer filled by a positioned read.
    Owned(Vec<u8>),
    /// Zero-copy slice into a memory-mapped file.
    Mapped {
        mmap: Arc<Mmap>,
        offset_in_map: usize,
        len: usize,
    },
}

// ---------------------------------------------------------------------------
// BufferHolder
// ---------------------------------------------------------------------------

/// A buffer holder returned by a [`Rebufferer`].
///
/// Contains either owned data (from a positioned read) or a reference-counted
/// view into a memory-mapped region. The holder exposes the usable byte slice
/// together with its absolute file offset.
pub struct BufferHolder {
    data: BufferData,
    /// Absolute file offset where this buffer starts.
    offset: u64,
}

impl BufferHolder {
    /// Creates a holder backed by an owned `Vec<u8>`.
    pub fn owned(data: Vec<u8>, offset: u64) -> Self {
        Self {
            data: BufferData::Owned(data),
            offset,
        }
    }

    /// Creates a holder backed by a slice of a memory-mapped file.
    pub fn mapped(mmap: Arc<Mmap>, offset: u64, offset_in_map: usize, len: usize) -> Self {
        Self {
            data: BufferData::Mapped {
                mmap,
                offset_in_map,
                len,
            },
            offset,
        }
    }

    /// Returns the usable bytes in this buffer.
    pub fn buffer(&self) -> &[u8] {
        match &self.data {
            BufferData::Owned(v) => v.as_slice(),
            BufferData::Mapped {
                mmap,
                offset_in_map,
                len,
            } => &mmap[*offset_in_map..*offset_in_map + *len],
        }
    }

    /// Absolute file offset where this buffer starts.
    pub fn offset(&self) -> u64 {
        self.offset
    }

    /// Number of valid bytes in the buffer.
    pub fn limit(&self) -> usize {
        match &self.data {
            BufferData::Owned(v) => v.len(),
            BufferData::Mapped { len, .. } => *len,
        }
    }

    /// Alias for [`buffer`](Self::buffer) -- returns the usable bytes.
    pub fn data(&self) -> &[u8] {
        self.buffer()
    }

    /// Convenience constructor for an owned buffer (alias for [`owned`](Self::owned)).
    pub fn new(data: Vec<u8>, offset: u64) -> Self {
        Self::owned(data, offset)
    }
}

impl std::fmt::Debug for BufferHolder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BufferHolder")
            .field("offset", &self.offset)
            .field("limit", &self.limit())
            .finish()
    }
}

// ---------------------------------------------------------------------------
// Rebufferer trait
// ---------------------------------------------------------------------------

/// Trait for demand-paged access to file contents.
///
/// Implementations provide chunk-aligned buffers at requested positions.
/// All implementations must be safe to share across threads.
pub trait Rebufferer: Send + Sync {
    /// Returns a buffer covering the chunk that contains `position`.
    ///
    /// The returned [`BufferHolder`] starts at the chunk-aligned offset that
    /// contains `position` and extends up to `chunk_size` bytes (or fewer at
    /// end-of-file).
    fn rebuffer(&self, position: u64) -> Result<BufferHolder, IoError>;

    /// Total length of the underlying file in bytes.
    fn file_length(&self) -> u64;

    /// Release any resources held by this rebufferer. Default is a no-op.
    fn close(&self) {}
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_owned_buffer_holder_access() {
        let data = vec![1u8, 2, 3, 4, 5];
        let holder = BufferHolder::owned(data.clone(), 100);
        assert_eq!(holder.buffer(), &[1, 2, 3, 4, 5]);
        assert_eq!(holder.offset(), 100);
        assert_eq!(holder.limit(), 5);
    }

    #[test]
    fn test_owned_buffer_holder_empty() {
        let holder = BufferHolder::owned(Vec::new(), 0);
        assert!(holder.buffer().is_empty());
        assert_eq!(holder.offset(), 0);
        assert_eq!(holder.limit(), 0);
    }

    #[test]
    fn test_mapped_buffer_holder_access() {
        // Create a temporary file to mmap.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.bin");
        std::fs::write(&path, b"hello world").unwrap();
        let file = std::fs::File::open(&path).unwrap();
        let mmap = Arc::new(unsafe { Mmap::map(&file).unwrap() });

        let holder = BufferHolder::mapped(Arc::clone(&mmap), 6, 6, 5);
        assert_eq!(holder.buffer(), b"world");
        assert_eq!(holder.offset(), 6);
        assert_eq!(holder.limit(), 5);
    }

    #[test]
    fn test_mapped_buffer_holder_full_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("full.bin");
        let content = b"abcdefghij";
        std::fs::write(&path, content).unwrap();
        let file = std::fs::File::open(&path).unwrap();
        let mmap = Arc::new(unsafe { Mmap::map(&file).unwrap() });

        let holder = BufferHolder::mapped(Arc::clone(&mmap), 0, 0, content.len());
        assert_eq!(holder.buffer(), content.as_slice());
        assert_eq!(holder.offset(), 0);
        assert_eq!(holder.limit(), content.len());
    }
}
