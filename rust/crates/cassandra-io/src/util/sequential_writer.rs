// Licensed under Apache License, Version 2.0.

//! Buffered sequential writer for SSTable and commit-log files.
//!
//! Wraps a [`File`] with an in-memory buffer and exposes helpers for writing
//! primitive types in big-endian format. On [`finish`](SequentialWriter::finish)
//! the writer flushes, fsyncs, and optionally renames from a temporary path to
//! the final destination.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.io.util.SequentialWriter`

use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use byteorder::{BigEndian, WriteBytesExt};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const DEFAULT_BUFFER_SIZE: usize = 65_536;

// ---------------------------------------------------------------------------
// SequentialWriter
// ---------------------------------------------------------------------------

/// Buffered, sequential-only file writer with fsync and atomic rename support.
pub struct SequentialWriter {
    inner: File,
    buffer: Vec<u8>,
    buffer_size: usize,
    position: u64,
    temp_path: PathBuf,
    final_path: Option<PathBuf>,
}

impl SequentialWriter {
    /// Opens `path` for writing with the default 64 KiB buffer.
    pub fn new(path: impl AsRef<Path>) -> io::Result<Self> {
        let temp_path = path.as_ref().to_path_buf();
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&temp_path)?;

        Ok(Self {
            inner: file,
            buffer: Vec::with_capacity(DEFAULT_BUFFER_SIZE),
            buffer_size: DEFAULT_BUFFER_SIZE,
            position: 0,
            temp_path,
            final_path: None,
        })
    }

    /// Builder: set a custom buffer size.
    pub fn with_buffer_size(mut self, size: usize) -> Self {
        self.buffer_size = size;
        self.buffer = Vec::with_capacity(size);
        self
    }

    /// Builder: set the final path. On [`finish`](Self::finish) the temp file
    /// is renamed to this path.
    pub fn with_finish_path(mut self, path: PathBuf) -> Self {
        self.final_path = Some(path);
        self
    }

    /// Total number of bytes written (buffered + flushed).
    pub fn position(&self) -> u64 {
        self.position
    }

    /// Flushes the buffer and fsyncs the underlying file descriptor.
    pub fn sync(&mut self) -> io::Result<()> {
        self.flush()?;
        self.inner.sync_all()
    }

    /// Flushes, fsyncs, and — if a finish path was set — atomically renames the
    /// temp file to the final path.
    pub fn finish(mut self) -> io::Result<()> {
        self.sync()?;
        if let Some(ref final_path) = self.final_path {
            fs::rename(&self.temp_path, final_path)?;
        }
        Ok(())
    }

    /// Drops the file handle and deletes the temp file from disk.
    pub fn abort(self) -> io::Result<()> {
        drop(self.inner);
        if self.temp_path.exists() {
            fs::remove_file(&self.temp_path)?;
        }
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Cassandra write helpers (big-endian)
    // -----------------------------------------------------------------------

    /// Writes a 32-bit signed integer in big-endian order.
    pub fn write_int(&mut self, v: i32) -> io::Result<()> {
        self.write_i32::<BigEndian>(v)
    }

    /// Writes a 64-bit signed integer in big-endian order.
    pub fn write_long(&mut self, v: i64) -> io::Result<()> {
        self.write_i64::<BigEndian>(v)
    }

    /// Writes a 16-bit signed integer in big-endian order.
    pub fn write_short(&mut self, v: i16) -> io::Result<()> {
        self.write_i16::<BigEndian>(v)
    }

    /// Writes a 32-bit float in big-endian order.
    pub fn write_float(&mut self, v: f32) -> io::Result<()> {
        self.write_f32::<BigEndian>(v)
    }

    /// Writes a 64-bit float in big-endian order.
    pub fn write_double(&mut self, v: f64) -> io::Result<()> {
        self.write_f64::<BigEndian>(v)
    }

    // -----------------------------------------------------------------------
    // Internal
    // -----------------------------------------------------------------------

    /// Flushes the internal buffer to the underlying file.
    fn flush_buffer(&mut self) -> io::Result<()> {
        if !self.buffer.is_empty() {
            self.inner.write_all(&self.buffer)?;
            self.buffer.clear();
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// std::io::Write
// ---------------------------------------------------------------------------

impl Write for SequentialWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let mut offset = 0;
        while offset < buf.len() {
            let remaining = self.buffer_size - self.buffer.len();
            let to_copy = remaining.min(buf.len() - offset);
            self.buffer.extend_from_slice(&buf[offset..offset + to_copy]);
            offset += to_copy;
            self.position += to_copy as u64;

            if self.buffer.len() >= self.buffer_size {
                self.flush_buffer()?;
            }
        }
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.flush_buffer()?;
        self.inner.flush()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;

    #[test]
    fn test_write_and_read_back() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("data.bin");

        let mut w = SequentialWriter::new(&path).unwrap();
        w.write_all(b"hello world").unwrap();
        w.finish().unwrap();

        let mut contents = Vec::new();
        File::open(&path).unwrap().read_to_end(&mut contents).unwrap();
        assert_eq!(contents, b"hello world");
    }

    #[test]
    fn test_large_write_crosses_buffer() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("large.bin");

        let mut w = SequentialWriter::new(&path)
            .unwrap()
            .with_buffer_size(16);

        // Write 100 bytes through a 16-byte buffer.
        let data: Vec<u8> = (0u8..100).collect();
        w.write_all(&data).unwrap();
        w.finish().unwrap();

        let mut contents = Vec::new();
        File::open(&path).unwrap().read_to_end(&mut contents).unwrap();
        assert_eq!(contents, data);
    }

    #[test]
    fn test_finish_renames_to_final_path() {
        let dir = tempfile::tempdir().unwrap();
        let tmp = dir.path().join("tmp.bin");
        let fin = dir.path().join("final.bin");

        let mut w = SequentialWriter::new(&tmp)
            .unwrap()
            .with_finish_path(fin.clone());
        w.write_all(b"renamed").unwrap();
        w.finish().unwrap();

        assert!(!tmp.exists(), "temp file should be gone after rename");
        assert!(fin.exists(), "final file should exist");

        let mut contents = Vec::new();
        File::open(&fin).unwrap().read_to_end(&mut contents).unwrap();
        assert_eq!(contents, b"renamed");
    }

    #[test]
    fn test_abort_deletes_temp_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("aborted.bin");

        let mut w = SequentialWriter::new(&path).unwrap();
        w.write_all(b"discard me").unwrap();
        w.abort().unwrap();

        assert!(!path.exists(), "temp file should be removed after abort");
    }

    #[test]
    fn test_position_tracking() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pos.bin");

        let mut w = SequentialWriter::new(&path)
            .unwrap()
            .with_buffer_size(32);

        assert_eq!(w.position(), 0);
        w.write_all(&[0u8; 10]).unwrap();
        assert_eq!(w.position(), 10);
        w.write_all(&[0u8; 50]).unwrap();
        assert_eq!(w.position(), 60);

        w.finish().unwrap();
    }

    #[test]
    fn test_write_helpers_big_endian() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("helpers.bin");

        let mut w = SequentialWriter::new(&path).unwrap();
        w.write_int(0x01020304).unwrap();
        w.write_long(0x0102030405060708).unwrap();
        w.write_short(0x0A0B).unwrap();
        w.finish().unwrap();

        let mut contents = Vec::new();
        File::open(&path).unwrap().read_to_end(&mut contents).unwrap();

        // int (4) + long (8) + short (2) = 14 bytes
        assert_eq!(contents.len(), 14);
        assert_eq!(&contents[0..4], &[0x01, 0x02, 0x03, 0x04]);
        assert_eq!(
            &contents[4..12],
            &[0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08]
        );
        assert_eq!(&contents[12..14], &[0x0A, 0x0B]);
    }
}
