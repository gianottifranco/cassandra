// Licensed under Apache License, Version 2.0.

//! Commit log segment: a single append-only file with header and CRC'd entries.
//!
//! ## Format
//!
//! ```text
//! ┌─────────────────────────────────────────────┐
//! │  Magic: [u8; 4] = b"CLSG"                  │
//! │  Version: u8 = 1                            │
//! │  Segment ID: u64 (big-endian)               │
//! │  Reserved: [u8; 3] = [0, 0, 0]             │
//! ├─────────────────────────────────────────────┤
//! │  Entry 1: [len: u32][crc32: u32][payload]   │
//! │  Entry 2: [len: u32][crc32: u32][payload]   │
//! │  ...                                        │
//! └─────────────────────────────────────────────┘
//! ```

use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use crc32fast::Hasher;

use super::{CommitLogError, Result, segment_filename};

const MAGIC: [u8; 4] = *b"CLSG";
const VERSION: u8 = 1;
const HEADER_SIZE: u64 = 4 + 1 + 8 + 3; // 16 bytes

/// A single commit log segment file.
pub struct Segment {
    id: u64,
    path: PathBuf,
    writer: Option<BufWriter<File>>,
    size: u64,
}

impl Segment {
    /// Create a new segment file with a header.
    pub fn create(dir: &Path, id: u64) -> Result<Self> {
        let path = dir.join(segment_filename(id));
        let mut file = BufWriter::new(
            OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&path)?,
        );

        // Write header
        file.write_all(&MAGIC)?;
        file.write_u8(VERSION)?;
        file.write_u64::<BigEndian>(id)?;
        file.write_all(&[0u8; 3])?; // reserved
        file.flush()?;

        Ok(Self {
            id,
            path,
            writer: Some(file),
            size: HEADER_SIZE,
        })
    }

    /// Open an existing segment file for reading.
    pub fn open_for_read(path: &Path) -> Result<Self> {
        let mut file = BufReader::new(File::open(path)?);

        // Read and verify header
        let mut magic = [0u8; 4];
        file.read_exact(&mut magic)?;
        if magic != MAGIC {
            return Err(CommitLogError::CorruptHeader {
                path: path.to_path_buf(),
            });
        }

        let version = file.read_u8()?;
        if version != VERSION {
            return Err(CommitLogError::CorruptHeader {
                path: path.to_path_buf(),
            });
        }

        let id = file.read_u64::<BigEndian>()?;
        let mut reserved = [0u8; 3];
        file.read_exact(&mut reserved)?;

        let size = fs::metadata(path)?.len();

        Ok(Self {
            id,
            path: path.to_path_buf(),
            writer: None,
            size,
        })
    }

    /// Segment ID.
    pub fn id(&self) -> u64 {
        self.id
    }

    /// Current size in bytes.
    pub fn size(&self) -> u64 {
        self.size
    }

    /// Append an entry to this segment. Returns the offset at which it was written.
    pub fn append_entry(&mut self, payload: &[u8]) -> Result<u64> {
        let writer = self.writer.as_mut().ok_or_else(|| {
            io::Error::new(io::ErrorKind::Other, "Segment not open for writing")
        })?;

        let offset = self.size;

        // Compute CRC over payload
        let mut hasher = Hasher::new();
        hasher.update(payload);
        let crc = hasher.finalize();

        // Write: [len: u32][crc: u32][payload]
        writer.write_u32::<BigEndian>(payload.len() as u32)?;
        writer.write_u32::<BigEndian>(crc)?;
        writer.write_all(payload)?;

        self.size += 4 + 4 + payload.len() as u64;

        Ok(offset)
    }

    /// Sync the segment file to disk.
    pub fn sync(&mut self) -> Result<()> {
        if let Some(writer) = self.writer.as_mut() {
            writer.flush()?;
            writer.get_ref().sync_all()?;
        }
        Ok(())
    }

    /// Read all entries from this (read-opened) segment.
    /// Returns an iterator over payloads. Stops at first structural error.
    pub fn read_all_entries(&self) -> Vec<Result<Vec<u8>>> {
        let mut results = Vec::new();

        let file = match File::open(&self.path) {
            Ok(f) => f,
            Err(e) => {
                results.push(Err(CommitLogError::Io(e)));
                return results;
            }
        };

        let mut reader = BufReader::new(file);

        // Skip header
        if reader.seek(SeekFrom::Start(HEADER_SIZE)).is_err() {
            return results;
        }

        loop {
            // Try to read entry length
            let len = match reader.read_u32::<BigEndian>() {
                Ok(l) => l,
                Err(ref e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => {
                    results.push(Err(CommitLogError::Io(e)));
                    break;
                }
            };

            // Read CRC
            let stored_crc = match reader.read_u32::<BigEndian>() {
                Ok(c) => c,
                Err(e) => {
                    results.push(Err(CommitLogError::Io(e)));
                    break;
                }
            };

            // Read payload
            let mut payload = vec![0u8; len as usize];
            if let Err(e) = reader.read_exact(&mut payload) {
                results.push(Err(CommitLogError::Io(e)));
                break;
            }

            // Verify CRC
            let mut hasher = Hasher::new();
            hasher.update(&payload);
            let computed_crc = hasher.finalize();

            if stored_crc != computed_crc {
                let current_pos = reader.stream_position().unwrap_or(0);
                results.push(Err(CommitLogError::CrcMismatch {
                    segment_id: self.id,
                    offset: current_pos - len as u64 - 8,
                    expected: stored_crc,
                    actual: computed_crc,
                }));
                break;
            }

            results.push(Ok(payload));
        }

        results
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn create_and_read_segment() {
        let dir = TempDir::new().unwrap();
        let mut seg = Segment::create(dir.path(), 1).unwrap();

        seg.append_entry(b"hello").unwrap();
        seg.append_entry(b"world").unwrap();
        seg.sync().unwrap();

        // Read it back
        let path = dir.path().join(segment_filename(1));
        let read_seg = Segment::open_for_read(&path).unwrap();
        assert_eq!(read_seg.id(), 1);

        let entries: Vec<_> = read_seg
            .read_all_entries()
            .into_iter()
            .map(|r| r.unwrap())
            .collect();

        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0], b"hello");
        assert_eq!(entries[1], b"world");
    }

    #[test]
    fn corrupt_crc_detected() {
        let dir = TempDir::new().unwrap();
        let mut seg = Segment::create(dir.path(), 2).unwrap();
        seg.append_entry(b"valid").unwrap();
        seg.sync().unwrap();

        // Corrupt the CRC by writing bad bytes after the header
        let path = dir.path().join(segment_filename(2));
        let mut file = OpenOptions::new().write(true).open(&path).unwrap();
        // Header = 16 bytes, then entry: [len=4][crc=4][payload]
        // Corrupt the CRC at offset 16+4 = 20
        file.seek(SeekFrom::Start(HEADER_SIZE + 4)).unwrap();
        file.write_all(&[0xFF, 0xFF, 0xFF, 0xFF]).unwrap();

        let read_seg = Segment::open_for_read(&path).unwrap();
        let entries = read_seg.read_all_entries();
        assert_eq!(entries.len(), 1);
        assert!(entries[0].is_err());
    }

    #[test]
    fn empty_segment_no_entries() {
        let dir = TempDir::new().unwrap();
        let seg = Segment::create(dir.path(), 3).unwrap();
        // Don't write any entries, just sync
        drop(seg);

        let path = dir.path().join(segment_filename(3));
        let read_seg = Segment::open_for_read(&path).unwrap();
        let entries = read_seg.read_all_entries();
        assert!(entries.is_empty());
    }

    #[test]
    fn header_validation() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("bad.log");
        fs::write(&path, b"NOT_A_SEGMENT").unwrap();

        let result = Segment::open_for_read(&path);
        assert!(result.is_err());
    }
}
