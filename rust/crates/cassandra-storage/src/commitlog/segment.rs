// Licensed under Apache License, Version 2.0.

//! Commit log segment: a single append-only file with header and CRC'd entries.
//!
//! ## Format
//!
//! ```text
//! ┌─────────────────────────────────────────────┐
//! │  Magic: [u8; 4] = b"CLSG"                  │
//! │  Version: u8 = 2                            │
//! │  Segment ID: u64 (big-endian)               │
//! │  Flags: u8 (bit0=compressed, bit1=encrypted)│
//! │  Reserved: [u8; 2]                          │
//! ├─────────────────────────────────────────────┤
//! │  Entry: [len:u32][flags:u8][crc32:u32][pay] │
//! │  ...                                        │
//! └─────────────────────────────────────────────┘
//! ```

use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use crc32fast::Hasher;

use super::{
    CommitLogError, Result,
    encrypted::{EncryptedSegmentWriter, PlainSegmentWriter},
    segment_filename,
};

const MAGIC: [u8; 4] = *b"CLSG";
const VERSION_1: u8 = 1;
const VERSION_2: u8 = 2;
const HEADER_SIZE: u64 = 4 + 1 + 8 + 1 + 2; // 16 bytes (same size as v1)

// Entry flags
const ENTRY_FLAG_COMPRESSED: u8 = 0x01;
const ENTRY_FLAG_ENCRYPTED: u8 = 0x02;

/// Segment-level flags stored in the header.
#[derive(Debug, Clone, Copy, Default)]
pub struct SegmentFlags {
    /// If true, entries in this segment may be LZ4-compressed.
    pub compression_enabled: bool,
    /// If true, entries in this segment may be encrypted.
    pub encryption_enabled: bool,
}

impl SegmentFlags {
    fn to_byte(self) -> u8 {
        let mut b = 0u8;
        if self.compression_enabled {
            b |= 0x01;
        }
        if self.encryption_enabled {
            b |= 0x02;
        }
        b
    }

    fn from_byte(b: u8) -> Self {
        Self {
            compression_enabled: (b & 0x01) != 0,
            encryption_enabled: (b & 0x02) != 0,
        }
    }
}

/// A single commit log segment file.
pub struct Segment {
    id: u64,
    path: PathBuf,
    writer: Option<BufWriter<File>>,
    size: u64,
    flags: SegmentFlags,
    version: u8,
}

impl Segment {
    /// Create a new segment file with a v2 header.
    pub fn create(dir: &Path, id: u64) -> Result<Self> {
        Self::create_with_flags(dir, id, SegmentFlags::default())
    }

    /// Create a new segment with explicit flags.
    pub fn create_with_flags(dir: &Path, id: u64, flags: SegmentFlags) -> Result<Self> {
        let path = dir.join(segment_filename(id));
        let mut file = BufWriter::new(
            OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&path)?,
        );

        // Write v2 header
        file.write_all(&MAGIC)?;
        file.write_u8(VERSION_2)?;
        file.write_u64::<BigEndian>(id)?;
        file.write_u8(flags.to_byte())?;
        file.write_all(&[0u8; 2])?; // reserved
        file.flush()?;

        Ok(Self {
            id,
            path,
            writer: Some(file),
            size: HEADER_SIZE,
            flags,
            version: VERSION_2,
        })
    }

    /// Recycle a segment file: truncate and write a fresh header.
    pub fn recycle(path: &Path, new_id: u64, flags: SegmentFlags) -> Result<Self> {
        let mut file = BufWriter::new(OpenOptions::new().write(true).truncate(true).open(path)?);

        file.write_all(&MAGIC)?;
        file.write_u8(VERSION_2)?;
        file.write_u64::<BigEndian>(new_id)?;
        file.write_u8(flags.to_byte())?;
        file.write_all(&[0u8; 2])?;
        file.flush()?;

        let new_path = path.parent().unwrap().join(segment_filename(new_id));
        if path != new_path {
            fs::rename(path, &new_path)?;
        }

        Ok(Self {
            id: new_id,
            path: new_path,
            writer: Some(file),
            size: HEADER_SIZE,
            flags,
            version: VERSION_2,
        })
    }

    /// Open an existing segment file for reading. Supports v1 and v2 headers.
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
        if version != VERSION_1 && version != VERSION_2 {
            return Err(CommitLogError::CorruptHeader {
                path: path.to_path_buf(),
            });
        }

        let id = file.read_u64::<BigEndian>()?;

        let flags = if version >= VERSION_2 {
            let fb = file.read_u8()?;
            let mut reserved = [0u8; 2];
            file.read_exact(&mut reserved)?;
            SegmentFlags::from_byte(fb)
        } else {
            // v1: 3 reserved bytes, no flags
            let mut reserved = [0u8; 3];
            file.read_exact(&mut reserved)?;
            SegmentFlags::default()
        };

        let size = fs::metadata(path)?.len();

        Ok(Self {
            id,
            path: path.to_path_buf(),
            writer: None,
            size,
            flags,
            version,
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

    /// Segment path.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// The segment flags.
    pub fn flags(&self) -> SegmentFlags {
        self.flags
    }

    /// Append an entry to this segment. Returns the offset at which it was written.
    /// If compression is enabled and beneficial, the entry is LZ4-compressed.
    pub fn append_entry(&mut self, payload: &[u8]) -> Result<u64> {
        self.append_entry_with_codec(payload, &PlainSegmentWriter)
    }

    /// Append an entry using an optional encryption codec. Compression is applied
    /// before encryption, matching Cassandra's commitlog write pipeline.
    pub fn append_entry_with_codec(
        &mut self,
        payload: &[u8],
        codec: &dyn EncryptedSegmentWriter,
    ) -> Result<u64> {
        let writer = self
            .writer
            .as_mut()
            .ok_or_else(|| io::Error::other("Segment not open for writing"))?;

        let offset = self.size;

        // Optionally compress
        let (mut actual_payload, mut entry_flags) =
            if self.flags.compression_enabled && payload.len() > 64 {
                let compressed = lz4_flex::compress_prepend_size(payload);
                if compressed.len() < payload.len() {
                    (compressed, ENTRY_FLAG_COMPRESSED)
                } else {
                    (payload.to_vec(), 0u8)
                }
            } else {
                (payload.to_vec(), 0u8)
            };

        if codec.is_encrypted() {
            actual_payload = codec
                .encode_block(&actual_payload)
                .map_err(CommitLogError::Serialization)?;
            entry_flags |= ENTRY_FLAG_ENCRYPTED;
        }

        // Compute CRC over actual payload
        let mut hasher = Hasher::new();
        hasher.update(&actual_payload);
        let crc = hasher.finalize();

        // Write: [len: u32][flags: u8][crc: u32][payload]
        writer.write_u32::<BigEndian>(actual_payload.len() as u32)?;
        writer.write_u8(entry_flags)?;
        writer.write_u32::<BigEndian>(crc)?;
        writer.write_all(&actual_payload)?;

        self.size += 4 + 1 + 4 + actual_payload.len() as u64;

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
    /// `corruption_policy` controls whether to skip corrupt entries or stop.
    pub fn read_all_entries(&self) -> Vec<Result<Vec<u8>>> {
        self.read_entries_with_policy(CorruptionPolicy::StopOnCorrupt)
    }

    /// Read entries with specified corruption handling.
    pub fn read_entries_with_policy(&self, policy: CorruptionPolicy) -> Vec<Result<Vec<u8>>> {
        self.read_entries_with_codec(policy, &PlainSegmentWriter)
    }

    /// Read entries using the supplied encryption codec for encrypted entry
    /// payloads.
    pub fn read_entries_with_codec(
        &self,
        policy: CorruptionPolicy,
        codec: &dyn EncryptedSegmentWriter,
    ) -> Vec<Result<Vec<u8>>> {
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

            // Read entry flags (v2) or CRC (v1 compat)
            let (entry_flags, stored_crc) = if self.version >= VERSION_2 {
                let flags = match reader.read_u8() {
                    Ok(f) => f,
                    Err(e) => {
                        results.push(Err(CommitLogError::Io(e)));
                        break;
                    }
                };
                let crc = match reader.read_u32::<BigEndian>() {
                    Ok(c) => c,
                    Err(e) => {
                        results.push(Err(CommitLogError::Io(e)));
                        break;
                    }
                };
                (flags, crc)
            } else {
                // v1: no flags byte, just [len][crc][payload]
                let crc = match reader.read_u32::<BigEndian>() {
                    Ok(c) => c,
                    Err(e) => {
                        results.push(Err(CommitLogError::Io(e)));
                        break;
                    }
                };
                (0u8, crc)
            };

            // Read payload
            let mut raw_payload = vec![0u8; len as usize];
            if let Err(e) = reader.read_exact(&mut raw_payload) {
                results.push(Err(CommitLogError::Io(e)));
                break;
            }

            // Verify CRC
            let mut hasher = Hasher::new();
            hasher.update(&raw_payload);
            let computed_crc = hasher.finalize();

            if stored_crc != computed_crc {
                let current_pos = reader.stream_position().unwrap_or(0);
                let err = CommitLogError::CrcMismatch {
                    segment_id: self.id,
                    offset: current_pos - len as u64 - 8,
                    expected: stored_crc,
                    actual: computed_crc,
                };
                match policy {
                    CorruptionPolicy::StopOnCorrupt => {
                        results.push(Err(err));
                        break;
                    }
                    CorruptionPolicy::SkipAndContinue => {
                        results.push(Err(err));
                        continue;
                    }
                }
            }

            let decoded_payload = if (entry_flags & ENTRY_FLAG_ENCRYPTED) != 0 {
                match codec.decode_block(&raw_payload) {
                    Ok(decoded) => decoded,
                    Err(e) => {
                        results.push(Err(CommitLogError::Serialization(format!(
                            "commitlog decryption failed: {e}"
                        ))));
                        match policy {
                            CorruptionPolicy::StopOnCorrupt => break,
                            CorruptionPolicy::SkipAndContinue => continue,
                        }
                    }
                }
            } else {
                raw_payload
            };

            // Decompress if needed
            let payload = if (entry_flags & ENTRY_FLAG_COMPRESSED) != 0 {
                match lz4_flex::decompress_size_prepended(&decoded_payload) {
                    Ok(decompressed) => decompressed,
                    Err(e) => {
                        results.push(Err(CommitLogError::Serialization(format!(
                            "LZ4 decompression failed: {e}"
                        ))));
                        match policy {
                            CorruptionPolicy::StopOnCorrupt => break,
                            CorruptionPolicy::SkipAndContinue => continue,
                        }
                    }
                }
            } else {
                decoded_payload
            };

            results.push(Ok(payload));
        }

        results
    }
}

/// How to handle corrupt entries during replay.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CorruptionPolicy {
    /// Stop at first corrupt entry (default, safest).
    #[default]
    StopOnCorrupt,
    /// Skip corrupt entries and continue replaying.
    SkipAndContinue,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commitlog::encrypted::{CommitLogEncryptor, EncryptingSegmentWriter};
    use std::sync::Arc;
    use tempfile::TempDir;

    #[derive(Debug)]
    struct XorEncryptor(u8);

    impl CommitLogEncryptor for XorEncryptor {
        fn encrypt_segment(&self, data: &[u8]) -> std::result::Result<Vec<u8>, String> {
            Ok(data.iter().map(|byte| byte ^ self.0).collect())
        }

        fn decrypt_segment(&self, data: &[u8]) -> std::result::Result<Vec<u8>, String> {
            self.encrypt_segment(data)
        }

        fn is_enabled(&self) -> bool {
            true
        }
    }

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
        // Header = 16 bytes, then entry: [len=4][flags=1][crc=4][payload]
        // Corrupt the CRC at offset 16+4+1 = 21
        file.seek(SeekFrom::Start(HEADER_SIZE + 5)).unwrap();
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

    #[test]
    fn compressed_entries() {
        let dir = TempDir::new().unwrap();
        let flags = SegmentFlags {
            compression_enabled: true,
            ..SegmentFlags::default()
        };
        let mut seg = Segment::create_with_flags(dir.path(), 10, flags).unwrap();

        // Large enough payload to benefit from compression
        let payload = "x".repeat(1024);
        seg.append_entry(payload.as_bytes()).unwrap();
        seg.sync().unwrap();

        let path = dir.path().join(segment_filename(10));
        let read_seg = Segment::open_for_read(&path).unwrap();
        assert!(read_seg.flags().compression_enabled);

        let entries: Vec<_> = read_seg
            .read_all_entries()
            .into_iter()
            .map(|r| r.unwrap())
            .collect();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0], payload.as_bytes());
    }

    #[test]
    fn compressed_encrypted_entries_round_trip_with_codec() {
        let dir = TempDir::new().unwrap();
        let flags = SegmentFlags {
            compression_enabled: true,
            encryption_enabled: true,
        };
        let codec = EncryptingSegmentWriter::new(Arc::new(XorEncryptor(0x5a)));
        let mut seg = Segment::create_with_flags(dir.path(), 11, flags).unwrap();

        let compressible_payload = vec![b'a'; 1024];
        let small_payload = b"small commitlog mutation".to_vec();
        seg.append_entry_with_codec(&compressible_payload, &codec)
            .unwrap();
        seg.append_entry_with_codec(&small_payload, &codec).unwrap();
        seg.sync().unwrap();

        let path = dir.path().join(segment_filename(11));
        let read_seg = Segment::open_for_read(&path).unwrap();
        assert!(read_seg.flags().compression_enabled);
        assert!(read_seg.flags().encryption_enabled);

        let entries: Vec<_> = read_seg
            .read_entries_with_codec(CorruptionPolicy::StopOnCorrupt, &codec)
            .into_iter()
            .map(|r| r.unwrap())
            .collect();
        assert_eq!(entries, vec![compressible_payload, small_payload]);
    }

    #[test]
    fn skip_and_continue_policy() {
        let dir = TempDir::new().unwrap();
        let mut seg = Segment::create(dir.path(), 20).unwrap();
        seg.append_entry(b"entry1").unwrap();
        seg.append_entry(b"entry2").unwrap();
        seg.append_entry(b"entry3").unwrap();
        seg.sync().unwrap();

        // Read back with StopOnCorrupt — should get all 3
        let path = dir.path().join(segment_filename(20));
        let read_seg = Segment::open_for_read(&path).unwrap();
        let entries = read_seg.read_entries_with_policy(CorruptionPolicy::SkipAndContinue);
        assert_eq!(entries.len(), 3);
        assert!(entries.iter().all(|e| e.is_ok()));
    }

    #[test]
    fn recycle_segment() {
        let dir = TempDir::new().unwrap();
        let mut seg = Segment::create(dir.path(), 100).unwrap();
        seg.append_entry(b"old_data").unwrap();
        seg.sync().unwrap();

        let old_path = dir.path().join(segment_filename(100));
        assert!(old_path.exists());

        // Recycle
        let mut recycled = Segment::recycle(&old_path, 200, SegmentFlags::default()).unwrap();
        recycled.append_entry(b"new_data").unwrap();
        recycled.sync().unwrap();

        assert_eq!(recycled.id(), 200);

        let new_path = dir.path().join(segment_filename(200));
        let read_seg = Segment::open_for_read(&new_path).unwrap();
        let entries: Vec<_> = read_seg
            .read_all_entries()
            .into_iter()
            .map(|r| r.unwrap())
            .collect();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0], b"new_data");
    }
}
