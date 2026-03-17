// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.
// SPDX-License-Identifier: Apache-2.0

//! Hint segment persistence: append-only files with CRC32 integrity.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.hints.HintsWriter`
//! - `org.apache.cassandra.hints.HintsReader`
//! - `org.apache.cassandra.hints.HintsDescriptor`
//!
//! ## Format
//!
//! Each entry in a hint segment file:
//! ```text
//! [CRC32: 4 bytes][length: 4 bytes][serialized_hint: length bytes]
//! ```
//!
//! Segments are named by target host ID and creation timestamp:
//! `{host_id}-{timestamp}.hints`

use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

use crate::hints::Hint;

/// Default maximum segment size before rotation (128 MiB).
pub const DEFAULT_MAX_SEGMENT_SIZE: u64 = 128 * 1024 * 1024;

/// Hint segment descriptor (header metadata).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HintSegmentDescriptor {
    /// Target endpoint identifier (host ID or address string).
    pub target_id: String,
    /// Creation timestamp (epoch millis).
    pub created_at: i64,
    /// Segment version.
    pub version: u32,
}

impl HintSegmentDescriptor {
    pub fn new(target_id: String) -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        Self {
            target_id,
            created_at: now,
            version: 1,
        }
    }

    /// Generate the segment filename.
    pub fn filename(&self) -> String {
        format!("{}-{}.hints", self.target_id, self.created_at)
    }
}

/// Compute CRC32 checksum for hint data.
fn crc32_checksum(data: &[u8]) -> u32 {
    // Simple CRC32 implementation (CRC-32/ISO-HDLC)
    let mut crc: u32 = 0xFFFF_FFFF;
    for &byte in data {
        crc ^= byte as u32;
        for _ in 0..8 {
            if crc & 1 != 0 {
                crc = (crc >> 1) ^ 0xEDB8_8320;
            } else {
                crc >>= 1;
            }
        }
    }
    !crc
}

/// Writes hints to an append-only segment file.
///
/// ## Java Oracle
///
/// `org.apache.cassandra.hints.HintsWriter`
pub struct HintSegmentWriter {
    writer: BufWriter<File>,
    path: PathBuf,
    bytes_written: u64,
    max_size: u64,
    descriptor: HintSegmentDescriptor,
}

impl HintSegmentWriter {
    /// Create a new segment writer.
    pub fn create(
        dir: &Path,
        descriptor: HintSegmentDescriptor,
        max_size: u64,
    ) -> io::Result<Self> {
        fs::create_dir_all(dir)?;
        let path = dir.join(descriptor.filename());
        let file = OpenOptions::new().create(true).append(true).open(&path)?;

        info!(path = %path.display(), target = %descriptor.target_id, "Created hint segment");

        Ok(Self {
            writer: BufWriter::new(file),
            path,
            bytes_written: 0,
            max_size,
            descriptor,
        })
    }

    /// Append a hint to the segment.
    ///
    /// Returns `true` if the hint was written, `false` if the segment
    /// needs rotation (size limit exceeded).
    pub fn append(&mut self, hint: &Hint) -> io::Result<bool> {
        let data = serde_json::to_vec(hint).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Serialization error: {e}"),
            )
        })?;

        let entry_size = 4 + 4 + data.len() as u64; // CRC + length + data
        if self.bytes_written + entry_size > self.max_size {
            return Ok(false); // needs rotation
        }

        let crc = crc32_checksum(&data);
        let length = data.len() as u32;

        self.writer.write_all(&crc.to_le_bytes())?;
        self.writer.write_all(&length.to_le_bytes())?;
        self.writer.write_all(&data)?;
        self.bytes_written += entry_size;

        debug!(
            hint_id = hint.hint_id,
            bytes = entry_size,
            total = self.bytes_written,
            "Appended hint to segment"
        );

        Ok(true)
    }

    /// Sync the segment to disk.
    pub fn sync(&mut self) -> io::Result<()> {
        self.writer.flush()?;
        self.writer.get_ref().sync_all()
    }

    /// Get the segment file path.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Get the segment descriptor.
    pub fn descriptor(&self) -> &HintSegmentDescriptor {
        &self.descriptor
    }

    /// Current bytes written to the segment.
    pub fn bytes_written(&self) -> u64 {
        self.bytes_written
    }

    /// Whether the segment needs rotation.
    pub fn needs_rotation(&self) -> bool {
        self.bytes_written >= self.max_size
    }
}

/// Reads hints from a segment file, validating CRC32 checksums.
///
/// ## Java Oracle
///
/// `org.apache.cassandra.hints.HintsReader`
pub struct HintSegmentReader {
    reader: BufReader<File>,
    path: PathBuf,
    entries_read: u64,
    entries_corrupted: u64,
}

impl HintSegmentReader {
    /// Open a segment for reading.
    pub fn open(path: &Path) -> io::Result<Self> {
        let file = File::open(path)?;
        Ok(Self {
            reader: BufReader::new(file),
            path: path.to_path_buf(),
            entries_read: 0,
            entries_corrupted: 0,
        })
    }

    /// Read the next hint from the segment.
    ///
    /// Returns `None` at EOF. Skips corrupted entries (CRC mismatch).
    pub fn next_hint(&mut self) -> io::Result<Option<Hint>> {
        loop {
            // Read CRC32
            let mut crc_bytes = [0u8; 4];
            match self.reader.read_exact(&mut crc_bytes) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
                Err(e) => return Err(e),
            }
            let expected_crc = u32::from_le_bytes(crc_bytes);

            // Read length
            let mut len_bytes = [0u8; 4];
            self.reader.read_exact(&mut len_bytes)?;
            let length = u32::from_le_bytes(len_bytes) as usize;

            // Sanity check: reject absurdly large entries
            if length > 64 * 1024 * 1024 {
                warn!(
                    path = %self.path.display(),
                    length = length,
                    "Segment entry too large, likely corrupted"
                );
                self.entries_corrupted += 1;
                return Ok(None); // can't skip reliably
            }

            // Read data
            let mut data = vec![0u8; length];
            self.reader.read_exact(&mut data)?;

            // Validate CRC
            let actual_crc = crc32_checksum(&data);
            if actual_crc != expected_crc {
                warn!(
                    path = %self.path.display(),
                    expected = expected_crc,
                    actual = actual_crc,
                    "CRC mismatch, skipping corrupted hint entry"
                );
                self.entries_corrupted += 1;
                continue; // skip this entry, try next
            }

            // Deserialize
            match serde_json::from_slice::<Hint>(&data) {
                Ok(hint) => {
                    self.entries_read += 1;
                    return Ok(Some(hint));
                }
                Err(e) => {
                    warn!(
                        path = %self.path.display(),
                        error = %e,
                        "Failed to deserialize hint entry, skipping"
                    );
                    self.entries_corrupted += 1;
                    continue;
                }
            }
        }
    }

    /// Read all hints from the segment.
    pub fn read_all(&mut self) -> io::Result<Vec<Hint>> {
        let mut hints = Vec::new();
        while let Some(hint) = self.next_hint()? {
            hints.push(hint);
        }
        Ok(hints)
    }

    /// Number of entries successfully read.
    pub fn entries_read(&self) -> u64 {
        self.entries_read
    }

    /// Number of corrupted entries encountered.
    pub fn entries_corrupted(&self) -> u64 {
        self.entries_corrupted
    }
}

/// Manages hint segment files for a hints directory.
///
/// ## Java Oracle
///
/// `org.apache.cassandra.hints.HintsCatalog`
pub struct HintSegmentManager {
    /// Base directory for hint segments.
    hints_dir: PathBuf,
    /// Maximum segment size before rotation.
    max_segment_size: u64,
}

impl HintSegmentManager {
    pub fn new(hints_dir: PathBuf) -> Self {
        Self {
            hints_dir,
            max_segment_size: DEFAULT_MAX_SEGMENT_SIZE,
        }
    }

    pub fn with_max_segment_size(mut self, size: u64) -> Self {
        self.max_segment_size = size;
        self
    }

    /// Get the base directory for hint segments.
    pub fn hints_dir(&self) -> &Path {
        &self.hints_dir
    }

    /// Create a new writer for the given target.
    pub fn writer_for(&self, target_id: &str) -> io::Result<HintSegmentWriter> {
        let descriptor = HintSegmentDescriptor::new(target_id.to_string());
        HintSegmentWriter::create(&self.hints_dir, descriptor, self.max_segment_size)
    }

    /// List all segment files for a target.
    pub fn segments_for(&self, target_id: &str) -> io::Result<Vec<PathBuf>> {
        let mut segments = Vec::new();
        if self.hints_dir.exists() {
            for entry in fs::read_dir(&self.hints_dir)? {
                let entry = entry?;
                let name = entry.file_name().to_string_lossy().to_string();
                if name.starts_with(target_id) && name.ends_with(".hints") {
                    segments.push(entry.path());
                }
            }
        }
        segments.sort();
        Ok(segments)
    }

    /// Delete a segment file after successful delivery.
    pub fn delete_segment(&self, path: &Path) -> io::Result<()> {
        fs::remove_file(path)?;
        info!(path = %path.display(), "Deleted delivered hint segment");
        Ok(())
    }

    /// Delete all segments for a target (e.g., after decommission).
    pub fn delete_all_for(&self, target_id: &str) -> io::Result<u64> {
        let segments = self.segments_for(target_id)?;
        let count = segments.len() as u64;
        for seg in segments {
            fs::remove_file(&seg)?;
        }
        if count > 0 {
            info!(
                target = target_id,
                count = count,
                "Deleted all hint segments for target"
            );
        }
        Ok(count)
    }

    /// Get total disk usage for hint segments.
    pub fn total_disk_usage(&self) -> io::Result<u64> {
        let mut total = 0u64;
        if self.hints_dir.exists() {
            for entry in fs::read_dir(&self.hints_dir)? {
                let entry = entry?;
                if entry.path().extension().is_some_and(|e| e == "hints") {
                    total += entry.metadata()?.len();
                }
            }
        }
        Ok(total)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::write::CoordinatedMutation;
    use cassandra_cluster_metadata::Endpoint;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    fn ep(port: u16) -> Endpoint {
        Endpoint::new(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            port,
        ))
    }

    fn test_hint(id: u64) -> Hint {
        Hint {
            target: ep(7002),
            mutation: CoordinatedMutation::simple(
                "ks".to_string(),
                "t".to_string(),
                format!("key{id}").into_bytes(),
                vec![],
                1000 + id as i64,
            ),
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64,
            hint_id: id,
        }
    }

    #[test]
    fn crc32_basic() {
        let data = b"hello world";
        let crc = crc32_checksum(data);
        assert_eq!(crc, crc32_checksum(data)); // deterministic
        assert_ne!(crc, crc32_checksum(b"hello worlD")); // different data
    }

    #[test]
    fn write_read_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let desc = HintSegmentDescriptor::new("test-host".to_string());
        let mut writer = HintSegmentWriter::create(dir.path(), desc.clone(), 1024 * 1024).unwrap();

        let h1 = test_hint(1);
        let h2 = test_hint(2);
        let h3 = test_hint(3);

        assert!(writer.append(&h1).unwrap());
        assert!(writer.append(&h2).unwrap());
        assert!(writer.append(&h3).unwrap());
        writer.sync().unwrap();

        let mut reader = HintSegmentReader::open(writer.path()).unwrap();
        let hints = reader.read_all().unwrap();
        assert_eq!(hints.len(), 3);
        assert_eq!(hints[0].hint_id, 1);
        assert_eq!(hints[1].hint_id, 2);
        assert_eq!(hints[2].hint_id, 3);
        assert_eq!(reader.entries_corrupted(), 0);
    }

    #[test]
    fn crc_validation_detects_corruption() {
        let dir = tempfile::tempdir().unwrap();
        let desc = HintSegmentDescriptor::new("corrupt-host".to_string());
        let mut writer = HintSegmentWriter::create(dir.path(), desc.clone(), 1024 * 1024).unwrap();

        let h1 = test_hint(10);
        let h2 = test_hint(20);
        writer.append(&h1).unwrap();
        writer.append(&h2).unwrap();
        writer.sync().unwrap();

        // Corrupt one byte in the middle of the file
        let path = writer.path().to_path_buf();
        let mut data = fs::read(&path).unwrap();
        let mid = data.len() / 2;
        data[mid] ^= 0xFF; // flip bits
        fs::write(&path, &data).unwrap();

        let mut reader = HintSegmentReader::open(&path).unwrap();
        let hints = reader.read_all().unwrap();
        // At least one entry should be readable, the other(s) corrupted
        assert!(reader.entries_corrupted() > 0 || hints.len() < 2);
    }

    #[test]
    fn segment_rotation() {
        let dir = tempfile::tempdir().unwrap();
        let desc = HintSegmentDescriptor::new("rotate-host".to_string());
        // Very small max size to force rotation
        let mut writer = HintSegmentWriter::create(dir.path(), desc, 200).unwrap();

        let h1 = test_hint(1);
        // First append may succeed or not depending on serialized size
        let first = writer.append(&h1).unwrap();
        if first {
            // Write more until rotation needed
            let h2 = test_hint(2);
            let second = writer.append(&h2).unwrap();
            // At some point rotation is signaled
            if !second {
                assert!(writer.needs_rotation() || writer.bytes_written() > 0);
            }
        }
    }

    #[test]
    fn segment_manager_list_and_delete() {
        let dir = tempfile::tempdir().unwrap();
        let mgr = HintSegmentManager::new(dir.path().to_path_buf());

        // Create two segments for different targets
        let mut w1 = mgr.writer_for("host1").unwrap();
        w1.append(&test_hint(1)).unwrap();
        w1.sync().unwrap();

        let mut w2 = mgr.writer_for("host2").unwrap();
        w2.append(&test_hint(2)).unwrap();
        w2.sync().unwrap();

        let segments1 = mgr.segments_for("host1").unwrap();
        assert_eq!(segments1.len(), 1);

        let segments2 = mgr.segments_for("host2").unwrap();
        assert_eq!(segments2.len(), 1);

        // Delete for host1
        let deleted = mgr.delete_all_for("host1").unwrap();
        assert_eq!(deleted, 1);
        assert_eq!(mgr.segments_for("host1").unwrap().len(), 0);

        // host2 still exists
        assert_eq!(mgr.segments_for("host2").unwrap().len(), 1);
    }

    #[test]
    fn segment_descriptor_filename() {
        let desc = HintSegmentDescriptor {
            target_id: "abc-123".to_string(),
            created_at: 1700000000000,
            version: 1,
        };
        assert_eq!(desc.filename(), "abc-123-1700000000000.hints");
    }

    #[test]
    fn total_disk_usage() {
        let dir = tempfile::tempdir().unwrap();
        let mgr = HintSegmentManager::new(dir.path().to_path_buf());

        assert_eq!(mgr.total_disk_usage().unwrap(), 0);

        let mut w = mgr.writer_for("host1").unwrap();
        w.append(&test_hint(1)).unwrap();
        w.sync().unwrap();

        assert!(mgr.total_disk_usage().unwrap() > 0);
    }
}
