// Licensed under Apache License, Version 2.0.

//! Binary metadata serialization for Statistics.db.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.io.sstable.metadata.MetadataSerializer`
//! - `org.apache.cassandra.io.sstable.metadata.StatsMetadata`
//!
//! ## Binary Format
//!
//! ```text
//! [magic: 4B "SSMT"]
//! [version: 1B]
//! [field_count: u16]
//! [partition_count: u64]
//! [row_count: u64]
//! [cell_count: u64]
//! [min_timestamp: i64]
//! [max_timestamp: i64]
//! [data_size: u64]
//! [index_size: u64]
//! [min_pk_len: u32][min_partition_key: bytes]
//! [max_pk_len: u32][max_partition_key: bytes]
//! ```

use std::io::{self, Cursor, Read, Write};

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

use super::writer::SSTableStats;

/// Magic bytes identifying the binary metadata format.
pub const METADATA_MAGIC: [u8; 4] = *b"SSMT";

/// Current binary metadata version.
const METADATA_VERSION: u8 = 1;

/// Number of fixed fields (before variable-length key fields).
const FIELD_COUNT: u16 = 9;

/// Complete SSTable metadata, a superset of [`SSTableStats`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SSTableMetadata {
    pub partition_count: u64,
    pub row_count: u64,
    pub cell_count: u64,
    pub min_timestamp: i64,
    pub max_timestamp: i64,
    pub data_size: u64,
    pub index_size: u64,
    pub min_partition_key: Vec<u8>,
    pub max_partition_key: Vec<u8>,
}

impl SSTableMetadata {
    /// Create metadata from writer stats plus partition-key bounds.
    pub fn from_stats(
        stats: &SSTableStats,
        min_partition_key: Vec<u8>,
        max_partition_key: Vec<u8>,
    ) -> Self {
        Self {
            partition_count: stats.partition_count,
            row_count: stats.row_count,
            cell_count: stats.cell_count,
            min_timestamp: stats.min_timestamp,
            max_timestamp: stats.max_timestamp,
            data_size: stats.data_size,
            index_size: stats.index_size,
            min_partition_key,
            max_partition_key,
        }
    }
}

// ─── Collector ──────────────────────────────────────────────────────────────

/// Accumulates statistics during an SSTable write.
#[derive(Debug, Default)]
pub struct MetadataCollector {
    pub partition_count: u64,
    pub row_count: u64,
    pub cell_count: u64,
    pub min_timestamp: i64,
    pub max_timestamp: i64,
    pub data_size: u64,
    pub index_size: u64,
    pub min_partition_key: Option<Vec<u8>>,
    pub max_partition_key: Option<Vec<u8>>,
}

impl MetadataCollector {
    pub fn new() -> Self {
        Self {
            min_timestamp: i64::MAX,
            max_timestamp: i64::MIN,
            ..Default::default()
        }
    }

    /// Record a partition key, updating min/max bounds.
    pub fn add_partition_key(&mut self, key: &[u8]) {
        self.partition_count += 1;
        if self.min_partition_key.is_none() || key < self.min_partition_key.as_deref().unwrap() {
            self.min_partition_key = Some(key.to_vec());
        }
        if self.max_partition_key.is_none() || key > self.max_partition_key.as_deref().unwrap() {
            self.max_partition_key = Some(key.to_vec());
        }
    }

    /// Record a cell timestamp.
    pub fn add_timestamp(&mut self, ts: i64) {
        self.min_timestamp = self.min_timestamp.min(ts);
        self.max_timestamp = self.max_timestamp.max(ts);
    }

    /// Build the final metadata snapshot.
    pub fn finish(self) -> SSTableMetadata {
        SSTableMetadata {
            partition_count: self.partition_count,
            row_count: self.row_count,
            cell_count: self.cell_count,
            min_timestamp: self.min_timestamp,
            max_timestamp: self.max_timestamp,
            data_size: self.data_size,
            index_size: self.index_size,
            min_partition_key: self.min_partition_key.unwrap_or_default(),
            max_partition_key: self.max_partition_key.unwrap_or_default(),
        }
    }
}

// ─── Serializer ─────────────────────────────────────────────────────────────

/// Serializes and deserializes [`SSTableMetadata`] in the binary format.
pub struct MetadataSerializer;

impl MetadataSerializer {
    /// Serialize metadata to a binary byte vector.
    pub fn serialize(meta: &SSTableMetadata) -> Vec<u8> {
        let mut buf = Vec::with_capacity(128);
        // Writing to a Vec<u8> never fails, unwrap is safe.
        Self::write_to(&mut buf, meta).expect("vec write");
        buf
    }

    fn write_to<W: Write>(w: &mut W, meta: &SSTableMetadata) -> io::Result<()> {
        w.write_all(&METADATA_MAGIC)?;
        w.write_u8(METADATA_VERSION)?;
        w.write_u16::<BigEndian>(FIELD_COUNT)?;
        w.write_u64::<BigEndian>(meta.partition_count)?;
        w.write_u64::<BigEndian>(meta.row_count)?;
        w.write_u64::<BigEndian>(meta.cell_count)?;
        w.write_i64::<BigEndian>(meta.min_timestamp)?;
        w.write_i64::<BigEndian>(meta.max_timestamp)?;
        w.write_u64::<BigEndian>(meta.data_size)?;
        w.write_u64::<BigEndian>(meta.index_size)?;
        // Variable-length keys
        w.write_u32::<BigEndian>(meta.min_partition_key.len() as u32)?;
        w.write_all(&meta.min_partition_key)?;
        w.write_u32::<BigEndian>(meta.max_partition_key.len() as u32)?;
        w.write_all(&meta.max_partition_key)?;
        Ok(())
    }

    /// Deserialize metadata from the binary format.
    pub fn deserialize(data: &[u8]) -> Result<SSTableMetadata, MetadataError> {
        if data.len() < 4 {
            return Err(MetadataError::TooShort);
        }
        let mut cur = Cursor::new(data);

        let mut magic = [0u8; 4];
        cur.read_exact(&mut magic)?;
        if magic != METADATA_MAGIC {
            return Err(MetadataError::BadMagic(magic));
        }

        let version = cur.read_u8()?;
        if version != METADATA_VERSION {
            return Err(MetadataError::UnsupportedVersion(version));
        }

        let field_count = cur.read_u16::<BigEndian>()?;
        if field_count != FIELD_COUNT {
            return Err(MetadataError::BadFieldCount(field_count));
        }

        let partition_count = cur.read_u64::<BigEndian>()?;
        let row_count = cur.read_u64::<BigEndian>()?;
        let cell_count = cur.read_u64::<BigEndian>()?;
        let min_timestamp = cur.read_i64::<BigEndian>()?;
        let max_timestamp = cur.read_i64::<BigEndian>()?;
        let data_size = cur.read_u64::<BigEndian>()?;
        let index_size = cur.read_u64::<BigEndian>()?;

        let min_pk_len = cur.read_u32::<BigEndian>()? as usize;
        let mut min_partition_key = vec![0u8; min_pk_len];
        cur.read_exact(&mut min_partition_key)?;

        let max_pk_len = cur.read_u32::<BigEndian>()? as usize;
        let mut max_partition_key = vec![0u8; max_pk_len];
        cur.read_exact(&mut max_partition_key)?;
        if cur.position() != data.len() as u64 {
            return Err(MetadataError::TrailingBytes(
                data.len() - cur.position() as usize,
            ));
        }

        Ok(SSTableMetadata {
            partition_count,
            row_count,
            cell_count,
            min_timestamp,
            max_timestamp,
            data_size,
            index_size,
            min_partition_key,
            max_partition_key,
        })
    }

    /// Try binary deserialization first; on failure, fall back to JSON
    /// (for backward compatibility with V1 Statistics.db files).
    pub fn deserialize_auto(data: &[u8]) -> Result<SSTableMetadata, MetadataError> {
        // Attempt binary first.
        if data.len() >= 4 && data[..4] == METADATA_MAGIC {
            return Self::deserialize(data);
        }
        // Fall back to JSON (V1 format stored SSTableStats as JSON).
        let stats: SSTableStats =
            serde_json::from_slice(data).map_err(MetadataError::JsonFallback)?;
        Ok(SSTableMetadata {
            partition_count: stats.partition_count,
            row_count: stats.row_count,
            cell_count: stats.cell_count,
            min_timestamp: stats.min_timestamp,
            max_timestamp: stats.max_timestamp,
            data_size: stats.data_size,
            index_size: stats.index_size,
            min_partition_key: Vec::new(),
            max_partition_key: Vec::new(),
        })
    }
}

// ─── Errors ─────────────────────────────────────────────────────────────────

/// Errors during metadata serialization / deserialization.
#[derive(Debug, thiserror::Error)]
pub enum MetadataError {
    #[error("metadata blob too short")]
    TooShort,
    #[error("bad magic bytes: {0:?}")]
    BadMagic([u8; 4]),
    #[error("unsupported metadata version: {0}")]
    UnsupportedVersion(u8),
    #[error("bad metadata field count: {0}")]
    BadFieldCount(u16),
    #[error("metadata has {0} trailing bytes")]
    TrailingBytes(usize),
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    #[error("JSON fallback failed: {0}")]
    JsonFallback(serde_json::Error),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_metadata() -> SSTableMetadata {
        SSTableMetadata {
            partition_count: 100,
            row_count: 500,
            cell_count: 1500,
            min_timestamp: 1000,
            max_timestamp: 2000,
            data_size: 65536,
            index_size: 4096,
            min_partition_key: vec![0x01, 0x02],
            max_partition_key: vec![0xFF, 0xFE],
        }
    }

    #[test]
    fn binary_round_trip() {
        let meta = sample_metadata();
        let bytes = MetadataSerializer::serialize(&meta);
        let decoded = MetadataSerializer::deserialize(&bytes).unwrap();
        assert_eq!(decoded, meta);
    }

    #[test]
    fn json_fallback() {
        let stats = SSTableStats {
            partition_count: 10,
            row_count: 20,
            cell_count: 30,
            min_timestamp: 100,
            max_timestamp: 200,
            data_size: 1024,
            index_size: 256,
        };
        let json = serde_json::to_vec(&stats).unwrap();
        let meta = MetadataSerializer::deserialize_auto(&json).unwrap();
        assert_eq!(meta.partition_count, 10);
        assert_eq!(meta.row_count, 20);
        assert_eq!(meta.cell_count, 30);
        assert_eq!(meta.min_timestamp, 100);
        assert_eq!(meta.max_timestamp, 200);
        // JSON fallback has no key bounds.
        assert!(meta.min_partition_key.is_empty());
        assert!(meta.max_partition_key.is_empty());
    }

    #[test]
    fn deserialize_auto_prefers_binary() {
        let meta = sample_metadata();
        let bytes = MetadataSerializer::serialize(&meta);
        let decoded = MetadataSerializer::deserialize_auto(&bytes).unwrap();
        assert_eq!(decoded, meta);
    }

    #[test]
    fn known_byte_vector() {
        let meta = SSTableMetadata {
            partition_count: 1,
            row_count: 2,
            cell_count: 3,
            min_timestamp: -1,
            max_timestamp: 100,
            data_size: 256,
            index_size: 64,
            min_partition_key: vec![0xAA],
            max_partition_key: vec![0xBB],
        };
        let bytes = MetadataSerializer::serialize(&meta);

        // Verify magic
        assert_eq!(&bytes[0..4], b"SSMT");
        // Verify version
        assert_eq!(bytes[4], 1);
        // Verify field count (big-endian u16 = 9)
        assert_eq!(&bytes[5..7], &[0x00, 0x09]);
        // Verify partition_count = 1 (big-endian u64)
        assert_eq!(&bytes[7..15], &[0, 0, 0, 0, 0, 0, 0, 1]);

        // Full round-trip
        let decoded = MetadataSerializer::deserialize(&bytes).unwrap();
        assert_eq!(decoded, meta);
    }

    #[test]
    fn too_short_data_errors() {
        assert!(MetadataSerializer::deserialize(&[0x00, 0x01]).is_err());
    }

    #[test]
    fn bad_magic_errors() {
        let mut bytes = MetadataSerializer::serialize(&sample_metadata());
        bytes[0] = 0x00; // corrupt magic
        let err = MetadataSerializer::deserialize(&bytes).unwrap_err();
        assert!(matches!(err, MetadataError::BadMagic(_)));
    }

    #[test]
    fn bad_field_count_errors() {
        let mut bytes = MetadataSerializer::serialize(&sample_metadata());
        bytes[5..7].copy_from_slice(&8u16.to_be_bytes());
        let err = MetadataSerializer::deserialize(&bytes).unwrap_err();
        assert!(matches!(err, MetadataError::BadFieldCount(8)));
    }

    #[test]
    fn trailing_bytes_error() {
        let mut bytes = MetadataSerializer::serialize(&sample_metadata());
        bytes.extend_from_slice(b"extra");
        let err = MetadataSerializer::deserialize(&bytes).unwrap_err();
        assert!(matches!(err, MetadataError::TrailingBytes(5)));
    }

    #[test]
    fn collector_tracks_bounds() {
        let mut c = MetadataCollector::new();
        c.add_partition_key(b"bbb");
        c.add_partition_key(b"aaa");
        c.add_partition_key(b"ccc");
        c.add_timestamp(100);
        c.add_timestamp(50);
        c.add_timestamp(200);
        c.row_count = 3;
        c.cell_count = 9;

        let meta = c.finish();
        assert_eq!(meta.min_partition_key, b"aaa");
        assert_eq!(meta.max_partition_key, b"ccc");
        assert_eq!(meta.min_timestamp, 50);
        assert_eq!(meta.max_timestamp, 200);
        assert_eq!(meta.partition_count, 3);
    }
}
