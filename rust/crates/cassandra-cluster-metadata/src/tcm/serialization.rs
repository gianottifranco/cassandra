// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

//! Binary serialization for TCM metadata log entries and epochs.
//!
//! Provides versioned serialization so that on-disk and on-wire formats
//! can evolve without breaking backward compatibility.
//!
//! ## Java Oracle
//!
//! - `serialization/Version.java`
//! - `serialization/MetadataSerializer.java`

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::node::NodeId;
use crate::tcm::{Epoch, MetadataLogEntry, Transformation};

// ─────────────────────────────────────────────────────────────────────────────
// SerializationVersion
// ─────────────────────────────────────────────────────────────────────────────

/// Wire/disk format version for TCM serialization.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SerializationVersion {
    V1,
    V2,
}

impl SerializationVersion {
    /// Encode the version as a single byte.
    pub fn as_u8(&self) -> u8 {
        match self {
            Self::V1 => 1,
            Self::V2 => 2,
        }
    }

    /// Decode a version from a single byte.
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            1 => Some(Self::V1),
            2 => Some(Self::V2),
            _ => None,
        }
    }

    /// The latest supported version.
    pub fn latest() -> Self {
        Self::V2
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// SerializationError
// ─────────────────────────────────────────────────────────────────────────────

/// Errors that can occur during TCM serialization or deserialization.
#[derive(Debug, thiserror::Error)]
pub enum SerializationError {
    #[error("buffer too short: needed {needed} bytes, only {available} available")]
    BufferTooShort { needed: usize, available: usize },

    #[error("unsupported serialization version: {0}")]
    UnsupportedVersion(u8),

    #[error("invalid data: {0}")]
    InvalidData(String),

    #[error("I/O error: {0}")]
    Io(String),
}

// ─────────────────────────────────────────────────────────────────────────────
// MetadataSerializer trait
// ─────────────────────────────────────────────────────────────────────────────

/// Trait for versioned serialization of TCM types.
pub trait MetadataSerializer {
    /// Serialize an epoch into the buffer.
    fn serialize_epoch(&self, epoch: &Epoch, buf: &mut Vec<u8>);

    /// Deserialize an epoch from the buffer, returning it and the number of
    /// bytes consumed.
    fn deserialize_epoch(&self, buf: &[u8]) -> Result<(Epoch, usize), SerializationError>;

    /// Serialize a metadata log entry into the buffer.
    fn serialize_entry(&self, entry: &MetadataLogEntry, buf: &mut Vec<u8>);

    /// Deserialize a metadata log entry from the buffer, returning it and the
    /// number of bytes consumed.
    fn deserialize_entry(
        &self,
        buf: &[u8],
    ) -> Result<(MetadataLogEntry, usize), SerializationError>;
}

// ─────────────────────────────────────────────────────────────────────────────
// Helper functions
// ─────────────────────────────────────────────────────────────────────────────

/// Write a single byte to the buffer.
pub fn write_u8(buf: &mut Vec<u8>, v: u8) {
    buf.push(v);
}

/// Write a `u64` in big-endian byte order to the buffer.
pub fn write_u64_be(buf: &mut Vec<u8>, v: u64) {
    buf.extend_from_slice(&v.to_be_bytes());
}

/// Write a length-prefixed byte slice to the buffer (u32 BE length + data).
pub fn write_bytes(buf: &mut Vec<u8>, data: &[u8]) {
    let len = data.len() as u32;
    buf.extend_from_slice(&len.to_be_bytes());
    buf.extend_from_slice(data);
}

/// Write a length-prefixed UTF-8 string to the buffer.
pub fn write_string(buf: &mut Vec<u8>, s: &str) {
    write_bytes(buf, s.as_bytes());
}

/// Read a single byte from `buf` at `offset`.
///
/// Returns the byte and the new offset.
pub fn read_u8(buf: &[u8], offset: usize) -> Result<(u8, usize), SerializationError> {
    if offset >= buf.len() {
        return Err(SerializationError::BufferTooShort {
            needed: offset + 1,
            available: buf.len(),
        });
    }
    Ok((buf[offset], offset + 1))
}

/// Read a big-endian `u64` from `buf` at `offset`.
///
/// Returns the value and the new offset.
pub fn read_u64_be(buf: &[u8], offset: usize) -> Result<(u64, usize), SerializationError> {
    let end = offset + 8;
    if end > buf.len() {
        return Err(SerializationError::BufferTooShort {
            needed: end,
            available: buf.len(),
        });
    }
    let bytes: [u8; 8] = buf[offset..end]
        .try_into()
        .expect("slice length is exactly 8");
    Ok((u64::from_be_bytes(bytes), end))
}

/// Read a length-prefixed byte slice from `buf` at `offset`.
///
/// Returns the bytes and the new offset.
pub fn read_bytes(buf: &[u8], offset: usize) -> Result<(Vec<u8>, usize), SerializationError> {
    let len_end = offset + 4;
    if len_end > buf.len() {
        return Err(SerializationError::BufferTooShort {
            needed: len_end,
            available: buf.len(),
        });
    }
    let len_bytes: [u8; 4] = buf[offset..len_end]
        .try_into()
        .expect("slice length is exactly 4");
    let len = u32::from_be_bytes(len_bytes) as usize;
    let data_end = len_end + len;
    if data_end > buf.len() {
        return Err(SerializationError::BufferTooShort {
            needed: data_end,
            available: buf.len(),
        });
    }
    Ok((buf[len_end..data_end].to_vec(), data_end))
}

/// Read a length-prefixed UTF-8 string from `buf` at `offset`.
///
/// Returns the string and the new offset.
pub fn read_string(buf: &[u8], offset: usize) -> Result<(String, usize), SerializationError> {
    let (bytes, new_offset) = read_bytes(buf, offset)?;
    let s = String::from_utf8(bytes)
        .map_err(|e| SerializationError::InvalidData(format!("invalid UTF-8: {e}")))?;
    Ok((s, new_offset))
}

// ─────────────────────────────────────────────────────────────────────────────
// V2Serializer
// ─────────────────────────────────────────────────────────────────────────────

/// Version 2 serializer for TCM metadata.
///
/// Epoch format: `[version_byte(2)] [epoch_u64_be]`
///
/// Entry format: `[version_byte(2)] [epoch_u64_be] [committed_by_uuid(16 bytes)]
///                [transformation_json_length_prefixed]`
pub struct V2Serializer;

impl MetadataSerializer for V2Serializer {
    fn serialize_epoch(&self, epoch: &Epoch, buf: &mut Vec<u8>) {
        write_u8(buf, SerializationVersion::V2.as_u8());
        write_u64_be(buf, epoch.value());
    }

    fn deserialize_epoch(&self, buf: &[u8]) -> Result<(Epoch, usize), SerializationError> {
        let (version_byte, offset) = read_u8(buf, 0)?;
        if version_byte != SerializationVersion::V2.as_u8() {
            return Err(SerializationError::UnsupportedVersion(version_byte));
        }
        let (value, offset) = read_u64_be(buf, offset)?;
        Ok((Epoch(value), offset))
    }

    fn serialize_entry(&self, entry: &MetadataLogEntry, buf: &mut Vec<u8>) {
        write_u8(buf, SerializationVersion::V2.as_u8());
        write_u64_be(buf, entry.epoch.value());
        buf.extend_from_slice(entry.committed_by.0.as_bytes());
        let json = serde_json::to_string(&entry.transformation)
            .expect("transformation should be serializable to JSON");
        write_string(buf, &json);
    }

    fn deserialize_entry(
        &self,
        buf: &[u8],
    ) -> Result<(MetadataLogEntry, usize), SerializationError> {
        let (version_byte, offset) = read_u8(buf, 0)?;
        if version_byte != SerializationVersion::V2.as_u8() {
            return Err(SerializationError::UnsupportedVersion(version_byte));
        }

        let (epoch_value, offset) = read_u64_be(buf, offset)?;

        // Read 16 bytes for the UUID.
        let uuid_end = offset + 16;
        if uuid_end > buf.len() {
            return Err(SerializationError::BufferTooShort {
                needed: uuid_end,
                available: buf.len(),
            });
        }
        let uuid = Uuid::from_slice(&buf[offset..uuid_end])
            .map_err(|e| SerializationError::InvalidData(format!("invalid UUID: {e}")))?;
        let offset = uuid_end;

        let (json_str, offset) = read_string(buf, offset)?;
        let transformation: Transformation = serde_json::from_str(&json_str)
            .map_err(|e| SerializationError::InvalidData(format!("invalid JSON: {e}")))?;

        let entry = MetadataLogEntry {
            epoch: Epoch(epoch_value),
            transformation,
            committed_by: NodeId::from_uuid(uuid),
        };
        Ok((entry, offset))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tcm::Transformation;

    fn node_id(n: u128) -> NodeId {
        NodeId::from_uuid(Uuid::from_u128(n))
    }

    #[test]
    fn round_trip_epoch_serialization() {
        let serializer = V2Serializer;
        let epoch = Epoch(42);

        let mut buf = Vec::new();
        serializer.serialize_epoch(&epoch, &mut buf);

        let (deserialized, consumed) = serializer.deserialize_epoch(&buf).unwrap();
        assert_eq!(deserialized, epoch);
        assert_eq!(consumed, buf.len());
    }

    #[test]
    fn round_trip_entry_serialization() {
        let serializer = V2Serializer;
        let entry = MetadataLogEntry {
            epoch: Epoch(7),
            transformation: Transformation::SchemaChange {
                schema_version: Uuid::from_u128(0xDEAD),
                description: "create table foo".into(),
            },
            committed_by: node_id(0xBEEF),
        };

        let mut buf = Vec::new();
        serializer.serialize_entry(&entry, &mut buf);

        let (deserialized, consumed) = serializer.deserialize_entry(&buf).unwrap();
        assert_eq!(consumed, buf.len());
        assert_eq!(deserialized.epoch, entry.epoch);
        assert_eq!(deserialized.committed_by, entry.committed_by);
        assert_eq!(deserialized.transformation, entry.transformation);
    }

    #[test]
    fn version_mismatch_error() {
        let serializer = V2Serializer;
        // Build a buffer with version byte = 1 (V1), which V2Serializer rejects.
        let mut buf = Vec::new();
        write_u8(&mut buf, 1);
        write_u64_be(&mut buf, 99);

        let result = serializer.deserialize_epoch(&buf);
        assert!(result.is_err());
        match result.unwrap_err() {
            SerializationError::UnsupportedVersion(v) => assert_eq!(v, 1),
            other => panic!("expected UnsupportedVersion, got: {other:?}"),
        }
    }

    #[test]
    fn buffer_too_short_error() {
        let serializer = V2Serializer;
        // A buffer with only the version byte and no epoch data.
        let buf = vec![2u8];
        let result = serializer.deserialize_epoch(&buf);
        assert!(result.is_err());
        match result.unwrap_err() {
            SerializationError::BufferTooShort { needed, available } => {
                assert!(needed > available);
            }
            other => panic!("expected BufferTooShort, got: {other:?}"),
        }
    }

    #[test]
    fn string_round_trip() {
        let original = "hello, cassandra!";
        let mut buf = Vec::new();
        write_string(&mut buf, original);

        let (result, consumed) = read_string(&buf, 0).unwrap();
        assert_eq!(result, original);
        assert_eq!(consumed, buf.len());
    }
}
