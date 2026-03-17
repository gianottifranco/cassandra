// Licensed under Apache License, Version 2.0.

//! SSTable format versioning and capability detection.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.io.sstable.format.Version`
//! - `org.apache.cassandra.io.sstable.format.big.BigFormat.BigVersion`

use std::fmt;
use std::str::FromStr;

/// SSTable on-disk format version.
///
/// Each version declares a set of capabilities so that readers can adapt
/// their parsing logic based on the version byte stored in the file header.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FormatVersion {
    /// V1: original format. JSON statistics, no range-tombstone markers,
    /// no summary-assisted lookup.
    V1,
    /// V2: binary metadata in Statistics.db, range-tombstone markers in
    /// Data.db, summary-assisted index lookup.
    V2,
}

/// Version byte constants.
const V1_BYTE: u8 = 1;
const V2_BYTE: u8 = 2;

impl FormatVersion {
    /// The current (latest) format version used for new SSTables.
    pub fn current() -> Self {
        FormatVersion::V2
    }

    /// Decode a version from the byte stored in a file header.
    pub fn from_version_byte(byte: u8) -> Result<Self, FormatVersionError> {
        match byte {
            V1_BYTE => Ok(FormatVersion::V1),
            V2_BYTE => Ok(FormatVersion::V2),
            other => Err(FormatVersionError::UnknownVersion(other)),
        }
    }

    /// Encode this version as the byte written into file headers.
    pub fn data_version_byte(self) -> u8 {
        match self {
            FormatVersion::V1 => V1_BYTE,
            FormatVersion::V2 => V2_BYTE,
        }
    }

    /// Whether this version can be read by the current code.
    pub fn is_readable(self) -> bool {
        matches!(self, FormatVersion::V1 | FormatVersion::V2)
    }

    // ── Capability queries ──────────────────────────────────────────

    /// V2+ stores Statistics.db in a compact binary format instead of JSON.
    pub fn has_binary_metadata(self) -> bool {
        matches!(self, FormatVersion::V2)
    }

    /// V2+ writes explicit range-tombstone boundary markers in Data.db.
    pub fn has_range_tombstone_markers(self) -> bool {
        matches!(self, FormatVersion::V2)
    }

    /// V2+ uses Summary.db entries to narrow index binary-search windows.
    pub fn has_summary_lookup(self) -> bool {
        matches!(self, FormatVersion::V2)
    }
}

// ─── Display / FromStr ──────────────────────────────────────────────────────

impl fmt::Display for FormatVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FormatVersion::V1 => write!(f, "v1"),
            FormatVersion::V2 => write!(f, "v2"),
        }
    }
}

impl FromStr for FormatVersion {
    type Err = FormatVersionError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "v1" | "1" => Ok(FormatVersion::V1),
            "v2" | "2" => Ok(FormatVersion::V2),
            _ => Err(FormatVersionError::ParseError(s.to_string())),
        }
    }
}

// ─── Errors ─────────────────────────────────────────────────────────────────

/// Errors related to format version handling.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum FormatVersionError {
    #[error("unknown SSTable format version byte: {0:#04x}")]
    UnknownVersion(u8),
    #[error("cannot parse format version from string: {0}")]
    ParseError(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_byte_round_trip_v1() {
        let v = FormatVersion::V1;
        let byte = v.data_version_byte();
        assert_eq!(FormatVersion::from_version_byte(byte).unwrap(), v);
    }

    #[test]
    fn version_byte_round_trip_v2() {
        let v = FormatVersion::V2;
        let byte = v.data_version_byte();
        assert_eq!(FormatVersion::from_version_byte(byte).unwrap(), v);
    }

    #[test]
    fn unknown_version_byte_errors() {
        let err = FormatVersion::from_version_byte(0xFF).unwrap_err();
        assert_eq!(err, FormatVersionError::UnknownVersion(0xFF));
    }

    #[test]
    fn v1_capabilities() {
        let v = FormatVersion::V1;
        assert!(!v.has_binary_metadata());
        assert!(!v.has_range_tombstone_markers());
        assert!(!v.has_summary_lookup());
        assert!(v.is_readable());
    }

    #[test]
    fn v2_capabilities() {
        let v = FormatVersion::V2;
        assert!(v.has_binary_metadata());
        assert!(v.has_range_tombstone_markers());
        assert!(v.has_summary_lookup());
        assert!(v.is_readable());
    }

    #[test]
    fn current_is_v2() {
        assert_eq!(FormatVersion::current(), FormatVersion::V2);
    }

    #[test]
    fn display_format() {
        assert_eq!(FormatVersion::V1.to_string(), "v1");
        assert_eq!(FormatVersion::V2.to_string(), "v2");
    }

    #[test]
    fn from_str_round_trip() {
        assert_eq!("v1".parse::<FormatVersion>().unwrap(), FormatVersion::V1);
        assert_eq!("V2".parse::<FormatVersion>().unwrap(), FormatVersion::V2);
        assert_eq!("1".parse::<FormatVersion>().unwrap(), FormatVersion::V1);
        assert_eq!("2".parse::<FormatVersion>().unwrap(), FormatVersion::V2);
    }

    #[test]
    fn from_str_invalid() {
        assert!("v3".parse::<FormatVersion>().is_err());
        assert!("abc".parse::<FormatVersion>().is_err());
    }
}
