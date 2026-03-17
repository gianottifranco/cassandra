// Licensed under Apache License, Version 2.0.

//! Error types for the cassandra-io crate.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.io.FSError`
//! - `org.apache.cassandra.io.compress.CorruptBlockException`

use thiserror::Error;

/// Unified error type for IO operations in the cassandra-io crate.
#[derive(Debug, Error)]
pub enum IoError {
    /// Wraps a standard IO error.
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    /// An error occurred during compression or decompression.
    #[error("Compression error: {0}")]
    Compression(String),

    /// The data is corrupt or cannot be decoded.
    #[error("Corrupt data: {0}")]
    CorruptData(String),

    /// A checksum verification failed.
    #[error("Checksum mismatch: expected {expected:#010x}, actual {actual:#010x}")]
    ChecksumMismatch { expected: u32, actual: u32 },

    /// The data format is invalid or unrecognized.
    #[error("Invalid format: {0}")]
    InvalidFormat(String),

    /// A buffer is too small for the requested operation.
    #[error("Buffer too small: needed {needed} bytes, only {available} available")]
    BufferTooSmall { needed: usize, available: usize },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_io_error_display() {
        let err = IoError::Compression("lz4 failed".into());
        assert!(err.to_string().contains("lz4 failed"));
    }

    #[test]
    fn test_checksum_mismatch_display() {
        let err = IoError::ChecksumMismatch {
            expected: 0xDEADBEEF,
            actual: 0xCAFEBABE,
        };
        let msg = err.to_string();
        assert!(msg.contains("0xdeadbeef"));
        assert!(msg.contains("0xcafebabe"));
    }

    #[test]
    fn test_buffer_too_small_display() {
        let err = IoError::BufferTooSmall {
            needed: 1024,
            available: 512,
        };
        let msg = err.to_string();
        assert!(msg.contains("1024"));
        assert!(msg.contains("512"));
    }

    #[test]
    fn test_from_io_error() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "not found");
        let err: IoError = io_err.into();
        assert!(matches!(err, IoError::Io(_)));
    }

    #[test]
    fn test_corrupt_data_display() {
        let err = IoError::CorruptData("bad block".into());
        assert!(err.to_string().contains("bad block"));
    }

    #[test]
    fn test_invalid_format_display() {
        let err = IoError::InvalidFormat("unknown header".into());
        assert!(err.to_string().contains("unknown header"));
    }
}
