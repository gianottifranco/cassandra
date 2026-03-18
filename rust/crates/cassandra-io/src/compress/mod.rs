// Licensed under Apache License, Version 2.0.

//! Compression framework for Cassandra SSTable data.
//!
//! Provides a unified [`ICompressor`] trait with implementations for LZ4,
//! Snappy, Zstd, Deflate, and a no-op passthrough.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.io.compress.ICompressor`
//! - `org.apache.cassandra.io.compress.CompressorFactory`

pub mod compressed_reader;
pub mod compressed_writer;
pub mod deflate;
pub mod lz4;
pub mod metadata;
pub mod noop;
pub mod snappy;
pub mod zstd_comp;

pub use deflate::DeflateCompressor;
pub use lz4::Lz4Compressor;
pub use noop::NoopCompressor;
pub use snappy::SnappyCompressor;
pub use zstd_comp::ZstdCompressor;

use std::fmt;
use std::str::FromStr;

use crate::error::IoError;

/// Default recommended buffer size (64 KiB).
const DEFAULT_BUFFER_SIZE: usize = 64 * 1024;

/// Identifies the compression algorithm in use.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CompressorType {
    Lz4,
    Snappy,
    Zstd,
    Deflate,
    Noop,
}

impl fmt::Display for CompressorType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CompressorType::Lz4 => write!(f, "lz4"),
            CompressorType::Snappy => write!(f, "snappy"),
            CompressorType::Zstd => write!(f, "zstd"),
            CompressorType::Deflate => write!(f, "deflate"),
            CompressorType::Noop => write!(f, "noop"),
        }
    }
}

impl FromStr for CompressorType {
    type Err = IoError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "lz4" => Ok(CompressorType::Lz4),
            "snappy" => Ok(CompressorType::Snappy),
            "zstd" | "zstandard" => Ok(CompressorType::Zstd),
            "deflate" => Ok(CompressorType::Deflate),
            "noop" | "none" => Ok(CompressorType::Noop),
            _ => Err(IoError::InvalidFormat(format!(
                "Unknown compressor type: '{s}'"
            ))),
        }
    }
}

/// Trait for compression and decompression of byte buffers.
///
/// Each implementation wraps a specific compression library and provides a
/// uniform interface used by the SSTable layer.
pub trait ICompressor: Send + Sync + fmt::Debug {
    /// Compresses `input` and appends the compressed bytes to `output`.
    fn compress(&self, input: &[u8], output: &mut Vec<u8>) -> Result<(), IoError>;

    /// Decompresses `input` into a new buffer.
    ///
    /// `uncompressed_length` is a hint for pre-allocating the output buffer;
    /// implementations may ignore it if the format is self-describing.
    fn decompress(&self, input: &[u8], uncompressed_length: usize) -> Result<Vec<u8>, IoError>;

    /// Returns the [`CompressorType`] for this compressor.
    fn compressor_type(&self) -> CompressorType;

    /// Recommended input buffer size for optimal compression. Defaults to 64 KiB.
    fn recommended_buffer_size(&self) -> usize {
        DEFAULT_BUFFER_SIZE
    }
}

/// Factory function that creates a boxed compressor for the given type.
pub fn create_compressor(compressor_type: CompressorType) -> Box<dyn ICompressor> {
    match compressor_type {
        CompressorType::Lz4 => Box::new(Lz4Compressor::new()),
        CompressorType::Snappy => Box::new(SnappyCompressor::new()),
        CompressorType::Zstd => Box::new(ZstdCompressor::new()),
        CompressorType::Deflate => Box::new(DeflateCompressor::new()),
        CompressorType::Noop => Box::new(NoopCompressor::new()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compressor_type_display() {
        assert_eq!(CompressorType::Lz4.to_string(), "lz4");
        assert_eq!(CompressorType::Snappy.to_string(), "snappy");
        assert_eq!(CompressorType::Zstd.to_string(), "zstd");
        assert_eq!(CompressorType::Deflate.to_string(), "deflate");
        assert_eq!(CompressorType::Noop.to_string(), "noop");
    }

    #[test]
    fn test_compressor_type_from_str() {
        assert_eq!(
            "lz4".parse::<CompressorType>().unwrap(),
            CompressorType::Lz4
        );
        assert_eq!(
            "LZ4".parse::<CompressorType>().unwrap(),
            CompressorType::Lz4
        );
        assert_eq!(
            "snappy".parse::<CompressorType>().unwrap(),
            CompressorType::Snappy
        );
        assert_eq!(
            "zstd".parse::<CompressorType>().unwrap(),
            CompressorType::Zstd
        );
        assert_eq!(
            "zstandard".parse::<CompressorType>().unwrap(),
            CompressorType::Zstd
        );
        assert_eq!(
            "deflate".parse::<CompressorType>().unwrap(),
            CompressorType::Deflate
        );
        assert_eq!(
            "noop".parse::<CompressorType>().unwrap(),
            CompressorType::Noop
        );
        assert_eq!(
            "none".parse::<CompressorType>().unwrap(),
            CompressorType::Noop
        );
    }

    #[test]
    fn test_compressor_type_from_str_unknown() {
        let result = "unknown".parse::<CompressorType>();
        assert!(result.is_err());
    }

    #[test]
    fn test_create_compressor_all_types() {
        let types = [
            CompressorType::Lz4,
            CompressorType::Snappy,
            CompressorType::Zstd,
            CompressorType::Deflate,
            CompressorType::Noop,
        ];
        for ct in &types {
            let c = create_compressor(*ct);
            assert_eq!(c.compressor_type(), *ct);

            let input = b"the quick brown fox jumps over the lazy dog";
            let mut compressed = Vec::new();
            c.compress(input, &mut compressed).unwrap();
            let decompressed = c.decompress(&compressed, input.len()).unwrap();
            assert_eq!(input.as_slice(), decompressed.as_slice());
        }
    }

    #[test]
    fn test_recommended_buffer_size_default() {
        let c = create_compressor(CompressorType::Lz4);
        assert_eq!(c.recommended_buffer_size(), 64 * 1024);
    }
}
