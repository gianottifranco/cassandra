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

use std::collections::HashMap;
use std::fmt;
use std::str::FromStr;

use crate::error::IoError;

/// Default recommended buffer size (64 KiB).
const DEFAULT_BUFFER_SIZE: usize = 64 * 1024;
pub const ZSTD_DICTIONARY_OPTION: &str = "dictionary";
pub const ZSTD_DICTIONARY_HEX_OPTION: &str = "dictionary_hex";
pub const ZSTD_LEVEL_OPTION: &str = "level";

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
        let simple_name = s.rsplit('.').next().unwrap_or(s).to_ascii_lowercase();
        match simple_name.as_str() {
            "lz4" | "lz4compressor" => Ok(CompressorType::Lz4),
            "snappy" | "snappycompressor" => Ok(CompressorType::Snappy),
            "zstd" | "zstandard" | "zstdcompressor" | "zstandardcompressor" => {
                Ok(CompressorType::Zstd)
            }
            "deflate" | "deflatecompressor" => Ok(CompressorType::Deflate),
            "noop" | "none" | "noopcompressor" => Ok(CompressorType::Noop),
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

/// Factory function that creates a compressor from metadata/config options.
pub fn create_compressor_with_options(
    compressor_type: CompressorType,
    options: &HashMap<String, String>,
) -> Result<Box<dyn ICompressor>, IoError> {
    match compressor_type {
        CompressorType::Zstd => {
            let level = options
                .get(ZSTD_LEVEL_OPTION)
                .and_then(|value| value.parse::<i32>().ok())
                .unwrap_or(3);
            if let Some(dictionary) = zstd_dictionary_from_options(options)? {
                Ok(Box::new(ZstdCompressor::with_dictionary(level, dictionary)))
            } else {
                Ok(Box::new(ZstdCompressor::with_level(level)))
            }
        }
        other => Ok(create_compressor(other)),
    }
}

fn zstd_dictionary_from_options(
    options: &HashMap<String, String>,
) -> Result<Option<Vec<u8>>, IoError> {
    if let Some(hex) = options.get(ZSTD_DICTIONARY_HEX_OPTION) {
        return decode_hex(hex).map(Some);
    }
    Ok(options
        .get(ZSTD_DICTIONARY_OPTION)
        .map(|dictionary| dictionary.as_bytes().to_vec())
        .filter(|dictionary| !dictionary.is_empty()))
}

fn decode_hex(input: &str) -> Result<Vec<u8>, IoError> {
    let bytes = input.trim().as_bytes();
    if bytes.len() % 2 != 0 {
        return Err(IoError::InvalidFormat(
            "Zstd dictionary_hex has odd length".into(),
        ));
    }

    let mut out = Vec::with_capacity(bytes.len() / 2);
    for pair in bytes.chunks_exact(2) {
        let high = hex_value(pair[0])?;
        let low = hex_value(pair[1])?;
        out.push((high << 4) | low);
    }
    Ok(out)
}

fn hex_value(byte: u8) -> Result<u8, IoError> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err(IoError::InvalidFormat(format!(
            "invalid Zstd dictionary_hex byte: {byte:#x}"
        ))),
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
        assert_eq!(
            "LZ4Compressor".parse::<CompressorType>().unwrap(),
            CompressorType::Lz4
        );
        assert_eq!(
            "org.apache.cassandra.io.compress.SnappyCompressor"
                .parse::<CompressorType>()
                .unwrap(),
            CompressorType::Snappy
        );
        assert_eq!(
            "org.apache.cassandra.io.compress.ZstdCompressor"
                .parse::<CompressorType>()
                .unwrap(),
            CompressorType::Zstd
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
    fn test_create_zstd_with_dictionary_options() {
        let mut options = HashMap::new();
        options.insert(
            ZSTD_DICTIONARY_OPTION.to_string(),
            "sstable-prefix".to_string(),
        );
        options.insert(ZSTD_LEVEL_OPTION.to_string(), "1".to_string());

        let c = create_compressor_with_options(CompressorType::Zstd, &options).unwrap();
        let input = b"sstable-prefix-row sstable-prefix-row";
        let mut compressed = Vec::new();
        c.compress(input, &mut compressed).unwrap();
        assert_eq!(c.decompress(&compressed, input.len()).unwrap(), input);
    }

    #[test]
    fn test_create_zstd_with_hex_dictionary_options() {
        let mut options = HashMap::new();
        options.insert(
            ZSTD_DICTIONARY_HEX_OPTION.to_string(),
            "73737461626c65".to_string(),
        );

        let c = create_compressor_with_options(CompressorType::Zstd, &options).unwrap();
        let input = b"sstable row sstable row";
        let mut compressed = Vec::new();
        c.compress(input, &mut compressed).unwrap();
        assert_eq!(c.decompress(&compressed, input.len()).unwrap(), input);
    }

    #[test]
    fn test_recommended_buffer_size_default() {
        let c = create_compressor(CompressorType::Lz4);
        assert_eq!(c.recommended_buffer_size(), 64 * 1024);
    }
}
