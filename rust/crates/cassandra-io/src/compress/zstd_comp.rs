// Licensed under Apache License, Version 2.0.

//! Zstandard compressor implementation.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.io.compress.ZstdCompressor`

use crate::compress::{CompressorType, ICompressor};
use crate::error::IoError;

/// Default Zstd compression level (matches Cassandra's default).
const DEFAULT_COMPRESSION_LEVEL: i32 = 3;

/// Zstandard compressor backed by the `zstd` crate.
#[derive(Debug, Clone)]
pub struct ZstdCompressor {
    level: i32,
}

impl Default for ZstdCompressor {
    fn default() -> Self {
        Self {
            level: DEFAULT_COMPRESSION_LEVEL,
        }
    }
}

impl ZstdCompressor {
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a compressor with a custom compression level.
    pub fn with_level(level: i32) -> Self {
        Self { level }
    }
}

impl ICompressor for ZstdCompressor {
    fn compress(&self, input: &[u8], output: &mut Vec<u8>) -> Result<(), IoError> {
        let compressed = zstd::encode_all(input, self.level)
            .map_err(|e| IoError::Compression(format!("Zstd compression failed: {e}")))?;
        output.extend_from_slice(&compressed);
        Ok(())
    }

    fn decompress(&self, input: &[u8], _uncompressed_length: usize) -> Result<Vec<u8>, IoError> {
        zstd::decode_all(input)
            .map_err(|e| IoError::Compression(format!("Zstd decompression failed: {e}")))
    }

    fn compressor_type(&self) -> CompressorType {
        CompressorType::Zstd
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn compressor() -> ZstdCompressor {
        ZstdCompressor::new()
    }

    #[test]
    fn test_round_trip_random_data() {
        let input: Vec<u8> = (0..4096).map(|i| (i * 7 + 13) as u8).collect();
        let mut compressed = Vec::new();
        compressor().compress(&input, &mut compressed).unwrap();
        let decompressed = compressor().decompress(&compressed, input.len()).unwrap();
        assert_eq!(input, decompressed);
    }

    #[test]
    fn test_round_trip_empty() {
        let input: Vec<u8> = vec![];
        let mut compressed = Vec::new();
        compressor().compress(&input, &mut compressed).unwrap();
        let decompressed = compressor().decompress(&compressed, 0).unwrap();
        assert_eq!(input, decompressed);
    }

    #[test]
    fn test_round_trip_large() {
        let input: Vec<u8> = vec![0xAB; 1_000_000];
        let mut compressed = Vec::new();
        compressor().compress(&input, &mut compressed).unwrap();
        let decompressed = compressor().decompress(&compressed, input.len()).unwrap();
        assert_eq!(input, decompressed);
    }

    #[test]
    fn test_round_trip_incompressible() {
        let input: Vec<u8> = (0u32..8192)
            .map(|i| (i.wrapping_mul(2654435761) >> 24) as u8)
            .collect();
        let mut compressed = Vec::new();
        compressor().compress(&input, &mut compressed).unwrap();
        let decompressed = compressor().decompress(&compressed, input.len()).unwrap();
        assert_eq!(input, decompressed);
    }

    #[test]
    fn test_compressor_type() {
        assert_eq!(compressor().compressor_type(), CompressorType::Zstd);
    }

    #[test]
    fn test_custom_level() {
        let c = ZstdCompressor::with_level(1);
        let input = b"hello world hello world hello world";
        let mut compressed = Vec::new();
        c.compress(input, &mut compressed).unwrap();
        let decompressed = c.decompress(&compressed, input.len()).unwrap();
        assert_eq!(input.as_slice(), decompressed.as_slice());
    }
}
