// Licensed under Apache License, Version 2.0.

//! LZ4 compressor implementation.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.io.compress.LZ4Compressor`

use lz4_flex::{compress_prepend_size, decompress_size_prepended};

use crate::compress::{CompressorType, ICompressor};
use crate::error::IoError;

/// LZ4 compressor backed by `lz4_flex`.
///
/// Uses the block-level LZ4 format with a prepended size header, matching
/// Cassandra's on-disk compression for SSTables.
#[derive(Debug, Clone, Default)]
pub struct Lz4Compressor;

impl Lz4Compressor {
    pub fn new() -> Self {
        Self
    }
}

impl ICompressor for Lz4Compressor {
    fn compress(&self, input: &[u8], output: &mut Vec<u8>) -> Result<(), IoError> {
        let compressed = compress_prepend_size(input);
        output.extend_from_slice(&compressed);
        Ok(())
    }

    fn decompress(&self, input: &[u8], _uncompressed_length: usize) -> Result<Vec<u8>, IoError> {
        decompress_size_prepended(input)
            .map_err(|e| IoError::Compression(format!("LZ4 decompression failed: {e}")))
    }

    fn compressor_type(&self) -> CompressorType {
        CompressorType::Lz4
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn compressor() -> Lz4Compressor {
        Lz4Compressor::new()
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
        // Pseudo-random bytes that resist compression.
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
        assert_eq!(compressor().compressor_type(), CompressorType::Lz4);
    }
}
