// Licensed under Apache License, Version 2.0.

//! No-op compressor (passthrough).
//!
//! ## Java Oracle
//! - `org.apache.cassandra.io.compress.NoopCompressor`

use crate::compress::{CompressorType, ICompressor};
use crate::error::IoError;

/// A compressor that performs no compression at all.
///
/// Data is copied through unchanged. Useful as a baseline for benchmarks
/// and when compression is disabled.
#[derive(Debug, Clone, Default)]
pub struct NoopCompressor;

impl NoopCompressor {
    pub fn new() -> Self {
        Self
    }
}

impl ICompressor for NoopCompressor {
    fn compress(&self, input: &[u8], output: &mut Vec<u8>) -> Result<(), IoError> {
        output.extend_from_slice(input);
        Ok(())
    }

    fn decompress(&self, input: &[u8], _uncompressed_length: usize) -> Result<Vec<u8>, IoError> {
        Ok(input.to_vec())
    }

    fn compressor_type(&self) -> CompressorType {
        CompressorType::Noop
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn compressor() -> NoopCompressor {
        NoopCompressor::new()
    }

    #[test]
    fn test_round_trip_random_data() {
        let input: Vec<u8> = (0..4096).map(|i| (i * 7 + 13) as u8).collect();
        let mut compressed = Vec::new();
        compressor().compress(&input, &mut compressed).unwrap();
        assert_eq!(input, compressed, "Noop should not alter data");
        let decompressed = compressor().decompress(&compressed, input.len()).unwrap();
        assert_eq!(input, decompressed);
    }

    #[test]
    fn test_round_trip_empty() {
        let input: Vec<u8> = vec![];
        let mut compressed = Vec::new();
        compressor().compress(&input, &mut compressed).unwrap();
        assert!(compressed.is_empty());
        let decompressed = compressor().decompress(&compressed, 0).unwrap();
        assert_eq!(input, decompressed);
    }

    #[test]
    fn test_round_trip_large() {
        let input: Vec<u8> = vec![0xAB; 1_000_000];
        let mut compressed = Vec::new();
        compressor().compress(&input, &mut compressed).unwrap();
        assert_eq!(input.len(), compressed.len());
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
        assert_eq!(input, compressed);
        let decompressed = compressor().decompress(&compressed, input.len()).unwrap();
        assert_eq!(input, decompressed);
    }

    #[test]
    fn test_compressor_type() {
        assert_eq!(compressor().compressor_type(), CompressorType::Noop);
    }
}
