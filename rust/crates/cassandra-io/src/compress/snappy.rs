// Licensed under Apache License, Version 2.0.

//! Snappy compressor implementation.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.io.compress.SnappyCompressor`

use snap::raw::{Decoder, Encoder};

use crate::compress::{CompressorType, ICompressor};
use crate::error::IoError;

/// Snappy compressor backed by the `snap` crate (raw format).
#[derive(Debug, Clone, Default)]
pub struct SnappyCompressor;

impl SnappyCompressor {
    pub fn new() -> Self {
        Self
    }
}

impl ICompressor for SnappyCompressor {
    fn compress(&self, input: &[u8], output: &mut Vec<u8>) -> Result<(), IoError> {
        let mut encoder = Encoder::new();
        let compressed = encoder.compress_vec(input).map_err(|e| {
            IoError::Compression(format!("Snappy compression failed: {e}"))
        })?;
        output.extend_from_slice(&compressed);
        Ok(())
    }

    fn decompress(&self, input: &[u8], _uncompressed_length: usize) -> Result<Vec<u8>, IoError> {
        let mut decoder = Decoder::new();
        decoder.decompress_vec(input).map_err(|e| {
            IoError::Compression(format!("Snappy decompression failed: {e}"))
        })
    }

    fn compressor_type(&self) -> CompressorType {
        CompressorType::Snappy
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn compressor() -> SnappyCompressor {
        SnappyCompressor::new()
    }

    #[test]
    fn test_round_trip_random_data() {
        let input: Vec<u8> = (0..4096).map(|i| (i * 7 + 13) as u8).collect();
        let mut compressed = Vec::new();
        compressor().compress(&input, &mut compressed).unwrap();
        let decompressed = compressor()
            .decompress(&compressed, input.len())
            .unwrap();
        assert_eq!(input, decompressed);
    }

    #[test]
    fn test_round_trip_empty() {
        let input: Vec<u8> = vec![];
        let mut compressed = Vec::new();
        compressor().compress(&input, &mut compressed).unwrap();
        let decompressed = compressor()
            .decompress(&compressed, 0)
            .unwrap();
        assert_eq!(input, decompressed);
    }

    #[test]
    fn test_round_trip_large() {
        let input: Vec<u8> = vec![0xAB; 1_000_000];
        let mut compressed = Vec::new();
        compressor().compress(&input, &mut compressed).unwrap();
        let decompressed = compressor()
            .decompress(&compressed, input.len())
            .unwrap();
        assert_eq!(input, decompressed);
    }

    #[test]
    fn test_round_trip_incompressible() {
        let input: Vec<u8> = (0u32..8192)
            .map(|i| (i.wrapping_mul(2654435761) >> 24) as u8)
            .collect();
        let mut compressed = Vec::new();
        compressor().compress(&input, &mut compressed).unwrap();
        let decompressed = compressor()
            .decompress(&compressed, input.len())
            .unwrap();
        assert_eq!(input, decompressed);
    }

    #[test]
    fn test_compressor_type() {
        assert_eq!(compressor().compressor_type(), CompressorType::Snappy);
    }
}
