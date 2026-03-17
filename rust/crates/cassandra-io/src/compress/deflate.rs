// Licensed under Apache License, Version 2.0.

//! Deflate compressor implementation.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.io.compress.DeflateCompressor`

use std::io::{Read, Write};

use flate2::read::DeflateDecoder;
use flate2::write::DeflateEncoder;
use flate2::Compression;

use crate::compress::{CompressorType, ICompressor};
use crate::error::IoError;

/// Deflate compressor backed by the `flate2` crate.
#[derive(Debug, Clone)]
pub struct DeflateCompressor {
    level: u32,
}

impl Default for DeflateCompressor {
    fn default() -> Self {
        Self { level: 6 }
    }
}

impl DeflateCompressor {
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a compressor with a custom compression level (0-9).
    pub fn with_level(level: u32) -> Self {
        Self { level }
    }
}

impl ICompressor for DeflateCompressor {
    fn compress(&self, input: &[u8], output: &mut Vec<u8>) -> Result<(), IoError> {
        let mut encoder = DeflateEncoder::new(Vec::new(), Compression::new(self.level));
        encoder.write_all(input).map_err(|e| {
            IoError::Compression(format!("Deflate compression failed: {e}"))
        })?;
        let compressed = encoder.finish().map_err(|e| {
            IoError::Compression(format!("Deflate compression finish failed: {e}"))
        })?;
        output.extend_from_slice(&compressed);
        Ok(())
    }

    fn decompress(&self, input: &[u8], _uncompressed_length: usize) -> Result<Vec<u8>, IoError> {
        let mut decoder = DeflateDecoder::new(input);
        let mut decompressed = Vec::new();
        decoder.read_to_end(&mut decompressed).map_err(|e| {
            IoError::Compression(format!("Deflate decompression failed: {e}"))
        })?;
        Ok(decompressed)
    }

    fn compressor_type(&self) -> CompressorType {
        CompressorType::Deflate
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn compressor() -> DeflateCompressor {
        DeflateCompressor::new()
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
        assert_eq!(compressor().compressor_type(), CompressorType::Deflate);
    }

    #[test]
    fn test_custom_level() {
        let c = DeflateCompressor::with_level(1);
        let input = b"hello world hello world hello world";
        let mut compressed = Vec::new();
        c.compress(input, &mut compressed).unwrap();
        let decompressed = c.decompress(&compressed, input.len()).unwrap();
        assert_eq!(input.as_slice(), decompressed.as_slice());
    }
}
