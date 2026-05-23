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
    dictionary: Option<Vec<u8>>,
}

impl Default for ZstdCompressor {
    fn default() -> Self {
        Self {
            level: DEFAULT_COMPRESSION_LEVEL,
            dictionary: None,
        }
    }
}

impl ZstdCompressor {
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a compressor with a custom compression level.
    pub fn with_level(level: i32) -> Self {
        Self {
            level,
            dictionary: None,
        }
    }

    /// Creates a compressor/decompressor pair using a raw Zstd dictionary.
    pub fn with_dictionary(level: i32, dictionary: impl Into<Vec<u8>>) -> Self {
        let dictionary = dictionary.into();
        Self {
            level,
            dictionary: if dictionary.is_empty() {
                None
            } else {
                Some(dictionary)
            },
        }
    }
}

impl ICompressor for ZstdCompressor {
    fn compress(&self, input: &[u8], output: &mut Vec<u8>) -> Result<(), IoError> {
        let compressed = if let Some(dictionary) = self.dictionary.as_deref() {
            zstd::bulk::Compressor::with_dictionary(self.level, dictionary)
                .and_then(|mut compressor| compressor.compress(input))
        } else {
            zstd::encode_all(input, self.level)
        }
        .map_err(|e| IoError::Compression(format!("Zstd compression failed: {e}")))?;
        output.extend_from_slice(&compressed);
        Ok(())
    }

    fn decompress(&self, input: &[u8], _uncompressed_length: usize) -> Result<Vec<u8>, IoError> {
        if let Some(dictionary) = self.dictionary.as_deref() {
            zstd::bulk::Decompressor::with_dictionary(dictionary)
                .and_then(|mut decompressor| decompressor.decompress(input, _uncompressed_length))
        } else {
            zstd::decode_all(input)
        }
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

    #[test]
    fn test_dictionary_round_trip() {
        let dictionary = b"partition_key clustering_key column_name tombstone timestamp";
        let c = ZstdCompressor::with_dictionary(3, dictionary.as_slice());
        let input = b"partition_key:abc clustering_key:001 column_name:value timestamp:123";

        let mut compressed = Vec::new();
        c.compress(input, &mut compressed).unwrap();
        let decompressed = c.decompress(&compressed, input.len()).unwrap();
        assert_eq!(input.as_slice(), decompressed.as_slice());
    }

    #[test]
    fn test_dictionary_required_for_decompression() {
        let dictionary = b"sstable repeated prefix dictionary";
        let with_dict = ZstdCompressor::with_dictionary(3, dictionary.as_slice());
        let without_dict = ZstdCompressor::new();
        let input = b"sstable repeated prefix dictionary row row row";

        let mut compressed = Vec::new();
        with_dict.compress(input, &mut compressed).unwrap();

        assert!(without_dict.decompress(&compressed, input.len()).is_err());
        assert_eq!(
            with_dict.decompress(&compressed, input.len()).unwrap(),
            input.as_slice()
        );
    }
}
