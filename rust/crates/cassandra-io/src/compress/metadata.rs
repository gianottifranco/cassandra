// Licensed under Apache License, Version 2.0.

//! Compression metadata and parameters for SSTable chunk-based compression.
//!
//! [`CompressionMetadata`] stores the chunk offset index and compressor
//! configuration that accompanies a compressed data file (the Java
//! `CompressionInfo.db` component). [`CompressionParams`] is the user-facing
//! configuration knob used when creating new compressed files.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.io.compress.CompressionMetadata`
//! - `org.apache.cassandra.io.compress.CompressionParams`

use std::collections::HashMap;
use std::io::{self, Read, Write};

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

use crate::compress::CompressorType;

// ---------------------------------------------------------------------------
// CompressionParams
// ---------------------------------------------------------------------------

/// User-facing compression configuration.
#[derive(Debug, Clone)]
pub struct CompressionParams {
    pub compressor_type: CompressorType,
    pub chunk_size: u32,
    pub options: HashMap<String, String>,
}

impl Default for CompressionParams {
    fn default() -> Self {
        Self {
            compressor_type: CompressorType::Lz4,
            chunk_size: 65536,
            options: HashMap::new(),
        }
    }
}

// ---------------------------------------------------------------------------
// CompressionMetadata
// ---------------------------------------------------------------------------

/// On-disk metadata for a compressed data file.
///
/// Holds the compressor identity, per-compressor options, chunk size, total
/// uncompressed data length, and the byte offset of every compressed chunk
/// inside the data file.
#[derive(Debug, Clone)]
pub struct CompressionMetadata {
    pub compressor_type: CompressorType,
    pub options: HashMap<String, String>,
    pub chunk_size: u32,
    pub data_length: u64,
    pub chunk_count: u32,
    pub chunk_offsets: Vec<u64>,
}

impl CompressionMetadata {
    /// Serialises to the binary format matching Java's `CompressionInfo.db`.
    ///
    /// Layout (all integers Big-Endian):
    /// ```text
    /// name_len(u16) | name_bytes
    /// options_count(u32) | [key_len(u16) key val_len(u16) val]*
    /// chunk_size(u32) | data_length(u64) | chunk_count(u32)
    /// offsets: chunk_count * u64
    /// ```
    pub fn write_to(&self, w: &mut impl Write) -> io::Result<()> {
        // Compressor name
        let name = self.compressor_type.to_string();
        w.write_u16::<BigEndian>(name.len() as u16)?;
        w.write_all(name.as_bytes())?;

        // Options
        w.write_u32::<BigEndian>(self.options.len() as u32)?;
        for (k, v) in &self.options {
            w.write_u16::<BigEndian>(k.len() as u16)?;
            w.write_all(k.as_bytes())?;
            w.write_u16::<BigEndian>(v.len() as u16)?;
            w.write_all(v.as_bytes())?;
        }

        // Chunk geometry
        w.write_u32::<BigEndian>(self.chunk_size)?;
        w.write_u64::<BigEndian>(self.data_length)?;
        w.write_u32::<BigEndian>(self.chunk_count)?;
        for &off in &self.chunk_offsets {
            w.write_u64::<BigEndian>(off)?;
        }
        Ok(())
    }

    /// Deserialises from the binary format produced by [`write_to`](Self::write_to).
    pub fn read_from(r: &mut impl Read) -> io::Result<Self> {
        // Compressor name
        let name_len = r.read_u16::<BigEndian>()? as usize;
        let mut name_buf = vec![0u8; name_len];
        r.read_exact(&mut name_buf)?;
        let name = String::from_utf8(name_buf)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let compressor_type: CompressorType =
            name.parse().map_err(|e: crate::error::IoError| {
                io::Error::new(io::ErrorKind::InvalidData, e.to_string())
            })?;

        // Options
        let opt_count = r.read_u32::<BigEndian>()? as usize;
        let mut options = HashMap::with_capacity(opt_count);
        for _ in 0..opt_count {
            let kl = r.read_u16::<BigEndian>()? as usize;
            let mut kb = vec![0u8; kl];
            r.read_exact(&mut kb)?;
            let key =
                String::from_utf8(kb).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

            let vl = r.read_u16::<BigEndian>()? as usize;
            let mut vb = vec![0u8; vl];
            r.read_exact(&mut vb)?;
            let val =
                String::from_utf8(vb).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

            options.insert(key, val);
        }

        // Chunk geometry
        let chunk_size = r.read_u32::<BigEndian>()?;
        let data_length = r.read_u64::<BigEndian>()?;
        let chunk_count = r.read_u32::<BigEndian>()?;
        let mut chunk_offsets = Vec::with_capacity(chunk_count as usize);
        for _ in 0..chunk_count {
            chunk_offsets.push(r.read_u64::<BigEndian>()?);
        }

        Ok(Self {
            compressor_type,
            options,
            chunk_size,
            data_length,
            chunk_count,
            chunk_offsets,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_metadata() -> CompressionMetadata {
        let mut opts = HashMap::new();
        opts.insert("level".into(), "3".into());
        CompressionMetadata {
            compressor_type: CompressorType::Lz4,
            options: opts,
            chunk_size: 65536,
            data_length: 1_000_000,
            chunk_count: 3,
            chunk_offsets: vec![0, 40000, 80000],
        }
    }

    #[test]
    fn test_round_trip() {
        let meta = sample_metadata();
        let mut buf = Vec::new();
        meta.write_to(&mut buf).unwrap();
        let restored = CompressionMetadata::read_from(&mut buf.as_slice()).unwrap();

        assert_eq!(restored.compressor_type, meta.compressor_type);
        assert_eq!(restored.chunk_size, meta.chunk_size);
        assert_eq!(restored.data_length, meta.data_length);
        assert_eq!(restored.chunk_count, meta.chunk_count);
        assert_eq!(restored.chunk_offsets, meta.chunk_offsets);
        assert_eq!(restored.options.get("level").unwrap(), "3");
    }

    #[test]
    fn test_known_bytes() {
        let meta = CompressionMetadata {
            compressor_type: CompressorType::Lz4,
            options: HashMap::new(),
            chunk_size: 256,
            data_length: 512,
            chunk_count: 2,
            chunk_offsets: vec![0, 100],
        };
        let mut buf = Vec::new();
        meta.write_to(&mut buf).unwrap();

        // name_len(2) + "lz4"(3) + opt_count(4) + chunk_size(4) +
        // data_length(8) + chunk_count(4) + 2*offset(16) = 41
        assert_eq!(buf.len(), 41);

        // First two bytes: name length = 3
        assert_eq!(buf[0], 0);
        assert_eq!(buf[1], 3);
        // Name
        assert_eq!(&buf[2..5], b"lz4");
        // Options count = 0
        assert_eq!(&buf[5..9], &[0, 0, 0, 0]);
    }

    #[test]
    fn test_empty_metadata() {
        let meta = CompressionMetadata {
            compressor_type: CompressorType::Noop,
            options: HashMap::new(),
            chunk_size: 65536,
            data_length: 0,
            chunk_count: 0,
            chunk_offsets: vec![],
        };
        let mut buf = Vec::new();
        meta.write_to(&mut buf).unwrap();
        let restored = CompressionMetadata::read_from(&mut buf.as_slice()).unwrap();
        assert_eq!(restored.chunk_count, 0);
        assert!(restored.chunk_offsets.is_empty());
        assert_eq!(restored.data_length, 0);
    }

    #[test]
    fn test_many_chunks() {
        let offsets: Vec<u64> = (0..1000).map(|i| i * 500).collect();
        let meta = CompressionMetadata {
            compressor_type: CompressorType::Snappy,
            options: HashMap::new(),
            chunk_size: 4096,
            data_length: 500_000,
            chunk_count: 1000,
            chunk_offsets: offsets.clone(),
        };
        let mut buf = Vec::new();
        meta.write_to(&mut buf).unwrap();
        let restored = CompressionMetadata::read_from(&mut buf.as_slice()).unwrap();
        assert_eq!(restored.chunk_count, 1000);
        assert_eq!(restored.chunk_offsets, offsets);
    }

    #[test]
    fn reads_java_compressor_class_names() {
        let mut buf = Vec::new();
        let name = "org.apache.cassandra.io.compress.ZstdCompressor";
        buf.extend_from_slice(&(name.len() as u16).to_be_bytes());
        buf.extend_from_slice(name.as_bytes());
        buf.extend_from_slice(&0u32.to_be_bytes());
        buf.extend_from_slice(&4096u32.to_be_bytes());
        buf.extend_from_slice(&8192u64.to_be_bytes());
        buf.extend_from_slice(&2u32.to_be_bytes());
        buf.extend_from_slice(&0u64.to_be_bytes());
        buf.extend_from_slice(&123u64.to_be_bytes());

        let restored = CompressionMetadata::read_from(&mut buf.as_slice()).unwrap();
        assert_eq!(restored.compressor_type, CompressorType::Zstd);
        assert_eq!(restored.chunk_size, 4096);
        assert_eq!(restored.data_length, 8192);
        assert_eq!(restored.chunk_offsets, vec![0, 123]);
    }

    #[test]
    fn test_default_params() {
        let params = CompressionParams::default();
        assert_eq!(params.compressor_type, CompressorType::Lz4);
        assert_eq!(params.chunk_size, 65536);
        assert!(params.options.is_empty());
    }
}
