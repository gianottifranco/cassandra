// Licensed under Apache License, Version 2.0.

//! Compressed chunk reader that implements [`Rebufferer`].
//!
//! Given a compressed data file and its [`CompressionMetadata`], the reader
//! seeks to the appropriate chunk, reads the compressed payload, verifies its
//! CRC-32 checksum, decompresses it, and returns the result as a
//! [`BufferHolder`].
//!
//! ## Java Oracle
//! - `org.apache.cassandra.io.compress.CompressedChunkReader`

use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
use std::path::Path;
use std::sync::{Arc, Mutex};

use byteorder::{BigEndian, ReadBytesExt};
use crc32fast::Hasher as Crc32Hasher;

use crate::compress::metadata::CompressionMetadata;
use crate::compress::{create_compressor, ICompressor};
use crate::error::IoError;
use crate::util::rebufferer::{BufferHolder, Rebufferer};

/// A [`Rebufferer`] that reads from a chunk-compressed data file.
pub struct CompressedChunkReader {
    file: Arc<Mutex<File>>,
    metadata: CompressionMetadata,
    compressor: Box<dyn ICompressor>,
}

impl CompressedChunkReader {
    /// Opens the compressed data file and pairs it with pre-loaded metadata.
    pub fn new(
        data_path: impl AsRef<Path>,
        metadata: CompressionMetadata,
    ) -> io::Result<Self> {
        let file = File::open(data_path)?;
        let compressor = create_compressor(metadata.compressor_type);
        Ok(Self {
            file: Arc::new(Mutex::new(file)),
            metadata,
            compressor,
        })
    }

    /// Reads, verifies, and decompresses the chunk at the given index.
    fn read_chunk(&self, chunk_index: usize) -> io::Result<Vec<u8>> {
        let offset = self.metadata.chunk_offsets[chunk_index];

        let mut file = self.file.lock().map_err(|e| {
            io::Error::new(io::ErrorKind::Other, format!("lock poisoned: {e}"))
        })?;
        file.seek(SeekFrom::Start(offset))?;

        // Read compressed_len(u32 BE) + compressed_data + crc32(u32 BE)
        let comp_len = file.read_u32::<BigEndian>()? as usize;
        let mut comp_data = vec![0u8; comp_len];
        file.read_exact(&mut comp_data)?;
        let stored_crc = file.read_u32::<BigEndian>()?;

        // Verify CRC-32
        let mut crc = Crc32Hasher::new();
        crc.update(&comp_data);
        let actual_crc = crc.finalize();
        if actual_crc != stored_crc {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                IoError::ChecksumMismatch {
                    expected: stored_crc,
                    actual: actual_crc,
                },
            ));
        }

        // Decompress
        let uncompressed_hint = self.metadata.chunk_size as usize;
        self.compressor
            .decompress(&comp_data, uncompressed_hint)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))
    }
}

impl Rebufferer for CompressedChunkReader {
    fn rebuffer(&self, position: u64) -> Result<BufferHolder, crate::error::IoError> {
        let chunk_index = (position / self.metadata.chunk_size as u64) as usize;
        if chunk_index >= self.metadata.chunk_count as usize {
            return Err(crate::error::IoError::Io(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!(
                    "position {position} is beyond data length {}",
                    self.metadata.data_length
                ),
            )));
        }

        let decompressed = self.read_chunk(chunk_index)?;
        let chunk_offset = chunk_index as u64 * self.metadata.chunk_size as u64;
        Ok(BufferHolder::owned(decompressed, chunk_offset))
    }

    fn file_length(&self) -> u64 {
        self.metadata.data_length
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compress::compressed_writer::CompressedSequentialWriter;
    use crate::compress::metadata::CompressionParams;
    use std::io::Write;

    /// Write data with the writer, then read it back with the reader.
    fn write_and_read(payload: &[u8], chunk_size: u32) -> (CompressionMetadata, Vec<Vec<u8>>) {
        let dir = tempfile::tempdir().unwrap();
        let data_path = dir.path().join("data.db");
        let meta_path = dir.path().join("meta.db");

        let params = CompressionParams {
            chunk_size,
            ..Default::default()
        };
        let mut writer = CompressedSequentialWriter::new(&data_path, &params).unwrap();
        writer.write_all(payload).unwrap();
        writer.finish(&meta_path).unwrap();

        let meta_bytes = std::fs::read(&meta_path).unwrap();
        let meta = CompressionMetadata::read_from(&mut meta_bytes.as_slice()).unwrap();

        let reader = CompressedChunkReader::new(&data_path, meta.clone()).unwrap();

        let mut chunks = Vec::new();
        for i in 0..meta.chunk_count {
            let holder = reader.rebuffer(i as u64 * chunk_size as u64).unwrap();
            chunks.push(holder.buffer().to_vec());
        }
        (meta, chunks)
    }

    #[test]
    fn test_single_chunk_round_trip() {
        let payload = b"hello compressed world";
        let (meta, chunks) = write_and_read(payload, 65536);
        assert_eq!(meta.chunk_count, 1);
        assert_eq!(chunks[0], payload);
    }

    #[test]
    fn test_multi_chunk_round_trip() {
        let chunk_size = 64u32;
        let payload: Vec<u8> = (0u8..200).collect();
        let (meta, chunks) = write_and_read(&payload, chunk_size);

        assert_eq!(meta.chunk_count, 4); // 64+64+64+8
        // Reassemble
        let mut reassembled = Vec::new();
        for c in &chunks {
            reassembled.extend_from_slice(c);
        }
        assert_eq!(reassembled, payload);
    }

    #[test]
    fn test_file_length() {
        let dir = tempfile::tempdir().unwrap();
        let data_path = dir.path().join("data.db");
        let meta_path = dir.path().join("meta.db");

        let params = CompressionParams::default();
        let mut writer = CompressedSequentialWriter::new(&data_path, &params).unwrap();
        writer.write_all(&[0u8; 999]).unwrap();
        writer.finish(&meta_path).unwrap();

        let meta_bytes = std::fs::read(&meta_path).unwrap();
        let meta = CompressionMetadata::read_from(&mut meta_bytes.as_slice()).unwrap();
        let reader = CompressedChunkReader::new(&data_path, meta).unwrap();
        assert_eq!(reader.file_length(), 999);
    }

    #[test]
    fn test_crc_corruption_detected() {
        let dir = tempfile::tempdir().unwrap();
        let data_path = dir.path().join("data.db");
        let meta_path = dir.path().join("meta.db");

        let params = CompressionParams {
            chunk_size: 1024,
            ..Default::default()
        };
        let mut writer = CompressedSequentialWriter::new(&data_path, &params).unwrap();
        writer.write_all(&[0xFFu8; 100]).unwrap();
        writer.finish(&meta_path).unwrap();

        // Corrupt a byte in the compressed data (byte 5, inside the
        // compressed payload after the 4-byte length header).
        let mut raw = std::fs::read(&data_path).unwrap();
        assert!(raw.len() > 5);
        raw[5] ^= 0xFF;
        std::fs::write(&data_path, &raw).unwrap();

        let meta_bytes = std::fs::read(&meta_path).unwrap();
        let meta = CompressionMetadata::read_from(&mut meta_bytes.as_slice()).unwrap();
        let reader = CompressedChunkReader::new(&data_path, meta).unwrap();

        let err = reader.rebuffer(0).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("mismatch") || msg.contains("Checksum"),
            "unexpected error message: {msg}"
        );
    }
}
