// Licensed under Apache License, Version 2.0.

//! Compressed sequential writer for SSTable data files.
//!
//! Buffers uncompressed data into fixed-size chunks, compresses each chunk on
//! the fly, and writes the compressed representation together with a CRC-32
//! integrity tag. On [`finish`](CompressedSequentialWriter::finish) the
//! accompanying [`CompressionMetadata`] is persisted to a separate file.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.io.compress.CompressedSequentialWriter`

use std::fs::File;
use std::io::{self, Write};
use std::path::Path;

use byteorder::{BigEndian, WriteBytesExt};
use crc32fast::Hasher as Crc32Hasher;

use crate::compress::{create_compressor, ICompressor};
use crate::compress::metadata::{CompressionMetadata, CompressionParams};

/// A sequential writer that transparently compresses data in fixed-size chunks.
pub struct CompressedSequentialWriter {
    inner: File,
    compressor: Box<dyn ICompressor>,
    uncompressed_buffer: Vec<u8>,
    chunk_size: usize,
    chunk_offsets: Vec<u64>,
    current_file_pos: u64,
    data_length: u64,
    params: CompressionParams,
}

impl CompressedSequentialWriter {
    /// Opens `path` for writing and prepares a chunk-compressed stream.
    pub fn new(path: impl AsRef<Path>, params: &CompressionParams) -> io::Result<Self> {
        let file = File::create(path)?;
        let compressor = create_compressor(params.compressor_type);
        Ok(Self {
            inner: file,
            compressor,
            uncompressed_buffer: Vec::with_capacity(params.chunk_size as usize),
            chunk_size: params.chunk_size as usize,
            chunk_offsets: Vec::new(),
            current_file_pos: 0,
            data_length: 0,
            params: params.clone(),
        })
    }

    /// Compresses and writes the current buffer as one chunk, then clears it.
    fn flush_chunk(&mut self) -> io::Result<()> {
        if self.uncompressed_buffer.is_empty() {
            return Ok(());
        }

        // Record the start offset of this chunk.
        self.chunk_offsets.push(self.current_file_pos);

        // Compress
        let mut compressed = Vec::new();
        self.compressor
            .compress(&self.uncompressed_buffer, &mut compressed)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

        // CRC-32 of the compressed bytes
        let mut crc = Crc32Hasher::new();
        crc.update(&compressed);
        let checksum = crc.finalize();

        // Write: compressed_len(u32 BE) | compressed_data | crc32(u32 BE)
        self.inner
            .write_u32::<BigEndian>(compressed.len() as u32)?;
        self.inner.write_all(&compressed)?;
        self.inner.write_u32::<BigEndian>(checksum)?;

        self.current_file_pos += 4 + compressed.len() as u64 + 4;
        self.uncompressed_buffer.clear();
        Ok(())
    }

    /// Flushes any remaining data, fsyncs the data file, and writes the
    /// [`CompressionMetadata`] to `metadata_path`.
    pub fn finish(mut self, metadata_path: impl AsRef<Path>) -> io::Result<()> {
        // Flush the final partial (or full) chunk.
        self.flush_chunk()?;
        self.inner.sync_all()?;

        let meta = CompressionMetadata {
            compressor_type: self.params.compressor_type,
            options: self.params.options.clone(),
            chunk_size: self.params.chunk_size,
            data_length: self.data_length,
            chunk_count: self.chunk_offsets.len() as u32,
            chunk_offsets: self.chunk_offsets,
        };

        let mut meta_file = File::create(metadata_path)?;
        meta.write_to(&mut meta_file)?;
        meta_file.sync_all()?;
        Ok(())
    }
}

impl Write for CompressedSequentialWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let mut remaining = buf;
        while !remaining.is_empty() {
            let space = self.chunk_size - self.uncompressed_buffer.len();
            let take = remaining.len().min(space);
            self.uncompressed_buffer
                .extend_from_slice(&remaining[..take]);
            self.data_length += take as u64;
            remaining = &remaining[take..];

            if self.uncompressed_buffer.len() == self.chunk_size {
                self.flush_chunk()?;
            }
        }
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        // Flushing mid-stream is a no-op; chunks are only emitted at
        // boundary or on `finish`.
        self.inner.flush()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;

    use byteorder::ReadBytesExt;

    #[test]
    fn test_write_and_verify_structure() {
        let dir = tempfile::tempdir().unwrap();
        let data_path = dir.path().join("data.db");
        let meta_path = dir.path().join("meta.db");

        let params = CompressionParams::default();
        let mut writer = CompressedSequentialWriter::new(&data_path, &params).unwrap();
        let payload = vec![0xABu8; 100];
        writer.write_all(&payload).unwrap();
        writer.finish(&meta_path).unwrap();

        // Verify data file structure: compressed_len(4) + data + crc(4)
        let data = std::fs::read(&data_path).unwrap();
        let mut cursor = io::Cursor::new(&data);
        let comp_len = cursor.read_u32::<BigEndian>().unwrap() as usize;
        let mut comp_data = vec![0u8; comp_len];
        cursor.read_exact(&mut comp_data).unwrap();
        let stored_crc = cursor.read_u32::<BigEndian>().unwrap();

        let mut crc = Crc32Hasher::new();
        crc.update(&comp_data);
        assert_eq!(crc.finalize(), stored_crc);

        // Metadata round-trip
        let meta_bytes = std::fs::read(&meta_path).unwrap();
        let meta =
            CompressionMetadata::read_from(&mut meta_bytes.as_slice()).unwrap();
        assert_eq!(meta.data_length, 100);
        assert_eq!(meta.chunk_count, 1);
    }

    #[test]
    fn test_multi_chunk_write() {
        let dir = tempfile::tempdir().unwrap();
        let data_path = dir.path().join("data.db");
        let meta_path = dir.path().join("meta.db");

        let chunk_size = 64u32;
        let params = CompressionParams {
            chunk_size,
            ..Default::default()
        };
        let mut writer = CompressedSequentialWriter::new(&data_path, &params).unwrap();
        // Write 2.5 chunks worth of data.
        let payload = vec![0x42u8; 160];
        writer.write_all(&payload).unwrap();
        writer.finish(&meta_path).unwrap();

        let meta_bytes = std::fs::read(&meta_path).unwrap();
        let meta =
            CompressionMetadata::read_from(&mut meta_bytes.as_slice()).unwrap();
        assert_eq!(meta.chunk_count, 3); // 64 + 64 + 32
        assert_eq!(meta.data_length, 160);
    }

    #[test]
    fn test_final_partial_chunk() {
        let dir = tempfile::tempdir().unwrap();
        let data_path = dir.path().join("data.db");
        let meta_path = dir.path().join("meta.db");

        let params = CompressionParams {
            chunk_size: 1024,
            ..Default::default()
        };
        let mut writer = CompressedSequentialWriter::new(&data_path, &params).unwrap();
        writer.write_all(&[1, 2, 3]).unwrap();
        writer.finish(&meta_path).unwrap();

        let meta_bytes = std::fs::read(&meta_path).unwrap();
        let meta =
            CompressionMetadata::read_from(&mut meta_bytes.as_slice()).unwrap();
        assert_eq!(meta.chunk_count, 1);
        assert_eq!(meta.data_length, 3);
    }
}
