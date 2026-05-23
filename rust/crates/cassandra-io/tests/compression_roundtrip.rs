// Licensed under Apache License, Version 2.0.

//! Round-trip integration tests: write compressed → read decompressed for each
//! compressor, verify byte-exact match.

use std::io::Write;
use tempfile::NamedTempFile;

use cassandra_io::compress::compressed_reader::CompressedChunkReader;
use cassandra_io::compress::compressed_writer::CompressedSequentialWriter;
use cassandra_io::compress::metadata::{CompressionMetadata, CompressionParams};
use cassandra_io::compress::{CompressorType, ZSTD_DICTIONARY_OPTION, ZSTD_LEVEL_OPTION};
use cassandra_io::util::rebufferer::Rebufferer;

fn roundtrip_with_compressor(ctype: CompressorType, data: &[u8]) {
    let params = CompressionParams {
        compressor_type: ctype,
        chunk_size: 16384, // 16 KiB chunks for more coverage
        options: Default::default(),
    };
    roundtrip_with_params(params, data);
}

fn roundtrip_with_params(params: CompressionParams, data: &[u8]) {
    let data_file = NamedTempFile::new().unwrap();
    let meta_file = NamedTempFile::new().unwrap();
    let compressor_type = params.compressor_type;

    // Write
    let mut writer = CompressedSequentialWriter::new(data_file.path(), &params).unwrap();
    writer.write_all(data).unwrap();
    writer.finish(meta_file.path()).unwrap();

    // Read metadata
    let mut meta_reader = std::fs::File::open(meta_file.path()).unwrap();
    let metadata = CompressionMetadata::read_from(&mut meta_reader).unwrap();
    assert_eq!(metadata.compressor_type, compressor_type);
    assert_eq!(metadata.data_length, data.len() as u64);

    // Read back all chunks and verify
    let reader = CompressedChunkReader::new(data_file.path(), metadata).unwrap();
    let mut reconstructed = Vec::new();
    let mut pos = 0u64;
    while pos < data.len() as u64 {
        let holder = reader.rebuffer(pos).unwrap();
        let buf = holder.buffer();
        reconstructed.extend_from_slice(buf);
        pos += buf.len() as u64;
    }

    assert_eq!(
        reconstructed.len(),
        data.len(),
        "Length mismatch for {compressor_type}"
    );
    assert_eq!(reconstructed, data, "Data mismatch for {compressor_type}");
}

#[test]
fn test_roundtrip_lz4() {
    let data: Vec<u8> = (0..100_000).map(|i| (i % 251) as u8).collect();
    roundtrip_with_compressor(CompressorType::Lz4, &data);
}

#[test]
fn test_roundtrip_snappy() {
    let data: Vec<u8> = (0..100_000).map(|i| (i % 251) as u8).collect();
    roundtrip_with_compressor(CompressorType::Snappy, &data);
}

#[test]
fn test_roundtrip_zstd() {
    let data: Vec<u8> = (0..100_000).map(|i| (i % 251) as u8).collect();
    roundtrip_with_compressor(CompressorType::Zstd, &data);
}

#[test]
fn test_roundtrip_zstd_with_dictionary() {
    let mut options = std::collections::HashMap::new();
    options.insert(
        ZSTD_DICTIONARY_OPTION.to_string(),
        "tenant_id partition_key clustering_key cell_name timestamp".to_string(),
    );
    options.insert(ZSTD_LEVEL_OPTION.to_string(), "3".to_string());
    let params = CompressionParams {
        compressor_type: CompressorType::Zstd,
        chunk_size: 4096,
        options,
    };
    let data = (0..5000)
        .flat_map(|i| {
            format!("tenant_id=42 partition_key=user-{i} clustering_key=event cell_name=value timestamp={i}\n")
                .into_bytes()
        })
        .collect::<Vec<_>>();
    roundtrip_with_params(params, &data);
}

#[test]
fn test_roundtrip_deflate() {
    let data: Vec<u8> = (0..100_000).map(|i| (i % 251) as u8).collect();
    roundtrip_with_compressor(CompressorType::Deflate, &data);
}

#[test]
fn test_roundtrip_noop() {
    let data: Vec<u8> = (0..100_000).map(|i| (i % 251) as u8).collect();
    roundtrip_with_compressor(CompressorType::Noop, &data);
}

#[test]
fn test_roundtrip_empty() {
    roundtrip_with_compressor(CompressorType::Lz4, &[]);
}

#[test]
fn test_roundtrip_single_byte() {
    roundtrip_with_compressor(CompressorType::Lz4, &[42]);
}

#[test]
fn test_roundtrip_exact_chunk_boundary() {
    // Exactly 2 chunks of 16384 bytes
    let data = vec![0x55u8; 16384 * 2];
    roundtrip_with_compressor(CompressorType::Lz4, &data);
}
