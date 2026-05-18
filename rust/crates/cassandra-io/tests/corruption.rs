// Licensed under Apache License, Version 2.0.

//! Corruption detection tests: verify that checksummed and compressed readers
//! detect bit-flips in stored data.

use std::io::{Read, Seek, SeekFrom, Write};
use tempfile::NamedTempFile;

use cassandra_io::compress::compressed_reader::CompressedChunkReader;
use cassandra_io::compress::compressed_writer::CompressedSequentialWriter;
use cassandra_io::compress::metadata::CompressionMetadata;
use cassandra_io::compress::metadata::CompressionParams;
use cassandra_io::util::checksummed_rebufferer::ChecksummedRebufferer;
use cassandra_io::util::chunk_reader::SimpleChunkReader;
use cassandra_io::util::data_integrity::DataIntegrityMetadata;
use cassandra_io::util::rebufferer::Rebufferer;

#[test]
fn test_checksummed_rebufferer_detects_corruption() {
    // Write known data
    let mut data_file = NamedTempFile::new().unwrap();
    let data = vec![0xABu8; 65536 * 3]; // 3 chunks
    data_file.write_all(&data).unwrap();
    data_file.flush().unwrap();

    // Compute correct checksums
    let chunk_size = 65536u32;
    let mut checksums = Vec::new();
    for chunk in data.chunks(chunk_size as usize) {
        checksums.push(crc32fast::hash(chunk));
    }
    let metadata = DataIntegrityMetadata::new(chunk_size, checksums);

    // Verify clean read works
    let reader = SimpleChunkReader::new(data_file.path(), chunk_size as usize).unwrap();
    let checked = ChecksummedRebufferer::new(Box::new(reader), metadata.clone());
    assert!(checked.rebuffer(0).is_ok());
    assert!(checked.rebuffer(65536).is_ok());
    assert!(checked.rebuffer(131072).is_ok());

    // Now corrupt the second chunk (byte at offset 65536)
    let file = data_file.as_file_mut();
    file.seek(SeekFrom::Start(65536)).unwrap();
    file.write_all(&[0x00]).unwrap(); // flip from 0xAB to 0x00
    file.flush().unwrap();

    // First chunk should still be fine
    let reader2 = SimpleChunkReader::new(data_file.path(), chunk_size as usize).unwrap();
    let checked2 = ChecksummedRebufferer::new(Box::new(reader2), metadata);
    assert!(checked2.rebuffer(0).is_ok());

    // Second chunk should fail
    let result = checked2.rebuffer(65536);
    assert!(
        result.is_err(),
        "Expected checksum mismatch on corrupted chunk"
    );
}

#[test]
fn test_compressed_reader_detects_crc_corruption() {
    let data_file = NamedTempFile::new().unwrap();
    let meta_file = NamedTempFile::new().unwrap();

    // Write compressed data
    let params = CompressionParams::default();
    let mut writer = CompressedSequentialWriter::new(data_file.path(), &params).unwrap();
    let data = vec![0x42u8; 65536 + 100]; // slightly more than 1 chunk
    writer.write_all(&data).unwrap();
    writer.finish(meta_file.path()).unwrap();

    // Verify clean read works
    let mut meta_reader = std::fs::File::open(meta_file.path()).unwrap();
    let metadata = CompressionMetadata::read_from(&mut meta_reader).unwrap();
    let reader = CompressedChunkReader::new(data_file.path(), metadata).unwrap();
    let holder = reader.rebuffer(0).unwrap();
    assert_eq!(&holder.buffer()[..10], &data[..10]);

    // Corrupt the compressed data (flip a byte near the start of the file)
    {
        let mut f = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(data_file.path())
            .unwrap();
        // Skip past the compressed_len (4 bytes), corrupt the first compressed byte
        f.seek(SeekFrom::Start(4)).unwrap();
        let mut byte = [0u8; 1];
        f.read_exact(&mut byte).unwrap();
        f.seek(SeekFrom::Start(4)).unwrap();
        f.write_all(&[byte[0] ^ 0xFF]).unwrap(); // flip all bits
        f.flush().unwrap();
    }

    // Re-read metadata and try to read — should detect CRC mismatch
    let mut meta_reader2 = std::fs::File::open(meta_file.path()).unwrap();
    let metadata2 = CompressionMetadata::read_from(&mut meta_reader2).unwrap();
    let reader2 = CompressedChunkReader::new(data_file.path(), metadata2).unwrap();
    let result = reader2.rebuffer(0);
    assert!(
        result.is_err(),
        "Expected CRC mismatch on corrupted compressed data"
    );
}
