// Licensed under Apache License, Version 2.0.

use std::io::{Seek, SeekFrom, Write};

use cassandra_io::util::data_input::DataInputPlus;
use cassandra_io::util::data_output::DataOutputPlus;
use cassandra_io::util::file_handle::FileHandle;
use cassandra_io::util::mmap_rebufferer::MmapRebufferer;
use cassandra_io::util::rebufferer::Rebufferer;
use cassandra_io::util::sequential_writer::SequentialWriter;
use tempfile::NamedTempFile;

#[test]
fn file_handle_random_reader_and_data_io_roundtrip() {
    let file = NamedTempFile::new().unwrap();
    let path = file.path().to_path_buf();

    let mut writer = SequentialWriter::new(&path).unwrap().with_buffer_size(8);
    writer.write_int(0x0102_0304).unwrap();
    writer.write_long(-9).unwrap();
    writer.write_vint(-12345).unwrap();
    writer.write_unsigned_vint(98_765).unwrap();
    writer.write_utf("cassandra-io").unwrap();
    writer.write_all(b"tail").unwrap();
    writer.finish().unwrap();

    let handle = FileHandle::builder(&path)
        .with_buffer_size(5)
        .with_mmap(false)
        .build()
        .unwrap();
    assert_eq!(
        handle.file_length().unwrap(),
        std::fs::metadata(&path).unwrap().len()
    );

    let mut reader = handle.create_reader().unwrap();
    assert_eq!(reader.read_int().unwrap(), 0x0102_0304);
    assert_eq!(reader.read_long().unwrap(), -9);
    assert_eq!(reader.read_vint().unwrap(), -12345);
    assert_eq!(reader.read_unsigned_vint().unwrap(), 98_765);
    assert_eq!(reader.read_utf().unwrap(), "cassandra-io");

    let mut tail = [0u8; 4];
    reader.read_fully(&mut tail).unwrap();
    assert_eq!(&tail, b"tail");

    reader.seek(SeekFrom::Start(4)).unwrap();
    assert_eq!(reader.read_long().unwrap(), -9);

    let mmap_handle = FileHandle::builder(&path).with_mmap(true).build().unwrap();
    let mut mmap_reader = mmap_handle.create_reader().unwrap();
    assert_eq!(mmap_reader.read_int().unwrap(), 0x0102_0304);

    let mmap = MmapRebufferer::new(&path, 8).unwrap();
    let first_chunk = mmap.rebuffer(0).unwrap();
    assert_eq!(&first_chunk.buffer()[..4], &0x0102_0304i32.to_be_bytes());
}
