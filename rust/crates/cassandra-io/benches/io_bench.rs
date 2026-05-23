// Licensed under Apache License, Version 2.0.

//! Benchmarks for IO operations: sequential writes, positioned reads,
//! compression throughput, and buffer pool operations.

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use std::io::Write;
use tempfile::NamedTempFile;

use cassandra_io::compress::{CompressorType, create_compressor};
use cassandra_io::util::buffer_pool::BufferPool;
use cassandra_io::util::chunk_reader::SimpleChunkReader;
use cassandra_io::util::rebufferer::Rebufferer;
use cassandra_io::util::sequential_writer::SequentialWriter;

fn bench_sequential_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("sequential_write");
    for size in [1_usize << 20, 10 << 20] {
        let label = format!("{}MB", size >> 20);
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::new("write", &label), &size, |b, &sz| {
            let data = vec![0xABu8; sz];
            b.iter(|| {
                let tmp = NamedTempFile::new().unwrap();
                let mut w = SequentialWriter::new(tmp.path()).unwrap();
                w.write_all(&data).unwrap();
                w.finish().unwrap();
            });
        });
    }
    group.finish();
}

fn bench_positioned_read(c: &mut Criterion) {
    let mut group = c.benchmark_group("positioned_read");
    let data = vec![0xCDu8; 1 << 20]; // 1 MB
    let mut tmp = NamedTempFile::new().unwrap();
    tmp.write_all(&data).unwrap();
    tmp.flush().unwrap();

    group.throughput(Throughput::Bytes(data.len() as u64));
    group.bench_function("1MB_64K_chunks", |b| {
        let reader = SimpleChunkReader::new(tmp.path(), 65536).unwrap();
        b.iter(|| {
            let mut pos = 0u64;
            while pos < data.len() as u64 {
                let _ = reader.rebuffer(pos).unwrap();
                pos += 65536;
            }
        });
    });
    group.finish();
}

fn bench_compression_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("compression");
    // Compressible data: repeated pattern
    let data_64k = vec![0x42u8; 65536];

    for ctype in [
        CompressorType::Lz4,
        CompressorType::Snappy,
        CompressorType::Zstd,
        CompressorType::Deflate,
    ] {
        let compressor = create_compressor(ctype);
        let label = format!("{ctype}");

        group.throughput(Throughput::Bytes(data_64k.len() as u64));
        group.bench_with_input(
            BenchmarkId::new("compress_64K", &label),
            &data_64k,
            |b, data| {
                let mut out = Vec::with_capacity(data.len());
                b.iter(|| {
                    out.clear();
                    compressor.compress(data, &mut out).unwrap();
                });
            },
        );

        // Compress once, then benchmark decompress
        let mut compressed = Vec::new();
        compressor.compress(&data_64k, &mut compressed).unwrap();
        group.bench_with_input(
            BenchmarkId::new("decompress_64K", &label),
            &compressed,
            |b, cdata| {
                b.iter(|| {
                    compressor.decompress(cdata, data_64k.len()).unwrap();
                });
            },
        );
    }
    group.finish();
}

fn bench_buffer_pool(c: &mut Criterion) {
    c.bench_function("buffer_pool_acquire_release", |b| {
        let pool = BufferPool::new(65536, 64);
        b.iter(|| {
            let buf = pool.acquire();
            pool.release(buf);
        });
    });
}

criterion_group!(
    benches,
    bench_sequential_write,
    bench_positioned_read,
    bench_compression_throughput,
    bench_buffer_pool,
);
criterion_main!(benches);
