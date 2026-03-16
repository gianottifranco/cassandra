// Licensed under Apache License, Version 2.0.

//! Streaming benchmarks: chunkify, checksum, rate limiter.

use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use uuid::Uuid;

use cassandra_streaming::transfer::{
    ChecksumAlgorithm, ChunkChecksum, StreamRateLimiter, StreamTransfer,
};

fn bench_chunkify(c: &mut Criterion) {
    let mut group = c.benchmark_group("chunkify");
    let data = vec![42u8; 1024 * 1024]; // 1 MB

    for chunk_size in [4096, 16384, 65536, 262144] {
        group.bench_with_input(
            BenchmarkId::new("size", chunk_size),
            &chunk_size,
            |b, &cs| {
                let id = Uuid::new_v4();
                b.iter(|| {
                    black_box(StreamTransfer::chunkify(id, &data, cs));
                });
            },
        );
    }
    group.finish();
}

fn bench_checksum_md5(c: &mut Criterion) {
    let data = vec![0u8; 65536]; // 64 KiB
    c.bench_function("checksum_md5_64k", |b| {
        b.iter(|| {
            black_box(ChunkChecksum::compute(&data, ChecksumAlgorithm::Md5));
        });
    });
}

fn bench_checksum_crc32(c: &mut Criterion) {
    let data = vec![0u8; 65536]; // 64 KiB
    c.bench_function("checksum_crc32_64k", |b| {
        b.iter(|| {
            black_box(ChunkChecksum::compute(&data, ChecksumAlgorithm::Crc32));
        });
    });
}

fn bench_rate_limiter(c: &mut Criterion) {
    let rl = StreamRateLimiter::new(100_000_000); // 100 MB/s
    c.bench_function("rate_limiter_acquire", |b| {
        b.iter(|| {
            black_box(rl.acquire(65536));
        });
    });
}

fn bench_checksum_verify(c: &mut Criterion) {
    let data = vec![1u8; 65536];
    let checksum_md5 = ChunkChecksum::compute(&data, ChecksumAlgorithm::Md5);
    let checksum_crc32 = ChunkChecksum::compute(&data, ChecksumAlgorithm::Crc32);

    let mut group = c.benchmark_group("checksum_verify");
    group.bench_function("md5_64k", |b| {
        b.iter(|| {
            black_box(checksum_md5.verify(&data));
        });
    });
    group.bench_function("crc32_64k", |b| {
        b.iter(|| {
            black_box(checksum_crc32.verify(&data));
        });
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_chunkify,
    bench_checksum_md5,
    bench_checksum_crc32,
    bench_rate_limiter,
    bench_checksum_verify,
);
criterion_main!(benches);
