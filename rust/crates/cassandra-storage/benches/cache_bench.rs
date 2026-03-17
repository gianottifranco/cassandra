// Licensed under Apache License, Version 2.0.

//! Benchmarks for the cache subsystem.

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use std::sync::Arc;

use cassandra_storage::cache::chunk_cache::{ChunkCache, ChunkCacheConfig};
use cassandra_storage::cache::counter_cache::{CounterCache, CounterCacheConfig};
use cassandra_storage::cache::row_cache::{RowCache, RowCacheConfig};
use cassandra_storage::memtable::partition::PartitionData;
use cassandra_storage::sstable::key_cache::{KeyCache, KeyCacheConfig};

fn key_cache_benchmarks(c: &mut Criterion) {
    let mut group = c.benchmark_group("key_cache");

    for size in [1_000usize, 10_000, 100_000] {
        let cache = KeyCache::new(KeyCacheConfig {
            max_entries: size * 2,
        });
        for i in 0..size {
            cache.put(1, format!("key_{i}").into_bytes(), i as u64 * 100);
        }

        group.bench_with_input(BenchmarkId::new("hit", size), &size, |b, &size| {
            let mut i = 0u64;
            b.iter(|| {
                let key = format!("key_{}", i % size as u64);
                black_box(cache.get(1, key.as_bytes()));
                i += 1;
            });
        });

        group.bench_with_input(BenchmarkId::new("miss", size), &size, |b, _| {
            b.iter(|| {
                black_box(cache.get(99, b"nonexistent"));
            });
        });

        group.bench_with_input(BenchmarkId::new("put", size), &size, |b, &size| {
            let mut i = 0u64;
            b.iter(|| {
                let key = format!("bench_put_{}", i % (size as u64 * 2));
                cache.put(1, key.into_bytes(), i * 100);
                i += 1;
            });
        });
    }
    group.finish();
}

fn key_cache_invalidate_benchmarks(c: &mut Criterion) {
    let mut group = c.benchmark_group("key_cache_invalidate");

    for sstable_count in [10u64, 50, 100] {
        let keys_per_sstable = 1_000usize;

        group.bench_with_input(
            BenchmarkId::new("invalidate_sstable", sstable_count),
            &sstable_count,
            |b, &sstable_count| {
                b.iter_with_setup(
                    || {
                        let cache = KeyCache::new(KeyCacheConfig {
                            max_entries: sstable_count as usize * keys_per_sstable * 2,
                        });
                        for ss in 0..sstable_count {
                            for i in 0..keys_per_sstable {
                                cache.put(ss, format!("key_{i}").into_bytes(), i as u64 * 100);
                            }
                        }
                        cache
                    },
                    |cache| {
                        black_box(cache.invalidate_sstable(sstable_count / 2));
                    },
                );
            },
        );
    }
    group.finish();
}

fn row_cache_benchmarks(c: &mut Criterion) {
    let mut group = c.benchmark_group("row_cache");

    let cache = RowCache::new(RowCacheConfig {
        max_entries: 10_000,
    });
    for i in 0..1_000u64 {
        cache.put(1, format!("pk_{i}").into_bytes(), PartitionData::new());
    }

    group.bench_function("hit", |b| {
        let mut i = 0u64;
        b.iter(|| {
            let key = format!("pk_{}", i % 1_000);
            black_box(cache.get(1, key.as_bytes()));
            i += 1;
        });
    });

    group.bench_function("miss", |b| {
        b.iter(|| {
            black_box(cache.get(99, b"nonexistent"));
        });
    });

    group.finish();
}

fn counter_cache_benchmarks(c: &mut Criterion) {
    let mut group = c.benchmark_group("counter_cache");

    let cache = CounterCache::new(CounterCacheConfig {
        max_entries: 10_000,
    });
    for i in 0..1_000u64 {
        cache.put(
            1,
            format!("pk_{i}").into_bytes(),
            b"counter_col".to_vec(),
            i.to_le_bytes().to_vec(),
        );
    }

    group.bench_function("hit", |b| {
        let mut i = 0u64;
        b.iter(|| {
            let key = format!("pk_{}", i % 1_000);
            black_box(cache.get(1, key.as_bytes(), b"counter_col"));
            i += 1;
        });
    });

    group.finish();
}

fn chunk_cache_benchmarks(c: &mut Criterion) {
    let mut group = c.benchmark_group("chunk_cache");

    let cache = ChunkCache::new(ChunkCacheConfig {
        max_size_bytes: 64 * 1024 * 1024,
    });
    for i in 0..1_000u64 {
        cache.put(1, i * 4096, Arc::new(vec![0u8; 4096]));
    }

    group.bench_function("hit_4k", |b| {
        let mut i = 0u64;
        b.iter(|| {
            black_box(cache.get(1, (i % 1_000) * 4096));
            i += 1;
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    key_cache_benchmarks,
    key_cache_invalidate_benchmarks,
    row_cache_benchmarks,
    counter_cache_benchmarks,
    chunk_cache_benchmarks,
);
criterion_main!(benches);
