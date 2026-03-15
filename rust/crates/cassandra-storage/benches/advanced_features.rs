// Licensed under Apache License, Version 2.0.
//! Criterion benchmarks for advanced features: counters, SAI, vectors.

use criterion::{criterion_group, criterion_main, Criterion, black_box};
use uuid::Uuid;

fn node(n: u8) -> Uuid {
    Uuid::from_u128(n as u128)
}

mod counter_benches {
    use super::*;
    use cassandra_storage::counter::CounterContext;

    pub fn merge_single_shard(c: &mut Criterion) {
        c.bench_function("counter_merge_1_shard", |b| {
            let mut ctx1 = CounterContext::new();
            ctx1.apply_local(node(1), 100);
            let mut ctx2 = CounterContext::new();
            ctx2.apply_local(node(2), 200);

            b.iter(|| {
                let mut c = ctx1.clone();
                c.merge(black_box(&ctx2));
                black_box(&c);
            });
        });
    }

    pub fn merge_many_shards(c: &mut Criterion) {
        c.bench_function("counter_merge_10_shards", |b| {
            let mut ctx1 = CounterContext::new();
            let mut ctx2 = CounterContext::new();
            for i in 0..10 {
                ctx1.apply_local(node(i), (i as i64 + 1) * 10);
                ctx2.apply_local(node(i), (i as i64 + 1) * 5);
            }

            b.iter(|| {
                let mut c = ctx1.clone();
                c.merge(black_box(&ctx2));
                black_box(&c);
            });
        });
    }

    pub fn serialize_deserialize(c: &mut Criterion) {
        c.bench_function("counter_serialize_10_shards", |b| {
            let mut ctx = CounterContext::new();
            for i in 0..10 {
                ctx.apply_local(node(i), (i as i64 + 1) * 100);
            }

            b.iter(|| {
                let bytes = ctx.serialize();
                let decoded = CounterContext::deserialize(black_box(&bytes)).unwrap();
                black_box(&decoded);
            });
        });
    }

    pub fn cleanup_benchmark(c: &mut Criterion) {
        c.bench_function("counter_cleanup_10_remote", |b| {
            let local = node(0);
            let mut ctx = CounterContext::new();
            ctx.apply_local(local, 100);
            for i in 1..10 {
                ctx.apply_local(node(i), (i as i64) * 10);
            }

            b.iter(|| {
                let mut c = ctx.clone();
                c.cleanup(black_box(local));
                black_box(&c);
            });
        });
    }
}

mod sai_benches {
    use super::*;
    use cassandra_storage::index::sai::SaiIndex;
    use cassandra_storage::index::sai::builder::{SaiSegmentBuilder, merge_segments};
    use cassandra_storage::index::{IndexEntry, SecondaryIndex};

    pub fn insert_search(c: &mut Criterion) {
        c.bench_function("sai_insert_1000_search", |b| {
            b.iter(|| {
                let idx = SaiIndex::create("bench_idx", "ks", "t", "col");
                for i in 0u32..1000 {
                    let term = i.to_be_bytes().to_vec();
                    let pk = format!("pk_{i}").into_bytes();
                    let _ = idx.insert(&IndexEntry {
                        term,
                        partition_key: pk,
                        clustering_key: vec![],
                    });
                }
                let results = idx.search(black_box(&500u32.to_be_bytes())).unwrap();
                black_box(results);
            });
        });
    }

    pub fn segment_merge(c: &mut Criterion) {
        c.bench_function("sai_merge_2_segments_100_terms", |b| {
            let mut b1 = SaiSegmentBuilder::new(1, "idx", "col");
            let mut b2 = SaiSegmentBuilder::new(2, "idx", "col");
            for i in 0u32..100 {
                b1.add(i.to_be_bytes().to_vec(), format!("pk_{i}").into_bytes(), vec![]);
                b2.add((i + 50).to_be_bytes().to_vec(), format!("pk2_{i}").into_bytes(), vec![]);
            }
            let s1 = b1.build();
            let s2 = b2.build();
            let segments = vec![s1, s2];

            b.iter(|| {
                let merged = merge_segments(black_box(&segments), 3);
                black_box(merged);
            });
        });
    }
}

criterion_group!(
    benches,
    counter_benches::merge_single_shard,
    counter_benches::merge_many_shards,
    counter_benches::serialize_deserialize,
    counter_benches::cleanup_benchmark,
    sai_benches::insert_search,
    sai_benches::segment_merge,
);
criterion_main!(benches);
