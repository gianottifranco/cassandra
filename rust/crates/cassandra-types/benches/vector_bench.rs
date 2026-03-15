// Licensed under Apache License, Version 2.0.
//! Criterion benchmarks for vector similarity computations.

use criterion::{criterion_group, criterion_main, Criterion, black_box};
use cassandra_types::vector::{VectorValue, cosine_similarity, euclidean_distance, dot_product};

fn bench_cosine(c: &mut Criterion) {
    let dim = 128;
    let a = VectorValue::new((0..dim).map(|i| i as f32 * 0.1).collect());
    let b = VectorValue::new((0..dim).map(|i| (dim - i) as f32 * 0.1).collect());

    c.bench_function("cosine_similarity_128d", |bench| {
        bench.iter(|| {
            black_box(cosine_similarity(black_box(&a), black_box(&b)));
        });
    });
}

fn bench_euclidean(c: &mut Criterion) {
    let dim = 128;
    let a = VectorValue::new((0..dim).map(|i| i as f32 * 0.1).collect());
    let b = VectorValue::new((0..dim).map(|i| (dim - i) as f32 * 0.1).collect());

    c.bench_function("euclidean_distance_128d", |bench| {
        bench.iter(|| {
            black_box(euclidean_distance(black_box(&a), black_box(&b)));
        });
    });
}

fn bench_dot_product(c: &mut Criterion) {
    let dim = 128;
    let a = VectorValue::new((0..dim).map(|i| i as f32 * 0.1).collect());
    let b = VectorValue::new((0..dim).map(|i| (dim - i) as f32 * 0.1).collect());

    c.bench_function("dot_product_128d", |bench| {
        bench.iter(|| {
            black_box(dot_product(black_box(&a), black_box(&b)));
        });
    });
}

fn bench_serialize(c: &mut Criterion) {
    let dim = 128;
    let v = VectorValue::new((0..dim).map(|i| i as f32 * 0.1).collect());

    c.bench_function("vector_serialize_128d", |bench| {
        bench.iter(|| {
            black_box(v.serialize());
        });
    });
}

fn bench_deserialize(c: &mut Criterion) {
    let dim = 128u32;
    let v = VectorValue::new((0..dim).map(|i| i as f32 * 0.1).collect());
    let bytes = v.serialize();

    c.bench_function("vector_deserialize_128d", |bench| {
        bench.iter(|| {
            black_box(VectorValue::deserialize(black_box(&bytes), dim).unwrap());
        });
    });
}

criterion_group!(
    benches,
    bench_cosine,
    bench_euclidean,
    bench_dot_product,
    bench_serialize,
    bench_deserialize,
);
criterion_main!(benches);
