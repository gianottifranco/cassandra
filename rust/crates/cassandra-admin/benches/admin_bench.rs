// Licensed under Apache License, Version 2.0.

//! Admin benchmarks: metrics gathering overhead.

use cassandra_admin::prometheus_metrics::MetricsRegistry;
use criterion::{Criterion, black_box, criterion_group, criterion_main};

fn bench_metrics_gather(c: &mut Criterion) {
    let registry = MetricsRegistry::new();
    // Pre-populate some data
    for _ in 0..100 {
        registry.inc_reads();
        registry.inc_writes();
        registry.observe_request_latency("read", 0.001);
    }
    registry.live_sstable_count.set(100);
    registry.pending_compactions.set(5);

    c.bench_function("metrics_gather_text", |b| {
        b.iter(|| {
            black_box(registry.gather_text());
        });
    });
}

fn bench_metrics_increment(c: &mut Criterion) {
    let registry = MetricsRegistry::new();

    c.bench_function("metrics_inc_reads", |b| {
        b.iter(|| {
            registry.inc_reads();
        });
    });
}

fn bench_latency_observe(c: &mut Criterion) {
    let registry = MetricsRegistry::new();

    c.bench_function("metrics_observe_latency", |b| {
        b.iter(|| {
            registry.observe_request_latency(black_box("read"), black_box(0.001));
        });
    });
}

criterion_group!(
    benches,
    bench_metrics_gather,
    bench_metrics_increment,
    bench_latency_observe
);
criterion_main!(benches);
