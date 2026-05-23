// Licensed under Apache License, Version 2.0.

//! Storage engine benchmarks.
//!
//! Measures performance of critical storage path operations against
//! budgets from ADR-015.
//!
//! ## Running
//!
//! ```bash
//! cargo bench -p cassandra-storage --bench engine_bench
//! ```

use criterion::{BatchSize, Criterion, black_box, criterion_group, criterion_main};

use cassandra_storage::commitlog::{CellMutation, CommitLogConfig, Mutation, MutationRow};
use cassandra_storage::engine::{EngineConfig, StorageEngine};
use cassandra_storage::memtable::partition::{Cell, Row};

use std::path::Path;

fn bench_engine(dir: &Path) -> StorageEngine {
    let config = EngineConfig {
        data_directories: vec![dir.join("data")],
        commitlog: CommitLogConfig {
            max_segment_size: 32 * 1024 * 1024,
            directory: dir.join("commitlog"),
            ..CommitLogConfig::default()
        },
        memtable_flush_threshold: 256 * 1024 * 1024,
        gc_grace_seconds: 86400,
        ..EngineConfig::default()
    };
    StorageEngine::open(config).expect("open engine")
}

fn make_row(ck: &[u8], col: &str, val: &[u8], ts: i64) -> Row {
    Row {
        clustering_key: ck.to_vec(),
        cells: vec![Cell {
            column: col.to_string(),
            value: Some(val.to_vec()),
            timestamp: ts,
            ttl: 0,
            local_deletion_time: None,
            is_tombstone: false,
        }],
        is_tombstone: false,
        local_deletion_time: None,
    }
}

fn make_mutation(keyspace: &str, table: &str, pk: Vec<u8>, rows: Vec<Row>, ts: i64) -> Mutation {
    Mutation {
        keyspace: keyspace.to_string(),
        table: table.to_string(),
        partition_key: pk,
        rows: rows
            .into_iter()
            .map(|row| MutationRow {
                clustering_key: row.clustering_key,
                cells: row
                    .cells
                    .into_iter()
                    .map(|cell| CellMutation {
                        column: cell.column,
                        value: cell.value,
                        timestamp: cell.timestamp,
                        ttl: cell.ttl,
                        local_deletion_time: cell.local_deletion_time,
                        is_tombstone: cell.is_tombstone,
                    })
                    .collect(),
                is_tombstone: row.is_tombstone,
                local_deletion_time: row.local_deletion_time,
            })
            .collect(),
        timestamp: ts,
        cdc_enabled: false,
        static_cells: Vec::new(),
        partition_tombstone: None,
        range_tombstones: Vec::new(),
    }
}

fn bench_memtable_write(c: &mut Criterion) {
    let dir = tempfile::TempDir::new().unwrap();
    let engine = bench_engine(dir.path());

    let mut counter = 0u64;

    c.bench_function("memtable_write_1kb", |b| {
        b.iter(|| {
            counter += 1;
            let pk = format!("pk-{counter}");
            let val = vec![0x42u8; 1024]; // 1 KiB value
            let mutation = make_mutation(
                "bench_ks",
                "bench_table",
                black_box(pk.as_bytes().to_vec()),
                vec![make_row(b"ck0", "data", &val, counter as i64)],
                counter as i64,
            );
            engine.apply_mutation(&mutation).unwrap();
        });
    });
}

fn bench_memtable_read(c: &mut Criterion) {
    let dir = tempfile::TempDir::new().unwrap();
    let engine = bench_engine(dir.path());

    // Pre-populate
    for i in 0..10_000 {
        let pk = format!("pk-{i}");
        let mutation = make_mutation(
            "bench_ks",
            "bench_table",
            pk.as_bytes().to_vec(),
            vec![make_row(b"ck", "v", b"value", i as i64)],
            i as i64,
        );
        engine.apply_mutation(&mutation).unwrap();
    }

    let mut counter = 0u64;

    c.bench_function("memtable_read_single_partition", |b| {
        b.iter(|| {
            counter = (counter + 1) % 10_000;
            let pk = format!("pk-{counter}");
            black_box(
                engine
                    .read_partition("bench_ks", "bench_table", pk.as_bytes())
                    .unwrap(),
            );
        });
    });
}

fn bench_sstable_read(c: &mut Criterion) {
    let dir = tempfile::TempDir::new().unwrap();
    let engine = bench_engine(dir.path());

    // Write + flush to create SSTable
    for i in 0..5_000 {
        let pk = format!("sst-pk-{i}");
        let mutation = make_mutation(
            "bench_ks",
            "sst_table",
            pk.as_bytes().to_vec(),
            vec![make_row(b"ck", "v", b"sstable-data", i as i64)],
            i as i64,
        );
        engine.apply_mutation(&mutation).unwrap();
    }
    engine.flush_cf("bench_ks.sst_table").unwrap();

    let mut counter = 0u64;

    c.bench_function("sstable_read_single_partition", |b| {
        b.iter(|| {
            counter = (counter + 1) % 5_000;
            let pk = format!("sst-pk-{counter}");
            black_box(
                engine
                    .read_partition("bench_ks", "sst_table", pk.as_bytes())
                    .unwrap(),
            );
        });
    });
}

fn bench_flush(c: &mut Criterion) {
    c.bench_function("flush_1000_partitions", |b| {
        b.iter_batched(
            || {
                let dir = tempfile::TempDir::new().unwrap();
                let engine = bench_engine(dir.path());

                for i in 0..1_000 {
                    let pk = format!("flush-pk-{i}");
                    let mutation = make_mutation(
                        "bench_ks",
                        "flush_table",
                        pk.as_bytes().to_vec(),
                        vec![make_row(b"ck", "v", &vec![0u8; 256], i as i64)],
                        i as i64,
                    );
                    engine.apply_mutation(&mutation).unwrap();
                }

                (dir, engine)
            },
            |(_dir, engine)| {
                engine.flush_cf("bench_ks.flush_table").unwrap();
            },
            BatchSize::PerIteration,
        );
    });
}

fn bench_snapshot(c: &mut Criterion) {
    let dir = tempfile::TempDir::new().unwrap();
    let engine = bench_engine(dir.path());

    for i in 0..500 {
        let pk = format!("snap-pk-{i}");
        let mutation = make_mutation(
            "bench_ks",
            "snap_table",
            pk.as_bytes().to_vec(),
            vec![make_row(b"ck", "v", &vec![0u8; 512], i as i64)],
            i as i64,
        );
        engine.apply_mutation(&mutation).unwrap();
    }
    engine.flush_cf("bench_ks.snap_table").unwrap();

    let mut snap_counter = 0u64;

    c.bench_function("snapshot_500_partitions", |b| {
        b.iter(|| {
            snap_counter += 1;
            let name = format!("bench-snap-{snap_counter}");
            black_box(
                engine
                    .snapshot(&name, "bench_ks", "snap_table", None)
                    .unwrap(),
            );
        });
    });
}

criterion_group!(
    benches,
    bench_memtable_write,
    bench_memtable_read,
    bench_sstable_read,
    bench_flush,
    bench_snapshot,
);
criterion_main!(benches);
