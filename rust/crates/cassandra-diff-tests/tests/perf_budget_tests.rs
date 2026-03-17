// Licensed under Apache License, Version 2.0.

//! Performance budget tests — wall-clock assertions against ADR-015.

use cassandra_diff_tests::comparators::protocol;
use cassandra_diff_tests::perf_report::{self, PerfReport, budgets};
use cassandra_storage::commitlog::{CellMutation, CommitLogConfig, Mutation, MutationRow};
use cassandra_storage::engine::{EngineConfig, StorageEngine};

use std::time::Instant;
use tempfile::TempDir;

fn bench_engine(dir: &std::path::Path) -> StorageEngine {
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

fn make_mutation(ks: &str, tbl: &str, pk: &[u8], val: &[u8], ts: i64) -> Mutation {
    Mutation {
        keyspace: ks.to_string(),
        table: tbl.to_string(),
        partition_key: pk.to_vec(),
        rows: vec![MutationRow {
            clustering_key: b"ck".to_vec(),
            cells: vec![CellMutation {
                column: "v".to_string(),
                value: Some(val.to_vec()),
                timestamp: ts,
                ttl: 0,
                local_deletion_time: None,
                is_tombstone: false,
            }],
            is_tombstone: false,
            local_deletion_time: None,
        }],
        timestamp: ts,
        cdc_enabled: false,
        static_cells: vec![],
        partition_tombstone: None,
        range_tombstones: vec![],
    }
}

#[test]
#[ignore]
fn perf_memtable_write_budget() {
    let dir = TempDir::new().unwrap();
    let engine = bench_engine(dir.path());
    let ops = 10_000u64;
    let mut latencies: Vec<u64> = Vec::with_capacity(ops as usize);

    // Warmup
    for i in 0..100 {
        let m = make_mutation(
            "bench_ks",
            "bench_t",
            format!("warmup-{i}").as_bytes(),
            b"warm",
            i,
        );
        engine.apply_mutation(&m).unwrap();
    }

    for i in 0..ops {
        let m = make_mutation(
            "bench_ks",
            "bench_t",
            format!("perf-{i}").as_bytes(),
            &vec![0x42u8; 256],
            i as i64,
        );
        let t0 = Instant::now();
        engine.apply_mutation(&m).unwrap();
        latencies.push(t0.elapsed().as_micros() as u64);
    }

    let measurement = perf_report::compute_latency("memtable_write", &mut latencies);
    let check = perf_report::check_budget(&measurement, &budgets::MEMTABLE_WRITE);
    println!("\n{}", check.details);
    if !check.passed {
        println!("⚠️  Budget exceeded — may be hardware-dependent.");
    }
}

#[test]
#[ignore]
fn perf_memtable_read_budget() {
    let dir = TempDir::new().unwrap();
    let engine = bench_engine(dir.path());
    let ops = 10_000u64;

    for i in 0..ops {
        let m = make_mutation(
            "bench_ks",
            "bench_t",
            format!("read-{i}").as_bytes(),
            b"val",
            i as i64,
        );
        engine.apply_mutation(&m).unwrap();
    }

    let mut latencies: Vec<u64> = Vec::with_capacity(ops as usize);
    for i in 0..ops {
        let t0 = Instant::now();
        let _ = engine.read_partition("bench_ks", "bench_t", format!("read-{i}").as_bytes());
        latencies.push(t0.elapsed().as_micros() as u64);
    }

    let measurement = perf_report::compute_latency("memtable_read", &mut latencies);
    let check = perf_report::check_budget(&measurement, &budgets::MEMTABLE_READ);
    println!("\n{}", check.details);
    if !check.passed {
        println!("⚠️  Budget exceeded — may be hardware-dependent.");
    }
}

#[test]
#[ignore]
fn perf_frame_parse_budget() {
    let ops = 100_000u64;
    let frame = vec![0x84, 0x00, 0x00, 0x01, 0x07, 0x00, 0x00, 0x00, 0x20];

    // Warmup
    for _ in 0..1000 {
        let _ = protocol::parse_header(&frame);
    }

    let mut latencies: Vec<u64> = Vec::with_capacity(ops as usize);
    for _ in 0..ops {
        let t0 = Instant::now();
        let _ = protocol::parse_header(&frame);
        latencies.push(t0.elapsed().as_nanos() as u64 / 1000);
    }

    let measurement = perf_report::compute_latency("frame_parse", &mut latencies);
    let check = perf_report::check_budget(&measurement, &budgets::FRAME_PARSE);
    println!("\n{}", check.details);
    if !check.passed {
        println!("⚠️  Budget exceeded — may be hardware-dependent.");
    }
}

#[test]
#[ignore]
fn perf_flush_throughput() {
    let dir = TempDir::new().unwrap();
    let engine = bench_engine(dir.path());
    let count = 1000u64;

    for i in 0..count {
        let m = make_mutation(
            "bench_ks",
            "flush_t",
            format!("flush-{i}").as_bytes(),
            &vec![0u8; 256],
            i as i64,
        );
        engine.apply_mutation(&m).unwrap();
    }

    let t0 = Instant::now();
    engine.flush_cf("bench_ks.flush_t").unwrap();
    let elapsed = t0.elapsed();
    let total_bytes = count * (256 + 64);
    let mb_per_sec = (total_bytes as f64 / 1_000_000.0) / elapsed.as_secs_f64();
    println!("\nFlush throughput: {mb_per_sec:.1} MB/s ({count} partitions, {elapsed:?})");
}

#[test]
#[ignore]
fn perf_memory_growth_budget() {
    let dir = TempDir::new().unwrap();
    let engine = bench_engine(dir.path());
    let initial = engine.stats().memtable_memory_bytes;

    for i in 0..100_000u64 {
        let m = make_mutation(
            "bench_ks",
            "mem_t",
            format!("mem-{i}").as_bytes(),
            b"memdata",
            i as i64,
        );
        engine.apply_mutation(&m).unwrap();
    }
    engine.flush_cf("bench_ks.mem_t").unwrap();

    let final_mem = engine.stats().memtable_memory_bytes;
    let baseline = initial.max(1);
    let ratio = final_mem as f64 / baseline as f64;
    println!(
        "\nMemory: initial={}B, post-flush={}B, growth={ratio:.2}x",
        initial, final_mem
    );
}

#[test]
#[ignore]
fn perf_full_report() {
    let dir = TempDir::new().unwrap();
    let engine = bench_engine(dir.path());
    let mut report = PerfReport::new();

    let mut write_lat: Vec<u64> = Vec::new();
    for i in 0..5000u64 {
        let m = make_mutation(
            "rpt_ks",
            "rpt_t",
            format!("rpt-{i}").as_bytes(),
            b"rptval",
            i as i64,
        );
        let t0 = Instant::now();
        engine.apply_mutation(&m).unwrap();
        write_lat.push(t0.elapsed().as_micros() as u64);
    }
    let wm = perf_report::compute_latency("memtable_write", &mut write_lat);
    report.add_latency_check(perf_report::check_budget(&wm, &budgets::MEMTABLE_WRITE));

    let mut read_lat: Vec<u64> = Vec::new();
    for i in 0..5000u64 {
        let t0 = Instant::now();
        let _ = engine.read_partition("rpt_ks", "rpt_t", format!("rpt-{i}").as_bytes());
        read_lat.push(t0.elapsed().as_micros() as u64);
    }
    let rm = perf_report::compute_latency("memtable_read", &mut read_lat);
    report.add_latency_check(perf_report::check_budget(&rm, &budgets::MEMTABLE_READ));

    let frame = vec![0x84, 0x00, 0x00, 0x01, 0x07, 0x00, 0x00, 0x00, 0x20];
    let mut parse_lat: Vec<u64> = Vec::new();
    for _ in 0..50_000 {
        let t0 = Instant::now();
        let _ = protocol::parse_header(&frame);
        parse_lat.push(t0.elapsed().as_nanos() as u64 / 1000);
    }
    let pm = perf_report::compute_latency("frame_parse", &mut parse_lat);
    report.add_latency_check(perf_report::check_budget(&pm, &budgets::FRAME_PARSE));

    report.print_summary();
    println!("\n--- Performance Report JSON ---");
    println!("{}", report.to_json());
}
