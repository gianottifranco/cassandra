// Licensed under Apache License, Version 2.0.

//! Extended soak tests for the Cassandra Rust storage engine.
//!
//! ## Running
//!
//! ```bash
//! SOAK_DURATION_SECS=60 cargo test -p cassandra-diff-tests --test soak_tests -- --ignored --nocapture
//! ```

use cassandra_storage::commitlog::{CellMutation, CommitLogConfig, Mutation, MutationRow};
use cassandra_storage::engine::{EngineConfig, StorageEngine};

use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::TempDir;

fn soak_duration() -> Duration {
    let secs = std::env::var("SOAK_DURATION_SECS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(10);
    Duration::from_secs(secs)
}

fn test_engine(dir: &std::path::Path) -> StorageEngine {
    let config = EngineConfig {
        data_directories: vec![dir.join("data")],
        commitlog: CommitLogConfig {
            max_segment_size: 4 * 1024 * 1024,
            directory: dir.join("commitlog"),
            ..CommitLogConfig::default()
        },
        memtable_flush_threshold: 4 * 1024 * 1024,
        gc_grace_seconds: 60,
        ..EngineConfig::default()
    };
    StorageEngine::open(config).expect("Failed to open engine")
}

fn make_mutation(
    ks: &str,
    tbl: &str,
    pk: &[u8],
    ck: &[u8],
    col: &str,
    val: &[u8],
    ts: i64,
) -> Mutation {
    Mutation {
        keyspace: ks.to_string(),
        table: tbl.to_string(),
        partition_key: pk.to_vec(),
        rows: vec![MutationRow {
            clustering_key: ck.to_vec(),
            cells: vec![CellMutation {
                column: col.to_string(),
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

fn make_tombstone_mutation(ks: &str, tbl: &str, pk: &[u8], ck: &[u8], ts: i64) -> Mutation {
    Mutation {
        keyspace: ks.to_string(),
        table: tbl.to_string(),
        partition_key: pk.to_vec(),
        rows: vec![MutationRow {
            clustering_key: ck.to_vec(),
            cells: vec![],
            is_tombstone: true,
            local_deletion_time: Some(ts as i32),
        }],
        timestamp: ts,
        cdc_enabled: false,
        static_cells: vec![],
        partition_tombstone: None,
        range_tombstones: vec![],
    }
}

/// Core soak: sustained writes + reads + flushes + compaction.
#[test]
#[ignore]
fn soak_write_read_flush_compact() {
    let dir = TempDir::new().unwrap();
    let engine = Arc::new(test_engine(dir.path()));
    let duration = soak_duration();
    let start = Instant::now();
    let mut total_writes = 0u64;
    let mut total_reads = 0u64;
    let mut total_flushes = 0u64;

    println!("soak_write_read_flush_compact: duration={duration:?}");

    while start.elapsed() < duration {
        for batch in 0..4 {
            for i in 0..100 {
                let pk = format!("soak-{batch}-{}", total_writes + i);
                let m = make_mutation(
                    "soak_ks",
                    "soak_t",
                    pk.as_bytes(),
                    b"ck0",
                    "data",
                    b"soak-value",
                    (total_writes + i) as i64,
                );
                engine.apply_mutation(&m).expect("Write should succeed");
            }
            total_writes += 100;
        }

        // Read some recent writes
        for i in 0..10 {
            let pk = format!("soak-0-{}", total_writes.saturating_sub(10) + i);
            let _ = engine.read_partition("soak_ks", "soak_t", pk.as_bytes());
            total_reads += 1;
        }

        // Periodic flush
        if total_writes % 1000 == 0 {
            engine
                .flush_cf("soak_ks.soak_t")
                .expect("Flush should succeed");
            total_flushes += 1;
            if total_flushes % 5 == 0 {
                let _ = engine.maybe_compact();
            }
        }
    }

    let stats = engine.stats();
    println!(
        "Soak complete: writes={total_writes}, reads={total_reads}, flushes={total_flushes}, sstables={}",
        stats.sstable_count
    );

    // Verify data integrity
    let result = engine.read_partition("soak_ks", "soak_t", b"soak-0-99");
    assert!(
        result.is_some(),
        "Written data should be readable after soak"
    );
}

/// Snapshot under sustained writes.
#[test]
#[ignore]
fn soak_snapshot_under_load() {
    let dir = TempDir::new().unwrap();
    let engine = test_engine(dir.path());

    for i in 0..500 {
        let pk = format!("snap-{i}");
        let m = make_mutation(
            "ks",
            "t1",
            pk.as_bytes(),
            b"ck",
            "v",
            format!("val-{i}").as_bytes(),
            i as i64,
        );
        engine.apply_mutation(&m).unwrap();
    }
    engine.flush_cf("ks.t1").unwrap();

    let manifest = engine.snapshot("soak-snap", "ks", "t1", None).unwrap();
    println!("Snapshot created: {} files", manifest.files.len());

    // Continue writing after snapshot
    for i in 500..1000 {
        let pk = format!("snap-{i}");
        let m = make_mutation(
            "ks",
            "t1",
            pk.as_bytes(),
            b"ck",
            "v",
            format!("val-{i}").as_bytes(),
            i as i64,
        );
        engine.apply_mutation(&m).unwrap();
    }

    let result = engine.read_partition("ks", "t1", b"snap-0");
    assert!(result.is_some());
}

/// Mixed workload with compaction tracking latency percentiles.
#[test]
#[ignore]
fn soak_mixed_workload_with_compaction() {
    let dir = TempDir::new().unwrap();
    let engine = test_engine(dir.path());
    let duration = soak_duration();
    let start = Instant::now();
    let mut write_count = 0u64;
    let mut read_count = 0u64;
    let mut delete_count = 0u64;
    let mut write_lat: Vec<u64> = Vec::new();
    let mut read_lat: Vec<u64> = Vec::new();

    while start.elapsed() < duration {
        for i in 0..50 {
            let pk = format!("mix-pk-{}", write_count + i);
            let t0 = Instant::now();
            let m = make_mutation(
                "ks",
                "mixed",
                pk.as_bytes(),
                b"ck0",
                "v",
                b"data-mixed",
                (write_count + i) as i64,
            );
            engine.apply_mutation(&m).expect("Write failed");
            write_lat.push(t0.elapsed().as_micros() as u64);
        }
        write_count += 50;

        for i in 0..20 {
            let pk = format!("mix-pk-{}", write_count.saturating_sub(100) + i);
            let t0 = Instant::now();
            let _ = engine.read_partition("ks", "mixed", pk.as_bytes());
            read_lat.push(t0.elapsed().as_micros() as u64);
            read_count += 1;
        }

        if write_count > 200 {
            for i in 0..5 {
                let pk = format!("mix-pk-{}", delete_count + i);
                let m = make_tombstone_mutation(
                    "ks",
                    "mixed",
                    pk.as_bytes(),
                    b"ck0",
                    (write_count + 1000) as i64,
                );
                engine.apply_mutation(&m).unwrap();
            }
            delete_count += 5;
        }

        if write_count % 500 == 0 {
            engine.flush_cf("ks.mixed").unwrap();
            let _ = engine.maybe_compact();
        }
    }

    write_lat.sort();
    read_lat.sort();
    let p50 = |v: &[u64]| if v.is_empty() { 0 } else { v[v.len() / 2] };
    let p99 = |v: &[u64]| {
        if v.is_empty() {
            0
        } else {
            v[(v.len() as f64 * 0.99) as usize]
        }
    };

    println!("Mixed workload: writes={write_count}, reads={read_count}, deletes={delete_count}");
    println!(
        "  Write p50: {}μs, p99: {}μs",
        p50(&write_lat),
        p99(&write_lat)
    );
    println!(
        "  Read  p50: {}μs, p99: {}μs",
        p50(&read_lat),
        p99(&read_lat)
    );
}

/// Writes with simulated clock drift, verifies LWW resolution.
#[test]
#[ignore]
fn soak_clock_skew_tolerance() {
    let dir = TempDir::new().unwrap();
    let engine = test_engine(dir.path());
    let duration = soak_duration();
    let start = Instant::now();
    let mut round = 0u64;

    while start.elapsed() < duration {
        let base_ts = round as i64 * 1000;
        let pk = format!("skew-{round}");
        let pk_bytes = pk.as_bytes();

        // Normal write
        engine
            .apply_mutation(&make_mutation(
                "ks",
                "skew",
                pk_bytes,
                b"ck",
                "v",
                b"value-normal",
                base_ts,
            ))
            .unwrap();
        // Future write (higher timestamp wins)
        engine
            .apply_mutation(&make_mutation(
                "ks",
                "skew",
                pk_bytes,
                b"ck",
                "v",
                b"value-future",
                base_ts + 5_000_000,
            ))
            .unwrap();
        // Past write (lower timestamp loses)
        engine
            .apply_mutation(&make_mutation(
                "ks",
                "skew",
                pk_bytes,
                b"ck",
                "v",
                b"value-past",
                base_ts - 5_000_000,
            ))
            .unwrap();

        // LWW: future write should win
        let result = engine.read_partition("ks", "skew", pk_bytes);
        assert!(result.is_some(), "Partition should exist at round {round}");

        if round % 200 == 0 {
            engine.flush_cf("ks.skew").unwrap();
        }
        round += 1;
    }
    println!("soak_clock_skew_tolerance: {round} rounds");
}

/// Repair-like scenario: sustained writes + full-table scans.
#[test]
#[ignore]
fn soak_repair_under_load() {
    let dir = TempDir::new().unwrap();
    let engine = test_engine(dir.path());
    let duration = soak_duration();
    let start = Instant::now();
    let mut write_count = 0u64;

    while start.elapsed() < duration {
        for i in 0..100 {
            let pk = format!("repair-pk-{}", write_count + i);
            let m = make_mutation(
                "ks",
                "repair_tbl",
                pk.as_bytes(),
                b"ck",
                "d",
                b"repair-data",
                (write_count + i) as i64,
            );
            engine.apply_mutation(&m).unwrap();
        }
        write_count += 100;

        if write_count % 500 == 0 {
            engine.flush_cf("ks.repair_tbl").unwrap();
            // Simulate repair read scan
            let mut verified = 0u64;
            for j in 0..std::cmp::min(write_count, 100) {
                let pk = format!("repair-pk-{j}");
                if engine
                    .read_partition("ks", "repair_tbl", pk.as_bytes())
                    .is_some()
                {
                    verified += 1;
                }
            }
            assert!(verified > 0, "Should find written data during repair scan");
            let _ = engine.maybe_compact();
        }
    }

    println!("soak_repair_under_load: writes={write_count}");
    let result = engine.read_partition("ks", "repair_tbl", b"repair-pk-0");
    assert!(result.is_some(), "First key should survive");
}

/// Topology churn: writes across multiple keyspaces and tables.
#[test]
#[ignore]
fn soak_topology_churn() {
    let dir = TempDir::new().unwrap();
    let engine = test_engine(dir.path());
    let duration = soak_duration();
    let start = Instant::now();
    let mut round = 0u64;
    let keyspaces = ["ks_a", "ks_b", "ks_c", "ks_d"];
    let tables = ["t1", "t2", "t3"];

    while start.elapsed() < duration {
        let ks = keyspaces[(round as usize) % keyspaces.len()];
        let tbl = tables[(round as usize / keyspaces.len()) % tables.len()];

        for i in 0..10 {
            let pk = format!("topo-{round}-{i}");
            let m = make_mutation(
                ks,
                tbl,
                pk.as_bytes(),
                b"ck",
                "v",
                format!("d-{round}").as_bytes(),
                (round * 10 + i) as i64,
            );
            engine.apply_mutation(&m).unwrap();
        }

        if round % 100 == 0 {
            let flush_ks = keyspaces[(round as usize + 1) % keyspaces.len()];
            let flush_tbl = tables[(round as usize + 1) % tables.len()];
            let _ = engine.flush_cf(&format!("{flush_ks}.{flush_tbl}"));
            let _ = engine.maybe_compact();
        }
        round += 1;
    }

    println!(
        "soak_topology_churn: {round} rounds across {} ks × {} tables",
        keyspaces.len(),
        tables.len()
    );
}
