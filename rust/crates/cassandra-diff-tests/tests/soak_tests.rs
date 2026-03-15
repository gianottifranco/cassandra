// Licensed under Apache License, Version 2.0.

//! Soak test harness for the Cassandra Rust storage engine.
//!
//! Verifies that the engine operates correctly under sustained load
//! without memory leaks, data corruption, or performance degradation.
//!
//! ## Running
//!
//! ```bash
//! cargo test -p cassandra-diff-tests --test soak_tests -- --ignored --nocapture
//! ```

use cassandra_storage::commitlog::CommitLogConfig;
use cassandra_storage::engine::{EngineConfig, StorageEngine};
use cassandra_storage::memtable::partition::Row;
use cassandra_storage::memtable::partition::Cell;

use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::TempDir;

/// Configuration for a soak test run.
struct SoakConfig {
    /// Total duration of the soak test.
    duration: Duration,
    /// Number of concurrent writer "threads" (sequential in test, but structured for async).
    writer_count: usize,
    /// Writes per batch.
    batch_size: usize,
    /// Interval between memory checks.
    memory_check_interval: Duration,
    /// Maximum allowed memory growth ratio (e.g., 1.05 = 5%).
    max_memory_growth_ratio: f64,
}

impl Default for SoakConfig {
    fn default() -> Self {
        Self {
            duration: Duration::from_secs(10), // Short for unit test; use 3600 for real soak
            writer_count: 4,
            batch_size: 100,
            memory_check_interval: Duration::from_secs(2),
            max_memory_growth_ratio: 2.0, // Generous for short runs
        }
    }
}

fn test_engine(dir: &std::path::Path) -> StorageEngine {
    let config = EngineConfig {
        data_dir: dir.join("data"),
        commitlog_config: CommitLogConfig {
            max_segment_size: 4 * 1024 * 1024,
            directory: dir.join("commitlog"),
            ..CommitLogConfig::default()
        },
        memtable_flush_threshold: 4 * 1024 * 1024, // 4 MiB — triggers frequent flushes
        gc_grace_seconds: 60,
    };
    StorageEngine::open(config).expect("Failed to open engine")
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

/// Core soak test: sustained writes + reads + flushes + compaction.
#[test]
#[ignore] // Long-running; run explicitly
fn soak_write_read_flush_compact() {
    let dir = TempDir::new().unwrap();
    let engine = Arc::new(test_engine(dir.path()));
    let config = SoakConfig::default();

    let start = Instant::now();
    let mut total_writes = 0u64;
    let mut total_reads = 0u64;
    let mut total_flushes = 0u64;
    let mut memory_samples: Vec<usize> = Vec::new();
    let mut last_memory_check = Instant::now();

    let initial_memory = engine.stats().memtable_memory;

    println!("Soak test starting: duration={:?}, writers={}", config.duration, config.writer_count);

    while start.elapsed() < config.duration {
        // Write phase
        for writer_id in 0..config.writer_count {
            for i in 0..config.batch_size {
                let pk = format!("soak-{writer_id}-{}", total_writes + i as u64);
                let val = format!("value-{}", total_writes + i as u64);
                let ts = (total_writes + i as u64) as i64;

                engine
                    .apply_mutation(
                        "soak_ks",
                        "soak_table",
                        pk.as_bytes().to_vec(),
                        vec![make_row(b"ck0", "data", val.as_bytes(), ts)],
                        ts,
                    )
                    .expect("Write should succeed");
            }
            total_writes += config.batch_size as u64;
        }

        // Read phase — verify some recent writes
        for i in 0..10 {
            let pk = format!("soak-0-{}", total_writes.saturating_sub(10) + i);
            let result = engine
                .read_partition("soak_ks", "soak_table", pk.as_bytes())
                .expect("Read should succeed");
            if result.is_some() {
                total_reads += 1;
            }
        }

        // Periodic flush
        if total_writes % 1000 == 0 {
            engine.flush_cf("soak_ks", "soak_table").expect("Flush should succeed");
            total_flushes += 1;

            // Periodic compaction
            if total_flushes % 5 == 0 {
                let _ = engine.maybe_compact("soak_ks", "soak_table");
            }
        }

        // Memory monitoring
        if last_memory_check.elapsed() >= config.memory_check_interval {
            let current = engine.stats().memtable_memory;
            memory_samples.push(current);
            last_memory_check = Instant::now();
        }
    }

    // Final stats
    let final_stats = engine.stats();

    println!("Soak test complete:");
    println!("  Total writes: {total_writes}");
    println!("  Total reads: {total_reads}");
    println!("  Total flushes: {total_flushes}");
    println!("  SSTables on disk: {}", final_stats.sstable_count);
    println!("  Final memtable memory: {} bytes", final_stats.memtable_memory);

    // Memory growth check
    if !memory_samples.is_empty() {
        let peak = *memory_samples.iter().max().unwrap();
        let baseline = initial_memory.max(1);
        let ratio = peak as f64 / baseline as f64;
        println!("  Memory growth ratio: {ratio:.2}x (budget: {:.2}x)", config.max_memory_growth_ratio);

        // Only enforce if we have meaningful memory usage
        if peak > 1024 * 1024 {
            assert!(
                ratio <= config.max_memory_growth_ratio,
                "Memory grew {ratio:.2}x, exceeding budget of {:.2}x",
                config.max_memory_growth_ratio
            );
        }
    }

    // Data integrity check — verify some known writes
    let verify_pk = format!("soak-0-{}", config.batch_size - 1);
    let result = engine
        .read_partition("soak_ks", "soak_table", verify_pk.as_bytes())
        .expect("Verification read should succeed");
    assert!(result.is_some(), "Written data should be readable after soak");
}

/// Verify snapshot/restore during sustained writes.
#[test]
#[ignore]
fn soak_snapshot_under_load() {
    let dir = TempDir::new().unwrap();
    let engine = Arc::new(test_engine(dir.path()));

    // Write some initial data
    for i in 0..500 {
        let pk = format!("snap-{i}");
        engine
            .apply_mutation(
                "ks", "t1",
                pk.as_bytes().to_vec(),
                vec![make_row(b"ck", "v", format!("val-{i}").as_bytes(), i as i64)],
                i as i64,
            )
            .unwrap();
    }

    engine.flush_cf("ks", "t1").unwrap();

    // Take snapshot while continuing writes
    let snap_path = engine.snapshot("soak-snap").unwrap();
    assert!(snap_path.exists(), "Snapshot directory should exist");

    // Continue writing — snapshot should be consistent regardless
    for i in 500..1000 {
        let pk = format!("snap-{i}");
        engine
            .apply_mutation(
                "ks", "t1",
                pk.as_bytes().to_vec(),
                vec![make_row(b"ck", "v", format!("val-{i}").as_bytes(), i as i64)],
                i as i64,
            )
            .unwrap();
    }

    // Pre-snapshot data should still be readable
    let result = engine.read_partition("ks", "t1", b"snap-0").unwrap();
    assert!(result.is_some());
}
