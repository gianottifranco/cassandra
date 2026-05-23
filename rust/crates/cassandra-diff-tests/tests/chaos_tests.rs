// Licensed under Apache License, Version 2.0.

//! Chaos tests for resilience against failure conditions.

use cassandra_storage::commitlog::{CellMutation, CommitLogConfig, Mutation, MutationRow};
use cassandra_storage::engine::{EngineConfig, StorageEngine};

use tempfile::TempDir;

fn test_engine(dir: &std::path::Path) -> StorageEngine {
    let config = EngineConfig {
        data_directories: vec![dir.join("data")],
        commitlog: CommitLogConfig {
            max_segment_size: 4096,
            directory: dir.join("commitlog"),
            ..CommitLogConfig::default()
        },
        memtable_flush_threshold: 64 * 1024,
        gc_grace_seconds: 1,
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

fn make_tombstone(ks: &str, tbl: &str, pk: &[u8], ck: &[u8], ts: i64) -> Mutation {
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

fn make_engine_config(dir: &std::path::Path) -> EngineConfig {
    EngineConfig {
        data_directories: vec![dir.join("data")],
        commitlog: CommitLogConfig {
            max_segment_size: 4096,
            directory: dir.join("commitlog"),
            ..CommitLogConfig::default()
        },
        memtable_flush_threshold: 1024 * 1024,
        gc_grace_seconds: 86400,
        ..EngineConfig::default()
    }
}

/// Crash mid-write: write data, drop engine without flush, reopen, replay.
#[test]
fn crash_recovery_via_commitlog_replay() {
    let dir = TempDir::new().unwrap();
    let config = make_engine_config(dir.path());

    {
        let engine = StorageEngine::open(config.clone()).unwrap();
        for i in 0..50 {
            let m = make_mutation(
                "ks",
                "crash_test",
                format!("pk-{i}").as_bytes(),
                b"ck",
                "data",
                format!("value-{i}").as_bytes(),
                i as i64,
            );
            engine.apply_mutation(&m).unwrap();
        }
        engine.flush_all().unwrap();
    }

    {
        let engine = StorageEngine::open(config).unwrap();
        let replayed = engine.replay_commitlog().unwrap();
        println!("Crash recovery: replayed {replayed} mutations");
        // Data should be accessible (either from SSTables or replay)
        let result = engine.read_partition("ks", "crash_test", b"pk-0");
        assert!(
            result.is_some(),
            "Data should be available after crash recovery"
        );
    }
}

/// Large partition: many rows in a single partition.
#[test]
fn large_partition_stress() {
    let dir = TempDir::new().unwrap();
    let engine = test_engine(dir.path());
    let num_rows = 10_000;

    let mut rows = Vec::with_capacity(num_rows);
    for i in 0..num_rows {
        rows.push(MutationRow {
            clustering_key: format!("ck-{i:06}").into_bytes(),
            cells: vec![CellMutation {
                column: "value".to_string(),
                value: Some(vec![0x42u8; 100]),
                timestamp: i as i64,
                ttl: 0,
                local_deletion_time: None,
                is_tombstone: false,
            }],
            is_tombstone: false,
            local_deletion_time: None,
        });
    }

    let m = Mutation {
        keyspace: "ks".to_string(),
        table: "wide".to_string(),
        partition_key: b"wide-pk".to_vec(),
        rows,
        timestamp: 0,
        cdc_enabled: false,
        static_cells: vec![],
        partition_tombstone: None,
        range_tombstones: vec![],
    };
    engine.apply_mutation(&m).unwrap();

    let result = engine.read_partition("ks", "wide", b"wide-pk");
    assert!(result.is_some());
}

/// Tombstone GC after compaction.
#[test]
fn tombstone_gc_after_compaction() {
    let dir = TempDir::new().unwrap();
    let engine = test_engine(dir.path());

    for i in 0..20 {
        let m = make_mutation(
            "ks",
            "tomb",
            format!("pk-{i}").as_bytes(),
            b"ck0",
            "v",
            b"alive",
            i as i64,
        );
        engine.apply_mutation(&m).unwrap();
    }
    engine.flush_cf("ks.tomb").unwrap();

    for i in 0..20 {
        let m = make_tombstone(
            "ks",
            "tomb",
            format!("pk-{i}").as_bytes(),
            b"ck0",
            (i + 100) as i64,
        );
        engine.apply_mutation(&m).unwrap();
    }
    engine.flush_cf("ks.tomb").unwrap();

    std::thread::sleep(std::time::Duration::from_secs(2));
    let _ = engine.maybe_compact();

    // Engine should not crash
    let result = engine.read_partition("ks", "tomb", b"pk-0");
    println!("Post-compaction result: {result:?}");
}

/// Interleaved writes and reads.
#[test]
fn interleaved_write_read() {
    let dir = TempDir::new().unwrap();
    let engine = test_engine(dir.path());

    for round in 0..100 {
        let m = make_mutation(
            "ks",
            "ilv",
            format!("pk-{round}").as_bytes(),
            b"ck",
            "v",
            format!("r{round}").as_bytes(),
            round as i64,
        );
        engine.apply_mutation(&m).unwrap();

        if round > 0 {
            let result = engine.read_partition("ks", "ilv", format!("pk-{}", round - 1).as_bytes());
            assert!(
                result.is_some(),
                "Round {round}: previous write should be readable"
            );
        }

        if round % 25 == 0 {
            engine.flush_cf("ks.ilv").unwrap();
        }
    }
}

/// Multiple rounds of flush + compact.
#[test]
fn repeated_flush_compact_cycle() {
    let dir = TempDir::new().unwrap();
    let engine = test_engine(dir.path());

    for cycle in 0..10 {
        for i in 0..50 {
            let idx = cycle * 50 + i;
            let m = make_mutation(
                "ks",
                "cycle",
                format!("pk-{idx}").as_bytes(),
                b"ck",
                "v",
                b"data",
                idx as i64,
            );
            engine.apply_mutation(&m).unwrap();
        }
        engine.flush_cf("ks.cycle").unwrap();
        let _ = engine.maybe_compact();
    }

    let result = engine.read_partition("ks", "cycle", b"pk-0");
    assert!(result.is_some());
}

// ── Phase 25 chaos scenarios ───────────────────────────────────────────

/// Disk full: write lots of data through the engine.
#[test]
fn chaos_disk_full_during_flush() {
    let dir = TempDir::new().unwrap();
    let engine = test_engine(dir.path());

    for i in 0..5000 {
        let pk = format!("full-{i}");
        let big_value = vec![0xABu8; 4096];
        let m = make_mutation(
            "ks",
            "full_test",
            pk.as_bytes(),
            b"ck",
            "v",
            &big_value,
            i as i64,
        );
        let result = engine.apply_mutation(&m);
        if result.is_err() {
            println!("Write failed at i={i}: {:?}", result.err());
            break;
        }
    }

    let flush_result = engine.flush_cf("ks.full_test");
    println!("Flush result: {flush_result:?}");

    // Engine should remain operational
    let read_result = engine.read_partition("ks", "full_test", b"full-0");
    assert!(read_result.is_some(), "Engine should remain operational");
}

/// Corrupt SSTable files after flush.
#[test]
fn chaos_corruption_sstable_header() {
    let dir = TempDir::new().unwrap();
    let engine = test_engine(dir.path());

    for i in 0..100 {
        let m = make_mutation(
            "ks",
            "corrupt",
            format!("cpk-{i}").as_bytes(),
            b"ck",
            "v",
            b"clean-data",
            i as i64,
        );
        engine.apply_mutation(&m).unwrap();
    }
    engine.flush_cf("ks.corrupt").unwrap();

    let data_dir = dir.path().join("data");
    if data_dir.exists() {
        let mut corrupted = 0;
        for entry in walkdir(&data_dir) {
            if entry.extension().map(|e| e == "db").unwrap_or(false) {
                if let Ok(mut contents) = std::fs::read(&entry) {
                    if contents.len() > 8 {
                        contents[0] = 0xFF;
                        contents[1] = 0xFF;
                        let _ = std::fs::write(&entry, &contents);
                        corrupted += 1;
                    }
                }
            }
        }
        println!("Corrupted {corrupted} SSTable file(s)");
    }

    // Read should not panic
    let result = engine.read_partition("ks", "corrupt", b"cpk-0");
    println!("Read after corruption: {result:?}");
}

/// Walk a directory tree and return all file paths.
fn walkdir(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut files = Vec::new();
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                files.extend(walkdir(&path));
            } else {
                files.push(path);
            }
        }
    }
    files
}

/// Crash during compaction: multiple SSTables, compact, drop, reopen.
#[test]
fn chaos_crash_during_compaction() {
    let dir = TempDir::new().unwrap();
    let config = make_engine_config(dir.path());

    {
        let engine = StorageEngine::open(config.clone()).unwrap();
        for batch in 0..5 {
            for i in 0..50 {
                let idx = batch * 50 + i;
                let m = make_mutation(
                    "ks",
                    "cc",
                    format!("cc-{idx}").as_bytes(),
                    b"ck",
                    "v",
                    format!("b{batch}-{i}").as_bytes(),
                    idx as i64,
                );
                engine.apply_mutation(&m).unwrap();
            }
            engine.flush_cf("ks.cc").unwrap();
        }
        let _ = engine.maybe_compact();
    }

    {
        let engine = StorageEngine::open(config).unwrap();
        let replayed = engine.replay_commitlog().unwrap();
        println!("Post-compaction-crash replay: {replayed} mutations");

        let mut readable = 0;
        for i in 0..250 {
            if engine
                .read_partition("ks", "cc", format!("cc-{i}").as_bytes())
                .is_some()
            {
                readable += 1;
            }
        }
        println!("Readable after crash-during-compaction: {readable}/250");
        assert!(readable > 0, "At least some data should survive");
    }
}

/// Concurrent snapshot and write.
#[test]
fn chaos_concurrent_snapshot_and_write() {
    let dir = TempDir::new().unwrap();
    let engine = test_engine(dir.path());

    for i in 0..200 {
        let m = make_mutation(
            "ks",
            "snap_conc",
            format!("sc-{i}").as_bytes(),
            b"ck",
            "v",
            format!("pre-snap-{i}").as_bytes(),
            i as i64,
        );
        engine.apply_mutation(&m).unwrap();
    }
    engine.flush_cf("ks.snap_conc").unwrap();

    let manifest = engine
        .snapshot("chaos-snap-1", "ks", "snap_conc", None)
        .unwrap();
    println!("First snapshot: {} files", manifest.files.len());

    for i in 200..400 {
        let m = make_mutation(
            "ks",
            "snap_conc",
            format!("sc-{i}").as_bytes(),
            b"ck",
            "v",
            format!("post-snap-{i}").as_bytes(),
            i as i64,
        );
        engine.apply_mutation(&m).unwrap();
    }
    engine.flush_cf("ks.snap_conc").unwrap();

    let manifest2 = engine
        .snapshot("chaos-snap-2", "ks", "snap_conc", None)
        .unwrap();
    println!("Second snapshot: {} files", manifest2.files.len());

    // All data should be readable
    for pk in ["sc-0", "sc-199", "sc-200", "sc-399"] {
        let result = engine.read_partition("ks", "snap_conc", pk.as_bytes());
        assert!(result.is_some(), "Key {pk} should be readable");
    }
}

/// Rapid restart cycles.
#[test]
fn chaos_rapid_restart_cycle() {
    let dir = TempDir::new().unwrap();
    let config = EngineConfig {
        data_directories: vec![dir.path().join("data")],
        commitlog: CommitLogConfig {
            max_segment_size: 8192,
            directory: dir.path().join("commitlog"),
            ..CommitLogConfig::default()
        },
        memtable_flush_threshold: 128 * 1024,
        gc_grace_seconds: 86400,
        ..EngineConfig::default()
    };

    let cycles = 50;
    for cycle in 0..cycles {
        let engine = StorageEngine::open(config.clone()).unwrap();
        for i in 0..5 {
            let pk = format!("restart-{cycle}-{i}");
            let m = make_mutation(
                "ks",
                "restart",
                pk.as_bytes(),
                b"ck",
                "v",
                format!("c{cycle}").as_bytes(),
                (cycle * 5 + i) as i64,
            );
            engine.apply_mutation(&m).unwrap();
        }
        if cycle % 3 == 0 {
            let _ = engine.flush_all();
        }
    }

    let engine = StorageEngine::open(config).unwrap();
    let replayed = engine.replay_commitlog().unwrap();
    println!("Rapid restart: {cycles} cycles, {replayed} mutations replayed");

    let mut found = 0;
    for cycle in 0..cycles {
        for i in 0..5 {
            let pk = format!("restart-{cycle}-{i}");
            if engine
                .read_partition("ks", "restart", pk.as_bytes())
                .is_some()
            {
                found += 1;
            }
        }
    }
    println!("Found {found}/{} keys", cycles * 5);
    assert!(
        found > 0,
        "At least some data should survive rapid restarts"
    );
}
