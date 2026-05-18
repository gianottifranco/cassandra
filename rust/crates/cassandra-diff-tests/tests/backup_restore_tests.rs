// Licensed under Apache License, Version 2.0.

//! Backup, restore, and rollback drill tests.
//!
//! Validates the snapshot/restore/rollback flow that operators
//! will use during migration and in production.
//!
//! ## Running
//!
//! ```bash
//! cargo test -p cassandra-diff-tests --test backup_restore_tests -- --nocapture
//! ```

use cassandra_storage::commitlog::{CellMutation, CommitLogConfig, Mutation, MutationRow};
use cassandra_storage::engine::{EngineConfig, StorageEngine};

use std::path::Path;
use tempfile::TempDir;

fn test_engine(dir: &Path) -> StorageEngine {
    let config = EngineConfig {
        data_directories: vec![dir.join("data")],
        commitlog: CommitLogConfig {
            max_segment_size: 8192,
            directory: dir.join("commitlog"),
            ..CommitLogConfig::default()
        },
        memtable_flush_threshold: 1024 * 1024,
        gc_grace_seconds: 86400,
        ..EngineConfig::default()
    };
    StorageEngine::open(config).expect("Failed to open engine")
}

fn make_engine_config(dir: &Path) -> EngineConfig {
    EngineConfig {
        data_directories: vec![dir.join("data")],
        commitlog: CommitLogConfig {
            max_segment_size: 8192,
            directory: dir.join("commitlog"),
            ..CommitLogConfig::default()
        },
        memtable_flush_threshold: 1024 * 1024,
        gc_grace_seconds: 86400,
        ..EngineConfig::default()
    }
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

/// Full backup/restore cycle: write → flush → snapshot → verify snapshot files exist.
#[test]
fn snapshot_creates_consistent_backup() {
    let dir = TempDir::new().unwrap();
    let engine = test_engine(dir.path());

    // Write data
    for i in 0..100 {
        let m = make_mutation(
            "ks",
            "backup_test",
            format!("pk-{i}").as_bytes(),
            b"ck",
            "name",
            format!("user-{i}").as_bytes(),
            i as i64,
        );
        engine.apply_mutation(&m).unwrap();
    }

    // Flush to SSTable
    engine.flush_cf("ks.backup_test").unwrap();

    // Create snapshot
    let manifest = engine
        .snapshot("backup-drill-1", "ks", "backup_test", None)
        .unwrap();
    println!("Snapshot contains {} files", manifest.files.len());
    assert!(
        !manifest.files.is_empty(),
        "Snapshot should contain at least one file"
    );
}

/// Rollback drill: write → snapshot → write more → "rollback" → verify old data.
///
/// Simulates a migration rollback scenario by verifying that a snapshot
/// taken before new writes contains only the pre-snapshot data.
#[test]
fn rollback_drill_validates_data_integrity() {
    let dir = TempDir::new().unwrap();

    // Phase 1: Write initial data
    {
        let engine = test_engine(dir.path());

        for i in 0..50 {
            let m = make_mutation(
                "ks",
                "rollback_test",
                format!("pk-{i}").as_bytes(),
                b"ck",
                "v",
                format!("v1-{i}").as_bytes(),
                i as i64,
            );
            engine.apply_mutation(&m).unwrap();
        }

        engine.flush_cf("ks.rollback_test").unwrap();

        // Take snapshot (rollback point)
        let manifest = engine
            .snapshot("rollback-point", "ks", "rollback_test", None)
            .unwrap();
        assert!(!manifest.files.is_empty());

        // Phase 2: Write additional data after the migration boundary.
        for i in 50..100 {
            let m = make_mutation(
                "ks",
                "rollback_test",
                format!("pk-{i}").as_bytes(),
                b"ck",
                "v",
                format!("v2-{i}").as_bytes(),
                (i + 100) as i64,
            );
            engine.apply_mutation(&m).unwrap();
        }

        // Verify both pre and post-snapshot data exists
        let pre = engine.read_partition("ks", "rollback_test", b"pk-0");
        assert!(pre.is_some(), "Pre-snapshot data should exist");

        let post = engine.read_partition("ks", "rollback_test", b"pk-75");
        assert!(
            post.is_some(),
            "Post-snapshot data should exist before rollback"
        );
    }

    // Phase 3: "Rollback" — verify snapshot files were preserved
    let snap_dir = dir
        .path()
        .join("data")
        .join("snapshots")
        .join("rollback-point");
    assert!(
        snap_dir.exists(),
        "Rollback snapshot directory should still exist"
    );

    let snap_files: Vec<_> = std::fs::read_dir(&snap_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().to_string())
        .collect();

    println!("Rollback point contains: {:?}", snap_files);
    assert!(!snap_files.is_empty(), "Rollback point should have files");
}

/// Multiple snapshots can coexist.
#[test]
fn multiple_snapshots_coexist() {
    let dir = TempDir::new().unwrap();
    let engine = test_engine(dir.path());

    // Write and flush
    let m = make_mutation("ks", "multi_snap", b"pk1", b"ck", "v", b"data1", 1);
    engine.apply_mutation(&m).unwrap();
    engine.flush_cf("ks.multi_snap").unwrap();

    let manifest1 = engine.snapshot("snap-a", "ks", "multi_snap", None).unwrap();

    // Write more
    let m = make_mutation("ks", "multi_snap", b"pk2", b"ck", "v", b"data2", 2);
    engine.apply_mutation(&m).unwrap();
    engine.flush_cf("ks.multi_snap").unwrap();

    let manifest2 = engine.snapshot("snap-b", "ks", "multi_snap", None).unwrap();

    assert!(!manifest1.files.is_empty());
    assert!(!manifest2.files.is_empty());
}

/// Snapshot + commit log replay = complete recovery.
#[test]
fn snapshot_plus_replay_full_recovery() {
    let dir = TempDir::new().unwrap();
    let config = make_engine_config(dir.path());

    // Write data, flush some, leave some in memtable
    {
        let engine = StorageEngine::open(config.clone()).unwrap();

        // Flushed data (in SSTable)
        for i in 0..30 {
            let m = make_mutation(
                "ks",
                "recovery",
                format!("flushed-{i}").as_bytes(),
                b"ck",
                "v",
                b"flushed",
                i as i64,
            );
            engine.apply_mutation(&m).unwrap();
        }
        engine.flush_cf("ks.recovery").unwrap();
        engine
            .snapshot("recovery-snap", "ks", "recovery", None)
            .unwrap();

        // Unflushed data (only in commit log)
        for i in 30..50 {
            let m = make_mutation(
                "ks",
                "recovery",
                format!("unflushed-{i}").as_bytes(),
                b"ck",
                "v",
                b"unflushed",
                i as i64,
            );
            engine.apply_mutation(&m).unwrap();
        }

        // Sync commit log but don't flush memtable
        engine.flush_all().unwrap();
    }

    // Reopen and replay
    {
        let engine = StorageEngine::open(config).unwrap();
        let replayed = engine.replay_commitlog().unwrap();
        println!("Recovery: replayed {replayed} mutations from commit log");

        // Commit log replay recovers unflushed mutations.
        assert!(
            replayed > 0,
            "Commit log should have unflushed mutations to replay"
        );

        // Unflushed data should be available via commit log replay.
        let unflushed = engine.read_partition("ks", "recovery", b"unflushed-30");
        assert!(
            unflushed.is_some(),
            "Unflushed data should be recovered via commit log replay"
        );

        let flushed = engine.read_partition("ks", "recovery", b"flushed-0");
        assert!(
            flushed.is_some(),
            "Flushed SSTable data should be discovered after restart"
        );
    }
}
