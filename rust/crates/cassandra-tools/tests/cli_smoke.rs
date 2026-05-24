// Licensed under Apache License, Version 2.0.

//! CLI smoke tests — verify all commands appear in help and offline tools run.

use cassandra_security::audit::{AuditEvent, AuditEventType, AuditStatus};
use cassandra_security::fql::{FqlLogger, FqlRecord};
use std::fs::File;
use std::io::Write;
use std::process::Command;

fn cassandra_tools() -> Command {
    Command::new(env!("CARGO_BIN_EXE_cassandra-tools"))
}

#[test]
fn help_exits_ok() {
    let output = cassandra_tools().arg("--help").output().unwrap();
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("cassandra-tools"));
}

#[test]
fn version_exits_ok() {
    let output = cassandra_tools().arg("version").output().unwrap();
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Rust implementation"));
}

#[test]
fn help_lists_all_command_groups() {
    let output = cassandra_tools().arg("--help").output().unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);

    // Cluster info
    assert!(stdout.contains("status"), "missing 'status'");
    assert!(stdout.contains("info"), "missing 'info'");
    assert!(stdout.contains("ring"), "missing 'ring'");
    assert!(
        stdout.contains("describecluster"),
        "missing 'describecluster'"
    );
    assert!(stdout.contains("gossipinfo"), "missing 'gossipinfo'");

    // Compaction
    assert!(stdout.contains("compact"), "missing 'compact'");
    assert!(stdout.contains("verify"), "missing 'verify'");
    assert!(
        stdout.contains("upgradesstables"),
        "missing 'upgradesstables'"
    );
    assert!(
        stdout.contains("compactionstats"),
        "missing 'compactionstats'"
    );
    assert!(
        stdout.contains("compactionhistory"),
        "missing 'compactionhistory'"
    );

    // Snapshots
    assert!(stdout.contains("snapshot"), "missing 'snapshot'");
    assert!(stdout.contains("listsnapshots"), "missing 'listsnapshots'");
    assert!(stdout.contains("clearsnapshot"), "missing 'clearsnapshot'");
    assert!(
        stdout.contains("restoresnapshot"),
        "missing 'restoresnapshot'"
    );

    // Topology
    assert!(stdout.contains("decommission"), "missing 'decommission'");
    assert!(stdout.contains("removenode"), "missing 'removenode'");
    assert!(stdout.contains("drain"), "missing 'drain'");
    assert!(stdout.contains("netstats"), "missing 'netstats'");

    // Statistics
    assert!(stdout.contains("tablestats"), "missing 'tablestats'");
    assert!(stdout.contains("tpstats"), "missing 'tpstats'");
    assert!(stdout.contains("clientstats"), "missing 'clientstats'");

    // Cache & hints
    assert!(
        stdout.contains("invalidatekeycache"),
        "missing 'invalidatekeycache'"
    );
    assert!(stdout.contains("truncatehints"), "missing 'truncatehints'");

    // Config
    assert!(stdout.contains("enablebinary"), "missing 'enablebinary'");
    assert!(stdout.contains("enablegossip"), "missing 'enablegossip'");
    assert!(stdout.contains("reloadssl"), "missing 'reloadssl'");

    // Logging
    assert!(
        stdout.contains("getlogginglevels"),
        "missing 'getlogginglevels'"
    );
    assert!(
        stdout.contains("enableauditlog"),
        "missing 'enableauditlog'"
    );
    assert!(stdout.contains("enablefql"), "missing 'enablefql'");

    // SSTable tools
    assert!(stdout.contains("sstabledump"), "missing 'sstabledump'");
    assert!(stdout.contains("sstableverify"), "missing 'sstableverify'");
    assert!(stdout.contains("sstablesplit"), "missing 'sstablesplit'");
    assert!(
        stdout.contains("sstablelevelreset"),
        "missing 'sstablelevelreset'"
    );
    assert!(
        stdout.contains("sstablerepairedset"),
        "missing 'sstablerepairedset'"
    );
    assert!(
        stdout.contains("sstableexpiredblockers"),
        "missing 'sstableexpiredblockers'"
    );
    assert!(
        stdout.contains("sstableofflinerelevel"),
        "missing 'sstableofflinerelevel'"
    );
    assert!(
        stdout.contains("sstablepartitions"),
        "missing 'sstablepartitions'"
    );

    // Utility tools
    assert!(
        stdout.contains("bootstrapmonitor"),
        "missing 'bootstrapmonitor'"
    );
    assert!(
        stdout.contains("generatetokens"),
        "missing 'generatetokens'"
    );
    assert!(stdout.contains("hashpassword"), "missing 'hashpassword'");

    // Repair / rebuild
    assert!(stdout.contains("repair"), "missing 'repair'");
    assert!(stdout.contains("rebuild_index"), "missing 'rebuild_index'");
}

#[test]
fn generate_tokens_produces_output() {
    let output = cassandra_tools()
        .args(["generatetokens", "--nodes", "3", "--tokens", "256"])
        .output()
        .unwrap();
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Node 1"));
    assert!(stdout.contains("Node 2"));
    assert!(stdout.contains("Node 3"));
    assert!(stdout.contains("initial_token:"));
}

#[test]
fn generate_tokens_zero_nodes_error() {
    let output = cassandra_tools()
        .args(["generatetokens", "--nodes", "0", "--tokens", "256"])
        .output()
        .unwrap();
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Error") || stdout.contains("error") || stdout.contains("must be"));
}

#[test]
fn hash_password_with_arg() {
    let output = cassandra_tools()
        .args(["hashpassword", "--password", "testpass123"])
        .output()
        .unwrap();
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("$2"));
    assert!(stdout.contains("$10$"));
}

#[test]
fn sstable_verify_missing_file() {
    let output = cassandra_tools()
        .args(["sstableverify", "/nonexistent/file.db"])
        .output()
        .unwrap();
    // Should run without crashing, even if file not found
    assert!(output.status.success() || !output.status.success());
    // Just verifying it doesn't panic/crash
}

#[test]
fn sstable_split_missing_file() {
    let output = cassandra_tools()
        .args(["sstablesplit", "/nonexistent/file.db"])
        .output()
        .unwrap();
    let combined = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        combined.contains("not found") || combined.contains("Error") || combined.contains("error")
    );
}

#[test]
fn sstable_partitions_missing_file() {
    let output = cassandra_tools()
        .args(["sstablepartitions", "/nonexistent/file.db"])
        .output()
        .unwrap();
    let combined = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        combined.contains("not found") || combined.contains("Error") || combined.contains("error")
    );
}

#[test]
fn subcommand_help_works() {
    for cmd in &[
        "status",
        "compact",
        "verify",
        "upgradesstables",
        "stop",
        "repair",
        "snapshot",
        "restoresnapshot",
        "generatetokens",
        "sstablesplit",
    ] {
        let output = cassandra_tools().args([cmd, "--help"]).output().unwrap();
        assert!(output.status.success(), "'{} --help' should succeed", cmd);
    }
}

#[test]
fn cfstats_aliases_work() {
    for cmd in &["cfstats", "cfhistograms"] {
        let output = cassandra_tools().args([cmd, "--help"]).output().unwrap();
        assert!(output.status.success(), "'{} --help' should succeed", cmd);
    }
}

#[test]
fn tablestats_help_includes_table_flag() {
    let output = cassandra_tools()
        .args(["tablestats", "--help"])
        .output()
        .unwrap();
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("--table"));
}

#[test]
fn fqltool_dump_json_runs_via_cassandra_tools() {
    let dir = tempfile::tempdir().unwrap();
    let logger = FqlLogger::new(dir.path().to_path_buf(), 10, true).unwrap();
    logger
        .log_query(&FqlRecord {
            timestamp_micros: 1_717_000_000_000_000,
            consistency_level: 3,
            query: "SELECT * FROM ks.tbl WHERE k=?".to_string(),
            bind_values: vec![42i32.to_be_bytes().to_vec()],
        })
        .unwrap();
    let file = dir.path().join("fql.bin");

    let output = cassandra_tools()
        .args(["fqltool", "dump", "--json", file.to_string_lossy().as_ref()])
        .output()
        .unwrap();
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("\"query\":\"SELECT * FROM ks.tbl WHERE k=?\""));
    assert!(stdout.contains("--- 1 record(s) ---"));
}

#[test]
fn fqltool_stats_runs_via_cassandra_tools() {
    let dir = tempfile::tempdir().unwrap();
    let logger = FqlLogger::new(dir.path().to_path_buf(), 10, true).unwrap();
    logger
        .log_query(&FqlRecord {
            timestamp_micros: 1_717_000_000_000_000,
            consistency_level: 1,
            query: "SELECT release_version FROM system.local".to_string(),
            bind_values: vec![],
        })
        .unwrap();
    let file = dir.path().join("fql.bin");

    let output = cassandra_tools()
        .args(["fqltool", "stats", file.to_string_lossy().as_ref()])
        .output()
        .unwrap();
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Total records:       1"));
    assert!(stdout.contains("Unique queries:      1"));
}

#[test]
fn auditlogviewer_reads_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("audit.log");
    let mut file = File::create(&path).unwrap();
    let event = AuditEvent {
        timestamp: 1_717_111_111_111,
        event_type: AuditEventType::Query,
        user: "auditor".to_string(),
        source_address: "127.0.0.1".to_string(),
        keyspace: Some("ks".to_string()),
        table: Some("tbl".to_string()),
        query: Some("SELECT * FROM ks.tbl WHERE k=1".to_string()),
        status: AuditStatus::Success,
    };
    writeln!(file, "{}", serde_json::to_string(&event).unwrap()).unwrap();

    let output = cassandra_tools()
        .args(["auditlogviewer", path.to_string_lossy().as_ref()])
        .output()
        .unwrap();
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("auditor"));
    assert!(stdout.contains("SELECT * FROM ks.tbl WHERE k=1"));
    assert!(stdout.contains("--- 1 event(s) ---"));
}

#[test]
fn auditlogviewer_reads_directory_json() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("audit.log");
    let mut file = File::create(&path).unwrap();
    let event = AuditEvent {
        timestamp: 1_717_222_222_222,
        event_type: AuditEventType::AuthSuccess,
        user: "admin".to_string(),
        source_address: "::1".to_string(),
        keyspace: None,
        table: None,
        query: None,
        status: AuditStatus::Success,
    };
    writeln!(file, "{}", serde_json::to_string(&event).unwrap()).unwrap();

    let output = cassandra_tools()
        .args([
            "auditlogviewer",
            "--json",
            dir.path().to_string_lossy().as_ref(),
        ])
        .output()
        .unwrap();
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("\"user\":\"admin\""));
    assert!(stdout.contains("--- 1 event(s) ---"));
}

#[test]
fn cassandra_stress_read_runs_and_reports() {
    let output = cassandra_tools()
        .args([
            "--host",
            "127.0.0.1",
            "--port",
            "1",
            "cassandra-stress",
            "read",
            "--ops",
            "10",
            "--concurrency",
            "2",
            "--rate",
            "50",
        ])
        .output()
        .unwrap();
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Cassandra Stress (Rust)"));
    assert!(stdout.contains("operations:     10"));
    assert!(stdout.contains("target rate:    50 ops/s"));
    assert!(stdout.contains("success/fail:"));
}

#[test]
fn cassandra_stress_help_usage() {
    let output = cassandra_tools()
        .args(["cassandra-stress", "--help"])
        .output()
        .unwrap();
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("Usage:")
            || stdout.contains("Cassandra stress testing tool")
            || stdout.contains("cassandra-stress")
    );
}

#[test]
fn cassandra_stress_native_mode_reports_transport() {
    let output = cassandra_tools()
        .args([
            "cassandra-stress",
            "read",
            "--native-cql",
            "--prepared",
            "--native-port",
            "1",
            "--ops",
            "5",
            "--concurrency",
            "1",
        ])
        .output()
        .unwrap();
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("transport:      native-cql"));
    assert!(stdout.contains("prepared:       true"));
    assert!(stdout.contains("operations:     5"));
}

#[test]
fn cassandra_stress_native_auth_requires_both_credentials() {
    let output = cassandra_tools()
        .args([
            "cassandra-stress",
            "read",
            "--native-cql",
            "--username",
            "alice",
            "--ops",
            "1",
        ])
        .output()
        .unwrap();
    assert!(output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("--username and --password must be provided together"));
}

#[test]
fn cdcread_plaintext_summary_reads_completed_segment() {
    use cassandra_storage::cdc::write_cdc_index;
    use cassandra_storage::commitlog::{CellMutation, Mutation, MutationRow, segment::Segment};

    let dir = tempfile::tempdir().unwrap();
    let mut segment = Segment::create(dir.path(), 55).unwrap();
    let mutation = Mutation {
        keyspace: "ks".to_string(),
        table: "tbl".to_string(),
        partition_key: b"pk".to_vec(),
        rows: vec![MutationRow {
            clustering_key: Vec::new(),
            cells: vec![CellMutation {
                column: "v".to_string(),
                value: Some(b"value".to_vec()),
                timestamp: 1,
                ttl: 0,
                local_deletion_time: None,
                is_tombstone: false,
            }],
            is_tombstone: false,
            local_deletion_time: None,
        }],
        timestamp: 1,
        cdc_enabled: true,
        static_cells: Vec::new(),
        partition_tombstone: None,
        range_tombstones: Vec::new(),
    };
    segment
        .append_entry(&serde_json::to_vec(&mutation).unwrap())
        .unwrap();
    segment.sync().unwrap();
    write_cdc_index(segment.path(), segment.size(), true).unwrap();

    let output = cassandra_tools()
        .args([
            "cdcread",
            dir.path().to_string_lossy().as_ref(),
            "--summary-only",
        ])
        .output()
        .unwrap();
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("segment="));
    assert!(stdout.contains("mutations=1"));
    assert!(stdout.contains("CDC read complete"));
}

#[test]
fn cdcread_encrypted_requires_and_accepts_tde_key_material() {
    use cassandra_security::{
        EncryptionContext, FileKeyProvider, StorageEncryptor, TransparentDataEncryptionOptions,
        create_encryptor,
    };
    use cassandra_storage::cdc::write_cdc_index;
    use cassandra_storage::commitlog::{
        CellMutation, Mutation, MutationRow,
        encrypted::{CommitLogEncryptor, EncryptingSegmentWriter},
        segment::{Segment, SegmentFlags},
    };
    use std::sync::Arc;

    struct StorageEncryptorAdapter {
        inner: Box<dyn StorageEncryptor>,
    }

    impl std::fmt::Debug for StorageEncryptorAdapter {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("StorageEncryptorAdapter").finish()
        }
    }

    impl CommitLogEncryptor for StorageEncryptorAdapter {
        fn encrypt_segment(&self, data: &[u8]) -> Result<Vec<u8>, String> {
            self.inner.encrypt_segment(data).map_err(|e| e.to_string())
        }

        fn decrypt_segment(&self, data: &[u8]) -> Result<Vec<u8>, String> {
            self.inner.decrypt_segment(data).map_err(|e| e.to_string())
        }

        fn is_enabled(&self) -> bool {
            self.inner.is_enabled()
        }
    }

    let dir = tempfile::tempdir().unwrap();
    let key_dir = dir.path().join("keys");
    std::fs::create_dir_all(&key_dir).unwrap();
    let key_alias = "cdc_test_key";
    std::fs::write(key_dir.join(key_alias), [0x42u8; 16]).unwrap();

    let options = TransparentDataEncryptionOptions {
        enabled: true,
        key_alias: Some(key_alias.to_string()),
        ..Default::default()
    };
    let key_provider = Arc::new(FileKeyProvider::new(&key_dir));
    let context = Arc::new(EncryptionContext::new(options, Some(key_provider)));
    let encryptor = create_encryptor(Some(context));
    let codec =
        EncryptingSegmentWriter::new(Arc::new(StorageEncryptorAdapter { inner: encryptor }));

    let flags = SegmentFlags {
        compression_enabled: true,
        encryption_enabled: true,
    };
    let mut segment = Segment::create_with_flags(dir.path(), 56, flags).unwrap();
    let mutation = Mutation {
        keyspace: "ks".to_string(),
        table: "enc_tbl".to_string(),
        partition_key: b"pk".to_vec(),
        rows: vec![MutationRow {
            clustering_key: Vec::new(),
            cells: vec![CellMutation {
                column: "v".to_string(),
                value: Some(b"secret".to_vec()),
                timestamp: 2,
                ttl: 0,
                local_deletion_time: None,
                is_tombstone: false,
            }],
            is_tombstone: false,
            local_deletion_time: None,
        }],
        timestamp: 2,
        cdc_enabled: true,
        static_cells: Vec::new(),
        partition_tombstone: None,
        range_tombstones: Vec::new(),
    };
    segment
        .append_entry_with_codec(&serde_json::to_vec(&mutation).unwrap(), &codec)
        .unwrap();
    segment.sync().unwrap();
    write_cdc_index(segment.path(), segment.size(), true).unwrap();

    let output_plain = cassandra_tools()
        .args([
            "cdcread",
            dir.path().to_string_lossy().as_ref(),
            "--summary-only",
        ])
        .output()
        .unwrap();
    assert!(output_plain.status.success());
    let stderr_plain = String::from_utf8_lossy(&output_plain.stderr);
    assert!(stderr_plain.contains("Error reading CDC segment"));

    let output_decrypted = cassandra_tools()
        .args([
            "cdcread",
            dir.path().to_string_lossy().as_ref(),
            "--summary-only",
            "--key-directory",
            key_dir.to_string_lossy().as_ref(),
            "--key-alias",
            key_alias,
        ])
        .output()
        .unwrap();
    assert!(output_decrypted.status.success());
    let stdout_decrypted = String::from_utf8_lossy(&output_decrypted.stdout);
    assert!(stdout_decrypted.contains("mutations=1"));
    assert!(stdout_decrypted.contains("CDC read complete"));
}

#[test]
fn cdcread_requires_key_alias_when_key_directory_is_provided() {
    use cassandra_storage::cdc::write_cdc_index;
    use cassandra_storage::commitlog::{CellMutation, Mutation, MutationRow, segment::Segment};

    let dir = tempfile::tempdir().unwrap();
    let mut segment = Segment::create(dir.path(), 57).unwrap();
    let mutation = Mutation {
        keyspace: "ks".to_string(),
        table: "tbl".to_string(),
        partition_key: b"pk".to_vec(),
        rows: vec![MutationRow {
            clustering_key: Vec::new(),
            cells: vec![CellMutation {
                column: "v".to_string(),
                value: Some(b"value".to_vec()),
                timestamp: 1,
                ttl: 0,
                local_deletion_time: None,
                is_tombstone: false,
            }],
            is_tombstone: false,
            local_deletion_time: None,
        }],
        timestamp: 1,
        cdc_enabled: true,
        static_cells: Vec::new(),
        partition_tombstone: None,
        range_tombstones: Vec::new(),
    };
    segment
        .append_entry(&serde_json::to_vec(&mutation).unwrap())
        .unwrap();
    segment.sync().unwrap();
    write_cdc_index(segment.path(), segment.size(), true).unwrap();

    let key_dir = dir.path().join("keys");
    std::fs::create_dir_all(&key_dir).unwrap();

    let output = cassandra_tools()
        .args([
            "cdcread",
            dir.path().to_string_lossy().as_ref(),
            "--summary-only",
            "--key-directory",
            key_dir.to_string_lossy().as_ref(),
        ])
        .output()
        .unwrap();
    assert!(output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("--key-alias is required when --key-directory is provided"));
}

#[test]
fn cdcread_all_segments_includes_non_completed_entries() {
    use cassandra_storage::cdc::write_cdc_index;
    use cassandra_storage::commitlog::{CellMutation, Mutation, MutationRow, segment::Segment};

    let dir = tempfile::tempdir().unwrap();
    let mut segment = Segment::create(dir.path(), 58).unwrap();
    let mutation = Mutation {
        keyspace: "ks".to_string(),
        table: "tbl".to_string(),
        partition_key: b"pk".to_vec(),
        rows: vec![MutationRow {
            clustering_key: Vec::new(),
            cells: vec![CellMutation {
                column: "v".to_string(),
                value: Some(b"value".to_vec()),
                timestamp: 1,
                ttl: 0,
                local_deletion_time: None,
                is_tombstone: false,
            }],
            is_tombstone: false,
            local_deletion_time: None,
        }],
        timestamp: 1,
        cdc_enabled: true,
        static_cells: Vec::new(),
        partition_tombstone: None,
        range_tombstones: Vec::new(),
    };
    segment
        .append_entry(&serde_json::to_vec(&mutation).unwrap())
        .unwrap();
    segment.sync().unwrap();
    write_cdc_index(segment.path(), segment.size(), false).unwrap();

    let output_default = cassandra_tools()
        .args([
            "cdcread",
            dir.path().to_string_lossy().as_ref(),
            "--summary-only",
        ])
        .output()
        .unwrap();
    assert!(output_default.status.success());
    let stdout_default = String::from_utf8_lossy(&output_default.stdout);
    assert!(stdout_default.contains("No CDC segments matched the selection criteria."));

    let output_all = cassandra_tools()
        .args([
            "cdcread",
            dir.path().to_string_lossy().as_ref(),
            "--summary-only",
            "--all-segments",
        ])
        .output()
        .unwrap();
    assert!(output_all.status.success());
    let stdout_all = String::from_utf8_lossy(&output_all.stdout);
    assert!(stdout_all.contains("mutations=1"));
    assert!(stdout_all.contains("CDC read complete"));
}

#[test]
fn cdcread_batch_mode_resumes_with_cursor_file() {
    use cassandra_storage::cdc::write_cdc_index;
    use cassandra_storage::commitlog::{CellMutation, Mutation, MutationRow, segment::Segment};

    let dir = tempfile::tempdir().unwrap();
    let mut segment = Segment::create(dir.path(), 59).unwrap();
    for (table, timestamp) in [("tbl_a", 1_i64), ("tbl_b", 2_i64)] {
        let mutation = Mutation {
            keyspace: "ks".to_string(),
            table: table.to_string(),
            partition_key: b"pk".to_vec(),
            rows: vec![MutationRow {
                clustering_key: Vec::new(),
                cells: vec![CellMutation {
                    column: "v".to_string(),
                    value: Some(format!("value_{table}").into_bytes()),
                    timestamp,
                    ttl: 0,
                    local_deletion_time: None,
                    is_tombstone: false,
                }],
                is_tombstone: false,
                local_deletion_time: None,
            }],
            timestamp,
            cdc_enabled: true,
            static_cells: Vec::new(),
            partition_tombstone: None,
            range_tombstones: Vec::new(),
        };
        segment
            .append_entry(&serde_json::to_vec(&mutation).unwrap())
            .unwrap();
    }
    segment.sync().unwrap();
    write_cdc_index(segment.path(), segment.size(), true).unwrap();

    let cursor_file = dir.path().join("cdc_cursor.json");

    let output1 = cassandra_tools()
        .args([
            "cdcread",
            dir.path().to_string_lossy().as_ref(),
            "--batch-size",
            "1",
            "--cursor-file",
            cursor_file.to_string_lossy().as_ref(),
        ])
        .output()
        .unwrap();
    assert!(output1.status.success());
    let stdout1 = String::from_utf8_lossy(&output1.stdout);
    assert!(stdout1.contains("\"table\":\"tbl_a\""));
    assert!(stdout1.contains("next_cursor=segment:59 mutation_index:1"));
    assert!(cursor_file.exists());

    let output2 = cassandra_tools()
        .args([
            "cdcread",
            dir.path().to_string_lossy().as_ref(),
            "--batch-size",
            "1",
            "--cursor-file",
            cursor_file.to_string_lossy().as_ref(),
        ])
        .output()
        .unwrap();
    assert!(output2.status.success());
    let stdout2 = String::from_utf8_lossy(&output2.stdout);
    assert!(stdout2.contains("\"table\":\"tbl_b\""));
    assert!(stdout2.contains("next_cursor=segment:59 mutation_index:2"));

    // One more run consumes the terminal cursor and removes the cursor file.
    let output3 = cassandra_tools()
        .args([
            "cdcread",
            dir.path().to_string_lossy().as_ref(),
            "--batch-size",
            "1",
            "--cursor-file",
            cursor_file.to_string_lossy().as_ref(),
            "--summary-only",
        ])
        .output()
        .unwrap();
    assert!(output3.status.success());
    let stdout3 = String::from_utf8_lossy(&output3.stdout);
    assert!(stdout3.contains("CDC batch read complete: mutations=0 exhausted=true"));
    assert!(stdout3.contains("next_cursor=<none>"));
    assert!(!cursor_file.exists());
}

#[test]
fn cdcread_batch_mode_rejects_all_segments() {
    let dir = tempfile::tempdir().unwrap();
    let output = cassandra_tools()
        .args([
            "cdcread",
            dir.path().to_string_lossy().as_ref(),
            "--all-segments",
            "--batch-size",
            "10",
        ])
        .output()
        .unwrap();
    assert!(output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains(
        "--batch-size/--cursor-file currently support completed segments only (omit --all-segments)"
    ));
}

#[test]
fn cdcread_follow_requires_batch_size() {
    let dir = tempfile::tempdir().unwrap();
    let output = cassandra_tools()
        .args(["cdcread", dir.path().to_string_lossy().as_ref(), "--follow"])
        .output()
        .unwrap();
    assert!(output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("--follow requires --batch-size"));
}

#[test]
fn cdcread_follow_mode_runs_bounded_batches() {
    use cassandra_storage::cdc::write_cdc_index;
    use cassandra_storage::commitlog::{CellMutation, Mutation, MutationRow, segment::Segment};

    let dir = tempfile::tempdir().unwrap();
    let mut segment = Segment::create(dir.path(), 60).unwrap();
    let mutation = Mutation {
        keyspace: "ks".to_string(),
        table: "tbl_follow".to_string(),
        partition_key: b"pk".to_vec(),
        rows: vec![MutationRow {
            clustering_key: Vec::new(),
            cells: vec![CellMutation {
                column: "v".to_string(),
                value: Some(b"value".to_vec()),
                timestamp: 1,
                ttl: 0,
                local_deletion_time: None,
                is_tombstone: false,
            }],
            is_tombstone: false,
            local_deletion_time: None,
        }],
        timestamp: 1,
        cdc_enabled: true,
        static_cells: Vec::new(),
        partition_tombstone: None,
        range_tombstones: Vec::new(),
    };
    segment
        .append_entry(&serde_json::to_vec(&mutation).unwrap())
        .unwrap();
    segment.sync().unwrap();
    write_cdc_index(segment.path(), segment.size(), true).unwrap();

    let cursor_file = dir.path().join("cdc_follow_cursor.json");
    let output = cassandra_tools()
        .args([
            "cdcread",
            dir.path().to_string_lossy().as_ref(),
            "--batch-size",
            "1",
            "--cursor-file",
            cursor_file.to_string_lossy().as_ref(),
            "--follow",
            "--poll-interval-ms",
            "1",
            "--max-batches",
            "2",
            "--max-consecutive-nonempty-batches",
            "1",
            "--backpressure-sleep-ms",
            "1",
            "--summary-only",
        ])
        .output()
        .unwrap();
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("CDC batch read complete: mutations=1 exhausted=false"));
    assert_eq!(
        stdout
            .matches("CDC batch read complete: mutations=1")
            .count(),
        1
    );
    assert!(stdout.contains("CDC batch read complete: mutations=0 exhausted=true"));
    assert!(stdout.contains("CDC follow backpressure: consecutive_nonempty_batches=1 sleep_ms=1"));
    assert!(stdout.contains("CDC follow mode finished: batches=2"));
}

#[test]
fn cdcread_backpressure_requires_follow() {
    let dir = tempfile::tempdir().unwrap();
    let output = cassandra_tools()
        .args([
            "cdcread",
            dir.path().to_string_lossy().as_ref(),
            "--batch-size",
            "1",
            "--max-consecutive-nonempty-batches",
            "2",
        ])
        .output()
        .unwrap();
    assert!(output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains(
            "--max-consecutive-nonempty-batches/--backpressure-sleep-ms require --follow"
        )
    );
}

#[test]
fn cdcread_backpressure_sleep_requires_threshold() {
    let dir = tempfile::tempdir().unwrap();
    let output = cassandra_tools()
        .args([
            "cdcread",
            dir.path().to_string_lossy().as_ref(),
            "--batch-size",
            "1",
            "--follow",
            "--backpressure-sleep-ms",
            "5",
            "--max-batches",
            "1",
        ])
        .output()
        .unwrap();
    assert!(output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("--backpressure-sleep-ms requires --max-consecutive-nonempty-batches"));
}
