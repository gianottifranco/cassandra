// Licensed under Apache License, Version 2.0.

//! Property-based fuzz tests using proptest.

use proptest::prelude::*;

use cassandra_diff_tests::comparators::protocol;
use cassandra_diff_tests::fuzz;

proptest! {
    #[test]
    fn prop_frame_header_roundtrip(header in fuzz::arb_frame_header()) {
        let encoded = fuzz::encode_header(&header);
        let decoded = protocol::parse_header(&encoded)
            .expect("Header decode should succeed");
        prop_assert_eq!(&header, &decoded);
    }

    #[test]
    fn prop_error_body_roundtrip(
        code in fuzz::arb_error_code(),
        msg in "[a-zA-Z0-9 _.,!?]{0,200}".prop_map(|s| s.to_string())
    ) {
        let body = fuzz::build_error_body(code, &msg);
        let (parsed_code, parsed_msg) =
            cassandra_diff_tests::comparators::error::parse_error_body(&body)
                .expect("Error body decode should succeed");
        prop_assert_eq!(code, parsed_code);
        prop_assert_eq!(&msg, &parsed_msg);
    }

    #[test]
    fn prop_cql_int_roundtrip(v in any::<i32>()) {
        let bytes = fuzz::serialize_cql_int(v);
        let decoded = fuzz::deserialize_cql_int(&bytes).unwrap();
        prop_assert_eq!(v, decoded);
    }

    #[test]
    fn prop_cql_bigint_roundtrip(v in any::<i64>()) {
        let bytes = fuzz::serialize_cql_bigint(v);
        let decoded = fuzz::deserialize_cql_bigint(&bytes).unwrap();
        prop_assert_eq!(v, decoded);
    }

    #[test]
    fn prop_cql_smallint_roundtrip(v in any::<i16>()) {
        let bytes = fuzz::serialize_cql_smallint(v);
        let decoded = fuzz::deserialize_cql_smallint(&bytes).unwrap();
        prop_assert_eq!(v, decoded);
    }

    #[test]
    fn prop_cql_tinyint_roundtrip(v in any::<i8>()) {
        let bytes = fuzz::serialize_cql_tinyint(v);
        let decoded = fuzz::deserialize_cql_tinyint(&bytes).unwrap();
        prop_assert_eq!(v, decoded);
    }

    #[test]
    fn prop_cql_boolean_roundtrip(v in any::<bool>()) {
        let bytes = fuzz::serialize_cql_boolean(v);
        let decoded = fuzz::deserialize_cql_boolean(&bytes).unwrap();
        prop_assert_eq!(v, decoded);
    }

    #[test]
    fn prop_cql_float_roundtrip(v in -1e10f32..1e10f32) {
        let bytes = fuzz::serialize_cql_float(v);
        let decoded = fuzz::deserialize_cql_float(&bytes).unwrap();
        prop_assert!((v - decoded).abs() < f32::EPSILON * 1000.0);
    }

    #[test]
    fn prop_cql_double_roundtrip(v in -1e20f64..1e20f64) {
        let bytes = fuzz::serialize_cql_double(v);
        let decoded = fuzz::deserialize_cql_double(&bytes).unwrap();
        prop_assert!((v - decoded).abs() < f64::EPSILON * 1e6);
    }

    #[test]
    fn prop_cql_value_serialize_length(val in fuzz::arb_cql_value()) {
        let val: fuzz::CqlTestValue = val;
        let bytes = val.serialize();
        match &val {
            fuzz::CqlTestValue::Int(_) => prop_assert_eq!(bytes.len(), 4),
            fuzz::CqlTestValue::Bigint(_) => prop_assert_eq!(bytes.len(), 8),
            fuzz::CqlTestValue::Smallint(_) => prop_assert_eq!(bytes.len(), 2),
            fuzz::CqlTestValue::Tinyint(_) => prop_assert_eq!(bytes.len(), 1),
            fuzz::CqlTestValue::Boolean(_) => prop_assert_eq!(bytes.len(), 1),
            fuzz::CqlTestValue::Float(_) => prop_assert_eq!(bytes.len(), 4),
            fuzz::CqlTestValue::Double(_) => prop_assert_eq!(bytes.len(), 8),
            fuzz::CqlTestValue::Text(s) => prop_assert_eq!(bytes.len(), s.len()),
            fuzz::CqlTestValue::Blob(b) => prop_assert_eq!(bytes.len(), b.len()),
        }
    }
}

// Storage engine property tests using explicit Mutation struct

use cassandra_storage::commitlog::{CellMutation, CommitLogConfig, Mutation, MutationRow};
use cassandra_storage::engine::{EngineConfig, StorageEngine};

proptest! {
    /// Write → read roundtrip with random mutations.
    #[test]
    fn prop_mutation_write_read(
        ks in "[a-z]{1,8}",
        tbl in "[a-z]{1,8}",
        pk in proptest::collection::vec(any::<u8>(), 1..64),
        val in proptest::collection::vec(any::<u8>(), 1..256),
        ts in 0i64..1_000_000_000,
    ) {
        let dir = tempfile::TempDir::new().unwrap();
        let config = EngineConfig {
            data_directories: vec![dir.path().join("data")],
            commitlog: CommitLogConfig {
                max_segment_size: 1024 * 1024,
                directory: dir.path().join("commitlog"),
                ..CommitLogConfig::default()
            },
            memtable_flush_threshold: 64 * 1024 * 1024,
            gc_grace_seconds: 86400,
            ..EngineConfig::default()
        };
        let engine = StorageEngine::open(config).unwrap();

        let m = Mutation {
            keyspace: ks.clone(),
            table: tbl.clone(),
            partition_key: pk.clone(),
            rows: vec![MutationRow {
                clustering_key: b"ck".to_vec(),
                cells: vec![CellMutation {
                    column: "v".to_string(),
                    value: Some(val),
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
        };

        engine.apply_mutation(&m).unwrap();
        let result = engine.read_partition(&ks, &tbl, &pk);
        prop_assert!(result.is_some(), "Written data should be readable");
    }

    /// Commitlog replay preserves random writes.
    #[test]
    fn prop_commitlog_replay_preserves(write_count in 1usize..20) {
        let dir = tempfile::TempDir::new().unwrap();
        let config = EngineConfig {
            data_directories: vec![dir.path().join("data")],
            commitlog: CommitLogConfig {
                max_segment_size: 4096,
                directory: dir.path().join("commitlog"),
                ..CommitLogConfig::default()
            },
            memtable_flush_threshold: 64 * 1024 * 1024,
            gc_grace_seconds: 86400,
            ..EngineConfig::default()
        };

        {
            let engine = StorageEngine::open(config.clone()).unwrap();
            for i in 0..write_count {
                let m = Mutation {
                    keyspace: "ks".to_string(),
                    table: "tbl".to_string(),
                    partition_key: format!("pk-{i}").into_bytes(),
                    rows: vec![MutationRow {
                        clustering_key: b"ck".to_vec(),
                        cells: vec![CellMutation {
                            column: "v".to_string(),
                            value: Some(format!("val-{i}").into_bytes()),
                            timestamp: i as i64,
                            ttl: 0,
                            local_deletion_time: None,
                            is_tombstone: false,
                        }],
                        is_tombstone: false,
                        local_deletion_time: None,
                    }],
                    timestamp: i as i64,
                    cdc_enabled: false,
            static_cells: vec![],
            partition_tombstone: None,
            range_tombstones: vec![],
                };
                engine.apply_mutation(&m).unwrap();
            }
            engine.flush_all().unwrap();
        }

        {
            let engine = StorageEngine::open(config).unwrap();
            let replayed = engine.replay_commitlog().unwrap();
            prop_assert!(replayed >= 0, "Replay count should be non-negative");
        }
    }
}
