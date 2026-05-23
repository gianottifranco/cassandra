// Licensed under Apache License, Version 2.0.

//! Integration tests for QueryProcessor: frame-level roundtrip tests.

use cassandra_native_protocol::frame::{Frame, FrameCodec, Opcode};
use cassandra_native_protocol::message::*;
use cassandra_native_protocol::response;
use cassandra_native_protocol::types;
use tokio_util::codec::{Decoder, Encoder};

// ─── Helpers ────────────────────────────────────────────────────────────────

fn decode_result_kind(frame: &Frame) -> i32 {
    let mut body: &[u8] = &frame.body;
    types::read_int(&mut body).unwrap()
}

fn decode_error_code(frame: &Frame) -> i32 {
    let mut body: &[u8] = &frame.body;
    types::read_int(&mut body).unwrap()
}

fn decode_error_message(frame: &Frame) -> String {
    let mut body: &[u8] = &frame.body;
    let _code = types::read_int(&mut body).unwrap();
    types::read_string(&mut body).unwrap()
}

// ─── Frame encoding/decoding roundtrip ──────────────────────────────────────

#[test]
fn void_result_frame_roundtrip() {
    let frame = response::void_result_frame(4, 1);
    assert_eq!(frame.header.opcode, Opcode::Result);
    assert_eq!(frame.header.stream_id, 1);
    assert_eq!(decode_result_kind(&frame), 0x0001); // Void

    // Roundtrip through codec
    let mut buf = bytes::BytesMut::new();
    let mut codec = FrameCodec;
    codec.encode(frame, &mut buf).unwrap();
    let decoded = codec.decode(&mut buf).unwrap().unwrap();
    assert_eq!(decoded.header.opcode, Opcode::Result);
    assert_eq!(decode_result_kind(&decoded), 0x0001);
}

#[test]
fn rows_result_frame_roundtrip() {
    let specs = vec![
        ColumnSpec {
            ksname: None,
            tablename: None,
            name: "id".to_string(),
            col_type: ColumnType::Uuid,
        },
        ColumnSpec {
            ksname: None,
            tablename: None,
            name: "name".to_string(),
            col_type: ColumnType::Varchar,
        },
    ];
    let rows = vec![vec![Some(vec![0u8; 16]), Some(b"Alice".to_vec())]];
    let frame = response::rows_result_frame(4, 2, "test_ks", "users", specs, rows);

    assert_eq!(frame.header.opcode, Opcode::Result);
    assert_eq!(frame.header.stream_id, 2);
    assert_eq!(decode_result_kind(&frame), 0x0002); // Rows

    // Verify body has proper metadata
    let mut body: &[u8] = &frame.body;
    let kind = types::read_int(&mut body).unwrap();
    assert_eq!(kind, 0x0002);

    // Read metadata flags
    let flags = types::read_int(&mut body).unwrap();
    assert_ne!(flags & rows_flags::GLOBAL_TABLES_SPEC, 0);

    // columns_count
    let col_count = types::read_int(&mut body).unwrap();
    assert_eq!(col_count, 2);

    // global table spec (ks, table)
    let ks = types::read_string(&mut body).unwrap();
    let tbl = types::read_string(&mut body).unwrap();
    assert_eq!(ks, "test_ks");
    assert_eq!(tbl, "users");

    // Codec roundtrip
    let mut buf = bytes::BytesMut::new();
    let mut codec = FrameCodec;
    codec.encode(frame, &mut buf).unwrap();
    let decoded = codec.decode(&mut buf).unwrap().unwrap();
    assert_eq!(decoded.header.opcode, Opcode::Result);
}

#[test]
fn prepared_result_frame_roundtrip() {
    let bind_meta = RowsMetadata {
        flags: 0,
        columns_count: 1,
        paging_state: None,
        new_metadata_id: None,
        global_table_spec: None,
        col_specs: vec![ColumnSpec {
            ksname: Some("ks".to_string()),
            tablename: Some("t".to_string()),
            name: "id".to_string(),
            col_type: ColumnType::Uuid,
        }],
    };
    let result_meta = RowsMetadata {
        flags: rows_flags::GLOBAL_TABLES_SPEC,
        columns_count: 2,
        paging_state: None,
        new_metadata_id: None,
        global_table_spec: Some(("ks".to_string(), "t".to_string())),
        col_specs: vec![
            ColumnSpec {
                ksname: None,
                tablename: None,
                name: "id".to_string(),
                col_type: ColumnType::Uuid,
            },
            ColumnSpec {
                ksname: None,
                tablename: None,
                name: "name".to_string(),
                col_type: ColumnType::Varchar,
            },
        ],
    };

    let prepared_result = PreparedResult {
        id: vec![0u8; 16],
        result_metadata_id: Some(vec![1u8; 16]),
        bind_metadata: bind_meta,
        result_metadata: result_meta,
    };

    let frame = response::encode_response(
        &Message::Result(ResultMessage::Prepared(prepared_result)),
        4,
        3,
    );

    assert_eq!(frame.header.opcode, Opcode::Result);
    assert_eq!(decode_result_kind(&frame), 0x0004); // Prepared

    // Codec roundtrip
    let mut buf = bytes::BytesMut::new();
    let mut codec = FrameCodec;
    codec.encode(frame, &mut buf).unwrap();
    let decoded = codec.decode(&mut buf).unwrap().unwrap();
    assert_eq!(decoded.header.opcode, Opcode::Result);
}

#[test]
fn prepared_result_v5_includes_result_metadata_id() {
    let metadata_id = vec![1u8; 16];
    let prepared_result = PreparedResult {
        id: vec![0u8; 16],
        result_metadata_id: Some(metadata_id.clone()),
        bind_metadata: RowsMetadata {
            flags: 0,
            columns_count: 0,
            paging_state: None,
            new_metadata_id: None,
            global_table_spec: None,
            col_specs: Vec::new(),
        },
        result_metadata: RowsMetadata {
            flags: 0,
            columns_count: 0,
            paging_state: None,
            new_metadata_id: None,
            global_table_spec: None,
            col_specs: Vec::new(),
        },
    };

    let frame = response::encode_response(
        &Message::Result(ResultMessage::Prepared(prepared_result)),
        5,
        4,
    );
    let mut body: &[u8] = &frame.body;
    assert_eq!(types::read_int(&mut body).unwrap(), 0x0004);
    assert_eq!(types::read_short_bytes(&mut body).unwrap(), vec![0u8; 16]);
    assert_eq!(types::read_short_bytes(&mut body).unwrap(), metadata_id);
}

#[test]
fn error_frame_correct_codes() {
    // SYNTAX_ERROR
    let frame = response::error_frame(4, 0, 0x2000, "Syntax error near 'SELCT'");
    assert_eq!(frame.header.opcode, Opcode::Error);
    assert_eq!(decode_error_code(&frame), 0x2000);
    assert!(decode_error_message(&frame).contains("SELCT"));

    // INVALID_QUERY
    let frame = response::error_frame(4, 0, 0x2200, "Table not found");
    assert_eq!(decode_error_code(&frame), 0x2200);

    // SERVER_ERROR
    let frame = response::error_frame(4, 0, 0x0000, "Internal failure");
    assert_eq!(decode_error_code(&frame), 0x0000);
}

#[test]
fn error_frame_from_cassandra_error() {
    use cassandra_common::CassandraError;
    use cassandra_native_protocol::error_codes;

    let errors: Vec<(CassandraError, i32)> = vec![
        (CassandraError::SyntaxError("bad sql".into()), 0x2000),
        (CassandraError::InvalidQuery("no table".into()), 0x2200),
        (CassandraError::ServerError("boom".into()), 0x0000),
        (CassandraError::ConfigError("bad config".into()), 0x2300),
        (CassandraError::Unauthorized("denied".into()), 0x2100),
    ];

    for (err, expected_code) in errors {
        let error_msg = error_codes::error_to_message(&err);
        let frame = response::encode_response(&Message::Error(error_msg), 4, 0);
        assert_eq!(frame.header.opcode, Opcode::Error);
        assert_eq!(decode_error_code(&frame), expected_code);
    }
}

#[test]
fn set_keyspace_result_roundtrip() {
    let frame = response::set_keyspace_frame(4, 0, "my_keyspace");
    assert_eq!(frame.header.opcode, Opcode::Result);
    assert_eq!(decode_result_kind(&frame), 0x0003); // SetKeyspace

    let mut body: &[u8] = &frame.body;
    let _kind = types::read_int(&mut body).unwrap();
    let ks = types::read_string(&mut body).unwrap();
    assert_eq!(ks, "my_keyspace");
}

#[test]
fn schema_change_result_roundtrip() {
    let frame = response::schema_change_frame(4, 0, "CREATED", "TABLE", "ks", Some("users"));
    assert_eq!(frame.header.opcode, Opcode::Result);
    assert_eq!(decode_result_kind(&frame), 0x0005); // SchemaChange

    // Codec roundtrip
    let mut buf = bytes::BytesMut::new();
    let mut codec = FrameCodec;
    codec.encode(frame, &mut buf).unwrap();
    let decoded = codec.decode(&mut buf).unwrap().unwrap();
    assert_eq!(decoded.header.opcode, Opcode::Result);
    assert_eq!(decode_result_kind(&decoded), 0x0005);
}

#[test]
fn select_result_encoding_empty_rows() {
    let specs = vec![ColumnSpec {
        ksname: None,
        tablename: None,
        name: "id".to_string(),
        col_type: ColumnType::Int,
    }];
    let frame = response::rows_result_frame(4, 0, "ks", "t", specs, vec![]);

    let mut body: &[u8] = &frame.body;
    let kind = types::read_int(&mut body).unwrap();
    assert_eq!(kind, 0x0002);

    let _flags = types::read_int(&mut body).unwrap();
    let col_count = types::read_int(&mut body).unwrap();
    assert_eq!(col_count, 1);

    // Skip global table spec
    let _ks = types::read_string(&mut body).unwrap();
    let _tbl = types::read_string(&mut body).unwrap();

    // Skip column name + type
    let col_name = types::read_string(&mut body).unwrap();
    assert_eq!(col_name, "id");
    let type_id = types::read_short(&mut body).unwrap();
    assert_eq!(type_id, 0x0009); // Int

    // rows_count
    let rows_count = types::read_int(&mut body).unwrap();
    assert_eq!(rows_count, 0);
}

#[test]
fn select_result_encoding_multiple_rows() {
    let specs = vec![
        ColumnSpec {
            ksname: None,
            tablename: None,
            name: "id".to_string(),
            col_type: ColumnType::Int,
        },
        ColumnSpec {
            ksname: None,
            tablename: None,
            name: "name".to_string(),
            col_type: ColumnType::Varchar,
        },
    ];

    let id1 = 1i32.to_be_bytes().to_vec();
    let id2 = 2i32.to_be_bytes().to_vec();
    let rows = vec![
        vec![Some(id1), Some(b"Alice".to_vec())],
        vec![Some(id2), Some(b"Bob".to_vec())],
    ];

    let frame = response::rows_result_frame(4, 0, "ks", "users", specs, rows);

    let mut body: &[u8] = &frame.body;
    let kind = types::read_int(&mut body).unwrap();
    assert_eq!(kind, 0x0002);

    let flags = types::read_int(&mut body).unwrap();
    assert_ne!(flags & rows_flags::GLOBAL_TABLES_SPEC, 0);

    let col_count = types::read_int(&mut body).unwrap();
    assert_eq!(col_count, 2);

    // global table spec
    let ks = types::read_string(&mut body).unwrap();
    let tbl = types::read_string(&mut body).unwrap();
    assert_eq!(ks, "ks");
    assert_eq!(tbl, "users");

    // col specs (name + type only, since global spec is set)
    let name1 = types::read_string(&mut body).unwrap();
    assert_eq!(name1, "id");
    let type1 = types::read_short(&mut body).unwrap();
    assert_eq!(type1, 0x0009); // Int

    let name2 = types::read_string(&mut body).unwrap();
    assert_eq!(name2, "name");
    let type2 = types::read_short(&mut body).unwrap();
    assert_eq!(type2, 0x000D); // Varchar

    // rows_count
    let rows_count = types::read_int(&mut body).unwrap();
    assert_eq!(rows_count, 2);
}
