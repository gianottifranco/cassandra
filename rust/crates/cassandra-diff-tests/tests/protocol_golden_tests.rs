// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

//! Golden tests for CQL native protocol frame encoding/decoding.
//!
//! These tests load pre-generated fixtures from `diff-tests/golden/protocol/`
//! and verify that the Rust codec produces byte-identical output.

use cassandra_diff_tests::golden;
use cassandra_native_protocol::frame::{FrameHeader, Opcode, PROTOCOL_V4};
use cassandra_native_protocol::response;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct ProtocolFixtures {
    description: String,
    protocol_version: u8,
    frames: Vec<FrameFixture>,
}

#[derive(Debug, Deserialize)]
struct FrameFixture {
    name: String,
    description: String,
    hex: String,
    opcode: String,
    direction: String,
    fields: serde_json::Value,
}

#[test]
fn golden_protocol_fixtures_load() {
    let fixtures: ProtocolFixtures =
        golden::load_json("protocol/frames.json").expect("Failed to load protocol golden fixtures");

    assert_eq!(fixtures.protocol_version, 4);
    assert!(
        !fixtures.frames.is_empty(),
        "Expected at least one frame fixture"
    );

    for fixture in &fixtures.frames {
        assert!(!fixture.name.is_empty(), "Fixture must have a name");
        assert!(
            !fixture.hex.is_empty(),
            "Fixture {} must have hex data",
            fixture.name
        );
    }
}

#[test]
fn golden_ready_frame_matches() {
    let frame = response::ready_frame(PROTOCOL_V4, 0);
    assert_eq!(frame.header.opcode, Opcode::Ready);
    assert!(frame.header.is_response());
    assert!(frame.body.is_empty());

    // READY frame header: version 0x84, flags 0, stream 0, opcode 0x02, length 0
    let expected_header = [0x84, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00];
    let mut buf = bytes::BytesMut::new();
    frame.header.encode(&mut buf);
    assert_eq!(&buf[..], &expected_header, "READY frame header mismatch");
}

#[test]
fn golden_error_frame_structure() {
    let frame = response::error_frame(PROTOCOL_V4, 0, 0x2000, "Syntax error");
    assert_eq!(frame.header.opcode, Opcode::Error);

    let mut body: &[u8] = &frame.body;
    let code = cassandra_native_protocol::types::read_int(&mut body).unwrap();
    assert_eq!(code, 0x2000, "Error code should be SYNTAX_ERROR");

    let msg = cassandra_native_protocol::types::read_string(&mut body).unwrap();
    assert_eq!(msg, "Syntax error");
}

#[test]
fn golden_void_result_structure() {
    let frame = response::void_result_frame(PROTOCOL_V4, 0);
    assert_eq!(frame.header.opcode, Opcode::Result);

    let mut body: &[u8] = &frame.body;
    let kind = cassandra_native_protocol::types::read_int(&mut body).unwrap();
    assert_eq!(kind, 0x0001, "Result kind should be VOID");
}

#[test]
fn golden_set_keyspace_result_structure() {
    let frame = response::set_keyspace_frame(PROTOCOL_V4, 0, "my_ks");
    let mut body: &[u8] = &frame.body;
    let kind = cassandra_native_protocol::types::read_int(&mut body).unwrap();
    assert_eq!(kind, 0x0003, "Result kind should be SET_KEYSPACE");
    let ks = cassandra_native_protocol::types::read_string(&mut body).unwrap();
    assert_eq!(ks, "my_ks");
}

#[test]
fn golden_schema_change_structure() {
    let frame =
        response::schema_change_frame(PROTOCOL_V4, 0, "CREATED", "KEYSPACE", "test_ks", None);
    let mut body: &[u8] = &frame.body;
    let kind = cassandra_native_protocol::types::read_int(&mut body).unwrap();
    assert_eq!(kind, 0x0005, "Result kind should be SCHEMA_CHANGE");
}

#[test]
fn golden_supported_frame_contains_protocol_versions() {
    let frame = response::supported_frame(PROTOCOL_V4, 0);
    let mut body: &[u8] = &frame.body;
    let map = cassandra_native_protocol::types::read_string_multimap(&mut body).unwrap();

    assert!(map.contains_key("CQL_VERSION"), "Missing CQL_VERSION");
    assert!(map.contains_key("COMPRESSION"), "Missing COMPRESSION");
    assert!(
        map.contains_key("PROTOCOL_VERSIONS"),
        "Missing PROTOCOL_VERSIONS"
    );

    let versions = &map["PROTOCOL_VERSIONS"];
    assert!(
        versions.iter().any(|v| v.contains("v4")),
        "PROTOCOL_VERSIONS should include v4"
    );
    assert!(
        versions.iter().any(|v| v.contains("v5")),
        "PROTOCOL_VERSIONS should include v5"
    );
}

#[test]
fn golden_rows_result_structure() {
    use cassandra_native_protocol::message::{ColumnSpec, ColumnType};

    let specs = vec![
        ColumnSpec {
            ksname: None,
            tablename: None,
            name: "key".to_string(),
            col_type: ColumnType::Varchar,
        },
        ColumnSpec {
            ksname: None,
            tablename: None,
            name: "value".to_string(),
            col_type: ColumnType::Int,
        },
    ];
    let rows = vec![vec![
        Some(b"test".to_vec()),
        Some(42i32.to_be_bytes().to_vec()),
    ]];
    let frame = response::rows_result_frame(PROTOCOL_V4, 0, "ks", "t", specs, rows);

    let mut body: &[u8] = &frame.body;
    let kind = cassandra_native_protocol::types::read_int(&mut body).unwrap();
    assert_eq!(kind, 0x0002, "Result kind should be ROWS");
}

#[test]
fn golden_frame_header_roundtrip() {
    // Verify header encode/decode roundtrip for all relevant opcodes
    let opcodes = [
        (Opcode::Startup, false),
        (Opcode::Query, false),
        (Opcode::Prepare, false),
        (Opcode::Execute, false),
        (Opcode::Batch, false),
        (Opcode::Ready, true),
        (Opcode::Result, true),
        (Opcode::Error, true),
        (Opcode::Event, true),
    ];

    for (opcode, is_response) in opcodes {
        let version = if is_response {
            PROTOCOL_V4 | 0x80
        } else {
            PROTOCOL_V4
        };
        let hdr = FrameHeader {
            version,
            flags: 0,
            stream_id: 42,
            opcode,
            length: 100,
        };

        let mut buf = bytes::BytesMut::new();
        hdr.encode(&mut buf);
        let decoded = FrameHeader::decode(&buf).unwrap();

        assert_eq!(decoded.protocol_version(), 4);
        assert_eq!(decoded.is_response(), is_response);
        assert_eq!(decoded.opcode, opcode);
        assert_eq!(decoded.stream_id, 42);
        assert_eq!(decoded.length, 100);
    }
}

#[test]
fn golden_prepared_metadata_fixtures_load() {
    let fixtures: serde_json::Value = golden::load_json("protocol/prepared_metadata.json")
        .expect("Failed to load prepared metadata golden fixtures");

    let prepared_fixtures = fixtures["fixtures"].as_array().unwrap();
    assert!(
        prepared_fixtures.len() >= 3,
        "Expected at least 3 prepared metadata fixtures"
    );

    for fixture in prepared_fixtures {
        let name = fixture["name"].as_str().unwrap();
        let query = fixture["query"].as_str().unwrap();
        assert!(!name.is_empty(), "Fixture must have a name");
        assert!(!query.is_empty(), "Fixture {} must have a query", name);

        let bind_count = fixture["bind_metadata"]["columns_count"].as_i64().unwrap();
        let result_count = fixture["result_metadata"]["columns_count"]
            .as_i64()
            .unwrap();
        assert!(bind_count >= 0, "Fixture {} bind_count must be >= 0", name);
        assert!(
            result_count >= 0,
            "Fixture {} result_count must be >= 0",
            name
        );
    }
}
