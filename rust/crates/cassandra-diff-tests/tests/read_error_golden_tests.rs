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

//! Golden tests for read error encoding (Prompt 14, WU-17).
//!
//! Loads golden JSON fixtures from `diff-tests/golden/read_errors/` and
//! verifies that the Rust error types produce matching error codes and
//! field structures.

use cassandra_diff_tests::golden;
use serde::Deserialize;
use std::collections::HashMap;

use cassandra_cluster_metadata::Endpoint;
use cassandra_coordinator::consistency::ConsistencyLevel;
use cassandra_coordinator::read::ReadError;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

/// Golden fixture schema for read errors.
#[derive(Debug, Deserialize)]
struct ReadErrorFixture {
    error_code: String,
    error_name: String,
    #[allow(dead_code)]
    description: String,
    fields: serde_json::Value,
    #[allow(dead_code)]
    java_class: String,
    #[allow(dead_code)]
    protocol_version: u8,
}

fn ep(port: u16) -> Endpoint {
    Endpoint::new(SocketAddr::new(
        IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
        port,
    ))
}

fn parse_error_code(s: &str) -> u32 {
    u32::from_str_radix(s.trim_start_matches("0x"), 16).unwrap()
}

// ─── ReadTimeout golden test ──────────────────────────────────────

#[test]
fn golden_read_timeout() {
    let fixture: ReadErrorFixture =
        golden::load_json("read_errors/read_timeout.json").expect("Failed to load fixture");

    assert_eq!(fixture.error_name, "READ_TIMEOUT");
    let expected_code = parse_error_code(&fixture.error_code);

    let fields = &fixture.fields;
    let cl_str = fields["cl"].as_str().unwrap();
    let required = fields["required"].as_u64().unwrap() as usize;
    let received = fields["received"].as_u64().unwrap() as usize;
    let data_present = fields["data_present"].as_bool().unwrap();

    let err = ReadError::Timeout {
        cl: ConsistencyLevel::Quorum,
        required,
        received,
        data_present,
    };

    assert_eq!(err.error_code(), expected_code);
    assert_eq!(ConsistencyLevel::Quorum.to_string(), cl_str);
}

// ─── ReadFailure golden test ──────────────────────────────────────

#[test]
fn golden_read_failure() {
    let fixture: ReadErrorFixture =
        golden::load_json("read_errors/read_failure.json").expect("Failed to load fixture");

    assert_eq!(fixture.error_name, "READ_FAILURE");
    let expected_code = parse_error_code(&fixture.error_code);

    let fields = &fixture.fields;
    let required = fields["required"].as_u64().unwrap() as usize;
    let received = fields["received"].as_u64().unwrap() as usize;
    let num_failures = fields["num_failures"].as_u64().unwrap() as usize;
    let data_present = fields["data_present"].as_bool().unwrap();

    let mut failure_map = HashMap::new();
    failure_map.insert(ep(7003), 1u16);

    let err = ReadError::ReadFailure {
        cl: ConsistencyLevel::All,
        required,
        received,
        num_failures,
        data_present,
        failure_map,
    };

    assert_eq!(err.error_code(), expected_code);
}

// ─── Unavailable golden test ──────────────────────────────────────

#[test]
fn golden_read_unavailable() {
    let fixture: ReadErrorFixture =
        golden::load_json("read_errors/unavailable.json").expect("Failed to load fixture");

    assert_eq!(fixture.error_name, "UNAVAILABLE");
    let expected_code = parse_error_code(&fixture.error_code);

    let fields = &fixture.fields;
    let required = fields["required"].as_u64().unwrap() as usize;
    let alive = fields["alive"].as_u64().unwrap() as usize;

    let err = ReadError::Unavailable {
        cl: ConsistencyLevel::Quorum,
        required,
        alive,
    };

    assert_eq!(err.error_code(), expected_code);
}

// ─── TombstoneOverwhelming golden test ────────────────────────────

#[test]
fn golden_tombstone_overwhelming() {
    let fixture: ReadErrorFixture = golden::load_json("read_errors/tombstone_overwhelming.json")
        .expect("Failed to load fixture");

    assert_eq!(fixture.error_name, "TOMBSTONE_OVERWHELMING");
    let expected_code = parse_error_code(&fixture.error_code);

    let fields = &fixture.fields;
    let count = fields["count"].as_u64().unwrap() as u32;
    let threshold = fields["threshold"].as_u64().unwrap() as u32;

    let err = ReadError::TombstoneOverwhelming { count, threshold };

    assert_eq!(err.error_code(), expected_code);
}
