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

//! Golden tests for write error encoding (WU-21).
//!
//! Loads golden JSON fixtures from `diff-tests/golden/write_errors/` and
//! verifies that the Rust error types produce matching error codes and
//! field structures.

use cassandra_diff_tests::golden;
use serde::Deserialize;
use std::collections::HashMap;

use cassandra_coordinator::consistency::ConsistencyLevel;
use cassandra_coordinator::write::{WriteError, WriteType};
use cassandra_cluster_metadata::Endpoint;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

/// Golden fixture schema for write errors.
#[derive(Debug, Deserialize)]
struct WriteErrorFixture {
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

// ─── WriteTimeout golden test ────────────────────────────────────

#[test]
fn golden_write_timeout() {
    let fixture: WriteErrorFixture =
        golden::load_json("write_errors/write_timeout.json").expect("Failed to load fixture");

    assert_eq!(fixture.error_name, "WRITE_TIMEOUT");
    let expected_code = parse_error_code(&fixture.error_code);

    let fields = &fixture.fields;
    let cl_str = fields["cl"].as_str().unwrap();
    let write_type_str = fields["write_type"].as_str().unwrap();
    let required = fields["required"].as_u64().unwrap() as usize;
    let received = fields["received"].as_u64().unwrap() as usize;
    let block_for = fields["block_for"].as_u64().unwrap() as usize;

    // Build the Rust error
    let err = WriteError::Timeout {
        cl: ConsistencyLevel::Quorum,
        write_type: WriteType::Simple,
        required,
        received,
        block_for,
    };

    assert_eq!(err.error_code(), expected_code);
    assert_eq!(ConsistencyLevel::Quorum.to_string(), cl_str);
    assert_eq!(WriteType::Simple.protocol_name(), write_type_str);
}

// ─── WriteFailure golden test ────────────────────────────────────

#[test]
fn golden_write_failure() {
    let fixture: WriteErrorFixture =
        golden::load_json("write_errors/write_failure.json").expect("Failed to load fixture");

    assert_eq!(fixture.error_name, "WRITE_FAILURE");
    let expected_code = parse_error_code(&fixture.error_code);

    let fields = &fixture.fields;
    let required = fields["required"].as_u64().unwrap() as usize;
    let received = fields["received"].as_u64().unwrap() as usize;
    let block_for = fields["block_for"].as_u64().unwrap() as usize;
    let num_failures = fields["num_failures"].as_u64().unwrap() as usize;

    let mut failure_map = HashMap::new();
    failure_map.insert(ep(7003), 1u16);

    let err = WriteError::WriteFailure {
        cl: ConsistencyLevel::All,
        write_type: WriteType::Batch,
        required,
        received,
        block_for,
        num_failures,
        failure_map,
    };

    assert_eq!(err.error_code(), expected_code);
}

// ─── Unavailable golden test ─────────────────────────────────────

#[test]
fn golden_unavailable() {
    let fixture: WriteErrorFixture =
        golden::load_json("write_errors/unavailable.json").expect("Failed to load fixture");

    assert_eq!(fixture.error_name, "UNAVAILABLE");
    let expected_code = parse_error_code(&fixture.error_code);

    let fields = &fixture.fields;
    let required = fields["required"].as_u64().unwrap() as usize;
    let alive = fields["alive"].as_u64().unwrap() as usize;

    let err = WriteError::Unavailable {
        cl: ConsistencyLevel::Quorum,
        required,
        alive,
    };

    assert_eq!(err.error_code(), expected_code);
}

// ─── Overloaded golden test ──────────────────────────────────────

#[test]
fn golden_overloaded() {
    let fixture: WriteErrorFixture =
        golden::load_json("write_errors/overloaded.json").expect("Failed to load fixture");

    assert_eq!(fixture.error_name, "OVERLOADED");
    let expected_code = parse_error_code(&fixture.error_code);

    let err = WriteError::Overloaded;
    assert_eq!(err.error_code(), expected_code);
}

// ─── IsBootstrapping golden test ─────────────────────────────────

#[test]
fn golden_is_bootstrapping() {
    let fixture: WriteErrorFixture =
        golden::load_json("write_errors/is_bootstrapping.json").expect("Failed to load fixture");

    assert_eq!(fixture.error_name, "IS_BOOTSTRAPPING");
    let expected_code = parse_error_code(&fixture.error_code);

    let err = WriteError::IsBootstrapping;
    assert_eq!(err.error_code(), expected_code);
}
