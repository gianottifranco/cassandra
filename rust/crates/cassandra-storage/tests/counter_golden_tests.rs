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

//! Golden tests for CounterContext binary format.
//! Ensures compatibility with Java Apache Cassandra `org.apache.cassandra.db.context.CounterContext`.

use cassandra_storage::counter::CounterContext;
use uuid::Uuid;

#[test]
fn test_golden_counter_context_serialization() {
    // Generate a known counter context
    let node_a = Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap();
    let node_b = Uuid::parse_str("00000000-0000-0000-0000-000000000002").unwrap();

    let mut ctx = CounterContext::new();
    ctx.apply_local(node_a, 10); // clock = 1, count = 10
    ctx.apply_local(node_b, -5); // clock = 1, count = -5

    let bytes = ctx.serialize();

    // Expected binary layout:
    // Header (2 bytes) = 0x00, 0x00
    // Node A (16 bytes) = 00..01, clock (8 bytes) = 1, count (8 bytes) = 10
    // Node B (16 bytes) = 00..02, clock (8 bytes) = 1, count (8 bytes) = -5

    let expected_header: [u8; 2] = [0, 0];
    assert_eq!(&bytes[0..2], &expected_header);

    // Node A (UUID)
    let expected_node_a_msb: [u8; 8] = [0, 0, 0, 0, 0, 0, 0, 0];
    let expected_node_a_lsb: [u8; 8] = [0, 0, 0, 0, 0, 0, 0, 1];
    assert_eq!(&bytes[2..10], &expected_node_a_msb);
    assert_eq!(&bytes[10..18], &expected_node_a_lsb);

    // Node A Clock (1)
    let expected_node_a_clock: [u8; 8] = [0, 0, 0, 0, 0, 0, 0, 1];
    assert_eq!(&bytes[18..26], &expected_node_a_clock);

    // Node A Count (10)
    let expected_node_a_count: [u8; 8] = [0, 0, 0, 0, 0, 0, 0, 10];
    assert_eq!(&bytes[26..34], &expected_node_a_count);

    // Node B (UUID)
    let expected_node_b_msb: [u8; 8] = [0, 0, 0, 0, 0, 0, 0, 0];
    let expected_node_b_lsb: [u8; 8] = [0, 0, 0, 0, 0, 0, 0, 2];
    assert_eq!(&bytes[34..42], &expected_node_b_msb);
    assert_eq!(&bytes[42..50], &expected_node_b_lsb);

    // Node B Clock (1)
    let expected_node_b_clock: [u8; 8] = [0, 0, 0, 0, 0, 0, 0, 1];
    assert_eq!(&bytes[50..58], &expected_node_b_clock);

    // Node B Count (-5)
    // -5 in two's complement = 0xFFFFFFFFFFFFFFFB
    let expected_node_b_count: [u8; 8] = [255, 255, 255, 255, 255, 255, 255, 251];
    assert_eq!(&bytes[58..66], &expected_node_b_count);

    // Total length: 2 (header) + 32 (shard A) + 32 (shard B) = 66
    assert_eq!(bytes.len(), 66);
}
