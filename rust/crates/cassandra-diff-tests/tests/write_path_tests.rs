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

//! Differential write-path tests (WU-20).
//!
//! Verifies write coordination behavior across consistency levels, hint
//! storage, batch atomicity, and error handling against expected behavior
//! documented in the Java oracle.

use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::Ordering;

use cassandra_cluster_metadata::{
    ClusterMetadata, Endpoint, NodeId, NodeInfo, SimpleSnitch, SimpleStrategy,
};
use cassandra_common::Token;
use cassandra_coordinator::batch::{BatchCoordinator, BatchLogManager, BatchType};
use cassandra_coordinator::consistency::ConsistencyLevel;
use cassandra_coordinator::hints::HintStore;
use cassandra_coordinator::write::{
    CellMutation, CoordinatedMutation, MutationRow, WriteCoordinator, WriteError, WriteType,
};

// ─── Helpers ─────────────────────────────────────────────────────

fn ep(port: u16) -> Endpoint {
    Endpoint::new(SocketAddr::new(
        IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
        port,
    ))
}

fn node(port: u16, tokens: Vec<i64>) -> NodeInfo {
    NodeInfo::new(
        NodeId::random(),
        ep(port),
        "dc1",
        "rack1",
        tokens.into_iter().map(Token::from_raw).collect(),
    )
}

fn setup_cluster() -> (Arc<ClusterMetadata>, Arc<WriteCoordinator>) {
    let cm = Arc::new(ClusterMetadata::new(node(7001, vec![-100])));
    cm.update_node(node(7002, vec![0]));
    cm.update_node(node(7003, vec![100]));

    let hint_store = Arc::new(HintStore::new(1000));
    let coordinator = Arc::new(WriteCoordinator::new(
        Arc::clone(&cm),
        ep(7001),
        hint_store,
    ));
    (cm, coordinator)
}

fn test_mutation() -> CoordinatedMutation {
    CoordinatedMutation::simple(
        "ks".to_string(),
        "users".to_string(),
        b"user1".to_vec(),
        vec![MutationRow {
            clustering_key: vec![],
            cells: vec![CellMutation {
                column: "name".to_string(),
                value: Some(b"Alice".to_vec()),
                timestamp: 1000,
                ttl: 0,
                is_tombstone: false,
                collection_op: None,
            }],
            is_tombstone: false,
            range_tombstone: None,
        }],
        1000,
    )
}

// ─── WU-20: Write at various CLs ────────────────────────────────

#[test]
fn test_write_mutations_various_cls() {
    let (_cm, coordinator) = setup_cluster();
    let strategy = SimpleStrategy::new(3);
    let snitch = SimpleSnitch;
    let mutation = test_mutation();

    // CL=ONE: needs 1 ack
    let r = coordinator
        .coordinate_write(&mutation, ConsistencyLevel::One, &strategy, &snitch)
        .unwrap();
    assert_eq!(r.acks_required, 1);
    assert!(r.acks_received >= 1);

    // CL=QUORUM: needs 2 acks (RF=3)
    let r = coordinator
        .coordinate_write(&mutation, ConsistencyLevel::Quorum, &strategy, &snitch)
        .unwrap();
    assert_eq!(r.acks_required, 2);
    assert!(r.acks_received >= 2);

    // CL=ALL: needs 3 acks (RF=3)
    let r = coordinator
        .coordinate_write(&mutation, ConsistencyLevel::All, &strategy, &snitch)
        .unwrap();
    assert_eq!(r.acks_required, 3);
    assert_eq!(r.acks_received, 3);
}

// ─── WU-20: Dead replica hint storage ────────────────────────────

#[test]
fn test_dead_replica_hint_storage() {
    let (cm, coordinator) = setup_cluster();
    let strategy = SimpleStrategy::new(3);
    let snitch = SimpleSnitch;

    // Mark one replica dead
    cm.mark_dead(&ep(7002));

    // CL=ONE should still succeed with hints stored for dead replica
    let r = coordinator
        .coordinate_write(&test_mutation(), ConsistencyLevel::One, &strategy, &snitch)
        .unwrap();
    assert!(r.acks_received >= 1);
    assert!(
        r.hints_stored >= 1,
        "Expected at least 1 hint stored for dead replica"
    );
}

// ─── WU-20: Batch atomicity (logged) ────────────────────────────

#[test]
fn test_batch_atomicity_logged() {
    let (_cm, coordinator) = setup_cluster();
    let strategy = SimpleStrategy::new(3);
    let snitch = SimpleSnitch;

    let batchlog = Arc::new(BatchLogManager::new());
    let batch_coord = BatchCoordinator::new(Arc::clone(&coordinator), Arc::clone(&batchlog));
    let mutations = vec![test_mutation()];

    let result = batch_coord.execute_batch(
        BatchType::Logged,
        mutations,
        ConsistencyLevel::One,
        &strategy,
        &snitch,
    );
    assert!(result.is_ok());

    // Logged batch: batchlog should have been stored then removed
    // After success, metrics show store count >= 1 and remove count >= 1
    assert!(
        batchlog.metrics.batches_stored.load(Ordering::Relaxed) >= 1,
        "Logged batch must store to batchlog"
    );
    assert!(
        batchlog.metrics.batches_removed.load(Ordering::Relaxed) >= 1,
        "Logged batch must remove from batchlog after success"
    );
}

// ─── WU-20: Batch atomicity (unlogged) ──────────────────────────

#[test]
fn test_batch_atomicity_unlogged() {
    let (_cm, coordinator) = setup_cluster();
    let strategy = SimpleStrategy::new(3);
    let snitch = SimpleSnitch;

    let batchlog = Arc::new(BatchLogManager::new());
    let batch_coord = BatchCoordinator::new(Arc::clone(&coordinator), Arc::clone(&batchlog));
    let mutations = vec![test_mutation()];

    let result = batch_coord.execute_batch(
        BatchType::Unlogged,
        mutations,
        ConsistencyLevel::One,
        &strategy,
        &snitch,
    );
    assert!(result.is_ok());

    // Unlogged batch: batchlog should NOT have been used
    assert_eq!(
        batchlog.metrics.batches_stored.load(Ordering::Relaxed),
        0,
        "Unlogged batch must not store to batchlog"
    );
}

// ─── WU-20: Write timeout behavior ──────────────────────────────

#[test]
fn test_write_timeout_behavior() {
    let (cm, coordinator) = setup_cluster();
    let strategy = SimpleStrategy::new(3);
    let snitch = SimpleSnitch;

    // Kill 2 of 3 replicas: CL=ALL cannot be satisfied
    cm.mark_dead(&ep(7002));
    cm.mark_dead(&ep(7003));

    let result = coordinator.coordinate_write(
        &test_mutation(),
        ConsistencyLevel::All,
        &strategy,
        &snitch,
    );
    assert!(result.is_err());
    match result.unwrap_err() {
        WriteError::Unavailable {
            cl,
            required,
            alive,
        } => {
            assert_eq!(cl, ConsistencyLevel::All);
            assert_eq!(required, 3);
            assert_eq!(alive, 1);
        }
        e => panic!("Expected Unavailable for insufficient acks, got {e:?}"),
    }
}

// ─── WU-20: WriteFailure with failure map ────────────────────────

#[test]
fn test_write_failure_with_failure_map() {
    // Construct a WriteFailure error and verify its structure matches
    // the Java native protocol error (error code 0x1500).
    let mut failure_map = HashMap::new();
    failure_map.insert(ep(7002), 0x0001u16); // RequestFailureReason

    let err = WriteError::WriteFailure {
        cl: ConsistencyLevel::Quorum,
        write_type: WriteType::Simple,
        required: 2,
        received: 1,
        block_for: 2,
        num_failures: 1,
        failure_map: failure_map.clone(),
    };

    assert_eq!(err.error_code(), 0x1500);

    // Verify the failure map contains the expected endpoint
    match &err {
        WriteError::WriteFailure {
            failure_map: fm, ..
        } => {
            assert_eq!(fm.len(), 1);
            assert!(fm.contains_key(&ep(7002)));
        }
        _ => unreachable!(),
    }
}
