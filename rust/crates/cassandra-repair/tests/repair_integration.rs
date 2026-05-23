// Licensed under Apache License, Version 2.0.

//! Integration tests for the cassandra-repair crate.
//!
//! Exercises the full repair flow across multiple modules.

use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;

use uuid::Uuid;

use cassandra_cluster_metadata::Endpoint;
use cassandra_common::Token;
use cassandra_repair::anti_compaction::{classify_partitions, is_range_fully_repaired};
use cassandra_repair::consistent_coordinator::{CoordinatorAction, CoordinatorSession};
use cassandra_repair::consistent_local::{
    InMemoryLocalSessionStore, LocalSession, LocalSessionStore,
};
use cassandra_repair::coordinator::{RepairCoordinator, RepairType};
use cassandra_repair::job::run_repair_job;
use cassandra_repair::merkle::MerkleTree;
use cassandra_repair::messages::ConsistentSessionState;
use cassandra_repair::options::{RepairOption, RepairParallelism};
use cassandra_repair::virtual_tables::{InMemoryRepairHistory, RepairHistoryQuery};

fn ep(port: u16) -> Endpoint {
    Endpoint::new(SocketAddr::new(
        IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
        port,
    ))
}

fn tok(v: i64) -> Token {
    Token::from_raw(v)
}

/// Test 1: Full repair flow — validate with pre-computed hashes, diff, verify sync descriptors.
#[test]
fn full_repair_flow_validate_diff_sync() {
    use cassandra_repair::merkle::hash_partition;
    let range = (tok(0), tok(1000));

    // Build trees directly with known hashes at known tokens.
    let parts_a = vec![
        (tok(100), hash_partition(b"k1", b"data_a")),
        (tok(500), hash_partition(b"k2", b"shared")),
    ];
    let parts_b = vec![
        (tok(100), hash_partition(b"k1", b"data_b")),
        (tok(500), hash_partition(b"k2", b"shared")),
    ];

    let tree_a = MerkleTree::build(range.0, range.1, 3, &parts_a);
    let tree_b = MerkleTree::build(range.0, range.1, 3, &parts_b);

    assert!(tree_a.total_partitions() > 0);
    assert!(tree_b.total_partitions() > 0);

    let mut trees = HashMap::new();
    trees.insert(ep(7001), tree_a);
    trees.insert(ep(7002), tree_b);

    let result = run_repair_job(trees, "ks", "t1");
    assert_eq!(result.trees_received, 2);
    assert!(
        !result.diffs.is_empty(),
        "Different data should produce diffs"
    );
    assert!(
        !result.sync_tasks.is_empty(),
        "Diffs should produce sync tasks"
    );

    for task in &result.sync_tasks {
        assert_eq!(task.keyspace, "ks");
        assert_eq!(task.table, "t1");
        assert!(!task.ranges.is_empty());
    }
}

/// Test 2: Consistent repair state machine — full prepare/propose/commit.
#[test]
fn consistent_repair_coordinator_and_local() {
    let parent_id = Uuid::new_v4();
    let participants = vec![ep(7001), ep(7002)];

    // Coordinator side
    let mut coord = CoordinatorSession::new(
        parent_id,
        participants.clone(),
        "ks".into(),
        vec!["t1".into()],
        vec![(tok(0), tok(100))],
    );

    let prepare_msgs = coord.prepare();
    assert_eq!(prepare_msgs.len(), 2);

    // Participant side
    let store = InMemoryLocalSessionStore::default();
    let mut local_sessions = Vec::new();

    for (endpoint, req) in &prepare_msgs {
        let (session, resp) = LocalSession::handle_prepare(req, ep(9000), *endpoint);
        store.save(&session).unwrap();
        local_sessions.push(session);

        let action = coord.handle_prepare_response(resp);
        if prepare_msgs.len() == 2 && local_sessions.len() < 2 {
            assert_eq!(action, CoordinatorAction::WaitForMore);
        }
    }

    assert_eq!(coord.state, ConsistentSessionState::Prepared);

    // Repair complete
    coord.repair_complete();
    assert_eq!(coord.state, ConsistentSessionState::FinalizeProposing);

    // Propose finalize
    let propose_msgs = coord.propose_finalize();
    assert_eq!(propose_msgs.len(), 2);

    for (i, local) in local_sessions.iter_mut().enumerate() {
        let promise = local.handle_finalize_propose(participants[i]);
        assert!(promise.success);
        coord.handle_promise(promise);
    }

    assert_eq!(coord.state, ConsistentSessionState::FinalizePromised);

    // Commit
    let commit_msgs = coord.commit();
    assert_eq!(commit_msgs.len(), 2);
    assert_eq!(coord.state, ConsistentSessionState::Committed);

    for local in &mut local_sessions {
        local.handle_commit().unwrap();
        assert_eq!(local.state, ConsistentSessionState::Committed);
    }
}

/// Test 3: No-diff repair — identical data completes immediately.
#[test]
fn no_diff_repair_completes_immediately() {
    use cassandra_repair::merkle::hash_partition;
    let range = (tok(0), tok(1000));
    let parts = vec![
        (tok(100), hash_partition(b"k1", b"d1")),
        (tok(500), hash_partition(b"k2", b"d2")),
    ];

    let tree_a = MerkleTree::build(range.0, range.1, 3, &parts);
    let tree_b = MerkleTree::build(range.0, range.1, 3, &parts);

    assert_eq!(tree_a.root_hash, tree_b.root_hash);

    let mut trees = HashMap::new();
    trees.insert(ep(7001), tree_a);
    trees.insert(ep(7002), tree_b);

    let result = run_repair_job(trees, "ks", "t1");
    assert!(result.diffs.is_empty());
    assert!(result.sync_tasks.is_empty());
}

/// Test 4: Repair with cancellation — cancel mid-flow, verify cleanup.
#[test]
fn repair_with_cancellation() {
    let rc = RepairCoordinator::new();
    let ranges = vec![(tok(0), tok(100)), (tok(100), tok(200))];
    let replicas = vec![ep(7001), ep(7002)];

    let _id = rc
        .start_repair(RepairType::Full, "ks", &["t1".into()], &ranges, &replicas)
        .unwrap();

    assert!(rc.status().is_some());
    assert!(!rc.is_cancelled());

    rc.cancel();
    assert!(rc.is_cancelled());
    assert!(rc.status().is_none()); // Cleaned up
    assert_eq!(rc.active_session_count(), 0);
}

/// Test 5: Anti-compaction partition classification.
#[test]
fn anti_compaction_classification() {
    let repaired_ranges = vec![(tok(0), tok(100)), (tok(200), tok(300))];

    // Classify tokens
    let tokens = vec![tok(50), tok(150), tok(250), tok(350)];
    let (repaired, unrepaired) = classify_partitions(&tokens, &repaired_ranges);
    assert_eq!(repaired.len(), 2); // 50 and 250
    assert_eq!(unrepaired.len(), 2); // 150 and 350

    // Range coverage check
    assert!(is_range_fully_repaired(
        (tok(0), tok(100)),
        &repaired_ranges
    ));
    assert!(!is_range_fully_repaired(
        (tok(0), tok(200)),
        &repaired_ranges
    ));
}

/// Test 6: Repair history tracking via InMemoryRepairHistory.
#[test]
fn repair_history_tracking() {
    let history = Arc::new(InMemoryRepairHistory::new());
    let rc = RepairCoordinator::with_tracker(history.clone());

    let ranges = vec![(tok(0), tok(100))];
    let replicas = vec![ep(7001)];

    let id = rc
        .start_repair(RepairType::Full, "ks", &["t1".into()], &ranges, &replicas)
        .unwrap();

    // History should have entries
    assert!(!history.is_empty());

    let all = history.query(&RepairHistoryQuery::default());
    assert!(all.iter().any(|e| e.parent_id == id));

    // Query by keyspace
    let ks_results = history.query(&RepairHistoryQuery {
        keyspace: Some("ks".into()),
        ..Default::default()
    });
    assert!(!ks_results.is_empty());

    let other_ks = history.query(&RepairHistoryQuery {
        keyspace: Some("nonexistent".into()),
        ..Default::default()
    });
    assert!(other_ks.is_empty());
}

/// Test 7: Parallelism modes and repair options.
#[test]
fn parallelism_modes_and_options() {
    let opt_seq = RepairOption::builder("ks")
        .parallelism(RepairParallelism::Sequential)
        .build();
    assert_eq!(opt_seq.parallelism, RepairParallelism::Sequential);

    let opt_par = RepairOption::builder("ks")
        .parallelism(RepairParallelism::Parallel)
        .tables(vec!["t1".into(), "t2".into()])
        .ranges(vec![(tok(0), tok(100))])
        .repair_type(RepairType::Incremental)
        .incremental(true)
        .build();
    assert_eq!(opt_par.parallelism, RepairParallelism::Parallel);
    assert_eq!(opt_par.tables.len(), 2);
    assert!(opt_par.incremental);

    let opt_dc = RepairOption::builder("ks")
        .parallelism(RepairParallelism::DatacenterAware)
        .data_centers(vec!["dc1".into(), "dc2".into()])
        .build();
    assert_eq!(opt_dc.parallelism, RepairParallelism::DatacenterAware);
    assert_eq!(opt_dc.data_centers.len(), 2);

    // Serde round-trip
    let json = serde_json::to_string(&opt_par).unwrap();
    let deser: RepairOption = serde_json::from_str(&json).unwrap();
    assert_eq!(deser.parallelism, RepairParallelism::Parallel);
}
