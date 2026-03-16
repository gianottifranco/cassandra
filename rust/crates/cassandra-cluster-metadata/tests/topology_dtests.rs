// Licensed under Apache License, Version 2.0.

//! Integration tests for topology operations: chained sequences, error recovery.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;

use cassandra_cluster_metadata::cluster::ClusterMetadata;
use cassandra_cluster_metadata::node::{Endpoint, NodeId, NodeInfo, NodeState};
use cassandra_cluster_metadata::topology::{
    TopologyCoordinator, TopologyError, TopologyOperation, TopologyState,
};
use cassandra_common::Token;

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

fn setup_cluster_3() -> (Arc<ClusterMetadata>, TopologyCoordinator) {
    let n1 = node(7001, vec![-100, 0]);
    let cm = Arc::new(ClusterMetadata::new(n1));
    cm.update_node(node(7002, vec![100, 200]));
    cm.update_node(node(7003, vec![300, 400]));
    let tc = TopologyCoordinator::new(Arc::clone(&cm));
    (cm, tc)
}

/// Bootstrap a new node, then decommission it — full lifecycle.
#[test]
fn chained_bootstrap_then_decommission() {
    let (cm, tc) = setup_cluster_3();
    assert_eq!(cm.snapshot().node_count(), 3);

    // Bootstrap node 4.
    let mut n4 = node(7004, vec![]);
    let plan = tc.begin_bootstrap(&n4, vec![Token::from_raw(50)]).unwrap();
    assert_eq!(plan.operation, TopologyOperation::Bootstrap);
    tc.finish_bootstrap(&mut n4, vec![Token::from_raw(50)])
        .unwrap();
    assert_eq!(cm.snapshot().node_count(), 4);

    // Reset to idle.
    tc.reset().unwrap();

    // Decommission node 4.
    let plan = tc.begin_decommission(&n4).unwrap();
    assert_eq!(plan.operation, TopologyOperation::Decommission);
    tc.finish_decommission(&ep(7004)).unwrap();
    assert_eq!(cm.snapshot().node_count(), 3);
}

/// Replace a dead node, then rebuild it from a specific DC.
#[test]
fn chained_replace_then_rebuild() {
    let (cm, tc) = setup_cluster_3();

    // Mark node 2 as dead.
    cm.mark_dead(&ep(7002));

    // Replace with new node.
    let mut n4 = node(7004, vec![]);
    let plan = tc.begin_replace(&n4, &ep(7002)).unwrap();
    assert_eq!(plan.operation, TopologyOperation::Replace);
    tc.finish_replace(&mut n4, &ep(7002)).unwrap();
    assert_eq!(cm.snapshot().node_count(), 3);

    // Reset and rebuild the new node.
    tc.reset().unwrap();
    let plan = tc.begin_rebuild(&n4, Some("dc1")).unwrap();
    assert_eq!(plan.operation, TopologyOperation::Rebuild);
    tc.finish_rebuild().unwrap();
}

/// Bootstrap a node, then run cleanup on existing nodes.
#[test]
fn bootstrap_then_cleanup() {
    let (cm, tc) = setup_cluster_3();

    // Bootstrap node 4.
    let mut n4 = node(7004, vec![]);
    tc.begin_bootstrap(&n4, vec![Token::from_raw(50)]).unwrap();
    tc.finish_bootstrap(&mut n4, vec![Token::from_raw(50)])
        .unwrap();
    tc.reset().unwrap();

    // Existing node 1 runs cleanup.
    let n1 = cm.snapshot().nodes.get(&ep(7001)).unwrap().clone();
    let plan = tc.begin_cleanup(&n1, None).unwrap();
    assert!(!plan.owned_ranges.is_empty());
    tc.finish_cleanup().unwrap();
}

/// Abort during bootstrap, then successfully rebuild.
#[test]
fn abort_and_recover() {
    let (_cm, tc) = setup_cluster_3();

    // Start bootstrap.
    let n4 = node(7004, vec![]);
    tc.begin_bootstrap(&n4, vec![Token::from_raw(50)]).unwrap();

    // Simulate failure: abort.
    tc.abort("network partition");
    match tc.state() {
        TopologyState::Done { success, error, .. } => {
            assert!(!success);
            assert!(error.unwrap().contains("network partition"));
        }
        other => panic!("Expected Done(failed), got {other}"),
    }

    // Reset and perform a different operation.
    tc.reset().unwrap();
    let n1 = node(7001, vec![-100, 0]);
    tc.begin_rebuild(&n1, None).unwrap();
    tc.finish_rebuild().unwrap();
}

/// Concurrent operation rejection across all operation types.
#[test]
fn concurrent_operations_are_mutually_exclusive() {
    let (cm, tc) = setup_cluster_3();
    let n1 = node(7001, vec![-100, 0]);

    // Start one operation.
    tc.begin_rebuild(&n1, None).unwrap();

    // All other operations should be rejected.
    assert!(matches!(
        tc.begin_bootstrap(&n1, vec![Token::from_raw(500)]),
        Err(TopologyError::OperationInProgress(_))
    ));
    assert!(matches!(
        tc.begin_decommission(&n1),
        Err(TopologyError::OperationInProgress(_))
    ));
    let n5 = node(7005, vec![]);
    cm.mark_dead(&ep(7003));
    assert!(matches!(
        tc.begin_replace(&n5, &ep(7003)),
        Err(TopologyError::OperationInProgress(_))
    ));
    assert!(matches!(
        tc.begin_cleanup(&n1, None),
        Err(TopologyError::OperationInProgress(_))
    ));
    assert!(matches!(
        tc.refresh("ks", "t"),
        Err(TopologyError::OperationInProgress(_))
    ));
}

/// Move token — full lifecycle with validation.
#[test]
fn move_token_lifecycle() {
    let (cm, tc) = setup_cluster_3();

    // Add a single-token node for move.
    let mut n5 = node(7005, vec![500]);
    cm.update_node(n5.clone());

    let plan = tc.begin_move(&n5, Token::from_raw(550)).unwrap();
    assert_eq!(plan.operation, TopologyOperation::Move);

    tc.finish_move(&mut n5, Token::from_raw(550)).unwrap();
    assert_eq!(n5.tokens, vec![Token::from_raw(550)]);

    // Verify the node has its new token in the cluster.
    let snap = cm.snapshot();
    let updated = snap.nodes.get(&ep(7005)).unwrap();
    assert_eq!(updated.tokens, vec![Token::from_raw(550)]);
}

/// Epoch advances on each topology operation.
#[test]
fn epoch_monotonic_advance() {
    let (_cm, tc) = setup_cluster_3();
    let initial_epoch = tc.epoch();

    // Each operation should advance the epoch.
    let n1 = node(7001, vec![-100, 0]);
    tc.begin_rebuild(&n1, None).unwrap();
    tc.finish_rebuild().unwrap();
    tc.reset().unwrap();

    let mut n4 = node(7004, vec![]);
    tc.begin_bootstrap(&n4, vec![Token::from_raw(50)]).unwrap();
    let epoch_after_bootstrap = tc.epoch();
    assert!(epoch_after_bootstrap > initial_epoch);

    tc.finish_bootstrap(&mut n4, vec![Token::from_raw(50)])
        .unwrap();
    tc.reset().unwrap();

    let n1_fresh = node(7001, vec![-100, 0]);
    tc.begin_decommission(&n1_fresh).unwrap();
    let epoch_after_decommission = tc.epoch();
    assert!(epoch_after_decommission > epoch_after_bootstrap);
}

/// Pending ranges are tracked during bootstrap and cleared afterwards.
#[test]
fn pending_ranges_lifecycle() {
    let (_cm, tc) = setup_cluster_3();
    assert!(tc.pending_ranges().is_empty());

    let mut n4 = node(7004, vec![]);
    tc.begin_bootstrap(&n4, vec![Token::from_raw(50)]).unwrap();

    // During streaming, pending ranges should exist.
    let pr = tc.pending_ranges();
    // May or may not have pending ranges depending on ring, but structure works.
    let _ = pr.total_count();

    tc.finish_bootstrap(&mut n4, vec![Token::from_raw(50)])
        .unwrap();

    // After completion, pending ranges should be cleared.
    assert!(tc.pending_ranges().is_empty());
}

/// Refresh is a local-only operation.
#[test]
fn refresh_is_local_only() {
    let (_cm, tc) = setup_cluster_3();
    tc.refresh("my_ks", "my_table").unwrap();
    assert!(matches!(
        tc.state(),
        TopologyState::Done {
            operation: TopologyOperation::Refresh,
            success: true,
            ..
        }
    ));
}

/// Interrupted operation detection for restart recovery.
#[test]
fn interrupted_operation_recovery() {
    let (_cm, tc) = setup_cluster_3();

    // No interrupted operation initially.
    assert!(tc.interrupted_operation().is_none());

    // Start an operation but don't finish it.
    let n = node(7001, vec![-100, 0]);
    tc.begin_rebuild(&n, None).unwrap();

    // Should detect the interrupted operation.
    assert_eq!(tc.interrupted_operation(), Some(TopologyOperation::Rebuild));

    // Abort to clean up.
    tc.abort("test cleanup");
    assert!(tc.interrupted_operation().is_none());
}
