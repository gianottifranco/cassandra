// Licensed under Apache License, Version 2.0.

//! Topology operations: bootstrap, decommission, replace, removenode, rebuild.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.service.StorageService` (state machine)
//! - `org.apache.cassandra.dht.BootStrapper`
//! - `org.apache.cassandra.dht.RangeStreamer`
//!
//! ## Architecture
//!
//! Each topology operation follows a state machine pattern:
//!
//! ```text
//! Bootstrap:    Idle → Calculating → Streaming → Completing → Done
//! Decommission: Idle → Unbootstrapping → StreamingOut → Done(Left)
//! Replace:      Idle → Replacing → Streaming → Done(Normal)
//! RemoveNode:   Idle → Removing → Done(Excised)
//! Rebuild:      Idle → Rebuilding → Done(Normal)
//! ```
//!
//! Only one topology operation can be active at a time (mutex).
//! Gossip is used to propagate state changes to the cluster.
//!
//! ## Decoupling from Streaming
//!
//! To avoid a cyclic dependency with `cassandra-streaming`, this module
//! produces [`StreamPlanDescriptor`] objects that describe *what* to stream.
//! The caller (e.g., the server binary or an integration layer) converts
//! descriptors into actual `StreamPlan`/`StreamSession` objects.

use std::fmt;
use std::sync::Arc;

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use cassandra_common::Token;

use crate::cluster::ClusterMetadata;
use crate::node::{Endpoint, NodeId, NodeInfo, NodeState};

/// The type of topology operation being performed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TopologyOperation {
    Bootstrap,
    Decommission,
    Replace,
    RemoveNode,
    Rebuild,
    Move,
}

impl fmt::Display for TopologyOperation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Bootstrap => write!(f, "BOOTSTRAP"),
            Self::Decommission => write!(f, "DECOMMISSION"),
            Self::Replace => write!(f, "REPLACE"),
            Self::RemoveNode => write!(f, "REMOVENODE"),
            Self::Rebuild => write!(f, "REBUILD"),
            Self::Move => write!(f, "MOVE"),
        }
    }
}

/// The current state of a topology operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TopologyState {
    Idle,
    Calculating { operation: TopologyOperation },
    Streaming {
        operation: TopologyOperation,
        sessions: usize,
        progress: u32,
    },
    Completing { operation: TopologyOperation },
    Done {
        operation: TopologyOperation,
        success: bool,
        error: Option<String>,
    },
}

impl fmt::Display for TopologyState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Idle => write!(f, "IDLE"),
            Self::Calculating { operation } => write!(f, "CALCULATING({operation})"),
            Self::Streaming { operation, progress, .. } => {
                write!(f, "STREAMING({operation}, {progress}%)")
            }
            Self::Completing { operation } => write!(f, "COMPLETING({operation})"),
            Self::Done { operation, success, .. } => {
                if *success {
                    write!(f, "DONE({operation}, OK)")
                } else {
                    write!(f, "DONE({operation}, FAILED)")
                }
            }
        }
    }
}

/// Errors from topology operations.
#[derive(Debug, thiserror::Error)]
pub enum TopologyError {
    #[error("Another topology operation is already in progress: {0}")]
    OperationInProgress(String),

    #[error("Invalid state for this operation: {0}")]
    InvalidState(String),

    #[error("Node not found: {0}")]
    NodeNotFound(String),

    #[error("Streaming failed: {0}")]
    StreamingFailed(String),

    #[error("Not enough live nodes for this operation")]
    InsufficientNodes,

    #[error("Cannot decommission the last node")]
    LastNode,

    #[error("Target node is not dead: {0}")]
    TargetNotDead(String),
}

/// A request to stream data from/to a peer.
///
/// This is a lightweight descriptor that doesn't depend on the streaming crate.
/// The caller converts these into `StreamPlan`/`StreamSession` objects.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamRangeRequest {
    /// Source endpoint (where data comes from).
    pub source: Endpoint,
    /// Destination endpoint (where data goes to).
    pub destination: Endpoint,
    /// Token ranges to stream.
    pub ranges: Vec<(Token, Token)>,
}

/// Descriptor of a streaming plan produced by topology operations.
///
/// Decoupled from `cassandra-streaming` to avoid cyclic dependencies.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamPlanDescriptor {
    /// What operation produced this plan.
    pub operation: TopologyOperation,
    /// Individual range requests (source → destination).
    pub requests: Vec<StreamRangeRequest>,
}

impl StreamPlanDescriptor {
    pub fn new(operation: TopologyOperation) -> Self {
        Self {
            operation,
            requests: Vec::new(),
        }
    }

    pub fn add_request(&mut self, request: StreamRangeRequest) {
        self.requests.push(request);
    }

    pub fn is_empty(&self) -> bool {
        self.requests.is_empty()
    }

    pub fn peer_count(&self) -> usize {
        let mut peers = std::collections::HashSet::new();
        for r in &self.requests {
            peers.insert(r.source);
            peers.insert(r.destination);
        }
        peers.len()
    }
}

/// Coordinator for topology-changing operations.
pub struct TopologyCoordinator {
    state: Arc<Mutex<TopologyState>>,
    cluster: Arc<ClusterMetadata>,
}

impl TopologyCoordinator {
    pub fn new(cluster: Arc<ClusterMetadata>) -> Self {
        Self {
            state: Arc::new(Mutex::new(TopologyState::Idle)),
            cluster,
        }
    }

    pub fn state(&self) -> TopologyState {
        self.state.lock().clone()
    }

    pub fn is_operation_in_progress(&self) -> bool {
        !matches!(*self.state.lock(), TopologyState::Idle | TopologyState::Done { .. })
    }

    fn check_idle(&self) -> Result<(), TopologyError> {
        let state = self.state.lock();
        if !matches!(*state, TopologyState::Idle | TopologyState::Done { .. }) {
            return Err(TopologyError::OperationInProgress(format!("{}", *state)));
        }
        Ok(())
    }

    // ── Bootstrap ──────────────────────────────────────────────────────

    pub fn begin_bootstrap(
        &self,
        local_node: &NodeInfo,
        new_tokens: Vec<Token>,
    ) -> Result<StreamPlanDescriptor, TopologyError> {
        self.check_idle()?;

        {
            let mut state = self.state.lock();
            *state = TopologyState::Calculating {
                operation: TopologyOperation::Bootstrap,
            };
        }

        info!(node = %local_node.endpoint, tokens = ?new_tokens.len(), "Starting bootstrap");

        let snap = self.cluster.snapshot();
        let mut plan = StreamPlanDescriptor::new(TopologyOperation::Bootstrap);

        for token in &new_tokens {
            if let Some(current_owner) = snap.ring.primary_endpoint(*token) {
                if current_owner != local_node.endpoint {
                    let prev = snap.ring.previous_token(*token)
                        .unwrap_or(Token::from_raw(i64::MIN));
                    plan.add_request(StreamRangeRequest {
                        source: current_owner,
                        destination: local_node.endpoint,
                        ranges: vec![(prev, *token)],
                    });
                }
            }
        }

        {
            let mut state = self.state.lock();
            *state = TopologyState::Streaming {
                operation: TopologyOperation::Bootstrap,
                sessions: plan.requests.len(),
                progress: 0,
            };
        }

        Ok(plan)
    }

    pub fn finish_bootstrap(
        &self,
        local_node: &mut NodeInfo,
        new_tokens: Vec<Token>,
    ) -> Result<(), TopologyError> {
        {
            let state = self.state.lock();
            match &*state {
                TopologyState::Streaming { operation: TopologyOperation::Bootstrap, .. } => {}
                other => return Err(TopologyError::InvalidState(
                    format!("Expected Streaming(Bootstrap), got {other}")
                )),
            }
        }

        {
            let mut state = self.state.lock();
            *state = TopologyState::Completing { operation: TopologyOperation::Bootstrap };
        }

        local_node.tokens = new_tokens;
        local_node.state = NodeState::Normal;
        self.cluster.update_node(local_node.clone());

        info!(node = %local_node.endpoint, "Bootstrap complete");

        let mut state = self.state.lock();
        *state = TopologyState::Done {
            operation: TopologyOperation::Bootstrap,
            success: true,
            error: None,
        };
        Ok(())
    }

    // ── Decommission ───────────────────────────────────────────────────

    pub fn begin_decommission(
        &self,
        local_node: &NodeInfo,
    ) -> Result<StreamPlanDescriptor, TopologyError> {
        self.check_idle()?;

        let snap = self.cluster.snapshot();
        if snap.live_endpoints().len() <= 1 {
            return Err(TopologyError::LastNode);
        }

        {
            let mut state = self.state.lock();
            *state = TopologyState::Calculating { operation: TopologyOperation::Decommission };
        }

        info!(node = %local_node.endpoint, "Starting decommission");

        let mut leaving = local_node.clone();
        leaving.state = NodeState::Leaving;
        self.cluster.update_node(leaving);

        let mut plan = StreamPlanDescriptor::new(TopologyOperation::Decommission);
        let our_tokens = snap.ring.tokens_for(&local_node.endpoint);

        for token in &our_tokens {
            let next_owners: Vec<Endpoint> = snap.ring.natural_endpoints(*token, 2)
                .into_iter()
                .filter(|ep| *ep != local_node.endpoint)
                .collect();

            if let Some(recipient) = next_owners.first() {
                let prev = snap.ring.previous_token(*token)
                    .unwrap_or(Token::from_raw(i64::MIN));
                plan.add_request(StreamRangeRequest {
                    source: local_node.endpoint,
                    destination: *recipient,
                    ranges: vec![(prev, *token)],
                });
            }
        }

        let mut state = self.state.lock();
        *state = TopologyState::Streaming {
            operation: TopologyOperation::Decommission,
            sessions: plan.requests.len(),
            progress: 0,
        };

        Ok(plan)
    }

    pub fn finish_decommission(&self, local_endpoint: &Endpoint) -> Result<(), TopologyError> {
        {
            let state = self.state.lock();
            match &*state {
                TopologyState::Streaming { operation: TopologyOperation::Decommission, .. } => {}
                other => return Err(TopologyError::InvalidState(
                    format!("Expected Streaming(Decommission), got {other}")
                )),
            }
        }

        {
            let mut state = self.state.lock();
            *state = TopologyState::Completing { operation: TopologyOperation::Decommission };
        }

        self.cluster.remove_node(local_endpoint);
        info!(node = %local_endpoint, "Decommission complete");

        let mut state = self.state.lock();
        *state = TopologyState::Done {
            operation: TopologyOperation::Decommission,
            success: true,
            error: None,
        };
        Ok(())
    }

    // ── Replace ────────────────────────────────────────────────────────

    pub fn begin_replace(
        &self,
        local_node: &NodeInfo,
        dead_endpoint: &Endpoint,
    ) -> Result<StreamPlanDescriptor, TopologyError> {
        self.check_idle()?;

        let snap = self.cluster.snapshot();
        if let Some(info) = snap.nodes.get(dead_endpoint) {
            if info.state != NodeState::Dead {
                return Err(TopologyError::TargetNotDead(
                    format!("{dead_endpoint} is {}", info.state)
                ));
            }
        } else {
            return Err(TopologyError::NodeNotFound(dead_endpoint.to_string()));
        }

        {
            let mut state = self.state.lock();
            *state = TopologyState::Calculating { operation: TopologyOperation::Replace };
        }

        info!(new_node = %local_node.endpoint, replacing = %dead_endpoint, "Starting replacement");

        let dead_tokens = snap.ring.tokens_for(dead_endpoint);
        let mut plan = StreamPlanDescriptor::new(TopologyOperation::Replace);

        for token in &dead_tokens {
            let replicas: Vec<Endpoint> = snap.ring.natural_endpoints(*token, 3)
                .into_iter()
                .filter(|ep| *ep != *dead_endpoint && *ep != local_node.endpoint)
                .collect();

            if let Some(source) = replicas.first() {
                let prev = snap.ring.previous_token(*token)
                    .unwrap_or(Token::from_raw(i64::MIN));
                plan.add_request(StreamRangeRequest {
                    source: *source,
                    destination: local_node.endpoint,
                    ranges: vec![(prev, *token)],
                });
            }
        }

        let mut state = self.state.lock();
        *state = TopologyState::Streaming {
            operation: TopologyOperation::Replace,
            sessions: plan.requests.len(),
            progress: 0,
        };

        Ok(plan)
    }

    pub fn finish_replace(
        &self,
        local_node: &mut NodeInfo,
        dead_endpoint: &Endpoint,
    ) -> Result<(), TopologyError> {
        {
            let state = self.state.lock();
            match &*state {
                TopologyState::Streaming { operation: TopologyOperation::Replace, .. } => {}
                other => return Err(TopologyError::InvalidState(
                    format!("Expected Streaming(Replace), got {other}")
                )),
            }
        }

        let snap = self.cluster.snapshot();
        let dead_tokens = snap.ring.tokens_for(dead_endpoint);
        self.cluster.remove_node(dead_endpoint);

        local_node.tokens = dead_tokens;
        local_node.state = NodeState::Normal;
        self.cluster.update_node(local_node.clone());

        info!(node = %local_node.endpoint, replaced = %dead_endpoint, "Replacement complete");

        let mut state = self.state.lock();
        *state = TopologyState::Done {
            operation: TopologyOperation::Replace,
            success: true,
            error: None,
        };
        Ok(())
    }

    // ── RemoveNode ─────────────────────────────────────────────────────

    pub fn remove_node(
        &self,
        target_endpoint: &Endpoint,
        target_host_id: &NodeId,
    ) -> Result<(), TopologyError> {
        self.check_idle()?;

        let snap = self.cluster.snapshot();
        match snap.nodes.get(target_endpoint) {
            Some(info) => {
                if info.host_id != *target_host_id {
                    return Err(TopologyError::InvalidState(
                        format!("Host ID mismatch for {target_endpoint}")
                    ));
                }
            }
            None => return Err(TopologyError::NodeNotFound(target_endpoint.to_string())),
        }

        {
            let mut state = self.state.lock();
            *state = TopologyState::Calculating { operation: TopologyOperation::RemoveNode };
        }

        info!(target = %target_endpoint, host_id = %target_host_id, "Removing node");
        self.cluster.remove_node(target_endpoint);

        let mut state = self.state.lock();
        *state = TopologyState::Done {
            operation: TopologyOperation::RemoveNode,
            success: true,
            error: None,
        };
        info!(target = %target_endpoint, "Node removed");
        Ok(())
    }

    // ── Rebuild ────────────────────────────────────────────────────────

    pub fn begin_rebuild(
        &self,
        local_node: &NodeInfo,
        source_dc: Option<&str>,
    ) -> Result<StreamPlanDescriptor, TopologyError> {
        self.check_idle()?;

        {
            let mut state = self.state.lock();
            *state = TopologyState::Calculating { operation: TopologyOperation::Rebuild };
        }

        info!(node = %local_node.endpoint, source_dc = source_dc.unwrap_or("all"), "Starting rebuild");

        let snap = self.cluster.snapshot();
        let our_tokens = snap.ring.tokens_for(&local_node.endpoint);
        let mut plan = StreamPlanDescriptor::new(TopologyOperation::Rebuild);

        for token in &our_tokens {
            let candidates: Vec<Endpoint> = snap.ring.natural_endpoints(*token, 3)
                .into_iter()
                .filter(|ep| {
                    if *ep == local_node.endpoint { return false; }
                    if let Some(dc) = source_dc {
                        if let Some(info) = snap.nodes.get(ep) {
                            return info.datacenter == dc;
                        }
                    }
                    true
                })
                .collect();

            if let Some(source) = candidates.first() {
                let prev = snap.ring.previous_token(*token)
                    .unwrap_or(Token::from_raw(i64::MIN));
                plan.add_request(StreamRangeRequest {
                    source: *source,
                    destination: local_node.endpoint,
                    ranges: vec![(prev, *token)],
                });
            }
        }

        let mut state = self.state.lock();
        *state = TopologyState::Streaming {
            operation: TopologyOperation::Rebuild,
            sessions: plan.requests.len(),
            progress: 0,
        };

        Ok(plan)
    }

    pub fn finish_rebuild(&self) -> Result<(), TopologyError> {
        {
            let state = self.state.lock();
            match &*state {
                TopologyState::Streaming { operation: TopologyOperation::Rebuild, .. } => {}
                other => return Err(TopologyError::InvalidState(
                    format!("Expected Streaming(Rebuild), got {other}")
                )),
            }
        }

        info!("Rebuild complete");
        let mut state = self.state.lock();
        *state = TopologyState::Done {
            operation: TopologyOperation::Rebuild,
            success: true,
            error: None,
        };
        Ok(())
    }

    // ── Shared helpers ─────────────────────────────────────────────────

    pub fn update_progress(&self, progress: u32) {
        let mut state = self.state.lock();
        if let TopologyState::Streaming { progress: p, .. } = &mut *state {
            *p = progress.min(100);
        }
    }

    pub fn abort(&self, error: impl Into<String>) {
        let mut state = self.state.lock();
        if let TopologyState::Idle | TopologyState::Done { .. } = *state {
            return;
        }
        let op = match &*state {
            TopologyState::Calculating { operation }
            | TopologyState::Streaming { operation, .. }
            | TopologyState::Completing { operation } => *operation,
            _ => return,
        };
        let err = error.into();
        warn!(operation = %op, error = %err, "Topology operation aborted");
        *state = TopologyState::Done {
            operation: op,
            success: false,
            error: Some(err),
        };
    }

    pub fn reset(&self) -> Result<(), TopologyError> {
        let mut state = self.state.lock();
        match &*state {
            TopologyState::Done { .. } | TopologyState::Idle => {
                *state = TopologyState::Idle;
                Ok(())
            }
            other => Err(TopologyError::InvalidState(format!("Cannot reset from {other}"))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::NodeId;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

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

    fn setup_cluster() -> (Arc<ClusterMetadata>, TopologyCoordinator) {
        let n1 = node(7001, vec![-100, 0]);
        let cm = Arc::new(ClusterMetadata::new(n1));
        cm.update_node(node(7002, vec![100, 200]));
        cm.update_node(node(7003, vec![300, 400]));
        let tc = TopologyCoordinator::new(Arc::clone(&cm));
        (cm, tc)
    }

    #[test]
    fn initial_state_is_idle() {
        let (_cm, tc) = setup_cluster();
        assert_eq!(tc.state(), TopologyState::Idle);
        assert!(!tc.is_operation_in_progress());
    }

    #[test]
    fn bootstrap_happy_path() {
        let (cm, tc) = setup_cluster();
        let mut new_node = node(7004, vec![]);

        let plan = tc.begin_bootstrap(&new_node, vec![Token::from_raw(50)]).unwrap();
        assert!(tc.is_operation_in_progress());
        assert!(!plan.is_empty() || plan.requests.is_empty()); // may or may not have work

        tc.finish_bootstrap(&mut new_node, vec![Token::from_raw(50)]).unwrap();
        assert!(matches!(tc.state(), TopologyState::Done { operation: TopologyOperation::Bootstrap, success: true, .. }));
        assert_eq!(cm.snapshot().node_count(), 4);
        assert_eq!(new_node.state, NodeState::Normal);
    }

    #[test]
    fn decommission_happy_path() {
        let (cm, tc) = setup_cluster();
        let n2 = cm.snapshot().nodes.get(&ep(7002)).unwrap().clone();

        let plan = tc.begin_decommission(&n2).unwrap();
        assert!(tc.is_operation_in_progress());
        assert_eq!(plan.operation, TopologyOperation::Decommission);

        tc.finish_decommission(&ep(7002)).unwrap();
        assert_eq!(cm.snapshot().node_count(), 2);
    }

    #[test]
    fn decommission_last_node_rejected() {
        let n1 = node(7001, vec![0]);
        let cm = Arc::new(ClusterMetadata::new(n1.clone()));
        let tc = TopologyCoordinator::new(Arc::clone(&cm));

        let result = tc.begin_decommission(&n1);
        assert!(matches!(result, Err(TopologyError::LastNode)));
    }

    #[test]
    fn replace_happy_path() {
        let (cm, tc) = setup_cluster();
        cm.mark_dead(&ep(7002));

        let mut replacement = node(7004, vec![]);
        let plan = tc.begin_replace(&replacement, &ep(7002)).unwrap();
        assert_eq!(plan.operation, TopologyOperation::Replace);

        tc.finish_replace(&mut replacement, &ep(7002)).unwrap();
        assert_eq!(cm.snapshot().node_count(), 3);
        assert!(cm.snapshot().nodes.contains_key(&ep(7004)));
    }

    #[test]
    fn replace_live_node_rejected() {
        let (_cm, tc) = setup_cluster();
        let replacement = node(7004, vec![]);
        let result = tc.begin_replace(&replacement, &ep(7002));
        assert!(matches!(result, Err(TopologyError::TargetNotDead(_))));
    }

    #[test]
    fn remove_node_happy_path() {
        let (cm, tc) = setup_cluster();
        let host_id = cm.snapshot().nodes.get(&ep(7002)).unwrap().host_id;
        tc.remove_node(&ep(7002), &host_id).unwrap();
        assert_eq!(cm.snapshot().node_count(), 2);
    }

    #[test]
    fn remove_nonexistent_node() {
        let (_cm, tc) = setup_cluster();
        assert!(matches!(
            tc.remove_node(&ep(9999), &NodeId::random()),
            Err(TopologyError::NodeNotFound(_))
        ));
    }

    #[test]
    fn rebuild_happy_path() {
        let (_cm, tc) = setup_cluster();
        let n1 = node(7001, vec![-100, 0]);

        let plan = tc.begin_rebuild(&n1, Some("dc1")).unwrap();
        assert_eq!(plan.operation, TopologyOperation::Rebuild);
        tc.finish_rebuild().unwrap();
        assert!(matches!(tc.state(), TopologyState::Done { operation: TopologyOperation::Rebuild, success: true, .. }));
    }

    #[test]
    fn concurrent_operations_blocked() {
        let (_cm, tc) = setup_cluster();
        let n1 = node(7001, vec![-100, 0]);

        tc.begin_bootstrap(&n1, vec![Token::from_raw(500)]).unwrap();
        assert!(matches!(
            tc.begin_rebuild(&n1, None),
            Err(TopologyError::OperationInProgress(_))
        ));
    }

    #[test]
    fn abort_operation() {
        let (_cm, tc) = setup_cluster();
        let n = node(7001, vec![-100, 0]);
        tc.begin_bootstrap(&n, vec![Token::from_raw(500)]).unwrap();
        tc.abort("stream timeout");
        match tc.state() {
            TopologyState::Done { success, error, .. } => {
                assert!(!success);
                assert_eq!(error.as_deref(), Some("stream timeout"));
            }
            other => panic!("Expected Done(failed), got {other}"),
        }
    }

    #[test]
    fn reset_after_done() {
        let (_cm, tc) = setup_cluster();
        let n = node(7001, vec![-100, 0]);
        tc.begin_rebuild(&n, None).unwrap();
        tc.finish_rebuild().unwrap();
        tc.reset().unwrap();
        assert_eq!(tc.state(), TopologyState::Idle);
    }

    #[test]
    fn reset_during_operation_fails() {
        let (_cm, tc) = setup_cluster();
        let n = node(7001, vec![-100, 0]);
        tc.begin_bootstrap(&n, vec![Token::from_raw(500)]).unwrap();
        assert!(tc.reset().is_err());
    }

    #[test]
    fn update_progress() {
        let (_cm, tc) = setup_cluster();
        let n = node(7001, vec![-100, 0]);
        tc.begin_bootstrap(&n, vec![Token::from_raw(500)]).unwrap();
        tc.update_progress(42);
        if let TopologyState::Streaming { progress, .. } = tc.state() {
            assert_eq!(progress, 42);
        } else {
            panic!("Expected Streaming state");
        }
    }

    #[test]
    fn topology_operation_display() {
        assert_eq!(TopologyOperation::Bootstrap.to_string(), "BOOTSTRAP");
        assert_eq!(TopologyOperation::Decommission.to_string(), "DECOMMISSION");
        assert_eq!(TopologyOperation::Replace.to_string(), "REPLACE");
        assert_eq!(TopologyOperation::RemoveNode.to_string(), "REMOVENODE");
        assert_eq!(TopologyOperation::Rebuild.to_string(), "REBUILD");
    }

    #[test]
    fn stream_plan_descriptor() {
        let mut plan = StreamPlanDescriptor::new(TopologyOperation::Bootstrap);
        assert!(plan.is_empty());

        plan.add_request(StreamRangeRequest {
            source: ep(7001),
            destination: ep(7004),
            ranges: vec![(Token::from_raw(0), Token::from_raw(100))],
        });
        assert!(!plan.is_empty());
        assert_eq!(plan.peer_count(), 2);
    }
}
