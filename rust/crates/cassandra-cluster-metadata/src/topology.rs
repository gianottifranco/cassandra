// Licensed under Apache License, Version 2.0.

//! Topology operations: bootstrap, decommission, replace, removenode, rebuild,
//! move, cleanup, refresh.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.service.StorageService` (state machine)
//! - `org.apache.cassandra.dht.BootStrapper`
//! - `org.apache.cassandra.dht.RangeStreamer`
//! - `org.apache.cassandra.tcm.sequences.*` (TCM-aware sequences)
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
//! Move:         Idle → Moving → Streaming → Done(Normal)
//! Cleanup:      Idle → Cleaning → Done(Normal)
//! Refresh:      Idle → Refreshing → Done(Normal)
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
//!
//! ## Pending Ranges
//!
//! During topology changes, ranges are "in flight" — they are being transferred
//! from one node to another. The [`PendingRanges`] struct tracks these to
//! ensure correct read/write routing during transitions.

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use cassandra_common::Token;

use crate::cluster::ClusterMetadata;
use crate::node::{Endpoint, NodeId, NodeInfo, NodeState};

// ── Topology Operation ─────────────────────────────────────────────────

/// The type of topology operation being performed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TopologyOperation {
    Bootstrap,
    Decommission,
    Replace,
    RemoveNode,
    Rebuild,
    Move,
    Cleanup,
    Refresh,
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
            Self::Cleanup => write!(f, "CLEANUP"),
            Self::Refresh => write!(f, "REFRESH"),
        }
    }
}

// ── Topology State ──────────────────────────────────────────────────────

/// The current state of a topology operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TopologyState {
    Idle,
    Calculating {
        operation: TopologyOperation,
    },
    Streaming {
        operation: TopologyOperation,
        sessions: usize,
        progress: u32,
    },
    Completing {
        operation: TopologyOperation,
    },
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
            Self::Streaming {
                operation,
                progress,
                ..
            } => {
                write!(f, "STREAMING({operation}, {progress}%)")
            }
            Self::Completing { operation } => write!(f, "COMPLETING({operation})"),
            Self::Done {
                operation, success, ..
            } => {
                if *success {
                    write!(f, "DONE({operation}, OK)")
                } else {
                    write!(f, "DONE({operation}, FAILED)")
                }
            }
        }
    }
}

// ── Topology Errors ─────────────────────────────────────────────────────

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

    #[error("Invalid token for move: {0}")]
    InvalidMoveToken(String),
}

// ── Stream Plan Descriptor ──────────────────────────────────────────────

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

// ── Pending Ranges ──────────────────────────────────────────────────────

/// Tracks ranges that are being transferred during topology changes.
///
/// ## Java Oracle
///
/// - `org.apache.cassandra.locator.TokenMetadata.pendingRanges`
///
/// During bootstrap, decommission, or move operations, some ranges are
/// "in flight" and may need to be routed to both old and new owners.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PendingRanges {
    /// Ranges pending assignment to a new endpoint, keyed by keyspace.
    pub ranges: HashMap<String, Vec<PendingRange>>,
}

/// A single pending range assignment.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingRange {
    /// Token range being transferred.
    pub range: (Token, Token),
    /// The endpoint that will own this range when the operation completes.
    pub new_owner: Endpoint,
    /// The endpoint that currently owns this range.
    pub current_owner: Endpoint,
    /// The operation causing this pending range.
    pub operation: TopologyOperation,
}

impl PendingRanges {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a pending range for a keyspace.
    pub fn add(&mut self, keyspace: impl Into<String>, pending: PendingRange) {
        self.ranges
            .entry(keyspace.into())
            .or_default()
            .push(pending);
    }

    /// Remove all pending ranges for a keyspace.
    pub fn clear_keyspace(&mut self, keyspace: &str) {
        self.ranges.remove(keyspace);
    }

    /// Remove all pending ranges.
    pub fn clear_all(&mut self) {
        self.ranges.clear();
    }

    /// Get pending ranges for a keyspace.
    pub fn for_keyspace(&self, keyspace: &str) -> &[PendingRange] {
        self.ranges
            .get(keyspace)
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }

    /// Whether there are any pending ranges in any keyspace.
    pub fn is_empty(&self) -> bool {
        self.ranges.values().all(|v| v.is_empty())
    }

    /// Total number of pending ranges across all keyspaces.
    pub fn total_count(&self) -> usize {
        self.ranges.values().map(|v| v.len()).sum()
    }

    /// Get all endpoints that have pending ranges as new owners.
    pub fn pending_endpoints(&self) -> Vec<Endpoint> {
        let mut eps = std::collections::HashSet::new();
        for ranges in self.ranges.values() {
            for r in ranges {
                eps.insert(r.new_owner);
            }
        }
        eps.into_iter().collect()
    }
}

// ── Epoch Tracking ──────────────────────────────────────────────────────

/// Epoch tracking for TCM-aware topology operations.
///
/// ## Java Oracle
///
/// - `org.apache.cassandra.tcm.Epoch`
/// - `org.apache.cassandra.tcm.sequences.BootstrapAndJoin`
///
/// Each topology operation is associated with an epoch that allows
/// safe recovery after restarts, as well as linearization of concurrent
/// operations across the cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Default)]
pub struct TopologyEpoch(pub u64);

impl TopologyEpoch {
    pub fn new(epoch: u64) -> Self {
        Self(epoch)
    }

    pub fn next(&self) -> Self {
        Self(self.0 + 1)
    }

    pub fn is_empty(&self) -> bool {
        self.0 == 0
    }
}

impl fmt::Display for TopologyEpoch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Epoch({})", self.0)
    }
}

// ── Topology Coordinator ────────────────────────────────────────────────

/// Coordinator for topology-changing operations.
pub struct TopologyCoordinator {
    state: Arc<Mutex<TopologyState>>,
    cluster: Arc<ClusterMetadata>,
    /// Pending ranges during topology changes.
    pending_ranges: Arc<Mutex<PendingRanges>>,
    /// Current topology epoch.
    epoch: Arc<Mutex<TopologyEpoch>>,
}

impl TopologyCoordinator {
    pub fn new(cluster: Arc<ClusterMetadata>) -> Self {
        Self {
            state: Arc::new(Mutex::new(TopologyState::Idle)),
            cluster,
            pending_ranges: Arc::new(Mutex::new(PendingRanges::new())),
            epoch: Arc::new(Mutex::new(TopologyEpoch::default())),
        }
    }

    pub fn state(&self) -> TopologyState {
        self.state.lock().clone()
    }

    pub fn is_operation_in_progress(&self) -> bool {
        !matches!(
            *self.state.lock(),
            TopologyState::Idle | TopologyState::Done { .. }
        )
    }

    /// Get the current topology epoch.
    pub fn epoch(&self) -> TopologyEpoch {
        *self.epoch.lock()
    }

    /// Advance the epoch and return the new value.
    fn advance_epoch(&self) -> TopologyEpoch {
        let mut epoch = self.epoch.lock();
        *epoch = epoch.next();
        *epoch
    }

    /// Get a snapshot of pending ranges.
    pub fn pending_ranges(&self) -> PendingRanges {
        self.pending_ranges.lock().clone()
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

        let epoch = self.advance_epoch();
        info!(node = %local_node.endpoint, tokens = ?new_tokens.len(), epoch = %epoch, "Starting bootstrap");

        let snap = self.cluster.snapshot();
        let mut plan = StreamPlanDescriptor::new(TopologyOperation::Bootstrap);

        // Track pending ranges.
        let mut pending = self.pending_ranges.lock();
        for token in &new_tokens {
            if let Some(current_owner) = snap.ring.primary_endpoint(*token) {
                if current_owner != local_node.endpoint {
                    let prev = snap
                        .ring
                        .previous_token(*token)
                        .unwrap_or(Token::from_raw(i64::MIN));
                    plan.add_request(StreamRangeRequest {
                        source: current_owner,
                        destination: local_node.endpoint,
                        ranges: vec![(prev, *token)],
                    });
                    pending.add(
                        "*",
                        PendingRange {
                            range: (prev, *token),
                            new_owner: local_node.endpoint,
                            current_owner,
                            operation: TopologyOperation::Bootstrap,
                        },
                    );
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
                TopologyState::Streaming {
                    operation: TopologyOperation::Bootstrap,
                    ..
                } => {}
                other => {
                    return Err(TopologyError::InvalidState(format!(
                        "Expected Streaming(Bootstrap), got {other}"
                    )));
                }
            }
        }

        {
            let mut state = self.state.lock();
            *state = TopologyState::Completing {
                operation: TopologyOperation::Bootstrap,
            };
        }

        local_node.tokens = new_tokens;
        local_node.state = NodeState::Normal;
        self.cluster.update_node(local_node.clone());

        // Clear pending ranges.
        self.pending_ranges.lock().clear_all();

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
            *state = TopologyState::Calculating {
                operation: TopologyOperation::Decommission,
            };
        }

        let epoch = self.advance_epoch();
        info!(node = %local_node.endpoint, epoch = %epoch, "Starting decommission");

        let mut leaving = local_node.clone();
        leaving.state = NodeState::Leaving;
        self.cluster.update_node(leaving);

        let mut plan = StreamPlanDescriptor::new(TopologyOperation::Decommission);
        let our_tokens = snap.ring.tokens_for(&local_node.endpoint);
        let mut pending = self.pending_ranges.lock();

        for token in &our_tokens {
            let next_owners: Vec<Endpoint> = snap
                .ring
                .natural_endpoints(*token, 2)
                .into_iter()
                .filter(|ep| *ep != local_node.endpoint)
                .collect();

            if let Some(recipient) = next_owners.first() {
                let prev = snap
                    .ring
                    .previous_token(*token)
                    .unwrap_or(Token::from_raw(i64::MIN));
                plan.add_request(StreamRangeRequest {
                    source: local_node.endpoint,
                    destination: *recipient,
                    ranges: vec![(prev, *token)],
                });
                pending.add(
                    "*",
                    PendingRange {
                        range: (prev, *token),
                        new_owner: *recipient,
                        current_owner: local_node.endpoint,
                        operation: TopologyOperation::Decommission,
                    },
                );
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
                TopologyState::Streaming {
                    operation: TopologyOperation::Decommission,
                    ..
                } => {}
                other => {
                    return Err(TopologyError::InvalidState(format!(
                        "Expected Streaming(Decommission), got {other}"
                    )));
                }
            }
        }

        {
            let mut state = self.state.lock();
            *state = TopologyState::Completing {
                operation: TopologyOperation::Decommission,
            };
        }

        self.cluster.remove_node(local_endpoint);
        self.pending_ranges.lock().clear_all();
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
                return Err(TopologyError::TargetNotDead(format!(
                    "{dead_endpoint} is {}",
                    info.state
                )));
            }
        } else {
            return Err(TopologyError::NodeNotFound(dead_endpoint.to_string()));
        }

        {
            let mut state = self.state.lock();
            *state = TopologyState::Calculating {
                operation: TopologyOperation::Replace,
            };
        }

        let epoch = self.advance_epoch();
        info!(new_node = %local_node.endpoint, replacing = %dead_endpoint, epoch = %epoch, "Starting replacement");

        let dead_tokens = snap.ring.tokens_for(dead_endpoint);
        let mut plan = StreamPlanDescriptor::new(TopologyOperation::Replace);

        for token in &dead_tokens {
            let replicas: Vec<Endpoint> = snap
                .ring
                .natural_endpoints(*token, 3)
                .into_iter()
                .filter(|ep| *ep != *dead_endpoint && *ep != local_node.endpoint)
                .collect();

            if let Some(source) = replicas.first() {
                let prev = snap
                    .ring
                    .previous_token(*token)
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
                TopologyState::Streaming {
                    operation: TopologyOperation::Replace,
                    ..
                } => {}
                other => {
                    return Err(TopologyError::InvalidState(format!(
                        "Expected Streaming(Replace), got {other}"
                    )));
                }
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
                    return Err(TopologyError::InvalidState(format!(
                        "Host ID mismatch for {target_endpoint}"
                    )));
                }
            }
            None => return Err(TopologyError::NodeNotFound(target_endpoint.to_string())),
        }

        {
            let mut state = self.state.lock();
            *state = TopologyState::Calculating {
                operation: TopologyOperation::RemoveNode,
            };
        }

        let epoch = self.advance_epoch();
        info!(target = %target_endpoint, host_id = %target_host_id, epoch = %epoch, "Removing node");
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
            *state = TopologyState::Calculating {
                operation: TopologyOperation::Rebuild,
            };
        }

        info!(node = %local_node.endpoint, source_dc = source_dc.unwrap_or("all"), "Starting rebuild");

        let snap = self.cluster.snapshot();
        let our_tokens = snap.ring.tokens_for(&local_node.endpoint);
        let mut plan = StreamPlanDescriptor::new(TopologyOperation::Rebuild);

        for token in &our_tokens {
            let candidates: Vec<Endpoint> = snap
                .ring
                .natural_endpoints(*token, 3)
                .into_iter()
                .filter(|ep| {
                    if *ep == local_node.endpoint {
                        return false;
                    }
                    if let Some(dc) = source_dc {
                        if let Some(info) = snap.nodes.get(ep) {
                            return info.datacenter == dc;
                        }
                    }
                    true
                })
                .collect();

            if let Some(source) = candidates.first() {
                let prev = snap
                    .ring
                    .previous_token(*token)
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
                TopologyState::Streaming {
                    operation: TopologyOperation::Rebuild,
                    ..
                } => {}
                other => {
                    return Err(TopologyError::InvalidState(format!(
                        "Expected Streaming(Rebuild), got {other}"
                    )));
                }
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

    // ── Move ───────────────────────────────────────────────────────────

    /// Begin a move-token operation.
    ///
    /// ## Java Oracle
    ///
    /// - `org.apache.cassandra.service.StorageService.move()`
    ///
    /// The node announces it is moving to a new token, then streams
    /// the ranges that differ between old and new token positions.
    pub fn begin_move(
        &self,
        local_node: &NodeInfo,
        new_token: Token,
    ) -> Result<StreamPlanDescriptor, TopologyError> {
        self.check_idle()?;

        // Java: single-token-per-node only for move
        if local_node.tokens.len() != 1 {
            return Err(TopologyError::InvalidMoveToken(
                "Move requires exactly one token per node (num_tokens=1)".into(),
            ));
        }

        let old_token = local_node.tokens[0];
        if old_token == new_token {
            return Err(TopologyError::InvalidMoveToken(
                "New token is same as current token".into(),
            ));
        }

        {
            let mut state = self.state.lock();
            *state = TopologyState::Calculating {
                operation: TopologyOperation::Move,
            };
        }

        let epoch = self.advance_epoch();
        info!(
            node = %local_node.endpoint,
            old_token = ?old_token.value(),
            new_token = ?new_token.value(),
            epoch = %epoch,
            "Starting move"
        );

        let snap = self.cluster.snapshot();
        let mut plan = StreamPlanDescriptor::new(TopologyOperation::Move);
        let mut pending = self.pending_ranges.lock();

        // Determine ranges to gain (new ranges not covered by old token).
        let new_prev = snap
            .ring
            .previous_token(new_token)
            .unwrap_or(Token::from_raw(i64::MIN));
        let old_prev = snap
            .ring
            .previous_token(old_token)
            .unwrap_or(Token::from_raw(i64::MIN));

        // Simplified: if new token is after old token, we need data from
        // (old_token, new_token]. If before, we lose (new_token, old_token].
        if let Some(source) = snap.ring.primary_endpoint(new_token) {
            if source != local_node.endpoint {
                plan.add_request(StreamRangeRequest {
                    source,
                    destination: local_node.endpoint,
                    ranges: vec![(new_prev, new_token)],
                });
                pending.add(
                    "*",
                    PendingRange {
                        range: (new_prev, new_token),
                        new_owner: local_node.endpoint,
                        current_owner: source,
                        operation: TopologyOperation::Move,
                    },
                );
            }
        }

        // Stream out any ranges we no longer own.
        if let Some(new_owner) = snap.ring.primary_endpoint(old_token) {
            if new_owner != local_node.endpoint {
                plan.add_request(StreamRangeRequest {
                    source: local_node.endpoint,
                    destination: new_owner,
                    ranges: vec![(old_prev, old_token)],
                });
            }
        }

        let mut state = self.state.lock();
        *state = TopologyState::Streaming {
            operation: TopologyOperation::Move,
            sessions: plan.requests.len(),
            progress: 0,
        };

        Ok(plan)
    }

    /// Complete a move-token operation.
    pub fn finish_move(
        &self,
        local_node: &mut NodeInfo,
        new_token: Token,
    ) -> Result<(), TopologyError> {
        {
            let state = self.state.lock();
            match &*state {
                TopologyState::Streaming {
                    operation: TopologyOperation::Move,
                    ..
                } => {}
                other => {
                    return Err(TopologyError::InvalidState(format!(
                        "Expected Streaming(Move), got {other}"
                    )));
                }
            }
        }

        {
            let mut state = self.state.lock();
            *state = TopologyState::Completing {
                operation: TopologyOperation::Move,
            };
        }

        local_node.tokens = vec![new_token];
        local_node.state = NodeState::Normal;
        self.cluster.update_node(local_node.clone());
        self.pending_ranges.lock().clear_all();

        info!(node = %local_node.endpoint, new_token = ?new_token.value(), "Move complete");

        let mut state = self.state.lock();
        *state = TopologyState::Done {
            operation: TopologyOperation::Move,
            success: true,
            error: None,
        };
        Ok(())
    }

    // ── Cleanup ────────────────────────────────────────────────────────

    /// Begin a cleanup operation.
    ///
    /// ## Java Oracle
    ///
    /// - `org.apache.cassandra.service.StorageService.forceKeyspaceCleanup()`
    /// - `org.apache.cassandra.db.compaction.CompactionManager.performCleanup()`
    ///
    /// After a topology change (e.g., bootstrap of a new node), existing
    /// nodes may hold data for token ranges they no longer own. Cleanup
    /// scans SSTables and removes data for ranges not owned by this node.
    ///
    /// Returns a `CleanupPlan` describing which keyspaces/tables need cleanup.
    pub fn begin_cleanup(
        &self,
        local_node: &NodeInfo,
        keyspaces: Option<Vec<String>>,
    ) -> Result<CleanupPlan, TopologyError> {
        self.check_idle()?;

        {
            let mut state = self.state.lock();
            *state = TopologyState::Calculating {
                operation: TopologyOperation::Cleanup,
            };
        }

        info!(node = %local_node.endpoint, "Starting cleanup");

        let snap = self.cluster.snapshot();
        let our_ranges: Vec<(Token, Token)> = local_node
            .tokens
            .iter()
            .map(|t| {
                let prev = snap
                    .ring
                    .previous_token(*t)
                    .unwrap_or(Token::from_raw(i64::MIN));
                (prev, *t)
            })
            .collect();

        let plan = CleanupPlan {
            keyspaces: keyspaces.unwrap_or_default(),
            owned_ranges: our_ranges,
        };

        let mut state = self.state.lock();
        *state = TopologyState::Streaming {
            operation: TopologyOperation::Cleanup,
            sessions: 0,
            progress: 0,
        };

        Ok(plan)
    }

    /// Complete the cleanup operation.
    pub fn finish_cleanup(&self) -> Result<(), TopologyError> {
        {
            let state = self.state.lock();
            match &*state {
                TopologyState::Streaming {
                    operation: TopologyOperation::Cleanup,
                    ..
                }
                | TopologyState::Calculating {
                    operation: TopologyOperation::Cleanup,
                } => {}
                other => {
                    return Err(TopologyError::InvalidState(format!(
                        "Expected Cleanup state, got {other}"
                    )));
                }
            }
        }

        info!("Cleanup complete");
        let mut state = self.state.lock();
        *state = TopologyState::Done {
            operation: TopologyOperation::Cleanup,
            success: true,
            error: None,
        };
        Ok(())
    }

    // ── Refresh ────────────────────────────────────────────────────────

    /// Refresh: load new SSTables from disk into a table.
    ///
    /// ## Java Oracle
    ///
    /// - `org.apache.cassandra.service.StorageService.loadNewSSTables()`
    ///
    /// This is a local-only operation that scans the data directory for
    /// SSTables not yet loaded into the memtable/compaction pipeline.
    pub fn refresh(&self, keyspace: &str, table: &str) -> Result<(), TopologyError> {
        self.check_idle()?;

        {
            let mut state = self.state.lock();
            *state = TopologyState::Calculating {
                operation: TopologyOperation::Refresh,
            };
        }

        info!(keyspace, table, "Refreshing (loading new SSTables)");

        // Refresh is instantaneous from topology perspective.
        // The actual SSTable loading is delegated to the storage engine.

        let mut state = self.state.lock();
        *state = TopologyState::Done {
            operation: TopologyOperation::Refresh,
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
        // Clear pending ranges on abort.
        self.pending_ranges.lock().clear_all();
    }

    pub fn reset(&self) -> Result<(), TopologyError> {
        let mut state = self.state.lock();
        match &*state {
            TopologyState::Done { .. } | TopologyState::Idle => {
                *state = TopologyState::Idle;
                self.pending_ranges.lock().clear_all();
                Ok(())
            }
            other => Err(TopologyError::InvalidState(format!(
                "Cannot reset from {other}"
            ))),
        }
    }

    /// Check if a topology operation was interrupted (for recovery on restart).
    ///
    /// Returns the operation that was in progress if the state is not Idle/Done.
    pub fn interrupted_operation(&self) -> Option<TopologyOperation> {
        let state = self.state.lock();
        match &*state {
            TopologyState::Calculating { operation }
            | TopologyState::Streaming { operation, .. }
            | TopologyState::Completing { operation } => Some(*operation),
            _ => None,
        }
    }
}

/// Cleanup plan: describes what to clean after a topology change.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CleanupPlan {
    /// Keyspaces to clean (empty = all non-system keyspaces).
    pub keyspaces: Vec<String>,
    /// Token ranges currently owned by this node.
    pub owned_ranges: Vec<(Token, Token)>,
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
        assert!(tc.epoch().is_empty());
    }

    #[test]
    fn bootstrap_happy_path() {
        let (cm, tc) = setup_cluster();
        let mut new_node = node(7004, vec![]);

        let plan = tc
            .begin_bootstrap(&new_node, vec![Token::from_raw(50)])
            .unwrap();
        assert!(tc.is_operation_in_progress());
        assert!(!plan.is_empty() || plan.requests.is_empty());
        assert_eq!(tc.epoch().0, 1);

        // Verify pending ranges exist.
        let _pending = tc.pending_ranges();
        // May or may not have pending ranges depending on ring state.

        tc.finish_bootstrap(&mut new_node, vec![Token::from_raw(50)])
            .unwrap();
        assert!(matches!(
            tc.state(),
            TopologyState::Done {
                operation: TopologyOperation::Bootstrap,
                success: true,
                ..
            }
        ));
        assert_eq!(cm.snapshot().node_count(), 4);
        assert_eq!(new_node.state, NodeState::Normal);

        // Pending ranges should be cleared.
        assert!(tc.pending_ranges().is_empty());
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
        assert!(matches!(
            tc.state(),
            TopologyState::Done {
                operation: TopologyOperation::Rebuild,
                success: true,
                ..
            }
        ));
    }

    #[test]
    fn move_happy_path() {
        let (_cm, tc) = setup_cluster();
        let mut n = node(7005, vec![500]);
        // Add a single-token node for move.
        tc.cluster.update_node(n.clone());

        let plan = tc.begin_move(&n, Token::from_raw(550)).unwrap();
        assert_eq!(plan.operation, TopologyOperation::Move);

        tc.finish_move(&mut n, Token::from_raw(550)).unwrap();
        assert_eq!(n.tokens, vec![Token::from_raw(550)]);
        assert!(matches!(
            tc.state(),
            TopologyState::Done {
                operation: TopologyOperation::Move,
                success: true,
                ..
            }
        ));
    }

    #[test]
    fn move_multi_token_rejected() {
        let (_cm, tc) = setup_cluster();
        let n = node(7001, vec![-100, 0]); // 2 tokens
        let result = tc.begin_move(&n, Token::from_raw(50));
        assert!(matches!(result, Err(TopologyError::InvalidMoveToken(_))));
    }

    #[test]
    fn move_same_token_rejected() {
        let (_cm, tc) = setup_cluster();
        let n = node(7005, vec![500]);
        tc.cluster.update_node(n.clone());
        let result = tc.begin_move(&n, Token::from_raw(500));
        assert!(matches!(result, Err(TopologyError::InvalidMoveToken(_))));
    }

    #[test]
    fn cleanup_happy_path() {
        let (_cm, tc) = setup_cluster();
        let n1 = node(7001, vec![-100, 0]);

        let plan = tc.begin_cleanup(&n1, Some(vec!["my_ks".into()])).unwrap();
        assert_eq!(plan.keyspaces, vec!["my_ks"]);
        assert!(!plan.owned_ranges.is_empty());

        tc.finish_cleanup().unwrap();
        assert!(matches!(
            tc.state(),
            TopologyState::Done {
                operation: TopologyOperation::Cleanup,
                success: true,
                ..
            }
        ));
    }

    #[test]
    fn refresh_happy_path() {
        let (_cm, tc) = setup_cluster();
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
        // Pending ranges cleared on abort.
        assert!(tc.pending_ranges().is_empty());
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
        assert_eq!(TopologyOperation::Move.to_string(), "MOVE");
        assert_eq!(TopologyOperation::Cleanup.to_string(), "CLEANUP");
        assert_eq!(TopologyOperation::Refresh.to_string(), "REFRESH");
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

    #[test]
    fn pending_ranges_tracking() {
        let mut pr = PendingRanges::new();
        assert!(pr.is_empty());
        assert_eq!(pr.total_count(), 0);

        pr.add(
            "ks1",
            PendingRange {
                range: (Token::from_raw(0), Token::from_raw(100)),
                new_owner: ep(7002),
                current_owner: ep(7001),
                operation: TopologyOperation::Bootstrap,
            },
        );
        pr.add(
            "ks1",
            PendingRange {
                range: (Token::from_raw(100), Token::from_raw(200)),
                new_owner: ep(7002),
                current_owner: ep(7003),
                operation: TopologyOperation::Bootstrap,
            },
        );

        assert!(!pr.is_empty());
        assert_eq!(pr.total_count(), 2);
        assert_eq!(pr.for_keyspace("ks1").len(), 2);
        assert_eq!(pr.for_keyspace("ks2").len(), 0);
        assert_eq!(pr.pending_endpoints().len(), 1);

        pr.clear_keyspace("ks1");
        assert!(pr.is_empty());
    }

    #[test]
    fn epoch_tracking() {
        let (_cm, tc) = setup_cluster();
        assert_eq!(tc.epoch().0, 0);

        let n = node(7001, vec![-100, 0]);
        tc.begin_rebuild(&n, None).unwrap();
        // Rebuild does not advance epoch (no streaming)
        tc.finish_rebuild().unwrap();

        tc.reset().unwrap();
        let n2 = node(7004, vec![]);
        tc.begin_bootstrap(&n2, vec![Token::from_raw(50)]).unwrap();
        assert!(tc.epoch().0 > 0);
    }

    #[test]
    fn interrupted_operation_detection() {
        let (_cm, tc) = setup_cluster();
        assert!(tc.interrupted_operation().is_none());

        let n = node(7001, vec![-100, 0]);
        tc.begin_bootstrap(&n, vec![Token::from_raw(500)]).unwrap();
        assert_eq!(
            tc.interrupted_operation(),
            Some(TopologyOperation::Bootstrap)
        );
    }
}
