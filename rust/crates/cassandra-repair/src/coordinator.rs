// Licensed under Apache License, Version 2.0.

//! Repair coordinator: drives full and incremental repair.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.repair.RepairRunnable`
//! - `org.apache.cassandra.repair.RepairCoordinator`
//! - `org.apache.cassandra.service.ActiveRepairService`

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use tracing::{info, warn};
use uuid::Uuid;

use cassandra_cluster_metadata::Endpoint;
use cassandra_common::Token;

use crate::history::{LoggingRepairHistoryTracker, RepairHistoryTracker};
use crate::merkle::MerkleTree;
use crate::metrics::RepairMetrics;
use crate::session::{RepairSession, RepairSessionId, RepairSessionState};

/// Type of repair.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RepairType {
    /// Full repair: rebuild all Merkle trees and sync all data.
    Full,
    /// Incremental repair: only repair unrepaired data (data added since last repair).
    Incremental,
    /// Preview repair: compute differences but do not stream data or change states.
    Preview,
}

impl std::fmt::Display for RepairType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Full => write!(f, "FULL"),
            Self::Incremental => write!(f, "INCREMENTAL"),
            Self::Preview => write!(f, "PREVIEW"),
        }
    }
}

/// Errors from the repair coordinator.
#[derive(Debug, thiserror::Error)]
pub enum RepairError {
    #[error("Repair already in progress: {0}")]
    AlreadyRunning(String),

    #[error("No replicas available for repair")]
    NoReplicas,

    #[error("Session not found: {0}")]
    SessionNotFound(String),

    #[error("Merkle tree exchange failed: {0}")]
    TreeExchangeFailed(String),

    #[error("Streaming failed: {0}")]
    StreamingFailed(String),

    #[error("Repair cancelled")]
    Cancelled,
}

/// Status of the overall repair coordinator.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepairStatus {
    pub repair_id: Uuid,
    pub repair_type: RepairType,
    pub keyspace: String,
    pub tables: Vec<String>,
    pub total_ranges: usize,
    pub completed_ranges: usize,
    pub failed_ranges: usize,
    pub active: bool,
}

/// Coordinator for running repairs across the cluster.
///
/// The coordinator:
/// 1. Snapshots data (for full repair)
/// 2. Builds Merkle trees for each range
/// 3. Exchanges trees with replicas
/// 4. Diffs trees to find mismatched ranges
/// 5. Streams mismatched data
/// 6. Marks data as repaired (incremental) or completes (full)
pub struct RepairCoordinator {
    /// Active repair sessions indexed by ID.
    sessions: Arc<Mutex<HashMap<RepairSessionId, RepairSession>>>,
    /// Current active repair ID (only one at a time).
    active_repair: Arc<Mutex<Option<Uuid>>>,
    /// Type of the current active repair.
    active_repair_type: Arc<Mutex<Option<RepairType>>>,
    /// Metrics.
    pub metrics: Arc<RepairMetrics>,
    /// Whether the repair has been cancelled.
    cancelled: Arc<std::sync::atomic::AtomicBool>,
    /// Tracker for system_distributed repair history
    pub history_tracker: Arc<dyn RepairHistoryTracker>,
}

impl RepairCoordinator {
    /// Create a new repair coordinator.
    pub fn new() -> Self {
        Self {
            sessions: Arc::new(Mutex::new(HashMap::new())),
            active_repair: Arc::new(Mutex::new(None)),
            active_repair_type: Arc::new(Mutex::new(None)),
            metrics: Arc::new(RepairMetrics::new()),
            cancelled: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            history_tracker: Arc::new(LoggingRepairHistoryTracker),
        }
    }

    /// Create a new coordinator with a specific history tracker.
    pub fn with_tracker(tracker: Arc<dyn RepairHistoryTracker>) -> Self {
        Self {
            sessions: Arc::new(Mutex::new(HashMap::new())),
            active_repair: Arc::new(Mutex::new(None)),
            active_repair_type: Arc::new(Mutex::new(None)),
            metrics: Arc::new(RepairMetrics::new()),
            cancelled: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            history_tracker: tracker,
        }
    }

    /// Start a repair for the given keyspace/tables/ranges.
    ///
    /// Returns a unique repair ID for tracking.
    pub fn start_repair(
        &self,
        repair_type: RepairType,
        keyspace: &str,
        tables: &[String],
        ranges: &[(Token, Token)],
        replicas: &[Endpoint],
    ) -> Result<Uuid, RepairError> {
        let mut active = self.active_repair.lock();
        if let Some(id) = *active {
            return Err(RepairError::AlreadyRunning(id.to_string()));
        }

        if replicas.is_empty() {
            return Err(RepairError::NoReplicas);
        }

        let repair_id = Uuid::new_v4();
        *active = Some(repair_id);
        *self.active_repair_type.lock() = Some(repair_type);
        self.cancelled
            .store(false, std::sync::atomic::Ordering::SeqCst);

        info!(
            repair_id = %repair_id,
            repair_type = %repair_type,
            keyspace = keyspace,
            ranges = ranges.len(),
            replicas = replicas.len(),
            "Starting repair"
        );

        self.history_tracker.record_parent_repair_start(
            repair_id,
            keyspace,
            tables,
            ranges,
            repair_type,
        );

        let mut sessions = self.sessions.lock();
        for range in ranges {
            let table_name = if tables.is_empty() {
                "*".to_string()
            } else {
                tables.join(",")
            };
            let session = RepairSession::new(
                repair_id,
                repair_type,
                keyspace,
                &table_name,
                *range,
                replicas.to_vec(),
            );

            self.history_tracker.record_session_start(
                session.id,
                repair_id,
                keyspace,
                &table_name,
                *range,
                replicas,
            );

            sessions.insert(session.id, session);
        }

        Ok(repair_id)
    }

    /// Get the status of the current repair.
    pub fn status(&self) -> Option<RepairStatus> {
        let active = self.active_repair.lock();
        let repair_id = (*active)?;
        let repair_type = (*self.active_repair_type.lock())?;

        let sessions = self.sessions.lock();
        let mut total = 0;
        let mut completed = 0;
        let mut failed = 0;
        let mut keyspace = String::new();
        let mut tables = Vec::new();

        for session in sessions.values() {
            total += 1;
            if keyspace.is_empty() {
                keyspace = session.keyspace.clone();
                tables.push(session.table.clone());
            }
            match session.state {
                RepairSessionState::Complete => completed += 1,
                RepairSessionState::Failed => failed += 1,
                _ => {}
            }
        }

        Some(RepairStatus {
            repair_id,
            repair_type,
            keyspace,
            tables,
            total_ranges: total,
            completed_ranges: completed,
            failed_ranges: failed,
            active: true,
        })
    }

    /// Process a Merkle tree exchange for a session.
    ///
    /// Takes the local tree and the remote tree, diffs them, and returns
    /// the mismatched ranges that need streaming.
    pub fn exchange_trees(
        &self,
        session_id: &RepairSessionId,
        local_tree: &MerkleTree,
        remote_tree: &MerkleTree,
    ) -> Result<Vec<(Token, Token)>, RepairError> {
        let mut sessions = self.sessions.lock();
        let session = sessions
            .get_mut(session_id)
            .ok_or_else(|| RepairError::SessionNotFound(session_id.to_string()))?;

        // Transition the session
        if session.state == RepairSessionState::Initialized {
            session
                .start_building_trees()
                .map_err(|e| RepairError::TreeExchangeFailed(e.to_string()))?;
        }
        if session.state == RepairSessionState::BuildingTrees {
            session
                .start_exchanging_trees()
                .map_err(|e| RepairError::TreeExchangeFailed(e.to_string()))?;
        }

        self.metrics.record_tree_exchanged();

        // Diff the trees
        let diffs = local_tree.diff(remote_tree);

        info!(
            session_id = %session_id,
            mismatched_ranges = diffs.len(),
            "Tree exchange complete"
        );

        if diffs.is_empty() {
            // No differences — complete the session
            session
                .complete()
                .map_err(|e| RepairError::TreeExchangeFailed(e.to_string()))?;
            self.metrics.session_completed();
            self.history_tracker.record_session_finish(
                *session_id,
                RepairSessionState::Complete,
                None,
            );
        } else {
            session
                .start_streaming(diffs.len())
                .map_err(|e| RepairError::StreamingFailed(e.to_string()))?;
        }

        Ok(diffs)
    }

    /// Mark a session as complete after streaming.
    pub fn complete_session(&self, session_id: &RepairSessionId) -> Result<(), RepairError> {
        let mut sessions = self.sessions.lock();
        let session = sessions
            .get_mut(session_id)
            .ok_or_else(|| RepairError::SessionNotFound(session_id.to_string()))?;

        session
            .complete()
            .map_err(|e| RepairError::StreamingFailed(e.to_string()))?;

        self.metrics.session_completed();
        self.metrics.record_range_repaired();

        // Check if all sessions are done
        let all_done = sessions.values().all(|s| {
            matches!(
                s.state,
                RepairSessionState::Complete | RepairSessionState::Failed
            )
        });

        self.history_tracker
            .record_session_finish(*session_id, RepairSessionState::Complete, None);

        if all_done {
            drop(sessions);
            self.finish_repair();
        }

        Ok(())
    }

    /// Cancel the current repair.
    pub fn cancel(&self) {
        self.cancelled
            .store(true, std::sync::atomic::Ordering::SeqCst);

        let mut sessions = self.sessions.lock();
        for session in sessions.values_mut() {
            if !matches!(
                session.state,
                RepairSessionState::Complete | RepairSessionState::Failed
            ) {
                session.cancel();
            }
        }
        drop(sessions);

        warn!("Repair cancelled");
        self.finish_repair();
    }

    /// Check if the repair has been cancelled.
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Finish the repair and clean up.
    fn finish_repair(&self) {
        let mut active = self.active_repair.lock();
        if let Some(id) = *active {
            info!(repair_id = %id, "Repair finished");

            let sessions = self.sessions.lock();
            let mut successful = Vec::new();
            let mut error = None;
            for s in sessions.values() {
                if s.state == RepairSessionState::Complete {
                    successful.push(s.range);
                } else if s.state == RepairSessionState::Failed {
                    error = s
                        .error
                        .clone()
                        .or_else(|| Some("Session failed".to_string()));
                } else if self.is_cancelled() {
                    error = Some("Repair cancelled".to_string());
                }
            }

            self.history_tracker
                .record_parent_repair_finish(id, &successful, error);
            drop(sessions);
        }
        *active = None;
        *self.active_repair_type.lock() = None;
        self.sessions.lock().clear();
    }

    /// Number of active sessions.
    pub fn active_session_count(&self) -> usize {
        self.sessions
            .lock()
            .values()
            .filter(|s| {
                !matches!(
                    s.state,
                    RepairSessionState::Complete
                        | RepairSessionState::Failed
                        | RepairSessionState::Cancelled
                )
            })
            .count()
    }
}

impl Default for RepairCoordinator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::merkle::hash_partition;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    fn ep(port: u16) -> Endpoint {
        Endpoint::new(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            port,
        ))
    }

    fn tok(v: i64) -> Token {
        Token::from_raw(v)
    }

    #[test]
    fn start_repair() {
        let rc = RepairCoordinator::new();
        let ranges = vec![(tok(0), tok(100)), (tok(100), tok(200))];
        let replicas = vec![ep(7001), ep(7002)];

        let id = rc
            .start_repair(RepairType::Full, "ks", &["t1".into()], &ranges, &replicas)
            .unwrap();

        let status = rc.status().unwrap();
        assert_eq!(status.repair_id, id);
        assert_eq!(status.repair_type, RepairType::Full);
        assert_eq!(status.total_ranges, 2);
        assert_eq!(status.completed_ranges, 0);
    }

    #[test]
    fn status_reports_active_repair_type() {
        let rc = RepairCoordinator::new();
        let ranges = vec![(tok(0), tok(100))];
        let replicas = vec![ep(7001)];

        rc.start_repair(
            RepairType::Incremental,
            "ks",
            &["t1".into()],
            &ranges,
            &replicas,
        )
        .unwrap();

        let status = rc.status().unwrap();
        assert_eq!(status.repair_type, RepairType::Incremental);
    }

    #[test]
    fn concurrent_repairs_blocked() {
        let rc = RepairCoordinator::new();
        let ranges = vec![(tok(0), tok(100))];
        let replicas = vec![ep(7001)];

        rc.start_repair(RepairType::Full, "ks", &[], &ranges, &replicas)
            .unwrap();

        let result = rc.start_repair(RepairType::Full, "ks2", &[], &ranges, &replicas);
        assert!(matches!(result, Err(RepairError::AlreadyRunning(_))));
    }

    #[test]
    fn no_replicas_error() {
        let rc = RepairCoordinator::new();
        let result = rc.start_repair(RepairType::Full, "ks", &[], &[(tok(0), tok(100))], &[]);
        assert!(matches!(result, Err(RepairError::NoReplicas)));
    }

    #[test]
    fn tree_exchange_no_diffs() {
        let rc = RepairCoordinator::new();
        let ranges = vec![(tok(0), tok(1000))];
        let replicas = vec![ep(7001)];

        rc.start_repair(RepairType::Full, "ks", &["t1".into()], &ranges, &replicas)
            .unwrap();

        // Get the session ID
        let sessions = rc.sessions.lock();
        let session_id = *sessions.keys().next().unwrap();
        drop(sessions);

        // Build identical trees
        let partitions = vec![
            (tok(100), hash_partition(b"k1", b"d1")),
            (tok(500), hash_partition(b"k2", b"d2")),
        ];
        let t1 = MerkleTree::build(tok(0), tok(1000), 2, &partitions);
        let t2 = MerkleTree::build(tok(0), tok(1000), 2, &partitions);

        let diffs = rc.exchange_trees(&session_id, &t1, &t2).unwrap();
        assert!(diffs.is_empty());

        // Session should be auto-completed
        let snap = rc.metrics.snapshot();
        assert_eq!(snap.sessions_completed, 1);
    }

    #[test]
    fn tree_exchange_with_diffs() {
        let rc = RepairCoordinator::new();
        let ranges = vec![(tok(0), tok(1000))];
        let replicas = vec![ep(7001)];

        rc.start_repair(RepairType::Full, "ks", &["t1".into()], &ranges, &replicas)
            .unwrap();

        let sessions = rc.sessions.lock();
        let session_id = *sessions.keys().next().unwrap();
        drop(sessions);

        let p1 = vec![(tok(100), hash_partition(b"k1", b"d1"))];
        let p2 = vec![(tok(100), hash_partition(b"k1", b"DIFFERENT"))];
        let t1 = MerkleTree::build(tok(0), tok(1000), 2, &p1);
        let t2 = MerkleTree::build(tok(0), tok(1000), 2, &p2);

        let diffs = rc.exchange_trees(&session_id, &t1, &t2).unwrap();
        assert!(!diffs.is_empty());

        // Complete the session after streaming
        rc.complete_session(&session_id).unwrap();
    }

    #[test]
    fn cancel_repair() {
        let rc = RepairCoordinator::new();
        let ranges = vec![(tok(0), tok(100))];
        let replicas = vec![ep(7001)];

        rc.start_repair(RepairType::Full, "ks", &[], &ranges, &replicas)
            .unwrap();
        assert!(!rc.is_cancelled());

        rc.cancel();
        assert!(rc.is_cancelled());
        assert!(rc.status().is_none()); // Repair finished
    }

    #[test]
    fn repair_type_display() {
        assert_eq!(RepairType::Full.to_string(), "FULL");
        assert_eq!(RepairType::Incremental.to_string(), "INCREMENTAL");
        assert_eq!(RepairType::Preview.to_string(), "PREVIEW");
    }
}
