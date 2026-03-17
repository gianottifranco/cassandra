// Licensed under Apache License, Version 2.0.

//! SyncTask and StreamingRepairTask: execute data transfer after diff.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.repair.SyncTask`
//! - `org.apache.cassandra.repair.LocalSyncTask`
//! - `org.apache.cassandra.repair.RemoteSyncTask`
//! - `org.apache.cassandra.repair.StreamingRepairTask`
//!
//! ## Design
//!
//! After a RepairJob identifies mismatched ranges via Merkle tree diff,
//! SyncTask implementations execute the actual data transfer using the
//! streaming subsystem.

use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use cassandra_cluster_metadata::Endpoint;
use cassandra_common::Token;
use cassandra_streaming::{StreamOperation, StreamPlan};

/// Result of a successful sync.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncResult {
    /// Total bytes streamed.
    pub bytes_streamed: u64,
    /// Number of ranges synced.
    pub ranges_synced: usize,
    /// Wall-clock duration of the sync.
    pub duration: Duration,
}

/// Errors that can occur during sync.
#[derive(Debug, thiserror::Error)]
pub enum SyncError {
    #[error("Streaming failed: {0}")]
    StreamingFailed(String),

    #[error("Remote sync failed on {endpoint}: {reason}")]
    RemoteFailed { endpoint: Endpoint, reason: String },

    #[error("Sync cancelled")]
    Cancelled,
}

/// A local sync task where this node participates in streaming.
///
/// Builds a `StreamPlan` with `StreamOperation::Repair` and requests
/// ranges from the source or transfers ranges to the target.
#[derive(Debug)]
pub struct LocalSyncTask {
    /// Source endpoint.
    pub source: Endpoint,
    /// Target endpoint.
    pub target: Endpoint,
    /// Ranges to sync.
    pub ranges: Vec<(Token, Token)>,
    /// Keyspace being repaired.
    pub keyspace: String,
    /// Tables being repaired.
    pub tables: Vec<String>,
    /// Whether this is a pull repair (we request ranges).
    pub is_pull: bool,
}

impl LocalSyncTask {
    /// Build the `StreamPlan` for this sync task.
    pub fn build_stream_plan(&self) -> StreamPlan {
        let plan = StreamPlan::new(StreamOperation::Repair);

        if self.is_pull {
            plan.request_ranges(
                self.source,
                self.keyspace.clone(),
                self.tables.clone(),
                self.ranges.clone(),
            )
        } else {
            plan.transfer_ranges(
                self.target,
                self.keyspace.clone(),
                self.tables.clone(),
                self.ranges.clone(),
            )
        }
    }

    /// Execute the sync task (constructs plan, returns result descriptor).
    ///
    /// Actual I/O is delegated to the streaming subsystem; this method
    /// only produces the plan and result metadata.
    pub fn execute(&self) -> Result<SyncResult, SyncError> {
        let start = Instant::now();
        let plan = self.build_stream_plan();

        if plan.is_empty() {
            return Ok(SyncResult {
                bytes_streamed: 0,
                ranges_synced: 0,
                duration: start.elapsed(),
            });
        }

        // In a real implementation, this would call
        // StreamCoordinator::execute_plan(). For now we return a
        // descriptor of what *would* be streamed.
        Ok(SyncResult {
            bytes_streamed: 0,
            ranges_synced: self.ranges.len(),
            duration: start.elapsed(),
        })
    }
}

/// A remote sync task — asks a remote node to stream to another remote.
///
/// Used when neither endpoint in a diff pair is the coordinator.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteSyncTask {
    /// Source endpoint to stream from.
    pub source: Endpoint,
    /// Target endpoint to stream to.
    pub target: Endpoint,
    /// Ranges to sync.
    pub ranges: Vec<(Token, Token)>,
    /// Keyspace.
    pub keyspace: String,
    /// Tables.
    pub tables: Vec<String>,
}

impl RemoteSyncTask {
    /// Build the message descriptor that would be sent to the source
    /// node to initiate streaming.
    pub fn build_descriptor(&self) -> RemoteSyncDescriptor {
        RemoteSyncDescriptor {
            source: self.source,
            target: self.target,
            ranges: self.ranges.clone(),
            keyspace: self.keyspace.clone(),
            tables: self.tables.clone(),
        }
    }
}

/// Wire-format descriptor sent to a remote node to initiate sync.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteSyncDescriptor {
    pub source: Endpoint,
    pub target: Endpoint,
    pub ranges: Vec<(Token, Token)>,
    pub keyspace: String,
    pub tables: Vec<String>,
}

/// Wrapper around the streaming subsystem for repair-specific streaming.
#[derive(Debug)]
pub struct StreamingRepairTask {
    /// The stream plan to execute.
    pub plan: StreamPlan,
    /// Session identifier for tracking.
    pub session_id: uuid::Uuid,
}

impl StreamingRepairTask {
    pub fn new(plan: StreamPlan, session_id: uuid::Uuid) -> Self {
        Self { plan, session_id }
    }

    /// Execute the streaming repair task.
    pub fn execute(&self) -> Result<SyncResult, SyncError> {
        let start = Instant::now();
        // Delegates to StreamCoordinator in a real implementation.
        Ok(SyncResult {
            bytes_streamed: 0,
            ranges_synced: self.plan.peer_count(),
            duration: start.elapsed(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
    fn local_sync_pull_builds_request_plan() {
        let task = LocalSyncTask {
            source: ep(7001),
            target: ep(7002),
            ranges: vec![(tok(0), tok(100)), (tok(100), tok(200))],
            keyspace: "ks".into(),
            tables: vec!["t1".into()],
            is_pull: true,
        };

        let plan = task.build_stream_plan();
        assert!(!plan.is_empty());
        assert_eq!(plan.peer_count(), 1);
    }

    #[test]
    fn local_sync_push_builds_transfer_plan() {
        let task = LocalSyncTask {
            source: ep(7001),
            target: ep(7002),
            ranges: vec![(tok(0), tok(100))],
            keyspace: "ks".into(),
            tables: vec!["t1".into()],
            is_pull: false,
        };

        let plan = task.build_stream_plan();
        assert!(!plan.is_empty());
    }

    #[test]
    fn local_sync_execute_succeeds() {
        let task = LocalSyncTask {
            source: ep(7001),
            target: ep(7002),
            ranges: vec![(tok(0), tok(100))],
            keyspace: "ks".into(),
            tables: vec!["t1".into()],
            is_pull: true,
        };

        let result = task.execute().unwrap();
        assert_eq!(result.ranges_synced, 1);
    }

    #[test]
    fn remote_sync_builds_descriptor() {
        let task = RemoteSyncTask {
            source: ep(7001),
            target: ep(7002),
            ranges: vec![(tok(0), tok(100))],
            keyspace: "ks".into(),
            tables: vec!["t1".into()],
        };

        let desc = task.build_descriptor();
        assert_eq!(desc.source, ep(7001));
        assert_eq!(desc.target, ep(7002));
        assert_eq!(desc.ranges.len(), 1);
    }

    #[test]
    fn remote_sync_serde() {
        let task = RemoteSyncTask {
            source: ep(7001),
            target: ep(7002),
            ranges: vec![(tok(0), tok(100))],
            keyspace: "ks".into(),
            tables: vec!["t1".into()],
        };
        let json = serde_json::to_string(&task).unwrap();
        let deser: RemoteSyncTask = serde_json::from_str(&json).unwrap();
        assert_eq!(deser.source, ep(7001));
    }

    #[test]
    fn streaming_repair_task_execute() {
        let plan = StreamPlan::new(StreamOperation::Repair);
        let task = StreamingRepairTask::new(plan, uuid::Uuid::new_v4());
        let result = task.execute().unwrap();
        assert_eq!(result.bytes_streamed, 0);
    }

    #[test]
    fn sync_error_display() {
        let e = SyncError::StreamingFailed("timeout".into());
        assert!(e.to_string().contains("timeout"));

        let e = SyncError::RemoteFailed {
            endpoint: ep(7001),
            reason: "down".into(),
        };
        assert!(e.to_string().contains("down"));
    }
}
