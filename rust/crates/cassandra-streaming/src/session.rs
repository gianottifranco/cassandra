// Licensed under Apache License, Version 2.0.

//! Streaming session: bidirectional channel between two nodes.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.streaming.StreamSession`
//! - `org.apache.cassandra.streaming.StreamResultFuture`

use std::collections::HashMap;
use std::fmt;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use cassandra_cluster_metadata::Endpoint;

use crate::transfer::{StreamRetryPolicy, StreamTransfer, TransferState};

/// Unique session identifier.
pub type StreamSessionId = Uuid;

/// State machine for a streaming session.
///
/// Matches Java's StreamSession states:
/// ```text
/// Initialized → Preparing → Streaming → Complete
///                                     ↘ Failed
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum StreamSessionState {
    /// Session created, not yet started.
    Initialized,
    /// Negotiating what to stream.
    Preparing,
    /// Actively transferring data.
    Streaming,
    /// All transfers completed successfully.
    Complete,
    /// Session failed (may be retried at plan level).
    Failed,
}

impl fmt::Display for StreamSessionState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Initialized => write!(f, "INITIALIZED"),
            Self::Preparing => write!(f, "PREPARING"),
            Self::Streaming => write!(f, "STREAMING"),
            Self::Complete => write!(f, "COMPLETE"),
            Self::Failed => write!(f, "FAILED"),
        }
    }
}

/// A bidirectional streaming session between two nodes.
///
/// Each session manages a set of outgoing and incoming transfers.
/// The session is identified by a UUID and tracks progress, state, and errors.
#[derive(Debug)]
pub struct StreamSession {
    /// Unique session identifier.
    pub id: StreamSessionId,
    /// Remote peer endpoint.
    pub peer: Endpoint,
    /// Current session state.
    pub state: StreamSessionState,
    /// Outgoing transfers (sending to peer).
    pub outgoing: HashMap<Uuid, StreamTransfer>,
    /// Incoming transfers (receiving from peer).
    pub incoming: HashMap<Uuid, StreamTransfer>,
    /// When the session was created.
    pub created_at: Instant,
    /// Description of the operation causing this session.
    pub description: String,
    /// Error message if the session failed.
    pub error: Option<String>,
    /// Retry policy for reconnection.
    pub retry_policy: StreamRetryPolicy,
    /// Number of reconnection attempts made.
    pub reconnect_attempts: u32,
    /// Keep-alive interval for long-running sessions.
    pub keep_alive_interval: Duration,
    /// Last time a keep-alive was sent/received.
    pub last_keep_alive: Instant,
}

impl StreamSession {
    /// Create a new session to stream with the given peer.
    pub fn new(peer: Endpoint, description: impl Into<String>) -> Self {
        let now = Instant::now();
        Self {
            id: Uuid::new_v4(),
            peer,
            state: StreamSessionState::Initialized,
            outgoing: HashMap::new(),
            incoming: HashMap::new(),
            created_at: now,
            description: description.into(),
            error: None,
            retry_policy: StreamRetryPolicy::default(),
            reconnect_attempts: 0,
            keep_alive_interval: Duration::from_secs(10),
            last_keep_alive: now,
        }
    }

    /// Create a session with a custom retry policy.
    pub fn with_retry_policy(mut self, policy: StreamRetryPolicy) -> Self {
        self.retry_policy = policy;
        self
    }

    /// Add an outgoing transfer to this session.
    pub fn add_outgoing(&mut self, transfer: StreamTransfer) {
        self.outgoing.insert(transfer.id, transfer);
    }

    /// Add an incoming transfer to this session.
    pub fn add_incoming(&mut self, transfer: StreamTransfer) {
        self.incoming.insert(transfer.id, transfer);
    }

    /// Transition to Preparing state.
    pub fn prepare(&mut self) -> Result<(), StreamSessionError> {
        if self.state != StreamSessionState::Initialized {
            return Err(StreamSessionError::InvalidTransition {
                from: self.state,
                to: StreamSessionState::Preparing,
            });
        }
        self.state = StreamSessionState::Preparing;
        Ok(())
    }

    /// Transition to Streaming state.
    pub fn start_streaming(&mut self) -> Result<(), StreamSessionError> {
        if self.state != StreamSessionState::Preparing {
            return Err(StreamSessionError::InvalidTransition {
                from: self.state,
                to: StreamSessionState::Streaming,
            });
        }
        self.state = StreamSessionState::Streaming;
        Ok(())
    }

    /// Transition to Complete state.
    pub fn complete(&mut self) -> Result<(), StreamSessionError> {
        if self.state != StreamSessionState::Streaming {
            return Err(StreamSessionError::InvalidTransition {
                from: self.state,
                to: StreamSessionState::Complete,
            });
        }
        // Verify all transfers are complete
        let all_out_done = self
            .outgoing
            .values()
            .all(|t| t.state == TransferState::Complete);
        let all_in_done = self
            .incoming
            .values()
            .all(|t| t.state == TransferState::Complete);

        if !all_out_done || !all_in_done {
            return Err(StreamSessionError::TransfersIncomplete);
        }

        self.state = StreamSessionState::Complete;
        Ok(())
    }

    /// Mark the session as failed with an error message.
    pub fn fail(&mut self, error: impl Into<String>) {
        self.state = StreamSessionState::Failed;
        self.error = Some(error.into());
    }

    /// Attempt to reconnect the session after a failure.
    ///
    /// Returns the delay to wait before reconnecting, or `None` if
    /// max retries are exhausted.
    pub fn try_reconnect(&mut self) -> Option<Duration> {
        if !self.retry_policy.should_retry(self.reconnect_attempts) {
            return None;
        }
        let delay = self.retry_policy.delay_for_attempt(self.reconnect_attempts);
        self.reconnect_attempts += 1;
        // Reset state to allow re-preparation
        self.state = StreamSessionState::Initialized;
        self.error = None;
        Some(delay)
    }

    /// Record a keep-alive acknowledgment.
    pub fn record_keep_alive(&mut self) {
        self.last_keep_alive = Instant::now();
    }

    /// Whether the session has timed out (no keep-alive within 3x interval).
    pub fn is_timed_out(&self) -> bool {
        self.last_keep_alive.elapsed() > self.keep_alive_interval * 3
    }

    /// Total bytes to send across all outgoing transfers.
    pub fn total_outgoing_bytes(&self) -> u64 {
        self.outgoing.values().map(|t| t.total_bytes).sum()
    }

    /// Total bytes to receive across all incoming transfers.
    pub fn total_incoming_bytes(&self) -> u64 {
        self.incoming.values().map(|t| t.total_bytes).sum()
    }

    /// Bytes sent so far.
    pub fn bytes_sent(&self) -> u64 {
        self.outgoing.values().map(|t| t.bytes_transferred).sum()
    }

    /// Bytes received so far.
    pub fn bytes_received(&self) -> u64 {
        self.incoming.values().map(|t| t.bytes_transferred).sum()
    }

    /// Overall progress (0.0 to 1.0) combining outgoing and incoming.
    pub fn progress(&self) -> f64 {
        let total = self.total_outgoing_bytes() + self.total_incoming_bytes();
        if total == 0 {
            return if self.state == StreamSessionState::Complete {
                1.0
            } else {
                0.0
            };
        }
        (self.bytes_sent() + self.bytes_received()) as f64 / total as f64
    }

    /// Duration since session creation.
    pub fn elapsed(&self) -> std::time::Duration {
        self.created_at.elapsed()
    }

    /// Summary of the session for logging/metrics.
    pub fn summary(&self) -> SessionSummary {
        SessionSummary {
            id: self.id,
            peer: self.peer,
            state: self.state,
            outgoing_count: self.outgoing.len(),
            incoming_count: self.incoming.len(),
            bytes_sent: self.bytes_sent(),
            bytes_received: self.bytes_received(),
            progress: self.progress(),
            description: self.description.clone(),
            error: self.error.clone(),
            reconnect_attempts: self.reconnect_attempts,
        }
    }
}

/// Serializable summary of a session for reporting.
#[derive(Debug, Clone, Serialize)]
pub struct SessionSummary {
    pub id: StreamSessionId,
    pub peer: Endpoint,
    pub state: StreamSessionState,
    pub outgoing_count: usize,
    pub incoming_count: usize,
    pub bytes_sent: u64,
    pub bytes_received: u64,
    pub progress: f64,
    pub description: String,
    pub error: Option<String>,
    pub reconnect_attempts: u32,
}

/// Errors from the streaming session state machine.
#[derive(Debug, thiserror::Error)]
pub enum StreamSessionError {
    #[error("Invalid state transition from {from} to {to}")]
    InvalidTransition {
        from: StreamSessionState,
        to: StreamSessionState,
    },

    #[error("Cannot complete: not all transfers are finished")]
    TransfersIncomplete,
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

    #[test]
    fn session_state_transitions_happy_path() {
        let mut session = StreamSession::new(ep(7002), "bootstrap");
        assert_eq!(session.state, StreamSessionState::Initialized);

        session.prepare().unwrap();
        assert_eq!(session.state, StreamSessionState::Preparing);

        session.start_streaming().unwrap();
        assert_eq!(session.state, StreamSessionState::Streaming);

        // Add completed transfers
        let mut t = StreamTransfer::new("ks".into(), "t".into(), vec![]);
        t.state = TransferState::Complete;
        session.add_outgoing(t);

        session.complete().unwrap();
        assert_eq!(session.state, StreamSessionState::Complete);
    }

    #[test]
    fn session_invalid_transition() {
        let mut session = StreamSession::new(ep(7002), "test");
        // Can't go from Initialized directly to Streaming
        let result = session.start_streaming();
        assert!(result.is_err());
    }

    #[test]
    fn session_cannot_complete_with_pending_transfers() {
        let mut session = StreamSession::new(ep(7002), "test");
        session.prepare().unwrap();
        session.start_streaming().unwrap();

        // Add a pending transfer
        let t = StreamTransfer::new("ks".into(), "t".into(), vec![]);
        session.add_outgoing(t);

        let result = session.complete();
        assert!(result.is_err());
    }

    #[test]
    fn session_failure() {
        let mut session = StreamSession::new(ep(7002), "test");
        session.fail("connection lost");
        assert_eq!(session.state, StreamSessionState::Failed);
        assert_eq!(session.error.as_deref(), Some("connection lost"));
    }

    #[test]
    fn session_progress() {
        let mut session = StreamSession::new(ep(7002), "test");
        let mut t1 = StreamTransfer::new("ks".into(), "t1".into(), vec![]);
        t1.total_bytes = 1000;
        t1.bytes_transferred = 500;
        session.add_outgoing(t1);

        let mut t2 = StreamTransfer::new("ks".into(), "t2".into(), vec![]);
        t2.total_bytes = 1000;
        t2.bytes_transferred = 1000;
        session.add_incoming(t2);

        // 1500 / 2000 = 0.75
        assert!((session.progress() - 0.75).abs() < f64::EPSILON);
    }

    #[test]
    fn session_summary() {
        let session = StreamSession::new(ep(7002), "bootstrap dc1");
        let summary = session.summary();
        assert_eq!(summary.peer, ep(7002));
        assert_eq!(summary.description, "bootstrap dc1");
        assert_eq!(summary.state, StreamSessionState::Initialized);
        assert_eq!(summary.reconnect_attempts, 0);
    }

    #[test]
    fn session_state_display() {
        assert_eq!(StreamSessionState::Initialized.to_string(), "INITIALIZED");
        assert_eq!(StreamSessionState::Preparing.to_string(), "PREPARING");
        assert_eq!(StreamSessionState::Streaming.to_string(), "STREAMING");
        assert_eq!(StreamSessionState::Complete.to_string(), "COMPLETE");
        assert_eq!(StreamSessionState::Failed.to_string(), "FAILED");
    }

    #[test]
    fn session_reconnect() {
        let mut session = StreamSession::new(ep(7002), "test");
        session.fail("timeout");
        assert_eq!(session.state, StreamSessionState::Failed);

        // First reconnect should succeed
        let delay = session.try_reconnect();
        assert!(delay.is_some());
        assert_eq!(session.state, StreamSessionState::Initialized);
        assert_eq!(session.reconnect_attempts, 1);
        assert!(session.error.is_none());
    }

    #[test]
    fn session_reconnect_exhausted() {
        let policy = StreamRetryPolicy {
            max_attempts: 2,
            ..StreamRetryPolicy::default()
        };
        let mut session = StreamSession::new(ep(7002), "test").with_retry_policy(policy);

        // Use up all reconnect attempts
        session.fail("timeout");
        assert!(session.try_reconnect().is_some());
        session.fail("timeout again");
        assert!(session.try_reconnect().is_some());
        session.fail("final timeout");
        assert!(session.try_reconnect().is_none()); // exhausted
    }

    #[test]
    fn session_keep_alive() {
        let mut session = StreamSession::new(ep(7002), "test");
        session.keep_alive_interval = Duration::from_millis(1);
        session.record_keep_alive();
        // Just recorded, should not be timed out
        assert!(!session.is_timed_out());
    }
}
