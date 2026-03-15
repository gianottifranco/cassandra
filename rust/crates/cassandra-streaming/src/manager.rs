// Licensed under Apache License, Version 2.0.

//! Stream manager: singleton managing all active stream sessions.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.streaming.StreamManager`

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;
use tracing::{info, warn};

use crate::metrics::StreamingMetrics;
use crate::session::{SessionSummary, StreamSession, StreamSessionId, StreamSessionState};

/// Central manager for all active streaming sessions.
///
/// Tracks active sessions, provides progress snapshots, and supports
/// cancellation. Thread-safe via internal locking.
pub struct StreamManager {
    /// Active sessions indexed by ID.
    sessions: RwLock<HashMap<StreamSessionId, StreamSession>>,
    /// Global streaming metrics.
    pub metrics: Arc<StreamingMetrics>,
}

impl StreamManager {
    /// Create a new stream manager.
    pub fn new() -> Self {
        Self {
            sessions: RwLock::new(HashMap::new()),
            metrics: Arc::new(StreamingMetrics::new()),
        }
    }

    /// Register a new streaming session.
    pub fn register_session(&self, session: StreamSession) -> StreamSessionId {
        let id = session.id;
        info!(
            session_id = %id,
            peer = %session.peer,
            description = %session.description,
            "Registering stream session"
        );
        self.metrics.session_started();
        self.sessions.write().insert(id, session);
        id
    }

    /// Mark a session as complete and remove it from active tracking.
    pub fn complete_session(&self, id: &StreamSessionId) {
        let mut sessions = self.sessions.write();
        if let Some(session) = sessions.get_mut(id) {
            if session.state != StreamSessionState::Complete {
                session.state = StreamSessionState::Complete;
            }
            info!(session_id = %id, "Stream session completed");
            self.metrics.session_completed();
        }
        sessions.remove(id);
    }

    /// Mark a session as failed.
    pub fn fail_session(&self, id: &StreamSessionId, error: impl Into<String>) {
        let error_msg = error.into();
        let mut sessions = self.sessions.write();
        if let Some(session) = sessions.get_mut(id) {
            session.fail(error_msg.clone());
            warn!(session_id = %id, error = %error_msg, "Stream session failed");
            self.metrics.session_failed();
        }
        sessions.remove(id);
    }

    /// Cancel a session.
    pub fn cancel_session(&self, id: &StreamSessionId) -> bool {
        let mut sessions = self.sessions.write();
        if let Some(session) = sessions.get_mut(id) {
            session.fail("cancelled by operator");
            self.metrics.session_failed();
            sessions.remove(id);
            info!(session_id = %id, "Stream session cancelled");
            true
        } else {
            false
        }
    }

    /// Get a summary of all active sessions.
    pub fn active_sessions(&self) -> Vec<SessionSummary> {
        self.sessions
            .read()
            .values()
            .map(|s| s.summary())
            .collect()
    }

    /// Number of active sessions.
    pub fn active_session_count(&self) -> usize {
        self.sessions.read().len()
    }

    /// Get summary for a specific session.
    pub fn session_summary(&self, id: &StreamSessionId) -> Option<SessionSummary> {
        self.sessions.read().get(id).map(|s| s.summary())
    }

    /// Access a session mutably via a closure.
    pub fn with_session_mut<F, R>(&self, id: &StreamSessionId, f: F) -> Option<R>
    where
        F: FnOnce(&mut StreamSession) -> R,
    {
        self.sessions.write().get_mut(id).map(f)
    }
}

impl Default for StreamManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cassandra_cluster_metadata::Endpoint;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    fn ep(port: u16) -> Endpoint {
        Endpoint::new(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            port,
        ))
    }

    #[test]
    fn register_and_complete() {
        let mgr = StreamManager::new();
        let session = StreamSession::new(ep(7002), "test");
        let id = mgr.register_session(session);

        assert_eq!(mgr.active_session_count(), 1);

        mgr.complete_session(&id);
        assert_eq!(mgr.active_session_count(), 0);

        let snap = mgr.metrics.snapshot();
        assert_eq!(snap.sessions_completed, 1);
    }

    #[test]
    fn register_and_fail() {
        let mgr = StreamManager::new();
        let session = StreamSession::new(ep(7002), "test");
        let id = mgr.register_session(session);

        mgr.fail_session(&id, "timeout");
        assert_eq!(mgr.active_session_count(), 0);

        let snap = mgr.metrics.snapshot();
        assert_eq!(snap.sessions_failed, 1);
    }

    #[test]
    fn cancel_session() {
        let mgr = StreamManager::new();
        let session = StreamSession::new(ep(7002), "test");
        let id = mgr.register_session(session);

        assert!(mgr.cancel_session(&id));
        assert_eq!(mgr.active_session_count(), 0);

        // Cancel non-existent
        assert!(!mgr.cancel_session(&uuid::Uuid::new_v4()));
    }

    #[test]
    fn multiple_sessions() {
        let mgr = StreamManager::new();
        let s1 = StreamSession::new(ep(7002), "bootstrap");
        let s2 = StreamSession::new(ep(7003), "repair");

        let id1 = mgr.register_session(s1);
        let id2 = mgr.register_session(s2);

        assert_eq!(mgr.active_session_count(), 2);

        let summaries = mgr.active_sessions();
        assert_eq!(summaries.len(), 2);

        mgr.complete_session(&id1);
        assert_eq!(mgr.active_session_count(), 1);

        mgr.complete_session(&id2);
        assert_eq!(mgr.active_session_count(), 0);
    }

    #[test]
    fn with_session_mut() {
        let mgr = StreamManager::new();
        let session = StreamSession::new(ep(7002), "test");
        let id = mgr.register_session(session);

        let result = mgr.with_session_mut(&id, |s| {
            s.prepare().unwrap();
            s.state
        });
        assert_eq!(result, Some(StreamSessionState::Preparing));
    }
}
