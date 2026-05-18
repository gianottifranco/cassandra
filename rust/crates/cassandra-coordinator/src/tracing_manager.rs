// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.
// SPDX-License-Identifier: Apache-2.0

//! Global tracing session store.
//!
//! Manages the lifecycle of [`TraceSession`]s: creation with probabilistic
//! sampling, active-session tracking, and a bounded ring buffer of recently
//! finished sessions.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.tracing.Tracing` — singleton session registry

use std::collections::VecDeque;
use std::time::{SystemTime, UNIX_EPOCH};

use dashmap::DashMap;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::tracing::{TraceEventRow, TraceSession};

/// Maximum number of finished sessions kept in the ring buffer.
const RECENT_CAPACITY: usize = 1000;

/// Configuration for the tracing subsystem.
#[derive(Debug, Clone)]
pub struct TracingConfig {
    /// Whether tracing is enabled at all.
    pub enabled: bool,
    /// Probability (0.0–1.0) that a new session is actually created.
    pub sample_rate: f64,
    /// Default TTL for persisted trace rows (seconds).
    pub default_ttl_secs: u64,
}

impl Default for TracingConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            sample_rate: 1.0,
            default_ttl_secs: 86_400,
        }
    }
}

/// A completed tracing session together with its finish timestamp.
#[derive(Debug, Clone)]
pub struct FinishedSession {
    /// The underlying trace session.
    pub session: TraceSession,
    /// Wall-clock epoch millis when the session was finished.
    pub finished_at_ms: u64,
}

/// Materialized row for `system_traces.sessions`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TraceSessionRow {
    pub session_id: Uuid,
    pub client: String,
    pub command: String,
    pub coordinator: String,
    pub coordinator_port: i32,
    pub duration: i32,
    pub parameters: Vec<(String, String)>,
    pub request: String,
    pub started_at: u64,
}

/// Materialized `system_traces` table rows.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SystemTracesRows {
    pub sessions: Vec<TraceSessionRow>,
    pub events: Vec<TraceEventRow>,
}

/// Global store for active and recently finished tracing sessions.
pub struct TracingManager {
    /// Currently in-flight sessions keyed by session ID.
    active: DashMap<Uuid, TraceSession>,
    /// Ring buffer of recently finished sessions (newest at back).
    recent: Mutex<VecDeque<FinishedSession>>,
    /// Tracing configuration.
    config: TracingConfig,
}

impl TracingManager {
    /// Create a new manager with the given configuration.
    pub fn new(config: TracingConfig) -> Self {
        Self {
            active: DashMap::new(),
            recent: Mutex::new(VecDeque::with_capacity(RECENT_CAPACITY)),
            config,
        }
    }

    /// Begin a new tracing session.
    ///
    /// Returns `None` when tracing is disabled or the random sample misses.
    pub fn begin_session(&self) -> Option<TraceSession> {
        if !self.config.enabled {
            return None;
        }
        if rand::random::<f64>() >= self.config.sample_rate {
            return None;
        }
        let session = TraceSession::new();
        self.active.insert(session.session_id, session.clone());
        Some(session)
    }

    /// Move a session from active to the recent ring buffer.
    ///
    /// No-op if `session_id` is not in the active map.
    pub fn finish_session(&self, session_id: Uuid) {
        if let Some((_, session)) = self.active.remove(&session_id) {
            let finished = FinishedSession {
                session,
                finished_at_ms: epoch_millis(),
            };
            let mut recent = self.recent.lock();
            if recent.len() == RECENT_CAPACITY {
                recent.pop_front();
            }
            recent.push_back(finished);
        }
    }

    /// Look up a session by ID, checking active sessions first, then recent.
    pub fn get_session(&self, session_id: &Uuid) -> Option<TraceSession> {
        if let Some(entry) = self.active.get(session_id) {
            return Some(entry.value().clone());
        }
        let recent = self.recent.lock();
        recent
            .iter()
            .find(|f| f.session.session_id == *session_id)
            .map(|f| f.session.clone())
    }

    /// Return the most recent finished sessions, newest first.
    pub fn list_recent_sessions(&self, limit: usize) -> Vec<FinishedSession> {
        let recent = self.recent.lock();
        recent.iter().rev().take(limit).cloned().collect()
    }

    /// Project recent finished sessions into rows matching the `system_traces`
    /// table definitions. This keeps the storage integration boundary explicit
    /// while preserving the Java-visible row shape.
    pub fn system_traces_rows(&self, limit: usize) -> SystemTracesRows {
        let recent = self.list_recent_sessions(limit);
        let mut rows = SystemTracesRows::default();

        for finished in recent {
            let session = &finished.session;
            rows.sessions.push(TraceSessionRow {
                session_id: session.session_id,
                client: "unknown".to_string(),
                command: "Execute CQL3 query".to_string(),
                coordinator: "local".to_string(),
                coordinator_port: 0,
                duration: session.duration_us().min(i32::MAX as u64) as i32,
                parameters: Vec::new(),
                request: "query".to_string(),
                started_at: session.started_at_ms,
            });

            for (idx, event) in session.events().iter().enumerate() {
                rows.events
                    .push(TraceEventRow::from_event(session.session_id, idx, event));
            }
        }

        rows
    }

    /// Number of currently active sessions.
    pub fn active_count(&self) -> usize {
        self.active.len()
    }
}

fn epoch_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    fn enabled_config() -> TracingConfig {
        TracingConfig {
            enabled: true,
            sample_rate: 1.0,
            default_ttl_secs: 3600,
        }
    }

    fn disabled_config() -> TracingConfig {
        TracingConfig {
            enabled: false,
            sample_rate: 1.0,
            default_ttl_secs: 3600,
        }
    }

    #[test]
    fn begin_session_returns_session_when_enabled() {
        let mgr = TracingManager::new(enabled_config());
        let session = mgr.begin_session();
        assert!(session.is_some());
        assert_eq!(mgr.active_count(), 1);
    }

    #[test]
    fn begin_session_returns_none_when_disabled() {
        let mgr = TracingManager::new(disabled_config());
        assert!(mgr.begin_session().is_none());
        assert_eq!(mgr.active_count(), 0);
    }

    #[test]
    fn begin_session_returns_none_when_sample_rate_zero() {
        let config = TracingConfig {
            enabled: true,
            sample_rate: 0.0,
            ..Default::default()
        };
        let mgr = TracingManager::new(config);
        // With sample_rate 0.0, random::<f64>() is always >= 0.0
        assert!(mgr.begin_session().is_none());
    }

    #[test]
    fn finish_session_moves_to_recent() {
        let mgr = TracingManager::new(enabled_config());
        let session = mgr.begin_session().unwrap();
        let id = session.session_id;

        assert_eq!(mgr.active_count(), 1);
        mgr.finish_session(id);
        assert_eq!(mgr.active_count(), 0);

        let recent = mgr.list_recent_sessions(10);
        assert_eq!(recent.len(), 1);
        assert_eq!(recent[0].session.session_id, id);
        assert!(recent[0].finished_at_ms > 0);
    }

    #[test]
    fn finish_session_noop_for_unknown_id() {
        let mgr = TracingManager::new(enabled_config());
        mgr.finish_session(Uuid::new_v4());
        assert_eq!(mgr.active_count(), 0);
        assert!(mgr.list_recent_sessions(10).is_empty());
    }

    #[test]
    fn get_session_finds_active() {
        let mgr = TracingManager::new(enabled_config());
        let session = mgr.begin_session().unwrap();
        let found = mgr.get_session(&session.session_id);
        assert!(found.is_some());
        assert_eq!(found.unwrap().session_id, session.session_id);
    }

    #[test]
    fn get_session_finds_recent() {
        let mgr = TracingManager::new(enabled_config());
        let session = mgr.begin_session().unwrap();
        let id = session.session_id;
        mgr.finish_session(id);

        let found = mgr.get_session(&id);
        assert!(found.is_some());
        assert_eq!(found.unwrap().session_id, id);
    }

    #[test]
    fn get_session_returns_none_for_unknown() {
        let mgr = TracingManager::new(enabled_config());
        assert!(mgr.get_session(&Uuid::new_v4()).is_none());
    }

    #[test]
    fn list_recent_sessions_respects_limit() {
        let mgr = TracingManager::new(enabled_config());
        for _ in 0..5 {
            let s = mgr.begin_session().unwrap();
            mgr.finish_session(s.session_id);
        }
        let recent = mgr.list_recent_sessions(3);
        assert_eq!(recent.len(), 3);
    }

    #[test]
    fn list_recent_sessions_newest_first() {
        let mgr = TracingManager::new(enabled_config());
        let s1 = mgr.begin_session().unwrap();
        mgr.finish_session(s1.session_id);
        let s2 = mgr.begin_session().unwrap();
        mgr.finish_session(s2.session_id);

        let recent = mgr.list_recent_sessions(10);
        assert_eq!(recent[0].session.session_id, s2.session_id);
        assert_eq!(recent[1].session.session_id, s1.session_id);
    }

    #[test]
    fn ring_buffer_evicts_oldest_at_capacity() {
        let config = enabled_config();
        let mgr = TracingManager::new(config);

        let mut first_id = Uuid::nil();
        for i in 0..=RECENT_CAPACITY {
            let s = mgr.begin_session().unwrap();
            if i == 0 {
                first_id = s.session_id;
            }
            mgr.finish_session(s.session_id);
        }

        // The very first session should have been evicted.
        assert!(mgr.get_session(&first_id).is_none());
        let recent = mgr.list_recent_sessions(RECENT_CAPACITY + 1);
        assert_eq!(recent.len(), RECENT_CAPACITY);
    }

    #[test]
    fn default_config_is_sane() {
        let cfg = TracingConfig::default();
        assert!(cfg.enabled);
        assert!((cfg.sample_rate - 1.0).abs() < f64::EPSILON);
        assert_eq!(cfg.default_ttl_secs, 86_400);
    }

    #[test]
    fn system_traces_rows_project_finished_sessions_and_events() {
        let mgr = TracingManager::new(enabled_config());
        let session = mgr.begin_session().unwrap();
        session.trace("coordinator", "Selecting replicas");
        session.trace("replica:7000", "Read command completed");
        let session_id = session.session_id;
        mgr.finish_session(session_id);

        let rows = mgr.system_traces_rows(10);
        assert_eq!(rows.sessions.len(), 1);
        assert_eq!(rows.sessions[0].session_id, session_id);
        assert_eq!(rows.sessions[0].command, "Execute CQL3 query");
        assert!(rows.sessions[0].started_at > 0);
        assert_eq!(rows.events.len(), 2);
        assert_eq!(rows.events[0].session_id, session_id);
        assert_eq!(rows.events[0].activity, "Selecting replicas");
    }
}
