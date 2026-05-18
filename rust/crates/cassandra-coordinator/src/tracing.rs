// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.
// SPDX-License-Identifier: Apache-2.0

//! Coordinator-level request tracing.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.tracing.Tracing`
//! - `org.apache.cassandra.tracing.TraceState`

use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// A tracing session for a single coordinated request.
#[derive(Debug, Clone)]
pub struct TraceSession {
    /// Unique session ID.
    pub session_id: Uuid,
    /// Wall-clock epoch millis when the session started.
    pub started_at_ms: u64,
    /// When the session was created.
    pub started_at: Instant,
    /// Events collected during this session.
    pub events: Arc<Mutex<Vec<TraceEvent>>>,
}

/// A single trace event within a session.
#[derive(Debug, Clone)]
pub struct TraceEvent {
    /// Activity description.
    pub activity: String,
    /// Source address/component.
    pub source: String,
    /// Elapsed microseconds since session start.
    pub elapsed_us: u64,
}

impl TraceSession {
    /// Start a new tracing session.
    pub fn new() -> Self {
        Self {
            session_id: Uuid::new_v4(),
            started_at_ms: epoch_millis(),
            started_at: Instant::now(),
            events: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Record a trace event.
    pub fn trace(&self, source: impl Into<String>, activity: impl Into<String>) {
        let elapsed_us = self.started_at.elapsed().as_micros() as u64;
        self.events.lock().push(TraceEvent {
            activity: activity.into(),
            source: source.into(),
            elapsed_us,
        });
    }

    /// Get all events (snapshot).
    pub fn events(&self) -> Vec<TraceEvent> {
        self.events.lock().clone()
    }

    /// Session duration so far.
    pub fn duration_us(&self) -> u64 {
        self.started_at.elapsed().as_micros() as u64
    }
}

impl Default for TraceSession {
    fn default() -> Self {
        Self::new()
    }
}

fn epoch_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Materialized row for `system_traces.events`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TraceEventRow {
    pub session_id: Uuid,
    pub event_id: Uuid,
    pub activity: String,
    pub source: String,
    pub source_port: i32,
    pub source_elapsed: i32,
    pub thread: String,
}

impl TraceEventRow {
    pub fn from_event(session_id: Uuid, index: usize, event: &TraceEvent) -> Self {
        Self {
            session_id,
            event_id: Uuid::from_u128(session_id.as_u128() ^ ((index as u128) + 1)),
            activity: event.activity.clone(),
            source: event.source.clone(),
            source_port: 0,
            source_elapsed: event.elapsed_us.min(i32::MAX as u64) as i32,
            thread: std::thread::current()
                .name()
                .unwrap_or("unknown")
                .to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trace_session_records_events() {
        let session = TraceSession::new();
        session.trace("coordinator", "Determining replicas");
        session.trace("coordinator", "Sending mutations");
        session.trace("replica:7002", "Mutation applied");

        let events = session.events();
        assert_eq!(events.len(), 3);
        assert_eq!(events[0].activity, "Determining replicas");
        assert_eq!(events[2].source, "replica:7002");
    }

    #[test]
    fn trace_session_elapsed() {
        let session = TraceSession::new();
        std::thread::sleep(std::time::Duration::from_millis(10));
        session.trace("test", "after sleep");

        let events = session.events();
        assert!(events[0].elapsed_us > 0);
    }

    #[test]
    fn trace_event_projects_to_system_traces_row() {
        let session = TraceSession::new();
        session.trace("coordinator", "Selecting replicas");

        let row = TraceEventRow::from_event(session.session_id, 0, &session.events()[0]);
        assert_eq!(row.session_id, session.session_id);
        assert_eq!(row.activity, "Selecting replicas");
        assert_eq!(row.source, "coordinator");
        assert!(row.source_elapsed >= 0);
    }
}
