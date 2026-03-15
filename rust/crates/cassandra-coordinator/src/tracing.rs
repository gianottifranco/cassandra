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
use std::time::Instant;

use parking_lot::Mutex;
use uuid::Uuid;

/// A tracing session for a single coordinated request.
#[derive(Debug, Clone)]
pub struct TraceSession {
    /// Unique session ID.
    pub session_id: Uuid,
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
}
