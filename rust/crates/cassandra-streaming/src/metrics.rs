// Licensed under Apache License, Version 2.0.

//! Streaming metrics: atomic counters for progress tracking.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.streaming.StreamResultFuture` (progress tracking)
//! - `org.apache.cassandra.metrics.StreamingMetrics`

use std::sync::atomic::{AtomicU64, Ordering};

/// Atomic counters for streaming progress.
#[derive(Debug)]
pub struct StreamingMetrics {
    pub bytes_sent: AtomicU64,
    pub bytes_received: AtomicU64,
    pub chunks_sent: AtomicU64,
    pub chunks_received: AtomicU64,
    pub sessions_active: AtomicU64,
    pub sessions_completed: AtomicU64,
    pub sessions_failed: AtomicU64,
    pub retries: AtomicU64,
}

impl StreamingMetrics {
    pub fn new() -> Self {
        Self {
            bytes_sent: AtomicU64::new(0),
            bytes_received: AtomicU64::new(0),
            chunks_sent: AtomicU64::new(0),
            chunks_received: AtomicU64::new(0),
            sessions_active: AtomicU64::new(0),
            sessions_completed: AtomicU64::new(0),
            sessions_failed: AtomicU64::new(0),
            retries: AtomicU64::new(0),
        }
    }

    pub fn record_bytes_sent(&self, n: u64) {
        self.bytes_sent.fetch_add(n, Ordering::Relaxed);
    }

    pub fn record_bytes_received(&self, n: u64) {
        self.bytes_received.fetch_add(n, Ordering::Relaxed);
    }

    pub fn record_chunk_sent(&self) {
        self.chunks_sent.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_chunk_received(&self) {
        self.chunks_received.fetch_add(1, Ordering::Relaxed);
    }

    pub fn session_started(&self) {
        self.sessions_active.fetch_add(1, Ordering::Relaxed);
    }

    pub fn session_completed(&self) {
        self.sessions_active.fetch_sub(1, Ordering::Relaxed);
        self.sessions_completed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn session_failed(&self) {
        self.sessions_active.fetch_sub(1, Ordering::Relaxed);
        self.sessions_failed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_retry(&self) {
        self.retries.fetch_add(1, Ordering::Relaxed);
    }

    /// Snapshot all counters for reporting.
    pub fn snapshot(&self) -> StreamingMetricsSnapshot {
        StreamingMetricsSnapshot {
            bytes_sent: self.bytes_sent.load(Ordering::Relaxed),
            bytes_received: self.bytes_received.load(Ordering::Relaxed),
            chunks_sent: self.chunks_sent.load(Ordering::Relaxed),
            chunks_received: self.chunks_received.load(Ordering::Relaxed),
            sessions_active: self.sessions_active.load(Ordering::Relaxed),
            sessions_completed: self.sessions_completed.load(Ordering::Relaxed),
            sessions_failed: self.sessions_failed.load(Ordering::Relaxed),
            retries: self.retries.load(Ordering::Relaxed),
        }
    }
}

impl Default for StreamingMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Point-in-time snapshot of streaming metrics.
#[derive(Debug, Clone, serde::Serialize)]
pub struct StreamingMetricsSnapshot {
    pub bytes_sent: u64,
    pub bytes_received: u64,
    pub chunks_sent: u64,
    pub chunks_received: u64,
    pub sessions_active: u64,
    pub sessions_completed: u64,
    pub sessions_failed: u64,
    pub retries: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metrics_counting() {
        let m = StreamingMetrics::new();
        m.record_bytes_sent(1024);
        m.record_bytes_sent(2048);
        m.record_bytes_received(512);
        m.record_chunk_sent();
        m.record_chunk_sent();
        m.record_chunk_received();
        m.session_started();
        m.session_started();
        m.session_completed();
        m.record_retry();

        let snap = m.snapshot();
        assert_eq!(snap.bytes_sent, 3072);
        assert_eq!(snap.bytes_received, 512);
        assert_eq!(snap.chunks_sent, 2);
        assert_eq!(snap.chunks_received, 1);
        assert_eq!(snap.sessions_active, 1);
        assert_eq!(snap.sessions_completed, 1);
        assert_eq!(snap.retries, 1);
    }

    #[test]
    fn session_failure_tracking() {
        let m = StreamingMetrics::new();
        m.session_started();
        m.session_failed();

        let snap = m.snapshot();
        assert_eq!(snap.sessions_active, 0);
        assert_eq!(snap.sessions_failed, 1);
    }
}
