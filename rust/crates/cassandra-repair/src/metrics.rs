// Licensed under Apache License, Version 2.0.

//! Repair metrics: atomic counters for repair progress.

use std::sync::atomic::{AtomicU64, Ordering};

/// Atomic counters for repair progress.
#[derive(Debug)]
pub struct RepairMetrics {
    pub trees_built: AtomicU64,
    pub trees_exchanged: AtomicU64,
    pub ranges_repaired: AtomicU64,
    pub bytes_streamed: AtomicU64,
    pub sessions_active: AtomicU64,
    pub sessions_completed: AtomicU64,
    pub sessions_failed: AtomicU64,
}

impl RepairMetrics {
    pub fn new() -> Self {
        Self {
            trees_built: AtomicU64::new(0),
            trees_exchanged: AtomicU64::new(0),
            ranges_repaired: AtomicU64::new(0),
            bytes_streamed: AtomicU64::new(0),
            sessions_active: AtomicU64::new(0),
            sessions_completed: AtomicU64::new(0),
            sessions_failed: AtomicU64::new(0),
        }
    }

    pub fn record_tree_built(&self) {
        self.trees_built.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_tree_exchanged(&self) {
        self.trees_exchanged.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_range_repaired(&self) {
        self.ranges_repaired.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_bytes_streamed(&self, n: u64) {
        self.bytes_streamed.fetch_add(n, Ordering::Relaxed);
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

    pub fn snapshot(&self) -> RepairMetricsSnapshot {
        RepairMetricsSnapshot {
            trees_built: self.trees_built.load(Ordering::Relaxed),
            trees_exchanged: self.trees_exchanged.load(Ordering::Relaxed),
            ranges_repaired: self.ranges_repaired.load(Ordering::Relaxed),
            bytes_streamed: self.bytes_streamed.load(Ordering::Relaxed),
            sessions_active: self.sessions_active.load(Ordering::Relaxed),
            sessions_completed: self.sessions_completed.load(Ordering::Relaxed),
            sessions_failed: self.sessions_failed.load(Ordering::Relaxed),
        }
    }
}

impl Default for RepairMetrics {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct RepairMetricsSnapshot {
    pub trees_built: u64,
    pub trees_exchanged: u64,
    pub ranges_repaired: u64,
    pub bytes_streamed: u64,
    pub sessions_active: u64,
    pub sessions_completed: u64,
    pub sessions_failed: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metrics_counting() {
        let m = RepairMetrics::new();
        m.record_tree_built();
        m.record_tree_built();
        m.record_tree_exchanged();
        m.record_range_repaired();
        m.record_bytes_streamed(1024);
        m.session_started();
        m.session_completed();

        let snap = m.snapshot();
        assert_eq!(snap.trees_built, 2);
        assert_eq!(snap.trees_exchanged, 1);
        assert_eq!(snap.ranges_repaired, 1);
        assert_eq!(snap.bytes_streamed, 1024);
        assert_eq!(snap.sessions_active, 0);
        assert_eq!(snap.sessions_completed, 1);
    }
}
