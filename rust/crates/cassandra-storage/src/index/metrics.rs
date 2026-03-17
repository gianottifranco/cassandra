// Licensed under Apache License, Version 2.0.

//! Index-level operational metrics (counters for inserts, searches, etc.).
//!
//! ## Java Oracle
//! - `org.apache.cassandra.index.SecondaryIndexManager` (metrics tracking)
//!
//! Follows the `CompactionMetrics` pattern: AtomicU64 counters with a
//! snapshot method for observation.

use std::sync::atomic::{AtomicU64, Ordering};

/// Operational metrics for the index subsystem.
#[derive(Debug, Default)]
pub struct IndexMetrics {
    pub inserts: AtomicU64,
    pub deletes: AtomicU64,
    pub searches: AtomicU64,
    pub range_searches: AtomicU64,
    pub vector_searches: AtomicU64,
    pub search_latency_us_total: AtomicU64,
    pub index_build_time_ms: AtomicU64,
}

impl IndexMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_insert(&self) {
        self.inserts.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_delete(&self) {
        self.deletes.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_search(&self, latency_us: u64) {
        self.searches.fetch_add(1, Ordering::Relaxed);
        self.search_latency_us_total
            .fetch_add(latency_us, Ordering::Relaxed);
    }

    pub fn record_range_search(&self, latency_us: u64) {
        self.range_searches.fetch_add(1, Ordering::Relaxed);
        self.search_latency_us_total
            .fetch_add(latency_us, Ordering::Relaxed);
    }

    pub fn record_vector_search(&self, latency_us: u64) {
        self.vector_searches.fetch_add(1, Ordering::Relaxed);
        self.search_latency_us_total
            .fetch_add(latency_us, Ordering::Relaxed);
    }

    pub fn record_build_time(&self, duration_ms: u64) {
        self.index_build_time_ms
            .fetch_add(duration_ms, Ordering::Relaxed);
    }

    /// Produce a point-in-time snapshot.
    pub fn snapshot(&self) -> IndexMetricsSnapshot {
        IndexMetricsSnapshot {
            inserts: self.inserts.load(Ordering::Relaxed),
            deletes: self.deletes.load(Ordering::Relaxed),
            searches: self.searches.load(Ordering::Relaxed),
            range_searches: self.range_searches.load(Ordering::Relaxed),
            vector_searches: self.vector_searches.load(Ordering::Relaxed),
            search_latency_us_total: self.search_latency_us_total.load(Ordering::Relaxed),
            index_build_time_ms: self.index_build_time_ms.load(Ordering::Relaxed),
        }
    }
}

/// An immutable snapshot of index metrics.
#[derive(Debug, Clone)]
pub struct IndexMetricsSnapshot {
    pub inserts: u64,
    pub deletes: u64,
    pub searches: u64,
    pub range_searches: u64,
    pub vector_searches: u64,
    pub search_latency_us_total: u64,
    pub index_build_time_ms: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_metrics_are_zero() {
        let m = IndexMetrics::new();
        let s = m.snapshot();
        assert_eq!(s.inserts, 0);
        assert_eq!(s.deletes, 0);
        assert_eq!(s.searches, 0);
        assert_eq!(s.range_searches, 0);
        assert_eq!(s.vector_searches, 0);
        assert_eq!(s.search_latency_us_total, 0);
        assert_eq!(s.index_build_time_ms, 0);
    }

    #[test]
    fn record_and_snapshot() {
        let m = IndexMetrics::new();
        m.record_insert();
        m.record_insert();
        m.record_delete();
        m.record_search(100);
        m.record_range_search(200);
        m.record_vector_search(50);
        m.record_build_time(500);

        let s = m.snapshot();
        assert_eq!(s.inserts, 2);
        assert_eq!(s.deletes, 1);
        assert_eq!(s.searches, 1);
        assert_eq!(s.range_searches, 1);
        assert_eq!(s.vector_searches, 1);
        assert_eq!(s.search_latency_us_total, 350); // 100 + 200 + 50
        assert_eq!(s.index_build_time_ms, 500);
    }
}
