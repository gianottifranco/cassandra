// Licensed under Apache License, Version 2.0.

//! Prometheus-native metrics registry and exposition.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.metrics.*` (Dropwizard Metrics)
//!
//! ## Design
//! Uses the `prometheus` crate directly instead of bridging from Dropwizard.
//! Metric names follow Prometheus conventions but preserve Cassandra semantics.
//! See ADR-013 for rationale.

use std::sync::Arc;

use parking_lot::RwLock;
use prometheus::{
    Counter, CounterVec, Encoder, Gauge, GaugeVec, Histogram, HistogramOpts, HistogramVec,
    IntCounter, IntCounterVec, IntGauge, IntGaugeVec, Opts, Registry, TextEncoder,
};
use tracing::warn;

/// Centralized metrics registry for Cassandra.
pub struct MetricsRegistry {
    registry: Registry,
    pub client_request_latency: HistogramVec,
    pub read_count: IntCounter,
    pub write_count: IntCounter,
    pub live_sstable_count: IntGauge,
    pub pending_compactions: IntGauge,
    pub connected_native_clients: IntGauge,
    pub tombstone_scanned: IntCounter,
    pub key_cache_hit_rate: Gauge,
    pub storage_load_bytes: IntGauge,
    pub exceptions_count: IntCounterVec,

    // Repair metrics
    pub repair_trees_built: IntGauge,
    pub repair_trees_exchanged: IntGauge,
    pub repair_ranges_repaired: IntGauge,
    pub repair_bytes_streamed: IntGauge,
    pub repair_sessions_active: IntGauge,
    pub repair_sessions_completed: IntGauge,
    pub repair_sessions_failed: IntGauge,
}

impl MetricsRegistry {
    /// Create a new metrics registry with pre-registered core metrics.
    pub fn new() -> Self {
        let registry = Registry::new();

        let client_request_latency = HistogramVec::new(
            HistogramOpts::new(
                "cassandra_client_request_latency_seconds",
                "Client request latency in seconds",
            )
            .buckets(vec![
                0.0001, 0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0,
                10.0,
            ]),
            &["operation"],
        )
        .expect("histogram creation");
        registry
            .register(Box::new(client_request_latency.clone()))
            .expect("register latency");

        let read_count = IntCounter::new(
            "cassandra_read_count_total",
            "Total number of read operations",
        )
        .expect("counter creation");
        registry
            .register(Box::new(read_count.clone()))
            .expect("register reads");

        let write_count = IntCounter::new(
            "cassandra_write_count_total",
            "Total number of write operations",
        )
        .expect("counter creation");
        registry
            .register(Box::new(write_count.clone()))
            .expect("register writes");

        let live_sstable_count = IntGauge::new(
            "cassandra_live_sstable_count",
            "Number of live SSTables across all tables",
        )
        .expect("gauge creation");
        registry
            .register(Box::new(live_sstable_count.clone()))
            .expect("register sstable count");

        let pending_compactions = IntGauge::new(
            "cassandra_pending_compactions",
            "Number of pending compaction tasks",
        )
        .expect("gauge creation");
        registry
            .register(Box::new(pending_compactions.clone()))
            .expect("register pending compactions");

        let connected_native_clients = IntGauge::new(
            "cassandra_connected_native_clients",
            "Number of connected native protocol clients",
        )
        .expect("gauge creation");
        registry
            .register(Box::new(connected_native_clients.clone()))
            .expect("register connected clients");

        let tombstone_scanned = IntCounter::new(
            "cassandra_tombstone_scanned_total",
            "Total tombstones scanned during reads",
        )
        .expect("counter creation");
        registry
            .register(Box::new(tombstone_scanned.clone()))
            .expect("register tombstones");

        let key_cache_hit_rate = Gauge::new(
            "cassandra_key_cache_hit_rate",
            "Key cache hit rate (0.0 - 1.0)",
        )
        .expect("gauge creation");
        registry
            .register(Box::new(key_cache_hit_rate.clone()))
            .expect("register cache hit rate");

        let storage_load_bytes = IntGauge::new(
            "cassandra_storage_load_bytes",
            "Total data stored on this node in bytes",
        )
        .expect("gauge creation");
        registry
            .register(Box::new(storage_load_bytes.clone()))
            .expect("register storage load");

        let exceptions_count = IntCounterVec::new(
            Opts::new("cassandra_exceptions_total", "Total exceptions by type"),
            &["type"],
        )
        .expect("counter vec creation");
        registry
            .register(Box::new(exceptions_count.clone()))
            .expect("register exceptions");

        let repair_trees_built =
            IntGauge::new("cassandra_repair_trees_built", "Total repair trees built")
                .expect("gauge");
        registry
            .register(Box::new(repair_trees_built.clone()))
            .unwrap();

        let repair_trees_exchanged = IntGauge::new(
            "cassandra_repair_trees_exchanged",
            "Total repair trees exchanged",
        )
        .expect("gauge");
        registry
            .register(Box::new(repair_trees_exchanged.clone()))
            .unwrap();

        let repair_ranges_repaired =
            IntGauge::new("cassandra_repair_ranges_repaired", "Total ranges repaired")
                .expect("gauge");
        registry
            .register(Box::new(repair_ranges_repaired.clone()))
            .unwrap();

        let repair_bytes_streamed = IntGauge::new(
            "cassandra_repair_bytes_streamed",
            "Total repair bytes streamed",
        )
        .expect("gauge");
        registry
            .register(Box::new(repair_bytes_streamed.clone()))
            .unwrap();

        let repair_sessions_active =
            IntGauge::new("cassandra_repair_sessions_active", "Active repair sessions")
                .expect("gauge");
        registry
            .register(Box::new(repair_sessions_active.clone()))
            .unwrap();

        let repair_sessions_completed = IntGauge::new(
            "cassandra_repair_sessions_completed",
            "Completed repair sessions",
        )
        .expect("gauge");
        registry
            .register(Box::new(repair_sessions_completed.clone()))
            .unwrap();

        let repair_sessions_failed =
            IntGauge::new("cassandra_repair_sessions_failed", "Failed repair sessions")
                .expect("gauge");
        registry
            .register(Box::new(repair_sessions_failed.clone()))
            .unwrap();

        Self {
            registry,
            client_request_latency,
            read_count,
            write_count,
            live_sstable_count,
            pending_compactions,
            connected_native_clients,
            tombstone_scanned,
            key_cache_hit_rate,
            storage_load_bytes,
            exceptions_count,
            repair_trees_built,
            repair_trees_exchanged,
            repair_ranges_repaired,
            repair_bytes_streamed,
            repair_sessions_active,
            repair_sessions_completed,
            repair_sessions_failed,
        }
    }

    /// Gather all metrics and encode as Prometheus text format.
    pub fn gather_text(&self) -> String {
        let encoder = TextEncoder::new();
        let metric_families = self.registry.gather();
        let mut buffer = Vec::new();
        encoder
            .encode(&metric_families, &mut buffer)
            .unwrap_or_else(|e| warn!("metrics encode error: {}", e));
        String::from_utf8(buffer).unwrap_or_default()
    }

    /// Record a client request latency observation.
    pub fn observe_request_latency(&self, operation: &str, duration_secs: f64) {
        self.client_request_latency
            .with_label_values(&[operation])
            .observe(duration_secs);
    }

    /// Increment read counter.
    pub fn inc_reads(&self) {
        self.read_count.inc();
    }

    /// Increment write counter.
    pub fn inc_writes(&self) {
        self.write_count.inc();
    }

    /// Record an exception.
    pub fn inc_exception(&self, exception_type: &str) {
        self.exceptions_count
            .with_label_values(&[exception_type])
            .inc();
    }

    /// Sync from repair metrics snapshot.
    pub fn sync_from_repair_metrics(
        &self,
        snapshot: &cassandra_repair::metrics::RepairMetricsSnapshot,
    ) {
        self.repair_trees_built.set(snapshot.trees_built as i64);
        self.repair_trees_exchanged
            .set(snapshot.trees_exchanged as i64);
        self.repair_ranges_repaired
            .set(snapshot.ranges_repaired as i64);
        self.repair_bytes_streamed
            .set(snapshot.bytes_streamed as i64);
        self.repair_sessions_active
            .set(snapshot.sessions_active as i64);
        self.repair_sessions_completed
            .set(snapshot.sessions_completed as i64);
        self.repair_sessions_failed
            .set(snapshot.sessions_failed as i64);
    }
}

impl Default for MetricsRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_creation() {
        let registry = MetricsRegistry::new();
        let text = registry.gather_text();
        // Should contain metric names even if values are zero
        assert!(text.contains("cassandra_read_count_total"));
        assert!(text.contains("cassandra_write_count_total"));
    }

    #[test]
    fn increment_counters() {
        let registry = MetricsRegistry::new();
        registry.inc_reads();
        registry.inc_reads();
        registry.inc_writes();

        let text = registry.gather_text();
        assert!(text.contains("cassandra_read_count_total 2"));
        assert!(text.contains("cassandra_write_count_total 1"));
    }

    #[test]
    fn observe_latency() {
        let registry = MetricsRegistry::new();
        registry.observe_request_latency("read", 0.005);
        registry.observe_request_latency("write", 0.010);

        let text = registry.gather_text();
        assert!(text.contains("cassandra_client_request_latency_seconds"));
        assert!(text.contains("operation=\"read\""));
    }

    #[test]
    fn gauge_operations() {
        let registry = MetricsRegistry::new();
        registry.live_sstable_count.set(42);
        registry.pending_compactions.set(3);
        registry.connected_native_clients.set(10);
        registry.key_cache_hit_rate.set(0.95);

        let text = registry.gather_text();
        assert!(text.contains("cassandra_live_sstable_count 42"));
        assert!(text.contains("cassandra_pending_compactions 3"));
    }

    #[test]
    fn exception_counter() {
        let registry = MetricsRegistry::new();
        registry.inc_exception("ReadTimeout");
        registry.inc_exception("ReadTimeout");
        registry.inc_exception("WriteTimeout");

        let text = registry.gather_text();
        assert!(text.contains("ReadTimeout"));
        assert!(text.contains("WriteTimeout"));
    }
}
