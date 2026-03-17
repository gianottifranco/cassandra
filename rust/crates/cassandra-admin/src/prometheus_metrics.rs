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

use prometheus::{
    Encoder, Gauge, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGauge, Opts,
    Registry, TextEncoder,
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
    pub key_cache_size: IntGauge,
    pub key_cache_hits_total: IntCounter,
    pub key_cache_misses_total: IntCounter,
    pub row_cache_hit_rate: Gauge,
    pub row_cache_size: IntGauge,
    pub counter_cache_hit_rate: Gauge,
    pub counter_cache_size: IntGauge,
    pub chunk_cache_hit_rate: Gauge,
    pub chunk_cache_size: IntGauge,
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

    // Commitlog metrics
    pub commitlog_bytes_written: IntCounter,
    pub commitlog_segments_active: IntGauge,
    pub commitlog_pending_tasks: IntGauge,

    // Compaction metrics
    pub compaction_bytes_compacted_total: IntCounter,
    pub compaction_tasks_completed_total: IntCounter,
    pub compaction_pending_tasks: IntGauge,

    // Streaming metrics
    pub streaming_bytes_sent: IntGauge,
    pub streaming_bytes_received: IntGauge,
    pub streaming_sessions_active: IntGauge,
    pub streaming_sessions_completed: IntGauge,
    pub streaming_sessions_failed: IntGauge,
    pub streaming_retries: IntGauge,
    pub streaming_checksum_failures: IntGauge,

    // Messaging metrics
    pub messaging_sent_total: IntCounter,
    pub messaging_received_total: IntCounter,
    pub messaging_dropped_total: IntCounter,

    // Table-level labeled metrics
    pub table_read_latency: HistogramVec,
    pub table_write_latency: HistogramVec,
    pub table_tombstones_scanned_total: IntCounterVec,

    // Paxos metrics
    pub paxos_propose_total: IntCounter,
    pub paxos_commit_total: IntCounter,
    pub paxos_contention_total: IntCounter,

    // Tracing metrics
    pub tracing_sessions_total: IntCounter,
    pub tracing_active_sessions: IntGauge,

    // TCM metrics
    pub tcm_epoch: IntGauge,
    pub tcm_commits_total: IntCounter,
}

/// Helper: create and register an IntCounter.
fn reg_counter(r: &Registry, name: &str, help: &str) -> IntCounter {
    let m = IntCounter::new(name, help).expect(name);
    r.register(Box::new(m.clone())).unwrap();
    m
}

/// Helper: create and register an IntGauge.
fn reg_gauge(r: &Registry, name: &str, help: &str) -> IntGauge {
    let m = IntGauge::new(name, help).expect(name);
    r.register(Box::new(m.clone())).unwrap();
    m
}

/// Helper: create and register a Gauge (f64).
fn reg_gauge_f64(r: &Registry, name: &str, help: &str) -> Gauge {
    let m = Gauge::new(name, help).expect(name);
    r.register(Box::new(m.clone())).unwrap();
    m
}

impl MetricsRegistry {
    /// Create a new metrics registry with pre-registered core metrics.
    pub fn new() -> Self {
        let registry = Registry::new();
        let r = &registry;

        let latency_buckets = vec![
            0.0001, 0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
        ];

        let client_request_latency = HistogramVec::new(
            HistogramOpts::new("cassandra_client_request_latency_seconds", "Client request latency in seconds")
                .buckets(latency_buckets.clone()),
            &["operation"],
        ).expect("histogram");
        r.register(Box::new(client_request_latency.clone())).unwrap();

        let read_count = reg_counter(r, "cassandra_read_count_total", "Total read operations");
        let write_count = reg_counter(r, "cassandra_write_count_total", "Total write operations");
        let live_sstable_count = reg_gauge(r, "cassandra_live_sstable_count", "Live SSTables across all tables");
        let pending_compactions = reg_gauge(r, "cassandra_pending_compactions", "Pending compaction tasks");
        let connected_native_clients = reg_gauge(r, "cassandra_connected_native_clients", "Connected native clients");
        let tombstone_scanned = reg_counter(r, "cassandra_tombstone_scanned_total", "Tombstones scanned during reads");

        // Cache metrics
        let key_cache_hit_rate = reg_gauge_f64(r, "cassandra_key_cache_hit_rate", "Key cache hit rate");
        let key_cache_size = reg_gauge(r, "cassandra_key_cache_size", "Key cache entries");
        let key_cache_hits_total = reg_counter(r, "cassandra_key_cache_hits_total", "Key cache hits");
        let key_cache_misses_total = reg_counter(r, "cassandra_key_cache_misses_total", "Key cache misses");
        let row_cache_hit_rate = reg_gauge_f64(r, "cassandra_row_cache_hit_rate", "Row cache hit rate");
        let row_cache_size = reg_gauge(r, "cassandra_row_cache_size", "Row cache entries");
        let counter_cache_hit_rate = reg_gauge_f64(r, "cassandra_counter_cache_hit_rate", "Counter cache hit rate");
        let counter_cache_size = reg_gauge(r, "cassandra_counter_cache_size", "Counter cache entries");
        let chunk_cache_hit_rate = reg_gauge_f64(r, "cassandra_chunk_cache_hit_rate", "Chunk cache hit rate");
        let chunk_cache_size = reg_gauge(r, "cassandra_chunk_cache_size", "Chunk cache bytes");
        let storage_load_bytes = reg_gauge(r, "cassandra_storage_load_bytes", "Data stored on node in bytes");

        let exceptions_count = IntCounterVec::new(
            Opts::new("cassandra_exceptions_total", "Total exceptions by type"), &["type"],
        ).expect("counter vec");
        r.register(Box::new(exceptions_count.clone())).unwrap();

        // Repair metrics
        let repair_trees_built = reg_gauge(r, "cassandra_repair_trees_built", "Repair trees built");
        let repair_trees_exchanged = reg_gauge(r, "cassandra_repair_trees_exchanged", "Repair trees exchanged");
        let repair_ranges_repaired = reg_gauge(r, "cassandra_repair_ranges_repaired", "Ranges repaired");
        let repair_bytes_streamed = reg_gauge(r, "cassandra_repair_bytes_streamed", "Repair bytes streamed");
        let repair_sessions_active = reg_gauge(r, "cassandra_repair_sessions_active", "Active repair sessions");
        let repair_sessions_completed = reg_gauge(r, "cassandra_repair_sessions_completed", "Completed repair sessions");
        let repair_sessions_failed = reg_gauge(r, "cassandra_repair_sessions_failed", "Failed repair sessions");

        // Commitlog metrics
        let commitlog_bytes_written = reg_counter(r, "cassandra_commitlog_bytes_written_total", "Commitlog bytes written");
        let commitlog_segments_active = reg_gauge(r, "cassandra_commitlog_segments_active", "Active commitlog segments");
        let commitlog_pending_tasks = reg_gauge(r, "cassandra_commitlog_pending_tasks", "Pending commitlog tasks");

        // Compaction metrics
        let compaction_bytes_compacted_total = reg_counter(r, "cassandra_compaction_bytes_compacted_total", "Bytes compacted");
        let compaction_tasks_completed_total = reg_counter(r, "cassandra_compaction_tasks_completed_total", "Compaction tasks completed");
        let compaction_pending_tasks = reg_gauge(r, "cassandra_compaction_pending_tasks", "Pending compaction tasks");

        // Streaming metrics
        let streaming_bytes_sent = reg_gauge(r, "cassandra_streaming_bytes_sent", "Streaming bytes sent");
        let streaming_bytes_received = reg_gauge(r, "cassandra_streaming_bytes_received", "Streaming bytes received");
        let streaming_sessions_active = reg_gauge(r, "cassandra_streaming_sessions_active", "Active streaming sessions");
        let streaming_sessions_completed = reg_gauge(r, "cassandra_streaming_sessions_completed", "Completed streaming sessions");
        let streaming_sessions_failed = reg_gauge(r, "cassandra_streaming_sessions_failed", "Failed streaming sessions");
        let streaming_retries = reg_gauge(r, "cassandra_streaming_retries", "Streaming retries");
        let streaming_checksum_failures = reg_gauge(r, "cassandra_streaming_checksum_failures", "Streaming checksum failures");

        // Messaging metrics
        let messaging_sent_total = reg_counter(r, "cassandra_messaging_sent_total", "Messages sent");
        let messaging_received_total = reg_counter(r, "cassandra_messaging_received_total", "Messages received");
        let messaging_dropped_total = reg_counter(r, "cassandra_messaging_dropped_total", "Messages dropped");

        // Table-level labeled metrics
        let table_read_latency = HistogramVec::new(
            HistogramOpts::new("cassandra_table_read_latency_seconds", "Per-table read latency").buckets(latency_buckets.clone()),
            &["keyspace", "table"],
        ).expect("histogram");
        r.register(Box::new(table_read_latency.clone())).unwrap();
        let table_write_latency = HistogramVec::new(
            HistogramOpts::new("cassandra_table_write_latency_seconds", "Per-table write latency").buckets(latency_buckets),
            &["keyspace", "table"],
        ).expect("histogram");
        r.register(Box::new(table_write_latency.clone())).unwrap();
        let table_tombstones_scanned_total = IntCounterVec::new(
            Opts::new("cassandra_table_tombstones_scanned_total", "Per-table tombstones scanned"),
            &["keyspace", "table"],
        ).expect("counter vec");
        r.register(Box::new(table_tombstones_scanned_total.clone())).unwrap();

        // Paxos metrics
        let paxos_propose_total = reg_counter(r, "cassandra_paxos_propose_total", "Paxos proposals");
        let paxos_commit_total = reg_counter(r, "cassandra_paxos_commit_total", "Paxos commits");
        let paxos_contention_total = reg_counter(r, "cassandra_paxos_contention_total", "Paxos contentions");

        // Tracing metrics
        let tracing_sessions_total = reg_counter(r, "cassandra_tracing_sessions_total", "Tracing sessions");
        let tracing_active_sessions = reg_gauge(r, "cassandra_tracing_active_sessions", "Active tracing sessions");

        // TCM metrics
        let tcm_epoch = reg_gauge(r, "cassandra_tcm_epoch", "Current TCM epoch");
        let tcm_commits_total = reg_counter(r, "cassandra_tcm_commits_total", "TCM commits");

        Self {
            registry, client_request_latency, read_count, write_count,
            live_sstable_count, pending_compactions, connected_native_clients, tombstone_scanned,
            key_cache_hit_rate, key_cache_size, key_cache_hits_total, key_cache_misses_total,
            row_cache_hit_rate, row_cache_size, counter_cache_hit_rate, counter_cache_size,
            chunk_cache_hit_rate, chunk_cache_size, storage_load_bytes, exceptions_count,
            repair_trees_built, repair_trees_exchanged, repair_ranges_repaired,
            repair_bytes_streamed, repair_sessions_active, repair_sessions_completed,
            repair_sessions_failed, commitlog_bytes_written, commitlog_segments_active,
            commitlog_pending_tasks, compaction_bytes_compacted_total,
            compaction_tasks_completed_total, compaction_pending_tasks,
            streaming_bytes_sent, streaming_bytes_received, streaming_sessions_active,
            streaming_sessions_completed, streaming_sessions_failed, streaming_retries,
            streaming_checksum_failures, messaging_sent_total, messaging_received_total,
            messaging_dropped_total, table_read_latency, table_write_latency,
            table_tombstones_scanned_total, paxos_propose_total, paxos_commit_total,
            paxos_contention_total, tracing_sessions_total, tracing_active_sessions,
            tcm_epoch, tcm_commits_total,
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
        self.client_request_latency.with_label_values(&[operation]).observe(duration_secs);
    }

    /// Increment read counter.
    pub fn inc_reads(&self) { self.read_count.inc(); }

    /// Increment write counter.
    pub fn inc_writes(&self) { self.write_count.inc(); }

    /// Record an exception.
    pub fn inc_exception(&self, exception_type: &str) {
        self.exceptions_count.with_label_values(&[exception_type]).inc();
    }

    /// Sync cache metrics from cache statistics snapshots.
    pub fn sync_cache_metrics(&self,
        key_hit_rate: f64, key_size: usize, row_hit_rate: f64, row_size: usize,
        counter_hit_rate: f64, counter_size: usize, chunk_hit_rate: f64, chunk_size: usize,
    ) {
        self.key_cache_hit_rate.set(key_hit_rate);
        self.key_cache_size.set(key_size as i64);
        self.row_cache_hit_rate.set(row_hit_rate);
        self.row_cache_size.set(row_size as i64);
        self.counter_cache_hit_rate.set(counter_hit_rate);
        self.counter_cache_size.set(counter_size as i64);
        self.chunk_cache_hit_rate.set(chunk_hit_rate);
        self.chunk_cache_size.set(chunk_size as i64);
    }

    /// Sync from repair metrics snapshot.
    pub fn sync_from_repair_metrics(&self, s: &cassandra_repair::metrics::RepairMetricsSnapshot) {
        self.repair_trees_built.set(s.trees_built as i64);
        self.repair_trees_exchanged.set(s.trees_exchanged as i64);
        self.repair_ranges_repaired.set(s.ranges_repaired as i64);
        self.repair_bytes_streamed.set(s.bytes_streamed as i64);
        self.repair_sessions_active.set(s.sessions_active as i64);
        self.repair_sessions_completed.set(s.sessions_completed as i64);
        self.repair_sessions_failed.set(s.sessions_failed as i64);
    }

    /// Sync from commitlog stats.
    pub fn sync_from_commitlog_stats(&self, bytes_written: u64, segments_active: i64, pending: i64) {
        self.commitlog_bytes_written.inc_by(bytes_written);
        self.commitlog_segments_active.set(segments_active);
        self.commitlog_pending_tasks.set(pending);
    }

    /// Sync from compaction stats.
    pub fn sync_from_compaction_stats(&self, bytes_compacted: u64, tasks_completed: u64, pending: i64) {
        self.compaction_bytes_compacted_total.inc_by(bytes_compacted);
        self.compaction_tasks_completed_total.inc_by(tasks_completed);
        self.compaction_pending_tasks.set(pending);
    }

    /// Sync from streaming stats.
    pub fn sync_from_streaming_stats(
        &self,
        bytes_sent: i64, bytes_received: i64,
        active: i64, completed: i64, failed: i64,
        retries: i64, checksum_failures: i64,
    ) {
        self.streaming_bytes_sent.set(bytes_sent);
        self.streaming_bytes_received.set(bytes_received);
        self.streaming_sessions_active.set(active);
        self.streaming_sessions_completed.set(completed);
        self.streaming_sessions_failed.set(failed);
        self.streaming_retries.set(retries);
        self.streaming_checksum_failures.set(checksum_failures);
    }

    /// Sync from messaging stats.
    pub fn sync_from_messaging_stats(&self, sent: u64, received: u64, dropped: u64) {
        self.messaging_sent_total.inc_by(sent);
        self.messaging_received_total.inc_by(received);
        self.messaging_dropped_total.inc_by(dropped);
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
    fn cache_metrics_sync() {
        let registry = MetricsRegistry::new();
        registry.sync_cache_metrics(0.85, 1000, 0.72, 500, 0.60, 200, 0.90, 4096);

        let text = registry.gather_text();
        assert!(text.contains("cassandra_key_cache_hit_rate 0.85"));
        assert!(text.contains("cassandra_key_cache_size 1000"));
        assert!(text.contains("cassandra_row_cache_hit_rate 0.72"));
        assert!(text.contains("cassandra_row_cache_size 500"));
        assert!(text.contains("cassandra_counter_cache_hit_rate 0.6"));
        assert!(text.contains("cassandra_counter_cache_size 200"));
        assert!(text.contains("cassandra_chunk_cache_hit_rate 0.9"));
        assert!(text.contains("cassandra_chunk_cache_size 4096"));
    }

    #[test]
    fn cache_metrics_in_text_output() {
        let registry = MetricsRegistry::new();
        let text = registry.gather_text();
        for name in &["key_cache_hit_rate", "key_cache_size", "key_cache_hits_total",
            "key_cache_misses_total", "row_cache_hit_rate", "row_cache_size",
            "counter_cache_hit_rate", "counter_cache_size", "chunk_cache_hit_rate",
            "chunk_cache_size"] {
            assert!(text.contains(&format!("cassandra_{name}")), "missing {name}");
        }
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

    #[test]
    fn commitlog_metrics_sync() {
        let registry = MetricsRegistry::new();
        registry.sync_from_commitlog_stats(1024, 3, 2);
        let text = registry.gather_text();
        assert!(text.contains("cassandra_commitlog_segments_active 3"));
        assert!(text.contains("cassandra_commitlog_pending_tasks 2"));
    }

    #[test]
    fn compaction_metrics_sync() {
        let registry = MetricsRegistry::new();
        registry.sync_from_compaction_stats(4096, 10, 5);
        let text = registry.gather_text();
        assert!(text.contains("cassandra_compaction_pending_tasks 5"));
    }

    #[test]
    fn streaming_metrics_sync() {
        let registry = MetricsRegistry::new();
        registry.sync_from_streaming_stats(100, 200, 1, 5, 0, 2, 0);
        let text = registry.gather_text();
        assert!(text.contains("cassandra_streaming_bytes_sent 100"));
        assert!(text.contains("cassandra_streaming_sessions_active 1"));
    }

    #[test]
    fn messaging_metrics_sync() {
        let registry = MetricsRegistry::new();
        registry.sync_from_messaging_stats(500, 400, 3);
        let text = registry.gather_text();
        assert!(text.contains("cassandra_messaging_sent_total 500"));
        assert!(text.contains("cassandra_messaging_dropped_total 3"));
    }

    #[test]
    fn paxos_metrics() {
        let registry = MetricsRegistry::new();
        registry.paxos_propose_total.inc();
        registry.paxos_commit_total.inc();
        registry.paxos_contention_total.inc();
        let text = registry.gather_text();
        assert!(text.contains("cassandra_paxos_propose_total 1"));
        assert!(text.contains("cassandra_paxos_commit_total 1"));
    }

    #[test]
    fn tracing_and_tcm_metrics() {
        let registry = MetricsRegistry::new();
        registry.tracing_sessions_total.inc();
        registry.tracing_active_sessions.set(2);
        registry.tcm_epoch.set(42);
        registry.tcm_commits_total.inc();
        let text = registry.gather_text();
        assert!(text.contains("cassandra_tracing_active_sessions 2"));
        assert!(text.contains("cassandra_tcm_epoch 42"));
    }

    #[test]
    fn table_level_metrics() {
        let registry = MetricsRegistry::new();
        registry.table_read_latency
            .with_label_values(&["ks1", "tbl1"])
            .observe(0.005);
        registry.table_tombstones_scanned_total
            .with_label_values(&["ks1", "tbl1"])
            .inc();
        let text = registry.gather_text();
        assert!(text.contains("cassandra_table_read_latency_seconds"));
        assert!(text.contains("cassandra_table_tombstones_scanned_total"));
    }
}
