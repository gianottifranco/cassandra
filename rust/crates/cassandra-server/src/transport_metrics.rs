// Licensed under Apache License, Version 2.0.

//! Transport-level metrics for the native protocol server.
//!
//! Tracks connection counts, request throughput, bytes transferred,
//! and authentication outcomes.

use std::net::IpAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use dashmap::DashMap;
use serde::Serialize;

/// Global transport metrics using atomic counters.
pub struct TransportMetrics {
    /// Currently active connections.
    pub active_connections: AtomicU64,
    /// Total connections accepted since start.
    pub connections_accepted: AtomicU64,
    /// Connections rejected due to resource limits.
    pub connections_rejected: AtomicU64,
    /// Total requests processed.
    pub requests_total: AtomicU64,
    /// Total bytes received from clients.
    pub bytes_in: AtomicU64,
    /// Total bytes sent to clients.
    pub bytes_out: AtomicU64,
    /// Successful authentication count.
    pub auth_success: AtomicU64,
    /// Failed authentication count.
    pub auth_failure: AtomicU64,
    /// Per-endpoint (IP) metrics.
    pub per_endpoint: Arc<DashMap<IpAddr, EndpointMetrics>>,
}

/// Per-endpoint metrics.
#[derive(Debug, Default)]
pub struct EndpointMetrics {
    pub active_connections: AtomicU64,
    pub requests_total: AtomicU64,
}

/// Serializable snapshot of transport metrics.
#[derive(Debug, Clone, Serialize)]
pub struct TransportMetricsSnapshot {
    pub active_connections: u64,
    pub connections_accepted: u64,
    pub connections_rejected: u64,
    pub requests_total: u64,
    pub bytes_in: u64,
    pub bytes_out: u64,
    pub auth_success: u64,
    pub auth_failure: u64,
}

impl TransportMetrics {
    pub fn new() -> Self {
        Self {
            active_connections: AtomicU64::new(0),
            connections_accepted: AtomicU64::new(0),
            connections_rejected: AtomicU64::new(0),
            requests_total: AtomicU64::new(0),
            bytes_in: AtomicU64::new(0),
            bytes_out: AtomicU64::new(0),
            auth_success: AtomicU64::new(0),
            auth_failure: AtomicU64::new(0),
            per_endpoint: Arc::new(DashMap::new()),
        }
    }

    /// Record a new connection accepted.
    pub fn connection_accepted(&self, ip: IpAddr) {
        self.active_connections.fetch_add(1, Ordering::Relaxed);
        self.connections_accepted.fetch_add(1, Ordering::Relaxed);

        self.per_endpoint
            .entry(ip)
            .or_default()
            .active_connections
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Record a connection closed.
    pub fn connection_closed(&self, ip: IpAddr) {
        self.active_connections.fetch_sub(1, Ordering::Relaxed);

        if let Some(ep) = self.per_endpoint.get(&ip) {
            ep.active_connections.fetch_sub(1, Ordering::Relaxed);
        }
    }

    /// Record a connection rejected.
    pub fn connection_rejected(&self) {
        self.connections_rejected.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a request processed.
    pub fn request_processed(&self, ip: Option<&IpAddr>) {
        self.requests_total.fetch_add(1, Ordering::Relaxed);
        if let Some(ip) = ip {
            if let Some(ep) = self.per_endpoint.get(ip) {
                ep.requests_total.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Record bytes received.
    pub fn record_bytes_in(&self, n: u64) {
        self.bytes_in.fetch_add(n, Ordering::Relaxed);
    }

    /// Record bytes sent.
    pub fn record_bytes_out(&self, n: u64) {
        self.bytes_out.fetch_add(n, Ordering::Relaxed);
    }

    /// Record an authentication outcome.
    pub fn record_auth(&self, success: bool) {
        if success {
            self.auth_success.fetch_add(1, Ordering::Relaxed);
        } else {
            self.auth_failure.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Create a point-in-time snapshot of global metrics.
    pub fn snapshot(&self) -> TransportMetricsSnapshot {
        TransportMetricsSnapshot {
            active_connections: self.active_connections.load(Ordering::Relaxed),
            connections_accepted: self.connections_accepted.load(Ordering::Relaxed),
            connections_rejected: self.connections_rejected.load(Ordering::Relaxed),
            requests_total: self.requests_total.load(Ordering::Relaxed),
            bytes_in: self.bytes_in.load(Ordering::Relaxed),
            bytes_out: self.bytes_out.load(Ordering::Relaxed),
            auth_success: self.auth_success.load(Ordering::Relaxed),
            auth_failure: self.auth_failure.load(Ordering::Relaxed),
        }
    }
}

impl Default for TransportMetrics {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn increment_and_snapshot() {
        let m = TransportMetrics::new();
        let ip: IpAddr = "10.0.0.1".parse().unwrap();

        m.connection_accepted(ip);
        m.connection_accepted(ip);
        m.request_processed(Some(&ip));
        m.record_bytes_in(1024);
        m.record_bytes_out(2048);
        m.record_auth(true);
        m.record_auth(false);

        let s = m.snapshot();
        assert_eq!(s.active_connections, 2);
        assert_eq!(s.connections_accepted, 2);
        assert_eq!(s.requests_total, 1);
        assert_eq!(s.bytes_in, 1024);
        assert_eq!(s.bytes_out, 2048);
        assert_eq!(s.auth_success, 1);
        assert_eq!(s.auth_failure, 1);
    }

    #[test]
    fn connection_close() {
        let m = TransportMetrics::new();
        let ip: IpAddr = "10.0.0.1".parse().unwrap();

        m.connection_accepted(ip);
        assert_eq!(m.snapshot().active_connections, 1);
        m.connection_closed(ip);
        assert_eq!(m.snapshot().active_connections, 0);
    }

    #[test]
    fn per_endpoint_tracking() {
        let m = TransportMetrics::new();
        let ip1: IpAddr = "10.0.0.1".parse().unwrap();
        let ip2: IpAddr = "10.0.0.2".parse().unwrap();

        m.connection_accepted(ip1);
        m.connection_accepted(ip1);
        m.connection_accepted(ip2);

        let ep1 = m.per_endpoint.get(&ip1).unwrap();
        assert_eq!(ep1.active_connections.load(Ordering::Relaxed), 2);
        let ep2 = m.per_endpoint.get(&ip2).unwrap();
        assert_eq!(ep2.active_connections.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn rejection_counting() {
        let m = TransportMetrics::new();
        m.connection_rejected();
        m.connection_rejected();
        assert_eq!(m.snapshot().connections_rejected, 2);
    }

    #[test]
    fn snapshot_serialization() {
        let m = TransportMetrics::new();
        m.connection_accepted("10.0.0.1".parse().unwrap());
        let snap = m.snapshot();
        let json = serde_json::to_string(&snap).unwrap();
        assert!(json.contains("\"active_connections\":1"));
    }
}
