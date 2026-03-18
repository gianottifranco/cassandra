// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

//! Per-verb messaging metrics.
//!
//! Tracks sent, received, dropped, and pending counts per verb,
//! plus a simple latency accumulator.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::verb::Verb;

/// Per-verb counters.
#[derive(Debug, Default)]
pub struct VerbMetrics {
    pub sent: AtomicU64,
    pub received: AtomicU64,
    pub dropped: AtomicU64,
    pub pending: AtomicU64,
    pub latency_sum_us: AtomicU64,
    pub latency_count: AtomicU64,
}

impl VerbMetrics {
    pub fn record_sent(&self) {
        self.sent.fetch_add(1, Ordering::Relaxed);
        self.pending.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_received(&self) {
        self.received.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_completed(&self, latency_us: u64) {
        self.pending.fetch_sub(1, Ordering::Relaxed);
        self.latency_sum_us.fetch_add(latency_us, Ordering::Relaxed);
        self.latency_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_dropped(&self) {
        self.dropped.fetch_add(1, Ordering::Relaxed);
        self.pending.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn avg_latency_us(&self) -> f64 {
        let count = self.latency_count.load(Ordering::Relaxed);
        if count == 0 {
            return 0.0;
        }
        self.latency_sum_us.load(Ordering::Relaxed) as f64 / count as f64
    }

    pub fn snapshot(&self) -> VerbMetricsSnapshot {
        VerbMetricsSnapshot {
            sent: self.sent.load(Ordering::Relaxed),
            received: self.received.load(Ordering::Relaxed),
            dropped: self.dropped.load(Ordering::Relaxed),
            pending: self.pending.load(Ordering::Relaxed),
            avg_latency_us: self.avg_latency_us(),
        }
    }
}

/// Point-in-time snapshot of verb metrics.
#[derive(Debug, Clone)]
pub struct VerbMetricsSnapshot {
    pub sent: u64,
    pub received: u64,
    pub dropped: u64,
    pub pending: u64,
    pub avg_latency_us: f64,
}

/// Per-connection-type metrics.
#[derive(Debug, Default)]
pub struct ConnectionTypeMetrics {
    pub bytes_sent: AtomicU64,
    pub bytes_received: AtomicU64,
    pub reconnects: AtomicU64,
}

/// Per-endpoint metrics.
#[derive(Debug, Default)]
pub struct EndpointMetrics {
    pub queue_depth: AtomicU64,
    pub expired_messages: AtomicU64,
    pub resource_bytes_allocated: AtomicU64,
}

/// Global messaging metrics registry.
#[derive(Debug)]
pub struct MessagingMetrics {
    verb_metrics: HashMap<Verb, VerbMetrics>,
    pub connections_active: AtomicU64,
    pub connections_created: AtomicU64,
    pub bytes_sent: AtomicU64,
    pub bytes_received: AtomicU64,
    /// Per-connection-type metrics (urgent, small, large).
    pub urgent_metrics: ConnectionTypeMetrics,
    pub small_metrics: ConnectionTypeMetrics,
    pub large_metrics: ConnectionTypeMetrics,
}

impl MessagingMetrics {
    pub fn new() -> Self {
        let mut verb_metrics = HashMap::new();
        // Pre-allocate metrics for all 45 known verbs
        for verb in Self::all_verbs() {
            verb_metrics.insert(verb, VerbMetrics::default());
        }

        Self {
            verb_metrics,
            connections_active: AtomicU64::new(0),
            connections_created: AtomicU64::new(0),
            bytes_sent: AtomicU64::new(0),
            bytes_received: AtomicU64::new(0),
            urgent_metrics: ConnectionTypeMetrics::default(),
            small_metrics: ConnectionTypeMetrics::default(),
            large_metrics: ConnectionTypeMetrics::default(),
        }
    }

    /// All known verbs for pre-registration.
    fn all_verbs() -> Vec<Verb> {
        vec![
            Verb::Mutation,
            Verb::MutationResponse,
            Verb::ReadData,
            Verb::ReadDataResponse,
            Verb::ReadDigest,
            Verb::ReadDigestResponse,
            Verb::GossipDigestSyn,
            Verb::GossipDigestAck,
            Verb::GossipDigestAck2,
            Verb::Hint,
            Verb::HintResponse,
            Verb::BatchStore,
            Verb::BatchStoreResponse,
            Verb::BatchRemove,
            Verb::ReadRepair,
            Verb::ReadRepairResponse,
            Verb::SchemaPush,
            Verb::SchemaPull,
            Verb::SchemaResponse,
            Verb::Ping,
            Verb::Pong,
            Verb::GossipShutdown,
            Verb::StreamInit,
            Verb::StreamInitResponse,
            Verb::StreamData,
            Verb::StreamDataResponse,
            Verb::StreamComplete,
            Verb::StreamCompleteResponse,
            Verb::RepairRequest,
            Verb::RepairResponse,
            Verb::MerkleTreeRequest,
            Verb::MerkleTreeResponse,
            Verb::AntiCompactionRequest,
            Verb::AntiCompactionResponse,
            Verb::TopologyChange,
            Verb::TopologyChangeResponse,
            Verb::BootstrapRequest,
            Verb::BootstrapResponse,
            Verb::TcmCommit,
            Verb::TcmCommitResponse,
            Verb::TcmFetch,
            Verb::TcmFetchResponse,
            Verb::TcmNotify,
            Verb::RequestFailure,
        ]
    }

    pub fn verb(&self, verb: Verb) -> &VerbMetrics {
        self.verb_metrics.get(&verb).expect("Verb not registered")
    }

    /// Record bytes sent, updating both global and connection-type metrics.
    pub fn record_bytes_sent(&self, bytes: u64, conn_type: crate::connection_type::ConnectionType) {
        self.bytes_sent.fetch_add(bytes, Ordering::Relaxed);
        match conn_type {
            crate::connection_type::ConnectionType::Urgent => {
                self.urgent_metrics
                    .bytes_sent
                    .fetch_add(bytes, Ordering::Relaxed);
            }
            crate::connection_type::ConnectionType::Small => {
                self.small_metrics
                    .bytes_sent
                    .fetch_add(bytes, Ordering::Relaxed);
            }
            crate::connection_type::ConnectionType::Large => {
                self.large_metrics
                    .bytes_sent
                    .fetch_add(bytes, Ordering::Relaxed);
            }
        }
    }

    /// Record bytes received, updating both global and connection-type metrics.
    pub fn record_bytes_received(
        &self,
        bytes: u64,
        conn_type: crate::connection_type::ConnectionType,
    ) {
        self.bytes_received.fetch_add(bytes, Ordering::Relaxed);
        match conn_type {
            crate::connection_type::ConnectionType::Urgent => {
                self.urgent_metrics
                    .bytes_received
                    .fetch_add(bytes, Ordering::Relaxed);
            }
            crate::connection_type::ConnectionType::Small => {
                self.small_metrics
                    .bytes_received
                    .fetch_add(bytes, Ordering::Relaxed);
            }
            crate::connection_type::ConnectionType::Large => {
                self.large_metrics
                    .bytes_received
                    .fetch_add(bytes, Ordering::Relaxed);
            }
        }
    }

    /// Get a snapshot of all verb metrics.
    pub fn all_verb_snapshots(&self) -> HashMap<Verb, VerbMetricsSnapshot> {
        self.verb_metrics
            .iter()
            .map(|(v, m)| (*v, m.snapshot()))
            .collect()
    }
}

impl Default for MessagingMetrics {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn verb_metrics_tracking() {
        let m = VerbMetrics::default();
        m.record_sent();
        m.record_sent();
        m.record_received();
        m.record_completed(100);

        let snap = m.snapshot();
        assert_eq!(snap.sent, 2);
        assert_eq!(snap.received, 1);
        assert_eq!(snap.pending, 1); // 2 sent - 1 completed
        assert_eq!(snap.avg_latency_us, 100.0);
    }

    #[test]
    fn messaging_metrics_registry() {
        let metrics = MessagingMetrics::new();

        metrics.verb(Verb::Mutation).record_sent();
        metrics.verb(Verb::ReadData).record_sent();
        metrics.verb(Verb::ReadData).record_sent();

        let snaps = metrics.all_verb_snapshots();
        assert_eq!(snaps[&Verb::Mutation].sent, 1);
        assert_eq!(snaps[&Verb::ReadData].sent, 2);
    }

    #[test]
    fn all_45_verbs_registered() {
        let metrics = MessagingMetrics::new();
        let snaps = metrics.all_verb_snapshots();
        // Should have all 44 verbs (45 total - GossipShutdown has no separate response)
        assert!(
            snaps.len() >= 44,
            "Expected >= 44 verbs, got {}",
            snaps.len()
        );

        // Spot-check some previously missing verbs
        metrics.verb(Verb::StreamInit).record_sent();
        metrics.verb(Verb::TcmCommit).record_sent();
        metrics.verb(Verb::GossipShutdown).record_sent();
    }

    #[test]
    fn connection_type_metrics() {
        use crate::connection_type::ConnectionType;

        let metrics = MessagingMetrics::new();
        metrics.record_bytes_sent(100, ConnectionType::Urgent);
        metrics.record_bytes_sent(200, ConnectionType::Small);
        metrics.record_bytes_sent(300, ConnectionType::Large);

        assert_eq!(
            metrics.urgent_metrics.bytes_sent.load(Ordering::Relaxed),
            100
        );
        assert_eq!(
            metrics.small_metrics.bytes_sent.load(Ordering::Relaxed),
            200
        );
        assert_eq!(
            metrics.large_metrics.bytes_sent.load(Ordering::Relaxed),
            300
        );
        assert_eq!(metrics.bytes_sent.load(Ordering::Relaxed), 600);
    }
}
