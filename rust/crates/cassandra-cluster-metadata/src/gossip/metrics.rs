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

//! Gossip-specific metrics: round counts, durations, cluster health.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.metrics.GossipMetrics`

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use crate::gossip::Gossiper;

/// Gossip metrics with atomic counters.
pub struct GossipMetrics {
    /// Number of gossip rounds completed successfully.
    pub rounds_completed: AtomicU64,
    /// Number of gossip rounds skipped (no target or error).
    pub rounds_skipped: AtomicU64,
    /// Last round duration in microseconds.
    pub round_duration_us: AtomicU64,
    /// Number of currently live endpoints.
    pub live_count: AtomicU64,
    /// Number of currently dead endpoints.
    pub dead_count: AtomicU64,
    /// Number of currently quarantined endpoints.
    pub quarantined_count: AtomicU64,
    /// Whether schema is in agreement (1 = yes, 0 = no).
    pub schema_agreement: AtomicU64,
}

/// Immutable snapshot of gossip metrics.
#[derive(Debug, Clone)]
pub struct GossipMetricsSnapshot {
    pub rounds_completed: u64,
    pub rounds_skipped: u64,
    pub round_duration_us: u64,
    pub live_count: u64,
    pub dead_count: u64,
    pub quarantined_count: u64,
    pub schema_agreement: bool,
}

impl GossipMetrics {
    /// Create new zeroed-out metrics.
    pub fn new() -> Self {
        Self {
            rounds_completed: AtomicU64::new(0),
            rounds_skipped: AtomicU64::new(0),
            round_duration_us: AtomicU64::new(0),
            live_count: AtomicU64::new(0),
            dead_count: AtomicU64::new(0),
            quarantined_count: AtomicU64::new(0),
            schema_agreement: AtomicU64::new(1),
        }
    }

    /// Record a completed gossip round.
    pub fn record_round(&self, duration: Duration) {
        self.rounds_completed.fetch_add(1, Ordering::Relaxed);
        self.round_duration_us
            .store(duration.as_micros() as u64, Ordering::Relaxed);
    }

    /// Record a skipped gossip round.
    pub fn record_skipped(&self) {
        self.rounds_skipped.fetch_add(1, Ordering::Relaxed);
    }

    /// Update cluster health metrics from the gossiper state.
    pub fn update_from_gossiper(&self, gossiper: &Gossiper) {
        self.live_count
            .store(gossiper.live_endpoints().len() as u64, Ordering::Relaxed);
        self.dead_count
            .store(gossiper.dead_endpoints().len() as u64, Ordering::Relaxed);
        self.schema_agreement.store(
            if gossiper.assess_schema_agreement() {
                1
            } else {
                0
            },
            Ordering::Relaxed,
        );
    }

    /// Take an immutable snapshot of the current metrics.
    pub fn snapshot(&self) -> GossipMetricsSnapshot {
        GossipMetricsSnapshot {
            rounds_completed: self.rounds_completed.load(Ordering::Relaxed),
            rounds_skipped: self.rounds_skipped.load(Ordering::Relaxed),
            round_duration_us: self.round_duration_us.load(Ordering::Relaxed),
            live_count: self.live_count.load(Ordering::Relaxed),
            dead_count: self.dead_count.load(Ordering::Relaxed),
            quarantined_count: self.quarantined_count.load(Ordering::Relaxed),
            schema_agreement: self.schema_agreement.load(Ordering::Relaxed) == 1,
        }
    }
}

impl Default for GossipMetrics {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gossip::SeedProvider;
    use crate::node::Endpoint;
    use std::net::SocketAddr;

    fn ep(port: u16) -> Endpoint {
        Endpoint::new(SocketAddr::new(
            std::net::IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1)),
            port,
        ))
    }

    #[test]
    fn counter_increments() {
        let metrics = GossipMetrics::new();

        metrics.record_round(Duration::from_micros(500));
        metrics.record_round(Duration::from_micros(700));
        metrics.record_skipped();

        let snap = metrics.snapshot();
        assert_eq!(snap.rounds_completed, 2);
        assert_eq!(snap.rounds_skipped, 1);
        assert_eq!(snap.round_duration_us, 700); // Last recorded
    }

    #[test]
    fn update_from_gossiper_reads_correct_counts() {
        let metrics = GossipMetrics::new();
        let gossiper = Gossiper::new(ep(7001), SeedProvider::new(vec![]), 1);

        // Add some live/dead endpoints
        gossiper.live_endpoints.write().push(ep(7002));
        gossiper.live_endpoints.write().push(ep(7003));
        gossiper.dead_endpoints.write().push(ep(7004));

        metrics.update_from_gossiper(&gossiper);

        let snap = metrics.snapshot();
        assert_eq!(snap.live_count, 2);
        assert_eq!(snap.dead_count, 1);
    }

    #[test]
    fn snapshot_consistency() {
        let metrics = GossipMetrics::new();

        metrics.record_round(Duration::from_micros(100));
        metrics.live_count.store(5, Ordering::Relaxed);
        metrics.dead_count.store(1, Ordering::Relaxed);
        metrics.schema_agreement.store(0, Ordering::Relaxed);

        let snap = metrics.snapshot();
        assert_eq!(snap.rounds_completed, 1);
        assert_eq!(snap.live_count, 5);
        assert_eq!(snap.dead_count, 1);
        assert!(!snap.schema_agreement);
    }

    #[test]
    fn initial_state() {
        let metrics = GossipMetrics::new();
        let snap = metrics.snapshot();
        assert_eq!(snap.rounds_completed, 0);
        assert_eq!(snap.rounds_skipped, 0);
        assert!(snap.schema_agreement); // Default is true (1)
    }
}
