// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.
// SPDX-License-Identifier: Apache-2.0

//! Hinted handoff store: persists mutations for temporarily down replicas.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.hints.HintsStore`
//! - `org.apache.cassandra.hints.HintsService`

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use parking_lot::RwLock;
use tracing::{debug, info};

use cassandra_cluster_metadata::Endpoint;
use crate::write::CoordinatedMutation;

/// A stored hint: a mutation to be replayed when the target node recovers.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Hint {
    /// Target endpoint that was down.
    pub target: Endpoint,
    /// The original mutation.
    pub mutation: CoordinatedMutation,
    /// When the hint was created (epoch millis).
    pub created_at: i64,
}

/// In-memory hint store.
///
/// In production, hints would be persisted to disk. This implementation
/// stores them in-memory behind a feature flag for testing.
pub struct HintStore {
    /// Hints indexed by target endpoint.
    hints: Arc<RwLock<HashMap<Endpoint, VecDeque<Hint>>>>,
    /// Maximum hints per endpoint (prevents OOM on prolonged outage).
    max_hints_per_endpoint: usize,
    /// Total hints stored.
    total_hints: Arc<std::sync::atomic::AtomicU64>,
    /// When true, hint delivery (drain) is paused during topology changes.
    /// Hints are still accepted and stored; only replay is blocked.
    paused: Arc<std::sync::atomic::AtomicBool>,
}

impl HintStore {
    pub fn new(max_hints_per_endpoint: usize) -> Self {
        Self {
            hints: Arc::new(RwLock::new(HashMap::new())),
            max_hints_per_endpoint,
            total_hints: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            paused: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    /// Store a hint for a down replica.
    pub fn store_hint(&self, target: Endpoint, mutation: CoordinatedMutation) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;

        let hint = Hint {
            target,
            mutation,
            created_at: now,
        };

        let mut hints = self.hints.write();
        let queue = hints.entry(target).or_default();

        if queue.len() >= self.max_hints_per_endpoint {
            queue.pop_front(); // Drop oldest hint
            debug!(target = %target, "Hint store full for endpoint, dropping oldest");
        } else {
            self.total_hints
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }

        queue.push_back(hint);
    }

    /// Get and drain all hints for a target endpoint (for replay).
    ///
    /// Returns empty if hint delivery is currently paused.
    pub fn drain_hints(&self, target: &Endpoint) -> Vec<Hint> {
        if self.is_paused() {
            debug!(target = %target, "Hint delivery is paused, skipping drain");
            return Vec::new();
        }

        let mut hints = self.hints.write();
        if let Some(queue) = hints.remove(target) {
            let count = queue.len() as u64;
            self.total_hints
                .fetch_sub(count, std::sync::atomic::Ordering::Relaxed);
            info!(target = %target, count = count, "Draining hints for replay");
            queue.into()
        } else {
            Vec::new()
        }
    }

    /// Number of pending hints for a target.
    pub fn hint_count(&self, target: &Endpoint) -> usize {
        self.hints
            .read()
            .get(target)
            .map(|q| q.len())
            .unwrap_or(0)
    }

    /// Total number of stored hints across all targets.
    pub fn total_hints(&self) -> u64 {
        self.total_hints
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Number of endpoints with pending hints.
    pub fn endpoints_with_hints(&self) -> usize {
        self.hints.read().len()
    }

    /// Pause hint delivery (during topology changes).
    ///
    /// Hints are still accepted and stored, but `drain_hints` will return
    /// empty until `resume()` is called.
    pub fn pause(&self) {
        self.paused.store(true, std::sync::atomic::Ordering::SeqCst);
        info!("Hint delivery paused");
    }

    /// Resume hint delivery after topology change completes.
    pub fn resume(&self) {
        self.paused.store(false, std::sync::atomic::Ordering::SeqCst);
        info!("Hint delivery resumed");
    }

    /// Whether hint delivery is currently paused.
    pub fn is_paused(&self) -> bool {
        self.paused.load(std::sync::atomic::Ordering::SeqCst)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::write::{CellMutation, MutationRow};
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    fn ep(port: u16) -> Endpoint {
        Endpoint::new(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            port,
        ))
    }

    fn test_mutation() -> CoordinatedMutation {
        CoordinatedMutation {
            keyspace: "ks".to_string(),
            table: "t".to_string(),
            partition_key: b"key".to_vec(),
            rows: vec![],
            timestamp: 1000,
        }
    }

    #[test]
    fn store_and_drain() {
        let store = HintStore::new(100);
        store.store_hint(ep(7002), test_mutation());
        store.store_hint(ep(7002), test_mutation());

        assert_eq!(store.hint_count(&ep(7002)), 2);
        assert_eq!(store.total_hints(), 2);

        let hints = store.drain_hints(&ep(7002));
        assert_eq!(hints.len(), 2);
        assert_eq!(store.hint_count(&ep(7002)), 0);
        assert_eq!(store.total_hints(), 0);
    }

    #[test]
    fn max_hints_drops_oldest() {
        let store = HintStore::new(2);
        store.store_hint(ep(7002), test_mutation());
        store.store_hint(ep(7002), test_mutation());
        store.store_hint(ep(7002), test_mutation()); // drops oldest

        assert_eq!(store.hint_count(&ep(7002)), 2);
    }

    #[test]
    fn multiple_endpoints() {
        let store = HintStore::new(100);
        store.store_hint(ep(7002), test_mutation());
        store.store_hint(ep(7003), test_mutation());

        assert_eq!(store.endpoints_with_hints(), 2);
        assert_eq!(store.hint_count(&ep(7002)), 1);
        assert_eq!(store.hint_count(&ep(7003)), 1);
    }
}
