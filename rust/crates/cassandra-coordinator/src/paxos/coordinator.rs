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

//! Paxos / LWT coordinator — orchestrates the full CAS round.
//!
//! ## Java Oracle
//!
//! `org.apache.cassandra.service.StorageProxy.cas()`
//! `org.apache.cassandra.service.paxos.PaxosState.propose()`
//!
//! ## Protocol
//!
//! 1. **Prepare** — Send `PaxosPrepare` to quorum of replicas.
//!    Collect `PaxosPromise` responses.
//!    - If any replica returns an in-progress proposal, the coordinator
//!      must adopt it (Paxos safety requirement).
//! 2. **Read** — Perform a quorum read at SERIAL consistency to get the
//!    current row value.
//! 3. **Evaluate** — Check IF conditions against current row.
//!    - If conditions fail, return the current row (CAS failure).
//! 4. **Propose** — Send `PaxosPropose` with the mutation to quorum.
//! 5. **Commit** — Send `PaxosCommit` to all replicas.
//!
//! ## Recovery
//!
//! If a prepare reveals an in-progress (accepted but not committed) proposal
//! from a previous round, the new proposer must finish that round first
//! before starting its own.  This is the Paxos liveness guarantee.
//!
//! ## Contention Handling
//!
//! Under contention, the coordinator uses exponential backoff with jitter
//! between retries.  The backoff prevents thundering-herd scenarios where
//! multiple proposers keep preempting each other.

use std::sync::Arc;

use dashmap::DashMap;
use tracing::{debug, info, warn};
use uuid::Uuid;

use super::ballot::Ballot;
use super::messages::*;
use super::state::{PaxosState, Proposal};
use super::storage::PaxosStorage;

/// Default maximum number of Paxos retries under contention.
const DEFAULT_MAX_CONTENTION_RETRIES: u32 = 4;

/// Default base backoff in microseconds for contention retry.
const DEFAULT_BASE_BACKOFF_MICROS: u64 = 100;

/// Paxos coordinator configuration.
///
/// Controls retry behavior, timeouts, and backoff parameters.
#[derive(Debug, Clone)]
pub struct PaxosConfig {
    /// Maximum number of retries under contention before aborting.
    pub max_contention_retries: u32,
    /// Base backoff in microseconds (exponentially increased per retry).
    pub base_backoff_micros: u64,
    /// Whether to use jitter in backoff delays (recommended for production).
    pub use_jitter: bool,
}

impl Default for PaxosConfig {
    fn default() -> Self {
        Self {
            max_contention_retries: DEFAULT_MAX_CONTENTION_RETRIES,
            base_backoff_micros: DEFAULT_BASE_BACKOFF_MICROS,
            use_jitter: true,
        }
    }
}

/// CAS operation result.
#[derive(Debug)]
pub enum CasResult {
    /// The CAS succeeded — the mutation was applied.
    Success,
    /// The CAS failed condition check. Contains the current row for
    /// the client to see why.
    ConditionNotMet {
        /// Serialized current row data.
        current_row: Vec<u8>,
    },
    /// Contention: too many retries, operation aborted.
    ContentionAborted {
        /// Number of attempts made.
        attempts: u32,
    },
    /// Unavailable: not enough replicas for SERIAL consistency.
    Unavailable { required: usize, alive: usize },
    /// Timeout during Paxos round.
    Timeout,
}

/// Errors from the Paxos coordinator.
#[derive(Debug, thiserror::Error)]
pub enum PaxosCoordinatorError {
    #[error("Not enough replicas for serial CL: required={required}, alive={alive}")]
    Unavailable { required: usize, alive: usize },

    #[error("Paxos round timed out after {attempts} attempts")]
    Timeout { attempts: u32 },

    #[error("Contention: exceeded max retries ({max})")]
    ContentionAborted { max: u32 },

    #[error("Internal error: {0}")]
    Internal(String),
}

/// Local replica — holds per-partition Paxos state.
///
/// In a real deployment, each replica is a remote node contacted via the
/// messaging service.  For unit testing and single-node validation, we
/// use this in-process implementation.
#[derive(Debug)]
pub struct PaxosReplica {
    /// Per-partition Paxos state.
    states: DashMap<Vec<u8>, PaxosState>,
    /// This replica's node id.
    pub node_id: Uuid,
    /// Optional persistent storage backend.
    storage: Option<Arc<PaxosStorage>>,
}

impl PaxosReplica {
    /// Create a new replica with the given node id (no persistence).
    pub fn new(node_id: Uuid) -> Self {
        Self {
            states: DashMap::new(),
            node_id,
            storage: None,
        }
    }

    /// Create a replica backed by persistent storage.
    pub fn with_storage(node_id: Uuid, storage: Arc<PaxosStorage>) -> Self {
        Self {
            states: DashMap::new(),
            node_id,
            storage: Some(storage),
        }
    }

    /// Recover uncommitted proposals from storage on startup.
    ///
    /// Returns the number of recovered entries.
    pub fn recover(&self, known_keys: &[Vec<u8>], cf_id: Uuid) -> usize {
        let storage = match &self.storage {
            Some(s) => s,
            None => return 0,
        };
        let uncommitted = storage.load_all_uncommitted(known_keys, cf_id);
        let count = uncommitted.len();
        for (key, state) in uncommitted {
            self.states.insert(key, state);
        }
        count
    }

    /// Handle a Prepare request.
    pub fn handle_prepare(&self, msg: &PaxosPrepare) -> PaxosPromise {
        let mut state = self.states.entry(msg.partition_key.clone()).or_default();

        let resp = state.prepare(msg.ballot);

        if resp.promised {
            if let Some(ref storage) = self.storage {
                if let Err(e) = storage.save_promise(&msg.partition_key, Uuid::nil(), msg.ballot) {
                    warn!(error = %e, "Failed to persist Paxos promise");
                }
            }
        }

        PaxosPromise {
            promised: resp.promised,
            ballot: resp.ballot,
            in_progress: resp.accepted,
            most_recent_commit: resp.committed,
        }
    }

    /// Handle a Propose request.
    pub fn handle_propose(&self, msg: &PaxosPropose) -> PaxosAccept {
        let mut state = self.states.entry(msg.partition_key.clone()).or_default();

        let resp = state.propose(msg.proposal.clone());

        if resp.accepted {
            if let Some(ref storage) = self.storage {
                if let Err(e) =
                    storage.save_proposal(&msg.partition_key, Uuid::nil(), &msg.proposal)
                {
                    warn!(error = %e, "Failed to persist Paxos proposal");
                }
            }
        }

        PaxosAccept {
            accepted: resp.accepted,
            ballot: resp.ballot,
        }
    }

    /// Handle a Commit message.
    pub fn handle_commit(&self, msg: &PaxosCommit) {
        let mut state = self.states.entry(msg.partition_key.clone()).or_default();

        state.commit(msg.proposal.clone());

        if let Some(ref storage) = self.storage {
            if let Err(e) = storage.save_commit(&msg.partition_key, Uuid::nil(), &msg.proposal) {
                warn!(error = %e, "Failed to persist Paxos commit");
            }
        }
    }

    /// Get the current state for a partition (for testing).
    pub fn get_state(&self, partition_key: &[u8]) -> Option<PaxosState> {
        self.states.get(partition_key).map(|s| s.clone())
    }
}

/// The Paxos coordinator — drives a CAS operation across a set of replicas.
///
/// In production, `replicas` would be contacted via the inter-node messaging
/// service.  Here we use `Arc<PaxosReplica>` for in-process coordination.
pub struct PaxosCoordinator {
    /// Our node's identity (used to generate ballots).
    node_id: Uuid,
    /// The replicas participating in this Paxos round.
    replicas: Vec<Arc<PaxosReplica>>,
    /// Quorum size (typically `rf / 2 + 1`).
    quorum_size: usize,
    /// Coordinator configuration.
    config: PaxosConfig,
}

impl PaxosCoordinator {
    pub fn new(node_id: Uuid, replicas: Vec<Arc<PaxosReplica>>, quorum_size: usize) -> Self {
        Self {
            node_id,
            replicas,
            quorum_size,
            config: PaxosConfig::default(),
        }
    }

    /// Create a coordinator with custom configuration.
    pub fn with_config(
        node_id: Uuid,
        replicas: Vec<Arc<PaxosReplica>>,
        quorum_size: usize,
        config: PaxosConfig,
    ) -> Self {
        Self {
            node_id,
            replicas,
            quorum_size,
            config,
        }
    }

    /// Compute contention backoff delay in microseconds for a given attempt.
    ///
    /// Uses exponential backoff: `base * 2^(attempt-1)`, optionally with
    /// random jitter.  In test mode (use_jitter=false), returns deterministic
    /// values for reproducibility.
    fn backoff_micros(&self, attempt: u32) -> u64 {
        let base = self.config.base_backoff_micros;
        let exp = base.saturating_mul(1u64 << (attempt.min(10) - 1));
        if self.config.use_jitter {
            // Simple jitter: backoff * [0.5, 1.5)
            // Using a low-quality random to avoid pulling in extra deps.
            let jitter_factor = 0.5 + (fastrand(exp) as f64 / u64::MAX as f64);
            (exp as f64 * jitter_factor) as u64
        } else {
            exp
        }
    }

    /// Execute a compare-and-set (CAS) operation.
    ///
    /// `condition_fn` evaluates the IF clause against the current row data.
    /// Returns `true` if the condition is met (proceed with mutation) or
    /// `false` (CAS fails, return current row to client).
    ///
    /// `mutation` is the serialized mutation to apply if conditions pass.
    pub async fn execute_cas<R, F1, F2>(
        &self,
        keyspace: &str,
        table: &str,
        partition_key: &[u8],
        mutation: Vec<u8>,
        read_current_fn: F1,
        condition_fn: F2,
    ) -> CasResult
    where
        R: std::future::Future<Output = Option<Vec<u8>>>,
        F1: Fn() -> R,
        F2: Fn(Option<&[u8]>) -> bool, // current_row -> condition_met
    {
        let mut attempts = 0u32;
        let max_retries = self.config.max_contention_retries;

        while attempts < max_retries {
            attempts += 1;

            // Generate a ballot
            let ballot = if attempts == 1 {
                Ballot::new(self.node_id)
            } else {
                // Contention backoff: async sleep
                let backoff_micros = self.backoff_micros(attempts);
                tokio::time::sleep(std::time::Duration::from_micros(backoff_micros)).await;
                // Ensure strictly newer ballot
                Ballot::with_timestamp(
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .expect("clock")
                        .as_micros() as i64
                        + attempts as i64,
                    self.node_id,
                )
            };

            debug!(
                attempt = attempts,
                ballot = %ballot,
                "Starting Paxos round"
            );

            // Phase 1: Prepare
            let prepare_msg = PaxosPrepare {
                partition_key: partition_key.to_vec(),
                keyspace: keyspace.to_string(),
                table: table.to_string(),
                ballot,
            };

            let promises: Vec<PaxosPromise> = self
                .replicas
                .iter()
                .map(|r| r.handle_prepare(&prepare_msg))
                .collect();

            let ack_count = promises.iter().filter(|p| p.promised).count();

            if ack_count < self.quorum_size {
                debug!(
                    acks = ack_count,
                    required = self.quorum_size,
                    "Prepare failed — retrying with backoff"
                );
                continue;
            }

            // Check for in-progress proposals (must adopt per Paxos)
            let in_progress = promises
                .iter()
                .filter_map(|p| p.in_progress.as_ref())
                .max_by_key(|p| p.ballot);

            let (proposal_mutation, adopted_in_progress) = if let Some(adopted) = in_progress {
                debug!(
                    adopted_ballot = %adopted.ballot,
                    "Adopting in-progress proposal — completing prior round (recovery)"
                );
                // Paxos safety: must finish the prior round with the adopted value.
                // We propose and commit the adopted mutation, then the caller's
                // CAS will need to retry with a fresh round.
                (adopted.mutation.clone(), true)
            } else {
                // No in-progress — we can propose our own mutation.
                // Phase 2: Read current row value at SERIAL consistency.
                // In Cassandra, Paxos prepare guarantees we have established a quorum
                // that won't accept older ballots, so a normal read here reflects the
                // latest Paxos string.
                let current_row_val = read_current_fn().await;

                if !condition_fn(current_row_val.as_deref()) {
                    return CasResult::ConditionNotMet {
                        current_row: current_row_val.unwrap_or_default(),
                    };
                }

                (mutation.clone(), false)
            };

            // Phase 2: Propose
            let propose_msg = PaxosPropose {
                partition_key: partition_key.to_vec(),
                keyspace: keyspace.to_string(),
                table: table.to_string(),
                proposal: Proposal {
                    ballot,
                    mutation: proposal_mutation.clone(),
                },
            };

            let accepts: Vec<PaxosAccept> = self
                .replicas
                .iter()
                .map(|r| r.handle_propose(&propose_msg))
                .collect();

            let accept_count = accepts.iter().filter(|a| a.accepted).count();

            if accept_count < self.quorum_size {
                debug!(
                    accepts = accept_count,
                    required = self.quorum_size,
                    "Propose failed — retrying with backoff"
                );
                continue;
            }

            // Phase 3: Commit
            let commit_msg = PaxosCommit {
                partition_key: partition_key.to_vec(),
                keyspace: keyspace.to_string(),
                table: table.to_string(),
                proposal: Proposal {
                    ballot,
                    mutation: proposal_mutation,
                },
            };

            for replica in &self.replicas {
                replica.handle_commit(&commit_msg);
            }

            info!(
                attempts,
                ballot = %ballot,
                "Paxos CAS committed successfully"
            );

            if adopted_in_progress {
                debug!("Completed adopted Paxos round; retrying caller CAS");
                continue;
            }

            return CasResult::Success;
        }

        warn!(max = max_retries, "CAS aborted due to contention");

        CasResult::ContentionAborted {
            attempts: max_retries,
        }
    }
}

/// Simple hash-based pseudo-random for jitter (avoids pulling `rand` dependency
/// into the hot path; production code should use proper PRNG).
fn fastrand(seed: u64) -> u64 {
    let mut x = seed;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    x
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node_a() -> Uuid {
        Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap()
    }

    fn node_b() -> Uuid {
        Uuid::parse_str("00000000-0000-0000-0000-000000000002").unwrap()
    }

    fn node_c() -> Uuid {
        Uuid::parse_str("00000000-0000-0000-0000-000000000003").unwrap()
    }

    fn setup_cluster() -> (Vec<Arc<PaxosReplica>>, PaxosCoordinator) {
        let r1 = Arc::new(PaxosReplica::new(node_a()));
        let r2 = Arc::new(PaxosReplica::new(node_b()));
        let r3 = Arc::new(PaxosReplica::new(node_c()));
        let replicas = vec![r1.clone(), r2.clone(), r3.clone()];

        let config = PaxosConfig {
            use_jitter: false, // deterministic for tests
            ..Default::default()
        };
        let coordinator = PaxosCoordinator::with_config(node_a(), replicas.clone(), 2, config);
        (replicas, coordinator)
    }

    #[tokio::test]
    async fn basic_cas_insert_if_not_exists() {
        let (_replicas, coordinator) = setup_cluster();

        let result = coordinator
            .execute_cas(
                "ks",
                "t1",
                b"user1",
                b"INSERT user1".to_vec(),
                || async { None },
                |current| current.is_none(), // IF NOT EXISTS
            )
            .await;

        assert!(matches!(result, CasResult::Success));
    }

    #[tokio::test]
    async fn cas_fails_when_row_exists() {
        let (_replicas, coordinator) = setup_cluster();

        // First insert succeeds
        let r1 = coordinator
            .execute_cas(
                "ks",
                "t1",
                b"user1",
                b"INSERT user1".to_vec(),
                || async { None },
                |_| true, // Always succeed first time
            )
            .await;
        assert!(matches!(r1, CasResult::Success));

        // Second insert with IF NOT EXISTS should fail
        let r2 = coordinator
            .execute_cas(
                "ks",
                "t1",
                b"user1",
                b"INSERT user1 again".to_vec(),
                || async { Some(b"INSERT user1".to_vec()) },
                |current| current.is_none(), // IF NOT EXISTS — row exists now
            )
            .await;

        assert!(matches!(r2, CasResult::ConditionNotMet { .. }));
    }

    #[tokio::test]
    async fn cas_condition_check() {
        let (_replicas, coordinator) = setup_cluster();

        // Insert initial value
        coordinator
            .execute_cas(
                "ks",
                "t1",
                b"pk",
                b"version=1".to_vec(),
                || async { None },
                |_| true,
            )
            .await;

        // Conditional update: only if current value is "version=1"
        let result = coordinator
            .execute_cas(
                "ks",
                "t1",
                b"pk",
                b"version=2".to_vec(),
                || async { Some(b"version=1".to_vec()) },
                |current| current.map_or(false, |v| v == b"version=1"),
            )
            .await;

        assert!(matches!(result, CasResult::Success));
    }

    #[tokio::test]
    async fn concurrent_proposers_one_wins() {
        // Simulate two coordinators trying to CAS the same key
        let r1 = Arc::new(PaxosReplica::new(node_a()));
        let r2 = Arc::new(PaxosReplica::new(node_b()));
        let r3 = Arc::new(PaxosReplica::new(node_c()));

        let replicas = vec![r1.clone(), r2.clone(), r3.clone()];
        let config = PaxosConfig {
            use_jitter: false,
            ..Default::default()
        };

        let coord_a = PaxosCoordinator::with_config(node_a(), replicas.clone(), 2, config.clone());
        let coord_b = PaxosCoordinator::with_config(node_b(), replicas.clone(), 2, config);

        let result_a = coord_a
            .execute_cas(
                "ks",
                "t",
                b"contested_key",
                b"A wins".to_vec(),
                || async { None },
                |current| current.is_none(),
            )
            .await;

        let result_b = coord_b
            .execute_cas(
                "ks",
                "t",
                b"contested_key",
                b"B wins".to_vec(),
                || async { Some(b"A wins".to_vec()) }, // B's read phase happens after A
                |current| current.is_none(),
            )
            .await;

        let a_ok = matches!(result_a, CasResult::Success);
        let b_ok = matches!(result_b, CasResult::Success);

        // At least one should succeed
        assert!(a_ok || b_ok, "At least one CAS must succeed");

        // If A succeeded, B should get ConditionNotMet
        if a_ok {
            assert!(
                matches!(result_b, CasResult::ConditionNotMet { .. }),
                "B should see condition-not-met after A succeeded"
            );
        }
    }

    #[tokio::test]
    async fn adopted_in_progress_proposal_is_completed_before_own_cas() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let r1 = Arc::new(PaxosReplica::new(node_a()));
        let r2 = Arc::new(PaxosReplica::new(node_b()));
        let r3 = Arc::new(PaxosReplica::new(node_c()));
        let replicas = vec![r1.clone(), r2.clone(), r3.clone()];

        let prior_ballot = Ballot::with_timestamp(1, node_a());
        let prepare = PaxosPrepare {
            partition_key: b"contested_key".to_vec(),
            keyspace: "ks".into(),
            table: "t".into(),
            ballot: prior_ballot,
        };
        for replica in &replicas {
            assert!(replica.handle_prepare(&prepare).promised);
        }

        let prior_proposal = Proposal {
            ballot: prior_ballot,
            mutation: b"A wins".to_vec(),
        };
        let propose = PaxosPropose {
            partition_key: b"contested_key".to_vec(),
            keyspace: "ks".into(),
            table: "t".into(),
            proposal: prior_proposal,
        };
        for replica in &replicas {
            assert!(replica.handle_propose(&propose).accepted);
        }

        let config = PaxosConfig {
            use_jitter: false,
            ..Default::default()
        };
        let coord_b = PaxosCoordinator::with_config(node_b(), replicas.clone(), 2, config);
        let reads = AtomicUsize::new(0);

        let result_b = coord_b
            .execute_cas(
                "ks",
                "t",
                b"contested_key",
                b"B wins".to_vec(),
                || {
                    reads.fetch_add(1, Ordering::SeqCst);
                    async { Some(b"A wins".to_vec()) }
                },
                |current| current.is_none(),
            )
            .await;

        assert!(matches!(result_b, CasResult::ConditionNotMet { .. }));
        assert_eq!(reads.load(Ordering::SeqCst), 1);
        for replica in &replicas {
            let state = replica.get_state(b"contested_key").unwrap();
            assert_eq!(state.committed.unwrap().mutation, b"A wins");
        }
    }

    #[tokio::test]
    async fn replica_state_persists_after_commit() {
        let (replicas, coordinator) = setup_cluster();

        coordinator
            .execute_cas(
                "ks",
                "t1",
                b"key1",
                b"committed_value".to_vec(),
                || async { None },
                |_| true,
            )
            .await;

        // All replicas should have the committed state
        for r in &replicas {
            let state = r.get_state(b"key1");
            assert!(state.is_some());
            let s = state.unwrap();
            assert!(s.committed.is_some());
            assert_eq!(s.committed.unwrap().mutation, b"committed_value");
        }
    }

    #[tokio::test]
    async fn linearizable_sequence() {
        let (_replicas, coordinator) = setup_cluster();

        // Series of CAS operations that must be linearizable
        // v0 → v1 → v2 → v3
        for i in 0..4 {
            let expected_current = if i == 0 {
                None
            } else {
                Some(format!("v{}", i - 1).into_bytes())
            };

            let new_value = format!("v{}", i).into_bytes();

            let expected_current_clone = expected_current.clone();
            let result = coordinator
                .execute_cas(
                    "ks",
                    "t1",
                    b"serial_key",
                    new_value,
                    move || {
                        let expected_current_clone = expected_current_clone.clone();
                        async move { expected_current_clone }
                    },
                    move |current| match (&expected_current, current) {
                        (None, None) => true,
                        (Some(exp), Some(cur)) => cur == exp.as_slice(),
                        _ => false,
                    },
                )
                .await;

            assert!(
                matches!(result, CasResult::Success),
                "Step {i} should succeed"
            );
        }
    }

    // ─── Additional contention and recovery tests ─────────────────────────

    #[tokio::test]
    async fn custom_config_max_retries() {
        let r1 = Arc::new(PaxosReplica::new(node_a()));
        let r2 = Arc::new(PaxosReplica::new(node_b()));
        let r3 = Arc::new(PaxosReplica::new(node_c()));
        let replicas = vec![r1, r2, r3];

        let config = PaxosConfig {
            max_contention_retries: 1,
            base_backoff_micros: 50,
            use_jitter: false,
        };

        let coordinator = PaxosCoordinator::with_config(node_a(), replicas, 2, config);

        // First CAS should still work with 1 retry
        let result = coordinator
            .execute_cas(
                "ks",
                "t",
                b"k",
                b"val".to_vec(),
                || async { None },
                |_| true,
            )
            .await;
        assert!(matches!(result, CasResult::Success));
    }

    #[test]
    fn backoff_increases_exponentially() {
        let config = PaxosConfig {
            base_backoff_micros: 100,
            use_jitter: false,
            ..Default::default()
        };
        let r1 = Arc::new(PaxosReplica::new(node_a()));
        let coord = PaxosCoordinator::with_config(node_a(), vec![r1], 1, config);

        let b1 = coord.backoff_micros(1); // 100 * 2^0 = 100
        let b2 = coord.backoff_micros(2); // 100 * 2^1 = 200
        let b3 = coord.backoff_micros(3); // 100 * 2^2 = 400
        assert_eq!(b1, 100);
        assert_eq!(b2, 200);
        assert_eq!(b3, 400);
    }

    #[tokio::test]
    async fn multiple_sequential_cas_on_same_key() {
        let (_replicas, coordinator) = setup_cluster();

        // Perform 10 sequential CAS updates
        for i in 0..10 {
            let prev = if i == 0 {
                None
            } else {
                Some(format!("val_{}", i - 1).into_bytes())
            };
            let new_val = format!("val_{}", i).into_bytes();

            let prev_clone = prev.clone();
            let result = coordinator
                .execute_cas(
                    "ks",
                    "t1",
                    b"counter_key",
                    new_val,
                    move || {
                        let prev_clone = prev_clone.clone();
                        async move { prev_clone }
                    },
                    move |current| match (&prev, current) {
                        (None, None) => true,
                        (Some(exp), Some(cur)) => cur == exp.as_slice(),
                        _ => false,
                    },
                )
                .await;
            assert!(matches!(result, CasResult::Success), "CAS {i} must succeed");
        }
    }

    #[tokio::test]
    async fn cas_on_different_partitions_are_independent() {
        let (_replicas, coordinator) = setup_cluster();

        // CAS on partition "a"
        let r1 = coordinator
            .execute_cas(
                "ks",
                "t",
                b"partition_a",
                b"value_a".to_vec(),
                || async { None },
                |current| current.is_none(),
            )
            .await;
        assert!(matches!(r1, CasResult::Success));

        // CAS on partition "b" is independent
        let r2 = coordinator
            .execute_cas(
                "ks",
                "t",
                b"partition_b",
                b"value_b".to_vec(),
                || async { None },
                |current| current.is_none(),
            )
            .await;
        assert!(matches!(r2, CasResult::Success));

        // Second CAS on "a" sees its own state
        let r3 = coordinator
            .execute_cas(
                "ks",
                "t",
                b"partition_a",
                b"value_a2".to_vec(),
                || async { Some(b"value_a".to_vec()) },
                |current| current.is_none(), // should fail — row exists
            )
            .await;
        assert!(matches!(r3, CasResult::ConditionNotMet { .. }));
    }

    #[test]
    fn paxos_config_defaults() {
        let config = PaxosConfig::default();
        assert_eq!(
            config.max_contention_retries,
            DEFAULT_MAX_CONTENTION_RETRIES
        );
        assert_eq!(config.base_backoff_micros, DEFAULT_BASE_BACKOFF_MICROS);
        assert!(config.use_jitter);
    }
}
