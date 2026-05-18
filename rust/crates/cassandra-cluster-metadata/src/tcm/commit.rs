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

//! Commit protocol for Transactional Cluster Metadata (TCM).
//!
//! Provides the abstraction for committing metadata transformations to the
//! cluster metadata log. A [`Processor`] validates epoch expectations and
//! applies transformations atomically, returning a [`CommitResult`] that
//! indicates success, rejection, or stale-epoch conditions.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.tcm.Processor`
//! - `org.apache.cassandra.tcm.Commit`

use std::cmp::min;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::node::NodeId;
use crate::tcm::{Epoch, TcmMetadata, Transformation};

// ─────────────────────────────────────────────────────────────────────────────
// CommitResult
// ─────────────────────────────────────────────────────────────────────────────

/// Outcome of a commit attempt against the metadata log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommitResult {
    /// The transformation was successfully committed at the given epoch.
    Success(Epoch),
    /// The transformation was rejected for a logical reason.
    Rejected(String),
    /// The caller's expected epoch does not match the current epoch.
    StaleEpoch { expected: Epoch, actual: Epoch },
}

// ─────────────────────────────────────────────────────────────────────────────
// CommitRequest
// ─────────────────────────────────────────────────────────────────────────────

/// A request to commit a transformation to the metadata log.
#[derive(Debug, Clone)]
pub struct CommitRequest {
    /// The transformation to apply.
    pub transformation: Transformation,
    /// The node submitting the commit.
    pub committed_by: NodeId,
    /// If `Some`, the commit is conditional on the current epoch matching.
    /// If `None`, the commit is unconditional.
    pub expected_epoch: Option<Epoch>,
}

impl CommitRequest {
    /// Create a new commit request.
    pub fn new(
        transformation: Transformation,
        committed_by: NodeId,
        expected_epoch: Option<Epoch>,
    ) -> Self {
        Self {
            transformation,
            committed_by,
            expected_epoch,
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// RetryPolicy
// ─────────────────────────────────────────────────────────────────────────────

/// Exponential backoff retry policy for commit attempts.
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    /// Maximum number of retries before giving up.
    pub max_retries: u32,
    /// Base delay in milliseconds for the first retry.
    pub base_delay_ms: u64,
    /// Maximum delay in milliseconds (cap for exponential backoff).
    pub max_delay_ms: u64,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_retries: 3,
            base_delay_ms: 100,
            max_delay_ms: 5000,
        }
    }
}

impl RetryPolicy {
    /// Calculate the delay for a given retry attempt (0-indexed).
    ///
    /// Uses exponential backoff: `base_delay_ms * 2^attempt`, capped at
    /// `max_delay_ms`.
    pub fn delay_for_attempt(&self, attempt: u32) -> u64 {
        let delay = self.base_delay_ms.saturating_mul(1u64 << attempt);
        min(delay, self.max_delay_ms)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Processor trait
// ─────────────────────────────────────────────────────────────────────────────

/// Abstraction for committing metadata transformations.
///
/// Implementations may be local (single-node) or distributed (Raft/Paxos).
pub trait Processor: Send + Sync {
    /// Attempt to commit a transformation.
    fn commit(&self, request: CommitRequest) -> CommitResult;

    /// Return the current epoch of the metadata log.
    fn current_epoch(&self) -> Epoch;
}

// ─────────────────────────────────────────────────────────────────────────────
// LocalProcessor
// ─────────────────────────────────────────────────────────────────────────────

/// A single-node commit processor backed by an in-memory [`TcmMetadata`].
///
/// Validates epoch expectations under the lock and applies transformations
/// atomically. Suitable for single-node deployments or testing.
pub struct LocalProcessor {
    metadata: Mutex<TcmMetadata>,
}

impl LocalProcessor {
    /// Create a new `LocalProcessor` wrapping the given metadata.
    pub fn new(metadata: TcmMetadata) -> Self {
        Self {
            metadata: Mutex::new(metadata),
        }
    }
}

impl Processor for LocalProcessor {
    fn commit(&self, request: CommitRequest) -> CommitResult {
        let mut metadata = self.metadata.lock().expect("metadata lock poisoned");

        // Check epoch expectation if provided.
        if let Some(expected) = request.expected_epoch {
            if expected != metadata.epoch {
                return CommitResult::StaleEpoch {
                    expected,
                    actual: metadata.epoch,
                };
            }
        }

        // Apply the transformation.
        match metadata.apply(request.transformation, request.committed_by) {
            Ok(epoch) => CommitResult::Success(epoch),
            Err(err) => CommitResult::Rejected(err.to_string()),
        }
    }

    fn current_epoch(&self) -> Epoch {
        let metadata = self.metadata.lock().expect("metadata lock poisoned");
        metadata.epoch
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// AtomicLongProcessor (testing)
// ─────────────────────────────────────────────────────────────────────────────

/// A minimal commit processor for testing that only tracks an epoch counter.
///
/// Ignores the transformation entirely; each commit simply increments the
/// internal epoch. Useful for testing retry logic and concurrency without
/// the overhead of full metadata state.
pub struct AtomicLongProcessor {
    epoch: AtomicU64,
}

impl AtomicLongProcessor {
    /// Create a new `AtomicLongProcessor` starting at the given epoch.
    pub fn new(initial_epoch: Epoch) -> Self {
        Self {
            epoch: AtomicU64::new(initial_epoch.value()),
        }
    }
}

impl Processor for AtomicLongProcessor {
    fn commit(&self, _request: CommitRequest) -> CommitResult {
        let new_val = self.epoch.fetch_add(1, Ordering::SeqCst) + 1;
        CommitResult::Success(Epoch(new_val))
    }

    fn current_epoch(&self) -> Epoch {
        Epoch(self.epoch.load(Ordering::SeqCst))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::Endpoint;
    use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
    use std::sync::Arc;
    use std::thread;
    use uuid::Uuid;

    fn node_id(n: u128) -> NodeId {
        NodeId::from_uuid(Uuid::from_u128(n))
    }

    fn ep(port: u16) -> Endpoint {
        Endpoint::new(SocketAddr::V4(SocketAddrV4::new(
            Ipv4Addr::new(127, 0, 0, 1),
            port,
        )))
    }

    fn register_transformation(n: u128, port: u16) -> Transformation {
        Transformation::Register {
            node_id: node_id(n),
            endpoint: ep(port),
            dc: "dc1".into(),
            rack: "rack1".into(),
        }
    }

    // ── CommitResult ─────────────────────────────────────────────────────

    #[test]
    fn commit_success() {
        let processor = LocalProcessor::new(TcmMetadata::new());
        let request = CommitRequest::new(register_transformation(1, 7001), node_id(1), None);

        let result = processor.commit(request);
        assert_eq!(result, CommitResult::Success(Epoch::FIRST));
        assert_eq!(processor.current_epoch(), Epoch::FIRST);
    }

    #[test]
    fn commit_success_with_matching_epoch() {
        let processor = LocalProcessor::new(TcmMetadata::new());

        // Commit without epoch check to advance to epoch 1.
        processor.commit(CommitRequest::new(
            register_transformation(1, 7001),
            node_id(1),
            None,
        ));

        // Now commit with expected_epoch = 1 (current).
        let request = CommitRequest::new(
            Transformation::SchemaChange {
                schema_version: Uuid::from_u128(42),
                description: "create table".into(),
            },
            node_id(1),
            Some(Epoch::FIRST),
        );
        let result = processor.commit(request);
        assert_eq!(result, CommitResult::Success(Epoch(2)));
    }

    #[test]
    fn commit_rejected_stale_epoch() {
        let processor = LocalProcessor::new(TcmMetadata::new());

        // Advance to epoch 1.
        processor.commit(CommitRequest::new(
            register_transformation(1, 7001),
            node_id(1),
            None,
        ));

        // Try to commit with stale expected_epoch = 0.
        let request = CommitRequest::new(
            register_transformation(2, 7002),
            node_id(2),
            Some(Epoch::EMPTY),
        );
        let result = processor.commit(request);
        assert_eq!(
            result,
            CommitResult::StaleEpoch {
                expected: Epoch::EMPTY,
                actual: Epoch::FIRST,
            }
        );
    }

    #[test]
    fn commit_rejected_duplicate_node() {
        let processor = LocalProcessor::new(TcmMetadata::new());

        // Register node 1.
        let result = processor.commit(CommitRequest::new(
            register_transformation(1, 7001),
            node_id(1),
            None,
        ));
        assert_eq!(result, CommitResult::Success(Epoch::FIRST));

        // Try to register node 1 again — should be rejected.
        let result = processor.commit(CommitRequest::new(
            register_transformation(1, 7001),
            node_id(1),
            None,
        ));
        assert!(matches!(result, CommitResult::Rejected(_)));
    }

    // ── Concurrent commits via LocalProcessor ────────────────────────────

    #[test]
    fn concurrent_commits_local_processor() {
        let processor = Arc::new(LocalProcessor::new(TcmMetadata::new()));
        let num_threads = 8;
        let commits_per_thread = 10;

        let handles: Vec<_> = (0..num_threads)
            .map(|t| {
                let proc = Arc::clone(&processor);
                thread::spawn(move || {
                    let mut successes = 0u32;
                    for i in 0..commits_per_thread {
                        let n = (t * 1000 + i + 1) as u128;
                        let port = (9000 + t * 1000 + i) as u16;
                        let request =
                            CommitRequest::new(register_transformation(n, port), node_id(n), None);
                        if let CommitResult::Success(_) = proc.commit(request) {
                            successes += 1;
                        }
                    }
                    successes
                })
            })
            .collect();

        let total_successes: u32 = handles.into_iter().map(|h| h.join().unwrap()).sum();
        let final_epoch = processor.current_epoch();

        // All commits should succeed (unique node IDs).
        assert_eq!(total_successes, num_threads * commits_per_thread);
        assert_eq!(
            final_epoch,
            Epoch((num_threads * commits_per_thread) as u64)
        );
    }

    // ── AtomicLongProcessor ──────────────────────────────────────────────

    #[test]
    fn atomic_long_processor_increments() {
        let processor = AtomicLongProcessor::new(Epoch::EMPTY);

        let r1 = processor.commit(CommitRequest::new(
            register_transformation(1, 7001),
            node_id(1),
            None,
        ));
        assert_eq!(r1, CommitResult::Success(Epoch::FIRST));

        let r2 = processor.commit(CommitRequest::new(
            register_transformation(2, 7002),
            node_id(2),
            None,
        ));
        assert_eq!(r2, CommitResult::Success(Epoch(2)));

        assert_eq!(processor.current_epoch(), Epoch(2));
    }

    #[test]
    fn atomic_long_processor_concurrent() {
        let processor = Arc::new(AtomicLongProcessor::new(Epoch::EMPTY));
        let num_threads = 8;
        let commits_per_thread = 100;

        let handles: Vec<_> = (0..num_threads)
            .map(|_| {
                let proc = Arc::clone(&processor);
                thread::spawn(move || {
                    for _ in 0..commits_per_thread {
                        proc.commit(CommitRequest::new(
                            Transformation::ForceSnapshot,
                            node_id(1),
                            None,
                        ));
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(
            processor.current_epoch(),
            Epoch(num_threads * commits_per_thread)
        );
    }

    // ── RetryPolicy ─────────────────────────────────────────────────────

    #[test]
    fn retry_policy_default() {
        let policy = RetryPolicy::default();
        assert_eq!(policy.max_retries, 3);
        assert_eq!(policy.base_delay_ms, 100);
        assert_eq!(policy.max_delay_ms, 5000);
    }

    #[test]
    fn retry_policy_exponential_backoff() {
        let policy = RetryPolicy {
            max_retries: 10,
            base_delay_ms: 100,
            max_delay_ms: 5000,
        };

        assert_eq!(policy.delay_for_attempt(0), 100); // 100 * 2^0
        assert_eq!(policy.delay_for_attempt(1), 200); // 100 * 2^1
        assert_eq!(policy.delay_for_attempt(2), 400); // 100 * 2^2
        assert_eq!(policy.delay_for_attempt(3), 800); // 100 * 2^3
        assert_eq!(policy.delay_for_attempt(4), 1600); // 100 * 2^4
        assert_eq!(policy.delay_for_attempt(5), 3200); // 100 * 2^5
        assert_eq!(policy.delay_for_attempt(6), 5000); // capped at max
        assert_eq!(policy.delay_for_attempt(7), 5000); // still capped
    }

    #[test]
    fn retry_policy_overflow_saturates() {
        let policy = RetryPolicy {
            max_retries: 100,
            base_delay_ms: 1000,
            max_delay_ms: 10_000,
        };

        // Large attempt values should saturate, not panic.
        assert_eq!(policy.delay_for_attempt(63), 10_000);
    }
}
