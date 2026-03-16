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

//! Async write response handler for collecting replica acks.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.service.AbstractWriteResponseHandler`
//! - `org.apache.cassandra.service.WriteResponseHandler`
//! - `org.apache.cassandra.service.DatacenterWriteResponseHandler`

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use tokio::sync::Notify;
use tracing::{debug, warn};

use cassandra_cluster_metadata::Endpoint;

use crate::consistency::ConsistencyLevel;
use crate::write::{WriteError, WriteResult, WriteType};

/// Reason a replica reported failure instead of ack.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RequestFailureReason {
    /// General unknown failure.
    Unknown,
    /// Read/write was rejected because of too many tombstones.
    TooManyTombstones,
    /// Node is overloaded.
    Overloaded,
    /// Index not available.
    IndexNotAvailable,
}

impl RequestFailureReason {
    /// Protocol failure code matching Java's `RequestFailureReason`.
    pub fn code(&self) -> u16 {
        match self {
            Self::Unknown => 0x0000,
            Self::TooManyTombstones => 0x0001,
            Self::Overloaded => 0x0002,
            Self::IndexNotAvailable => 0x0003,
        }
    }
}

/// Tracks replica responses for a write operation and determines
/// when a consistency level has been satisfied.
///
/// Thread-safe: acks and failures can be recorded concurrently from
/// multiple Tokio tasks handling remote replica responses.
pub struct WriteResponseHandler {
    /// Consistency level being enforced.
    cl: ConsistencyLevel,
    /// Write type for error reporting.
    write_type: WriteType,
    /// Number of acks needed to satisfy CL.
    required: usize,
    /// Number of acks received so far.
    acks_received: AtomicUsize,
    /// Failure map: endpoint → reason.
    failures: Mutex<HashMap<Endpoint, RequestFailureReason>>,
    /// Total number of replicas contacted.
    total_replicas: usize,
    /// List of endpoints that were contacted.
    contacted: Vec<Endpoint>,
    /// Instant the handler was created (for timeout tracking).
    created_at: Instant,
    /// Write timeout.
    timeout: Duration,
    /// Notifier signaled when CL is met or failure is detected.
    notify: Arc<Notify>,
    /// Whether the ideal CL count has been reached (all replicas ack'd).
    all_acked: AtomicUsize,
    /// Number of hints stored for non-responding replicas.
    hints_stored: AtomicUsize,
}

impl WriteResponseHandler {
    /// Create a new handler.
    ///
    /// # Arguments
    /// - `cl`: target consistency level
    /// - `write_type`: type of write (Simple, Batch, etc.)
    /// - `required`: number of acks needed for CL
    /// - `contacted`: list of contacted replicas
    /// - `timeout`: maximum wait duration
    pub fn new(
        cl: ConsistencyLevel,
        write_type: WriteType,
        required: usize,
        contacted: Vec<Endpoint>,
        timeout: Duration,
    ) -> Self {
        let total_replicas = contacted.len();
        Self {
            cl,
            write_type,
            required,
            acks_received: AtomicUsize::new(0),
            failures: Mutex::new(HashMap::new()),
            total_replicas,
            contacted,
            created_at: Instant::now(),
            timeout,
            notify: Arc::new(Notify::new()),
            all_acked: AtomicUsize::new(0),
            hints_stored: AtomicUsize::new(0),
        }
    }

    /// Record a successful ack from a replica.
    ///
    /// Returns `true` if this ack satisfies the CL.
    pub fn on_response(&self, _from: &Endpoint) -> bool {
        let prev = self.acks_received.fetch_add(1, Ordering::AcqRel);
        let new_count = prev + 1;

        debug!(
            acks = new_count,
            required = self.required,
            cl = %self.cl,
            "Write ack received"
        );

        if new_count == self.total_replicas {
            self.all_acked.store(1, Ordering::Release);
        }

        if new_count >= self.required {
            self.notify.notify_waiters();
            true
        } else {
            false
        }
    }

    /// Record a failure from a replica.
    ///
    /// Checks whether it's still possible to satisfy the CL; if not,
    /// signals completion so the coordinator can return an error promptly.
    pub fn on_failure(&self, from: Endpoint, reason: RequestFailureReason) {
        warn!(
            endpoint = %from,
            reason = ?reason,
            cl = %self.cl,
            "Write failure from replica"
        );

        {
            let mut failures = self.failures.lock();
            failures.insert(from, reason);
        }

        // Check if CL is still achievable
        let failure_count = self.failures.lock().len();
        let acks = self.acks_received.load(Ordering::Acquire);
        let remaining = self.total_replicas.saturating_sub(acks + failure_count);

        if acks + remaining < self.required {
            // CL can never be met — signal early
            self.notify.notify_waiters();
        }
    }

    /// Record that a hint was stored for a non-responding replica.
    pub fn on_hint_stored(&self) {
        self.hints_stored.fetch_add(1, Ordering::Relaxed);
    }

    /// Wait for the CL to be satisfied or timeout.
    ///
    /// Returns the write result or an appropriate error.
    pub async fn await_completion(&self) -> Result<WriteResult, WriteError> {
        let remaining = self.timeout.saturating_sub(self.created_at.elapsed());

        if remaining.is_zero() {
            return self.make_timeout_error();
        }

        // Fast path: already satisfied
        if self.is_cl_met() {
            return self.make_result();
        }

        // Wait with timeout
        let result = tokio::time::timeout(remaining, async {
            loop {
                if self.is_cl_met() || self.is_cl_impossible() {
                    break;
                }
                self.notify.notified().await;
            }
        })
        .await;

        match result {
            Ok(()) => {
                if self.is_cl_met() {
                    self.make_result()
                } else {
                    // CL impossible due to failures
                    self.make_failure_error()
                }
            }
            Err(_) => self.make_timeout_error(),
        }
    }

    /// Whether the CL has been satisfied.
    fn is_cl_met(&self) -> bool {
        let acks = self.acks_received.load(Ordering::Acquire);
        if self.cl == ConsistencyLevel::Any {
            acks + self.hints_stored.load(Ordering::Relaxed) > 0
        } else {
            acks >= self.required
        }
    }

    /// Whether the CL can still be achieved.
    fn is_cl_impossible(&self) -> bool {
        let failure_count = self.failures.lock().len();
        let acks = self.acks_received.load(Ordering::Acquire);
        let remaining = self.total_replicas.saturating_sub(acks + failure_count);
        acks + remaining < self.required
    }

    fn make_result(&self) -> Result<WriteResult, WriteError> {
        Ok(WriteResult {
            acks_received: self.acks_received.load(Ordering::Acquire),
            acks_required: self.required,
            contacted_replicas: self.contacted.clone(),
            hints_stored: self.hints_stored.load(Ordering::Relaxed),
        })
    }

    fn make_timeout_error(&self) -> Result<WriteResult, WriteError> {
        let acks = self.acks_received.load(Ordering::Acquire);
        Err(WriteError::Timeout {
            cl: self.cl,
            write_type: self.write_type,
            required: self.required,
            received: acks,
            block_for: self.required,
        })
    }

    fn make_failure_error(&self) -> Result<WriteResult, WriteError> {
        let acks = self.acks_received.load(Ordering::Acquire);
        let failures = self.failures.lock();
        let failure_map: HashMap<Endpoint, u16> = failures
            .iter()
            .map(|(ep, reason)| (*ep, reason.code()))
            .collect();
        Err(WriteError::WriteFailure {
            cl: self.cl,
            write_type: self.write_type,
            required: self.required,
            received: acks,
            block_for: self.required,
            num_failures: failures.len(),
            failure_map,
        })
    }

    /// Current number of acks received.
    pub fn current_acks(&self) -> usize {
        self.acks_received.load(Ordering::Acquire)
    }

    /// Current failure count.
    pub fn failure_count(&self) -> usize {
        self.failures.lock().len()
    }

    /// Elapsed time since creation.
    pub fn elapsed(&self) -> Duration {
        self.created_at.elapsed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    fn ep(port: u16) -> Endpoint {
        Endpoint::new(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            port,
        ))
    }

    #[tokio::test]
    async fn handler_satisfies_cl_one() {
        let contacted = vec![ep(7001), ep(7002), ep(7003)];
        let handler = WriteResponseHandler::new(
            ConsistencyLevel::One,
            WriteType::Simple,
            1,
            contacted,
            Duration::from_secs(2),
        );

        // One ack should satisfy
        assert!(handler.on_response(&ep(7001)));
        let result = handler.await_completion().await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap().acks_received, 1);
    }

    #[tokio::test]
    async fn handler_satisfies_cl_quorum() {
        let contacted = vec![ep(7001), ep(7002), ep(7003)];
        let handler = WriteResponseHandler::new(
            ConsistencyLevel::Quorum,
            WriteType::Simple,
            2,
            contacted,
            Duration::from_secs(2),
        );

        assert!(!handler.on_response(&ep(7001))); // 1 of 2, not yet
        assert!(handler.on_response(&ep(7002))); // 2 of 2, done!

        let result = handler.await_completion().await;
        assert!(result.is_ok());
        let r = result.unwrap();
        assert_eq!(r.acks_received, 2);
        assert_eq!(r.acks_required, 2);
    }

    #[tokio::test]
    async fn handler_write_failure() {
        let contacted = vec![ep(7001), ep(7002), ep(7003)];
        let handler = WriteResponseHandler::new(
            ConsistencyLevel::All,
            WriteType::Simple,
            3,
            contacted,
            Duration::from_secs(2),
        );

        handler.on_response(&ep(7001));
        handler.on_response(&ep(7002));
        handler.on_failure(ep(7003), RequestFailureReason::Unknown);

        let result = handler.await_completion().await;
        assert!(result.is_err());
        match result.unwrap_err() {
            WriteError::WriteFailure {
                num_failures,
                received,
                ..
            } => {
                assert_eq!(num_failures, 1);
                assert_eq!(received, 2);
            }
            e => panic!("Expected WriteFailure, got {e:?}"),
        }
    }

    #[tokio::test]
    async fn handler_timeout() {
        let contacted = vec![ep(7001), ep(7002), ep(7003)];
        let handler = WriteResponseHandler::new(
            ConsistencyLevel::All,
            WriteType::Simple,
            3,
            contacted,
            Duration::from_millis(50), // very short timeout
        );

        handler.on_response(&ep(7001));
        // Only 1 ack, need 3 — will timeout

        let result = handler.await_completion().await;
        assert!(result.is_err());
        match result.unwrap_err() {
            WriteError::Timeout {
                received, required, ..
            } => {
                assert_eq!(received, 1);
                assert_eq!(required, 3);
            }
            e => panic!("Expected Timeout, got {e:?}"),
        }
    }

    #[tokio::test]
    async fn handler_cl_any_with_hint() {
        let contacted = vec![ep(7001)];
        let handler = WriteResponseHandler::new(
            ConsistencyLevel::Any,
            WriteType::Simple,
            1,
            contacted,
            Duration::from_secs(2),
        );

        // No acks, but a hint counts for CL=ANY
        handler.on_hint_stored();

        let result = handler.await_completion().await;
        assert!(result.is_ok());
    }

    #[test]
    fn failure_reason_codes() {
        assert_eq!(RequestFailureReason::Unknown.code(), 0x0000);
        assert_eq!(RequestFailureReason::TooManyTombstones.code(), 0x0001);
        assert_eq!(RequestFailureReason::Overloaded.code(), 0x0002);
        assert_eq!(RequestFailureReason::IndexNotAvailable.code(), 0x0003);
    }

    #[tokio::test]
    async fn handler_failure_makes_cl_impossible_early_exit() {
        let contacted = vec![ep(7001), ep(7002)];
        let handler = WriteResponseHandler::new(
            ConsistencyLevel::All,
            WriteType::Simple,
            2,
            contacted,
            Duration::from_secs(5),
        );

        // One failure immediately makes CL=ALL impossible with 2 replicas
        handler.on_failure(ep(7001), RequestFailureReason::Overloaded);

        let result = handler.await_completion().await;
        assert!(result.is_err());
    }
}
