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

//! Three-tier byte-based resource limits for internode messaging.
//!
//! Limits are enforced at three levels:
//! - Per-connection: 4 MiB
//! - Per-endpoint: 128 MiB
//! - Global: 512 MiB
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.net.ResourceLimits`
//! - `org.apache.cassandra.net.ResourceLimits.Limit`
//! - `org.apache.cassandra.net.ResourceLimits.EndpointAndGlobal`

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// Default per-connection byte limit (4 MiB).
pub const DEFAULT_CONNECTION_LIMIT: usize = 4 * 1024 * 1024;

/// Default per-endpoint byte limit (128 MiB).
pub const DEFAULT_ENDPOINT_LIMIT: usize = 128 * 1024 * 1024;

/// Default global byte limit (512 MiB).
pub const DEFAULT_GLOBAL_LIMIT: usize = 512 * 1024 * 1024;

/// A single atomic byte-count limit with try-allocate/release semantics.
#[derive(Debug)]
pub struct Limit {
    allocated: AtomicUsize,
    max_bytes: usize,
}

impl Limit {
    /// Create a new limit with the given maximum byte count.
    pub fn new(max_bytes: usize) -> Self {
        Self {
            allocated: AtomicUsize::new(0),
            max_bytes,
        }
    }

    /// Try to allocate `bytes`. Returns `true` on success.
    ///
    /// Uses a CAS loop to ensure atomic allocation without exceeding
    /// the maximum.
    pub fn try_allocate(&self, bytes: usize) -> bool {
        loop {
            let current = self.allocated.load(Ordering::Acquire);
            if current + bytes > self.max_bytes {
                return false;
            }
            if self
                .allocated
                .compare_exchange_weak(
                    current,
                    current + bytes,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_ok()
            {
                return true;
            }
        }
    }

    /// Release `bytes` previously allocated.
    pub fn release(&self, bytes: usize) {
        self.allocated.fetch_sub(bytes, Ordering::Release);
    }

    /// Current number of bytes allocated.
    pub fn allocated(&self) -> usize {
        self.allocated.load(Ordering::Relaxed)
    }

    /// Maximum byte limit.
    pub fn max_bytes(&self) -> usize {
        self.max_bytes
    }

    /// Remaining capacity in bytes.
    pub fn remaining(&self) -> usize {
        self.max_bytes.saturating_sub(self.allocated())
    }
}

/// Three-tier limits for a single endpoint.
///
/// All three tiers must succeed for an allocation to proceed:
/// connection → endpoint → global.
#[derive(Debug, Clone)]
pub struct EndpointLimits {
    connection: Arc<Limit>,
    endpoint: Arc<Limit>,
    global: Arc<Limit>,
}

impl EndpointLimits {
    /// Create endpoint limits with the given tiers.
    pub fn new(connection: Arc<Limit>, endpoint: Arc<Limit>, global: Arc<Limit>) -> Self {
        Self {
            connection,
            endpoint,
            global,
        }
    }

    /// Create with default limits.
    pub fn with_defaults(global: Arc<Limit>) -> Self {
        Self {
            connection: Arc::new(Limit::new(DEFAULT_CONNECTION_LIMIT)),
            endpoint: Arc::new(Limit::new(DEFAULT_ENDPOINT_LIMIT)),
            global,
        }
    }

    /// Try to allocate `bytes` across all three tiers.
    ///
    /// Returns a `Permit` on success that automatically releases
    /// the allocation when dropped. Returns `None` if any tier
    /// would be exceeded.
    pub fn try_allocate(&self, bytes: usize) -> Option<Permit> {
        if !self.connection.try_allocate(bytes) {
            return None;
        }
        if !self.endpoint.try_allocate(bytes) {
            self.connection.release(bytes);
            return None;
        }
        if !self.global.try_allocate(bytes) {
            self.endpoint.release(bytes);
            self.connection.release(bytes);
            return None;
        }
        Some(Permit {
            bytes,
            connection: Arc::clone(&self.connection),
            endpoint: Arc::clone(&self.endpoint),
            global: Arc::clone(&self.global),
        })
    }

    /// Get the connection-level limit.
    pub fn connection_limit(&self) -> &Limit {
        &self.connection
    }

    /// Get the endpoint-level limit.
    pub fn endpoint_limit(&self) -> &Limit {
        &self.endpoint
    }

    /// Get the global limit.
    pub fn global_limit(&self) -> &Limit {
        &self.global
    }
}

/// RAII permit that releases allocated bytes on drop.
#[derive(Debug)]
pub struct Permit {
    bytes: usize,
    connection: Arc<Limit>,
    endpoint: Arc<Limit>,
    global: Arc<Limit>,
}

impl Permit {
    /// Number of bytes held by this permit.
    pub fn bytes(&self) -> usize {
        self.bytes
    }
}

impl Drop for Permit {
    fn drop(&mut self) {
        self.connection.release(self.bytes);
        self.endpoint.release(self.bytes);
        self.global.release(self.bytes);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn limit_basic_allocate_release() {
        let limit = Limit::new(100);
        assert!(limit.try_allocate(50));
        assert_eq!(limit.allocated(), 50);
        assert_eq!(limit.remaining(), 50);

        assert!(limit.try_allocate(50));
        assert_eq!(limit.allocated(), 100);

        // Should fail — at capacity
        assert!(!limit.try_allocate(1));

        limit.release(30);
        assert_eq!(limit.allocated(), 70);
        assert!(limit.try_allocate(30));
    }

    #[test]
    fn limit_zero_allocation() {
        let limit = Limit::new(100);
        assert!(limit.try_allocate(0));
        assert_eq!(limit.allocated(), 0);
    }

    #[test]
    fn limit_exact_capacity() {
        let limit = Limit::new(100);
        assert!(limit.try_allocate(100));
        assert!(!limit.try_allocate(1));
    }

    #[test]
    fn endpoint_limits_three_tier() {
        let global = Arc::new(Limit::new(1000));
        let limits = EndpointLimits::with_defaults(Arc::clone(&global));

        let permit = limits.try_allocate(100).expect("should succeed");
        assert_eq!(permit.bytes(), 100);
        assert_eq!(limits.connection_limit().allocated(), 100);
        assert_eq!(limits.endpoint_limit().allocated(), 100);
        assert_eq!(global.allocated(), 100);

        drop(permit);
        assert_eq!(limits.connection_limit().allocated(), 0);
        assert_eq!(limits.endpoint_limit().allocated(), 0);
        assert_eq!(global.allocated(), 0);
    }

    #[test]
    fn endpoint_limits_connection_exhaustion() {
        let global = Arc::new(Limit::new(DEFAULT_GLOBAL_LIMIT));
        let connection = Arc::new(Limit::new(100));
        let endpoint = Arc::new(Limit::new(DEFAULT_ENDPOINT_LIMIT));
        let limits = EndpointLimits::new(connection, endpoint, global);

        let _p1 = limits.try_allocate(100).expect("should succeed");
        assert!(limits.try_allocate(1).is_none());
    }

    #[test]
    fn endpoint_limits_global_exhaustion() {
        let global = Arc::new(Limit::new(200));
        let l1 = EndpointLimits::with_defaults(Arc::clone(&global));
        let l2 = EndpointLimits::with_defaults(Arc::clone(&global));

        let _p1 = l1.try_allocate(150).expect("should succeed");
        // Global only has 50 left
        assert!(l2.try_allocate(100).is_none());

        let _p2 = l2.try_allocate(50).expect("should succeed with 50");
        assert_eq!(global.allocated(), 200);
    }

    #[test]
    fn permit_raii_release() {
        let global = Arc::new(Limit::new(1000));
        let limits = EndpointLimits::with_defaults(Arc::clone(&global));

        {
            let _p = limits.try_allocate(500).unwrap();
            assert_eq!(global.allocated(), 500);
        }
        // Permit dropped
        assert_eq!(global.allocated(), 0);
    }

    #[test]
    fn endpoint_limits_rollback_on_failure() {
        // Endpoint limit smaller than connection limit
        let global = Arc::new(Limit::new(DEFAULT_GLOBAL_LIMIT));
        let connection = Arc::new(Limit::new(1000));
        let endpoint = Arc::new(Limit::new(50));
        let limits = EndpointLimits::new(connection, endpoint, Arc::clone(&global));

        // This should fail at the endpoint tier and rollback connection
        assert!(limits.try_allocate(100).is_none());
        assert_eq!(limits.connection_limit().allocated(), 0);
        assert_eq!(global.allocated(), 0);
    }
}
