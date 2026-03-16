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

//! Short-read protection for coordinator reads.
//!
//! When a replica returns fewer rows than expected (because of tombstones,
//! TTL expirations, or range deletions filtering out rows), the coordinator
//! transparently re-queries that replica starting from where it left off.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.service.reads.ShortReadProtection`
//! - `org.apache.cassandra.service.reads.ShortReadRowsProtection`
//! - `org.apache.cassandra.service.reads.ShortReadPartitionsProtection`

/// Tracks whether a replica response was a short read and whether
/// a retry is needed.
///
/// A short read occurs when a replica returns fewer rows than the requested
/// limit, even though more data may exist beyond the tombstoned/TTLd rows.
#[derive(Debug, Clone)]
pub struct ShortReadProtection {
    /// Maximum retries to prevent infinite loops.
    pub max_retries: u32,
    /// Current retry count.
    pub retries: u32,
    /// Whether short-read protection is enabled (always true in production).
    pub enabled: bool,
}

impl ShortReadProtection {
    /// Default: enabled with max 3 retries.
    pub fn new() -> Self {
        Self {
            max_retries: 3,
            retries: 0,
            enabled: true,
        }
    }

    /// Disabled short-read protection (for testing).
    pub fn disabled() -> Self {
        Self {
            max_retries: 0,
            retries: 0,
            enabled: false,
        }
    }

    /// Check if a response is a short read that needs a retry.
    ///
    /// A short read is detected when:
    /// 1. The replica returned fewer rows than the per-partition or page limit
    /// 2. There might be more rows beyond the last returned row
    /// 3. Tombstones were encountered that may have hidden rows
    pub fn needs_retry(
        &self,
        rows_requested: usize,
        rows_received: usize,
        tombstones_encountered: u32,
    ) -> bool {
        if !self.enabled {
            return false;
        }
        if self.retries >= self.max_retries {
            return false;
        }
        // If we got fewer rows than requested AND we saw tombstones,
        // there might be more live rows beyond the tombstones.
        rows_received < rows_requested && tombstones_encountered > 0
    }

    /// Record a retry.
    pub fn record_retry(&mut self) {
        self.retries += 1;
    }

    /// Build a retry command: adjusts the start position to continue
    /// after the last returned row.
    pub fn retry_bounds(
        &self,
        last_clustering_key: &[u8],
        remaining_limit: usize,
    ) -> ShortReadRetry {
        ShortReadRetry {
            start_after: last_clustering_key.to_vec(),
            adjusted_limit: remaining_limit,
        }
    }

    /// Whether we've exhausted retries.
    pub fn max_retries_reached(&self) -> bool {
        self.retries >= self.max_retries
    }
}

impl Default for ShortReadProtection {
    fn default() -> Self {
        Self::new()
    }
}

/// Parameters for a short-read retry query.
#[derive(Debug, Clone)]
pub struct ShortReadRetry {
    /// Start reading after this clustering key (exclusive).
    pub start_after: Vec<u8>,
    /// Adjusted limit for the retry.
    pub adjusted_limit: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn short_read_detection_with_tombstones() {
        let srp = ShortReadProtection::new();
        // Requested 100, got 80, saw 20 tombstones → needs retry
        assert!(srp.needs_retry(100, 80, 20));
    }

    #[test]
    fn no_short_read_when_fully_satisfied() {
        let srp = ShortReadProtection::new();
        // Got exactly what we asked for → no retry
        assert!(!srp.needs_retry(100, 100, 5));
    }

    #[test]
    fn no_short_read_without_tombstones() {
        let srp = ShortReadProtection::new();
        // Got fewer rows but no tombstones → partition is just smaller
        assert!(!srp.needs_retry(100, 50, 0));
    }

    #[test]
    fn max_retries_respected() {
        let mut srp = ShortReadProtection::new();
        srp.max_retries = 2;
        srp.record_retry();
        srp.record_retry();
        assert!(!srp.needs_retry(100, 50, 10));
        assert!(srp.max_retries_reached());
    }

    #[test]
    fn disabled_never_retries() {
        let srp = ShortReadProtection::disabled();
        assert!(!srp.needs_retry(100, 50, 50));
    }

    #[test]
    fn retry_bounds_construction() {
        let srp = ShortReadProtection::new();
        let retry = srp.retry_bounds(b"last_ck", 50);
        assert_eq!(retry.start_after, b"last_ck");
        assert_eq!(retry.adjusted_limit, 50);
    }
}
