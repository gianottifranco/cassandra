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

//! Paxos ballot: a totally-ordered proposal identifier.
//!
//! ## Java Oracle
//!
//! `org.apache.cassandra.service.paxos.Ballot`
//!
//! In Java Cassandra, the ballot is a timeuuid that encodes the timestamp
//! and the proposing node's identity.  We model this as a `(timestamp_micros,
//! node_id)` pair with lexicographic ordering — timestamp first, then node id
//! as tie-breaker.  This guarantees total ordering across all nodes as long as
//! clocks have microsecond granularity (same assumption as Java).

use std::cmp::Ordering;
use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// A Paxos ballot — the unique, totally-ordered identifier for a proposal.
///
/// ## Ordering
///
/// Ballots are compared first by `timestamp_micros` (higher = newer),
/// then by `node_id` as an arbitrary tie-breaker.  This mirrors the Java
/// implementation's timeuuid comparison.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct Ballot {
    /// Microseconds since UNIX epoch when this ballot was created.
    pub timestamp_micros: i64,
    /// The UUID of the node that created this ballot.
    pub node_id: Uuid,
}

impl Ballot {
    /// Create a new ballot with the current wall-clock time.
    pub fn new(node_id: Uuid) -> Self {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock before epoch")
            .as_micros() as i64;

        Self {
            timestamp_micros: ts,
            node_id,
        }
    }

    /// Create a ballot with an explicit timestamp (useful for tests & recovery).
    pub fn with_timestamp(timestamp_micros: i64, node_id: Uuid) -> Self {
        Self {
            timestamp_micros,
            node_id,
        }
    }

    /// The "zero" ballot — always compares less than any real ballot.
    pub fn none() -> Self {
        Self {
            timestamp_micros: 0,
            node_id: Uuid::nil(),
        }
    }

    /// Returns `true` if this is the zero / nil ballot.
    pub fn is_none(&self) -> bool {
        self.timestamp_micros == 0 && self.node_id.is_nil()
    }

    /// Create a ballot strictly newer than `other` (same node).
    ///
    /// Used during contention: the new proposer must create a ballot that
    /// supersedes an observed one.
    pub fn newer_than(other: &Ballot, node_id: Uuid) -> Self {
        Self {
            // +1 µs guarantees strict ordering even for same node
            timestamp_micros: other.timestamp_micros + 1,
            node_id,
        }
    }
}

impl Ord for Ballot {
    fn cmp(&self, other: &Self) -> Ordering {
        self.timestamp_micros
            .cmp(&other.timestamp_micros)
            .then_with(|| self.node_id.cmp(&other.node_id))
    }
}

impl PartialOrd for Ballot {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl fmt::Display for Ballot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Ballot(ts={}, node={})",
            self.timestamp_micros, self.node_id
        )
    }
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

    #[test]
    fn ballot_ordering_by_timestamp() {
        let b1 = Ballot::with_timestamp(100, node_a());
        let b2 = Ballot::with_timestamp(200, node_a());
        assert!(b1 < b2);
    }

    #[test]
    fn ballot_ordering_by_node_on_tie() {
        let b1 = Ballot::with_timestamp(100, node_a());
        let b2 = Ballot::with_timestamp(100, node_b());
        // node_a < node_b by UUID ordering
        assert!(b1 < b2);
    }

    #[test]
    fn ballot_none_is_smallest() {
        let none = Ballot::none();
        let real = Ballot::with_timestamp(1, node_a());
        assert!(none < real);
        assert!(none.is_none());
        assert!(!real.is_none());
    }

    #[test]
    fn newer_than_supersedes() {
        let old = Ballot::with_timestamp(100, node_a());
        let new = Ballot::newer_than(&old, node_b());
        assert!(new > old);
    }

    #[test]
    fn equality() {
        let b1 = Ballot::with_timestamp(42, node_a());
        let b2 = Ballot::with_timestamp(42, node_a());
        assert_eq!(b1, b2);
    }

    #[test]
    fn display_format() {
        let b = Ballot::with_timestamp(12345, node_a());
        let s = format!("{}", b);
        assert!(s.contains("12345"));
    }
}
