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

//! Scoped range locking for topology operations.
//!
//! Extends the basic [`LockedRanges`] model with per-keyspace tracking and
//! configurable lock scopes (read, write, exclusive) so that concurrent
//! topology operations can proceed when their ranges and access modes do
//! not conflict.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.tcm.sequences.LockedRanges`
//! - `org.apache.cassandra.tcm.sequences.LockedRanges.AffectedRanges`

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use cassandra_common::token::TokenRange;

use crate::node::NodeId;

// ─────────────────────────────────────────────────────────────────────────────
// LockScope
// ─────────────────────────────────────────────────────────────────────────────

/// Access mode for a range lock.
///
/// Determines which combinations of concurrent locks are permitted on
/// overlapping token ranges.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LockScope {
    /// Shared read access. Multiple reads on the same range are allowed.
    Read,
    /// Write access. Conflicts with other writes and reads.
    Write,
    /// Exclusive access. Conflicts with everything.
    Exclusive,
}

impl LockScope {
    /// Returns `true` when two scopes conflict (i.e. cannot coexist on
    /// overlapping ranges).
    ///
    /// Only `Read`/`Read` is conflict-free; every other combination conflicts.
    pub fn conflicts_with(&self, other: &LockScope) -> bool {
        !matches!((self, other), (LockScope::Read, LockScope::Read))
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// LockRequest
// ─────────────────────────────────────────────────────────────────────────────

/// A request to acquire range locks within a single keyspace.
#[derive(Debug, Clone)]
pub struct LockRequest {
    /// Unique identifier for the operation requesting the lock.
    pub operation_id: Uuid,
    /// The keyspace whose token ranges are being locked.
    pub keyspace: String,
    /// Token ranges to lock.
    pub ranges: Vec<TokenRange>,
    /// Desired access mode.
    pub scope: LockScope,
    /// The node that originated this request.
    pub requested_by: NodeId,
}

// ─────────────────────────────────────────────────────────────────────────────
// LockResult
// ─────────────────────────────────────────────────────────────────────────────

/// Outcome of a [`LockRequest`].
#[derive(Debug, Clone)]
pub enum LockResult {
    /// The lock was successfully granted.
    Granted,
    /// The lock was denied due to a conflict with an existing operation.
    Denied {
        reason: String,
        conflicting_operation: Uuid,
    },
}

// ─────────────────────────────────────────────────────────────────────────────
// LockError
// ─────────────────────────────────────────────────────────────────────────────

/// Errors from lock management operations.
#[derive(Debug, thiserror::Error)]
pub enum LockError {
    #[error("Operation {0} not found")]
    OperationNotFound(Uuid),

    #[error("Keyspace '{0}' not found")]
    KeyspaceNotFound(String),

    #[error("Conflict detected: operation {operation} on range {range}")]
    ConflictDetected {
        operation: Uuid,
        range: TokenRange,
    },
}

// ─────────────────────────────────────────────────────────────────────────────
// AffectedRanges
// ─────────────────────────────────────────────────────────────────────────────

/// Per-keyspace collection of token ranges affected by an operation.
#[derive(Debug, Clone, Default)]
pub struct AffectedRanges {
    ranges: HashMap<String, Vec<TokenRange>>,
}

impl AffectedRanges {
    /// Create an empty collection.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add affected ranges for a keyspace.
    pub fn add(&mut self, keyspace: String, ranges: Vec<TokenRange>) {
        self.ranges.entry(keyspace).or_default().extend(ranges);
    }

    /// Get affected ranges for a keyspace.
    pub fn get(&self, keyspace: &str) -> Option<&Vec<TokenRange>> {
        self.ranges.get(keyspace)
    }

    /// Check whether any affected range for the given keyspace overlaps
    /// with the provided range.
    ///
    /// Uses simplified overlap check for non-wrapping ranges:
    /// `a.start < b.end && b.start < a.end`.
    pub fn intersects(&self, keyspace: &str, range: &TokenRange) -> bool {
        if let Some(ranges) = self.ranges.get(keyspace) {
            for r in ranges {
                if ranges_overlap(r, range) {
                    return true;
                }
            }
        }
        false
    }

    /// All keyspaces that have affected ranges.
    pub fn all_keyspaces(&self) -> Vec<&String> {
        self.ranges.keys().collect()
    }

    /// Whether no keyspaces have been added.
    pub fn is_empty(&self) -> bool {
        self.ranges.is_empty()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// EnhancedLockedRanges
// ─────────────────────────────────────────────────────────────────────────────

/// Extended range-locking registry with per-keyspace tracking and scoped
/// conflict detection.
///
/// Unlike [`LockedRanges`](crate::tcm::LockedRanges) which only tracks
/// operation → ranges, this struct additionally indexes locks per-keyspace
/// and considers [`LockScope`] when deciding whether two overlapping locks
/// conflict.
#[derive(Debug, Clone, Default)]
pub struct EnhancedLockedRanges {
    /// operation_id → (original request, scope).
    locks: HashMap<Uuid, (LockRequest, LockScope)>,
    /// keyspace → list of (operation_id, range, scope) entries.
    per_keyspace: HashMap<String, Vec<(Uuid, TokenRange, LockScope)>>,
}

impl EnhancedLockedRanges {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Attempt to acquire a lock. Returns [`LockResult::Granted`] if no
    /// conflicting lock exists, or [`LockResult::Denied`] otherwise.
    pub fn try_lock(&mut self, request: LockRequest) -> LockResult {
        // Check for conflicts against existing locks in the same keyspace.
        if let Some(existing) = self.per_keyspace.get(&request.keyspace) {
            for (existing_op, existing_range, existing_scope) in existing {
                if !request.scope.conflicts_with(existing_scope) {
                    continue;
                }
                for req_range in &request.ranges {
                    if ranges_overlap(existing_range, req_range) {
                        return LockResult::Denied {
                            reason: format!(
                                "Range {} conflicts with existing lock on {} (scope {:?} vs {:?})",
                                req_range, existing_range, request.scope, existing_scope,
                            ),
                            conflicting_operation: *existing_op,
                        };
                    }
                }
            }
        }

        // No conflicts — grant and store.
        let op_id = request.operation_id;
        let scope = request.scope;
        let keyspace = request.keyspace.clone();

        let ks_entry = self.per_keyspace.entry(keyspace).or_default();
        for range in &request.ranges {
            ks_entry.push((op_id, *range, scope));
        }

        self.locks.insert(op_id, (request, scope));

        LockResult::Granted
    }

    /// Release all locks held by the given operation.
    pub fn unlock(&mut self, operation_id: &Uuid) -> Result<(), LockError> {
        let (request, _scope) = self
            .locks
            .remove(operation_id)
            .ok_or(LockError::OperationNotFound(*operation_id))?;

        if let Some(ks_locks) = self.per_keyspace.get_mut(&request.keyspace) {
            ks_locks.retain(|(op, _, _)| op != operation_id);
            if ks_locks.is_empty() {
                self.per_keyspace.remove(&request.keyspace);
            }
        }

        Ok(())
    }

    /// Check whether any lock is held on the given range in the specified
    /// keyspace (regardless of scope).
    pub fn is_locked(&self, keyspace: &str, range: &TokenRange) -> bool {
        if let Some(locks) = self.per_keyspace.get(keyspace) {
            for (_, locked_range, _) in locks {
                if ranges_overlap(locked_range, range) {
                    return true;
                }
            }
        }
        false
    }

    /// All lock entries for a given keyspace.
    pub fn locks_for_keyspace(&self, keyspace: &str) -> Vec<&(Uuid, TokenRange, LockScope)> {
        match self.per_keyspace.get(keyspace) {
            Some(locks) => locks.iter().collect(),
            None => Vec::new(),
        }
    }

    /// Total number of active lock operations.
    pub fn lock_count(&self) -> usize {
        self.locks.len()
    }

    /// Whether any locks are currently held.
    pub fn has_locks(&self) -> bool {
        !self.locks.is_empty()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Check if two non-wrapping token ranges overlap.
fn ranges_overlap(a: &TokenRange, b: &TokenRange) -> bool {
    if a.wraps_around() || b.wraps_around() {
        // Conservative: assume overlap for wrapping ranges.
        return true;
    }
    a.start < b.end && b.start < a.end
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use cassandra_common::token::Token;

    fn node_id(n: u128) -> NodeId {
        NodeId::from_uuid(Uuid::from_u128(n))
    }

    fn make_request(
        op: u128,
        keyspace: &str,
        start: i64,
        end: i64,
        scope: LockScope,
    ) -> LockRequest {
        LockRequest {
            operation_id: Uuid::from_u128(op),
            keyspace: keyspace.to_string(),
            ranges: vec![TokenRange::new(Token::from_raw(start), Token::from_raw(end))],
            scope,
            requested_by: node_id(1),
        }
    }

    // ── LockScope conflicts ──────────────────────────────────────────────

    #[test]
    fn scope_read_read_no_conflict() {
        assert!(!LockScope::Read.conflicts_with(&LockScope::Read));
    }

    #[test]
    fn scope_read_write_conflicts() {
        assert!(LockScope::Read.conflicts_with(&LockScope::Write));
        assert!(LockScope::Write.conflicts_with(&LockScope::Read));
    }

    #[test]
    fn scope_read_exclusive_conflicts() {
        assert!(LockScope::Read.conflicts_with(&LockScope::Exclusive));
        assert!(LockScope::Exclusive.conflicts_with(&LockScope::Read));
    }

    #[test]
    fn scope_write_write_conflicts() {
        assert!(LockScope::Write.conflicts_with(&LockScope::Write));
    }

    #[test]
    fn scope_write_exclusive_conflicts() {
        assert!(LockScope::Write.conflicts_with(&LockScope::Exclusive));
        assert!(LockScope::Exclusive.conflicts_with(&LockScope::Write));
    }

    #[test]
    fn scope_exclusive_exclusive_conflicts() {
        assert!(LockScope::Exclusive.conflicts_with(&LockScope::Exclusive));
    }

    // ── Per-keyspace tracking ────────────────────────────────────────────

    #[test]
    fn different_keyspaces_do_not_conflict() {
        let mut lr = EnhancedLockedRanges::new();

        let req1 = make_request(1, "ks1", 0, 100, LockScope::Write);
        assert!(matches!(lr.try_lock(req1), LockResult::Granted));

        // Same range, different keyspace — should succeed.
        let req2 = make_request(2, "ks2", 0, 100, LockScope::Write);
        assert!(matches!(lr.try_lock(req2), LockResult::Granted));

        assert_eq!(lr.lock_count(), 2);
    }

    #[test]
    fn same_keyspace_overlapping_write_denied() {
        let mut lr = EnhancedLockedRanges::new();

        let req1 = make_request(1, "ks1", 0, 100, LockScope::Write);
        assert!(matches!(lr.try_lock(req1), LockResult::Granted));

        let req2 = make_request(2, "ks1", 50, 150, LockScope::Write);
        match lr.try_lock(req2) {
            LockResult::Denied { conflicting_operation, .. } => {
                assert_eq!(conflicting_operation, Uuid::from_u128(1));
            }
            LockResult::Granted => panic!("expected denial"),
        }
    }

    #[test]
    fn same_keyspace_overlapping_read_granted() {
        let mut lr = EnhancedLockedRanges::new();

        let req1 = make_request(1, "ks1", 0, 100, LockScope::Read);
        assert!(matches!(lr.try_lock(req1), LockResult::Granted));

        let req2 = make_request(2, "ks1", 50, 150, LockScope::Read);
        assert!(matches!(lr.try_lock(req2), LockResult::Granted));
    }

    #[test]
    fn exclusive_blocks_everything() {
        let mut lr = EnhancedLockedRanges::new();

        let req1 = make_request(1, "ks1", 0, 100, LockScope::Exclusive);
        assert!(matches!(lr.try_lock(req1), LockResult::Granted));

        // Read on overlapping range denied.
        let req2 = make_request(2, "ks1", 50, 150, LockScope::Read);
        assert!(matches!(lr.try_lock(req2), LockResult::Denied { .. }));

        // Write on overlapping range denied.
        let req3 = make_request(3, "ks1", 50, 150, LockScope::Write);
        assert!(matches!(lr.try_lock(req3), LockResult::Denied { .. }));
    }

    // ── AffectedRanges ──────────────────────────────────────────────────

    #[test]
    fn affected_ranges_intersection() {
        let mut ar = AffectedRanges::new();
        ar.add(
            "ks1".to_string(),
            vec![TokenRange::new(Token::from_raw(10), Token::from_raw(50))],
        );

        // Overlapping.
        assert!(ar.intersects("ks1", &TokenRange::new(Token::from_raw(30), Token::from_raw(60))));

        // Disjoint.
        assert!(!ar.intersects(
            "ks1",
            &TokenRange::new(Token::from_raw(60), Token::from_raw(80))
        ));

        // Unknown keyspace.
        assert!(!ar.intersects(
            "ks2",
            &TokenRange::new(Token::from_raw(30), Token::from_raw(60))
        ));
    }

    #[test]
    fn affected_ranges_all_keyspaces_and_empty() {
        let mut ar = AffectedRanges::new();
        assert!(ar.is_empty());

        ar.add("ks1".to_string(), vec![]);
        ar.add("ks2".to_string(), vec![]);

        assert!(!ar.is_empty());

        let mut ks: Vec<&str> = ar.all_keyspaces().iter().map(|s| s.as_str()).collect();
        ks.sort();
        assert_eq!(ks, vec!["ks1", "ks2"]);
    }

    // ── Lock and unlock flow ─────────────────────────────────────────────

    #[test]
    fn lock_then_unlock_allows_relock() {
        let mut lr = EnhancedLockedRanges::new();

        let req1 = make_request(1, "ks1", 0, 100, LockScope::Write);
        assert!(matches!(lr.try_lock(req1), LockResult::Granted));
        assert!(lr.has_locks());
        assert_eq!(lr.lock_count(), 1);

        // Unlock.
        lr.unlock(&Uuid::from_u128(1)).unwrap();
        assert!(!lr.has_locks());
        assert_eq!(lr.lock_count(), 0);

        // Same range can now be locked again.
        let req2 = make_request(2, "ks1", 0, 100, LockScope::Write);
        assert!(matches!(lr.try_lock(req2), LockResult::Granted));
    }

    #[test]
    fn unlock_unknown_operation_returns_error() {
        let mut lr = EnhancedLockedRanges::new();
        let result = lr.unlock(&Uuid::from_u128(999));
        assert!(result.is_err());
    }

    #[test]
    fn is_locked_checks_keyspace() {
        let mut lr = EnhancedLockedRanges::new();
        let range = TokenRange::new(Token::from_raw(0), Token::from_raw(100));

        let req = make_request(1, "ks1", 0, 100, LockScope::Read);
        lr.try_lock(req);

        assert!(lr.is_locked("ks1", &range));
        assert!(!lr.is_locked("ks2", &range));
    }

    #[test]
    fn locks_for_keyspace_returns_entries() {
        let mut lr = EnhancedLockedRanges::new();

        let req = make_request(1, "ks1", 0, 100, LockScope::Write);
        lr.try_lock(req);

        let entries = lr.locks_for_keyspace("ks1");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].0, Uuid::from_u128(1));

        assert!(lr.locks_for_keyspace("ks2").is_empty());
    }
}
