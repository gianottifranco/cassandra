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

//! TTL-based cleanup for tracing sessions.
//!
//! Expired tracing sessions are removed by [`TracingCleanupTask`] which
//! delegates to an [`ExpirableSessionStore`] for the actual eviction.
//! [`InMemorySessionStore`] provides a bounded, timestamp-ordered store
//! backed by a `VecDeque` under a `parking_lot::Mutex`.

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::SystemTime;

use parking_lot::Mutex;
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Trait
// ---------------------------------------------------------------------------

/// A store of tracing sessions that supports removing entries older than a
/// given cutoff timestamp.
pub trait ExpirableSessionStore: Send + Sync {
    /// Remove all sessions with `finished_at_ms < cutoff_ms`.
    /// Returns the number of sessions removed.
    fn remove_expired(&self, cutoff_ms: u64) -> usize;
}

// ---------------------------------------------------------------------------
// TimestampedEntry
// ---------------------------------------------------------------------------

/// A tracing session together with the timestamp (epoch millis) at which it
/// finished.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimestampedEntry {
    pub finished_at_ms: u64,
    pub session_id: Uuid,
}

// ---------------------------------------------------------------------------
// InMemorySessionStore
// ---------------------------------------------------------------------------

/// Bounded, in-memory store of [`TimestampedEntry`] values.
///
/// Entries are kept in insertion order inside a `VecDeque`.  When the store
/// reaches `capacity`, the oldest entry is evicted on the next `add`.
pub struct InMemorySessionStore {
    entries: Mutex<VecDeque<TimestampedEntry>>,
    capacity: usize,
}

impl InMemorySessionStore {
    /// Create a new store that holds at most `capacity` entries.
    pub fn new(capacity: usize) -> Self {
        Self {
            entries: Mutex::new(VecDeque::with_capacity(capacity)),
            capacity,
        }
    }

    /// Record a finished tracing session.  If the store is already at
    /// capacity the oldest entry is evicted first.
    pub fn add(&self, session_id: Uuid, finished_at_ms: u64) {
        let mut entries = self.entries.lock();
        if entries.len() >= self.capacity {
            entries.pop_front();
        }
        entries.push_back(TimestampedEntry {
            finished_at_ms,
            session_id,
        });
    }

    /// Number of entries currently stored.
    pub fn len(&self) -> usize {
        self.entries.lock().len()
    }

    /// Returns `true` when the store contains no entries.
    pub fn is_empty(&self) -> bool {
        self.entries.lock().is_empty()
    }
}

impl ExpirableSessionStore for InMemorySessionStore {
    fn remove_expired(&self, cutoff_ms: u64) -> usize {
        let mut entries = self.entries.lock();
        let before = entries.len();
        entries.retain(|e| e.finished_at_ms >= cutoff_ms);
        before - entries.len()
    }
}

// ---------------------------------------------------------------------------
// TracingCleanupTask
// ---------------------------------------------------------------------------

/// Runs a single cleanup pass against an [`ExpirableSessionStore`],
/// removing sessions whose finish timestamp is older than `ttl_ms`
/// milliseconds from the current wall-clock time.
pub struct TracingCleanupTask {
    store: Arc<dyn ExpirableSessionStore>,
    ttl_ms: u64,
}

impl TracingCleanupTask {
    pub fn new(store: Arc<dyn ExpirableSessionStore>, ttl_ms: u64) -> Self {
        Self { store, ttl_ms }
    }

    /// Execute one cleanup pass.  Returns the number of sessions removed.
    pub fn run_once(&self) -> usize {
        let now_ms = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let cutoff = now_ms.saturating_sub(self.ttl_ms);
        self.store.remove_expired(cutoff)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_uuid(n: u128) -> Uuid {
        Uuid::from_u128(n)
    }

    // -- InMemorySessionStore basic ------------------------------------------

    #[test]
    fn test_new_store_is_empty() {
        let store = InMemorySessionStore::new(10);
        assert!(store.is_empty());
        assert_eq!(store.len(), 0);
    }

    #[test]
    fn test_add_increases_len() {
        let store = InMemorySessionStore::new(10);
        store.add(make_uuid(1), 100);
        assert_eq!(store.len(), 1);
        assert!(!store.is_empty());
    }

    #[test]
    fn test_add_multiple_entries() {
        let store = InMemorySessionStore::new(10);
        for i in 0..5 {
            store.add(make_uuid(i), i as u64 * 100);
        }
        assert_eq!(store.len(), 5);
    }

    // -- capacity eviction ---------------------------------------------------

    #[test]
    fn test_evicts_oldest_when_at_capacity() {
        let store = InMemorySessionStore::new(3);
        store.add(make_uuid(1), 100);
        store.add(make_uuid(2), 200);
        store.add(make_uuid(3), 300);
        assert_eq!(store.len(), 3);

        // Adding a 4th should evict uuid(1)
        store.add(make_uuid(4), 400);
        assert_eq!(store.len(), 3);

        // The oldest remaining entry should be uuid(2) at 200ms
        let entries = store.entries.lock();
        assert_eq!(entries.front().unwrap().session_id, make_uuid(2));
        assert_eq!(entries.back().unwrap().session_id, make_uuid(4));
    }

    #[test]
    fn test_capacity_one() {
        let store = InMemorySessionStore::new(1);
        store.add(make_uuid(1), 100);
        store.add(make_uuid(2), 200);
        assert_eq!(store.len(), 1);
        let entries = store.entries.lock();
        assert_eq!(entries[0].session_id, make_uuid(2));
    }

    // -- remove_expired ------------------------------------------------------

    #[test]
    fn test_remove_expired_removes_old_entries() {
        let store = InMemorySessionStore::new(10);
        store.add(make_uuid(1), 100);
        store.add(make_uuid(2), 200);
        store.add(make_uuid(3), 300);

        let removed = store.remove_expired(200);
        assert_eq!(removed, 1); // uuid(1) at 100 < 200
        assert_eq!(store.len(), 2);
    }

    #[test]
    fn test_remove_expired_keeps_entries_at_cutoff() {
        let store = InMemorySessionStore::new(10);
        store.add(make_uuid(1), 200);
        store.add(make_uuid(2), 200);

        let removed = store.remove_expired(200);
        assert_eq!(removed, 0);
        assert_eq!(store.len(), 2);
    }

    #[test]
    fn test_remove_expired_removes_all_when_all_old() {
        let store = InMemorySessionStore::new(10);
        store.add(make_uuid(1), 10);
        store.add(make_uuid(2), 20);

        let removed = store.remove_expired(1000);
        assert_eq!(removed, 2);
        assert!(store.is_empty());
    }

    #[test]
    fn test_remove_expired_noop_on_empty_store() {
        let store = InMemorySessionStore::new(10);
        let removed = store.remove_expired(1000);
        assert_eq!(removed, 0);
    }

    // -- TracingCleanupTask --------------------------------------------------

    #[test]
    fn test_cleanup_task_removes_expired_sessions() {
        let store = Arc::new(InMemorySessionStore::new(100));

        // Add entries far in the past
        store.add(make_uuid(1), 1000);
        store.add(make_uuid(2), 2000);

        // Add an entry at "now" so it survives
        let now_ms = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        store.add(make_uuid(3), now_ms);

        let task = TracingCleanupTask::new(store.clone(), 5_000);
        let removed = task.run_once();

        assert_eq!(removed, 2);
        assert_eq!(store.len(), 1);
    }

    #[test]
    fn test_cleanup_task_zero_ttl_removes_nothing_current() {
        let store = Arc::new(InMemorySessionStore::new(10));
        let now_ms = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        store.add(make_uuid(1), now_ms);

        let task = TracingCleanupTask::new(store.clone(), 0);
        let removed = task.run_once();

        // Entry at now_ms should survive (now - 0 = now, and now >= now)
        assert_eq!(removed, 0);
        assert_eq!(store.len(), 1);
    }

    #[test]
    fn test_cleanup_task_large_ttl_removes_nothing() {
        let store = Arc::new(InMemorySessionStore::new(10));
        store.add(make_uuid(1), 1);
        store.add(make_uuid(2), 2);

        // TTL so large that cutoff saturates to 0
        let task = TracingCleanupTask::new(store.clone(), u64::MAX);
        let removed = task.run_once();

        assert_eq!(removed, 0);
        assert_eq!(store.len(), 2);
    }
}
