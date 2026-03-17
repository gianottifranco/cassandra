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

//! Persistent log storage abstraction for TCM metadata entries.
//!
//! Provides a storage-level [`Entry`] type, an in-memory [`LogState`] buffer,
//! and a [`LogStorage`] trait for pluggable backends (in-memory, disk, etc.).
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.tcm.log.LogState`
//! - `org.apache.cassandra.tcm.log.Entry`
//! - `org.apache.cassandra.tcm.log.LogStorage`
//! - `org.apache.cassandra.tcm.log.LocalLog`

use std::collections::BTreeMap;
use std::sync::Mutex;

use serde::{Deserialize, Serialize};

use crate::node::NodeId;
use crate::tcm::{Epoch, SealedPeriod, TcmSnapshot, Transformation};

// ─────────────────────────────────────────────────────────────────────────────
// Entry
// ─────────────────────────────────────────────────────────────────────────────

/// A storage-level log entry carrying a unique id, epoch, transformation,
/// and the node that committed it.
///
/// Distinct from [`MetadataLogEntry`](super::MetadataLogEntry) which is the
/// in-memory replay representation; `Entry` adds a storage-assigned `id`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Entry {
    /// Storage-assigned unique identifier.
    pub id: u64,
    /// The epoch this entry was committed at.
    pub epoch: Epoch,
    /// The transformation applied.
    pub transformation: Transformation,
    /// The node that committed this entry.
    pub committed_by: NodeId,
}

// ─────────────────────────────────────────────────────────────────────────────
// LogState
// ─────────────────────────────────────────────────────────────────────────────

/// In-memory ordered buffer of log entries with a base epoch.
///
/// `base_epoch` represents the epoch of the last snapshot (or `Epoch::EMPTY`
/// if no snapshot has been taken). Only entries *after* `base_epoch` are
/// kept in the buffer.
#[derive(Debug, Clone)]
pub struct LogState {
    /// Epoch of the last snapshot that precedes these entries.
    base_epoch: Epoch,
    /// Entries ordered by epoch.
    entries: Vec<Entry>,
}

impl LogState {
    /// Create a new empty `LogState` starting from the given base epoch.
    pub fn new(base_epoch: Epoch) -> Self {
        Self {
            base_epoch,
            entries: Vec::new(),
        }
    }

    /// Whether the buffer contains no entries.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Number of entries in the buffer.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Append an entry to the buffer.
    pub fn append(&mut self, entry: Entry) {
        self.entries.push(entry);
    }

    /// Return a slice of entries whose epoch is strictly greater than `epoch`.
    pub fn entries_since(&self, epoch: Epoch) -> &[Entry] {
        let start = self.entries.partition_point(|e| e.epoch <= epoch);
        &self.entries[start..]
    }

    /// The latest epoch in the buffer, or `base_epoch` if empty.
    pub fn latest_epoch(&self) -> Epoch {
        self.entries
            .last()
            .map(|e| e.epoch)
            .unwrap_or(self.base_epoch)
    }

    /// The base epoch of this log state.
    pub fn base_epoch(&self) -> Epoch {
        self.base_epoch
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// LogStorageError
// ─────────────────────────────────────────────────────────────────────────────

/// Errors produced by [`LogStorage`] implementations.
#[derive(Debug, thiserror::Error)]
pub enum LogStorageError {
    /// An entry with the given epoch already exists.
    #[error("duplicate entry at {0}")]
    DuplicateEntry(Epoch),

    /// The requested epoch was not found in the log.
    #[error("epoch not found: {0}")]
    EpochNotFound(Epoch),

    /// No snapshot exists at the requested epoch.
    #[error("snapshot not found at {0}")]
    SnapshotNotFound(Epoch),

    /// A generic storage backend failure.
    #[error("storage failure: {0}")]
    StorageFailure(String),
}

// ─────────────────────────────────────────────────────────────────────────────
// LogStorage trait
// ─────────────────────────────────────────────────────────────────────────────

/// Pluggable backend for persisting TCM log entries and snapshots.
pub trait LogStorage: Send + Sync {
    /// Persist a single entry. Returns `DuplicateEntry` if the epoch
    /// already exists.
    fn append(&self, entry: Entry) -> Result<(), LogStorageError>;

    /// Retrieve all entries whose epoch is strictly greater than `epoch`.
    fn entries_since(&self, epoch: Epoch) -> Result<Vec<Entry>, LogStorageError>;

    /// The latest epoch currently stored, or `Epoch::EMPTY` if empty.
    fn latest_epoch(&self) -> Epoch;

    /// Persist a snapshot at its embedded epoch.
    fn store_snapshot(&self, snapshot: TcmSnapshot) -> Result<(), LogStorageError>;

    /// Retrieve the snapshot at exactly the given epoch, if any.
    fn get_snapshot(&self, epoch: Epoch) -> Result<Option<TcmSnapshot>, LogStorageError>;

    /// Retrieve the most recent snapshot, if any.
    fn get_latest_snapshot(&self) -> Result<Option<TcmSnapshot>, LogStorageError>;

    /// Seal a period of the log up to the given epoch.
    fn seal_period(
        &self,
        epoch: Epoch,
        entry_count: usize,
    ) -> Result<SealedPeriod, LogStorageError>;

    /// Return all sealed periods, ordered by epoch.
    fn sealed_periods(&self) -> Result<Vec<SealedPeriod>, LogStorageError>;
}

// ─────────────────────────────────────────────────────────────────────────────
// InMemoryLogStorage
// ─────────────────────────────────────────────────────────────────────────────

/// Internal mutable state for [`InMemoryLogStorage`].
struct InMemoryState {
    entries: Vec<Entry>,
    snapshots: BTreeMap<Epoch, TcmSnapshot>,
    sealed_periods: Vec<SealedPeriod>,
}

/// A fully in-memory [`LogStorage`] implementation backed by a `Mutex`.
///
/// Suitable for tests and lightweight single-node setups.
pub struct InMemoryLogStorage {
    state: Mutex<InMemoryState>,
}

impl InMemoryLogStorage {
    /// Create a new empty in-memory log storage.
    pub fn new() -> Self {
        Self {
            state: Mutex::new(InMemoryState {
                entries: Vec::new(),
                snapshots: BTreeMap::new(),
                sealed_periods: Vec::new(),
            }),
        }
    }
}

impl Default for InMemoryLogStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl LogStorage for InMemoryLogStorage {
    fn append(&self, entry: Entry) -> Result<(), LogStorageError> {
        let mut state = self
            .state
            .lock()
            .map_err(|e| LogStorageError::StorageFailure(e.to_string()))?;

        if state.entries.iter().any(|e| e.epoch == entry.epoch) {
            return Err(LogStorageError::DuplicateEntry(entry.epoch));
        }

        state.entries.push(entry);
        Ok(())
    }

    fn entries_since(&self, epoch: Epoch) -> Result<Vec<Entry>, LogStorageError> {
        let state = self
            .state
            .lock()
            .map_err(|e| LogStorageError::StorageFailure(e.to_string()))?;

        let start = state.entries.partition_point(|e| e.epoch <= epoch);
        Ok(state.entries[start..].to_vec())
    }

    fn latest_epoch(&self) -> Epoch {
        self.state
            .lock()
            .ok()
            .and_then(|s| s.entries.last().map(|e| e.epoch))
            .unwrap_or(Epoch::EMPTY)
    }

    fn store_snapshot(&self, snapshot: TcmSnapshot) -> Result<(), LogStorageError> {
        let mut state = self
            .state
            .lock()
            .map_err(|e| LogStorageError::StorageFailure(e.to_string()))?;

        state.snapshots.insert(snapshot.epoch, snapshot);
        Ok(())
    }

    fn get_snapshot(&self, epoch: Epoch) -> Result<Option<TcmSnapshot>, LogStorageError> {
        let state = self
            .state
            .lock()
            .map_err(|e| LogStorageError::StorageFailure(e.to_string()))?;

        Ok(state.snapshots.get(&epoch).cloned())
    }

    fn get_latest_snapshot(&self) -> Result<Option<TcmSnapshot>, LogStorageError> {
        let state = self
            .state
            .lock()
            .map_err(|e| LogStorageError::StorageFailure(e.to_string()))?;

        Ok(state.snapshots.values().last().cloned())
    }

    fn seal_period(
        &self,
        epoch: Epoch,
        entry_count: usize,
    ) -> Result<SealedPeriod, LogStorageError> {
        let mut state = self
            .state
            .lock()
            .map_err(|e| LogStorageError::StorageFailure(e.to_string()))?;

        let period = SealedPeriod::seal(epoch, entry_count);
        state.sealed_periods.push(period.clone());
        Ok(period)
    }

    fn sealed_periods(&self) -> Result<Vec<SealedPeriod>, LogStorageError> {
        let state = self
            .state
            .lock()
            .map_err(|e| LogStorageError::StorageFailure(e.to_string()))?;

        Ok(state.sealed_periods.clone())
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    fn node_id(n: u128) -> NodeId {
        NodeId::from_uuid(Uuid::from_u128(n))
    }

    fn make_entry(id: u64, epoch: u64) -> Entry {
        Entry {
            id,
            epoch: Epoch(epoch),
            transformation: Transformation::ForceSnapshot,
            committed_by: node_id(1),
        }
    }

    // ── LogState tests ──────────────────────────────────────────────────

    #[test]
    fn log_state_empty() {
        let state = LogState::new(Epoch::EMPTY);
        assert!(state.is_empty());
        assert_eq!(state.len(), 0);
        assert_eq!(state.latest_epoch(), Epoch::EMPTY);
    }

    #[test]
    fn log_state_append_and_len() {
        let mut state = LogState::new(Epoch::EMPTY);
        state.append(make_entry(1, 1));
        state.append(make_entry(2, 2));
        state.append(make_entry(3, 3));

        assert!(!state.is_empty());
        assert_eq!(state.len(), 3);
        assert_eq!(state.latest_epoch(), Epoch(3));
    }

    #[test]
    fn log_state_entries_since() {
        let mut state = LogState::new(Epoch::EMPTY);
        for i in 1..=5 {
            state.append(make_entry(i, i));
        }

        // All entries after epoch 0
        assert_eq!(state.entries_since(Epoch::EMPTY).len(), 5);
        // Entries after epoch 3 → epochs 4, 5
        assert_eq!(state.entries_since(Epoch(3)).len(), 2);
        assert_eq!(state.entries_since(Epoch(3))[0].epoch, Epoch(4));
        // Entries after epoch 5 → none
        assert_eq!(state.entries_since(Epoch(5)).len(), 0);
        // Entries after epoch 0 using FIRST → epochs 2..5
        assert_eq!(state.entries_since(Epoch::FIRST).len(), 4);
    }

    #[test]
    fn log_state_base_epoch() {
        let state = LogState::new(Epoch(10));
        assert_eq!(state.base_epoch(), Epoch(10));
        assert_eq!(state.latest_epoch(), Epoch(10));
    }

    // ── InMemoryLogStorage tests ────────────────────────────────────────

    #[test]
    fn storage_append_and_retrieve() {
        let storage = InMemoryLogStorage::new();

        storage.append(make_entry(1, 1)).unwrap();
        storage.append(make_entry(2, 2)).unwrap();
        storage.append(make_entry(3, 3)).unwrap();

        assert_eq!(storage.latest_epoch(), Epoch(3));

        let entries = storage.entries_since(Epoch::EMPTY).unwrap();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].epoch, Epoch(1));
        assert_eq!(entries[2].epoch, Epoch(3));
    }

    #[test]
    fn storage_entries_since_filtering() {
        let storage = InMemoryLogStorage::new();
        for i in 1..=5 {
            storage.append(make_entry(i, i)).unwrap();
        }

        let entries = storage.entries_since(Epoch(3)).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].epoch, Epoch(4));
        assert_eq!(entries[1].epoch, Epoch(5));
    }

    #[test]
    fn storage_duplicate_entry_error() {
        let storage = InMemoryLogStorage::new();

        storage.append(make_entry(1, 1)).unwrap();
        let result = storage.append(make_entry(2, 1));

        assert!(result.is_err());
        assert!(
            matches!(result.unwrap_err(), LogStorageError::DuplicateEntry(e) if e == Epoch(1))
        );
    }

    #[test]
    fn storage_snapshot_store_and_get() {
        let storage = InMemoryLogStorage::new();

        let snapshot = TcmSnapshot {
            epoch: Epoch(5),
            nodes: Vec::new(),
            schema_version: None,
            in_progress: Vec::new(),
            created_at_millis: 1234567890,
        };

        storage.store_snapshot(snapshot.clone()).unwrap();

        let retrieved = storage.get_snapshot(Epoch(5)).unwrap();
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().epoch, Epoch(5));

        // Non-existent snapshot returns None
        let missing = storage.get_snapshot(Epoch(99)).unwrap();
        assert!(missing.is_none());
    }

    #[test]
    fn storage_get_latest_snapshot() {
        let storage = InMemoryLogStorage::new();

        // No snapshots yet
        assert!(storage.get_latest_snapshot().unwrap().is_none());

        let snap1 = TcmSnapshot {
            epoch: Epoch(3),
            nodes: Vec::new(),
            schema_version: None,
            in_progress: Vec::new(),
            created_at_millis: 100,
        };
        let snap2 = TcmSnapshot {
            epoch: Epoch(7),
            nodes: Vec::new(),
            schema_version: None,
            in_progress: Vec::new(),
            created_at_millis: 200,
        };

        storage.store_snapshot(snap1).unwrap();
        storage.store_snapshot(snap2).unwrap();

        let latest = storage.get_latest_snapshot().unwrap().unwrap();
        assert_eq!(latest.epoch, Epoch(7));
    }

    #[test]
    fn storage_sealed_periods() {
        let storage = InMemoryLogStorage::new();

        assert!(storage.sealed_periods().unwrap().is_empty());

        let p1 = storage.seal_period(Epoch(5), 5).unwrap();
        assert_eq!(p1.epoch, Epoch(5));
        assert_eq!(p1.entry_count, 5);

        let p2 = storage.seal_period(Epoch(10), 5).unwrap();
        assert_eq!(p2.epoch, Epoch(10));

        let periods = storage.sealed_periods().unwrap();
        assert_eq!(periods.len(), 2);
        assert_eq!(periods[0].epoch, Epoch(5));
        assert_eq!(periods[1].epoch, Epoch(10));
    }

    #[test]
    fn storage_latest_epoch_empty() {
        let storage = InMemoryLogStorage::new();
        assert_eq!(storage.latest_epoch(), Epoch::EMPTY);
    }
}
