// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.
// SPDX-License-Identifier: Apache-2.0

//! Batch log: distributed batch safety.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.batchlog.BatchlogManager`
//! - `org.apache.cassandra.batchlog.Batch`
//!
//! The batch log ensures atomicity of batches by writing a log entry
//! to replicas before executing the mutations, and removing it after
//! all mutations succeed.

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;
use uuid::Uuid;

use crate::write::CoordinatedMutation;

/// A batch log entry.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct BatchEntry {
    /// Unique batch ID.
    pub id: Uuid,
    /// All mutations in this batch.
    pub mutations: Vec<CoordinatedMutation>,
    /// Creation timestamp (epoch millis).
    pub created_at: i64,
}

/// In-memory batch log manager.
///
/// Stores pending batch entries. In production, these would be replicated
/// to other nodes for durability.
pub struct BatchLogManager {
    entries: Arc<RwLock<HashMap<Uuid, BatchEntry>>>,
}

impl BatchLogManager {
    pub fn new() -> Self {
        Self {
            entries: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Store a new batch entry before executing mutations.
    pub fn store(&self, mutations: Vec<CoordinatedMutation>) -> Uuid {
        let id = Uuid::new_v4();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;

        let entry = BatchEntry {
            id,
            mutations,
            created_at: now,
        };

        self.entries.write().insert(id, entry);
        id
    }

    /// Remove a batch entry after all mutations succeed.
    pub fn remove(&self, id: &Uuid) -> Option<BatchEntry> {
        self.entries.write().remove(id)
    }

    /// Get a pending batch entry.
    pub fn get(&self, id: &Uuid) -> Option<BatchEntry> {
        self.entries.read().get(id).cloned()
    }

    /// Get all pending batch entries (for replay on recovery).
    pub fn pending_entries(&self) -> Vec<BatchEntry> {
        self.entries.read().values().cloned().collect()
    }

    /// Number of pending batch entries.
    pub fn pending_count(&self) -> usize {
        self.entries.read().len()
    }
}

impl Default for BatchLogManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn store_and_remove() {
        let blm = BatchLogManager::new();

        let id = blm.store(vec![test_mutation()]);
        assert_eq!(blm.pending_count(), 1);

        let entry = blm.remove(&id).unwrap();
        assert_eq!(entry.id, id);
        assert_eq!(blm.pending_count(), 0);
    }

    #[test]
    fn get_entry() {
        let blm = BatchLogManager::new();
        let id = blm.store(vec![test_mutation(), test_mutation()]);

        let entry = blm.get(&id).unwrap();
        assert_eq!(entry.mutations.len(), 2);
    }

    #[test]
    fn pending_entries() {
        let blm = BatchLogManager::new();
        blm.store(vec![test_mutation()]);
        blm.store(vec![test_mutation()]);

        assert_eq!(blm.pending_entries().len(), 2);
    }
}
