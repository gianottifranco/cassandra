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

//! # Secondary Index Subsystem
//!
//! Abstracts secondary indexing with pluggable backends.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.index.Index` — the main index interface
//! - `org.apache.cassandra.index.SecondaryIndexManager`
//! - `org.apache.cassandra.index.internal` — legacy built-in indexes
//! - `org.apache.cassandra.index.sai` — Storage Attached Indexes
//!
//! ## Architecture
//!
//! - [`SecondaryIndex`] trait — pluggable index backend
//! - [`IndexManager`] — registers indexes, routes write notifications
//! - [`legacy`] — legacy key-based secondary index
//! - [`sai`] — Storage Attached Indexing (deep storage integration)

pub mod legacy;
pub mod sai;

use std::fmt;
use serde::{Deserialize, Serialize};

/// Index type classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum IndexType {
    /// Legacy key-based secondary index (inverted index table).
    Legacy,
    /// Storage Attached Index (SAI) — per-SSTable index segments.
    Sai,
}

/// Metadata for a secondary index definition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexDefinition {
    /// Index name (user-specified or auto-generated).
    pub name: String,
    /// Keyspace.
    pub keyspace: String,
    /// Base table.
    pub table: String,
    /// Indexed column name.
    pub column: String,
    /// Index type.
    pub index_type: IndexType,
    /// Custom options (e.g., SAI-specific configuration).
    pub options: std::collections::HashMap<String, String>,
}

/// A secondary index entry — maps an index term to a base-table location.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexEntry {
    /// The indexed value (serialized term).
    pub term: Vec<u8>,
    /// Partition key of the base table row.
    pub partition_key: Vec<u8>,
    /// Clustering key of the base table row (empty for partition-level).
    pub clustering_key: Vec<u8>,
}

/// The secondary index trait — implemented by each index backend.
pub trait SecondaryIndex: Send + Sync + fmt::Debug {
    /// Insert an index entry for a newly written row.
    fn insert(&self, entry: &IndexEntry) -> Result<(), IndexError>;

    /// Delete an index entry for a removed/updated row.
    fn delete(&self, entry: &IndexEntry) -> Result<(), IndexError>;

    /// Search for base-table locations matching the given term.
    ///
    /// Returns `(partition_key, clustering_key)` pairs.
    fn search(&self, term: &[u8]) -> Result<Vec<IndexEntry>, IndexError>;

    /// Search for base-table locations within a range `[start, end]`.
    fn range_search(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
    ) -> Result<Vec<IndexEntry>, IndexError>;

    /// Truncate all index data.
    fn truncate(&self) -> Result<(), IndexError>;

    /// Get the index definition.
    fn definition(&self) -> &IndexDefinition;
}

/// Manages all secondary indexes for a column family.
#[derive(Debug)]
pub struct IndexManager {
    /// Registered indexes by name.
    indexes: Vec<Box<dyn SecondaryIndex>>,
}

impl IndexManager {
    pub fn new() -> Self {
        Self {
            indexes: Vec::new(),
        }
    }

    /// Register a new index.
    pub fn register(&mut self, index: Box<dyn SecondaryIndex>) {
        indexes_push(&mut self.indexes, index);
    }

    /// Notify all relevant indexes about a write.
    pub fn on_write(
        &self,
        partition_key: &[u8],
        clustering_key: &[u8],
        column: &str,
        value: &[u8],
    ) -> Result<(), IndexError> {
        let entry = IndexEntry {
            term: value.to_vec(),
            partition_key: partition_key.to_vec(),
            clustering_key: clustering_key.to_vec(),
        };

        for idx in &self.indexes {
            if idx.definition().column == column {
                idx.insert(&entry)?;
            }
        }
        Ok(())
    }

    /// Notify all relevant indexes about a delete.
    pub fn on_delete(
        &self,
        partition_key: &[u8],
        clustering_key: &[u8],
        column: &str,
        old_value: &[u8],
    ) -> Result<(), IndexError> {
        let entry = IndexEntry {
            term: old_value.to_vec(),
            partition_key: partition_key.to_vec(),
            clustering_key: clustering_key.to_vec(),
        };

        for idx in &self.indexes {
            if idx.definition().column == column {
                idx.delete(&entry)?;
            }
        }
        Ok(())
    }

    /// Search an index by name for exact match.
    pub fn search(&self, index_name: &str, term: &[u8]) -> Result<Vec<IndexEntry>, IndexError> {
        for idx in &self.indexes {
            if idx.definition().name == index_name {
                return idx.search(term);
            }
        }
        Err(IndexError::NotFound(index_name.to_string()))
    }

    /// Get the number of registered indexes.
    pub fn count(&self) -> usize {
        self.indexes.len()
    }
}

impl Default for IndexManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Helper to avoid the borrow-checker issue with &mut self.indexes.
fn indexes_push(indexes: &mut Vec<Box<dyn SecondaryIndex>>, index: Box<dyn SecondaryIndex>) {
    indexes.push(index);
}

/// Index errors.
#[derive(Debug, thiserror::Error)]
pub enum IndexError {
    #[error("Index not found: {0}")]
    NotFound(String),

    #[error("Index write failed: {0}")]
    WriteFailed(String),

    #[error("Index read failed: {0}")]
    ReadFailed(String),

    #[error("Index build failed: {0}")]
    BuildFailed(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn index_manager_basics() {
        let mgr = IndexManager::new();
        assert_eq!(mgr.count(), 0);
    }

    #[test]
    fn index_entry_equality() {
        let e1 = IndexEntry {
            term: b"value".to_vec(),
            partition_key: b"pk".to_vec(),
            clustering_key: b"ck".to_vec(),
        };
        let e2 = e1.clone();
        assert_eq!(e1, e2);
    }
}
