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
pub mod lifecycle;
pub mod metrics;
pub mod sai;
#[cfg(feature = "sasi")]
pub mod sasi;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;

/// Index type classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum IndexType {
    /// Legacy key-based secondary index (inverted index table).
    Legacy,
    /// Storage Attached Index (SAI) — per-SSTable index segments.
    Sai,
    /// Experimental SASI index.
    Sasi,
}

/// Index build status tracking.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IndexStatus {
    /// Index is currently being built/rebuilt.
    Building,
    /// Index data is fully built.
    Built,
    /// Index is ready to serve queries.
    QueryReady,
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
    pub options: HashMap<String, String>,
}

impl IndexDefinition {
    /// Convert from schema-level `IndexMetadata` to storage-level `IndexDefinition`.
    ///
    /// Mapping: `Keys`/`Composites` → `Legacy`, `Custom` with SAI class → `Sai`, else → `Legacy`.
    pub fn from_metadata(
        meta: &cassandra_schema::index::IndexMetadata,
        keyspace: &str,
        table: &str,
    ) -> Self {
        let column = meta.target_column().cloned().unwrap_or_default();

        let index_type = match meta.kind {
            cassandra_schema::index::IndexKind::Keys
            | cassandra_schema::index::IndexKind::Composites => IndexType::Legacy,
            cassandra_schema::index::IndexKind::Custom => {
                if let Some(class) = meta.index_class_name() {
                    if class.contains("StorageAttachedIndex") {
                        IndexType::Sai
                    } else {
                        IndexType::Legacy
                    }
                } else {
                    IndexType::Legacy
                }
            }
        };

        Self {
            name: meta.name.clone(),
            keyspace: keyspace.to_string(),
            table: table.to_string(),
            column,
            index_type,
            options: meta.options.clone(),
        }
    }

    /// Convert back to schema-level `IndexMetadata`.
    pub fn to_metadata(&self) -> cassandra_schema::index::IndexMetadata {
        use cassandra_schema::index::{IndexKind, IndexMetadata};

        let kind = match self.index_type {
            IndexType::Legacy => IndexKind::Keys,
            IndexType::Sai | IndexType::Sasi => IndexKind::Custom,
        };

        let mut options = self.options.clone();
        if !self.column.is_empty() {
            options
                .entry("target".to_string())
                .or_insert_with(|| self.column.clone());
        }

        IndexMetadata::new(self.name.clone(), self.name.clone(), kind, options)
    }
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

    /// Perform a vector kNN search.
    fn search_vector(
        &self,
        _vector: &[u8],
        _top_k: usize,
    ) -> Result<Vec<(IndexEntry, f32)>, IndexError> {
        Err(IndexError::ReadFailed("Vector search not supported".into()))
    }

    /// Truncate all index data.
    fn truncate(&self) -> Result<(), IndexError>;

    fn definition(&self) -> &IndexDefinition;

    /// Add a pre-built SAI segment to the index.
    /// Default implementation is no-op, overridden by `SaiIndex`.
    fn add_sai_segment(
        &self,
        _segment: crate::index::sai::builder::SaiSegment,
    ) -> Result<(), IndexError> {
        Ok(())
    }
}

/// Manages all secondary indexes for a column family.
#[derive(Debug)]
pub struct IndexManager {
    /// Registered indexes by name.
    indexes: RwLock<Vec<Box<dyn SecondaryIndex>>>,
    /// Build status for each index.
    statuses: RwLock<HashMap<String, IndexStatus>>,
}

impl IndexManager {
    pub fn new() -> Self {
        Self {
            indexes: RwLock::new(Vec::new()),
            statuses: RwLock::new(HashMap::new()),
        }
    }

    /// Register a new index.
    pub fn register(&self, index: Box<dyn SecondaryIndex>) {
        let name = index.definition().name.clone();
        self.indexes.write().push(index);
        self.statuses.write().insert(name, IndexStatus::QueryReady);
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

        let indexes = self.indexes.read();
        for idx in indexes.iter() {
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

        let indexes = self.indexes.read();
        for idx in indexes.iter() {
            if idx.definition().column == column {
                idx.delete(&entry)?;
            }
        }
        Ok(())
    }

    /// Search an index by name for exact match.
    /// Returns empty results if the index is not queryable.
    pub fn search(&self, index_name: &str, term: &[u8]) -> Result<Vec<IndexEntry>, IndexError> {
        if !self.is_queryable(index_name) {
            return Ok(Vec::new());
        }
        let indexes = self.indexes.read();
        for idx in indexes.iter() {
            if idx.definition().name == index_name {
                return idx.search(term);
            }
        }
        Err(IndexError::NotFound(index_name.to_string()))
    }

    /// Search an index by name using a vector kNN query.
    /// Returns empty results if the index is not queryable.
    pub fn search_vector(
        &self,
        index_name: &str,
        vector: &[u8],
        top_k: usize,
    ) -> Result<Vec<(IndexEntry, f32)>, IndexError> {
        if !self.is_queryable(index_name) {
            return Ok(Vec::new());
        }
        let indexes = self.indexes.read();
        for idx in indexes.iter() {
            if idx.definition().name == index_name {
                return idx.search_vector(vector, top_k);
            }
        }
        Err(IndexError::NotFound(index_name.to_string()))
    }

    /// Forward a newly built SAI segment to the corresponding index.
    pub fn add_sai_segment(
        &self,
        segment: crate::index::sai::builder::SaiSegment,
    ) -> Result<(), IndexError> {
        let indexes = self.indexes.read();
        for idx in indexes.iter() {
            if idx.definition().column == segment.column
                && idx.definition().name == segment.index_name
            {
                idx.add_sai_segment(segment.clone())?;
            }
        }
        Ok(())
    }

    /// Build SAI segments for a newly flushed SSTable.
    pub fn build_sai_segments(
        &self,
        generation: u64,
        partitions: &[(Vec<u8>, crate::memtable::partition::PartitionData)],
    ) -> Result<(), IndexError> {
        let mut targets = Vec::new();
        {
            let indexes = self.indexes.read();
            for idx in indexes.iter() {
                if idx.definition().index_type == IndexType::Sai {
                    targets.push((
                        idx.definition().name.clone(),
                        idx.definition().column.clone(),
                    ));
                }
            }
        }

        if targets.is_empty() {
            return Ok(());
        }

        let mut builders: Vec<_> = targets
            .iter()
            .map(|(name, col)| {
                crate::index::sai::builder::SaiSegmentBuilder::new(generation, name, col)
            })
            .collect();

        // Feed rows
        for (pk, data) in partitions {
            for (ck, row) in &data.rows {
                if row.is_tombstone {
                    continue;
                }
                for cell in &row.cells {
                    if cell.is_tombstone || cell.value.is_none() {
                        continue;
                    }
                    let val = cell.value.as_ref().unwrap();

                    for (i, (_, target_col)) in targets.iter().enumerate() {
                        if cell.column == *target_col {
                            builders[i].add(val.clone(), pk.clone(), ck.clone());
                        }
                    }
                }
            }
        }

        // Add built segments to indexes
        for builder in builders {
            let segment = builder.build();
            self.add_sai_segment(segment)?;
        }

        Ok(())
    }

    // ─── Status Tracking ─────────────────────────────────────────────

    /// Set the build status for an index.
    pub fn set_status(&self, name: &str, status: IndexStatus) {
        self.statuses.write().insert(name.to_string(), status);
    }

    /// Get the build status for an index.
    pub fn get_status(&self, name: &str) -> Option<IndexStatus> {
        self.statuses.read().get(name).copied()
    }

    /// Returns `true` if the index is ready to serve queries.
    pub fn is_queryable(&self, name: &str) -> bool {
        self.statuses
            .read()
            .get(name)
            .map(|s| *s == IndexStatus::QueryReady)
            .unwrap_or(true) // allow queries if status unknown (backward compat)
    }

    // ─── Lifecycle Methods ──────────────────────────────────────────

    /// Unregister an index by name. Returns `true` if the index was found and removed.
    pub fn unregister(&self, name: &str) -> bool {
        let mut indexes = self.indexes.write();
        let before = indexes.len();
        indexes.retain(|idx| idx.definition().name != name);
        self.statuses.write().remove(name);
        indexes.len() < before
    }

    /// Get the definition of an index by name.
    pub fn get_definition(&self, name: &str) -> Option<IndexDefinition> {
        let indexes = self.indexes.read();
        indexes
            .iter()
            .find(|idx| idx.definition().name == name)
            .map(|idx| idx.definition().clone())
    }

    /// List all registered index names.
    pub fn list_names(&self) -> Vec<String> {
        self.indexes
            .read()
            .iter()
            .map(|idx| idx.definition().name.clone())
            .collect()
    }

    /// Returns `true` if an index with the given name is registered.
    pub fn has_index(&self, name: &str) -> bool {
        self.indexes
            .read()
            .iter()
            .any(|idx| idx.definition().name == name)
    }

    /// Find the index name that covers the given column, if any.
    pub fn get_index_for_column(&self, column: &str) -> Option<String> {
        self.indexes
            .read()
            .iter()
            .find(|idx| idx.definition().column == column)
            .map(|idx| idx.definition().name.clone())
    }

    /// Get the number of registered indexes.
    pub fn count(&self) -> usize {
        self.indexes.read().len()
    }
}

impl Default for IndexManager {
    fn default() -> Self {
        Self::new()
    }
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

    #[error("General index error: {0}")]
    General(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_legacy_def(name: &str, column: &str) -> IndexDefinition {
        IndexDefinition {
            name: name.into(),
            column: column.into(),
            index_type: IndexType::Legacy,
            options: HashMap::new(),
            keyspace: "ks".into(),
            table: "tbl".into(),
        }
    }

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

    #[test]
    fn test_build_sai_segments() {
        let mgr = IndexManager::new();
        let def = IndexDefinition {
            name: "test_idx".into(),
            column: "col1".into(),
            index_type: IndexType::Sai,
            options: HashMap::new(),
            keyspace: "ks".into(),
            table: "tbl".into(),
        };

        let sai_index = crate::index::sai::SaiIndex::new(def);
        mgr.register(Box::new(sai_index));

        let row = crate::memtable::partition::Row {
            clustering_key: b"ck".to_vec(),
            cells: vec![crate::memtable::partition::Cell {
                column: "col1".into(),
                value: Some(b"val1".to_vec()),
                timestamp: 1,
                ttl: 0,
                local_deletion_time: None,
                is_tombstone: false,
            }],
            is_tombstone: false,
            local_deletion_time: None,
        };

        let mut pd = crate::memtable::partition::PartitionData::new();
        pd.apply_row(row);

        mgr.build_sai_segments(1, &[(b"pk".to_vec(), pd)]).unwrap();

        let results = mgr.search("test_idx", b"val1").unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].partition_key, b"pk");
    }

    // ─── Unit 2: Conversion tests ───────────────────────────────────

    #[test]
    fn from_metadata_keys() {
        use cassandra_schema::index::{IndexKind, IndexMetadata};
        let mut opts = HashMap::new();
        opts.insert("target".into(), "email".into());
        let meta = IndexMetadata::new("id1".into(), "email_idx".into(), IndexKind::Keys, opts);
        let def = IndexDefinition::from_metadata(&meta, "ks", "users");
        assert_eq!(def.name, "email_idx");
        assert_eq!(def.keyspace, "ks");
        assert_eq!(def.table, "users");
        assert_eq!(def.column, "email");
        assert_eq!(def.index_type, IndexType::Legacy);
    }

    #[test]
    fn from_metadata_composites() {
        use cassandra_schema::index::{IndexKind, IndexMetadata};
        let mut opts = HashMap::new();
        opts.insert("target".into(), "col1".into());
        let meta = IndexMetadata::new("id2".into(), "comp_idx".into(), IndexKind::Composites, opts);
        let def = IndexDefinition::from_metadata(&meta, "ks", "tbl");
        assert_eq!(def.index_type, IndexType::Legacy);
    }

    #[test]
    fn from_metadata_custom_sai() {
        use cassandra_schema::index::{IndexKind, IndexMetadata};
        let mut opts = HashMap::new();
        opts.insert("target".into(), "col1".into());
        opts.insert(
            "class_name".into(),
            "org.apache.cassandra.index.sai.StorageAttachedIndex".into(),
        );
        let meta = IndexMetadata::new("id3".into(), "sai_idx".into(), IndexKind::Custom, opts);
        let def = IndexDefinition::from_metadata(&meta, "ks", "tbl");
        assert_eq!(def.index_type, IndexType::Sai);
    }

    #[test]
    fn from_metadata_custom_non_sai() {
        use cassandra_schema::index::{IndexKind, IndexMetadata};
        let mut opts = HashMap::new();
        opts.insert("target".into(), "col1".into());
        opts.insert("class_name".into(), "com.example.MyCustomIndex".into());
        let meta = IndexMetadata::new("id4".into(), "custom_idx".into(), IndexKind::Custom, opts);
        let def = IndexDefinition::from_metadata(&meta, "ks", "tbl");
        assert_eq!(def.index_type, IndexType::Legacy);
    }

    #[test]
    fn to_metadata_round_trip() {
        use cassandra_schema::index::IndexKind;
        let def = make_legacy_def("my_idx", "col1");
        let meta = def.to_metadata();
        assert_eq!(meta.name, "my_idx");
        assert_eq!(meta.kind, IndexKind::Keys);
        assert_eq!(meta.target_column().unwrap(), "col1");
    }

    // ─── Unit 3: Status tracking tests ──────────────────────────────

    #[test]
    fn status_transitions() {
        let mgr = IndexManager::new();
        assert_eq!(mgr.get_status("idx"), None);

        mgr.set_status("idx", IndexStatus::Building);
        assert_eq!(mgr.get_status("idx"), Some(IndexStatus::Building));
        assert!(!mgr.is_queryable("idx"));

        mgr.set_status("idx", IndexStatus::Built);
        assert!(!mgr.is_queryable("idx"));

        mgr.set_status("idx", IndexStatus::QueryReady);
        assert!(mgr.is_queryable("idx"));
    }

    #[test]
    fn search_skips_non_queryable() {
        let mgr = IndexManager::new();
        let def = make_legacy_def("idx", "col1");
        let index = crate::index::legacy::LegacyIndex::new(def);
        mgr.register(Box::new(index));

        // Insert data
        mgr.on_write(b"pk", b"ck", "col1", b"val").unwrap();

        // Should find results when queryable
        let results = mgr.search("idx", b"val").unwrap();
        assert_eq!(results.len(), 1);

        // Mark as building — search should return empty
        mgr.set_status("idx", IndexStatus::Building);
        let results = mgr.search("idx", b"val").unwrap();
        assert!(results.is_empty());

        // Mark as query-ready again
        mgr.set_status("idx", IndexStatus::QueryReady);
        let results = mgr.search("idx", b"val").unwrap();
        assert_eq!(results.len(), 1);
    }

    // ─── Unit 4: Lifecycle methods tests ────────────────────────────

    #[test]
    fn lifecycle_methods() {
        let mgr = IndexManager::new();
        let def = make_legacy_def("idx1", "col1");
        let index = crate::index::legacy::LegacyIndex::new(def);
        mgr.register(Box::new(index));

        assert!(mgr.has_index("idx1"));
        assert!(!mgr.has_index("idx2"));

        assert_eq!(mgr.list_names(), vec!["idx1".to_string()]);

        let def = mgr.get_definition("idx1").unwrap();
        assert_eq!(def.column, "col1");
        assert!(mgr.get_definition("nonexistent").is_none());

        assert_eq!(mgr.get_index_for_column("col1"), Some("idx1".to_string()));
        assert!(mgr.get_index_for_column("col2").is_none());

        // Unregister
        assert!(mgr.unregister("idx1"));
        assert!(!mgr.has_index("idx1"));
        assert_eq!(mgr.count(), 0);
        assert!(!mgr.unregister("idx1")); // already gone
    }

    #[test]
    fn register_sets_query_ready() {
        let mgr = IndexManager::new();
        let def = make_legacy_def("idx1", "col1");
        let index = crate::index::legacy::LegacyIndex::new(def);
        mgr.register(Box::new(index));
        assert_eq!(mgr.get_status("idx1"), Some(IndexStatus::QueryReady));
    }
}
