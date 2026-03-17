// Licensed under Apache License, Version 2.0.

//! Immutable schema catalog with Arc-based snapshots.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.schema.Schema`
//! - `org.apache.cassandra.schema.SchemaManager`
//!
//! ## Design
//! The catalog holds an `Arc<SchemaSnapshot>`. Reads clone the Arc (lock-free).
//! Mutations produce a new `SchemaCatalog` with a new `Arc<SchemaSnapshot>`.

use crate::keyspace::KeyspaceMetadata;
use crate::table::TableMetadata;
use crate::view::ViewMetadata;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;

/// An immutable point-in-time snapshot of the schema.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SchemaSnapshot {
    /// All keyspaces, keyed by name.
    pub keyspaces: BTreeMap<String, KeyspaceMetadata>,
    /// Monotonically increasing schema version.
    pub version: u64,
}

impl SchemaSnapshot {
    /// Create an empty snapshot.
    pub fn empty() -> Self {
        Self {
            keyspaces: BTreeMap::new(),
            version: 0,
        }
    }

    /// Look up a keyspace.
    pub fn keyspace(&self, name: &str) -> Option<&KeyspaceMetadata> {
        self.keyspaces.get(name)
    }

    /// Look up a table across all keyspaces.
    pub fn table(&self, keyspace: &str, table: &str) -> Option<&TableMetadata> {
        self.keyspace(keyspace)?.table(table)
    }

    /// Total number of keyspaces.
    pub fn keyspace_count(&self) -> usize {
        self.keyspaces.len()
    }

    /// Total number of tables across all keyspaces.
    pub fn table_count(&self) -> usize {
        self.keyspaces.values().map(|ks| ks.table_count()).sum()
    }

    /// Look up a view across all keyspaces.
    pub fn view(&self, keyspace: &str, view: &str) -> Option<&ViewMetadata> {
        self.keyspace(keyspace)?.view(view)
    }
}

/// The schema catalog provides concurrent access to the current schema.
///
/// It wraps `Arc<SchemaSnapshot>` so reads never block. Writes create
/// a new snapshot and atomically swap the Arc.
#[derive(Debug, Clone)]
pub struct SchemaCatalog {
    inner: Arc<SchemaSnapshot>,
}

impl SchemaCatalog {
    /// Create an empty catalog.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(SchemaSnapshot::empty()),
        }
    }

    /// Create a catalog from an existing snapshot.
    pub fn from_snapshot(snapshot: SchemaSnapshot) -> Self {
        Self {
            inner: Arc::new(snapshot),
        }
    }

    /// Get an immutable snapshot of the current schema.
    /// This is cheap (Arc clone) and lock-free.
    pub fn snapshot(&self) -> Arc<SchemaSnapshot> {
        Arc::clone(&self.inner)
    }

    /// Apply a keyspace addition/update and return a new catalog.
    pub fn with_keyspace(&self, ks: KeyspaceMetadata) -> Self {
        let mut snapshot = (*self.inner).clone();
        snapshot.keyspaces.insert(ks.name.clone(), ks);
        snapshot.version += 1;
        Self {
            inner: Arc::new(snapshot),
        }
    }

    /// Remove a keyspace and return a new catalog.
    pub fn without_keyspace(&self, name: &str) -> Self {
        let mut snapshot = (*self.inner).clone();
        snapshot.keyspaces.remove(name);
        snapshot.version += 1;
        Self {
            inner: Arc::new(snapshot),
        }
    }

    /// Current schema version.
    pub fn version(&self) -> u64 {
        self.inner.version
    }
}

impl Default for SchemaCatalog {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::column::ColumnMetadata;
    use crate::keyspace::{KeyspaceMetadata, KeyspaceParams};
    use crate::table::TableMetadataBuilder;
    use cassandra_types::CqlType;

    #[test]
    fn empty_catalog() {
        let cat = SchemaCatalog::new();
        assert_eq!(cat.snapshot().keyspace_count(), 0);
        assert_eq!(cat.version(), 0);
    }

    #[test]
    fn add_keyspace() {
        let cat = SchemaCatalog::new();
        let ks = KeyspaceMetadata::new("test_ks", KeyspaceParams::default());
        let cat2 = cat.with_keyspace(ks);
        assert_eq!(cat2.snapshot().keyspace_count(), 1);
        assert_eq!(cat2.version(), 1);
    }

    #[test]
    fn snapshot_immutability() {
        let cat = SchemaCatalog::new();
        let snap_before = cat.snapshot();
        let ks = KeyspaceMetadata::new("ks", KeyspaceParams::default());
        let cat2 = cat.with_keyspace(ks);

        // Old snapshot is unchanged
        assert_eq!(snap_before.keyspace_count(), 0);
        // New snapshot has the keyspace
        assert_eq!(cat2.snapshot().keyspace_count(), 1);
    }

    #[test]
    fn arc_semantics() {
        let cat = SchemaCatalog::new();
        let ks = KeyspaceMetadata::new("ks", KeyspaceParams::default());
        let cat = cat.with_keyspace(ks);

        let snap1 = cat.snapshot();
        let snap2 = cat.snapshot();
        // Both point to the same data
        assert!(Arc::ptr_eq(&snap1, &snap2));
    }

    #[test]
    fn table_lookup() {
        let table = TableMetadataBuilder::new("ks", "users")
            .add_column(ColumnMetadata::partition_key("id", 0, CqlType::Uuid))
            .build();
        let ks = KeyspaceMetadata::new("ks", KeyspaceParams::default()).with_table(table);
        let cat = SchemaCatalog::new().with_keyspace(ks);
        assert!(cat.snapshot().table("ks", "users").is_some());
        assert!(cat.snapshot().table("ks", "nonexistent").is_none());
    }

    #[test]
    fn remove_keyspace() {
        let ks = KeyspaceMetadata::new("ks", KeyspaceParams::default());
        let cat = SchemaCatalog::new().with_keyspace(ks);
        let cat2 = cat.without_keyspace("ks");
        assert_eq!(cat2.snapshot().keyspace_count(), 0);
        assert_eq!(cat2.version(), 2); // version incremented
    }

    #[test]
    fn version_increments() {
        let cat = SchemaCatalog::new();
        let cat = cat.with_keyspace(KeyspaceMetadata::new("ks1", KeyspaceParams::default()));
        let cat = cat.with_keyspace(KeyspaceMetadata::new("ks2", KeyspaceParams::default()));
        assert_eq!(cat.version(), 2);
    }
}
