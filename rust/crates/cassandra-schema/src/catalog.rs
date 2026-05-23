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

use crate::index::IndexMetadata;
use crate::keyspace::KeyspaceMetadata;
use crate::table::TableMetadata;
use crate::user_function::{UserAggregate, UserFunction};
use crate::user_type::UserType;
use crate::view::ViewMetadata;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;

/// A conflict found while merging two divergent schema snapshots.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaMergeConflict {
    pub path: String,
    pub reason: String,
}

impl SchemaMergeConflict {
    fn new(path: impl Into<String>, reason: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            reason: reason.into(),
        }
    }
}

/// Result of a conservative schema snapshot merge.
#[derive(Debug, Clone, PartialEq)]
pub struct SchemaMergeReport {
    pub snapshot: SchemaSnapshot,
    pub conflicts: Vec<SchemaMergeConflict>,
    pub changed: bool,
}

impl SchemaMergeReport {
    pub fn is_clean(&self) -> bool {
        self.conflicts.is_empty()
    }
}

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

    /// Look up an index on a table.
    pub fn index(&self, keyspace: &str, table: &str, index: &str) -> Option<&IndexMetadata> {
        self.table(keyspace, table)?.index(index)
    }

    /// Look up a user-defined type by keyspace and name.
    pub fn user_type(&self, keyspace: &str, name: &str) -> Option<&UserType> {
        self.keyspace(keyspace)?.user_type(name)
    }

    /// Look up a user-defined function by keyspace and canonical signature.
    pub fn function(&self, keyspace: &str, signature: &str) -> Option<&UserFunction> {
        self.keyspace(keyspace)?.function(signature)
    }

    /// Look up a user-defined aggregate by keyspace and canonical signature.
    pub fn aggregate(&self, keyspace: &str, signature: &str) -> Option<&UserAggregate> {
        self.keyspace(keyspace)?.aggregate(signature)
    }

    /// Total number of materialized views across all keyspaces.
    pub fn view_count(&self) -> usize {
        self.keyspaces.values().map(|ks| ks.view_count()).sum()
    }

    /// Total number of secondary indexes across all keyspaces.
    pub fn index_count(&self) -> usize {
        self.keyspaces
            .values()
            .flat_map(|ks| ks.tables.values())
            .map(|table| table.indexes.len())
            .sum()
    }

    /// Total number of user-defined types across all keyspaces.
    pub fn type_count(&self) -> usize {
        self.keyspaces.values().map(|ks| ks.type_count()).sum()
    }

    /// Total number of user-defined functions across all keyspaces.
    pub fn function_count(&self) -> usize {
        self.keyspaces.values().map(|ks| ks.function_count()).sum()
    }

    /// Total number of user-defined aggregates across all keyspaces.
    pub fn aggregate_count(&self) -> usize {
        self.keyspaces.values().map(|ks| ks.aggregate_count()).sum()
    }

    /// Conservatively merge another snapshot into this one.
    ///
    /// Definitions that exist only on one side are added. Identical
    /// definitions are accepted. Different definitions for the same schema path
    /// are reported as conflicts and left unchanged.
    pub fn merge_divergent(&self, remote: &SchemaSnapshot) -> SchemaMergeReport {
        let mut merged = self.clone();
        let mut conflicts = Vec::new();
        let mut changed = false;

        for (keyspace_name, remote_ks) in &remote.keyspaces {
            match self.keyspaces.get(keyspace_name) {
                None => {
                    merged
                        .keyspaces
                        .insert(keyspace_name.clone(), remote_ks.clone());
                    changed = true;
                }
                Some(local_ks) if local_ks == remote_ks => {}
                Some(local_ks) => {
                    match merge_keyspace_metadata(keyspace_name, local_ks, remote_ks) {
                        Ok(merged_ks) => {
                            if &merged_ks != local_ks {
                                merged.keyspaces.insert(keyspace_name.clone(), merged_ks);
                                changed = true;
                            }
                        }
                        Err(mut found) => conflicts.append(&mut found),
                    }
                }
            }
        }

        if changed {
            merged.version = self.version.max(remote.version) + 1;
        }

        SchemaMergeReport {
            snapshot: merged,
            conflicts,
            changed,
        }
    }
}

fn merge_keyspace_metadata(
    keyspace_name: &str,
    local: &KeyspaceMetadata,
    remote: &KeyspaceMetadata,
) -> Result<KeyspaceMetadata, Vec<SchemaMergeConflict>> {
    let mut conflicts = Vec::new();

    if local.kind != remote.kind {
        conflicts.push(SchemaMergeConflict::new(
            format!("{keyspace_name}.kind"),
            "keyspace kind differs",
        ));
    }
    if local.params != remote.params {
        conflicts.push(SchemaMergeConflict::new(
            format!("{keyspace_name}.params"),
            "keyspace params differ",
        ));
    }

    let mut merged = local.clone();
    merge_named_map(
        &format!("{keyspace_name}.tables"),
        &mut merged.tables,
        &remote.tables,
        &mut conflicts,
    );
    merge_named_map(
        &format!("{keyspace_name}.views"),
        &mut merged.views,
        &remote.views,
        &mut conflicts,
    );
    merge_named_map(
        &format!("{keyspace_name}.types"),
        &mut merged.types,
        &remote.types,
        &mut conflicts,
    );
    merge_named_map(
        &format!("{keyspace_name}.functions"),
        &mut merged.functions,
        &remote.functions,
        &mut conflicts,
    );
    merge_named_map(
        &format!("{keyspace_name}.aggregates"),
        &mut merged.aggregates,
        &remote.aggregates,
        &mut conflicts,
    );

    if conflicts.is_empty() {
        Ok(merged)
    } else {
        Err(conflicts)
    }
}

fn merge_named_map<T>(
    path: &str,
    local: &mut BTreeMap<String, T>,
    remote: &BTreeMap<String, T>,
    conflicts: &mut Vec<SchemaMergeConflict>,
) where
    T: Clone + PartialEq,
{
    for (name, remote_value) in remote {
        match local.get(name) {
            None => {
                local.insert(name.clone(), remote_value.clone());
            }
            Some(local_value) if local_value == remote_value => {}
            Some(_) => conflicts.push(SchemaMergeConflict::new(
                format!("{path}.{name}"),
                "definition differs",
            )),
        }
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

    /// Look up a keyspace in the current snapshot.
    pub fn keyspace(&self, name: &str) -> Option<&KeyspaceMetadata> {
        self.inner.keyspace(name)
    }

    /// Look up a table in the current snapshot.
    pub fn table(&self, keyspace: &str, table: &str) -> Option<&TableMetadata> {
        self.inner.table(keyspace, table)
    }

    /// Look up a view in the current snapshot.
    pub fn view(&self, keyspace: &str, view: &str) -> Option<&ViewMetadata> {
        self.inner.view(keyspace, view)
    }

    /// Look up an index in the current snapshot.
    pub fn index(&self, keyspace: &str, table: &str, index: &str) -> Option<&IndexMetadata> {
        self.inner.index(keyspace, table, index)
    }

    /// Look up a user-defined type in the current snapshot.
    pub fn user_type(&self, keyspace: &str, name: &str) -> Option<&UserType> {
        self.inner.user_type(keyspace, name)
    }

    /// Look up a user-defined function by canonical signature in the current snapshot.
    pub fn function(&self, keyspace: &str, signature: &str) -> Option<&UserFunction> {
        self.inner.function(keyspace, signature)
    }

    /// Look up a user-defined aggregate by canonical signature in the current snapshot.
    pub fn aggregate(&self, keyspace: &str, signature: &str) -> Option<&UserAggregate> {
        self.inner.aggregate(keyspace, signature)
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
    use crate::index::{IndexKind, IndexMetadata};
    use crate::keyspace::{KeyspaceMetadata, KeyspaceParams};
    use crate::table::TableMetadataBuilder;
    use crate::user_function::{UserAggregate, UserFunction};
    use crate::user_type::UserType;
    use cassandra_types::CqlType;
    use std::collections::HashMap;

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
        assert!(cat.table("ks", "users").is_some());
        assert!(cat.snapshot().table("ks", "nonexistent").is_none());
    }

    #[test]
    fn direct_lookup_for_nested_schema_metadata() {
        let mut index_options = HashMap::new();
        index_options.insert("target".to_string(), "email".to_string());
        let index = IndexMetadata::new(
            "email_idx_id".to_string(),
            "email_idx".to_string(),
            IndexKind::Keys,
            index_options,
        );
        let table = TableMetadataBuilder::new("ks", "users")
            .add_column(ColumnMetadata::partition_key("id", 0, CqlType::Uuid))
            .add_index(index)
            .build();
        let user_type = UserType::new("ks", "address")
            .with_field("street", "text")
            .with_field("zip", "int");
        let function = UserFunction::new("ks", "normalize", "text", "java", "return input.trim();")
            .with_arg("input", "varchar");
        let function_signature = function.signature();
        let aggregate = UserAggregate::new("ks", "concat_all", "text", "concat_state")
            .with_arg_type("varchar")
            .with_return_type("varchar");
        let aggregate_signature = aggregate.signature();
        let ks = KeyspaceMetadata::new("ks", KeyspaceParams::default())
            .with_table(table)
            .with_type(user_type)
            .with_function(function)
            .with_aggregate(aggregate);
        let cat = SchemaCatalog::new().with_keyspace(ks);
        let snapshot = cat.snapshot();

        assert_eq!(
            snapshot.index("ks", "users", "email_idx").unwrap().name,
            "email_idx"
        );
        assert_eq!(
            cat.index("ks", "users", "email_idx").unwrap().name,
            "email_idx"
        );
        assert_eq!(
            snapshot.user_type("ks", "address").unwrap().field_count(),
            2
        );
        assert_eq!(cat.user_type("ks", "address").unwrap().field_count(), 2);
        assert_eq!(
            snapshot
                .function("ks", &function_signature)
                .unwrap()
                .return_type,
            "text"
        );
        assert_eq!(
            cat.function("ks", &function_signature).unwrap().arg_types,
            vec!["text"]
        );
        assert_eq!(
            snapshot
                .aggregate("ks", &aggregate_signature)
                .unwrap()
                .return_type,
            "text"
        );
        assert_eq!(
            cat.aggregate("ks", &aggregate_signature).unwrap().arg_types,
            vec!["text"]
        );

        assert_eq!(snapshot.table_count(), 1);
        assert_eq!(snapshot.index_count(), 1);
        assert_eq!(snapshot.type_count(), 1);
        assert_eq!(snapshot.function_count(), 1);
        assert_eq!(snapshot.aggregate_count(), 1);
    }

    #[test]
    fn merge_divergent_snapshots_combines_independent_keyspaces() {
        let mut local = SchemaSnapshot::empty();
        local.version = 3;
        local.keyspaces.insert(
            "ks_local".to_string(),
            KeyspaceMetadata::new("ks_local", KeyspaceParams::default()),
        );
        let mut remote = SchemaSnapshot::empty();
        remote.version = 4;
        remote.keyspaces.insert(
            "ks_remote".to_string(),
            KeyspaceMetadata::new("ks_remote", KeyspaceParams::default()),
        );

        let report = local.merge_divergent(&remote);

        assert!(report.is_clean());
        assert!(report.changed);
        assert_eq!(report.snapshot.version, 5);
        assert!(report.snapshot.keyspace("ks_local").is_some());
        assert!(report.snapshot.keyspace("ks_remote").is_some());
    }

    #[test]
    fn merge_divergent_snapshots_combines_independent_objects_in_keyspace() {
        let table = TableMetadataBuilder::new("ks", "users")
            .add_column(ColumnMetadata::partition_key("id", 0, CqlType::Uuid))
            .build();
        let mut local = SchemaSnapshot::empty();
        local.keyspaces.insert(
            "ks".to_string(),
            KeyspaceMetadata::new("ks", KeyspaceParams::default()).with_table(table),
        );
        let mut remote = SchemaSnapshot::empty();
        remote.keyspaces.insert(
            "ks".to_string(),
            KeyspaceMetadata::new("ks", KeyspaceParams::default())
                .with_type(UserType::new("ks", "address").with_field("street", "text")),
        );

        let report = local.merge_divergent(&remote);

        assert!(report.is_clean());
        assert!(report.changed);
        assert!(report.snapshot.table("ks", "users").is_some());
        assert!(report.snapshot.user_type("ks", "address").is_some());
    }

    #[test]
    fn merge_divergent_snapshots_reports_conflicting_definitions() {
        let local_table = TableMetadataBuilder::new("ks", "users")
            .add_column(ColumnMetadata::partition_key("id", 0, CqlType::Uuid))
            .comment("local")
            .build();
        let remote_table = TableMetadataBuilder::new("ks", "users")
            .add_column(ColumnMetadata::partition_key("id", 0, CqlType::Uuid))
            .comment("remote")
            .build();
        let mut local = SchemaSnapshot::empty();
        local.keyspaces.insert(
            "ks".to_string(),
            KeyspaceMetadata::new("ks", KeyspaceParams::default()).with_table(local_table),
        );
        let mut remote = SchemaSnapshot::empty();
        remote.keyspaces.insert(
            "ks".to_string(),
            KeyspaceMetadata::new("ks", KeyspaceParams::default()).with_table(remote_table),
        );

        let report = local.merge_divergent(&remote);

        assert!(!report.is_clean());
        assert!(!report.changed);
        assert_eq!(
            report.conflicts,
            vec![SchemaMergeConflict {
                path: "ks.tables.users".to_string(),
                reason: "definition differs".to_string(),
            }]
        );
        assert_eq!(
            report.snapshot.table("ks", "users").unwrap().params.comment,
            "local"
        );
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
