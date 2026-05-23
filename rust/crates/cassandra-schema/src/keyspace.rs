// Licensed under Apache License, Version 2.0.

//! Keyspace metadata.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.schema.KeyspaceMetadata`
//! - `org.apache.cassandra.schema.KeyspaceParams`

use crate::table::TableMetadata;
use crate::user_function::{UserAggregate, UserFunction};
use crate::user_type::UserType;
use crate::view::ViewMetadata;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// The kind of keyspace.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
pub enum KeyspaceKind {
    #[default]
    Regular,
    Virtual,
}

/// Replication parameters for a keyspace.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicationParams {
    /// Replication strategy class name.
    pub strategy_class: String,
    /// Strategy-specific options (e.g., "replication_factor" → "3").
    pub options: BTreeMap<String, String>,
}

impl ReplicationParams {
    /// Simple strategy with given RF.
    pub fn simple(replication_factor: u32) -> Self {
        let mut options = BTreeMap::new();
        options.insert(
            "replication_factor".to_string(),
            replication_factor.to_string(),
        );
        Self {
            strategy_class: "org.apache.cassandra.locator.SimpleStrategy".to_string(),
            options,
        }
    }

    /// Network topology strategy with per-DC replication factors.
    pub fn network_topology(dc_rfs: BTreeMap<String, u32>) -> Self {
        let options = dc_rfs
            .into_iter()
            .map(|(dc, rf)| (dc, rf.to_string()))
            .collect();
        Self {
            strategy_class: "org.apache.cassandra.locator.NetworkTopologyStrategy".to_string(),
            options,
        }
    }

    /// Local strategy (for system keyspaces).
    pub fn local() -> Self {
        Self {
            strategy_class: "org.apache.cassandra.locator.LocalStrategy".to_string(),
            options: BTreeMap::new(),
        }
    }
}

/// Keyspace-level parameters.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct KeyspaceParams {
    pub replication: ReplicationParams,
    #[serde(default = "default_durable_writes")]
    pub durable_writes: bool,
    #[serde(default)]
    pub comment: String,
}

fn default_durable_writes() -> bool {
    true
}

impl Default for KeyspaceParams {
    fn default() -> Self {
        Self {
            replication: ReplicationParams::simple(1),
            durable_writes: true,
            comment: String::new(),
        }
    }
}

/// Metadata for a keyspace.
///
/// This is an immutable value type. Modifications produce a new instance
/// via the `with_*` methods.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct KeyspaceMetadata {
    pub name: String,
    pub kind: KeyspaceKind,
    pub params: KeyspaceParams,
    pub tables: BTreeMap<String, TableMetadata>,
    #[serde(default)]
    pub views: BTreeMap<String, ViewMetadata>,
    #[serde(default)]
    pub types: BTreeMap<String, UserType>,
    #[serde(default)]
    pub functions: BTreeMap<String, UserFunction>,
    #[serde(default)]
    pub aggregates: BTreeMap<String, UserAggregate>,
}

impl KeyspaceMetadata {
    /// Create a new keyspace metadata.
    pub fn new(name: impl Into<String>, params: KeyspaceParams) -> Self {
        Self {
            name: name.into(),
            kind: KeyspaceKind::Regular,
            params,
            tables: BTreeMap::new(),
            views: BTreeMap::new(),
            types: BTreeMap::new(),
            functions: BTreeMap::new(),
            aggregates: BTreeMap::new(),
        }
    }

    /// Create a system keyspace with local replication.
    pub fn system(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            kind: KeyspaceKind::Regular,
            params: KeyspaceParams {
                replication: ReplicationParams::local(),
                durable_writes: true,
                comment: String::new(),
            },
            tables: BTreeMap::new(),
            views: BTreeMap::new(),
            types: BTreeMap::new(),
            functions: BTreeMap::new(),
            aggregates: BTreeMap::new(),
        }
    }

    /// Return a new `KeyspaceMetadata` with the given table added.
    pub fn with_table(mut self, table: TableMetadata) -> Self {
        self.tables.insert(table.name.clone(), table);
        self
    }

    /// Return a new `KeyspaceMetadata` with the given table removed.
    pub fn without_table(mut self, table_name: &str) -> Self {
        self.tables.remove(table_name);
        self
    }

    /// Return a new `KeyspaceMetadata` with updated params.
    pub fn with_params(mut self, params: KeyspaceParams) -> Self {
        self.params = params;
        self
    }

    /// Look up a table by name.
    pub fn table(&self, name: &str) -> Option<&TableMetadata> {
        self.tables.get(name)
    }

    /// Number of tables in this keyspace.
    pub fn table_count(&self) -> usize {
        self.tables.len()
    }

    /// Find the table that contains an index with the given name.
    pub fn find_indexed_table(&self, index_name: &str) -> Option<&str> {
        for (table_name, table) in &self.tables {
            if table.index(index_name).is_some() {
                return Some(table_name.as_str());
            }
        }
        None
    }

    /// Return a new `KeyspaceMetadata` with the given index added to the specified table.
    pub fn with_table_index(mut self, table_name: &str, idx: crate::index::IndexMetadata) -> Self {
        if let Some(table) = self.tables.remove(table_name) {
            let updated = table.with_index(idx);
            self.tables.insert(table_name.to_string(), updated);
        }
        self
    }

    /// Return a new `KeyspaceMetadata` with the named index removed from the specified table.
    pub fn without_table_index(mut self, table_name: &str, index_name: &str) -> Self {
        if let Some(table) = self.tables.remove(table_name) {
            let updated = table.without_index(index_name);
            self.tables.insert(table_name.to_string(), updated);
        }
        self
    }

    // ─── Views ─────────────────────────────────────────────────────────

    /// Return a new `KeyspaceMetadata` with the given view added.
    pub fn with_view(mut self, view: ViewMetadata) -> Self {
        self.views.insert(view.name.clone(), view);
        self
    }

    /// Return a new `KeyspaceMetadata` with the given view removed.
    pub fn without_view(mut self, view_name: &str) -> Self {
        self.views.remove(view_name);
        self
    }

    /// Look up a view by name.
    pub fn view(&self, name: &str) -> Option<&ViewMetadata> {
        self.views.get(name)
    }

    /// Number of views in this keyspace.
    pub fn view_count(&self) -> usize {
        self.views.len()
    }

    // ─── User-Defined Types ────────────────────────────────────────────

    /// Return a new `KeyspaceMetadata` with the given type added.
    pub fn with_type(mut self, udt: UserType) -> Self {
        self.types.insert(udt.name.clone(), udt);
        self
    }

    /// Return a new `KeyspaceMetadata` with the given type removed.
    pub fn without_type(mut self, type_name: &str) -> Self {
        self.types.remove(type_name);
        self
    }

    /// Look up a user-defined type by name.
    pub fn user_type(&self, name: &str) -> Option<&UserType> {
        self.types.get(name)
    }

    /// Number of user-defined types in this keyspace.
    pub fn type_count(&self) -> usize {
        self.types.len()
    }

    // ─── User-Defined Functions ────────────────────────────────────────

    /// Return a new `KeyspaceMetadata` with the given function added.
    pub fn with_function(mut self, func: UserFunction) -> Self {
        self.functions.insert(func.signature(), func);
        self
    }

    /// Return a new `KeyspaceMetadata` with the given function removed by signature.
    pub fn without_function(mut self, signature: &str) -> Self {
        self.functions.remove(signature);
        self
    }

    /// Look up a function by signature.
    pub fn function(&self, signature: &str) -> Option<&UserFunction> {
        self.functions.get(signature)
    }

    /// Number of user-defined functions in this keyspace.
    pub fn function_count(&self) -> usize {
        self.functions.len()
    }

    // ─── User-Defined Aggregates ───────────────────────────────────────

    /// Return a new `KeyspaceMetadata` with the given aggregate added.
    pub fn with_aggregate(mut self, agg: UserAggregate) -> Self {
        self.aggregates.insert(agg.signature(), agg);
        self
    }

    /// Return a new `KeyspaceMetadata` with the given aggregate removed by signature.
    pub fn without_aggregate(mut self, signature: &str) -> Self {
        self.aggregates.remove(signature);
        self
    }

    /// Look up an aggregate by signature.
    pub fn aggregate(&self, signature: &str) -> Option<&UserAggregate> {
        self.aggregates.get(signature)
    }

    /// Number of user-defined aggregates in this keyspace.
    pub fn aggregate_count(&self) -> usize {
        self.aggregates.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::column::ColumnMetadata;
    use crate::table::TableMetadataBuilder;
    use cassandra_types::CqlType;

    #[test]
    fn new_keyspace() {
        let ks = KeyspaceMetadata::new("test_ks", KeyspaceParams::default());
        assert_eq!(ks.name, "test_ks");
        assert_eq!(ks.table_count(), 0);
        assert!(ks.params.durable_writes);
    }

    #[test]
    fn system_keyspace() {
        let ks = KeyspaceMetadata::system("system");
        assert_eq!(
            ks.params.replication.strategy_class,
            "org.apache.cassandra.locator.LocalStrategy"
        );
    }

    #[test]
    fn with_table() {
        let table = TableMetadataBuilder::new("test_ks", "users")
            .add_column(ColumnMetadata::partition_key("id", 0, CqlType::Uuid))
            .build();
        let ks = KeyspaceMetadata::new("test_ks", KeyspaceParams::default()).with_table(table);
        assert_eq!(ks.table_count(), 1);
        assert!(ks.table("users").is_some());
    }

    #[test]
    fn immutable_with() {
        let ks1 = KeyspaceMetadata::new("ks", KeyspaceParams::default());
        let table = TableMetadataBuilder::new("ks", "t1")
            .add_column(ColumnMetadata::partition_key("id", 0, CqlType::Int))
            .build();
        let ks2 = ks1.clone().with_table(table);
        assert_eq!(ks1.table_count(), 0); // original unchanged
        assert_eq!(ks2.table_count(), 1);
    }

    #[test]
    fn without_table() {
        let table = TableMetadataBuilder::new("ks", "t1")
            .add_column(ColumnMetadata::partition_key("id", 0, CqlType::Int))
            .build();
        let ks = KeyspaceMetadata::new("ks", KeyspaceParams::default())
            .with_table(table)
            .without_table("t1");
        assert_eq!(ks.table_count(), 0);
    }

    #[test]
    fn replication_simple() {
        let rep = ReplicationParams::simple(3);
        assert!(rep.strategy_class.contains("SimpleStrategy"));
        assert_eq!(rep.options.get("replication_factor").unwrap(), "3");
    }

    #[test]
    fn find_indexed_table() {
        use crate::index::{IndexKind, IndexMetadata};
        use std::collections::HashMap;

        let mut opts = HashMap::new();
        opts.insert("target".into(), "email".into());
        let idx = IndexMetadata::new("id1".into(), "email_idx".into(), IndexKind::Keys, opts);
        let table = TableMetadataBuilder::new("ks", "users")
            .add_column(ColumnMetadata::partition_key("id", 0, CqlType::Int))
            .add_index(idx)
            .build();
        let ks = KeyspaceMetadata::new("ks", KeyspaceParams::default()).with_table(table);

        assert_eq!(ks.find_indexed_table("email_idx"), Some("users"));
        assert_eq!(ks.find_indexed_table("nonexistent"), None);
    }

    #[test]
    fn with_and_without_table_index() {
        use crate::index::{IndexKind, IndexMetadata};
        use std::collections::HashMap;

        let table = TableMetadataBuilder::new("ks", "users")
            .add_column(ColumnMetadata::partition_key("id", 0, CqlType::Int))
            .build();
        let ks = KeyspaceMetadata::new("ks", KeyspaceParams::default()).with_table(table);

        let idx = IndexMetadata::new(
            "id1".into(),
            "name_idx".into(),
            IndexKind::Keys,
            HashMap::new(),
        );
        let ks2 = ks.clone().with_table_index("users", idx);
        assert!(ks2.table("users").unwrap().index("name_idx").is_some());
        // original unchanged
        assert!(ks.table("users").unwrap().index("name_idx").is_none());

        let ks3 = ks2.without_table_index("users", "name_idx");
        assert!(ks3.table("users").unwrap().index("name_idx").is_none());
    }
}
