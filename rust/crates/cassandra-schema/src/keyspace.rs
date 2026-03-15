// Licensed under Apache License, Version 2.0.

//! Keyspace metadata.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.schema.KeyspaceMetadata`
//! - `org.apache.cassandra.schema.KeyspaceParams`

use std::collections::BTreeMap;
use serde::{Deserialize, Serialize};
use crate::table::TableMetadata;

/// The kind of keyspace.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum KeyspaceKind {
    Regular,
    Virtual,
}

impl Default for KeyspaceKind {
    fn default() -> Self { Self::Regular }
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
        options.insert("replication_factor".to_string(), replication_factor.to_string());
        Self {
            strategy_class: "org.apache.cassandra.locator.SimpleStrategy".to_string(),
            options,
        }
    }

    /// Network topology strategy with per-DC replication factors.
    pub fn network_topology(dc_rfs: BTreeMap<String, u32>) -> Self {
        let options = dc_rfs.into_iter()
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
}

fn default_durable_writes() -> bool { true }

impl Default for KeyspaceParams {
    fn default() -> Self {
        Self {
            replication: ReplicationParams::simple(1),
            durable_writes: true,
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
}

impl KeyspaceMetadata {
    /// Create a new keyspace metadata.
    pub fn new(name: impl Into<String>, params: KeyspaceParams) -> Self {
        Self {
            name: name.into(),
            kind: KeyspaceKind::Regular,
            params,
            tables: BTreeMap::new(),
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
            },
            tables: BTreeMap::new(),
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table::TableMetadataBuilder;
    use crate::column::ColumnMetadata;
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
        assert_eq!(ks.params.replication.strategy_class,
            "org.apache.cassandra.locator.LocalStrategy");
    }

    #[test]
    fn with_table() {
        let table = TableMetadataBuilder::new("test_ks", "users")
            .add_column(ColumnMetadata::partition_key("id", 0, CqlType::Uuid))
            .build();
        let ks = KeyspaceMetadata::new("test_ks", KeyspaceParams::default())
            .with_table(table);
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
}
