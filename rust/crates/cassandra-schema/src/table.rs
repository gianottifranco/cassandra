// Licensed under Apache License, Version 2.0.

//! Table metadata.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.schema.TableMetadata`

use crate::column::{ColumnKind, ColumnMetadata};
use crate::index::IndexMetadata;
use crate::table_id::TableId;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Flags on a table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TableFlag {
    Super,
    Counter,
    Dense,
    Compound,
}

/// Transactional logic routing mode for a table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
#[derive(Default)]
pub enum TransactionalMode {
    /// No strict serializable transactions.
    #[default]
    Off,
    /// Traditional Paxos LWT.
    Paxos,
    /// Accord distributed transactions.
    Accord,
    /// Migration mode.
    Mixed,
}

/// Table-level parameters.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TableParams {
    #[serde(default = "default_gc_grace")]
    pub gc_grace_seconds: i32,
    #[serde(default)]
    pub default_time_to_live: i32,
    #[serde(default = "default_bloom_fp")]
    pub bloom_filter_fp_chance: f64,
    #[serde(default = "default_min_index_interval")]
    pub min_index_interval: i32,
    #[serde(default = "default_max_index_interval")]
    pub max_index_interval: i32,
    #[serde(default = "default_crc_check_chance")]
    pub crc_check_chance: f64,
    #[serde(default)]
    pub comment: String,
    #[serde(default)]
    pub compaction: BTreeMap<String, String>,
    #[serde(default)]
    pub compression: BTreeMap<String, String>,
    #[serde(default)]
    pub transactional_mode: TransactionalMode,
}

fn default_gc_grace() -> i32 {
    864_000
} // 10 days
fn default_bloom_fp() -> f64 {
    0.01
}
fn default_min_index_interval() -> i32 {
    128
}
fn default_max_index_interval() -> i32 {
    2048
}
fn default_crc_check_chance() -> f64 {
    1.0
}

impl Default for TableParams {
    fn default() -> Self {
        Self {
            gc_grace_seconds: default_gc_grace(),
            default_time_to_live: 0,
            bloom_filter_fp_chance: default_bloom_fp(),
            min_index_interval: default_min_index_interval(),
            max_index_interval: default_max_index_interval(),
            crc_check_chance: default_crc_check_chance(),
            comment: String::new(),
            compaction: BTreeMap::new(),
            compression: BTreeMap::new(),
            transactional_mode: TransactionalMode::default(),
        }
    }
}

/// Metadata for a table.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TableMetadata {
    pub keyspace: String,
    pub name: String,
    pub id: TableId,
    pub columns: Vec<ColumnMetadata>,
    pub indexes: Vec<IndexMetadata>,
    pub flags: Vec<TableFlag>,
    pub params: TableParams,
}

impl TableMetadata {
    /// Get partition key columns, sorted by position.
    pub fn partition_key_columns(&self) -> Vec<&ColumnMetadata> {
        let mut cols: Vec<_> = self
            .columns
            .iter()
            .filter(|c| c.kind == ColumnKind::PartitionKey)
            .collect();
        cols.sort_by_key(|c| c.position);
        cols
    }

    /// Get clustering columns, sorted by position.
    pub fn clustering_columns(&self) -> Vec<&ColumnMetadata> {
        let mut cols: Vec<_> = self
            .columns
            .iter()
            .filter(|c| c.kind == ColumnKind::Clustering)
            .collect();
        cols.sort_by_key(|c| c.position);
        cols
    }

    /// Get regular columns.
    pub fn regular_columns(&self) -> Vec<&ColumnMetadata> {
        self.columns
            .iter()
            .filter(|c| c.kind == ColumnKind::Regular)
            .collect()
    }

    /// Get static columns.
    pub fn static_columns(&self) -> Vec<&ColumnMetadata> {
        self.columns
            .iter()
            .filter(|c| c.kind == ColumnKind::Static)
            .collect()
    }

    /// Look up a column by name.
    pub fn column(&self, name: &str) -> Option<&ColumnMetadata> {
        self.columns.iter().find(|c| c.name == name)
    }

    /// Full qualified name: `keyspace.table`.
    pub fn full_name(&self) -> String {
        format!("{}.{}", self.keyspace, self.name)
    }

    /// Returns `true` if this is a counter table.
    pub fn is_counter(&self) -> bool {
        self.flags.contains(&TableFlag::Counter)
    }

    /// Look up an index by name.
    pub fn index(&self, name: &str) -> Option<&IndexMetadata> {
        self.indexes.iter().find(|i| i.name == name)
    }

    /// Return a new `TableMetadata` with the given index added.
    pub fn with_index(mut self, idx: IndexMetadata) -> Self {
        self.indexes.push(idx);
        self
    }

    /// Return a new `TableMetadata` with the named index removed.
    pub fn without_index(mut self, name: &str) -> Self {
        self.indexes.retain(|i| i.name != name);
        self
    }
}

/// Builder for constructing `TableMetadata`.
pub struct TableMetadataBuilder {
    keyspace: String,
    name: String,
    id: TableId,
    columns: Vec<ColumnMetadata>,
    indexes: Vec<IndexMetadata>,
    flags: Vec<TableFlag>,
    params: TableParams,
}

impl TableMetadataBuilder {
    pub fn new(keyspace: impl Into<String>, name: impl Into<String>) -> Self {
        let ks = keyspace.into();
        let n = name.into();
        let id = TableId::for_system_table(&ks, &n);
        Self {
            keyspace: ks,
            name: n,
            id,
            columns: Vec::new(),
            indexes: Vec::new(),
            flags: vec![TableFlag::Compound],
            params: TableParams::default(),
        }
    }

    pub fn id(mut self, id: TableId) -> Self {
        self.id = id;
        self
    }
    pub fn add_column(mut self, col: ColumnMetadata) -> Self {
        self.columns.push(col);
        self
    }
    pub fn add_index(mut self, idx: IndexMetadata) -> Self {
        self.indexes.push(idx);
        self
    }
    pub fn flags(mut self, flags: Vec<TableFlag>) -> Self {
        self.flags = flags;
        self
    }
    pub fn params(mut self, params: TableParams) -> Self {
        self.params = params;
        self
    }
    pub fn gc_grace(mut self, seconds: i32) -> Self {
        self.params.gc_grace_seconds = seconds;
        self
    }
    pub fn comment(mut self, c: impl Into<String>) -> Self {
        self.params.comment = c.into();
        self
    }

    pub fn build(self) -> TableMetadata {
        TableMetadata {
            keyspace: self.keyspace,
            name: self.name,
            id: self.id,
            columns: self.columns,
            indexes: self.indexes,
            flags: self.flags,
            params: self.params,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::column::ClusteringOrder;
    use cassandra_types::CqlType;

    fn sample_table() -> TableMetadata {
        TableMetadataBuilder::new("test_ks", "users")
            .add_column(ColumnMetadata::partition_key("user_id", 0, CqlType::Uuid))
            .add_column(ColumnMetadata::clustering(
                "created_at",
                0,
                CqlType::Timestamp,
                ClusteringOrder::Desc,
            ))
            .add_column(ColumnMetadata::regular("name", CqlType::Varchar))
            .add_column(ColumnMetadata::regular("email", CqlType::Varchar))
            .add_column(ColumnMetadata::static_col("account_type", CqlType::Varchar))
            .gc_grace(86400)
            .comment("User accounts")
            .build()
    }

    #[test]
    fn partition_key() {
        let t = sample_table();
        let pk = t.partition_key_columns();
        assert_eq!(pk.len(), 1);
        assert_eq!(pk[0].name, "user_id");
    }

    #[test]
    fn clustering() {
        let t = sample_table();
        let ck = t.clustering_columns();
        assert_eq!(ck.len(), 1);
        assert_eq!(ck[0].name, "created_at");
    }

    #[test]
    fn column_lookup() {
        let t = sample_table();
        assert!(t.column("name").is_some());
        assert!(t.column("nonexistent").is_none());
    }

    #[test]
    fn full_name() {
        let t = sample_table();
        assert_eq!(t.full_name(), "test_ks.users");
    }

    #[test]
    fn static_columns() {
        let t = sample_table();
        assert_eq!(t.static_columns().len(), 1);
    }

    #[test]
    fn with_and_without_index() {
        use crate::index::{IndexKind, IndexMetadata};
        use std::collections::HashMap;

        let t = sample_table();
        assert!(t.indexes.is_empty());

        let idx = IndexMetadata::new(
            "id1".into(),
            "email_idx".into(),
            IndexKind::Keys,
            HashMap::new(),
        );
        let t2 = t.clone().with_index(idx);
        assert_eq!(t2.indexes.len(), 1);
        assert!(t2.index("email_idx").is_some());
        // original unchanged
        assert!(t.indexes.is_empty());

        let t3 = t2.without_index("email_idx");
        assert!(t3.indexes.is_empty());
    }
}
