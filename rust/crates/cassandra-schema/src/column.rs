// Licensed under Apache License, Version 2.0.

//! Column metadata.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.schema.ColumnMetadata`

use std::fmt;
use serde::{Deserialize, Serialize};
use cassandra_types::CqlType;

/// The kind of column within a table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ColumnKind {
    /// Part of the partition key.
    PartitionKey,
    /// Part of the clustering key.
    Clustering,
    /// A regular column.
    Regular,
    /// A static column (one value per partition).
    Static,
}

impl ColumnKind {
    /// Returns `true` if this column is part of the primary key.
    pub fn is_primary_key(&self) -> bool {
        matches!(self, Self::PartitionKey | Self::Clustering)
    }
}

impl fmt::Display for ColumnKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PartitionKey => write!(f, "partition_key"),
            Self::Clustering => write!(f, "clustering"),
            Self::Regular => write!(f, "regular"),
            Self::Static => write!(f, "static"),
        }
    }
}

/// Clustering order for a clustering column.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ClusteringOrder {
    Asc,
    Desc,
    None,
}

impl Default for ClusteringOrder {
    fn default() -> Self { Self::None }
}

/// Metadata for a single column in a table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnMetadata {
    /// Column name.
    pub name: String,
    /// Column kind (partition_key, clustering, regular, static).
    pub kind: ColumnKind,
    /// Position within its kind group (0-indexed).
    /// For partition key: position in composite key.
    /// For clustering: position in clustering order.
    /// For regular/static: always 0.
    pub position: u32,
    /// CQL type of this column.
    pub column_type: CqlType,
    /// Clustering order (only meaningful for clustering columns).
    #[serde(default)]
    pub clustering_order: ClusteringOrder,
    /// Dynamic Data Masking configuration (function_name, args)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub masked_with: Option<(String, Vec<String>)>,
}

impl ColumnMetadata {
    /// Create a new column metadata.
    pub fn new(
        name: String,
        kind: ColumnKind,
        position: u32,
        column_type: CqlType,
        clustering_order: ClusteringOrder,
        masked_with: Option<(String, Vec<String>)>,
    ) -> Self {
        Self { name, kind, position, column_type, clustering_order, masked_with }
    }

    /// Convenience: create a partition key column.
    pub fn partition_key(name: impl Into<String>, position: u32, cql_type: CqlType) -> Self {
        Self::new(name.into(), ColumnKind::PartitionKey, position, cql_type, ClusteringOrder::None, None)
    }

    /// Convenience: create a clustering column.
    pub fn clustering(name: impl Into<String>, position: u32, cql_type: CqlType, order: ClusteringOrder) -> Self {
        Self::new(name.into(), ColumnKind::Clustering, position, cql_type, order, None)
    }

    /// Convenience: create a regular column.
    pub fn regular(name: impl Into<String>, cql_type: CqlType) -> Self {
        Self::new(name.into(), ColumnKind::Regular, 0, cql_type, ClusteringOrder::None, None)
    }

    /// Convenience: create a static column.
    pub fn static_col(name: impl Into<String>, cql_type: CqlType) -> Self {
        Self::new(name.into(), ColumnKind::Static, 0, cql_type, ClusteringOrder::None, None)
    }

    /// Add masking configuration to this column.
    pub fn masked_with(mut self, function_name: String, args: Vec<String>) -> Self {
        self.masked_with = Some((function_name, args));
        self
    }

    /// Returns `true` if this column is part of the primary key.
    pub fn is_primary_key(&self) -> bool {
        self.kind.is_primary_key()
    }
}

impl fmt::Display for ColumnMetadata {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {} ({})", self.name, self.column_type, self.kind)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partition_key_column() {
        let col = ColumnMetadata::partition_key("id", 0, CqlType::Uuid);
        assert!(col.is_primary_key());
        assert_eq!(col.kind, ColumnKind::PartitionKey);
        assert_eq!(col.position, 0);
    }

    #[test]
    fn clustering_column() {
        let col = ColumnMetadata::clustering("ts", 0, CqlType::Timestamp, ClusteringOrder::Desc);
        assert!(col.is_primary_key());
        assert_eq!(col.clustering_order, ClusteringOrder::Desc);
    }

    #[test]
    fn regular_column() {
        let col = ColumnMetadata::regular("value", CqlType::Varchar);
        assert!(!col.is_primary_key());
        assert_eq!(col.kind, ColumnKind::Regular);
    }

    #[test]
    fn display() {
        let col = ColumnMetadata::partition_key("user_id", 0, CqlType::Uuid);
        let s = col.to_string();
        assert!(s.contains("user_id"));
        assert!(s.contains("uuid"));
    }
}
