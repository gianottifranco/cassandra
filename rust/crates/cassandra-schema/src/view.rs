// Licensed under Apache License, Version 2.0.

//! Materialized view metadata.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.schema.ViewMetadata`

use serde::{Deserialize, Serialize};

/// Metadata for a materialized view.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ViewMetadata {
    /// View name.
    pub name: String,
    /// Keyspace containing the view.
    pub keyspace: String,
    /// Name of the base table.
    pub base_table_name: String,
    /// Whether the view includes all columns from the base table.
    pub include_all_columns: bool,
    /// WHERE clause filter (as CQL text).
    pub where_clause: String,
    /// Column names included in the view.
    pub columns: Vec<String>,
    /// Partition key column names for the view.
    pub partition_key: Vec<String>,
    /// Clustering key column names for the view.
    pub clustering_key: Vec<String>,
}

impl ViewMetadata {
    /// Create a new view metadata.
    pub fn new(
        name: impl Into<String>,
        keyspace: impl Into<String>,
        base_table_name: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            keyspace: keyspace.into(),
            base_table_name: base_table_name.into(),
            include_all_columns: false,
            where_clause: String::new(),
            columns: Vec::new(),
            partition_key: Vec::new(),
            clustering_key: Vec::new(),
        }
    }

    /// Set the WHERE clause.
    pub fn with_where_clause(mut self, clause: impl Into<String>) -> Self {
        self.where_clause = clause.into();
        self
    }

    /// Set include_all_columns.
    pub fn with_include_all_columns(mut self, include_all: bool) -> Self {
        self.include_all_columns = include_all;
        self
    }

    /// Add a column to the view.
    pub fn with_column(mut self, column: impl Into<String>) -> Self {
        self.columns.push(column.into());
        self
    }

    /// Set partition key columns.
    pub fn with_partition_key(mut self, pk: Vec<String>) -> Self {
        self.partition_key = pk;
        self
    }

    /// Set clustering key columns.
    pub fn with_clustering_key(mut self, ck: Vec<String>) -> Self {
        self.clustering_key = ck;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_view_metadata() {
        let view = ViewMetadata::new("users_by_email", "ks", "users")
            .with_where_clause("email IS NOT NULL")
            .with_include_all_columns(false)
            .with_column("email")
            .with_column("name")
            .with_partition_key(vec!["email".into()])
            .with_clustering_key(vec!["id".into()]);

        assert_eq!(view.name, "users_by_email");
        assert_eq!(view.keyspace, "ks");
        assert_eq!(view.base_table_name, "users");
        assert_eq!(view.where_clause, "email IS NOT NULL");
        assert!(!view.include_all_columns);
        assert_eq!(view.columns.len(), 2);
        assert_eq!(view.partition_key, vec!["email"]);
        assert_eq!(view.clustering_key, vec!["id"]);
    }

    #[test]
    fn serde_round_trip() {
        let view = ViewMetadata::new("v1", "ks", "t1")
            .with_where_clause("x IS NOT NULL")
            .with_column("x");
        let json = serde_json::to_string(&view).unwrap();
        let deserialized: ViewMetadata = serde_json::from_str(&json).unwrap();
        assert_eq!(view, deserialized);
    }
}
