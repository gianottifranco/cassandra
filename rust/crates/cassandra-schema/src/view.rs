// Licensed under Apache License, Version 2.0.

//! Materialized view metadata.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.schema.ViewMetadata`

use std::collections::BTreeMap;

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
    /// Table options attached to the materialized view.
    pub options: BTreeMap<String, String>,
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
            options: BTreeMap::new(),
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

    /// Set view table options.
    pub fn with_options<I, K, V>(mut self, options: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        self.options = options
            .into_iter()
            .map(|(key, value)| (key.into(), value.into()))
            .collect();
        self
    }

    /// Return a new `ViewMetadata` with the given option upserted.
    pub fn with_option(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.options.insert(key.into(), value.into());
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
    fn view_options_are_stored() {
        let view = ViewMetadata::new("users_by_email", "ks", "users")
            .with_option("gc_grace_seconds", "3600")
            .with_option("comment", "by email");

        assert_eq!(
            view.options.get("gc_grace_seconds").map(String::as_str),
            Some("3600")
        );
        assert_eq!(
            view.options.get("comment").map(String::as_str),
            Some("by email")
        );
    }

    #[test]
    fn serde_round_trip() {
        let view = ViewMetadata::new("v1", "ks", "t1")
            .with_where_clause("x IS NOT NULL")
            .with_column("x")
            .with_option("comment", "view");
        let json = serde_json::to_string(&view).unwrap();
        let deserialized: ViewMetadata = serde_json::from_str(&json).unwrap();
        assert_eq!(view, deserialized);
    }
}
