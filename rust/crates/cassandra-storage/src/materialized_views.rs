// Licensed under Apache License, Version 2.0.

//! # Materialized Views (Stub)
//!
//! ## Status: DEFERRED — behind `materialized-views` feature flag
//!
//! Materialized views are deprecated upstream in Apache Cassandra.
//! This module provides type definitions and stubs only.
//!
//! ## Java Oracle
//!
//! `org.apache.cassandra.db.view.View`
//! `org.apache.cassandra.db.view.ViewManager`
//!
//! ## Gap Documentation
//!
//! - **What's missing**: Full MV maintenance (base table → view table
//!   mutation propagation), view builder, read repair for views,
//!   view selection on coordinator path.
//! - **Why deferred**: Upstream has deprecated MVs. Community consensus
//!   is to not invest further; SAI + denormalization is preferred.
//! - **Closure path**: If MV support is required, implement view mutation
//!   propagation as a write-path hook in the storage engine, similar to
//!   index maintenance. Estimated effort: 2-3 weeks.
//!
//! ## Usage
//!
//! ```toml
//! [dependencies]
//! cassandra-storage = { path = ".", features = ["materialized-views"] }
//! ```

use serde::{Deserialize, Serialize};

/// Definition of a materialized view.
///
/// Mirrors `org.apache.cassandra.schema.ViewMetadata`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MaterializedViewDefinition {
    /// View name.
    pub name: String,
    /// Keyspace of both base table and view.
    pub keyspace: String,
    /// Base table name.
    pub base_table: String,
    /// View table name (auto-generated).
    pub view_table: String,
    /// Columns included in the view.
    pub included_columns: Vec<String>,
    /// WHERE clause filter (as CQL text).
    pub where_clause: String,
}

/// Stub view manager. All operations return errors.
#[derive(Debug, Default)]
pub struct ViewManager {
    _definitions: Vec<MaterializedViewDefinition>,
}

impl ViewManager {
    pub fn new() -> Self {
        Self {
            _definitions: Vec::new(),
        }
    }

    /// Register a view definition. Stub — always returns error.
    pub fn register(&mut self, _def: MaterializedViewDefinition) -> Result<(), String> {
        Err("Materialized views are not implemented. Feature is deferred (upstream deprecated).".to_string())
    }

    /// Check if any views exist for a base table.
    pub fn has_views_for(&self, _keyspace: &str, _table: &str) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn view_manager_stub() {
        let mut mgr = ViewManager::new();
        let def = MaterializedViewDefinition {
            name: "mv_test".to_string(),
            keyspace: "ks".to_string(),
            base_table: "users".to_string(),
            view_table: "users_by_email".to_string(),
            included_columns: vec!["email".to_string(), "name".to_string()],
            where_clause: "email IS NOT NULL".to_string(),
        };
        assert!(mgr.register(def).is_err());
        assert!(!mgr.has_views_for("ks", "users"));
    }
}
