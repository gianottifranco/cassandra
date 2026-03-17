// Licensed under Apache License, Version 2.0.

//! # Materialized Views
//!
//! ## Status: ACTIVE — write-path integration via feature flag
//!
//! Materialized views maintain secondary representations of base table data.
//! When a base table is mutated, the view manager computes delta mutations
//! for each affected view and applies them.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.db.view.View`
//! - `org.apache.cassandra.db.view.ViewManager`
//! - `org.apache.cassandra.db.view.ViewUpdateGenerator`
//!
//! ## Architecture
//!
//! 1. `ViewManager` stores view definitions per keyspace/table
//! 2. On write, coordinator calls `generate_view_updates()`
//! 3. `ViewUpdateGenerator` diffs old vs new partition state
//! 4. Generated view mutations are applied at CL=ONE (best effort)
//!
//! ## Gap Documentation
//!
//! - **Partial**: Full MV builder (backfill) is TODO
//! - **Partial**: View read-repair integration is TODO
//! - **Active**: Write-path fanout is implemented
//! - Upstream has deprecated MVs. This implementation preserves baseline behavior.

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

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
    /// Whether the view includes all columns from the base table.
    pub include_all_columns: bool,
    /// View primary key columns (for PK recomputation from base row).
    #[serde(default)]
    pub view_pk_columns: Vec<String>,
}

/// A generated view mutation from a base table write.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ViewMutation {
    /// View keyspace.
    pub keyspace: String,
    /// View table name.
    pub view_table: String,
    /// Partition key for the view (may differ from base).
    pub partition_key: Vec<u8>,
    /// Whether this is a delete (tombstone) in the view.
    pub is_delete: bool,
    /// Column values to write/update in the view.
    pub columns: HashMap<String, Option<Vec<u8>>>,
    /// Timestamp for the view mutation.
    pub timestamp: i64,
}

/// Result of view update generation.
#[derive(Debug)]
pub struct ViewUpdateResult {
    /// Mutations to apply to views.
    pub mutations: Vec<ViewMutation>,
    /// Whether any view update failed validation.
    pub had_errors: bool,
}

/// Type alias for the views index: (keyspace, base_table) → [view definitions].
type ViewIndex = HashMap<(String, String), Vec<MaterializedViewDefinition>>;

/// Manages materialized view definitions and generates view updates.
///
/// ## Java Oracle
///
/// `org.apache.cassandra.db.view.ViewManager`
pub struct ViewManager {
    /// Views indexed by (keyspace, base_table) → [view definitions].
    views: Arc<RwLock<ViewIndex>>,
}

impl ViewManager {
    pub fn new() -> Self {
        Self {
            views: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Register a view definition.
    pub fn register(&self, def: MaterializedViewDefinition) -> Result<(), String> {
        let key = (def.keyspace.clone(), def.base_table.clone());
        let mut views = self.views.write();
        let entry = views.entry(key).or_default();

        // Check for duplicate
        if entry.iter().any(|v| v.name == def.name) {
            return Err(format!("View '{}' already exists", def.name));
        }

        info!(
            view = %def.name,
            keyspace = %def.keyspace,
            base = %def.base_table,
            "Registered materialized view"
        );

        entry.push(def);
        Ok(())
    }

    /// Unregister a view by name.
    pub fn unregister(&self, keyspace: &str, view_name: &str) -> bool {
        let mut views = self.views.write();
        let mut found = false;

        for entry in views.values_mut() {
            let before = entry.len();
            entry.retain(|v| !(v.keyspace == keyspace && v.name == view_name));
            if entry.len() < before {
                found = true;
            }
        }

        // Remove empty entries
        views.retain(|_, v| !v.is_empty());
        found
    }

    /// Check if any views exist for a base table.
    pub fn has_views_for(&self, keyspace: &str, table: &str) -> bool {
        self.views
            .read()
            .get(&(keyspace.to_string(), table.to_string()))
            .is_some_and(|v| !v.is_empty())
    }

    /// Get view definitions for a base table.
    pub fn get_views_for(&self, keyspace: &str, table: &str) -> Vec<MaterializedViewDefinition> {
        self.views
            .read()
            .get(&(keyspace.to_string(), table.to_string()))
            .cloned()
            .unwrap_or_default()
    }

    /// Generate view mutations for a base table write.
    ///
    /// This is the write-path hook called after applying the base
    /// table mutation locally.
    ///
    /// ## Java Oracle
    ///
    /// `ViewUpdateGenerator.generateViewUpdates()`
    ///
    /// ## Arguments
    /// - `keyspace`: base table keyspace
    /// - `table`: base table name
    /// - `partition_key`: base partition key
    /// - `columns`: column name → value from the base mutation
    /// - `timestamp`: mutation timestamp
    /// - `is_delete`: whether this is a deletion
    pub fn generate_view_updates(
        &self,
        keyspace: &str,
        table: &str,
        partition_key: &[u8],
        columns: &HashMap<String, Option<Vec<u8>>>,
        timestamp: i64,
        is_delete: bool,
    ) -> ViewUpdateResult {
        let view_defs = self.get_views_for(keyspace, table);

        if view_defs.is_empty() {
            return ViewUpdateResult {
                mutations: Vec::new(),
                had_errors: false,
            };
        }

        let mut mutations = Vec::new();
        let mut had_errors = false;

        for view_def in &view_defs {
            match self.generate_single_view_update(
                view_def,
                partition_key,
                columns,
                timestamp,
                is_delete,
            ) {
                Ok(mutation) => {
                    if let Some(m) = mutation {
                        mutations.push(m);
                    }
                }
                Err(e) => {
                    warn!(
                        view = %view_def.name,
                        error = %e,
                        "Failed to generate view update"
                    );
                    had_errors = true;
                }
            }
        }

        debug!(
            keyspace = %keyspace,
            table = %table,
            view_updates = mutations.len(),
            "Generated view mutations"
        );

        ViewUpdateResult {
            mutations,
            had_errors,
        }
    }

    /// Generate a mutation for a single view definition.
    fn generate_single_view_update(
        &self,
        view_def: &MaterializedViewDefinition,
        partition_key: &[u8],
        columns: &HashMap<String, Option<Vec<u8>>>,
        timestamp: i64,
        is_delete: bool,
    ) -> Result<Option<ViewMutation>, String> {
        // Evaluate WHERE clause filter
        if !is_delete && !evaluate_where_clause(&view_def.where_clause, columns) {
            return Ok(None);
        }

        // Filter columns included in the view
        let view_columns: HashMap<String, Option<Vec<u8>>> = if view_def.include_all_columns {
            columns.clone()
        } else {
            columns
                .iter()
                .filter(|(k, _)| view_def.included_columns.contains(k))
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect()
        };

        // If no relevant columns changed, skip this view
        if view_columns.is_empty() && !is_delete {
            return Ok(None);
        }

        // Compute view partition key from view_pk_columns if specified
        let view_pk = compute_view_pk(
            &view_def.view_pk_columns,
            columns,
            partition_key,
        );

        Ok(Some(ViewMutation {
            keyspace: view_def.keyspace.clone(),
            view_table: view_def.view_table.clone(),
            partition_key: view_pk,
            is_delete,
            columns: view_columns,
            timestamp,
        }))
    }

    /// Total number of view definitions.
    pub fn view_count(&self) -> usize {
        self.views.read().values().map(|v| v.len()).sum()
    }
}

impl Default for ViewManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Evaluate a WHERE clause against a set of column values.
///
/// Supports two predicate forms:
/// - `column_name IS NOT NULL` — passes if column exists and has a non-None value
/// - `column_name = 'literal'` — passes if column value matches the literal bytes
///
/// Multiple predicates separated by `AND` must all pass.
/// An empty WHERE clause always passes.
pub fn evaluate_where_clause(
    where_clause: &str,
    columns: &HashMap<String, Option<Vec<u8>>>,
) -> bool {
    let clause = where_clause.trim();
    if clause.is_empty() {
        return true;
    }

    // Split on AND (case-insensitive)
    for predicate in clause.split(" AND ") {
        let predicate = predicate.trim();
        if predicate.is_empty() {
            continue;
        }

        // IS NOT NULL predicate
        if let Some(col_name) = predicate
            .strip_suffix(" IS NOT NULL")
            .or_else(|| predicate.strip_suffix(" is not null"))
        {
            let col_name = col_name.trim();
            match columns.get(col_name) {
                Some(Some(_)) => continue, // has a non-null value
                _ => return false,
            }
        }

        // Equality predicate: column = 'value'
        if let Some((col_part, val_part)) = predicate.split_once('=') {
            let col_name = col_part.trim();
            let val_str = val_part.trim().trim_matches('\'');

            match columns.get(col_name) {
                Some(Some(bytes)) => {
                    if bytes != val_str.as_bytes() {
                        return false;
                    }
                }
                _ => return false,
            }
            continue;
        }
    }

    true
}

/// Compute the view partition key from the specified view PK columns.
///
/// If `view_pk_columns` is empty, falls back to the base partition key.
/// Otherwise, concatenates the byte values of each PK column from the mutation.
fn compute_view_pk(
    view_pk_columns: &[String],
    columns: &HashMap<String, Option<Vec<u8>>>,
    base_pk: &[u8],
) -> Vec<u8> {
    if view_pk_columns.is_empty() {
        return base_pk.to_vec();
    }

    let mut pk = Vec::new();
    for col in view_pk_columns {
        if let Some(Some(val)) = columns.get(col) {
            pk.extend_from_slice(val);
        } else {
            // If a PK column is missing, fall back to base PK
            return base_pk.to_vec();
        }
    }
    pk
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_view_def() -> MaterializedViewDefinition {
        MaterializedViewDefinition {
            name: "users_by_email".to_string(),
            keyspace: "ks".to_string(),
            base_table: "users".to_string(),
            view_table: "users_by_email".to_string(),
            included_columns: vec!["email".to_string(), "name".to_string()],
            where_clause: "email IS NOT NULL".to_string(),
            include_all_columns: false,
            view_pk_columns: Vec::new(),
        }
    }

    #[test]
    fn register_and_lookup() {
        let mgr = ViewManager::new();
        assert!(mgr.register(test_view_def()).is_ok());
        assert!(mgr.has_views_for("ks", "users"));
        assert!(!mgr.has_views_for("ks", "other"));
        assert_eq!(mgr.view_count(), 1);
    }

    #[test]
    fn duplicate_registration_fails() {
        let mgr = ViewManager::new();
        assert!(mgr.register(test_view_def()).is_ok());
        assert!(mgr.register(test_view_def()).is_err());
    }

    #[test]
    fn unregister_view() {
        let mgr = ViewManager::new();
        mgr.register(test_view_def()).unwrap();
        assert!(mgr.unregister("ks", "users_by_email"));
        assert!(!mgr.has_views_for("ks", "users"));
        assert_eq!(mgr.view_count(), 0);
    }

    #[test]
    fn generate_view_updates_insert() {
        let mgr = ViewManager::new();
        mgr.register(test_view_def()).unwrap();

        let mut columns = HashMap::new();
        columns.insert("email".to_string(), Some(b"alice@example.com".to_vec()));
        columns.insert("name".to_string(), Some(b"Alice".to_vec()));
        columns.insert("age".to_string(), Some(b"30".to_vec())); // not in view

        let result = mgr.generate_view_updates("ks", "users", b"user1", &columns, 1000, false);

        assert!(!result.had_errors);
        assert_eq!(result.mutations.len(), 1);

        let vm = &result.mutations[0];
        assert_eq!(vm.view_table, "users_by_email");
        assert!(!vm.is_delete);
        // Only included columns should be present
        assert!(vm.columns.contains_key("email"));
        assert!(vm.columns.contains_key("name"));
        assert!(!vm.columns.contains_key("age"));
    }

    #[test]
    fn generate_view_updates_delete() {
        let mgr = ViewManager::new();
        mgr.register(test_view_def()).unwrap();

        let columns = HashMap::new();
        let result = mgr.generate_view_updates("ks", "users", b"user1", &columns, 1000, true);

        assert_eq!(result.mutations.len(), 1);
        assert!(result.mutations[0].is_delete);
    }

    #[test]
    fn no_views_returns_empty() {
        let mgr = ViewManager::new();

        let columns = HashMap::new();
        let result = mgr.generate_view_updates("ks", "users", b"user1", &columns, 1000, false);

        assert!(result.mutations.is_empty());
    }

    #[test]
    fn get_views_for() {
        let mgr = ViewManager::new();
        mgr.register(test_view_def()).unwrap();

        let views = mgr.get_views_for("ks", "users");
        assert_eq!(views.len(), 1);
        assert_eq!(views[0].name, "users_by_email");
    }

    #[test]
    fn include_all_columns() {
        let mgr = ViewManager::new();
        let mut def = test_view_def();
        def.name = "users_full".to_string();
        def.view_table = "users_full".to_string();
        def.include_all_columns = true;
        mgr.register(def).unwrap();

        let mut columns = HashMap::new();
        columns.insert("email".to_string(), Some(b"alice@example.com".to_vec()));
        columns.insert("age".to_string(), Some(b"30".to_vec()));

        let result = mgr.generate_view_updates("ks", "users", b"user1", &columns, 1000, false);

        let vm = &result.mutations[0];
        assert!(vm.columns.contains_key("email"));
        assert!(vm.columns.contains_key("age"));
    }

    // ─── WHERE clause evaluation tests ────────────────────────────────

    #[test]
    fn where_clause_is_not_null_passes() {
        let mut cols = HashMap::new();
        cols.insert("email".to_string(), Some(b"a@b.com".to_vec()));
        assert!(evaluate_where_clause("email IS NOT NULL", &cols));
    }

    #[test]
    fn where_clause_is_not_null_fails_missing() {
        let cols = HashMap::new();
        assert!(!evaluate_where_clause("email IS NOT NULL", &cols));
    }

    #[test]
    fn where_clause_is_not_null_fails_null() {
        let mut cols = HashMap::new();
        cols.insert("email".to_string(), None);
        assert!(!evaluate_where_clause("email IS NOT NULL", &cols));
    }

    #[test]
    fn where_clause_equality_passes() {
        let mut cols = HashMap::new();
        cols.insert("status".to_string(), Some(b"active".to_vec()));
        assert!(evaluate_where_clause("status = 'active'", &cols));
    }

    #[test]
    fn where_clause_equality_fails() {
        let mut cols = HashMap::new();
        cols.insert("status".to_string(), Some(b"inactive".to_vec()));
        assert!(!evaluate_where_clause("status = 'active'", &cols));
    }

    #[test]
    fn where_clause_empty_passes() {
        let cols = HashMap::new();
        assert!(evaluate_where_clause("", &cols));
    }

    #[test]
    fn where_clause_multiple_predicates() {
        let mut cols = HashMap::new();
        cols.insert("email".to_string(), Some(b"a@b.com".to_vec()));
        cols.insert("status".to_string(), Some(b"active".to_vec()));
        assert!(evaluate_where_clause(
            "email IS NOT NULL AND status = 'active'",
            &cols
        ));
    }

    #[test]
    fn where_clause_filters_view_update() {
        let mgr = ViewManager::new();
        mgr.register(test_view_def()).unwrap();

        // email is null → should be filtered out by WHERE email IS NOT NULL
        let mut columns = HashMap::new();
        columns.insert("email".to_string(), None);
        columns.insert("name".to_string(), Some(b"Alice".to_vec()));

        let result = mgr.generate_view_updates("ks", "users", b"user1", &columns, 1000, false);
        assert!(result.mutations.is_empty());
    }

    // ─── PK recomputation tests ───────────────────────────────────────

    #[test]
    fn view_pk_from_columns() {
        let pk = compute_view_pk(
            &["email".to_string()],
            &{
                let mut m = HashMap::new();
                m.insert("email".to_string(), Some(b"a@b.com".to_vec()));
                m
            },
            b"base_pk",
        );
        assert_eq!(pk, b"a@b.com");
    }

    #[test]
    fn view_pk_fallback_to_base() {
        let pk = compute_view_pk(
            &["missing_col".to_string()],
            &HashMap::new(),
            b"base_pk",
        );
        assert_eq!(pk, b"base_pk");
    }

    #[test]
    fn view_pk_empty_columns_uses_base() {
        let pk = compute_view_pk(&[], &HashMap::new(), b"base_pk");
        assert_eq!(pk, b"base_pk");
    }

    #[test]
    fn view_pk_concatenates_multiple() {
        let mut cols = HashMap::new();
        cols.insert("a".to_string(), Some(b"X".to_vec()));
        cols.insert("b".to_string(), Some(b"Y".to_vec()));
        let pk = compute_view_pk(
            &["a".to_string(), "b".to_string()],
            &cols,
            b"base",
        );
        assert_eq!(pk, b"XY");
    }

    #[test]
    fn view_with_pk_recomputation() {
        let mgr = ViewManager::new();
        let mut def = test_view_def();
        def.name = "users_by_email_pk".to_string();
        def.view_table = "users_by_email_pk".to_string();
        def.view_pk_columns = vec!["email".to_string()];
        mgr.register(def).unwrap();

        let mut columns = HashMap::new();
        columns.insert("email".to_string(), Some(b"alice@example.com".to_vec()));
        columns.insert("name".to_string(), Some(b"Alice".to_vec()));

        let result = mgr.generate_view_updates("ks", "users", b"base_pk", &columns, 1000, false);
        assert_eq!(result.mutations.len(), 1);
        assert_eq!(result.mutations[0].partition_key, b"alice@example.com");
    }
}
