// Licensed under Apache License, Version 2.0.

//! View builder for backfilling materialized views from base table data.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.db.view.ViewBuilder`
//! - `org.apache.cassandra.db.view.ViewManager.buildAllViews()`
//!
//! ## Architecture
//!
//! The ViewBuilder scans all partitions of a base table and generates
//! view mutations for each row. This is used when a new materialized
//! view is created on an existing table with data.

use std::collections::HashMap;

use tracing::info;

use crate::materialized_views::{ViewManager, ViewMutation};

/// Result of a view build operation.
#[derive(Debug)]
pub struct ViewBuildResult {
    /// Total rows processed from the base table.
    pub rows_processed: usize,
    /// Total view mutations generated.
    pub mutations_generated: usize,
    /// Errors encountered during build.
    pub errors: Vec<String>,
}

/// Builds a materialized view by scanning base table data.
pub struct ViewBuilder<'a> {
    view_manager: &'a ViewManager,
}

impl<'a> ViewBuilder<'a> {
    /// Create a new view builder.
    pub fn new(view_manager: &'a ViewManager) -> Self {
        Self { view_manager }
    }

    /// Build view mutations from a set of base table rows.
    ///
    /// Each row is represented as (partition_key, columns, timestamp).
    /// Returns all generated view mutations and a build result summary.
    pub fn build_from_rows(
        &self,
        keyspace: &str,
        table: &str,
        rows: &[(Vec<u8>, HashMap<String, Option<Vec<u8>>>, i64)],
    ) -> (Vec<ViewMutation>, ViewBuildResult) {
        let mut all_mutations = Vec::new();
        let mut errors = Vec::new();
        let mut rows_processed = 0;

        for (pk, columns, timestamp) in rows {
            rows_processed += 1;

            let result = self
                .view_manager
                .generate_view_updates(keyspace, table, pk, columns, *timestamp, false);

            if result.had_errors {
                errors.push(format!(
                    "Error generating view updates for row with pk {:?}",
                    pk
                ));
            }

            all_mutations.extend(result.mutations);
        }

        info!(
            keyspace = %keyspace,
            table = %table,
            rows_processed,
            mutations = all_mutations.len(),
            errors = errors.len(),
            "View build complete"
        );

        let build_result = ViewBuildResult {
            rows_processed,
            mutations_generated: all_mutations.len(),
            errors,
        };

        (all_mutations, build_result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::materialized_views::MaterializedViewDefinition;

    fn test_view_def() -> MaterializedViewDefinition {
        MaterializedViewDefinition {
            name: "users_by_email".to_string(),
            keyspace: "ks".to_string(),
            base_table: "users".to_string(),
            view_table: "users_by_email".to_string(),
            included_columns: vec!["email".to_string(), "name".to_string()],
            where_clause: "email IS NOT NULL".to_string(),
            include_all_columns: false,
            view_pk_columns: vec!["email".to_string()],
        }
    }

    #[test]
    fn build_from_rows_basic() {
        let mgr = ViewManager::new();
        mgr.register(test_view_def()).unwrap();

        let builder = ViewBuilder::new(&mgr);
        let rows = vec![
            (
                b"pk1".to_vec(),
                {
                    let mut m = HashMap::new();
                    m.insert("email".to_string(), Some(b"a@b.com".to_vec()));
                    m.insert("name".to_string(), Some(b"Alice".to_vec()));
                    m
                },
                1000i64,
            ),
            (
                b"pk2".to_vec(),
                {
                    let mut m = HashMap::new();
                    m.insert("email".to_string(), Some(b"b@c.com".to_vec()));
                    m.insert("name".to_string(), Some(b"Bob".to_vec()));
                    m
                },
                2000i64,
            ),
        ];

        let (mutations, result) = builder.build_from_rows("ks", "users", &rows);
        assert_eq!(result.rows_processed, 2);
        assert_eq!(result.mutations_generated, 2);
        assert!(result.errors.is_empty());
        assert_eq!(mutations.len(), 2);
        // PK should be recomputed from email
        assert_eq!(mutations[0].partition_key, b"a@b.com");
        assert_eq!(mutations[1].partition_key, b"b@c.com");
    }

    #[test]
    fn build_from_rows_filters_by_where() {
        let mgr = ViewManager::new();
        mgr.register(test_view_def()).unwrap();

        let builder = ViewBuilder::new(&mgr);
        let rows = vec![
            (
                b"pk1".to_vec(),
                {
                    let mut m = HashMap::new();
                    m.insert("email".to_string(), Some(b"a@b.com".to_vec()));
                    m.insert("name".to_string(), Some(b"Alice".to_vec()));
                    m
                },
                1000i64,
            ),
            (
                b"pk2".to_vec(),
                {
                    let mut m = HashMap::new();
                    m.insert("email".to_string(), None); // email is null, should be filtered
                    m.insert("name".to_string(), Some(b"Bob".to_vec()));
                    m
                },
                2000i64,
            ),
        ];

        let (mutations, result) = builder.build_from_rows("ks", "users", &rows);
        assert_eq!(result.rows_processed, 2);
        assert_eq!(result.mutations_generated, 1); // Only Alice passes WHERE
        assert_eq!(mutations.len(), 1);
    }

    #[test]
    fn build_with_no_views() {
        let mgr = ViewManager::new();
        let builder = ViewBuilder::new(&mgr);
        let rows = vec![(b"pk1".to_vec(), HashMap::new(), 1000i64)];
        let (mutations, result) = builder.build_from_rows("ks", "users", &rows);
        assert_eq!(result.rows_processed, 1);
        assert_eq!(result.mutations_generated, 0);
        assert!(mutations.is_empty());
    }

    #[test]
    fn build_with_empty_rows() {
        let mgr = ViewManager::new();
        mgr.register(test_view_def()).unwrap();
        let builder = ViewBuilder::new(&mgr);
        let (mutations, result) = builder.build_from_rows("ks", "users", &[]);
        assert_eq!(result.rows_processed, 0);
        assert_eq!(result.mutations_generated, 0);
        assert!(mutations.is_empty());
    }
}
