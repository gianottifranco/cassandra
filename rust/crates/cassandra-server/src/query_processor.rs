// Licensed under Apache License, Version 2.0.

//! Central query orchestrator: parse → plan → authorize → execute.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.cql3.QueryProcessor`

use std::sync::Arc;

use parking_lot::RwLock;
use tracing::debug;

use cassandra_common::CassandraError;
use cassandra_cql::planner::{self, PlanError, QueryPlan};
use cassandra_cql::prepared::PreparedCache;
use cassandra_native_protocol::message::{
    BatchMessage, ColumnSpec, ColumnType, PreparedResult, QueryParams, RowsMetadata,
};
use cassandra_schema::SchemaCatalog;

use crate::executor::{ExecutorError, QueryExecutor, QueryResult};

/// Central query processing orchestrator.
///
/// Holds references to the executor, prepared statement cache, and schema catalog.
/// All query types (simple, prepared, batch) are dispatched through this struct.
pub struct QueryProcessor {
    executor: Arc<QueryExecutor>,
    prepared_cache: Arc<PreparedCache>,
    catalog: Arc<RwLock<SchemaCatalog>>,
}

impl QueryProcessor {
    pub fn new(
        executor: Arc<QueryExecutor>,
        prepared_cache: Arc<PreparedCache>,
        catalog: Arc<RwLock<SchemaCatalog>>,
    ) -> Self {
        Self {
            executor,
            prepared_cache,
            catalog,
        }
    }

    /// Process a simple QUERY message.
    pub fn process_query(
        &self,
        cql: &str,
        _params: &QueryParams,
        user: Option<&str>,
        keyspace: Option<&str>,
    ) -> Result<QueryResult, CassandraError> {
        let stmt = cassandra_cql::parser::parse(cql)
            .map_err(|e| CassandraError::SyntaxError(e.to_string()))?;

        let schema = self.catalog.read().snapshot();
        let plan = planner::plan(&stmt, &schema, keyspace)
            .map_err(plan_error_to_cassandra)?;

        let result = self
            .executor
            .execute(&plan, user)
            .map_err(executor_error_to_cassandra)?;

        Ok(result)
    }

    /// Process a PREPARE message.
    pub fn process_prepare(
        &self,
        cql: &str,
        keyspace: Option<&str>,
    ) -> Result<PreparedResult, CassandraError> {
        let schema_version = 0; // GAP(gap_guard_tcm): track real schema version — tracked in gap_guards.rs
        let prepared = self
            .prepared_cache
            .prepare_with_keyspace(cql, schema_version, keyspace)
            .map_err(CassandraError::SyntaxError)?;

        // Build bind metadata: resolve types from schema where possible.
        // Falls back to Blob for any bind variable whose target column cannot be determined.
        let bind_specs =
            self.resolve_bind_specs(&prepared.statement, keyspace, prepared.bind_count);

        // Build result metadata by planning and inspecting the result shape
        let result_specs = self.infer_result_metadata(cql, keyspace);

        let bind_metadata = RowsMetadata {
            flags: 0,
            columns_count: bind_specs.len() as i32,
            paging_state: None,
            new_metadata_id: None,
            global_table_spec: None,
            col_specs: bind_specs,
        };

        let result_metadata = RowsMetadata {
            flags: 0,
            columns_count: result_specs.len() as i32,
            paging_state: None,
            new_metadata_id: None,
            global_table_spec: None,
            col_specs: result_specs,
        };

        Ok(PreparedResult {
            id: prepared.id.to_vec(),
            result_metadata_id: prepared.result_metadata_id.map(|id| id.to_vec()),
            bind_metadata,
            result_metadata,
        })
    }

    /// Process an EXECUTE message (execute a prepared statement).
    pub fn process_execute(
        &self,
        id: &[u8],
        params: &QueryParams,
        user: Option<&str>,
        keyspace: Option<&str>,
    ) -> Result<QueryResult, CassandraError> {
        let id_arr: [u8; 16] = id
            .try_into()
            .map_err(|_| CassandraError::InvalidQuery("Invalid prepared statement ID".into()))?;

        let prepared = self
            .prepared_cache
            .get(&id_arr)
            .ok_or_else(|| CassandraError::Unprepared(id.to_vec()))?;

        debug!(query = %prepared.query, "Executing prepared statement");

        // Use the prepared statement's keyspace context, falling back to connection keyspace
        let effective_keyspace = prepared.keyspace.as_deref().or(keyspace);

        self.process_query(&prepared.query, params, user, effective_keyspace)
    }

    /// Process a BATCH message.
    pub fn process_batch(
        &self,
        batch: &BatchMessage,
        user: Option<&str>,
        keyspace: Option<&str>,
    ) -> Result<QueryResult, CassandraError> {
        for query in &batch.queries {
            if query.is_prepared {
                let params = QueryParams::default();
                self.process_execute(&query.query_or_id, &params, user, keyspace)?;
            } else {
                let cql = String::from_utf8(query.query_or_id.clone())
                    .map_err(|_| CassandraError::InvalidQuery("Invalid UTF-8 in batch query".into()))?;
                let params = QueryParams::default();
                self.process_query(&cql, &params, user, keyspace)?;
            }
        }
        Ok(QueryResult::Void)
    }

    /// Execute an internal CQL query (no auth, system context).
    pub fn execute_internal(
        &self,
        cql: &str,
    ) -> Result<QueryResult, CassandraError> {
        self.process_query(cql, &QueryParams::default(), Some("system"), None)
    }

    /// Resolve bind variable types from the schema for a prepared statement.
    ///
    /// For INSERT and UPDATE, each bind marker in a column position is typed using
    /// the column's declared type. Unknown positions fall back to `Blob`.
    ///
    /// ## Java Oracle
    /// `QueryProcessor.buildBindVariables` / `CQL3Type` resolution
    fn resolve_bind_specs(
        &self,
        statement: &cassandra_cql::ast::Statement,
        keyspace: Option<&str>,
        bind_count: usize,
    ) -> Vec<ColumnSpec> {
        use cassandra_cql::ast::{Statement, Term};

        // Build a position-indexed map: bind_marker_index → ColumnSpec
        let mut result: Vec<ColumnSpec> = (0..bind_count)
            .map(|i| ColumnSpec {
                ksname: None,
                tablename: None,
                name: format!("column{}", i),
                col_type: ColumnType::Blob,
            })
            .collect();

        let schema = self.catalog.read().snapshot();

        match statement {
            Statement::Insert(ins) => {
                let ks = ins.keyspace.as_deref().or(keyspace).unwrap_or_default();
                if let Some(table_meta) = schema.table(ks, &ins.table) {
                    let mut bind_idx = 0usize;
                    for (col_name, term) in ins.columns.iter().zip(ins.values.iter()) {
                        if matches!(term, Term::BindMarker(_)) {
                            if let Some(col) = table_meta.column(col_name) {
                                if bind_idx < result.len() {
                                    result[bind_idx] = ColumnSpec {
                                        ksname: Some(ks.to_string()),
                                        tablename: Some(ins.table.clone()),
                                        name: col_name.clone(),
                                        col_type: ColumnType::from_cql_type(&col.column_type),
                                    };
                                }
                            }
                            bind_idx += 1;
                        }
                    }
                }
            }
            Statement::Update(upd) => {
                let ks = upd.keyspace.as_deref().or(keyspace).unwrap_or_default();
                if let Some(table_meta) = schema.table(ks, &upd.table) {
                    let mut bind_idx = 0usize;
                    for assign in &upd.assignments {
                        if matches!(assign.value, Term::BindMarker(_)) {
                            if let Some(col) = table_meta.column(&assign.column) {
                                if bind_idx < result.len() {
                                    result[bind_idx] = ColumnSpec {
                                        ksname: Some(ks.to_string()),
                                        tablename: Some(upd.table.clone()),
                                        name: assign.column.clone(),
                                        col_type: ColumnType::from_cql_type(&col.column_type),
                                    };
                                }
                            }
                            bind_idx += 1;
                        }
                    }
                }
            }
            _ => {} // SELECT and other statements keep Blob fallback
        }

        result
    }

    /// Infer result column metadata for a query by parsing and planning.
    fn infer_result_metadata(&self, cql: &str, keyspace: Option<&str>) -> Vec<ColumnSpec> {
        let stmt = match cassandra_cql::parser::parse(cql) {
            Ok(s) => s,
            Err(_) => return Vec::new(),
        };
        let schema = self.catalog.read().snapshot();
        let plan = match planner::plan(&stmt, &schema, keyspace) {
            Ok(p) => p,
            Err(_) => return Vec::new(),
        };

        if let QueryPlan::Select(sel) = &plan {
            if let Some(table_meta) = schema.table(&sel.keyspace, &sel.table) {
                use cassandra_cql::ast::{SelectColumns, Selector};
                let cols: Vec<&cassandra_schema::ColumnMetadata> = match &sel.columns {
                    SelectColumns::All => table_meta.columns.iter().collect(),
                    SelectColumns::Named(selectors) => selectors
                        .iter()
                        .filter_map(|s| match s {
                            Selector::Column(name) => {
                                table_meta.columns.iter().find(|c| c.name == *name)
                            }
                            _ => None,
                        })
                        .collect(),
                };
                return cols
                    .into_iter()
                    .map(|c| ColumnSpec {
                        ksname: Some(sel.keyspace.clone()),
                        tablename: Some(sel.table.clone()),
                        name: c.name.clone(),
                        col_type: ColumnType::from_cql_type(&c.column_type),
                    })
                    .collect();
            }
        }

        Vec::new()
    }
}

fn plan_error_to_cassandra(err: PlanError) -> CassandraError {
    match err {
        PlanError::InvalidQuery(msg) => CassandraError::InvalidQuery(msg),
        PlanError::SyntaxError(msg) => CassandraError::SyntaxError(msg),
        PlanError::AlreadyExists { ks, name } => CassandraError::AlreadyExists {
            ks,
            table: name,
        },
    }
}

fn executor_error_to_cassandra(err: ExecutorError) -> CassandraError {
    match err {
        ExecutorError::InvalidQuery(msg) => CassandraError::InvalidQuery(msg),
        ExecutorError::KeyspaceNotFound(ks) => {
            CassandraError::InvalidQuery(format!("Keyspace '{}' not found", ks))
        }
        ExecutorError::TableNotFound(ks, table) => {
            CassandraError::InvalidQuery(format!("Table '{}.{}' not found", ks, table))
        }
        ExecutorError::StorageError(msg) => CassandraError::ServerError(msg),
        ExecutorError::SchemaError(msg) => CassandraError::ConfigError(msg),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plan_error_invalid_query() {
        let err = PlanError::InvalidQuery("bad".into());
        let ce = plan_error_to_cassandra(err);
        assert!(matches!(ce, CassandraError::InvalidQuery(_)));
    }

    #[test]
    fn plan_error_syntax() {
        let err = PlanError::SyntaxError("parse fail".into());
        let ce = plan_error_to_cassandra(err);
        assert!(matches!(ce, CassandraError::SyntaxError(_)));
    }

    #[test]
    fn plan_error_already_exists() {
        let err = PlanError::AlreadyExists {
            ks: "ks".into(),
            name: "tbl".into(),
        };
        let ce = plan_error_to_cassandra(err);
        assert!(matches!(ce, CassandraError::AlreadyExists { .. }));
    }

    #[test]
    fn executor_error_mappings() {
        let cases: Vec<(ExecutorError, i32)> = vec![
            (ExecutorError::InvalidQuery("x".into()), 0x2200),
            (ExecutorError::KeyspaceNotFound("x".into()), 0x2200),
            (ExecutorError::TableNotFound("x".into(), "y".into()), 0x2200),
            (ExecutorError::StorageError("x".into()), 0x0000),
            (ExecutorError::SchemaError("x".into()), 0x2300),
        ];
        for (err, expected_code) in cases {
            let ce = executor_error_to_cassandra(err);
            assert_eq!(ce.error_code(), Some(expected_code));
        }
    }
}
