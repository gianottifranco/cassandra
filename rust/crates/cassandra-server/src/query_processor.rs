// Licensed under Apache License, Version 2.0.

//! Central query orchestrator: parse → plan → authorize → execute.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.cql3.QueryProcessor`

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use byteorder::{BigEndian, ByteOrder};
use parking_lot::RwLock;
use tracing::debug;

use cassandra_common::CassandraError;
use cassandra_cql::ast::{
    AssignmentOp, BindMarker, Literal, Relation, Select, SelectColumns, Selector, Statement, Term,
    Update, UsingClause,
};
use cassandra_cql::functions::FunctionRegistry;
use cassandra_cql::planner::{self, PlanError, QueryPlan};
use cassandra_cql::prepared::PreparedCache;
use cassandra_native_protocol::message::{
    BatchMessage, ColumnSpec, ColumnType, PreparedResult, QueryParams, RowsMetadata, query_flags,
};
use cassandra_schema::{SchemaCatalog, SchemaSnapshot};
use cassandra_types::{CqlType, codec::CqlValue};

use crate::executor::{ExecutorError, QueryExecutor, QueryResult};

/// Result of executing a prepared statement, with optional metadata change info.
///
/// When the schema has changed since the statement was prepared, `metadata_changed`
/// is set to `true` and `new_metadata_id` contains the recomputed result metadata ID.
/// The server layer uses this to set the METADATA_CHANGED flag in the response.
#[derive(Debug, Clone)]
pub struct ExecuteResult {
    pub result: QueryResult,
    pub metadata_changed: bool,
    pub new_metadata_id: Option<Vec<u8>>,
    pub omit_result_metadata: bool,
}

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
        params: &QueryParams,
        user: Option<&str>,
        keyspace: Option<&str>,
    ) -> Result<QueryResult, CassandraError> {
        let mut stmt = cassandra_cql::parser::parse(cql)
            .map_err(|e| CassandraError::SyntaxError(e.to_string()))?;

        let effective_keyspace = params.keyspace.as_deref().or(keyspace);
        let schema = self.catalog.read().snapshot();
        bind_statement(&mut stmt, params, effective_keyspace, &schema)?;
        let plan =
            planner::plan(&stmt, &schema, effective_keyspace).map_err(plan_error_to_cassandra)?;

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
        let schema_version = self.catalog.read().version();
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
        let result_metadata_id =
            PreparedCache::compute_result_metadata_id_from_specs(&result_specs);
        self.prepared_cache
            .update_result_metadata_id(&prepared.id, result_metadata_id);

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
            result_metadata_id: Some(result_metadata_id.to_vec()),
            bind_metadata,
            result_metadata,
        })
    }

    /// Process an EXECUTE message (execute a prepared statement).
    ///
    /// Returns an `ExecuteResult` which includes the query result and optional
    /// metadata change information. If the schema version has changed since the
    /// statement was prepared, the result metadata ID is recomputed and compared
    /// with the stored one. A mismatch sets `metadata_changed = true` so the
    /// server can include the METADATA_CHANGED flag in the response.
    pub fn process_execute(
        &self,
        id: &[u8],
        client_result_metadata_id: Option<&[u8]>,
        params: &QueryParams,
        user: Option<&str>,
        keyspace: Option<&str>,
    ) -> Result<ExecuteResult, CassandraError> {
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

        let result = self.process_query(&prepared.query, params, user, effective_keyspace)?;

        // Detect metadata changes from the actual current result metadata.
        let current_schema_version = self.catalog.read().version();
        let current_result_specs = self.infer_result_metadata(&prepared.query, effective_keyspace);
        let current_metadata_id =
            PreparedCache::compute_result_metadata_id_from_specs(&current_result_specs);
        let prepared_metadata_changed = current_schema_version > prepared.schema_version
            && prepared
                .result_metadata_id
                .is_none_or(|old_id| old_id != current_metadata_id);
        let client_metadata_changed = client_result_metadata_id
            .is_some_and(|client_id| client_id != current_metadata_id.as_slice());
        let metadata_changed = prepared_metadata_changed || client_metadata_changed;
        let new_metadata_id = metadata_changed.then(|| current_metadata_id.to_vec());
        let omit_result_metadata = !metadata_changed
            && (client_result_metadata_id.is_some()
                || params.flags & query_flags::SKIP_METADATA != 0);

        Ok(ExecuteResult {
            result,
            metadata_changed,
            new_metadata_id,
            omit_result_metadata,
        })
    }

    /// Process a BATCH message.
    pub fn process_batch(
        &self,
        batch: &BatchMessage,
        user: Option<&str>,
        keyspace: Option<&str>,
    ) -> Result<QueryResult, CassandraError> {
        let effective_keyspace = batch.keyspace.as_deref().or(keyspace);
        for query in &batch.queries {
            if query.is_prepared {
                let params = QueryParams {
                    values: query.values.clone(),
                    keyspace: batch.keyspace.clone(),
                    ..QueryParams::default()
                };
                // Discard metadata change info for batch; only the final result matters.
                let _exec = self.process_execute(
                    &query.query_or_id,
                    None,
                    &params,
                    user,
                    effective_keyspace,
                )?;
            } else {
                let cql = String::from_utf8(query.query_or_id.clone()).map_err(|_| {
                    CassandraError::InvalidQuery("Invalid UTF-8 in batch query".into())
                })?;
                let params = QueryParams {
                    values: query.values.clone(),
                    keyspace: batch.keyspace.clone(),
                    ..QueryParams::default()
                };
                self.process_query(&cql, &params, user, effective_keyspace)?;
            }
        }
        Ok(QueryResult::Void)
    }

    /// Execute an internal CQL query (no auth, system context).
    pub fn execute_internal(&self, cql: &str) -> Result<QueryResult, CassandraError> {
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
                return match &sel.columns {
                    SelectColumns::All => table_meta
                        .columns
                        .iter()
                        .map(|c| ColumnSpec {
                            ksname: Some(sel.keyspace.clone()),
                            tablename: Some(sel.table.clone()),
                            name: c.name.clone(),
                            col_type: ColumnType::from_cql_type(&c.column_type),
                        })
                        .collect(),
                    SelectColumns::Named(selectors) => selectors
                        .iter()
                        .filter_map(|selector| {
                            result_metadata_for_selector(
                                selector,
                                table_meta,
                                &sel.keyspace,
                                &sel.table,
                            )
                        })
                        .collect(),
                };
            }
        }

        Vec::new()
    }
}

fn result_metadata_for_selector(
    selector: &Selector,
    table: &cassandra_schema::table::TableMetadata,
    keyspace: &str,
    table_name: &str,
) -> Option<ColumnSpec> {
    let cql_type = selector_result_type_for_metadata(selector, table)?;
    Some(ColumnSpec {
        ksname: Some(keyspace.to_string()),
        tablename: Some(table_name.to_string()),
        name: selector_output_name_for_metadata(selector),
        col_type: ColumnType::from_cql_type(&cql_type),
    })
}

fn selector_result_type_for_metadata(
    selector: &Selector,
    table: &cassandra_schema::table::TableMetadata,
) -> Option<CqlType> {
    match selector {
        Selector::Column(name) => table.column(name).map(|column| column.column_type.clone()),
        Selector::Alias { selector, .. } => selector_result_type_for_metadata(selector, table),
        Selector::Count => Some(CqlType::Bigint),
        Selector::WritetimeOrTtl(kind, _) if kind.eq_ignore_ascii_case("ttl") => Some(CqlType::Int),
        Selector::WritetimeOrTtl(_, _) => Some(CqlType::Bigint),
        Selector::Cast { target, .. } => target.resolve(),
        Selector::Function(name, args) => {
            let arg_types = args
                .iter()
                .map(|arg| selector_result_type_for_metadata(arg, table))
                .collect::<Option<Vec<_>>>()?;
            FunctionRegistry::with_builtins()
                .resolve(name, &arg_types)
                .map(|function| function.return_type())
        }
    }
}

fn selector_output_name_for_metadata(selector: &Selector) -> String {
    match selector {
        Selector::Column(name) => name.clone(),
        Selector::Alias { alias, .. } => alias.clone(),
        Selector::Count => "count".to_string(),
        Selector::Function(name, args) => {
            let args = if args.is_empty() {
                "*".to_string()
            } else {
                args.iter()
                    .map(selector_output_name_for_metadata)
                    .collect::<Vec<_>>()
                    .join(", ")
            };
            format!("{}({})", name.to_ascii_lowercase(), args)
        }
        Selector::Cast { selector, target } => {
            let target = target
                .resolve()
                .map(|target| target.cql_name())
                .unwrap_or_else(|| "unknown".to_string());
            format!(
                "cast({} as {})",
                selector_output_name_for_metadata(selector),
                target
            )
        }
        Selector::WritetimeOrTtl(kind, column) => format!("{}({})", kind, column),
    }
}

struct BindCursor<'a> {
    values: &'a [Option<Vec<u8>>],
    names: Option<HashMap<&'a str, usize>>,
    used_named_values: HashSet<usize>,
    next: usize,
}

impl<'a> BindCursor<'a> {
    fn new(params: &'a QueryParams) -> Result<Self, CassandraError> {
        let names = match params.value_names.as_ref() {
            Some(names) => {
                if names.len() != params.values.len() {
                    return Err(CassandraError::InvalidQuery(format!(
                        "Named bind values mismatch: {} names for {} values",
                        names.len(),
                        params.values.len()
                    )));
                }
                let mut map = HashMap::with_capacity(names.len());
                for (idx, name) in names.iter().enumerate() {
                    if map.insert(name.as_str(), idx).is_some() {
                        return Err(CassandraError::InvalidQuery(format!(
                            "Duplicate bind value name '{}'",
                            name
                        )));
                    }
                }
                Some(map)
            }
            None => None,
        };

        Ok(Self {
            values: &params.values,
            names,
            used_named_values: HashSet::new(),
            next: 0,
        })
    }

    fn next_marker_term(
        &mut self,
        marker: &BindMarker,
        cql_type: Option<&CqlType>,
    ) -> Result<Term, CassandraError> {
        match marker {
            BindMarker::Named(name) => {
                if self.names.is_some() {
                    self.named_term(name, cql_type)
                } else {
                    self.next_positional_term(cql_type)
                }
            }
            BindMarker::Anonymous => {
                if self.names.is_some() {
                    return Err(CassandraError::InvalidQuery(
                        "Named bind values cannot satisfy anonymous bind markers".into(),
                    ));
                }
                self.next_positional_term(cql_type)
            }
        }
    }

    fn next_positional_term(&mut self, cql_type: Option<&CqlType>) -> Result<Term, CassandraError> {
        let value = self.values.get(self.next).ok_or_else(|| {
            CassandraError::InvalidQuery("Not enough bind values for prepared statement".into())
        })?;
        self.next += 1;
        bind_value_to_term(value.as_deref(), cql_type)
    }

    fn named_term(
        &mut self,
        name: &str,
        cql_type: Option<&CqlType>,
    ) -> Result<Term, CassandraError> {
        let idx = self
            .names
            .as_ref()
            .and_then(|names| names.get(name).copied())
            .ok_or_else(|| {
                CassandraError::InvalidQuery(format!("Missing bind value for '{}'", name))
            })?;
        self.used_named_values.insert(idx);
        bind_value_to_term(self.values[idx].as_deref(), cql_type)
    }

    fn finish(&self) -> Result<(), CassandraError> {
        if let Some(names) = &self.names {
            if self.used_named_values.len() == self.values.len() {
                return Ok(());
            }
            let used = &self.used_named_values;
            let unused = names
                .iter()
                .find_map(|(name, idx)| (!used.contains(idx)).then_some(*name))
                .unwrap_or("<unknown>");
            return Err(CassandraError::InvalidQuery(format!(
                "Unused named bind value '{}'",
                unused
            )));
        }

        if self.next == self.values.len() {
            Ok(())
        } else {
            Err(CassandraError::InvalidQuery(format!(
                "Too many bind values for prepared statement: expected {}, got {}",
                self.next,
                self.values.len()
            )))
        }
    }
}

fn bind_statement(
    stmt: &mut Statement,
    params: &QueryParams,
    keyspace: Option<&str>,
    schema: &SchemaSnapshot,
) -> Result<(), CassandraError> {
    let mut cursor = BindCursor::new(params)?;
    bind_statement_terms(stmt, &mut cursor, keyspace, schema)?;
    cursor.finish()
}

fn bind_statement_terms(
    stmt: &mut Statement,
    cursor: &mut BindCursor<'_>,
    keyspace: Option<&str>,
    schema: &SchemaSnapshot,
) -> Result<(), CassandraError> {
    match stmt {
        Statement::Select(select) => bind_select(select, cursor, keyspace, schema)?,
        Statement::Insert(insert) => {
            let ks = insert.keyspace.as_deref().or(keyspace).unwrap_or_default();
            let table = schema.table(ks, &insert.table);
            for (column, value) in insert.columns.iter().zip(insert.values.iter_mut()) {
                let cql_type = table
                    .and_then(|table| table.column(column))
                    .map(|column| &column.column_type);
                bind_term(value, cursor, cql_type)?;
            }
            if let Some(json) = insert.json.as_mut() {
                bind_term(json, cursor, Some(&CqlType::Varchar))?;
            }
            bind_using_clauses(&mut insert.using, cursor)?;
        }
        Statement::Update(update) => bind_update(update, cursor, keyspace, schema)?,
        Statement::Delete(delete) => {
            let ks = delete.keyspace.as_deref().or(keyspace).unwrap_or_default();
            let table = schema.table(ks, &delete.table);
            bind_relations(&mut delete.where_clause, cursor, table)?;
            bind_relations(&mut delete.if_conditions, cursor, table)?;
            bind_using_clauses(&mut delete.using, cursor)?;
        }
        Statement::Batch(batch) => {
            for statement in &mut batch.statements {
                bind_statement_terms(statement, cursor, keyspace, schema)?;
            }
            bind_using_clauses(&mut batch.using, cursor)?;
        }
        _ => {}
    }
    Ok(())
}

fn bind_select(
    select: &mut Select,
    cursor: &mut BindCursor<'_>,
    keyspace: Option<&str>,
    schema: &SchemaSnapshot,
) -> Result<(), CassandraError> {
    let ks = select.keyspace.as_deref().or(keyspace).unwrap_or_default();
    let table = schema.table(ks, &select.table);
    bind_relations(&mut select.where_clause, cursor, table)?;
    if let Some(limit) = select.limit.as_mut() {
        bind_term(limit, cursor, Some(&CqlType::Int))?;
    }
    if let Some(limit) = select.per_partition_limit.as_mut() {
        bind_term(limit, cursor, Some(&CqlType::Int))?;
    }
    Ok(())
}

fn bind_update(
    update: &mut Update,
    cursor: &mut BindCursor<'_>,
    keyspace: Option<&str>,
    schema: &SchemaSnapshot,
) -> Result<(), CassandraError> {
    let ks = update.keyspace.as_deref().or(keyspace).unwrap_or_default();
    let table = schema.table(ks, &update.table);
    for assignment in &mut update.assignments {
        let cql_type = table
            .and_then(|table| table.column(&assignment.column))
            .map(|column| &column.column_type);
        if let AssignmentOp::MapPut { key } = &mut assignment.op {
            let key_type = match cql_type {
                Some(CqlType::Map(key_type, _, _)) => Some(key_type.as_ref()),
                _ => None,
            };
            bind_term(key, cursor, key_type)?;
        }
        bind_term(&mut assignment.value, cursor, cql_type)?;
    }
    bind_relations(&mut update.where_clause, cursor, table)?;
    bind_relations(&mut update.if_conditions, cursor, table)?;
    bind_using_clauses(&mut update.using, cursor)?;
    Ok(())
}

fn bind_relations(
    relations: &mut [Relation],
    cursor: &mut BindCursor<'_>,
    table: Option<&cassandra_schema::TableMetadata>,
) -> Result<(), CassandraError> {
    for relation in relations {
        let cql_type = table
            .and_then(|table| table.column(&relation.column))
            .map(|column| &column.column_type);
        bind_term(&mut relation.value, cursor, cql_type)?;
    }
    Ok(())
}

fn bind_using_clauses(
    clauses: &mut [UsingClause],
    cursor: &mut BindCursor<'_>,
) -> Result<(), CassandraError> {
    for clause in clauses {
        match clause {
            UsingClause::Timestamp(term) => bind_term(term, cursor, Some(&CqlType::Timestamp))?,
            UsingClause::Ttl(term) => bind_term(term, cursor, Some(&CqlType::Int))?,
        }
    }
    Ok(())
}

fn bind_term(
    term: &mut Term,
    cursor: &mut BindCursor<'_>,
    cql_type: Option<&CqlType>,
) -> Result<(), CassandraError> {
    match term {
        Term::BindMarker(marker) => {
            *term = cursor.next_marker_term(marker, cql_type)?;
        }
        Term::FunctionCall(_, args) | Term::CollectionLiteral(args) | Term::TupleLiteral(args) => {
            for arg in args {
                bind_term(arg, cursor, None)?;
            }
        }
        Term::TypeHint(type_hint, inner) => {
            let hinted_type = type_hint.resolve();
            bind_term(inner, cursor, hinted_type.as_ref().or(cql_type))?;
        }
        Term::CollectionElement { key, value } => match cql_type {
            Some(CqlType::List(inner, _)) => {
                bind_term(key, cursor, Some(&CqlType::Int))?;
                bind_term(value, cursor, Some(inner))?;
            }
            Some(CqlType::Map(key_type, value_type, _)) => {
                bind_term(key, cursor, Some(key_type))?;
                bind_term(value, cursor, Some(value_type))?;
            }
            Some(CqlType::Set(inner, _)) => {
                bind_term(key, cursor, Some(inner))?;
                bind_term(value, cursor, Some(inner))?;
            }
            _ => {
                bind_term(key, cursor, None)?;
                bind_term(value, cursor, None)?;
            }
        },
        Term::MapLiteral(entries) => {
            for (key, value) in entries {
                bind_term(key, cursor, None)?;
                bind_term(value, cursor, None)?;
            }
        }
        Term::Literal(_) => {}
    }
    Ok(())
}

fn bind_value_to_term(
    value: Option<&[u8]>,
    cql_type: Option<&CqlType>,
) -> Result<Term, CassandraError> {
    let Some(value) = value else {
        return Ok(Term::Literal(Literal::Null));
    };

    let literal = match cql_type {
        Some(CqlType::Boolean) => Literal::Boolean(value.first().copied().unwrap_or(0) != 0),
        Some(CqlType::Tinyint) => {
            let byte = *value
                .first()
                .ok_or_else(|| CassandraError::InvalidQuery("Invalid tinyint bind value".into()))?;
            Literal::Integer(i8::from_be_bytes([byte]) as i64)
        }
        Some(CqlType::Smallint) => {
            if value.len() != 2 {
                return Err(CassandraError::InvalidQuery(
                    "Invalid smallint bind value".into(),
                ));
            }
            Literal::Integer(BigEndian::read_i16(value) as i64)
        }
        Some(CqlType::Int) => {
            if value.len() != 4 {
                return Err(CassandraError::InvalidQuery(
                    "Invalid int bind value".into(),
                ));
            }
            Literal::Integer(BigEndian::read_i32(value) as i64)
        }
        Some(CqlType::Bigint | CqlType::Counter | CqlType::Timestamp | CqlType::Time) => {
            if value.len() != 8 {
                return Err(CassandraError::InvalidQuery(
                    "Invalid 64-bit integer bind value".into(),
                ));
            }
            Literal::Integer(BigEndian::read_i64(value))
        }
        Some(CqlType::Float) => {
            if value.len() != 4 {
                return Err(CassandraError::InvalidQuery(
                    "Invalid float bind value".into(),
                ));
            }
            Literal::Float(BigEndian::read_f32(value) as f64)
        }
        Some(CqlType::Double) => {
            if value.len() != 8 {
                return Err(CassandraError::InvalidQuery(
                    "Invalid double bind value".into(),
                ));
            }
            Literal::Float(BigEndian::read_f64(value))
        }
        Some(CqlType::Ascii | CqlType::Varchar) => {
            let text = std::str::from_utf8(value)
                .map_err(|_| CassandraError::InvalidQuery("Invalid UTF-8 bind value".into()))?;
            Literal::String(text.to_string())
        }
        Some(CqlType::Uuid | CqlType::Timeuuid) => {
            let uuid = uuid::Uuid::from_slice(value)
                .map_err(|_| CassandraError::InvalidQuery("Invalid UUID bind value".into()))?;
            Literal::Uuid(uuid.to_string())
        }
        Some(CqlType::Date) => {
            if value.len() != 4 {
                return Err(CassandraError::InvalidQuery(
                    "Invalid date bind value".into(),
                ));
            }
            Literal::Integer(BigEndian::read_u32(value) as i64)
        }
        Some(
            cql_type @ (CqlType::List(_, _)
            | CqlType::Set(_, _)
            | CqlType::Map(_, _, _)
            | CqlType::Tuple(_)
            | CqlType::Udt { .. }
            | CqlType::Vector(_, _)),
        ) => {
            let decoded = CqlValue::deserialize_value(cql_type, value).map_err(|err| {
                CassandraError::InvalidQuery(format!("Invalid complex bind value: {}", err))
            })?;
            Literal::Blob(decoded.serialize_value())
        }
        _ => Literal::Blob(value.to_vec()),
    };

    Ok(Term::Literal(literal))
}

fn plan_error_to_cassandra(err: PlanError) -> CassandraError {
    match err {
        PlanError::InvalidQuery(msg) => CassandraError::InvalidQuery(msg),
        PlanError::SyntaxError(msg) => CassandraError::SyntaxError(msg),
        PlanError::AlreadyExists { ks, name } => CassandraError::AlreadyExists { ks, table: name },
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
    use cassandra_native_protocol::types::Consistency;
    use cassandra_schema::{
        ClusteringOrder, ColumnMetadata, KeyspaceMetadata, KeyspaceParams, TableMetadataBuilder,
    };
    use cassandra_security::{AllowAllAuthorizer, InMemoryRoleManager};
    use cassandra_storage::commitlog::CommitLogConfig;
    use cassandra_storage::engine::{EngineConfig, StorageEngine};
    use tempfile::TempDir;

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

    #[test]
    fn prepared_execute_binds_values_through_query_processor() {
        let (_temp, processor) = test_processor();

        let insert = processor
            .process_prepare(
                "INSERT INTO events (id, bucket, name) VALUES (?, ?, ?)",
                Some("ks"),
            )
            .unwrap();
        let insert_result = processor
            .process_execute(
                &insert.id,
                None,
                &params(vec![
                    Some(int_bytes(7)),
                    Some(int_bytes(1)),
                    Some(b"prepared".to_vec()),
                ]),
                Some("cassandra"),
                Some("ks"),
            )
            .unwrap();
        assert!(matches!(insert_result.result, QueryResult::Void));

        let select = processor
            .process_prepare("SELECT name FROM events WHERE id = ?", Some("ks"))
            .unwrap();
        let select_result = processor
            .process_execute(
                &select.id,
                None,
                &params(vec![Some(int_bytes(7))]),
                Some("cassandra"),
                Some("ks"),
            )
            .unwrap();

        let QueryResult::Rows { rows, .. } = select_result.result else {
            panic!("expected rows from prepared select");
        };
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0].as_deref(), Some(b"prepared".as_slice()));
    }

    #[test]
    fn prepared_execute_rejects_extra_bind_values() {
        let (_temp, processor) = test_processor();
        let prepared = processor
            .process_prepare("SELECT name FROM events WHERE id = ?", Some("ks"))
            .unwrap();

        let err = processor
            .process_execute(
                &prepared.id,
                None,
                &params(vec![Some(int_bytes(7)), Some(int_bytes(1))]),
                Some("cassandra"),
                Some("ks"),
            )
            .unwrap_err();

        assert!(
            matches!(err, CassandraError::InvalidQuery(msg) if msg.contains("Too many bind values"))
        );
    }

    #[test]
    fn prepared_execute_rejects_missing_bind_values() {
        let (_temp, processor) = test_processor();
        let prepared = processor
            .process_prepare("SELECT name FROM events WHERE id = ?", Some("ks"))
            .unwrap();

        let err = processor
            .process_execute(
                &prepared.id,
                None,
                &QueryParams::default(),
                Some("cassandra"),
                Some("ks"),
            )
            .unwrap_err();

        assert!(
            matches!(err, CassandraError::InvalidQuery(msg) if msg.contains("Not enough bind values"))
        );
    }

    #[test]
    fn prepared_execute_binds_named_values_by_name() {
        let (_temp, processor) = test_processor();

        let insert = processor
            .process_prepare(
                "INSERT INTO events (id, bucket, name) VALUES (:id, :bucket, :name)",
                Some("ks"),
            )
            .unwrap();
        processor
            .process_execute(
                &insert.id,
                None,
                &named_params(vec![
                    ("name", Some(b"named".to_vec())),
                    ("id", Some(int_bytes(9))),
                    ("bucket", Some(int_bytes(1))),
                ]),
                Some("cassandra"),
                Some("ks"),
            )
            .unwrap();

        let select = processor
            .process_prepare("SELECT name FROM events WHERE id = :id", Some("ks"))
            .unwrap();
        let result = processor
            .process_execute(
                &select.id,
                None,
                &named_params(vec![("id", Some(int_bytes(9)))]),
                Some("cassandra"),
                Some("ks"),
            )
            .unwrap();

        let QueryResult::Rows { rows, .. } = result.result else {
            panic!("expected rows from prepared select");
        };
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0].as_deref(), Some(b"named".as_slice()));
    }

    #[test]
    fn prepared_execute_rejects_unused_named_value() {
        let (_temp, processor) = test_processor();
        let prepared = processor
            .process_prepare("SELECT name FROM events WHERE id = :id", Some("ks"))
            .unwrap();

        let err = processor
            .process_execute(
                &prepared.id,
                None,
                &named_params(vec![
                    ("id", Some(int_bytes(7))),
                    ("extra", Some(int_bytes(1))),
                ]),
                Some("cassandra"),
                Some("ks"),
            )
            .unwrap_err();

        assert!(
            matches!(err, CassandraError::InvalidQuery(msg) if msg.contains("Unused named bind value"))
        );
    }

    #[test]
    fn prepared_execute_validates_complex_bind_values() {
        let (_temp, processor) = test_processor();
        let scores_type = CqlType::List(Box::new(CqlType::Int), false);
        let scores = CqlValue::List(vec![CqlValue::Int(10), CqlValue::Int(20)]).serialize_value();

        let insert = processor
            .process_prepare(
                "INSERT INTO events (id, bucket, name, scores) VALUES (?, ?, ?, ?)",
                Some("ks"),
            )
            .unwrap();
        processor
            .process_execute(
                &insert.id,
                None,
                &params(vec![
                    Some(int_bytes(11)),
                    Some(int_bytes(1)),
                    Some(b"complex".to_vec()),
                    Some(scores.clone()),
                ]),
                Some("cassandra"),
                Some("ks"),
            )
            .unwrap();

        let select = processor
            .process_prepare("SELECT scores FROM events WHERE id = ?", Some("ks"))
            .unwrap();
        let result = processor
            .process_execute(
                &select.id,
                None,
                &params(vec![Some(int_bytes(11))]),
                Some("cassandra"),
                Some("ks"),
            )
            .unwrap();

        let QueryResult::Rows { rows, .. } = result.result else {
            panic!("expected rows from prepared select");
        };
        assert_eq!(rows[0][0].as_deref(), Some(scores.as_slice()));
        assert!(CqlValue::deserialize_value(&scores_type, rows[0][0].as_deref().unwrap()).is_ok());
    }

    #[test]
    fn prepared_execute_rejects_invalid_complex_bind_value() {
        let (_temp, processor) = test_processor();
        let insert = processor
            .process_prepare(
                "INSERT INTO events (id, bucket, name, scores) VALUES (?, ?, ?, ?)",
                Some("ks"),
            )
            .unwrap();

        let err = processor
            .process_execute(
                &insert.id,
                None,
                &params(vec![
                    Some(int_bytes(12)),
                    Some(int_bytes(1)),
                    Some(b"bad".to_vec()),
                    Some(vec![0, 0, 0, 1, 0, 0, 0, 3]),
                ]),
                Some("cassandra"),
                Some("ks"),
            )
            .unwrap_err();

        assert!(
            matches!(err, CassandraError::InvalidQuery(msg) if msg.contains("Invalid complex bind value"))
        );
    }

    #[test]
    fn query_params_keyspace_overrides_connection_keyspace() {
        let (_temp, processor) = test_processor();

        let mut insert_params = params(vec![
            Some(int_bytes(21)),
            Some(int_bytes(1)),
            Some(b"override".to_vec()),
        ]);
        insert_params.keyspace = Some("ks2".to_string());
        processor
            .process_query(
                "INSERT INTO events (id, bucket, name) VALUES (?, ?, ?)",
                &insert_params,
                Some("cassandra"),
                Some("ks"),
            )
            .unwrap();

        let mut override_select_params = params(vec![Some(int_bytes(21))]);
        override_select_params.keyspace = Some("ks2".to_string());
        let override_result = processor
            .process_query(
                "SELECT name FROM events WHERE id = ?",
                &override_select_params,
                Some("cassandra"),
                Some("ks"),
            )
            .unwrap();

        let QueryResult::Rows { rows, .. } = override_result else {
            panic!("expected rows from overridden keyspace select");
        };
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0].as_deref(), Some(b"override".as_slice()));

        let default_result = processor
            .process_query(
                "SELECT name FROM events WHERE id = ?",
                &params(vec![Some(int_bytes(21))]),
                Some("cassandra"),
                Some("ks"),
            )
            .unwrap();

        let QueryResult::Rows { rows, .. } = default_result else {
            panic!("expected rows from connection keyspace select");
        };
        assert!(rows.is_empty());
    }

    #[test]
    fn comment_on_table_updates_table_metadata() {
        let (_temp, processor) = test_processor();

        let result = processor
            .process_query(
                "COMMENT ON TABLE events IS 'event stream table'",
                &QueryParams::default(),
                Some("cassandra"),
                Some("ks"),
            )
            .unwrap();
        assert!(matches!(result, QueryResult::SchemaChange { .. }));

        let snapshot = processor.catalog.read().snapshot();
        let table = snapshot.table("ks", "events").unwrap();
        assert_eq!(table.params.comment, "event stream table");
    }

    #[test]
    fn comment_on_keyspace_updates_metadata_without_dropping_tables() {
        let (_temp, processor) = test_processor();

        let result = processor
            .process_query(
                "COMMENT ON KEYSPACE ks IS 'primary app keyspace'",
                &QueryParams::default(),
                Some("cassandra"),
                Some("ks"),
            )
            .unwrap();
        assert!(matches!(result, QueryResult::SchemaChange { .. }));

        let snapshot = processor.catalog.read().snapshot();
        let keyspace = snapshot.keyspace("ks").unwrap();
        assert_eq!(keyspace.params.comment, "primary app keyspace");
        assert!(keyspace.table("events").is_some());
        drop(snapshot);

        let describe_result = processor
            .process_query(
                "DESCRIBE KEYSPACE ks",
                &QueryParams::default(),
                Some("cassandra"),
                Some("ks"),
            )
            .unwrap();
        let QueryResult::Rows { rows, .. } = describe_result else {
            panic!("expected DESCRIBE KEYSPACE rows");
        };
        let ddl = String::from_utf8(rows[0][0].clone().unwrap()).unwrap();
        assert!(ddl.contains("COMMENT ON KEYSPACE ks IS 'primary app keyspace';"));
    }

    #[test]
    fn comment_on_column_type_and_field_update_metadata_and_describe() {
        let (_temp, processor) = test_processor();

        let result = processor
            .process_query(
                "COMMENT ON COLUMN events.name IS 'display name'",
                &QueryParams::default(),
                Some("cassandra"),
                Some("ks"),
            )
            .unwrap();
        assert!(matches!(result, QueryResult::SchemaChange { .. }));

        processor
            .process_query(
                "CREATE TYPE address (street text, zip int)",
                &QueryParams::default(),
                Some("cassandra"),
                Some("ks"),
            )
            .unwrap();
        processor
            .process_query(
                "COMMENT ON TYPE address IS 'postal address'",
                &QueryParams::default(),
                Some("cassandra"),
                Some("ks"),
            )
            .unwrap();
        processor
            .process_query(
                "COMMENT ON FIELD address.street IS 'street line'",
                &QueryParams::default(),
                Some("cassandra"),
                Some("ks"),
            )
            .unwrap();

        let snapshot = processor.catalog.read().snapshot();
        let table = snapshot.table("ks", "events").unwrap();
        assert_eq!(table.column("name").unwrap().comment, "display name");
        let user_type = snapshot.user_type("ks", "address").unwrap();
        assert_eq!(user_type.comment, "postal address");
        assert_eq!(
            user_type.field_comments.get("street").map(String::as_str),
            Some("street line")
        );
        drop(snapshot);

        let table_describe = processor
            .process_query(
                "DESCRIBE TABLE events",
                &QueryParams::default(),
                Some("cassandra"),
                Some("ks"),
            )
            .unwrap();
        let QueryResult::Rows { rows, .. } = table_describe else {
            panic!("expected DESCRIBE TABLE rows");
        };
        let table_ddl = String::from_utf8(rows[0][0].clone().unwrap()).unwrap();
        assert!(table_ddl.contains("COMMENT ON COLUMN ks.events.name IS 'display name';"));

        let type_describe = processor
            .process_query(
                "DESCRIBE TYPE address",
                &QueryParams::default(),
                Some("cassandra"),
                Some("ks"),
            )
            .unwrap();
        let QueryResult::Rows { rows, .. } = type_describe else {
            panic!("expected DESCRIBE TYPE rows");
        };
        let type_ddl = String::from_utf8(rows[0][0].clone().unwrap()).unwrap();
        assert!(type_ddl.contains("COMMENT ON TYPE ks.address IS 'postal address';"));
        assert!(type_ddl.contains("COMMENT ON FIELD ks.address.street IS 'street line';"));

        let generic_table_describe = processor
            .process_query(
                "DESCRIBE events",
                &QueryParams::default(),
                Some("cassandra"),
                Some("ks"),
            )
            .unwrap();
        let QueryResult::Rows { rows, .. } = generic_table_describe else {
            panic!("expected generic DESCRIBE table rows");
        };
        let generic_table_ddl = String::from_utf8(rows[0][0].clone().unwrap()).unwrap();
        assert!(generic_table_ddl.contains("CREATE TABLE ks.events"));

        let generic_type_describe = processor
            .process_query(
                "DESCRIBE address",
                &QueryParams::default(),
                Some("cassandra"),
                Some("ks"),
            )
            .unwrap();
        let QueryResult::Rows { rows, .. } = generic_type_describe else {
            panic!("expected generic DESCRIBE type rows");
        };
        let generic_type_ddl = String::from_utf8(rows[0][0].clone().unwrap()).unwrap();
        assert!(generic_type_ddl.contains("CREATE TYPE ks.address"));

        let generic_keyspace_describe = processor
            .process_query(
                "DESCRIBE ks",
                &QueryParams::default(),
                Some("cassandra"),
                Some("ks"),
            )
            .unwrap();
        let QueryResult::Rows { rows, .. } = generic_keyspace_describe else {
            panic!("expected generic DESCRIBE keyspace rows");
        };
        let generic_keyspace_ddl = String::from_utf8(rows[0][0].clone().unwrap()).unwrap();
        assert!(generic_keyspace_ddl.contains("CREATE KEYSPACE ks"));
    }

    #[test]
    fn prepared_metadata_id_uses_result_columns_and_client_id() {
        let (_temp, processor) = test_processor();
        let select_name = processor
            .process_prepare("SELECT name FROM events WHERE id = ?", Some("ks"))
            .unwrap();
        let select_scores = processor
            .process_prepare("SELECT scores FROM events WHERE id = ?", Some("ks"))
            .unwrap();

        assert_ne!(
            select_name.result_metadata_id,
            select_scores.result_metadata_id
        );
        let current_id = select_name.result_metadata_id.as_deref().unwrap();
        let stale_id = vec![0u8; current_id.len()];

        let unchanged = processor
            .process_execute(
                &select_name.id,
                Some(current_id),
                &params(vec![Some(int_bytes(42))]),
                Some("cassandra"),
                Some("ks"),
            )
            .unwrap();
        assert!(!unchanged.metadata_changed);
        assert!(unchanged.new_metadata_id.is_none());
        assert!(unchanged.omit_result_metadata);

        let changed = processor
            .process_execute(
                &select_name.id,
                Some(&stale_id),
                &params(vec![Some(int_bytes(42))]),
                Some("cassandra"),
                Some("ks"),
            )
            .unwrap();
        assert!(changed.metadata_changed);
        assert!(!changed.omit_result_metadata);
        assert_eq!(changed.new_metadata_id.as_deref(), Some(current_id));
    }

    #[test]
    fn prepared_result_metadata_includes_selection_selectors() {
        let (_temp, processor) = test_processor();
        let prepared = processor
            .process_prepare(
                "SELECT name AS event_name, writetime(name), ttl(name), now() AS generated_at, cast(bucket AS text) AS bucket_text FROM events",
                Some("ks"),
            )
            .unwrap();

        let specs = &prepared.result_metadata.col_specs;
        assert_eq!(specs.len(), 5);
        assert_eq!(specs[0].name, "event_name");
        assert_eq!(specs[0].col_type.id(), ColumnType::Varchar.id());
        assert_eq!(specs[1].name, "writetime(name)");
        assert_eq!(specs[1].col_type.id(), ColumnType::Bigint.id());
        assert_eq!(specs[2].name, "ttl(name)");
        assert_eq!(specs[2].col_type.id(), ColumnType::Int.id());
        assert_eq!(specs[3].name, "generated_at");
        assert_eq!(specs[3].col_type.id(), ColumnType::Timeuuid.id());
        assert_eq!(specs[4].name, "bucket_text");
        assert_eq!(specs[4].col_type.id(), ColumnType::Varchar.id());
    }

    fn test_processor() -> (TempDir, QueryProcessor) {
        let temp = TempDir::new().unwrap();
        let engine = Arc::new(
            StorageEngine::open(EngineConfig {
                data_directories: vec![temp.path().join("data")],
                commitlog: CommitLogConfig {
                    directory: temp.path().join("commitlog"),
                    ..CommitLogConfig::default()
                },
                ..EngineConfig::default()
            })
            .unwrap(),
        );

        let keyspace =
            KeyspaceMetadata::new("ks", KeyspaceParams::default()).with_table(events_table("ks"));
        let keyspace2 =
            KeyspaceMetadata::new("ks2", KeyspaceParams::default()).with_table(events_table("ks2"));
        let catalog = Arc::new(RwLock::new(
            SchemaCatalog::new()
                .with_keyspace(keyspace)
                .with_keyspace(keyspace2),
        ));
        let prepared_cache = Arc::new(PreparedCache::new());
        let executor = Arc::new(
            QueryExecutor::new(
                engine,
                Arc::clone(&catalog),
                Arc::new(InMemoryRoleManager::new()),
                Arc::new(AllowAllAuthorizer),
            )
            .with_prepared_cache(Arc::clone(&prepared_cache)),
        );

        (
            temp,
            QueryProcessor::new(executor, prepared_cache, Arc::clone(&catalog)),
        )
    }

    fn events_table(keyspace: &str) -> cassandra_schema::TableMetadata {
        TableMetadataBuilder::new(keyspace, "events")
            .add_column(ColumnMetadata::partition_key("id", 0, CqlType::Int))
            .add_column(ColumnMetadata::clustering(
                "bucket",
                0,
                CqlType::Int,
                ClusteringOrder::Asc,
            ))
            .add_column(ColumnMetadata::regular("name", CqlType::Varchar))
            .add_column(ColumnMetadata::regular(
                "scores",
                CqlType::List(Box::new(CqlType::Int), false),
            ))
            .build()
    }

    fn params(values: Vec<Option<Vec<u8>>>) -> QueryParams {
        QueryParams {
            consistency: Consistency::One,
            values,
            ..QueryParams::default()
        }
    }

    fn named_params(values: Vec<(&str, Option<Vec<u8>>)>) -> QueryParams {
        QueryParams {
            consistency: Consistency::One,
            value_names: Some(values.iter().map(|(name, _)| (*name).to_string()).collect()),
            values: values.into_iter().map(|(_, value)| value).collect(),
            ..QueryParams::default()
        }
    }

    fn int_bytes(value: i32) -> Vec<u8> {
        let mut bytes = vec![0u8; 4];
        BigEndian::write_i32(&mut bytes, value);
        bytes
    }
}
