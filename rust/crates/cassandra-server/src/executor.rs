// Licensed under Apache License, Version 2.0.

//! Query executor: executes planned CQL queries against the storage engine.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.cql3.QueryProcessor`
//! - `org.apache.cassandra.cql3.statements.*Statement`

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use parking_lot::RwLock;
use tracing::{debug, info};

use cassandra_cql::ast::{
    Assignment, ClusteringOrder as AstClusteringOrder, Literal, Relation, SelectColumns, Selector,
    Term,
};
use cassandra_cql::planner::{
    AlterKeyspacePlan, BatchPlan, CreateKeyspacePlan, CreateTablePlan, DeletePlan,
    DropKeyspacePlan, DropTablePlan, InsertPlan, QueryPlan, SelectPlan, TruncatePlan, UpdatePlan,
    UsePlan,
};
use cassandra_schema::{
    ClusteringOrder, ColumnKind, ColumnMetadata, KeyspaceMetadata, KeyspaceParams,
    ReplicationParams, SchemaCatalog,
};
use cassandra_storage::engine::StorageEngine;
use cassandra_storage::memtable::partition::{Cell, Row};
use cassandra_types::CqlType;

// ─── Errors ────────────────────────────────────────────────────────────────

#[derive(Debug, thiserror::Error)]
pub enum ExecutorError {
    #[error("Invalid query: {0}")]
    InvalidQuery(String),
    #[error("Keyspace '{0}' not found")]
    KeyspaceNotFound(String),
    #[error("Table '{0}.{1}' not found")]
    TableNotFound(String, String),
    #[error("Storage error: {0}")]
    StorageError(String),
    #[error("Schema error: {0}")]
    SchemaError(String),
}

// ─── Query Result ──────────────────────────────────────────────────────────

/// Result of executing a query.
#[derive(Debug, Clone)]
pub enum QueryResult {
    SchemaChange {
        change_type: String,
        target: String,
        keyspace: String,
        name: Option<String>,
    },
    Rows {
        columns: Vec<ResultColumn>,
        rows: Vec<Vec<Option<Vec<u8>>>>,
    },
    Void,
    SetKeyspace(String),
}

#[derive(Debug, Clone)]
pub struct ResultColumn {
    pub keyspace: String,
    pub table: String,
    pub name: String,
    pub cql_type: CqlType,
}

// ─── Executor ──────────────────────────────────────────────────────────────

pub struct QueryExecutor {
    engine: Arc<StorageEngine>,
    catalog: Arc<RwLock<SchemaCatalog>>,
}

impl QueryExecutor {
    pub fn new(engine: Arc<StorageEngine>, catalog: Arc<RwLock<SchemaCatalog>>) -> Self {
        Self { engine, catalog }
    }

    pub fn execute(&self, plan: &QueryPlan) -> Result<QueryResult, ExecutorError> {
        match plan {
            QueryPlan::Use(u) => self.execute_use(u),
            QueryPlan::CreateKeyspace(ck) => self.execute_create_keyspace(ck),
            QueryPlan::AlterKeyspace(ak) => self.execute_alter_keyspace(ak),
            QueryPlan::DropKeyspace(dk) => self.execute_drop_keyspace(dk),
            QueryPlan::CreateTable(ct) => self.execute_create_table(ct),
            QueryPlan::AlterTable(_) => Ok(QueryResult::Void), // TODO
            QueryPlan::DropTable(dt) => self.execute_drop_table(dt),
            QueryPlan::Insert(ins) => self.execute_insert(ins),
            QueryPlan::Update(upd) => self.execute_update(upd),
            QueryPlan::Delete(del) => self.execute_delete(del),
            QueryPlan::Select(sel) => self.execute_select(sel),
            QueryPlan::Truncate(_) => {
                info!("TRUNCATE executed (stub)");
                Ok(QueryResult::Void)
            }
            QueryPlan::Batch(batch) => self.execute_batch(batch),
        }
    }

    // ─── DDL ───────────────────────────────────────────────────────────

    fn execute_use(&self, plan: &UsePlan) -> Result<QueryResult, ExecutorError> {
        debug!(keyspace = %plan.keyspace, "USE");
        Ok(QueryResult::SetKeyspace(plan.keyspace.clone()))
    }

    fn execute_create_keyspace(
        &self,
        plan: &CreateKeyspacePlan,
    ) -> Result<QueryResult, ExecutorError> {
        let strategy_class = plan
            .replication
            .get("class")
            .cloned()
            .unwrap_or_else(|| "SimpleStrategy".into());

        let mut options = std::collections::BTreeMap::new();
        for (k, v) in &plan.replication {
            if k != "class" {
                options.insert(k.clone(), v.clone());
            }
        }

        let params = KeyspaceParams {
            durable_writes: plan.durable_writes,
            replication: ReplicationParams {
                strategy_class,
                options,
            },
        };

        let ks = KeyspaceMetadata::new(&plan.name, params);
        let mut catalog = self.catalog.write();
        *catalog = catalog.with_keyspace(ks);

        info!(keyspace = %plan.name, "Created keyspace");
        Ok(QueryResult::SchemaChange {
            change_type: "CREATED".into(),
            target: "KEYSPACE".into(),
            keyspace: plan.name.clone(),
            name: None,
        })
    }

    fn execute_alter_keyspace(
        &self,
        plan: &AlterKeyspacePlan,
    ) -> Result<QueryResult, ExecutorError> {
        let catalog = self.catalog.read();
        let snapshot = catalog.snapshot();
        let existing = snapshot
            .keyspace(&plan.name)
            .ok_or_else(|| ExecutorError::KeyspaceNotFound(plan.name.clone()))?;

        let mut params = existing.params.clone();
        if let Some(ref repl) = plan.replication {
            let strategy_class = repl
                .get("class")
                .cloned()
                .unwrap_or(params.replication.strategy_class.clone());
            let mut options = std::collections::BTreeMap::new();
            for (k, v) in repl {
                if k != "class" {
                    options.insert(k.clone(), v.clone());
                }
            }
            params.replication = ReplicationParams {
                strategy_class,
                options,
            };
        }
        if let Some(dw) = plan.durable_writes {
            params.durable_writes = dw;
        }

        drop(catalog);

        let ks = KeyspaceMetadata::new(&plan.name, params);
        let mut catalog = self.catalog.write();
        *catalog = catalog.with_keyspace(ks);

        Ok(QueryResult::SchemaChange {
            change_type: "UPDATED".into(),
            target: "KEYSPACE".into(),
            keyspace: plan.name.clone(),
            name: None,
        })
    }

    fn execute_drop_keyspace(
        &self,
        plan: &DropKeyspacePlan,
    ) -> Result<QueryResult, ExecutorError> {
        let mut catalog = self.catalog.write();
        *catalog = catalog.without_keyspace(&plan.name);
        info!(keyspace = %plan.name, "Dropped keyspace");
        Ok(QueryResult::SchemaChange {
            change_type: "DROPPED".into(),
            target: "KEYSPACE".into(),
            keyspace: plan.name.clone(),
            name: None,
        })
    }

    fn execute_create_table(
        &self,
        plan: &CreateTablePlan,
    ) -> Result<QueryResult, ExecutorError> {
        use cassandra_schema::TableMetadataBuilder;

        let mut builder = TableMetadataBuilder::new(&plan.keyspace, &plan.name);

        for (i, col) in plan.columns.iter().enumerate() {
            let kind = if plan.partition_key.contains(&col.name) {
                ColumnKind::PartitionKey
            } else if plan.clustering_key.contains(&col.name) {
                ColumnKind::Clustering
            } else if col.is_static {
                ColumnKind::Static
            } else {
                ColumnKind::Regular
            };

            let position = if kind == ColumnKind::PartitionKey {
                plan.partition_key
                    .iter()
                    .position(|n| n == &col.name)
                    .unwrap_or(0) as u32
            } else if kind == ColumnKind::Clustering {
                plan.clustering_key
                    .iter()
                    .position(|n| n == &col.name)
                    .unwrap_or(0) as u32
            } else {
                i as u32
            };

            let clustering_order = if kind == ColumnKind::Clustering {
                plan.clustering_order
                    .iter()
                    .find(|(n, _)| n == &col.name)
                    .map(|(_, o)| match o {
                        AstClusteringOrder::Asc => ClusteringOrder::Asc,
                        AstClusteringOrder::Desc => ClusteringOrder::Desc,
                    })
                    .unwrap_or(ClusteringOrder::Asc)
            } else {
                ClusteringOrder::None
            };

            builder = builder.add_column(ColumnMetadata::new(
                col.name.clone(),
                kind,
                position,
                col.cql_type.clone(),
                clustering_order,
            ));
        }

        let table = builder.build();

        let catalog = self.catalog.read();
        let snapshot = catalog.snapshot();
        let ks = snapshot
            .keyspace(&plan.keyspace)
            .ok_or_else(|| ExecutorError::KeyspaceNotFound(plan.keyspace.clone()))?
            .clone()
            .with_table(table);
        drop(catalog);

        let mut catalog = self.catalog.write();
        *catalog = catalog.with_keyspace(ks);

        info!(keyspace = %plan.keyspace, table = %plan.name, "Created table");
        Ok(QueryResult::SchemaChange {
            change_type: "CREATED".into(),
            target: "TABLE".into(),
            keyspace: plan.keyspace.clone(),
            name: Some(plan.name.clone()),
        })
    }

    fn execute_drop_table(&self, plan: &DropTablePlan) -> Result<QueryResult, ExecutorError> {
        let catalog = self.catalog.read();
        let snapshot = catalog.snapshot();
        let ks = snapshot
            .keyspace(&plan.keyspace)
            .ok_or_else(|| ExecutorError::KeyspaceNotFound(plan.keyspace.clone()))?
            .clone()
            .without_table(&plan.name);
        drop(catalog);

        let mut catalog = self.catalog.write();
        *catalog = catalog.with_keyspace(ks);

        Ok(QueryResult::SchemaChange {
            change_type: "DROPPED".into(),
            target: "TABLE".into(),
            keyspace: plan.keyspace.clone(),
            name: Some(plan.name.clone()),
        })
    }

    // ─── DML ───────────────────────────────────────────────────────────

    fn execute_insert(&self, plan: &InsertPlan) -> Result<QueryResult, ExecutorError> {
        let now = current_timestamp_micros();

        let catalog = self.catalog.read();
        let snapshot = catalog.snapshot();
        let table_meta = snapshot
            .table(&plan.keyspace, &plan.table)
            .ok_or_else(|| {
                ExecutorError::TableNotFound(plan.keyspace.clone(), plan.table.clone())
            })?;

        let pk_cols = table_meta.partition_key_columns();
        let ck_cols = table_meta.clustering_columns();
        let pk_names: Vec<&str> = pk_cols.iter().map(|c| c.name.as_str()).collect();
        let ck_names: Vec<&str> = ck_cols.iter().map(|c| c.name.as_str()).collect();

        let mut pk_bytes = Vec::new();
        let mut ck_bytes = Vec::new();
        let mut cells = Vec::new();

        for (i, col_name) in plan.columns.iter().enumerate() {
            let val_bytes = if i < plan.values.len() {
                term_to_bytes(&plan.values[i])
            } else {
                None
            };

            if pk_names.contains(&col_name.as_str()) {
                if let Some(v) = &val_bytes {
                    pk_bytes.extend_from_slice(v);
                }
            } else if ck_names.contains(&col_name.as_str()) {
                if let Some(v) = &val_bytes {
                    ck_bytes.extend_from_slice(v);
                }
            } else {
                cells.push(Cell {
                    column: col_name.clone(),
                    value: val_bytes,
                    timestamp: now,
                    ttl: 0,
                    local_deletion_time: None,
                    is_tombstone: false,
                });
            }
        }

        drop(catalog);

        let row = Row {
            clustering_key: ck_bytes,
            cells,
            is_tombstone: false,
            local_deletion_time: None,
        };

        self.engine
            .apply_mutation(&plan.keyspace, &plan.table, pk_bytes, vec![row], now)
            .map_err(|e| ExecutorError::StorageError(e.to_string()))?;

        Ok(QueryResult::Void)
    }

    fn execute_update(&self, plan: &UpdatePlan) -> Result<QueryResult, ExecutorError> {
        let now = current_timestamp_micros();

        let catalog = self.catalog.read();
        let snapshot = catalog.snapshot();
        let table_meta = snapshot
            .table(&plan.keyspace, &plan.table)
            .ok_or_else(|| {
                ExecutorError::TableNotFound(plan.keyspace.clone(), plan.table.clone())
            })?;

        let pk_cols = table_meta.partition_key_columns();
        let ck_cols = table_meta.clustering_columns();
        let pk_names: Vec<&str> = pk_cols.iter().map(|c| c.name.as_str()).collect();
        let ck_names: Vec<&str> = ck_cols.iter().map(|c| c.name.as_str()).collect();

        let mut pk_bytes = Vec::new();
        let mut ck_bytes = Vec::new();

        for rel in &plan.where_clause {
            let val = term_to_bytes(&rel.value);
            if pk_names.contains(&rel.column.as_str()) {
                if let Some(v) = val {
                    pk_bytes.extend_from_slice(&v);
                }
            } else if ck_names.contains(&rel.column.as_str()) {
                if let Some(v) = val {
                    ck_bytes.extend_from_slice(&v);
                }
            }
        }

        let cells: Vec<Cell> = plan
            .assignments
            .iter()
            .map(|a| Cell {
                column: a.column.clone(),
                value: term_to_bytes(&a.value),
                timestamp: now,
                ttl: 0,
                local_deletion_time: None,
                is_tombstone: false,
            })
            .collect();

        drop(catalog);

        let row = Row {
            clustering_key: ck_bytes,
            cells,
            is_tombstone: false,
            local_deletion_time: None,
        };

        self.engine
            .apply_mutation(&plan.keyspace, &plan.table, pk_bytes, vec![row], now)
            .map_err(|e| ExecutorError::StorageError(e.to_string()))?;

        Ok(QueryResult::Void)
    }

    fn execute_delete(&self, plan: &DeletePlan) -> Result<QueryResult, ExecutorError> {
        let now = current_timestamp_micros();
        let now_secs = (now / 1_000_000) as i32;

        let catalog = self.catalog.read();
        let snapshot = catalog.snapshot();
        let table_meta = snapshot
            .table(&plan.keyspace, &plan.table)
            .ok_or_else(|| {
                ExecutorError::TableNotFound(plan.keyspace.clone(), plan.table.clone())
            })?;

        let pk_cols = table_meta.partition_key_columns();
        let ck_cols = table_meta.clustering_columns();
        let pk_names: Vec<&str> = pk_cols.iter().map(|c| c.name.as_str()).collect();
        let ck_names: Vec<&str> = ck_cols.iter().map(|c| c.name.as_str()).collect();

        let mut pk_bytes = Vec::new();
        let mut ck_bytes = Vec::new();

        for rel in &plan.where_clause {
            let val = term_to_bytes(&rel.value);
            if pk_names.contains(&rel.column.as_str()) {
                if let Some(v) = val {
                    pk_bytes.extend_from_slice(&v);
                }
            } else if ck_names.contains(&rel.column.as_str()) {
                if let Some(v) = val {
                    ck_bytes.extend_from_slice(&v);
                }
            }
        }

        drop(catalog);

        let is_row_delete = plan.columns.is_empty();

        let cells = if is_row_delete {
            Vec::new()
        } else {
            plan.columns
                .iter()
                .map(|col| Cell {
                    column: col.clone(),
                    value: None,
                    timestamp: now,
                    ttl: 0,
                    local_deletion_time: Some(now_secs),
                    is_tombstone: true,
                })
                .collect()
        };

        let row = Row {
            clustering_key: ck_bytes,
            cells,
            is_tombstone: is_row_delete,
            local_deletion_time: if is_row_delete {
                Some(now_secs)
            } else {
                None
            },
        };

        self.engine
            .apply_mutation(&plan.keyspace, &plan.table, pk_bytes, vec![row], now)
            .map_err(|e| ExecutorError::StorageError(e.to_string()))?;

        Ok(QueryResult::Void)
    }

    fn execute_select(&self, plan: &SelectPlan) -> Result<QueryResult, ExecutorError> {
        let catalog = self.catalog.read();
        let snapshot = catalog.snapshot();
        let table_meta = snapshot
            .table(&plan.keyspace, &plan.table)
            .ok_or_else(|| {
                ExecutorError::TableNotFound(plan.keyspace.clone(), plan.table.clone())
            })?;

        let pk_cols = table_meta.partition_key_columns();
        let pk_names: Vec<&str> = pk_cols.iter().map(|c| c.name.as_str()).collect();

        let mut pk_bytes = Vec::new();
        for rel in &plan.where_clause {
            if pk_names.contains(&rel.column.as_str()) {
                if let Some(v) = term_to_bytes(&rel.value) {
                    pk_bytes.extend_from_slice(&v);
                }
            }
        }

        // Build result column metadata
        let result_columns: Vec<ResultColumn> = match &plan.columns {
            SelectColumns::All => table_meta
                .columns
                .iter()
                .map(|col| ResultColumn {
                    keyspace: plan.keyspace.clone(),
                    table: plan.table.clone(),
                    name: col.name.clone(),
                    cql_type: col.column_type.clone(),
                })
                .collect(),
            SelectColumns::Named(selectors) => selectors
                .iter()
                .filter_map(|sel| match sel {
                    Selector::Column(name) => {
                        let col = table_meta.columns.iter().find(|c| c.name == *name)?;
                        Some(ResultColumn {
                            keyspace: plan.keyspace.clone(),
                            table: plan.table.clone(),
                            name: col.name.clone(),
                            cql_type: col.column_type.clone(),
                        })
                    }
                    _ => None,
                })
                .collect(),
        };

        drop(catalog);

        if pk_bytes.is_empty() {
            return Ok(QueryResult::Rows {
                columns: result_columns,
                rows: Vec::new(),
            });
        }

        let partition = self
            .engine
            .read_partition(&plan.keyspace, &plan.table, &pk_bytes)
            .map_err(|e| ExecutorError::StorageError(e.to_string()))?;

        let mut result_rows = Vec::new();

        if let Some(pd) = partition {
            let now_secs =
                (current_timestamp_micros() / 1_000_000) as i32;
            let live = pd.live_rows(now_secs);

            for row in live {
                let mut result_row = Vec::new();
                for rc in &result_columns {
                    let value = row
                        .cells
                        .iter()
                        .find(|c| c.column == rc.name && c.is_live_at(now_secs))
                        .and_then(|c| c.value.clone());
                    result_row.push(value);
                }
                result_rows.push(result_row);
            }

            if let Some(ref limit_term) = plan.limit {
                if let Some(limit) = term_to_i64(limit_term) {
                    result_rows.truncate(limit as usize);
                }
            }
        }

        Ok(QueryResult::Rows {
            columns: result_columns,
            rows: result_rows,
        })
    }

    fn execute_batch(&self, plan: &BatchPlan) -> Result<QueryResult, ExecutorError> {
        for sub in &plan.plans {
            self.execute(sub)?;
        }
        Ok(QueryResult::Void)
    }
}

// ─── Helpers ───────────────────────────────────────────────────────────────

fn current_timestamp_micros() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as i64
}

fn term_to_bytes(term: &Term) -> Option<Vec<u8>> {
    match term {
        Term::Literal(lit) => literal_to_bytes(lit),
        Term::BindMarker(_) => None,
        Term::FunctionCall(_, _) => None,
        Term::TypeHint(_, inner) => term_to_bytes(inner),
        Term::CollectionLiteral(_) | Term::MapLiteral(_) | Term::TupleLiteral(_) => None,
    }
}

fn literal_to_bytes(lit: &Literal) -> Option<Vec<u8>> {
    match lit {
        Literal::String(s) => Some(s.as_bytes().to_vec()),
        Literal::Integer(n) => Some(n.to_be_bytes().to_vec()),
        Literal::Float(f) => Some(f.to_be_bytes().to_vec()),
        Literal::Boolean(b) => Some(vec![if *b { 1 } else { 0 }]),
        Literal::Blob(bytes) => Some(bytes.clone()),
        Literal::Uuid(s) => parse_uuid_bytes(s),
        Literal::Null => None,
    }
}

fn term_to_i64(term: &Term) -> Option<i64> {
    match term {
        Term::Literal(Literal::Integer(n)) => Some(*n),
        _ => None,
    }
}

fn parse_uuid_bytes(s: &str) -> Option<Vec<u8>> {
    let hex: String = s.chars().filter(|c| *c != '-').collect();
    (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).ok())
        .collect()
}
