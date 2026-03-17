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

use cassandra_security::Resource as SecurityResource;
use cassandra_security::{Authorizer, Permission, Role, RoleManager, RoleOptions};

use cassandra_cql::ast::{
    ClusteringOrder as AstClusteringOrder, DescribeTarget, Literal, RelationOp, SelectColumns,
    Selector, Term,
};
use crate::term_binding::typed_term_to_bytes;
use cassandra_cql::planner::{
    AlterKeyspacePlan, AlterMaterializedViewPlan, AlterRolePlan, BatchPlan,
    CreateAggregatePlan, CreateFunctionPlan, CreateIndexPlan, CreateKeyspacePlan,
    CreateMaterializedViewPlan, CreateRolePlan, CreateTablePlan, CreateTriggerPlan,
    CreateTypePlan, DeletePlan, DropAggregatePlan, DropFunctionPlan, DropIndexPlan,
    DropKeyspacePlan, DropMaterializedViewPlan, DropRolePlan, DropTablePlan, DropTriggerPlan,
    DescribePlan, DropTypePlan, GrantPlan, InsertPlan, ListRolesPlan, QueryPlan, RevokePlan,
    SelectPlan, UpdatePlan, UsePlan,
};
use cassandra_cql::prepared::PreparedCache;
use cassandra_cql::triggers::{MutationEvent, MutationType, TriggerRegistry};
use cassandra_cql::uda::UdaRegistry;
use cassandra_cql::udf::{UdfMetadata, UdfRegistry};
use cassandra_cql::udf_wasm::WasmUdfExecutor;
use cassandra_schema::{
    ClusteringOrder, ColumnKind, ColumnMetadata, KeyspaceMetadata, KeyspaceParams,
    ReplicationParams, SchemaCatalog, TriggerDefinition, UserAggregate, UserFunction, UserType,
    ViewMetadata,
};
use cassandra_storage::commitlog::{
    CellMutation, Mutation, MutationRow, RangeTombstoneMarker as CommitlogRangeTombstone,
    TombstoneMarker,
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
    #[allow(dead_code)]
    #[error("Schema error: {0}")]
    SchemaError(String),
}

// ─── Query Result ──────────────────────────────────────────────────────────

/// Result of executing a query.
#[allow(dead_code)]
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

#[allow(dead_code)]
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
    role_manager: Arc<dyn RoleManager>,
    authorizer: Arc<dyn Authorizer>,
    prepared_cache: Option<Arc<PreparedCache>>,
    udf_registry: Arc<UdfRegistry>,
    uda_registry: Arc<UdaRegistry>,
    trigger_registry: Arc<TriggerRegistry>,
}

impl QueryExecutor {
    pub fn new(
        engine: Arc<StorageEngine>,
        catalog: Arc<RwLock<SchemaCatalog>>,
        role_manager: Arc<dyn RoleManager>,
        authorizer: Arc<dyn Authorizer>,
    ) -> Self {
        Self {
            engine,
            catalog,
            role_manager,
            authorizer,
            prepared_cache: None,
            udf_registry: Arc::new(UdfRegistry::new()),
            uda_registry: Arc::new(UdaRegistry::new()),
            trigger_registry: Arc::new(TriggerRegistry::new()),
        }
    }

    /// Set the prepared statement cache for schema-change invalidation.
    pub fn with_prepared_cache(mut self, cache: Arc<PreparedCache>) -> Self {
        self.prepared_cache = Some(cache);
        self
    }

    pub fn catalog(&self) -> Arc<RwLock<SchemaCatalog>> {
        Arc::clone(&self.catalog)
    }

    pub fn execute(
        &self,
        plan: &QueryPlan,
        user: Option<&str>,
    ) -> Result<QueryResult, ExecutorError> {
        let result = match plan {
            QueryPlan::Use(u) => self.execute_use(u),
            QueryPlan::CreateKeyspace(ck) => self.execute_create_keyspace(ck),
            QueryPlan::AlterKeyspace(ak) => self.execute_alter_keyspace(ak),
            QueryPlan::DropKeyspace(dk) => self.execute_drop_keyspace(dk),
            QueryPlan::CreateTable(ct) => self.execute_create_table(ct),
            QueryPlan::AlterTable(_) => Ok(QueryResult::Void), // GAP(gap_guard_cql_functions): executor function dispatch — tracked in gap_guards.rs
            QueryPlan::DropTable(dt) => self.execute_drop_table(dt),
            QueryPlan::Insert(ins) => self.execute_insert(ins),
            QueryPlan::Update(upd) => self.execute_update(upd),
            QueryPlan::Delete(del) => self.execute_delete(del),
            QueryPlan::Select(sel) => self.execute_select(sel, user),
            QueryPlan::Truncate(_) => {
                info!("TRUNCATE executed (stub)");
                Ok(QueryResult::Void)
            }
            QueryPlan::Batch(batch) => self.execute_batch(batch, user),
            QueryPlan::CreateRole(cr) => self.execute_create_role(cr),
            QueryPlan::AlterRole(ar) => self.execute_alter_role(ar),
            QueryPlan::DropRole(dr) => self.execute_drop_role(dr),
            QueryPlan::Grant(gr) => self.execute_grant(gr),
            QueryPlan::Revoke(rv) => self.execute_revoke(rv),
            QueryPlan::ListRoles(lr) => self.execute_list_roles(lr),
            QueryPlan::CreateIndex(ci) => self.execute_create_index(ci),
            QueryPlan::DropIndex(di) => self.execute_drop_index(di),
            QueryPlan::CreateMaterializedView(cmv) => self.execute_create_mv(cmv),
            QueryPlan::DropMaterializedView(dmv) => self.execute_drop_mv(dmv),
            QueryPlan::AlterMaterializedView(amv) => self.execute_alter_mv(amv),
            QueryPlan::CreateType(ct) => self.execute_create_type(ct),
            QueryPlan::DropType(dt) => self.execute_drop_type(dt),
            QueryPlan::CreateFunction(cf) => self.execute_create_function(cf),
            QueryPlan::DropFunction(df) => self.execute_drop_function(df),
            QueryPlan::CreateAggregate(ca) => self.execute_create_aggregate(ca),
            QueryPlan::DropAggregate(da) => self.execute_drop_aggregate(da),
            QueryPlan::CreateTrigger(ct) => self.execute_create_trigger(ct),
            QueryPlan::DropTrigger(dt) => self.execute_drop_trigger(dt),
            QueryPlan::Describe(desc) => self.execute_describe(desc),
        };

        // Invalidate prepared cache after schema-altering DDL
        if let Ok(ref result) = result {
            if plan.is_schema_altering() {
                if let Some(ref cache) = self.prepared_cache {
                    let version = self.catalog.read().version();
                    cache.invalidate_for_schema_change(version);
                }
            }
        }

        result
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

    fn execute_drop_keyspace(&self, plan: &DropKeyspacePlan) -> Result<QueryResult, ExecutorError> {
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

    fn execute_create_table(&self, plan: &CreateTablePlan) -> Result<QueryResult, ExecutorError> {
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
                col.masked_with.clone(),
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

    fn execute_create_index(&self, plan: &CreateIndexPlan) -> Result<QueryResult, ExecutorError> {
        use cassandra_schema::index::{IndexKind, IndexMetadata};
        use cassandra_storage::index::{IndexDefinition, IndexType};

        // ── Validation ──────────────────────────────────────────────────
        let is_sai = plan
            .custom_class
            .as_deref()
            .map(|c| c.contains("StorageAttachedIndex"))
            .unwrap_or(false);

        #[cfg(not(feature = "sasi"))]
        {
            let is_sasi = plan
                .custom_class
                .as_deref()
                .map(|c| c.contains("SASIIndex"))
                .unwrap_or(false);
            if is_sasi {
                return Err(ExecutorError::InvalidQuery(
                    "SASI index support is not enabled (feature flag 'sasi' is disabled)".into(),
                ));
            }
        }

        // Validate SAI options if applicable
        if is_sai {
            let warnings =
                cassandra_config::sai_options::validate_index_definition(&plan.options);
            for w in &warnings {
                debug!(warning = %w, "SAI index option warning");
            }
        }

        // Check for duplicate index on same column
        {
            let catalog = self.catalog.read();
            let snapshot = catalog.snapshot();
            if let Some(table_meta) = snapshot.table(&plan.keyspace, &plan.table) {
                for idx in &table_meta.indexes {
                    if idx.target_column() == Some(&plan.column)
                        && idx.name != plan.index_name
                    {
                        return Err(ExecutorError::InvalidQuery(format!(
                            "An index already exists on column '{}' (index '{}')",
                            plan.column, idx.name
                        )));
                    }
                }
            }
        }

        let kind = if plan.custom_class.is_some() {
            IndexKind::Custom
        } else {
            IndexKind::Keys
        };

        let mut options = plan.options.clone();
        options.insert("target".to_string(), plan.column.clone());
        if let Some(ref class) = plan.custom_class {
            options.insert("class_name".to_string(), class.clone());
        }

        let idx_meta = IndexMetadata::new(
            plan.index_name.clone(),
            plan.index_name.clone(),
            kind,
            options,
        );

        // Update schema catalog
        let catalog = self.catalog.read();
        let snapshot = catalog.snapshot();
        let ks = snapshot
            .keyspace(&plan.keyspace)
            .ok_or_else(|| ExecutorError::KeyspaceNotFound(plan.keyspace.clone()))?
            .clone()
            .with_table_index(&plan.table, idx_meta);
        drop(catalog);

        let mut catalog = self.catalog.write();
        *catalog = catalog.with_keyspace(ks);
        drop(catalog);

        // Register in storage engine
        let index_type = if plan
            .custom_class
            .as_deref()
            .map(|c| c.contains("StorageAttachedIndex"))
            .unwrap_or(false)
        {
            IndexType::Sai
        } else {
            IndexType::Legacy
        };

        let def = IndexDefinition {
            name: plan.index_name.clone(),
            keyspace: plan.keyspace.clone(),
            table: plan.table.clone(),
            column: plan.column.clone(),
            index_type,
            options: plan.options.clone(),
        };

        let cf_name = format!("{}.{}", plan.keyspace, plan.table);
        let _ = self.engine.rebuild_index(&cf_name, def);

        info!(
            keyspace = %plan.keyspace,
            table = %plan.table,
            index = %plan.index_name,
            "Created index"
        );

        Ok(QueryResult::SchemaChange {
            change_type: "CREATED".into(),
            target: "INDEX".into(),
            keyspace: plan.keyspace.clone(),
            name: Some(plan.index_name.clone()),
        })
    }

    fn execute_drop_index(&self, plan: &DropIndexPlan) -> Result<QueryResult, ExecutorError> {
        let catalog = self.catalog.read();
        let snapshot = catalog.snapshot();
        let ks_meta = snapshot
            .keyspace(&plan.keyspace)
            .ok_or_else(|| ExecutorError::KeyspaceNotFound(plan.keyspace.clone()))?;

        let table_name = match ks_meta.find_indexed_table(&plan.index_name) {
            Some(t) => t.to_string(),
            None => {
                if plan.if_exists {
                    return Ok(QueryResult::Void);
                }
                return Err(ExecutorError::InvalidQuery(format!(
                    "Index '{}' not found in keyspace '{}'",
                    plan.index_name, plan.keyspace
                )));
            }
        };

        let updated_ks = ks_meta
            .clone()
            .without_table_index(&table_name, &plan.index_name);
        drop(catalog);

        let mut catalog = self.catalog.write();
        *catalog = catalog.with_keyspace(updated_ks);
        drop(catalog);

        // Unregister from storage engine
        let cf_name = format!("{}.{}", plan.keyspace, table_name);
        let mgrs = self.engine.index_managers.read();
        if let Some(mgr) = mgrs.get(&cf_name) {
            mgr.unregister(&plan.index_name);
        }

        info!(
            keyspace = %plan.keyspace,
            index = %plan.index_name,
            "Dropped index"
        );

        Ok(QueryResult::SchemaChange {
            change_type: "DROPPED".into(),
            target: "INDEX".into(),
            keyspace: plan.keyspace.clone(),
            name: Some(plan.index_name.clone()),
        })
    }

    // ─── MV DDL ────────────────────────────────────────────────────────

    fn execute_create_mv(
        &self,
        plan: &CreateMaterializedViewPlan,
    ) -> Result<QueryResult, ExecutorError> {
        let columns: Vec<String> = match &plan.select_columns {
            SelectColumns::All => Vec::new(),
            SelectColumns::Named(selectors) => selectors
                .iter()
                .filter_map(|s| match s {
                    Selector::Column(name) => Some(name.clone()),
                    _ => None,
                })
                .collect(),
        };

        let include_all = matches!(plan.select_columns, SelectColumns::All);
        let where_clause_str = plan
            .where_clause
            .iter()
            .map(|r| format!("{} {:?} {:?}", r.column, r.op, r.value))
            .collect::<Vec<_>>()
            .join(" AND ");

        let view = ViewMetadata::new(&plan.name, &plan.keyspace, &plan.base_table)
            .with_include_all_columns(include_all)
            .with_where_clause(where_clause_str)
            .with_partition_key(plan.partition_key.clone())
            .with_clustering_key(plan.clustering_key.clone());

        let view = columns.into_iter().fold(view, |v, col| v.with_column(col));

        let catalog = self.catalog.read();
        let snapshot = catalog.snapshot();
        let ks = snapshot
            .keyspace(&plan.keyspace)
            .ok_or_else(|| ExecutorError::KeyspaceNotFound(plan.keyspace.clone()))?
            .clone()
            .with_view(view);
        drop(catalog);

        let mut catalog = self.catalog.write();
        *catalog = catalog.with_keyspace(ks);

        info!(keyspace = %plan.keyspace, view = %plan.name, "Created materialized view");
        Ok(QueryResult::SchemaChange {
            change_type: "CREATED".into(),
            target: "TABLE".into(),
            keyspace: plan.keyspace.clone(),
            name: Some(plan.name.clone()),
        })
    }

    fn execute_drop_mv(
        &self,
        plan: &DropMaterializedViewPlan,
    ) -> Result<QueryResult, ExecutorError> {
        let catalog = self.catalog.read();
        let snapshot = catalog.snapshot();
        let ks = snapshot
            .keyspace(&plan.keyspace)
            .ok_or_else(|| ExecutorError::KeyspaceNotFound(plan.keyspace.clone()))?
            .clone()
            .without_view(&plan.name);
        drop(catalog);

        let mut catalog = self.catalog.write();
        *catalog = catalog.with_keyspace(ks);

        info!(keyspace = %plan.keyspace, view = %plan.name, "Dropped materialized view");
        Ok(QueryResult::SchemaChange {
            change_type: "DROPPED".into(),
            target: "TABLE".into(),
            keyspace: plan.keyspace.clone(),
            name: Some(plan.name.clone()),
        })
    }

    fn execute_alter_mv(
        &self,
        plan: &AlterMaterializedViewPlan,
    ) -> Result<QueryResult, ExecutorError> {
        // ALTER MV only changes table options, which we store as view metadata.
        // For now, acknowledge the change without modifying stored options.
        info!(keyspace = %plan.keyspace, view = %plan.name, "Altered materialized view (options update stub)");
        Ok(QueryResult::SchemaChange {
            change_type: "UPDATED".into(),
            target: "TABLE".into(),
            keyspace: plan.keyspace.clone(),
            name: Some(plan.name.clone()),
        })
    }

    // ─── UDT/UDF/UDA/Trigger DDL ─────────────────────────────────────

    fn execute_create_type(&self, plan: &CreateTypePlan) -> Result<QueryResult, ExecutorError> {
        let mut udt = UserType::new(&plan.keyspace, &plan.name);
        for (field_name, field_type) in &plan.fields {
            udt = udt.with_field(field_name, field_type);
        }

        let catalog = self.catalog.read();
        let snapshot = catalog.snapshot();
        let ks = snapshot
            .keyspace(&plan.keyspace)
            .ok_or_else(|| ExecutorError::KeyspaceNotFound(plan.keyspace.clone()))?
            .clone()
            .with_type(udt);
        drop(catalog);

        let mut catalog = self.catalog.write();
        *catalog = catalog.with_keyspace(ks);

        info!(keyspace = %plan.keyspace, type_name = %plan.name, "Created type");
        Ok(QueryResult::SchemaChange {
            change_type: "CREATED".into(),
            target: "TYPE".into(),
            keyspace: plan.keyspace.clone(),
            name: Some(plan.name.clone()),
        })
    }

    fn execute_drop_type(&self, plan: &DropTypePlan) -> Result<QueryResult, ExecutorError> {
        let catalog = self.catalog.read();
        let snapshot = catalog.snapshot();
        let ks = snapshot
            .keyspace(&plan.keyspace)
            .ok_or_else(|| ExecutorError::KeyspaceNotFound(plan.keyspace.clone()))?
            .clone()
            .without_type(&plan.name);
        drop(catalog);

        let mut catalog = self.catalog.write();
        *catalog = catalog.with_keyspace(ks);

        info!(keyspace = %plan.keyspace, type_name = %plan.name, "Dropped type");
        Ok(QueryResult::SchemaChange {
            change_type: "DROPPED".into(),
            target: "TYPE".into(),
            keyspace: plan.keyspace.clone(),
            name: Some(plan.name.clone()),
        })
    }

    fn execute_create_function(
        &self,
        plan: &CreateFunctionPlan,
    ) -> Result<QueryResult, ExecutorError> {
        let mut udf =
            UserFunction::new(&plan.keyspace, &plan.name, &plan.return_type, &plan.language, &plan.body)
                .with_called_on_null_input(plan.called_on_null_input);
        for (arg_name, arg_type) in &plan.args {
            udf = udf.with_arg(arg_name, arg_type);
        }

        let catalog = self.catalog.read();
        let snapshot = catalog.snapshot();
        let ks = snapshot
            .keyspace(&plan.keyspace)
            .ok_or_else(|| ExecutorError::KeyspaceNotFound(plan.keyspace.clone()))?
            .clone()
            .with_function(udf);
        drop(catalog);

        let mut catalog = self.catalog.write();
        *catalog = catalog.with_keyspace(ks);

        // Wire UDF executor into the runtime registry based on language.
        match plan.language.as_str() {
            "wasm" => {
                let bytecode = plan.body.as_bytes();
                match WasmUdfExecutor::new(plan.name.clone(), bytecode) {
                    Ok(executor) => {
                        let metadata = UdfMetadata {
                            keyspace: plan.keyspace.clone(),
                            name: plan.name.clone(),
                            args: plan.args.clone(),
                            return_type: plan.return_type.clone(),
                            language: plan.language.clone(),
                            body: plan.body.clone(),
                            called_on_null_input: plan.called_on_null_input,
                        };
                        if let Err(e) = self
                            .udf_registry
                            .register(metadata, Arc::new(executor))
                        {
                            debug!(error = %e, "Failed to register WASM UDF executor");
                        }
                    }
                    Err(e) => {
                        debug!(
                            function = %plan.name,
                            error = %e,
                            "WASM UDF executor not available; metadata stored but function cannot execute"
                        );
                    }
                }
            }
            "rust" => {
                debug!(
                    function = %plan.name,
                    "Native Rust UDF registered in schema; executor must be provided at compile time"
                );
            }
            other => {
                debug!(
                    function = %plan.name,
                    language = %other,
                    "Unsupported UDF language; metadata stored but function cannot execute"
                );
            }
        }

        info!(keyspace = %plan.keyspace, function = %plan.name, "Created function");
        Ok(QueryResult::SchemaChange {
            change_type: "CREATED".into(),
            target: "FUNCTION".into(),
            keyspace: plan.keyspace.clone(),
            name: Some(plan.name.clone()),
        })
    }

    fn execute_drop_function(
        &self,
        plan: &DropFunctionPlan,
    ) -> Result<QueryResult, ExecutorError> {
        let signature = format!("{}({})", plan.name, plan.arg_types.join(", "));

        let catalog = self.catalog.read();
        let snapshot = catalog.snapshot();
        let ks = snapshot
            .keyspace(&plan.keyspace)
            .ok_or_else(|| ExecutorError::KeyspaceNotFound(plan.keyspace.clone()))?
            .clone()
            .without_function(&signature);
        drop(catalog);

        let mut catalog = self.catalog.write();
        *catalog = catalog.with_keyspace(ks);

        info!(keyspace = %plan.keyspace, function = %plan.name, "Dropped function");
        Ok(QueryResult::SchemaChange {
            change_type: "DROPPED".into(),
            target: "FUNCTION".into(),
            keyspace: plan.keyspace.clone(),
            name: Some(plan.name.clone()),
        })
    }

    fn execute_create_aggregate(
        &self,
        plan: &CreateAggregatePlan,
    ) -> Result<QueryResult, ExecutorError> {
        let mut uda = UserAggregate::new(&plan.keyspace, &plan.name, &plan.stype, &plan.sfunc);
        for arg_type in &plan.arg_types {
            uda = uda.with_arg_type(arg_type);
        }
        if let Some(ref ff) = plan.finalfunc {
            uda = uda.with_finalfunc(ff);
        }
        if let Some(ref ic) = plan.initcond {
            uda = uda.with_initcond(ic);
        }

        let catalog = self.catalog.read();
        let snapshot = catalog.snapshot();
        let ks = snapshot
            .keyspace(&plan.keyspace)
            .ok_or_else(|| ExecutorError::KeyspaceNotFound(plan.keyspace.clone()))?
            .clone()
            .with_aggregate(uda);
        drop(catalog);

        let mut catalog = self.catalog.write();
        *catalog = catalog.with_keyspace(ks);

        // Wire UDA: resolve SFUNC and FINALFUNC from UdfRegistry, register in UdaRegistry.
        let uda_metadata = cassandra_cql::uda::UdaMetadata {
            keyspace: plan.keyspace.clone(),
            name: plan.name.clone(),
            arg_types: plan.arg_types.clone(),
            state_type: plan.stype.clone(),
            sfunc_name: plan.sfunc.clone(),
            finalfunc_name: plan.finalfunc.clone(),
            initcond: plan.initcond.clone(),
        };

        // Attempt to resolve the state function from the UDF registry.
        // Build a signature for sfunc: it takes (state_type, arg_types...) as arguments.
        let mut sfunc_args: Vec<(String, String)> = vec![("state".into(), plan.stype.clone())];
        for (i, arg_type) in plan.arg_types.iter().enumerate() {
            sfunc_args.push((format!("arg{}", i), arg_type.clone()));
        }
        let sfunc_resolved = self.udf_registry.get(&plan.keyspace, &plan.sfunc, &sfunc_args);

        if sfunc_resolved.is_some() {
            debug!(
                aggregate = %plan.name,
                sfunc = %plan.sfunc,
                "SFUNC resolved from UDF registry for aggregate"
            );
        } else {
            debug!(
                aggregate = %plan.name,
                sfunc = %plan.sfunc,
                "SFUNC not found in UDF registry; aggregate metadata stored but cannot execute yet"
            );
        }

        if let Some(ref ff_name) = plan.finalfunc {
            let finalfunc_args = vec![("state".into(), plan.stype.clone())];
            let ff_resolved = self.udf_registry.get(&plan.keyspace, ff_name, &finalfunc_args);
            if ff_resolved.is_some() {
                debug!(
                    aggregate = %plan.name,
                    finalfunc = %ff_name,
                    "FINALFUNC resolved from UDF registry for aggregate"
                );
            } else {
                debug!(
                    aggregate = %plan.name,
                    finalfunc = %ff_name,
                    "FINALFUNC not found in UDF registry; aggregate may not finalize correctly"
                );
            }
        }

        if let Err(e) = self.uda_registry.register(uda_metadata) {
            debug!(error = %e, "Failed to register UDA in runtime registry");
        }

        info!(keyspace = %plan.keyspace, aggregate = %plan.name, "Created aggregate");
        Ok(QueryResult::SchemaChange {
            change_type: "CREATED".into(),
            target: "FUNCTION".into(),
            keyspace: plan.keyspace.clone(),
            name: Some(plan.name.clone()),
        })
    }

    fn execute_drop_aggregate(
        &self,
        plan: &DropAggregatePlan,
    ) -> Result<QueryResult, ExecutorError> {
        let signature = format!("{}({})", plan.name, plan.arg_types.join(", "));

        let catalog = self.catalog.read();
        let snapshot = catalog.snapshot();
        let ks = snapshot
            .keyspace(&plan.keyspace)
            .ok_or_else(|| ExecutorError::KeyspaceNotFound(plan.keyspace.clone()))?
            .clone()
            .without_aggregate(&signature);
        drop(catalog);

        let mut catalog = self.catalog.write();
        *catalog = catalog.with_keyspace(ks);

        info!(keyspace = %plan.keyspace, aggregate = %plan.name, "Dropped aggregate");
        Ok(QueryResult::SchemaChange {
            change_type: "DROPPED".into(),
            target: "FUNCTION".into(),
            keyspace: plan.keyspace.clone(),
            name: Some(plan.name.clone()),
        })
    }

    fn execute_create_trigger(
        &self,
        plan: &CreateTriggerPlan,
    ) -> Result<QueryResult, ExecutorError> {
        let trigger = TriggerDefinition::new(&plan.name, &plan.trigger_class);

        let catalog = self.catalog.read();
        let snapshot = catalog.snapshot();
        let ks_meta = snapshot
            .keyspace(&plan.keyspace)
            .ok_or_else(|| ExecutorError::KeyspaceNotFound(plan.keyspace.clone()))?;
        let table = ks_meta
            .table(&plan.table)
            .ok_or_else(|| ExecutorError::TableNotFound(plan.keyspace.clone(), plan.table.clone()))?;

        let updated_table = table.clone().with_trigger(trigger);
        let ks = ks_meta.clone().with_table(updated_table);
        drop(catalog);

        let mut catalog = self.catalog.write();
        *catalog = catalog.with_keyspace(ks);

        info!(keyspace = %plan.keyspace, table = %plan.table, trigger = %plan.name, "Created trigger");
        Ok(QueryResult::SchemaChange {
            change_type: "CREATED".into(),
            target: "TABLE".into(),
            keyspace: plan.keyspace.clone(),
            name: Some(plan.table.clone()),
        })
    }

    fn execute_drop_trigger(
        &self,
        plan: &DropTriggerPlan,
    ) -> Result<QueryResult, ExecutorError> {
        let catalog = self.catalog.read();
        let snapshot = catalog.snapshot();
        let ks_meta = snapshot
            .keyspace(&plan.keyspace)
            .ok_or_else(|| ExecutorError::KeyspaceNotFound(plan.keyspace.clone()))?;
        let table = ks_meta
            .table(&plan.table)
            .ok_or_else(|| ExecutorError::TableNotFound(plan.keyspace.clone(), plan.table.clone()))?;

        let updated_table = table.clone().without_trigger(&plan.name);
        let ks = ks_meta.clone().with_table(updated_table);
        drop(catalog);

        let mut catalog = self.catalog.write();
        *catalog = catalog.with_keyspace(ks);

        info!(keyspace = %plan.keyspace, table = %plan.table, trigger = %plan.name, "Dropped trigger");
        Ok(QueryResult::SchemaChange {
            change_type: "DROPPED".into(),
            target: "TABLE".into(),
            keyspace: plan.keyspace.clone(),
            name: Some(plan.table.clone()),
        })
    }

    // ─── DML ───────────────────────────────────────────────────────────

    fn execute_insert(&self, plan: &InsertPlan) -> Result<QueryResult, ExecutorError> {
        let now = plan.using_timestamp.unwrap_or_else(current_timestamp_micros);
        let ttl = plan.using_ttl.unwrap_or(0);
        let now_secs = (current_timestamp_micros() / 1_000_000) as i32;
        let local_deletion_time = if ttl > 0 {
            Some(now_secs + ttl)
        } else {
            None
        };

        // Check triggers before applying the mutation.
        if self.trigger_registry.has_triggers(&plan.keyspace, &plan.table) {
            let event = MutationEvent {
                keyspace: plan.keyspace.clone(),
                table: plan.table.clone(),
                partition_key: vec![], // Would be filled from actual mutation data
                mutation_type: MutationType::Insert,
            };
            debug!(?event, "Trigger check: triggers registered for table on INSERT");
            // TODO: Execute triggers via loaded implementations
        }

        let catalog = self.catalog.read();
        let snapshot = catalog.snapshot();
        let table_meta = snapshot.table(&plan.keyspace, &plan.table).ok_or_else(|| {
            ExecutorError::TableNotFound(plan.keyspace.clone(), plan.table.clone())
        })?;

        let pk_cols = table_meta.partition_key_columns();
        let ck_cols = table_meta.clustering_columns();
        let pk_names: Vec<&str> = pk_cols.iter().map(|c| c.name.as_str()).collect();
        let ck_names: Vec<&str> = ck_cols.iter().map(|c| c.name.as_str()).collect();

        // Handle INSERT JSON: parse JSON term into columns/values
        let (effective_columns, effective_values) = if let Some(ref json_term) = plan.json {
            parse_json_insert(json_term)?
        } else {
            (plan.columns.clone(), plan.values.clone())
        };

        let mut pk_bytes = Vec::new();
        let mut ck_bytes = Vec::new();
        let mut cells = Vec::new();
        let mut static_cells_vec = Vec::new();

        for (i, col_name) in effective_columns.iter().enumerate() {
            let val_bytes = if i < effective_values.len() {
                // Use typed binding when the column type is known from schema.
                if let Some(col_meta) = table_meta.column(col_name) {
                    typed_term_to_bytes(&effective_values[i], &col_meta.column_type)
                } else {
                    term_to_bytes(&effective_values[i])
                }
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
                // WU-15: Route static columns to static_cells.
                let is_static = table_meta
                    .column(col_name)
                    .map(|c| c.kind == ColumnKind::Static)
                    .unwrap_or(false);
                let cell = Cell {
                    column: col_name.clone(),
                    value: val_bytes,
                    timestamp: now,
                    ttl,
                    local_deletion_time,
                    is_tombstone: false,
                };
                if is_static {
                    static_cells_vec.push(cell);
                } else {
                    cells.push(cell);
                }
            }
        }

        drop(catalog);

        let row = Row {
            clustering_key: ck_bytes,
            cells,
            is_tombstone: false,
            local_deletion_time: None,
        };

        let static_cell_mutations: Vec<CellMutation> = static_cells_vec
            .into_iter()
            .map(|c| CellMutation {
                column: c.column,
                value: c.value,
                timestamp: c.timestamp,
                ttl: c.ttl,
                local_deletion_time: c.local_deletion_time,
                is_tombstone: c.is_tombstone,
            })
            .collect();

        let mutation = Mutation {
            keyspace: plan.keyspace.clone(),
            table: plan.table.clone(),
            partition_key: pk_bytes.clone(),
            rows: vec![row_to_mutation_row(row)],
            timestamp: now,
            cdc_enabled: false,
            static_cells: static_cell_mutations,
            partition_tombstone: None,
            range_tombstones: Vec::new(),
        };

        self.engine
            .apply_mutation(&mutation)
            .map_err(|e| ExecutorError::StorageError(e.to_string()))?;

        Ok(QueryResult::Void)
    }

    fn execute_update(&self, plan: &UpdatePlan) -> Result<QueryResult, ExecutorError> {
        let now = plan.using_timestamp.unwrap_or_else(current_timestamp_micros);
        let ttl = plan.using_ttl.unwrap_or(0);
        let now_secs = (current_timestamp_micros() / 1_000_000) as i32;
        let local_deletion_time = if ttl > 0 {
            Some(now_secs + ttl)
        } else {
            None
        };

        // Check triggers before applying the mutation.
        if self.trigger_registry.has_triggers(&plan.keyspace, &plan.table) {
            let event = MutationEvent {
                keyspace: plan.keyspace.clone(),
                table: plan.table.clone(),
                partition_key: vec![], // Would be filled from actual mutation data
                mutation_type: MutationType::Update,
            };
            debug!(?event, "Trigger check: triggers registered for table on UPDATE");
            // TODO: Execute triggers via loaded implementations
        }

        let catalog = self.catalog.read();
        let snapshot = catalog.snapshot();
        let table_meta = snapshot.table(&plan.keyspace, &plan.table).ok_or_else(|| {
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

        // WU-16: Detect collection operations from assignment patterns.
        // The AST Assignment has column + value. Collection ops are expressed as:
        //   col = col + {elements}  -> Append
        //   col = col - {elements}  -> Remove
        //   col[key] = value        -> MapPut
        // Currently the AST does not distinguish these syntactically from regular
        // assignments, so we set collection_op on the CellMutation when we detect
        // a CollectionLiteral or MapLiteral value (future parser improvements will
        // enable full detection). For now, we pass the value through and mark
        // the cells with the appropriate TTL and timestamp.
        let cells: Vec<Cell> = plan
            .assignments
            .iter()
            .map(|a| Cell {
                column: a.column.clone(),
                value: term_to_bytes(&a.value),
                timestamp: now,
                ttl,
                local_deletion_time,
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

        let mutation = Mutation {
            keyspace: plan.keyspace.clone(),
            table: plan.table.clone(),
            partition_key: pk_bytes.clone(),
            rows: vec![row_to_mutation_row(row)],
            timestamp: now,
            cdc_enabled: false,
            static_cells: Vec::new(),
            partition_tombstone: None,
            range_tombstones: Vec::new(),
        };

        self.engine
            .apply_mutation(&mutation)
            .map_err(|e| ExecutorError::StorageError(e.to_string()))?;

        Ok(QueryResult::Void)
    }

    fn execute_delete(&self, plan: &DeletePlan) -> Result<QueryResult, ExecutorError> {
        let now = plan.using_timestamp.unwrap_or_else(current_timestamp_micros);
        let now_secs = (current_timestamp_micros() / 1_000_000) as i32;

        // Check triggers before applying the mutation.
        if self.trigger_registry.has_triggers(&plan.keyspace, &plan.table) {
            let event = MutationEvent {
                keyspace: plan.keyspace.clone(),
                table: plan.table.clone(),
                partition_key: vec![], // Would be filled from actual mutation data
                mutation_type: MutationType::Delete,
            };
            debug!(?event, "Trigger check: triggers registered for table on DELETE");
            // TODO: Execute triggers via loaded implementations
        }

        let catalog = self.catalog.read();
        let snapshot = catalog.snapshot();
        let table_meta = snapshot.table(&plan.keyspace, &plan.table).ok_or_else(|| {
            ExecutorError::TableNotFound(plan.keyspace.clone(), plan.table.clone())
        })?;

        let pk_cols = table_meta.partition_key_columns();
        let ck_cols = table_meta.clustering_columns();
        let pk_names: Vec<&str> = pk_cols.iter().map(|c| c.name.as_str()).collect();
        let ck_names: Vec<&str> = ck_cols.iter().map(|c| c.name.as_str()).collect();

        let mut pk_bytes = Vec::new();
        let mut ck_eq_bytes = Vec::new();
        let mut has_ck_eq = false;
        let mut ck_range_start: Option<Vec<u8>> = None;
        let mut ck_range_end: Option<Vec<u8>> = None;
        let mut has_ck_range = false;

        for rel in &plan.where_clause {
            let val = term_to_bytes(&rel.value);
            if pk_names.contains(&rel.column.as_str()) {
                if let Some(v) = val {
                    pk_bytes.extend_from_slice(&v);
                }
            } else if ck_names.contains(&rel.column.as_str()) {
                match rel.op {
                    RelationOp::Eq => {
                        if let Some(v) = val {
                            ck_eq_bytes.extend_from_slice(&v);
                            has_ck_eq = true;
                        }
                    }
                    RelationOp::Gt | RelationOp::Gte => {
                        if let Some(v) = val {
                            ck_range_start = Some(v);
                            has_ck_range = true;
                        }
                    }
                    RelationOp::Lt | RelationOp::Lte => {
                        if let Some(v) = val {
                            ck_range_end = Some(v);
                            has_ck_range = true;
                        }
                    }
                    _ => {
                        if let Some(v) = val {
                            ck_eq_bytes.extend_from_slice(&v);
                            has_ck_eq = true;
                        }
                    }
                }
            }
        }

        // WU-17: Determine tombstone type based on WHERE clause conditions.
        let has_ck_columns = !ck_names.is_empty();
        let is_partition_delete = has_ck_columns && !has_ck_eq && !has_ck_range;
        let is_range_delete = has_ck_range;

        drop(catalog);

        let mut partition_tombstone = None;
        let mut range_tombstones = Vec::new();

        if is_partition_delete && plan.columns.is_empty() {
            // Partition tombstone: DELETE FROM t WHERE pk = X (no clustering columns specified)
            partition_tombstone = Some(TombstoneMarker {
                timestamp: now,
                local_deletion_time: now_secs,
            });

            let mutation = Mutation {
                keyspace: plan.keyspace.clone(),
                table: plan.table.clone(),
                partition_key: pk_bytes.clone(),
                rows: Vec::new(),
                timestamp: now,
                cdc_enabled: false,
                static_cells: Vec::new(),
                partition_tombstone,
                range_tombstones,
            };

            self.engine
                .apply_mutation(&mutation)
                .map_err(|e| ExecutorError::StorageError(e.to_string()))?;

            return Ok(QueryResult::Void);
        }

        if is_range_delete && plan.columns.is_empty() {
            // Range tombstone: DELETE FROM t WHERE pk = X AND ck > Y AND ck < Z
            range_tombstones.push(CommitlogRangeTombstone {
                start: ck_range_start.unwrap_or_default(),
                end: ck_range_end.unwrap_or_default(),
                timestamp: now,
                local_deletion_time: now_secs,
            });

            let mutation = Mutation {
                keyspace: plan.keyspace.clone(),
                table: plan.table.clone(),
                partition_key: pk_bytes.clone(),
                rows: Vec::new(),
                timestamp: now,
                cdc_enabled: false,
                static_cells: Vec::new(),
                partition_tombstone: None,
                range_tombstones,
            };

            self.engine
                .apply_mutation(&mutation)
                .map_err(|e| ExecutorError::StorageError(e.to_string()))?;

            return Ok(QueryResult::Void);
        }

        // Row tombstone or column tombstone (current behavior).
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
            clustering_key: ck_eq_bytes,
            cells,
            is_tombstone: is_row_delete,
            local_deletion_time: if is_row_delete { Some(now_secs) } else { None },
        };

        let mutation = Mutation {
            keyspace: plan.keyspace.clone(),
            table: plan.table.clone(),
            partition_key: pk_bytes.clone(),
            rows: vec![row_to_mutation_row(row)],
            timestamp: now,
            cdc_enabled: false,
            static_cells: Vec::new(),
            partition_tombstone: None,
            range_tombstones: Vec::new(),
        };

        self.engine
            .apply_mutation(&mutation)
            .map_err(|e| ExecutorError::StorageError(e.to_string()))?;

        Ok(QueryResult::Void)
    }

    fn execute_select(
        &self,
        plan: &SelectPlan,
        user: Option<&str>,
    ) -> Result<QueryResult, ExecutorError> {
        let catalog = self.catalog.read();
        let snapshot = catalog.snapshot();
        let table_meta = snapshot.table(&plan.keyspace, &plan.table).ok_or_else(|| {
            ExecutorError::TableNotFound(plan.keyspace.clone(), plan.table.clone())
        })?;

        let pk_cols = table_meta.partition_key_columns();
        let pk_names: Vec<&str> = pk_cols.iter().map(|c| c.name.as_str()).collect();

        let mut pk_bytes = Vec::new();
        let mut index_searches = Vec::new();
        for rel in &plan.where_clause {
            if pk_names.contains(&rel.column.as_str()) {
                if let Some(v) = term_to_bytes(&rel.value) {
                    pk_bytes.extend_from_slice(&v);
                }
            } else if let Some(idx) = table_meta
                .indexes
                .iter()
                .find(|i| i.target_column() == Some(&rel.column))
            {
                index_searches.push((rel.clone(), idx.clone()));
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

        // Determine if DDM must be applied (User lacks UNMASK permission)
        // By default, if user is not available or authorizer triggers, we mask.
        // For phase 7, if Authorizer traits properly resolve "has_permission", we use them.
        let mut apply_masking = false;

        if let Some(username) = user {
            use cassandra_security::authz::Permission;
            let resource = cassandra_security::authz::Resource::Table {
                keyspace: plan.keyspace.clone(),
                table: plan.table.clone(),
            };
            if self
                .authorizer
                .authorize(username, &resource, Permission::Unmask)
                .is_err()
            {
                apply_masking = true;
            }
        } else {
            // Unauthenticated requests should definitely be masked if there are policies
            apply_masking = true;
        }

        // Setup ephemeral masking registry for this query
        let mut masking_registry = cassandra_security::masking::MaskingRegistry::new();
        if apply_masking {
            for col in &table_meta.columns {
                if let Some((func, args)) = &col.masked_with {
                    masking_registry.add_policy(cassandra_security::masking::ColumnMaskingConfig {
                        keyspace: plan.keyspace.clone(),
                        table: plan.table.clone(),
                        column: col.name.clone(),
                        function_name: func.clone(),
                        function_args: args.clone(),
                    });
                }
            }
        }

        drop(catalog);

        if pk_bytes.is_empty() {
            if !index_searches.is_empty() {
                let (rel, idx) = &index_searches[0];
                let is_vector = idx
                    .options
                    .get("class_name")
                    .map(|s| s.contains("StorageAttachedIndex"))
                    .unwrap_or(false)
                    && idx.options.contains_key("vector_dimensions");

                let term_bytes = term_to_bytes(&rel.value).unwrap_or_default();

                let search_results = if is_vector {
                    let k = plan.limit.as_ref().and_then(term_to_i64).unwrap_or(10) as usize;
                    self.engine
                        .search_vector_index(&plan.keyspace, &plan.table, &idx.name, &term_bytes, k)
                        .map_err(|e: Box<dyn std::error::Error>| {
                            ExecutorError::StorageError(e.to_string())
                        })?
                        .into_iter()
                        .map(|(pd, _score)| pd)
                        .collect::<Vec<_>>()
                } else {
                    self.engine
                        .search_index(&plan.keyspace, &plan.table, &idx.name, &term_bytes)
                        .map_err(|e: Box<dyn std::error::Error>| {
                            ExecutorError::StorageError(e.to_string())
                        })?
                };

                let mut result_rows = Vec::new();
                let now_secs = (current_timestamp_micros() / 1_000_000) as i32;

                for pd in search_results {
                    let live: Vec<&Row> = pd.live_rows(now_secs);
                    for row in live {
                        let mut result_row = Vec::new();
                        for rc in &result_columns {
                            let mut value = row
                                .cells
                                .iter()
                                .find(|c| c.column == rc.name && c.is_live_at(now_secs))
                                .and_then(|c| c.value.clone());

                            if apply_masking {
                                if let Some(v) = &value {
                                    if let Some(masked) = masking_registry.apply_mask(
                                        &plan.keyspace,
                                        &plan.table,
                                        &rc.name,
                                        v,
                                    ) {
                                        value = Some(masked);
                                    }
                                }
                            }
                            result_row.push(value);
                        }
                        result_rows.push(result_row);
                    }
                }

                if let Some(ref limit_term) = plan.limit {
                    if let Some(limit) = term_to_i64(limit_term) {
                        result_rows.truncate(limit as usize);
                    }
                }

                return wrap_select_json(plan.json, result_columns, result_rows);
            }

            return wrap_select_json(plan.json, result_columns, Vec::new());
        }

        let partition = self
            .engine
            .read_partition(&plan.keyspace, &plan.table, &pk_bytes);

        let mut result_rows = Vec::new();

        if let Some(pd) = partition {
            let now_secs = (current_timestamp_micros() / 1_000_000) as i32;
            let live: Vec<&Row> = pd.live_rows(now_secs);

            for row in live {
                let mut result_row = Vec::new();
                for rc in &result_columns {
                    let mut value = row
                        .cells
                        .iter()
                        .find(|c| c.column == rc.name && c.is_live_at(now_secs))
                        .and_then(|c| c.value.clone());

                    if apply_masking {
                        if let Some(v) = &value {
                            if let Some(masked) = masking_registry.apply_mask(
                                &plan.keyspace,
                                &plan.table,
                                &rc.name,
                                v,
                            ) {
                                value = Some(masked);
                            }
                        }
                    }
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

        wrap_select_json(plan.json, result_columns, result_rows)
    }

    fn execute_batch(
        &self,
        plan: &BatchPlan,
        user: Option<&str>,
    ) -> Result<QueryResult, ExecutorError> {
        // Validate counter/non-counter mixing:
        // Counter batches must only contain counter mutations and vice versa.
        let is_counter_batch = matches!(
            plan.batch_type,
            cassandra_cql::ast::BatchType::Counter
        );
        for sub in &plan.plans {
            match sub {
                QueryPlan::Insert(_) | QueryPlan::Update(_) | QueryPlan::Delete(_) => {
                    // Standard DML is not allowed in counter batches
                    if is_counter_batch {
                        return Err(ExecutorError::InvalidQuery(
                            "Cannot mix counter and non-counter mutations in a batch".to_string(),
                        ));
                    }
                }
                _ => {
                    return Err(ExecutorError::InvalidQuery(
                        "Batch statements may only contain INSERT, UPDATE, or DELETE".to_string(),
                    ));
                }
            }
        }

        // Build Vec<Mutation> from sub-plans (collect mutations instead of applying one-by-one).
        // TODO(WU-06): When StorageProxy is wired into QueryExecutor, delegate to
        // BatchCoordinator instead of applying mutations directly through the engine.
        // That will enable proper batchlog protocol, consistency level enforcement,
        // and replica fan-out for batch operations.
        let now = current_timestamp_micros();
        let mut collected_mutations: Vec<Mutation> = Vec::with_capacity(plan.plans.len());

        for sub in &plan.plans {
            let mutation = match sub {
                QueryPlan::Insert(ins) => self.build_insert_mutation(ins, now)?,
                QueryPlan::Update(upd) => self.build_update_mutation(upd, now)?,
                QueryPlan::Delete(del) => self.build_delete_mutation(del, now)?,
                _ => unreachable!("Validated above: only INSERT/UPDATE/DELETE in batch"),
            };
            collected_mutations.push(mutation);
        }

        // Apply all collected mutations through the engine
        for mutation in &collected_mutations {
            self.engine
                .apply_mutation(mutation)
                .map_err(|e| ExecutorError::StorageError(e.to_string()))?;
        }

        Ok(QueryResult::Void)
    }

    /// Build a Mutation from an InsertPlan without applying it.
    fn build_insert_mutation(
        &self,
        plan: &InsertPlan,
        now: i64,
    ) -> Result<Mutation, ExecutorError> {
        let catalog = self.catalog.read();
        let snapshot = catalog.snapshot();
        let table_meta = snapshot.table(&plan.keyspace, &plan.table).ok_or_else(|| {
            ExecutorError::TableNotFound(plan.keyspace.clone(), plan.table.clone())
        })?;

        let pk_cols = table_meta.partition_key_columns();
        let ck_cols = table_meta.clustering_columns();
        let pk_names: Vec<&str> = pk_cols.iter().map(|c| c.name.as_str()).collect();
        let ck_names: Vec<&str> = ck_cols.iter().map(|c| c.name.as_str()).collect();

        let (effective_columns, effective_values) = if let Some(ref json_term) = plan.json {
            parse_json_insert(json_term)?
        } else {
            (plan.columns.clone(), plan.values.clone())
        };

        let mut pk_bytes = Vec::new();
        let mut ck_bytes = Vec::new();
        let mut cells = Vec::new();

        for (i, col_name) in effective_columns.iter().enumerate() {
            let val_bytes = if i < effective_values.len() {
                if let Some(col_meta) = table_meta.column(col_name) {
                    typed_term_to_bytes(&effective_values[i], &col_meta.column_type)
                } else {
                    term_to_bytes(&effective_values[i])
                }
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

        Ok(Mutation {
            keyspace: plan.keyspace.clone(),
            table: plan.table.clone(),
            partition_key: pk_bytes,
            rows: vec![row_to_mutation_row(row)],
            timestamp: now,
            cdc_enabled: false,
            static_cells: Vec::new(),
            partition_tombstone: None,
            range_tombstones: Vec::new(),
        })
    }

    /// Build a Mutation from an UpdatePlan without applying it.
    fn build_update_mutation(
        &self,
        plan: &UpdatePlan,
        now: i64,
    ) -> Result<Mutation, ExecutorError> {
        let catalog = self.catalog.read();
        let snapshot = catalog.snapshot();
        let table_meta = snapshot.table(&plan.keyspace, &plan.table).ok_or_else(|| {
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

        Ok(Mutation {
            keyspace: plan.keyspace.clone(),
            table: plan.table.clone(),
            partition_key: pk_bytes,
            rows: vec![row_to_mutation_row(row)],
            timestamp: now,
            cdc_enabled: false,
            static_cells: Vec::new(),
            partition_tombstone: None,
            range_tombstones: Vec::new(),
        })
    }

    /// Build a Mutation from a DeletePlan without applying it.
    fn build_delete_mutation(
        &self,
        plan: &DeletePlan,
        now: i64,
    ) -> Result<Mutation, ExecutorError> {
        let now_secs = (now / 1_000_000) as i32;

        let catalog = self.catalog.read();
        let snapshot = catalog.snapshot();
        let table_meta = snapshot.table(&plan.keyspace, &plan.table).ok_or_else(|| {
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
            local_deletion_time: if is_row_delete { Some(now_secs) } else { None },
        };

        Ok(Mutation {
            keyspace: plan.keyspace.clone(),
            table: plan.table.clone(),
            partition_key: pk_bytes,
            rows: vec![row_to_mutation_row(row)],
            timestamp: now,
            cdc_enabled: false,
            static_cells: Vec::new(),
            partition_tombstone: None,
            range_tombstones: Vec::new(),
        })
    }

    // ─── DCL / Role Management ─────────────────────────────────────────────

    fn execute_create_role(&self, plan: &CreateRolePlan) -> Result<QueryResult, ExecutorError> {
        if !plan.if_not_exists && self.role_manager.role_exists(&plan.name) {
            return Err(ExecutorError::InvalidQuery(format!(
                "Role '{}' already exists",
                plan.name
            )));
        }

        let hashed_password = plan
            .password
            .as_deref()
            .map(|p| cassandra_security::auth::hash_password(p).unwrap());

        self.role_manager.create_role(Role {
            name: plan.name.clone(),
            is_superuser: plan.is_superuser,
            can_login: plan.can_login,
            hashed_password,
            member_of: vec![],
            network_permissions: None,
        });

        info!(role = %plan.name, "Created role");
        Ok(QueryResult::Void)
    }

    fn execute_alter_role(&self, plan: &AlterRolePlan) -> Result<QueryResult, ExecutorError> {
        let opts = RoleOptions {
            is_superuser: plan.superuser,
            can_login: plan.login,
            password: plan.password.clone(),
            network_permissions: None,
        };

        self.role_manager
            .alter_role(&plan.name, opts)
            .map_err(|e| ExecutorError::InvalidQuery(e.to_string()))?;

        info!(role = %plan.name, "Altered role");
        Ok(QueryResult::Void)
    }

    fn execute_drop_role(&self, plan: &DropRolePlan) -> Result<QueryResult, ExecutorError> {
        if !plan.if_exists && !self.role_manager.role_exists(&plan.name) {
            return Err(ExecutorError::InvalidQuery(format!(
                "Role '{}' doesn't exist",
                plan.name
            )));
        }

        if self.role_manager.role_exists(&plan.name) {
            self.role_manager
                .drop_role(&plan.name)
                .map_err(|e| ExecutorError::InvalidQuery(e.to_string()))?;
            info!(role = %plan.name, "Dropped role");
        }
        Ok(QueryResult::Void)
    }

    fn execute_grant(&self, plan: &GrantPlan) -> Result<QueryResult, ExecutorError> {
        let sec_resource = ast_resource_to_security(&plan.resource)?;
        for p_str in &plan.permissions {
            let p = parse_permission(p_str)?;
            self.authorizer
                .grant("cassandra", &plan.role, &sec_resource, p)
                .map_err(|e| ExecutorError::InvalidQuery(e.to_string()))?;
        }
        info!(role = %plan.role, "Granted permissions");
        Ok(QueryResult::Void)
    }

    fn execute_revoke(&self, plan: &RevokePlan) -> Result<QueryResult, ExecutorError> {
        let sec_resource = ast_resource_to_security(&plan.resource)?;
        for p_str in &plan.permissions {
            let p = parse_permission(p_str)?;
            self.authorizer
                .revoke("cassandra", &plan.role, &sec_resource, p)
                .map_err(|e| ExecutorError::InvalidQuery(e.to_string()))?;
        }
        info!(role = %plan.role, "Revoked permissions");
        Ok(QueryResult::Void)
    }

    fn execute_list_roles(&self, _plan: &ListRolesPlan) -> Result<QueryResult, ExecutorError> {
        let _roles = self.role_manager.list_roles();
        // Return void for now as we don't return virtual tables fully here yet.
        Ok(QueryResult::Void)
    }

    // ─── DESCRIBE ─────────────────────────────────────────────────────────

    fn execute_describe(&self, plan: &DescribePlan) -> Result<QueryResult, ExecutorError> {
        let catalog = self.catalog.read();
        let snapshot = catalog.snapshot();

        let ddl_text = match &plan.target {
            DescribeTarget::Cluster => {
                format!("Cluster: cassandra\nPartitioner: Murmur3Partitioner")
            }
            DescribeTarget::FullSchema => {
                let mut output = String::new();
                for (ks_name, ks_meta) in &snapshot.keyspaces {
                    output.push_str(&format!(
                        "CREATE KEYSPACE {} WITH replication = {{}};\n\n",
                        ks_name
                    ));
                    for (table_name, _table_meta) in &ks_meta.tables {
                        output.push_str(&format!(
                            "CREATE TABLE {}.{} (...);\n\n",
                            ks_name, table_name
                        ));
                    }
                }
                output
            }
            DescribeTarget::Keyspace(name) => {
                if snapshot.keyspace(name).is_some() {
                    format!("CREATE KEYSPACE {} WITH replication = {{}};", name)
                } else {
                    return Err(ExecutorError::InvalidQuery(format!(
                        "Keyspace '{}' not found",
                        name
                    )));
                }
            }
            DescribeTarget::Table(ks_opt, table_name) => {
                let ks = ks_opt.as_deref().unwrap_or("system");
                if snapshot.table(ks, table_name).is_some() {
                    format!("CREATE TABLE {}.{} (...);", ks, table_name)
                } else {
                    return Err(ExecutorError::TableNotFound(
                        ks.to_string(),
                        table_name.clone(),
                    ));
                }
            }
            DescribeTarget::Type(ks_opt, name) => {
                let ks = ks_opt.as_deref().unwrap_or("system");
                format!("CREATE TYPE {}.{} (...);", ks, name)
            }
            DescribeTarget::Function(ks_opt, name) => {
                let ks = ks_opt.as_deref().unwrap_or("system");
                format!("CREATE FUNCTION {}.{} (...);", ks, name)
            }
            DescribeTarget::Aggregate(ks_opt, name) => {
                let ks = ks_opt.as_deref().unwrap_or("system");
                format!("CREATE AGGREGATE {}.{} (...);", ks, name)
            }
            DescribeTarget::Generic(name) => {
                format!("DESCRIBE {};", name)
            }
        };

        let columns = vec![ResultColumn {
            keyspace: String::new(),
            table: String::new(),
            name: "describe_text".to_string(),
            cql_type: CqlType::Varchar,
        }];

        let rows = vec![vec![Some(ddl_text.into_bytes())]];

        Ok(QueryResult::Rows { columns, rows })
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

fn row_to_mutation_row(row: Row) -> MutationRow {
    MutationRow {
        clustering_key: row.clustering_key,
        cells: row
            .cells
            .into_iter()
            .map(|c| CellMutation {
                column: c.column,
                value: c.value,
                timestamp: c.timestamp,
                ttl: c.ttl,
                local_deletion_time: c.local_deletion_time,
                is_tombstone: c.is_tombstone,
            })
            .collect(),
        is_tombstone: row.is_tombstone,
        local_deletion_time: row.local_deletion_time,
    }
}

fn parse_permission(p: &str) -> Result<Permission, ExecutorError> {
    match p.to_uppercase().as_str() {
        "CREATE" => Ok(Permission::Create),
        "ALTER" => Ok(Permission::Alter),
        "DROP" => Ok(Permission::Drop),
        "SELECT" => Ok(Permission::Select),
        "MODIFY" => Ok(Permission::Modify),
        "AUTHORIZE" => Ok(Permission::Authorize),
        "DESCRIBE" => Ok(Permission::Describe),
        "EXECUTE" => Ok(Permission::Execute),
        "UNMASK" => Ok(Permission::Unmask),
        "SELECT_MASKED" => Ok(Permission::SelectMasked),
        _ => Err(ExecutorError::InvalidQuery(format!(
            "Unknown permission '{}'",
            p
        ))),
    }
}

fn ast_resource_to_security(
    r: &cassandra_cql::ast::Resource,
) -> Result<SecurityResource, ExecutorError> {
    use cassandra_cql::ast::Resource as AstRes;
    match r {
        AstRes::AllKeyspaces => Ok(SecurityResource::Root),
        AstRes::Keyspace(ks) => Ok(SecurityResource::Keyspace(ks.clone())),
        AstRes::Table { keyspace, table } => Ok(SecurityResource::Table {
            keyspace: keyspace.clone().unwrap_or_else(|| "system".into()), // Needs active keyspace context ideally
            table: table.clone(),
        }),
        AstRes::AllRoles => Ok(SecurityResource::Root),
        AstRes::Role(r) => Ok(SecurityResource::Role(r.clone())),
        AstRes::AllFunctions => Ok(SecurityResource::Root),
        AstRes::FunctionInKeyspace(ks) => Ok(SecurityResource::Keyspace(ks.clone())),
        AstRes::Function { keyspace, name, .. } => Ok(SecurityResource::Function {
            keyspace: keyspace.clone().unwrap_or_else(|| "system".into()),
            name: name.clone(),
        }),
        AstRes::AllMBeans => Ok(SecurityResource::Root),
        AstRes::MBean(m) => Ok(SecurityResource::Jmx(m.clone())),
        AstRes::MBeanPattern(p) => Ok(SecurityResource::Jmx(p.clone())),
    }
}

/// Wrap SELECT results as JSON if `json` is true.
///
/// When SELECT JSON is used, each row is collapsed into a single `[json]` column
/// containing a JSON object string where keys are the original column names and
/// values are the UTF-8-decoded cell bytes (or null).
fn wrap_select_json(
    json: bool,
    columns: Vec<ResultColumn>,
    rows: Vec<Vec<Option<Vec<u8>>>>,
) -> Result<QueryResult, ExecutorError> {
    if !json {
        return Ok(QueryResult::Rows { columns, rows });
    }

    let json_column = ResultColumn {
        keyspace: columns.first().map(|c| c.keyspace.clone()).unwrap_or_default(),
        table: columns.first().map(|c| c.table.clone()).unwrap_or_default(),
        name: "[json]".to_string(),
        cql_type: CqlType::Varchar,
    };

    let json_rows: Vec<Vec<Option<Vec<u8>>>> = rows
        .iter()
        .map(|row| {
            let mut obj = String::from("{");
            for (i, (col, val)) in columns.iter().zip(row.iter()).enumerate() {
                if i > 0 {
                    obj.push_str(", ");
                }
                obj.push('"');
                obj.push_str(&col.name);
                obj.push_str("\": ");
                match val {
                    Some(bytes) => {
                        if let Ok(s) = std::str::from_utf8(bytes) {
                            obj.push('"');
                            obj.push_str(s);
                            obj.push('"');
                        } else {
                            obj.push_str("\"0x");
                            for b in bytes {
                                obj.push_str(&format!("{:02x}", b));
                            }
                            obj.push('"');
                        }
                    }
                    None => obj.push_str("null"),
                }
            }
            obj.push('}');
            vec![Some(obj.into_bytes())]
        })
        .collect();

    Ok(QueryResult::Rows {
        columns: vec![json_column],
        rows: json_rows,
    })
}

/// Parse a JSON string term into column names and values for INSERT JSON.
fn parse_json_insert(
    json_term: &Term,
) -> Result<(Vec<String>, Vec<Term>), ExecutorError> {
    let json_str = match json_term {
        Term::Literal(Literal::String(s)) => s.clone(),
        _ => {
            return Err(ExecutorError::InvalidQuery(
                "INSERT JSON requires a string literal".into(),
            ));
        }
    };

    let trimmed = json_str.trim();
    if !trimmed.starts_with('{') || !trimmed.ends_with('}') {
        return Err(ExecutorError::InvalidQuery(
            "INSERT JSON value must be a JSON object".into(),
        ));
    }

    let inner = &trimmed[1..trimmed.len() - 1];
    let mut columns = Vec::new();
    let mut values = Vec::new();

    let mut chars = inner.chars().peekable();
    loop {
        while chars.peek().map_or(false, |c| c.is_whitespace()) {
            chars.next();
        }
        if chars.peek().is_none() {
            break;
        }

        if chars.next() != Some('"') {
            return Err(ExecutorError::InvalidQuery(
                "Expected quoted key in JSON object".into(),
            ));
        }
        let mut key = String::new();
        loop {
            match chars.next() {
                Some('"') => break,
                Some(c) => key.push(c),
                None => {
                    return Err(ExecutorError::InvalidQuery(
                        "Unterminated key in JSON".into(),
                    ))
                }
            }
        }

        while chars.peek().map_or(false, |c| c.is_whitespace()) {
            chars.next();
        }
        if chars.next() != Some(':') {
            return Err(ExecutorError::InvalidQuery(
                "Expected ':' after key in JSON".into(),
            ));
        }
        while chars.peek().map_or(false, |c| c.is_whitespace()) {
            chars.next();
        }

        let term = match chars.peek() {
            Some('"') => {
                chars.next();
                let mut val = String::new();
                loop {
                    match chars.next() {
                        Some('\\') => {
                            if let Some(c) = chars.next() {
                                val.push(c);
                            }
                        }
                        Some('"') => break,
                        Some(c) => val.push(c),
                        None => {
                            return Err(ExecutorError::InvalidQuery(
                                "Unterminated string in JSON".into(),
                            ))
                        }
                    }
                }
                Term::Literal(Literal::String(val))
            }
            Some('n') => {
                for expected in ['n', 'u', 'l', 'l'] {
                    if chars.next() != Some(expected) {
                        return Err(ExecutorError::InvalidQuery(
                            "Invalid JSON value".into(),
                        ));
                    }
                }
                Term::Literal(Literal::Null)
            }
            Some('t') => {
                for expected in ['t', 'r', 'u', 'e'] {
                    if chars.next() != Some(expected) {
                        return Err(ExecutorError::InvalidQuery(
                            "Invalid JSON value".into(),
                        ));
                    }
                }
                Term::Literal(Literal::Boolean(true))
            }
            Some('f') => {
                for expected in ['f', 'a', 'l', 's', 'e'] {
                    if chars.next() != Some(expected) {
                        return Err(ExecutorError::InvalidQuery(
                            "Invalid JSON value".into(),
                        ));
                    }
                }
                Term::Literal(Literal::Boolean(false))
            }
            Some(c) if c.is_ascii_digit() || *c == '-' => {
                let mut num_str = String::new();
                let mut is_float = false;
                while let Some(&c) = chars.peek() {
                    if c.is_ascii_digit() || c == '-' || c == '+' || c == 'e' || c == 'E' {
                        num_str.push(c);
                        chars.next();
                    } else if c == '.' {
                        is_float = true;
                        num_str.push(c);
                        chars.next();
                    } else {
                        break;
                    }
                }
                if is_float {
                    let f: f64 = num_str.parse().map_err(|_| {
                        ExecutorError::InvalidQuery(format!("Invalid float: {}", num_str))
                    })?;
                    Term::Literal(Literal::Float(f))
                } else {
                    let n: i64 = num_str.parse().map_err(|_| {
                        ExecutorError::InvalidQuery(format!("Invalid integer: {}", num_str))
                    })?;
                    Term::Literal(Literal::Integer(n))
                }
            }
            _ => {
                return Err(ExecutorError::InvalidQuery(
                    "Unexpected character in JSON value".into(),
                ));
            }
        };

        columns.push(key);
        values.push(term);

        while chars.peek().map_or(false, |c| c.is_whitespace()) {
            chars.next();
        }
        if chars.peek() == Some(&',') {
            chars.next();
        }
    }

    Ok((columns, values))
}
