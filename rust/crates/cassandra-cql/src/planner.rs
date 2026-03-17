// Licensed under Apache License, Version 2.0.

//! Query planner: validates AST against schema and produces executable plans.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.cql3.statements.SelectStatement`
//! - `org.apache.cassandra.cql3.statements.ModificationStatement`

use crate::ast::*;
use crate::parser::ParseError;
use crate::restrictions::RestrictionSet;
use cassandra_schema::SchemaSnapshot;
use std::collections::HashMap;

/// A planned, validated query ready for (stub) execution.
#[derive(Debug, Clone)]
pub enum QueryPlan {
    CreateKeyspace(CreateKeyspacePlan),
    AlterKeyspace(AlterKeyspacePlan),
    DropKeyspace(DropKeyspacePlan),
    CreateTable(CreateTablePlan),
    AlterTable(AlterTablePlan),
    DropTable(DropTablePlan),
    Select(SelectPlan),
    Insert(InsertPlan),
    Update(UpdatePlan),
    Delete(DeletePlan),
    Use(UsePlan),
    Truncate(TruncatePlan),
    Batch(BatchPlan),
    CreateRole(CreateRolePlan),
    AlterRole(AlterRolePlan),
    DropRole(DropRolePlan),
    Grant(GrantPlan),
    Revoke(RevokePlan),
    ListRoles(ListRolesPlan),
    CreateIndex(CreateIndexPlan),
    DropIndex(DropIndexPlan),
    CreateMaterializedView(CreateMaterializedViewPlan),
    DropMaterializedView(DropMaterializedViewPlan),
    AlterMaterializedView(AlterMaterializedViewPlan),
    CreateType(CreateTypePlan),
    DropType(DropTypePlan),
    CreateFunction(CreateFunctionPlan),
    DropFunction(DropFunctionPlan),
    CreateAggregate(CreateAggregatePlan),
    DropAggregate(DropAggregatePlan),
    CreateTrigger(CreateTriggerPlan),
    DropTrigger(DropTriggerPlan),
    Describe(DescribePlan),
}

impl QueryPlan {
    pub fn is_schema_altering(&self) -> bool {
        matches!(
            self,
            QueryPlan::CreateKeyspace(_)
                | QueryPlan::AlterKeyspace(_)
                | QueryPlan::DropKeyspace(_)
                | QueryPlan::CreateTable(_)
                | QueryPlan::AlterTable(_)
                | QueryPlan::DropTable(_)
                | QueryPlan::CreateIndex(_)
                | QueryPlan::DropIndex(_)
                | QueryPlan::CreateMaterializedView(_)
                | QueryPlan::DropMaterializedView(_)
                | QueryPlan::AlterMaterializedView(_)
                | QueryPlan::CreateType(_)
                | QueryPlan::DropType(_)
                | QueryPlan::CreateFunction(_)
                | QueryPlan::DropFunction(_)
                | QueryPlan::CreateAggregate(_)
                | QueryPlan::DropAggregate(_)
                | QueryPlan::CreateTrigger(_)
                | QueryPlan::DropTrigger(_)
        )
    }
}

#[derive(Debug, Clone)]
pub struct CreateKeyspacePlan {
    pub name: String,
    pub if_not_exists: bool,
    pub replication: HashMap<String, String>,
    pub durable_writes: bool,
}

#[derive(Debug, Clone)]
pub struct AlterKeyspacePlan {
    pub name: String,
    pub replication: Option<HashMap<String, String>>,
    pub durable_writes: Option<bool>,
}

#[derive(Debug, Clone)]
pub struct DropKeyspacePlan {
    pub name: String,
    pub if_exists: bool,
}

#[derive(Debug, Clone)]
pub struct CreateTablePlan {
    pub keyspace: String,
    pub name: String,
    pub if_not_exists: bool,
    pub columns: Vec<ResolvedColumnDef>,
    pub partition_key: Vec<String>,
    pub clustering_key: Vec<String>,
    pub clustering_order: Vec<(String, ClusteringOrder)>,
    pub options: HashMap<String, String>,
}

#[derive(Debug, Clone)]
pub struct ResolvedColumnDef {
    pub name: String,
    pub cql_type: cassandra_types::CqlType,
    pub is_static: bool,
    pub masked_with: Option<(String, Vec<String>)>,
}

#[derive(Debug, Clone)]
pub struct AlterTablePlan {
    pub keyspace: String,
    pub name: String,
    pub operation: AlterTableOp,
}

#[derive(Debug, Clone)]
pub struct DropTablePlan {
    pub keyspace: String,
    pub name: String,
    pub if_exists: bool,
}

#[derive(Debug, Clone)]
pub struct SelectPlan {
    pub keyspace: String,
    pub table: String,
    pub columns: SelectColumns,
    pub distinct: bool,
    pub json: bool,
    pub where_clause: Vec<Relation>,
    pub order_by: Vec<(String, ClusteringOrder)>,
    pub limit: Option<Term>,
    pub allow_filtering: bool,
    /// Validated restrictions (None if table metadata was not available).
    pub restrictions: Option<RestrictionSet>,
    /// ANN clause detected from ORDER BY ... ANN OF [...] LIMIT k.
    pub ann_clause: Option<AnnClause>,
}

/// Approximate Nearest Neighbor clause for vector search.
#[derive(Debug, Clone)]
pub struct AnnClause {
    /// The vector column name.
    pub column: String,
    /// The query vector literal.
    pub vector_literal: Vec<f32>,
    /// Number of results to return.
    pub top_k: usize,
}

#[derive(Debug, Clone)]
pub struct InsertPlan {
    pub keyspace: String,
    pub table: String,
    pub columns: Vec<String>,
    pub values: Vec<Term>,
    pub if_not_exists: bool,
    /// INSERT JSON term, if present.
    pub json: Option<Term>,
    /// Client-supplied timestamp from USING TIMESTAMP clause.
    pub using_timestamp: Option<i64>,
    /// Client-supplied TTL from USING TTL clause.
    pub using_ttl: Option<i32>,
}

#[derive(Debug, Clone)]
pub struct UpdatePlan {
    pub keyspace: String,
    pub table: String,
    pub assignments: Vec<Assignment>,
    pub where_clause: Vec<Relation>,
    pub if_exists: bool,
    /// Client-supplied timestamp from USING TIMESTAMP clause.
    pub using_timestamp: Option<i64>,
    /// Client-supplied TTL from USING TTL clause.
    pub using_ttl: Option<i32>,
}

#[derive(Debug, Clone)]
pub struct DeletePlan {
    pub keyspace: String,
    pub table: String,
    pub columns: Vec<String>,
    pub where_clause: Vec<Relation>,
    pub if_exists: bool,
    /// Client-supplied timestamp from USING TIMESTAMP clause.
    pub using_timestamp: Option<i64>,
    /// Client-supplied TTL from USING TTL clause.
    pub using_ttl: Option<i32>,
}

#[derive(Debug, Clone)]
pub struct UsePlan {
    pub keyspace: String,
}

#[derive(Debug, Clone)]
pub struct TruncatePlan {
    pub keyspace: String,
    pub table: String,
}

#[derive(Debug, Clone)]
pub struct BatchPlan {
    pub batch_type: BatchType,
    pub plans: Vec<QueryPlan>,
}

#[derive(Debug, Clone)]
pub struct CreateRolePlan {
    pub name: String,
    pub if_not_exists: bool,
    pub is_superuser: bool,
    pub can_login: bool,
    pub password: Option<String>,
}

#[derive(Debug, Clone)]
pub struct AlterRolePlan {
    pub name: String,
    pub password: Option<String>,
    pub superuser: Option<bool>,
    pub login: Option<bool>,
}

#[derive(Debug, Clone)]
pub struct DropRolePlan {
    pub name: String,
    pub if_exists: bool,
}

#[derive(Debug, Clone)]
pub struct GrantPlan {
    pub permissions: Vec<String>,
    pub resource: Resource,
    pub role: String,
}

#[derive(Debug, Clone)]
pub struct RevokePlan {
    pub permissions: Vec<String>,
    pub resource: Resource,
    pub role: String,
}

#[derive(Debug, Clone)]
pub struct ListRolesPlan {
    pub of_role: Option<String>,
    pub no_recursive: bool,
}

#[derive(Debug, Clone)]
pub struct CreateIndexPlan {
    pub keyspace: String,
    pub table: String,
    pub index_name: String,
    pub column: String,
    pub kind: String,
    pub custom_class: Option<String>,
    pub options: HashMap<String, String>,
    pub if_not_exists: bool,
}

#[derive(Debug, Clone)]
pub struct DropIndexPlan {
    pub keyspace: String,
    pub index_name: String,
    pub if_exists: bool,
}

// ─── Materialized View Plans ─────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct CreateMaterializedViewPlan {
    pub keyspace: String,
    pub name: String,
    pub if_not_exists: bool,
    pub base_table: String,
    pub select_columns: SelectColumns,
    pub where_clause: Vec<Relation>,
    pub partition_key: Vec<String>,
    pub clustering_key: Vec<String>,
    pub clustering_order: Vec<(String, ClusteringOrder)>,
    pub options: HashMap<String, String>,
}

#[derive(Debug, Clone)]
pub struct DropMaterializedViewPlan {
    pub keyspace: String,
    pub name: String,
    pub if_exists: bool,
}

#[derive(Debug, Clone)]
pub struct AlterMaterializedViewPlan {
    pub keyspace: String,
    pub name: String,
    pub options: HashMap<String, String>,
}

// ─── UDT Plans ───────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct CreateTypePlan {
    pub keyspace: String,
    pub name: String,
    pub if_not_exists: bool,
    pub fields: Vec<(String, String)>,
}

#[derive(Debug, Clone)]
pub struct DropTypePlan {
    pub keyspace: String,
    pub name: String,
    pub if_exists: bool,
}

// ─── UDF Plans ───────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct CreateFunctionPlan {
    pub keyspace: String,
    pub name: String,
    pub or_replace: bool,
    pub if_not_exists: bool,
    pub args: Vec<(String, String)>,
    pub called_on_null_input: bool,
    pub return_type: String,
    pub language: String,
    pub body: String,
}

#[derive(Debug, Clone)]
pub struct DropFunctionPlan {
    pub keyspace: String,
    pub name: String,
    pub if_exists: bool,
    pub arg_types: Vec<String>,
}

// ─── UDA Plans ───────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct CreateAggregatePlan {
    pub keyspace: String,
    pub name: String,
    pub or_replace: bool,
    pub if_not_exists: bool,
    pub arg_types: Vec<String>,
    pub sfunc: String,
    pub stype: String,
    pub finalfunc: Option<String>,
    pub initcond: Option<String>,
}

#[derive(Debug, Clone)]
pub struct DropAggregatePlan {
    pub keyspace: String,
    pub name: String,
    pub if_exists: bool,
    pub arg_types: Vec<String>,
}

// ─── Trigger Plans ───────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct CreateTriggerPlan {
    pub keyspace: String,
    pub table: String,
    pub name: String,
    pub if_not_exists: bool,
    pub trigger_class: String,
}

#[derive(Debug, Clone)]
pub struct DropTriggerPlan {
    pub keyspace: String,
    pub table: String,
    pub name: String,
    pub if_exists: bool,
}

#[derive(Debug, Clone)]
pub struct DescribePlan {
    pub target: DescribeTarget,
}

/// Plan a parsed statement against the current schema.
///
/// `active_keyspace` is the USE'd keyspace for unqualified table names.
pub fn plan(
    stmt: &Statement,
    schema: &SchemaSnapshot,
    active_keyspace: Option<&str>,
) -> Result<QueryPlan, PlanError> {
    match stmt {
        Statement::Use(u) => {
            // Validate keyspace exists.
            if schema.keyspace(&u.keyspace).is_none() {
                return Err(PlanError::InvalidQuery(format!(
                    "Keyspace '{}' does not exist",
                    u.keyspace
                )));
            }
            Ok(QueryPlan::Use(UsePlan {
                keyspace: u.keyspace.clone(),
            }))
        }

        Statement::CreateKeyspace(ck) => {
            if !ck.if_not_exists && schema.keyspace(&ck.name).is_some() {
                return Err(PlanError::AlreadyExists {
                    ks: ck.name.clone(),
                    name: String::new(),
                });
            }
            Ok(QueryPlan::CreateKeyspace(CreateKeyspacePlan {
                name: ck.name.clone(),
                if_not_exists: ck.if_not_exists,
                replication: ck.replication.clone(),
                durable_writes: ck.durable_writes.unwrap_or(true),
            }))
        }

        Statement::AlterKeyspace(ak) => {
            if schema.keyspace(&ak.name).is_none() {
                return Err(PlanError::InvalidQuery(format!(
                    "Keyspace '{}' does not exist",
                    ak.name
                )));
            }
            Ok(QueryPlan::AlterKeyspace(AlterKeyspacePlan {
                name: ak.name.clone(),
                replication: ak.replication.clone(),
                durable_writes: ak.durable_writes,
            }))
        }

        Statement::DropKeyspace(dk) => {
            if !dk.if_exists && schema.keyspace(&dk.name).is_none() {
                return Err(PlanError::InvalidQuery(format!(
                    "Keyspace '{}' does not exist",
                    dk.name
                )));
            }
            Ok(QueryPlan::DropKeyspace(DropKeyspacePlan {
                name: dk.name.clone(),
                if_exists: dk.if_exists,
            }))
        }

        Statement::CreateTable(ct) => {
            let ks = resolve_keyspace(ct.keyspace.as_deref(), active_keyspace)?;
            if !ct.if_not_exists {
                if let Some(ks_meta) = schema.keyspace(&ks) {
                    if ks_meta.table(&ct.name).is_some() {
                        return Err(PlanError::AlreadyExists {
                            ks: ks.clone(),
                            name: ct.name.clone(),
                        });
                    }
                }
            }

            let mut resolved_cols = Vec::with_capacity(ct.columns.len());
            for col in &ct.columns {
                let cql_type = col.cql_type.resolve().ok_or_else(|| {
                    PlanError::InvalidQuery(format!("Unknown type for column '{}'", col.name))
                })?;

                let masked_with = col.masked_with.as_ref().map(|(func, args)| {
                    let arg_strs = args
                        .iter()
                        .map(|t| match t {
                            crate::ast::Term::Literal(crate::ast::Literal::String(s)) => s.clone(),
                            crate::ast::Term::Literal(crate::ast::Literal::Integer(i)) => {
                                i.to_string()
                            }
                            crate::ast::Term::Literal(crate::ast::Literal::Float(f)) => {
                                f.to_string()
                            }
                            crate::ast::Term::Literal(crate::ast::Literal::Boolean(b)) => {
                                b.to_string()
                            }
                            _ => format!("{:?}", t),
                        })
                        .collect();
                    (func.clone(), arg_strs)
                });

                resolved_cols.push(ResolvedColumnDef {
                    name: col.name.clone(),
                    cql_type,
                    is_static: col.is_static,
                    masked_with,
                });
            }

            Ok(QueryPlan::CreateTable(CreateTablePlan {
                keyspace: ks,
                name: ct.name.clone(),
                if_not_exists: ct.if_not_exists,
                columns: resolved_cols,
                partition_key: ct.partition_key.clone(),
                clustering_key: ct.clustering_key.clone(),
                clustering_order: ct.clustering_order.clone(),
                options: ct.options.clone(),
            }))
        }

        Statement::AlterTable(at) => {
            let ks = resolve_keyspace(at.keyspace.as_deref(), active_keyspace)?;
            Ok(QueryPlan::AlterTable(AlterTablePlan {
                keyspace: ks,
                name: at.name.clone(),
                operation: at.operation.clone(),
            }))
        }

        Statement::DropTable(dt) => {
            let ks = resolve_keyspace(dt.keyspace.as_deref(), active_keyspace)?;
            if !dt.if_exists {
                if let Some(ks_meta) = schema.keyspace(&ks) {
                    if ks_meta.table(&dt.name).is_none() {
                        return Err(PlanError::InvalidQuery(format!(
                            "Table '{}.{}' does not exist",
                            ks, dt.name
                        )));
                    }
                }
            }
            Ok(QueryPlan::DropTable(DropTablePlan {
                keyspace: ks,
                name: dt.name.clone(),
                if_exists: dt.if_exists,
            }))
        }

        Statement::Select(s) => {
            let ks = resolve_keyspace(s.keyspace.as_deref(), active_keyspace)?;

            // Validate against table metadata if available.
            let restrictions = if let Some(table_meta) = schema.table(&ks, &s.table) {
                // Validate SELECT column names exist in table metadata.
                if let SelectColumns::Named(ref selectors) = s.columns {
                    validate_selectors(selectors, table_meta)?;
                }

                // Validate WHERE clause restrictions.
                if !s.where_clause.is_empty() {
                    let restriction_set =
                        crate::restrictions::statement_restrictions::build(
                            &s.where_clause,
                            table_meta,
                            s.allow_filtering,
                        )
                        .map_err(|e| PlanError::InvalidQuery(e.to_string()))?;
                    Some(restriction_set)
                } else {
                    Some(crate::restrictions::RestrictionSet::default())
                }
            } else {
                None
            };

            // Detect ANN clause: placeholder detection — in practice, the AST
            // would encode ANN OF explicitly. For now, ann_clause is None.
            let ann_clause: Option<AnnClause> = None;

            Ok(QueryPlan::Select(SelectPlan {
                keyspace: ks,
                table: s.table.clone(),
                columns: s.columns.clone(),
                distinct: s.distinct,
                json: s.json,
                where_clause: s.where_clause.clone(),
                order_by: s.order_by.clone(),
                limit: s.limit.clone(),
                allow_filtering: s.allow_filtering,
                restrictions,
                ann_clause,
            }))
        }

        Statement::Insert(i) => {
            let ks = resolve_keyspace(i.keyspace.as_deref(), active_keyspace)?;

            // Validate column count matches value count.
            if !i.columns.is_empty() && i.columns.len() != i.values.len() {
                return Err(PlanError::InvalidQuery(format!(
                    "Unmatched column names/values for INSERT: {} columns but {} values",
                    i.columns.len(),
                    i.values.len()
                )));
            }

            let (using_timestamp, using_ttl) = extract_using(&i.using);
            Ok(QueryPlan::Insert(InsertPlan {
                keyspace: ks,
                table: i.table.clone(),
                columns: i.columns.clone(),
                values: i.values.clone(),
                if_not_exists: i.if_not_exists,
                json: i.json.clone(),
                using_timestamp,
                using_ttl,
            }))
        }

        Statement::Update(u) => {
            let ks = resolve_keyspace(u.keyspace.as_deref(), active_keyspace)?;
            let (using_timestamp, using_ttl) = extract_using(&u.using);
            Ok(QueryPlan::Update(UpdatePlan {
                keyspace: ks,
                table: u.table.clone(),
                assignments: u.assignments.clone(),
                where_clause: u.where_clause.clone(),
                if_exists: u.if_exists,
                using_timestamp,
                using_ttl,
            }))
        }

        Statement::Delete(d) => {
            let ks = resolve_keyspace(d.keyspace.as_deref(), active_keyspace)?;
            let (using_timestamp, using_ttl) = extract_using(&d.using);
            Ok(QueryPlan::Delete(DeletePlan {
                keyspace: ks,
                table: d.table.clone(),
                columns: d.columns.clone(),
                where_clause: d.where_clause.clone(),
                if_exists: d.if_exists,
                using_timestamp,
                using_ttl,
            }))
        }

        Statement::Truncate(t) => {
            let ks = resolve_keyspace(t.keyspace.as_deref(), active_keyspace)?;
            Ok(QueryPlan::Truncate(TruncatePlan {
                keyspace: ks,
                table: t.table.clone(),
            }))
        }

        Statement::Batch(b) => {
            let mut plans = Vec::with_capacity(b.statements.len());
            for stmt in &b.statements {
                plans.push(plan(stmt, schema, active_keyspace)?);
            }
            Ok(QueryPlan::Batch(BatchPlan {
                batch_type: b.batch_type,
                plans,
            }))
        }

        Statement::CreateRole(cr) => Ok(QueryPlan::CreateRole(CreateRolePlan {
            name: cr.name.clone(),
            if_not_exists: cr.if_not_exists,
            is_superuser: cr.superuser.unwrap_or(false),
            can_login: cr.login.unwrap_or(false),
            password: cr.password.clone(),
        })),

        Statement::AlterRole(ar) => Ok(QueryPlan::AlterRole(AlterRolePlan {
            name: ar.name.clone(),
            password: ar.password.clone(),
            superuser: ar.superuser,
            login: ar.login,
        })),

        Statement::DropRole(dr) => Ok(QueryPlan::DropRole(DropRolePlan {
            name: dr.name.clone(),
            if_exists: dr.if_exists,
        })),

        Statement::Grant(gr) => Ok(QueryPlan::Grant(GrantPlan {
            permissions: gr.permissions.clone(),
            resource: gr.resource.clone(),
            role: gr.role.clone(),
        })),

        Statement::Revoke(rv) => Ok(QueryPlan::Revoke(RevokePlan {
            permissions: rv.permissions.clone(),
            resource: rv.resource.clone(),
            role: rv.role.clone(),
        })),

        Statement::ListRoles(lr) => Ok(QueryPlan::ListRoles(ListRolesPlan {
            of_role: lr.of_role.clone(),
            no_recursive: lr.no_recursive,
        })),

        Statement::CreateIndex(ci) => {
            let ks = resolve_keyspace(ci.keyspace.as_deref(), active_keyspace)?;

            // Validate keyspace and table exist
            let ks_meta = schema
                .keyspace(&ks)
                .ok_or_else(|| PlanError::InvalidQuery(format!("Keyspace '{}' does not exist", ks)))?;

            let table_meta = ks_meta
                .table(&ci.table)
                .ok_or_else(|| {
                    PlanError::InvalidQuery(format!("Table '{}.{}' does not exist", ks, ci.table))
                })?;

            // Validate column exists
            if table_meta.column(&ci.column).is_none() {
                return Err(PlanError::InvalidQuery(format!(
                    "Column '{}' does not exist in table '{}.{}'",
                    ci.column, ks, ci.table
                )));
            }

            // Validate column is not a partition key
            let pk_names: Vec<&str> = table_meta
                .partition_key_columns()
                .iter()
                .map(|c| c.name.as_str())
                .collect();
            if pk_names.contains(&ci.column.as_str()) {
                return Err(PlanError::InvalidQuery(format!(
                    "Cannot create secondary index on partition key column '{}'",
                    ci.column
                )));
            }

            // Auto-generate name if not provided
            let index_name = ci
                .name
                .clone()
                .unwrap_or_else(|| format!("{}_{}_idx", ci.table, ci.column));

            // Check for duplicate index name
            if !ci.if_not_exists && table_meta.index(&index_name).is_some() {
                return Err(PlanError::AlreadyExists {
                    ks: ks.clone(),
                    name: index_name,
                });
            }

            let kind = if ci.custom_class.is_some() {
                "CUSTOM".to_string()
            } else {
                "KEYS".to_string()
            };

            Ok(QueryPlan::CreateIndex(CreateIndexPlan {
                keyspace: ks,
                table: ci.table.clone(),
                index_name,
                column: ci.column.clone(),
                kind,
                custom_class: ci.custom_class.clone(),
                options: ci.options.clone(),
                if_not_exists: ci.if_not_exists,
            }))
        }

        Statement::DropIndex(di) => {
            let ks = resolve_keyspace(di.keyspace.as_deref(), active_keyspace)?;

            if !di.if_exists {
                // Verify the index exists somewhere in the keyspace
                if let Some(ks_meta) = schema.keyspace(&ks) {
                    if ks_meta.find_indexed_table(&di.name).is_none() {
                        return Err(PlanError::InvalidQuery(format!(
                            "Index '{}' does not exist in keyspace '{}'",
                            di.name, ks
                        )));
                    }
                }
            }

            Ok(QueryPlan::DropIndex(DropIndexPlan {
                keyspace: ks,
                index_name: di.name.clone(),
                if_exists: di.if_exists,
            }))
        }

        Statement::CreateMaterializedView(cmv) => {
            let ks = resolve_keyspace(cmv.keyspace.as_deref(), active_keyspace)?;
            if let Some(ks_meta) = schema.keyspace(&ks) {
                if ks_meta.table(&cmv.select.table).is_none() {
                    return Err(PlanError::InvalidQuery(format!(
                        "Base table '{}.{}' does not exist",
                        ks, cmv.select.table
                    )));
                }
                if !cmv.if_not_exists && ks_meta.view(&cmv.name).is_some() {
                    return Err(PlanError::AlreadyExists {
                        ks: ks.clone(),
                        name: cmv.name.clone(),
                    });
                }
            }
            Ok(QueryPlan::CreateMaterializedView(CreateMaterializedViewPlan {
                keyspace: ks,
                name: cmv.name.clone(),
                if_not_exists: cmv.if_not_exists,
                base_table: cmv.select.table.clone(),
                select_columns: cmv.select.columns.clone(),
                where_clause: cmv.select.where_clause.clone(),
                partition_key: cmv.partition_key.clone(),
                clustering_key: cmv.clustering_key.clone(),
                clustering_order: cmv.clustering_order.clone(),
                options: cmv.options.clone(),
            }))
        }

        Statement::DropMaterializedView(dmv) => {
            let ks = resolve_keyspace(dmv.keyspace.as_deref(), active_keyspace)?;
            if !dmv.if_exists {
                if let Some(ks_meta) = schema.keyspace(&ks) {
                    if ks_meta.view(&dmv.name).is_none() {
                        return Err(PlanError::InvalidQuery(format!(
                            "Materialized view '{}.{}' does not exist",
                            ks, dmv.name
                        )));
                    }
                }
            }
            Ok(QueryPlan::DropMaterializedView(DropMaterializedViewPlan {
                keyspace: ks,
                name: dmv.name.clone(),
                if_exists: dmv.if_exists,
            }))
        }

        Statement::AlterMaterializedView(amv) => {
            let ks = resolve_keyspace(amv.keyspace.as_deref(), active_keyspace)?;
            Ok(QueryPlan::AlterMaterializedView(AlterMaterializedViewPlan {
                keyspace: ks,
                name: amv.name.clone(),
                options: amv.options.clone(),
            }))
        }

        Statement::CreateType(ct) => {
            let ks = resolve_keyspace(ct.keyspace.as_deref(), active_keyspace)?;
            if !ct.if_not_exists {
                if let Some(ks_meta) = schema.keyspace(&ks) {
                    if ks_meta.user_type(&ct.name).is_some() {
                        return Err(PlanError::AlreadyExists {
                            ks: ks.clone(),
                            name: ct.name.clone(),
                        });
                    }
                }
            }
            let fields = ct.fields.iter()
                .map(|(name, typ)| (name.clone(), format!("{:?}", typ)))
                .collect();
            Ok(QueryPlan::CreateType(CreateTypePlan {
                keyspace: ks,
                name: ct.name.clone(),
                if_not_exists: ct.if_not_exists,
                fields,
            }))
        }

        Statement::DropType(dt) => {
            let ks = resolve_keyspace(dt.keyspace.as_deref(), active_keyspace)?;
            if !dt.if_exists {
                if let Some(ks_meta) = schema.keyspace(&ks) {
                    if ks_meta.user_type(&dt.name).is_none() {
                        return Err(PlanError::InvalidQuery(format!(
                            "Type '{}.{}' does not exist",
                            ks, dt.name
                        )));
                    }
                }
            }
            Ok(QueryPlan::DropType(DropTypePlan {
                keyspace: ks,
                name: dt.name.clone(),
                if_exists: dt.if_exists,
            }))
        }

        Statement::AlterType(_at) => {
            Err(PlanError::InvalidQuery(
                "ALTER TYPE is not yet fully supported".into(),
            ))
        }

        Statement::CreateFunction(cf) => {
            let ks = resolve_keyspace(cf.keyspace.as_deref(), active_keyspace)?;
            let args: Vec<(String, String)> = cf.args.iter()
                .map(|(name, typ)| (name.clone(), format!("{:?}", typ)))
                .collect();
            Ok(QueryPlan::CreateFunction(CreateFunctionPlan {
                keyspace: ks,
                name: cf.name.clone(),
                or_replace: cf.or_replace,
                if_not_exists: cf.if_not_exists,
                args,
                called_on_null_input: cf.called_on_null_input,
                return_type: format!("{:?}", cf.return_type),
                language: cf.language.clone(),
                body: cf.body.clone(),
            }))
        }

        Statement::DropFunction(df) => {
            let ks = resolve_keyspace(df.keyspace.as_deref(), active_keyspace)?;
            let arg_types: Vec<String> = df.arg_types.iter()
                .map(|t| format!("{:?}", t))
                .collect();
            Ok(QueryPlan::DropFunction(DropFunctionPlan {
                keyspace: ks,
                name: df.name.clone(),
                if_exists: df.if_exists,
                arg_types,
            }))
        }

        Statement::CreateAggregate(ca) => {
            let ks = resolve_keyspace(ca.keyspace.as_deref(), active_keyspace)?;
            let arg_types: Vec<String> = ca.arg_types.iter()
                .map(|t| format!("{:?}", t))
                .collect();
            let initcond = ca.initcond.as_ref().map(|t| format!("{:?}", t));
            Ok(QueryPlan::CreateAggregate(CreateAggregatePlan {
                keyspace: ks,
                name: ca.name.clone(),
                or_replace: ca.or_replace,
                if_not_exists: ca.if_not_exists,
                arg_types,
                sfunc: ca.sfunc.clone(),
                stype: format!("{:?}", ca.stype),
                finalfunc: ca.finalfunc.clone(),
                initcond,
            }))
        }

        Statement::DropAggregate(da) => {
            let ks = resolve_keyspace(da.keyspace.as_deref(), active_keyspace)?;
            let arg_types: Vec<String> = da.arg_types.iter()
                .map(|t| format!("{:?}", t))
                .collect();
            Ok(QueryPlan::DropAggregate(DropAggregatePlan {
                keyspace: ks,
                name: da.name.clone(),
                if_exists: da.if_exists,
                arg_types,
            }))
        }

        Statement::CreateTrigger(ct) => {
            let ks = resolve_keyspace(ct.keyspace.as_deref(), active_keyspace)?;
            Ok(QueryPlan::CreateTrigger(CreateTriggerPlan {
                keyspace: ks,
                table: ct.table.clone(),
                name: ct.name.clone(),
                if_not_exists: ct.if_not_exists,
                trigger_class: ct.trigger_class.clone(),
            }))
        }

        Statement::DropTrigger(dt) => {
            let ks = resolve_keyspace(dt.keyspace.as_deref(), active_keyspace)?;
            Ok(QueryPlan::DropTrigger(DropTriggerPlan {
                keyspace: ks,
                table: dt.table.clone(),
                name: dt.name.clone(),
                if_exists: dt.if_exists,
            }))
        }

        Statement::Describe(desc) => Ok(QueryPlan::Describe(DescribePlan {
            target: desc.target.clone(),
        })),

        // Statement types not yet fully plannable
        _ => Err(PlanError::InvalidQuery(
            "statement type not yet supported by the planner".into(),
        )),
    }
}

/// Validate that SELECT selectors reference valid columns.
fn validate_selectors(
    selectors: &[Selector],
    table: &cassandra_schema::table::TableMetadata,
) -> Result<(), PlanError> {
    for selector in selectors {
        validate_selector(selector, table)?;
    }
    Ok(())
}

fn validate_selector(
    selector: &Selector,
    table: &cassandra_schema::table::TableMetadata,
) -> Result<(), PlanError> {
    match selector {
        Selector::Column(name) => {
            if table.column(name).is_none() {
                return Err(PlanError::InvalidQuery(format!(
                    "Undefined column name '{}'",
                    name
                )));
            }
        }
        Selector::Function(_, args) => {
            for arg in args {
                validate_selector(arg, table)?;
            }
        }
        Selector::Alias { selector, .. } => {
            validate_selector(selector, table)?;
        }
        Selector::Count | Selector::WritetimeOrTtl(_, _) => {}
    }
    Ok(())
}

/// Extract timestamp and TTL from a USING clause list.
fn extract_using(using: &[UsingClause]) -> (Option<i64>, Option<i32>) {
    let mut ts = None;
    let mut ttl = None;
    for clause in using {
        match clause {
            UsingClause::Timestamp(term) => {
                if let Term::Literal(Literal::Integer(v)) = term {
                    ts = Some(*v);
                }
            }
            UsingClause::Ttl(term) => {
                if let Term::Literal(Literal::Integer(v)) = term {
                    ttl = Some(*v as i32);
                }
            }
        }
    }
    (ts, ttl)
}

fn resolve_keyspace(explicit: Option<&str>, active: Option<&str>) -> Result<String, PlanError> {
    match explicit.or(active) {
        Some(ks) => Ok(ks.to_string()),
        None => Err(PlanError::InvalidQuery(
            "No keyspace has been specified. USE a keyspace or qualify the table name.".into(),
        )),
    }
}

/// Detect an ANN clause from the ORDER BY and WHERE clause.
///
/// In Cassandra 5, the syntax is:
/// `ORDER BY <col> ANN OF [x, y, z] LIMIT k`
///
/// Since the AST doesn't yet encode ANN OF natively, this is a placeholder
/// that returns None. When the parser adds ANN support, this function will
/// extract the vector literal and top_k from the AST.
#[allow(unused_variables)]
fn detect_ann_clause(
    order_by: &[(String, ClusteringOrder)],
    limit: &Option<Term>,
    where_clause: &[Relation],
) -> Option<AnnClause> {
    // GAP(gap_guard_index_differential_testing): Implement when AST supports ANN OF syntax — tracked in gap_guards.rs
    // For now, ANN is not parseable and this always returns None.
    None
}

/// Planner error.
#[derive(Debug, Clone)]
pub enum PlanError {
    InvalidQuery(String),
    SyntaxError(String),
    AlreadyExists { ks: String, name: String },
}

impl std::fmt::Display for PlanError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PlanError::InvalidQuery(m) => write!(f, "Invalid query: {}", m),
            PlanError::SyntaxError(m) => write!(f, "Syntax error: {}", m),
            PlanError::AlreadyExists { ks, name } => {
                if name.is_empty() {
                    write!(f, "Keyspace '{}' already exists", ks)
                } else {
                    write!(f, "Table '{}.{}' already exists", ks, name)
                }
            }
        }
    }
}
impl std::error::Error for PlanError {}

impl From<ParseError> for PlanError {
    fn from(e: ParseError) -> Self {
        PlanError::SyntaxError(e.message)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser;
    use cassandra_schema::{KeyspaceMetadata, KeyspaceParams, SchemaSnapshot};

    fn test_schema() -> SchemaSnapshot {
        let ks = KeyspaceMetadata::new("test_ks", KeyspaceParams::default());
        let mut snapshot = SchemaSnapshot::empty();
        snapshot.keyspaces.insert("test_ks".to_string(), ks);
        snapshot
    }

    #[test]
    fn plan_use_valid() {
        let schema = test_schema();
        let stmt = parser::parse("USE test_ks").unwrap();
        let p = plan(&stmt, &schema, None).unwrap();
        assert!(matches!(p, QueryPlan::Use(_)));
    }

    #[test]
    fn plan_use_invalid() {
        let schema = test_schema();
        let stmt = parser::parse("USE nonexistent").unwrap();
        assert!(plan(&stmt, &schema, None).is_err());
    }

    #[test]
    fn plan_create_keyspace() {
        let schema = SchemaSnapshot::empty();
        let stmt = parser::parse(
            "CREATE KEYSPACE new_ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}"
        ).unwrap();
        let p = plan(&stmt, &schema, None).unwrap();
        match p {
            QueryPlan::CreateKeyspace(ck) => {
                assert_eq!(ck.name, "new_ks");
                assert!(ck.durable_writes);
            }
            _ => panic!("expected CreateKeyspace"),
        }
    }

    #[test]
    fn plan_create_keyspace_already_exists() {
        let schema = test_schema();
        let stmt = parser::parse(
            "CREATE KEYSPACE test_ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}"
        ).unwrap();
        assert!(plan(&stmt, &schema, None).is_err());
    }

    #[test]
    fn plan_create_keyspace_if_not_exists() {
        let schema = test_schema();
        let stmt = parser::parse(
            "CREATE KEYSPACE IF NOT EXISTS test_ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}"
        ).unwrap();
        let p = plan(&stmt, &schema, None).unwrap();
        assert!(matches!(p, QueryPlan::CreateKeyspace(_)));
    }

    #[test]
    fn plan_select_no_keyspace() {
        let schema = test_schema();
        let stmt = parser::parse("SELECT * FROM users").unwrap();
        // No active keyspace → error.
        assert!(plan(&stmt, &schema, None).is_err());
    }

    #[test]
    fn plan_select_with_active_keyspace() {
        let schema = test_schema();
        let stmt = parser::parse("SELECT * FROM users").unwrap();
        let p = plan(&stmt, &schema, Some("test_ks")).unwrap();
        match p {
            QueryPlan::Select(s) => {
                assert_eq!(s.keyspace, "test_ks");
                assert_eq!(s.table, "users");
            }
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn plan_select_qualified() {
        let schema = test_schema();
        let stmt = parser::parse("SELECT * FROM test_ks.users").unwrap();
        let p = plan(&stmt, &schema, None).unwrap();
        match p {
            QueryPlan::Select(s) => assert_eq!(s.keyspace, "test_ks"),
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn plan_is_schema_altering() {
        let schema = SchemaSnapshot::empty();
        let stmt = parser::parse("CREATE KEYSPACE k WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}").unwrap();
        let p = plan(&stmt, &schema, None).unwrap();
        assert!(p.is_schema_altering());
    }

    fn schema_with_table() -> SchemaSnapshot {
        use cassandra_schema::column::ColumnMetadata;
        use cassandra_schema::table::TableMetadataBuilder;
        use cassandra_types::CqlType;

        let table = TableMetadataBuilder::new("test_ks", "users")
            .add_column(ColumnMetadata::partition_key("id", 0, CqlType::Int))
            .add_column(ColumnMetadata::regular("email", CqlType::Varchar))
            .add_column(ColumnMetadata::regular("name", CqlType::Varchar))
            .build();
        let ks =
            KeyspaceMetadata::new("test_ks", KeyspaceParams::default()).with_table(table);
        let mut snapshot = SchemaSnapshot::empty();
        snapshot.keyspaces.insert("test_ks".to_string(), ks);
        snapshot
    }

    #[test]
    fn plan_create_index_basic() {
        let schema = schema_with_table();
        let stmt =
            parser::parse("CREATE INDEX email_idx ON test_ks.users (email)").unwrap();
        let p = plan(&stmt, &schema, None).unwrap();
        assert!(p.is_schema_altering());
        match p {
            QueryPlan::CreateIndex(ci) => {
                assert_eq!(ci.keyspace, "test_ks");
                assert_eq!(ci.table, "users");
                assert_eq!(ci.index_name, "email_idx");
                assert_eq!(ci.column, "email");
                assert_eq!(ci.kind, "KEYS");
                assert!(ci.custom_class.is_none());
            }
            _ => panic!("expected CreateIndex, got {:?}", p),
        }
    }

    #[test]
    fn plan_create_index_auto_name() {
        let schema = schema_with_table();
        let stmt =
            parser::parse("CREATE INDEX ON test_ks.users (email)").unwrap();
        let p = plan(&stmt, &schema, None).unwrap();
        match p {
            QueryPlan::CreateIndex(ci) => {
                assert_eq!(ci.index_name, "users_email_idx");
            }
            _ => panic!("expected CreateIndex"),
        }
    }

    #[test]
    fn plan_create_index_on_partition_key_fails() {
        let schema = schema_with_table();
        let stmt =
            parser::parse("CREATE INDEX ON test_ks.users (id)").unwrap();
        assert!(plan(&stmt, &schema, None).is_err());
    }

    #[test]
    fn plan_create_index_nonexistent_column_fails() {
        let schema = schema_with_table();
        let stmt =
            parser::parse("CREATE INDEX ON test_ks.users (nonexistent)").unwrap();
        assert!(plan(&stmt, &schema, None).is_err());
    }

    #[test]
    fn plan_create_index_if_not_exists() {
        let schema = schema_with_table();
        let stmt = parser::parse(
            "CREATE INDEX IF NOT EXISTS ON test_ks.users (email)",
        )
        .unwrap();
        let p = plan(&stmt, &schema, None).unwrap();
        match p {
            QueryPlan::CreateIndex(ci) => assert!(ci.if_not_exists),
            _ => panic!("expected CreateIndex"),
        }
    }

    #[test]
    fn plan_drop_index_if_exists() {
        let schema = schema_with_table();
        let stmt =
            parser::parse("DROP INDEX IF EXISTS test_ks.some_idx").unwrap();
        let p = plan(&stmt, &schema, None).unwrap();
        match p {
            QueryPlan::DropIndex(di) => {
                assert_eq!(di.keyspace, "test_ks");
                assert_eq!(di.index_name, "some_idx");
                assert!(di.if_exists);
            }
            _ => panic!("expected DropIndex"),
        }
    }

    #[test]
    fn plan_drop_index_nonexistent_fails() {
        let schema = schema_with_table();
        let stmt =
            parser::parse("DROP INDEX test_ks.nonexistent_idx").unwrap();
        assert!(plan(&stmt, &schema, None).is_err());
    }

    // ── WU-14: Client timestamps and TTL from USING clause ──

    #[test]
    fn plan_insert_using_timestamp() {
        let schema = test_schema();
        let stmt = parser::parse(
            "INSERT INTO test_ks.users (id, email) VALUES (1, 'a@b.com') USING TIMESTAMP 12345",
        )
        .unwrap();
        let p = plan(&stmt, &schema, Some("test_ks")).unwrap();
        match p {
            QueryPlan::Insert(ip) => {
                assert_eq!(ip.using_timestamp, Some(12345));
                assert_eq!(ip.using_ttl, None);
            }
            _ => panic!("expected Insert"),
        }
    }

    #[test]
    fn plan_insert_using_ttl() {
        let schema = test_schema();
        let stmt = parser::parse(
            "INSERT INTO test_ks.users (id, email) VALUES (1, 'a@b.com') USING TTL 3600",
        )
        .unwrap();
        let p = plan(&stmt, &schema, Some("test_ks")).unwrap();
        match p {
            QueryPlan::Insert(ip) => {
                assert_eq!(ip.using_timestamp, None);
                assert_eq!(ip.using_ttl, Some(3600));
            }
            _ => panic!("expected Insert"),
        }
    }

    #[test]
    fn plan_insert_using_timestamp_and_ttl() {
        let schema = test_schema();
        let stmt = parser::parse(
            "INSERT INTO test_ks.users (id, email) VALUES (1, 'a@b.com') USING TIMESTAMP 999 AND TTL 60",
        )
        .unwrap();
        let p = plan(&stmt, &schema, Some("test_ks")).unwrap();
        match p {
            QueryPlan::Insert(ip) => {
                assert_eq!(ip.using_timestamp, Some(999));
                assert_eq!(ip.using_ttl, Some(60));
            }
            _ => panic!("expected Insert"),
        }
    }

    #[test]
    fn plan_update_using_timestamp() {
        let schema = test_schema();
        let stmt = parser::parse(
            "UPDATE test_ks.users USING TIMESTAMP 5000 SET email = 'x@y' WHERE id = 1",
        )
        .unwrap();
        let p = plan(&stmt, &schema, Some("test_ks")).unwrap();
        match p {
            QueryPlan::Update(up) => {
                assert_eq!(up.using_timestamp, Some(5000));
                assert_eq!(up.using_ttl, None);
            }
            _ => panic!("expected Update"),
        }
    }

    #[test]
    fn plan_delete_using_timestamp() {
        let schema = test_schema();
        let stmt = parser::parse(
            "DELETE FROM test_ks.users USING TIMESTAMP 7000 WHERE id = 1",
        )
        .unwrap();
        let p = plan(&stmt, &schema, Some("test_ks")).unwrap();
        match p {
            QueryPlan::Delete(dp) => {
                assert_eq!(dp.using_timestamp, Some(7000));
                assert_eq!(dp.using_ttl, None);
            }
            _ => panic!("expected Delete"),
        }
    }

    #[test]
    fn plan_insert_no_using_clause() {
        let schema = test_schema();
        let stmt = parser::parse(
            "INSERT INTO test_ks.users (id, email) VALUES (1, 'a@b.com')",
        )
        .unwrap();
        let p = plan(&stmt, &schema, Some("test_ks")).unwrap();
        match p {
            QueryPlan::Insert(ip) => {
                assert_eq!(ip.using_timestamp, None);
                assert_eq!(ip.using_ttl, None);
            }
            _ => panic!("expected Insert"),
        }
    }

    // ── WU-14: extract_using helper ──

    #[test]
    fn extract_using_empty() {
        let (ts, ttl) = super::extract_using(&[]);
        assert_eq!(ts, None);
        assert_eq!(ttl, None);
    }

    #[test]
    fn extract_using_timestamp_only() {
        let clauses = vec![UsingClause::Timestamp(Term::Literal(Literal::Integer(42)))];
        let (ts, ttl) = super::extract_using(&clauses);
        assert_eq!(ts, Some(42));
        assert_eq!(ttl, None);
    }

    #[test]
    fn extract_using_ttl_only() {
        let clauses = vec![UsingClause::Ttl(Term::Literal(Literal::Integer(300)))];
        let (ts, ttl) = super::extract_using(&clauses);
        assert_eq!(ts, None);
        assert_eq!(ttl, Some(300));
    }

    #[test]
    fn extract_using_both() {
        let clauses = vec![
            UsingClause::Timestamp(Term::Literal(Literal::Integer(100))),
            UsingClause::Ttl(Term::Literal(Literal::Integer(60))),
        ];
        let (ts, ttl) = super::extract_using(&clauses);
        assert_eq!(ts, Some(100));
        assert_eq!(ttl, Some(60));
    }
}
