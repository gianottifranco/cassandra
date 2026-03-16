// Licensed under Apache License, Version 2.0.

//! Query planner: validates AST against schema and produces executable plans.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.cql3.statements.SelectStatement`
//! - `org.apache.cassandra.cql3.statements.ModificationStatement`

use std::collections::HashMap;
use cassandra_schema::SchemaSnapshot;
use crate::ast::*;
use crate::parser::ParseError;

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
}

#[derive(Debug, Clone)]
pub struct InsertPlan {
    pub keyspace: String,
    pub table: String,
    pub columns: Vec<String>,
    pub values: Vec<Term>,
    pub if_not_exists: bool,
}

#[derive(Debug, Clone)]
pub struct UpdatePlan {
    pub keyspace: String,
    pub table: String,
    pub assignments: Vec<Assignment>,
    pub where_clause: Vec<Relation>,
    pub if_exists: bool,
}

#[derive(Debug, Clone)]
pub struct DeletePlan {
    pub keyspace: String,
    pub table: String,
    pub columns: Vec<String>,
    pub where_clause: Vec<Relation>,
    pub if_exists: bool,
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
                resolved_cols.push(ResolvedColumnDef {
                    name: col.name.clone(),
                    cql_type,
                    is_static: col.is_static,
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
            // TODO(phase-3+): Validate columns against table metadata.
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
            }))
        }

        Statement::Insert(i) => {
            let ks = resolve_keyspace(i.keyspace.as_deref(), active_keyspace)?;
            Ok(QueryPlan::Insert(InsertPlan {
                keyspace: ks,
                table: i.table.clone(),
                columns: i.columns.clone(),
                values: i.values.clone(),
                if_not_exists: i.if_not_exists,
            }))
        }

        Statement::Update(u) => {
            let ks = resolve_keyspace(u.keyspace.as_deref(), active_keyspace)?;
            Ok(QueryPlan::Update(UpdatePlan {
                keyspace: ks,
                table: u.table.clone(),
                assignments: u.assignments.clone(),
                where_clause: u.where_clause.clone(),
                if_exists: u.if_exists,
            }))
        }

        Statement::Delete(d) => {
            let ks = resolve_keyspace(d.keyspace.as_deref(), active_keyspace)?;
            Ok(QueryPlan::Delete(DeletePlan {
                keyspace: ks,
                table: d.table.clone(),
                columns: d.columns.clone(),
                where_clause: d.where_clause.clone(),
                if_exists: d.if_exists,
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

        // Phase 12: new statement types not yet fully plannable
        _ => Err(PlanError::InvalidQuery(
            "statement type not yet supported by the planner".into(),
        )),
    }
}

fn resolve_keyspace(
    explicit: Option<&str>,
    active: Option<&str>,
) -> Result<String, PlanError> {
    match explicit.or(active) {
        Some(ks) => Ok(ks.to_string()),
        None => Err(PlanError::InvalidQuery(
            "No keyspace has been specified. USE a keyspace or qualify the table name.".into(),
        )),
    }
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
    use cassandra_schema::{SchemaSnapshot, KeyspaceMetadata, KeyspaceParams};
    use crate::parser;

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
}
