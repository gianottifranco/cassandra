// Licensed under Apache License, Version 2.0.

//! CQL Abstract Syntax Tree.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.cql3.statements.*`
//! - `org.apache.cassandra.cql3.functions.*`
//! - `org.apache.cassandra.triggers.*`
//! - `org.apache.cassandra.auth.*`

use std::collections::HashMap;

/// Top-level CQL statement.
#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    // ── DDL ──
    CreateKeyspace(CreateKeyspace),
    AlterKeyspace(AlterKeyspace),
    DropKeyspace(DropKeyspace),
    CreateTable(CreateTable),
    AlterTable(AlterTable),
    DropTable(DropTable),
    CreateIndex(CreateIndex),
    DropIndex(DropIndex),
    CreateMaterializedView(CreateMaterializedView),
    DropMaterializedView(DropMaterializedView),
    CreateType(CreateType),
    DropType(DropType),
    CreateFunction(CreateFunction),
    DropFunction(DropFunction),
    CreateAggregate(CreateAggregate),
    DropAggregate(DropAggregate),
    CreateTrigger(CreateTrigger),
    DropTrigger(DropTrigger),
    // ── DML ──
    Select(Select),
    Insert(Insert),
    Update(Update),
    Delete(Delete),
    // ── DCL ──
    CreateRole(CreateRole),
    AlterRole(AlterRole),
    DropRole(DropRole),
    Grant(GrantStatement),
    Revoke(RevokeStatement),
    ListRoles(ListRolesStatement),
    ListPermissions(ListPermissionsStatement),
    // ── Utility ──
    Use(UseStatement),
    Truncate(TruncateStatement),
    Batch(BatchStatement),
}

impl Statement {
    /// Returns true if this is a DDL statement that modifies schema.
    pub fn is_schema_altering(&self) -> bool {
        matches!(
            self,
            Statement::CreateKeyspace(_)
                | Statement::AlterKeyspace(_)
                | Statement::DropKeyspace(_)
                | Statement::CreateTable(_)
                | Statement::AlterTable(_)
                | Statement::DropTable(_)
                | Statement::CreateIndex(_)
                | Statement::DropIndex(_)
                | Statement::CreateMaterializedView(_)
                | Statement::DropMaterializedView(_)
                | Statement::CreateType(_)
                | Statement::DropType(_)
                | Statement::CreateFunction(_)
                | Statement::DropFunction(_)
                | Statement::CreateAggregate(_)
                | Statement::DropAggregate(_)
                | Statement::CreateTrigger(_)
                | Statement::DropTrigger(_)
        )
    }

    /// Returns true if this is a DCL statement that modifies auth state.
    pub fn is_auth_altering(&self) -> bool {
        matches!(
            self,
            Statement::CreateRole(_)
                | Statement::AlterRole(_)
                | Statement::DropRole(_)
                | Statement::Grant(_)
                | Statement::Revoke(_)
        )
    }
}

// ─── DDL Statements ─────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
pub struct CreateKeyspace {
    pub name: String,
    pub if_not_exists: bool,
    pub replication: HashMap<String, String>,
    pub durable_writes: Option<bool>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AlterKeyspace {
    pub name: String,
    pub replication: Option<HashMap<String, String>>,
    pub durable_writes: Option<bool>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DropKeyspace {
    pub name: String,
    pub if_exists: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateTable {
    pub keyspace: Option<String>,
    pub name: String,
    pub if_not_exists: bool,
    pub columns: Vec<ColumnDef>,
    pub partition_key: Vec<String>,
    pub clustering_key: Vec<String>,
    pub clustering_order: Vec<(String, ClusteringOrder)>,
    pub options: HashMap<String, String>,
    pub compact_storage: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ColumnDef {
    pub name: String,
    pub cql_type: CqlTypeName,
    pub is_static: bool,
}

/// CQL type name as parsed (not yet resolved against the type system).
#[derive(Debug, Clone, PartialEq)]
pub enum CqlTypeName {
    Simple(String),
    List(Box<CqlTypeName>),
    Set(Box<CqlTypeName>),
    Map(Box<CqlTypeName>, Box<CqlTypeName>),
    Tuple(Vec<CqlTypeName>),
    Frozen(Box<CqlTypeName>),
}

impl CqlTypeName {
    /// Resolve to a CqlType from the type registry.
    pub fn resolve(&self) -> Option<cassandra_types::CqlType> {
        use cassandra_types::CqlType;
        match self {
            CqlTypeName::Simple(name) => CqlType::from_cql_name(name),
            CqlTypeName::List(inner) => {
                Some(CqlType::List(Box::new(inner.resolve()?), false))
            }
            CqlTypeName::Set(inner) => {
                Some(CqlType::Set(Box::new(inner.resolve()?), false))
            }
            CqlTypeName::Map(k, v) => {
                Some(CqlType::Map(Box::new(k.resolve()?), Box::new(v.resolve()?), false))
            }
            CqlTypeName::Tuple(types) => {
                let resolved: Option<Vec<_>> = types.iter().map(|t| t.resolve()).collect();
                Some(CqlType::Tuple(resolved?))
            }
            CqlTypeName::Frozen(inner) => {
                let resolved = inner.resolve()?;
                match resolved {
                    CqlType::List(i, _) => Some(CqlType::List(i, true)),
                    CqlType::Set(i, _) => Some(CqlType::Set(i, true)),
                    CqlType::Map(k, v, _) => Some(CqlType::Map(k, v, true)),
                    other => Some(other),
                }
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClusteringOrder {
    Asc,
    Desc,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AlterTable {
    pub keyspace: Option<String>,
    pub name: String,
    pub operation: AlterTableOp,
}

#[derive(Debug, Clone, PartialEq)]
pub enum AlterTableOp {
    AddColumn(ColumnDef),
    DropColumn(String),
    AlterColumn(String, CqlTypeName),
    WithOptions(HashMap<String, String>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct DropTable {
    pub keyspace: Option<String>,
    pub name: String,
    pub if_exists: bool,
}

// ─── DML Statements ─────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
pub struct Select {
    pub distinct: bool,
    pub json: bool,
    pub columns: SelectColumns,
    pub keyspace: Option<String>,
    pub table: String,
    pub where_clause: Vec<Relation>,
    pub order_by: Vec<(String, ClusteringOrder)>,
    pub limit: Option<Term>,
    pub per_partition_limit: Option<Term>,
    pub allow_filtering: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SelectColumns {
    All,
    Named(Vec<Selector>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum Selector {
    Column(String),
    Function(String, Vec<Selector>),
    Alias { selector: Box<Selector>, alias: String },
    Count,
    WritetimeOrTtl(String, String),
}

#[derive(Debug, Clone, PartialEq)]
pub struct Relation {
    pub column: String,
    pub op: RelationOp,
    pub value: Term,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelationOp {
    Eq,
    Neq,
    Lt,
    Gt,
    Lte,
    Gte,
    In,
    Contains,
    ContainsKey,
}

/// A CQL term (value or bind marker).
#[derive(Debug, Clone, PartialEq)]
pub enum Term {
    Literal(Literal),
    BindMarker(BindMarker),
    FunctionCall(String, Vec<Term>),
    TypeHint(CqlTypeName, Box<Term>),
    CollectionLiteral(Vec<Term>),
    MapLiteral(Vec<(Term, Term)>),
    TupleLiteral(Vec<Term>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum Literal {
    String(String),
    Integer(i64),
    Float(f64),
    Blob(Vec<u8>),
    Uuid(String),
    Boolean(bool),
    Null,
}

#[derive(Debug, Clone, PartialEq)]
pub enum BindMarker {
    Anonymous,
    Named(String),
}

#[derive(Debug, Clone, PartialEq)]
pub struct Insert {
    pub keyspace: Option<String>,
    pub table: String,
    pub if_not_exists: bool,
    pub columns: Vec<String>,
    pub values: Vec<Term>,
    pub json: Option<Term>,
    pub using: Vec<UsingClause>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Update {
    pub keyspace: Option<String>,
    pub table: String,
    pub using: Vec<UsingClause>,
    pub assignments: Vec<Assignment>,
    pub where_clause: Vec<Relation>,
    pub if_exists: bool,
    pub if_conditions: Vec<Relation>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Assignment {
    pub column: String,
    pub value: Term,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Delete {
    pub columns: Vec<String>,
    pub keyspace: Option<String>,
    pub table: String,
    pub using: Vec<UsingClause>,
    pub where_clause: Vec<Relation>,
    pub if_exists: bool,
    pub if_conditions: Vec<Relation>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum UsingClause {
    Timestamp(Term),
    Ttl(Term),
}

// ─── Utility Statements ─────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
pub struct UseStatement {
    pub keyspace: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TruncateStatement {
    pub keyspace: Option<String>,
    pub table: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BatchStatement {
    pub batch_type: BatchType,
    pub statements: Vec<Statement>,
    pub using: Vec<UsingClause>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BatchType {
    Logged,
    Unlogged,
    Counter,
}

// ─── Index Statements ───────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
pub struct CreateIndex {
    pub name: Option<String>,
    pub if_not_exists: bool,
    pub keyspace: Option<String>,
    pub table: String,
    pub column: String,
    /// E.g. "keys(col)", "values(col)", "entries(col)", "full(col)".
    pub index_target: Option<String>,
    pub custom_class: Option<String>,
    pub options: HashMap<String, String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DropIndex {
    pub keyspace: Option<String>,
    pub name: String,
    pub if_exists: bool,
}

// ─── Materialized View Statements ───────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
pub struct CreateMaterializedView {
    pub keyspace: Option<String>,
    pub name: String,
    pub if_not_exists: bool,
    pub select: Select,
    pub partition_key: Vec<String>,
    pub clustering_key: Vec<String>,
    pub clustering_order: Vec<(String, ClusteringOrder)>,
    pub options: HashMap<String, String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DropMaterializedView {
    pub keyspace: Option<String>,
    pub name: String,
    pub if_exists: bool,
}

// ─── UDT Statements ────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
pub struct CreateType {
    pub keyspace: Option<String>,
    pub name: String,
    pub if_not_exists: bool,
    pub fields: Vec<(String, CqlTypeName)>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DropType {
    pub keyspace: Option<String>,
    pub name: String,
    pub if_exists: bool,
}

// ─── UDF Statements ────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
pub struct CreateFunction {
    pub keyspace: Option<String>,
    pub name: String,
    pub or_replace: bool,
    pub if_not_exists: bool,
    pub args: Vec<(String, CqlTypeName)>,
    pub called_on_null_input: bool,
    pub return_type: CqlTypeName,
    pub language: String,
    pub body: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DropFunction {
    pub keyspace: Option<String>,
    pub name: String,
    pub if_exists: bool,
    pub arg_types: Vec<CqlTypeName>,
}

// ─── UDA Statements ────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
pub struct CreateAggregate {
    pub keyspace: Option<String>,
    pub name: String,
    pub or_replace: bool,
    pub if_not_exists: bool,
    pub arg_types: Vec<CqlTypeName>,
    pub sfunc: String,
    pub stype: CqlTypeName,
    pub finalfunc: Option<String>,
    pub initcond: Option<Term>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DropAggregate {
    pub keyspace: Option<String>,
    pub name: String,
    pub if_exists: bool,
    pub arg_types: Vec<CqlTypeName>,
}

// ─── Trigger Statements ────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
pub struct CreateTrigger {
    pub name: String,
    pub if_not_exists: bool,
    pub keyspace: Option<String>,
    pub table: String,
    pub trigger_class: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DropTrigger {
    pub name: String,
    pub if_exists: bool,
    pub keyspace: Option<String>,
    pub table: String,
}

// ─── DCL Statements ────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
pub struct CreateRole {
    pub name: String,
    pub if_not_exists: bool,
    pub password: Option<String>,
    pub superuser: Option<bool>,
    pub login: Option<bool>,
    pub options: HashMap<String, String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AlterRole {
    pub name: String,
    pub password: Option<String>,
    pub superuser: Option<bool>,
    pub login: Option<bool>,
    pub options: HashMap<String, String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DropRole {
    pub name: String,
    pub if_exists: bool,
}

/// Permission resource target.
#[derive(Debug, Clone, PartialEq)]
pub enum Resource {
    AllKeyspaces,
    Keyspace(String),
    Table { keyspace: Option<String>, table: String },
    AllRoles,
    Role(String),
    AllFunctions,
    FunctionInKeyspace(String),
    Function { keyspace: Option<String>, name: String, arg_types: Vec<CqlTypeName> },
    AllMBeans,
    MBean(String),
    MBeanPattern(String),
}

#[derive(Debug, Clone, PartialEq)]
pub struct GrantStatement {
    pub permissions: Vec<String>,
    pub resource: Resource,
    pub role: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RevokeStatement {
    pub permissions: Vec<String>,
    pub resource: Resource,
    pub role: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ListRolesStatement {
    pub of_role: Option<String>,
    pub no_recursive: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ListPermissionsStatement {
    pub permissions: Vec<String>,
    pub resource: Option<Resource>,
    pub of_role: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_simple_type() {
        let t = CqlTypeName::Simple("int".into());
        assert_eq!(t.resolve(), Some(cassandra_types::CqlType::Int));
    }

    #[test]
    fn resolve_list_type() {
        let t = CqlTypeName::List(Box::new(CqlTypeName::Simple("text".into())));
        let resolved = t.resolve().unwrap();
        assert!(matches!(resolved, cassandra_types::CqlType::List(_, false)));
    }

    #[test]
    fn resolve_frozen_map() {
        let t = CqlTypeName::Frozen(Box::new(CqlTypeName::Map(
            Box::new(CqlTypeName::Simple("text".into())),
            Box::new(CqlTypeName::Simple("int".into())),
        )));
        let resolved = t.resolve().unwrap();
        assert!(matches!(resolved, cassandra_types::CqlType::Map(_, _, true)));
    }

    #[test]
    fn is_schema_altering() {
        let stmt = Statement::CreateKeyspace(CreateKeyspace {
            name: "ks".into(),
            if_not_exists: false,
            replication: HashMap::new(),
            durable_writes: None,
        });
        assert!(stmt.is_schema_altering());

        let stmt = Statement::Select(Select {
            distinct: false,
            json: false,
            columns: SelectColumns::All,
            keyspace: None,
            table: "t".into(),
            where_clause: vec![],
            order_by: vec![],
            limit: None,
            per_partition_limit: None,
            allow_filtering: false,
        });
        assert!(!stmt.is_schema_altering());
    }
}
