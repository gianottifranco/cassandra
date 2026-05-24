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
    // ── Phase 3 additions ──
    AlterType(AlterType),
    AlterMaterializedView(AlterMaterializedView),
    Describe(DescribeStatement),
    Comment(CommentStatement),
    // ── Accord transactions ──
    Transaction(TransactionStatement),
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
                | Statement::AlterType(_)
                | Statement::AlterMaterializedView(_)
                | Statement::Comment(_)
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommentStatement {
    pub target: CommentTarget,
    pub comment: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommentTarget {
    Keyspace(String),
    Table {
        keyspace: Option<String>,
        table: String,
    },
    Column {
        keyspace: Option<String>,
        table: String,
        column: String,
    },
    Type {
        keyspace: Option<String>,
        name: String,
    },
    Field {
        keyspace: Option<String>,
        type_name: String,
        field: String,
    },
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
    pub masked_with: Option<(String, Vec<Term>)>,
    pub constraints: Vec<ColumnConstraint>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ColumnConstraint {
    Scalar {
        column: String,
        op: ConstraintRelationOp,
        term: String,
    },
    Function {
        name: String,
        args: Vec<String>,
        op: ConstraintRelationOp,
        term: String,
    },
    UnaryFunction {
        name: String,
        args: Vec<String>,
    },
    NotNull,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConstraintRelationOp {
    Eq,
    NotEq,
    Lt,
    Lte,
    Gt,
    Gte,
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
    Vector(Box<CqlTypeName>, u32),
}

impl CqlTypeName {
    /// Resolve to a CqlType from the type registry.
    pub fn resolve(&self) -> Option<cassandra_types::CqlType> {
        use cassandra_types::CqlType;
        match self {
            CqlTypeName::Simple(name) => CqlType::from_cql_name(name),
            CqlTypeName::List(inner) => Some(CqlType::List(Box::new(inner.resolve()?), false)),
            CqlTypeName::Set(inner) => Some(CqlType::Set(Box::new(inner.resolve()?), false)),
            CqlTypeName::Map(k, v) => Some(CqlType::Map(
                Box::new(k.resolve()?),
                Box::new(v.resolve()?),
                false,
            )),
            CqlTypeName::Tuple(types) => {
                let resolved: Option<Vec<_>> = types.iter().map(|t| t.resolve()).collect();
                Some(CqlType::Tuple(resolved?))
            }
            CqlTypeName::Vector(inner, dimensions) => {
                Some(CqlType::Vector(Box::new(inner.resolve()?), *dimensions))
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
    CommentColumn(String, String),
    AlterConstraints(String, Vec<ColumnConstraint>),
    DropConstraints(String),
    MaskColumn(String, String, Vec<Term>), // col, func, args
    DropMask(String),                      // col
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
    pub group_by: Vec<String>,
    pub order_by: Vec<(String, ClusteringOrder)>,
    pub ann_order_by: Option<SelectAnnOrder>,
    pub limit: Option<Term>,
    pub per_partition_limit: Option<Term>,
    pub allow_filtering: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SelectAnnOrder {
    pub column: String,
    pub vector_literal: Vec<f32>,
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
    Cast {
        selector: Box<Selector>,
        target: CqlTypeName,
    },
    Alias {
        selector: Box<Selector>,
        alias: String,
    },
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
    Like,
}

/// A CQL term (value or bind marker).
#[derive(Debug, Clone, PartialEq)]
pub enum Term {
    Literal(Literal),
    BindMarker(BindMarker),
    FunctionCall(String, Vec<Term>),
    TypeHint(CqlTypeName, Box<Term>),
    CollectionElement { key: Box<Term>, value: Box<Term> },
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
    pub json_default: JsonDefault,
    pub using: Vec<UsingClause>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum JsonDefault {
    #[default]
    Null,
    Unset,
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
    pub op: AssignmentOp,
}

#[derive(Debug, Clone, PartialEq)]
pub enum AssignmentOp {
    Set,
    CollectionAppend,
    CollectionPrepend,
    CollectionRemove,
    MapPut { key: Term },
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

// ─── Transaction Statements ─────────────────────────────────────────────

/// LET binding in a transaction.
#[derive(Debug, Clone, PartialEq)]
pub struct LetBinding {
    /// Variable name bound by LET.
    pub name: String,
    /// The SELECT statement that provides the value.
    pub select: Select,
}

/// RETURNING clause for transaction results.
#[derive(Debug, Clone, PartialEq)]
pub struct ReturningClause {
    /// Column references (may include LET variable refs).
    pub columns: Vec<Selector>,
}

/// BEGIN TRANSACTION ... COMMIT TRANSACTION
///
/// ## Java Oracle
/// - `org.apache.cassandra.cql3.statements.TransactionStatement`
///
/// ## Syntax
/// ```text
/// BEGIN TRANSACTION
///   LET <name> = (<select>);
///   ...
///   <insert|update|delete>;
///   ...
/// COMMIT TRANSACTION
///   [RETURNING <selector>, ...]
/// ;
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct TransactionStatement {
    /// LET bindings (read phase).
    pub let_bindings: Vec<LetBinding>,
    /// DML statements (write phase): INSERT, UPDATE, DELETE.
    pub statements: Vec<Statement>,
    /// Optional RETURNING clause.
    pub returning: Option<ReturningClause>,
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

// ─── ALTER TYPE / ALTER MV / DESCRIBE ──────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
pub struct AlterType {
    pub keyspace: Option<String>,
    pub name: String,
    pub operation: AlterTypeOp,
}

#[derive(Debug, Clone, PartialEq)]
pub enum AlterTypeOp {
    AddField(String, CqlTypeName),
    RenameField(String, String),
    AlterFieldType(String, CqlTypeName),
    CommentType(String),
    CommentField(String, String),
}

#[derive(Debug, Clone, PartialEq)]
pub struct AlterMaterializedView {
    pub keyspace: Option<String>,
    pub name: String,
    pub options: HashMap<String, String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DescribeStatement {
    pub target: DescribeTarget,
}

#[derive(Debug, Clone, PartialEq)]
pub enum DescribeTarget {
    Cluster,
    FullSchema,
    Keyspace(String),
    Table(Option<String>, String),
    Type(Option<String>, String),
    Function(Option<String>, String),
    Aggregate(Option<String>, String),
    Generic(String),
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
    pub hashed_password: Option<String>,
    pub superuser: Option<bool>,
    pub login: Option<bool>,
    pub datacenter_access: Option<RoleAccess>,
    pub cidr_access: Option<RoleAccess>,
    pub options: HashMap<String, String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AlterRole {
    pub name: String,
    pub if_exists: bool,
    pub password: Option<String>,
    pub hashed_password: Option<String>,
    pub superuser: Option<bool>,
    pub login: Option<bool>,
    pub datacenter_access: Option<RoleAccess>,
    pub cidr_access: Option<RoleAccess>,
    pub options: HashMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RoleAccess {
    All,
    Restricted(Vec<String>),
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
    Table {
        keyspace: Option<String>,
        table: String,
    },
    AllRoles,
    Role(String),
    AllFunctions,
    FunctionInKeyspace(String),
    Function {
        keyspace: Option<String>,
        name: String,
        arg_types: Vec<CqlTypeName>,
    },
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
        assert!(matches!(
            resolved,
            cassandra_types::CqlType::Map(_, _, true)
        ));
    }

    #[test]
    fn resolve_vector_type() {
        let t = CqlTypeName::Vector(Box::new(CqlTypeName::Simple("float".into())), 3);
        assert_eq!(
            t.resolve(),
            Some(cassandra_types::CqlType::Vector(
                Box::new(cassandra_types::CqlType::Float),
                3
            ))
        );
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
            group_by: vec![],
            order_by: vec![],
            ann_order_by: None,
            limit: None,
            per_partition_limit: None,
            allow_filtering: false,
        });
        assert!(!stmt.is_schema_altering());
    }
}
