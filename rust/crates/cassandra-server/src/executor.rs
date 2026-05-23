// Licensed under Apache License, Version 2.0.

//! Query executor: executes planned CQL queries against the storage engine.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.cql3.QueryProcessor`
//! - `org.apache.cassandra.cql3.statements.*Statement`

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use byteorder::{BigEndian, ByteOrder};
use cassandra_cql::functions::decimal_math::DecimalValue;
use num_bigint::BigInt;
use num_traits::Zero;
use parking_lot::RwLock;
use tracing::{debug, info};

use cassandra_security::Resource as SecurityResource;
use cassandra_security::roles::NetworkPermissions;
use cassandra_security::{Authorizer, Permission, Role, RoleManager, RoleOptions};

use crate::term_binding::typed_term_to_bytes;
use cassandra_cql::ast::{
    AlterTableOp, AlterTypeOp, Assignment, AssignmentOp, ClusteringOrder as AstClusteringOrder,
    ColumnConstraint as AstColumnConstraint, ConstraintRelationOp as AstConstraintRelationOp,
    DescribeTarget, JsonDefault, Literal, Relation, RelationOp, RoleAccess, SelectColumns,
    Selector, Term,
};
use cassandra_cql::functions::{CqlFunction, FunctionRegistry};
use cassandra_cql::parser;
use cassandra_cql::planner::{
    AlterKeyspacePlan, AlterMaterializedViewPlan, AlterRolePlan, AlterTablePlan, AlterTypePlan,
    BatchPlan, CreateAggregatePlan, CreateFunctionPlan, CreateIndexPlan, CreateKeyspacePlan,
    CreateMaterializedViewPlan, CreateRolePlan, CreateTablePlan, CreateTriggerPlan, CreateTypePlan,
    DeletePlan, DescribePlan, DropAggregatePlan, DropFunctionPlan, DropIndexPlan, DropKeyspacePlan,
    DropMaterializedViewPlan, DropRolePlan, DropTablePlan, DropTriggerPlan, DropTypePlan,
    GrantPlan, InsertPlan, ListPermissionsPlan, ListRolesPlan, QueryPlan, RevokePlan, SelectPlan,
    TruncatePlan, UpdatePlan, UsePlan,
};
use cassandra_cql::prepared::PreparedCache;
use cassandra_cql::triggers::{MutationEvent, MutationType, TriggerMutation, TriggerRegistry};
use cassandra_cql::uda::{UdaMetadata, UdaRegistry};
use cassandra_cql::udf::{UdfExecutor, UdfMetadata, UdfRegistry, UdfValue};
use cassandra_cql::udf_wasm::WasmUdfExecutor;
use cassandra_schema::table::TransactionalMode;
use cassandra_schema::{
    ClusteringOrder, ColumnConstraintMetadata, ColumnKind, ColumnMetadata,
    ConstraintRelationOp as SchemaConstraintRelationOp, DroppedColumn, IndexMetadata,
    KeyspaceMetadata, KeyspaceParams, ReplicationParams, SchemaCatalog, TableMetadata, TableParams,
    TriggerDefinition, UserAggregate, UserFunction, UserType, ViewMetadata,
    canonical_cql_type_name,
};
use cassandra_storage::commitlog::{
    CellMutation, Mutation, MutationRow, RangeTombstoneMarker as CommitlogRangeTombstone,
    TombstoneMarker,
};
use cassandra_storage::engine::StorageEngine;
use cassandra_storage::memtable::partition::{Cell, Row};
use cassandra_types::{
    CqlType, codec::CqlValue, native::parse_cql_type, type_compat::is_compatible_with,
};

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
        /// WU-06: Opaque paging state for the next page, if more rows exist.
        paging_state: Option<Vec<u8>>,
        /// WU-06: Server-side warnings to relay to the client.
        warnings: Vec<String>,
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

pub type BatchMutationSink = Arc<
    dyn Fn(cassandra_cql::ast::BatchType, &[Mutation]) -> Result<(), ExecutorError> + Send + Sync,
>;

#[derive(Debug, Clone)]
enum JavaCompatUdfBody {
    Identity(usize),
    Lower(usize),
    Upper(usize),
    Add(usize, usize),
    Subtract(usize, usize),
    Multiply(usize, usize),
    Divide(usize, usize),
    Remainder(usize, usize),
    Compare(JavaCompatComparison, usize, usize),
    Length(usize),
    Trim(usize),
    IsEmpty(usize),
    Contains(usize, usize),
    StartsWith(usize, usize),
    EndsWith(usize, usize),
    Substring(usize, usize),
    SubstringRange(usize, usize, usize),
    IndexOf(usize, usize),
    LastIndexOf(usize, usize),
    Replace(usize, usize, usize),
    ContainsOperands(JavaCompatOperand, JavaCompatOperand),
    StartsWithOperands(JavaCompatOperand, JavaCompatOperand),
    EndsWithOperands(JavaCompatOperand, JavaCompatOperand),
    SubstringOperands(JavaCompatOperand, JavaCompatOperand),
    SubstringRangeOperands(JavaCompatOperand, JavaCompatOperand, JavaCompatOperand),
    IndexOfOperands(JavaCompatOperand, JavaCompatOperand),
    LastIndexOfOperands(JavaCompatOperand, JavaCompatOperand),
    ReplaceOperands(JavaCompatOperand, JavaCompatOperand, JavaCompatOperand),
    Negate(usize),
    Not(usize),
    And(usize, usize),
    Or(usize, usize),
    AddOperands(JavaCompatOperand, JavaCompatOperand),
    SubtractOperands(JavaCompatOperand, JavaCompatOperand),
    MultiplyOperands(JavaCompatOperand, JavaCompatOperand),
    DivideOperands(JavaCompatOperand, JavaCompatOperand),
    RemainderOperands(JavaCompatOperand, JavaCompatOperand),
    CompareOperands(JavaCompatComparison, JavaCompatOperand, JavaCompatOperand),
}

#[derive(Debug, Clone)]
enum JavaCompatOperand {
    Arg(usize),
    Literal(JavaCompatLiteral),
}

#[derive(Debug, Clone)]
enum JavaCompatLiteral {
    Int(i64),
    Float(f64),
    Text(String),
    Boolean(bool),
}

#[derive(Debug, Clone, Copy)]
enum JavaCompatComparison {
    Eq,
    Ne,
    Gt,
    Gte,
    Lt,
    Lte,
}

#[derive(Debug, Clone)]
struct JavaCompatUdfExecutor {
    body: JavaCompatUdfBody,
    return_type: String,
    called_on_null_input: bool,
}

impl JavaCompatUdfExecutor {
    fn new(plan: &CreateFunctionPlan) -> Result<Self, String> {
        let body = parse_java_compat_body(&plan.body, &plan.args)
            .ok_or_else(|| format!("Unsupported Java UDF body: {}", plan.body))?;
        Ok(Self {
            body,
            return_type: plan.return_type.to_ascii_lowercase(),
            called_on_null_input: plan.called_on_null_input,
        })
    }
}

impl UdfExecutor for JavaCompatUdfExecutor {
    fn execute(&self, args: &[UdfValue]) -> Result<UdfValue, String> {
        if !self.called_on_null_input && args.iter().any(|arg| matches!(arg, UdfValue::Null)) {
            return Ok(UdfValue::Null);
        }

        match self.body {
            JavaCompatUdfBody::Identity(index) => {
                Ok(args.get(index).cloned().unwrap_or(UdfValue::Null))
            }
            JavaCompatUdfBody::Lower(index) => {
                map_java_text_arg(args.get(index), |value| value.to_ascii_lowercase())
            }
            JavaCompatUdfBody::Upper(index) => {
                map_java_text_arg(args.get(index), |value| value.to_ascii_uppercase())
            }
            JavaCompatUdfBody::Add(lhs, rhs) => execute_java_add(&self.return_type, args, lhs, rhs),
            JavaCompatUdfBody::Subtract(lhs, rhs) => {
                execute_java_subtract(&self.return_type, args, lhs, rhs)
            }
            JavaCompatUdfBody::Multiply(lhs, rhs) => {
                execute_java_multiply(&self.return_type, args, lhs, rhs)
            }
            JavaCompatUdfBody::Divide(lhs, rhs) => {
                execute_java_divide(&self.return_type, args, lhs, rhs)
            }
            JavaCompatUdfBody::Remainder(lhs, rhs) => {
                execute_java_remainder(&self.return_type, args, lhs, rhs)
            }
            JavaCompatUdfBody::Compare(op, lhs, rhs) => {
                execute_java_compare(&self.return_type, args, lhs, rhs, op)
            }
            JavaCompatUdfBody::Length(index) => execute_java_text_length(args, index),
            JavaCompatUdfBody::Trim(index) => {
                map_java_text_arg(args.get(index), |value| value.trim().to_string())
            }
            JavaCompatUdfBody::IsEmpty(index) => {
                execute_java_text_predicate(args, index, str::is_empty)
            }
            JavaCompatUdfBody::Contains(lhs, rhs) => {
                execute_java_text_binary_predicate(args, lhs, rhs, |value, needle| {
                    value.contains(needle)
                })
            }
            JavaCompatUdfBody::StartsWith(lhs, rhs) => {
                execute_java_text_binary_predicate(args, lhs, rhs, |value, prefix| {
                    value.starts_with(prefix)
                })
            }
            JavaCompatUdfBody::EndsWith(lhs, rhs) => {
                execute_java_text_binary_predicate(args, lhs, rhs, |value, suffix| {
                    value.ends_with(suffix)
                })
            }
            JavaCompatUdfBody::Substring(value, begin) => {
                execute_java_substring(args, value, begin, None)
            }
            JavaCompatUdfBody::SubstringRange(value, begin, end) => {
                execute_java_substring(args, value, begin, Some(end))
            }
            JavaCompatUdfBody::IndexOf(value, needle) => {
                execute_java_text_index(args, value, needle, false)
            }
            JavaCompatUdfBody::LastIndexOf(value, needle) => {
                execute_java_text_index(args, value, needle, true)
            }
            JavaCompatUdfBody::Replace(value, target, replacement) => {
                execute_java_text_replace(args, value, target, replacement)
            }
            JavaCompatUdfBody::ContainsOperands(ref lhs, ref rhs) => {
                execute_java_text_binary_predicate_operands(args, lhs, rhs, |value, needle| {
                    value.contains(needle)
                })
            }
            JavaCompatUdfBody::StartsWithOperands(ref lhs, ref rhs) => {
                execute_java_text_binary_predicate_operands(args, lhs, rhs, |value, prefix| {
                    value.starts_with(prefix)
                })
            }
            JavaCompatUdfBody::EndsWithOperands(ref lhs, ref rhs) => {
                execute_java_text_binary_predicate_operands(args, lhs, rhs, |value, suffix| {
                    value.ends_with(suffix)
                })
            }
            JavaCompatUdfBody::SubstringOperands(ref value, ref begin) => {
                execute_java_substring_operands(args, value, begin, None)
            }
            JavaCompatUdfBody::SubstringRangeOperands(ref value, ref begin, ref end) => {
                execute_java_substring_operands(args, value, begin, Some(end))
            }
            JavaCompatUdfBody::IndexOfOperands(ref value, ref needle) => {
                execute_java_text_index_operands(args, value, needle, false)
            }
            JavaCompatUdfBody::LastIndexOfOperands(ref value, ref needle) => {
                execute_java_text_index_operands(args, value, needle, true)
            }
            JavaCompatUdfBody::ReplaceOperands(ref value, ref target, ref replacement) => {
                execute_java_text_replace_operands(args, value, target, replacement)
            }
            JavaCompatUdfBody::Negate(index) => execute_java_negate(&self.return_type, args, index),
            JavaCompatUdfBody::Not(index) => execute_java_not(&self.return_type, args, index),
            JavaCompatUdfBody::And(lhs, rhs) => {
                execute_java_boolean_binary(&self.return_type, args, lhs, rhs, |lhs, rhs| {
                    lhs && rhs
                })
            }
            JavaCompatUdfBody::Or(lhs, rhs) => {
                execute_java_boolean_binary(&self.return_type, args, lhs, rhs, |lhs, rhs| {
                    lhs || rhs
                })
            }
            JavaCompatUdfBody::AddOperands(ref lhs, ref rhs) => {
                execute_java_add_operands(&self.return_type, args, lhs, rhs)
            }
            JavaCompatUdfBody::SubtractOperands(ref lhs, ref rhs) => {
                execute_java_subtract_operands(&self.return_type, args, lhs, rhs)
            }
            JavaCompatUdfBody::MultiplyOperands(ref lhs, ref rhs) => {
                execute_java_multiply_operands(&self.return_type, args, lhs, rhs)
            }
            JavaCompatUdfBody::DivideOperands(ref lhs, ref rhs) => {
                execute_java_divide_operands(&self.return_type, args, lhs, rhs)
            }
            JavaCompatUdfBody::RemainderOperands(ref lhs, ref rhs) => {
                execute_java_remainder_operands(&self.return_type, args, lhs, rhs)
            }
            JavaCompatUdfBody::CompareOperands(op, ref lhs, ref rhs) => {
                execute_java_compare_operands(&self.return_type, args, lhs, rhs, op)
            }
        }
    }

    fn language(&self) -> &str {
        "java"
    }
}

fn parse_java_compat_body(body: &str, args: &[(String, String)]) -> Option<JavaCompatUdfBody> {
    let raw_expression = body
        .trim()
        .strip_prefix("return ")
        .and_then(|value| value.strip_suffix(';'))?
        .trim();
    let normalized = raw_expression.to_ascii_lowercase();
    let expression = normalized.as_str();

    if let Some(body) = expression.strip_prefix('!') {
        if let Some((lhs, rhs)) = parse_java_equals_call(body.trim(), args) {
            return Some(JavaCompatUdfBody::Compare(
                JavaCompatComparison::Ne,
                lhs,
                rhs,
            ));
        }
        if let Some(raw_body) = raw_expression.strip_prefix('!') {
            if let Some((lhs, rhs)) = parse_java_equals_operands(raw_body.trim(), args) {
                return Some(JavaCompatUdfBody::CompareOperands(
                    JavaCompatComparison::Ne,
                    lhs,
                    rhs,
                ));
            }
        }
        return java_arg_index(body.trim(), args).map(JavaCompatUdfBody::Not);
    }
    if let Some((lhs, rhs)) = parse_java_equals_call(expression, args) {
        return Some(JavaCompatUdfBody::Compare(
            JavaCompatComparison::Eq,
            lhs,
            rhs,
        ));
    }
    if let Some((lhs, rhs)) = parse_java_equals_operands(raw_expression, args) {
        return Some(JavaCompatUdfBody::CompareOperands(
            JavaCompatComparison::Eq,
            lhs,
            rhs,
        ));
    }

    if let Some(arg_name) = expression.strip_suffix(".length()") {
        return java_arg_index(arg_name.trim(), args).map(JavaCompatUdfBody::Length);
    }
    if let Some(arg_name) = expression.strip_suffix(".trim()") {
        return java_arg_index(arg_name.trim(), args).map(JavaCompatUdfBody::Trim);
    }
    if let Some(arg_name) = expression.strip_suffix(".isempty()") {
        return java_arg_index(arg_name.trim(), args).map(JavaCompatUdfBody::IsEmpty);
    }
    if let Some((lhs, rhs)) = parse_java_text_binary_call(expression, ".contains(", args) {
        return Some(JavaCompatUdfBody::Contains(lhs, rhs));
    }
    if let Some((lhs, rhs)) =
        parse_java_text_binary_call_operands(raw_expression, ".contains(", args)
    {
        return Some(JavaCompatUdfBody::ContainsOperands(lhs, rhs));
    }
    if let Some((lhs, rhs)) = parse_java_text_binary_call(expression, ".startswith(", args) {
        return Some(JavaCompatUdfBody::StartsWith(lhs, rhs));
    }
    if let Some((lhs, rhs)) =
        parse_java_text_binary_call_operands(raw_expression, ".startswith(", args)
    {
        return Some(JavaCompatUdfBody::StartsWithOperands(lhs, rhs));
    }
    if let Some((lhs, rhs)) = parse_java_text_binary_call(expression, ".endswith(", args) {
        return Some(JavaCompatUdfBody::EndsWith(lhs, rhs));
    }
    if let Some((lhs, rhs)) =
        parse_java_text_binary_call_operands(raw_expression, ".endswith(", args)
    {
        return Some(JavaCompatUdfBody::EndsWithOperands(lhs, rhs));
    }
    if let Some((value, begin, end)) = parse_java_substring_call(expression, args) {
        return match end {
            Some(end) => Some(JavaCompatUdfBody::SubstringRange(value, begin, end)),
            None => Some(JavaCompatUdfBody::Substring(value, begin)),
        };
    }
    if let Some((value, begin, end)) = parse_java_substring_operands(raw_expression, args) {
        return match end {
            Some(end) => Some(JavaCompatUdfBody::SubstringRangeOperands(value, begin, end)),
            None => Some(JavaCompatUdfBody::SubstringOperands(value, begin)),
        };
    }
    if let Some((lhs, rhs)) = parse_java_text_binary_call(expression, ".indexof(", args) {
        return Some(JavaCompatUdfBody::IndexOf(lhs, rhs));
    }
    if let Some((lhs, rhs)) =
        parse_java_text_binary_call_operands(raw_expression, ".indexof(", args)
    {
        return Some(JavaCompatUdfBody::IndexOfOperands(lhs, rhs));
    }
    if let Some((lhs, rhs)) = parse_java_text_binary_call(expression, ".lastindexof(", args) {
        return Some(JavaCompatUdfBody::LastIndexOf(lhs, rhs));
    }
    if let Some((lhs, rhs)) =
        parse_java_text_binary_call_operands(raw_expression, ".lastindexof(", args)
    {
        return Some(JavaCompatUdfBody::LastIndexOfOperands(lhs, rhs));
    }
    if let Some((value, target, replacement)) = parse_java_replace_call(expression, args) {
        return Some(JavaCompatUdfBody::Replace(value, target, replacement));
    }
    if let Some((value, target, replacement)) = parse_java_replace_operands(raw_expression, args) {
        return Some(JavaCompatUdfBody::ReplaceOperands(
            value,
            target,
            replacement,
        ));
    }

    if let Some((lhs, rhs)) = expression.split_once("&&") {
        let lhs = lhs.trim();
        let rhs = rhs.trim();
        if let (Some(lhs), Some(rhs)) = (java_arg_index(lhs, args), java_arg_index(rhs, args)) {
            return Some(JavaCompatUdfBody::And(lhs, rhs));
        }
        return None;
    }

    if let Some((lhs, rhs)) = expression.split_once("||") {
        let lhs = lhs.trim();
        let rhs = rhs.trim();
        if let (Some(lhs), Some(rhs)) = (java_arg_index(lhs, args), java_arg_index(rhs, args)) {
            return Some(JavaCompatUdfBody::Or(lhs, rhs));
        }
        return None;
    }

    for (operator, comparison) in [
        (">=", JavaCompatComparison::Gte),
        ("<=", JavaCompatComparison::Lte),
        ("==", JavaCompatComparison::Eq),
        ("!=", JavaCompatComparison::Ne),
        (">", JavaCompatComparison::Gt),
        ("<", JavaCompatComparison::Lt),
    ] {
        if let Some((lhs, rhs)) = expression.split_once(operator) {
            let lhs = lhs.trim();
            let rhs = rhs.trim();
            if let (Some(lhs), Some(rhs)) = (java_arg_index(lhs, args), java_arg_index(rhs, args)) {
                return Some(JavaCompatUdfBody::Compare(comparison, lhs, rhs));
            }
            if let Some((lhs, rhs)) = parse_java_binary_operands_str(raw_expression, operator, args)
            {
                return Some(JavaCompatUdfBody::CompareOperands(comparison, lhs, rhs));
            }
            return None;
        }
    }

    if let Some(arg_name) = expression.strip_prefix('-') {
        return java_arg_index(arg_name.trim(), args).map(JavaCompatUdfBody::Negate);
    }

    if let Some((lhs, rhs)) = expression.split_once('+') {
        let lhs = lhs.trim();
        let rhs = rhs.trim();
        if let (Some(lhs), Some(rhs)) = (java_arg_index(lhs, args), java_arg_index(rhs, args)) {
            return Some(JavaCompatUdfBody::Add(lhs, rhs));
        }
        if let Some((lhs, rhs)) = parse_java_binary_operands(raw_expression, '+', args) {
            return Some(JavaCompatUdfBody::AddOperands(lhs, rhs));
        }
        return None;
    }

    if let Some((lhs, rhs)) = expression.split_once('-') {
        let lhs = lhs.trim();
        let rhs = rhs.trim();
        if let (Some(lhs), Some(rhs)) = (java_arg_index(lhs, args), java_arg_index(rhs, args)) {
            return Some(JavaCompatUdfBody::Subtract(lhs, rhs));
        }
        if let Some((lhs, rhs)) = parse_java_binary_operands(raw_expression, '-', args) {
            return Some(JavaCompatUdfBody::SubtractOperands(lhs, rhs));
        }
        return None;
    }

    if let Some((lhs, rhs)) = expression.split_once('*') {
        let lhs = lhs.trim();
        let rhs = rhs.trim();
        if let (Some(lhs), Some(rhs)) = (java_arg_index(lhs, args), java_arg_index(rhs, args)) {
            return Some(JavaCompatUdfBody::Multiply(lhs, rhs));
        }
        if let Some((lhs, rhs)) = parse_java_binary_operands(raw_expression, '*', args) {
            return Some(JavaCompatUdfBody::MultiplyOperands(lhs, rhs));
        }
        return None;
    }

    if let Some((lhs, rhs)) = expression.split_once('/') {
        let lhs = lhs.trim();
        let rhs = rhs.trim();
        if let (Some(lhs), Some(rhs)) = (java_arg_index(lhs, args), java_arg_index(rhs, args)) {
            return Some(JavaCompatUdfBody::Divide(lhs, rhs));
        }
        if let Some((lhs, rhs)) = parse_java_binary_operands(raw_expression, '/', args) {
            return Some(JavaCompatUdfBody::DivideOperands(lhs, rhs));
        }
        return None;
    }

    if let Some((lhs, rhs)) = expression.split_once('%') {
        let lhs = lhs.trim();
        let rhs = rhs.trim();
        if let (Some(lhs), Some(rhs)) = (java_arg_index(lhs, args), java_arg_index(rhs, args)) {
            return Some(JavaCompatUdfBody::Remainder(lhs, rhs));
        }
        if let Some((lhs, rhs)) = parse_java_binary_operands(raw_expression, '%', args) {
            return Some(JavaCompatUdfBody::RemainderOperands(lhs, rhs));
        }
        return None;
    }

    if let Some(arg_name) = expression.strip_suffix(".tolowercase()") {
        return java_arg_index(arg_name.trim(), args).map(JavaCompatUdfBody::Lower);
    }
    if let Some(arg_name) = expression.strip_suffix(".touppercase()") {
        return java_arg_index(arg_name.trim(), args).map(JavaCompatUdfBody::Upper);
    }

    java_arg_index(expression, args).map(JavaCompatUdfBody::Identity)
}

fn parse_java_equals_call(expression: &str, args: &[(String, String)]) -> Option<(usize, usize)> {
    let (lhs, rhs) = expression.split_once(".equals(")?;
    let rhs = rhs.strip_suffix(')')?.trim();
    Some((
        java_arg_index(lhs.trim(), args)?,
        java_arg_index(rhs, args)?,
    ))
}

fn parse_java_equals_operands(
    expression: &str,
    args: &[(String, String)],
) -> Option<(JavaCompatOperand, JavaCompatOperand)> {
    let normalized = expression.to_ascii_lowercase();
    let method_index = normalized.find(".equals(")?;
    let lhs = parse_java_operand(expression[..method_index].trim(), args)?;
    let rhs_start = method_index + ".equals(".len();
    let rhs = expression[rhs_start..].strip_suffix(')')?.trim();
    let rhs = parse_java_operand(rhs, args)?;
    java_non_arg_arg_operands(lhs, rhs)
}

fn parse_java_text_binary_call(
    expression: &str,
    method: &str,
    args: &[(String, String)],
) -> Option<(usize, usize)> {
    let (lhs, rhs) = expression.split_once(method)?;
    let rhs = rhs.strip_suffix(')')?.trim();
    Some((
        java_arg_index(lhs.trim(), args)?,
        java_arg_index(rhs, args)?,
    ))
}

fn parse_java_text_binary_call_operands(
    expression: &str,
    method: &str,
    args: &[(String, String)],
) -> Option<(JavaCompatOperand, JavaCompatOperand)> {
    let normalized = expression.to_ascii_lowercase();
    let method_index = normalized.find(method)?;
    let lhs = parse_java_operand(expression[..method_index].trim(), args)?;
    let rhs_start = method_index + method.len();
    let rhs = expression[rhs_start..].strip_suffix(')')?.trim();
    let rhs = parse_java_operand(rhs, args)?;
    java_non_arg_arg_operands(lhs, rhs)
}

fn parse_java_substring_call(
    expression: &str,
    args: &[(String, String)],
) -> Option<(usize, usize, Option<usize>)> {
    let (value, params) = expression.split_once(".substring(")?;
    let params = params.strip_suffix(')')?;
    let mut params = params.split(',').map(str::trim);
    let begin = params.next()?;
    let end = params.next();
    if params.next().is_some() {
        return None;
    }
    let end = match end {
        Some(end) => Some(java_arg_index(end, args)?),
        None => None,
    };
    Some((
        java_arg_index(value.trim(), args)?,
        java_arg_index(begin, args)?,
        end,
    ))
}

fn parse_java_substring_operands(
    expression: &str,
    args: &[(String, String)],
) -> Option<(
    JavaCompatOperand,
    JavaCompatOperand,
    Option<JavaCompatOperand>,
)> {
    let normalized = expression.to_ascii_lowercase();
    let method = ".substring(";
    let method_index = normalized.find(method)?;
    let value = parse_java_operand(expression[..method_index].trim(), args)?;
    let params_start = method_index + method.len();
    let params = expression[params_start..].strip_suffix(')')?;
    let mut params = params.split(',').map(str::trim);
    let begin = parse_java_operand(params.next()?, args)?;
    let end = match params.next() {
        Some(end) => Some(parse_java_operand(end, args)?),
        None => None,
    };
    if params.next().is_some() {
        return None;
    }
    if matches!(
        (&value, &begin, &end),
        (JavaCompatOperand::Arg(_), JavaCompatOperand::Arg(_), None)
    ) || matches!(
        (&value, &begin, &end),
        (
            JavaCompatOperand::Arg(_),
            JavaCompatOperand::Arg(_),
            Some(JavaCompatOperand::Arg(_))
        )
    ) {
        return None;
    }
    Some((value, begin, end))
}

fn parse_java_replace_call(
    expression: &str,
    args: &[(String, String)],
) -> Option<(usize, usize, usize)> {
    let (value, params) = expression.split_once(".replace(")?;
    let params = params.strip_suffix(')')?;
    let mut params = params.split(',').map(str::trim);
    let target = params.next()?;
    let replacement = params.next()?;
    if params.next().is_some() {
        return None;
    }
    Some((
        java_arg_index(value.trim(), args)?,
        java_arg_index(target, args)?,
        java_arg_index(replacement, args)?,
    ))
}

fn parse_java_replace_operands(
    expression: &str,
    args: &[(String, String)],
) -> Option<(JavaCompatOperand, JavaCompatOperand, JavaCompatOperand)> {
    let normalized = expression.to_ascii_lowercase();
    let method = ".replace(";
    let method_index = normalized.find(method)?;
    let value = parse_java_operand(expression[..method_index].trim(), args)?;
    let params_start = method_index + method.len();
    let params = expression[params_start..].strip_suffix(')')?;
    let mut params = params.split(',').map(str::trim);
    let target = parse_java_operand(params.next()?, args)?;
    let replacement = parse_java_operand(params.next()?, args)?;
    if params.next().is_some() {
        return None;
    }
    if matches!(
        (&value, &target, &replacement),
        (
            JavaCompatOperand::Arg(_),
            JavaCompatOperand::Arg(_),
            JavaCompatOperand::Arg(_)
        )
    ) {
        return None;
    }
    Some((value, target, replacement))
}

fn parse_java_binary_operands(
    expression: &str,
    operator: char,
    args: &[(String, String)],
) -> Option<(JavaCompatOperand, JavaCompatOperand)> {
    parse_java_binary_operands_str(expression, &operator.to_string(), args)
}

fn parse_java_binary_operands_str(
    expression: &str,
    operator: &str,
    args: &[(String, String)],
) -> Option<(JavaCompatOperand, JavaCompatOperand)> {
    let (lhs, rhs) = expression.split_once(operator)?;
    let lhs = parse_java_operand(lhs.trim(), args)?;
    let rhs = parse_java_operand(rhs.trim(), args)?;
    java_non_arg_arg_operands(lhs, rhs)
}

fn java_non_arg_arg_operands(
    lhs: JavaCompatOperand,
    rhs: JavaCompatOperand,
) -> Option<(JavaCompatOperand, JavaCompatOperand)> {
    match (&lhs, &rhs) {
        (JavaCompatOperand::Arg(_), JavaCompatOperand::Arg(_)) => None,
        _ => Some((lhs, rhs)),
    }
}

fn parse_java_operand(token: &str, args: &[(String, String)]) -> Option<JavaCompatOperand> {
    if let Some(index) = java_arg_index(token, args) {
        return Some(JavaCompatOperand::Arg(index));
    }
    parse_java_literal(token).map(JavaCompatOperand::Literal)
}

fn parse_java_literal(token: &str) -> Option<JavaCompatLiteral> {
    let token = token.trim();
    if token.len() >= 2 && token.starts_with('"') && token.ends_with('"') {
        return Some(JavaCompatLiteral::Text(
            token[1..token.len() - 1].to_string(),
        ));
    }
    if token.eq_ignore_ascii_case("true") {
        return Some(JavaCompatLiteral::Boolean(true));
    }
    if token.eq_ignore_ascii_case("false") {
        return Some(JavaCompatLiteral::Boolean(false));
    }
    if token.contains('.') {
        return token.parse::<f64>().ok().map(JavaCompatLiteral::Float);
    }
    token.parse::<i64>().ok().map(JavaCompatLiteral::Int)
}

fn java_arg_index(name: &str, args: &[(String, String)]) -> Option<usize> {
    args.iter()
        .position(|(arg_name, _)| arg_name.eq_ignore_ascii_case(name))
}

fn map_java_text_arg(
    value: Option<&UdfValue>,
    f: impl FnOnce(&str) -> String,
) -> Result<UdfValue, String> {
    match value {
        Some(UdfValue::Text(value)) => Ok(UdfValue::Text(f(value))),
        Some(UdfValue::Null) | None => Ok(UdfValue::Null),
        Some(other) => Err(format!("Expected text argument, got {other:?}")),
    }
}

fn execute_java_text_length(args: &[UdfValue], index: usize) -> Result<UdfValue, String> {
    match args.get(index) {
        Some(UdfValue::Text(value)) => Ok(UdfValue::Int(value.chars().count() as i32)),
        Some(UdfValue::Null) | None => Ok(UdfValue::Null),
        Some(other) => Err(format!("Expected text argument, got {other:?}")),
    }
}

fn execute_java_text_predicate(
    args: &[UdfValue],
    index: usize,
    predicate: impl FnOnce(&str) -> bool,
) -> Result<UdfValue, String> {
    match args.get(index) {
        Some(UdfValue::Text(value)) => Ok(UdfValue::Boolean(predicate(value))),
        Some(UdfValue::Null) | None => Ok(UdfValue::Null),
        Some(other) => Err(format!("Expected text argument, got {other:?}")),
    }
}

fn execute_java_text_binary_predicate(
    args: &[UdfValue],
    lhs: usize,
    rhs: usize,
    predicate: impl FnOnce(&str, &str) -> bool,
) -> Result<UdfValue, String> {
    let (lhs, rhs) = java_binary_args(args, lhs, rhs)?;
    execute_java_text_binary_predicate_values(lhs, rhs, predicate)
}

fn execute_java_text_binary_predicate_operands(
    args: &[UdfValue],
    lhs: &JavaCompatOperand,
    rhs: &JavaCompatOperand,
    predicate: impl FnOnce(&str, &str) -> bool,
) -> Result<UdfValue, String> {
    let lhs = java_runtime_operand_value(args, lhs);
    let rhs = java_runtime_operand_value(args, rhs);
    execute_java_text_binary_predicate_values(&lhs, &rhs, predicate)
}

fn execute_java_text_binary_predicate_values(
    lhs: &UdfValue,
    rhs: &UdfValue,
    predicate: impl FnOnce(&str, &str) -> bool,
) -> Result<UdfValue, String> {
    match (lhs, rhs) {
        (UdfValue::Text(lhs), UdfValue::Text(rhs)) => Ok(UdfValue::Boolean(predicate(lhs, rhs))),
        (UdfValue::Null, _) | (_, UdfValue::Null) => Ok(UdfValue::Null),
        (lhs, rhs) => Err(format!("Expected text arguments, got {lhs:?} and {rhs:?}")),
    }
}

fn execute_java_substring(
    args: &[UdfValue],
    value_index: usize,
    begin_index: usize,
    end_index: Option<usize>,
) -> Result<UdfValue, String> {
    let Some(value) = args.get(value_index) else {
        return Ok(UdfValue::Null);
    };
    let Some(begin) = args.get(begin_index) else {
        return Ok(UdfValue::Null);
    };
    let end = end_index.and_then(|index| args.get(index));
    execute_java_substring_values(value, begin, end)
}

fn execute_java_substring_operands(
    args: &[UdfValue],
    value: &JavaCompatOperand,
    begin: &JavaCompatOperand,
    end: Option<&JavaCompatOperand>,
) -> Result<UdfValue, String> {
    let value = java_runtime_operand_value(args, value);
    let begin = java_runtime_operand_value(args, begin);
    let end = end.map(|end| java_runtime_operand_value(args, end));
    execute_java_substring_values(&value, &begin, end.as_ref())
}

fn execute_java_substring_values(
    value: &UdfValue,
    begin: &UdfValue,
    end: Option<&UdfValue>,
) -> Result<UdfValue, String> {
    match (value, begin, end) {
        (UdfValue::Text(_), UdfValue::Null, _)
        | (UdfValue::Text(_), _, Some(UdfValue::Null))
        | (UdfValue::Null, _, _) => Ok(UdfValue::Null),
        (UdfValue::Text(value), begin, None) => {
            let begin = java_index_value(begin)
                .ok_or_else(|| format!("Expected int substring begin argument, got {begin:?}"))?;
            java_substring_chars(value, begin, value.chars().count() as isize)
        }
        (UdfValue::Text(value), begin, Some(end)) => {
            let begin = java_index_value(begin)
                .ok_or_else(|| format!("Expected int substring begin argument, got {begin:?}"))?;
            let end = java_index_value(end)
                .ok_or_else(|| format!("Expected int substring end argument, got {end:?}"))?;
            java_substring_chars(value, begin, end)
        }
        (value, begin, end) => Err(format!(
            "Expected text and int substring arguments, got {value:?}, {begin:?}, {end:?}"
        )),
    }
}

fn java_substring_chars(value: &str, begin: isize, end: isize) -> Result<UdfValue, String> {
    let len = value.chars().count() as isize;
    if begin < 0 || end < begin || end > len {
        return Err(format!(
            "Substring bounds out of range: begin={begin}, end={end}, length={len}"
        ));
    }
    let begin = begin as usize;
    let len = (end - begin as isize) as usize;
    Ok(UdfValue::Text(
        value.chars().skip(begin).take(len).collect(),
    ))
}

fn execute_java_text_index(
    args: &[UdfValue],
    value_index: usize,
    needle_index: usize,
    last: bool,
) -> Result<UdfValue, String> {
    let (value, needle) = java_binary_args(args, value_index, needle_index)?;
    execute_java_text_index_values(value, needle, last)
}

fn execute_java_text_index_operands(
    args: &[UdfValue],
    value: &JavaCompatOperand,
    needle: &JavaCompatOperand,
    last: bool,
) -> Result<UdfValue, String> {
    let value = java_runtime_operand_value(args, value);
    let needle = java_runtime_operand_value(args, needle);
    execute_java_text_index_values(&value, &needle, last)
}

fn execute_java_text_index_values(
    value: &UdfValue,
    needle: &UdfValue,
    last: bool,
) -> Result<UdfValue, String> {
    match (value, needle) {
        (UdfValue::Text(value), UdfValue::Text(needle)) => {
            let byte_index = if last {
                value.rfind(needle)
            } else {
                value.find(needle)
            };
            let char_index = byte_index
                .map(|idx| value[..idx].chars().count() as i32)
                .unwrap_or(-1);
            Ok(UdfValue::Int(char_index))
        }
        (UdfValue::Null, _) | (_, UdfValue::Null) => Ok(UdfValue::Null),
        (value, needle) => Err(format!(
            "Expected text index arguments, got {value:?} and {needle:?}"
        )),
    }
}

fn execute_java_text_replace(
    args: &[UdfValue],
    value_index: usize,
    target_index: usize,
    replacement_index: usize,
) -> Result<UdfValue, String> {
    let Some(value) = args.get(value_index) else {
        return Ok(UdfValue::Null);
    };
    let Some(target) = args.get(target_index) else {
        return Ok(UdfValue::Null);
    };
    let Some(replacement) = args.get(replacement_index) else {
        return Ok(UdfValue::Null);
    };
    execute_java_text_replace_values(value, target, replacement)
}

fn execute_java_text_replace_operands(
    args: &[UdfValue],
    value: &JavaCompatOperand,
    target: &JavaCompatOperand,
    replacement: &JavaCompatOperand,
) -> Result<UdfValue, String> {
    let value = java_runtime_operand_value(args, value);
    let target = java_runtime_operand_value(args, target);
    let replacement = java_runtime_operand_value(args, replacement);
    execute_java_text_replace_values(&value, &target, &replacement)
}

fn execute_java_text_replace_values(
    value: &UdfValue,
    target: &UdfValue,
    replacement: &UdfValue,
) -> Result<UdfValue, String> {
    match (value, target, replacement) {
        (UdfValue::Text(value), UdfValue::Text(target), UdfValue::Text(replacement)) => {
            Ok(UdfValue::Text(value.replace(target, replacement)))
        }
        (UdfValue::Null, _, _) | (_, UdfValue::Null, _) | (_, _, UdfValue::Null) => {
            Ok(UdfValue::Null)
        }
        (value, target, replacement) => Err(format!(
            "Expected text replace arguments, got {value:?}, {target:?}, {replacement:?}"
        )),
    }
}

fn java_runtime_operand_value(args: &[UdfValue], operand: &JavaCompatOperand) -> UdfValue {
    match operand {
        JavaCompatOperand::Arg(index) => args.get(*index).cloned().unwrap_or(UdfValue::Null),
        JavaCompatOperand::Literal(literal) => java_literal_comparison_value(literal),
    }
}

fn java_index_value(value: &UdfValue) -> Option<isize> {
    match value {
        UdfValue::Tinyint(value) => Some(*value as isize),
        UdfValue::Smallint(value) => Some(*value as isize),
        UdfValue::Int(value) => isize::try_from(*value).ok(),
        UdfValue::Long(value) => isize::try_from(*value).ok(),
        _ => None,
    }
}

fn execute_java_negate(
    return_type: &str,
    args: &[UdfValue],
    index: usize,
) -> Result<UdfValue, String> {
    let Some(value) = args.get(index) else {
        return Ok(UdfValue::Null);
    };
    match (return_type, value) {
        (_, UdfValue::Null) => Ok(UdfValue::Null),
        ("tinyint", UdfValue::Tinyint(value)) => Ok(UdfValue::Tinyint(value.saturating_neg())),
        ("smallint", UdfValue::Smallint(value)) => Ok(UdfValue::Smallint(value.saturating_neg())),
        ("int", UdfValue::Int(value)) => Ok(UdfValue::Int(value.saturating_neg())),
        ("bigint", UdfValue::Long(value)) => Ok(UdfValue::Long(value.saturating_neg())),
        ("counter", UdfValue::Counter(value)) => Ok(UdfValue::Counter(value.saturating_neg())),
        ("float", UdfValue::Float(value)) => Ok(UdfValue::Float(-*value)),
        ("double", UdfValue::Double(value)) => Ok(UdfValue::Double(-*value)),
        (_, value) => Err(format!(
            "Java unary negation UDF is not supported for return type {return_type} and argument {value:?}"
        )),
    }
}

fn execute_java_not(
    return_type: &str,
    args: &[UdfValue],
    index: usize,
) -> Result<UdfValue, String> {
    if return_type != "boolean" {
        return Err(format!(
            "Java boolean NOT UDF must return boolean, got {return_type}"
        ));
    }
    match args.get(index) {
        Some(UdfValue::Boolean(value)) => Ok(UdfValue::Boolean(!value)),
        Some(UdfValue::Null) | None => Ok(UdfValue::Null),
        Some(value) => Err(format!("Expected boolean argument, got {value:?}")),
    }
}

fn execute_java_boolean_binary(
    return_type: &str,
    args: &[UdfValue],
    lhs: usize,
    rhs: usize,
    op: impl FnOnce(bool, bool) -> bool,
) -> Result<UdfValue, String> {
    if return_type != "boolean" {
        return Err(format!(
            "Java boolean binary UDF must return boolean, got {return_type}"
        ));
    }
    let (lhs, rhs) = java_binary_args(args, lhs, rhs)?;
    match (lhs, rhs) {
        (UdfValue::Boolean(lhs), UdfValue::Boolean(rhs)) => Ok(UdfValue::Boolean(op(*lhs, *rhs))),
        (UdfValue::Null, _) | (_, UdfValue::Null) => Ok(UdfValue::Null),
        (lhs, rhs) => Err(format!(
            "Expected boolean arguments, got {lhs:?} and {rhs:?}"
        )),
    }
}

fn java_binary_args(
    args: &[UdfValue],
    lhs: usize,
    rhs: usize,
) -> Result<(&UdfValue, &UdfValue), String> {
    let lhs = args
        .get(lhs)
        .ok_or_else(|| "Missing left Java UDF argument".to_string())?;
    let rhs = args
        .get(rhs)
        .ok_or_else(|| "Missing right Java UDF argument".to_string())?;
    Ok((lhs, rhs))
}

fn execute_java_add_operands(
    return_type: &str,
    args: &[UdfValue],
    lhs: &JavaCompatOperand,
    rhs: &JavaCompatOperand,
) -> Result<UdfValue, String> {
    let lhs = java_operand_value(return_type, args, lhs)?;
    let rhs = java_operand_value(return_type, args, rhs)?;
    execute_java_add_values(return_type, &lhs, &rhs)
}

fn java_operand_value(
    return_type: &str,
    args: &[UdfValue],
    operand: &JavaCompatOperand,
) -> Result<UdfValue, String> {
    match operand {
        JavaCompatOperand::Arg(index) => Ok(args.get(*index).cloned().unwrap_or(UdfValue::Null)),
        JavaCompatOperand::Literal(literal) => java_literal_value(return_type, literal),
    }
}

fn java_literal_value(return_type: &str, literal: &JavaCompatLiteral) -> Result<UdfValue, String> {
    match literal {
        JavaCompatLiteral::Int(value) => match return_type {
            "tinyint" => i8::try_from(*value)
                .map(UdfValue::Tinyint)
                .map_err(|_| format!("Integer literal {value} is out of range for tinyint")),
            "smallint" => i16::try_from(*value)
                .map(UdfValue::Smallint)
                .map_err(|_| format!("Integer literal {value} is out of range for smallint")),
            "int" => i32::try_from(*value)
                .map(UdfValue::Int)
                .map_err(|_| format!("Integer literal {value} is out of range for int")),
            "bigint" => Ok(UdfValue::Long(*value)),
            "counter" => Ok(UdfValue::Counter(*value)),
            "float" => Ok(UdfValue::Float(*value as f32)),
            "double" => Ok(UdfValue::Double(*value as f64)),
            _ => Ok(UdfValue::Text(value.to_string())),
        },
        JavaCompatLiteral::Float(value) => match return_type {
            "float" => Ok(UdfValue::Float(*value as f32)),
            "double" => Ok(UdfValue::Double(*value)),
            _ => Ok(UdfValue::Text(value.to_string())),
        },
        JavaCompatLiteral::Text(value) => Ok(UdfValue::Text(value.clone())),
        JavaCompatLiteral::Boolean(value) => Ok(UdfValue::Boolean(*value)),
    }
}

fn java_literal_comparison_value(literal: &JavaCompatLiteral) -> UdfValue {
    match literal {
        JavaCompatLiteral::Int(value) => UdfValue::Long(*value),
        JavaCompatLiteral::Float(value) => UdfValue::Double(*value),
        JavaCompatLiteral::Text(value) => UdfValue::Text(value.clone()),
        JavaCompatLiteral::Boolean(value) => UdfValue::Boolean(*value),
    }
}

fn execute_java_add(
    return_type: &str,
    args: &[UdfValue],
    lhs: usize,
    rhs: usize,
) -> Result<UdfValue, String> {
    let (lhs, rhs) = java_binary_args(args, lhs, rhs)?;
    execute_java_add_values(return_type, lhs, rhs)
}

fn execute_java_add_values(
    return_type: &str,
    lhs: &UdfValue,
    rhs: &UdfValue,
) -> Result<UdfValue, String> {
    match return_type {
        "tinyint" => match (lhs, rhs) {
            (UdfValue::Tinyint(lhs), UdfValue::Tinyint(rhs)) => {
                Ok(UdfValue::Tinyint(lhs.saturating_add(*rhs)))
            }
            (UdfValue::Null, _) | (_, UdfValue::Null) => Ok(UdfValue::Null),
            (lhs, rhs) => Err(format!(
                "Expected tinyint arguments, got {lhs:?} and {rhs:?}"
            )),
        },
        "smallint" => match (lhs, rhs) {
            (UdfValue::Smallint(lhs), UdfValue::Smallint(rhs)) => {
                Ok(UdfValue::Smallint(lhs.saturating_add(*rhs)))
            }
            (UdfValue::Null, _) | (_, UdfValue::Null) => Ok(UdfValue::Null),
            (lhs, rhs) => Err(format!(
                "Expected smallint arguments, got {lhs:?} and {rhs:?}"
            )),
        },
        "int" => match (lhs, rhs) {
            (UdfValue::Int(lhs), UdfValue::Int(rhs)) => Ok(UdfValue::Int(lhs.saturating_add(*rhs))),
            (UdfValue::Null, _) | (_, UdfValue::Null) => Ok(UdfValue::Null),
            (lhs, rhs) => Err(format!("Expected int arguments, got {lhs:?} and {rhs:?}")),
        },
        "bigint" => match (lhs, rhs) {
            (UdfValue::Long(lhs), UdfValue::Long(rhs)) => {
                Ok(UdfValue::Long(lhs.saturating_add(*rhs)))
            }
            (UdfValue::Null, _) | (_, UdfValue::Null) => Ok(UdfValue::Null),
            (lhs, rhs) => Err(format!(
                "Expected bigint arguments, got {lhs:?} and {rhs:?}"
            )),
        },
        "counter" => match (lhs, rhs) {
            (UdfValue::Counter(lhs), UdfValue::Counter(rhs)) => {
                Ok(UdfValue::Counter(lhs.saturating_add(*rhs)))
            }
            (UdfValue::Null, _) | (_, UdfValue::Null) => Ok(UdfValue::Null),
            (lhs, rhs) => Err(format!(
                "Expected counter arguments, got {lhs:?} and {rhs:?}"
            )),
        },
        "float" => match (lhs, rhs) {
            (UdfValue::Float(lhs), UdfValue::Float(rhs)) => Ok(UdfValue::Float(*lhs + *rhs)),
            (UdfValue::Null, _) | (_, UdfValue::Null) => Ok(UdfValue::Null),
            (lhs, rhs) => Err(format!("Expected float arguments, got {lhs:?} and {rhs:?}")),
        },
        "double" => match (lhs, rhs) {
            (UdfValue::Double(lhs), UdfValue::Double(rhs)) => Ok(UdfValue::Double(*lhs + *rhs)),
            (UdfValue::Null, _) | (_, UdfValue::Null) => Ok(UdfValue::Null),
            (lhs, rhs) => Err(format!(
                "Expected double arguments, got {lhs:?} and {rhs:?}"
            )),
        },
        "blob" => match (lhs, rhs) {
            (UdfValue::Blob(lhs), UdfValue::Blob(rhs)) => {
                let mut out = lhs.clone();
                out.extend(rhs);
                Ok(UdfValue::Blob(out))
            }
            (UdfValue::Null, _) | (_, UdfValue::Null) => Ok(UdfValue::Null),
            (lhs, rhs) => Err(format!("Expected blob arguments, got {lhs:?} and {rhs:?}")),
        },
        _ => match (lhs, rhs) {
            (UdfValue::Text(lhs), UdfValue::Text(rhs)) => Ok(UdfValue::Text(format!("{lhs}{rhs}"))),
            (UdfValue::Null, _) | (_, UdfValue::Null) => Ok(UdfValue::Null),
            (lhs, rhs) => Err(format!("Expected text arguments, got {lhs:?} and {rhs:?}")),
        },
    }
}

fn execute_java_subtract(
    return_type: &str,
    args: &[UdfValue],
    lhs: usize,
    rhs: usize,
) -> Result<UdfValue, String> {
    let (lhs, rhs) = java_binary_args(args, lhs, rhs)?;
    execute_java_subtract_values(return_type, lhs, rhs)
}

fn execute_java_subtract_operands(
    return_type: &str,
    args: &[UdfValue],
    lhs: &JavaCompatOperand,
    rhs: &JavaCompatOperand,
) -> Result<UdfValue, String> {
    let lhs = java_operand_value(return_type, args, lhs)?;
    let rhs = java_operand_value(return_type, args, rhs)?;
    execute_java_subtract_values(return_type, &lhs, &rhs)
}

fn execute_java_subtract_values(
    return_type: &str,
    lhs: &UdfValue,
    rhs: &UdfValue,
) -> Result<UdfValue, String> {
    match return_type {
        "tinyint" => match (lhs, rhs) {
            (UdfValue::Tinyint(lhs), UdfValue::Tinyint(rhs)) => {
                Ok(UdfValue::Tinyint(lhs.saturating_sub(*rhs)))
            }
            (UdfValue::Null, _) | (_, UdfValue::Null) => Ok(UdfValue::Null),
            (lhs, rhs) => Err(format!(
                "Expected tinyint arguments, got {lhs:?} and {rhs:?}"
            )),
        },
        "smallint" => match (lhs, rhs) {
            (UdfValue::Smallint(lhs), UdfValue::Smallint(rhs)) => {
                Ok(UdfValue::Smallint(lhs.saturating_sub(*rhs)))
            }
            (UdfValue::Null, _) | (_, UdfValue::Null) => Ok(UdfValue::Null),
            (lhs, rhs) => Err(format!(
                "Expected smallint arguments, got {lhs:?} and {rhs:?}"
            )),
        },
        "int" => match (lhs, rhs) {
            (UdfValue::Int(lhs), UdfValue::Int(rhs)) => Ok(UdfValue::Int(lhs.saturating_sub(*rhs))),
            (UdfValue::Null, _) | (_, UdfValue::Null) => Ok(UdfValue::Null),
            (lhs, rhs) => Err(format!("Expected int arguments, got {lhs:?} and {rhs:?}")),
        },
        "bigint" => match (lhs, rhs) {
            (UdfValue::Long(lhs), UdfValue::Long(rhs)) => {
                Ok(UdfValue::Long(lhs.saturating_sub(*rhs)))
            }
            (UdfValue::Null, _) | (_, UdfValue::Null) => Ok(UdfValue::Null),
            (lhs, rhs) => Err(format!(
                "Expected bigint arguments, got {lhs:?} and {rhs:?}"
            )),
        },
        "counter" => match (lhs, rhs) {
            (UdfValue::Counter(lhs), UdfValue::Counter(rhs)) => {
                Ok(UdfValue::Counter(lhs.saturating_sub(*rhs)))
            }
            (UdfValue::Null, _) | (_, UdfValue::Null) => Ok(UdfValue::Null),
            (lhs, rhs) => Err(format!(
                "Expected counter arguments, got {lhs:?} and {rhs:?}"
            )),
        },
        "float" => match (lhs, rhs) {
            (UdfValue::Float(lhs), UdfValue::Float(rhs)) => Ok(UdfValue::Float(*lhs - *rhs)),
            (UdfValue::Null, _) | (_, UdfValue::Null) => Ok(UdfValue::Null),
            (lhs, rhs) => Err(format!("Expected float arguments, got {lhs:?} and {rhs:?}")),
        },
        "double" => match (lhs, rhs) {
            (UdfValue::Double(lhs), UdfValue::Double(rhs)) => Ok(UdfValue::Double(*lhs - *rhs)),
            (UdfValue::Null, _) | (_, UdfValue::Null) => Ok(UdfValue::Null),
            (lhs, rhs) => Err(format!(
                "Expected double arguments, got {lhs:?} and {rhs:?}"
            )),
        },
        _ => Err(format!(
            "Java subtraction UDF is not supported for return type {return_type}"
        )),
    }
}

fn execute_java_multiply(
    return_type: &str,
    args: &[UdfValue],
    lhs: usize,
    rhs: usize,
) -> Result<UdfValue, String> {
    let (lhs, rhs) = java_binary_args(args, lhs, rhs)?;
    execute_java_multiply_values(return_type, lhs, rhs)
}

fn execute_java_multiply_operands(
    return_type: &str,
    args: &[UdfValue],
    lhs: &JavaCompatOperand,
    rhs: &JavaCompatOperand,
) -> Result<UdfValue, String> {
    let lhs = java_operand_value(return_type, args, lhs)?;
    let rhs = java_operand_value(return_type, args, rhs)?;
    execute_java_multiply_values(return_type, &lhs, &rhs)
}

fn execute_java_multiply_values(
    return_type: &str,
    lhs: &UdfValue,
    rhs: &UdfValue,
) -> Result<UdfValue, String> {
    match return_type {
        "tinyint" => match (lhs, rhs) {
            (UdfValue::Tinyint(lhs), UdfValue::Tinyint(rhs)) => {
                Ok(UdfValue::Tinyint(lhs.saturating_mul(*rhs)))
            }
            (UdfValue::Null, _) | (_, UdfValue::Null) => Ok(UdfValue::Null),
            (lhs, rhs) => Err(format!(
                "Expected tinyint arguments, got {lhs:?} and {rhs:?}"
            )),
        },
        "smallint" => match (lhs, rhs) {
            (UdfValue::Smallint(lhs), UdfValue::Smallint(rhs)) => {
                Ok(UdfValue::Smallint(lhs.saturating_mul(*rhs)))
            }
            (UdfValue::Null, _) | (_, UdfValue::Null) => Ok(UdfValue::Null),
            (lhs, rhs) => Err(format!(
                "Expected smallint arguments, got {lhs:?} and {rhs:?}"
            )),
        },
        "int" => match (lhs, rhs) {
            (UdfValue::Int(lhs), UdfValue::Int(rhs)) => Ok(UdfValue::Int(lhs.saturating_mul(*rhs))),
            (UdfValue::Null, _) | (_, UdfValue::Null) => Ok(UdfValue::Null),
            (lhs, rhs) => Err(format!("Expected int arguments, got {lhs:?} and {rhs:?}")),
        },
        "bigint" => match (lhs, rhs) {
            (UdfValue::Long(lhs), UdfValue::Long(rhs)) => {
                Ok(UdfValue::Long(lhs.saturating_mul(*rhs)))
            }
            (UdfValue::Null, _) | (_, UdfValue::Null) => Ok(UdfValue::Null),
            (lhs, rhs) => Err(format!(
                "Expected bigint arguments, got {lhs:?} and {rhs:?}"
            )),
        },
        "counter" => match (lhs, rhs) {
            (UdfValue::Counter(lhs), UdfValue::Counter(rhs)) => {
                Ok(UdfValue::Counter(lhs.saturating_mul(*rhs)))
            }
            (UdfValue::Null, _) | (_, UdfValue::Null) => Ok(UdfValue::Null),
            (lhs, rhs) => Err(format!(
                "Expected counter arguments, got {lhs:?} and {rhs:?}"
            )),
        },
        "float" => match (lhs, rhs) {
            (UdfValue::Float(lhs), UdfValue::Float(rhs)) => Ok(UdfValue::Float(*lhs * *rhs)),
            (UdfValue::Null, _) | (_, UdfValue::Null) => Ok(UdfValue::Null),
            (lhs, rhs) => Err(format!("Expected float arguments, got {lhs:?} and {rhs:?}")),
        },
        "double" => match (lhs, rhs) {
            (UdfValue::Double(lhs), UdfValue::Double(rhs)) => Ok(UdfValue::Double(*lhs * *rhs)),
            (UdfValue::Null, _) | (_, UdfValue::Null) => Ok(UdfValue::Null),
            (lhs, rhs) => Err(format!(
                "Expected double arguments, got {lhs:?} and {rhs:?}"
            )),
        },
        _ => Err(format!(
            "Java multiplication UDF is not supported for return type {return_type}"
        )),
    }
}

fn execute_java_divide(
    return_type: &str,
    args: &[UdfValue],
    lhs: usize,
    rhs: usize,
) -> Result<UdfValue, String> {
    let (lhs, rhs) = java_binary_args(args, lhs, rhs)?;
    execute_java_divide_values(return_type, lhs, rhs)
}

fn execute_java_divide_operands(
    return_type: &str,
    args: &[UdfValue],
    lhs: &JavaCompatOperand,
    rhs: &JavaCompatOperand,
) -> Result<UdfValue, String> {
    let lhs = java_operand_value(return_type, args, lhs)?;
    let rhs = java_operand_value(return_type, args, rhs)?;
    execute_java_divide_values(return_type, &lhs, &rhs)
}

fn execute_java_divide_values(
    return_type: &str,
    lhs: &UdfValue,
    rhs: &UdfValue,
) -> Result<UdfValue, String> {
    match return_type {
        "tinyint" => match (lhs, rhs) {
            (UdfValue::Tinyint(_), UdfValue::Tinyint(0)) => Err("Division by zero".to_string()),
            (UdfValue::Tinyint(lhs), UdfValue::Tinyint(rhs)) => {
                Ok(UdfValue::Tinyint(lhs.wrapping_div(*rhs)))
            }
            (UdfValue::Null, _) | (_, UdfValue::Null) => Ok(UdfValue::Null),
            (lhs, rhs) => Err(format!(
                "Expected tinyint arguments, got {lhs:?} and {rhs:?}"
            )),
        },
        "smallint" => match (lhs, rhs) {
            (UdfValue::Smallint(_), UdfValue::Smallint(0)) => Err("Division by zero".to_string()),
            (UdfValue::Smallint(lhs), UdfValue::Smallint(rhs)) => {
                Ok(UdfValue::Smallint(lhs.wrapping_div(*rhs)))
            }
            (UdfValue::Null, _) | (_, UdfValue::Null) => Ok(UdfValue::Null),
            (lhs, rhs) => Err(format!(
                "Expected smallint arguments, got {lhs:?} and {rhs:?}"
            )),
        },
        "int" => match (lhs, rhs) {
            (UdfValue::Int(_), UdfValue::Int(0)) => Err("Division by zero".to_string()),
            (UdfValue::Int(lhs), UdfValue::Int(rhs)) => Ok(UdfValue::Int(lhs.wrapping_div(*rhs))),
            (UdfValue::Null, _) | (_, UdfValue::Null) => Ok(UdfValue::Null),
            (lhs, rhs) => Err(format!("Expected int arguments, got {lhs:?} and {rhs:?}")),
        },
        "bigint" => match (lhs, rhs) {
            (UdfValue::Long(_), UdfValue::Long(0)) => Err("Division by zero".to_string()),
            (UdfValue::Long(lhs), UdfValue::Long(rhs)) => {
                Ok(UdfValue::Long(lhs.wrapping_div(*rhs)))
            }
            (UdfValue::Null, _) | (_, UdfValue::Null) => Ok(UdfValue::Null),
            (lhs, rhs) => Err(format!(
                "Expected bigint arguments, got {lhs:?} and {rhs:?}"
            )),
        },
        "counter" => match (lhs, rhs) {
            (UdfValue::Counter(_), UdfValue::Counter(0)) => Err("Division by zero".to_string()),
            (UdfValue::Counter(lhs), UdfValue::Counter(rhs)) => {
                Ok(UdfValue::Counter(lhs.wrapping_div(*rhs)))
            }
            (UdfValue::Null, _) | (_, UdfValue::Null) => Ok(UdfValue::Null),
            (lhs, rhs) => Err(format!(
                "Expected counter arguments, got {lhs:?} and {rhs:?}"
            )),
        },
        "float" => match (lhs, rhs) {
            (UdfValue::Float(lhs), UdfValue::Float(rhs)) => Ok(UdfValue::Float(*lhs / *rhs)),
            (UdfValue::Null, _) | (_, UdfValue::Null) => Ok(UdfValue::Null),
            (lhs, rhs) => Err(format!("Expected float arguments, got {lhs:?} and {rhs:?}")),
        },
        "double" => match (lhs, rhs) {
            (UdfValue::Double(lhs), UdfValue::Double(rhs)) => Ok(UdfValue::Double(*lhs / *rhs)),
            (UdfValue::Null, _) | (_, UdfValue::Null) => Ok(UdfValue::Null),
            (lhs, rhs) => Err(format!(
                "Expected double arguments, got {lhs:?} and {rhs:?}"
            )),
        },
        _ => Err(format!(
            "Java division UDF is not supported for return type {return_type}"
        )),
    }
}

fn execute_java_remainder(
    return_type: &str,
    args: &[UdfValue],
    lhs: usize,
    rhs: usize,
) -> Result<UdfValue, String> {
    let (lhs, rhs) = java_binary_args(args, lhs, rhs)?;
    execute_java_remainder_values(return_type, lhs, rhs)
}

fn execute_java_remainder_operands(
    return_type: &str,
    args: &[UdfValue],
    lhs: &JavaCompatOperand,
    rhs: &JavaCompatOperand,
) -> Result<UdfValue, String> {
    let lhs = java_operand_value(return_type, args, lhs)?;
    let rhs = java_operand_value(return_type, args, rhs)?;
    execute_java_remainder_values(return_type, &lhs, &rhs)
}

fn execute_java_remainder_values(
    return_type: &str,
    lhs: &UdfValue,
    rhs: &UdfValue,
) -> Result<UdfValue, String> {
    match return_type {
        "tinyint" => match (lhs, rhs) {
            (UdfValue::Tinyint(_), UdfValue::Tinyint(0)) => Err("Division by zero".to_string()),
            (UdfValue::Tinyint(lhs), UdfValue::Tinyint(rhs)) => {
                Ok(UdfValue::Tinyint(lhs.wrapping_rem(*rhs)))
            }
            (UdfValue::Null, _) | (_, UdfValue::Null) => Ok(UdfValue::Null),
            (lhs, rhs) => Err(format!(
                "Expected tinyint arguments, got {lhs:?} and {rhs:?}"
            )),
        },
        "smallint" => match (lhs, rhs) {
            (UdfValue::Smallint(_), UdfValue::Smallint(0)) => Err("Division by zero".to_string()),
            (UdfValue::Smallint(lhs), UdfValue::Smallint(rhs)) => {
                Ok(UdfValue::Smallint(lhs.wrapping_rem(*rhs)))
            }
            (UdfValue::Null, _) | (_, UdfValue::Null) => Ok(UdfValue::Null),
            (lhs, rhs) => Err(format!(
                "Expected smallint arguments, got {lhs:?} and {rhs:?}"
            )),
        },
        "int" => match (lhs, rhs) {
            (UdfValue::Int(_), UdfValue::Int(0)) => Err("Division by zero".to_string()),
            (UdfValue::Int(lhs), UdfValue::Int(rhs)) => Ok(UdfValue::Int(lhs.wrapping_rem(*rhs))),
            (UdfValue::Null, _) | (_, UdfValue::Null) => Ok(UdfValue::Null),
            (lhs, rhs) => Err(format!("Expected int arguments, got {lhs:?} and {rhs:?}")),
        },
        "bigint" => match (lhs, rhs) {
            (UdfValue::Long(_), UdfValue::Long(0)) => Err("Division by zero".to_string()),
            (UdfValue::Long(lhs), UdfValue::Long(rhs)) => {
                Ok(UdfValue::Long(lhs.wrapping_rem(*rhs)))
            }
            (UdfValue::Null, _) | (_, UdfValue::Null) => Ok(UdfValue::Null),
            (lhs, rhs) => Err(format!(
                "Expected bigint arguments, got {lhs:?} and {rhs:?}"
            )),
        },
        "counter" => match (lhs, rhs) {
            (UdfValue::Counter(_), UdfValue::Counter(0)) => Err("Division by zero".to_string()),
            (UdfValue::Counter(lhs), UdfValue::Counter(rhs)) => {
                Ok(UdfValue::Counter(lhs.wrapping_rem(*rhs)))
            }
            (UdfValue::Null, _) | (_, UdfValue::Null) => Ok(UdfValue::Null),
            (lhs, rhs) => Err(format!(
                "Expected counter arguments, got {lhs:?} and {rhs:?}"
            )),
        },
        "float" => match (lhs, rhs) {
            (UdfValue::Float(lhs), UdfValue::Float(rhs)) => Ok(UdfValue::Float(*lhs % *rhs)),
            (UdfValue::Null, _) | (_, UdfValue::Null) => Ok(UdfValue::Null),
            (lhs, rhs) => Err(format!("Expected float arguments, got {lhs:?} and {rhs:?}")),
        },
        "double" => match (lhs, rhs) {
            (UdfValue::Double(lhs), UdfValue::Double(rhs)) => Ok(UdfValue::Double(*lhs % *rhs)),
            (UdfValue::Null, _) | (_, UdfValue::Null) => Ok(UdfValue::Null),
            (lhs, rhs) => Err(format!(
                "Expected double arguments, got {lhs:?} and {rhs:?}"
            )),
        },
        _ => Err(format!(
            "Java remainder UDF is not supported for return type {return_type}"
        )),
    }
}

fn execute_java_compare(
    return_type: &str,
    args: &[UdfValue],
    lhs: usize,
    rhs: usize,
    op: JavaCompatComparison,
) -> Result<UdfValue, String> {
    let (lhs, rhs) = java_binary_args(args, lhs, rhs)?;
    execute_java_compare_values(return_type, lhs, rhs, op)
}

fn execute_java_compare_operands(
    return_type: &str,
    args: &[UdfValue],
    lhs: &JavaCompatOperand,
    rhs: &JavaCompatOperand,
    op: JavaCompatComparison,
) -> Result<UdfValue, String> {
    let lhs = java_comparison_operand_value(args, lhs);
    let rhs = java_comparison_operand_value(args, rhs);
    execute_java_compare_values(return_type, &lhs, &rhs, op)
}

fn java_comparison_operand_value(args: &[UdfValue], operand: &JavaCompatOperand) -> UdfValue {
    match operand {
        JavaCompatOperand::Arg(index) => args.get(*index).cloned().unwrap_or(UdfValue::Null),
        JavaCompatOperand::Literal(literal) => java_literal_comparison_value(literal),
    }
}

fn execute_java_compare_values(
    return_type: &str,
    lhs: &UdfValue,
    rhs: &UdfValue,
    op: JavaCompatComparison,
) -> Result<UdfValue, String> {
    if return_type != "boolean" {
        return Err(format!(
            "Java comparison UDF must return boolean, got {return_type}"
        ));
    }
    match (lhs, rhs) {
        (UdfValue::Null, _) | (_, UdfValue::Null) => Ok(UdfValue::Null),
        (UdfValue::Boolean(lhs), UdfValue::Boolean(rhs)) => match op {
            JavaCompatComparison::Eq => Ok(UdfValue::Boolean(lhs == rhs)),
            JavaCompatComparison::Ne => Ok(UdfValue::Boolean(lhs != rhs)),
            _ => Err("Java boolean comparison only supports == and !=".to_string()),
        },
        (UdfValue::Text(lhs), UdfValue::Text(rhs)) => match op {
            JavaCompatComparison::Eq => Ok(UdfValue::Boolean(lhs == rhs)),
            JavaCompatComparison::Ne => Ok(UdfValue::Boolean(lhs != rhs)),
            _ => Err("Java text comparison only supports == and !=".to_string()),
        },
        _ => {
            let lhs = java_numeric_arg(lhs)
                .ok_or_else(|| format!("Expected numeric comparison argument, got {lhs:?}"))?;
            let rhs = java_numeric_arg(rhs)
                .ok_or_else(|| format!("Expected numeric comparison argument, got {rhs:?}"))?;
            let result = match op {
                JavaCompatComparison::Eq => lhs == rhs,
                JavaCompatComparison::Ne => lhs != rhs,
                JavaCompatComparison::Gt => lhs > rhs,
                JavaCompatComparison::Gte => lhs >= rhs,
                JavaCompatComparison::Lt => lhs < rhs,
                JavaCompatComparison::Lte => lhs <= rhs,
            };
            Ok(UdfValue::Boolean(result))
        }
    }
}

fn java_numeric_arg(value: &UdfValue) -> Option<f64> {
    match value {
        UdfValue::Tinyint(value) => Some(*value as f64),
        UdfValue::Smallint(value) => Some(*value as f64),
        UdfValue::Int(value) => Some(*value as f64),
        UdfValue::Long(value) => Some(*value as f64),
        UdfValue::Counter(value) => Some(*value as f64),
        UdfValue::Float(value) => Some(*value as f64),
        UdfValue::Double(value) => Some(*value),
        _ => None,
    }
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
    batch_mutation_sink: Option<BatchMutationSink>,
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
            batch_mutation_sink: None,
        }
    }

    /// Set the prepared statement cache for schema-change invalidation.
    pub fn with_prepared_cache(mut self, cache: Arc<PreparedCache>) -> Self {
        self.prepared_cache = Some(cache);
        self
    }

    pub fn with_trigger_registry(mut self, registry: Arc<TriggerRegistry>) -> Self {
        self.trigger_registry = registry;
        self
    }

    pub fn with_batch_mutation_sink(mut self, sink: BatchMutationSink) -> Self {
        self.batch_mutation_sink = Some(sink);
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
            QueryPlan::AlterTable(at) => self.execute_alter_table(at),
            QueryPlan::DropTable(dt) => self.execute_drop_table(dt),
            QueryPlan::Insert(ins) => self.execute_insert(ins),
            QueryPlan::Update(upd) => self.execute_update(upd),
            QueryPlan::Delete(del) => self.execute_delete(del),
            QueryPlan::Select(sel) => self.execute_select(sel, user),
            QueryPlan::Truncate(tr) => self.execute_truncate(tr),
            QueryPlan::Batch(batch) => self.execute_batch(batch, user),
            QueryPlan::CreateRole(cr) => self.execute_create_role(cr),
            QueryPlan::AlterRole(ar) => self.execute_alter_role(ar),
            QueryPlan::DropRole(dr) => self.execute_drop_role(dr),
            QueryPlan::Grant(gr) => self.execute_grant(gr),
            QueryPlan::Revoke(rv) => self.execute_revoke(rv),
            QueryPlan::ListRoles(lr) => self.execute_list_roles(lr),
            QueryPlan::ListPermissions(lp) => self.execute_list_permissions(lp, user),
            QueryPlan::CreateIndex(ci) => self.execute_create_index(ci),
            QueryPlan::DropIndex(di) => self.execute_drop_index(di),
            QueryPlan::CreateMaterializedView(cmv) => self.execute_create_mv(cmv),
            QueryPlan::DropMaterializedView(dmv) => self.execute_drop_mv(dmv),
            QueryPlan::AlterMaterializedView(amv) => self.execute_alter_mv(amv),
            QueryPlan::CreateType(ct) => self.execute_create_type(ct),
            QueryPlan::AlterType(at) => self.execute_alter_type(at),
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
        if result.is_ok() {
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
        {
            let catalog = self.catalog.read();
            let snapshot = catalog.snapshot();
            if snapshot.keyspace(&plan.name).is_some() {
                if plan.if_not_exists {
                    return Ok(QueryResult::Void);
                }
                return Err(ExecutorError::InvalidQuery(format!(
                    "Keyspace '{}' already exists",
                    plan.name
                )));
            }
        }

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
            comment: String::new(),
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
        if let Some(comment) = &plan.comment {
            params.comment = comment.clone();
        }

        drop(catalog);

        let ks = existing.clone().with_params(params);
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
        {
            let catalog = self.catalog.read();
            let snapshot = catalog.snapshot();
            if snapshot.keyspace(&plan.name).is_none() {
                if plan.if_exists {
                    return Ok(QueryResult::Void);
                }
                return Err(ExecutorError::KeyspaceNotFound(plan.name.clone()));
            }
        }

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

        let builtin_registry = FunctionRegistry::with_builtins();
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

            let column = ColumnMetadata::new(
                col.name.clone(),
                kind,
                position,
                col.cql_type.clone(),
                clustering_order,
                col.masked_with.clone(),
            )
            .with_constraints(col.constraints.clone());
            if let Some((function_name, args)) = &column.masked_with {
                validate_stored_cql_column_mask(&builtin_registry, &column, function_name, args)?;
            }
            builder = builder.add_column(column);
        }

        let mut table = builder.build();
        apply_table_options(&mut table.params, &plan.options)?;

        let catalog = self.catalog.read();
        let snapshot = catalog.snapshot();
        let existing_ks = snapshot
            .keyspace(&plan.keyspace)
            .ok_or_else(|| ExecutorError::KeyspaceNotFound(plan.keyspace.clone()))?;
        if existing_ks.table(&plan.name).is_some() {
            if plan.if_not_exists {
                return Ok(QueryResult::Void);
            }
            return Err(ExecutorError::InvalidQuery(format!(
                "Table '{}.{}' already exists",
                plan.keyspace, plan.name
            )));
        }
        let ks = existing_ks.clone().with_table(table);
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

    fn execute_alter_table(&self, plan: &AlterTablePlan) -> Result<QueryResult, ExecutorError> {
        let catalog = self.catalog.read();
        let snapshot = catalog.snapshot();
        let ks = snapshot
            .keyspace(&plan.keyspace)
            .ok_or_else(|| ExecutorError::KeyspaceNotFound(plan.keyspace.clone()))?;
        let mut table = ks
            .table(&plan.name)
            .ok_or_else(|| ExecutorError::TableNotFound(plan.keyspace.clone(), plan.name.clone()))?
            .clone();

        match &plan.operation {
            AlterTableOp::AddColumn(col) => {
                if table.column(&col.name).is_some() {
                    return Err(ExecutorError::InvalidQuery(format!(
                        "Column '{}' already exists in {}.{}",
                        col.name, plan.keyspace, plan.name
                    )));
                }
                let column_type = col.cql_type.resolve().ok_or_else(|| {
                    ExecutorError::InvalidQuery(format!("Unknown type for column '{}'", col.name))
                })?;
                let kind = if col.is_static {
                    ColumnKind::Static
                } else {
                    ColumnKind::Regular
                };
                let position = table
                    .columns
                    .iter()
                    .filter(|c| c.kind == kind)
                    .map(|c| c.position)
                    .max()
                    .map_or(0, |p| p + 1);
                let builtin_registry = FunctionRegistry::with_builtins();
                let mut column = ColumnMetadata::new(
                    col.name.clone(),
                    kind,
                    position,
                    column_type,
                    ClusteringOrder::None,
                    None,
                )
                .with_constraints(
                    col.constraints
                        .iter()
                        .map(resolve_column_constraint)
                        .collect(),
                );
                if let Some((function_name, args)) = &col.masked_with {
                    let mask_args =
                        validate_cql_column_mask(&builtin_registry, &column, function_name, args)?;
                    column.masked_with = Some((function_name.clone(), mask_args));
                }
                table.columns.push(column);
            }
            AlterTableOp::DropColumn(column_name) => {
                let Some(existing) = table.column(column_name).cloned() else {
                    return Err(ExecutorError::InvalidQuery(format!(
                        "Column '{}' does not exist in {}.{}",
                        column_name, plan.keyspace, plan.name
                    )));
                };
                if existing.is_primary_key() {
                    return Err(ExecutorError::InvalidQuery(format!(
                        "Cannot drop primary key column '{}'",
                        column_name
                    )));
                }
                table.columns.retain(|c| c.name != *column_name);
                table = table.with_dropped_column(DroppedColumn::new(
                    column_name,
                    existing.column_type.to_string(),
                    current_timestamp_micros(),
                    existing.kind,
                ));
            }
            AlterTableOp::AlterColumn(column_name, cql_type) => {
                let Some(column) = table.columns.iter_mut().find(|c| c.name == *column_name) else {
                    return Err(ExecutorError::InvalidQuery(format!(
                        "Column '{}' does not exist in {}.{}",
                        column_name, plan.keyspace, plan.name
                    )));
                };
                if column.is_primary_key() {
                    return Err(ExecutorError::InvalidQuery(format!(
                        "Cannot alter primary key column '{}'",
                        column_name
                    )));
                }
                column.column_type = cql_type.resolve().ok_or_else(|| {
                    ExecutorError::InvalidQuery(format!(
                        "Unknown type for column '{}'",
                        column_name
                    ))
                })?;
            }
            AlterTableOp::CommentColumn(column_name, comment) => {
                let Some(column) = table.columns.iter_mut().find(|c| c.name == *column_name) else {
                    return Err(ExecutorError::InvalidQuery(format!(
                        "Column '{}' does not exist in {}.{}",
                        column_name, plan.keyspace, plan.name
                    )));
                };
                column.comment = comment.clone();
            }
            AlterTableOp::AlterConstraints(column_name, constraints) => {
                let Some(column) = table.columns.iter_mut().find(|c| c.name == *column_name) else {
                    return Err(ExecutorError::InvalidQuery(format!(
                        "Column '{}' does not exist in {}.{}",
                        column_name, plan.keyspace, plan.name
                    )));
                };
                column.constraints = constraints.iter().map(resolve_column_constraint).collect();
            }
            AlterTableOp::DropConstraints(column_name) => {
                let Some(column) = table.columns.iter_mut().find(|c| c.name == *column_name) else {
                    return Err(ExecutorError::InvalidQuery(format!(
                        "Column '{}' does not exist in {}.{}",
                        column_name, plan.keyspace, plan.name
                    )));
                };
                column.constraints.clear();
            }
            AlterTableOp::MaskColumn(column_name, function_name, args) => {
                let Some(column) = table.columns.iter_mut().find(|c| c.name == *column_name) else {
                    return Err(ExecutorError::InvalidQuery(format!(
                        "Column '{}' does not exist in {}.{}",
                        column_name, plan.keyspace, plan.name
                    )));
                };
                let builtin_registry = FunctionRegistry::with_builtins();
                let mask_args =
                    validate_cql_column_mask(&builtin_registry, column, function_name, args)?;
                column.masked_with = Some((function_name.clone(), mask_args));
            }
            AlterTableOp::DropMask(column_name) => {
                let Some(column) = table.columns.iter_mut().find(|c| c.name == *column_name) else {
                    return Err(ExecutorError::InvalidQuery(format!(
                        "Column '{}' does not exist in {}.{}",
                        column_name, plan.keyspace, plan.name
                    )));
                };
                column.masked_with = None;
            }
            AlterTableOp::WithOptions(options) => {
                apply_table_options(&mut table.params, options)?;
            }
        }

        let updated_ks = ks.clone().with_table(table);
        drop(catalog);

        let mut catalog = self.catalog.write();
        *catalog = catalog.with_keyspace(updated_ks);

        Ok(QueryResult::SchemaChange {
            change_type: "UPDATED".into(),
            target: "TABLE".into(),
            keyspace: plan.keyspace.clone(),
            name: Some(plan.name.clone()),
        })
    }

    fn execute_drop_table(&self, plan: &DropTablePlan) -> Result<QueryResult, ExecutorError> {
        let catalog = self.catalog.read();
        let snapshot = catalog.snapshot();
        let existing_ks = snapshot
            .keyspace(&plan.keyspace)
            .ok_or_else(|| ExecutorError::KeyspaceNotFound(plan.keyspace.clone()))?;
        if existing_ks.table(&plan.name).is_none() {
            if plan.if_exists {
                return Ok(QueryResult::Void);
            }
            return Err(ExecutorError::TableNotFound(
                plan.keyspace.clone(),
                plan.name.clone(),
            ));
        }
        let ks = existing_ks.clone().without_table(&plan.name);
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

    fn execute_truncate(&self, plan: &TruncatePlan) -> Result<QueryResult, ExecutorError> {
        let catalog = self.catalog.read();
        let snapshot = catalog.snapshot();
        let ks = snapshot
            .keyspace(&plan.keyspace)
            .ok_or_else(|| ExecutorError::KeyspaceNotFound(plan.keyspace.clone()))?;
        if ks.table(&plan.table).is_none() {
            return Err(ExecutorError::TableNotFound(
                plan.keyspace.clone(),
                plan.table.clone(),
            ));
        }
        drop(catalog);

        self.engine
            .truncate_table(&plan.keyspace, &plan.table)
            .map_err(|e| ExecutorError::StorageError(e.to_string()))?;
        Ok(QueryResult::Void)
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
            let warnings = cassandra_config::sai_options::validate_index_definition(&plan.options);
            for w in &warnings {
                debug!(warning = %w, "SAI index option warning");
            }
            cassandra_storage::index::sai::analyzer::analyzer_from_options(&plan.options).map_err(
                |err| ExecutorError::InvalidQuery(format!("Invalid SAI analyzer options: {err}")),
            )?;
        }

        // Check for duplicate index on same column
        {
            let catalog = self.catalog.read();
            let snapshot = catalog.snapshot();
            let table_meta = snapshot.table(&plan.keyspace, &plan.table).ok_or_else(|| {
                ExecutorError::TableNotFound(plan.keyspace.clone(), plan.table.clone())
            })?;
            for idx in &table_meta.indexes {
                if idx.name == plan.index_name {
                    if plan.if_not_exists {
                        return Ok(QueryResult::Void);
                    }
                    return Err(ExecutorError::InvalidQuery(format!(
                        "Index '{}' already exists",
                        plan.index_name
                    )));
                }
                if idx.target_column() == Some(&plan.column) {
                    return Err(ExecutorError::InvalidQuery(format!(
                        "An index already exists on column '{}' (index '{}')",
                        plan.column, idx.name
                    )));
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
        let storage_options = options.clone();

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
        let index_type = match plan.custom_class.as_deref() {
            Some(class) if class.contains("StorageAttachedIndex") => IndexType::Sai,
            Some(class) if class.contains("SASIIndex") => IndexType::Sasi,
            _ => IndexType::Legacy,
        };

        let def = IndexDefinition {
            name: plan.index_name.clone(),
            keyspace: plan.keyspace.clone(),
            table: plan.table.clone(),
            column: plan.column.clone(),
            index_type,
            options: storage_options,
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
            .with_clustering_key(plan.clustering_key.clone())
            .with_options(plan.options.clone());

        let view = columns.into_iter().fold(view, |v, col| v.with_column(col));

        let catalog = self.catalog.read();
        let snapshot = catalog.snapshot();
        let existing_ks = snapshot
            .keyspace(&plan.keyspace)
            .ok_or_else(|| ExecutorError::KeyspaceNotFound(plan.keyspace.clone()))?;
        if existing_ks.view(&plan.name).is_some() {
            if plan.if_not_exists {
                return Ok(QueryResult::Void);
            }
            return Err(ExecutorError::InvalidQuery(format!(
                "Materialized view '{}.{}' already exists",
                plan.keyspace, plan.name
            )));
        }
        if existing_ks.table(&plan.base_table).is_none() {
            return Err(ExecutorError::TableNotFound(
                plan.keyspace.clone(),
                plan.base_table.clone(),
            ));
        }
        let ks = existing_ks.clone().with_view(view);
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
        let existing_ks = snapshot
            .keyspace(&plan.keyspace)
            .ok_or_else(|| ExecutorError::KeyspaceNotFound(plan.keyspace.clone()))?;
        if existing_ks.view(&plan.name).is_none() {
            if plan.if_exists {
                return Ok(QueryResult::Void);
            }
            return Err(ExecutorError::TableNotFound(
                plan.keyspace.clone(),
                plan.name.clone(),
            ));
        }
        let ks = existing_ks.clone().without_view(&plan.name);
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
        let catalog = self.catalog.read();
        let snapshot = catalog.snapshot();
        let keyspace = snapshot
            .keyspace(&plan.keyspace)
            .ok_or_else(|| ExecutorError::KeyspaceNotFound(plan.keyspace.clone()))?;
        let mut view = keyspace.view(&plan.name).cloned().ok_or_else(|| {
            ExecutorError::TableNotFound(plan.keyspace.clone(), plan.name.clone())
        })?;

        for (key, value) in &plan.options {
            view.options.insert(key.clone(), value.clone());
        }

        let updated_keyspace = keyspace.clone().with_view(view);
        drop(catalog);

        let mut catalog = self.catalog.write();
        *catalog = catalog.with_keyspace(updated_keyspace);

        info!(keyspace = %plan.keyspace, view = %plan.name, options = plan.options.len(), "Altered materialized view");
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
        let existing_ks = snapshot
            .keyspace(&plan.keyspace)
            .ok_or_else(|| ExecutorError::KeyspaceNotFound(plan.keyspace.clone()))?;
        if existing_ks.user_type(&plan.name).is_some() {
            if plan.if_not_exists {
                return Ok(QueryResult::Void);
            }
            return Err(ExecutorError::InvalidQuery(format!(
                "Type '{}.{}' already exists",
                plan.keyspace, plan.name
            )));
        }
        let ks = existing_ks.clone().with_type(udt);
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

    fn execute_alter_type(&self, plan: &AlterTypePlan) -> Result<QueryResult, ExecutorError> {
        let catalog = self.catalog.read();
        let snapshot = catalog.snapshot();
        let existing_ks = snapshot
            .keyspace(&plan.keyspace)
            .ok_or_else(|| ExecutorError::KeyspaceNotFound(plan.keyspace.clone()))?;
        let mut udt = existing_ks.user_type(&plan.name).cloned().ok_or_else(|| {
            ExecutorError::InvalidQuery(format!(
                "Type '{}.{}' does not exist",
                plan.keyspace, plan.name
            ))
        })?;

        match &plan.operation {
            AlterTypeOp::AddField(field_name, field_type) => {
                if udt
                    .field_names
                    .iter()
                    .any(|existing| existing.eq_ignore_ascii_case(field_name))
                {
                    return Err(ExecutorError::InvalidQuery(format!(
                        "Field '{}' already exists in type '{}.{}'",
                        field_name, plan.keyspace, plan.name
                    )));
                }
                udt.field_names.push(field_name.clone());
                udt.field_types
                    .push(canonical_ast_cql_type_name(field_type));
            }
            AlterTypeOp::RenameField(from, to) => {
                let Some(index) = udt
                    .field_names
                    .iter()
                    .position(|existing| existing.eq_ignore_ascii_case(from))
                else {
                    return Err(ExecutorError::InvalidQuery(format!(
                        "Field '{}' does not exist in type '{}.{}'",
                        from, plan.keyspace, plan.name
                    )));
                };
                if !from.eq_ignore_ascii_case(to)
                    && udt
                        .field_names
                        .iter()
                        .any(|existing| existing.eq_ignore_ascii_case(to))
                {
                    return Err(ExecutorError::InvalidQuery(format!(
                        "Field '{}' already exists in type '{}.{}'",
                        to, plan.keyspace, plan.name
                    )));
                }
                let old_name = udt.field_names[index].clone();
                udt.field_names[index] = to.clone();
                if let Some(comment) = udt.field_comments.remove(&old_name) {
                    udt.field_comments.insert(to.clone(), comment);
                }
            }
            AlterTypeOp::AlterFieldType(field_name, field_type) => {
                let Some(index) = udt
                    .field_names
                    .iter()
                    .position(|existing| existing.eq_ignore_ascii_case(field_name))
                else {
                    return Err(ExecutorError::InvalidQuery(format!(
                        "Field '{}' does not exist in type '{}.{}'",
                        field_name, plan.keyspace, plan.name
                    )));
                };
                udt.field_types[index] = canonical_ast_cql_type_name(field_type);
            }
            AlterTypeOp::CommentType(comment) => {
                udt.comment = comment.clone();
            }
            AlterTypeOp::CommentField(field_name, comment) => {
                let Some(existing_field_name) = udt
                    .field_names
                    .iter()
                    .find(|existing| existing.eq_ignore_ascii_case(field_name))
                    .cloned()
                else {
                    return Err(ExecutorError::InvalidQuery(format!(
                        "Field '{}' does not exist in type '{}.{}'",
                        field_name, plan.keyspace, plan.name
                    )));
                };
                udt.field_comments
                    .insert(existing_field_name, comment.clone());
            }
        }

        let ks = existing_ks.clone().with_type(udt);
        drop(catalog);

        let mut catalog = self.catalog.write();
        *catalog = catalog.with_keyspace(ks);

        info!(keyspace = %plan.keyspace, type_name = %plan.name, "Altered type");
        Ok(QueryResult::SchemaChange {
            change_type: "UPDATED".into(),
            target: "TYPE".into(),
            keyspace: plan.keyspace.clone(),
            name: Some(plan.name.clone()),
        })
    }

    fn execute_drop_type(&self, plan: &DropTypePlan) -> Result<QueryResult, ExecutorError> {
        let catalog = self.catalog.read();
        let snapshot = catalog.snapshot();
        let existing_ks = snapshot
            .keyspace(&plan.keyspace)
            .ok_or_else(|| ExecutorError::KeyspaceNotFound(plan.keyspace.clone()))?;
        if existing_ks.user_type(&plan.name).is_none() {
            if plan.if_exists {
                return Ok(QueryResult::Void);
            }
            return Err(ExecutorError::InvalidQuery(format!(
                "Type '{}.{}' does not exist",
                plan.keyspace, plan.name
            )));
        }
        let ks = existing_ks.clone().without_type(&plan.name);
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
        let canonical_args = plan
            .args
            .iter()
            .map(|(name, arg_type)| (name.clone(), canonical_cql_type_name(arg_type)))
            .collect::<Vec<_>>();
        let canonical_return_type = canonical_cql_type_name(&plan.return_type);
        let mut udf = UserFunction::new(
            &plan.keyspace,
            &plan.name,
            &canonical_return_type,
            &plan.language,
            &plan.body,
        )
        .with_called_on_null_input(plan.called_on_null_input);
        for (arg_name, arg_type) in &canonical_args {
            udf = udf.with_arg(arg_name, arg_type);
        }
        let signature = udf.signature();

        let catalog = self.catalog.read();
        let snapshot = catalog.snapshot();
        let existing_ks = snapshot
            .keyspace(&plan.keyspace)
            .ok_or_else(|| ExecutorError::KeyspaceNotFound(plan.keyspace.clone()))?;
        if existing_ks.aggregate(&signature).is_some() {
            return Err(ExecutorError::InvalidQuery(format!(
                "Function '{}' cannot replace an aggregate",
                plan.name
            )));
        }
        if let Some(existing_function) = existing_ks.function(&signature) {
            if plan.if_not_exists {
                return Ok(QueryResult::Void);
            }
            if !plan.or_replace {
                return Err(ExecutorError::InvalidQuery(format!(
                    "Function '{}' already exists",
                    signature
                )));
            }
            if plan.called_on_null_input != existing_function.called_on_null_input {
                return Err(ExecutorError::InvalidQuery(format!(
                    "Function '{}' must have {} directive",
                    plan.name,
                    if plan.called_on_null_input {
                        "CALLED ON NULL INPUT"
                    } else {
                        "RETURNS NULL ON NULL INPUT"
                    }
                )));
            }
            if !cql_type_name_is_compatible(&canonical_return_type, &existing_function.return_type)
            {
                return Err(ExecutorError::InvalidQuery(format!(
                    "Cannot replace function '{}', the new return type {} is not compatible with the return type {} of existing function",
                    plan.name, canonical_return_type, existing_function.return_type
                )));
            }
        }
        let ks = existing_ks.clone().with_function(udf);
        drop(catalog);

        let mut catalog = self.catalog.write();
        *catalog = catalog.with_keyspace(ks);

        // Wire UDF executor into the runtime registry based on language.
        match plan.language.to_ascii_lowercase().as_str() {
            "wasm" => {
                let bytecode = plan.body.as_bytes();
                match WasmUdfExecutor::new(plan.name.clone(), bytecode) {
                    Ok(executor) => {
                        let metadata = UdfMetadata {
                            keyspace: plan.keyspace.clone(),
                            name: plan.name.clone(),
                            args: canonical_args.clone(),
                            return_type: canonical_return_type.clone(),
                            language: plan.language.clone(),
                            body: plan.body.clone(),
                            called_on_null_input: plan.called_on_null_input,
                        };
                        if let Err(e) = self.udf_registry.register(metadata, Arc::new(executor)) {
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
            "java" => match JavaCompatUdfExecutor::new(plan) {
                Ok(executor) => {
                    let metadata = UdfMetadata {
                        keyspace: plan.keyspace.clone(),
                        name: plan.name.clone(),
                        args: canonical_args.clone(),
                        return_type: canonical_return_type.clone(),
                        language: plan.language.clone(),
                        body: plan.body.clone(),
                        called_on_null_input: plan.called_on_null_input,
                    };
                    if let Err(e) = self.udf_registry.register(metadata, Arc::new(executor)) {
                        debug!(error = %e, "Failed to register Java-compatible UDF executor");
                    }
                }
                Err(e) => {
                    debug!(
                        function = %plan.name,
                        error = %e,
                        "Java UDF body is not supported by the compatibility executor; metadata stored but function cannot execute"
                    );
                }
            },
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

    fn execute_drop_function(&self, plan: &DropFunctionPlan) -> Result<QueryResult, ExecutorError> {
        let canonical_arg_types = plan
            .arg_types
            .iter()
            .map(|arg_type| canonical_cql_type_name(arg_type))
            .collect::<Vec<_>>();
        let signature = format!("{}({})", plan.name, canonical_arg_types.join(", "));

        let catalog = self.catalog.read();
        let snapshot = catalog.snapshot();
        let existing_ks = snapshot
            .keyspace(&plan.keyspace)
            .ok_or_else(|| ExecutorError::KeyspaceNotFound(plan.keyspace.clone()))?;
        if existing_ks.function(&signature).is_none() {
            if plan.if_exists {
                return Ok(QueryResult::Void);
            }
            return Err(ExecutorError::InvalidQuery(format!(
                "Function '{}.{}' does not exist",
                plan.keyspace, signature
            )));
        }
        let ks = existing_ks.clone().without_function(&signature);
        drop(catalog);

        let mut catalog = self.catalog.write();
        *catalog = catalog.with_keyspace(ks);

        let runtime_args = plan
            .arg_types
            .iter()
            .map(|arg_type| (String::new(), canonical_cql_type_name(arg_type)))
            .collect::<Vec<_>>();
        self.udf_registry
            .unregister(&plan.keyspace, &plan.name, &runtime_args);

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
        let canonical_arg_types = plan
            .arg_types
            .iter()
            .map(|arg_type| canonical_cql_type_name(arg_type))
            .collect::<Vec<_>>();
        let canonical_state_type = canonical_cql_type_name(&plan.stype);
        let catalog = self.catalog.read();
        let snapshot = catalog.snapshot();
        let existing_ks = snapshot
            .keyspace(&plan.keyspace)
            .ok_or_else(|| ExecutorError::KeyspaceNotFound(plan.keyspace.clone()))?;
        let signature = format!("{}({})", plan.name, canonical_arg_types.join(", "));
        if existing_ks.function(&signature).is_some() {
            return Err(ExecutorError::InvalidQuery(format!(
                "Aggregate '{}' cannot replace a function",
                plan.name
            )));
        }
        if existing_ks.aggregate(&signature).is_some() {
            if plan.if_not_exists {
                return Ok(QueryResult::Void);
            }
            if !plan.or_replace {
                return Err(ExecutorError::InvalidQuery(format!(
                    "Aggregate '{}' already exists",
                    signature
                )));
            }
        }

        let mut state_arg_types = vec![canonical_state_type.clone()];
        state_arg_types.extend(canonical_arg_types.clone());
        let state_signature = format!("{}({})", plan.sfunc, state_arg_types.join(", "));
        let state_function = existing_ks.function(&state_signature).ok_or_else(|| {
            if existing_ks.aggregate(&state_signature).is_some() {
                ExecutorError::InvalidQuery(format!(
                    "State function {} isn't a scalar function",
                    state_signature
                ))
            } else {
                ExecutorError::InvalidQuery(format!(
                    "State function {} doesn't exist",
                    state_signature
                ))
            }
        })?;
        if state_function.return_type != canonical_state_type {
            return Err(ExecutorError::InvalidQuery(format!(
                "State function {} return type must be the same as the first argument type - check STYPE, argument and return types",
                state_signature
            )));
        }
        if !state_function.called_on_null_input && plan.initcond.is_none() {
            return Err(ExecutorError::InvalidQuery(format!(
                "Cannot create aggregate '{}' without INITCOND because state function {} does not accept 'null' arguments",
                plan.name, plan.sfunc
            )));
        }
        if let Some(initcond) = &plan.initcond {
            validate_aggregate_initcond(initcond, &canonical_state_type)?;
        }

        let mut aggregate_return_type = state_function.return_type.clone();
        if let Some(ref finalfunc) = plan.finalfunc {
            let final_signature = format!("{}({})", finalfunc, canonical_state_type);
            match existing_ks.function(&final_signature) {
                Some(final_function) => {
                    aggregate_return_type = final_function.return_type.clone();
                }
                None => {
                    if existing_ks.aggregate(&final_signature).is_some() {
                        return Err(ExecutorError::InvalidQuery(format!(
                            "Final function {} isn't a scalar function",
                            final_signature
                        )));
                    }
                    return Err(ExecutorError::InvalidQuery(format!(
                        "Final function {} doesn't exist",
                        final_signature
                    )));
                }
            }
        }

        if let Some(existing_aggregate) = existing_ks.aggregate(&signature) {
            if !cql_type_name_is_compatible(&aggregate_return_type, &existing_aggregate.return_type)
            {
                return Err(ExecutorError::InvalidQuery(format!(
                    "Cannot replace aggregate '{}', the new return type {} isn't compatible with the return type {} of existing function",
                    plan.name, aggregate_return_type, existing_aggregate.return_type
                )));
            }
        }

        let mut uda = UserAggregate::new(
            &plan.keyspace,
            &plan.name,
            &canonical_state_type,
            &plan.sfunc,
        )
        .with_return_type(&aggregate_return_type);
        for arg_type in &canonical_arg_types {
            uda = uda.with_arg_type(arg_type);
        }
        if let Some(ref ff) = plan.finalfunc {
            uda = uda.with_finalfunc(ff);
        }
        if let Some(ref ic) = plan.initcond {
            uda = uda.with_initcond(ic);
        }

        let ks = existing_ks.clone().with_aggregate(uda);
        drop(catalog);

        let mut catalog = self.catalog.write();
        *catalog = catalog.with_keyspace(ks);

        // Wire UDA: resolve SFUNC and FINALFUNC from UdfRegistry, register in UdaRegistry.
        let uda_metadata = cassandra_cql::uda::UdaMetadata {
            keyspace: plan.keyspace.clone(),
            name: plan.name.clone(),
            arg_types: canonical_arg_types.clone(),
            state_type: canonical_state_type.clone(),
            return_type: aggregate_return_type,
            sfunc_name: plan.sfunc.clone(),
            finalfunc_name: plan.finalfunc.clone(),
            initcond: plan.initcond.clone(),
        };

        // Attempt to resolve the state function from the UDF registry.
        // Build a signature for sfunc: it takes (state_type, arg_types...) as arguments.
        let mut sfunc_args: Vec<(String, String)> =
            vec![("state".into(), canonical_state_type.clone())];
        for (i, arg_type) in canonical_arg_types.iter().enumerate() {
            sfunc_args.push((format!("arg{}", i), arg_type.clone()));
        }
        let sfunc_resolved = self
            .udf_registry
            .get(&plan.keyspace, &plan.sfunc, &sfunc_args);

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
            let finalfunc_args = vec![("state".into(), canonical_state_type.clone())];
            let ff_resolved = self
                .udf_registry
                .get(&plan.keyspace, ff_name, &finalfunc_args);
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
        let canonical_arg_types = plan
            .arg_types
            .iter()
            .map(|arg_type| canonical_cql_type_name(arg_type))
            .collect::<Vec<_>>();
        let signature = format!("{}({})", plan.name, canonical_arg_types.join(", "));

        let catalog = self.catalog.read();
        let snapshot = catalog.snapshot();
        let existing_ks = snapshot
            .keyspace(&plan.keyspace)
            .ok_or_else(|| ExecutorError::KeyspaceNotFound(plan.keyspace.clone()))?;
        if existing_ks.aggregate(&signature).is_none() {
            if plan.if_exists {
                return Ok(QueryResult::Void);
            }
            return Err(ExecutorError::InvalidQuery(format!(
                "Aggregate '{}.{}' does not exist",
                plan.keyspace, signature
            )));
        }
        let ks = existing_ks.clone().without_aggregate(&signature);
        drop(catalog);

        let mut catalog = self.catalog.write();
        *catalog = catalog.with_keyspace(ks);

        self.uda_registry
            .unregister(&plan.keyspace, &plan.name, &canonical_arg_types);

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
        let table = ks_meta.table(&plan.table).ok_or_else(|| {
            ExecutorError::TableNotFound(plan.keyspace.clone(), plan.table.clone())
        })?;
        if table.trigger(&plan.name).is_some() {
            if plan.if_not_exists {
                return Ok(QueryResult::Void);
            }
            return Err(ExecutorError::InvalidQuery(format!(
                "Trigger '{}' already exists on {}.{}",
                plan.name, plan.keyspace, plan.table
            )));
        }

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

    fn execute_drop_trigger(&self, plan: &DropTriggerPlan) -> Result<QueryResult, ExecutorError> {
        let catalog = self.catalog.read();
        let snapshot = catalog.snapshot();
        let ks_meta = snapshot
            .keyspace(&plan.keyspace)
            .ok_or_else(|| ExecutorError::KeyspaceNotFound(plan.keyspace.clone()))?;
        let table = ks_meta.table(&plan.table).ok_or_else(|| {
            ExecutorError::TableNotFound(plan.keyspace.clone(), plan.table.clone())
        })?;
        if table.trigger(&plan.name).is_none() {
            if plan.if_exists {
                return Ok(QueryResult::Void);
            }
            return Err(ExecutorError::InvalidQuery(format!(
                "Trigger '{}' does not exist on {}.{}",
                plan.name, plan.keyspace, plan.table
            )));
        }

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
        let now = plan
            .using_timestamp
            .unwrap_or_else(current_timestamp_micros);
        let ttl = plan.using_ttl.unwrap_or(0);
        let now_secs = (current_timestamp_micros() / 1_000_000) as i32;
        let local_deletion_time = if ttl > 0 { Some(now_secs + ttl) } else { None };

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
            parse_json_insert(json_term, table_meta, plan.json_default)?
        } else {
            (plan.columns.clone(), plan.values.clone())
        };

        let mut pk_bytes = Vec::new();
        let mut ck_bytes = Vec::new();
        let mut cells = Vec::new();
        let mut static_cells_vec = Vec::new();
        let mut constraint_values: BTreeMap<String, Option<Vec<u8>>> = BTreeMap::new();
        let builtin_registry = FunctionRegistry::with_builtins();

        for (i, col_name) in effective_columns.iter().enumerate() {
            let val_bytes = if i < effective_values.len() {
                // Use typed binding when the column type is known from schema.
                if let Some(col_meta) = table_meta.column(col_name) {
                    term_to_bytes_with_functions(
                        &effective_values[i],
                        &col_meta.column_type,
                        &builtin_registry,
                        &self.udf_registry,
                        &plan.keyspace,
                    )?
                } else {
                    term_to_bytes(&effective_values[i])
                }
            } else {
                None
            };

            if table_meta.column(col_name).is_some() {
                constraint_values.insert(col_name.clone(), val_bytes.clone());
            }

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

        validate_write_constraints(table_meta, &constraint_values, true)?;

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

        let trigger_mutations =
            self.execute_triggers(&plan.keyspace, &plan.table, &pk_bytes, MutationType::Insert)?;
        self.engine
            .apply_mutation(&mutation)
            .map_err(|e| ExecutorError::StorageError(e.to_string()))?;
        self.apply_trigger_mutations(trigger_mutations, now)?;

        Ok(QueryResult::Void)
    }

    fn execute_update(&self, plan: &UpdatePlan) -> Result<QueryResult, ExecutorError> {
        let now = plan
            .using_timestamp
            .unwrap_or_else(current_timestamp_micros);
        let ttl = plan.using_ttl.unwrap_or(0);
        let now_secs = (current_timestamp_micros() / 1_000_000) as i32;
        let local_deletion_time = if ttl > 0 { Some(now_secs + ttl) } else { None };

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
        let builtin_registry = FunctionRegistry::with_builtins();

        for rel in &plan.where_clause {
            let val = relation_value_to_bytes(table_meta, rel);
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

        let existing_partition = self
            .engine
            .read_partition(&plan.keyspace, &plan.table, &pk_bytes);
        let existing_row = existing_partition
            .as_ref()
            .and_then(|partition| partition.rows.get(&ck_bytes));

        let mut constraint_values: BTreeMap<String, Option<Vec<u8>>> = BTreeMap::new();
        let cells: Vec<Cell> = plan
            .assignments
            .iter()
            .map(|a| {
                let value = if let Some(col_meta) = table_meta.column(&a.column) {
                    let current = existing_row.and_then(|row| {
                        current_live_cell_value(row, &a.column, now_secs)
                            .map(|bytes| bytes.to_vec())
                    });
                    assignment_value_to_bytes(
                        a,
                        &col_meta.column_type,
                        current.as_deref(),
                        &builtin_registry,
                        &self.udf_registry,
                        &plan.keyspace,
                    )?
                } else {
                    term_to_bytes(&a.value)
                };
                if table_meta.column(&a.column).is_some() {
                    constraint_values.insert(a.column.clone(), value.clone());
                }
                Ok(Cell {
                    column: a.column.clone(),
                    value,
                    timestamp: now,
                    ttl,
                    local_deletion_time,
                    is_tombstone: false,
                })
            })
            .collect::<Result<Vec<_>, ExecutorError>>()?;

        validate_write_constraints(table_meta, &constraint_values, false)?;

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

        let trigger_mutations =
            self.execute_triggers(&plan.keyspace, &plan.table, &pk_bytes, MutationType::Update)?;
        self.engine
            .apply_mutation(&mutation)
            .map_err(|e| ExecutorError::StorageError(e.to_string()))?;
        self.apply_trigger_mutations(trigger_mutations, now)?;

        Ok(QueryResult::Void)
    }

    fn execute_delete(&self, plan: &DeletePlan) -> Result<QueryResult, ExecutorError> {
        let now = plan
            .using_timestamp
            .unwrap_or_else(current_timestamp_micros);
        let now_secs = (current_timestamp_micros() / 1_000_000) as i32;

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
            let val = relation_value_to_bytes(table_meta, rel);
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

        let mut range_tombstones = Vec::new();

        if is_partition_delete && plan.columns.is_empty() {
            // Partition tombstone: DELETE FROM t WHERE pk = X (no clustering columns specified)
            let mutation = Mutation {
                keyspace: plan.keyspace.clone(),
                table: plan.table.clone(),
                partition_key: pk_bytes.clone(),
                rows: Vec::new(),
                timestamp: now,
                cdc_enabled: false,
                static_cells: Vec::new(),
                partition_tombstone: Some(TombstoneMarker {
                    timestamp: now,
                    local_deletion_time: now_secs,
                }),
                range_tombstones,
            };

            let trigger_mutations = self.execute_triggers(
                &plan.keyspace,
                &plan.table,
                &pk_bytes,
                MutationType::Delete,
            )?;
            self.engine
                .apply_mutation(&mutation)
                .map_err(|e| ExecutorError::StorageError(e.to_string()))?;
            self.apply_trigger_mutations(trigger_mutations, now)?;

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

            let trigger_mutations = self.execute_triggers(
                &plan.keyspace,
                &plan.table,
                &pk_bytes,
                MutationType::Delete,
            )?;
            self.engine
                .apply_mutation(&mutation)
                .map_err(|e| ExecutorError::StorageError(e.to_string()))?;
            self.apply_trigger_mutations(trigger_mutations, now)?;

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

        let trigger_mutations =
            self.execute_triggers(&plan.keyspace, &plan.table, &pk_bytes, MutationType::Delete)?;
        self.engine
            .apply_mutation(&mutation)
            .map_err(|e| ExecutorError::StorageError(e.to_string()))?;
        self.apply_trigger_mutations(trigger_mutations, now)?;

        Ok(QueryResult::Void)
    }

    fn execute_triggers(
        &self,
        keyspace: &str,
        table: &str,
        partition_key: &[u8],
        mutation_type: MutationType,
    ) -> Result<Vec<TriggerMutation>, ExecutorError> {
        if !self.trigger_registry.has_triggers(keyspace, table) {
            return Ok(Vec::new());
        }
        let event = MutationEvent {
            keyspace: keyspace.to_string(),
            table: table.to_string(),
            partition_key: vec![partition_key.to_vec()],
            mutation_type,
        };
        debug!(?event, "Executing triggers for mutation");
        self.trigger_registry
            .execute(&event)
            .map_err(ExecutorError::InvalidQuery)
    }

    fn apply_trigger_mutations(
        &self,
        trigger_mutations: Vec<TriggerMutation>,
        timestamp: i64,
    ) -> Result<(), ExecutorError> {
        for trigger_mutation in trigger_mutations {
            let mutation = trigger_mutation_to_commitlog_mutation(trigger_mutation, timestamp);
            self.engine
                .apply_mutation(&mutation)
                .map_err(|e| ExecutorError::StorageError(e.to_string()))?;
        }
        Ok(())
    }

    fn execute_select(
        &self,
        plan: &SelectPlan,
        user: Option<&str>,
    ) -> Result<QueryResult, ExecutorError> {
        let catalog = self.catalog.read();
        let snapshot = catalog.snapshot();
        let keyspace_meta = snapshot.keyspace(&plan.keyspace).ok_or_else(|| {
            ExecutorError::TableNotFound(plan.keyspace.clone(), plan.table.clone())
        })?;
        let table_meta = keyspace_meta.table(&plan.table).ok_or_else(|| {
            ExecutorError::TableNotFound(plan.keyspace.clone(), plan.table.clone())
        })?;

        let pk_cols = table_meta.partition_key_columns();
        let pk_names: Vec<String> = pk_cols.iter().map(|c| c.name.clone()).collect();

        let mut pk_bytes = Vec::new();
        let mut index_searches = Vec::new();
        for rel in &plan.where_clause {
            if pk_names.iter().any(|name| name == &rel.column) {
                let value = table_meta
                    .column(&rel.column)
                    .and_then(|col| typed_term_to_bytes(&rel.value, &col.column_type))
                    .or_else(|| term_to_bytes(&rel.value));
                if let Some(v) = value {
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
        let ann_index_search = plan.ann_clause.as_ref().and_then(|ann| {
            table_meta
                .indexes
                .iter()
                .find(|idx| idx.target_column() == Some(&ann.column) && is_vector_index(idx))
                .map(|idx| (ann, idx))
        });
        if plan.ann_clause.is_some() && ann_index_search.is_none() {
            return Err(ExecutorError::InvalidQuery(
                "ANN ORDER BY requires a vector index on the target column".to_string(),
            ));
        }

        let builtin_registry = FunctionRegistry::with_builtins();
        let selector_arg_types: Vec<Option<CqlType>> = match &plan.columns {
            SelectColumns::Named(selectors) => selectors
                .iter()
                .map(|sel| {
                    selector_argument_types_with_functions(
                        sel,
                        table_meta,
                        keyspace_meta,
                        &builtin_registry,
                    )
                    .into_iter()
                    .next()
                })
                .collect(),
            SelectColumns::All => Vec::new(),
        };
        let selector_all_arg_types: Vec<Vec<CqlType>> = match &plan.columns {
            SelectColumns::Named(selectors) => selectors
                .iter()
                .map(|sel| {
                    selector_argument_types_with_functions(
                        sel,
                        table_meta,
                        keyspace_meta,
                        &builtin_registry,
                    )
                })
                .collect(),
            SelectColumns::All => Vec::new(),
        };

        let user_aggregate_selectors: Vec<Option<ResolvedUserAggregate>> = match &plan.columns {
            SelectColumns::Named(selectors) => selectors
                .iter()
                .enumerate()
                .map(|(idx, selector)| {
                    self.resolve_user_aggregate_selector(
                        &plan.keyspace,
                        selector,
                        selector_all_arg_types
                            .get(idx)
                            .map(Vec::as_slice)
                            .unwrap_or(&[]),
                        keyspace_meta,
                    )
                })
                .collect::<Result<Vec<_>, _>>()?,
            SelectColumns::All => Vec::new(),
        };
        let user_function_selectors: Vec<Option<ResolvedUserFunction>> = match &plan.columns {
            SelectColumns::Named(selectors) => selectors
                .iter()
                .enumerate()
                .map(|(idx, selector)| {
                    if user_aggregate_selectors
                        .get(idx)
                        .is_some_and(|aggregate| aggregate.is_some())
                    {
                        return Ok(None);
                    }
                    self.resolve_user_function_selector(
                        &plan.keyspace,
                        selector,
                        selector_all_arg_types
                            .get(idx)
                            .map(Vec::as_slice)
                            .unwrap_or(&[]),
                        keyspace_meta,
                    )
                })
                .collect::<Result<Vec<_>, _>>()?,
            SelectColumns::All => Vec::new(),
        };
        let builtin_function_selectors: Vec<Option<ResolvedBuiltinFunction>> = match &plan.columns {
            SelectColumns::Named(selectors) => selectors
                .iter()
                .enumerate()
                .map(|(idx, selector)| {
                    if user_aggregate_selectors
                        .get(idx)
                        .is_some_and(|aggregate| aggregate.is_some())
                        || user_function_selectors
                            .get(idx)
                            .is_some_and(|function| function.is_some())
                    {
                        return None;
                    }
                    resolve_builtin_function_selector(
                        selector,
                        selector_all_arg_types
                            .get(idx)
                            .map(Vec::as_slice)
                            .unwrap_or(&[]),
                        &builtin_registry,
                    )
                })
                .collect(),
            SelectColumns::All => Vec::new(),
        };

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
                .enumerate()
                .map(|(idx, sel)| {
                    result_column_for_selector(
                        sel,
                        table_meta,
                        &plan.keyspace,
                        &plan.table,
                        user_aggregate_selectors
                            .get(idx)
                            .and_then(|agg| agg.as_ref()),
                        user_function_selectors.get(idx).and_then(|f| f.as_ref()),
                        builtin_function_selectors.get(idx).and_then(|f| f.as_ref()),
                        keyspace_meta,
                        &builtin_registry,
                    )
                    .ok_or_else(|| {
                        ExecutorError::InvalidQuery(format!(
                            "Unsupported SELECT selector '{}'",
                            selector_output_name(sel)
                        ))
                    })
                })
                .collect::<Result<Vec<_>, _>>()?,
        };

        // WU-04: Extract clustering column names for slice filtering
        let ck_names: Vec<String> = table_meta
            .clustering_columns()
            .iter()
            .map(|c| c.name.clone())
            .collect();

        // WU-07: Extract static column names
        let static_col_names: Vec<String> = table_meta
            .columns
            .iter()
            .filter(|c| c.kind == ColumnKind::Static)
            .map(|c| c.name.clone())
            .collect();

        let table_resource = SecurityResource::Table {
            keyspace: plan.keyspace.clone(),
            table: plan.table.clone(),
        };
        let user_has_unmask = user.is_some_and(|username| {
            self.authorizer
                .authorize(username, &table_resource, Permission::Unmask)
                .is_ok()
        });
        if let Some(username) = user {
            if !user_has_unmask {
                let restricted_masked_columns =
                    masked_restricted_columns(table_meta, &plan.where_clause, &pk_names);
                if !restricted_masked_columns.is_empty()
                    && self
                        .authorizer
                        .authorize(username, &table_resource, Permission::SelectMasked)
                        .is_err()
                {
                    return Err(ExecutorError::InvalidQuery(format!(
                        "User {} has no UNMASK nor SELECT_MASKED permission on table {}.{}, cannot query masked columns [{}]",
                        username,
                        plan.keyspace,
                        plan.table,
                        restricted_masked_columns.join(", ")
                    )));
                }
            }
        }
        // Unauthenticated requests are masked; users with UNMASK see clear values.
        let apply_masking = user.is_none() || !user_has_unmask;

        drop(catalog);

        let selectors = match &plan.columns {
            SelectColumns::Named(selectors) => Some(selectors.as_slice()),
            SelectColumns::All => None,
        };
        let has_aggregates = selectors.is_some_and(|selectors| {
            selectors.iter().enumerate().any(|(idx, selector)| {
                selector_is_aggregate(selector)
                    || user_aggregate_selectors
                        .get(idx)
                        .is_some_and(|aggregate| aggregate.is_some())
            })
        });
        let is_count = selectors.is_some_and(|selectors| {
            selectors.len() == 1
                && selectors.iter().all(is_count_selector)
                && plan.group_by.is_empty()
        });

        let pk_result_indices: Vec<usize> = result_columns
            .iter()
            .enumerate()
            .filter_map(|(idx, col)| {
                if pk_names.iter().any(|name| name == &col.name) {
                    Some(idx)
                } else {
                    None
                }
            })
            .collect();
        let row_limit = plan
            .limit
            .as_ref()
            .and_then(term_to_i64)
            .map(|l| l as usize)
            .unwrap_or(usize::MAX);
        let now_secs = (current_timestamp_micros() / 1_000_000) as i32;

        let column_value =
            |column_name: &str, partition_key: &[u8], row: &Row, static_row: Option<&Row>| {
                let mut value = raw_column_value(
                    &pk_names,
                    &ck_names,
                    &static_col_names,
                    column_name,
                    partition_key,
                    row,
                    static_row,
                    now_secs,
                );

                if apply_masking {
                    if let Some(v) = &value {
                        if let Some(masked) =
                            apply_cql_column_mask(table_meta, &builtin_registry, column_name, v)
                        {
                            value = masked;
                        }
                    }
                }

                value
            };
        let row_matches_where = |partition_key: &[u8], row: &Row, static_row: Option<&Row>| {
            row_matches_relations(
                &plan.where_clause,
                table_meta,
                &pk_names,
                &ck_names,
                &static_col_names,
                partition_key,
                row,
                static_row,
                now_secs,
            )
        };

        let make_result_row = |partition_key: &[u8],
                               row: &Row,
                               static_row: Option<&Row>|
         -> Result<Vec<Option<Vec<u8>>>, ExecutorError> {
            match selectors {
                Some(selectors) => selectors
                    .iter()
                    .enumerate()
                    .map(|(idx, selector)| {
                        select_value_for_selector(
                            selector,
                            partition_key,
                            row,
                            static_row,
                            &column_value,
                            &pk_names,
                            &ck_names,
                            &static_col_names,
                            now_secs,
                            table_meta,
                            &builtin_registry,
                            &plan.keyspace,
                            keyspace_meta,
                            &self.udf_registry,
                            user_function_selectors.get(idx).and_then(|f| f.as_ref()),
                            builtin_function_selectors.get(idx).and_then(|f| f.as_ref()),
                        )
                    })
                    .collect(),
                None => Ok(result_columns
                    .iter()
                    .map(|rc| column_value(&rc.name, partition_key, row, static_row))
                    .collect::<Vec<_>>()),
            }
        };

        let make_aggregate_input = |partition_key: &[u8],
                                    row: &Row,
                                    static_row: Option<&Row>|
         -> Result<
            (Vec<Option<Vec<u8>>>, Vec<Vec<Option<Vec<u8>>>>),
            ExecutorError,
        > {
            let group_key = plan
                .group_by
                .iter()
                .map(|column| {
                    raw_column_value(
                        &pk_names,
                        &ck_names,
                        &static_col_names,
                        column,
                        partition_key,
                        row,
                        static_row,
                        now_secs,
                    )
                })
                .collect::<Vec<_>>();

            let values = selectors
                .unwrap_or(&[])
                .iter()
                .map(|selector| {
                    selector_aggregate_values(
                        selector,
                        partition_key,
                        row,
                        static_row,
                        &column_value,
                        &pk_names,
                        &ck_names,
                        &static_col_names,
                        now_secs,
                        table_meta,
                        &builtin_registry,
                        &plan.keyspace,
                        keyspace_meta,
                        &self.udf_registry,
                    )
                })
                .collect::<Result<Vec<_>, _>>()?;

            Ok((group_key, values))
        };

        if pk_bytes.is_empty() {
            let indexed_search_results = if let Some((ann, idx)) = ann_index_search {
                let term_bytes = vector_literal_to_bytes(&ann.vector_literal);
                Some((
                    self.engine
                        .search_vector_index_with_keys(
                            &plan.keyspace,
                            &plan.table,
                            &idx.name,
                            &term_bytes,
                            ann.top_k,
                        )
                        .map_err(|e: Box<dyn std::error::Error>| {
                            ExecutorError::StorageError(e.to_string())
                        })?
                        .into_iter()
                        .map(|(partition_key, pd, _score)| (partition_key, pd))
                        .collect::<Vec<_>>(),
                    None,
                ))
            } else if !index_searches.is_empty() {
                let (rel, idx) = &index_searches[0];
                let is_vector = idx
                    .options
                    .get("class_name")
                    .map(|s| s.contains("StorageAttachedIndex"))
                    .unwrap_or(false)
                    && idx.options.contains_key("vector_dimensions");

                let term_bytes = term_to_bytes(&rel.value).unwrap_or_default();

                let results = if is_vector {
                    let k = plan.limit.as_ref().and_then(term_to_i64).unwrap_or(10) as usize;
                    self.engine
                        .search_vector_index_with_keys(
                            &plan.keyspace,
                            &plan.table,
                            &idx.name,
                            &term_bytes,
                            k,
                        )
                        .map_err(|e: Box<dyn std::error::Error>| {
                            ExecutorError::StorageError(e.to_string())
                        })?
                        .into_iter()
                        .map(|(partition_key, pd, _score)| (partition_key, pd))
                        .collect::<Vec<_>>()
                } else {
                    self.engine
                        .search_index_with_keys(&plan.keyspace, &plan.table, &idx.name, &term_bytes)
                        .map_err(|e: Box<dyn std::error::Error>| {
                            ExecutorError::StorageError(e.to_string())
                        })?
                };
                Some((results, Some(rel.clone())))
            } else {
                None
            };

            if let Some((search_results, satisfied_relation)) = indexed_search_results {
                let mut result_rows = Vec::new();
                let mut aggregate_inputs = Vec::new();
                let residual_where = satisfied_relation.as_ref().map(|satisfied| {
                    plan.where_clause
                        .iter()
                        .filter(|rel| *rel != satisfied)
                        .cloned()
                        .collect::<Vec<_>>()
                });

                for (partition_key, pd) in search_results {
                    let live: Vec<&Row> = pd.live_rows(now_secs);
                    let static_row = pd.rows.get(&Vec::new());
                    for row in live {
                        let matches_where = if let Some(residual_where) = residual_where.as_ref() {
                            row_matches_relations(
                                residual_where,
                                table_meta,
                                &pk_names,
                                &ck_names,
                                &static_col_names,
                                &partition_key,
                                row,
                                static_row,
                                now_secs,
                            )
                        } else {
                            row_matches_where(&partition_key, row, static_row)
                        };
                        if !matches_where {
                            continue;
                        }
                        if has_aggregates {
                            aggregate_inputs.push(make_aggregate_input(
                                &partition_key,
                                row,
                                static_row,
                            )?);
                        } else {
                            result_rows.push(make_result_row(&partition_key, row, static_row)?);
                        }
                    }
                }

                if has_aggregates {
                    result_rows = finalize_aggregate_rows(
                        selectors.unwrap_or(&[]),
                        &selector_arg_types,
                        &user_aggregate_selectors,
                        aggregate_inputs,
                    )?;
                }

                if plan.distinct {
                    result_rows = cassandra_cql::selection::post_process::apply_distinct(
                        result_rows,
                        &pk_result_indices,
                    );
                }
                if result_rows.len() > row_limit {
                    result_rows.truncate(row_limit);
                }

                let (result_rows, pg_state, pg_warnings) =
                    apply_paging(result_rows, plan.page_size, &plan.paging_state);
                return wrap_select_json(
                    plan.json,
                    result_columns,
                    result_rows,
                    pg_state,
                    pg_warnings,
                );
            }

            let partitions = self.engine.scan_all_partitions(&plan.keyspace, &plan.table);
            let mut result_rows = Vec::new();
            let mut aggregate_inputs = Vec::new();
            let mut count = 0i64;

            'partitions: for (partition_key, pd) in partitions {
                let live: Vec<&Row> = pd.live_rows(now_secs);
                if is_count {
                    let static_row = pd.rows.get(&Vec::new());
                    count += live
                        .iter()
                        .filter(|row| row_matches_where(&partition_key, row, static_row))
                        .count() as i64;
                    continue;
                }

                let static_row = pd.rows.get(&Vec::new());
                for row in live {
                    if !row_matches_where(&partition_key, row, static_row) {
                        continue;
                    }
                    if has_aggregates {
                        aggregate_inputs.push(make_aggregate_input(
                            &partition_key,
                            row,
                            static_row,
                        )?);
                    } else {
                        result_rows.push(make_result_row(&partition_key, row, static_row)?);
                    }
                    if !has_aggregates && !plan.distinct && result_rows.len() >= row_limit {
                        break 'partitions;
                    }
                }
            }

            if is_count {
                return Ok(QueryResult::Rows {
                    columns: result_columns,
                    rows: vec![vec![Some(count.to_be_bytes().to_vec())]],
                    paging_state: None,
                    warnings: Vec::new(),
                });
            }

            if has_aggregates {
                result_rows = finalize_aggregate_rows(
                    selectors.unwrap_or(&[]),
                    &selector_arg_types,
                    &user_aggregate_selectors,
                    aggregate_inputs,
                )?;
            }

            if plan.distinct {
                result_rows = cassandra_cql::selection::post_process::apply_distinct(
                    result_rows,
                    &pk_result_indices,
                );
            }
            if result_rows.len() > row_limit {
                result_rows.truncate(row_limit);
            }

            let (result_rows, pg_state, pg_warnings) =
                apply_paging(result_rows, plan.page_size, &plan.paging_state);
            return wrap_select_json(
                plan.json,
                result_columns,
                result_rows,
                pg_state,
                pg_warnings,
            );
        }

        let partition = self
            .engine
            .read_partition(&plan.keyspace, &plan.table, &pk_bytes);

        let mut result_rows = Vec::new();
        let mut aggregate_inputs = Vec::new();

        // WU-11: COUNT(*) early return
        if is_count {
            let count = if let Some(pd) = partition {
                let static_row = pd.rows.get(&Vec::new());
                pd.live_rows(now_secs)
                    .iter()
                    .filter(|row| row_matches_where(&pk_bytes, row, static_row))
                    .count()
            } else {
                0
            };
            let count_column = ResultColumn {
                keyspace: plan.keyspace.clone(),
                table: plan.table.clone(),
                name: "count".to_string(),
                cql_type: CqlType::Bigint,
            };
            let count_bytes = (count as i64).to_be_bytes().to_vec();
            return Ok(QueryResult::Rows {
                columns: vec![count_column],
                rows: vec![vec![Some(count_bytes)]],
                paging_state: None,
                warnings: Vec::new(),
            });
        }

        if let Some(pd) = partition {
            let now_secs = (current_timestamp_micros() / 1_000_000) as i32;
            let live: Vec<&Row> = pd.live_rows(now_secs);

            // WU-04: Filter by clustering restrictions
            let mut live: Vec<&Row> = if !ck_names.is_empty() {
                let mut ck_lower: Option<Vec<u8>> = None;
                let mut ck_upper: Option<Vec<u8>> = None;
                let mut ck_lower_inclusive = true;
                let mut ck_upper_inclusive = true;
                for rel in &plan.where_clause {
                    if ck_names.contains(&rel.column) {
                        if let Some(v) = term_to_bytes(&rel.value) {
                            match rel.op {
                                RelationOp::Eq => {
                                    ck_lower = Some(v.clone());
                                    ck_upper = Some(v);
                                }
                                RelationOp::Gt => {
                                    ck_lower = Some(v);
                                    ck_lower_inclusive = false;
                                }
                                RelationOp::Gte => {
                                    ck_lower = Some(v);
                                }
                                RelationOp::Lt => {
                                    ck_upper = Some(v);
                                    ck_upper_inclusive = false;
                                }
                                RelationOp::Lte => {
                                    ck_upper = Some(v);
                                }
                                _ => {}
                            }
                        }
                    }
                }
                live.into_iter()
                    .filter(|row| {
                        let ck = &row.clustering_key;
                        let start_ok = match &ck_lower {
                            Some(lower) => {
                                if ck_lower_inclusive {
                                    ck >= lower
                                } else {
                                    ck > lower
                                }
                            }
                            None => true,
                        };
                        let end_ok = match &ck_upper {
                            Some(upper) => {
                                if ck_upper_inclusive {
                                    ck <= upper
                                } else {
                                    ck < upper
                                }
                            }
                            None => true,
                        };
                        start_ok && end_ok
                    })
                    .collect()
            } else {
                live
            };

            if plan
                .order_by
                .iter()
                .any(|(_, ord)| matches!(ord, AstClusteringOrder::Desc))
            {
                live.reverse();
            }

            let static_row = pd.rows.get(&Vec::new());
            for row in live {
                if !row_matches_where(&pk_bytes, row, static_row) {
                    continue;
                }
                if result_rows.len() >= row_limit {
                    break;
                }
                if has_aggregates {
                    aggregate_inputs.push(make_aggregate_input(&pk_bytes, row, static_row)?);
                } else {
                    result_rows.push(make_result_row(&pk_bytes, row, static_row)?);
                }
            }
        }

        if has_aggregates {
            result_rows = finalize_aggregate_rows(
                selectors.unwrap_or(&[]),
                &selector_arg_types,
                &user_aggregate_selectors,
                aggregate_inputs,
            )?;
        }

        if plan.distinct {
            result_rows = cassandra_cql::selection::post_process::apply_distinct(
                result_rows,
                &pk_result_indices,
            );
        }
        if result_rows.len() > row_limit {
            result_rows.truncate(row_limit);
        }

        let (result_rows, pg_state, pg_warnings) =
            apply_paging(result_rows, plan.page_size, &plan.paging_state);
        wrap_select_json(
            plan.json,
            result_columns,
            result_rows,
            pg_state,
            pg_warnings,
        )
    }

    fn execute_batch(
        &self,
        plan: &BatchPlan,
        _user: Option<&str>,
    ) -> Result<QueryResult, ExecutorError> {
        // Validate counter/non-counter mixing:
        // Counter batches must only contain counter mutations and vice versa.
        let is_counter_batch = matches!(plan.batch_type, cassandra_cql::ast::BatchType::Counter);
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

        // Build Vec<Mutation> from sub-plans before handing the batch to the
        // configured sink. Production server wiring can install a
        // StorageProxy/BatchCoordinator-backed sink; tests and embedded
        // single-node execution fall back to local storage.
        let now = current_timestamp_micros();
        let mut collected_mutations: Vec<Mutation> = Vec::with_capacity(plan.plans.len());

        for sub in &plan.plans {
            let (mutation, mutation_type) = match sub {
                QueryPlan::Insert(ins) => {
                    (self.build_insert_mutation(ins, now)?, MutationType::Insert)
                }
                QueryPlan::Update(upd) => {
                    (self.build_update_mutation(upd, now)?, MutationType::Update)
                }
                QueryPlan::Delete(del) => {
                    (self.build_delete_mutation(del, now)?, MutationType::Delete)
                }
                _ => unreachable!("Validated above: only INSERT/UPDATE/DELETE in batch"),
            };
            let trigger_mutations = self.execute_triggers(
                &mutation.keyspace,
                &mutation.table,
                &mutation.partition_key,
                mutation_type,
            )?;
            collected_mutations.push(mutation);
            collected_mutations.extend(trigger_mutations.into_iter().map(|trigger_mutation| {
                trigger_mutation_to_commitlog_mutation(trigger_mutation, now)
            }));
        }

        if let Some(sink) = &self.batch_mutation_sink {
            sink(plan.batch_type, &collected_mutations)?;
        } else {
            for mutation in &collected_mutations {
                self.engine
                    .apply_mutation(mutation)
                    .map_err(|e| ExecutorError::StorageError(e.to_string()))?;
            }
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
            parse_json_insert(json_term, table_meta, plan.json_default)?
        } else {
            (plan.columns.clone(), plan.values.clone())
        };

        let mut pk_bytes = Vec::new();
        let mut ck_bytes = Vec::new();
        let mut cells = Vec::new();
        let builtin_registry = FunctionRegistry::with_builtins();

        for (i, col_name) in effective_columns.iter().enumerate() {
            let val_bytes = if i < effective_values.len() {
                if let Some(col_meta) = table_meta.column(col_name) {
                    term_to_bytes_with_functions(
                        &effective_values[i],
                        &col_meta.column_type,
                        &builtin_registry,
                        &self.udf_registry,
                        &plan.keyspace,
                    )?
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
        let builtin_registry = FunctionRegistry::with_builtins();

        for rel in &plan.where_clause {
            let val = relation_value_to_bytes(table_meta, rel);
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
            .map(|a| {
                let value = if let Some(col_meta) = table_meta.column(&a.column) {
                    term_to_bytes_with_functions(
                        &a.value,
                        &col_meta.column_type,
                        &builtin_registry,
                        &self.udf_registry,
                        &plan.keyspace,
                    )?
                } else {
                    term_to_bytes(&a.value)
                };
                Ok(Cell {
                    column: a.column.clone(),
                    value,
                    timestamp: now,
                    ttl: 0,
                    local_deletion_time: None,
                    is_tombstone: false,
                })
            })
            .collect::<Result<Vec<_>, ExecutorError>>()?;

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
        let mut ck_eq_bytes = Vec::new();
        let mut has_ck_eq = false;
        let mut ck_range_start: Option<Vec<u8>> = None;
        let mut ck_range_end: Option<Vec<u8>> = None;
        let mut has_ck_range = false;

        for rel in &plan.where_clause {
            let val = relation_value_to_bytes(table_meta, rel);
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

        let has_ck_columns = !ck_names.is_empty();
        let is_partition_delete = has_ck_columns && !has_ck_eq && !has_ck_range;
        let is_range_delete = has_ck_range;

        drop(catalog);

        if is_partition_delete && plan.columns.is_empty() {
            return Ok(Mutation {
                keyspace: plan.keyspace.clone(),
                table: plan.table.clone(),
                partition_key: pk_bytes,
                rows: Vec::new(),
                timestamp: now,
                cdc_enabled: false,
                static_cells: Vec::new(),
                partition_tombstone: Some(TombstoneMarker {
                    timestamp: now,
                    local_deletion_time: now_secs,
                }),
                range_tombstones: Vec::new(),
            });
        }

        if is_range_delete && plan.columns.is_empty() {
            return Ok(Mutation {
                keyspace: plan.keyspace.clone(),
                table: plan.table.clone(),
                partition_key: pk_bytes,
                rows: Vec::new(),
                timestamp: now,
                cdc_enabled: false,
                static_cells: Vec::new(),
                partition_tombstone: None,
                range_tombstones: vec![CommitlogRangeTombstone {
                    start: ck_range_start.unwrap_or_default(),
                    end: ck_range_end.unwrap_or_default(),
                    timestamp: now,
                    local_deletion_time: now_secs,
                }],
            });
        }

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
        if self.role_manager.role_exists(&plan.name) {
            if plan.if_not_exists {
                return Ok(QueryResult::Void);
            }
            return Err(ExecutorError::InvalidQuery(format!(
                "Role '{}' already exists",
                plan.name
            )));
        }

        let hashed_password = match plan.hashed_password.clone() {
            Some(hashed_password) => Some(hashed_password),
            None => plan
                .password
                .as_deref()
                .map(|p| cassandra_security::auth::hash_password(p).unwrap()),
        };

        self.role_manager.create_role(Role {
            name: plan.name.clone(),
            is_superuser: plan.is_superuser,
            can_login: plan.can_login,
            hashed_password,
            member_of: vec![],
            network_permissions: role_network_permissions(
                plan.datacenter_access.as_ref(),
                plan.cidr_access.as_ref(),
            ),
        });

        info!(role = %plan.name, "Created role");
        Ok(QueryResult::Void)
    }

    fn execute_alter_role(&self, plan: &AlterRolePlan) -> Result<QueryResult, ExecutorError> {
        if !self.role_manager.role_exists(&plan.name) && plan.if_exists {
            return Ok(QueryResult::Void);
        }

        let opts = RoleOptions {
            is_superuser: plan.superuser,
            can_login: plan.login,
            password: plan.password.clone(),
            hashed_password: plan.hashed_password.clone(),
            network_permissions: role_network_permissions(
                plan.datacenter_access.as_ref(),
                plan.cidr_access.as_ref(),
            ),
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

    fn execute_list_roles(&self, plan: &ListRolesPlan) -> Result<QueryResult, ExecutorError> {
        let mut roles = self.role_manager.list_roles();
        roles.sort_by(|a, b| a.name.cmp(&b.name));

        if let Some(ref role_name) = plan.of_role {
            let base = self.role_manager.get_role(role_name).ok_or_else(|| {
                ExecutorError::InvalidQuery(format!("Role '{}' doesn't exist", role_name))
            })?;
            let allowed: std::collections::HashSet<String> = if plan.no_recursive {
                std::iter::once(base.name).chain(base.member_of).collect()
            } else {
                self.role_manager
                    .get_all_roles(role_name)
                    .into_iter()
                    .collect()
            };
            roles.retain(|role| allowed.contains(&role.name));
        }

        let columns = vec![
            ResultColumn {
                keyspace: "system_auth".to_string(),
                table: "roles".to_string(),
                name: "role".to_string(),
                cql_type: CqlType::Varchar,
            },
            ResultColumn {
                keyspace: "system_auth".to_string(),
                table: "roles".to_string(),
                name: "super".to_string(),
                cql_type: CqlType::Boolean,
            },
            ResultColumn {
                keyspace: "system_auth".to_string(),
                table: "roles".to_string(),
                name: "login".to_string(),
                cql_type: CqlType::Boolean,
            },
        ];

        let rows = roles
            .into_iter()
            .map(|role| {
                vec![
                    Some(role.name.into_bytes()),
                    Some(vec![u8::from(role.is_superuser)]),
                    Some(vec![u8::from(role.can_login)]),
                ]
            })
            .collect();

        Ok(QueryResult::Rows {
            columns,
            rows,
            paging_state: None,
            warnings: Vec::new(),
        })
    }

    fn execute_list_permissions(
        &self,
        plan: &ListPermissionsPlan,
        user: Option<&str>,
    ) -> Result<QueryResult, ExecutorError> {
        let role = plan
            .of_role
            .as_deref()
            .or(user)
            .unwrap_or("cassandra")
            .to_string();
        let resource = match &plan.resource {
            Some(resource) => ast_resource_to_security(resource)?,
            None => SecurityResource::Root,
        };
        let requested = requested_permissions(&plan.permissions)?;
        let mut permissions = self.authorizer.list_permissions(&role, &resource);
        permissions.retain(|(permission, _)| {
            requested
                .as_ref()
                .is_none_or(|requested| requested.contains(permission))
        });
        permissions.sort_by(|(left_perm, left_res), (right_perm, right_res)| {
            left_res
                .to_string()
                .cmp(&right_res.to_string())
                .then_with(|| left_perm.to_string().cmp(&right_perm.to_string()))
        });

        let columns = vec![
            ResultColumn {
                keyspace: "system_auth".to_string(),
                table: "role_permissions".to_string(),
                name: "role".to_string(),
                cql_type: CqlType::Varchar,
            },
            ResultColumn {
                keyspace: "system_auth".to_string(),
                table: "role_permissions".to_string(),
                name: "resource".to_string(),
                cql_type: CqlType::Varchar,
            },
            ResultColumn {
                keyspace: "system_auth".to_string(),
                table: "role_permissions".to_string(),
                name: "permission".to_string(),
                cql_type: CqlType::Varchar,
            },
        ];
        let rows = permissions
            .into_iter()
            .map(|(permission, resource)| {
                vec![
                    Some(role.clone().into_bytes()),
                    Some(resource.to_string().into_bytes()),
                    Some(permission.to_string().into_bytes()),
                ]
            })
            .collect();

        Ok(QueryResult::Rows {
            columns,
            rows,
            paging_state: None,
            warnings: Vec::new(),
        })
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
                for ks_meta in snapshot.keyspaces.values() {
                    output.push_str(&Self::render_keyspace_ddl(ks_meta));
                    output.push_str("\n\n");
                    for user_type in ks_meta.types.values() {
                        output.push_str(&Self::render_type_ddl(user_type));
                        output.push_str("\n\n");
                    }
                    for function in ks_meta.functions.values() {
                        output.push_str(&Self::render_function_ddl(function));
                        output.push_str("\n\n");
                    }
                    for aggregate in ks_meta.aggregates.values() {
                        output.push_str(&Self::render_aggregate_ddl(aggregate));
                        output.push_str("\n\n");
                    }
                    for table_meta in ks_meta.tables.values() {
                        output.push_str(&Self::render_table_ddl(table_meta));
                        output.push_str("\n\n");
                    }
                }
                output
            }
            DescribeTarget::Keyspace(name) => {
                let Some(ks_meta) = snapshot.keyspace(name) else {
                    return Err(ExecutorError::InvalidQuery(format!(
                        "Keyspace '{}' not found",
                        name
                    )));
                };
                let mut output = Self::render_keyspace_ddl(ks_meta);
                for user_type in ks_meta.types.values() {
                    output.push_str("\n\n");
                    output.push_str(&Self::render_type_ddl(user_type));
                }
                for function in ks_meta.functions.values() {
                    output.push_str("\n\n");
                    output.push_str(&Self::render_function_ddl(function));
                }
                for aggregate in ks_meta.aggregates.values() {
                    output.push_str("\n\n");
                    output.push_str(&Self::render_aggregate_ddl(aggregate));
                }
                for table_meta in ks_meta.tables.values() {
                    output.push_str("\n\n");
                    output.push_str(&Self::render_table_ddl(table_meta));
                }
                output
            }
            DescribeTarget::Table(ks_opt, table_name) => {
                let ks = ks_opt.as_deref().unwrap_or("system");
                let Some(table_meta) = snapshot.table(ks, table_name) else {
                    return Err(ExecutorError::TableNotFound(
                        ks.to_string(),
                        table_name.clone(),
                    ));
                };
                Self::render_table_ddl(table_meta)
            }
            DescribeTarget::Type(ks_opt, name) => {
                let ks = ks_opt.as_deref().unwrap_or("system");
                let Some(user_type) = snapshot.keyspace(ks).and_then(|keyspace| {
                    keyspace
                        .types
                        .values()
                        .find(|user_type| user_type.name == *name)
                }) else {
                    return Err(ExecutorError::InvalidQuery(format!(
                        "Type '{}.{}' not found",
                        ks, name
                    )));
                };
                Self::render_type_ddl(user_type)
            }
            DescribeTarget::Function(ks_opt, name) => {
                let ks = ks_opt.as_deref().unwrap_or("system");
                let Some(function) = snapshot.keyspace(ks).and_then(|keyspace| {
                    keyspace
                        .functions
                        .values()
                        .find(|function| function.name == *name)
                }) else {
                    return Err(ExecutorError::InvalidQuery(format!(
                        "Function '{}.{}' not found",
                        ks, name
                    )));
                };
                Self::render_function_ddl(function)
            }
            DescribeTarget::Aggregate(ks_opt, name) => {
                let ks = ks_opt.as_deref().unwrap_or("system");
                let Some(aggregate) = snapshot.keyspace(ks).and_then(|keyspace| {
                    keyspace
                        .aggregates
                        .values()
                        .find(|aggregate| aggregate.name == *name)
                }) else {
                    return Err(ExecutorError::InvalidQuery(format!(
                        "Aggregate '{}.{}' not found",
                        ks, name
                    )));
                };
                Self::render_aggregate_ddl(aggregate)
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

        Ok(QueryResult::Rows {
            columns,
            rows,
            paging_state: None,
            warnings: Vec::new(),
        })
    }

    fn render_keyspace_ddl(keyspace: &KeyspaceMetadata) -> String {
        let mut replication = Vec::new();
        replication.push(format!(
            "'class': '{}'",
            Self::short_strategy_name(&keyspace.params.replication.strategy_class)
        ));
        for (key, value) in &keyspace.params.replication.options {
            replication.push(format!("'{}': '{}'", key, value));
        }
        let mut ddl = format!(
            "CREATE KEYSPACE {} WITH replication = {{{}}} AND durable_writes = {};",
            keyspace.name,
            replication.join(", "),
            keyspace.params.durable_writes
        );
        if !keyspace.params.comment.is_empty() {
            ddl.push_str(&format!(
                "\nCOMMENT ON KEYSPACE {} IS '{}';",
                keyspace.name,
                keyspace.params.comment.replace('\'', "''")
            ));
        }
        ddl
    }

    fn render_table_ddl(table: &TableMetadata) -> String {
        let mut lines = Vec::new();
        let mut columns = table.columns.clone();
        columns.sort_by(|left, right| {
            Self::column_render_rank(left.kind)
                .cmp(&Self::column_render_rank(right.kind))
                .then_with(|| left.position.cmp(&right.position))
                .then_with(|| left.name.cmp(&right.name))
        });
        for column in columns {
            let static_suffix = if column.kind == ColumnKind::Static {
                " static"
            } else {
                ""
            };
            lines.push(format!(
                "    {} {}{}",
                column.name,
                column.column_type.cql_name(),
                static_suffix
            ));
        }
        lines.push(format!(
            "    PRIMARY KEY ({})",
            Self::render_primary_key(table)
        ));

        let mut ddl = format!(
            "CREATE TABLE {}.{} (\n{}\n)",
            table.keyspace,
            table.name,
            lines.join(",\n")
        );
        let clustering_order = Self::render_clustering_order(table);
        if !clustering_order.is_empty() {
            ddl.push_str(&format!(" WITH CLUSTERING ORDER BY ({})", clustering_order));
        }
        ddl.push(';');
        for column in table
            .columns
            .iter()
            .filter(|column| !column.comment.is_empty())
        {
            ddl.push_str(&format!(
                "\nCOMMENT ON COLUMN {}.{}.{} IS '{}';",
                table.keyspace,
                table.name,
                column.name,
                column.comment.replace('\'', "''")
            ));
        }
        ddl
    }

    fn render_type_ddl(user_type: &UserType) -> String {
        let fields = user_type
            .field_names
            .iter()
            .zip(user_type.field_types.iter())
            .map(|(name, cql_type)| format!("    {} {}", name, cql_type))
            .collect::<Vec<_>>()
            .join(",\n");
        let mut ddl = format!(
            "CREATE TYPE {}.{} (\n{}\n);",
            user_type.keyspace, user_type.name, fields
        );
        if !user_type.comment.is_empty() {
            ddl.push_str(&format!(
                "\nCOMMENT ON TYPE {}.{} IS '{}';",
                user_type.keyspace,
                user_type.name,
                user_type.comment.replace('\'', "''")
            ));
        }
        for field_name in &user_type.field_names {
            if let Some(comment) = user_type.field_comments.get(field_name) {
                if !comment.is_empty() {
                    ddl.push_str(&format!(
                        "\nCOMMENT ON FIELD {}.{}.{} IS '{}';",
                        user_type.keyspace,
                        user_type.name,
                        field_name,
                        comment.replace('\'', "''")
                    ));
                }
            }
        }
        ddl
    }

    fn render_function_ddl(function: &UserFunction) -> String {
        let args = function
            .arg_names
            .iter()
            .zip(function.arg_types.iter())
            .map(|(name, cql_type)| format!("{} {}", name, cql_type))
            .collect::<Vec<_>>()
            .join(", ");
        let null_clause = if function.called_on_null_input {
            "CALLED ON NULL INPUT"
        } else {
            "RETURNS NULL ON NULL INPUT"
        };
        format!(
            "CREATE FUNCTION {}.{}({})\n    {} RETURNS {}\n    LANGUAGE {}\n    AS '{}';",
            function.keyspace,
            function.name,
            args,
            null_clause,
            function.return_type,
            function.language,
            function.body.replace('\'', "''")
        )
    }

    fn render_aggregate_ddl(aggregate: &UserAggregate) -> String {
        let mut ddl = format!(
            "CREATE AGGREGATE {}.{}({})\n    SFUNC {}\n    STYPE {}",
            aggregate.keyspace,
            aggregate.name,
            aggregate.arg_types.join(", "),
            aggregate.sfunc,
            aggregate.state_type
        );
        if let Some(finalfunc) = &aggregate.finalfunc {
            ddl.push_str(&format!("\n    FINALFUNC {}", finalfunc));
        }
        if !aggregate.return_type.is_empty() && aggregate.return_type != aggregate.state_type {
            ddl.push_str(&format!("\n    RETURNS {}", aggregate.return_type));
        }
        if let Some(initcond) = &aggregate.initcond {
            ddl.push_str(&format!("\n    INITCOND {}", initcond));
        }
        ddl.push(';');
        ddl
    }

    fn column_render_rank(kind: ColumnKind) -> u8 {
        match kind {
            ColumnKind::PartitionKey => 0,
            ColumnKind::Clustering => 1,
            ColumnKind::Static => 2,
            ColumnKind::Regular => 3,
        }
    }

    fn render_primary_key(table: &TableMetadata) -> String {
        let partition_key = table.partition_key_columns();
        let clustering = table.clustering_columns();
        let partition = if partition_key.len() > 1 {
            format!(
                "({})",
                partition_key
                    .iter()
                    .map(|column| column.name.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        } else {
            partition_key
                .first()
                .map(|column| column.name.clone())
                .unwrap_or_default()
        };
        let mut parts = vec![partition];
        parts.extend(clustering.iter().map(|column| column.name.clone()));
        parts.join(", ")
    }

    fn render_clustering_order(table: &TableMetadata) -> String {
        table
            .clustering_columns()
            .into_iter()
            .filter_map(|column| match column.clustering_order {
                ClusteringOrder::Asc => Some(format!("{} ASC", column.name)),
                ClusteringOrder::Desc => Some(format!("{} DESC", column.name)),
                ClusteringOrder::None => None,
            })
            .collect::<Vec<_>>()
            .join(", ")
    }

    fn short_strategy_name(strategy: &str) -> &str {
        strategy.rsplit('.').next().unwrap_or(strategy)
    }

    fn resolve_user_aggregate_selector(
        &self,
        keyspace: &str,
        selector: &Selector,
        arg_types: &[CqlType],
        keyspace_meta: &KeyspaceMetadata,
    ) -> Result<Option<ResolvedUserAggregate>, ExecutorError> {
        let Some((name, _arg_selectors)) = user_aggregate_selector_parts(selector) else {
            return Ok(None);
        };
        if is_builtin_aggregate_name(name) {
            return Ok(None);
        }
        if arg_types.is_empty() {
            return Ok(None);
        }
        let arg_type_names = arg_types.iter().map(CqlType::cql_name).collect::<Vec<_>>();
        let Some(metadata) = self.uda_registry.get(keyspace, name, &arg_type_names) else {
            let signature = format!("{}({})", name, arg_type_names.join(", "));
            if keyspace_meta.aggregate(&signature).is_some() {
                return Err(ExecutorError::InvalidQuery(format!(
                    "Aggregate {}.{} cannot execute because runtime metadata is unavailable",
                    keyspace, signature
                )));
            }
            return Ok(None);
        };
        let state_type = parse_cql_type(&metadata.state_type).ok_or_else(|| {
            ExecutorError::InvalidQuery(format!(
                "Aggregate '{}.{}' has unsupported state type {}",
                keyspace, metadata.name, metadata.state_type
            ))
        })?;
        let return_type = parse_cql_type(&metadata.return_type).ok_or_else(|| {
            ExecutorError::InvalidQuery(format!(
                "Aggregate '{}.{}' has unsupported return type {}",
                keyspace, metadata.name, metadata.return_type
            ))
        })?;
        let arg_types = metadata
            .arg_types
            .iter()
            .map(|arg_type| {
                parse_cql_type(arg_type).ok_or_else(|| {
                    ExecutorError::InvalidQuery(format!(
                        "Aggregate '{}.{}' has unsupported argument type {}",
                        keyspace, metadata.name, arg_type
                    ))
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let mut sfunc_args = vec![("state".to_string(), metadata.state_type.clone())];
        for (idx, arg_type) in metadata.arg_types.iter().enumerate() {
            sfunc_args.push((format!("arg{}", idx), arg_type.clone()));
        }
        let (_, sfunc) = self
            .udf_registry
            .get(keyspace, &metadata.sfunc_name, &sfunc_args)
            .ok_or_else(|| {
                ExecutorError::InvalidQuery(format!(
                    "State function {} for aggregate {} cannot execute",
                    metadata.sfunc_name, metadata.name
                ))
            })?;
        let finalfunc = match &metadata.finalfunc_name {
            Some(finalfunc_name) => {
                let finalfunc_args = vec![("state".to_string(), metadata.state_type.clone())];
                let (_, finalfunc) = self
                    .udf_registry
                    .get(keyspace, finalfunc_name, &finalfunc_args)
                    .ok_or_else(|| {
                        ExecutorError::InvalidQuery(format!(
                            "Final function {} for aggregate {} cannot execute",
                            finalfunc_name, metadata.name
                        ))
                    })?;
                Some(finalfunc)
            }
            None => None,
        };
        let init_value = match &metadata.initcond {
            Some(initcond) => initial_udf_value(initcond, &state_type)?,
            None => UdfValue::Null,
        };
        Ok(Some(ResolvedUserAggregate {
            metadata,
            return_type,
            arg_types,
            sfunc,
            finalfunc,
            init_value,
        }))
    }

    fn resolve_user_function_selector(
        &self,
        keyspace: &str,
        selector: &Selector,
        arg_types: &[CqlType],
        keyspace_meta: &KeyspaceMetadata,
    ) -> Result<Option<ResolvedUserFunction>, ExecutorError> {
        let Some((name, _arg_selectors)) = user_function_selector_parts(selector) else {
            return Ok(None);
        };
        if is_builtin_aggregate_name(name) {
            return Ok(None);
        }
        let arg_type_names = arg_types.iter().map(CqlType::cql_name).collect::<Vec<_>>();
        let udf_args = arg_type_names
            .iter()
            .enumerate()
            .map(|(idx, arg_type)| (format!("arg{}", idx), arg_type.clone()))
            .collect::<Vec<_>>();
        let Some((metadata, executor)) = self.udf_registry.get(keyspace, name, &udf_args) else {
            let signature = format!("{}({})", name, arg_type_names.join(", "));
            if keyspace_meta.function(&signature).is_some() {
                return Err(ExecutorError::InvalidQuery(format!(
                    "Function {}.{} cannot execute because runtime metadata is unavailable",
                    keyspace, signature
                )));
            }
            return Ok(None);
        };
        let return_type = parse_cql_type(&metadata.return_type).ok_or_else(|| {
            ExecutorError::InvalidQuery(format!(
                "Function '{}.{}' has unsupported return type {}",
                keyspace, metadata.name, metadata.return_type
            ))
        })?;
        let arg_types = metadata
            .args
            .iter()
            .map(|(_, arg_type)| {
                parse_cql_type(arg_type).ok_or_else(|| {
                    ExecutorError::InvalidQuery(format!(
                        "Function '{}.{}' has unsupported argument type {}",
                        keyspace, metadata.name, arg_type
                    ))
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Some(ResolvedUserFunction {
            metadata,
            return_type,
            arg_types,
            executor,
        }))
    }
}

// ─── Helpers ───────────────────────────────────────────────────────────────

struct ResolvedUserAggregate {
    metadata: UdaMetadata,
    return_type: CqlType,
    arg_types: Vec<CqlType>,
    sfunc: Arc<dyn UdfExecutor>,
    finalfunc: Option<Arc<dyn UdfExecutor>>,
    init_value: UdfValue,
}

struct ResolvedUserFunction {
    metadata: UdfMetadata,
    return_type: CqlType,
    arg_types: Vec<CqlType>,
    executor: Arc<dyn UdfExecutor>,
}

struct ResolvedBuiltinFunction {
    return_type: CqlType,
    function: Arc<dyn CqlFunction>,
}

fn result_column_for_selector(
    selector: &Selector,
    table: &cassandra_schema::table::TableMetadata,
    keyspace: &str,
    table_name: &str,
    user_aggregate: Option<&ResolvedUserAggregate>,
    user_function: Option<&ResolvedUserFunction>,
    builtin_function: Option<&ResolvedBuiltinFunction>,
    keyspace_meta: &KeyspaceMetadata,
    registry: &FunctionRegistry,
) -> Option<ResultColumn> {
    match selector {
        Selector::Column(name) => {
            let col = table.columns.iter().find(|c| c.name == *name)?;
            Some(ResultColumn {
                keyspace: keyspace.to_string(),
                table: table_name.to_string(),
                name: col.name.clone(),
                cql_type: col.column_type.clone(),
            })
        }
        Selector::Alias { selector, alias } => {
            let mut column = result_column_for_selector(
                selector,
                table,
                keyspace,
                table_name,
                user_aggregate,
                user_function,
                builtin_function,
                keyspace_meta,
                registry,
            )?;
            column.name = alias.clone();
            Some(column)
        }
        Selector::Count => Some(ResultColumn {
            keyspace: keyspace.to_string(),
            table: table_name.to_string(),
            name: "count".to_string(),
            cql_type: CqlType::Bigint,
        }),
        Selector::Cast { target, .. } => Some(ResultColumn {
            keyspace: keyspace.to_string(),
            table: table_name.to_string(),
            name: selector_output_name(selector),
            cql_type: target.resolve()?,
        }),
        Selector::WritetimeOrTtl(kind, _) => Some(ResultColumn {
            keyspace: keyspace.to_string(),
            table: table_name.to_string(),
            name: selector_output_name(selector),
            cql_type: if kind.eq_ignore_ascii_case("ttl") {
                CqlType::Int
            } else {
                CqlType::Bigint
            },
        }),
        Selector::Function(name, _) if is_aggregate_name(name) => {
            let arg_type =
                selector_argument_types_with_functions(selector, table, keyspace_meta, registry)
                    .into_iter()
                    .next();
            Some(ResultColumn {
                keyspace: keyspace.to_string(),
                table: table_name.to_string(),
                name: selector_output_name(selector),
                cql_type: aggregate_output_type(name, arg_type.as_ref()),
            })
        }
        Selector::Function(_, _) if user_aggregate.is_some() => {
            let user_aggregate = user_aggregate?;
            Some(ResultColumn {
                keyspace: keyspace.to_string(),
                table: table_name.to_string(),
                name: selector_output_name(selector),
                cql_type: user_aggregate.return_type.clone(),
            })
        }
        Selector::Function(_, _) if user_function.is_some() => {
            let user_function = user_function?;
            Some(ResultColumn {
                keyspace: keyspace.to_string(),
                table: table_name.to_string(),
                name: selector_output_name(selector),
                cql_type: user_function.return_type.clone(),
            })
        }
        Selector::Function(_, _) if builtin_function.is_some() => {
            let builtin_function = builtin_function?;
            Some(ResultColumn {
                keyspace: keyspace.to_string(),
                table: table_name.to_string(),
                name: selector_output_name(selector),
                cql_type: builtin_function.return_type.clone(),
            })
        }
        _ => None,
    }
}

fn selector_output_name(selector: &Selector) -> String {
    match selector {
        Selector::Column(name) => name.clone(),
        Selector::Alias { alias, .. } => alias.clone(),
        Selector::Count => "count".to_string(),
        Selector::Function(name, args) => {
            if is_count_rows_name(name) && args.is_empty() {
                return "count".to_string();
            }
            let args = if args.is_empty() {
                "*".to_string()
            } else {
                args.iter()
                    .map(selector_output_name)
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
            format!("cast({} as {})", selector_output_name(selector), target)
        }
        Selector::WritetimeOrTtl(kind, col) => format!("{}({})", kind, col),
    }
}

fn selector_argument_types_with_functions(
    selector: &Selector,
    table: &cassandra_schema::table::TableMetadata,
    keyspace_meta: &KeyspaceMetadata,
    registry: &FunctionRegistry,
) -> Vec<CqlType> {
    match selector {
        Selector::Function(_, args) => args
            .iter()
            .filter_map(|arg| {
                selector_result_type_with_functions(arg, table, keyspace_meta, registry)
            })
            .collect(),
        Selector::Alias { selector, .. } => {
            selector_argument_types_with_functions(selector, table, keyspace_meta, registry)
        }
        _ => selector_result_type_with_functions(selector, table, keyspace_meta, registry)
            .into_iter()
            .collect(),
    }
}

fn selector_result_type(
    selector: &Selector,
    table: &cassandra_schema::table::TableMetadata,
) -> Option<CqlType> {
    match selector {
        Selector::Column(name) => table.column(name).map(|c| c.column_type.clone()),
        Selector::Alias { selector, .. } => selector_result_type(selector, table),
        Selector::Count => Some(CqlType::Bigint),
        Selector::Cast { target, .. } => target.resolve(),
        Selector::WritetimeOrTtl(kind, _) => {
            if kind.eq_ignore_ascii_case("ttl") {
                Some(CqlType::Int)
            } else {
                Some(CqlType::Bigint)
            }
        }
        Selector::Function(name, _) if is_count_rows_selector_name(name) => Some(CqlType::Bigint),
        Selector::Function(name, args) if is_aggregate_name(name) => {
            let arg_type = args
                .first()
                .and_then(|arg| selector_result_type(arg, table));
            Some(aggregate_output_type(name, arg_type.as_ref()))
        }
        _ => None,
    }
}

fn selector_result_type_with_functions(
    selector: &Selector,
    table: &cassandra_schema::table::TableMetadata,
    keyspace_meta: &KeyspaceMetadata,
    registry: &FunctionRegistry,
) -> Option<CqlType> {
    match selector {
        Selector::Function(name, args)
            if !name.eq_ignore_ascii_case("count") && !is_aggregate_name(name) =>
        {
            let arg_types = args
                .iter()
                .map(|arg| selector_result_type_with_functions(arg, table, keyspace_meta, registry))
                .collect::<Option<Vec<_>>>()?;
            if let Some(function) = registry.resolve(name, &arg_types) {
                return Some(function.return_type());
            }
            let signature = function_signature(name, &arg_types);
            keyspace_meta
                .function(&signature)
                .and_then(|function| parse_cql_type(&function.return_type))
        }
        Selector::Alias { selector, .. } => {
            selector_result_type_with_functions(selector, table, keyspace_meta, registry)
        }
        Selector::Cast { target, .. } => target.resolve(),
        _ => selector_result_type(selector, table),
    }
}

fn select_value_for_selector<F>(
    selector: &Selector,
    partition_key: &[u8],
    row: &Row,
    static_row: Option<&Row>,
    column_value: &F,
    pk_names: &[String],
    ck_names: &[String],
    static_col_names: &[String],
    now_secs: i32,
    table: &cassandra_schema::table::TableMetadata,
    registry: &FunctionRegistry,
    keyspace: &str,
    keyspace_meta: &KeyspaceMetadata,
    udf_registry: &UdfRegistry,
    user_function: Option<&ResolvedUserFunction>,
    builtin_function: Option<&ResolvedBuiltinFunction>,
) -> Result<Option<Vec<u8>>, ExecutorError>
where
    F: Fn(&str, &[u8], &Row, Option<&Row>) -> Option<Vec<u8>>,
{
    match selector {
        Selector::Column(name) => Ok(column_value(name, partition_key, row, static_row)),
        Selector::Alias { selector, .. } => select_value_for_selector(
            selector,
            partition_key,
            row,
            static_row,
            column_value,
            pk_names,
            ck_names,
            static_col_names,
            now_secs,
            table,
            registry,
            keyspace,
            keyspace_meta,
            udf_registry,
            user_function,
            builtin_function,
        ),
        Selector::Cast { selector, target } => cast_selector_value(
            selector,
            target,
            partition_key,
            row,
            static_row,
            column_value,
            pk_names,
            ck_names,
            static_col_names,
            now_secs,
            table,
            registry,
            keyspace,
            keyspace_meta,
            udf_registry,
        ),
        Selector::Function(_, args) => {
            let Some(function) = user_function else {
                let Some(function) = builtin_function else {
                    return Ok(None);
                };
                let arg_values = args
                    .iter()
                    .map(|arg| {
                        select_scalar_argument_value(
                            arg,
                            partition_key,
                            row,
                            static_row,
                            column_value,
                            pk_names,
                            ck_names,
                            static_col_names,
                            now_secs,
                            table,
                            registry,
                            keyspace,
                            keyspace_meta,
                            udf_registry,
                        )
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let arg_refs = arg_values
                    .iter()
                    .map(|value| value.as_deref())
                    .collect::<Vec<_>>();
                return function.function.execute(&arg_refs).map_err(|e| {
                    ExecutorError::InvalidQuery(format!(
                        "Failed to execute built-in function {}: {}",
                        function.function.name(),
                        e
                    ))
                });
            };
            let arg_values = args
                .iter()
                .map(|arg| {
                    select_scalar_argument_value(
                        arg,
                        partition_key,
                        row,
                        static_row,
                        column_value,
                        pk_names,
                        ck_names,
                        static_col_names,
                        now_secs,
                        table,
                        registry,
                        keyspace,
                        keyspace_meta,
                        udf_registry,
                    )
                })
                .collect::<Result<Vec<_>, _>>()?;
            if arg_values.len() != function.arg_types.len() {
                return Err(ExecutorError::InvalidQuery(format!(
                    "Function {} expected {} arguments, got {}",
                    function.metadata.name,
                    function.arg_types.len(),
                    arg_values.len()
                )));
            }
            let args = function
                .arg_types
                .iter()
                .zip(arg_values.iter())
                .map(|(arg_type, value)| {
                    UdfValue::deserialize_arg(arg_type, value.as_deref()).map_err(|e| {
                        ExecutorError::InvalidQuery(format!(
                            "Failed to deserialize function argument: {}",
                            e
                        ))
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            let value = function.executor.execute(&args).map_err(|e| {
                ExecutorError::InvalidQuery(format!(
                    "Failed to execute function {}: {}",
                    function.metadata.name, e
                ))
            })?;
            value.serialize_return(&function.return_type).map_err(|e| {
                ExecutorError::InvalidQuery(format!(
                    "Failed to serialize function result for {}: {}",
                    function.metadata.name, e
                ))
            })
        }
        Selector::WritetimeOrTtl(kind, column) => Ok(writetime_or_ttl_value(
            kind,
            column,
            pk_names,
            ck_names,
            static_col_names,
            row,
            static_row,
            now_secs,
        )),
        Selector::Count => Ok(None),
    }
}

fn select_scalar_argument_value<F>(
    selector: &Selector,
    partition_key: &[u8],
    row: &Row,
    static_row: Option<&Row>,
    column_value: &F,
    pk_names: &[String],
    ck_names: &[String],
    static_col_names: &[String],
    now_secs: i32,
    table: &cassandra_schema::table::TableMetadata,
    registry: &FunctionRegistry,
    keyspace: &str,
    keyspace_meta: &KeyspaceMetadata,
    udf_registry: &UdfRegistry,
) -> Result<Option<Vec<u8>>, ExecutorError>
where
    F: Fn(&str, &[u8], &Row, Option<&Row>) -> Option<Vec<u8>>,
{
    match selector {
        Selector::Column(name) => Ok(column_value(name, partition_key, row, static_row)),
        Selector::Alias { selector, .. } => select_scalar_argument_value(
            selector,
            partition_key,
            row,
            static_row,
            column_value,
            pk_names,
            ck_names,
            static_col_names,
            now_secs,
            table,
            registry,
            keyspace,
            keyspace_meta,
            udf_registry,
        ),
        Selector::Cast { selector, target } => cast_selector_value(
            selector,
            target,
            partition_key,
            row,
            static_row,
            column_value,
            pk_names,
            ck_names,
            static_col_names,
            now_secs,
            table,
            registry,
            keyspace,
            keyspace_meta,
            udf_registry,
        ),
        Selector::Function(name, args) if !is_builtin_aggregate_name(name) => {
            let arg_types = args
                .iter()
                .map(|arg| selector_result_type_with_functions(arg, table, keyspace_meta, registry))
                .collect::<Option<Vec<_>>>()
                .ok_or_else(|| {
                    ExecutorError::InvalidQuery(format!(
                        "Unsupported SELECT selector '{}'",
                        selector_output_name(selector)
                    ))
                })?;
            let arg_values = args
                .iter()
                .map(|arg| {
                    select_scalar_argument_value(
                        arg,
                        partition_key,
                        row,
                        static_row,
                        column_value,
                        pk_names,
                        ck_names,
                        static_col_names,
                        now_secs,
                        table,
                        registry,
                        keyspace,
                        keyspace_meta,
                        udf_registry,
                    )
                })
                .collect::<Result<Vec<_>, _>>()?;
            if let Some(function) = registry.resolve(name, &arg_types) {
                let arg_refs = arg_values
                    .iter()
                    .map(|value| value.as_deref())
                    .collect::<Vec<_>>();
                return function.execute(&arg_refs).map_err(|e| {
                    ExecutorError::InvalidQuery(format!(
                        "Failed to execute built-in function {}: {}",
                        function.name(),
                        e
                    ))
                });
            }

            let udf_args = udf_arg_signature(&arg_types);
            let Some((metadata, executor)) = udf_registry.get(keyspace, name, &udf_args) else {
                let signature = function_signature(name, &arg_types);
                if keyspace_meta.function(&signature).is_some() {
                    return Err(ExecutorError::InvalidQuery(format!(
                        "Function {}.{} cannot execute because runtime metadata is unavailable",
                        keyspace, signature
                    )));
                }
                return Err(ExecutorError::InvalidQuery(format!(
                    "Unsupported SELECT selector '{}'",
                    selector_output_name(selector)
                )));
            };
            let parsed_arg_types = metadata
                .args
                .iter()
                .map(|(_, arg_type)| {
                    parse_cql_type(arg_type).ok_or_else(|| {
                        ExecutorError::InvalidQuery(format!(
                            "Function '{}.{}' has unsupported argument type {}",
                            keyspace, metadata.name, arg_type
                        ))
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            let return_type = parse_cql_type(&metadata.return_type).ok_or_else(|| {
                ExecutorError::InvalidQuery(format!(
                    "Function '{}.{}' has unsupported return type {}",
                    keyspace, metadata.name, metadata.return_type
                ))
            })?;
            let udf_values = parsed_arg_types
                .iter()
                .zip(arg_values.iter())
                .map(|(arg_type, value)| {
                    UdfValue::deserialize_arg(arg_type, value.as_deref()).map_err(|e| {
                        ExecutorError::InvalidQuery(format!(
                            "Failed to deserialize function argument: {}",
                            e
                        ))
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            let value = executor.execute(&udf_values).map_err(|e| {
                ExecutorError::InvalidQuery(format!(
                    "Failed to execute function {}: {}",
                    metadata.name, e
                ))
            })?;
            value.serialize_return(&return_type).map_err(|e| {
                ExecutorError::InvalidQuery(format!(
                    "Failed to serialize function result for {}: {}",
                    metadata.name, e
                ))
            })
        }
        Selector::WritetimeOrTtl(kind, column) => Ok(writetime_or_ttl_value(
            kind,
            column,
            pk_names,
            ck_names,
            static_col_names,
            row,
            static_row,
            now_secs,
        )),
        Selector::Count | Selector::Function(_, _) => Ok(None),
    }
}

fn cast_selector_value<F>(
    selector: &Selector,
    target: &cassandra_cql::ast::CqlTypeName,
    partition_key: &[u8],
    row: &Row,
    static_row: Option<&Row>,
    column_value: &F,
    pk_names: &[String],
    ck_names: &[String],
    static_col_names: &[String],
    now_secs: i32,
    table: &cassandra_schema::table::TableMetadata,
    registry: &FunctionRegistry,
    keyspace: &str,
    keyspace_meta: &KeyspaceMetadata,
    udf_registry: &UdfRegistry,
) -> Result<Option<Vec<u8>>, ExecutorError>
where
    F: Fn(&str, &[u8], &Row, Option<&Row>) -> Option<Vec<u8>>,
{
    let source_type = selector_result_type_with_functions(selector, table, keyspace_meta, registry)
        .ok_or_else(|| {
            ExecutorError::InvalidQuery(format!(
                "Unsupported CAST source selector '{}'",
                selector_output_name(selector)
            ))
        })?;
    let target_type = target.resolve().ok_or_else(|| {
        ExecutorError::InvalidQuery(format!("Unsupported CAST target type '{:?}'", target))
    })?;
    let value = select_scalar_argument_value(
        selector,
        partition_key,
        row,
        static_row,
        column_value,
        pk_names,
        ck_names,
        static_col_names,
        now_secs,
        table,
        registry,
        keyspace,
        keyspace_meta,
        udf_registry,
    )?;
    if source_type == target_type {
        return Ok(value);
    }
    let Some(function) = registry.resolve_with_return("cast", &[source_type], &target_type) else {
        return Err(ExecutorError::InvalidQuery(format!(
            "Unsupported CAST from {} to {}",
            selector_result_type_with_functions(selector, table, keyspace_meta, registry)
                .map(|ty| ty.cql_name())
                .unwrap_or_else(|| "unknown".to_string()),
            target_type.cql_name()
        )));
    };
    let arg_refs = [value.as_deref()];
    function.execute(&arg_refs).map_err(|e| {
        ExecutorError::InvalidQuery(format!(
            "Failed to execute CAST to {}: {}",
            target_type.cql_name(),
            e
        ))
    })
}

fn writetime_or_ttl_value(
    kind: &str,
    column: &str,
    pk_names: &[String],
    ck_names: &[String],
    static_col_names: &[String],
    row: &Row,
    static_row: Option<&Row>,
    now_secs: i32,
) -> Option<Vec<u8>> {
    let cell = cell_for_column(
        column,
        pk_names,
        ck_names,
        static_col_names,
        row,
        static_row,
        now_secs,
    )?;
    match kind.to_ascii_lowercase().as_str() {
        "writetime" | "maxwritetime" => Some(cell.timestamp.to_be_bytes().to_vec()),
        "ttl" if cell.ttl > 0 => Some(cell.ttl.to_be_bytes().to_vec()),
        "ttl" => None,
        _ => None,
    }
}

fn cell_for_column<'a>(
    column: &str,
    pk_names: &[String],
    ck_names: &[String],
    static_col_names: &[String],
    row: &'a Row,
    static_row: Option<&'a Row>,
    now_secs: i32,
) -> Option<&'a Cell> {
    if pk_names.iter().any(|name| name == column) || ck_names.iter().any(|name| name == column) {
        return None;
    }
    let cells = if static_col_names.iter().any(|name| name == column) {
        static_row.map(|row| row.cells.as_slice()).unwrap_or(&[])
    } else {
        row.cells.as_slice()
    };
    cells
        .iter()
        .find(|cell| cell.column == column && cell.is_live_at(now_secs))
}

fn aggregate_output_type(name: &str, arg_type: Option<&CqlType>) -> CqlType {
    match name.to_ascii_lowercase().as_str() {
        "count" | "count_rows" | "countrows" => CqlType::Bigint,
        "sum" | "avg" => arg_type.cloned().unwrap_or(CqlType::Blob),
        "min" | "max" => arg_type.cloned().unwrap_or(CqlType::Blob),
        _ => CqlType::Blob,
    }
}

fn function_signature(name: &str, arg_types: &[CqlType]) -> String {
    let arg_type_names = arg_types.iter().map(CqlType::cql_name).collect::<Vec<_>>();
    format!("{}({})", name, arg_type_names.join(", "))
}

fn role_network_permissions(
    datacenter_access: Option<&RoleAccess>,
    cidr_access: Option<&RoleAccess>,
) -> Option<NetworkPermissions> {
    if datacenter_access.is_none() && cidr_access.is_none() {
        return None;
    }

    let (all_datacenters, allowed_datacenters) = match datacenter_access {
        Some(RoleAccess::All) => (true, Vec::new()),
        Some(RoleAccess::Restricted(values)) => (false, values.clone()),
        None => (true, Vec::new()),
    };
    let (all_cidrs, allowed_cidrs) = match cidr_access {
        Some(RoleAccess::All) => (true, Vec::new()),
        Some(RoleAccess::Restricted(values)) => (false, values.clone()),
        None => (true, Vec::new()),
    };

    Some(NetworkPermissions {
        all_datacenters,
        allowed_datacenters,
        all_cidrs,
        allowed_cidrs,
    })
}

fn udf_arg_signature(arg_types: &[CqlType]) -> Vec<(String, String)> {
    arg_types
        .iter()
        .enumerate()
        .map(|(idx, arg_type)| (format!("arg{}", idx), arg_type.cql_name()))
        .collect()
}

fn canonical_ast_cql_type_name(cql_type: &cassandra_cql::ast::CqlTypeName) -> String {
    cql_type
        .resolve()
        .map(|resolved| resolved.cql_name())
        .unwrap_or_else(|| canonical_cql_type_name(&render_ast_cql_type_name(cql_type)))
}

fn render_ast_cql_type_name(cql_type: &cassandra_cql::ast::CqlTypeName) -> String {
    match cql_type {
        cassandra_cql::ast::CqlTypeName::Simple(name) => name.clone(),
        cassandra_cql::ast::CqlTypeName::List(inner) => {
            format!("list<{}>", render_ast_cql_type_name(inner))
        }
        cassandra_cql::ast::CqlTypeName::Set(inner) => {
            format!("set<{}>", render_ast_cql_type_name(inner))
        }
        cassandra_cql::ast::CqlTypeName::Map(key, value) => {
            format!(
                "map<{}, {}>",
                render_ast_cql_type_name(key),
                render_ast_cql_type_name(value)
            )
        }
        cassandra_cql::ast::CqlTypeName::Tuple(types) => {
            let inner = types
                .iter()
                .map(render_ast_cql_type_name)
                .collect::<Vec<_>>()
                .join(", ");
            format!("tuple<{}>", inner)
        }
        cassandra_cql::ast::CqlTypeName::Frozen(inner) => {
            format!("frozen<{}>", render_ast_cql_type_name(inner))
        }
        cassandra_cql::ast::CqlTypeName::Vector(inner, dimensions) => {
            format!(
                "vector<{}, {}>",
                render_ast_cql_type_name(inner),
                dimensions
            )
        }
    }
}

fn cql_type_name_is_compatible(new_type: &str, existing_type: &str) -> bool {
    let new_type = canonical_cql_type_name(new_type);
    let existing_type = canonical_cql_type_name(existing_type);
    match (parse_cql_type(&new_type), parse_cql_type(&existing_type)) {
        (Some(new_type), Some(existing_type)) => is_compatible_with(&new_type, &existing_type),
        _ => new_type == existing_type,
    }
}

fn validate_aggregate_initcond(initcond: &str, state_type: &str) -> Result<(), ExecutorError> {
    let Some(state_type) = parse_cql_type(state_type) else {
        return Ok(());
    };
    let term = parser::parse_term(initcond).map_err(|_| {
        ExecutorError::InvalidQuery(format!(
            "Invalid value for INITCOND of type {}",
            state_type.cql_name()
        ))
    })?;
    if matches!(term, Term::Literal(Literal::Null)) {
        return Ok(());
    }
    if !term_matches_cql_type(&term, &state_type) {
        return Err(ExecutorError::InvalidQuery(format!(
            "Invalid value for INITCOND of type {}",
            state_type.cql_name()
        )));
    }
    let bytes = typed_term_to_bytes(&term, &state_type).ok_or_else(|| {
        ExecutorError::InvalidQuery(format!(
            "Invalid value for INITCOND of type {}",
            state_type.cql_name()
        ))
    })?;
    if bytes.is_empty()
        && !matches!(
            state_type,
            CqlType::Ascii | CqlType::Varchar | CqlType::Blob
        )
    {
        return Err(ExecutorError::InvalidQuery(
            "INITCOND must not be empty for all types except TEXT, ASCII, BLOB".to_string(),
        ));
    }
    Ok(())
}

fn initial_udf_value(initcond: &str, state_type: &CqlType) -> Result<UdfValue, ExecutorError> {
    let term = parser::parse_term(initcond).map_err(|_| {
        ExecutorError::InvalidQuery(format!(
            "Invalid value for INITCOND of type {}",
            state_type.cql_name()
        ))
    })?;
    if matches!(term, Term::Literal(Literal::Null)) {
        return Ok(UdfValue::Null);
    }
    let bytes = typed_term_to_bytes(&term, state_type).ok_or_else(|| {
        ExecutorError::InvalidQuery(format!(
            "Invalid value for INITCOND of type {}",
            state_type.cql_name()
        ))
    })?;
    UdfValue::deserialize_arg(state_type, Some(&bytes)).map_err(|e| {
        ExecutorError::InvalidQuery(format!(
            "Invalid value for INITCOND of type {}: {}",
            state_type.cql_name(),
            e
        ))
    })
}

fn term_matches_cql_type(term: &Term, cql_type: &CqlType) -> bool {
    match term {
        Term::Literal(Literal::Null) => true,
        Term::Literal(Literal::Integer(_)) => matches!(
            cql_type,
            CqlType::Tinyint
                | CqlType::Smallint
                | CqlType::Int
                | CqlType::Bigint
                | CqlType::Counter
                | CqlType::Varint
                | CqlType::Float
                | CqlType::Double
                | CqlType::Decimal
                | CqlType::Timestamp
                | CqlType::Date
                | CqlType::Time
        ),
        Term::Literal(Literal::Float(_)) => {
            matches!(
                cql_type,
                CqlType::Float | CqlType::Double | CqlType::Decimal
            )
        }
        Term::Literal(Literal::String(_)) => matches!(cql_type, CqlType::Ascii | CqlType::Varchar),
        Term::Literal(Literal::Blob(_)) => matches!(cql_type, CqlType::Blob),
        Term::Literal(Literal::Uuid(_)) => matches!(cql_type, CqlType::Uuid | CqlType::Timeuuid),
        Term::Literal(Literal::Boolean(_)) => matches!(cql_type, CqlType::Boolean),
        Term::TypeHint(type_hint, inner) => type_hint.resolve().is_some_and(|hinted_type| {
            is_compatible_with(&hinted_type, cql_type) && term_matches_cql_type(inner, &hinted_type)
        }),
        Term::CollectionElement { key, value } => match cql_type {
            CqlType::List(inner, _) => {
                term_matches_cql_type(key, &CqlType::Int) && term_matches_cql_type(value, inner)
            }
            CqlType::Map(key_type, value_type, _) => {
                term_matches_cql_type(key, key_type) && term_matches_cql_type(value, value_type)
            }
            _ => false,
        },
        Term::CollectionLiteral(values) => match cql_type {
            CqlType::List(inner, _) | CqlType::Set(inner, _) => values
                .iter()
                .all(|value| term_matches_cql_type(value, inner)),
            CqlType::Vector(inner, dimensions) => {
                values.len() == *dimensions as usize
                    && values
                        .iter()
                        .all(|value| term_matches_cql_type(value, inner))
            }
            _ => false,
        },
        Term::MapLiteral(values) => match cql_type {
            CqlType::Map(key_type, value_type, _) => values.iter().all(|(key, value)| {
                term_matches_cql_type(key, key_type) && term_matches_cql_type(value, value_type)
            }),
            _ => false,
        },
        Term::TupleLiteral(values) => match cql_type {
            CqlType::Tuple(field_types) => {
                values.len() == field_types.len()
                    && values
                        .iter()
                        .zip(field_types)
                        .all(|(value, field_type)| term_matches_cql_type(value, field_type))
            }
            _ => false,
        },
        Term::BindMarker(_) | Term::FunctionCall(_, _) => false,
    }
}

fn finalize_aggregate_rows(
    selectors: &[Selector],
    selector_arg_types: &[Option<CqlType>],
    user_aggregates: &[Option<ResolvedUserAggregate>],
    inputs: Vec<(Vec<Option<Vec<u8>>>, Vec<Vec<Option<Vec<u8>>>>)>,
) -> Result<Vec<Vec<Option<Vec<u8>>>>, ExecutorError> {
    let mut groups: BTreeMap<Vec<Option<Vec<u8>>>, Vec<AggregateState>> = BTreeMap::new();

    if inputs.is_empty() {
        return Ok(vec![
            aggregate_initial_states(selectors, selector_arg_types, user_aggregates)
                .into_iter()
                .map(AggregateState::finalize)
                .collect::<Result<Vec<_>, _>>()?,
        ]);
    }

    for (group_key, values) in inputs {
        if !groups.contains_key(&group_key) {
            groups.insert(
                group_key.clone(),
                aggregate_initial_states(selectors, selector_arg_types, user_aggregates),
            );
        }
        let states = groups.get_mut(&group_key).expect("inserted group state");
        for (idx, state) in states.iter_mut().enumerate() {
            state.accumulate(values.get(idx).map(Vec::as_slice).unwrap_or(&[]))?;
        }
    }

    groups
        .into_values()
        .map(|states| {
            states
                .into_iter()
                .map(AggregateState::finalize)
                .collect::<Result<Vec<_>, _>>()
        })
        .collect()
}

fn aggregate_initial_states(
    selectors: &[Selector],
    selector_arg_types: &[Option<CqlType>],
    user_aggregates: &[Option<ResolvedUserAggregate>],
) -> Vec<AggregateState> {
    selectors
        .iter()
        .enumerate()
        .map(|(idx, selector)| {
            let arg_type = selector_arg_types.get(idx).and_then(|t| t.clone());
            if let Some(Some(user_aggregate)) = user_aggregates.get(idx) {
                return AggregateState::UserDefined {
                    state: cassandra_cql::uda::AggregateState::new(
                        user_aggregate.metadata.clone(),
                        Arc::clone(&user_aggregate.sfunc),
                        user_aggregate.finalfunc.as_ref().map(Arc::clone),
                        user_aggregate.init_value.clone(),
                    ),
                    arg_types: user_aggregate.arg_types.clone(),
                    return_type: user_aggregate.return_type.clone(),
                };
            }
            match aggregate_selector_name(selector).as_deref() {
                Some("count") => AggregateState::Count {
                    count: 0,
                    count_non_null: count_selector_counts_non_null(selector),
                },
                Some("sum") => AggregateState::Sum {
                    accumulator: SumAccumulator::new(arg_type.unwrap_or(CqlType::Blob)),
                },
                Some("avg") => AggregateState::Avg {
                    accumulator: AvgAccumulator::new(arg_type.unwrap_or(CqlType::Blob)),
                },
                Some("min") => AggregateState::MinMax {
                    value: None,
                    cql_type: arg_type.unwrap_or(CqlType::Blob),
                    pick_max: false,
                },
                Some("max") => AggregateState::MinMax {
                    value: None,
                    cql_type: arg_type.unwrap_or(CqlType::Blob),
                    pick_max: true,
                },
                _ => AggregateState::PassThrough { value: None },
            }
        })
        .collect()
}

enum AggregateState {
    PassThrough {
        value: Option<Vec<u8>>,
    },
    Count {
        count: i64,
        count_non_null: bool,
    },
    Sum {
        accumulator: SumAccumulator,
    },
    Avg {
        accumulator: AvgAccumulator,
    },
    MinMax {
        value: Option<Vec<u8>>,
        cql_type: CqlType,
        pick_max: bool,
    },
    UserDefined {
        state: cassandra_cql::uda::AggregateState,
        arg_types: Vec<CqlType>,
        return_type: CqlType,
    },
}

impl AggregateState {
    fn accumulate(&mut self, values: &[Option<Vec<u8>>]) -> Result<(), ExecutorError> {
        match self {
            AggregateState::PassThrough { value: stored } => {
                let value = values.first().and_then(|value| value.as_ref());
                if let Some(value) = value {
                    *stored = Some(value.clone());
                }
            }
            AggregateState::Count {
                count,
                count_non_null,
            } => {
                if !*count_non_null || values.iter().any(Option::is_some) {
                    *count += 1;
                }
            }
            AggregateState::Sum { accumulator } => {
                let value = values.first().and_then(|value| value.as_ref());
                if let Some(value) = value {
                    accumulator
                        .add(value)
                        .map_err(ExecutorError::InvalidQuery)?;
                }
            }
            AggregateState::Avg { accumulator } => {
                let value = values.first().and_then(|value| value.as_ref());
                if let Some(value) = value {
                    accumulator
                        .add(value)
                        .map_err(ExecutorError::InvalidQuery)?;
                }
            }
            AggregateState::MinMax {
                value: stored,
                cql_type,
                pick_max,
            } => {
                let value = values.first().and_then(|value| value.as_ref());
                let Some(value) = value else {
                    return Ok(());
                };
                let replace = stored.as_ref().is_none_or(|stored| {
                    let ordering =
                        cassandra_types::comparator::compare_bytes(cql_type, value, stored);
                    if *pick_max {
                        ordering.is_gt()
                    } else {
                        ordering.is_lt()
                    }
                });
                if replace {
                    *stored = Some(value.clone());
                }
            }
            AggregateState::UserDefined {
                state, arg_types, ..
            } => {
                if values.len() != arg_types.len() {
                    return Err(ExecutorError::InvalidQuery(format!(
                        "Aggregate expected {} arguments, got {}",
                        arg_types.len(),
                        values.len()
                    )));
                }
                let args = arg_types
                    .iter()
                    .zip(values.iter())
                    .map(|(arg_type, value)| {
                        UdfValue::deserialize_arg(arg_type, value.as_deref()).map_err(|e| {
                            ExecutorError::InvalidQuery(format!(
                                "Failed to deserialize aggregate argument: {}",
                                e
                            ))
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                state.accumulate(&args).map_err(|e| {
                    ExecutorError::InvalidQuery(format!(
                        "Failed to execute aggregate state function: {}",
                        e
                    ))
                })?;
            }
        }
        Ok(())
    }

    fn finalize(self) -> Result<Option<Vec<u8>>, ExecutorError> {
        match self {
            AggregateState::PassThrough { value } => Ok(value),
            AggregateState::Count { count, .. } => Ok(Some(count.to_be_bytes().to_vec())),
            AggregateState::Sum { accumulator } => accumulator
                .finalize()
                .map(Some)
                .map_err(ExecutorError::InvalidQuery),
            AggregateState::Avg { accumulator } => accumulator
                .finalize()
                .map(Some)
                .map_err(ExecutorError::InvalidQuery),
            AggregateState::MinMax { value, .. } => Ok(value),
            AggregateState::UserDefined {
                state, return_type, ..
            } => {
                let value = state.finalize().map_err(|e| {
                    ExecutorError::InvalidQuery(format!(
                        "Failed to execute aggregate final function: {}",
                        e
                    ))
                })?;
                value.serialize_return(&return_type).map_err(|e| {
                    ExecutorError::InvalidQuery(format!(
                        "Failed to serialize aggregate result: {}",
                        e
                    ))
                })
            }
        }
    }
}

enum SumAccumulator {
    Tinyint(i8),
    Smallint(i16),
    Int(i32),
    Bigint(i64),
    Counter(i64),
    Float(KahanAccumulator),
    Double(KahanAccumulator),
    Varint(BigInt),
    Decimal(DecimalValue),
    Unsupported(CqlType),
}

impl SumAccumulator {
    fn new(cql_type: CqlType) -> Self {
        match cql_type {
            CqlType::Tinyint => Self::Tinyint(0),
            CqlType::Smallint => Self::Smallint(0),
            CqlType::Int => Self::Int(0),
            CqlType::Bigint => Self::Bigint(0),
            CqlType::Counter => Self::Counter(0),
            CqlType::Float => Self::Float(KahanAccumulator::default()),
            CqlType::Double => Self::Double(KahanAccumulator::default()),
            CqlType::Varint => Self::Varint(BigInt::zero()),
            CqlType::Decimal => Self::Decimal(DecimalValue::from_i64(0)),
            other => Self::Unsupported(other),
        }
    }

    fn add(&mut self, value: &[u8]) -> Result<(), String> {
        match self {
            Self::Tinyint(total) => *total = total.wrapping_add(read_i8_value(value)?),
            Self::Smallint(total) => *total = total.wrapping_add(read_i16_value(value)?),
            Self::Int(total) => *total = total.wrapping_add(read_i32_value(value)?),
            Self::Bigint(total) | Self::Counter(total) => {
                *total = total.wrapping_add(read_i64_value(value)?)
            }
            Self::Float(total) => total.add(read_f32_value(value)? as f64),
            Self::Double(total) => total.add(read_f64_value(value)?),
            Self::Varint(total) => *total += BigInt::from_signed_bytes_be(value),
            Self::Decimal(total) => *total = total.add(&DecimalValue::from_bytes(value)?),
            Self::Unsupported(cql_type) => {
                return Err(format!("unsupported sum type {}", cql_type.cql_name()));
            }
        }
        Ok(())
    }

    fn finalize(self) -> Result<Vec<u8>, String> {
        Ok(match self {
            Self::Tinyint(total) => vec![total as u8],
            Self::Smallint(total) => total.to_be_bytes().to_vec(),
            Self::Int(total) => total.to_be_bytes().to_vec(),
            Self::Bigint(total) | Self::Counter(total) => total.to_be_bytes().to_vec(),
            Self::Float(total) => (total.compute() as f32).to_be_bytes().to_vec(),
            Self::Double(total) => total.compute().to_be_bytes().to_vec(),
            Self::Varint(total) => total.to_signed_bytes_be(),
            Self::Decimal(total) => total.to_bytes(),
            Self::Unsupported(cql_type) => {
                return Err(format!("unsupported sum type {}", cql_type.cql_name()));
            }
        })
    }
}

enum AvgAccumulator {
    Tinyint { sum: BigInt, count: i64 },
    Smallint { sum: BigInt, count: i64 },
    Int { sum: BigInt, count: i64 },
    Bigint { sum: BigInt, count: i64 },
    Counter { sum: BigInt, count: i64 },
    Float { sum: KahanAccumulator, count: i64 },
    Double { sum: KahanAccumulator, count: i64 },
    Varint { sum: BigInt, count: i64 },
    Decimal { avg: DecimalValue, count: i64 },
    Unsupported(CqlType),
}

impl AvgAccumulator {
    fn new(cql_type: CqlType) -> Self {
        match cql_type {
            CqlType::Tinyint => Self::Tinyint {
                sum: BigInt::zero(),
                count: 0,
            },
            CqlType::Smallint => Self::Smallint {
                sum: BigInt::zero(),
                count: 0,
            },
            CqlType::Int => Self::Int {
                sum: BigInt::zero(),
                count: 0,
            },
            CqlType::Bigint => Self::Bigint {
                sum: BigInt::zero(),
                count: 0,
            },
            CqlType::Counter => Self::Counter {
                sum: BigInt::zero(),
                count: 0,
            },
            CqlType::Float => Self::Float {
                sum: KahanAccumulator::default(),
                count: 0,
            },
            CqlType::Double => Self::Double {
                sum: KahanAccumulator::default(),
                count: 0,
            },
            CqlType::Varint => Self::Varint {
                sum: BigInt::zero(),
                count: 0,
            },
            CqlType::Decimal => Self::Decimal {
                avg: DecimalValue::from_i64(0),
                count: 0,
            },
            other => Self::Unsupported(other),
        }
    }

    fn add(&mut self, value: &[u8]) -> Result<(), String> {
        match self {
            Self::Tinyint { sum, count } => {
                *sum += BigInt::from(read_i8_value(value)?);
                *count += 1;
            }
            Self::Smallint { sum, count } => {
                *sum += BigInt::from(read_i16_value(value)?);
                *count += 1;
            }
            Self::Int { sum, count } => {
                *sum += BigInt::from(read_i32_value(value)?);
                *count += 1;
            }
            Self::Bigint { sum, count } | Self::Counter { sum, count } => {
                *sum += BigInt::from(read_i64_value(value)?);
                *count += 1;
            }
            Self::Float { sum, count } => {
                sum.add(read_f32_value(value)? as f64);
                *count += 1;
            }
            Self::Double { sum, count } => {
                sum.add(read_f64_value(value)?);
                *count += 1;
            }
            Self::Varint { sum, count } => {
                *sum += BigInt::from_signed_bytes_be(value);
                *count += 1;
            }
            Self::Decimal { avg, count } => {
                *count += 1;
                let number = DecimalValue::from_bytes(value)?;
                let delta = number.subtract(avg);
                *avg = avg.add(&delta.divide_i64_half_even_same_scale(*count)?);
            }
            Self::Unsupported(cql_type) => {
                return Err(format!("unsupported avg type {}", cql_type.cql_name()));
            }
        }
        Ok(())
    }

    fn finalize(self) -> Result<Vec<u8>, String> {
        Ok(match self {
            Self::Tinyint { sum, count } => vec![bigint_avg(&sum, count) as i8 as u8],
            Self::Smallint { sum, count } => {
                (bigint_avg(&sum, count) as i16).to_be_bytes().to_vec()
            }
            Self::Int { sum, count } => (bigint_avg(&sum, count) as i32).to_be_bytes().to_vec(),
            Self::Bigint { sum, count } | Self::Counter { sum, count } => {
                (bigint_avg(&sum, count) as i64).to_be_bytes().to_vec()
            }
            Self::Float { sum, count } => {
                let value = if count == 0 {
                    0.0
                } else {
                    sum.compute() / count as f64
                };
                (value as f32).to_be_bytes().to_vec()
            }
            Self::Double { sum, count } => {
                let value = if count == 0 {
                    0.0
                } else {
                    sum.compute() / count as f64
                };
                value.to_be_bytes().to_vec()
            }
            Self::Varint { sum, count } => {
                if count == 0 {
                    BigInt::zero().to_signed_bytes_be()
                } else {
                    (sum / BigInt::from(count)).to_signed_bytes_be()
                }
            }
            Self::Decimal { avg, .. } => avg.to_bytes(),
            Self::Unsupported(cql_type) => {
                return Err(format!("unsupported avg type {}", cql_type.cql_name()));
            }
        })
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct KahanAccumulator {
    sum: f64,
    compensation: f64,
    simple_sum: f64,
}

impl KahanAccumulator {
    fn add(&mut self, value: f64) {
        self.simple_sum += value;
        let tmp = value - self.compensation;
        let rounded = self.sum + tmp;
        self.compensation = (rounded - self.sum) - tmp;
        self.sum = rounded;
    }

    fn compute(self) -> f64 {
        let result = self.sum + self.compensation;
        if result.is_nan() && self.simple_sum.is_infinite() {
            self.simple_sum
        } else {
            result
        }
    }
}

fn bigint_avg(sum: &BigInt, count: i64) -> i128 {
    if count == 0 {
        return 0;
    }
    let quotient = sum / BigInt::from(count);
    let bytes = quotient.to_signed_bytes_be();
    let sign = if bytes.first().is_some_and(|b| b & 0x80 != 0) {
        0xFF
    } else {
        0x00
    };
    let mut padded = [sign; 16];
    let take = bytes.len().min(16);
    padded[16 - take..].copy_from_slice(&bytes[bytes.len() - take..]);
    i128::from_be_bytes(padded)
}

fn read_i8_value(value: &[u8]) -> Result<i8, String> {
    if value.len() != 1 {
        return Err(format!("invalid tinyint bytes: got {}", value.len()));
    }
    Ok(value[0] as i8)
}

fn read_i16_value(value: &[u8]) -> Result<i16, String> {
    let bytes: [u8; 2] = value
        .try_into()
        .map_err(|_| format!("invalid smallint bytes: got {}", value.len()))?;
    Ok(i16::from_be_bytes(bytes))
}

fn read_i32_value(value: &[u8]) -> Result<i32, String> {
    let bytes: [u8; 4] = value
        .try_into()
        .map_err(|_| format!("invalid int bytes: got {}", value.len()))?;
    Ok(i32::from_be_bytes(bytes))
}

fn read_i64_value(value: &[u8]) -> Result<i64, String> {
    let bytes: [u8; 8] = value
        .try_into()
        .map_err(|_| format!("invalid bigint bytes: got {}", value.len()))?;
    Ok(i64::from_be_bytes(bytes))
}

fn read_f32_value(value: &[u8]) -> Result<f32, String> {
    let bytes: [u8; 4] = value
        .try_into()
        .map_err(|_| format!("invalid float bytes: got {}", value.len()))?;
    Ok(f32::from_be_bytes(bytes))
}

fn read_f64_value(value: &[u8]) -> Result<f64, String> {
    let bytes: [u8; 8] = value
        .try_into()
        .map_err(|_| format!("invalid double bytes: got {}", value.len()))?;
    Ok(f64::from_be_bytes(bytes))
}

fn aggregate_selector_name(selector: &Selector) -> Option<String> {
    match selector {
        Selector::Count => Some("count".to_string()),
        Selector::Function(name, _) if is_count_rows_name(name) => Some("count".to_string()),
        Selector::Function(name, _) if is_builtin_aggregate_name(name) => {
            Some(name.to_ascii_lowercase())
        }
        Selector::Alias { selector, .. } => aggregate_selector_name(selector),
        Selector::Cast { .. } => None,
        _ => None,
    }
}

fn count_selector_counts_non_null(selector: &Selector) -> bool {
    match selector {
        Selector::Function(name, args) if name.eq_ignore_ascii_case("count") => !args.is_empty(),
        Selector::Alias { selector, .. } => count_selector_counts_non_null(selector),
        _ => false,
    }
}

fn selector_aggregate_values<F>(
    selector: &Selector,
    partition_key: &[u8],
    row: &Row,
    static_row: Option<&Row>,
    column_value: &F,
    pk_names: &[String],
    ck_names: &[String],
    static_col_names: &[String],
    now_secs: i32,
    table: &cassandra_schema::table::TableMetadata,
    registry: &FunctionRegistry,
    keyspace: &str,
    keyspace_meta: &KeyspaceMetadata,
    udf_registry: &UdfRegistry,
) -> Result<Vec<Option<Vec<u8>>>, ExecutorError>
where
    F: Fn(&str, &[u8], &Row, Option<&Row>) -> Option<Vec<u8>>,
{
    match selector {
        Selector::Column(name) => Ok(vec![column_value(name, partition_key, row, static_row)]),
        Selector::Cast { .. } => Ok(vec![aggregate_argument_value(
            selector,
            partition_key,
            row,
            static_row,
            column_value,
            pk_names,
            ck_names,
            static_col_names,
            now_secs,
            table,
            registry,
            keyspace,
            keyspace_meta,
            udf_registry,
        )?]),
        Selector::Alias { selector, .. } => selector_aggregate_values(
            selector,
            partition_key,
            row,
            static_row,
            column_value,
            pk_names,
            ck_names,
            static_col_names,
            now_secs,
            table,
            registry,
            keyspace,
            keyspace_meta,
            udf_registry,
        ),
        Selector::Function(_, args) => args
            .iter()
            .map(|arg| {
                aggregate_argument_value(
                    arg,
                    partition_key,
                    row,
                    static_row,
                    column_value,
                    pk_names,
                    ck_names,
                    static_col_names,
                    now_secs,
                    table,
                    registry,
                    keyspace,
                    keyspace_meta,
                    udf_registry,
                )
            })
            .collect(),
        Selector::Count => Ok(Vec::new()),
        _ => Ok(Vec::new()),
    }
}

fn aggregate_argument_value<F>(
    selector: &Selector,
    partition_key: &[u8],
    row: &Row,
    static_row: Option<&Row>,
    column_value: &F,
    pk_names: &[String],
    ck_names: &[String],
    static_col_names: &[String],
    now_secs: i32,
    table: &cassandra_schema::table::TableMetadata,
    registry: &FunctionRegistry,
    keyspace: &str,
    keyspace_meta: &KeyspaceMetadata,
    udf_registry: &UdfRegistry,
) -> Result<Option<Vec<u8>>, ExecutorError>
where
    F: Fn(&str, &[u8], &Row, Option<&Row>) -> Option<Vec<u8>>,
{
    match selector {
        Selector::Column(name) => Ok(column_value(name, partition_key, row, static_row)),
        Selector::Alias { selector, .. } => aggregate_argument_value(
            selector,
            partition_key,
            row,
            static_row,
            column_value,
            pk_names,
            ck_names,
            static_col_names,
            now_secs,
            table,
            registry,
            keyspace,
            keyspace_meta,
            udf_registry,
        ),
        Selector::Function(name, _) if !is_builtin_aggregate_name(name) => {
            select_scalar_argument_value(
                selector,
                partition_key,
                row,
                static_row,
                column_value,
                pk_names,
                ck_names,
                static_col_names,
                now_secs,
                table,
                registry,
                keyspace,
                keyspace_meta,
                udf_registry,
            )
        }
        Selector::Cast { .. } => select_scalar_argument_value(
            selector,
            partition_key,
            row,
            static_row,
            column_value,
            pk_names,
            ck_names,
            static_col_names,
            now_secs,
            table,
            registry,
            keyspace,
            keyspace_meta,
            udf_registry,
        ),
        Selector::WritetimeOrTtl(kind, column) => Ok(writetime_or_ttl_value(
            kind,
            column,
            pk_names,
            ck_names,
            static_col_names,
            row,
            static_row,
            now_secs,
        )),
        _ => Ok(None),
    }
}

fn user_aggregate_selector_parts(selector: &Selector) -> Option<(&str, &[Selector])> {
    match selector {
        Selector::Function(name, args) => Some((name.as_str(), args.as_slice())),
        Selector::Alias { selector, .. } => user_aggregate_selector_parts(selector),
        Selector::Cast { .. } => None,
        _ => None,
    }
}

fn user_function_selector_parts(selector: &Selector) -> Option<(&str, &[Selector])> {
    match selector {
        Selector::Function(name, args) => Some((name.as_str(), args.as_slice())),
        Selector::Alias { selector, .. } => user_function_selector_parts(selector),
        Selector::Cast { .. } => None,
        _ => None,
    }
}

fn resolve_builtin_function_selector(
    selector: &Selector,
    arg_types: &[CqlType],
    registry: &FunctionRegistry,
) -> Option<ResolvedBuiltinFunction> {
    let (name, _) = user_function_selector_parts(selector)?;
    if is_builtin_aggregate_name(name) {
        return None;
    }
    let function = registry.resolve(name, arg_types)?;
    Some(ResolvedBuiltinFunction {
        return_type: function.return_type(),
        function,
    })
}

fn selector_is_aggregate(selector: &Selector) -> bool {
    aggregate_selector_name(selector).is_some()
}

fn is_aggregate_name(name: &str) -> bool {
    is_builtin_aggregate_name(name)
}

fn is_builtin_aggregate_name(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "count" | "count_rows" | "countrows" | "sum" | "avg" | "min" | "max"
    )
}

fn is_count_rows_selector_name(name: &str) -> bool {
    name.eq_ignore_ascii_case("count") || is_count_rows_name(name)
}

fn is_count_rows_name(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "count_rows" | "countrows"
    )
}

fn is_integral_type(cql_type: &CqlType) -> bool {
    matches!(
        cql_type,
        CqlType::Tinyint
            | CqlType::Smallint
            | CqlType::Int
            | CqlType::Bigint
            | CqlType::Counter
            | CqlType::Varint
    )
}

fn current_timestamp_micros() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as i64
}

fn apply_cql_column_mask(
    table: &TableMetadata,
    registry: &FunctionRegistry,
    column_name: &str,
    value: &[u8],
) -> Option<Option<Vec<u8>>> {
    let column = table.column(column_name)?;
    let (function_name, args) = column.masked_with.as_ref()?;
    let arg_types = mask_function_arg_types(function_name, &column.column_type, args)?;
    let mut arg_values = vec![Some(value.to_vec())];
    for (arg, arg_type) in args.iter().zip(arg_types.iter().skip(1)) {
        arg_values.push(mask_metadata_arg_to_bytes(arg, arg_type)?);
    }
    let function = registry.resolve(function_name, &arg_types)?;
    let arg_refs = arg_values
        .iter()
        .map(|value| value.as_deref())
        .collect::<Vec<_>>();
    if function.return_type() != column.column_type {
        return None;
    }
    function.execute(&arg_refs).ok()
}

fn masked_restricted_columns(
    table: &TableMetadata,
    relations: &[Relation],
    partition_key_columns: &[String],
) -> Vec<String> {
    let mut columns = Vec::new();
    for relation in relations {
        if relation.column.eq_ignore_ascii_case("token") {
            for column_name in partition_key_columns {
                if table_column(table, column_name)
                    .is_some_and(|column| column.masked_with.is_some())
                {
                    columns.push(column_name.clone());
                }
            }
        } else if table_column(table, &relation.column)
            .is_some_and(|column| column.masked_with.is_some())
        {
            columns.push(relation.column.clone());
        }
    }
    columns.sort();
    columns.dedup();
    columns
}

fn validate_cql_column_mask(
    registry: &FunctionRegistry,
    column: &ColumnMetadata,
    function_name: &str,
    args: &[Term],
) -> Result<Vec<String>, ExecutorError> {
    let metadata_args = args.iter().map(term_to_metadata_string).collect::<Vec<_>>();
    let arg_types = mask_function_arg_types(function_name, &column.column_type, &metadata_args)
        .ok_or_else(|| {
            ExecutorError::InvalidQuery(format!(
                "Invalid arguments for mask {} on column '{}' of type {}",
                function_name,
                column.name,
                column.column_type.cql_name()
            ))
        })?;
    for (arg, arg_type) in args.iter().zip(arg_types.iter().skip(1)) {
        let bytes = mask_argument_to_bytes(arg, arg_type, registry).map_err(|err| {
            ExecutorError::InvalidQuery(format!(
                "Invalid mask argument for {} on column '{}': {}",
                function_name, column.name, err
            ))
        })?;
        if bytes.is_none() && !term_is_null(arg) {
            return Err(ExecutorError::InvalidQuery(format!(
                "Invalid mask argument {:?} for expected type {}",
                arg,
                arg_type.cql_name()
            )));
        }
    }
    validate_stored_cql_column_mask(registry, column, function_name, &metadata_args)?;
    Ok(metadata_args)
}

fn validate_stored_cql_column_mask(
    registry: &FunctionRegistry,
    column: &ColumnMetadata,
    function_name: &str,
    args: &[String],
) -> Result<(), ExecutorError> {
    let arg_types =
        mask_function_arg_types(function_name, &column.column_type, args).ok_or_else(|| {
            ExecutorError::InvalidQuery(format!(
                "Invalid arguments for mask {} on column '{}' of type {}",
                function_name,
                column.name,
                column.column_type.cql_name()
            ))
        })?;
    let mut validation_args = vec![Some(b"validation".to_vec())];
    for (arg, arg_type) in args.iter().zip(arg_types.iter().skip(1)) {
        let bytes = mask_metadata_arg_to_bytes(arg, arg_type).ok_or_else(|| {
            ExecutorError::InvalidQuery(format!(
                "Invalid mask argument {:?} for expected type {}",
                arg,
                arg_type.cql_name()
            ))
        })?;
        validation_args.push(bytes);
    }

    let Some(function) = registry.resolve(function_name, &arg_types) else {
        return Err(ExecutorError::InvalidQuery(format!(
            "Unknown mask function {} for arguments ({})",
            function_name,
            arg_types
                .iter()
                .map(CqlType::cql_name)
                .collect::<Vec<_>>()
                .join(", ")
        )));
    };
    if function.return_type() != column.column_type {
        return Err(ExecutorError::InvalidQuery(format!(
            "Mask function {} returns {}, but column '{}' has type {}",
            function_name,
            function.return_type().cql_name(),
            column.name,
            column.column_type.cql_name()
        )));
    }
    let arg_refs = validation_args
        .iter()
        .map(|value| value.as_deref())
        .collect::<Vec<_>>();
    function.execute(&arg_refs).map_err(|err| {
        ExecutorError::InvalidQuery(format!(
            "Invalid mask argument for {} on column '{}': {}",
            function_name, column.name, err
        ))
    })?;
    Ok(())
}

fn mask_argument_to_bytes(
    arg: &Term,
    arg_type: &CqlType,
    registry: &FunctionRegistry,
) -> Result<Option<Vec<u8>>, ExecutorError> {
    if term_is_null(arg) {
        return Ok(None);
    }
    let empty_udfs = UdfRegistry::new();
    if !mask_argument_matches_cql_type(arg, arg_type, registry, &empty_udfs) {
        return Ok(None);
    }
    term_to_bytes_with_functions(arg, arg_type, registry, &empty_udfs, "")
}

fn mask_argument_matches_cql_type(
    arg: &Term,
    arg_type: &CqlType,
    registry: &FunctionRegistry,
    udf_registry: &UdfRegistry,
) -> bool {
    match arg {
        Term::FunctionCall(_, _) => infer_term_type_with_functions(arg, registry, udf_registry, "")
            .is_some_and(|actual| is_compatible_with(&actual, arg_type)),
        _ => term_matches_cql_type(arg, arg_type),
    }
}

fn term_is_null(term: &Term) -> bool {
    match term {
        Term::Literal(Literal::Null) => true,
        Term::TypeHint(_, inner) => term_is_null(inner),
        _ => false,
    }
}

fn mask_function_arg_types(
    function_name: &str,
    column_type: &CqlType,
    args: &[String],
) -> Option<Vec<CqlType>> {
    let mut arg_types = vec![column_type.clone()];
    match function_name.to_ascii_lowercase().as_str() {
        "mask_default" | "mask_null" if args.is_empty() => Some(arg_types),
        "mask_hash" if args.is_empty() => Some(arg_types),
        "mask_hash" if args.len() == 1 => {
            arg_types.push(CqlType::Varchar);
            Some(arg_types)
        }
        "mask_inner" | "mask_outer" if args.len() == 2 || args.len() == 3 => {
            arg_types.push(CqlType::Int);
            arg_types.push(CqlType::Int);
            if args.len() == 3 {
                arg_types.push(CqlType::Varchar);
            }
            Some(arg_types)
        }
        "mask_replace" if args.len() == 1 => {
            arg_types.push(column_type.clone());
            Some(arg_types)
        }
        _ => None,
    }
}

fn mask_metadata_arg_to_bytes(arg: &str, cql_type: &CqlType) -> Option<Option<Vec<u8>>> {
    if arg.eq_ignore_ascii_case("null") {
        return Some(None);
    }
    let bytes = match cql_type {
        CqlType::Ascii | CqlType::Varchar => arg.as_bytes().to_vec(),
        CqlType::Boolean => vec![if arg.parse::<bool>().ok()? { 1 } else { 0 }],
        CqlType::Tinyint => vec![arg.parse::<i8>().ok()? as u8],
        CqlType::Smallint => arg.parse::<i16>().ok()?.to_be_bytes().to_vec(),
        CqlType::Int => arg.parse::<i32>().ok()?.to_be_bytes().to_vec(),
        CqlType::Bigint | CqlType::Counter | CqlType::Timestamp | CqlType::Time => {
            arg.parse::<i64>().ok()?.to_be_bytes().to_vec()
        }
        CqlType::Float => arg.parse::<f32>().ok()?.to_be_bytes().to_vec(),
        CqlType::Double => arg.parse::<f64>().ok()?.to_be_bytes().to_vec(),
        CqlType::Varint => arg.parse::<BigInt>().ok()?.to_signed_bytes_be(),
        CqlType::Decimal => cassandra_types::bigint::string_to_decimal(arg).ok()?,
        CqlType::Blob => parse_blob_metadata_arg(arg)?,
        CqlType::Uuid | CqlType::Timeuuid => parse_uuid_bytes(arg)?,
        CqlType::Date => arg.parse::<u32>().ok()?.to_be_bytes().to_vec(),
        _ => return None,
    };
    Some(Some(bytes))
}

fn parse_blob_metadata_arg(arg: &str) -> Option<Vec<u8>> {
    let hex = arg.strip_prefix("0x").or_else(|| arg.strip_prefix("0X"))?;
    if hex.len() % 2 != 0 {
        return None;
    }
    (0..hex.len())
        .step_by(2)
        .map(|idx| u8::from_str_radix(&hex[idx..idx + 2], 16).ok())
        .collect()
}

fn term_to_metadata_string(term: &Term) -> String {
    match term {
        Term::Literal(Literal::String(s)) => s.clone(),
        Term::Literal(Literal::Integer(i)) => i.to_string(),
        Term::Literal(Literal::Float(f)) => f.to_string(),
        Term::Literal(Literal::Blob(bytes)) => {
            let hex: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
            format!("0x{hex}")
        }
        Term::Literal(Literal::Boolean(b)) => b.to_string(),
        Term::Literal(Literal::Uuid(u)) => u.to_string(),
        Term::Literal(Literal::Null) => "null".to_string(),
        Term::TypeHint(_, inner) => term_to_metadata_string(inner),
        _ => format!("{term:?}"),
    }
}

fn resolve_column_constraint(constraint: &AstColumnConstraint) -> ColumnConstraintMetadata {
    match constraint {
        AstColumnConstraint::Scalar { column, op, term } => ColumnConstraintMetadata::Scalar {
            column: column.clone(),
            op: resolve_constraint_op(*op),
            term: term.clone(),
        },
        AstColumnConstraint::Function {
            name,
            args,
            op,
            term,
        } => ColumnConstraintMetadata::Function {
            name: name.clone(),
            args: args.clone(),
            op: resolve_constraint_op(*op),
            term: term.clone(),
        },
        AstColumnConstraint::UnaryFunction { name, args } => {
            ColumnConstraintMetadata::UnaryFunction {
                name: name.clone(),
                args: args.clone(),
            }
        }
        AstColumnConstraint::NotNull => ColumnConstraintMetadata::NotNull,
    }
}

fn resolve_constraint_op(op: AstConstraintRelationOp) -> SchemaConstraintRelationOp {
    match op {
        AstConstraintRelationOp::Eq => SchemaConstraintRelationOp::Eq,
        AstConstraintRelationOp::NotEq => SchemaConstraintRelationOp::NotEq,
        AstConstraintRelationOp::Lt => SchemaConstraintRelationOp::Lt,
        AstConstraintRelationOp::Lte => SchemaConstraintRelationOp::Lte,
        AstConstraintRelationOp::Gt => SchemaConstraintRelationOp::Gt,
        AstConstraintRelationOp::Gte => SchemaConstraintRelationOp::Gte,
    }
}

fn apply_table_options(
    params: &mut TableParams,
    options: &std::collections::HashMap<String, String>,
) -> Result<(), ExecutorError> {
    for (key, value) in options {
        match key.as_str() {
            "gc_grace_seconds" => {
                params.gc_grace_seconds = parse_table_i32_option(key, value)?;
            }
            "default_time_to_live" => {
                params.default_time_to_live = parse_table_i32_option(key, value)?;
            }
            "min_index_interval" => {
                params.min_index_interval = parse_table_i32_option(key, value)?;
            }
            "max_index_interval" => {
                params.max_index_interval = parse_table_i32_option(key, value)?;
            }
            "memtable_flush_period_in_ms" => {
                params.memtable_flush_period_in_ms = parse_table_i32_option(key, value)?;
            }
            "bloom_filter_fp_chance" => {
                params.bloom_filter_fp_chance = parse_table_f64_option(key, value)?;
            }
            "crc_check_chance" => {
                params.crc_check_chance = parse_table_f64_option(key, value)?;
            }
            "comment" => {
                params.comment = value.clone();
            }
            "allow_auto_snapshot" => {
                params.allow_auto_snapshot = parse_table_bool_option(key, value)?;
            }
            "incremental_backups" => {
                params.incremental_backups = parse_table_bool_option(key, value)?;
            }
            "compaction" => {
                params.compaction = parse_table_map_option(key, value)?;
            }
            "compression" => {
                params.compression = parse_table_map_option(key, value)?;
            }
            "caching" => {
                params.caching = parse_table_map_option(key, value)?;
            }
            "read_repair" => {
                params.read_repair = value.clone();
            }
            "speculative_retry" => {
                params.speculative_retry = value.clone();
            }
            "additional_write_policy" => {
                params.additional_write_policy = value.clone();
            }
            "memtable" => {
                params.memtable = value.clone();
            }
            "fast_path" => {
                params.fast_path = value.clone();
            }
            "transactional_migration_from" => {
                params.transactional_migration_from = value.clone();
            }
            "auto_repair" => {
                params.auto_repair = parse_table_map_option(key, value)?;
            }
            "cdc" => {
                params.cdc = parse_table_bool_option(key, value)?;
            }
            "transactional_mode" => {
                params.transactional_mode = parse_transactional_mode_option(value)?;
            }
            _ => {
                return Err(ExecutorError::InvalidQuery(format!(
                    "Unsupported table option '{}'",
                    key
                )));
            }
        }
    }
    Ok(())
}

fn parse_transactional_mode_option(value: &str) -> Result<TransactionalMode, ExecutorError> {
    match value.to_ascii_lowercase().as_str() {
        "off" => Ok(TransactionalMode::Off),
        "paxos" => Ok(TransactionalMode::Paxos),
        "accord" => Ok(TransactionalMode::Accord),
        "mixed" => Ok(TransactionalMode::Mixed),
        _ => Err(ExecutorError::InvalidQuery(format!(
            "Invalid transactional_mode value '{}'",
            value
        ))),
    }
}

fn parse_table_i32_option(key: &str, value: &str) -> Result<i32, ExecutorError> {
    value.parse::<i32>().map_err(|_| {
        ExecutorError::InvalidQuery(format!(
            "Invalid integer value '{}' for option '{}'",
            value, key
        ))
    })
}

fn parse_table_f64_option(key: &str, value: &str) -> Result<f64, ExecutorError> {
    value.parse::<f64>().map_err(|_| {
        ExecutorError::InvalidQuery(format!(
            "Invalid floating value '{}' for option '{}'",
            value, key
        ))
    })
}

fn parse_table_bool_option(key: &str, value: &str) -> Result<bool, ExecutorError> {
    match value.to_ascii_lowercase().as_str() {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => Err(ExecutorError::InvalidQuery(format!(
            "Invalid boolean value '{}' for option '{}'",
            value, key
        ))),
    }
}

fn parse_table_map_option(
    key: &str,
    value: &str,
) -> Result<std::collections::BTreeMap<String, String>, ExecutorError> {
    serde_json::from_str::<std::collections::BTreeMap<String, String>>(value).map_err(|_| {
        ExecutorError::InvalidQuery(format!(
            "Invalid map value '{}' for option '{}'",
            value, key
        ))
    })
}

fn validate_write_constraints(
    table: &TableMetadata,
    values: &BTreeMap<String, Option<Vec<u8>>>,
    require_all: bool,
) -> Result<(), ExecutorError> {
    for column in &table.columns {
        if column.constraints.is_empty() {
            continue;
        }
        if !require_all && !values.contains_key(&column.name) {
            continue;
        }

        for constraint in &column.constraints {
            validate_column_constraint(table, column, constraint, values, require_all)?;
        }
    }
    Ok(())
}

fn validate_column_constraint(
    table: &TableMetadata,
    column: &ColumnMetadata,
    constraint: &ColumnConstraintMetadata,
    values: &BTreeMap<String, Option<Vec<u8>>>,
    require_all: bool,
) -> Result<(), ExecutorError> {
    match constraint {
        ColumnConstraintMetadata::NotNull => {
            if values.get(&column.name).and_then(|v| v.as_ref()).is_none() {
                return Err(ExecutorError::InvalidQuery(format!(
                    "Column '{}' violates CHECK NOT NULL",
                    column.name
                )));
            }
        }
        ColumnConstraintMetadata::Scalar {
            column: constraint_column,
            op,
            term,
        } => {
            if !require_all && !values.contains_key(constraint_column) {
                return Ok(());
            }
            let Some(target_column) = table.column(constraint_column) else {
                return Err(ExecutorError::InvalidQuery(format!(
                    "CHECK constraint references unknown column '{}'",
                    constraint_column
                )));
            };
            let Some(Some(actual)) = values.get(constraint_column) else {
                return Ok(());
            };
            if !compare_constraint_value(actual, &target_column.column_type, term, *op)? {
                return Err(ExecutorError::InvalidQuery(format!(
                    "Column '{}' violates CHECK {} {:?} {}",
                    column.name, constraint_column, op, term
                )));
            }
        }
        ColumnConstraintMetadata::Function {
            name,
            args,
            op,
            term,
        } => {
            let target_column_name = args.first().map(String::as_str).unwrap_or(&column.name);
            if !require_all && !values.contains_key(target_column_name) {
                return Ok(());
            }
            let Some(target_column) = table.column(target_column_name) else {
                return Err(ExecutorError::InvalidQuery(format!(
                    "CHECK constraint function '{}' references unknown column '{}'",
                    name, target_column_name
                )));
            };
            let Some(Some(actual)) = values.get(target_column_name) else {
                return Ok(());
            };
            if name.eq_ignore_ascii_case("regexp") {
                if !compare_regexp_constraint(actual, &target_column.column_type, term, *op)? {
                    return Err(ExecutorError::InvalidQuery(format!(
                        "Column '{}' violates CHECK {}() {:?} {}",
                        column.name, name, op, term
                    )));
                }
                return Ok(());
            }
            let Some(function_value) =
                constraint_function_value(name, actual, &target_column.column_type)
            else {
                return Err(ExecutorError::InvalidQuery(format!(
                    "Unsupported CHECK constraint function '{}'",
                    name
                )));
            };
            if !compare_numeric_constraint(function_value, term, *op)? {
                return Err(ExecutorError::InvalidQuery(format!(
                    "Column '{}' violates CHECK {}() {:?} {}",
                    column.name, name, op, term
                )));
            }
        }
        ColumnConstraintMetadata::UnaryFunction { name, args } => {
            validate_unary_constraint(column, name, args, values, require_all)?;
        }
    }
    Ok(())
}

fn validate_unary_constraint(
    column: &ColumnMetadata,
    name: &str,
    args: &[String],
    values: &BTreeMap<String, Option<Vec<u8>>>,
    require_all: bool,
) -> Result<(), ExecutorError> {
    if !require_all && !values.contains_key(&column.name) {
        return Ok(());
    }

    match name.to_ascii_lowercase().as_str() {
        "not_null" | "not null" => {
            if !args.is_empty() {
                return Err(ExecutorError::InvalidQuery(format!(
                    "CHECK constraint function '{}' does not accept arguments",
                    name
                )));
            }
            if values.get(&column.name).and_then(|v| v.as_ref()).is_none() {
                return Err(ExecutorError::InvalidQuery(format!(
                    "Column '{}' violates CHECK NOT NULL",
                    column.name
                )));
            }
            Ok(())
        }
        "json" => {
            if !args.is_empty() {
                return Err(ExecutorError::InvalidQuery(format!(
                    "CHECK constraint function '{}' does not accept arguments",
                    name
                )));
            }
            let Some(Some(actual)) = values.get(&column.name) else {
                return Ok(());
            };
            validate_json_constraint_value(column, actual)
        }
        _ => Err(ExecutorError::InvalidQuery(format!(
            "Unsupported CHECK constraint unary function '{}'",
            name
        ))),
    }
}

fn validate_json_constraint_value(
    column: &ColumnMetadata,
    actual: &[u8],
) -> Result<(), ExecutorError> {
    if !matches!(column.column_type, CqlType::Ascii | CqlType::Varchar) {
        return Err(ExecutorError::InvalidQuery(
            "json CHECK constraint only supports text and ascii columns".to_string(),
        ));
    }
    let actual = String::from_utf8(actual.to_vec()).map_err(|err| {
        ExecutorError::InvalidQuery(format!(
            "Column '{}' violates CHECK json: {}",
            column.name, err
        ))
    })?;
    serde_json::from_str::<serde_json::Value>(&actual).map_err(|err| {
        ExecutorError::InvalidQuery(format!(
            "Column '{}' violates CHECK json: {}",
            column.name, err
        ))
    })?;
    Ok(())
}

fn compare_regexp_constraint(
    actual: &[u8],
    cql_type: &CqlType,
    pattern: &str,
    op: SchemaConstraintRelationOp,
) -> Result<bool, ExecutorError> {
    if !matches!(
        op,
        SchemaConstraintRelationOp::Eq | SchemaConstraintRelationOp::NotEq
    ) {
        return Err(ExecutorError::InvalidQuery(format!(
            "regexp CHECK constraint only supports = and != operators, got {:?}",
            op
        )));
    }
    if !matches!(cql_type, CqlType::Ascii | CqlType::Varchar) {
        return Err(ExecutorError::InvalidQuery(
            "regexp CHECK constraint only supports text and ascii columns".to_string(),
        ));
    }

    let regex = regex::Regex::new(&format!("^(?:{})$", pattern)).map_err(|err| {
        ExecutorError::InvalidQuery(format!(
            "Invalid regexp CHECK constraint pattern '{}': {}",
            pattern, err
        ))
    })?;
    let actual = String::from_utf8_lossy(actual);
    let matches = regex.is_match(&actual);
    Ok(match op {
        SchemaConstraintRelationOp::Eq => matches,
        SchemaConstraintRelationOp::NotEq => !matches,
        _ => unreachable!("operator restricted above"),
    })
}

fn constraint_function_value(name: &str, actual: &[u8], cql_type: &CqlType) -> Option<f64> {
    match name.to_ascii_lowercase().as_str() {
        "length" => match cql_type {
            CqlType::Ascii | CqlType::Varchar => {
                Some(String::from_utf8_lossy(actual).chars().count() as f64)
            }
            _ => Some(actual.len() as f64),
        },
        "octet_length" => Some(actual.len() as f64),
        _ => None,
    }
}

fn compare_constraint_value(
    actual: &[u8],
    cql_type: &CqlType,
    expected: &str,
    op: SchemaConstraintRelationOp,
) -> Result<bool, ExecutorError> {
    match cql_type {
        CqlType::Tinyint
        | CqlType::Smallint
        | CqlType::Int
        | CqlType::Bigint
        | CqlType::Counter
        | CqlType::Timestamp
        | CqlType::Time
        | CqlType::Float
        | CqlType::Double
        | CqlType::Decimal
        | CqlType::Varint => {
            let Some(actual) = numeric_constraint_value(actual, cql_type) else {
                return Ok(false);
            };
            compare_numeric_constraint(actual, expected, op)
        }
        CqlType::Boolean => {
            let actual = actual.first().copied().unwrap_or(0) != 0;
            let expected = expected.parse::<bool>().map_err(|_| {
                ExecutorError::InvalidQuery(format!(
                    "Invalid boolean CHECK constraint value '{}'",
                    expected
                ))
            })?;
            Ok(compare_ordering(actual.cmp(&expected), op))
        }
        CqlType::Ascii | CqlType::Varchar => {
            let actual = String::from_utf8_lossy(actual);
            Ok(compare_ordering(actual.as_ref().cmp(expected), op))
        }
        _ => Ok(compare_ordering(actual.cmp(expected.as_bytes()), op)),
    }
}

fn numeric_constraint_value(actual: &[u8], cql_type: &CqlType) -> Option<f64> {
    match cql_type {
        CqlType::Tinyint => actual.first().map(|b| (*b as i8) as f64),
        CqlType::Smallint if actual.len() >= 2 => Some(BigEndian::read_i16(actual) as f64),
        CqlType::Int if actual.len() >= 4 => Some(BigEndian::read_i32(actual) as f64),
        CqlType::Bigint | CqlType::Counter | CqlType::Timestamp | CqlType::Time
            if actual.len() >= 8 =>
        {
            Some(BigEndian::read_i64(actual) as f64)
        }
        CqlType::Float if actual.len() >= 4 => Some(BigEndian::read_f32(actual) as f64),
        CqlType::Double | CqlType::Decimal if actual.len() >= 8 => {
            Some(BigEndian::read_f64(actual))
        }
        CqlType::Varint => Some(actual.iter().fold(0i128, |acc, b| (acc << 8) | *b as i128) as f64),
        _ => None,
    }
}

fn compare_numeric_constraint(
    actual: f64,
    expected: &str,
    op: SchemaConstraintRelationOp,
) -> Result<bool, ExecutorError> {
    let expected = expected.parse::<f64>().map_err(|_| {
        ExecutorError::InvalidQuery(format!(
            "Invalid numeric CHECK constraint value '{}'",
            expected
        ))
    })?;
    let ordering = actual
        .partial_cmp(&expected)
        .unwrap_or(std::cmp::Ordering::Less);
    Ok(compare_ordering(ordering, op))
}

fn compare_ordering(ordering: std::cmp::Ordering, op: SchemaConstraintRelationOp) -> bool {
    match op {
        SchemaConstraintRelationOp::Eq => ordering == std::cmp::Ordering::Equal,
        SchemaConstraintRelationOp::NotEq => ordering != std::cmp::Ordering::Equal,
        SchemaConstraintRelationOp::Lt => ordering == std::cmp::Ordering::Less,
        SchemaConstraintRelationOp::Lte => ordering != std::cmp::Ordering::Greater,
        SchemaConstraintRelationOp::Gt => ordering == std::cmp::Ordering::Greater,
        SchemaConstraintRelationOp::Gte => ordering != std::cmp::Ordering::Less,
    }
}

fn term_to_bytes(term: &Term) -> Option<Vec<u8>> {
    match term {
        Term::Literal(lit) => literal_to_bytes(lit),
        Term::BindMarker(_) => None,
        Term::FunctionCall(_, _) => None,
        Term::TypeHint(type_hint, inner) => type_hint
            .resolve()
            .and_then(|hinted_type| typed_term_to_bytes(inner, &hinted_type))
            .or_else(|| term_to_bytes(inner)),
        Term::CollectionElement { .. } => None,
        Term::CollectionLiteral(_) | Term::MapLiteral(_) | Term::TupleLiteral(_) => None,
    }
}

fn relation_value_to_bytes(table: &TableMetadata, relation: &Relation) -> Option<Vec<u8>> {
    table
        .column(&relation.column)
        .and_then(|column| typed_term_to_bytes(&relation.value, &column.column_type))
        .or_else(|| term_to_bytes(&relation.value))
}

fn current_live_cell_value<'a>(row: &'a Row, column: &str, now_secs: i32) -> Option<&'a [u8]> {
    row.cells
        .iter()
        .rev()
        .find(|cell| cell.column == column && cell.is_live_at(now_secs))
        .and_then(|cell| cell.value.as_deref())
}

fn assignment_value_to_bytes(
    assignment: &Assignment,
    target: &CqlType,
    current: Option<&[u8]>,
    registry: &FunctionRegistry,
    udf_registry: &UdfRegistry,
    keyspace: &str,
) -> Result<Option<Vec<u8>>, ExecutorError> {
    match &assignment.op {
        AssignmentOp::Set => term_to_bytes_with_functions(
            &assignment.value,
            target,
            registry,
            udf_registry,
            keyspace,
        ),
        AssignmentOp::CollectionAppend
        | AssignmentOp::CollectionPrepend
        | AssignmentOp::CollectionRemove
        | AssignmentOp::MapPut { .. } => apply_collection_assignment(
            assignment,
            target,
            current,
            registry,
            udf_registry,
            keyspace,
        ),
    }
}

fn apply_collection_assignment(
    assignment: &Assignment,
    target: &CqlType,
    current: Option<&[u8]>,
    registry: &FunctionRegistry,
    udf_registry: &UdfRegistry,
    keyspace: &str,
) -> Result<Option<Vec<u8>>, ExecutorError> {
    let mut current_value = decode_existing_collection(current, target)?;
    match &assignment.op {
        AssignmentOp::CollectionAppend => match (target, &mut current_value) {
            (CqlType::List(_, _), CqlValue::List(values)) => {
                let mut delta = collection_delta_values(
                    &assignment.value,
                    target,
                    registry,
                    udf_registry,
                    keyspace,
                )?;
                values.append(&mut delta);
            }
            (CqlType::Set(_, _), CqlValue::Set(values)) => {
                let delta = collection_delta_values(
                    &assignment.value,
                    target,
                    registry,
                    udf_registry,
                    keyspace,
                )?;
                append_unique(values, delta);
            }
            (CqlType::Map(_, _, _), CqlValue::Map(entries)) => {
                let delta =
                    map_delta_entries(&assignment.value, target, registry, udf_registry, keyspace)?;
                upsert_map_entries(entries, delta);
            }
            _ => invalid_collection_assignment(assignment, target)?,
        },
        AssignmentOp::CollectionPrepend => match (target, &mut current_value) {
            (CqlType::List(_, _), CqlValue::List(values)) => {
                let mut delta = collection_delta_values(
                    &assignment.value,
                    target,
                    registry,
                    udf_registry,
                    keyspace,
                )?;
                delta.append(values);
                *values = delta;
            }
            (CqlType::Set(_, _), CqlValue::Set(values)) => {
                let delta = collection_delta_values(
                    &assignment.value,
                    target,
                    registry,
                    udf_registry,
                    keyspace,
                )?;
                append_unique(values, delta);
            }
            (CqlType::Map(_, _, _), CqlValue::Map(entries)) => {
                let delta =
                    map_delta_entries(&assignment.value, target, registry, udf_registry, keyspace)?;
                upsert_map_entries(entries, delta);
            }
            _ => invalid_collection_assignment(assignment, target)?,
        },
        AssignmentOp::CollectionRemove => match (target, &mut current_value) {
            (CqlType::List(_, _), CqlValue::List(values))
            | (CqlType::Set(_, _), CqlValue::Set(values)) => {
                let delta = collection_delta_values(
                    &assignment.value,
                    target,
                    registry,
                    udf_registry,
                    keyspace,
                )?;
                values.retain(|value| !delta.iter().any(|candidate| candidate == value));
            }
            (CqlType::Map(key_type, _, _), CqlValue::Map(entries)) => {
                let keys = map_removal_keys(
                    &assignment.value,
                    key_type,
                    registry,
                    udf_registry,
                    keyspace,
                )?;
                entries.retain(|(key, _)| !keys.iter().any(|candidate| candidate == key));
            }
            _ => invalid_collection_assignment(assignment, target)?,
        },
        AssignmentOp::MapPut { key } => {
            let CqlType::Map(key_type, value_type, _) = target else {
                invalid_collection_assignment(assignment, target)?;
                unreachable!();
            };
            let CqlValue::Map(entries) = &mut current_value else {
                invalid_collection_assignment(assignment, target)?;
                unreachable!();
            };
            let key =
                term_to_cql_value_with_functions(key, key_type, registry, udf_registry, keyspace)?;
            let value = term_to_cql_value_with_functions(
                &assignment.value,
                value_type,
                registry,
                udf_registry,
                keyspace,
            )?;
            upsert_map_entries(entries, vec![(key, value)]);
        }
        AssignmentOp::Set => unreachable!("set assignments are handled before collection updates"),
    }
    Ok(Some(current_value.serialize_value()))
}

fn invalid_collection_assignment(
    assignment: &Assignment,
    target: &CqlType,
) -> Result<(), ExecutorError> {
    Err(ExecutorError::InvalidQuery(format!(
        "Collection assignment is not valid for column '{}' of type {}",
        assignment.column,
        target.cql_name()
    )))
}

fn decode_existing_collection(
    current: Option<&[u8]>,
    target: &CqlType,
) -> Result<CqlValue, ExecutorError> {
    if let Some(bytes) = current {
        return CqlValue::deserialize_value(target, bytes).map_err(|err| {
            ExecutorError::InvalidQuery(format!(
                "Invalid existing collection value for {}: {}",
                target.cql_name(),
                err
            ))
        });
    }
    match target {
        CqlType::List(_, _) => Ok(CqlValue::List(Vec::new())),
        CqlType::Set(_, _) => Ok(CqlValue::Set(Vec::new())),
        CqlType::Map(_, _, _) => Ok(CqlValue::Map(Vec::new())),
        _ => Err(ExecutorError::InvalidQuery(format!(
            "Collection assignment is not valid for type {}",
            target.cql_name()
        ))),
    }
}

fn collection_delta_values(
    term: &Term,
    target: &CqlType,
    registry: &FunctionRegistry,
    udf_registry: &UdfRegistry,
    keyspace: &str,
) -> Result<Vec<CqlValue>, ExecutorError> {
    match term_to_cql_value_with_functions(term, target, registry, udf_registry, keyspace)? {
        CqlValue::List(values) | CqlValue::Set(values) => Ok(values),
        other => Err(ExecutorError::InvalidQuery(format!(
            "Expected collection delta for {}, got {:?}",
            target.cql_name(),
            other
        ))),
    }
}

fn map_delta_entries(
    term: &Term,
    target: &CqlType,
    registry: &FunctionRegistry,
    udf_registry: &UdfRegistry,
    keyspace: &str,
) -> Result<Vec<(CqlValue, CqlValue)>, ExecutorError> {
    match term_to_cql_value_with_functions(term, target, registry, udf_registry, keyspace)? {
        CqlValue::Map(entries) => Ok(entries),
        other => Err(ExecutorError::InvalidQuery(format!(
            "Expected map delta for {}, got {:?}",
            target.cql_name(),
            other
        ))),
    }
}

fn map_removal_keys(
    term: &Term,
    key_type: &CqlType,
    registry: &FunctionRegistry,
    udf_registry: &UdfRegistry,
    keyspace: &str,
) -> Result<Vec<CqlValue>, ExecutorError> {
    match term {
        Term::CollectionLiteral(values) | Term::TupleLiteral(values) => values
            .iter()
            .map(|value| {
                term_to_cql_value_with_functions(value, key_type, registry, udf_registry, keyspace)
            })
            .collect(),
        Term::MapLiteral(entries) => entries
            .iter()
            .map(|(key, _)| {
                term_to_cql_value_with_functions(key, key_type, registry, udf_registry, keyspace)
            })
            .collect(),
        _ => Ok(vec![term_to_cql_value_with_functions(
            term,
            key_type,
            registry,
            udf_registry,
            keyspace,
        )?]),
    }
}

fn term_to_cql_value_with_functions(
    term: &Term,
    target: &CqlType,
    registry: &FunctionRegistry,
    udf_registry: &UdfRegistry,
    keyspace: &str,
) -> Result<CqlValue, ExecutorError> {
    let bytes = term_to_bytes_with_functions(term, target, registry, udf_registry, keyspace)?
        .ok_or_else(|| {
            ExecutorError::InvalidQuery(format!("Invalid value for type {}", target.cql_name()))
        })?;
    CqlValue::deserialize_value(target, &bytes).map_err(|err| {
        ExecutorError::InvalidQuery(format!(
            "Invalid value for type {}: {}",
            target.cql_name(),
            err
        ))
    })
}

fn append_unique(values: &mut Vec<CqlValue>, delta: Vec<CqlValue>) {
    for value in delta {
        if !values.iter().any(|existing| existing == &value) {
            values.push(value);
        }
    }
}

fn upsert_map_entries(entries: &mut Vec<(CqlValue, CqlValue)>, delta: Vec<(CqlValue, CqlValue)>) {
    for (key, value) in delta {
        if let Some((_, existing_value)) = entries
            .iter_mut()
            .find(|(existing_key, _)| existing_key == &key)
        {
            *existing_value = value;
        } else {
            entries.push((key, value));
        }
    }
}

fn term_to_bytes_with_functions(
    term: &Term,
    target: &CqlType,
    registry: &FunctionRegistry,
    udf_registry: &UdfRegistry,
    keyspace: &str,
) -> Result<Option<Vec<u8>>, ExecutorError> {
    match term {
        Term::FunctionCall(name, args) => {
            evaluate_function_term(name, args, target, registry, udf_registry, keyspace)
        }
        Term::CollectionLiteral(values) => collection_term_to_bytes_with_functions(
            values,
            target,
            registry,
            udf_registry,
            keyspace,
        ),
        Term::MapLiteral(values) => {
            map_term_to_bytes_with_functions(values, target, registry, udf_registry, keyspace)
        }
        Term::TupleLiteral(values) => {
            tuple_term_to_bytes_with_functions(values, target, registry, udf_registry, keyspace)
        }
        Term::TypeHint(type_hint, inner) => {
            let hinted_type = type_hint.resolve().ok_or_else(|| {
                ExecutorError::InvalidQuery(format!("Unsupported type hint '{:?}'", type_hint))
            })?;
            term_to_bytes_with_functions(inner, &hinted_type, registry, udf_registry, keyspace)
        }
        _ => Ok(typed_term_to_bytes(term, target).or_else(|| term_to_bytes(term))),
    }
}

fn collection_term_to_bytes_with_functions(
    values: &[Term],
    target: &CqlType,
    registry: &FunctionRegistry,
    udf_registry: &UdfRegistry,
    keyspace: &str,
) -> Result<Option<Vec<u8>>, ExecutorError> {
    let (CqlType::List(inner, _) | CqlType::Set(inner, _)) = target else {
        return Ok(typed_term_to_bytes(
            &Term::CollectionLiteral(values.to_vec()),
            target,
        ));
    };

    let mut buf = Vec::new();
    push_i32(&mut buf, values.len() as i32);
    for value in values {
        let bytes = term_to_bytes_with_functions(value, inner, registry, udf_registry, keyspace)?
            .ok_or_else(|| {
            ExecutorError::InvalidQuery(format!(
                "Invalid collection element for target type {}",
                target.cql_name()
            ))
        })?;
        push_i32(&mut buf, bytes.len() as i32);
        buf.extend_from_slice(&bytes);
    }
    Ok(Some(buf))
}

fn map_term_to_bytes_with_functions(
    values: &[(Term, Term)],
    target: &CqlType,
    registry: &FunctionRegistry,
    udf_registry: &UdfRegistry,
    keyspace: &str,
) -> Result<Option<Vec<u8>>, ExecutorError> {
    let CqlType::Map(key_type, value_type, _) = target else {
        return Ok(typed_term_to_bytes(
            &Term::MapLiteral(values.to_vec()),
            target,
        ));
    };

    let mut buf = Vec::new();
    push_i32(&mut buf, values.len() as i32);
    for (key, value) in values {
        let key_bytes =
            term_to_bytes_with_functions(key, key_type, registry, udf_registry, keyspace)?
                .ok_or_else(|| {
                    ExecutorError::InvalidQuery(format!(
                        "Invalid map key for target type {}",
                        target.cql_name()
                    ))
                })?;
        push_i32(&mut buf, key_bytes.len() as i32);
        buf.extend_from_slice(&key_bytes);

        let value_bytes =
            term_to_bytes_with_functions(value, value_type, registry, udf_registry, keyspace)?
                .ok_or_else(|| {
                    ExecutorError::InvalidQuery(format!(
                        "Invalid map value for target type {}",
                        target.cql_name()
                    ))
                })?;
        push_i32(&mut buf, value_bytes.len() as i32);
        buf.extend_from_slice(&value_bytes);
    }
    Ok(Some(buf))
}

fn tuple_term_to_bytes_with_functions(
    values: &[Term],
    target: &CqlType,
    registry: &FunctionRegistry,
    udf_registry: &UdfRegistry,
    keyspace: &str,
) -> Result<Option<Vec<u8>>, ExecutorError> {
    let CqlType::Tuple(field_types) = target else {
        return Ok(typed_term_to_bytes(
            &Term::TupleLiteral(values.to_vec()),
            target,
        ));
    };
    if values.len() != field_types.len() {
        return Ok(None);
    }

    let mut buf = Vec::new();
    for (value, field_type) in values.iter().zip(field_types) {
        match term_to_bytes_with_functions(value, field_type, registry, udf_registry, keyspace)? {
            Some(bytes) => {
                push_i32(&mut buf, bytes.len() as i32);
                buf.extend_from_slice(&bytes);
            }
            None => push_i32(&mut buf, -1),
        }
    }
    Ok(Some(buf))
}

fn push_i32(buf: &mut Vec<u8>, value: i32) {
    let mut bytes = [0u8; 4];
    BigEndian::write_i32(&mut bytes, value);
    buf.extend_from_slice(&bytes);
}

fn evaluate_function_term(
    name: &str,
    args: &[Term],
    target: &CqlType,
    registry: &FunctionRegistry,
    udf_registry: &UdfRegistry,
    keyspace: &str,
) -> Result<Option<Vec<u8>>, ExecutorError> {
    let arg_types = args
        .iter()
        .map(|arg| infer_term_type_with_functions(arg, registry, udf_registry, keyspace))
        .collect::<Option<Vec<_>>>()
        .ok_or_else(|| {
            ExecutorError::InvalidQuery(format!(
                "Cannot infer argument types for function term '{}'",
                name
            ))
        })?;
    if let Some(function) = registry.resolve_with_return(name, &arg_types, target) {
        let expected_types = function.arg_types();
        if expected_types.len() != args.len() {
            return Err(ExecutorError::InvalidQuery(format!(
                "Unsupported variadic function term '{}'",
                name
            )));
        }
        let arg_values = args
            .iter()
            .zip(expected_types.iter())
            .map(|(arg, arg_type)| {
                term_to_bytes_with_functions(arg, arg_type, registry, udf_registry, keyspace)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let arg_refs = arg_values
            .iter()
            .map(|value| value.as_deref())
            .collect::<Vec<_>>();
        return function.execute(&arg_refs).map_err(|err| {
            ExecutorError::InvalidQuery(format!(
                "Failed to execute function term '{}': {}",
                name, err
            ))
        });
    }

    evaluate_udf_function_term(
        name,
        args,
        &arg_types,
        target,
        registry,
        udf_registry,
        keyspace,
    )
}

fn evaluate_udf_function_term(
    name: &str,
    args: &[Term],
    arg_types: &[CqlType],
    target: &CqlType,
    registry: &FunctionRegistry,
    udf_registry: &UdfRegistry,
    keyspace: &str,
) -> Result<Option<Vec<u8>>, ExecutorError> {
    let arg_signature = udf_arg_signature(arg_types);
    let Some((metadata, executor)) = udf_registry.get(keyspace, name, &arg_signature) else {
        return Err(ExecutorError::InvalidQuery(format!(
            "Unsupported function term '{}' for target type {}",
            name,
            target.cql_name()
        )));
    };
    let return_type = parse_cql_type(&metadata.return_type).ok_or_else(|| {
        ExecutorError::InvalidQuery(format!(
            "Unsupported UDF return type '{}' for function term '{}'",
            metadata.return_type, name
        ))
    })?;
    if !is_compatible_with(&return_type, target) {
        return Err(ExecutorError::InvalidQuery(format!(
            "UDF term '{}' returns {}, incompatible with target type {}",
            name,
            return_type.cql_name(),
            target.cql_name()
        )));
    }

    let mut udf_args = Vec::with_capacity(args.len());
    for (arg, arg_type) in args.iter().zip(arg_types) {
        let bytes = term_to_bytes_with_functions(arg, arg_type, registry, udf_registry, keyspace)?;
        udf_args.push(
            UdfValue::deserialize_arg(arg_type, bytes.as_deref()).map_err(|err| {
                ExecutorError::InvalidQuery(format!(
                    "Failed to deserialize argument for UDF term '{}': {}",
                    name, err
                ))
            })?,
        );
    }
    let result = executor.execute(&udf_args).map_err(|err| {
        ExecutorError::InvalidQuery(format!("Failed to execute UDF term '{}': {}", name, err))
    })?;
    result.serialize_return(target).map_err(|err| {
        ExecutorError::InvalidQuery(format!("Failed to serialize UDF term '{}': {}", name, err))
    })
}

fn infer_term_type_with_functions(
    term: &Term,
    registry: &FunctionRegistry,
    udf_registry: &UdfRegistry,
    keyspace: &str,
) -> Option<CqlType> {
    match term {
        Term::Literal(Literal::String(_)) => Some(CqlType::Varchar),
        Term::Literal(Literal::Integer(_)) => Some(CqlType::Int),
        Term::Literal(Literal::Float(_)) => Some(CqlType::Double),
        Term::Literal(Literal::Boolean(_)) => Some(CqlType::Boolean),
        Term::Literal(Literal::Blob(_)) => Some(CqlType::Blob),
        Term::Literal(Literal::Uuid(_)) => Some(CqlType::Uuid),
        Term::Literal(Literal::Null) => None,
        Term::TypeHint(type_hint, _) => type_hint.resolve(),
        Term::FunctionCall(name, args) => {
            let arg_types = args
                .iter()
                .map(|arg| infer_term_type_with_functions(arg, registry, udf_registry, keyspace))
                .collect::<Option<Vec<_>>>()?;
            registry
                .resolve(name, &arg_types)
                .map(|function| function.return_type())
                .or_else(|| {
                    udf_registry
                        .get(keyspace, name, &udf_arg_signature(&arg_types))
                        .and_then(|(metadata, _)| parse_cql_type(&metadata.return_type))
                })
        }
        Term::BindMarker(_)
        | Term::CollectionElement { .. }
        | Term::CollectionLiteral(_)
        | Term::MapLiteral(_)
        | Term::TupleLiteral(_) => None,
    }
}

fn is_vector_index(index: &IndexMetadata) -> bool {
    index
        .options
        .get("class_name")
        .is_some_and(|class_name| class_name.contains("StorageAttachedIndex"))
        && index.options.contains_key("vector_dimensions")
}

fn vector_literal_to_bytes(values: &[f32]) -> Vec<u8> {
    values
        .iter()
        .flat_map(|value| value.to_be_bytes())
        .collect()
}

#[allow(clippy::too_many_arguments)]
fn raw_column_value(
    pk_names: &[String],
    ck_names: &[String],
    static_col_names: &[String],
    column_name: &str,
    partition_key: &[u8],
    row: &Row,
    static_row: Option<&Row>,
    now_secs: i32,
) -> Option<Vec<u8>> {
    if pk_names.iter().any(|name| name == column_name) {
        Some(partition_key.to_vec())
    } else if ck_names.len() == 1 && ck_names[0] == column_name {
        Some(row.clustering_key.clone())
    } else if static_col_names.iter().any(|name| name == column_name) {
        static_row
            .and_then(|static_row| {
                static_row
                    .cells
                    .iter()
                    .find(|c| c.column == column_name && c.is_live_at(now_secs))
                    .and_then(|c| c.value.clone())
            })
            .or_else(|| {
                row.cells
                    .iter()
                    .find(|c| c.column == column_name && c.is_live_at(now_secs))
                    .and_then(|c| c.value.clone())
            })
    } else {
        row.cells
            .iter()
            .find(|c| c.column == column_name && c.is_live_at(now_secs))
            .and_then(|c| c.value.clone())
    }
}

#[allow(clippy::too_many_arguments)]
fn row_matches_relations(
    relations: &[Relation],
    table_meta: &TableMetadata,
    pk_names: &[String],
    ck_names: &[String],
    static_col_names: &[String],
    partition_key: &[u8],
    row: &Row,
    static_row: Option<&Row>,
    now_secs: i32,
) -> bool {
    relations.iter().all(|rel| {
        if rel.column.eq_ignore_ascii_case("token") {
            return token_relation_matches(rel, partition_key);
        }
        if rel.column.starts_with('(') {
            return tuple_relation_matches(table_meta, rel, row);
        }
        let current = raw_column_value(
            pk_names,
            ck_names,
            static_col_names,
            &rel.column,
            partition_key,
            row,
            static_row,
            now_secs,
        );
        relation_matches(table_meta, rel, current.as_deref())
    })
}

fn token_relation_matches(rel: &Relation, partition_key: &[u8]) -> bool {
    let current = cassandra_common::murmur3::murmur3_token(partition_key);
    let expected_values = token_expected_values(rel);
    match rel.op {
        RelationOp::Eq => expected_values
            .first()
            .is_some_and(|expected| current == *expected),
        RelationOp::Neq => expected_values
            .first()
            .is_none_or(|expected| current != *expected),
        RelationOp::Lt | RelationOp::Gt | RelationOp::Lte | RelationOp::Gte => {
            let Some(expected) = expected_values.first() else {
                return false;
            };
            match rel.op {
                RelationOp::Lt => current < *expected,
                RelationOp::Gt => current > *expected,
                RelationOp::Lte => current <= *expected,
                RelationOp::Gte => current >= *expected,
                _ => false,
            }
        }
        RelationOp::In => expected_values.iter().any(|expected| current == *expected),
        RelationOp::Contains | RelationOp::ContainsKey | RelationOp::Like => false,
    }
}

fn token_expected_values(rel: &Relation) -> Vec<i64> {
    match (&rel.op, &rel.value) {
        (RelationOp::In, Term::CollectionLiteral(terms) | Term::TupleLiteral(terms)) => {
            terms.iter().filter_map(term_to_i64).collect()
        }
        _ => term_to_i64(&rel.value).into_iter().collect(),
    }
}

fn tuple_relation_matches(table_meta: &TableMetadata, rel: &Relation, row: &Row) -> bool {
    let Some(columns) = relation_tuple_columns(&rel.column) else {
        return true;
    };
    let Some(current) = clustering_tuple_bytes(table_meta, &columns, row) else {
        return false;
    };
    let expected_values = tuple_expected_values(table_meta, &columns, rel);

    match rel.op {
        RelationOp::Eq => expected_values
            .first()
            .is_some_and(|expected| current.as_slice() == expected.as_slice()),
        RelationOp::Neq => expected_values
            .first()
            .is_none_or(|expected| current.as_slice() != expected.as_slice()),
        RelationOp::Lt | RelationOp::Gt | RelationOp::Lte | RelationOp::Gte => {
            let Some(expected) = expected_values.first() else {
                return false;
            };
            match rel.op {
                RelationOp::Lt => current.as_slice() < expected.as_slice(),
                RelationOp::Gt => current.as_slice() > expected.as_slice(),
                RelationOp::Lte => current.as_slice() <= expected.as_slice(),
                RelationOp::Gte => current.as_slice() >= expected.as_slice(),
                _ => false,
            }
        }
        RelationOp::In => expected_values
            .iter()
            .any(|expected| current.as_slice() == expected.as_slice()),
        RelationOp::Contains | RelationOp::ContainsKey | RelationOp::Like => false,
    }
}

fn relation_tuple_columns(column: &str) -> Option<Vec<String>> {
    let inner = column.trim().strip_prefix('(')?.strip_suffix(')')?;
    let columns: Vec<String> = inner
        .split(',')
        .map(|part| part.trim().to_string())
        .filter(|part| !part.is_empty())
        .collect();
    if columns.len() > 1 {
        Some(columns)
    } else {
        None
    }
}

fn tuple_expected_values(
    table_meta: &TableMetadata,
    columns: &[String],
    rel: &Relation,
) -> Vec<Vec<u8>> {
    match (&rel.op, &rel.value) {
        (RelationOp::In, Term::CollectionLiteral(terms)) => terms
            .iter()
            .filter_map(|term| tuple_term_bytes(table_meta, columns, term))
            .collect(),
        _ => tuple_term_bytes(table_meta, columns, &rel.value)
            .into_iter()
            .collect(),
    }
}

fn tuple_term_bytes(
    table_meta: &TableMetadata,
    columns: &[String],
    term: &Term,
) -> Option<Vec<u8>> {
    let Term::TupleLiteral(values) = term else {
        return None;
    };
    if values.len() != columns.len() {
        return None;
    }
    let mut bytes = Vec::new();
    for (column, value) in columns.iter().zip(values) {
        let col_type = &table_column(table_meta, column)?.column_type;
        bytes.extend(typed_term_to_bytes(value, col_type).or_else(|| term_to_bytes(value))?);
    }
    Some(bytes)
}

fn table_column<'a>(
    table_meta: &'a TableMetadata,
    column: &str,
) -> Option<&'a cassandra_schema::ColumnMetadata> {
    table_meta
        .columns
        .iter()
        .find(|metadata| metadata.name.eq_ignore_ascii_case(column))
}

fn clustering_tuple_bytes(
    table_meta: &TableMetadata,
    columns: &[String],
    row: &Row,
) -> Option<Vec<u8>> {
    let clustering_columns = table_meta.clustering_columns();
    if columns.iter().any(|requested| {
        !clustering_columns
            .iter()
            .any(|column| column.name.eq_ignore_ascii_case(requested))
    }) {
        return None;
    }

    let requested_all = clustering_columns.len() == columns.len()
        && clustering_columns
            .iter()
            .zip(columns)
            .all(|(column, requested)| column.name.eq_ignore_ascii_case(requested));
    if requested_all {
        return Some(row.clustering_key.clone());
    }

    let mut offset = 0usize;
    let mut segments: Vec<(&str, &[u8])> = Vec::new();
    for column in clustering_columns {
        let width = fixed_width_cql_type(&column.column_type)?;
        let end = offset.checked_add(width)?;
        let bytes = row.clustering_key.get(offset..end)?;
        segments.push((&column.name, bytes));
        offset = end;
    }
    if offset != row.clustering_key.len() {
        return None;
    }

    let mut selected = Vec::new();
    for requested in columns {
        let (_, bytes) = segments
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case(requested))?;
        selected.extend_from_slice(bytes);
    }
    Some(selected)
}

fn fixed_width_cql_type(cql_type: &CqlType) -> Option<usize> {
    match cql_type {
        CqlType::Boolean | CqlType::Tinyint => Some(1),
        CqlType::Smallint => Some(2),
        CqlType::Int | CqlType::Float | CqlType::Date => Some(4),
        CqlType::Bigint
        | CqlType::Counter
        | CqlType::Double
        | CqlType::Timestamp
        | CqlType::Time => Some(8),
        CqlType::Uuid | CqlType::Timeuuid => Some(16),
        _ => None,
    }
}

fn relation_matches(table_meta: &TableMetadata, rel: &Relation, current: Option<&[u8]>) -> bool {
    let expected_values = relation_expected_values(table_meta, rel);

    match rel.op {
        RelationOp::Eq => {
            matches!((current, expected_values.first().and_then(|v| v.as_deref())), (Some(current), Some(expected)) if current == expected)
        }
        RelationOp::Neq => {
            !matches!((current, expected_values.first().and_then(|v| v.as_deref())), (Some(current), Some(expected)) if current == expected)
        }
        RelationOp::Lt | RelationOp::Gt | RelationOp::Lte | RelationOp::Gte => {
            let Some(current) = current else {
                return false;
            };
            let Some(expected) = expected_values.first().and_then(|v| v.as_deref()) else {
                return false;
            };
            let ordering = current.cmp(expected);
            match rel.op {
                RelationOp::Lt => ordering == std::cmp::Ordering::Less,
                RelationOp::Gt => ordering == std::cmp::Ordering::Greater,
                RelationOp::Lte => ordering != std::cmp::Ordering::Greater,
                RelationOp::Gte => ordering != std::cmp::Ordering::Less,
                _ => false,
            }
        }
        RelationOp::In => {
            let Some(current) = current else {
                return false;
            };
            expected_values
                .iter()
                .any(|expected| expected.as_deref() == Some(current))
        }
        RelationOp::Contains | RelationOp::ContainsKey => {
            let Some(current) = current else {
                return false;
            };
            expected_values
                .iter()
                .filter_map(|expected| expected.as_deref())
                .any(|expected| {
                    !expected.is_empty() && current.windows(expected.len()).any(|w| w == expected)
                })
        }
        RelationOp::Like => {
            let Some(current) = current.and_then(|bytes| std::str::from_utf8(bytes).ok()) else {
                return false;
            };
            let Some(pattern) = expected_values
                .first()
                .and_then(|v| v.as_deref())
                .and_then(|bytes| std::str::from_utf8(bytes).ok())
            else {
                return false;
            };
            like_matches(current, pattern)
        }
    }
}

fn relation_expected_values(table_meta: &TableMetadata, rel: &Relation) -> Vec<Option<Vec<u8>>> {
    let operand_type = relation_operand_type(table_meta, rel);

    match (&rel.op, &rel.value) {
        (RelationOp::In, Term::CollectionLiteral(terms) | Term::TupleLiteral(terms)) => terms
            .iter()
            .map(|term| {
                operand_type
                    .and_then(|cql_type| typed_term_to_bytes(term, cql_type))
                    .or_else(|| term_to_bytes(term))
            })
            .collect(),
        _ => vec![
            operand_type
                .and_then(|cql_type| typed_term_to_bytes(&rel.value, cql_type))
                .or_else(|| term_to_bytes(&rel.value)),
        ],
    }
}

fn relation_operand_type<'a>(table_meta: &'a TableMetadata, rel: &Relation) -> Option<&'a CqlType> {
    let column_type = &table_column(table_meta, &rel.column)?.column_type;
    match (&rel.op, column_type) {
        (RelationOp::Contains, CqlType::List(inner, _) | CqlType::Set(inner, _)) => {
            Some(inner.as_ref())
        }
        (RelationOp::Contains, CqlType::Map(_, value, _)) => Some(value.as_ref()),
        (RelationOp::ContainsKey, CqlType::Map(key, _, _)) => Some(key.as_ref()),
        _ => Some(column_type),
    }
}

fn like_matches(value: &str, pattern: &str) -> bool {
    let value_chars: Vec<char> = value.chars().collect();
    let pattern_chars: Vec<char> = pattern.chars().collect();
    let mut dp = vec![vec![false; value_chars.len() + 1]; pattern_chars.len() + 1];
    dp[0][0] = true;

    for i in 1..=pattern_chars.len() {
        if pattern_chars[i - 1] == '%' {
            dp[i][0] = dp[i - 1][0];
        }
    }

    for i in 1..=pattern_chars.len() {
        for j in 1..=value_chars.len() {
            dp[i][j] = match pattern_chars[i - 1] {
                '%' => dp[i - 1][j] || dp[i][j - 1],
                '_' => dp[i - 1][j - 1],
                ch => dp[i - 1][j - 1] && ch == value_chars[j - 1],
            };
        }
    }

    dp[pattern_chars.len()][value_chars.len()]
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

fn is_count_selector(selector: &Selector) -> bool {
    match selector {
        Selector::Count => true,
        Selector::Alias { selector, .. } => is_count_selector(selector),
        _ => false,
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

fn trigger_mutation_to_commitlog_mutation(
    trigger_mutation: TriggerMutation,
    timestamp: i64,
) -> Mutation {
    let partition_key = trigger_mutation.partition_key.concat();
    let cells = trigger_mutation
        .mutations
        .into_iter()
        .map(|(column, value)| CellMutation {
            column,
            value: Some(value),
            timestamp,
            ttl: 0,
            local_deletion_time: None,
            is_tombstone: false,
        })
        .collect();

    Mutation {
        keyspace: trigger_mutation.keyspace,
        table: trigger_mutation.table,
        partition_key,
        rows: vec![MutationRow {
            clustering_key: Vec::new(),
            cells,
            is_tombstone: false,
            local_deletion_time: None,
        }],
        timestamp,
        cdc_enabled: false,
        static_cells: Vec::new(),
        partition_tombstone: None,
        range_tombstones: Vec::new(),
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

fn requested_permissions(permissions: &[String]) -> Result<Option<Vec<Permission>>, ExecutorError> {
    if permissions
        .iter()
        .any(|permission| permission.eq_ignore_ascii_case("all"))
    {
        return Ok(None);
    }
    permissions
        .iter()
        .map(|permission| parse_permission(permission))
        .collect::<Result<Vec<_>, _>>()
        .map(Some)
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
fn apply_paging(
    mut rows: Vec<Vec<Option<Vec<u8>>>>,
    page_size: Option<i32>,
    paging_state: &Option<Vec<u8>>,
) -> (Vec<Vec<Option<Vec<u8>>>>, Option<Vec<u8>>, Vec<String>) {
    let warnings = Vec::new();
    let start_offset = if let Some(ps) = paging_state {
        if ps.len() >= 4 {
            u32::from_be_bytes([ps[0], ps[1], ps[2], ps[3]]) as usize
        } else {
            0
        }
    } else {
        0
    };
    if start_offset > 0 {
        if start_offset >= rows.len() {
            return (Vec::new(), None, warnings);
        }
        rows = rows.split_off(start_offset);
    }
    let new_paging_state = if let Some(ps) = page_size {
        let ps = ps as usize;
        if ps > 0 && rows.len() > ps {
            rows.truncate(ps);
            let next_offset = (start_offset + ps) as u32;
            Some(next_offset.to_be_bytes().to_vec())
        } else {
            None
        }
    } else {
        None
    };
    (rows, new_paging_state, warnings)
}

fn wrap_select_json(
    json: bool,
    columns: Vec<ResultColumn>,
    rows: Vec<Vec<Option<Vec<u8>>>>,
    paging_state: Option<Vec<u8>>,
    warnings: Vec<String>,
) -> Result<QueryResult, ExecutorError> {
    if !json {
        return Ok(QueryResult::Rows {
            columns,
            rows,
            paging_state,
            warnings,
        });
    }

    let json_column = ResultColumn {
        keyspace: columns
            .first()
            .map(|c| c.keyspace.clone())
            .unwrap_or_default(),
        table: columns.first().map(|c| c.table.clone()).unwrap_or_default(),
        name: "[json]".to_string(),
        cql_type: CqlType::Varchar,
    };

    let json_rows: Vec<Vec<Option<Vec<u8>>>> = rows
        .iter()
        .map(|row| {
            let mut obj = serde_json::Map::new();
            for (col, val) in columns.iter().zip(row.iter()) {
                let json_value = match val {
                    Some(bytes) => cql_bytes_to_json_value(&col.cql_type, bytes),
                    None => serde_json::Value::Null,
                };
                obj.insert(col.name.clone(), json_value);
            }
            vec![Some(
                serde_json::Value::Object(obj).to_string().into_bytes(),
            )]
        })
        .collect();

    Ok(QueryResult::Rows {
        columns: vec![json_column],
        rows: json_rows,
        paging_state,
        warnings,
    })
}

/// Parse a JSON string term into column names and values for INSERT JSON.
fn parse_json_insert(
    json_term: &Term,
    table_meta: &TableMetadata,
    json_default: JsonDefault,
) -> Result<(Vec<String>, Vec<Term>), ExecutorError> {
    let json_str = match json_term {
        Term::Literal(Literal::String(s)) => s.clone(),
        _ => {
            return Err(ExecutorError::InvalidQuery(
                "INSERT JSON requires a string literal".into(),
            ));
        }
    };

    let parsed: serde_json::Value = serde_json::from_str(&json_str).map_err(|err| {
        ExecutorError::InvalidQuery(format!("Invalid INSERT JSON object: {}", err))
    })?;
    let serde_json::Value::Object(object) = parsed else {
        return Err(ExecutorError::InvalidQuery(
            "INSERT JSON value must be a JSON object".into(),
        ));
    };

    let mut columns = Vec::with_capacity(object.len());
    let mut values = Vec::with_capacity(object.len());
    for (column, value) in object {
        columns.push(column);
        values.push(json_value_to_term(value)?);
    }
    if json_default == JsonDefault::Null {
        for column in &table_meta.columns {
            if !columns
                .iter()
                .any(|present| present.eq_ignore_ascii_case(&column.name))
            {
                columns.push(column.name.clone());
                values.push(Term::Literal(Literal::Null));
            }
        }
    }

    Ok((columns, values))
}

fn json_value_to_term(value: serde_json::Value) -> Result<Term, ExecutorError> {
    Ok(match value {
        serde_json::Value::Null => Term::Literal(Literal::Null),
        serde_json::Value::Bool(value) => Term::Literal(Literal::Boolean(value)),
        serde_json::Value::Number(value) => {
            if let Some(value) = value.as_i64() {
                Term::Literal(Literal::Integer(value))
            } else {
                let value = value.as_f64().ok_or_else(|| {
                    ExecutorError::InvalidQuery(format!("Invalid JSON number: {}", value))
                })?;
                Term::Literal(Literal::Float(value))
            }
        }
        serde_json::Value::String(value) => Term::Literal(Literal::String(value)),
        serde_json::Value::Array(values) => Term::CollectionLiteral(
            values
                .into_iter()
                .map(json_value_to_term)
                .collect::<Result<Vec<_>, _>>()?,
        ),
        serde_json::Value::Object(values) => Term::MapLiteral(
            values
                .into_iter()
                .map(|(key, value)| {
                    Ok((
                        Term::Literal(Literal::String(key)),
                        json_value_to_term(value)?,
                    ))
                })
                .collect::<Result<Vec<_>, ExecutorError>>()?,
        ),
    })
}

fn cql_bytes_to_json_value(cql_type: &CqlType, bytes: &[u8]) -> serde_json::Value {
    match CqlValue::deserialize_value(cql_type, bytes) {
        Ok(value) => cql_value_to_json(value),
        Err(_) => serde_json::Value::String(hex_bytes(bytes)),
    }
}

fn cql_value_to_json(value: CqlValue) -> serde_json::Value {
    match value {
        CqlValue::Null => serde_json::Value::Null,
        CqlValue::Ascii(value) | CqlValue::Varchar(value) => serde_json::Value::String(value),
        CqlValue::Int(value) => serde_json::json!(value),
        CqlValue::Smallint(value) => serde_json::json!(value),
        CqlValue::Tinyint(value) => serde_json::json!(value),
        CqlValue::Bigint(value)
        | CqlValue::Counter(value)
        | CqlValue::Timestamp(value)
        | CqlValue::Time(value) => serde_json::json!(value),
        CqlValue::Boolean(value) => serde_json::json!(value),
        CqlValue::Float(value) => serde_json::Number::from_f64(value as f64)
            .map(serde_json::Value::Number)
            .unwrap_or_else(|| serde_json::Value::String(value.to_string())),
        CqlValue::Double(value) => serde_json::Number::from_f64(value)
            .map(serde_json::Value::Number)
            .unwrap_or_else(|| serde_json::Value::String(value.to_string())),
        CqlValue::Blob(bytes) | CqlValue::Varint(bytes) => {
            serde_json::Value::String(hex_bytes(&bytes))
        }
        CqlValue::Decimal { unscaled, scale } => serde_json::Value::String(format!(
            "{}e-{}",
            BigInt::from_signed_bytes_be(&unscaled),
            scale
        )),
        CqlValue::Uuid(bytes) | CqlValue::Timeuuid(bytes) => {
            serde_json::Value::String(uuid::Uuid::from_bytes(bytes).to_string())
        }
        CqlValue::Inet(addr) => serde_json::Value::String(addr.to_string()),
        CqlValue::Date(value) => serde_json::json!(value),
        CqlValue::Duration {
            months,
            days,
            nanoseconds,
        } => serde_json::Value::String(format!("{months}mo{days}d{nanoseconds}ns")),
        CqlValue::Empty => serde_json::Value::String(String::new()),
        CqlValue::List(values) | CqlValue::Set(values) => {
            serde_json::Value::Array(values.into_iter().map(cql_value_to_json).collect())
        }
        CqlValue::Map(entries) => {
            let mut obj = serde_json::Map::new();
            for (key, value) in entries {
                obj.insert(json_object_key(key), cql_value_to_json(value));
            }
            serde_json::Value::Object(obj)
        }
        CqlValue::Tuple(values) => serde_json::Value::Array(
            values
                .into_iter()
                .map(|value| {
                    value
                        .map(cql_value_to_json)
                        .unwrap_or(serde_json::Value::Null)
                })
                .collect(),
        ),
        CqlValue::Udt(fields) => {
            let mut obj = serde_json::Map::new();
            for (name, value) in fields {
                obj.insert(
                    name,
                    value
                        .map(cql_value_to_json)
                        .unwrap_or(serde_json::Value::Null),
                );
            }
            serde_json::Value::Object(obj)
        }
        CqlValue::Vector(vector) => serde_json::Value::Array(
            vector
                .values
                .into_iter()
                .map(|value| {
                    serde_json::Number::from_f64(value as f64)
                        .map(serde_json::Value::Number)
                        .unwrap_or_else(|| serde_json::Value::String(value.to_string()))
                })
                .collect(),
        ),
    }
}

fn json_object_key(value: CqlValue) -> String {
    match cql_value_to_json(value) {
        serde_json::Value::String(value) => value,
        other => other.to_string(),
    }
}

fn hex_bytes(bytes: &[u8]) -> String {
    let mut value = String::from("0x");
    for byte in bytes {
        value.push_str(&format!("{byte:02x}"));
    }
    value
}

#[cfg(test)]
mod tests {
    use super::*;
    use cassandra_cql::ast::{ColumnDef, CqlTypeName, Relation};
    use cassandra_schema::{ColumnMetadata, TableMetadataBuilder};
    use cassandra_security::{AllowAllAuthorizer, CassandraAuthorizer, InMemoryRoleManager};
    use cassandra_storage::commitlog::CommitLogConfig;
    use cassandra_storage::engine::EngineConfig;
    use std::sync::atomic::Ordering;
    use tempfile::TempDir;

    fn int_term(value: i64) -> Term {
        Term::Literal(Literal::Integer(value))
    }

    fn text_term(value: &str) -> Term {
        Term::Literal(Literal::String(value.to_string()))
    }

    fn blob_term(value: &[u8]) -> Term {
        Term::Literal(Literal::Blob(value.to_vec()))
    }

    #[test]
    fn vector_literal_bytes_are_big_endian_f32() {
        let bytes = vector_literal_to_bytes(&[1.0, -2.5, 3.25]);
        let expected = [1.0_f32, -2.5, 3.25]
            .into_iter()
            .flat_map(f32::to_be_bytes)
            .collect::<Vec<_>>();
        assert_eq!(bytes, expected);
    }

    #[test]
    fn token_relation_filters_partition_key_bytes() {
        let token = cassandra_common::murmur3::murmur3_token(&1i32.to_be_bytes());
        let rel = Relation {
            column: "token".to_string(),
            op: RelationOp::Eq,
            value: int_term(token),
        };

        assert!(token_relation_matches(&rel, &1i32.to_be_bytes()));
        assert!(!token_relation_matches(&rel, &2i32.to_be_bytes()));
    }

    #[test]
    fn tuple_relation_filters_clustering_bytes() {
        let table = TableMetadataBuilder::new("ks", "tuple_events")
            .add_column(ColumnMetadata::partition_key("pk", 0, CqlType::Int))
            .add_column(ColumnMetadata::clustering(
                "ck1",
                0,
                CqlType::Int,
                ClusteringOrder::Asc,
            ))
            .add_column(ColumnMetadata::clustering(
                "ck2",
                1,
                CqlType::Int,
                ClusteringOrder::Asc,
            ))
            .build();
        let row = Row {
            clustering_key: [1i32.to_be_bytes(), 2i32.to_be_bytes()].concat(),
            cells: Vec::new(),
            is_tombstone: false,
            local_deletion_time: None,
        };

        let eq = Relation {
            column: "(ck1,ck2)".to_string(),
            op: RelationOp::Eq,
            value: Term::TupleLiteral(vec![int_term(1), int_term(2)]),
        };
        let gt = Relation {
            column: "(ck1,ck2)".to_string(),
            op: RelationOp::Gt,
            value: Term::TupleLiteral(vec![int_term(1), int_term(1)]),
        };
        let lt = Relation {
            column: "(ck1,ck2)".to_string(),
            op: RelationOp::Lt,
            value: Term::TupleLiteral(vec![int_term(1), int_term(1)]),
        };

        assert!(tuple_relation_matches(&table, &eq, &row));
        assert!(tuple_relation_matches(&table, &gt, &row));
        assert!(!tuple_relation_matches(&table, &lt, &row));
    }

    #[test]
    fn contains_relations_use_collection_element_types() {
        let table = TableMetadataBuilder::new("ks", "collections")
            .add_column(ColumnMetadata::partition_key("pk", 0, CqlType::Int))
            .add_column(ColumnMetadata::regular(
                "tags",
                CqlType::List(Box::new(CqlType::Int), false),
            ))
            .add_column(ColumnMetadata::regular(
                "attrs",
                CqlType::Map(Box::new(CqlType::Varchar), Box::new(CqlType::Int), false),
            ))
            .build();
        let tags = typed_term_to_bytes(
            &Term::CollectionLiteral(vec![int_term(7), int_term(9)]),
            &CqlType::List(Box::new(CqlType::Int), false),
        )
        .unwrap();
        let attrs = typed_term_to_bytes(
            &Term::MapLiteral(vec![(text_term("k"), int_term(11))]),
            &CqlType::Map(Box::new(CqlType::Varchar), Box::new(CqlType::Int), false),
        )
        .unwrap();

        assert!(relation_matches(
            &table,
            &Relation {
                column: "tags".to_string(),
                op: RelationOp::Contains,
                value: int_term(7),
            },
            Some(&tags),
        ));
        assert!(relation_matches(
            &table,
            &Relation {
                column: "attrs".to_string(),
                op: RelationOp::ContainsKey,
                value: text_term("k"),
            },
            Some(&attrs),
        ));
    }

    #[test]
    fn insert_json_parses_nested_json_values() {
        let table = TableMetadataBuilder::new("ks", "json_events")
            .add_column(ColumnMetadata::partition_key("id", 0, CqlType::Int))
            .add_column(ColumnMetadata::regular(
                "tags",
                CqlType::List(Box::new(CqlType::Varchar), false),
            ))
            .add_column(ColumnMetadata::regular(
                "attrs",
                CqlType::Map(
                    Box::new(CqlType::Varchar),
                    Box::new(CqlType::Varchar),
                    false,
                ),
            ))
            .add_column(ColumnMetadata::regular("active", CqlType::Boolean))
            .build();

        let (columns, values) = parse_json_insert(
            &text_term(r#"{"id":1,"tags":["a","b"],"attrs":{"tier":"gold"},"active":true}"#),
            &table,
            JsonDefault::Unset,
        )
        .unwrap();

        assert_eq!(columns, vec!["active", "attrs", "id", "tags"]);
        assert_eq!(values[0], Term::Literal(Literal::Boolean(true)));
        assert!(matches!(values[1], Term::MapLiteral(ref entries) if entries.len() == 1));
        assert_eq!(values[2], Term::Literal(Literal::Integer(1)));
        assert!(matches!(values[3], Term::CollectionLiteral(ref items) if items.len() == 2));
    }

    #[test]
    fn insert_json_default_null_adds_omitted_columns() {
        let table = TableMetadataBuilder::new("ks", "json_events")
            .add_column(ColumnMetadata::partition_key("id", 0, CqlType::Int))
            .add_column(ColumnMetadata::regular("name", CqlType::Varchar))
            .build();

        let (columns, values) =
            parse_json_insert(&text_term(r#"{"id":1}"#), &table, JsonDefault::Null).unwrap();

        assert_eq!(columns, vec!["id", "name"]);
        assert_eq!(values[0], Term::Literal(Literal::Integer(1)));
        assert_eq!(values[1], Term::Literal(Literal::Null));
    }

    #[test]
    fn vector_index_detection_requires_sai_dimensions() {
        let mut options = std::collections::HashMap::new();
        options.insert(
            "class_name".to_string(),
            "org.apache.cassandra.index.sai.StorageAttachedIndex".to_string(),
        );
        options.insert("vector_dimensions".to_string(), "3".to_string());
        let vector_index = IndexMetadata::new(
            "idx".to_string(),
            "idx".to_string(),
            cassandra_schema::index::IndexKind::Custom,
            options,
        );
        assert!(is_vector_index(&vector_index));

        let no_dimensions = IndexMetadata::new(
            "idx".to_string(),
            "idx".to_string(),
            cassandra_schema::index::IndexKind::Custom,
            std::collections::HashMap::from([(
                "class_name".to_string(),
                "org.apache.cassandra.index.sai.StorageAttachedIndex".to_string(),
            )]),
        );
        assert!(!is_vector_index(&no_dimensions));
    }

    fn test_executor() -> (QueryExecutor, TempDir) {
        test_executor_with_authorizer(false)
    }

    fn test_executor_with_authorizer(cassandra_authorizer: bool) -> (QueryExecutor, TempDir) {
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

        let table = TableMetadataBuilder::new("ks", "events")
            .add_column(ColumnMetadata::partition_key("id", 0, CqlType::Int))
            .add_column(ColumnMetadata::clustering(
                "bucket",
                0,
                CqlType::Int,
                ClusteringOrder::Asc,
            ))
            .add_column(ColumnMetadata::regular("name", CqlType::Varchar))
            .build();
        let audit_table = TableMetadataBuilder::new("ks", "audit")
            .add_column(ColumnMetadata::partition_key("id", 0, CqlType::Int))
            .add_column(ColumnMetadata::regular("event_type", CqlType::Varchar))
            .build();
        let keyspace = KeyspaceMetadata::new("ks", KeyspaceParams::default())
            .with_table(table)
            .with_table(audit_table);
        let catalog = Arc::new(RwLock::new(SchemaCatalog::new().with_keyspace(keyspace)));

        let role_manager = Arc::new(InMemoryRoleManager::new());
        let authorizer: Arc<dyn Authorizer> = if cassandra_authorizer {
            Arc::new(CassandraAuthorizer::new(Arc::clone(&role_manager)))
        } else {
            Arc::new(AllowAllAuthorizer)
        };
        let executor = QueryExecutor::new(engine, catalog, role_manager, authorizer);
        (executor, temp)
    }

    fn insert_event(executor: &QueryExecutor, id: i64, bucket: i64, name: &str) {
        let plan = QueryPlan::Insert(InsertPlan {
            keyspace: "ks".to_string(),
            table: "events".to_string(),
            columns: vec!["id".to_string(), "bucket".to_string(), "name".to_string()],
            values: vec![int_term(id), int_term(bucket), text_term(name)],
            if_not_exists: false,
            json: None,
            json_default: JsonDefault::Null,
            using_timestamp: None,
            using_ttl: None,
        });
        executor.execute(&plan, None).unwrap();
    }

    fn insert_event_without_name(executor: &QueryExecutor, id: i64, bucket: i64) {
        let plan = QueryPlan::Insert(InsertPlan {
            keyspace: "ks".to_string(),
            table: "events".to_string(),
            columns: vec!["id".to_string(), "bucket".to_string()],
            values: vec![int_term(id), int_term(bucket)],
            if_not_exists: false,
            json: None,
            json_default: JsonDefault::Null,
            using_timestamp: None,
            using_ttl: None,
        });
        executor.execute(&plan, None).unwrap();
    }

    fn add_event_payload_column(executor: &QueryExecutor) {
        executor
            .execute(
                &QueryPlan::AlterTable(AlterTablePlan {
                    keyspace: "ks".to_string(),
                    name: "events".to_string(),
                    operation: AlterTableOp::AddColumn(ColumnDef {
                        name: "payload".to_string(),
                        cql_type: CqlTypeName::Simple("blob".to_string()),
                        is_static: false,
                        masked_with: None,
                        constraints: Vec::new(),
                    }),
                }),
                None,
            )
            .unwrap();
    }

    fn insert_event_with_payload(
        executor: &QueryExecutor,
        id: i64,
        bucket: i64,
        name: &str,
        payload: &[u8],
    ) {
        let plan = QueryPlan::Insert(InsertPlan {
            keyspace: "ks".to_string(),
            table: "events".to_string(),
            columns: vec![
                "id".to_string(),
                "bucket".to_string(),
                "name".to_string(),
                "payload".to_string(),
            ],
            values: vec![
                int_term(id),
                int_term(bucket),
                text_term(name),
                blob_term(payload),
            ],
            if_not_exists: false,
            json: None,
            json_default: JsonDefault::Null,
            using_timestamp: None,
            using_ttl: None,
        });
        executor.execute(&plan, None).unwrap();
    }

    fn mask_event_name(executor: &QueryExecutor, function_name: &str, args: Vec<Term>) {
        mask_event_column(executor, "name", function_name, args);
    }

    fn mask_event_column(
        executor: &QueryExecutor,
        column_name: &str,
        function_name: &str,
        args: Vec<Term>,
    ) {
        executor
            .execute(
                &QueryPlan::AlterTable(AlterTablePlan {
                    keyspace: "ks".to_string(),
                    name: "events".to_string(),
                    operation: AlterTableOp::MaskColumn(
                        column_name.to_string(),
                        function_name.to_string(),
                        args,
                    ),
                }),
                None,
            )
            .unwrap();
    }

    fn create_role_plan(name: &str) -> QueryPlan {
        QueryPlan::CreateRole(CreateRolePlan {
            name: name.to_string(),
            if_not_exists: false,
            password: None,
            hashed_password: None,
            is_superuser: false,
            can_login: true,
            datacenter_access: None,
            cidr_access: None,
            options: Default::default(),
        })
    }

    fn create_int_state_function(executor: &QueryExecutor, name: &str, return_type: &str) {
        executor
            .execute(
                &QueryPlan::CreateFunction(CreateFunctionPlan {
                    keyspace: "ks".to_string(),
                    name: name.to_string(),
                    or_replace: false,
                    if_not_exists: false,
                    args: vec![
                        ("state".to_string(), "int".to_string()),
                        ("value".to_string(), "int".to_string()),
                    ],
                    called_on_null_input: false,
                    return_type: return_type.to_string(),
                    language: "java".to_string(),
                    body: "return state + value;".to_string(),
                }),
                None,
            )
            .unwrap();
    }

    fn create_int_identity_function(executor: &QueryExecutor, name: &str) {
        executor
            .execute(
                &QueryPlan::CreateFunction(CreateFunctionPlan {
                    keyspace: "ks".to_string(),
                    name: name.to_string(),
                    or_replace: false,
                    if_not_exists: false,
                    args: vec![("value".to_string(), "int".to_string())],
                    called_on_null_input: false,
                    return_type: "int".to_string(),
                    language: "java".to_string(),
                    body: "return value;".to_string(),
                }),
                None,
            )
            .unwrap();
    }

    fn create_java_binary_function(
        executor: &QueryExecutor,
        name: &str,
        cql_type: &str,
        body: &str,
    ) -> Vec<(String, String)> {
        create_java_binary_function_with_return(executor, name, cql_type, cql_type, body)
    }

    fn create_java_binary_function_with_return(
        executor: &QueryExecutor,
        name: &str,
        arg_type: &str,
        return_type: &str,
        body: &str,
    ) -> Vec<(String, String)> {
        let args = vec![
            ("a".to_string(), arg_type.to_string()),
            ("b".to_string(), arg_type.to_string()),
        ];
        executor
            .execute(
                &QueryPlan::CreateFunction(CreateFunctionPlan {
                    keyspace: "ks".to_string(),
                    name: name.to_string(),
                    or_replace: false,
                    if_not_exists: false,
                    args: args.clone(),
                    called_on_null_input: false,
                    return_type: return_type.to_string(),
                    language: "java".to_string(),
                    body: body.to_string(),
                }),
                None,
            )
            .unwrap();
        args
    }

    fn create_java_unary_function_with_return(
        executor: &QueryExecutor,
        name: &str,
        arg_type: &str,
        return_type: &str,
        body: &str,
    ) -> Vec<(String, String)> {
        let args = vec![("value".to_string(), arg_type.to_string())];
        executor
            .execute(
                &QueryPlan::CreateFunction(CreateFunctionPlan {
                    keyspace: "ks".to_string(),
                    name: name.to_string(),
                    or_replace: false,
                    if_not_exists: false,
                    args: args.clone(),
                    called_on_null_input: false,
                    return_type: return_type.to_string(),
                    language: "java".to_string(),
                    body: body.to_string(),
                }),
                None,
            )
            .unwrap();
        args
    }

    fn create_java_function_with_args(
        executor: &QueryExecutor,
        name: &str,
        args: Vec<(String, String)>,
        return_type: &str,
        body: &str,
    ) -> Vec<(String, String)> {
        executor
            .execute(
                &QueryPlan::CreateFunction(CreateFunctionPlan {
                    keyspace: "ks".to_string(),
                    name: name.to_string(),
                    or_replace: false,
                    if_not_exists: false,
                    args: args.clone(),
                    called_on_null_input: false,
                    return_type: return_type.to_string(),
                    language: "java".to_string(),
                    body: body.to_string(),
                }),
                None,
            )
            .unwrap();
        args
    }

    fn create_int_final_function(executor: &QueryExecutor, name: &str, return_type: &str) {
        executor
            .execute(
                &QueryPlan::CreateFunction(CreateFunctionPlan {
                    keyspace: "ks".to_string(),
                    name: name.to_string(),
                    or_replace: false,
                    if_not_exists: false,
                    args: vec![("state".to_string(), "int".to_string())],
                    called_on_null_input: false,
                    return_type: return_type.to_string(),
                    language: "java".to_string(),
                    body: "return state;".to_string(),
                }),
                None,
            )
            .unwrap();
    }

    fn create_tuple_state_function(executor: &QueryExecutor, name: &str) {
        executor
            .execute(
                &QueryPlan::CreateFunction(CreateFunctionPlan {
                    keyspace: "ks".to_string(),
                    name: name.to_string(),
                    or_replace: false,
                    if_not_exists: false,
                    args: vec![
                        ("state".to_string(), "tuple<int, bigint>".to_string()),
                        ("value".to_string(), "int".to_string()),
                    ],
                    called_on_null_input: false,
                    return_type: "tuple<int, bigint>".to_string(),
                    language: "java".to_string(),
                    body: "return state;".to_string(),
                }),
                None,
            )
            .unwrap();
    }

    fn create_text_state_function(executor: &QueryExecutor, name: &str) {
        executor
            .execute(
                &QueryPlan::CreateFunction(CreateFunctionPlan {
                    keyspace: "ks".to_string(),
                    name: name.to_string(),
                    or_replace: false,
                    if_not_exists: false,
                    args: vec![
                        ("state".to_string(), "text".to_string()),
                        ("value".to_string(), "text".to_string()),
                    ],
                    called_on_null_input: false,
                    return_type: "text".to_string(),
                    language: "java".to_string(),
                    body: "return state;".to_string(),
                }),
                None,
            )
            .unwrap();
    }

    fn create_text_final_upper_function(executor: &QueryExecutor, name: &str) {
        executor
            .execute(
                &QueryPlan::CreateFunction(CreateFunctionPlan {
                    keyspace: "ks".to_string(),
                    name: name.to_string(),
                    or_replace: false,
                    if_not_exists: false,
                    args: vec![("state".to_string(), "text".to_string())],
                    called_on_null_input: false,
                    return_type: "text".to_string(),
                    language: "java".to_string(),
                    body: "return state.toUpperCase();".to_string(),
                }),
                None,
            )
            .unwrap();
    }

    fn create_text_upper_function(executor: &QueryExecutor, name: &str) {
        executor
            .execute(
                &QueryPlan::CreateFunction(CreateFunctionPlan {
                    keyspace: "ks".to_string(),
                    name: name.to_string(),
                    or_replace: false,
                    if_not_exists: false,
                    args: vec![("value".to_string(), "text".to_string())],
                    called_on_null_input: false,
                    return_type: "text".to_string(),
                    language: "java".to_string(),
                    body: "return value.toUpperCase();".to_string(),
                }),
                None,
            )
            .unwrap();
    }

    struct SumTwoIntStateFunction;

    impl UdfExecutor for SumTwoIntStateFunction {
        fn execute(&self, args: &[UdfValue]) -> Result<UdfValue, String> {
            match args {
                [
                    UdfValue::Int(state),
                    UdfValue::Int(left),
                    UdfValue::Int(right),
                ] => Ok(UdfValue::Int(
                    state.saturating_add(*left).saturating_add(*right),
                )),
                other => Err(format!("unexpected aggregate arguments: {other:?}")),
            }
        }

        fn language(&self) -> &str {
            "rust"
        }
    }

    fn register_sum_two_int_aggregate(executor: &QueryExecutor) {
        executor
            .udf_registry
            .register(
                UdfMetadata {
                    keyspace: "ks".to_string(),
                    name: "plus_pair".to_string(),
                    args: vec![
                        ("state".to_string(), "int".to_string()),
                        ("arg0".to_string(), "int".to_string()),
                        ("arg1".to_string(), "int".to_string()),
                    ],
                    return_type: "int".to_string(),
                    language: "rust".to_string(),
                    body: String::new(),
                    called_on_null_input: false,
                },
                Arc::new(SumTwoIntStateFunction),
            )
            .unwrap();
        executor
            .uda_registry
            .register(UdaMetadata {
                keyspace: "ks".to_string(),
                name: "sum_pair".to_string(),
                arg_types: vec!["int".to_string(), "int".to_string()],
                state_type: "int".to_string(),
                return_type: "int".to_string(),
                sfunc_name: "plus_pair".to_string(),
                finalfunc_name: None,
                initcond: Some("0".to_string()),
            })
            .unwrap();
    }

    #[test]
    fn create_java_function_registers_compatible_executor() {
        let (executor, _temp) = test_executor();
        let echo_args = vec![("val".to_string(), "text".to_string())];

        executor
            .execute(
                &QueryPlan::CreateFunction(CreateFunctionPlan {
                    keyspace: "ks".to_string(),
                    name: "echo".to_string(),
                    or_replace: false,
                    if_not_exists: false,
                    args: echo_args.clone(),
                    called_on_null_input: false,
                    return_type: "text".to_string(),
                    language: "java".to_string(),
                    body: "return val;".to_string(),
                }),
                None,
            )
            .unwrap();

        let (_, echo) = executor
            .udf_registry
            .get("ks", "echo", &echo_args)
            .expect("compatible Java UDF should be executable");
        assert_eq!(echo.language(), "java");
        assert_eq!(
            echo.execute(&[UdfValue::Text("Ada".to_string())]).unwrap(),
            UdfValue::Text("Ada".to_string())
        );

        let plus_args = vec![
            ("a".to_string(), "int".to_string()),
            ("b".to_string(), "int".to_string()),
        ];
        executor
            .execute(
                &QueryPlan::CreateFunction(CreateFunctionPlan {
                    keyspace: "ks".to_string(),
                    name: "plus".to_string(),
                    or_replace: false,
                    if_not_exists: false,
                    args: plus_args.clone(),
                    called_on_null_input: false,
                    return_type: "int".to_string(),
                    language: "java".to_string(),
                    body: "return a + b;".to_string(),
                }),
                None,
            )
            .unwrap();

        let (_, plus) = executor
            .udf_registry
            .get("ks", "plus", &plus_args)
            .expect("compatible Java addition UDF should be executable");
        assert_eq!(
            plus.execute(&[UdfValue::Int(2), UdfValue::Int(3)]).unwrap(),
            UdfValue::Int(5)
        );

        let minus_args = vec![
            ("a".to_string(), "int".to_string()),
            ("b".to_string(), "int".to_string()),
        ];
        executor
            .execute(
                &QueryPlan::CreateFunction(CreateFunctionPlan {
                    keyspace: "ks".to_string(),
                    name: "minus".to_string(),
                    or_replace: false,
                    if_not_exists: false,
                    args: minus_args.clone(),
                    called_on_null_input: false,
                    return_type: "int".to_string(),
                    language: "java".to_string(),
                    body: "return a - b;".to_string(),
                }),
                None,
            )
            .unwrap();

        let (_, minus) = executor
            .udf_registry
            .get("ks", "minus", &minus_args)
            .expect("compatible Java subtraction UDF should be executable");
        assert_eq!(
            minus
                .execute(&[UdfValue::Int(7), UdfValue::Int(2)])
                .unwrap(),
            UdfValue::Int(5)
        );

        let multiply_args = vec![
            ("a".to_string(), "int".to_string()),
            ("b".to_string(), "int".to_string()),
        ];
        executor
            .execute(
                &QueryPlan::CreateFunction(CreateFunctionPlan {
                    keyspace: "ks".to_string(),
                    name: "multiply".to_string(),
                    or_replace: false,
                    if_not_exists: false,
                    args: multiply_args.clone(),
                    called_on_null_input: false,
                    return_type: "int".to_string(),
                    language: "java".to_string(),
                    body: "return a * b;".to_string(),
                }),
                None,
            )
            .unwrap();

        let (_, multiply) = executor
            .udf_registry
            .get("ks", "multiply", &multiply_args)
            .expect("compatible Java multiplication UDF should be executable");
        assert_eq!(
            multiply
                .execute(&[UdfValue::Int(6), UdfValue::Int(7)])
                .unwrap(),
            UdfValue::Int(42)
        );

        let divide_args = vec![
            ("a".to_string(), "int".to_string()),
            ("b".to_string(), "int".to_string()),
        ];
        executor
            .execute(
                &QueryPlan::CreateFunction(CreateFunctionPlan {
                    keyspace: "ks".to_string(),
                    name: "divide".to_string(),
                    or_replace: false,
                    if_not_exists: false,
                    args: divide_args.clone(),
                    called_on_null_input: false,
                    return_type: "int".to_string(),
                    language: "java".to_string(),
                    body: "return a / b;".to_string(),
                }),
                None,
            )
            .unwrap();

        let (_, divide) = executor
            .udf_registry
            .get("ks", "divide", &divide_args)
            .expect("compatible Java division UDF should be executable");
        assert_eq!(
            divide
                .execute(&[UdfValue::Int(7), UdfValue::Int(2)])
                .unwrap(),
            UdfValue::Int(3)
        );

        let smallint_args =
            create_java_binary_function(&executor, "smallint_plus", "smallint", "return a + b;");
        let (_, smallint_plus) = executor
            .udf_registry
            .get("ks", "smallint_plus", &smallint_args)
            .expect("compatible Java smallint addition UDF should be executable");
        assert_eq!(
            smallint_plus
                .execute(&[UdfValue::Smallint(300), UdfValue::Smallint(22)])
                .unwrap(),
            UdfValue::Smallint(322)
        );

        let tinyint_args =
            create_java_binary_function(&executor, "tinyint_multiply", "tinyint", "return a * b;");
        let (_, tinyint_multiply) = executor
            .udf_registry
            .get("ks", "tinyint_multiply", &tinyint_args)
            .expect("compatible Java tinyint multiplication UDF should be executable");
        assert_eq!(
            tinyint_multiply
                .execute(&[UdfValue::Tinyint(6), UdfValue::Tinyint(7)])
                .unwrap(),
            UdfValue::Tinyint(42)
        );

        let double_args =
            create_java_binary_function(&executor, "double_minus", "double", "return a - b;");
        let (_, double_minus) = executor
            .udf_registry
            .get("ks", "double_minus", &double_args)
            .expect("compatible Java double subtraction UDF should be executable");
        assert_eq!(
            double_minus
                .execute(&[UdfValue::Double(7.5), UdfValue::Double(2.25)])
                .unwrap(),
            UdfValue::Double(5.25)
        );

        let float_args =
            create_java_binary_function(&executor, "float_divide", "float", "return a / b;");
        let (_, float_divide) = executor
            .udf_registry
            .get("ks", "float_divide", &float_args)
            .expect("compatible Java float division UDF should be executable");
        assert_eq!(
            float_divide
                .execute(&[UdfValue::Float(7.5), UdfValue::Float(2.5)])
                .unwrap(),
            UdfValue::Float(3.0)
        );

        let modulo_args = create_java_binary_function(&executor, "modulo", "int", "return a % b;");
        let (_, modulo) = executor
            .udf_registry
            .get("ks", "modulo", &modulo_args)
            .expect("compatible Java modulo UDF should be executable");
        assert_eq!(
            modulo
                .execute(&[UdfValue::Int(17), UdfValue::Int(5)])
                .unwrap(),
            UdfValue::Int(2)
        );

        let greater_args = create_java_binary_function_with_return(
            &executor,
            "is_greater",
            "int",
            "boolean",
            "return a > b;",
        );
        let (_, is_greater) = executor
            .udf_registry
            .get("ks", "is_greater", &greater_args)
            .expect("compatible Java greater-than UDF should be executable");
        assert_eq!(
            is_greater
                .execute(&[UdfValue::Int(9), UdfValue::Int(3)])
                .unwrap(),
            UdfValue::Boolean(true)
        );

        let lte_args = create_java_binary_function_with_return(
            &executor,
            "double_lte",
            "double",
            "boolean",
            "return a <= b;",
        );
        let (_, double_lte) = executor
            .udf_registry
            .get("ks", "double_lte", &lte_args)
            .expect("compatible Java less-than-or-equal UDF should be executable");
        assert_eq!(
            double_lte
                .execute(&[UdfValue::Double(2.5), UdfValue::Double(2.5)])
                .unwrap(),
            UdfValue::Boolean(true)
        );

        let ne_args = create_java_binary_function_with_return(
            &executor,
            "smallint_ne",
            "smallint",
            "boolean",
            "return a != b;",
        );
        let (_, smallint_ne) = executor
            .udf_registry
            .get("ks", "smallint_ne", &ne_args)
            .expect("compatible Java not-equal UDF should be executable");
        assert_eq!(
            smallint_ne
                .execute(&[UdfValue::Smallint(7), UdfValue::Smallint(8)])
                .unwrap(),
            UdfValue::Boolean(true)
        );

        let text_equals_args = create_java_binary_function_with_return(
            &executor,
            "text_equals",
            "text",
            "boolean",
            "return a.equals(b);",
        );
        let (_, text_equals) = executor
            .udf_registry
            .get("ks", "text_equals", &text_equals_args)
            .expect("compatible Java text equals UDF should be executable");
        assert_eq!(
            text_equals
                .execute(&[
                    UdfValue::Text("ada".to_string()),
                    UdfValue::Text("ada".to_string())
                ])
                .unwrap(),
            UdfValue::Boolean(true)
        );

        let text_not_equals_args = create_java_binary_function_with_return(
            &executor,
            "text_not_equals",
            "text",
            "boolean",
            "return !a.equals(b);",
        );
        let (_, text_not_equals) = executor
            .udf_registry
            .get("ks", "text_not_equals", &text_not_equals_args)
            .expect("compatible Java text not-equals UDF should be executable");
        assert_eq!(
            text_not_equals
                .execute(&[
                    UdfValue::Text("ada".to_string()),
                    UdfValue::Text("grace".to_string())
                ])
                .unwrap(),
            UdfValue::Boolean(true)
        );

        let length_args = create_java_unary_function_with_return(
            &executor,
            "text_length",
            "text",
            "int",
            "return value.length();",
        );
        let (_, text_length) = executor
            .udf_registry
            .get("ks", "text_length", &length_args)
            .expect("compatible Java text length UDF should be executable");
        assert_eq!(
            text_length
                .execute(&[UdfValue::Text("cafe".to_string())])
                .unwrap(),
            UdfValue::Int(4)
        );

        let trim_args = create_java_unary_function_with_return(
            &executor,
            "text_trim",
            "text",
            "text",
            "return value.trim();",
        );
        let (_, text_trim) = executor
            .udf_registry
            .get("ks", "text_trim", &trim_args)
            .expect("compatible Java text trim UDF should be executable");
        assert_eq!(
            text_trim
                .execute(&[UdfValue::Text("  ada  ".to_string())])
                .unwrap(),
            UdfValue::Text("ada".to_string())
        );

        let empty_args = create_java_unary_function_with_return(
            &executor,
            "text_empty",
            "text",
            "boolean",
            "return value.isEmpty();",
        );
        let (_, text_empty) = executor
            .udf_registry
            .get("ks", "text_empty", &empty_args)
            .expect("compatible Java text isEmpty UDF should be executable");
        assert_eq!(
            text_empty
                .execute(&[UdfValue::Text(String::new())])
                .unwrap(),
            UdfValue::Boolean(true)
        );

        let contains_args = create_java_binary_function_with_return(
            &executor,
            "text_contains",
            "text",
            "boolean",
            "return a.contains(b);",
        );
        let (_, text_contains) = executor
            .udf_registry
            .get("ks", "text_contains", &contains_args)
            .expect("compatible Java text contains UDF should be executable");
        assert_eq!(
            text_contains
                .execute(&[
                    UdfValue::Text("cassandra".to_string()),
                    UdfValue::Text("sand".to_string())
                ])
                .unwrap(),
            UdfValue::Boolean(true)
        );

        let starts_with_args = create_java_binary_function_with_return(
            &executor,
            "text_starts_with",
            "text",
            "boolean",
            "return a.startsWith(b);",
        );
        let (_, text_starts_with) = executor
            .udf_registry
            .get("ks", "text_starts_with", &starts_with_args)
            .expect("compatible Java text startsWith UDF should be executable");
        assert_eq!(
            text_starts_with
                .execute(&[
                    UdfValue::Text("cassandra".to_string()),
                    UdfValue::Text("cass".to_string())
                ])
                .unwrap(),
            UdfValue::Boolean(true)
        );

        let ends_with_args = create_java_binary_function_with_return(
            &executor,
            "text_ends_with",
            "text",
            "boolean",
            "return a.endsWith(b);",
        );
        let (_, text_ends_with) = executor
            .udf_registry
            .get("ks", "text_ends_with", &ends_with_args)
            .expect("compatible Java text endsWith UDF should be executable");
        assert_eq!(
            text_ends_with
                .execute(&[
                    UdfValue::Text("cassandra".to_string()),
                    UdfValue::Text("dra".to_string())
                ])
                .unwrap(),
            UdfValue::Boolean(true)
        );

        let substring_args = create_java_function_with_args(
            &executor,
            "text_substring",
            vec![
                ("value".to_string(), "text".to_string()),
                ("begin".to_string(), "int".to_string()),
            ],
            "text",
            "return value.substring(begin);",
        );
        let (_, text_substring) = executor
            .udf_registry
            .get("ks", "text_substring", &substring_args)
            .expect("compatible Java text substring UDF should be executable");
        assert_eq!(
            text_substring
                .execute(&[UdfValue::Text("cassandra".to_string()), UdfValue::Int(4)])
                .unwrap(),
            UdfValue::Text("andra".to_string())
        );

        let substring_range_args = create_java_function_with_args(
            &executor,
            "text_substring_range",
            vec![
                ("value".to_string(), "text".to_string()),
                ("begin".to_string(), "int".to_string()),
                ("end".to_string(), "int".to_string()),
            ],
            "text",
            "return value.substring(begin, end);",
        );
        let (_, text_substring_range) = executor
            .udf_registry
            .get("ks", "text_substring_range", &substring_range_args)
            .expect("compatible Java text substring range UDF should be executable");
        assert_eq!(
            text_substring_range
                .execute(&[
                    UdfValue::Text("cassandra".to_string()),
                    UdfValue::Int(1),
                    UdfValue::Int(4)
                ])
                .unwrap(),
            UdfValue::Text("ass".to_string())
        );

        let index_of_args = create_java_binary_function_with_return(
            &executor,
            "text_index_of",
            "text",
            "int",
            "return a.indexOf(b);",
        );
        let (_, text_index_of) = executor
            .udf_registry
            .get("ks", "text_index_of", &index_of_args)
            .expect("compatible Java text indexOf UDF should be executable");
        assert_eq!(
            text_index_of
                .execute(&[
                    UdfValue::Text("cassandra".to_string()),
                    UdfValue::Text("sand".to_string())
                ])
                .unwrap(),
            UdfValue::Int(3)
        );

        let last_index_of_args = create_java_binary_function_with_return(
            &executor,
            "text_last_index_of",
            "text",
            "int",
            "return a.lastIndexOf(b);",
        );
        let (_, text_last_index_of) = executor
            .udf_registry
            .get("ks", "text_last_index_of", &last_index_of_args)
            .expect("compatible Java text lastIndexOf UDF should be executable");
        assert_eq!(
            text_last_index_of
                .execute(&[
                    UdfValue::Text("cassandra".to_string()),
                    UdfValue::Text("a".to_string())
                ])
                .unwrap(),
            UdfValue::Int(8)
        );

        let replace_args = create_java_function_with_args(
            &executor,
            "text_replace",
            vec![
                ("value".to_string(), "text".to_string()),
                ("target".to_string(), "text".to_string()),
                ("replacement".to_string(), "text".to_string()),
            ],
            "text",
            "return value.replace(target, replacement);",
        );
        let (_, text_replace) = executor
            .udf_registry
            .get("ks", "text_replace", &replace_args)
            .expect("compatible Java text replace UDF should be executable");
        assert_eq!(
            text_replace
                .execute(&[
                    UdfValue::Text("cassandra".to_string()),
                    UdfValue::Text("a".to_string()),
                    UdfValue::Text("A".to_string())
                ])
                .unwrap(),
            UdfValue::Text("cAssAndrA".to_string())
        );

        let negate_args = create_java_unary_function_with_return(
            &executor,
            "negate_int",
            "int",
            "int",
            "return -value;",
        );
        let (_, negate_int) = executor
            .udf_registry
            .get("ks", "negate_int", &negate_args)
            .expect("compatible Java unary negation UDF should be executable");
        assert_eq!(
            negate_int.execute(&[UdfValue::Int(7)]).unwrap(),
            UdfValue::Int(-7)
        );

        let not_args = create_java_unary_function_with_return(
            &executor,
            "not_bool",
            "boolean",
            "boolean",
            "return !value;",
        );
        let (_, not_bool) = executor
            .udf_registry
            .get("ks", "not_bool", &not_args)
            .expect("compatible Java boolean NOT UDF should be executable");
        assert_eq!(
            not_bool.execute(&[UdfValue::Boolean(false)]).unwrap(),
            UdfValue::Boolean(true)
        );

        let and_args = create_java_binary_function_with_return(
            &executor,
            "bool_and",
            "boolean",
            "boolean",
            "return a && b;",
        );
        let (_, bool_and) = executor
            .udf_registry
            .get("ks", "bool_and", &and_args)
            .expect("compatible Java boolean AND UDF should be executable");
        assert_eq!(
            bool_and
                .execute(&[UdfValue::Boolean(true), UdfValue::Boolean(false)])
                .unwrap(),
            UdfValue::Boolean(false)
        );

        let or_args = create_java_binary_function_with_return(
            &executor,
            "bool_or",
            "boolean",
            "boolean",
            "return a || b;",
        );
        let (_, bool_or) = executor
            .udf_registry
            .get("ks", "bool_or", &or_args)
            .expect("compatible Java boolean OR UDF should be executable");
        assert_eq!(
            bool_or
                .execute(&[UdfValue::Boolean(true), UdfValue::Boolean(false)])
                .unwrap(),
            UdfValue::Boolean(true)
        );

        let increment_args = create_java_unary_function_with_return(
            &executor,
            "increment",
            "int",
            "int",
            "return value + 1;",
        );
        let (_, increment) = executor
            .udf_registry
            .get("ks", "increment", &increment_args)
            .expect("compatible Java numeric literal addition UDF should be executable");
        assert_eq!(
            increment.execute(&[UdfValue::Int(41)]).unwrap(),
            UdfValue::Int(42)
        );

        let decrement_args = create_java_unary_function_with_return(
            &executor,
            "decrement",
            "int",
            "int",
            "return value - 1;",
        );
        let (_, decrement) = executor
            .udf_registry
            .get("ks", "decrement", &decrement_args)
            .expect("compatible Java numeric literal subtraction UDF should be executable");
        assert_eq!(
            decrement.execute(&[UdfValue::Int(42)]).unwrap(),
            UdfValue::Int(41)
        );

        let double_value_args = create_java_unary_function_with_return(
            &executor,
            "double_value",
            "int",
            "int",
            "return value * 2;",
        );
        let (_, double_value) = executor
            .udf_registry
            .get("ks", "double_value", &double_value_args)
            .expect("compatible Java numeric literal multiplication UDF should be executable");
        assert_eq!(
            double_value.execute(&[UdfValue::Int(21)]).unwrap(),
            UdfValue::Int(42)
        );

        let halve_value_args = create_java_unary_function_with_return(
            &executor,
            "halve_value",
            "int",
            "int",
            "return value / 2;",
        );
        let (_, halve_value) = executor
            .udf_registry
            .get("ks", "halve_value", &halve_value_args)
            .expect("compatible Java numeric literal division UDF should be executable");
        assert_eq!(
            halve_value.execute(&[UdfValue::Int(84)]).unwrap(),
            UdfValue::Int(42)
        );

        let remainder_literal_args = create_java_unary_function_with_return(
            &executor,
            "remainder_literal",
            "int",
            "int",
            "return value % 5;",
        );
        let (_, remainder_literal) = executor
            .udf_registry
            .get("ks", "remainder_literal", &remainder_literal_args)
            .expect("compatible Java numeric literal remainder UDF should be executable");
        assert_eq!(
            remainder_literal.execute(&[UdfValue::Int(42)]).unwrap(),
            UdfValue::Int(2)
        );

        let suffix_args = create_java_unary_function_with_return(
            &executor,
            "text_suffix",
            "text",
            "text",
            "return value + \"_x\";",
        );
        let (_, text_suffix) = executor
            .udf_registry
            .get("ks", "text_suffix", &suffix_args)
            .expect("compatible Java text literal suffix UDF should be executable");
        assert_eq!(
            text_suffix
                .execute(&[UdfValue::Text("ada".to_string())])
                .unwrap(),
            UdfValue::Text("ada_x".to_string())
        );

        let prefix_args = create_java_unary_function_with_return(
            &executor,
            "text_prefix",
            "text",
            "text",
            "return \"db_\" + value;",
        );
        let (_, text_prefix) = executor
            .udf_registry
            .get("ks", "text_prefix", &prefix_args)
            .expect("compatible Java text literal prefix UDF should be executable");
        assert_eq!(
            text_prefix
                .execute(&[UdfValue::Text("node".to_string())])
                .unwrap(),
            UdfValue::Text("db_node".to_string())
        );

        let greater_than_literal_args = create_java_unary_function_with_return(
            &executor,
            "greater_than_literal",
            "int",
            "boolean",
            "return value > 10;",
        );
        let (_, greater_than_literal) = executor
            .udf_registry
            .get("ks", "greater_than_literal", &greater_than_literal_args)
            .expect("compatible Java numeric literal comparison UDF should be executable");
        assert_eq!(
            greater_than_literal.execute(&[UdfValue::Int(11)]).unwrap(),
            UdfValue::Boolean(true)
        );
        assert_eq!(
            greater_than_literal.execute(&[UdfValue::Int(10)]).unwrap(),
            UdfValue::Boolean(false)
        );

        let text_equals_literal_args = create_java_unary_function_with_return(
            &executor,
            "text_equals_literal",
            "text",
            "boolean",
            "return value.equals(\"Ada\");",
        );
        let (_, text_equals_literal) = executor
            .udf_registry
            .get("ks", "text_equals_literal", &text_equals_literal_args)
            .expect("compatible Java text literal equals UDF should be executable");
        assert_eq!(
            text_equals_literal
                .execute(&[UdfValue::Text("Ada".to_string())])
                .unwrap(),
            UdfValue::Boolean(true)
        );
        assert_eq!(
            text_equals_literal
                .execute(&[UdfValue::Text("ada".to_string())])
                .unwrap(),
            UdfValue::Boolean(false)
        );

        let text_not_equals_literal_args = create_java_unary_function_with_return(
            &executor,
            "text_not_equals_literal",
            "text",
            "boolean",
            "return !value.equals(\"skip\");",
        );
        let (_, text_not_equals_literal) = executor
            .udf_registry
            .get(
                "ks",
                "text_not_equals_literal",
                &text_not_equals_literal_args,
            )
            .expect("compatible Java text literal not-equals UDF should be executable");
        assert_eq!(
            text_not_equals_literal
                .execute(&[UdfValue::Text("keep".to_string())])
                .unwrap(),
            UdfValue::Boolean(true)
        );

        let boolean_equals_literal_args = create_java_unary_function_with_return(
            &executor,
            "boolean_equals_literal",
            "boolean",
            "boolean",
            "return value == true;",
        );
        let (_, boolean_equals_literal) = executor
            .udf_registry
            .get("ks", "boolean_equals_literal", &boolean_equals_literal_args)
            .expect("compatible Java boolean literal comparison UDF should be executable");
        assert_eq!(
            boolean_equals_literal
                .execute(&[UdfValue::Boolean(true)])
                .unwrap(),
            UdfValue::Boolean(true)
        );

        let contains_literal_args = create_java_unary_function_with_return(
            &executor,
            "contains_literal",
            "text",
            "boolean",
            "return value.contains(\"db\");",
        );
        let (_, contains_literal) = executor
            .udf_registry
            .get("ks", "contains_literal", &contains_literal_args)
            .expect("compatible Java text contains literal UDF should be executable");
        assert_eq!(
            contains_literal
                .execute(&[UdfValue::Text("node-db-1".to_string())])
                .unwrap(),
            UdfValue::Boolean(true)
        );

        let substring_literal_args = create_java_unary_function_with_return(
            &executor,
            "substring_literal",
            "text",
            "text",
            "return value.substring(2);",
        );
        let (_, substring_literal) = executor
            .udf_registry
            .get("ks", "substring_literal", &substring_literal_args)
            .expect("compatible Java text substring literal UDF should be executable");
        assert_eq!(
            substring_literal
                .execute(&[UdfValue::Text("abcdef".to_string())])
                .unwrap(),
            UdfValue::Text("cdef".to_string())
        );

        let substring_range_literal_args = create_java_unary_function_with_return(
            &executor,
            "substring_range_literal",
            "text",
            "text",
            "return value.substring(1, 4);",
        );
        let (_, substring_range_literal) = executor
            .udf_registry
            .get(
                "ks",
                "substring_range_literal",
                &substring_range_literal_args,
            )
            .expect("compatible Java text substring range literal UDF should be executable");
        assert_eq!(
            substring_range_literal
                .execute(&[UdfValue::Text("abcdef".to_string())])
                .unwrap(),
            UdfValue::Text("bcd".to_string())
        );

        let replace_literal_args = create_java_unary_function_with_return(
            &executor,
            "replace_literal",
            "text",
            "text",
            "return value.replace(\"old\", \"new\");",
        );
        let (_, replace_literal) = executor
            .udf_registry
            .get("ks", "replace_literal", &replace_literal_args)
            .expect("compatible Java text replace literal UDF should be executable");
        assert_eq!(
            replace_literal
                .execute(&[UdfValue::Text("old_path".to_string())])
                .unwrap(),
            UdfValue::Text("new_path".to_string())
        );
    }

    #[test]
    fn select_executes_user_defined_scalar_function() {
        let (executor, _temp) = test_executor();
        create_text_upper_function(&executor, "upper_name");
        insert_event(&executor, 1, 1, "alice");

        let plan = QueryPlan::Select(SelectPlan {
            keyspace: "ks".to_string(),
            table: "events".to_string(),
            columns: SelectColumns::Named(vec![Selector::Alias {
                selector: Box::new(Selector::Function(
                    "upper_name".to_string(),
                    vec![Selector::Column("name".to_string())],
                )),
                alias: "upper".to_string(),
            }]),
            distinct: false,
            json: false,
            where_clause: Vec::new(),
            group_by: Vec::new(),
            order_by: Vec::new(),
            limit: None,
            allow_filtering: false,
            restrictions: None,
            ann_clause: None,
            page_size: None,
            paging_state: None,
        });

        let QueryResult::Rows { columns, rows, .. } = executor.execute(&plan, None).unwrap() else {
            panic!("expected rows");
        };
        assert_eq!(columns[0].name, "upper");
        assert_eq!(columns[0].cql_type, CqlType::Varchar);
        assert_eq!(rows, vec![vec![Some(b"ALICE".to_vec())]]);
    }

    #[test]
    fn select_executes_builtin_scalar_function() {
        let (executor, _temp) = test_executor();
        insert_event(&executor, 1, -3, "alice");

        let plan = QueryPlan::Select(SelectPlan {
            keyspace: "ks".to_string(),
            table: "events".to_string(),
            columns: SelectColumns::Named(vec![Selector::Function(
                "abs".to_string(),
                vec![Selector::Column("bucket".to_string())],
            )]),
            distinct: false,
            json: false,
            where_clause: Vec::new(),
            group_by: Vec::new(),
            order_by: Vec::new(),
            limit: None,
            allow_filtering: false,
            restrictions: None,
            ann_clause: None,
            page_size: None,
            paging_state: None,
        });

        let QueryResult::Rows { columns, rows, .. } = executor.execute(&plan, None).unwrap() else {
            panic!("expected rows");
        };
        assert_eq!(columns[0].name, "abs(bucket)");
        assert_eq!(columns[0].cql_type, CqlType::Int);
        assert_eq!(rows, vec![vec![Some(3i32.to_be_bytes().to_vec())]]);
    }

    #[test]
    fn select_executes_to_json_with_argument_cql_type() {
        let (executor, _temp) = test_executor();
        insert_event(&executor, 1, 42, "alice");

        let plan = QueryPlan::Select(SelectPlan {
            keyspace: "ks".to_string(),
            table: "events".to_string(),
            columns: SelectColumns::Named(vec![
                Selector::Function(
                    "to_json".to_string(),
                    vec![Selector::Column("bucket".to_string())],
                ),
                Selector::Function(
                    "tojson".to_string(),
                    vec![Selector::Column("name".to_string())],
                ),
            ]),
            distinct: false,
            json: false,
            where_clause: Vec::new(),
            group_by: Vec::new(),
            order_by: Vec::new(),
            limit: None,
            allow_filtering: false,
            restrictions: None,
            ann_clause: None,
            page_size: None,
            paging_state: None,
        });

        let QueryResult::Rows { columns, rows, .. } = executor.execute(&plan, None).unwrap() else {
            panic!("expected rows");
        };
        assert_eq!(columns[0].name, "to_json(bucket)");
        assert_eq!(columns[0].cql_type, CqlType::Varchar);
        assert_eq!(columns[1].name, "tojson(name)");
        assert_eq!(columns[1].cql_type, CqlType::Varchar);
        assert_eq!(
            rows,
            vec![vec![Some(b"42".to_vec()), Some(b"\"alice\"".to_vec())]]
        );
    }

    #[test]
    fn select_executes_cast_selector_with_target_type() {
        let (executor, _temp) = test_executor();
        insert_event(&executor, 1, 42, "alice");

        let plan = QueryPlan::Select(SelectPlan {
            keyspace: "ks".to_string(),
            table: "events".to_string(),
            columns: SelectColumns::Named(vec![Selector::Alias {
                selector: Box::new(Selector::Cast {
                    selector: Box::new(Selector::Column("bucket".to_string())),
                    target: CqlTypeName::Simple("text".to_string()),
                }),
                alias: "bucket_text".to_string(),
            }]),
            distinct: false,
            json: false,
            where_clause: Vec::new(),
            group_by: Vec::new(),
            order_by: Vec::new(),
            limit: None,
            allow_filtering: false,
            restrictions: None,
            ann_clause: None,
            page_size: None,
            paging_state: None,
        });

        let QueryResult::Rows { columns, rows, .. } = executor.execute(&plan, None).unwrap() else {
            panic!("expected rows");
        };
        assert_eq!(columns[0].name, "bucket_text");
        assert_eq!(columns[0].cql_type, CqlType::Varchar);
        assert_eq!(rows, vec![vec![Some(b"42".to_vec())]]);
    }

    #[test]
    fn insert_executes_builtin_function_call_terms() {
        let (executor, _temp) = test_executor();

        let plan = QueryPlan::Insert(InsertPlan {
            keyspace: "ks".to_string(),
            table: "events".to_string(),
            columns: vec!["id".to_string(), "bucket".to_string(), "name".to_string()],
            values: vec![
                int_term(1),
                Term::FunctionCall("abs".to_string(), vec![int_term(-7)]),
                text_term("computed"),
            ],
            if_not_exists: false,
            json: None,
            json_default: JsonDefault::Null,
            using_timestamp: None,
            using_ttl: None,
        });
        executor.execute(&plan, None).unwrap();

        let rows = select_all_events(&executor);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][1].as_deref(), Some(7i32.to_be_bytes().as_slice()));
    }

    #[test]
    fn insert_executes_from_json_with_column_receiver_type() {
        let (executor, _temp) = test_executor();

        let plan = QueryPlan::Insert(InsertPlan {
            keyspace: "ks".to_string(),
            table: "events".to_string(),
            columns: vec!["id".to_string(), "bucket".to_string(), "name".to_string()],
            values: vec![
                int_term(1),
                Term::FunctionCall("from_json".to_string(), vec![text_term("17")]),
                Term::FunctionCall("fromjson".to_string(), vec![text_term("\"json-name\"")]),
            ],
            if_not_exists: false,
            json: None,
            json_default: JsonDefault::Null,
            using_timestamp: None,
            using_ttl: None,
        });
        executor.execute(&plan, None).unwrap();

        let rows = select_all_events(&executor);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][1].as_deref(), Some(17i32.to_be_bytes().as_slice()));
        assert_eq!(rows[0][2].as_deref(), Some(b"json-name".as_slice()));
    }

    #[test]
    fn insert_executes_udf_function_call_terms() {
        let (executor, _temp) = test_executor();
        create_int_identity_function(&executor, "identity_term");

        let plan = QueryPlan::Insert(InsertPlan {
            keyspace: "ks".to_string(),
            table: "events".to_string(),
            columns: vec!["id".to_string(), "bucket".to_string(), "name".to_string()],
            values: vec![
                int_term(1),
                Term::FunctionCall("identity_term".to_string(), vec![int_term(13)]),
                text_term("udf"),
            ],
            if_not_exists: false,
            json: None,
            json_default: JsonDefault::Null,
            using_timestamp: None,
            using_ttl: None,
        });
        executor.execute(&plan, None).unwrap();

        let rows = select_all_events(&executor);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][1].as_deref(), Some(13i32.to_be_bytes().as_slice()));
    }

    #[test]
    fn insert_executes_nested_function_call_terms() {
        let (executor, _temp) = test_executor();
        create_int_identity_function(&executor, "identity_term");

        let plan = QueryPlan::Insert(InsertPlan {
            keyspace: "ks".to_string(),
            table: "events".to_string(),
            columns: vec!["id".to_string(), "bucket".to_string(), "name".to_string()],
            values: vec![
                int_term(1),
                Term::FunctionCall(
                    "identity_term".to_string(),
                    vec![Term::FunctionCall("abs".to_string(), vec![int_term(-21)])],
                ),
                text_term("nested"),
            ],
            if_not_exists: false,
            json: None,
            json_default: JsonDefault::Null,
            using_timestamp: None,
            using_ttl: None,
        });
        executor.execute(&plan, None).unwrap();

        let rows = select_all_events(&executor);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][1].as_deref(), Some(21i32.to_be_bytes().as_slice()));
    }

    #[test]
    fn collection_terms_execute_nested_function_elements() {
        let registry = FunctionRegistry::with_builtins();
        let udf_registry = UdfRegistry::new();
        let term = Term::CollectionLiteral(vec![Term::FunctionCall(
            "abs".to_string(),
            vec![int_term(-4)],
        )]);

        let bytes = term_to_bytes_with_functions(
            &term,
            &CqlType::List(Box::new(CqlType::Int), false),
            &registry,
            &udf_registry,
            "ks",
        )
        .unwrap()
        .unwrap();

        assert_eq!(BigEndian::read_i32(&bytes[0..4]), 1);
        assert_eq!(BigEndian::read_i32(&bytes[4..8]), 4);
        assert_eq!(BigEndian::read_i32(&bytes[8..12]), 4);
    }

    #[test]
    fn update_collection_assignments_merge_with_existing_values() {
        let (executor, _temp) = test_executor();
        executor
            .execute(
                &QueryPlan::CreateTable(CreateTablePlan {
                    keyspace: "ks".to_string(),
                    name: "collections".to_string(),
                    if_not_exists: false,
                    columns: vec![
                        cassandra_cql::planner::ResolvedColumnDef {
                            name: "id".to_string(),
                            cql_type: CqlType::Int,
                            is_static: false,
                            masked_with: None,
                            constraints: Vec::new(),
                        },
                        cassandra_cql::planner::ResolvedColumnDef {
                            name: "vals".to_string(),
                            cql_type: CqlType::List(Box::new(CqlType::Int), false),
                            is_static: false,
                            masked_with: None,
                            constraints: Vec::new(),
                        },
                        cassandra_cql::planner::ResolvedColumnDef {
                            name: "tags".to_string(),
                            cql_type: CqlType::Set(Box::new(CqlType::Varchar), false),
                            is_static: false,
                            masked_with: None,
                            constraints: Vec::new(),
                        },
                        cassandra_cql::planner::ResolvedColumnDef {
                            name: "attrs".to_string(),
                            cql_type: CqlType::Map(
                                Box::new(CqlType::Varchar),
                                Box::new(CqlType::Int),
                                false,
                            ),
                            is_static: false,
                            masked_with: None,
                            constraints: Vec::new(),
                        },
                    ],
                    partition_key: vec!["id".to_string()],
                    clustering_key: Vec::new(),
                    clustering_order: Vec::new(),
                    options: std::collections::HashMap::new(),
                }),
                None,
            )
            .unwrap();

        executor
            .execute(
                &QueryPlan::Insert(InsertPlan {
                    keyspace: "ks".to_string(),
                    table: "collections".to_string(),
                    columns: vec![
                        "id".to_string(),
                        "vals".to_string(),
                        "tags".to_string(),
                        "attrs".to_string(),
                    ],
                    values: vec![
                        int_term(1),
                        Term::CollectionLiteral(vec![int_term(2)]),
                        Term::CollectionLiteral(vec![text_term("a")]),
                        Term::MapLiteral(vec![(text_term("x"), int_term(1))]),
                    ],
                    if_not_exists: false,
                    json: None,
                    json_default: JsonDefault::Null,
                    using_timestamp: Some(10),
                    using_ttl: None,
                }),
                None,
            )
            .unwrap();

        executor
            .execute(
                &QueryPlan::Update(UpdatePlan {
                    keyspace: "ks".to_string(),
                    table: "collections".to_string(),
                    assignments: vec![
                        Assignment {
                            column: "vals".to_string(),
                            value: Term::CollectionLiteral(vec![int_term(1)]),
                            op: AssignmentOp::CollectionPrepend,
                        },
                        Assignment {
                            column: "tags".to_string(),
                            value: Term::CollectionLiteral(vec![text_term("b")]),
                            op: AssignmentOp::CollectionAppend,
                        },
                        Assignment {
                            column: "attrs".to_string(),
                            value: int_term(2),
                            op: AssignmentOp::MapPut {
                                key: text_term("y"),
                            },
                        },
                    ],
                    where_clause: vec![Relation {
                        column: "id".to_string(),
                        op: RelationOp::Eq,
                        value: int_term(1),
                    }],
                    if_exists: false,
                    using_timestamp: Some(20),
                    using_ttl: None,
                }),
                None,
            )
            .unwrap();

        executor
            .execute(
                &QueryPlan::Update(UpdatePlan {
                    keyspace: "ks".to_string(),
                    table: "collections".to_string(),
                    assignments: vec![
                        Assignment {
                            column: "vals".to_string(),
                            value: Term::CollectionLiteral(vec![int_term(3)]),
                            op: AssignmentOp::CollectionAppend,
                        },
                        Assignment {
                            column: "tags".to_string(),
                            value: Term::CollectionLiteral(vec![text_term("a")]),
                            op: AssignmentOp::CollectionRemove,
                        },
                        Assignment {
                            column: "attrs".to_string(),
                            value: Term::CollectionLiteral(vec![text_term("x")]),
                            op: AssignmentOp::CollectionRemove,
                        },
                    ],
                    where_clause: vec![Relation {
                        column: "id".to_string(),
                        op: RelationOp::Eq,
                        value: int_term(1),
                    }],
                    if_exists: false,
                    using_timestamp: Some(30),
                    using_ttl: None,
                }),
                None,
            )
            .unwrap();

        let partition = executor
            .engine
            .read_partition("ks", "collections", &1i32.to_be_bytes())
            .unwrap();
        let row = partition.rows.get(&Vec::new()).unwrap();
        let cell = |name: &str| {
            row.cells
                .iter()
                .find(|cell| cell.column == name)
                .and_then(|cell| cell.value.as_deref())
                .unwrap()
        };

        assert_eq!(
            CqlValue::deserialize_value(
                &CqlType::List(Box::new(CqlType::Int), false),
                cell("vals")
            )
            .unwrap(),
            CqlValue::List(vec![CqlValue::Int(1), CqlValue::Int(2), CqlValue::Int(3)])
        );
        assert_eq!(
            CqlValue::deserialize_value(
                &CqlType::Set(Box::new(CqlType::Varchar), false),
                cell("tags")
            )
            .unwrap(),
            CqlValue::Set(vec![CqlValue::Varchar("b".to_string())])
        );
        assert_eq!(
            CqlValue::deserialize_value(
                &CqlType::Map(Box::new(CqlType::Varchar), Box::new(CqlType::Int), false),
                cell("attrs")
            )
            .unwrap(),
            CqlValue::Map(vec![(CqlValue::Varchar("y".to_string()), CqlValue::Int(2))])
        );
    }

    #[test]
    fn select_executes_nested_builtin_scalar_function() {
        let (executor, _temp) = test_executor();
        insert_event(&executor, 1, 1, "alice");

        let plan = QueryPlan::Select(SelectPlan {
            keyspace: "ks".to_string(),
            table: "events".to_string(),
            columns: SelectColumns::Named(vec![Selector::Function(
                "length".to_string(),
                vec![Selector::Function(
                    "toJson".to_string(),
                    vec![Selector::Column("name".to_string())],
                )],
            )]),
            distinct: false,
            json: false,
            where_clause: Vec::new(),
            group_by: Vec::new(),
            order_by: Vec::new(),
            limit: None,
            allow_filtering: false,
            restrictions: None,
            ann_clause: None,
            page_size: None,
            paging_state: None,
        });

        let QueryResult::Rows { columns, rows, .. } = executor.execute(&plan, None).unwrap() else {
            panic!("expected rows");
        };
        assert_eq!(columns[0].name, "length(tojson(name))");
        assert_eq!(columns[0].cql_type, CqlType::Int);
        assert_eq!(rows, vec![vec![Some(7i32.to_be_bytes().to_vec())]]);
    }

    #[test]
    fn select_executes_length_and_octet_length_with_java_text_semantics() {
        let (executor, _temp) = test_executor();
        insert_event(&executor, 1, 1, "a😀b");

        let plan = QueryPlan::Select(SelectPlan {
            keyspace: "ks".to_string(),
            table: "events".to_string(),
            columns: SelectColumns::Named(vec![
                Selector::Function(
                    "length".to_string(),
                    vec![Selector::Column("name".to_string())],
                ),
                Selector::Function(
                    "octet_length".to_string(),
                    vec![Selector::Column("name".to_string())],
                ),
            ]),
            distinct: false,
            json: false,
            where_clause: Vec::new(),
            group_by: Vec::new(),
            order_by: Vec::new(),
            limit: None,
            allow_filtering: false,
            restrictions: None,
            ann_clause: None,
            page_size: None,
            paging_state: None,
        });

        let QueryResult::Rows { columns, rows, .. } = executor.execute(&plan, None).unwrap() else {
            panic!("expected rows");
        };
        assert_eq!(columns[0].name, "length(name)");
        assert_eq!(columns[1].name, "octet_length(name)");
        assert_eq!(columns[0].cql_type, CqlType::Int);
        assert_eq!(columns[1].cql_type, CqlType::Int);
        assert_eq!(
            rows,
            vec![vec![
                Some(4i32.to_be_bytes().to_vec()),
                Some(6i32.to_be_bytes().to_vec())
            ]]
        );
    }

    #[test]
    fn select_executes_java_math_log10_builtin_selector() {
        let (executor, _temp) = test_executor();
        insert_event(&executor, 1, 1000, "alice");

        let plan = QueryPlan::Select(SelectPlan {
            keyspace: "ks".to_string(),
            table: "events".to_string(),
            columns: SelectColumns::Named(vec![Selector::Function(
                "log10".to_string(),
                vec![Selector::Column("bucket".to_string())],
            )]),
            distinct: false,
            json: false,
            where_clause: Vec::new(),
            group_by: Vec::new(),
            order_by: Vec::new(),
            limit: None,
            allow_filtering: false,
            restrictions: None,
            ann_clause: None,
            page_size: None,
            paging_state: None,
        });

        let QueryResult::Rows { columns, rows, .. } = executor.execute(&plan, None).unwrap() else {
            panic!("expected rows");
        };
        assert_eq!(columns[0].name, "log10(bucket)");
        assert_eq!(columns[0].cql_type, CqlType::Int);
        assert_eq!(rows, vec![vec![Some(3i32.to_be_bytes().to_vec())]]);
    }

    #[test]
    fn select_rejects_unsupported_function_selector() {
        let (executor, _temp) = test_executor();
        insert_event(&executor, 1, 1, "alice");

        let plan = QueryPlan::Select(SelectPlan {
            keyspace: "ks".to_string(),
            table: "events".to_string(),
            columns: SelectColumns::Named(vec![Selector::Function(
                "missing_function".to_string(),
                vec![Selector::Column("name".to_string())],
            )]),
            distinct: false,
            json: false,
            where_clause: Vec::new(),
            group_by: Vec::new(),
            order_by: Vec::new(),
            limit: None,
            allow_filtering: false,
            restrictions: None,
            ann_clause: None,
            page_size: None,
            paging_state: None,
        });

        let err = executor.execute(&plan, None).unwrap_err();
        assert!(matches!(
            err,
            ExecutorError::InvalidQuery(msg)
                if msg.contains("Unsupported SELECT selector 'missing_function(name)'")
        ));
    }

    #[test]
    fn select_rejects_builtin_function_type_mismatch() {
        let (executor, _temp) = test_executor();
        insert_event(&executor, 1, 1, "alice");

        let plan = QueryPlan::Select(SelectPlan {
            keyspace: "ks".to_string(),
            table: "events".to_string(),
            columns: SelectColumns::Named(vec![Selector::Function(
                "abs".to_string(),
                vec![Selector::Column("name".to_string())],
            )]),
            distinct: false,
            json: false,
            where_clause: Vec::new(),
            group_by: Vec::new(),
            order_by: Vec::new(),
            limit: None,
            allow_filtering: false,
            restrictions: None,
            ann_clause: None,
            page_size: None,
            paging_state: None,
        });

        let err = executor.execute(&plan, None).unwrap_err();
        assert!(matches!(
            err,
            ExecutorError::InvalidQuery(msg)
                if msg.contains("Unsupported SELECT selector 'abs(name)'")
        ));
    }

    #[test]
    fn select_projects_writetime_and_ttl() {
        let (executor, _temp) = test_executor();
        executor
            .execute(
                &QueryPlan::Insert(InsertPlan {
                    keyspace: "ks".to_string(),
                    table: "events".to_string(),
                    columns: vec!["id".to_string(), "bucket".to_string(), "name".to_string()],
                    values: vec![int_term(1), int_term(1), text_term("alice")],
                    if_not_exists: false,
                    json: None,
                    json_default: JsonDefault::Null,
                    using_timestamp: Some(123_456_789),
                    using_ttl: Some(3600),
                }),
                None,
            )
            .unwrap();

        let plan = QueryPlan::Select(SelectPlan {
            keyspace: "ks".to_string(),
            table: "events".to_string(),
            columns: SelectColumns::Named(vec![
                Selector::WritetimeOrTtl("writetime".to_string(), "name".to_string()),
                Selector::WritetimeOrTtl("ttl".to_string(), "name".to_string()),
            ]),
            distinct: false,
            json: false,
            where_clause: Vec::new(),
            group_by: Vec::new(),
            order_by: Vec::new(),
            limit: None,
            allow_filtering: false,
            restrictions: None,
            ann_clause: None,
            page_size: None,
            paging_state: None,
        });

        let QueryResult::Rows { columns, rows, .. } = executor.execute(&plan, None).unwrap() else {
            panic!("expected rows");
        };
        assert_eq!(columns[0].name, "writetime(name)");
        assert_eq!(columns[0].cql_type, CqlType::Bigint);
        assert_eq!(columns[1].name, "ttl(name)");
        assert_eq!(columns[1].cql_type, CqlType::Int);
        assert_eq!(
            rows,
            vec![vec![
                Some(123_456_789i64.to_be_bytes().to_vec()),
                Some(3600i32.to_be_bytes().to_vec()),
            ]]
        );
    }

    #[test]
    fn create_unsafe_java_function_stores_metadata_only() {
        let (executor, _temp) = test_executor();
        let args = vec![("val".to_string(), "text".to_string())];

        let result = executor
            .execute(
                &QueryPlan::CreateFunction(CreateFunctionPlan {
                    keyspace: "ks".to_string(),
                    name: "unsafe_body".to_string(),
                    or_replace: false,
                    if_not_exists: false,
                    args: args.clone(),
                    called_on_null_input: false,
                    return_type: "text".to_string(),
                    language: "java".to_string(),
                    body: "System.exit(1);".to_string(),
                }),
                None,
            )
            .unwrap();

        assert!(matches!(result, QueryResult::SchemaChange { .. }));
        assert!(
            executor
                .udf_registry
                .get("ks", "unsafe_body", &args)
                .is_none()
        );
    }

    #[test]
    fn create_function_respects_duplicate_flags() {
        let (executor, _temp) = test_executor();
        let args = vec![("val".to_string(), "text".to_string())];

        let create_echo = |body: &str, or_replace: bool, if_not_exists: bool| {
            QueryPlan::CreateFunction(CreateFunctionPlan {
                keyspace: "ks".to_string(),
                name: "echo".to_string(),
                or_replace,
                if_not_exists,
                args: args.clone(),
                called_on_null_input: false,
                return_type: "text".to_string(),
                language: "java".to_string(),
                body: body.to_string(),
            })
        };

        executor
            .execute(&create_echo("return val;", false, false), None)
            .unwrap();
        assert!(matches!(
            executor.execute(&create_echo("return val.toUpperCase();", false, false), None),
            Err(ExecutorError::InvalidQuery(msg)) if msg.contains("already exists")
        ));

        let result = executor
            .execute(&create_echo("return val.toUpperCase();", false, true), None)
            .unwrap();
        assert!(matches!(result, QueryResult::Void));
        let (_, echo) = executor.udf_registry.get("ks", "echo", &args).unwrap();
        assert_eq!(
            echo.execute(&[UdfValue::Text("Ada".to_string())]).unwrap(),
            UdfValue::Text("Ada".to_string())
        );

        executor
            .execute(&create_echo("return val.toUpperCase();", true, false), None)
            .unwrap();
        let (_, echo) = executor.udf_registry.get("ks", "echo", &args).unwrap();
        assert_eq!(
            echo.execute(&[UdfValue::Text("Ada".to_string())]).unwrap(),
            UdfValue::Text("ADA".to_string())
        );
    }

    #[test]
    fn function_signatures_canonicalize_cql_type_aliases() {
        let (executor, _temp) = test_executor();
        let varchar_args = vec![("val".to_string(), "varchar".to_string())];
        let text_args = vec![("val".to_string(), "text".to_string())];

        executor
            .execute(
                &QueryPlan::CreateFunction(CreateFunctionPlan {
                    keyspace: "ks".to_string(),
                    name: "alias_echo".to_string(),
                    or_replace: false,
                    if_not_exists: false,
                    args: varchar_args,
                    called_on_null_input: false,
                    return_type: "varchar".to_string(),
                    language: "java".to_string(),
                    body: "return val;".to_string(),
                }),
                None,
            )
            .unwrap();

        assert!(
            executor
                .udf_registry
                .get("ks", "alias_echo", &text_args)
                .is_some()
        );

        let duplicate = QueryPlan::CreateFunction(CreateFunctionPlan {
            keyspace: "ks".to_string(),
            name: "alias_echo".to_string(),
            or_replace: false,
            if_not_exists: false,
            args: text_args.clone(),
            called_on_null_input: false,
            return_type: "text".to_string(),
            language: "java".to_string(),
            body: "return val;".to_string(),
        });
        assert!(matches!(
            executor.execute(&duplicate, None),
            Err(ExecutorError::InvalidQuery(msg)) if msg.contains("already exists")
        ));

        executor
            .execute(
                &QueryPlan::DropFunction(DropFunctionPlan {
                    keyspace: "ks".to_string(),
                    name: "alias_echo".to_string(),
                    if_exists: false,
                    arg_types: vec!["text".to_string()],
                }),
                None,
            )
            .unwrap();
        assert!(
            executor
                .udf_registry
                .get("ks", "alias_echo", &text_args)
                .is_none()
        );
    }

    #[test]
    fn create_or_replace_function_validates_existing_signature_contract() {
        let (executor, _temp) = test_executor();
        let args = vec![("val".to_string(), "text".to_string())];

        executor
            .execute(
                &QueryPlan::CreateFunction(CreateFunctionPlan {
                    keyspace: "ks".to_string(),
                    name: "echo".to_string(),
                    or_replace: false,
                    if_not_exists: false,
                    args: args.clone(),
                    called_on_null_input: false,
                    return_type: "text".to_string(),
                    language: "java".to_string(),
                    body: "return val;".to_string(),
                }),
                None,
            )
            .unwrap();

        let changed_null_directive = QueryPlan::CreateFunction(CreateFunctionPlan {
            keyspace: "ks".to_string(),
            name: "echo".to_string(),
            or_replace: true,
            if_not_exists: false,
            args: args.clone(),
            called_on_null_input: true,
            return_type: "text".to_string(),
            language: "java".to_string(),
            body: "return val;".to_string(),
        });
        assert!(matches!(
            executor.execute(&changed_null_directive, None),
            Err(ExecutorError::InvalidQuery(msg)) if msg.contains("must have CALLED ON NULL INPUT directive")
        ));

        let changed_return_type = QueryPlan::CreateFunction(CreateFunctionPlan {
            keyspace: "ks".to_string(),
            name: "echo".to_string(),
            or_replace: true,
            if_not_exists: false,
            args,
            called_on_null_input: false,
            return_type: "int".to_string(),
            language: "java".to_string(),
            body: "return 0;".to_string(),
        });
        assert!(matches!(
            executor.execute(&changed_return_type, None),
            Err(ExecutorError::InvalidQuery(msg)) if msg.contains("new return type int is not compatible")
        ));
    }

    #[test]
    fn create_or_replace_function_allows_compatible_return_type() {
        let (executor, _temp) = test_executor();
        let args = vec![("val".to_string(), "text".to_string())];

        executor
            .execute(
                &QueryPlan::CreateFunction(CreateFunctionPlan {
                    keyspace: "ks".to_string(),
                    name: "echo_text".to_string(),
                    or_replace: false,
                    if_not_exists: false,
                    args: args.clone(),
                    called_on_null_input: false,
                    return_type: "text".to_string(),
                    language: "java".to_string(),
                    body: "return val;".to_string(),
                }),
                None,
            )
            .unwrap();

        executor
            .execute(
                &QueryPlan::CreateFunction(CreateFunctionPlan {
                    keyspace: "ks".to_string(),
                    name: "echo_text".to_string(),
                    or_replace: true,
                    if_not_exists: false,
                    args: args.clone(),
                    called_on_null_input: false,
                    return_type: "ascii".to_string(),
                    language: "java".to_string(),
                    body: "return val;".to_string(),
                }),
                None,
            )
            .unwrap();
        let (metadata, _) = executor.udf_registry.get("ks", "echo_text", &args).unwrap();
        assert_eq!(metadata.return_type, "ascii");

        let (executor, _temp) = test_executor();
        executor
            .execute(
                &QueryPlan::CreateFunction(CreateFunctionPlan {
                    keyspace: "ks".to_string(),
                    name: "echo_ascii".to_string(),
                    or_replace: false,
                    if_not_exists: false,
                    args: args.clone(),
                    called_on_null_input: false,
                    return_type: "ascii".to_string(),
                    language: "java".to_string(),
                    body: "return val;".to_string(),
                }),
                None,
            )
            .unwrap();
        let incompatible_replace = QueryPlan::CreateFunction(CreateFunctionPlan {
            keyspace: "ks".to_string(),
            name: "echo_ascii".to_string(),
            or_replace: true,
            if_not_exists: false,
            args,
            called_on_null_input: false,
            return_type: "text".to_string(),
            language: "java".to_string(),
            body: "return val;".to_string(),
        });
        assert!(matches!(
            executor.execute(&incompatible_replace, None),
            Err(ExecutorError::InvalidQuery(msg)) if msg.contains("new return type text is not compatible")
        ));
    }

    #[test]
    fn drop_function_unregisters_runtime_executor() {
        let (executor, _temp) = test_executor();
        let args = vec![("val".to_string(), "text".to_string())];

        executor
            .execute(
                &QueryPlan::CreateFunction(CreateFunctionPlan {
                    keyspace: "ks".to_string(),
                    name: "echo".to_string(),
                    or_replace: false,
                    if_not_exists: false,
                    args: args.clone(),
                    called_on_null_input: false,
                    return_type: "text".to_string(),
                    language: "java".to_string(),
                    body: "return val;".to_string(),
                }),
                None,
            )
            .unwrap();
        assert!(executor.udf_registry.get("ks", "echo", &args).is_some());

        executor
            .execute(
                &QueryPlan::DropFunction(DropFunctionPlan {
                    keyspace: "ks".to_string(),
                    name: "echo".to_string(),
                    if_exists: false,
                    arg_types: vec!["text".to_string()],
                }),
                None,
            )
            .unwrap();

        assert!(executor.udf_registry.get("ks", "echo", &args).is_none());
    }

    #[test]
    fn drop_function_respects_if_exists() {
        let (executor, _temp) = test_executor();

        let missing = QueryPlan::DropFunction(DropFunctionPlan {
            keyspace: "ks".to_string(),
            name: "missing".to_string(),
            if_exists: false,
            arg_types: vec!["text".to_string()],
        });
        assert!(matches!(
            executor.execute(&missing, None),
            Err(ExecutorError::InvalidQuery(msg)) if msg.contains("does not exist")
        ));

        let no_op = QueryPlan::DropFunction(DropFunctionPlan {
            keyspace: "ks".to_string(),
            name: "missing".to_string(),
            if_exists: true,
            arg_types: vec!["text".to_string()],
        });
        assert!(matches!(
            executor.execute(&no_op, None).unwrap(),
            QueryResult::Void
        ));
    }

    #[test]
    fn create_aggregate_respects_duplicate_flags() {
        let (executor, _temp) = test_executor();
        let arg_types = vec!["int".to_string()];
        create_int_state_function(&executor, "plus", "int");
        create_int_state_function(&executor, "plus_v2", "int");

        let create_sum = |sfunc: &str, or_replace: bool, if_not_exists: bool| {
            QueryPlan::CreateAggregate(CreateAggregatePlan {
                keyspace: "ks".to_string(),
                name: "sum_int".to_string(),
                or_replace,
                if_not_exists,
                arg_types: arg_types.clone(),
                sfunc: sfunc.to_string(),
                stype: "int".to_string(),
                finalfunc: None,
                initcond: Some("0".to_string()),
            })
        };

        executor
            .execute(&create_sum("plus", false, false), None)
            .unwrap();
        assert!(matches!(
            executor.execute(&create_sum("plus_v2", false, false), None),
            Err(ExecutorError::InvalidQuery(msg)) if msg.contains("already exists")
        ));

        let result = executor
            .execute(&create_sum("plus_v2", false, true), None)
            .unwrap();
        assert!(matches!(result, QueryResult::Void));
        let aggregate = executor
            .uda_registry
            .get("ks", "sum_int", &arg_types)
            .unwrap();
        assert_eq!(aggregate.sfunc_name, "plus");
        assert_eq!(aggregate.return_type, "int");

        executor
            .execute(&create_sum("plus_v2", true, false), None)
            .unwrap();
        let aggregate = executor
            .uda_registry
            .get("ks", "sum_int", &arg_types)
            .unwrap();
        assert_eq!(aggregate.sfunc_name, "plus_v2");
    }

    #[test]
    fn create_aggregate_validates_state_function() {
        let (executor, _temp) = test_executor();
        let arg_types = vec!["int".to_string()];

        let missing = QueryPlan::CreateAggregate(CreateAggregatePlan {
            keyspace: "ks".to_string(),
            name: "sum_int".to_string(),
            or_replace: false,
            if_not_exists: false,
            arg_types: arg_types.clone(),
            sfunc: "missing_plus".to_string(),
            stype: "int".to_string(),
            finalfunc: None,
            initcond: Some("0".to_string()),
        });
        assert!(matches!(
            executor.execute(&missing, None),
            Err(ExecutorError::InvalidQuery(msg)) if msg.contains("State function missing_plus(int, int) doesn't exist")
        ));

        create_int_state_function(&executor, "bad_plus", "bigint");
        let wrong_return = QueryPlan::CreateAggregate(CreateAggregatePlan {
            keyspace: "ks".to_string(),
            name: "sum_int".to_string(),
            or_replace: false,
            if_not_exists: false,
            arg_types,
            sfunc: "bad_plus".to_string(),
            stype: "int".to_string(),
            finalfunc: None,
            initcond: Some("0".to_string()),
        });
        assert!(matches!(
            executor.execute(&wrong_return, None),
            Err(ExecutorError::InvalidQuery(msg)) if msg.contains("return type must be the same")
        ));
    }

    #[test]
    fn create_aggregate_validates_final_function() {
        let (executor, _temp) = test_executor();
        let arg_types = vec!["int".to_string()];
        create_int_state_function(&executor, "plus", "int");

        let aggregate = QueryPlan::CreateAggregate(CreateAggregatePlan {
            keyspace: "ks".to_string(),
            name: "sum_int".to_string(),
            or_replace: false,
            if_not_exists: false,
            arg_types,
            sfunc: "plus".to_string(),
            stype: "int".to_string(),
            finalfunc: Some("missing_final".to_string()),
            initcond: Some("0".to_string()),
        });

        assert!(matches!(
            executor.execute(&aggregate, None),
            Err(ExecutorError::InvalidQuery(msg)) if msg.contains("Final function missing_final(int) doesn't exist")
        ));
    }

    #[test]
    fn create_aggregate_validates_initcond_against_state_type() {
        let (executor, _temp) = test_executor();
        create_int_state_function(&executor, "plus", "int");

        let invalid_initcond = QueryPlan::CreateAggregate(CreateAggregatePlan {
            keyspace: "ks".to_string(),
            name: "sum_int".to_string(),
            or_replace: false,
            if_not_exists: false,
            arg_types: vec!["int".to_string()],
            sfunc: "plus".to_string(),
            stype: "int".to_string(),
            finalfunc: None,
            initcond: Some("'bad'".to_string()),
        });
        assert!(matches!(
            executor.execute(&invalid_initcond, None),
            Err(ExecutorError::InvalidQuery(msg)) if msg.contains("Invalid value for INITCOND of type int")
        ));

        create_tuple_state_function(&executor, "tuple_state");
        let tuple_initcond = QueryPlan::CreateAggregate(CreateAggregatePlan {
            keyspace: "ks".to_string(),
            name: "mean_int".to_string(),
            or_replace: false,
            if_not_exists: false,
            arg_types: vec!["int".to_string()],
            sfunc: "tuple_state".to_string(),
            stype: "tuple<int, bigint>".to_string(),
            finalfunc: None,
            initcond: Some("(0, 0)".to_string()),
        });
        assert!(matches!(
            executor.execute(&tuple_initcond, None).unwrap(),
            QueryResult::SchemaChange { .. }
        ));
    }

    #[test]
    fn create_or_replace_aggregate_validates_return_type_compatibility() {
        let (executor, _temp) = test_executor();
        create_int_state_function(&executor, "plus", "int");
        create_int_final_function(&executor, "finish_text", "text");

        executor
            .execute(
                &QueryPlan::CreateAggregate(CreateAggregatePlan {
                    keyspace: "ks".to_string(),
                    name: "sum_int".to_string(),
                    or_replace: false,
                    if_not_exists: false,
                    arg_types: vec!["int".to_string()],
                    sfunc: "plus".to_string(),
                    stype: "int".to_string(),
                    finalfunc: Some("finish_text".to_string()),
                    initcond: Some("0".to_string()),
                }),
                None,
            )
            .unwrap();
        let aggregate = executor
            .uda_registry
            .get("ks", "sum_int", &["int".to_string()])
            .unwrap();
        assert_eq!(aggregate.return_type, "text");

        let incompatible_replace = QueryPlan::CreateAggregate(CreateAggregatePlan {
            keyspace: "ks".to_string(),
            name: "sum_int".to_string(),
            or_replace: true,
            if_not_exists: false,
            arg_types: vec!["int".to_string()],
            sfunc: "plus".to_string(),
            stype: "int".to_string(),
            finalfunc: None,
            initcond: Some("0".to_string()),
        });
        assert!(matches!(
            executor.execute(&incompatible_replace, None),
            Err(ExecutorError::InvalidQuery(msg)) if msg.contains("new return type int isn't compatible")
        ));
    }

    #[test]
    fn aggregate_signatures_canonicalize_cql_type_aliases() {
        let (executor, _temp) = test_executor();
        executor
            .execute(
                &QueryPlan::CreateFunction(CreateFunctionPlan {
                    keyspace: "ks".to_string(),
                    name: "text_state".to_string(),
                    or_replace: false,
                    if_not_exists: false,
                    args: vec![
                        ("state".to_string(), "varchar".to_string()),
                        ("value".to_string(), "varchar".to_string()),
                    ],
                    called_on_null_input: false,
                    return_type: "varchar".to_string(),
                    language: "java".to_string(),
                    body: "return state;".to_string(),
                }),
                None,
            )
            .unwrap();

        executor
            .execute(
                &QueryPlan::CreateAggregate(CreateAggregatePlan {
                    keyspace: "ks".to_string(),
                    name: "first_text".to_string(),
                    or_replace: false,
                    if_not_exists: false,
                    arg_types: vec!["text".to_string()],
                    sfunc: "text_state".to_string(),
                    stype: "text".to_string(),
                    finalfunc: None,
                    initcond: Some("''".to_string()),
                }),
                None,
            )
            .unwrap();

        assert!(
            executor
                .uda_registry
                .get("ks", "first_text", &["varchar".to_string()])
                .is_some()
        );
    }

    #[test]
    fn functions_and_aggregates_share_signature_namespace() {
        let (executor, _temp) = test_executor();
        create_int_state_function(&executor, "plus", "int");
        executor
            .execute(
                &QueryPlan::CreateFunction(CreateFunctionPlan {
                    keyspace: "ks".to_string(),
                    name: "sum_int".to_string(),
                    or_replace: false,
                    if_not_exists: false,
                    args: vec![("value".to_string(), "int".to_string())],
                    called_on_null_input: false,
                    return_type: "int".to_string(),
                    language: "java".to_string(),
                    body: "return value;".to_string(),
                }),
                None,
            )
            .unwrap();

        let aggregate = QueryPlan::CreateAggregate(CreateAggregatePlan {
            keyspace: "ks".to_string(),
            name: "sum_int".to_string(),
            or_replace: false,
            if_not_exists: true,
            arg_types: vec!["int".to_string()],
            sfunc: "plus".to_string(),
            stype: "int".to_string(),
            finalfunc: None,
            initcond: Some("0".to_string()),
        });
        assert!(matches!(
            executor.execute(&aggregate, None),
            Err(ExecutorError::InvalidQuery(msg)) if msg.contains("cannot replace a function")
        ));

        let (executor, _temp) = test_executor();
        create_int_state_function(&executor, "plus", "int");
        executor
            .execute(
                &QueryPlan::CreateAggregate(CreateAggregatePlan {
                    keyspace: "ks".to_string(),
                    name: "sum_int".to_string(),
                    or_replace: false,
                    if_not_exists: false,
                    arg_types: vec!["int".to_string()],
                    sfunc: "plus".to_string(),
                    stype: "int".to_string(),
                    finalfunc: None,
                    initcond: Some("0".to_string()),
                }),
                None,
            )
            .unwrap();

        let function = QueryPlan::CreateFunction(CreateFunctionPlan {
            keyspace: "ks".to_string(),
            name: "sum_int".to_string(),
            or_replace: false,
            if_not_exists: true,
            args: vec![("value".to_string(), "int".to_string())],
            called_on_null_input: false,
            return_type: "int".to_string(),
            language: "java".to_string(),
            body: "return value;".to_string(),
        });
        assert!(matches!(
            executor.execute(&function, None),
            Err(ExecutorError::InvalidQuery(msg)) if msg.contains("cannot replace an aggregate")
        ));
    }

    #[test]
    fn drop_aggregate_respects_if_exists() {
        let (executor, _temp) = test_executor();

        let missing = QueryPlan::DropAggregate(DropAggregatePlan {
            keyspace: "ks".to_string(),
            name: "missing".to_string(),
            if_exists: false,
            arg_types: vec!["int".to_string()],
        });
        assert!(matches!(
            executor.execute(&missing, None),
            Err(ExecutorError::InvalidQuery(msg)) if msg.contains("does not exist")
        ));

        let no_op = QueryPlan::DropAggregate(DropAggregatePlan {
            keyspace: "ks".to_string(),
            name: "missing".to_string(),
            if_exists: true,
            arg_types: vec!["int".to_string()],
        });
        assert!(matches!(
            executor.execute(&no_op, None).unwrap(),
            QueryResult::Void
        ));
    }

    #[test]
    fn drop_aggregate_unregisters_runtime_metadata() {
        let (executor, _temp) = test_executor();
        let arg_types = vec!["int".to_string()];
        create_int_state_function(&executor, "plus", "int");

        executor
            .execute(
                &QueryPlan::CreateAggregate(CreateAggregatePlan {
                    keyspace: "ks".to_string(),
                    name: "sum_int".to_string(),
                    or_replace: false,
                    if_not_exists: false,
                    arg_types: arg_types.clone(),
                    sfunc: "plus".to_string(),
                    stype: "int".to_string(),
                    finalfunc: None,
                    initcond: Some("0".to_string()),
                }),
                None,
            )
            .unwrap();
        assert!(
            executor
                .uda_registry
                .get("ks", "sum_int", &arg_types)
                .is_some()
        );

        executor
            .execute(
                &QueryPlan::DropAggregate(DropAggregatePlan {
                    keyspace: "ks".to_string(),
                    name: "sum_int".to_string(),
                    if_exists: false,
                    arg_types: arg_types.clone(),
                }),
                None,
            )
            .unwrap();

        assert!(
            executor
                .uda_registry
                .get("ks", "sum_int", &arg_types)
                .is_none()
        );
    }

    #[test]
    fn create_index_registers_augmented_storage_options() {
        let (executor, _temp) = test_executor();

        let plan = QueryPlan::CreateIndex(CreateIndexPlan {
            keyspace: "ks".to_string(),
            table: "events".to_string(),
            index_name: "events_name_sai_idx".to_string(),
            column: "name".to_string(),
            kind: "custom".to_string(),
            custom_class: Some("org.apache.cassandra.index.sai.StorageAttachedIndex".to_string()),
            options: std::collections::HashMap::from([(
                "case_sensitive".to_string(),
                "false".to_string(),
            )]),
            if_not_exists: false,
        });

        executor.execute(&plan, None).unwrap();

        let mgrs = executor.engine.index_managers.read();
        let definition = mgrs
            .get("ks.events")
            .and_then(|mgr| mgr.get_definition("events_name_sai_idx"))
            .expect("storage index definition should be registered");

        assert_eq!(
            definition.index_type,
            cassandra_storage::index::IndexType::Sai
        );
        assert_eq!(
            definition.options.get("target").map(String::as_str),
            Some("name")
        );
        assert_eq!(
            definition.options.get("class_name").map(String::as_str),
            Some("org.apache.cassandra.index.sai.StorageAttachedIndex")
        );
        assert_eq!(
            definition.options.get("case_sensitive").map(String::as_str),
            Some("false")
        );
    }

    #[cfg(not(feature = "sasi"))]
    #[test]
    fn create_sasi_index_requires_sasi_feature() {
        let (executor, _temp) = test_executor();

        let plan = QueryPlan::CreateIndex(CreateIndexPlan {
            keyspace: "ks".to_string(),
            table: "events".to_string(),
            index_name: "events_name_sasi_idx".to_string(),
            column: "name".to_string(),
            kind: "custom".to_string(),
            custom_class: Some("org.apache.cassandra.index.sasi.SASIIndex".to_string()),
            options: std::collections::HashMap::new(),
            if_not_exists: false,
        });

        assert!(matches!(
            executor.execute(&plan, None),
            Err(ExecutorError::InvalidQuery(msg))
                if msg.contains("SASI index support is not enabled")
        ));
    }

    #[test]
    fn create_index_respects_if_not_exists_and_requires_table() {
        let (executor, _temp) = test_executor();

        let duplicate = |if_not_exists| {
            QueryPlan::CreateIndex(CreateIndexPlan {
                keyspace: "ks".to_string(),
                table: "events".to_string(),
                index_name: "events_name_idx".to_string(),
                column: "name".to_string(),
                kind: "keys".to_string(),
                custom_class: None,
                options: Default::default(),
                if_not_exists,
            })
        };

        executor.execute(&duplicate(false), None).unwrap();
        assert!(matches!(
            executor.execute(&duplicate(false), None),
            Err(ExecutorError::InvalidQuery(msg)) if msg.contains("already exists")
        ));
        assert!(matches!(
            executor.execute(&duplicate(true), None).unwrap(),
            QueryResult::Void
        ));

        let missing_table = QueryPlan::CreateIndex(CreateIndexPlan {
            keyspace: "ks".to_string(),
            table: "missing".to_string(),
            index_name: "missing_name_idx".to_string(),
            column: "name".to_string(),
            kind: "keys".to_string(),
            custom_class: None,
            options: Default::default(),
            if_not_exists: false,
        });
        assert!(matches!(
            executor.execute(&missing_table, None),
            Err(ExecutorError::TableNotFound(ks, table)) if ks == "ks" && table == "missing"
        ));
    }

    #[test]
    fn create_sai_index_rejects_invalid_analyzer_options() {
        let (executor, _temp) = test_executor();

        let plan = QueryPlan::CreateIndex(CreateIndexPlan {
            keyspace: "ks".to_string(),
            table: "events".to_string(),
            index_name: "events_bad_sai_idx".to_string(),
            column: "name".to_string(),
            kind: "custom".to_string(),
            custom_class: Some("org.apache.cassandra.index.sai.StorageAttachedIndex".to_string()),
            options: std::collections::HashMap::from([(
                "case_sensitive".to_string(),
                "not-a-bool".to_string(),
            )]),
            if_not_exists: false,
        });

        assert!(matches!(
            executor.execute(&plan, None),
            Err(ExecutorError::InvalidQuery(msg))
                if msg.contains("Invalid SAI analyzer options")
                    && msg.contains("case_sensitive")
        ));
    }

    fn create_constrained_table(executor: &QueryExecutor) {
        let plan = QueryPlan::CreateTable(CreateTablePlan {
            keyspace: "ks".to_string(),
            name: "constrained".to_string(),
            if_not_exists: false,
            columns: vec![
                cassandra_cql::planner::ResolvedColumnDef {
                    name: "id".to_string(),
                    cql_type: CqlType::Int,
                    is_static: false,
                    masked_with: None,
                    constraints: Vec::new(),
                },
                cassandra_cql::planner::ResolvedColumnDef {
                    name: "v".to_string(),
                    cql_type: CqlType::Int,
                    is_static: false,
                    masked_with: None,
                    constraints: vec![
                        ColumnConstraintMetadata::Scalar {
                            column: "v".to_string(),
                            op: SchemaConstraintRelationOp::Gte,
                            term: "0".to_string(),
                        },
                        ColumnConstraintMetadata::Scalar {
                            column: "v".to_string(),
                            op: SchemaConstraintRelationOp::Lt,
                            term: "100".to_string(),
                        },
                    ],
                },
                cassandra_cql::planner::ResolvedColumnDef {
                    name: "name".to_string(),
                    cql_type: CqlType::Varchar,
                    is_static: false,
                    masked_with: None,
                    constraints: vec![ColumnConstraintMetadata::Function {
                        name: "length".to_string(),
                        args: Vec::new(),
                        op: SchemaConstraintRelationOp::Lte,
                        term: "5".to_string(),
                    }],
                },
                cassandra_cql::planner::ResolvedColumnDef {
                    name: "payload".to_string(),
                    cql_type: CqlType::Blob,
                    is_static: false,
                    masked_with: None,
                    constraints: vec![ColumnConstraintMetadata::Function {
                        name: "octet_length".to_string(),
                        args: Vec::new(),
                        op: SchemaConstraintRelationOp::NotEq,
                        term: "0".to_string(),
                    }],
                },
                cassandra_cql::planner::ResolvedColumnDef {
                    name: "required".to_string(),
                    cql_type: CqlType::Int,
                    is_static: false,
                    masked_with: None,
                    constraints: vec![ColumnConstraintMetadata::NotNull],
                },
                cassandra_cql::planner::ResolvedColumnDef {
                    name: "code".to_string(),
                    cql_type: CqlType::Varchar,
                    is_static: false,
                    masked_with: None,
                    constraints: vec![ColumnConstraintMetadata::Function {
                        name: "regexp".to_string(),
                        args: Vec::new(),
                        op: SchemaConstraintRelationOp::Eq,
                        term: "a..b".to_string(),
                    }],
                },
                cassandra_cql::planner::ResolvedColumnDef {
                    name: "json_body".to_string(),
                    cql_type: CqlType::Varchar,
                    is_static: false,
                    masked_with: None,
                    constraints: vec![ColumnConstraintMetadata::UnaryFunction {
                        name: "json".to_string(),
                        args: Vec::new(),
                    }],
                },
            ],
            partition_key: vec!["id".to_string()],
            clustering_key: Vec::new(),
            clustering_order: Vec::new(),
            options: Default::default(),
        });
        executor.execute(&plan, None).unwrap();
    }

    fn constrained_insert(values: Vec<(&str, Term)>) -> QueryPlan {
        QueryPlan::Insert(InsertPlan {
            keyspace: "ks".to_string(),
            table: "constrained".to_string(),
            columns: values.iter().map(|(name, _)| name.to_string()).collect(),
            values: values.into_iter().map(|(_, value)| value).collect(),
            if_not_exists: false,
            json: None,
            json_default: JsonDefault::Null,
            using_timestamp: None,
            using_ttl: None,
        })
    }

    #[test]
    fn insert_enforces_column_check_constraints() {
        let (executor, _temp) = test_executor();
        create_constrained_table(&executor);

        let valid = constrained_insert(vec![
            ("id", int_term(1)),
            ("v", int_term(42)),
            ("name", text_term("short")),
            ("payload", Term::Literal(Literal::Blob(vec![1]))),
            ("required", int_term(7)),
            ("code", text_term("acdb")),
            ("json_body", text_term(r#"{"ok":true}"#)),
        ]);
        executor.execute(&valid, None).unwrap();

        let too_large = constrained_insert(vec![
            ("id", int_term(2)),
            ("v", int_term(100)),
            ("name", text_term("short")),
            ("payload", Term::Literal(Literal::Blob(vec![1]))),
            ("required", int_term(7)),
        ]);
        assert!(matches!(
            executor.execute(&too_large, None),
            Err(ExecutorError::InvalidQuery(msg)) if msg.contains("violates CHECK")
        ));

        let long_name = constrained_insert(vec![
            ("id", int_term(3)),
            ("v", int_term(10)),
            ("name", text_term("toolong")),
            ("payload", Term::Literal(Literal::Blob(vec![1]))),
            ("required", int_term(7)),
        ]);
        assert!(matches!(
            executor.execute(&long_name, None),
            Err(ExecutorError::InvalidQuery(msg)) if msg.contains("violates CHECK")
        ));

        let bad_code = constrained_insert(vec![
            ("id", int_term(5)),
            ("v", int_term(10)),
            ("name", text_term("ok")),
            ("payload", Term::Literal(Literal::Blob(vec![1]))),
            ("required", int_term(7)),
            ("code", text_term("aaaaaaa")),
        ]);
        assert!(matches!(
            executor.execute(&bad_code, None),
            Err(ExecutorError::InvalidQuery(msg)) if msg.contains("violates CHECK")
        ));

        let bad_json = constrained_insert(vec![
            ("id", int_term(6)),
            ("v", int_term(10)),
            ("name", text_term("ok")),
            ("payload", Term::Literal(Literal::Blob(vec![1]))),
            ("required", int_term(7)),
            ("code", text_term("acdb")),
            ("json_body", text_term("not-json")),
        ]);
        assert!(matches!(
            executor.execute(&bad_json, None),
            Err(ExecutorError::InvalidQuery(msg))
                if msg.contains("violates CHECK json")
        ));

        let missing_required = constrained_insert(vec![
            ("id", int_term(4)),
            ("v", int_term(10)),
            ("name", text_term("ok")),
            ("payload", Term::Literal(Literal::Blob(vec![1]))),
        ]);
        assert!(matches!(
            executor.execute(&missing_required, None),
            Err(ExecutorError::InvalidQuery(msg)) if msg.contains("NOT NULL")
        ));
    }

    #[test]
    fn update_enforces_column_check_constraints() {
        let (executor, _temp) = test_executor();
        create_constrained_table(&executor);

        let valid = constrained_insert(vec![
            ("id", int_term(1)),
            ("v", int_term(42)),
            ("name", text_term("short")),
            ("payload", Term::Literal(Literal::Blob(vec![1]))),
            ("required", int_term(7)),
            ("code", text_term("acdb")),
            ("json_body", text_term(r#"{"ok":true}"#)),
        ]);
        executor.execute(&valid, None).unwrap();

        let invalid_update = QueryPlan::Update(UpdatePlan {
            keyspace: "ks".to_string(),
            table: "constrained".to_string(),
            assignments: vec![cassandra_cql::ast::Assignment {
                column: "v".to_string(),
                value: int_term(-1),
                op: cassandra_cql::ast::AssignmentOp::Set,
            }],
            where_clause: vec![Relation {
                column: "id".to_string(),
                op: RelationOp::Eq,
                value: int_term(1),
            }],
            if_exists: false,
            using_timestamp: None,
            using_ttl: None,
        });
        assert!(matches!(
            executor.execute(&invalid_update, None),
            Err(ExecutorError::InvalidQuery(msg)) if msg.contains("violates CHECK")
        ));
    }

    #[test]
    fn create_role_if_not_exists_is_no_op() {
        let (executor, _temp) = test_executor();
        executor
            .execute(&create_role_plan("analyst"), None)
            .unwrap();

        let duplicate = QueryPlan::CreateRole(CreateRolePlan {
            name: "analyst".to_string(),
            if_not_exists: false,
            password: None,
            hashed_password: None,
            is_superuser: true,
            can_login: false,
            datacenter_access: None,
            cidr_access: None,
            options: Default::default(),
        });
        assert!(matches!(
            executor.execute(&duplicate, None),
            Err(ExecutorError::InvalidQuery(msg)) if msg.contains("already exists")
        ));

        let no_op = QueryPlan::CreateRole(CreateRolePlan {
            name: "analyst".to_string(),
            if_not_exists: true,
            password: None,
            hashed_password: None,
            is_superuser: true,
            can_login: false,
            datacenter_access: None,
            cidr_access: None,
            options: Default::default(),
        });
        assert!(matches!(
            executor.execute(&no_op, None).unwrap(),
            QueryResult::Void
        ));
        let role = executor.role_manager.get_role("analyst").unwrap();
        assert!(!role.is_superuser);
        assert!(role.can_login);
    }

    #[test]
    fn alter_role_if_exists_is_no_op_for_missing_role() {
        let (executor, _temp) = test_executor();
        let alter = QueryPlan::AlterRole(AlterRolePlan {
            name: "missing_role".to_string(),
            if_exists: true,
            password: Some("secret".to_string()),
            hashed_password: None,
            superuser: None,
            login: None,
            datacenter_access: None,
            cidr_access: None,
            options: Default::default(),
        });

        assert!(matches!(
            executor.execute(&alter, None).unwrap(),
            QueryResult::Void
        ));
        assert!(!executor.role_manager.role_exists("missing_role"));
    }

    #[test]
    fn create_and_alter_role_apply_network_access_options() {
        let (executor, _temp) = test_executor();
        let create = QueryPlan::CreateRole(CreateRolePlan {
            name: "analyst".to_string(),
            if_not_exists: false,
            password: None,
            hashed_password: None,
            is_superuser: false,
            can_login: true,
            datacenter_access: Some(RoleAccess::Restricted(vec!["dc1".to_string()])),
            cidr_access: Some(RoleAccess::All),
            options: Default::default(),
        });
        executor.execute(&create, None).unwrap();
        let role = executor.role_manager.get_role("analyst").unwrap();
        let network = role.network_permissions.unwrap();
        assert!(!network.all_datacenters);
        assert_eq!(network.allowed_datacenters, vec!["dc1"]);
        assert!(network.all_cidrs);

        let alter = QueryPlan::AlterRole(AlterRolePlan {
            name: "analyst".to_string(),
            if_exists: false,
            password: None,
            hashed_password: None,
            superuser: None,
            login: None,
            datacenter_access: Some(RoleAccess::All),
            cidr_access: Some(RoleAccess::Restricted(vec![
                "region1".to_string(),
                "region2".to_string(),
            ])),
            options: Default::default(),
        });
        executor.execute(&alter, None).unwrap();
        let role = executor.role_manager.get_role("analyst").unwrap();
        let network = role.network_permissions.unwrap();
        assert!(network.all_datacenters);
        assert!(!network.all_cidrs);
        assert_eq!(network.allowed_cidrs, vec!["region1", "region2"]);
    }

    #[test]
    fn create_and_alter_role_store_hashed_password_verbatim() {
        let (executor, _temp) = test_executor();
        let initial_hash = "$2b$12$prehashed-create";
        let create = QueryPlan::CreateRole(CreateRolePlan {
            name: "hashed_user".to_string(),
            if_not_exists: false,
            password: None,
            hashed_password: Some(initial_hash.to_string()),
            is_superuser: false,
            can_login: true,
            datacenter_access: None,
            cidr_access: None,
            options: Default::default(),
        });
        executor.execute(&create, None).unwrap();
        let role = executor.role_manager.get_role("hashed_user").unwrap();
        assert_eq!(role.hashed_password.as_deref(), Some(initial_hash));

        let updated_hash = "$2b$12$prehashed-alter";
        let alter = QueryPlan::AlterRole(AlterRolePlan {
            name: "hashed_user".to_string(),
            if_exists: false,
            password: None,
            hashed_password: Some(updated_hash.to_string()),
            superuser: None,
            login: None,
            datacenter_access: None,
            cidr_access: None,
            options: Default::default(),
        });
        executor.execute(&alter, None).unwrap();
        let role = executor.role_manager.get_role("hashed_user").unwrap();
        assert_eq!(role.hashed_password.as_deref(), Some(updated_hash));
    }

    #[test]
    fn list_roles_returns_role_rows() {
        let (executor, _temp) = test_executor();
        executor
            .execute(&create_role_plan("analyst"), None)
            .unwrap();

        let plan = QueryPlan::ListRoles(ListRolesPlan {
            of_role: None,
            no_recursive: false,
        });

        let QueryResult::Rows { columns, rows, .. } = executor.execute(&plan, None).unwrap() else {
            panic!("expected LIST ROLES rows");
        };
        assert_eq!(columns[0].name, "role");
        let role_names: Vec<String> = rows
            .iter()
            .map(|row| String::from_utf8(row[0].clone().unwrap()).unwrap())
            .collect();
        assert!(role_names.contains(&"analyst".to_string()));
        assert!(role_names.contains(&"cassandra".to_string()));
    }

    #[test]
    fn list_roles_of_role_respects_recursive_flag() {
        let (executor, _temp) = test_executor();
        executor.execute(&create_role_plan("top"), None).unwrap();
        executor.execute(&create_role_plan("mid"), None).unwrap();
        executor.execute(&create_role_plan("child"), None).unwrap();
        executor.role_manager.grant_role("top", "mid").unwrap();
        executor.role_manager.grant_role("mid", "child").unwrap();

        let recursive = QueryPlan::ListRoles(ListRolesPlan {
            of_role: Some("child".to_string()),
            no_recursive: false,
        });
        let QueryResult::Rows { rows, .. } = executor.execute(&recursive, None).unwrap() else {
            panic!("expected recursive LIST ROLES rows");
        };
        let recursive_names: Vec<String> = rows
            .iter()
            .map(|row| String::from_utf8(row[0].clone().unwrap()).unwrap())
            .collect();
        assert!(recursive_names.contains(&"child".to_string()));
        assert!(recursive_names.contains(&"mid".to_string()));
        assert!(recursive_names.contains(&"top".to_string()));

        let no_recursive = QueryPlan::ListRoles(ListRolesPlan {
            of_role: Some("child".to_string()),
            no_recursive: true,
        });
        let QueryResult::Rows { rows, .. } = executor.execute(&no_recursive, None).unwrap() else {
            panic!("expected non-recursive LIST ROLES rows");
        };
        let direct_names: Vec<String> = rows
            .iter()
            .map(|row| String::from_utf8(row[0].clone().unwrap()).unwrap())
            .collect();
        assert!(direct_names.contains(&"child".to_string()));
        assert!(direct_names.contains(&"mid".to_string()));
        assert!(!direct_names.contains(&"top".to_string()));
    }

    #[test]
    fn list_permissions_returns_granted_permission_rows() {
        let (executor, _temp) = test_executor_with_authorizer(true);
        executor
            .execute(&create_role_plan("analyst"), None)
            .unwrap();
        let resource = cassandra_cql::ast::Resource::Table {
            keyspace: Some("ks".to_string()),
            table: "events".to_string(),
        };
        executor
            .execute(
                &QueryPlan::Grant(GrantPlan {
                    permissions: vec!["SELECT".to_string()],
                    resource: resource.clone(),
                    role: "analyst".to_string(),
                }),
                None,
            )
            .unwrap();

        let QueryResult::Rows { columns, rows, .. } = executor
            .execute(
                &QueryPlan::ListPermissions(ListPermissionsPlan {
                    permissions: vec!["SELECT".to_string()],
                    resource: Some(resource),
                    of_role: Some("analyst".to_string()),
                }),
                None,
            )
            .unwrap()
        else {
            panic!("expected LIST PERMISSIONS rows");
        };
        assert_eq!(
            columns
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>(),
            vec!["role", "resource", "permission"]
        );
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0].as_deref(), Some(b"analyst".as_slice()));
        assert_eq!(rows[0][1].as_deref(), Some(b"TABLE ks.events".as_slice()));
        assert_eq!(rows[0][2].as_deref(), Some(b"SELECT".as_slice()));
    }

    #[test]
    fn describe_table_returns_schema_ddl() {
        let (executor, _temp) = test_executor();

        let QueryResult::Rows { rows, .. } = executor
            .execute(
                &QueryPlan::Describe(DescribePlan {
                    target: DescribeTarget::Table(Some("ks".to_string()), "events".to_string()),
                }),
                None,
            )
            .unwrap()
        else {
            panic!("expected DESCRIBE rows");
        };
        let ddl = String::from_utf8(rows[0][0].clone().unwrap()).unwrap();
        assert!(ddl.contains("CREATE TABLE ks.events"));
        assert!(ddl.contains("id int"));
        assert!(ddl.contains("bucket int"));
        assert!(ddl.contains("name text") || ddl.contains("name varchar"));
        assert!(ddl.contains("PRIMARY KEY (id, bucket)"));
    }

    #[test]
    fn describe_schema_objects_return_metadata_ddl() {
        let (executor, _temp) = test_executor();
        executor
            .execute(
                &QueryPlan::CreateType(CreateTypePlan {
                    keyspace: "ks".to_string(),
                    name: "address".to_string(),
                    if_not_exists: false,
                    fields: vec![
                        ("street".to_string(), "text".to_string()),
                        ("zip".to_string(), "int".to_string()),
                    ],
                }),
                None,
            )
            .unwrap();
        create_int_identity_function(&executor, "identity_for_describe");
        create_int_state_function(&executor, "state_for_describe", "int");
        executor
            .execute(
                &QueryPlan::CreateAggregate(CreateAggregatePlan {
                    keyspace: "ks".to_string(),
                    name: "sum_for_describe".to_string(),
                    or_replace: false,
                    if_not_exists: false,
                    arg_types: vec!["int".to_string()],
                    sfunc: "state_for_describe".to_string(),
                    stype: "int".to_string(),
                    finalfunc: None,
                    initcond: Some("0".to_string()),
                }),
                None,
            )
            .unwrap();

        let describe_text = |target| {
            let QueryResult::Rows { rows, .. } = executor
                .execute(&QueryPlan::Describe(DescribePlan { target }), None)
                .unwrap()
            else {
                panic!("expected DESCRIBE rows");
            };
            String::from_utf8(rows[0][0].clone().unwrap()).unwrap()
        };

        let type_ddl = describe_text(DescribeTarget::Type(
            Some("ks".to_string()),
            "address".to_string(),
        ));
        assert!(type_ddl.contains("CREATE TYPE ks.address"));
        assert!(type_ddl.contains("street text"));

        let function_ddl = describe_text(DescribeTarget::Function(
            Some("ks".to_string()),
            "identity_for_describe".to_string(),
        ));
        assert!(function_ddl.contains("CREATE FUNCTION ks.identity_for_describe"));
        assert!(function_ddl.contains("RETURNS int"));

        let aggregate_ddl = describe_text(DescribeTarget::Aggregate(
            Some("ks".to_string()),
            "sum_for_describe".to_string(),
        ));
        assert!(aggregate_ddl.contains("CREATE AGGREGATE ks.sum_for_describe(int)"));
        assert!(aggregate_ddl.contains("SFUNC state_for_describe"));
        assert!(aggregate_ddl.contains("INITCOND 0"));
    }

    #[test]
    fn alter_type_updates_udt_metadata() {
        let (executor, _temp) = test_executor();
        executor
            .execute(
                &QueryPlan::CreateType(CreateTypePlan {
                    keyspace: "ks".to_string(),
                    name: "address".to_string(),
                    if_not_exists: false,
                    fields: vec![
                        ("street".to_string(), "text".to_string()),
                        ("zip".to_string(), "int".to_string()),
                    ],
                }),
                None,
            )
            .unwrap();

        executor
            .execute(
                &QueryPlan::AlterType(AlterTypePlan {
                    keyspace: "ks".to_string(),
                    name: "address".to_string(),
                    operation: AlterTypeOp::AddField(
                        "country".to_string(),
                        CqlTypeName::Simple("text".to_string()),
                    ),
                }),
                None,
            )
            .unwrap();
        executor
            .execute(
                &QueryPlan::AlterType(AlterTypePlan {
                    keyspace: "ks".to_string(),
                    name: "address".to_string(),
                    operation: AlterTypeOp::RenameField(
                        "zip".to_string(),
                        "postal_code".to_string(),
                    ),
                }),
                None,
            )
            .unwrap();
        let result = executor
            .execute(
                &QueryPlan::AlterType(AlterTypePlan {
                    keyspace: "ks".to_string(),
                    name: "address".to_string(),
                    operation: AlterTypeOp::AlterFieldType(
                        "postal_code".to_string(),
                        CqlTypeName::Simple("bigint".to_string()),
                    ),
                }),
                None,
            )
            .unwrap();
        assert!(matches!(
            result,
            QueryResult::SchemaChange {
                ref change_type,
                ref target,
                ..
            } if change_type == "UPDATED" && target == "TYPE"
        ));

        let QueryResult::Rows { rows, .. } = executor
            .execute(
                &QueryPlan::Describe(DescribePlan {
                    target: DescribeTarget::Type(Some("ks".to_string()), "address".to_string()),
                }),
                None,
            )
            .unwrap()
        else {
            panic!("expected DESCRIBE rows");
        };
        let ddl = String::from_utf8(rows[0][0].clone().unwrap()).unwrap();
        assert!(ddl.contains("street text"));
        assert!(ddl.contains("country text"));
        assert!(ddl.contains("postal_code bigint"));
        assert!(!ddl.contains("zip int"));
    }

    fn select_all_events(executor: &QueryExecutor) -> Vec<Vec<Option<Vec<u8>>>> {
        let plan = QueryPlan::Select(SelectPlan {
            keyspace: "ks".to_string(),
            table: "events".to_string(),
            columns: SelectColumns::Named(vec![
                Selector::Column("id".to_string()),
                Selector::Column("bucket".to_string()),
                Selector::Column("name".to_string()),
            ]),
            distinct: false,
            json: false,
            where_clause: Vec::new(),
            group_by: Vec::new(),
            order_by: Vec::new(),
            limit: None,
            allow_filtering: false,
            restrictions: None,
            ann_clause: None,
            page_size: None,
            paging_state: None,
        });

        let QueryResult::Rows { rows, .. } = executor.execute(&plan, None).unwrap() else {
            panic!("expected rows");
        };
        rows
    }

    #[test]
    fn execute_batch_uses_configured_mutation_sink() {
        let (executor, _temp) = test_executor();
        let observed = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let observed_in_sink = Arc::clone(&observed);
        let executor = executor.with_batch_mutation_sink(Arc::new(move |batch_type, mutations| {
            assert!(matches!(batch_type, cassandra_cql::ast::BatchType::Logged));
            observed_in_sink.store(mutations.len(), Ordering::Relaxed);
            Ok(())
        }));

        let plan = QueryPlan::Batch(BatchPlan {
            batch_type: cassandra_cql::ast::BatchType::Logged,
            plans: vec![QueryPlan::Insert(InsertPlan {
                keyspace: "ks".to_string(),
                table: "events".to_string(),
                columns: vec!["id".to_string(), "bucket".to_string(), "name".to_string()],
                values: vec![int_term(10), int_term(1), text_term("batched")],
                if_not_exists: false,
                json: None,
                json_default: JsonDefault::Null,
                using_timestamp: None,
                using_ttl: None,
            })],
        });

        assert!(matches!(
            executor.execute(&plan, None),
            Ok(QueryResult::Void)
        ));
        assert_eq!(observed.load(Ordering::Relaxed), 1);
        assert!(select_all_events(&executor).is_empty());
    }

    #[test]
    fn execute_batch_includes_loaded_trigger_mutations() {
        let (executor, _temp) = test_executor();
        let registry = Arc::new(TriggerRegistry::new());
        registry
            .register_with_implementation(
                cassandra_cql::triggers::TriggerMetadata {
                    name: "audit".to_string(),
                    keyspace: "ks".to_string(),
                    table: "events".to_string(),
                    trigger_class: "audit.Trigger".to_string(),
                },
                Arc::new(AuditTrigger),
            )
            .unwrap();
        let executor = executor.with_trigger_registry(registry);

        let plan = QueryPlan::Batch(BatchPlan {
            batch_type: cassandra_cql::ast::BatchType::Logged,
            plans: vec![QueryPlan::Insert(InsertPlan {
                keyspace: "ks".to_string(),
                table: "events".to_string(),
                columns: vec!["id".to_string(), "bucket".to_string(), "name".to_string()],
                values: vec![int_term(11), int_term(1), text_term("batched")],
                if_not_exists: false,
                json: None,
                json_default: JsonDefault::Null,
                using_timestamp: None,
                using_ttl: None,
            })],
        });

        executor.execute(&plan, None).unwrap();

        let audit = executor
            .engine
            .read_partition("ks", "audit", &11i32.to_be_bytes())
            .unwrap();
        let row = audit.rows.values().next().unwrap();
        assert_eq!(row.cells[0].column, "event_type");
        assert_eq!(row.cells[0].value.as_deref(), Some(b"insert".as_slice()));
    }

    #[test]
    fn batch_delete_builder_preserves_partition_tombstone() {
        let (executor, _temp) = test_executor();
        let mutation = executor
            .build_delete_mutation(
                &DeletePlan {
                    keyspace: "ks".to_string(),
                    table: "events".to_string(),
                    columns: Vec::new(),
                    where_clause: vec![Relation {
                        column: "id".to_string(),
                        op: RelationOp::Eq,
                        value: int_term(7),
                    }],
                    if_exists: false,
                    using_timestamp: None,
                    using_ttl: None,
                },
                1_000_000,
            )
            .unwrap();

        assert!(mutation.rows.is_empty());
        assert!(mutation.partition_tombstone.is_some());
        assert!(mutation.range_tombstones.is_empty());
    }

    #[test]
    fn batch_delete_builder_preserves_range_tombstone() {
        let (executor, _temp) = test_executor();
        let mutation = executor
            .build_delete_mutation(
                &DeletePlan {
                    keyspace: "ks".to_string(),
                    table: "events".to_string(),
                    columns: Vec::new(),
                    where_clause: vec![
                        Relation {
                            column: "id".to_string(),
                            op: RelationOp::Eq,
                            value: int_term(7),
                        },
                        Relation {
                            column: "bucket".to_string(),
                            op: RelationOp::Gt,
                            value: int_term(10),
                        },
                        Relation {
                            column: "bucket".to_string(),
                            op: RelationOp::Lt,
                            value: int_term(20),
                        },
                    ],
                    if_exists: false,
                    using_timestamp: None,
                    using_ttl: None,
                },
                1_000_000,
            )
            .unwrap();

        assert!(mutation.rows.is_empty());
        assert!(mutation.partition_tombstone.is_none());
        assert_eq!(mutation.range_tombstones.len(), 1);
        assert!(!mutation.range_tombstones[0].start.is_empty());
        assert!(!mutation.range_tombstones[0].end.is_empty());
    }

    #[test]
    fn create_table_applies_table_options() {
        let (executor, _temp) = test_executor();

        let plan = QueryPlan::CreateTable(CreateTablePlan {
            keyspace: "ks".to_string(),
            name: "configured".to_string(),
            if_not_exists: false,
            columns: vec![
                cassandra_cql::planner::ResolvedColumnDef {
                    name: "id".to_string(),
                    cql_type: CqlType::Int,
                    is_static: false,
                    masked_with: None,
                    constraints: Vec::new(),
                },
                cassandra_cql::planner::ResolvedColumnDef {
                    name: "name".to_string(),
                    cql_type: CqlType::Varchar,
                    is_static: false,
                    masked_with: None,
                    constraints: Vec::new(),
                },
            ],
            partition_key: vec!["id".to_string()],
            clustering_key: Vec::new(),
            clustering_order: Vec::new(),
            options: std::collections::HashMap::from([
                ("comment".to_string(), "created with params".to_string()),
                ("default_time_to_live".to_string(), "120".to_string()),
                ("transactional_mode".to_string(), "accord".to_string()),
                (
                    "compression".to_string(),
                    serde_json::json!({
                        "class": "LZ4Compressor",
                        "chunk_length_in_kb": "32"
                    })
                    .to_string(),
                ),
                (
                    "caching".to_string(),
                    serde_json::json!({
                        "keys": "ALL",
                        "rows_per_partition": "NONE"
                    })
                    .to_string(),
                ),
                ("speculative_retry".to_string(), "99p".to_string()),
                ("additional_write_policy".to_string(), "NEVER".to_string()),
                ("allow_auto_snapshot".to_string(), "false".to_string()),
                ("incremental_backups".to_string(), "true".to_string()),
                ("memtable_flush_period_in_ms".to_string(), "250".to_string()),
                ("memtable".to_string(), "default".to_string()),
                ("fast_path".to_string(), "read_local".to_string()),
                (
                    "transactional_migration_from".to_string(),
                    "paxos".to_string(),
                ),
                (
                    "auto_repair".to_string(),
                    serde_json::json!({
                        "enabled": "true"
                    })
                    .to_string(),
                ),
                ("cdc".to_string(), "true".to_string()),
            ]),
        });

        executor.execute(&plan, None).unwrap();

        let snapshot = executor.catalog.read().snapshot();
        let table = snapshot.table("ks", "configured").unwrap();
        assert_eq!(table.params.comment, "created with params");
        assert_eq!(table.params.default_time_to_live, 120);
        assert_eq!(table.params.transactional_mode, TransactionalMode::Accord);
        assert_eq!(
            table
                .params
                .compression
                .get("chunk_length_in_kb")
                .map(String::as_str),
            Some("32")
        );
        assert_eq!(
            table.params.caching.get("keys").map(String::as_str),
            Some("ALL")
        );
        assert_eq!(table.params.speculative_retry, "99p");
        assert_eq!(table.params.additional_write_policy, "NEVER");
        assert!(!table.params.allow_auto_snapshot);
        assert!(table.params.incremental_backups);
        assert_eq!(table.params.memtable_flush_period_in_ms, 250);
        assert_eq!(table.params.memtable, "default");
        assert_eq!(table.params.fast_path, "read_local");
        assert_eq!(table.params.transactional_migration_from, "paxos");
        assert_eq!(
            table.params.auto_repair.get("enabled").map(String::as_str),
            Some("true")
        );
        assert!(table.params.cdc);
    }

    #[test]
    fn create_keyspace_respects_if_not_exists() {
        let (executor, _temp) = test_executor();

        let duplicate = QueryPlan::CreateKeyspace(CreateKeyspacePlan {
            name: "ks".to_string(),
            if_not_exists: false,
            replication: std::collections::HashMap::new(),
            durable_writes: true,
        });
        assert!(matches!(
            executor.execute(&duplicate, None),
            Err(ExecutorError::InvalidQuery(msg)) if msg.contains("already exists")
        ));

        let no_op = QueryPlan::CreateKeyspace(CreateKeyspacePlan {
            name: "ks".to_string(),
            if_not_exists: true,
            replication: std::collections::HashMap::new(),
            durable_writes: true,
        });
        assert!(matches!(
            executor.execute(&no_op, None).unwrap(),
            QueryResult::Void
        ));
    }

    #[test]
    fn create_table_respects_if_not_exists() {
        let (executor, _temp) = test_executor();
        let duplicate_table = |if_not_exists| {
            QueryPlan::CreateTable(CreateTablePlan {
                keyspace: "ks".to_string(),
                name: "events".to_string(),
                if_not_exists,
                columns: vec![cassandra_cql::planner::ResolvedColumnDef {
                    name: "id".to_string(),
                    cql_type: CqlType::Int,
                    is_static: false,
                    masked_with: None,
                    constraints: Vec::new(),
                }],
                partition_key: vec!["id".to_string()],
                clustering_key: Vec::new(),
                clustering_order: Vec::new(),
                options: Default::default(),
            })
        };

        assert!(matches!(
            executor.execute(&duplicate_table(false), None),
            Err(ExecutorError::InvalidQuery(msg)) if msg.contains("already exists")
        ));
        assert!(matches!(
            executor.execute(&duplicate_table(true), None).unwrap(),
            QueryResult::Void
        ));
    }

    #[test]
    fn drop_keyspace_respects_if_exists() {
        let (executor, _temp) = test_executor();

        let missing = QueryPlan::DropKeyspace(DropKeyspacePlan {
            name: "missing".to_string(),
            if_exists: false,
        });
        assert!(matches!(
            executor.execute(&missing, None),
            Err(ExecutorError::KeyspaceNotFound(name)) if name == "missing"
        ));

        let no_op = QueryPlan::DropKeyspace(DropKeyspacePlan {
            name: "missing".to_string(),
            if_exists: true,
        });
        assert!(matches!(
            executor.execute(&no_op, None).unwrap(),
            QueryResult::Void
        ));
    }

    #[test]
    fn drop_table_respects_if_exists() {
        let (executor, _temp) = test_executor();

        let missing = QueryPlan::DropTable(DropTablePlan {
            keyspace: "ks".to_string(),
            name: "missing".to_string(),
            if_exists: false,
        });
        assert!(matches!(
            executor.execute(&missing, None),
            Err(ExecutorError::TableNotFound(ks, table)) if ks == "ks" && table == "missing"
        ));

        let no_op = QueryPlan::DropTable(DropTablePlan {
            keyspace: "ks".to_string(),
            name: "missing".to_string(),
            if_exists: true,
        });
        assert!(matches!(
            executor.execute(&no_op, None).unwrap(),
            QueryResult::Void
        ));
    }

    #[test]
    fn create_and_drop_type_respect_existence_flags() {
        let (executor, _temp) = test_executor();

        let create = |if_not_exists| {
            QueryPlan::CreateType(CreateTypePlan {
                keyspace: "ks".to_string(),
                name: "address".to_string(),
                if_not_exists,
                fields: vec![("street".to_string(), "text".to_string())],
            })
        };

        executor.execute(&create(false), None).unwrap();
        assert!(matches!(
            executor.execute(&create(false), None),
            Err(ExecutorError::InvalidQuery(msg)) if msg.contains("already exists")
        ));
        assert!(matches!(
            executor.execute(&create(true), None).unwrap(),
            QueryResult::Void
        ));

        let drop_existing = QueryPlan::DropType(DropTypePlan {
            keyspace: "ks".to_string(),
            name: "address".to_string(),
            if_exists: false,
        });
        executor.execute(&drop_existing, None).unwrap();

        let drop_missing = QueryPlan::DropType(DropTypePlan {
            keyspace: "ks".to_string(),
            name: "address".to_string(),
            if_exists: false,
        });
        assert!(matches!(
            executor.execute(&drop_missing, None),
            Err(ExecutorError::InvalidQuery(msg)) if msg.contains("does not exist")
        ));

        let drop_missing_if_exists = QueryPlan::DropType(DropTypePlan {
            keyspace: "ks".to_string(),
            name: "address".to_string(),
            if_exists: true,
        });
        assert!(matches!(
            executor.execute(&drop_missing_if_exists, None).unwrap(),
            QueryResult::Void
        ));
    }

    #[test]
    fn create_and_drop_materialized_view_respect_existence_flags() {
        let (executor, _temp) = test_executor();

        let create = |name: &str, base_table: &str, if_not_exists| {
            QueryPlan::CreateMaterializedView(CreateMaterializedViewPlan {
                keyspace: "ks".to_string(),
                name: name.to_string(),
                if_not_exists,
                base_table: base_table.to_string(),
                select_columns: SelectColumns::All,
                where_clause: Vec::new(),
                partition_key: vec!["id".to_string()],
                clustering_key: Vec::new(),
                clustering_order: Vec::new(),
                options: Default::default(),
            })
        };

        executor
            .execute(&create("events_by_id", "events", false), None)
            .unwrap();
        assert!(matches!(
            executor.execute(&create("events_by_id", "events", false), None),
            Err(ExecutorError::InvalidQuery(msg)) if msg.contains("already exists")
        ));
        assert!(matches!(
            executor
                .execute(&create("events_by_id", "events", true), None)
                .unwrap(),
            QueryResult::Void
        ));
        assert!(matches!(
            executor.execute(&create("bad_view", "missing", false), None),
            Err(ExecutorError::TableNotFound(ks, table)) if ks == "ks" && table == "missing"
        ));

        let drop_existing = QueryPlan::DropMaterializedView(DropMaterializedViewPlan {
            keyspace: "ks".to_string(),
            name: "events_by_id".to_string(),
            if_exists: false,
        });
        executor.execute(&drop_existing, None).unwrap();

        let drop_missing = QueryPlan::DropMaterializedView(DropMaterializedViewPlan {
            keyspace: "ks".to_string(),
            name: "events_by_id".to_string(),
            if_exists: false,
        });
        assert!(matches!(
            executor.execute(&drop_missing, None),
            Err(ExecutorError::TableNotFound(ks, view)) if ks == "ks" && view == "events_by_id"
        ));

        let drop_missing_if_exists = QueryPlan::DropMaterializedView(DropMaterializedViewPlan {
            keyspace: "ks".to_string(),
            name: "events_by_id".to_string(),
            if_exists: true,
        });
        assert!(matches!(
            executor.execute(&drop_missing_if_exists, None).unwrap(),
            QueryResult::Void
        ));
    }

    #[test]
    fn create_and_drop_trigger_respect_existence_flags() {
        let (executor, _temp) = test_executor();

        let create = |if_not_exists| {
            QueryPlan::CreateTrigger(CreateTriggerPlan {
                keyspace: "ks".to_string(),
                table: "events".to_string(),
                name: "audit_trigger".to_string(),
                if_not_exists,
                trigger_class: "org.example.AuditTrigger".to_string(),
            })
        };

        executor.execute(&create(false), None).unwrap();
        assert!(matches!(
            executor.execute(&create(false), None),
            Err(ExecutorError::InvalidQuery(msg)) if msg.contains("already exists")
        ));
        assert!(matches!(
            executor.execute(&create(true), None).unwrap(),
            QueryResult::Void
        ));

        let drop_existing = QueryPlan::DropTrigger(DropTriggerPlan {
            keyspace: "ks".to_string(),
            table: "events".to_string(),
            name: "audit_trigger".to_string(),
            if_exists: false,
        });
        executor.execute(&drop_existing, None).unwrap();

        let drop_missing = QueryPlan::DropTrigger(DropTriggerPlan {
            keyspace: "ks".to_string(),
            table: "events".to_string(),
            name: "audit_trigger".to_string(),
            if_exists: false,
        });
        assert!(matches!(
            executor.execute(&drop_missing, None),
            Err(ExecutorError::InvalidQuery(msg)) if msg.contains("does not exist")
        ));

        let drop_missing_if_exists = QueryPlan::DropTrigger(DropTriggerPlan {
            keyspace: "ks".to_string(),
            table: "events".to_string(),
            name: "audit_trigger".to_string(),
            if_exists: true,
        });
        assert!(matches!(
            executor.execute(&drop_missing_if_exists, None).unwrap(),
            QueryResult::Void
        ));
    }

    #[test]
    fn alter_table_updates_catalog_and_returns_schema_change() {
        let (executor, _temp) = test_executor();

        let add_column = QueryPlan::AlterTable(AlterTablePlan {
            keyspace: "ks".to_string(),
            name: "events".to_string(),
            operation: AlterTableOp::AddColumn(ColumnDef {
                name: "status".to_string(),
                cql_type: CqlTypeName::Simple("text".to_string()),
                is_static: false,
                masked_with: None,
                constraints: Vec::new(),
            }),
        });

        assert!(matches!(
            executor.execute(&add_column, None).unwrap(),
            QueryResult::SchemaChange {
                change_type,
                target,
                keyspace,
                name: Some(name),
            } if change_type == "UPDATED"
                && target == "TABLE"
                && keyspace == "ks"
                && name == "events"
        ));
        assert!(
            executor
                .catalog
                .read()
                .snapshot()
                .table("ks", "events")
                .unwrap()
                .column("status")
                .is_some()
        );

        let alter_options = QueryPlan::AlterTable(AlterTablePlan {
            keyspace: "ks".to_string(),
            name: "events".to_string(),
            operation: AlterTableOp::WithOptions(std::collections::HashMap::from([
                ("comment".to_string(), "hot path events".to_string()),
                ("gc_grace_seconds".to_string(), "3600".to_string()),
                (
                    "compaction".to_string(),
                    serde_json::json!({
                        "class": "SizeTieredCompactionStrategy",
                        "min_threshold": "2"
                    })
                    .to_string(),
                ),
                (
                    "compression".to_string(),
                    serde_json::json!({
                        "class": "LZ4Compressor",
                        "chunk_length_in_kb": "64"
                    })
                    .to_string(),
                ),
                (
                    "caching".to_string(),
                    serde_json::json!({
                        "keys": "ALL",
                        "rows_per_partition": "10"
                    })
                    .to_string(),
                ),
                ("read_repair".to_string(), "BLOCKING".to_string()),
                ("speculative_retry".to_string(), "NONE".to_string()),
                ("additional_write_policy".to_string(), "99p".to_string()),
                ("allow_auto_snapshot".to_string(), "false".to_string()),
                ("incremental_backups".to_string(), "true".to_string()),
                ("memtable_flush_period_in_ms".to_string(), "500".to_string()),
                ("memtable".to_string(), "skiplist".to_string()),
                ("fast_path".to_string(), "write_local".to_string()),
                (
                    "transactional_migration_from".to_string(),
                    "paxos".to_string(),
                ),
                (
                    "auto_repair".to_string(),
                    serde_json::json!({
                        "enabled": "false"
                    })
                    .to_string(),
                ),
                ("cdc".to_string(), "true".to_string()),
                ("transactional_mode".to_string(), "mixed".to_string()),
            ])),
        });
        executor.execute(&alter_options, None).unwrap();
        let snapshot = executor.catalog.read().snapshot();
        let table = snapshot.table("ks", "events").unwrap();
        assert_eq!(table.params.comment, "hot path events");
        assert_eq!(table.params.gc_grace_seconds, 3600);
        assert_eq!(
            table.params.compaction.get("class").map(String::as_str),
            Some("SizeTieredCompactionStrategy")
        );
        assert_eq!(
            table
                .params
                .compression
                .get("chunk_length_in_kb")
                .map(String::as_str),
            Some("64")
        );
        assert_eq!(
            table.params.caching.get("keys").map(String::as_str),
            Some("ALL")
        );
        assert_eq!(table.params.read_repair, "BLOCKING");
        assert_eq!(table.params.speculative_retry, "NONE");
        assert_eq!(table.params.additional_write_policy, "99p");
        assert!(!table.params.allow_auto_snapshot);
        assert!(table.params.incremental_backups);
        assert_eq!(table.params.memtable_flush_period_in_ms, 500);
        assert_eq!(table.params.memtable, "skiplist");
        assert_eq!(table.params.fast_path, "write_local");
        assert_eq!(table.params.transactional_migration_from, "paxos");
        assert_eq!(
            table.params.auto_repair.get("enabled").map(String::as_str),
            Some("false")
        );
        assert!(table.params.cdc);
        assert_eq!(table.params.transactional_mode, TransactionalMode::Mixed);

        let drop_column = QueryPlan::AlterTable(AlterTablePlan {
            keyspace: "ks".to_string(),
            name: "events".to_string(),
            operation: AlterTableOp::DropColumn("status".to_string()),
        });
        executor.execute(&drop_column, None).unwrap();
        let snapshot = executor.catalog.read().snapshot();
        let table = snapshot.table("ks", "events").unwrap();
        assert!(table.column("status").is_none());
        assert!(table.dropped_columns.iter().any(|c| c.name == "status"));
    }

    #[test]
    fn unkeyed_select_scans_all_partitions() {
        let (executor, _temp) = test_executor();
        insert_event(&executor, 1, 1, "a");
        insert_event(&executor, 1, 2, "b");
        insert_event(&executor, 2, 1, "c");

        let plan = QueryPlan::Select(SelectPlan {
            keyspace: "ks".to_string(),
            table: "events".to_string(),
            columns: SelectColumns::Named(vec![
                Selector::Column("id".to_string()),
                Selector::Column("bucket".to_string()),
                Selector::Column("name".to_string()),
            ]),
            distinct: false,
            json: false,
            where_clause: Vec::new(),
            group_by: Vec::new(),
            order_by: Vec::new(),
            limit: None,
            allow_filtering: false,
            restrictions: None,
            ann_clause: None,
            page_size: None,
            paging_state: None,
        });

        let QueryResult::Rows { rows, .. } = executor.execute(&plan, None).unwrap() else {
            panic!("expected rows");
        };
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn unkeyed_select_filters_non_key_like_restriction() {
        let (executor, _temp) = test_executor();
        insert_event(&executor, 1, 1, "alpha");
        insert_event(&executor, 2, 1, "bravo");
        insert_event(&executor, 3, 1, "alpine");

        let plan = QueryPlan::Select(SelectPlan {
            keyspace: "ks".to_string(),
            table: "events".to_string(),
            columns: SelectColumns::Named(vec![
                Selector::Column("id".to_string()),
                Selector::Column("name".to_string()),
            ]),
            distinct: false,
            json: false,
            where_clause: vec![Relation {
                column: "name".to_string(),
                op: RelationOp::Like,
                value: text_term("alp%"),
            }],
            group_by: Vec::new(),
            order_by: Vec::new(),
            limit: None,
            allow_filtering: true,
            restrictions: None,
            ann_clause: None,
            page_size: None,
            paging_state: None,
        });

        let QueryResult::Rows { rows, .. } = executor.execute(&plan, None).unwrap() else {
            panic!("expected rows");
        };
        let mut names: Vec<String> = rows
            .iter()
            .map(|row| String::from_utf8(row[1].clone().unwrap()).unwrap())
            .collect();
        names.sort();
        assert_eq!(names, vec!["alpha".to_string(), "alpine".to_string()]);
    }

    #[test]
    fn unkeyed_select_filters_token_restriction() {
        let (executor, _temp) = test_executor();
        insert_event(&executor, 1, 1, "one");
        insert_event(&executor, 2, 1, "two");

        let token = cassandra_common::murmur3::murmur3_token(&1i32.to_be_bytes());
        let plan = QueryPlan::Select(SelectPlan {
            keyspace: "ks".to_string(),
            table: "events".to_string(),
            columns: SelectColumns::Named(vec![
                Selector::Column("id".to_string()),
                Selector::Column("name".to_string()),
            ]),
            distinct: false,
            json: false,
            where_clause: vec![Relation {
                column: "token".to_string(),
                op: RelationOp::Eq,
                value: int_term(token),
            }],
            group_by: Vec::new(),
            order_by: Vec::new(),
            limit: None,
            allow_filtering: true,
            restrictions: None,
            ann_clause: None,
            page_size: None,
            paging_state: None,
        });

        let QueryResult::Rows { rows, .. } = executor.execute(&plan, None).unwrap() else {
            panic!("expected rows");
        };
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0].as_deref(), Some(1i32.to_be_bytes().as_slice()));
        assert_eq!(rows[0][1].as_deref(), Some(b"one".as_slice()));
    }

    #[test]
    fn keyed_select_filters_non_key_restriction() {
        let (executor, _temp) = test_executor();
        insert_event(&executor, 1, 1, "keep");
        insert_event(&executor, 1, 2, "drop");

        let plan = QueryPlan::Select(SelectPlan {
            keyspace: "ks".to_string(),
            table: "events".to_string(),
            columns: SelectColumns::Named(vec![
                Selector::Column("bucket".to_string()),
                Selector::Column("name".to_string()),
            ]),
            distinct: false,
            json: false,
            where_clause: vec![
                Relation {
                    column: "id".to_string(),
                    op: RelationOp::Eq,
                    value: int_term(1),
                },
                Relation {
                    column: "name".to_string(),
                    op: RelationOp::Eq,
                    value: text_term("keep"),
                },
            ],
            group_by: Vec::new(),
            order_by: Vec::new(),
            limit: None,
            allow_filtering: true,
            restrictions: None,
            ann_clause: None,
            page_size: None,
            paging_state: None,
        });

        let QueryResult::Rows { rows, .. } = executor.execute(&plan, None).unwrap() else {
            panic!("expected rows");
        };
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Some(1i32.to_be_bytes().to_vec()));
        assert_eq!(rows[0][1], Some(b"keep".to_vec()));
    }

    #[test]
    fn count_star_applies_where_filter() {
        let (executor, _temp) = test_executor();
        insert_event(&executor, 1, 1, "alpha");
        insert_event(&executor, 2, 1, "beta");
        insert_event(&executor, 3, 1, "alpine");

        let plan = QueryPlan::Select(SelectPlan {
            keyspace: "ks".to_string(),
            table: "events".to_string(),
            columns: SelectColumns::Named(vec![Selector::Count]),
            distinct: false,
            json: false,
            where_clause: vec![Relation {
                column: "name".to_string(),
                op: RelationOp::Like,
                value: text_term("al%"),
            }],
            group_by: Vec::new(),
            order_by: Vec::new(),
            limit: None,
            allow_filtering: true,
            restrictions: None,
            ann_clause: None,
            page_size: None,
            paging_state: None,
        });

        let QueryResult::Rows { rows, .. } = executor.execute(&plan, None).unwrap() else {
            panic!("expected rows");
        };
        let count = i64::from_be_bytes(rows[0][0].as_ref().unwrap().as_slice().try_into().unwrap());
        assert_eq!(count, 2);
    }

    #[test]
    fn count_rows_aliases_aggregate_rows() {
        let (executor, _temp) = test_executor();
        insert_event(&executor, 1, 1, "alpha");
        insert_event(&executor, 2, 1, "beta");
        insert_event(&executor, 3, 1, "gamma");

        for name in ["count_rows", "countRows"] {
            let plan = QueryPlan::Select(SelectPlan {
                keyspace: "ks".to_string(),
                table: "events".to_string(),
                columns: SelectColumns::Named(vec![Selector::Function(
                    name.to_string(),
                    Vec::new(),
                )]),
                distinct: false,
                json: false,
                where_clause: Vec::new(),
                group_by: Vec::new(),
                order_by: Vec::new(),
                limit: None,
                allow_filtering: false,
                restrictions: None,
                ann_clause: None,
                page_size: None,
                paging_state: None,
            });

            let QueryResult::Rows { columns, rows, .. } = executor.execute(&plan, None).unwrap()
            else {
                panic!("expected rows");
            };
            assert_eq!(columns[0].name, "count");
            assert_eq!(columns[0].cql_type, CqlType::Bigint);
            let count =
                i64::from_be_bytes(rows[0][0].as_ref().unwrap().as_slice().try_into().unwrap());
            assert_eq!(count, 3);
        }
    }

    #[test]
    fn count_column_skips_null_values() {
        let (executor, _temp) = test_executor();
        insert_event(&executor, 1, 1, "alpha");
        insert_event_without_name(&executor, 2, 1);
        insert_event(&executor, 3, 1, "gamma");

        let plan = QueryPlan::Select(SelectPlan {
            keyspace: "ks".to_string(),
            table: "events".to_string(),
            columns: SelectColumns::Named(vec![Selector::Function(
                "count".to_string(),
                vec![Selector::Column("name".to_string())],
            )]),
            distinct: false,
            json: false,
            where_clause: Vec::new(),
            group_by: Vec::new(),
            order_by: Vec::new(),
            limit: None,
            allow_filtering: false,
            restrictions: None,
            ann_clause: None,
            page_size: None,
            paging_state: None,
        });

        let QueryResult::Rows { columns, rows, .. } = executor.execute(&plan, None).unwrap() else {
            panic!("expected rows");
        };
        assert_eq!(columns[0].name, "count(name)");
        assert_eq!(columns[0].cql_type, CqlType::Bigint);
        let count = i64::from_be_bytes(rows[0][0].as_ref().unwrap().as_slice().try_into().unwrap());
        assert_eq!(count, 2);
    }

    #[test]
    fn select_applies_mask_default_via_cql_function_registry() {
        let (executor, _temp) = test_executor();
        insert_event(&executor, 1, 1, "secret");
        mask_event_name(&executor, "mask_default", Vec::new());

        let plan = QueryPlan::Select(SelectPlan {
            keyspace: "ks".to_string(),
            table: "events".to_string(),
            columns: SelectColumns::Named(vec![Selector::Column("name".to_string())]),
            distinct: false,
            json: false,
            where_clause: Vec::new(),
            group_by: Vec::new(),
            order_by: Vec::new(),
            limit: None,
            allow_filtering: false,
            restrictions: None,
            ann_clause: None,
            page_size: None,
            paging_state: None,
        });

        let QueryResult::Rows { rows, .. } = executor.execute(&plan, None).unwrap() else {
            panic!("expected rows");
        };
        assert_eq!(rows[0][0], Some(b"****".to_vec()));
    }

    #[test]
    fn select_applies_partial_and_replace_masks_with_schema_args() {
        let (executor, _temp) = test_executor();
        insert_event(&executor, 1, 1, "secret");
        mask_event_name(
            &executor,
            "mask_inner",
            vec![int_term(1), int_term(1), text_term("#")],
        );

        let select_name = QueryPlan::Select(SelectPlan {
            keyspace: "ks".to_string(),
            table: "events".to_string(),
            columns: SelectColumns::Named(vec![Selector::Column("name".to_string())]),
            distinct: false,
            json: false,
            where_clause: Vec::new(),
            group_by: Vec::new(),
            order_by: Vec::new(),
            limit: None,
            allow_filtering: false,
            restrictions: None,
            ann_clause: None,
            page_size: None,
            paging_state: None,
        });

        let QueryResult::Rows { rows, .. } = executor.execute(&select_name, None).unwrap() else {
            panic!("expected rows");
        };
        assert_eq!(rows[0][0], Some(b"s####t".to_vec()));

        mask_event_name(&executor, "mask_replace", vec![text_term("REDACTED")]);
        let QueryResult::Rows { rows, .. } = executor.execute(&select_name, None).unwrap() else {
            panic!("expected rows");
        };
        assert_eq!(rows[0][0], Some(b"REDACTED".to_vec()));
    }

    #[test]
    fn select_applies_mask_hash_blob_column_as_blob_digest() {
        let (executor, _temp) = test_executor();
        add_event_payload_column(&executor);
        insert_event_with_payload(&executor, 1, 1, "secret", b"secret");
        mask_event_column(&executor, "payload", "mask_hash", Vec::new());

        let plan = QueryPlan::Select(SelectPlan {
            keyspace: "ks".to_string(),
            table: "events".to_string(),
            columns: SelectColumns::Named(vec![Selector::Column("payload".to_string())]),
            distinct: false,
            json: false,
            where_clause: Vec::new(),
            group_by: Vec::new(),
            order_by: Vec::new(),
            limit: None,
            allow_filtering: false,
            restrictions: None,
            ann_clause: None,
            page_size: None,
            paging_state: None,
        });

        let QueryResult::Rows { rows, .. } = executor.execute(&plan, None).unwrap() else {
            panic!("expected rows");
        };
        assert_eq!(
            rows[0][0],
            Some(
                [
                    0x2b, 0xb8, 0x0d, 0x53, 0x7b, 0x1d, 0xa3, 0xe3, 0x8b, 0xd3, 0x03, 0x61, 0xaa,
                    0x85, 0x56, 0x86, 0xbd, 0xe0, 0xea, 0xcd, 0x71, 0x62, 0xfe, 0xf6, 0xa2, 0x5f,
                    0xe9, 0x7b, 0xf5, 0x27, 0xa2, 0x5b,
                ]
                .to_vec()
            )
        );

        mask_event_column(
            &executor,
            "payload",
            "mask_hash",
            vec![text_term("SHA3-256")],
        );
        let QueryResult::Rows { rows, .. } = executor.execute(&plan, None).unwrap() else {
            panic!("expected rows");
        };
        assert_eq!(
            rows[0][0],
            Some(
                [
                    0xf5, 0xa5, 0x20, 0x7a, 0x87, 0x29, 0xb1, 0xf7, 0x09, 0xcb, 0x71, 0x03, 0x11,
                    0x75, 0x1e, 0xb2, 0xfc, 0x8a, 0xca, 0xd5, 0xa1, 0xfb, 0x8a, 0xc9, 0x91, 0xb7,
                    0x36, 0xe6, 0x9b, 0x65, 0x29, 0xa3,
                ]
                .to_vec()
            )
        );
    }

    #[test]
    fn alter_table_rejects_invalid_mask_definitions() {
        let (executor, _temp) = test_executor();

        let unknown = executor.execute(
            &QueryPlan::AlterTable(AlterTablePlan {
                keyspace: "ks".to_string(),
                name: "events".to_string(),
                operation: AlterTableOp::MaskColumn(
                    "name".to_string(),
                    "mask_unknown".to_string(),
                    Vec::new(),
                ),
            }),
            None,
        );
        assert!(matches!(unknown, Err(ExecutorError::InvalidQuery(_))));

        let wrong_arg_type = executor.execute(
            &QueryPlan::AlterTable(AlterTablePlan {
                keyspace: "ks".to_string(),
                name: "events".to_string(),
                operation: AlterTableOp::MaskColumn(
                    "name".to_string(),
                    "mask_inner".to_string(),
                    vec![text_term("not-int"), int_term(1)],
                ),
            }),
            None,
        );
        assert!(matches!(
            wrong_arg_type,
            Err(ExecutorError::InvalidQuery(_))
        ));

        let incompatible_column = executor.execute(
            &QueryPlan::AlterTable(AlterTablePlan {
                keyspace: "ks".to_string(),
                name: "events".to_string(),
                operation: AlterTableOp::MaskColumn(
                    "bucket".to_string(),
                    "mask_inner".to_string(),
                    vec![int_term(1), int_term(1)],
                ),
            }),
            None,
        );
        assert!(matches!(
            incompatible_column,
            Err(ExecutorError::InvalidQuery(_))
        ));

        let invalid_replace_value = executor.execute(
            &QueryPlan::AlterTable(AlterTablePlan {
                keyspace: "ks".to_string(),
                name: "events".to_string(),
                operation: AlterTableOp::MaskColumn(
                    "bucket".to_string(),
                    "mask_replace".to_string(),
                    vec![text_term("not-an-int")],
                ),
            }),
            None,
        );
        assert!(matches!(
            invalid_replace_value,
            Err(ExecutorError::InvalidQuery(_))
        ));

        let type_altering_hash = executor.execute(
            &QueryPlan::AlterTable(AlterTablePlan {
                keyspace: "ks".to_string(),
                name: "events".to_string(),
                operation: AlterTableOp::MaskColumn(
                    "name".to_string(),
                    "mask_hash".to_string(),
                    Vec::new(),
                ),
            }),
            None,
        );
        assert!(matches!(
            type_altering_hash,
            Err(ExecutorError::InvalidQuery(_))
        ));

        add_event_payload_column(&executor);
        let invalid_hash_algorithm = executor.execute(
            &QueryPlan::AlterTable(AlterTablePlan {
                keyspace: "ks".to_string(),
                name: "events".to_string(),
                operation: AlterTableOp::MaskColumn(
                    "payload".to_string(),
                    "mask_hash".to_string(),
                    vec![text_term("unknown-algorithm")],
                ),
            }),
            None,
        );
        assert!(matches!(
            invalid_hash_algorithm,
            Err(ExecutorError::InvalidQuery(_))
        ));

        let invalid_partial_padding = executor.execute(
            &QueryPlan::AlterTable(AlterTablePlan {
                keyspace: "ks".to_string(),
                name: "events".to_string(),
                operation: AlterTableOp::MaskColumn(
                    "name".to_string(),
                    "mask_inner".to_string(),
                    vec![int_term(1), int_term(1), text_term("xx")],
                ),
            }),
            None,
        );
        assert!(matches!(
            invalid_partial_padding,
            Err(ExecutorError::InvalidQuery(_))
        ));

        let add_type_altering_mask = executor.execute(
            &QueryPlan::AlterTable(AlterTablePlan {
                keyspace: "ks".to_string(),
                name: "events".to_string(),
                operation: AlterTableOp::AddColumn(ColumnDef {
                    name: "masked_text".to_string(),
                    cql_type: CqlTypeName::Simple("text".to_string()),
                    is_static: false,
                    masked_with: Some(("mask_hash".to_string(), Vec::new())),
                    constraints: Vec::new(),
                }),
            }),
            None,
        );
        assert!(matches!(
            add_type_altering_mask,
            Err(ExecutorError::InvalidQuery(_))
        ));

        let create_type_altering_mask = executor.execute(
            &QueryPlan::CreateTable(CreateTablePlan {
                keyspace: "ks".to_string(),
                name: "bad_masked_table".to_string(),
                if_not_exists: false,
                columns: vec![
                    cassandra_cql::planner::ResolvedColumnDef {
                        name: "k".to_string(),
                        cql_type: CqlType::Int,
                        is_static: false,
                        masked_with: None,
                        constraints: Vec::new(),
                    },
                    cassandra_cql::planner::ResolvedColumnDef {
                        name: "v".to_string(),
                        cql_type: CqlType::Varchar,
                        is_static: false,
                        masked_with: Some(("mask_hash".to_string(), Vec::new())),
                        constraints: Vec::new(),
                    },
                ],
                partition_key: vec!["k".to_string()],
                clustering_key: Vec::new(),
                clustering_order: Vec::new(),
                options: std::collections::HashMap::new(),
            }),
            None,
        );
        assert!(matches!(
            create_type_altering_mask,
            Err(ExecutorError::InvalidQuery(_))
        ));
    }

    #[test]
    fn select_requires_select_masked_when_filtering_on_masked_column() {
        let (executor, _temp) = test_executor_with_authorizer(true);
        executor
            .execute(&create_role_plan("analyst"), None)
            .unwrap();
        insert_event(&executor, 1, 1, "secret");
        mask_event_column(&executor, "bucket", "mask_default", Vec::new());

        let select_by_masked_column = QueryPlan::Select(SelectPlan {
            keyspace: "ks".to_string(),
            table: "events".to_string(),
            columns: SelectColumns::Named(vec![
                Selector::Column("id".to_string()),
                Selector::Column("bucket".to_string()),
                Selector::Column("name".to_string()),
            ]),
            distinct: false,
            json: false,
            where_clause: vec![Relation {
                column: "bucket".to_string(),
                op: RelationOp::Eq,
                value: int_term(1),
            }],
            group_by: Vec::new(),
            order_by: Vec::new(),
            limit: None,
            allow_filtering: true,
            restrictions: None,
            ann_clause: None,
            page_size: None,
            paging_state: None,
        });

        assert!(matches!(
            executor.execute(&select_by_masked_column, Some("analyst")),
            Err(ExecutorError::InvalidQuery(msg))
                if msg.contains("no UNMASK nor SELECT_MASKED")
                    && msg.contains("[bucket]")
        ));

        let resource = SecurityResource::Table {
            keyspace: "ks".to_string(),
            table: "events".to_string(),
        };
        executor
            .authorizer
            .grant("cassandra", "analyst", &resource, Permission::SelectMasked)
            .unwrap();
        let QueryResult::Rows { rows, .. } = executor
            .execute(&select_by_masked_column, Some("analyst"))
            .unwrap()
        else {
            panic!("expected rows");
        };
        assert_eq!(rows[0][1], Some(0i32.to_be_bytes().to_vec()));

        executor
            .authorizer
            .grant("cassandra", "analyst", &resource, Permission::Unmask)
            .unwrap();
        let QueryResult::Rows { rows, .. } = executor
            .execute(&select_by_masked_column, Some("analyst"))
            .unwrap()
        else {
            panic!("expected rows");
        };
        assert_eq!(rows[0][1], Some(1i32.to_be_bytes().to_vec()));
    }

    #[test]
    fn select_requires_select_masked_when_token_restricts_masked_partition_key() {
        let (executor, _temp) = test_executor_with_authorizer(true);
        executor
            .execute(&create_role_plan("analyst"), None)
            .unwrap();
        insert_event(&executor, 1, 1, "secret");
        mask_event_column(&executor, "id", "mask_default", Vec::new());

        let token = cassandra_common::murmur3::murmur3_token(&1i32.to_be_bytes());
        let select_by_masked_token = QueryPlan::Select(SelectPlan {
            keyspace: "ks".to_string(),
            table: "events".to_string(),
            columns: SelectColumns::Named(vec![
                Selector::Column("id".to_string()),
                Selector::Column("bucket".to_string()),
                Selector::Column("name".to_string()),
            ]),
            distinct: false,
            json: false,
            where_clause: vec![Relation {
                column: "token".to_string(),
                op: RelationOp::Eq,
                value: int_term(token),
            }],
            group_by: Vec::new(),
            order_by: Vec::new(),
            limit: None,
            allow_filtering: true,
            restrictions: None,
            ann_clause: None,
            page_size: None,
            paging_state: None,
        });

        assert!(matches!(
            executor.execute(&select_by_masked_token, Some("analyst")),
            Err(ExecutorError::InvalidQuery(msg))
                if msg.contains("no UNMASK nor SELECT_MASKED")
                    && msg.contains("[id]")
        ));
    }

    #[test]
    fn select_json_formats_masked_values_with_cql_types() {
        let (executor, _temp) = test_executor();
        insert_event(&executor, 1, 7, "secret");
        mask_event_column(&executor, "bucket", "mask_default", Vec::new());
        mask_event_column(
            &executor,
            "name",
            "mask_replace",
            vec![text_term("REDACTED")],
        );

        let plan = QueryPlan::Select(SelectPlan {
            keyspace: "ks".to_string(),
            table: "events".to_string(),
            columns: SelectColumns::Named(vec![
                Selector::Column("id".to_string()),
                Selector::Column("bucket".to_string()),
                Selector::Column("name".to_string()),
            ]),
            distinct: false,
            json: true,
            where_clause: Vec::new(),
            group_by: Vec::new(),
            order_by: Vec::new(),
            limit: None,
            allow_filtering: false,
            restrictions: None,
            ann_clause: None,
            page_size: None,
            paging_state: None,
        });

        let QueryResult::Rows { columns, rows, .. } = executor.execute(&plan, None).unwrap() else {
            panic!("expected rows");
        };
        assert_eq!(columns[0].name, "[json]");
        let json = String::from_utf8(rows[0][0].clone().unwrap()).unwrap();
        let value: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(value["id"], serde_json::json!(1));
        assert_eq!(value["bucket"], serde_json::json!(0));
        assert_eq!(value["name"], serde_json::json!("REDACTED"));
    }

    #[derive(Debug)]
    struct AuditTrigger;

    impl cassandra_cql::triggers::Trigger for AuditTrigger {
        fn augment(
            &self,
            event: &MutationEvent,
        ) -> Result<Vec<cassandra_cql::triggers::TriggerMutation>, String> {
            Ok(vec![cassandra_cql::triggers::TriggerMutation {
                keyspace: event.keyspace.clone(),
                table: "audit".to_string(),
                partition_key: event.partition_key.clone(),
                mutations: vec![("event_type".to_string(), b"insert".to_vec())],
            }])
        }
    }

    #[test]
    fn insert_executes_loaded_trigger_mutations() {
        let (executor, _temp) = test_executor();
        let registry = Arc::new(TriggerRegistry::new());
        registry
            .register_with_implementation(
                cassandra_cql::triggers::TriggerMetadata {
                    name: "audit".to_string(),
                    keyspace: "ks".to_string(),
                    table: "events".to_string(),
                    trigger_class: "audit.Trigger".to_string(),
                },
                Arc::new(AuditTrigger),
            )
            .unwrap();
        let executor = executor.with_trigger_registry(registry);

        insert_event(&executor, 7, 1, "created");

        let audit = executor
            .engine
            .read_partition("ks", "audit", &7i32.to_be_bytes())
            .unwrap();
        let row = audit.rows.values().next().unwrap();
        assert_eq!(row.cells[0].column, "event_type");
        assert_eq!(row.cells[0].value.as_deref(), Some(b"insert".as_slice()));
    }

    #[test]
    fn truncate_clears_memtable_and_sstable_data() {
        let (executor, _temp) = test_executor();
        insert_event(&executor, 1, 1, "a");
        executor.engine.flush_cf("ks.events").unwrap();
        insert_event(&executor, 2, 1, "b");

        assert!(
            executor
                .engine
                .read_partition("ks", "events", &1i32.to_be_bytes())
                .is_some()
        );
        assert!(
            executor
                .engine
                .read_partition("ks", "events", &2i32.to_be_bytes())
                .is_some()
        );

        let truncate = QueryPlan::Truncate(TruncatePlan {
            keyspace: "ks".to_string(),
            table: "events".to_string(),
        });
        assert!(matches!(
            executor.execute(&truncate, None).unwrap(),
            QueryResult::Void
        ));

        assert!(select_all_events(&executor).is_empty());
        assert!(
            executor
                .engine
                .read_partition("ks", "events", &1i32.to_be_bytes())
                .is_none()
        );
        assert!(
            executor
                .engine
                .read_partition("ks", "events", &2i32.to_be_bytes())
                .is_none()
        );
    }

    #[test]
    fn truncate_missing_table_fails() {
        let (executor, _temp) = test_executor();
        let truncate = QueryPlan::Truncate(TruncatePlan {
            keyspace: "ks".to_string(),
            table: "missing".to_string(),
        });

        assert!(matches!(
            executor.execute(&truncate, None),
            Err(ExecutorError::TableNotFound(ref ks, ref table))
                if ks == "ks" && table == "missing"
        ));
    }

    #[test]
    fn alter_materialized_view_updates_stored_options() {
        let (executor, _temp) = test_executor();
        let create = QueryPlan::CreateMaterializedView(CreateMaterializedViewPlan {
            keyspace: "ks".to_string(),
            name: "events_by_name".to_string(),
            if_not_exists: false,
            base_table: "events".to_string(),
            select_columns: SelectColumns::Named(vec![
                Selector::Column("id".to_string()),
                Selector::Column("bucket".to_string()),
                Selector::Column("name".to_string()),
            ]),
            where_clause: Vec::new(),
            partition_key: vec!["name".to_string()],
            clustering_key: vec!["id".to_string(), "bucket".to_string()],
            clustering_order: Vec::new(),
            options: std::collections::HashMap::from([(
                "gc_grace_seconds".to_string(),
                "86400".to_string(),
            )]),
        });
        executor.execute(&create, None).unwrap();

        let alter = QueryPlan::AlterMaterializedView(AlterMaterializedViewPlan {
            keyspace: "ks".to_string(),
            name: "events_by_name".to_string(),
            options: std::collections::HashMap::from([
                ("gc_grace_seconds".to_string(), "3600".to_string()),
                ("comment".to_string(), "by name".to_string()),
            ]),
        });
        assert!(matches!(
            executor.execute(&alter, None).unwrap(),
            QueryResult::SchemaChange { .. }
        ));

        let catalog = executor.catalog.read();
        let snapshot = catalog.snapshot();
        let view = snapshot.view("ks", "events_by_name").unwrap();
        assert_eq!(
            view.options.get("gc_grace_seconds").map(String::as_str),
            Some("3600")
        );
        assert_eq!(
            view.options.get("comment").map(String::as_str),
            Some("by name")
        );
    }

    #[test]
    fn alter_missing_materialized_view_fails() {
        let (executor, _temp) = test_executor();
        let alter = QueryPlan::AlterMaterializedView(AlterMaterializedViewPlan {
            keyspace: "ks".to_string(),
            name: "missing_view".to_string(),
            options: std::collections::HashMap::from([(
                "gc_grace_seconds".to_string(),
                "3600".to_string(),
            )]),
        });

        assert!(matches!(
            executor.execute(&alter, None),
            Err(ExecutorError::TableNotFound(ref ks, ref table))
                if ks == "ks" && table == "missing_view"
        ));
    }

    #[test]
    fn select_distinct_returns_one_row_per_partition_key() {
        let (executor, _temp) = test_executor();
        insert_event(&executor, 1, 1, "a");
        insert_event(&executor, 1, 2, "b");
        insert_event(&executor, 2, 1, "c");

        let plan = QueryPlan::Select(SelectPlan {
            keyspace: "ks".to_string(),
            table: "events".to_string(),
            columns: SelectColumns::Named(vec![Selector::Column("id".to_string())]),
            distinct: true,
            json: false,
            where_clause: Vec::new(),
            group_by: Vec::new(),
            order_by: Vec::new(),
            limit: None,
            allow_filtering: false,
            restrictions: None,
            ann_clause: None,
            page_size: None,
            paging_state: None,
        });

        let QueryResult::Rows { rows, .. } = executor.execute(&plan, None).unwrap() else {
            panic!("expected rows");
        };
        assert_eq!(rows.len(), 2);
        assert!(
            rows.iter()
                .any(|row| row[0] == Some(1i32.to_be_bytes().to_vec()))
        );
        assert!(
            rows.iter()
                .any(|row| row[0] == Some(2i32.to_be_bytes().to_vec()))
        );
    }

    #[test]
    fn indexed_select_projects_partition_key_columns() {
        let (executor, _temp) = test_executor();

        executor
            .execute(
                &QueryPlan::CreateIndex(CreateIndexPlan {
                    keyspace: "ks".to_string(),
                    table: "events".to_string(),
                    index_name: "events_name_idx".to_string(),
                    column: "name".to_string(),
                    kind: "keys".to_string(),
                    custom_class: None,
                    options: std::collections::HashMap::new(),
                    if_not_exists: false,
                }),
                None,
            )
            .unwrap();
        insert_event(&executor, 7, 1, "indexed");

        let plan = QueryPlan::Select(SelectPlan {
            keyspace: "ks".to_string(),
            table: "events".to_string(),
            columns: SelectColumns::Named(vec![
                Selector::Column("id".to_string()),
                Selector::Column("name".to_string()),
            ]),
            distinct: false,
            json: false,
            where_clause: vec![Relation {
                column: "name".to_string(),
                op: RelationOp::Eq,
                value: text_term("indexed"),
            }],
            group_by: Vec::new(),
            order_by: Vec::new(),
            limit: None,
            allow_filtering: false,
            restrictions: None,
            ann_clause: None,
            page_size: None,
            paging_state: None,
        });

        let QueryResult::Rows { rows, .. } = executor.execute(&plan, None).unwrap() else {
            panic!("expected rows");
        };
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Some(7i32.to_be_bytes().to_vec()));
        assert_eq!(rows[0][1], Some(b"indexed".to_vec()));
    }

    #[test]
    fn sai_indexed_select_applies_non_tokenizing_analyzer() {
        let (executor, _temp) = test_executor();

        executor
            .execute(
                &QueryPlan::CreateIndex(CreateIndexPlan {
                    keyspace: "ks".to_string(),
                    table: "events".to_string(),
                    index_name: "events_name_sai_idx".to_string(),
                    column: "name".to_string(),
                    kind: "custom".to_string(),
                    custom_class: Some(
                        "org.apache.cassandra.index.sai.StorageAttachedIndex".to_string(),
                    ),
                    options: std::collections::HashMap::from([(
                        "case_sensitive".to_string(),
                        "false".to_string(),
                    )]),
                    if_not_exists: false,
                }),
                None,
            )
            .unwrap();
        insert_event(&executor, 8, 1, "MiXeD");

        let plan = QueryPlan::Select(SelectPlan {
            keyspace: "ks".to_string(),
            table: "events".to_string(),
            columns: SelectColumns::Named(vec![
                Selector::Column("id".to_string()),
                Selector::Column("name".to_string()),
            ]),
            distinct: false,
            json: false,
            where_clause: vec![Relation {
                column: "name".to_string(),
                op: RelationOp::Eq,
                value: text_term("mixed"),
            }],
            group_by: Vec::new(),
            order_by: Vec::new(),
            limit: None,
            allow_filtering: false,
            restrictions: None,
            ann_clause: None,
            page_size: None,
            paging_state: None,
        });

        let QueryResult::Rows { rows, .. } = executor.execute(&plan, None).unwrap() else {
            panic!("expected rows");
        };
        assert_eq!(
            rows,
            vec![vec![
                Some(8i32.to_be_bytes().to_vec()),
                Some(b"MiXeD".to_vec())
            ]]
        );
    }

    #[test]
    fn ann_select_projects_partition_key_columns() {
        let (executor, _temp) = test_executor();

        executor
            .execute(
                &QueryPlan::CreateTable(CreateTablePlan {
                    keyspace: "ks".to_string(),
                    name: "items".to_string(),
                    if_not_exists: false,
                    columns: vec![
                        cassandra_cql::planner::ResolvedColumnDef {
                            name: "id".to_string(),
                            cql_type: CqlType::Int,
                            is_static: false,
                            masked_with: None,
                            constraints: Vec::new(),
                        },
                        cassandra_cql::planner::ResolvedColumnDef {
                            name: "embedding".to_string(),
                            cql_type: CqlType::Vector(Box::new(CqlType::Float), 3),
                            is_static: false,
                            masked_with: None,
                            constraints: Vec::new(),
                        },
                    ],
                    partition_key: vec!["id".to_string()],
                    clustering_key: Vec::new(),
                    clustering_order: Vec::new(),
                    options: Default::default(),
                }),
                None,
            )
            .unwrap();

        executor
            .execute(
                &QueryPlan::CreateIndex(CreateIndexPlan {
                    keyspace: "ks".to_string(),
                    table: "items".to_string(),
                    index_name: "items_embedding_sai_idx".to_string(),
                    column: "embedding".to_string(),
                    kind: "custom".to_string(),
                    custom_class: Some(
                        "org.apache.cassandra.index.sai.StorageAttachedIndex".to_string(),
                    ),
                    options: std::collections::HashMap::from([
                        ("vector_dimensions".to_string(), "3".to_string()),
                        (
                            "vector_similarity_metric".to_string(),
                            "dot_product".to_string(),
                        ),
                    ]),
                    if_not_exists: false,
                }),
                None,
            )
            .unwrap();

        for (id, x) in [(1, 0.1), (2, 0.9)] {
            executor
                .execute(
                    &QueryPlan::Insert(InsertPlan {
                        keyspace: "ks".to_string(),
                        table: "items".to_string(),
                        columns: vec!["id".to_string(), "embedding".to_string()],
                        values: vec![
                            int_term(id),
                            Term::CollectionLiteral(vec![
                                Term::Literal(Literal::Float(x)),
                                Term::Literal(Literal::Float(0.0)),
                                Term::Literal(Literal::Float(0.0)),
                            ]),
                        ],
                        if_not_exists: false,
                        json: None,
                        json_default: JsonDefault::Null,
                        using_timestamp: None,
                        using_ttl: None,
                    }),
                    None,
                )
                .unwrap();
        }

        let plan = QueryPlan::Select(SelectPlan {
            keyspace: "ks".to_string(),
            table: "items".to_string(),
            columns: SelectColumns::Named(vec![Selector::Column("id".to_string())]),
            distinct: false,
            json: false,
            where_clause: Vec::new(),
            group_by: Vec::new(),
            order_by: Vec::new(),
            limit: None,
            allow_filtering: false,
            restrictions: None,
            ann_clause: Some(cassandra_cql::planner::AnnClause {
                column: "embedding".to_string(),
                vector_literal: vec![1.0, 0.0, 0.0],
                top_k: 1,
            }),
            page_size: None,
            paging_state: None,
        });

        let QueryResult::Rows { rows, .. } = executor.execute(&plan, None).unwrap() else {
            panic!("expected rows");
        };
        assert_eq!(rows, vec![vec![Some(2i32.to_be_bytes().to_vec())]]);
    }

    #[test]
    fn count_star_scans_all_partitions() {
        let (executor, _temp) = test_executor();
        insert_event(&executor, 1, 1, "a");
        insert_event(&executor, 1, 2, "b");
        insert_event(&executor, 2, 1, "c");

        let plan = QueryPlan::Select(SelectPlan {
            keyspace: "ks".to_string(),
            table: "events".to_string(),
            columns: SelectColumns::Named(vec![Selector::Count]),
            distinct: false,
            json: false,
            where_clause: Vec::new(),
            group_by: Vec::new(),
            order_by: Vec::new(),
            limit: None,
            allow_filtering: false,
            restrictions: None,
            ann_clause: None,
            page_size: None,
            paging_state: None,
        });

        let QueryResult::Rows { rows, .. } = executor.execute(&plan, None).unwrap() else {
            panic!("expected rows");
        };
        let count = i64::from_be_bytes(rows[0][0].as_ref().unwrap().as_slice().try_into().unwrap());
        assert_eq!(count, 3);
    }

    #[test]
    fn sum_aggregate_scans_all_partitions() {
        let (executor, _temp) = test_executor();
        insert_event(&executor, 1, 1, "a");
        insert_event(&executor, 1, 2, "b");
        insert_event(&executor, 2, 4, "c");

        let plan = QueryPlan::Select(SelectPlan {
            keyspace: "ks".to_string(),
            table: "events".to_string(),
            columns: SelectColumns::Named(vec![Selector::Function(
                "sum".to_string(),
                vec![Selector::Column("bucket".to_string())],
            )]),
            distinct: false,
            json: false,
            where_clause: Vec::new(),
            group_by: Vec::new(),
            order_by: Vec::new(),
            limit: None,
            allow_filtering: false,
            restrictions: None,
            ann_clause: None,
            page_size: None,
            paging_state: None,
        });

        let QueryResult::Rows { columns, rows, .. } = executor.execute(&plan, None).unwrap() else {
            panic!("expected rows");
        };
        assert_eq!(columns[0].name, "sum(bucket)");
        assert_eq!(columns[0].cql_type, CqlType::Int);
        let sum = i32::from_be_bytes(rows[0][0].as_ref().unwrap().as_slice().try_into().unwrap());
        assert_eq!(sum, 7);
    }

    #[test]
    fn aggregate_executes_builtin_scalar_argument() {
        let (executor, _temp) = test_executor();
        insert_event(&executor, 1, -1, "a");
        insert_event(&executor, 1, -2, "b");
        insert_event(&executor, 2, -4, "c");

        let plan = QueryPlan::Select(SelectPlan {
            keyspace: "ks".to_string(),
            table: "events".to_string(),
            columns: SelectColumns::Named(vec![Selector::Function(
                "sum".to_string(),
                vec![Selector::Function(
                    "abs".to_string(),
                    vec![Selector::Column("bucket".to_string())],
                )],
            )]),
            distinct: false,
            json: false,
            where_clause: Vec::new(),
            group_by: Vec::new(),
            order_by: Vec::new(),
            limit: None,
            allow_filtering: false,
            restrictions: None,
            ann_clause: None,
            page_size: None,
            paging_state: None,
        });

        let QueryResult::Rows { columns, rows, .. } = executor.execute(&plan, None).unwrap() else {
            panic!("expected rows");
        };
        assert_eq!(columns[0].name, "sum(abs(bucket))");
        assert_eq!(columns[0].cql_type, CqlType::Int);
        let sum = i32::from_be_bytes(rows[0][0].as_ref().unwrap().as_slice().try_into().unwrap());
        assert_eq!(sum, 7);
    }

    #[test]
    fn builtin_sum_and_avg_preserve_java_argument_types() {
        assert_eq!(
            aggregate_output_type("sum", Some(&CqlType::Tinyint)),
            CqlType::Tinyint
        );
        assert_eq!(
            aggregate_output_type("avg", Some(&CqlType::Decimal)),
            CqlType::Decimal
        );

        let mut tiny_sum = SumAccumulator::new(CqlType::Tinyint);
        tiny_sum.add(&[120u8]).unwrap();
        tiny_sum.add(&[10u8]).unwrap();
        assert_eq!(tiny_sum.finalize().unwrap(), vec![130u8]);

        let mut int_avg = AvgAccumulator::new(CqlType::Int);
        int_avg.add(&5i32.to_be_bytes()).unwrap();
        int_avg.add(&2i32.to_be_bytes()).unwrap();
        assert_eq!(int_avg.finalize().unwrap(), 3i32.to_be_bytes().to_vec());
    }

    #[test]
    fn builtin_sum_and_avg_support_varint_and_decimal_states() {
        let mut varint_sum = SumAccumulator::new(CqlType::Varint);
        let huge = BigInt::parse_bytes(b"123456789012345678901234567890", 10)
            .unwrap()
            .to_signed_bytes_be();
        varint_sum.add(&huge).unwrap();
        varint_sum
            .add(&BigInt::from(10).to_signed_bytes_be())
            .unwrap();
        assert_eq!(
            BigInt::from_signed_bytes_be(&varint_sum.finalize().unwrap()).to_string(),
            "123456789012345678901234567900"
        );

        let mut decimal_avg = AvgAccumulator::new(CqlType::Decimal);
        decimal_avg
            .add(&cassandra_types::bigint::string_to_decimal("1.0").unwrap())
            .unwrap();
        decimal_avg
            .add(&cassandra_types::bigint::string_to_decimal("2.0").unwrap())
            .unwrap();
        assert_eq!(
            cassandra_types::bigint::decimal_to_string(&decimal_avg.finalize().unwrap()).unwrap(),
            "1.5"
        );
    }

    #[test]
    fn aggregate_executes_user_defined_scalar_argument() {
        let (executor, _temp) = test_executor();
        create_int_identity_function(&executor, "identity_bucket");
        insert_event(&executor, 1, 1, "a");
        insert_event(&executor, 1, 2, "b");
        insert_event(&executor, 2, 4, "c");

        let plan = QueryPlan::Select(SelectPlan {
            keyspace: "ks".to_string(),
            table: "events".to_string(),
            columns: SelectColumns::Named(vec![Selector::Function(
                "sum".to_string(),
                vec![Selector::Function(
                    "identity_bucket".to_string(),
                    vec![Selector::Column("bucket".to_string())],
                )],
            )]),
            distinct: false,
            json: false,
            where_clause: Vec::new(),
            group_by: Vec::new(),
            order_by: Vec::new(),
            limit: None,
            allow_filtering: false,
            restrictions: None,
            ann_clause: None,
            page_size: None,
            paging_state: None,
        });

        let QueryResult::Rows { columns, rows, .. } = executor.execute(&plan, None).unwrap() else {
            panic!("expected rows");
        };
        assert_eq!(columns[0].name, "sum(identity_bucket(bucket))");
        assert_eq!(columns[0].cql_type, CqlType::Int);
        let sum = i32::from_be_bytes(rows[0][0].as_ref().unwrap().as_slice().try_into().unwrap());
        assert_eq!(sum, 7);
    }

    #[test]
    fn user_defined_aggregate_executes_state_function() {
        let (executor, _temp) = test_executor();
        create_int_state_function(&executor, "plus", "int");
        executor
            .execute(
                &QueryPlan::CreateAggregate(CreateAggregatePlan {
                    keyspace: "ks".to_string(),
                    name: "sum_bucket".to_string(),
                    or_replace: false,
                    if_not_exists: false,
                    arg_types: vec!["int".to_string()],
                    sfunc: "plus".to_string(),
                    stype: "int".to_string(),
                    finalfunc: None,
                    initcond: Some("0".to_string()),
                }),
                None,
            )
            .unwrap();
        insert_event(&executor, 1, 1, "a");
        insert_event(&executor, 1, 2, "b");
        insert_event(&executor, 2, 4, "c");

        let plan = QueryPlan::Select(SelectPlan {
            keyspace: "ks".to_string(),
            table: "events".to_string(),
            columns: SelectColumns::Named(vec![Selector::Function(
                "sum_bucket".to_string(),
                vec![Selector::Column("bucket".to_string())],
            )]),
            distinct: false,
            json: false,
            where_clause: Vec::new(),
            group_by: Vec::new(),
            order_by: Vec::new(),
            limit: None,
            allow_filtering: false,
            restrictions: None,
            ann_clause: None,
            page_size: None,
            paging_state: None,
        });

        let QueryResult::Rows { columns, rows, .. } = executor.execute(&plan, None).unwrap() else {
            panic!("expected rows");
        };
        assert_eq!(columns[0].name, "sum_bucket(bucket)");
        assert_eq!(columns[0].cql_type, CqlType::Int);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Some(7i32.to_be_bytes().to_vec()));
    }

    #[test]
    fn user_defined_aggregate_executes_final_function() {
        let (executor, _temp) = test_executor();
        create_text_state_function(&executor, "keep_state");
        create_text_final_upper_function(&executor, "finish_upper");
        executor
            .execute(
                &QueryPlan::CreateAggregate(CreateAggregatePlan {
                    keyspace: "ks".to_string(),
                    name: "seed_name".to_string(),
                    or_replace: false,
                    if_not_exists: false,
                    arg_types: vec!["text".to_string()],
                    sfunc: "keep_state".to_string(),
                    stype: "text".to_string(),
                    finalfunc: Some("finish_upper".to_string()),
                    initcond: Some("'seed'".to_string()),
                }),
                None,
            )
            .unwrap();
        insert_event(&executor, 1, 1, "alice");
        insert_event(&executor, 2, 1, "bob");

        let plan = QueryPlan::Select(SelectPlan {
            keyspace: "ks".to_string(),
            table: "events".to_string(),
            columns: SelectColumns::Named(vec![Selector::Function(
                "seed_name".to_string(),
                vec![Selector::Column("name".to_string())],
            )]),
            distinct: false,
            json: false,
            where_clause: Vec::new(),
            group_by: Vec::new(),
            order_by: Vec::new(),
            limit: None,
            allow_filtering: false,
            restrictions: None,
            ann_clause: None,
            page_size: None,
            paging_state: None,
        });

        let QueryResult::Rows { columns, rows, .. } = executor.execute(&plan, None).unwrap() else {
            panic!("expected rows");
        };
        assert_eq!(columns[0].name, "seed_name(name)");
        assert_eq!(columns[0].cql_type, CqlType::Varchar);
        assert_eq!(rows, vec![vec![Some(b"SEED".to_vec())]]);
    }

    #[test]
    fn user_defined_aggregate_executes_multiple_arguments() {
        let (executor, _temp) = test_executor();
        register_sum_two_int_aggregate(&executor);
        insert_event(&executor, 1, 1, "a");
        insert_event(&executor, 1, 2, "b");
        insert_event(&executor, 2, 4, "c");

        let plan = QueryPlan::Select(SelectPlan {
            keyspace: "ks".to_string(),
            table: "events".to_string(),
            columns: SelectColumns::Named(vec![Selector::Function(
                "sum_pair".to_string(),
                vec![
                    Selector::Column("id".to_string()),
                    Selector::Column("bucket".to_string()),
                ],
            )]),
            distinct: false,
            json: false,
            where_clause: Vec::new(),
            group_by: Vec::new(),
            order_by: Vec::new(),
            limit: None,
            allow_filtering: false,
            restrictions: None,
            ann_clause: None,
            page_size: None,
            paging_state: None,
        });

        let QueryResult::Rows { columns, rows, .. } = executor.execute(&plan, None).unwrap() else {
            panic!("expected rows");
        };
        assert_eq!(columns[0].name, "sum_pair(id, bucket)");
        assert_eq!(columns[0].cql_type, CqlType::Int);
        assert_eq!(rows, vec![vec![Some(11i32.to_be_bytes().to_vec())]]);
    }

    #[test]
    fn user_defined_aggregate_requires_runtime_metadata() {
        let (executor, _temp) = test_executor();
        let aggregate = UserAggregate::new("ks", "schema_only_sum", "int", "plus")
            .with_arg_type("int")
            .with_return_type("int")
            .with_initcond("0");
        {
            let mut catalog = executor.catalog.write();
            let snapshot = catalog.snapshot();
            let keyspace = snapshot
                .keyspace("ks")
                .unwrap()
                .clone()
                .with_aggregate(aggregate);
            *catalog = catalog.with_keyspace(keyspace);
        }

        let plan = QueryPlan::Select(SelectPlan {
            keyspace: "ks".to_string(),
            table: "events".to_string(),
            columns: SelectColumns::Named(vec![Selector::Function(
                "schema_only_sum".to_string(),
                vec![Selector::Column("bucket".to_string())],
            )]),
            distinct: false,
            json: false,
            where_clause: Vec::new(),
            group_by: Vec::new(),
            order_by: Vec::new(),
            limit: None,
            allow_filtering: false,
            restrictions: None,
            ann_clause: None,
            page_size: None,
            paging_state: None,
        });

        let err = executor.execute(&plan, None).unwrap_err();
        assert!(matches!(
            err,
            ExecutorError::InvalidQuery(msg) if msg.contains("runtime metadata is unavailable")
        ));
    }

    #[test]
    fn group_by_partition_key_aggregates_rows() {
        let (executor, _temp) = test_executor();
        insert_event(&executor, 1, 1, "a");
        insert_event(&executor, 1, 2, "b");
        insert_event(&executor, 2, 4, "c");

        let plan = QueryPlan::Select(SelectPlan {
            keyspace: "ks".to_string(),
            table: "events".to_string(),
            columns: SelectColumns::Named(vec![
                Selector::Column("id".to_string()),
                Selector::Count,
                Selector::Function(
                    "sum".to_string(),
                    vec![Selector::Column("bucket".to_string())],
                ),
            ]),
            distinct: false,
            json: false,
            where_clause: Vec::new(),
            group_by: vec!["id".to_string()],
            order_by: Vec::new(),
            limit: None,
            allow_filtering: false,
            restrictions: None,
            ann_clause: None,
            page_size: None,
            paging_state: None,
        });

        let QueryResult::Rows { rows, .. } = executor.execute(&plan, None).unwrap() else {
            panic!("expected rows");
        };

        assert_eq!(rows.len(), 2);
        let first_id =
            i32::from_be_bytes(rows[0][0].as_ref().unwrap().as_slice().try_into().unwrap());
        let first_count =
            i64::from_be_bytes(rows[0][1].as_ref().unwrap().as_slice().try_into().unwrap());
        let first_sum =
            i32::from_be_bytes(rows[0][2].as_ref().unwrap().as_slice().try_into().unwrap());
        let second_id =
            i32::from_be_bytes(rows[1][0].as_ref().unwrap().as_slice().try_into().unwrap());
        let second_count =
            i64::from_be_bytes(rows[1][1].as_ref().unwrap().as_slice().try_into().unwrap());
        let second_sum =
            i32::from_be_bytes(rows[1][2].as_ref().unwrap().as_slice().try_into().unwrap());

        assert_eq!((first_id, first_count, first_sum), (1, 2, 3));
        assert_eq!((second_id, second_count, second_sum), (2, 1, 4));
    }

    #[test]
    fn group_by_uses_clear_values_before_masking_projection() {
        let (executor, _temp) = test_executor();
        insert_event(&executor, 1, 1, "a");
        insert_event(&executor, 1, 2, "b");
        insert_event(&executor, 2, 4, "c");
        mask_event_column(&executor, "id", "mask_default", Vec::new());

        let plan = QueryPlan::Select(SelectPlan {
            keyspace: "ks".to_string(),
            table: "events".to_string(),
            columns: SelectColumns::Named(vec![
                Selector::Column("id".to_string()),
                Selector::Count,
                Selector::Function(
                    "sum".to_string(),
                    vec![Selector::Column("bucket".to_string())],
                ),
            ]),
            distinct: false,
            json: false,
            where_clause: Vec::new(),
            group_by: vec!["id".to_string()],
            order_by: Vec::new(),
            limit: None,
            allow_filtering: false,
            restrictions: None,
            ann_clause: None,
            page_size: None,
            paging_state: None,
        });

        let QueryResult::Rows { rows, .. } = executor.execute(&plan, None).unwrap() else {
            panic!("expected rows");
        };

        assert_eq!(rows.len(), 2);
        let first_id =
            i32::from_be_bytes(rows[0][0].as_ref().unwrap().as_slice().try_into().unwrap());
        let first_count =
            i64::from_be_bytes(rows[0][1].as_ref().unwrap().as_slice().try_into().unwrap());
        let first_sum =
            i32::from_be_bytes(rows[0][2].as_ref().unwrap().as_slice().try_into().unwrap());
        let second_id =
            i32::from_be_bytes(rows[1][0].as_ref().unwrap().as_slice().try_into().unwrap());
        let second_count =
            i64::from_be_bytes(rows[1][1].as_ref().unwrap().as_slice().try_into().unwrap());
        let second_sum =
            i32::from_be_bytes(rows[1][2].as_ref().unwrap().as_slice().try_into().unwrap());

        assert_eq!((first_id, first_count, first_sum), (0, 2, 3));
        assert_eq!((second_id, second_count, second_sum), (0, 1, 4));
    }

    #[test]
    fn paged_select_applies_masks_on_every_page() {
        let (executor, _temp) = test_executor();
        insert_event(&executor, 1, 1, "a");
        insert_event(&executor, 2, 2, "b");
        insert_event(&executor, 3, 3, "c");
        mask_event_column(&executor, "id", "mask_default", Vec::new());
        mask_event_column(&executor, "bucket", "mask_default", Vec::new());
        mask_event_column(
            &executor,
            "name",
            "mask_replace",
            vec![text_term("REDACTED")],
        );

        let mut paging_state = None;
        let mut rows_seen = 0;
        loop {
            let plan = QueryPlan::Select(SelectPlan {
                keyspace: "ks".to_string(),
                table: "events".to_string(),
                columns: SelectColumns::Named(vec![
                    Selector::Column("id".to_string()),
                    Selector::Column("bucket".to_string()),
                    Selector::Column("name".to_string()),
                ]),
                distinct: false,
                json: false,
                where_clause: Vec::new(),
                group_by: Vec::new(),
                order_by: Vec::new(),
                limit: None,
                allow_filtering: false,
                restrictions: None,
                ann_clause: None,
                page_size: Some(1),
                paging_state: paging_state.clone(),
            });

            let QueryResult::Rows {
                rows,
                paging_state: next_state,
                ..
            } = executor.execute(&plan, None).unwrap()
            else {
                panic!("expected rows");
            };
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0][0], Some(0i32.to_be_bytes().to_vec()));
            assert_eq!(rows[0][1], Some(0i32.to_be_bytes().to_vec()));
            assert_eq!(rows[0][2], Some(b"REDACTED".to_vec()));
            rows_seen += 1;

            paging_state = next_state;
            if paging_state.is_none() {
                break;
            }
        }
        assert_eq!(rows_seen, 3);
    }

    #[test]
    fn order_by_desc_limit_uses_clear_clustering_values_before_masking() {
        let (executor, _temp) = test_executor();
        insert_event(&executor, 1, 1, "first");
        insert_event(&executor, 1, 2, "middle");
        insert_event(&executor, 1, 3, "last");
        mask_event_column(&executor, "bucket", "mask_default", Vec::new());

        let plan = QueryPlan::Select(SelectPlan {
            keyspace: "ks".to_string(),
            table: "events".to_string(),
            columns: SelectColumns::Named(vec![
                Selector::Column("id".to_string()),
                Selector::Column("bucket".to_string()),
                Selector::Column("name".to_string()),
            ]),
            distinct: false,
            json: false,
            where_clause: vec![Relation {
                column: "id".to_string(),
                op: RelationOp::Eq,
                value: int_term(1),
            }],
            group_by: Vec::new(),
            order_by: vec![("bucket".to_string(), AstClusteringOrder::Desc)],
            limit: Some(int_term(1)),
            allow_filtering: false,
            restrictions: None,
            ann_clause: None,
            page_size: None,
            paging_state: None,
        });

        let QueryResult::Rows { rows, .. } = executor.execute(&plan, None).unwrap() else {
            panic!("expected rows");
        };
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][1], Some(0i32.to_be_bytes().to_vec()));
        assert_eq!(rows[0][2], Some(b"last".to_vec()));
    }

    #[test]
    fn keyed_select_projects_primary_key_columns() {
        let (executor, _temp) = test_executor();
        insert_event(&executor, 7, 3, "a");

        let plan = QueryPlan::Select(SelectPlan {
            keyspace: "ks".to_string(),
            table: "events".to_string(),
            columns: SelectColumns::Named(vec![
                Selector::Column("id".to_string()),
                Selector::Column("bucket".to_string()),
                Selector::Column("name".to_string()),
            ]),
            distinct: false,
            json: false,
            where_clause: vec![Relation {
                column: "id".to_string(),
                op: RelationOp::Eq,
                value: int_term(7),
            }],
            group_by: Vec::new(),
            order_by: Vec::new(),
            limit: None,
            allow_filtering: false,
            restrictions: None,
            ann_clause: None,
            page_size: None,
            paging_state: None,
        });

        let QueryResult::Rows { rows, .. } = executor.execute(&plan, None).unwrap() else {
            panic!("expected rows");
        };
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Some(7i32.to_be_bytes().to_vec()));
        assert_eq!(rows[0][1], Some(3i32.to_be_bytes().to_vec()));
        assert_eq!(rows[0][2], Some(b"a".to_vec()));
    }
}
