// Licensed under Apache License, Version 2.0.

//! WHERE clause restriction validation.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.cql3.restrictions.StatementRestrictions`
//! - `org.apache.cassandra.cql3.restrictions.PartitionKeyRestrictions`
//! - `org.apache.cassandra.cql3.restrictions.ClusteringColumnRestrictions`

pub mod clustering;
pub mod partition_key;
pub mod statement_restrictions;
pub mod type_compat;

use crate::ast::RelationOp;
use std::fmt;

/// The kind of restriction on a column.
#[derive(Debug, Clone, PartialEq)]
pub enum RestrictionKind {
    Eq,
    In,
    Range { op: RelationOp },
    Contains,
    ContainsKey,
    Like,
    IsNotNull,
    TokenBased,
}

/// A validated restriction on a single column.
#[derive(Debug, Clone)]
pub struct ColumnRestriction {
    pub column_name: String,
    pub kind: RestrictionKind,
}

/// Classified restrictions for a query.
#[derive(Debug, Clone, Default)]
pub struct RestrictionSet {
    pub partition_key_restrictions: Vec<ColumnRestriction>,
    pub clustering_restrictions: Vec<ColumnRestriction>,
    pub non_key_restrictions: Vec<ColumnRestriction>,
    pub needs_filtering: bool,
    pub is_token_based: bool,
}

/// Restriction validation error.
#[derive(Debug, Clone)]
pub enum RestrictionError {
    /// Missing partition key restrictions.
    MissingPartitionKey(String),
    /// Invalid clustering restriction order.
    InvalidClusteringOrder(String),
    /// Operator not compatible with column type.
    IncompatibleOperator(String),
    /// Secondary index required.
    NeedsFiltering(String),
    /// General restriction error.
    Invalid(String),
}

impl fmt::Display for RestrictionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RestrictionError::MissingPartitionKey(m) => write!(f, "{}", m),
            RestrictionError::InvalidClusteringOrder(m) => write!(f, "{}", m),
            RestrictionError::IncompatibleOperator(m) => write!(f, "{}", m),
            RestrictionError::NeedsFiltering(m) => write!(f, "{}", m),
            RestrictionError::Invalid(m) => write!(f, "{}", m),
        }
    }
}

impl std::error::Error for RestrictionError {}

pub(crate) fn tuple_columns(column: &str) -> Option<Vec<String>> {
    let trimmed = column.trim();
    let inner = trimmed.strip_prefix('(')?.strip_suffix(')')?;
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
