// Licensed under Apache License, Version 2.0.

//! LWT IF condition evaluation.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.cql3.conditions.ColumnCondition`

pub mod evaluator;
pub mod statement;

pub use evaluator::{ConditionEvaluator, ConditionResult};
pub use statement::{ConditionStatement, VariableCondition};
