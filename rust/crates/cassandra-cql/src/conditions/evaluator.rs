// Licensed under Apache License, Version 2.0.

//! Condition evaluator for LWT IF clauses.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.cql3.conditions.ColumnCondition`

use crate::ast::{Literal, Relation, RelationOp, Term};
use cassandra_types::CqlType;
use std::collections::HashMap;

/// Result of evaluating IF conditions.
#[derive(Debug, Clone)]
pub struct ConditionResult {
    /// Whether the operation was applied.
    pub applied: bool,
    /// Current row values (for the `[applied]` result set).
    /// None if the row doesn't exist.
    pub current_values: Option<Vec<Option<Vec<u8>>>>,
}

/// Evaluates IF conditions for LWT operations.
pub struct ConditionEvaluator;

impl ConditionEvaluator {
    /// Evaluate IF EXISTS: the operation is applied only if the row exists.
    pub fn eval_if_exists(row_exists: bool) -> ConditionResult {
        ConditionResult {
            applied: row_exists,
            current_values: None,
        }
    }

    /// Evaluate IF NOT EXISTS: the operation is applied only if the row doesn't exist.
    pub fn eval_if_not_exists(
        row_exists: bool,
        current_values: Option<Vec<Option<Vec<u8>>>>,
    ) -> ConditionResult {
        ConditionResult {
            applied: !row_exists,
            current_values: if row_exists { current_values } else { None },
        }
    }

    /// Evaluate column-level IF conditions.
    ///
    /// `conditions` are the IF clause relations.
    /// `columns` maps column names to indices in `current_row`.
    /// `column_types` maps column names to their CQL types.
    /// `current_row` is the current row data (None if row doesn't exist).
    pub fn eval_conditions(
        conditions: &[Relation],
        columns: &HashMap<String, usize>,
        column_types: &HashMap<String, CqlType>,
        current_row: Option<&[Option<Vec<u8>>]>,
    ) -> ConditionResult {
        let row = match current_row {
            Some(r) => r,
            None => {
                return ConditionResult {
                    applied: false,
                    current_values: None,
                }
            }
        };

        for condition in conditions {
            let col_idx = match columns.get(&condition.column) {
                Some(idx) => *idx,
                None => {
                    return ConditionResult {
                        applied: false,
                        current_values: Some(row.to_vec()),
                    }
                }
            };

            let current_value = row.get(col_idx).and_then(|v| v.as_deref());
            let cql_type = column_types.get(&condition.column);
            let expected = term_to_bytes(&condition.value);

            let matches = eval_single_condition(
                condition.op,
                current_value,
                expected.as_deref(),
                cql_type,
            );

            if !matches {
                return ConditionResult {
                    applied: false,
                    current_values: Some(row.to_vec()),
                };
            }
        }

        ConditionResult {
            applied: true,
            current_values: Some(row.to_vec()),
        }
    }
}

fn eval_single_condition(
    op: RelationOp,
    current: Option<&[u8]>,
    expected: Option<&[u8]>,
    cql_type: Option<&CqlType>,
) -> bool {
    match op {
        RelationOp::Eq => match (current, expected) {
            (None, None) => true,
            (Some(a), Some(b)) => a == b,
            _ => false,
        },
        RelationOp::Neq => match (current, expected) {
            (None, None) => false,
            (Some(a), Some(b)) => a != b,
            _ => true,
        },
        RelationOp::Lt | RelationOp::Gt | RelationOp::Lte | RelationOp::Gte => {
            match (current, expected) {
                (Some(a), Some(b)) => {
                    let default_type = CqlType::Blob;
                    let t = cql_type.unwrap_or(&default_type);
                    let ord = cassandra_types::comparator::compare_bytes(t, a, b);
                    match op {
                        RelationOp::Lt => ord == std::cmp::Ordering::Less,
                        RelationOp::Gt => ord == std::cmp::Ordering::Greater,
                        RelationOp::Lte => {
                            ord == std::cmp::Ordering::Less || ord == std::cmp::Ordering::Equal
                        }
                        RelationOp::Gte => {
                            ord == std::cmp::Ordering::Greater || ord == std::cmp::Ordering::Equal
                        }
                        _ => unreachable!(),
                    }
                }
                _ => false,
            }
        }
        RelationOp::In => {
            // For IN, the expected value should be a list of values.
            // Simplified: just do equality check for now.
            match (current, expected) {
                (Some(a), Some(b)) => a == b,
                _ => false,
            }
        }
        _ => false, // Contains, ContainsKey not used in IF conditions
    }
}

fn term_to_bytes(term: &Term) -> Option<Vec<u8>> {
    match term {
        Term::Literal(lit) => match lit {
            Literal::Integer(v) => Some(v.to_be_bytes().to_vec()),
            Literal::Float(v) => Some(v.to_be_bytes().to_vec()),
            Literal::String(s) => Some(s.as_bytes().to_vec()),
            Literal::Boolean(b) => Some(vec![if *b { 1 } else { 0 }]),
            Literal::Blob(b) => Some(b.clone()),
            Literal::Uuid(s) => uuid::Uuid::parse_str(s).ok().map(|u| u.as_bytes().to_vec()),
            Literal::Null => None,
        },
        _ => None, // bind markers and function calls need runtime resolution
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn int_bytes(v: i64) -> Vec<u8> {
        v.to_be_bytes().to_vec()
    }

    #[test]
    fn if_exists_row_present() {
        let result = ConditionEvaluator::eval_if_exists(true);
        assert!(result.applied);
    }

    #[test]
    fn if_exists_row_absent() {
        let result = ConditionEvaluator::eval_if_exists(false);
        assert!(!result.applied);
    }

    #[test]
    fn if_not_exists_row_absent() {
        let result = ConditionEvaluator::eval_if_not_exists(false, None);
        assert!(result.applied);
    }

    #[test]
    fn if_not_exists_row_present() {
        let current = vec![Some(int_bytes(42))];
        let result = ConditionEvaluator::eval_if_not_exists(true, Some(current));
        assert!(!result.applied);
    }

    #[test]
    fn condition_eq_match() {
        let mut columns = HashMap::new();
        columns.insert("age".to_string(), 0);
        let mut types = HashMap::new();
        types.insert("age".to_string(), CqlType::Bigint);

        let row = vec![Some(int_bytes(30))];
        let conditions = vec![Relation {
            column: "age".to_string(),
            op: RelationOp::Eq,
            value: Term::Literal(Literal::Integer(30)),
        }];

        let result =
            ConditionEvaluator::eval_conditions(&conditions, &columns, &types, Some(&row));
        assert!(result.applied);
    }

    #[test]
    fn condition_eq_no_match() {
        let mut columns = HashMap::new();
        columns.insert("age".to_string(), 0);
        let mut types = HashMap::new();
        types.insert("age".to_string(), CqlType::Bigint);

        let row = vec![Some(int_bytes(30))];
        let conditions = vec![Relation {
            column: "age".to_string(),
            op: RelationOp::Eq,
            value: Term::Literal(Literal::Integer(25)),
        }];

        let result =
            ConditionEvaluator::eval_conditions(&conditions, &columns, &types, Some(&row));
        assert!(!result.applied);
        assert!(result.current_values.is_some());
    }

    #[test]
    fn condition_neq() {
        let mut columns = HashMap::new();
        columns.insert("age".to_string(), 0);
        let mut types = HashMap::new();
        types.insert("age".to_string(), CqlType::Bigint);

        let row = vec![Some(int_bytes(30))];
        let conditions = vec![Relation {
            column: "age".to_string(),
            op: RelationOp::Neq,
            value: Term::Literal(Literal::Integer(25)),
        }];

        let result =
            ConditionEvaluator::eval_conditions(&conditions, &columns, &types, Some(&row));
        assert!(result.applied);
    }

    #[test]
    fn condition_gt() {
        let mut columns = HashMap::new();
        columns.insert("age".to_string(), 0);
        let mut types = HashMap::new();
        types.insert("age".to_string(), CqlType::Bigint);

        let row = vec![Some(int_bytes(30))];
        let conditions = vec![Relation {
            column: "age".to_string(),
            op: RelationOp::Gt,
            value: Term::Literal(Literal::Integer(25)),
        }];

        let result =
            ConditionEvaluator::eval_conditions(&conditions, &columns, &types, Some(&row));
        assert!(result.applied);
    }

    #[test]
    fn condition_no_row() {
        let mut columns = HashMap::new();
        columns.insert("age".to_string(), 0);
        let types = HashMap::new();

        let conditions = vec![Relation {
            column: "age".to_string(),
            op: RelationOp::Eq,
            value: Term::Literal(Literal::Integer(30)),
        }];

        let result = ConditionEvaluator::eval_conditions(&conditions, &columns, &types, None);
        assert!(!result.applied);
    }

    #[test]
    fn condition_null_eq_null() {
        let mut columns = HashMap::new();
        columns.insert("v".to_string(), 0);
        let types = HashMap::new();

        let row = vec![None]; // null value
        let conditions = vec![Relation {
            column: "v".to_string(),
            op: RelationOp::Eq,
            value: Term::Literal(Literal::Null),
        }];

        let result =
            ConditionEvaluator::eval_conditions(&conditions, &columns, &types, Some(&row));
        assert!(result.applied);
    }

    #[test]
    fn multiple_conditions_all_must_match() {
        let mut columns = HashMap::new();
        columns.insert("a".to_string(), 0);
        columns.insert("b".to_string(), 1);
        let mut types = HashMap::new();
        types.insert("a".to_string(), CqlType::Bigint);
        types.insert("b".to_string(), CqlType::Bigint);

        let row = vec![Some(int_bytes(10)), Some(int_bytes(20))];
        let conditions = vec![
            Relation {
                column: "a".to_string(),
                op: RelationOp::Eq,
                value: Term::Literal(Literal::Integer(10)),
            },
            Relation {
                column: "b".to_string(),
                op: RelationOp::Eq,
                value: Term::Literal(Literal::Integer(99)), // doesn't match
            },
        ];

        let result =
            ConditionEvaluator::eval_conditions(&conditions, &columns, &types, Some(&row));
        assert!(!result.applied);
    }
}
