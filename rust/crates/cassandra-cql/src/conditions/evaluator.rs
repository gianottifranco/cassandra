// Licensed under Apache License, Version 2.0.

//! Condition evaluator for LWT IF clauses.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.cql3.conditions.ColumnCondition`

use crate::ast::{Literal, Relation, RelationOp, Term};
use cassandra_types::CqlType;
use cassandra_types::codec::CqlValue;
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
                };
            }
        };

        for condition in conditions {
            let col_idx = match columns.get(&condition.column) {
                Some(idx) => *idx,
                None => {
                    return ConditionResult {
                        applied: false,
                        current_values: Some(row.to_vec()),
                    };
                }
            };

            let current_value = row.get(col_idx).and_then(|v| v.as_deref());
            let cql_type = column_types.get(&condition.column);
            let matches = if let Term::CollectionElement { key, value } = &condition.value {
                eval_collection_element_condition(condition.op, current_value, key, value, cql_type)
            } else if condition.op == RelationOp::In {
                eval_in_condition(current_value, &condition.value)
            } else if matches!(condition.op, RelationOp::Contains | RelationOp::ContainsKey) {
                eval_collection_contains_condition(
                    condition.op,
                    current_value,
                    &condition.value,
                    cql_type,
                )
            } else {
                let expected = cql_type
                    .and_then(|typ| term_to_typed_bytes(&condition.value, typ))
                    .or_else(|| term_to_bytes(&condition.value));
                eval_single_condition(condition.op, current_value, expected.as_deref(), cql_type)
            };

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
        RelationOp::In => match (current, expected) {
            (Some(a), Some(b)) => a == b,
            _ => false,
        },
        _ => false,
    }
}

fn eval_collection_element_condition(
    op: RelationOp,
    current: Option<&[u8]>,
    key: &Term,
    expected: &Term,
    cql_type: Option<&CqlType>,
) -> bool {
    let (Some(current), Some(cql_type)) = (current, cql_type) else {
        return false;
    };

    match cql_type {
        CqlType::List(inner, _) => {
            let Some(index) = collection_index(key) else {
                return false;
            };
            let Ok(CqlValue::List(values)) = CqlValue::deserialize_value(cql_type, current) else {
                return false;
            };
            let Some(actual) = values.get(index).map(CqlValue::serialize_value) else {
                return false;
            };
            let expected = term_to_typed_bytes(expected, inner).or_else(|| term_to_bytes(expected));
            eval_single_condition(op, Some(&actual), expected.as_deref(), Some(inner))
        }
        CqlType::Map(key_type, value_type, _) => {
            let Some(key_value) = term_to_cql_value(key, key_type) else {
                return false;
            };
            let Ok(CqlValue::Map(entries)) = CqlValue::deserialize_value(cql_type, current) else {
                return false;
            };
            let Some((_, actual_value)) = entries
                .iter()
                .find(|(candidate, _)| candidate == &key_value)
            else {
                return false;
            };
            let actual = actual_value.serialize_value();
            let expected =
                term_to_typed_bytes(expected, value_type).or_else(|| term_to_bytes(expected));
            eval_single_condition(op, Some(&actual), expected.as_deref(), Some(value_type))
        }
        _ => false,
    }
}

fn eval_collection_contains_condition(
    op: RelationOp,
    current: Option<&[u8]>,
    expected: &Term,
    cql_type: Option<&CqlType>,
) -> bool {
    let (Some(current), Some(cql_type)) = (current, cql_type) else {
        return false;
    };

    match (op, cql_type) {
        (RelationOp::Contains, CqlType::List(inner, _) | CqlType::Set(inner, _)) => {
            let Some(expected) =
                term_to_typed_bytes(expected, inner).or_else(|| term_to_bytes(expected))
            else {
                return false;
            };
            let Ok(value) = CqlValue::deserialize_value(cql_type, current) else {
                return false;
            };
            match value {
                CqlValue::List(values) | CqlValue::Set(values) => values
                    .iter()
                    .any(|candidate| candidate.serialize_value() == expected),
                _ => false,
            }
        }
        (RelationOp::Contains, CqlType::Map(_, value_type, _)) => {
            let Some(expected) =
                term_to_typed_bytes(expected, value_type).or_else(|| term_to_bytes(expected))
            else {
                return false;
            };
            let Ok(CqlValue::Map(entries)) = CqlValue::deserialize_value(cql_type, current) else {
                return false;
            };
            entries
                .iter()
                .any(|(_, value)| value.serialize_value() == expected)
        }
        (RelationOp::ContainsKey, CqlType::Map(key_type, _, _)) => {
            let Some(expected) = term_to_cql_value(expected, key_type) else {
                return false;
            };
            let Ok(CqlValue::Map(entries)) = CqlValue::deserialize_value(cql_type, current) else {
                return false;
            };
            entries.iter().any(|(key, _)| key == &expected)
        }
        _ => false,
    }
}

fn collection_index(term: &Term) -> Option<usize> {
    match term {
        Term::Literal(Literal::Integer(value)) => usize::try_from(*value).ok(),
        _ => None,
    }
}

fn eval_in_condition(current: Option<&[u8]>, term: &Term) -> bool {
    let Some(current) = current else {
        return false;
    };
    term_to_byte_list(term)
        .into_iter()
        .flatten()
        .any(|candidate| candidate.as_slice() == current)
}

fn term_to_byte_list(term: &Term) -> Vec<Option<Vec<u8>>> {
    match term {
        Term::CollectionLiteral(values) | Term::TupleLiteral(values) => {
            values.iter().map(term_to_bytes).collect()
        }
        _ => vec![term_to_bytes(term)],
    }
}

fn term_to_typed_bytes(term: &Term, cql_type: &CqlType) -> Option<Vec<u8>> {
    term_to_cql_value(term, cql_type).map(|value| value.serialize_value())
}

fn term_to_cql_value(term: &Term, cql_type: &CqlType) -> Option<CqlValue> {
    match (term, cql_type) {
        (Term::Literal(Literal::Null), _) => Some(CqlValue::Null),
        (Term::Literal(Literal::String(value)), CqlType::Ascii) => {
            Some(CqlValue::Ascii(value.clone()))
        }
        (Term::Literal(Literal::String(value)), CqlType::Varchar) => {
            Some(CqlValue::Varchar(value.clone()))
        }
        (Term::Literal(Literal::Integer(value)), CqlType::Bigint) => Some(CqlValue::Bigint(*value)),
        (Term::Literal(Literal::Integer(value)), CqlType::Counter) => {
            Some(CqlValue::Counter(*value))
        }
        (Term::Literal(Literal::Integer(value)), CqlType::Timestamp) => {
            Some(CqlValue::Timestamp(*value))
        }
        (Term::Literal(Literal::Integer(value)), CqlType::Time) => Some(CqlValue::Time(*value)),
        (Term::Literal(Literal::Integer(value)), CqlType::Int) => {
            Some(CqlValue::Int(i32::try_from(*value).ok()?))
        }
        (Term::Literal(Literal::Integer(value)), CqlType::Smallint) => {
            Some(CqlValue::Smallint(i16::try_from(*value).ok()?))
        }
        (Term::Literal(Literal::Integer(value)), CqlType::Tinyint) => {
            Some(CqlValue::Tinyint(i8::try_from(*value).ok()?))
        }
        (Term::Literal(Literal::Integer(value)), CqlType::Float) => {
            Some(CqlValue::Float(*value as f32))
        }
        (Term::Literal(Literal::Integer(value)), CqlType::Double) => {
            Some(CqlValue::Double(*value as f64))
        }
        (Term::Literal(Literal::Float(value)), CqlType::Float) => {
            Some(CqlValue::Float(*value as f32))
        }
        (Term::Literal(Literal::Float(value)), CqlType::Double) => Some(CqlValue::Double(*value)),
        (Term::Literal(Literal::Boolean(value)), CqlType::Boolean) => {
            Some(CqlValue::Boolean(*value))
        }
        (Term::Literal(Literal::Blob(value)), CqlType::Blob) => Some(CqlValue::Blob(value.clone())),
        (Term::Literal(Literal::Uuid(value)), CqlType::Uuid) => uuid::Uuid::parse_str(value)
            .ok()
            .map(|uuid| CqlValue::Uuid(*uuid.as_bytes())),
        (Term::Literal(Literal::Uuid(value)), CqlType::Timeuuid) => uuid::Uuid::parse_str(value)
            .ok()
            .map(|uuid| CqlValue::Timeuuid(*uuid.as_bytes())),
        _ => None,
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

        let result = ConditionEvaluator::eval_conditions(&conditions, &columns, &types, Some(&row));
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

        let result = ConditionEvaluator::eval_conditions(&conditions, &columns, &types, Some(&row));
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

        let result = ConditionEvaluator::eval_conditions(&conditions, &columns, &types, Some(&row));
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

        let result = ConditionEvaluator::eval_conditions(&conditions, &columns, &types, Some(&row));
        assert!(result.applied);
    }

    #[test]
    fn condition_in_collection_matches_member() {
        let mut columns = HashMap::new();
        columns.insert("age".to_string(), 0);
        let mut types = HashMap::new();
        types.insert("age".to_string(), CqlType::Bigint);

        let row = vec![Some(int_bytes(30))];
        let conditions = vec![Relation {
            column: "age".to_string(),
            op: RelationOp::In,
            value: Term::CollectionLiteral(vec![
                Term::Literal(Literal::Integer(25)),
                Term::Literal(Literal::Integer(30)),
                Term::Literal(Literal::Integer(35)),
            ]),
        }];

        let result = ConditionEvaluator::eval_conditions(&conditions, &columns, &types, Some(&row));
        assert!(result.applied);
    }

    #[test]
    fn condition_in_collection_rejects_missing_member() {
        let mut columns = HashMap::new();
        columns.insert("age".to_string(), 0);
        let mut types = HashMap::new();
        types.insert("age".to_string(), CqlType::Bigint);

        let row = vec![Some(int_bytes(30))];
        let conditions = vec![Relation {
            column: "age".to_string(),
            op: RelationOp::In,
            value: Term::CollectionLiteral(vec![
                Term::Literal(Literal::Integer(10)),
                Term::Literal(Literal::Integer(20)),
            ]),
        }];

        let result = ConditionEvaluator::eval_conditions(&conditions, &columns, &types, Some(&row));
        assert!(!result.applied);
    }

    #[test]
    fn collection_element_conditions_match_map_and_list_values() {
        let mut columns = HashMap::new();
        columns.insert("attrs".to_string(), 0);
        columns.insert("scores".to_string(), 1);
        let mut types = HashMap::new();
        types.insert(
            "attrs".to_string(),
            CqlType::Map(
                Box::new(CqlType::Varchar),
                Box::new(CqlType::Varchar),
                false,
            ),
        );
        types.insert(
            "scores".to_string(),
            CqlType::List(Box::new(CqlType::Int), false),
        );

        let row = vec![
            Some(
                CqlValue::Map(vec![(
                    CqlValue::Varchar("state".to_string()),
                    CqlValue::Varchar("open".to_string()),
                )])
                .serialize_value(),
            ),
            Some(CqlValue::List(vec![CqlValue::Int(3), CqlValue::Int(7)]).serialize_value()),
        ];
        let conditions = vec![
            Relation {
                column: "attrs".to_string(),
                op: RelationOp::Eq,
                value: Term::CollectionElement {
                    key: Box::new(Term::Literal(Literal::String("state".to_string()))),
                    value: Box::new(Term::Literal(Literal::String("open".to_string()))),
                },
            },
            Relation {
                column: "scores".to_string(),
                op: RelationOp::Eq,
                value: Term::CollectionElement {
                    key: Box::new(Term::Literal(Literal::Integer(1))),
                    value: Box::new(Term::Literal(Literal::Integer(7))),
                },
            },
        ];

        let result = ConditionEvaluator::eval_conditions(&conditions, &columns, &types, Some(&row));
        assert!(result.applied);
    }

    #[test]
    fn collection_contains_conditions_match_values_and_map_keys() {
        let mut columns = HashMap::new();
        columns.insert("tags".to_string(), 0);
        columns.insert("attrs".to_string(), 1);
        let mut types = HashMap::new();
        types.insert(
            "tags".to_string(),
            CqlType::Set(Box::new(CqlType::Varchar), false),
        );
        types.insert(
            "attrs".to_string(),
            CqlType::Map(Box::new(CqlType::Varchar), Box::new(CqlType::Int), false),
        );

        let row = vec![
            Some(
                CqlValue::Set(vec![
                    CqlValue::Varchar("hot".to_string()),
                    CqlValue::Varchar("ready".to_string()),
                ])
                .serialize_value(),
            ),
            Some(
                CqlValue::Map(vec![(
                    CqlValue::Varchar("count".to_string()),
                    CqlValue::Int(3),
                )])
                .serialize_value(),
            ),
        ];
        let conditions = vec![
            Relation {
                column: "tags".to_string(),
                op: RelationOp::Contains,
                value: Term::Literal(Literal::String("ready".to_string())),
            },
            Relation {
                column: "attrs".to_string(),
                op: RelationOp::ContainsKey,
                value: Term::Literal(Literal::String("count".to_string())),
            },
            Relation {
                column: "attrs".to_string(),
                op: RelationOp::Contains,
                value: Term::Literal(Literal::Integer(3)),
            },
        ];

        let result = ConditionEvaluator::eval_conditions(&conditions, &columns, &types, Some(&row));
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

        let result = ConditionEvaluator::eval_conditions(&conditions, &columns, &types, Some(&row));
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

        let result = ConditionEvaluator::eval_conditions(&conditions, &columns, &types, Some(&row));
        assert!(!result.applied);
    }
}
