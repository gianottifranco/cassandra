// Licensed under Apache License, Version 2.0.

//! WHERE clause bind variable resolution for SELECT/DELETE/UPDATE statements.
//!
//! Resolves [`Relation`] terms in WHERE clauses to typed byte arrays using
//! the column type information from the schema.  This bridges the gap between
//! the parsed AST and the storage engine's byte-oriented interface.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.cql3.restrictions.SingleColumnRestriction`
//! - `org.apache.cassandra.cql3.statements.SelectStatement.buildRestrictions`

use cassandra_cql::ast::{Relation, RelationOp, Term};
use cassandra_types::CqlType;

use crate::term_binding::typed_term_to_bytes;

/// A resolved WHERE restriction with typed byte values.
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedRestriction {
    pub column: String,
    pub op: RelationOp,
    pub value: ResolvedValue,
}

/// The resolved value(s) for a restriction.
#[derive(Debug, Clone, PartialEq)]
pub enum ResolvedValue {
    /// A single typed byte value.
    Single(Vec<u8>),
    /// An IN list of typed byte values.
    InList(Vec<Vec<u8>>),
    /// A bind marker that will be resolved at execute time.
    Unresolved,
}

/// Resolve a list of WHERE clause relations to typed byte values.
///
/// `column_types` is a lookup function that returns the CQL type for a given
/// column name.  Returns `None` for the entire batch if any column is unknown.
pub fn resolve_where_clause<F>(
    relations: &[Relation],
    column_types: F,
) -> Option<Vec<ResolvedRestriction>>
where
    F: Fn(&str) -> Option<CqlType>,
{
    relations
        .iter()
        .map(|rel| resolve_relation(rel, &column_types))
        .collect()
}

fn resolve_relation<F>(rel: &Relation, column_types: &F) -> Option<ResolvedRestriction>
where
    F: Fn(&str) -> Option<CqlType>,
{
    let cql_type = column_types(&rel.column)?;

    let value = match (&rel.op, &rel.value) {
        // IN list: resolve each element
        (RelationOp::In, Term::CollectionLiteral(items)) => {
            let resolved: Vec<Vec<u8>> = items
                .iter()
                .filter_map(|item| typed_term_to_bytes(item, &cql_type))
                .collect();
            ResolvedValue::InList(resolved)
        }
        (RelationOp::In, Term::TupleLiteral(items)) => {
            let resolved: Vec<Vec<u8>> = items
                .iter()
                .filter_map(|item| typed_term_to_bytes(item, &cql_type))
                .collect();
            ResolvedValue::InList(resolved)
        }
        // Bind marker: unresolved
        (_, Term::BindMarker(_)) => ResolvedValue::Unresolved,
        // Standard single-value restriction
        (_, term) => match typed_term_to_bytes(term, &cql_type) {
            Some(bytes) => ResolvedValue::Single(bytes),
            None => ResolvedValue::Unresolved,
        },
    };

    Some(ResolvedRestriction {
        column: rel.column.clone(),
        op: rel.op,
        value,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use cassandra_cql::ast::{BindMarker, Literal};

    fn type_lookup(name: &str) -> Option<CqlType> {
        match name {
            "id" => Some(CqlType::Int),
            "name" => Some(CqlType::Varchar),
            "age" => Some(CqlType::Int),
            "ts" => Some(CqlType::Bigint),
            _ => None,
        }
    }

    #[test]
    fn resolve_eq_int() {
        let relations = vec![Relation {
            column: "id".into(),
            op: RelationOp::Eq,
            value: Term::Literal(Literal::Integer(42)),
        }];
        let resolved = resolve_where_clause(&relations, type_lookup).unwrap();
        assert_eq!(resolved.len(), 1);
        assert_eq!(resolved[0].column, "id");
        assert_eq!(resolved[0].op, RelationOp::Eq);
        assert_eq!(
            resolved[0].value,
            ResolvedValue::Single(vec![0x00, 0x00, 0x00, 0x2A])
        );
    }

    #[test]
    fn resolve_range() {
        let relations = vec![
            Relation {
                column: "age".into(),
                op: RelationOp::Gte,
                value: Term::Literal(Literal::Integer(18)),
            },
            Relation {
                column: "age".into(),
                op: RelationOp::Lt,
                value: Term::Literal(Literal::Integer(65)),
            },
        ];
        let resolved = resolve_where_clause(&relations, type_lookup).unwrap();
        assert_eq!(resolved.len(), 2);
        assert_eq!(resolved[0].op, RelationOp::Gte);
        assert_eq!(resolved[1].op, RelationOp::Lt);
    }

    #[test]
    fn resolve_in_list() {
        let relations = vec![Relation {
            column: "id".into(),
            op: RelationOp::In,
            value: Term::CollectionLiteral(vec![
                Term::Literal(Literal::Integer(1)),
                Term::Literal(Literal::Integer(2)),
                Term::Literal(Literal::Integer(3)),
            ]),
        }];
        let resolved = resolve_where_clause(&relations, type_lookup).unwrap();
        assert_eq!(resolved.len(), 1);
        match &resolved[0].value {
            ResolvedValue::InList(items) => assert_eq!(items.len(), 3),
            _ => panic!("expected InList"),
        }
    }

    #[test]
    fn resolve_bind_marker() {
        let relations = vec![Relation {
            column: "id".into(),
            op: RelationOp::Eq,
            value: Term::BindMarker(BindMarker::Anonymous),
        }];
        let resolved = resolve_where_clause(&relations, type_lookup).unwrap();
        assert_eq!(resolved[0].value, ResolvedValue::Unresolved);
    }

    #[test]
    fn unknown_column_returns_none() {
        let relations = vec![Relation {
            column: "unknown".into(),
            op: RelationOp::Eq,
            value: Term::Literal(Literal::Integer(1)),
        }];
        assert!(resolve_where_clause(&relations, type_lookup).is_none());
    }

    #[test]
    fn resolve_text_eq() {
        let relations = vec![Relation {
            column: "name".into(),
            op: RelationOp::Eq,
            value: Term::Literal(Literal::String("alice".into())),
        }];
        let resolved = resolve_where_clause(&relations, type_lookup).unwrap();
        assert_eq!(
            resolved[0].value,
            ResolvedValue::Single(b"alice".to_vec())
        );
    }
}
