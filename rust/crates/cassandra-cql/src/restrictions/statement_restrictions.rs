// Licensed under Apache License, Version 2.0.

//! Top-level statement restrictions builder.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.cql3.restrictions.StatementRestrictions`

use super::clustering::validate_clustering;
use super::partition_key::validate_partition_key;
use super::type_compat::{validate_non_key_restrictions, validate_operator_type_compat};
use super::{ColumnRestriction, RestrictionError, RestrictionKind, RestrictionSet};
use crate::ast::Relation;
use cassandra_schema::column::ColumnKind;
use cassandra_schema::table::TableMetadata;

/// Build and validate all restrictions for a statement.
///
/// This is the single entry point used by the planner.
pub fn build(
    relations: &[Relation],
    table: &TableMetadata,
    allow_filtering: bool,
) -> Result<RestrictionSet, RestrictionError> {
    if relations.is_empty() {
        return Ok(RestrictionSet::default());
    }

    // Validate operator-type compatibility for all relations
    for rel in relations {
        if rel.column.eq_ignore_ascii_case("token") {
            continue; // token() is handled specially
        }
        if let Some(col_meta) = table.column(&rel.column) {
            validate_operator_type_compat(&col_meta.column_type, rel.op)?;
        }
        // Unknown columns are caught below during classification
    }

    // Classify relations by column kind
    let mut pk_relations = Vec::new();
    let mut ck_relations = Vec::new();
    let mut non_key_relations = Vec::new();

    for rel in relations {
        if rel.column.eq_ignore_ascii_case("token") {
            pk_relations.push(rel.clone());
            continue;
        }

        match table.column(&rel.column) {
            Some(col) => match col.kind {
                ColumnKind::PartitionKey => pk_relations.push(rel.clone()),
                ColumnKind::Clustering => ck_relations.push(rel.clone()),
                ColumnKind::Regular | ColumnKind::Static => non_key_relations.push(rel.clone()),
            },
            None => {
                return Err(RestrictionError::Invalid(format!(
                    "Undefined column name '{}'",
                    rel.column
                )));
            }
        }
    }

    // Validate partition key restrictions
    let pk_restrictions = validate_partition_key(&pk_relations, table)?;

    // Validate clustering restrictions
    let ck_restrictions = validate_clustering(&ck_relations, table)?;

    // Validate non-key restrictions
    let mut needs_filtering = false;
    let mut nk_restrictions = Vec::new();

    for rel in &non_key_relations {
        let requires_filtering =
            validate_non_key_restrictions(&rel.column, table, allow_filtering)?;
        if requires_filtering {
            needs_filtering = true;
        }
        nk_restrictions.push(ColumnRestriction {
            column_name: rel.column.clone(),
            kind: match rel.op {
                crate::ast::RelationOp::Eq => RestrictionKind::Eq,
                crate::ast::RelationOp::In => RestrictionKind::In,
                crate::ast::RelationOp::Contains => RestrictionKind::Contains,
                crate::ast::RelationOp::ContainsKey => RestrictionKind::ContainsKey,
                op => RestrictionKind::Range { op },
            },
        });
    }

    let is_token_based = pk_restrictions
        .iter()
        .any(|r| matches!(r.kind, RestrictionKind::TokenBased));

    Ok(RestrictionSet {
        partition_key_restrictions: pk_restrictions,
        clustering_restrictions: ck_restrictions,
        non_key_restrictions: nk_restrictions,
        needs_filtering,
        is_token_based,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{Literal, RelationOp, Term};
    use cassandra_schema::column::{ClusteringOrder, ColumnMetadata};
    use cassandra_schema::table::TableMetadataBuilder;
    use cassandra_types::CqlType;

    fn test_table() -> TableMetadata {
        TableMetadataBuilder::new("ks", "t")
            .add_column(ColumnMetadata::partition_key("pk", 0, CqlType::Int))
            .add_column(ColumnMetadata::clustering(
                "ck",
                0,
                CqlType::Int,
                ClusteringOrder::Asc,
            ))
            .add_column(ColumnMetadata::regular("v", CqlType::Varchar))
            .add_column(ColumnMetadata::regular("n", CqlType::Int))
            .build()
    }

    fn rel(col: &str, op: RelationOp) -> Relation {
        Relation {
            column: col.to_string(),
            op,
            value: Term::Literal(Literal::Integer(1)),
        }
    }

    #[test]
    fn empty_relations() {
        let table = test_table();
        let result = build(&[], &table, false).unwrap();
        assert!(result.partition_key_restrictions.is_empty());
    }

    #[test]
    fn pk_only() {
        let table = test_table();
        let relations = vec![rel("pk", RelationOp::Eq)];
        let result = build(&relations, &table, false).unwrap();
        assert_eq!(result.partition_key_restrictions.len(), 1);
        assert!(!result.needs_filtering);
    }

    #[test]
    fn pk_and_ck() {
        let table = test_table();
        let relations = vec![rel("pk", RelationOp::Eq), rel("ck", RelationOp::Gt)];
        let result = build(&relations, &table, false).unwrap();
        assert_eq!(result.partition_key_restrictions.len(), 1);
        assert_eq!(result.clustering_restrictions.len(), 1);
    }

    #[test]
    fn non_key_without_filtering_rejected() {
        let table = test_table();
        let relations = vec![rel("pk", RelationOp::Eq), rel("v", RelationOp::Eq)];
        let result = build(&relations, &table, false);
        assert!(result.is_err());
    }

    #[test]
    fn non_key_with_filtering() {
        let table = test_table();
        let relations = vec![rel("pk", RelationOp::Eq), rel("v", RelationOp::Eq)];
        let result = build(&relations, &table, true).unwrap();
        assert!(result.needs_filtering);
        assert_eq!(result.non_key_restrictions.len(), 1);
    }

    #[test]
    fn undefined_column() {
        let table = test_table();
        let relations = vec![
            rel("pk", RelationOp::Eq),
            rel("nonexistent", RelationOp::Eq),
        ];
        let result = build(&relations, &table, false);
        assert!(result.is_err());
    }

    #[test]
    fn token_based() {
        let table = test_table();
        let relations = vec![rel("token", RelationOp::Gt)];
        let result = build(&relations, &table, false).unwrap();
        assert!(result.is_token_based);
    }

    #[test]
    fn mixed_where_clause() {
        let table = test_table();
        let relations = vec![
            rel("pk", RelationOp::Eq),
            rel("ck", RelationOp::Eq),
            rel("n", RelationOp::Gt),
        ];
        let result = build(&relations, &table, true).unwrap();
        assert_eq!(result.partition_key_restrictions.len(), 1);
        assert_eq!(result.clustering_restrictions.len(), 1);
        assert_eq!(result.non_key_restrictions.len(), 1);
        assert!(result.needs_filtering);
    }
}
