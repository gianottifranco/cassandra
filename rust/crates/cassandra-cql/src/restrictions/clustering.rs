// Licensed under Apache License, Version 2.0.

//! Clustering column restriction validation.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.cql3.restrictions.ClusteringColumnRestrictions`

use super::{ColumnRestriction, RestrictionError, RestrictionKind};
use crate::ast::{Relation, RelationOp};
use cassandra_schema::table::TableMetadata;

/// Validate clustering column restrictions.
///
/// Rules:
/// - Restrictions must form a contiguous prefix of the clustering columns.
/// - Slice (range) restrictions are only allowed on the last restricted column.
/// - IN is only allowed on the last restricted column.
pub fn validate_clustering(
    relations: &[Relation],
    table: &TableMetadata,
) -> Result<Vec<ColumnRestriction>, RestrictionError> {
    let ck_columns = table.clustering_columns();
    if ck_columns.is_empty() {
        return Ok(vec![]);
    }

    // Build a map of column_name -> (index, restrictions)
    let mut ck_map: Vec<Vec<&Relation>> = vec![Vec::new(); ck_columns.len()];

    for relation in relations {
        for (i, ck_col) in ck_columns.iter().enumerate() {
            if relation.column.eq_ignore_ascii_case(&ck_col.name) {
                ck_map[i].push(relation);
            }
        }
    }

    // Find the last restricted clustering column
    let mut last_restricted = None;
    for (i, rels) in ck_map.iter().enumerate() {
        if !rels.is_empty() {
            last_restricted = Some(i);
        }
    }

    let last_restricted = match last_restricted {
        Some(i) => i,
        None => return Ok(vec![]), // no clustering restrictions
    };

    // Verify contiguous prefix: no gaps before the last restricted column
    for i in 0..last_restricted {
        if ck_map[i].is_empty() {
            return Err(RestrictionError::InvalidClusteringOrder(format!(
                "Clustering column '{}' must be restricted before '{}' can be restricted",
                ck_columns[i].name, ck_columns[last_restricted].name
            )));
        }
    }

    // Build restrictions and validate slice/IN position
    let mut restrictions = Vec::new();

    for i in 0..=last_restricted {
        for rel in &ck_map[i] {
            let kind = match rel.op {
                RelationOp::Eq => RestrictionKind::Eq,
                RelationOp::In => {
                    if i < last_restricted {
                        return Err(RestrictionError::InvalidClusteringOrder(
                            "IN is only supported on the last clustering column in the restriction"
                                .into(),
                        ));
                    }
                    RestrictionKind::In
                }
                op @ (RelationOp::Lt | RelationOp::Gt | RelationOp::Lte | RelationOp::Gte) => {
                    if i < last_restricted {
                        return Err(RestrictionError::InvalidClusteringOrder(
                            "Slice restrictions are only supported on the last clustering column"
                                .into(),
                        ));
                    }
                    RestrictionKind::Range { op }
                }
                op => {
                    return Err(RestrictionError::IncompatibleOperator(format!(
                        "Operator {:?} is not supported on clustering column '{}'",
                        op, ck_columns[i].name
                    )));
                }
            };

            restrictions.push(ColumnRestriction {
                column_name: ck_columns[i].name.clone(),
                kind,
            });
        }
    }

    Ok(restrictions)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{Literal, Term};
    use cassandra_schema::column::{ClusteringOrder, ColumnMetadata};
    use cassandra_schema::table::TableMetadataBuilder;
    use cassandra_types::CqlType;

    fn table_with_clustering() -> TableMetadata {
        TableMetadataBuilder::new("ks", "t")
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
            .add_column(ColumnMetadata::regular("v", CqlType::Varchar))
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
    fn no_clustering_restrictions() {
        let table = table_with_clustering();
        let result = validate_clustering(&[], &table).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn first_ck_eq() {
        let table = table_with_clustering();
        let relations = vec![rel("ck1", RelationOp::Eq)];
        let result = validate_clustering(&relations, &table).unwrap();
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn contiguous_prefix() {
        let table = table_with_clustering();
        let relations = vec![rel("ck1", RelationOp::Eq), rel("ck2", RelationOp::Eq)];
        let result = validate_clustering(&relations, &table).unwrap();
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn gap_rejected() {
        let table = table_with_clustering();
        // Skip ck1, only restrict ck2
        let relations = vec![rel("ck2", RelationOp::Eq)];
        // ck2 is at index 1, but ck1 at index 0 is not restricted
        // Actually, this should work since ck2's relations go into ck_map[1]
        // and last_restricted = 1, but ck_map[0] is empty => gap error
        let result = validate_clustering(&relations, &table);
        assert!(result.is_err());
    }

    #[test]
    fn slice_on_last() {
        let table = table_with_clustering();
        let relations = vec![rel("ck1", RelationOp::Eq), rel("ck2", RelationOp::Gt)];
        let result = validate_clustering(&relations, &table).unwrap();
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn slice_not_on_last_rejected() {
        let table = table_with_clustering();
        let relations = vec![rel("ck1", RelationOp::Gt), rel("ck2", RelationOp::Eq)];
        let result = validate_clustering(&relations, &table);
        assert!(result.is_err());
    }

    #[test]
    fn in_on_last() {
        let table = table_with_clustering();
        let relations = vec![rel("ck1", RelationOp::Eq), rel("ck2", RelationOp::In)];
        let result = validate_clustering(&relations, &table).unwrap();
        assert_eq!(result.len(), 2);
    }
}
