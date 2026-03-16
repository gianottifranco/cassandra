// Licensed under Apache License, Version 2.0.

//! Partition key restriction validation.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.cql3.restrictions.PartitionKeyRestrictions`

use super::{ColumnRestriction, RestrictionError, RestrictionKind};
use crate::ast::{Relation, RelationOp};
use cassandra_schema::table::TableMetadata;

/// Validate partition key restrictions.
///
/// Rules:
/// - All partition key columns must have equality or IN restrictions, OR
/// - A token-based restriction is used.
/// - IN is only allowed on the last partition key column.
pub fn validate_partition_key(
    relations: &[Relation],
    table: &TableMetadata,
) -> Result<Vec<ColumnRestriction>, RestrictionError> {
    let pk_columns = table.partition_key_columns();

    // Check for token-based restrictions
    let has_token = relations.iter().any(|r| r.column.eq_ignore_ascii_case("token"));
    if has_token {
        return Ok(vec![ColumnRestriction {
            column_name: "token".to_string(),
            kind: RestrictionKind::TokenBased,
        }]);
    }

    // Collect restrictions on partition key columns
    let mut pk_restrictions: Vec<Option<ColumnRestriction>> = vec![None; pk_columns.len()];

    for relation in relations {
        for (i, pk_col) in pk_columns.iter().enumerate() {
            if relation.column.eq_ignore_ascii_case(&pk_col.name) {
                let kind = match relation.op {
                    RelationOp::Eq => RestrictionKind::Eq,
                    RelationOp::In => RestrictionKind::In,
                    _ => {
                        return Err(RestrictionError::MissingPartitionKey(
                            "Only EQ and IN relation are supported on the partition key \
                             (unless you use the token() function or allow filtering)"
                                .to_string(),
                        ));
                    }
                };
                pk_restrictions[i] = Some(ColumnRestriction {
                    column_name: pk_col.name.clone(),
                    kind,
                });
            }
        }
    }

    // Validate all PK columns are restricted
    for (i, restriction) in pk_restrictions.iter().enumerate() {
        if restriction.is_none() {
            return Err(RestrictionError::MissingPartitionKey(format!(
                "Some partition key parts are missing: {}",
                pk_columns[i].name
            )));
        }
    }

    // Validate IN is only on the last PK column
    for (i, restriction) in pk_restrictions.iter().enumerate() {
        if let Some(r) = restriction {
            if matches!(r.kind, RestrictionKind::In) && i < pk_columns.len() - 1 {
                return Err(RestrictionError::MissingPartitionKey(
                    "IN is only supported on the last column of the partition key"
                        .to_string(),
                ));
            }
        }
    }

    Ok(pk_restrictions.into_iter().flatten().collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{Literal, Term};
    use cassandra_schema::column::ColumnMetadata;
    use cassandra_schema::table::TableMetadataBuilder;
    use cassandra_types::CqlType;

    fn simple_pk_table() -> TableMetadata {
        TableMetadataBuilder::new("ks", "t")
            .add_column(ColumnMetadata::partition_key("id", 0, CqlType::Int))
            .add_column(ColumnMetadata::regular("v", CqlType::Varchar))
            .build()
    }

    fn composite_pk_table() -> TableMetadata {
        TableMetadataBuilder::new("ks", "t")
            .add_column(ColumnMetadata::partition_key("a", 0, CqlType::Int))
            .add_column(ColumnMetadata::partition_key("b", 1, CqlType::Int))
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
    fn simple_pk_eq() {
        let table = simple_pk_table();
        let relations = vec![rel("id", RelationOp::Eq)];
        let result = validate_partition_key(&relations, &table).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].column_name, "id");
    }

    #[test]
    fn missing_pk() {
        let table = simple_pk_table();
        let result = validate_partition_key(&[], &table);
        assert!(result.is_err());
    }

    #[test]
    fn composite_pk_complete() {
        let table = composite_pk_table();
        let relations = vec![rel("a", RelationOp::Eq), rel("b", RelationOp::Eq)];
        let result = validate_partition_key(&relations, &table).unwrap();
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn composite_pk_partial() {
        let table = composite_pk_table();
        let relations = vec![rel("a", RelationOp::Eq)];
        let result = validate_partition_key(&relations, &table);
        assert!(result.is_err());
    }

    #[test]
    fn pk_range_rejected() {
        let table = simple_pk_table();
        let relations = vec![rel("id", RelationOp::Gt)];
        let result = validate_partition_key(&relations, &table);
        assert!(result.is_err());
    }

    #[test]
    fn token_based() {
        let table = simple_pk_table();
        let relations = vec![rel("token", RelationOp::Gt)];
        let result = validate_partition_key(&relations, &table).unwrap();
        assert!(matches!(result[0].kind, RestrictionKind::TokenBased));
    }

    #[test]
    fn in_on_last_pk_column() {
        let table = composite_pk_table();
        let relations = vec![rel("a", RelationOp::Eq), rel("b", RelationOp::In)];
        let result = validate_partition_key(&relations, &table);
        assert!(result.is_ok());
    }

    #[test]
    fn in_on_non_last_pk_rejected() {
        let table = composite_pk_table();
        let relations = vec![rel("a", RelationOp::In), rel("b", RelationOp::Eq)];
        let result = validate_partition_key(&relations, &table);
        assert!(result.is_err());
    }
}
