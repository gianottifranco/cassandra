// Licensed under Apache License, Version 2.0.

//! Operator-type compatibility validation.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.cql3.restrictions.SingleColumnRestriction`

use super::RestrictionError;
use crate::ast::RelationOp;
use cassandra_types::CqlType;

/// Validate that an operator is compatible with a column type.
pub fn validate_operator_type_compat(
    column_type: &CqlType,
    op: RelationOp,
) -> Result<(), RestrictionError> {
    match op {
        RelationOp::Contains => {
            if !column_type.is_collection() {
                return Err(RestrictionError::IncompatibleOperator(
                    "CONTAINS is only supported on collection columns".into(),
                ));
            }
        }
        RelationOp::ContainsKey => {
            if !matches!(column_type, CqlType::Map(_, _, _)) {
                return Err(RestrictionError::IncompatibleOperator(
                    "CONTAINS KEY is only supported on map columns".into(),
                ));
            }
        }
        RelationOp::Like => {
            if !matches!(column_type, CqlType::Ascii | CqlType::Varchar) {
                return Err(RestrictionError::IncompatibleOperator(
                    "LIKE is only supported on text columns".into(),
                ));
            }
        }
        RelationOp::Lt | RelationOp::Gt | RelationOp::Lte | RelationOp::Gte => {
            if !is_comparable(column_type) {
                return Err(RestrictionError::IncompatibleOperator(format!(
                    "Type {} does not support range queries",
                    column_type.cql_name()
                )));
            }
        }
        // Eq, Neq, In are supported on all types
        RelationOp::Eq | RelationOp::Neq | RelationOp::In => {}
    }
    Ok(())
}

/// Check if a type supports comparison operators.
fn is_comparable(cql_type: &CqlType) -> bool {
    matches!(
        cql_type,
        CqlType::Int
            | CqlType::Bigint
            | CqlType::Smallint
            | CqlType::Tinyint
            | CqlType::Float
            | CqlType::Double
            | CqlType::Decimal
            | CqlType::Varint
            | CqlType::Timestamp
            | CqlType::Date
            | CqlType::Time
            | CqlType::Uuid
            | CqlType::Timeuuid
            | CqlType::Ascii
            | CqlType::Varchar
            | CqlType::Blob
            | CqlType::Inet
            | CqlType::Duration
    )
}

/// Validate non-key column restrictions.
///
/// Non-key column restrictions require either a secondary index or ALLOW FILTERING.
pub fn validate_non_key_restrictions(
    column_name: &str,
    table: &cassandra_schema::table::TableMetadata,
    allow_filtering: bool,
) -> Result<bool, RestrictionError> {
    // Check if there's a secondary index on this column
    let has_index = table.indexes.iter().any(|idx| {
        idx.target_column()
            .map(|t| t.eq_ignore_ascii_case(column_name))
            .unwrap_or(false)
    });

    if has_index {
        return Ok(false); // no filtering needed
    }

    if allow_filtering {
        return Ok(true); // filtering needed but allowed
    }

    Err(RestrictionError::NeedsFiltering(
        "Cannot execute this query as it might involve data filtering and thus may have \
         unpredictable performance. If you want to execute this query despite the performance \
         unpredictability, use ALLOW FILTERING"
            .to_string(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn eq_always_valid() {
        assert!(validate_operator_type_compat(&CqlType::Int, RelationOp::Eq).is_ok());
        assert!(validate_operator_type_compat(&CqlType::Blob, RelationOp::Eq).is_ok());
        assert!(
            validate_operator_type_compat(
                &CqlType::List(Box::new(CqlType::Int), false),
                RelationOp::Eq
            )
            .is_ok()
        );
    }

    #[test]
    fn contains_requires_collection() {
        assert!(
            validate_operator_type_compat(
                &CqlType::List(Box::new(CqlType::Int), false),
                RelationOp::Contains
            )
            .is_ok()
        );

        assert!(validate_operator_type_compat(&CqlType::Int, RelationOp::Contains).is_err());
    }

    #[test]
    fn contains_key_requires_map() {
        assert!(
            validate_operator_type_compat(
                &CqlType::Map(Box::new(CqlType::Varchar), Box::new(CqlType::Int), false),
                RelationOp::ContainsKey
            )
            .is_ok()
        );

        assert!(
            validate_operator_type_compat(
                &CqlType::List(Box::new(CqlType::Int), false),
                RelationOp::ContainsKey
            )
            .is_err()
        );
    }

    #[test]
    fn like_requires_text() {
        assert!(validate_operator_type_compat(&CqlType::Varchar, RelationOp::Like).is_ok());
        assert!(validate_operator_type_compat(&CqlType::Ascii, RelationOp::Like).is_ok());
        assert!(validate_operator_type_compat(&CqlType::Int, RelationOp::Like).is_err());
    }

    #[test]
    fn range_requires_comparable() {
        assert!(validate_operator_type_compat(&CqlType::Int, RelationOp::Gt).is_ok());
        assert!(validate_operator_type_compat(&CqlType::Varchar, RelationOp::Lt).is_ok());

        // Collections are not comparable
        assert!(
            validate_operator_type_compat(
                &CqlType::List(Box::new(CqlType::Int), false),
                RelationOp::Gt
            )
            .is_err()
        );
    }
}
