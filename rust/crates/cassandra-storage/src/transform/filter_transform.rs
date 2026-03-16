// Licensed under Apache License, Version 2.0.

//! Filter transform: evaluates `RowFilter` expressions against row cells.
//!
//! ## Java Oracle
//! `org.apache.cassandra.db.transform.Filter`

use crate::filter::row_filter::{FilterExpression, Operator, RowFilter};
use crate::rows::unfiltered::{ColumnData, RowData};
use super::transformation::Transformation;

/// Evaluates a `RowFilter` against each row, dropping non-matching rows.
pub struct FilterTransform {
    pub filter: RowFilter,
}

impl FilterTransform {
    pub fn new(filter: RowFilter) -> Self {
        Self { filter }
    }

    /// Evaluate a single expression against a row.
    fn evaluate_expression(row: &RowData, expr: &FilterExpression) -> bool {
        match expr {
            FilterExpression::Simple {
                column,
                operator,
                value,
            } => {
                let cell_value = match row.columns.get(column.as_str()) {
                    Some(ColumnData::Simple(cell)) => cell.value.as_deref(),
                    _ => None,
                };

                match cell_value {
                    None => {
                        // NULL handling: only NEQ matches null (null != anything is true)
                        matches!(operator, Operator::Neq)
                    }
                    Some(cv) => Self::compare_bytes(cv, operator, value),
                }
            }
            FilterExpression::MapEquality {
                column,
                key,
                value,
            } => {
                // For map equality, look for a cell with path matching key
                if let Some(ColumnData::Complex(ccd)) = row.columns.get(column.as_str()) {
                    use crate::rows::cell::CellPath;
                    if let Some(cell) = ccd.cells.get(&CellPath(key.clone())) {
                        return cell.value.as_deref() == Some(value.as_slice());
                    }
                }
                false
            }
            FilterExpression::Custom { column, .. } => {
                // Custom expressions (SAI/SASI) are evaluated by the index;
                // at the storage layer, just check the column exists.
                row.columns.contains_key(column.as_str())
            }
        }
    }

    fn compare_bytes(cell_value: &[u8], operator: &Operator, filter_value: &[u8]) -> bool {
        match operator {
            Operator::Eq => cell_value == filter_value,
            Operator::Neq => cell_value != filter_value,
            Operator::Lt => cell_value < filter_value,
            Operator::Lte => cell_value <= filter_value,
            Operator::Gt => cell_value > filter_value,
            Operator::Gte => cell_value >= filter_value,
            Operator::Contains => {
                // For collections: check if value appears in cell bytes
                // Simplified: substring match on raw bytes
                cell_value
                    .windows(filter_value.len())
                    .any(|w| w == filter_value)
            }
            Operator::ContainsKey => {
                // Similar to Contains for keys
                cell_value
                    .windows(filter_value.len())
                    .any(|w| w == filter_value)
            }
            Operator::Ann => true, // ANN is handled by index layer
        }
    }
}

impl Transformation for FilterTransform {
    fn apply_to_row(&mut self, row: RowData) -> Option<RowData> {
        // All expressions must match (AND semantics)
        let matches = self
            .filter
            .expressions
            .iter()
            .all(|expr| Self::evaluate_expression(&row, expr));
        if matches { Some(row) } else { None }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rows::cell::CellData;
    use crate::rows::liveness::LivenessInfo;
    use cassandra_common::ttl::NO_TTL;

    fn row_with_cell(ck: &[u8], col: &str, val: &[u8]) -> RowData {
        let mut row = RowData::new(ck.to_vec());
        row.liveness_info = LivenessInfo::create(100);
        row.add_cell(CellData {
            column: col.to_string(),
            value: Some(val.to_vec()),
            timestamp: 100,
            ttl: NO_TTL,
            local_deletion_time: i32::MAX,
            path: None,
        });
        row
    }

    #[test]
    fn eq_match() {
        let filter = RowFilter::none().with(FilterExpression::Simple {
            column: "name".to_string(),
            operator: Operator::Eq,
            value: b"alice".to_vec(),
        });
        let mut ft = FilterTransform::new(filter);

        let row = row_with_cell(b"ck", "name", b"alice");
        assert!(ft.apply_to_row(row).is_some());

        let row = row_with_cell(b"ck", "name", b"bob");
        assert!(ft.apply_to_row(row).is_none());
    }

    #[test]
    fn gt_comparison() {
        let filter = RowFilter::none().with(FilterExpression::Simple {
            column: "age".to_string(),
            operator: Operator::Gt,
            value: vec![20],
        });
        let mut ft = FilterTransform::new(filter);

        let row = row_with_cell(b"ck", "age", &[30]);
        assert!(ft.apply_to_row(row).is_some());

        let row = row_with_cell(b"ck", "age", &[10]);
        assert!(ft.apply_to_row(row).is_none());
    }

    #[test]
    fn null_handling() {
        let filter = RowFilter::none().with(FilterExpression::Simple {
            column: "missing".to_string(),
            operator: Operator::Eq,
            value: b"val".to_vec(),
        });
        let mut ft = FilterTransform::new(filter);

        let row = row_with_cell(b"ck", "other", b"val");
        assert!(ft.apply_to_row(row).is_none()); // missing column -> null -> no match
    }

    #[test]
    fn multiple_expressions_and() {
        let filter = RowFilter::none()
            .with(FilterExpression::Simple {
                column: "a".to_string(),
                operator: Operator::Eq,
                value: b"1".to_vec(),
            })
            .with(FilterExpression::Simple {
                column: "b".to_string(),
                operator: Operator::Eq,
                value: b"2".to_vec(),
            });
        let mut ft = FilterTransform::new(filter);

        let mut row = RowData::new(b"ck".to_vec());
        row.liveness_info = LivenessInfo::create(100);
        row.add_cell(CellData {
            column: "a".to_string(),
            value: Some(b"1".to_vec()),
            timestamp: 100,
            ttl: NO_TTL,
            local_deletion_time: i32::MAX,
            path: None,
        });
        row.add_cell(CellData {
            column: "b".to_string(),
            value: Some(b"2".to_vec()),
            timestamp: 100,
            ttl: NO_TTL,
            local_deletion_time: i32::MAX,
            path: None,
        });
        assert!(ft.apply_to_row(row).is_some());
    }
}
