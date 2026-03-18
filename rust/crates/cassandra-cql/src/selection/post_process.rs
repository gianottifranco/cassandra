// Licensed under Apache License, Version 2.0.

//! Post-processing stages: DISTINCT, ORDER BY, LIMIT.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.cql3.selection.Selection`

use cassandra_types::CqlType;
use std::cmp::Ordering;
use std::collections::HashSet;

/// Remove duplicate rows based on partition key columns.
pub fn apply_distinct(
    rows: Vec<Vec<Option<Vec<u8>>>>,
    pk_column_indices: &[usize],
) -> Vec<Vec<Option<Vec<u8>>>> {
    let mut seen = HashSet::new();
    let mut result = Vec::new();

    for row in rows {
        let key: Vec<Option<Vec<u8>>> = pk_column_indices
            .iter()
            .map(|&i| row.get(i).cloned().flatten())
            .collect();

        // Use bytes as hash key
        let key_bytes: Vec<u8> = key
            .iter()
            .flat_map(|v| {
                let bytes = v.as_deref().unwrap_or(&[]);
                let len = (bytes.len() as u32).to_be_bytes();
                len.into_iter()
                    .chain(bytes.iter().copied())
                    .collect::<Vec<_>>()
            })
            .collect();

        if seen.insert(key_bytes) {
            result.push(row);
        }
    }

    result
}

/// Sort rows by the given columns and orders.
pub fn apply_order_by(
    mut rows: Vec<Vec<Option<Vec<u8>>>>,
    order_columns: &[(usize, CqlType, bool)], // (index, type, is_desc)
) -> Vec<Vec<Option<Vec<u8>>>> {
    rows.sort_by(|a, b| {
        for &(idx, ref cql_type, is_desc) in order_columns {
            let left = a.get(idx).and_then(|v| v.as_deref());
            let right = b.get(idx).and_then(|v| v.as_deref());

            let ord = match (left, right) {
                (None, None) => Ordering::Equal,
                (None, Some(_)) => Ordering::Less,
                (Some(_), None) => Ordering::Greater,
                (Some(l), Some(r)) => cassandra_types::comparator::compare_bytes(cql_type, l, r),
            };

            let ord = if is_desc { ord.reverse() } else { ord };
            if ord != Ordering::Equal {
                return ord;
            }
        }
        Ordering::Equal
    });

    rows
}

/// Truncate rows to the given limit.
pub fn apply_limit(rows: Vec<Vec<Option<Vec<u8>>>>, limit: usize) -> Vec<Vec<Option<Vec<u8>>>> {
    rows.into_iter().take(limit).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn int_val(v: i32) -> Option<Vec<u8>> {
        Some(v.to_be_bytes().to_vec())
    }

    #[test]
    fn distinct_removes_duplicates() {
        let rows = vec![
            vec![int_val(1), Some(b"a".to_vec())],
            vec![int_val(1), Some(b"b".to_vec())],
            vec![int_val(2), Some(b"c".to_vec())],
        ];
        let result = apply_distinct(rows, &[0]);
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn order_by_asc() {
        let rows = vec![vec![int_val(3)], vec![int_val(1)], vec![int_val(2)]];
        let result = apply_order_by(rows, &[(0, CqlType::Int, false)]);
        assert_eq!(result[0][0].as_ref().unwrap(), &1i32.to_be_bytes().to_vec());
        assert_eq!(result[2][0].as_ref().unwrap(), &3i32.to_be_bytes().to_vec());
    }

    #[test]
    fn order_by_desc() {
        let rows = vec![vec![int_val(1)], vec![int_val(3)], vec![int_val(2)]];
        let result = apply_order_by(rows, &[(0, CqlType::Int, true)]);
        assert_eq!(result[0][0].as_ref().unwrap(), &3i32.to_be_bytes().to_vec());
        assert_eq!(result[2][0].as_ref().unwrap(), &1i32.to_be_bytes().to_vec());
    }

    #[test]
    fn limit_truncates() {
        let rows = vec![vec![int_val(1)], vec![int_val(2)], vec![int_val(3)]];
        let result = apply_limit(rows, 2);
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn limit_larger_than_rows() {
        let rows = vec![vec![int_val(1)]];
        let result = apply_limit(rows, 100);
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn order_by_with_nulls() {
        let rows = vec![vec![int_val(2)], vec![None], vec![int_val(1)]];
        let result = apply_order_by(rows, &[(0, CqlType::Int, false)]);
        assert!(result[0][0].is_none()); // nulls first
        assert_eq!(result[1][0].as_ref().unwrap(), &1i32.to_be_bytes().to_vec());
    }
}
