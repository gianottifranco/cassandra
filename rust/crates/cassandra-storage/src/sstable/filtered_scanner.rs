// Licensed under Apache License, Version 2.0.

//! Filtered scanner: applies column, clustering, row, and limit filters to partition data.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.db.partitions.UnfilteredPartitionIterator`
//! - `org.apache.cassandra.db.filter.ColumnFilter`
//! - `org.apache.cassandra.db.filter.ClusteringIndexSliceFilter`
//! - `org.apache.cassandra.db.filter.DataLimits`
//! - `org.apache.cassandra.db.filter.RowFilter`

use std::collections::BTreeMap;

use crate::filter::clustering_filter::ClusteringIndexFilter;
use crate::filter::column_filter::ColumnFilter;
use crate::filter::data_limits::DataLimits;
use crate::filter::row_filter::{FilterExpression, Operator, RowFilter};
use crate::memtable::partition::{PartitionData, Row};

/// Bundle of optional filters to apply when scanning partitions.
#[derive(Debug, Clone)]
pub struct ScanOptions {
    pub column_filter: Option<ColumnFilter>,
    pub clustering_filter: Option<ClusteringIndexFilter>,
    pub data_limits: Option<DataLimits>,
    pub row_filter: Option<RowFilter>,
}

impl ScanOptions {
    /// No filters (pass everything through).
    pub fn none() -> Self {
        Self {
            column_filter: None,
            clustering_filter: None,
            data_limits: None,
            row_filter: None,
        }
    }
}

impl Default for ScanOptions {
    fn default() -> Self {
        Self::none()
    }
}

/// Applies all configured filters to a `PartitionData`, returning a filtered copy.
pub fn apply_filters(partition: PartitionData, options: &ScanOptions) -> PartitionData {
    let mut rows = partition.rows;

    // 1. Clustering filter: retain only rows whose clustering key is selected.
    if let Some(ref cf) = options.clustering_filter {
        rows = rows
            .into_iter()
            .filter(|(ck, _)| cf.selects(ck))
            .collect();
    }

    // 2. Row filter: evaluate expressions on cell values.
    if let Some(ref rf) = options.row_filter {
        if !rf.is_empty() {
            rows = rows
                .into_iter()
                .filter(|(_, row)| evaluate_row_filter(row, rf))
                .collect();
        }
    }

    // 3. Data limits: stop after N rows.
    if let Some(ref dl) = options.data_limits {
        let limit = dl.rows_limit() as usize;
        if rows.len() > limit {
            rows = rows.into_iter().take(limit).collect();
        }
    }

    // 4. Column filter: retain only cells for requested columns.
    if let Some(ref col_filter) = options.column_filter {
        rows = rows
            .into_iter()
            .map(|(ck, row)| {
                let filtered_cells = row
                    .cells
                    .into_iter()
                    .filter(|cell| col_filter.fetches(&cell.column))
                    .collect();
                let filtered_row = Row {
                    clustering_key: row.clustering_key,
                    cells: filtered_cells,
                    is_tombstone: row.is_tombstone,
                    local_deletion_time: row.local_deletion_time,
                };
                (ck, filtered_row)
            })
            .collect();
    }

    PartitionData {
        rows,
        tombstone_timestamp: partition.tombstone_timestamp,
        tombstone_local_deletion_time: partition.tombstone_local_deletion_time,
    }
}

/// Reads partitions from a source and applies filters.
pub struct FilteredPartitionReader {
    options: ScanOptions,
}

impl FilteredPartitionReader {
    pub fn new(options: ScanOptions) -> Self {
        Self { options }
    }

    /// Apply filters to a single partition.
    pub fn filter_partition(&self, partition: PartitionData) -> PartitionData {
        apply_filters(partition, &self.options)
    }

    /// Apply filters to a sequence of partitions, respecting global data limits.
    pub fn filter_all(
        &self,
        partitions: Vec<(Vec<u8>, PartitionData)>,
    ) -> Vec<(Vec<u8>, PartitionData)> {
        let global_limit = self
            .options
            .data_limits
            .as_ref()
            .map(|dl| dl.rows_limit() as usize)
            .unwrap_or(usize::MAX);

        let mut result = Vec::new();
        let mut total_rows = 0usize;

        // Build per-partition options without the global data limit
        // (we track it ourselves across partitions).
        let mut per_partition_opts = self.options.clone();
        per_partition_opts.data_limits = None;

        for (key, partition) in partitions {
            if total_rows >= global_limit {
                break;
            }
            let remaining = global_limit - total_rows;
            let filtered = apply_filters(partition, &per_partition_opts);

            // Apply remaining global limit.
            let limited_rows: BTreeMap<Vec<u8>, Row> =
                filtered.rows.into_iter().take(remaining).collect();
            total_rows += limited_rows.len();

            if !limited_rows.is_empty() {
                result.push((
                    key,
                    PartitionData {
                        rows: limited_rows,
                        tombstone_timestamp: filtered.tombstone_timestamp,
                        tombstone_local_deletion_time: filtered
                            .tombstone_local_deletion_time,
                    },
                ));
            }
        }

        result
    }
}

/// Evaluate a row filter against a row. All expressions must match (AND semantics).
fn evaluate_row_filter(row: &Row, filter: &RowFilter) -> bool {
    filter
        .expressions
        .iter()
        .all(|expr| evaluate_expression(row, expr))
}

/// Evaluate a single filter expression against a row.
fn evaluate_expression(row: &Row, expr: &FilterExpression) -> bool {
    match expr {
        FilterExpression::Simple {
            column,
            operator,
            value,
        } => {
            let cell = row.cells.iter().find(|c| c.column == *column);
            match operator {
                Operator::Eq => match cell {
                    Some(c) => c.value.as_deref() == Some(value.as_slice()),
                    None => false,
                },
                // Other operators: pass through (not yet implemented).
                _ => true,
            }
        }
        // MapEquality, Custom: pass through (not yet implemented).
        _ => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::filter::clustering_filter::{
        ClusteringBound, ClusteringIndexFilter, Slice, Slices,
    };
    use crate::memtable::partition::Cell;
    use std::collections::BTreeSet;

    fn make_row(ck: &[u8], columns: &[(&str, &[u8])]) -> Row {
        Row {
            clustering_key: ck.to_vec(),
            cells: columns
                .iter()
                .map(|(name, val)| Cell {
                    column: name.to_string(),
                    value: Some(val.to_vec()),
                    timestamp: 1000,
                    ttl: 0,
                    local_deletion_time: None,
                    is_tombstone: false,
                })
                .collect(),
            is_tombstone: false,
            local_deletion_time: None,
        }
    }

    fn make_partition(rows: Vec<Row>) -> PartitionData {
        let mut pd = PartitionData::new();
        for row in rows {
            pd.apply_row(row);
        }
        pd
    }

    #[test]
    fn column_filter_removes_unneeded_columns() {
        let pd = make_partition(vec![
            make_row(b"ck1", &[("name", b"alice"), ("age", b"30"), ("score", b"99")]),
        ]);

        let mut fetched = BTreeSet::new();
        fetched.insert("name".to_string());
        fetched.insert("age".to_string());
        let mut queried = BTreeSet::new();
        queried.insert("name".to_string());
        queried.insert("age".to_string());

        let options = ScanOptions {
            column_filter: Some(ColumnFilter::OnlyQueried { fetched, queried }),
            clustering_filter: None,
            data_limits: None,
            row_filter: None,
        };

        let filtered = apply_filters(pd, &options);
        let row = filtered.rows.get(&b"ck1".to_vec()).unwrap();
        let col_names: Vec<&str> = row.cells.iter().map(|c| c.column.as_str()).collect();
        assert!(col_names.contains(&"name"));
        assert!(col_names.contains(&"age"));
        assert!(!col_names.contains(&"score"));
    }

    #[test]
    fn clustering_slice_retains_correct_rows() {
        let pd = make_partition(vec![
            make_row(b"a", &[("x", b"1")]),
            make_row(b"b", &[("x", b"2")]),
            make_row(b"c", &[("x", b"3")]),
            make_row(b"d", &[("x", b"4")]),
        ]);

        let options = ScanOptions {
            column_filter: None,
            clustering_filter: Some(ClusteringIndexFilter::Slice(Slices {
                slices: vec![Slice {
                    start: ClusteringBound {
                        values: b"b".to_vec(),
                        inclusive: true,
                    },
                    end: ClusteringBound {
                        values: b"c".to_vec(),
                        inclusive: true,
                    },
                }],
                is_reversed: false,
            })),
            data_limits: None,
            row_filter: None,
        };

        let filtered = apply_filters(pd, &options);
        assert_eq!(filtered.rows.len(), 2);
        assert!(filtered.rows.contains_key(&b"b".to_vec()));
        assert!(filtered.rows.contains_key(&b"c".to_vec()));
    }

    #[test]
    fn data_limit_stops_at_n_rows() {
        let pd = make_partition(vec![
            make_row(b"a", &[("x", b"1")]),
            make_row(b"b", &[("x", b"2")]),
            make_row(b"c", &[("x", b"3")]),
            make_row(b"d", &[("x", b"4")]),
            make_row(b"e", &[("x", b"5")]),
        ]);

        let options = ScanOptions {
            column_filter: None,
            clustering_filter: None,
            data_limits: Some(DataLimits::CqlLimit {
                rows_limit: 3,
                per_partition_limit: u32::MAX,
            }),
            row_filter: None,
        };

        let filtered = apply_filters(pd, &options);
        assert_eq!(filtered.rows.len(), 3);
    }

    #[test]
    fn row_filter_evaluates_eq() {
        let pd = make_partition(vec![
            make_row(b"ck1", &[("name", b"alice")]),
            make_row(b"ck2", &[("name", b"bob")]),
            make_row(b"ck3", &[("name", b"alice")]),
        ]);

        let options = ScanOptions {
            column_filter: None,
            clustering_filter: None,
            data_limits: None,
            row_filter: Some(
                RowFilter::none().with(FilterExpression::Simple {
                    column: "name".to_string(),
                    operator: Operator::Eq,
                    value: b"alice".to_vec(),
                }),
            ),
        };

        let filtered = apply_filters(pd, &options);
        assert_eq!(filtered.rows.len(), 2);
        assert!(filtered.rows.contains_key(&b"ck1".to_vec()));
        assert!(filtered.rows.contains_key(&b"ck3".to_vec()));
    }

    #[test]
    fn combined_filters() {
        let pd = make_partition(vec![
            make_row(b"a", &[("name", b"alice"), ("score", b"99")]),
            make_row(b"b", &[("name", b"bob"), ("score", b"88")]),
            make_row(b"c", &[("name", b"alice"), ("score", b"77")]),
            make_row(b"d", &[("name", b"alice"), ("score", b"66")]),
        ]);

        let mut fetched = BTreeSet::new();
        fetched.insert("name".to_string());
        let mut queried = BTreeSet::new();
        queried.insert("name".to_string());

        let options = ScanOptions {
            column_filter: Some(ColumnFilter::OnlyQueried { fetched, queried }),
            clustering_filter: Some(ClusteringIndexFilter::Slice(Slices {
                slices: vec![Slice {
                    start: ClusteringBound {
                        values: b"a".to_vec(),
                        inclusive: true,
                    },
                    end: ClusteringBound {
                        values: b"c".to_vec(),
                        inclusive: true,
                    },
                }],
                is_reversed: false,
            })),
            data_limits: Some(DataLimits::CqlLimit {
                rows_limit: 2,
                per_partition_limit: u32::MAX,
            }),
            row_filter: Some(
                RowFilter::none().with(FilterExpression::Simple {
                    column: "name".to_string(),
                    operator: Operator::Eq,
                    value: b"alice".to_vec(),
                }),
            ),
        };

        let filtered = apply_filters(pd, &options);
        // Clustering: a, b, c kept (d excluded)
        // Row filter: bob excluded -> a, c remain
        // Data limit: 2 rows -> a, c (exactly 2)
        // Column filter: only "name" column
        assert_eq!(filtered.rows.len(), 2);
        for (_, row) in &filtered.rows {
            assert_eq!(row.cells.len(), 1);
            assert_eq!(row.cells[0].column, "name");
            assert_eq!(row.cells[0].value.as_deref(), Some(b"alice".as_slice()));
        }
    }
}
