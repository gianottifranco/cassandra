// Licensed under Apache License, Version 2.0.

//! # Query Planners
//!
//! Query planners evaluate a `ReadCommand` (including its `RowFilter`) against
//! available schema/indexes to determine the optimal execution path.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.index.IndexRegistry`
//! - `org.apache.cassandra.index.sai.plan.QueryController`
//! - `org.apache.cassandra.cql3.statements.SelectStatement`

use crate::read::command::{Expression, Operator, RowFilter};

/// Represents the execution strategy chosen by the planner.
#[derive(Debug, Clone, PartialEq)]
pub enum QueryPlan {
    /// A sequential scan over the partition(s).
    FullScan,
    /// An index-assisted scan using the specified index name.
    IndexScan {
        index_name: String,
        expression: Expression,
    },
    /// An Approximate Nearest Neighbor (ANN) search using a Vector Index.
    AnnSearch {
        index_name: String,
        expression: Expression,
    },
}

/// Schema/index catalog interface for the planner to query index availability.
pub trait SchemaIndexCatalog {
    /// Returns the name of the index and its type (e.g. "sai_vector" or "legacy")
    /// if an index exists for the given column.
    fn get_index_for_column(
        &self,
        keyspace: &str,
        table: &str,
        column: &str,
    ) -> Option<(String, String)>;
}

pub struct QueryPlanner<'a> {
    catalog: &'a dyn SchemaIndexCatalog,
}

impl<'a> QueryPlanner<'a> {
    pub fn new(catalog: &'a dyn SchemaIndexCatalog) -> Self {
        Self { catalog }
    }

    /// Generates a query plan for the given keyspace, table, and row filter.
    ///
    /// Strategy:
    /// 1. ANN queries always take priority (vector search dictates ordering).
    /// 2. Among non-ANN candidates, rank by index type: SAI > Legacy.
    /// 3. Fallback to FullScan if no indexes match.
    pub fn plan_read(&self, keyspace: &str, table: &str, filter: &RowFilter) -> QueryPlan {
        if filter.is_empty() {
            return QueryPlan::FullScan;
        }

        // 1. Look for an ANN (Vector Search) query first, as it dictates ordering.
        for expr in &filter.expressions {
            if expr.operator == Operator::Ann {
                if let Some((idx_name, _)) =
                    self.catalog
                        .get_index_for_column(keyspace, table, &expr.column)
                {
                    return QueryPlan::AnnSearch {
                        index_name: idx_name,
                        expression: expr.clone(),
                    };
                }
            }
        }

        // 2. Collect all candidate indexes, then rank by type (SAI > Legacy).
        let mut candidates: Vec<(String, String, Expression)> = Vec::new();
        for expr in &filter.expressions {
            if let Some((idx_name, idx_type)) =
                self.catalog
                    .get_index_for_column(keyspace, table, &expr.column)
            {
                candidates.push((idx_name, idx_type, expr.clone()));
            }
        }

        if !candidates.is_empty() {
            // Sort: "sai" > anything else (like "legacy")
            candidates.sort_by(|a, b| {
                let rank = |t: &str| -> u8 {
                    match t {
                        "sai" => 2,
                        _ => 1, // legacy, sasi, etc.
                    }
                };
                rank(&b.1).cmp(&rank(&a.1))
            });

            let best = &candidates[0];
            return QueryPlan::IndexScan {
                index_name: best.0.clone(),
                expression: best.2.clone(),
            };
        }

        // Fallback
        QueryPlan::FullScan
    }
}

impl SchemaIndexCatalog for cassandra_schema::SchemaCatalog {
    fn get_index_for_column(
        &self,
        keyspace: &str,
        table: &str,
        column: &str,
    ) -> Option<(String, String)> {
        self.snapshot()
            .table(keyspace, table)?
            .indexes
            .iter()
            .find(|index| index.target_column().is_some_and(|target| target == column))
            .map(|index| (index.name.clone(), planner_index_type(index)))
    }
}

impl SchemaIndexCatalog for cassandra_schema::catalog::SchemaSnapshot {
    fn get_index_for_column(
        &self,
        keyspace: &str,
        table: &str,
        column: &str,
    ) -> Option<(String, String)> {
        self.table(keyspace, table)?
            .indexes
            .iter()
            .find(|index| index.target_column().is_some_and(|target| target == column))
            .map(|index| (index.name.clone(), planner_index_type(index)))
    }
}

fn planner_index_type(index: &cassandra_schema::index::IndexMetadata) -> String {
    match index.kind {
        cassandra_schema::index::IndexKind::Custom => index
            .index_class_name()
            .map(|class| {
                if class.contains("StorageAttachedIndex") {
                    "sai".to_string()
                } else if class.contains("SASI") {
                    "sasi".to_string()
                } else {
                    "custom".to_string()
                }
            })
            .unwrap_or_else(|| "custom".to_string()),
        cassandra_schema::index::IndexKind::Keys
        | cassandra_schema::index::IndexKind::Composites => "legacy".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct MockCatalog;
    impl SchemaIndexCatalog for MockCatalog {
        fn get_index_for_column(&self, ks: &str, tbl: &str, col: &str) -> Option<(String, String)> {
            if ks == "ks" && tbl == "t1" {
                match col {
                    "email" => Some(("email_idx".to_string(), "legacy".to_string())),
                    "embedding" => Some(("vec_idx".to_string(), "sai".to_string())),
                    _ => None,
                }
            } else {
                None
            }
        }
    }

    #[test]
    fn exact_match_index_scan() {
        let planner = QueryPlanner::new(&MockCatalog);
        let filter = RowFilter::default().add_expression(Expression {
            column: "email".to_string(),
            operator: Operator::Eq,
            value: b"alice@example.com".to_vec(),
        });

        let plan = planner.plan_read("ks", "t1", &filter);
        match plan {
            QueryPlan::IndexScan { index_name, .. } => assert_eq!(index_name, "email_idx"),
            _ => panic!("Expected IndexScan"),
        }
    }

    #[test]
    fn ann_search_plan() {
        let planner = QueryPlanner::new(&MockCatalog);
        let filter = RowFilter::default().add_expression(Expression {
            column: "embedding".to_string(),
            operator: Operator::Ann,
            value: vec![1, 2, 3], // dummy bytes
        });

        let plan = planner.plan_read("ks", "t1", &filter);
        match plan {
            QueryPlan::AnnSearch { index_name, .. } => assert_eq!(index_name, "vec_idx"),
            _ => panic!("Expected AnnSearch"),
        }
    }

    #[test]
    fn full_scan_on_no_index() {
        let planner = QueryPlanner::new(&MockCatalog);
        let filter = RowFilter::default().add_expression(Expression {
            column: "age".to_string(),
            operator: Operator::Eq,
            value: b"30".to_vec(),
        });

        let plan = planner.plan_read("ks", "t1", &filter);
        assert_eq!(plan, QueryPlan::FullScan);
    }

    /// Mock catalog with multiple indexes for best-index selection testing.
    struct MultiIndexCatalog;
    impl SchemaIndexCatalog for MultiIndexCatalog {
        fn get_index_for_column(&self, ks: &str, tbl: &str, col: &str) -> Option<(String, String)> {
            if ks == "ks" && tbl == "t1" {
                match col {
                    "email" => Some(("email_legacy_idx".to_string(), "legacy".to_string())),
                    "name" => Some(("name_sai_idx".to_string(), "sai".to_string())),
                    _ => None,
                }
            } else {
                None
            }
        }
    }

    #[test]
    fn best_index_prefers_sai_over_legacy() {
        let planner = QueryPlanner::new(&MultiIndexCatalog);
        // Filter references both legacy-indexed and sai-indexed columns
        let filter = RowFilter::default()
            .add_expression(Expression {
                column: "email".to_string(),
                operator: Operator::Eq,
                value: b"alice@example.com".to_vec(),
            })
            .add_expression(Expression {
                column: "name".to_string(),
                operator: Operator::Eq,
                value: b"Alice".to_vec(),
            });

        let plan = planner.plan_read("ks", "t1", &filter);
        match plan {
            QueryPlan::IndexScan { index_name, .. } => {
                assert_eq!(index_name, "name_sai_idx", "should prefer SAI over legacy");
            }
            _ => panic!("Expected IndexScan"),
        }
    }

    #[test]
    fn ann_still_takes_priority_over_sai() {
        // ANN on a non-indexed column won't match, but let's use a catalog that has vector
        struct VecCatalog;
        impl SchemaIndexCatalog for VecCatalog {
            fn get_index_for_column(
                &self,
                _ks: &str,
                _tbl: &str,
                col: &str,
            ) -> Option<(String, String)> {
                match col {
                    "vec" => Some(("vec_idx".to_string(), "sai".to_string())),
                    "name" => Some(("name_sai".to_string(), "sai".to_string())),
                    _ => None,
                }
            }
        }

        let planner = QueryPlanner::new(&VecCatalog);
        let filter = RowFilter::default()
            .add_expression(Expression {
                column: "vec".to_string(),
                operator: Operator::Ann,
                value: vec![1, 2, 3],
            })
            .add_expression(Expression {
                column: "name".to_string(),
                operator: Operator::Eq,
                value: b"Alice".to_vec(),
            });

        let plan = planner.plan_read("ks", "t1", &filter);
        match plan {
            QueryPlan::AnnSearch { index_name, .. } => {
                assert_eq!(index_name, "vec_idx", "ANN should take priority");
            }
            _ => panic!("Expected AnnSearch"),
        }
    }
}
