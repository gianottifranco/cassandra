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

/// A stub schema catalog interface for the planner to query index availability.
pub trait SchemaCatalogStub {
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
    catalog: &'a dyn SchemaCatalogStub,
}

impl<'a> QueryPlanner<'a> {
    pub fn new(catalog: &'a dyn SchemaCatalogStub) -> Self {
        Self { catalog }
    }

    /// Generates a query plan for the given keyspace, table, and row filter.
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

        // 2. Look for strict equality or bounding exact matches.
        // In a real planner, we'd rank indexes by selectivity or index type (SAI > SASI > Legacy).
        for expr in &filter.expressions {
            if let Some((idx_name, _)) =
                self.catalog
                    .get_index_for_column(keyspace, table, &expr.column)
            {
                return QueryPlan::IndexScan {
                    index_name: idx_name,
                    expression: expr.clone(),
                };
            }
        }

        // Fallback
        QueryPlan::FullScan
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct MockCatalog;
    impl SchemaCatalogStub for MockCatalog {
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
}
