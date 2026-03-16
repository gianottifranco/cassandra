// Licensed under Apache License, Version 2.0.

//! Rich row filter with typed expressions.
//!
//! ## Java Oracle
//! `org.apache.cassandra.db.filter.RowFilter`

/// Comparison operator for filter expressions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Operator {
    Eq,
    Lt,
    Lte,
    Gt,
    Gte,
    Neq,
    Contains,
    ContainsKey,
    /// Approximate Nearest Neighbor (for vector search).
    Ann,
}

/// A filter expression.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FilterExpression {
    /// Simple column comparison: column op value.
    Simple {
        column: String,
        operator: Operator,
        value: Vec<u8>,
    },
    /// Map equality: column[key] = value.
    MapEquality {
        column: String,
        key: Vec<u8>,
        value: Vec<u8>,
    },
    /// Custom expression (for SAI/SASI index).
    Custom {
        column: String,
        operator: Operator,
        value: Vec<u8>,
        index_name: String,
    },
}

impl FilterExpression {
    /// The column this expression applies to.
    pub fn column(&self) -> &str {
        match self {
            Self::Simple { column, .. }
            | Self::MapEquality { column, .. }
            | Self::Custom { column, .. } => column,
        }
    }
}

/// Rich row filter: a conjunction (AND) of filter expressions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowFilter {
    pub expressions: Vec<FilterExpression>,
}

impl RowFilter {
    /// Empty filter (matches everything).
    pub fn none() -> Self {
        Self {
            expressions: Vec::new(),
        }
    }

    /// Returns `true` if this filter has no expressions.
    pub fn is_empty(&self) -> bool {
        self.expressions.is_empty()
    }

    /// Add an expression to this filter.
    pub fn add(&mut self, expr: FilterExpression) {
        self.expressions.push(expr);
    }

    /// Builder pattern: add and return self.
    pub fn with(mut self, expr: FilterExpression) -> Self {
        self.add(expr);
        self
    }

    /// Get all columns referenced by filter expressions.
    pub fn columns(&self) -> Vec<&str> {
        self.expressions.iter().map(|e| e.column()).collect()
    }
}

impl Default for RowFilter {
    fn default() -> Self {
        Self::none()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_filter() {
        let f = RowFilter::none();
        assert!(f.is_empty());
    }

    #[test]
    fn add_expressions() {
        let f = RowFilter::none()
            .with(FilterExpression::Simple {
                column: "age".to_string(),
                operator: Operator::Gt,
                value: vec![0, 0, 0, 21],
            })
            .with(FilterExpression::Simple {
                column: "name".to_string(),
                operator: Operator::Eq,
                value: b"alice".to_vec(),
            });
        assert_eq!(f.expressions.len(), 2);
        assert!(!f.is_empty());
    }

    #[test]
    fn columns() {
        let f = RowFilter::none()
            .with(FilterExpression::Simple {
                column: "age".to_string(),
                operator: Operator::Gt,
                value: vec![],
            })
            .with(FilterExpression::MapEquality {
                column: "props".to_string(),
                key: b"color".to_vec(),
                value: b"red".to_vec(),
            });
        let cols = f.columns();
        assert_eq!(cols, vec!["age", "props"]);
    }

    #[test]
    fn custom_expression() {
        let f = RowFilter::none().with(FilterExpression::Custom {
            column: "embedding".to_string(),
            operator: Operator::Ann,
            value: vec![1, 2, 3],
            index_name: "my_sai_index".to_string(),
        });
        assert_eq!(f.expressions[0].column(), "embedding");
    }
}
