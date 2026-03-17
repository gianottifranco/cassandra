// Licensed under Apache License, Version 2.0.

//! Accord-style multi-partition IF conditions.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.cql3.statements.ConditionStatement`
//!
//! Unlike Paxos LWT conditions (which are per-partition IF clauses),
//! Accord condition statements can span multiple partitions and reference
//! LET-bound variables from TransactionStatement.

use crate::ast::Relation;

/// A condition referencing a LET-bound variable.
#[derive(Debug, Clone, PartialEq)]
pub struct VariableCondition {
    /// Name of the LET-bound variable.
    pub variable: String,
    /// Column within the variable's result set.
    pub column: String,
    /// The condition relation.
    pub relation: Relation,
}

/// Multi-partition condition for Accord transactions.
///
/// Can reference variables from LET bindings in a TransactionStatement,
/// enabling cross-partition conditional logic.
#[derive(Debug, Clone, PartialEq)]
pub struct ConditionStatement {
    /// Standard column conditions (same as LWT IF).
    pub column_conditions: Vec<Relation>,
    /// Variable-reference conditions (Accord-specific).
    pub variable_conditions: Vec<VariableCondition>,
}

impl ConditionStatement {
    pub fn new() -> Self {
        Self {
            column_conditions: Vec::new(),
            variable_conditions: Vec::new(),
        }
    }

    /// Add a standard column condition.
    pub fn add_column_condition(&mut self, condition: Relation) {
        self.column_conditions.push(condition);
    }

    /// Add a variable-reference condition.
    pub fn add_variable_condition(&mut self, condition: VariableCondition) {
        self.variable_conditions.push(condition);
    }

    /// Check if this statement has any conditions.
    pub fn is_empty(&self) -> bool {
        self.column_conditions.is_empty() && self.variable_conditions.is_empty()
    }

    /// Total number of conditions.
    pub fn len(&self) -> usize {
        self.column_conditions.len() + self.variable_conditions.len()
    }

    /// Check if this uses any variable references (Accord-only feature).
    pub fn has_variable_conditions(&self) -> bool {
        !self.variable_conditions.is_empty()
    }
}

impl Default for ConditionStatement {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{Literal, RelationOp, Term};

    #[test]
    fn empty_condition_statement() {
        let cs = ConditionStatement::new();
        assert!(cs.is_empty());
        assert_eq!(cs.len(), 0);
        assert!(!cs.has_variable_conditions());
    }

    #[test]
    fn add_column_condition() {
        let mut cs = ConditionStatement::new();
        cs.add_column_condition(Relation {
            column: "age".to_string(),
            op: RelationOp::Gt,
            value: Term::Literal(Literal::Integer(18)),
        });
        assert_eq!(cs.len(), 1);
        assert!(!cs.is_empty());
        assert!(!cs.has_variable_conditions());
    }

    #[test]
    fn add_variable_condition() {
        let mut cs = ConditionStatement::new();
        cs.add_variable_condition(VariableCondition {
            variable: "row1".to_string(),
            column: "balance".to_string(),
            relation: Relation {
                column: "balance".to_string(),
                op: RelationOp::Gte,
                value: Term::Literal(Literal::Integer(100)),
            },
        });
        assert_eq!(cs.len(), 1);
        assert!(cs.has_variable_conditions());
    }

    #[test]
    fn mixed_conditions() {
        let mut cs = ConditionStatement::new();
        cs.add_column_condition(Relation {
            column: "status".to_string(),
            op: RelationOp::Eq,
            value: Term::Literal(Literal::String("active".to_string())),
        });
        cs.add_variable_condition(VariableCondition {
            variable: "ref".to_string(),
            column: "id".to_string(),
            relation: Relation {
                column: "id".to_string(),
                op: RelationOp::Eq,
                value: Term::Literal(Literal::Integer(42)),
            },
        });
        assert_eq!(cs.len(), 2);
        assert!(cs.has_variable_conditions());
    }
}
