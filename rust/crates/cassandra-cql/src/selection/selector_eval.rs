// Licensed under Apache License, Version 2.0.

//! Selector evaluation against row data.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.cql3.selection.Selector`

use crate::ast::Selector;
use crate::functions::FunctionRegistry;
use std::collections::HashMap;

/// Cell-level metadata for WRITETIME/TTL selectors.
#[derive(Debug, Clone, Default)]
pub struct CellMeta {
    pub timestamp: Option<i64>,
    pub ttl: Option<i32>,
}

/// Evaluates selectors against row data.
pub struct SelectorEvaluator<'a> {
    registry: &'a FunctionRegistry,
}

impl<'a> SelectorEvaluator<'a> {
    pub fn new(registry: &'a FunctionRegistry) -> Self {
        Self { registry }
    }

    /// Evaluate a single selector against a row.
    ///
    /// `columns` maps column names to their index in the row data.
    /// `row` contains serialized cell values.
    /// `cell_metadata` optionally maps column names to their cell-level metadata
    /// (timestamp, TTL) for WRITETIME/TTL selectors.
    pub fn evaluate(
        &self,
        selector: &Selector,
        columns: &HashMap<String, usize>,
        row: &[Option<Vec<u8>>],
        cell_metadata: Option<&HashMap<String, CellMeta>>,
    ) -> Option<Vec<u8>> {
        match selector {
            Selector::Column(name) => {
                let idx = columns.get(name.as_str())?;
                row.get(*idx)?.clone()
            }
            Selector::Function(name, args) => {
                let func = self.registry.resolve_by_name(name)?;
                let mut arg_values: Vec<Option<Vec<u8>>> = Vec::with_capacity(args.len());
                for arg in args {
                    arg_values.push(self.evaluate(arg, columns, row, cell_metadata));
                }

                let arg_refs: Vec<Option<&[u8]>> =
                    arg_values.iter().map(|v| v.as_deref()).collect();

                func.execute(&arg_refs).unwrap_or_default()
            }
            Selector::Cast { selector, target } => {
                let value = self.evaluate(selector, columns, row, cell_metadata);
                let target = target.resolve()?;
                let func = self.registry.resolve_with_return("cast", &[], &target)?;
                let arg_refs = [value.as_deref()];
                func.execute(&arg_refs).unwrap_or_default()
            }
            Selector::Alias { selector, .. } => {
                self.evaluate(selector, columns, row, cell_metadata)
            }
            Selector::Count => {
                // Count returns 1 for each row (aggregation handles the sum)
                Some(1i64.to_be_bytes().to_vec())
            }
            Selector::WritetimeOrTtl(kind, col) => {
                let meta = cell_metadata?.get(col.as_str())?;
                match kind.as_str() {
                    "writetime" | "maxwritetime" => {
                        let ts = meta.timestamp?;
                        Some(ts.to_be_bytes().to_vec())
                    }
                    "ttl" => {
                        let ttl = meta.ttl?;
                        Some(ttl.to_be_bytes().to_vec())
                    }
                    _ => None,
                }
            }
        }
    }

    /// Evaluate all selectors for a row, returning the projected values.
    pub fn evaluate_row(
        &self,
        selectors: &[Selector],
        columns: &HashMap<String, usize>,
        row: &[Option<Vec<u8>>],
        cell_metadata: Option<&HashMap<String, CellMeta>>,
    ) -> Vec<Option<Vec<u8>>> {
        selectors
            .iter()
            .map(|sel| self.evaluate(sel, columns, row, cell_metadata))
            .collect()
    }

    /// Get the output column name for a selector.
    pub fn output_name(selector: &Selector) -> String {
        match selector {
            Selector::Column(name) => name.clone(),
            Selector::Function(name, _) => format!("{}(...)", name),
            Selector::Cast { selector, target } => {
                let target = target
                    .resolve()
                    .map(|target| target.cql_name())
                    .unwrap_or_else(|| "unknown".to_string());
                format!("cast({} as {})", Self::output_name(selector), target)
            }
            Selector::Alias { alias, .. } => alias.clone(),
            Selector::Count => "count".to_string(),
            Selector::WritetimeOrTtl(kind, col) => format!("{}({})", kind, col),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::functions::FunctionRegistry;

    fn make_columns() -> HashMap<String, usize> {
        let mut m = HashMap::new();
        m.insert("id".to_string(), 0);
        m.insert("name".to_string(), 1);
        m.insert("age".to_string(), 2);
        m
    }

    fn make_row() -> Vec<Option<Vec<u8>>> {
        vec![
            Some(1i32.to_be_bytes().to_vec()),
            Some(b"Alice".to_vec()),
            Some(30i32.to_be_bytes().to_vec()),
        ]
    }

    #[test]
    fn eval_column() {
        let registry = FunctionRegistry::new();
        let eval = SelectorEvaluator::new(&registry);
        let cols = make_columns();
        let row = make_row();

        let result = eval.evaluate(&Selector::Column("name".into()), &cols, &row, None);
        assert_eq!(result, Some(b"Alice".to_vec()));
    }

    #[test]
    fn eval_column_missing() {
        let registry = FunctionRegistry::new();
        let eval = SelectorEvaluator::new(&registry);
        let cols = make_columns();
        let row = make_row();

        let result = eval.evaluate(&Selector::Column("nonexistent".into()), &cols, &row, None);
        assert!(result.is_none());
    }

    #[test]
    fn eval_alias() {
        let registry = FunctionRegistry::new();
        let eval = SelectorEvaluator::new(&registry);
        let cols = make_columns();
        let row = make_row();

        let sel = Selector::Alias {
            selector: Box::new(Selector::Column("name".into())),
            alias: "user_name".into(),
        };
        let result = eval.evaluate(&sel, &cols, &row, None);
        assert_eq!(result, Some(b"Alice".to_vec()));
    }

    #[test]
    fn eval_count() {
        let registry = FunctionRegistry::new();
        let eval = SelectorEvaluator::new(&registry);
        let cols = make_columns();
        let row = make_row();

        let result = eval.evaluate(&Selector::Count, &cols, &row, None);
        assert_eq!(result, Some(1i64.to_be_bytes().to_vec()));
    }

    #[test]
    fn eval_function() {
        let registry = FunctionRegistry::with_builtins();
        let eval = SelectorEvaluator::new(&registry);
        let cols = make_columns();
        let row = make_row();

        // Call now() which takes no args from the row
        let sel = Selector::Function("now".into(), vec![]);
        let result = eval.evaluate(&sel, &cols, &row, None);
        assert!(result.is_some());
        assert_eq!(result.unwrap().len(), 16); // timeuuid = 16 bytes
    }

    #[test]
    fn eval_row_multiple() {
        let registry = FunctionRegistry::new();
        let eval = SelectorEvaluator::new(&registry);
        let cols = make_columns();
        let row = make_row();

        let selectors = vec![
            Selector::Column("id".into()),
            Selector::Column("name".into()),
        ];
        let results = eval.evaluate_row(&selectors, &cols, &row, None);
        assert_eq!(results.len(), 2);
        assert_eq!(results[0], Some(1i32.to_be_bytes().to_vec()));
        assert_eq!(results[1], Some(b"Alice".to_vec()));
    }

    #[test]
    fn output_names() {
        assert_eq!(
            SelectorEvaluator::output_name(&Selector::Column("id".into())),
            "id"
        );
        assert_eq!(
            SelectorEvaluator::output_name(&Selector::Alias {
                selector: Box::new(Selector::Column("id".into())),
                alias: "user_id".into()
            }),
            "user_id"
        );
        assert_eq!(SelectorEvaluator::output_name(&Selector::Count), "count");
    }
}
