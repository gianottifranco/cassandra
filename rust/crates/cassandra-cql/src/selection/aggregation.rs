// Licensed under Apache License, Version 2.0.

//! Aggregation pipeline for aggregate selectors.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.cql3.selection.Selection`

use crate::ast::Selector;
use crate::functions::FunctionRegistry;

/// Detects whether a selector list includes aggregate functions.
pub fn has_aggregates(selectors: &[Selector]) -> bool {
    selectors.iter().any(is_aggregate)
}

fn is_aggregate(selector: &Selector) -> bool {
    match selector {
        Selector::Count => true,
        Selector::Function(name, _) => {
            let lower = name.to_lowercase();
            matches!(
                lower.as_str(),
                "count" | "count_rows" | "countrows" | "sum" | "avg" | "min" | "max"
            )
        }
        Selector::Cast { .. } => false,
        Selector::Alias { selector, .. } => is_aggregate(selector),
        _ => false,
    }
}

/// Pipeline that accumulates aggregate state across rows.
pub struct AggregationPipeline {
    /// For each selector position, the aggregate function + its current state.
    states: Vec<AggregateState>,
}

enum AggregateState {
    /// A non-aggregate selector: just keeps the last value seen.
    PassThrough(Option<Vec<u8>>),
    /// Count(*) special case.
    CountStar { count: i64 },
}

impl AggregationPipeline {
    pub fn new(selectors: &[Selector], _registry: &FunctionRegistry) -> Self {
        let states = selectors
            .iter()
            .map(|sel| match sel {
                Selector::Count => AggregateState::CountStar { count: 0 },
                Selector::Function(name, _) => {
                    let lower = name.to_lowercase();
                    if matches!(lower.as_str(), "count" | "count_rows" | "countrows") {
                        AggregateState::CountStar { count: 0 }
                    } else {
                        // Non-count aggregates are resolved by the registry-backed pipeline.
                        AggregateState::PassThrough(None)
                    }
                }
                Selector::Cast { .. } => AggregateState::PassThrough(None),
                _ => AggregateState::PassThrough(None),
            })
            .collect();

        Self { states }
    }

    /// Accumulate a row's evaluated values.
    pub fn accumulate(&mut self, values: &[Option<Vec<u8>>]) {
        for (i, state) in self.states.iter_mut().enumerate() {
            let value = values.get(i).and_then(|v| v.as_ref());
            match state {
                AggregateState::CountStar { count } => {
                    *count += 1;
                }
                AggregateState::PassThrough(v) => {
                    if let Some(val) = value {
                        *v = Some(val.clone());
                    }
                }
            }
        }
    }

    /// Produce the final aggregated row.
    pub fn finalize(self) -> Vec<Option<Vec<u8>>> {
        self.states
            .into_iter()
            .map(|state| match state {
                AggregateState::CountStar { count } => Some(count.to_be_bytes().to_vec()),
                AggregateState::PassThrough(v) => v,
            })
            .collect()
    }
}

/// Simple count-based aggregation that doesn't need the full pipeline.
pub fn simple_count(row_count: i64) -> Vec<Option<Vec<u8>>> {
    vec![Some(row_count.to_be_bytes().to_vec())]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detect_aggregates() {
        assert!(has_aggregates(&[Selector::Count]));
        assert!(has_aggregates(&[Selector::Function(
            "sum".into(),
            vec![Selector::Column("x".into())]
        )]));
        assert!(has_aggregates(&[Selector::Function(
            "count_rows".into(),
            Vec::new()
        )]));
        assert!(has_aggregates(&[Selector::Function(
            "countRows".into(),
            Vec::new()
        )]));
        assert!(!has_aggregates(&[Selector::Column("x".into())]));
    }

    #[test]
    fn count_star_aggregation() {
        let registry = FunctionRegistry::new();
        let selectors = vec![Selector::Count];
        let mut pipeline = AggregationPipeline::new(&selectors, &registry);

        // Feed 5 rows
        for _ in 0..5 {
            pipeline.accumulate(&[Some(1i64.to_be_bytes().to_vec())]);
        }

        let result = pipeline.finalize();
        assert_eq!(result.len(), 1);
        let count = i64::from_be_bytes(result[0].as_ref().unwrap().as_slice().try_into().unwrap());
        assert_eq!(count, 5);
    }

    #[test]
    fn mixed_aggregate_passthrough() {
        let registry = FunctionRegistry::new();
        let selectors = vec![Selector::Column("name".into()), Selector::Count];
        let mut pipeline = AggregationPipeline::new(&selectors, &registry);

        pipeline.accumulate(&[Some(b"Alice".to_vec()), Some(1i64.to_be_bytes().to_vec())]);
        pipeline.accumulate(&[Some(b"Bob".to_vec()), Some(1i64.to_be_bytes().to_vec())]);

        let result = pipeline.finalize();
        assert_eq!(result.len(), 2);
        // PassThrough keeps last value
        assert_eq!(result[0], Some(b"Bob".to_vec()));
        // Count = 2
        let count = i64::from_be_bytes(result[1].as_ref().unwrap().as_slice().try_into().unwrap());
        assert_eq!(count, 2);
    }
}
