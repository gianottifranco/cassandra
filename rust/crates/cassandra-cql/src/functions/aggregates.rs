// Licensed under Apache License, Version 2.0.

//! Aggregate functions: count, sum, avg, min, max.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.cql3.functions.AggregateFcts`

use super::registry::CqlFunction;
use cassandra_types::CqlType;

/// Aggregate function trait: extends CqlFunction with state accumulation.
pub trait AggregateFunction: CqlFunction {
    /// Initial accumulator state (serialized bytes).
    fn init_state(&self) -> Option<Vec<u8>>;

    /// Accumulate a value into state. Returns new state.
    fn accumulate(
        &self,
        state: Option<&[u8]>,
        value: Option<&[u8]>,
    ) -> Result<Option<Vec<u8>>, String>;

    /// Finalize state into result.
    fn finalize(&self, state: Option<&[u8]>) -> Result<Option<Vec<u8>>, String>;
}

// ── Count(*) ──────────────────────────────────────────────────────────

pub struct CountStarAggregate;

impl CqlFunction for CountStarAggregate {
    fn name(&self) -> &str {
        "count"
    }
    fn arg_types(&self) -> Vec<CqlType> {
        vec![]
    }
    fn return_type(&self) -> CqlType {
        CqlType::Bigint
    }
    fn execute(&self, _args: &[Option<&[u8]>]) -> Result<Option<Vec<u8>>, String> {
        // For scalar execution, count(*) just returns 1 for each row
        Ok(Some(1i64.to_be_bytes().to_vec()))
    }
}

impl AggregateFunction for CountStarAggregate {
    fn init_state(&self) -> Option<Vec<u8>> {
        Some(0i64.to_be_bytes().to_vec())
    }

    fn accumulate(
        &self,
        state: Option<&[u8]>,
        _value: Option<&[u8]>,
    ) -> Result<Option<Vec<u8>>, String> {
        let count = state
            .and_then(|b| b.try_into().ok())
            .map(i64::from_be_bytes)
            .unwrap_or(0);
        Ok(Some((count + 1).to_be_bytes().to_vec()))
    }

    fn finalize(&self, state: Option<&[u8]>) -> Result<Option<Vec<u8>>, String> {
        Ok(state.map(|s| s.to_vec()))
    }
}

// ── Count(col) ────────────────────────────────────────────────────────

pub struct CountColumnAggregate;

impl CqlFunction for CountColumnAggregate {
    fn name(&self) -> &str {
        "count"
    }
    fn arg_types(&self) -> Vec<CqlType> {
        vec![CqlType::Blob] // accepts any type
    }
    fn return_type(&self) -> CqlType {
        CqlType::Bigint
    }
    fn execute(&self, args: &[Option<&[u8]>]) -> Result<Option<Vec<u8>>, String> {
        let val = if args.first().and_then(|a| *a).is_some() {
            1i64
        } else {
            0i64
        };
        Ok(Some(val.to_be_bytes().to_vec()))
    }
}

impl AggregateFunction for CountColumnAggregate {
    fn init_state(&self) -> Option<Vec<u8>> {
        Some(0i64.to_be_bytes().to_vec())
    }

    fn accumulate(
        &self,
        state: Option<&[u8]>,
        value: Option<&[u8]>,
    ) -> Result<Option<Vec<u8>>, String> {
        let count = state
            .and_then(|b| b.try_into().ok())
            .map(i64::from_be_bytes)
            .unwrap_or(0);
        let inc = if value.is_some() { 1 } else { 0 };
        Ok(Some((count + inc).to_be_bytes().to_vec()))
    }

    fn finalize(&self, state: Option<&[u8]>) -> Result<Option<Vec<u8>>, String> {
        Ok(state.map(|s| s.to_vec()))
    }
}

// ── Sum ───────────────────────────────────────────────────────────────

macro_rules! define_sum {
    ($name:ident, $cql_type:expr, $rust_type:ty) => {
        pub struct $name;

        impl CqlFunction for $name {
            fn name(&self) -> &str {
                "sum"
            }
            fn arg_types(&self) -> Vec<CqlType> {
                vec![$cql_type]
            }
            fn return_type(&self) -> CqlType {
                $cql_type
            }
            fn execute(&self, args: &[Option<&[u8]>]) -> Result<Option<Vec<u8>>, String> {
                match args.first().and_then(|a| *a) {
                    Some(b) => Ok(Some(b.to_vec())),
                    None => Ok(None),
                }
            }
        }

        impl AggregateFunction for $name {
            fn init_state(&self) -> Option<Vec<u8>> {
                Some((0 as $rust_type).to_be_bytes().to_vec())
            }

            fn accumulate(
                &self,
                state: Option<&[u8]>,
                value: Option<&[u8]>,
            ) -> Result<Option<Vec<u8>>, String> {
                let s = state
                    .and_then(|b| b.try_into().ok())
                    .map(<$rust_type>::from_be_bytes)
                    .unwrap_or(0 as $rust_type);
                let v = value
                    .and_then(|b| b.try_into().ok())
                    .map(<$rust_type>::from_be_bytes)
                    .unwrap_or(0 as $rust_type);
                Ok(Some((s + v).to_be_bytes().to_vec()))
            }

            fn finalize(&self, state: Option<&[u8]>) -> Result<Option<Vec<u8>>, String> {
                Ok(state.map(|s| s.to_vec()))
            }
        }
    };
}

define_sum!(SumInt, CqlType::Int, i32);
define_sum!(SumBigint, CqlType::Bigint, i64);

pub struct SumFloat;

impl CqlFunction for SumFloat {
    fn name(&self) -> &str {
        "sum"
    }
    fn arg_types(&self) -> Vec<CqlType> {
        vec![CqlType::Float]
    }
    fn return_type(&self) -> CqlType {
        CqlType::Float
    }
    fn execute(&self, args: &[Option<&[u8]>]) -> Result<Option<Vec<u8>>, String> {
        match args.first().and_then(|a| *a) {
            Some(b) => Ok(Some(b.to_vec())),
            None => Ok(None),
        }
    }
}

impl AggregateFunction for SumFloat {
    fn init_state(&self) -> Option<Vec<u8>> {
        Some(0.0f32.to_be_bytes().to_vec())
    }

    fn accumulate(
        &self,
        state: Option<&[u8]>,
        value: Option<&[u8]>,
    ) -> Result<Option<Vec<u8>>, String> {
        let s = state
            .and_then(|b| b.try_into().ok())
            .map(f32::from_be_bytes)
            .unwrap_or(0.0);
        let v = value
            .and_then(|b| b.try_into().ok())
            .map(f32::from_be_bytes)
            .unwrap_or(0.0);
        Ok(Some((s + v).to_be_bytes().to_vec()))
    }

    fn finalize(&self, state: Option<&[u8]>) -> Result<Option<Vec<u8>>, String> {
        Ok(state.map(|s| s.to_vec()))
    }
}

pub struct SumDouble;

impl CqlFunction for SumDouble {
    fn name(&self) -> &str {
        "sum"
    }
    fn arg_types(&self) -> Vec<CqlType> {
        vec![CqlType::Double]
    }
    fn return_type(&self) -> CqlType {
        CqlType::Double
    }
    fn execute(&self, args: &[Option<&[u8]>]) -> Result<Option<Vec<u8>>, String> {
        match args.first().and_then(|a| *a) {
            Some(b) => Ok(Some(b.to_vec())),
            None => Ok(None),
        }
    }
}

impl AggregateFunction for SumDouble {
    fn init_state(&self) -> Option<Vec<u8>> {
        Some(0.0f64.to_be_bytes().to_vec())
    }

    fn accumulate(
        &self,
        state: Option<&[u8]>,
        value: Option<&[u8]>,
    ) -> Result<Option<Vec<u8>>, String> {
        let s = state
            .and_then(|b| b.try_into().ok())
            .map(f64::from_be_bytes)
            .unwrap_or(0.0);
        let v = value
            .and_then(|b| b.try_into().ok())
            .map(f64::from_be_bytes)
            .unwrap_or(0.0);
        Ok(Some((s + v).to_be_bytes().to_vec()))
    }

    fn finalize(&self, state: Option<&[u8]>) -> Result<Option<Vec<u8>>, String> {
        Ok(state.map(|s| s.to_vec()))
    }
}

// ── Avg ───────────────────────────────────────────────────────────────
// State: (sum: f64, count: i64) = 16 bytes

pub struct AvgInt;

impl CqlFunction for AvgInt {
    fn name(&self) -> &str {
        "avg"
    }
    fn arg_types(&self) -> Vec<CqlType> {
        vec![CqlType::Int]
    }
    fn return_type(&self) -> CqlType {
        CqlType::Int
    }
    fn execute(&self, args: &[Option<&[u8]>]) -> Result<Option<Vec<u8>>, String> {
        match args.first().and_then(|a| *a) {
            Some(b) => Ok(Some(b.to_vec())),
            None => Ok(None),
        }
    }
}

impl AggregateFunction for AvgInt {
    fn init_state(&self) -> Option<Vec<u8>> {
        let mut state = Vec::with_capacity(16);
        state.extend_from_slice(&0.0f64.to_be_bytes());
        state.extend_from_slice(&0i64.to_be_bytes());
        Some(state)
    }

    fn accumulate(
        &self,
        state: Option<&[u8]>,
        value: Option<&[u8]>,
    ) -> Result<Option<Vec<u8>>, String> {
        let (sum, count) = decode_avg_state(state);
        let v = value
            .and_then(|b| b.try_into().ok())
            .map(|b| i32::from_be_bytes(b) as f64)
            .unwrap_or(0.0);
        Ok(Some(encode_avg_state(sum + v, count + 1)))
    }

    fn finalize(&self, state: Option<&[u8]>) -> Result<Option<Vec<u8>>, String> {
        let (sum, count) = decode_avg_state(state);
        if count == 0 {
            return Ok(Some(0i32.to_be_bytes().to_vec()));
        }
        Ok(Some(((sum / count as f64) as i32).to_be_bytes().to_vec()))
    }
}

pub struct AvgBigint;

impl CqlFunction for AvgBigint {
    fn name(&self) -> &str {
        "avg"
    }
    fn arg_types(&self) -> Vec<CqlType> {
        vec![CqlType::Bigint]
    }
    fn return_type(&self) -> CqlType {
        CqlType::Bigint
    }
    fn execute(&self, args: &[Option<&[u8]>]) -> Result<Option<Vec<u8>>, String> {
        match args.first().and_then(|a| *a) {
            Some(b) => Ok(Some(b.to_vec())),
            None => Ok(None),
        }
    }
}

impl AggregateFunction for AvgBigint {
    fn init_state(&self) -> Option<Vec<u8>> {
        let mut state = Vec::with_capacity(16);
        state.extend_from_slice(&0.0f64.to_be_bytes());
        state.extend_from_slice(&0i64.to_be_bytes());
        Some(state)
    }

    fn accumulate(
        &self,
        state: Option<&[u8]>,
        value: Option<&[u8]>,
    ) -> Result<Option<Vec<u8>>, String> {
        let (sum, count) = decode_avg_state(state);
        let v = value
            .and_then(|b| b.try_into().ok())
            .map(|b| i64::from_be_bytes(b) as f64)
            .unwrap_or(0.0);
        Ok(Some(encode_avg_state(sum + v, count + 1)))
    }

    fn finalize(&self, state: Option<&[u8]>) -> Result<Option<Vec<u8>>, String> {
        let (sum, count) = decode_avg_state(state);
        if count == 0 {
            return Ok(Some(0i64.to_be_bytes().to_vec()));
        }
        Ok(Some(((sum / count as f64) as i64).to_be_bytes().to_vec()))
    }
}

pub struct AvgFloat;

impl CqlFunction for AvgFloat {
    fn name(&self) -> &str {
        "avg"
    }
    fn arg_types(&self) -> Vec<CqlType> {
        vec![CqlType::Float]
    }
    fn return_type(&self) -> CqlType {
        CqlType::Float
    }
    fn execute(&self, args: &[Option<&[u8]>]) -> Result<Option<Vec<u8>>, String> {
        match args.first().and_then(|a| *a) {
            Some(b) => Ok(Some(b.to_vec())),
            None => Ok(None),
        }
    }
}

impl AggregateFunction for AvgFloat {
    fn init_state(&self) -> Option<Vec<u8>> {
        let mut state = Vec::with_capacity(16);
        state.extend_from_slice(&0.0f64.to_be_bytes());
        state.extend_from_slice(&0i64.to_be_bytes());
        Some(state)
    }

    fn accumulate(
        &self,
        state: Option<&[u8]>,
        value: Option<&[u8]>,
    ) -> Result<Option<Vec<u8>>, String> {
        let (sum, count) = decode_avg_state(state);
        let v = value
            .and_then(|b| b.try_into().ok())
            .map(|b| f32::from_be_bytes(b) as f64)
            .unwrap_or(0.0);
        Ok(Some(encode_avg_state(sum + v, count + 1)))
    }

    fn finalize(&self, state: Option<&[u8]>) -> Result<Option<Vec<u8>>, String> {
        let (sum, count) = decode_avg_state(state);
        if count == 0 {
            return Ok(Some(0.0f32.to_be_bytes().to_vec()));
        }
        Ok(Some(((sum / count as f64) as f32).to_be_bytes().to_vec()))
    }
}

pub struct AvgDouble;

impl CqlFunction for AvgDouble {
    fn name(&self) -> &str {
        "avg"
    }
    fn arg_types(&self) -> Vec<CqlType> {
        vec![CqlType::Double]
    }
    fn return_type(&self) -> CqlType {
        CqlType::Double
    }
    fn execute(&self, args: &[Option<&[u8]>]) -> Result<Option<Vec<u8>>, String> {
        match args.first().and_then(|a| *a) {
            Some(b) => Ok(Some(b.to_vec())),
            None => Ok(None),
        }
    }
}

impl AggregateFunction for AvgDouble {
    fn init_state(&self) -> Option<Vec<u8>> {
        let mut state = Vec::with_capacity(16);
        state.extend_from_slice(&0.0f64.to_be_bytes());
        state.extend_from_slice(&0i64.to_be_bytes());
        Some(state)
    }

    fn accumulate(
        &self,
        state: Option<&[u8]>,
        value: Option<&[u8]>,
    ) -> Result<Option<Vec<u8>>, String> {
        let (sum, count) = decode_avg_state(state);
        let v = value
            .and_then(|b| b.try_into().ok())
            .map(f64::from_be_bytes)
            .unwrap_or(0.0);
        Ok(Some(encode_avg_state(sum + v, count + 1)))
    }

    fn finalize(&self, state: Option<&[u8]>) -> Result<Option<Vec<u8>>, String> {
        let (sum, count) = decode_avg_state(state);
        if count == 0 {
            return Ok(Some(0.0f64.to_be_bytes().to_vec()));
        }
        Ok(Some((sum / count as f64).to_be_bytes().to_vec()))
    }
}

fn decode_avg_state(state: Option<&[u8]>) -> (f64, i64) {
    match state {
        Some(b) if b.len() >= 16 => {
            let sum = f64::from_be_bytes(b[..8].try_into().unwrap());
            let count = i64::from_be_bytes(b[8..16].try_into().unwrap());
            (sum, count)
        }
        _ => (0.0, 0),
    }
}

fn encode_avg_state(sum: f64, count: i64) -> Vec<u8> {
    let mut state = Vec::with_capacity(16);
    state.extend_from_slice(&sum.to_be_bytes());
    state.extend_from_slice(&count.to_be_bytes());
    state
}

// ── Min/Max ───────────────────────────────────────────────────────────

pub struct MinAggregate {
    cql_type: CqlType,
}

impl MinAggregate {
    pub fn new(cql_type: CqlType) -> Self {
        Self { cql_type }
    }
}

impl CqlFunction for MinAggregate {
    fn name(&self) -> &str {
        "min"
    }
    fn arg_types(&self) -> Vec<CqlType> {
        vec![self.cql_type.clone()]
    }
    fn return_type(&self) -> CqlType {
        self.cql_type.clone()
    }
    fn execute(&self, args: &[Option<&[u8]>]) -> Result<Option<Vec<u8>>, String> {
        match args.first().and_then(|a| *a) {
            Some(b) => Ok(Some(b.to_vec())),
            None => Ok(None),
        }
    }
}

impl AggregateFunction for MinAggregate {
    fn init_state(&self) -> Option<Vec<u8>> {
        None
    }

    fn accumulate(
        &self,
        state: Option<&[u8]>,
        value: Option<&[u8]>,
    ) -> Result<Option<Vec<u8>>, String> {
        match (state, value) {
            (None, v) => Ok(v.map(|b| b.to_vec())),
            (s, None) => Ok(s.map(|b| b.to_vec())),
            (Some(s), Some(v)) => {
                let ord = cassandra_types::comparator::compare_bytes(&self.cql_type, s, v);
                if ord == std::cmp::Ordering::Less || ord == std::cmp::Ordering::Equal {
                    Ok(Some(s.to_vec()))
                } else {
                    Ok(Some(v.to_vec()))
                }
            }
        }
    }

    fn finalize(&self, state: Option<&[u8]>) -> Result<Option<Vec<u8>>, String> {
        Ok(state.map(|s| s.to_vec()))
    }
}

pub struct MaxAggregate {
    cql_type: CqlType,
}

impl MaxAggregate {
    pub fn new(cql_type: CqlType) -> Self {
        Self { cql_type }
    }
}

impl CqlFunction for MaxAggregate {
    fn name(&self) -> &str {
        "max"
    }
    fn arg_types(&self) -> Vec<CqlType> {
        vec![self.cql_type.clone()]
    }
    fn return_type(&self) -> CqlType {
        self.cql_type.clone()
    }
    fn execute(&self, args: &[Option<&[u8]>]) -> Result<Option<Vec<u8>>, String> {
        match args.first().and_then(|a| *a) {
            Some(b) => Ok(Some(b.to_vec())),
            None => Ok(None),
        }
    }
}

impl AggregateFunction for MaxAggregate {
    fn init_state(&self) -> Option<Vec<u8>> {
        None
    }

    fn accumulate(
        &self,
        state: Option<&[u8]>,
        value: Option<&[u8]>,
    ) -> Result<Option<Vec<u8>>, String> {
        match (state, value) {
            (None, v) => Ok(v.map(|b| b.to_vec())),
            (s, None) => Ok(s.map(|b| b.to_vec())),
            (Some(s), Some(v)) => {
                let ord = cassandra_types::comparator::compare_bytes(&self.cql_type, s, v);
                if ord == std::cmp::Ordering::Greater || ord == std::cmp::Ordering::Equal {
                    Ok(Some(s.to_vec()))
                } else {
                    Ok(Some(v.to_vec()))
                }
            }
        }
    }

    fn finalize(&self, state: Option<&[u8]>) -> Result<Option<Vec<u8>>, String> {
        Ok(state.map(|s| s.to_vec()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn count_star_accumulate() {
        let agg = CountStarAggregate;
        let state = agg.init_state();
        let s = agg.accumulate(state.as_deref(), Some(&[1, 2, 3])).unwrap();
        let s = agg.accumulate(s.as_deref(), Some(&[4, 5, 6])).unwrap();
        let s = agg.accumulate(s.as_deref(), None).unwrap(); // null still counts for count(*)
        let result = agg.finalize(s.as_deref()).unwrap().unwrap();
        assert_eq!(i64::from_be_bytes(result.try_into().unwrap()), 3);
    }

    #[test]
    fn count_column_skips_null() {
        let agg = CountColumnAggregate;
        let state = agg.init_state();
        let s = agg.accumulate(state.as_deref(), Some(&[1, 2, 3])).unwrap();
        let s = agg.accumulate(s.as_deref(), None).unwrap(); // null doesn't count
        let result = agg.finalize(s.as_deref()).unwrap().unwrap();
        assert_eq!(i64::from_be_bytes(result.try_into().unwrap()), 1);
    }

    #[test]
    fn sum_int() {
        let agg = SumInt;
        let state = agg.init_state();
        let s = agg
            .accumulate(state.as_deref(), Some(&10i32.to_be_bytes()))
            .unwrap();
        let s = agg
            .accumulate(s.as_deref(), Some(&20i32.to_be_bytes()))
            .unwrap();
        let result = agg.finalize(s.as_deref()).unwrap().unwrap();
        assert_eq!(i32::from_be_bytes(result.try_into().unwrap()), 30);
    }

    #[test]
    fn avg_int() {
        let agg = AvgInt;
        let state = agg.init_state();
        let s = agg
            .accumulate(state.as_deref(), Some(&10i32.to_be_bytes()))
            .unwrap();
        let s = agg
            .accumulate(s.as_deref(), Some(&20i32.to_be_bytes()))
            .unwrap();
        let s = agg
            .accumulate(s.as_deref(), Some(&30i32.to_be_bytes()))
            .unwrap();
        let result = agg.finalize(s.as_deref()).unwrap().unwrap();
        assert_eq!(i32::from_be_bytes(result.try_into().unwrap()), 20);
    }

    #[test]
    fn min_max_int() {
        let min_agg = MinAggregate::new(CqlType::Int);
        let max_agg = MaxAggregate::new(CqlType::Int);

        let values = [5i32, 2, 8, 1, 9];
        let mut min_state = min_agg.init_state();
        let mut max_state = max_agg.init_state();

        for v in &values {
            let bytes = v.to_be_bytes();
            min_state = min_agg
                .accumulate(min_state.as_deref(), Some(&bytes))
                .unwrap();
            max_state = max_agg
                .accumulate(max_state.as_deref(), Some(&bytes))
                .unwrap();
        }

        let min_result = min_agg.finalize(min_state.as_deref()).unwrap().unwrap();
        let max_result = max_agg.finalize(max_state.as_deref()).unwrap().unwrap();

        assert_eq!(i32::from_be_bytes(min_result.try_into().unwrap()), 1);
        assert_eq!(i32::from_be_bytes(max_result.try_into().unwrap()), 9);
    }
}
