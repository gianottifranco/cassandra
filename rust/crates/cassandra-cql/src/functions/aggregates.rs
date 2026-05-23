// Licensed under Apache License, Version 2.0.

//! Aggregate functions: count, sum, avg, min, max.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.cql3.functions.AggregateFcts`

use super::registry::{CqlFunction, FunctionRegistry};
use crate::functions::decimal_math::DecimalValue;
use cassandra_types::CqlType;
use num_bigint::BigInt;
use num_traits::{ToPrimitive, Zero};
use std::sync::Arc;

/// Register aggregate functions that Cassandra exposes through the native
/// function catalog.
pub fn register_all(registry: &FunctionRegistry) {
    registry.register(Arc::new(CountRowsAggregate { name: "count_rows" }));
    registry.register(Arc::new(CountRowsAggregate { name: "countRows" }));
}

/// Resolve aggregate functions whose signatures depend on the concrete
/// argument type.
pub fn resolve_dynamic(name: &str, arg_types: &[CqlType]) -> Option<Arc<dyn CqlFunction>> {
    if name.eq_ignore_ascii_case("count") && arg_types.len() == 1 {
        return Some(Arc::new(CountColumnAggregate::new(arg_types[0].clone())));
    }
    if arg_types.len() != 1 {
        return None;
    }

    let cql_type = arg_types[0].clone();
    match name.to_ascii_lowercase().as_str() {
        "sum" if is_numeric_aggregate_type(&cql_type) => {
            Some(Arc::new(NumericSumAggregate::new(cql_type)))
        }
        "avg" if is_numeric_aggregate_type(&cql_type) => {
            Some(Arc::new(NumericAvgAggregate::new(cql_type)))
        }
        "min" => Some(Arc::new(MinAggregate::new(cql_type))),
        "max" => Some(Arc::new(MaxAggregate::new(cql_type))),
        _ => None,
    }
}

fn is_numeric_aggregate_type(cql_type: &CqlType) -> bool {
    matches!(
        cql_type,
        CqlType::Tinyint
            | CqlType::Smallint
            | CqlType::Int
            | CqlType::Bigint
            | CqlType::Counter
            | CqlType::Float
            | CqlType::Double
            | CqlType::Varint
            | CqlType::Decimal
    )
}

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

/// Native count rows function used by Cassandra for COUNT(*) and COUNT(1).
pub struct CountRowsAggregate {
    name: &'static str,
}

impl CqlFunction for CountRowsAggregate {
    fn name(&self) -> &str {
        self.name
    }
    fn arg_types(&self) -> Vec<CqlType> {
        Vec::new()
    }
    fn return_type(&self) -> CqlType {
        CqlType::Bigint
    }
    fn execute(&self, _args: &[Option<&[u8]>]) -> Result<Option<Vec<u8>>, String> {
        Ok(Some(1i64.to_be_bytes().to_vec()))
    }
}

impl AggregateFunction for CountRowsAggregate {
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

pub struct CountColumnAggregate {
    cql_type: CqlType,
}

impl CountColumnAggregate {
    pub fn new(cql_type: CqlType) -> Self {
        Self { cql_type }
    }
}

impl CqlFunction for CountColumnAggregate {
    fn name(&self) -> &str {
        "count"
    }
    fn arg_types(&self) -> Vec<CqlType> {
        vec![self.cql_type.clone()]
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

struct NumericSumAggregate {
    cql_type: CqlType,
}

impl NumericSumAggregate {
    fn new(cql_type: CqlType) -> Self {
        Self { cql_type }
    }
}

impl CqlFunction for NumericSumAggregate {
    fn name(&self) -> &str {
        "sum"
    }
    fn arg_types(&self) -> Vec<CqlType> {
        vec![self.cql_type.clone()]
    }
    fn return_type(&self) -> CqlType {
        self.cql_type.clone()
    }
    fn execute(&self, args: &[Option<&[u8]>]) -> Result<Option<Vec<u8>>, String> {
        Ok(args.first().and_then(|arg| *arg).map(|arg| arg.to_vec()))
    }
}

impl AggregateFunction for NumericSumAggregate {
    fn init_state(&self) -> Option<Vec<u8>> {
        Some(zero_numeric_value(&self.cql_type))
    }

    fn accumulate(
        &self,
        state: Option<&[u8]>,
        value: Option<&[u8]>,
    ) -> Result<Option<Vec<u8>>, String> {
        let Some(value) = value else {
            return Ok(Some(
                state
                    .map(|state| state.to_vec())
                    .unwrap_or_else(|| zero_numeric_value(&self.cql_type)),
            ));
        };
        let state = state
            .map(|state| state.to_vec())
            .unwrap_or_else(|| zero_numeric_value(&self.cql_type));
        Ok(Some(add_numeric_value(&self.cql_type, &state, value)?))
    }

    fn finalize(&self, state: Option<&[u8]>) -> Result<Option<Vec<u8>>, String> {
        Ok(Some(
            state
                .map(|state| state.to_vec())
                .unwrap_or_else(|| zero_numeric_value(&self.cql_type)),
        ))
    }
}

struct NumericAvgAggregate {
    cql_type: CqlType,
}

impl NumericAvgAggregate {
    fn new(cql_type: CqlType) -> Self {
        Self { cql_type }
    }
}

impl CqlFunction for NumericAvgAggregate {
    fn name(&self) -> &str {
        "avg"
    }
    fn arg_types(&self) -> Vec<CqlType> {
        vec![self.cql_type.clone()]
    }
    fn return_type(&self) -> CqlType {
        self.cql_type.clone()
    }
    fn execute(&self, args: &[Option<&[u8]>]) -> Result<Option<Vec<u8>>, String> {
        Ok(args.first().and_then(|arg| *arg).map(|arg| arg.to_vec()))
    }
}

impl AggregateFunction for NumericAvgAggregate {
    fn init_state(&self) -> Option<Vec<u8>> {
        Some(encode_avg_numeric_state(
            0,
            &zero_avg_sum_value(&self.cql_type),
        ))
    }

    fn accumulate(
        &self,
        state: Option<&[u8]>,
        value: Option<&[u8]>,
    ) -> Result<Option<Vec<u8>>, String> {
        let Some(value) = value else {
            return Ok(Some(state.map(|state| state.to_vec()).unwrap_or_else(
                || encode_avg_numeric_state(0, &zero_avg_sum_value(&self.cql_type)),
            )));
        };
        let (count, sum) = decode_avg_numeric_state(state);
        let sum = add_avg_sum_value(&self.cql_type, &sum, value)?;
        Ok(Some(encode_avg_numeric_state(count + 1, &sum)))
    }

    fn finalize(&self, state: Option<&[u8]>) -> Result<Option<Vec<u8>>, String> {
        let (count, sum) = decode_avg_numeric_state(state);
        if count == 0 {
            return Ok(Some(zero_numeric_value(&self.cql_type)));
        }
        avg_sum_to_result(&self.cql_type, &sum, count).map(Some)
    }
}

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

fn zero_numeric_value(cql_type: &CqlType) -> Vec<u8> {
    match cql_type {
        CqlType::Tinyint => vec![0],
        CqlType::Smallint => 0i16.to_be_bytes().to_vec(),
        CqlType::Int => 0i32.to_be_bytes().to_vec(),
        CqlType::Bigint | CqlType::Counter => 0i64.to_be_bytes().to_vec(),
        CqlType::Float => 0.0f32.to_be_bytes().to_vec(),
        CqlType::Double => 0.0f64.to_be_bytes().to_vec(),
        CqlType::Varint => BigInt::zero().to_signed_bytes_be(),
        CqlType::Decimal => DecimalValue::from_i64(0).to_bytes(),
        _ => Vec::new(),
    }
}

fn add_numeric_value(cql_type: &CqlType, state: &[u8], value: &[u8]) -> Result<Vec<u8>, String> {
    match cql_type {
        CqlType::Tinyint => Ok(vec![read_i8(state)?.wrapping_add(read_i8(value)?) as u8]),
        CqlType::Smallint => Ok(read_i16(state)?
            .wrapping_add(read_i16(value)?)
            .to_be_bytes()
            .to_vec()),
        CqlType::Int => Ok(read_i32(state)?
            .wrapping_add(read_i32(value)?)
            .to_be_bytes()
            .to_vec()),
        CqlType::Bigint | CqlType::Counter => Ok(read_i64(state)?
            .wrapping_add(read_i64(value)?)
            .to_be_bytes()
            .to_vec()),
        CqlType::Float => Ok((read_f32(state)? + read_f32(value)?).to_be_bytes().to_vec()),
        CqlType::Double => Ok((read_f64(state)? + read_f64(value)?).to_be_bytes().to_vec()),
        CqlType::Varint => {
            let sum = BigInt::from_signed_bytes_be(state) + BigInt::from_signed_bytes_be(value);
            Ok(sum.to_signed_bytes_be())
        }
        CqlType::Decimal => Ok(DecimalValue::from_bytes(state)?
            .add(&DecimalValue::from_bytes(value)?)
            .to_bytes()),
        _ => Err(format!(
            "{} is not a numeric aggregate type",
            cql_type.cql_name()
        )),
    }
}

fn zero_avg_sum_value(cql_type: &CqlType) -> Vec<u8> {
    match cql_type {
        CqlType::Tinyint
        | CqlType::Smallint
        | CqlType::Int
        | CqlType::Bigint
        | CqlType::Counter
        | CqlType::Varint => BigInt::zero().to_signed_bytes_be(),
        CqlType::Float | CqlType::Double => 0.0f64.to_be_bytes().to_vec(),
        CqlType::Decimal => DecimalValue::from_i64(0).to_bytes(),
        _ => Vec::new(),
    }
}

fn encode_avg_numeric_state(count: i64, sum: &[u8]) -> Vec<u8> {
    let mut state = count.to_be_bytes().to_vec();
    state.extend_from_slice(sum);
    state
}

fn decode_avg_numeric_state(state: Option<&[u8]>) -> (i64, Vec<u8>) {
    match state {
        Some(bytes) if bytes.len() >= 8 => {
            let count = i64::from_be_bytes(bytes[..8].try_into().expect("slice has 8 bytes"));
            (count, bytes[8..].to_vec())
        }
        _ => (0, Vec::new()),
    }
}

fn add_avg_sum_value(cql_type: &CqlType, sum: &[u8], value: &[u8]) -> Result<Vec<u8>, String> {
    match cql_type {
        CqlType::Tinyint
        | CqlType::Smallint
        | CqlType::Int
        | CqlType::Bigint
        | CqlType::Counter
        | CqlType::Varint => {
            let sum = BigInt::from_signed_bytes_be(sum) + numeric_value_to_bigint(cql_type, value)?;
            Ok(sum.to_signed_bytes_be())
        }
        CqlType::Float | CqlType::Double => {
            let sum = if sum.is_empty() { 0.0 } else { read_f64(sum)? };
            let value = match cql_type {
                CqlType::Float => read_f32(value)? as f64,
                CqlType::Double => read_f64(value)?,
                _ => unreachable!(),
            };
            Ok((sum + value).to_be_bytes().to_vec())
        }
        CqlType::Decimal => {
            let sum = if sum.is_empty() {
                DecimalValue::from_i64(0)
            } else {
                DecimalValue::from_bytes(sum)?
            };
            Ok(sum.add(&DecimalValue::from_bytes(value)?).to_bytes())
        }
        _ => Err(format!(
            "{} is not a numeric aggregate type",
            cql_type.cql_name()
        )),
    }
}

fn avg_sum_to_result(cql_type: &CqlType, sum: &[u8], count: i64) -> Result<Vec<u8>, String> {
    match cql_type {
        CqlType::Tinyint => Ok(vec![
            (BigInt::from_signed_bytes_be(sum) / count)
                .to_i8()
                .unwrap_or(0) as u8,
        ]),
        CqlType::Smallint => Ok(((BigInt::from_signed_bytes_be(sum) / count)
            .to_i16()
            .unwrap_or(0))
        .to_be_bytes()
        .to_vec()),
        CqlType::Int => Ok(((BigInt::from_signed_bytes_be(sum) / count)
            .to_i32()
            .unwrap_or(0))
        .to_be_bytes()
        .to_vec()),
        CqlType::Bigint | CqlType::Counter => Ok(((BigInt::from_signed_bytes_be(sum) / count)
            .to_i64()
            .unwrap_or(0))
        .to_be_bytes()
        .to_vec()),
        CqlType::Varint => Ok((BigInt::from_signed_bytes_be(sum) / count).to_signed_bytes_be()),
        CqlType::Float => Ok(((read_f64(sum)? / count as f64) as f32)
            .to_be_bytes()
            .to_vec()),
        CqlType::Double => Ok((read_f64(sum)? / count as f64).to_be_bytes().to_vec()),
        CqlType::Decimal => Ok(DecimalValue::from_bytes(sum)?
            .divide_i64_half_even_same_scale(count)?
            .to_bytes()),
        _ => Err(format!(
            "{} is not a numeric aggregate type",
            cql_type.cql_name()
        )),
    }
}

fn numeric_value_to_bigint(cql_type: &CqlType, value: &[u8]) -> Result<BigInt, String> {
    match cql_type {
        CqlType::Tinyint => Ok(BigInt::from(read_i8(value)?)),
        CqlType::Smallint => Ok(BigInt::from(read_i16(value)?)),
        CqlType::Int => Ok(BigInt::from(read_i32(value)?)),
        CqlType::Bigint | CqlType::Counter => Ok(BigInt::from(read_i64(value)?)),
        CqlType::Varint => Ok(BigInt::from_signed_bytes_be(value)),
        _ => Err(format!(
            "{} is not an integer aggregate type",
            cql_type.cql_name()
        )),
    }
}

fn read_i8(value: &[u8]) -> Result<i8, String> {
    value
        .first()
        .map(|value| *value as i8)
        .ok_or_else(|| "invalid tinyint bytes: expected 1 byte, got 0".to_string())
}

fn read_i16(value: &[u8]) -> Result<i16, String> {
    let bytes: [u8; 2] = value
        .try_into()
        .map_err(|_| format!("invalid smallint bytes: got {}", value.len()))?;
    Ok(i16::from_be_bytes(bytes))
}

fn read_i32(value: &[u8]) -> Result<i32, String> {
    let bytes: [u8; 4] = value
        .try_into()
        .map_err(|_| format!("invalid int bytes: got {}", value.len()))?;
    Ok(i32::from_be_bytes(bytes))
}

fn read_i64(value: &[u8]) -> Result<i64, String> {
    let bytes: [u8; 8] = value
        .try_into()
        .map_err(|_| format!("invalid bigint bytes: got {}", value.len()))?;
    Ok(i64::from_be_bytes(bytes))
}

fn read_f32(value: &[u8]) -> Result<f32, String> {
    let bytes: [u8; 4] = value
        .try_into()
        .map_err(|_| format!("invalid float bytes: got {}", value.len()))?;
    Ok(f32::from_be_bytes(bytes))
}

fn read_f64(value: &[u8]) -> Result<f64, String> {
    let bytes: [u8; 8] = value
        .try_into()
        .map_err(|_| format!("invalid double bytes: got {}", value.len()))?;
    Ok(f64::from_be_bytes(bytes))
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
    fn count_rows_resolves_current_and_legacy_names() {
        let registry = FunctionRegistry::with_builtins();

        for name in ["count_rows", "countRows"] {
            let function = registry
                .resolve(name, &[])
                .unwrap_or_else(|| panic!("missing {name}"));
            assert_eq!(function.return_type(), CqlType::Bigint);
            let result = function.execute(&[]).unwrap().unwrap();
            assert_eq!(i64::from_be_bytes(result.try_into().unwrap()), 1);
        }
    }

    #[test]
    fn count_column_skips_null() {
        let agg = CountColumnAggregate::new(CqlType::Varchar);
        let state = agg.init_state();
        let s = agg.accumulate(state.as_deref(), Some(&[1, 2, 3])).unwrap();
        let s = agg.accumulate(s.as_deref(), None).unwrap(); // null doesn't count
        let result = agg.finalize(s.as_deref()).unwrap().unwrap();
        assert_eq!(i64::from_be_bytes(result.try_into().unwrap()), 1);
    }

    #[test]
    fn count_column_resolves_for_concrete_argument_types() {
        let registry = FunctionRegistry::with_builtins();
        let function = registry
            .resolve("count", &[CqlType::Int])
            .expect("count(int) should resolve");

        assert_eq!(function.arg_types(), vec![CqlType::Int]);
        assert_eq!(function.return_type(), CqlType::Bigint);
        let present = function.execute(&[Some(&[1, 2, 3])]).unwrap().unwrap();
        let missing = function.execute(&[None]).unwrap().unwrap();
        assert_eq!(i64::from_be_bytes(present.try_into().unwrap()), 1);
        assert_eq!(i64::from_be_bytes(missing.try_into().unwrap()), 0);
    }

    #[test]
    fn aggregate_catalog_resolves_java_numeric_signatures() {
        let registry = FunctionRegistry::with_builtins();
        let numeric_types = [
            CqlType::Tinyint,
            CqlType::Smallint,
            CqlType::Int,
            CqlType::Bigint,
            CqlType::Counter,
            CqlType::Float,
            CqlType::Double,
            CqlType::Varint,
            CqlType::Decimal,
        ];

        for cql_type in numeric_types {
            for name in ["sum", "avg", "min", "max"] {
                let function = registry
                    .resolve(name, std::slice::from_ref(&cql_type))
                    .unwrap_or_else(|| panic!("missing {name}({})", cql_type.cql_name()));
                assert_eq!(function.arg_types(), vec![cql_type.clone()]);
                assert_eq!(function.return_type(), cql_type);
            }
        }
    }

    #[test]
    fn dynamic_sum_avg_cover_extended_numeric_types() {
        let sum = NumericSumAggregate::new(CqlType::Tinyint);
        let state = sum.init_state();
        let state = sum.accumulate(state.as_deref(), Some(&[120u8])).unwrap();
        let state = sum.accumulate(state.as_deref(), Some(&[10u8])).unwrap();
        assert_eq!(state.unwrap(), vec![130u8]);

        let avg = NumericAvgAggregate::new(CqlType::Varint);
        let state = avg.init_state();
        let state = avg.accumulate(state.as_deref(), Some(&[10u8])).unwrap();
        let state = avg.accumulate(state.as_deref(), Some(&[20u8])).unwrap();
        let result = avg.finalize(state.as_deref()).unwrap().unwrap();
        assert_eq!(BigInt::from_signed_bytes_be(&result), BigInt::from(15));

        let decimal = DecimalValue::from_string("1.50").unwrap().to_bytes();
        let avg = NumericAvgAggregate::new(CqlType::Decimal);
        let state = avg.init_state();
        let state = avg.accumulate(state.as_deref(), Some(&decimal)).unwrap();
        let state = avg.accumulate(state.as_deref(), Some(&decimal)).unwrap();
        let result =
            DecimalValue::from_bytes(&avg.finalize(state.as_deref()).unwrap().unwrap()).unwrap();
        assert_eq!(result, DecimalValue::from_string("1.50").unwrap());
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
