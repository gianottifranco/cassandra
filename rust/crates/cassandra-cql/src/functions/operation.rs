// Licensed under Apache License, Version 2.0.

//! Operation functions backing CQL arithmetic operators.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.cql3.functions.OperationFcts`

use super::registry::{CqlFunction, FunctionRegistry};
use crate::functions::decimal_math::DecimalValue;
use cassandra_types::{CqlType, CqlValue};
use num_bigint::BigInt;
use num_traits::Zero;
use std::sync::Arc;

pub fn register_all(registry: &FunctionRegistry) {
    let numeric_types = [
        CqlType::Tinyint,
        CqlType::Smallint,
        CqlType::Int,
        CqlType::Bigint,
        CqlType::Float,
        CqlType::Double,
        CqlType::Counter,
    ];

    for left in &numeric_types {
        for right in &numeric_types {
            let return_type = operation_return_type(left, right);
            for op in Operation::ALL {
                registry.register(Arc::new(NumericOperationFunction {
                    op,
                    left: left.clone(),
                    right: right.clone(),
                    return_type: return_type.clone(),
                }));
            }
        }
        registry.register(Arc::new(NumericNegationFunction {
            input: left.clone(),
        }));
    }

    let varint_integer_types = [
        CqlType::Tinyint,
        CqlType::Smallint,
        CqlType::Int,
        CqlType::Bigint,
        CqlType::Counter,
        CqlType::Varint,
    ];
    for left in &varint_integer_types {
        for right in &varint_integer_types {
            if *left == CqlType::Varint || *right == CqlType::Varint {
                for op in Operation::ALL {
                    registry.register(Arc::new(VarintOperationFunction {
                        op,
                        left: left.clone(),
                        right: right.clone(),
                    }));
                }
            }
        }
    }
    registry.register(Arc::new(VarintNegationFunction));

    let decimal_numeric_types = [
        CqlType::Tinyint,
        CqlType::Smallint,
        CqlType::Int,
        CqlType::Bigint,
        CqlType::Float,
        CqlType::Double,
        CqlType::Counter,
        CqlType::Varint,
        CqlType::Decimal,
    ];
    for left in &decimal_numeric_types {
        for right in &decimal_numeric_types {
            if operation_return_type(left, right) == CqlType::Decimal {
                for op in Operation::ALL {
                    registry.register(Arc::new(DecimalOperationFunction {
                        op,
                        left: left.clone(),
                        right: right.clone(),
                    }));
                }
            }
        }
    }
    registry.register(Arc::new(DecimalNegationFunction));

    for op in [Operation::Add, Operation::Subtract] {
        registry.register(Arc::new(TemporalOperationFunction {
            op,
            temporal: CqlType::Timestamp,
        }));
        registry.register(Arc::new(TemporalOperationFunction {
            op,
            temporal: CqlType::Date,
        }));
    }

    registry.register(Arc::new(StringOperationFunction {
        left: CqlType::Ascii,
        right: CqlType::Ascii,
        return_type: CqlType::Ascii,
    }));
    registry.register(Arc::new(StringOperationFunction {
        left: CqlType::Varchar,
        right: CqlType::Varchar,
        return_type: CqlType::Varchar,
    }));
    registry.register(Arc::new(StringOperationFunction {
        left: CqlType::Varchar,
        right: CqlType::Ascii,
        return_type: CqlType::Varchar,
    }));
    registry.register(Arc::new(StringOperationFunction {
        left: CqlType::Ascii,
        right: CqlType::Varchar,
        return_type: CqlType::Varchar,
    }));
}

#[derive(Debug, Clone, Copy)]
enum Operation {
    Add,
    Subtract,
    Multiply,
    Divide,
    Modulo,
}

impl Operation {
    const ALL: [Operation; 5] = [
        Operation::Add,
        Operation::Subtract,
        Operation::Multiply,
        Operation::Divide,
        Operation::Modulo,
    ];

    fn function_name(self) -> &'static str {
        match self {
            Operation::Add => "_add",
            Operation::Subtract => "_substract",
            Operation::Multiply => "_multiply",
            Operation::Divide => "_divide",
            Operation::Modulo => "_modulo",
        }
    }
}

struct NumericOperationFunction {
    op: Operation,
    left: CqlType,
    right: CqlType,
    return_type: CqlType,
}

struct NumericNegationFunction {
    input: CqlType,
}

struct VarintOperationFunction {
    op: Operation,
    left: CqlType,
    right: CqlType,
}

struct VarintNegationFunction;

struct DecimalOperationFunction {
    op: Operation,
    left: CqlType,
    right: CqlType,
}

struct DecimalNegationFunction;

struct TemporalOperationFunction {
    op: Operation,
    temporal: CqlType,
}

struct StringOperationFunction {
    left: CqlType,
    right: CqlType,
    return_type: CqlType,
}

impl CqlFunction for NumericOperationFunction {
    fn name(&self) -> &str {
        self.op.function_name()
    }

    fn arg_types(&self) -> Vec<CqlType> {
        vec![self.left.clone(), self.right.clone()]
    }

    fn return_type(&self) -> CqlType {
        self.return_type.clone()
    }

    fn execute(&self, args: &[Option<&[u8]>]) -> Result<Option<Vec<u8>>, String> {
        let Some(left) = args.first().and_then(|arg| *arg) else {
            return Ok(None);
        };
        let Some(right) = args.get(1).and_then(|arg| *arg) else {
            return Ok(None);
        };

        match self.return_type {
            CqlType::Tinyint => {
                let left = read_i8(&self.left, left)?;
                let right = read_i8(&self.right, right)?;
                Ok(Some(vec![eval_i8(self.op, left, right)? as u8]))
            }
            CqlType::Smallint => {
                let left = read_i16(&self.left, left)?;
                let right = read_i16(&self.right, right)?;
                Ok(Some(eval_i16(self.op, left, right)?.to_be_bytes().to_vec()))
            }
            CqlType::Int => {
                let left = read_i32(&self.left, left)?;
                let right = read_i32(&self.right, right)?;
                Ok(Some(eval_i32(self.op, left, right)?.to_be_bytes().to_vec()))
            }
            CqlType::Bigint => {
                let left = read_i64(&self.left, left)?;
                let right = read_i64(&self.right, right)?;
                Ok(Some(eval_i64(self.op, left, right)?.to_be_bytes().to_vec()))
            }
            CqlType::Float => {
                let left = read_f32(&self.left, left)?;
                let right = read_f32(&self.right, right)?;
                Ok(Some(eval_f32(self.op, left, right).to_be_bytes().to_vec()))
            }
            CqlType::Double => {
                let left = read_f64(&self.left, left)?;
                let right = read_f64(&self.right, right)?;
                Ok(Some(eval_f64(self.op, left, right).to_be_bytes().to_vec()))
            }
            _ => Err(format!(
                "unsupported operation return type {}",
                self.return_type.cql_name()
            )),
        }
    }
}

impl CqlFunction for NumericNegationFunction {
    fn name(&self) -> &str {
        "_negate"
    }

    fn arg_types(&self) -> Vec<CqlType> {
        vec![self.input.clone()]
    }

    fn return_type(&self) -> CqlType {
        self.input.clone()
    }

    fn execute(&self, args: &[Option<&[u8]>]) -> Result<Option<Vec<u8>>, String> {
        let Some(input) = args.first().and_then(|arg| *arg) else {
            return Ok(None);
        };

        match self.input {
            CqlType::Tinyint => Ok(Some(
                vec![read_i8(&self.input, input)?.wrapping_neg() as u8],
            )),
            CqlType::Smallint => Ok(Some(
                read_i16(&self.input, input)?
                    .wrapping_neg()
                    .to_be_bytes()
                    .to_vec(),
            )),
            CqlType::Int => Ok(Some(
                read_i32(&self.input, input)?
                    .wrapping_neg()
                    .to_be_bytes()
                    .to_vec(),
            )),
            CqlType::Bigint | CqlType::Counter => Ok(Some(
                read_i64(&self.input, input)?
                    .wrapping_neg()
                    .to_be_bytes()
                    .to_vec(),
            )),
            CqlType::Float => Ok(Some(
                (-read_f32(&self.input, input)?).to_be_bytes().to_vec(),
            )),
            CqlType::Double => Ok(Some(
                (-read_f64(&self.input, input)?).to_be_bytes().to_vec(),
            )),
            _ => Err(format!(
                "unsupported negation type {}",
                self.input.cql_name()
            )),
        }
    }
}

impl CqlFunction for VarintOperationFunction {
    fn name(&self) -> &str {
        self.op.function_name()
    }

    fn arg_types(&self) -> Vec<CqlType> {
        vec![self.left.clone(), self.right.clone()]
    }

    fn return_type(&self) -> CqlType {
        CqlType::Varint
    }

    fn execute(&self, args: &[Option<&[u8]>]) -> Result<Option<Vec<u8>>, String> {
        let Some(left) = args.first().and_then(|arg| *arg) else {
            return Ok(None);
        };
        let Some(right) = args.get(1).and_then(|arg| *arg) else {
            return Ok(None);
        };

        let left = read_bigint(&self.left, left)?;
        let right = read_bigint(&self.right, right)?;
        let result = match self.op {
            Operation::Add => left + right,
            Operation::Subtract => left - right,
            Operation::Multiply => left * right,
            Operation::Divide => {
                if right.is_zero() {
                    return Err("division by zero".to_string());
                }
                left / right
            }
            Operation::Modulo => {
                if right.is_zero() {
                    return Err("modulo by zero".to_string());
                }
                left % right
            }
        };
        Ok(Some(result.to_signed_bytes_be()))
    }
}

impl CqlFunction for VarintNegationFunction {
    fn name(&self) -> &str {
        "_negate"
    }

    fn arg_types(&self) -> Vec<CqlType> {
        vec![CqlType::Varint]
    }

    fn return_type(&self) -> CqlType {
        CqlType::Varint
    }

    fn execute(&self, args: &[Option<&[u8]>]) -> Result<Option<Vec<u8>>, String> {
        let Some(input) = args.first().and_then(|arg| *arg) else {
            return Ok(None);
        };
        Ok(Some(
            (-read_bigint(&CqlType::Varint, input)?).to_signed_bytes_be(),
        ))
    }
}

impl CqlFunction for DecimalOperationFunction {
    fn name(&self) -> &str {
        self.op.function_name()
    }

    fn arg_types(&self) -> Vec<CqlType> {
        vec![self.left.clone(), self.right.clone()]
    }

    fn return_type(&self) -> CqlType {
        CqlType::Decimal
    }

    fn execute(&self, args: &[Option<&[u8]>]) -> Result<Option<Vec<u8>>, String> {
        let Some(left) = args.first().and_then(|arg| *arg) else {
            return Ok(None);
        };
        let Some(right) = args.get(1).and_then(|arg| *arg) else {
            return Ok(None);
        };

        let left = read_decimal_value(&self.left, left)?;
        let right = read_decimal_value(&self.right, right)?;
        let result = match self.op {
            Operation::Add => left.add(&right),
            Operation::Subtract => left.subtract(&right),
            Operation::Multiply => left.multiply(&right),
            Operation::Divide => left.divide(&right)?,
            Operation::Modulo => left.remainder(&right)?,
        };
        Ok(Some(result.to_bytes()))
    }
}

impl CqlFunction for DecimalNegationFunction {
    fn name(&self) -> &str {
        "_negate"
    }

    fn arg_types(&self) -> Vec<CqlType> {
        vec![CqlType::Decimal]
    }

    fn return_type(&self) -> CqlType {
        CqlType::Decimal
    }

    fn execute(&self, args: &[Option<&[u8]>]) -> Result<Option<Vec<u8>>, String> {
        let Some(input) = args.first().and_then(|arg| *arg) else {
            return Ok(None);
        };
        Ok(Some(
            read_decimal_value(&CqlType::Decimal, input)?
                .negate()
                .to_bytes(),
        ))
    }
}

impl CqlFunction for TemporalOperationFunction {
    fn name(&self) -> &str {
        self.op.function_name()
    }

    fn arg_types(&self) -> Vec<CqlType> {
        vec![self.temporal.clone(), CqlType::Duration]
    }

    fn return_type(&self) -> CqlType {
        self.temporal.clone()
    }

    fn execute(&self, args: &[Option<&[u8]>]) -> Result<Option<Vec<u8>>, String> {
        let Some(temporal) = args.first().and_then(|arg| *arg) else {
            return Ok(None);
        };
        let Some(duration) = args.get(1).and_then(|arg| *arg) else {
            return Ok(None);
        };
        let duration = decode_duration(duration)?;

        match self.temporal {
            CqlType::Timestamp => {
                let millis = read_i64(&CqlType::Timestamp, temporal)?;
                let result = apply_duration(millis, duration, self.op)?;
                Ok(Some(result.to_be_bytes().to_vec()))
            }
            CqlType::Date => {
                if duration.nanoseconds != 0 {
                    return Err("date duration operations require day precision".to_string());
                }
                let millis = date_bytes_to_millis(temporal)?;
                let result = apply_duration(millis, duration, self.op)?;
                Ok(Some(millis_to_date_bytes(result)))
            }
            _ => Err(format!(
                "unsupported temporal operation type {}",
                self.temporal.cql_name()
            )),
        }
    }
}

impl CqlFunction for StringOperationFunction {
    fn name(&self) -> &str {
        "_add"
    }

    fn arg_types(&self) -> Vec<CqlType> {
        vec![self.left.clone(), self.right.clone()]
    }

    fn return_type(&self) -> CqlType {
        self.return_type.clone()
    }

    fn execute(&self, args: &[Option<&[u8]>]) -> Result<Option<Vec<u8>>, String> {
        let Some(left) = args.first().and_then(|arg| *arg) else {
            return Ok(None);
        };
        let Some(right) = args.get(1).and_then(|arg| *arg) else {
            return Ok(None);
        };
        validate_text(&self.left, left)?;
        validate_text(&self.right, right)?;
        let mut result = Vec::with_capacity(left.len() + right.len());
        result.extend_from_slice(left);
        result.extend_from_slice(right);
        Ok(Some(result))
    }
}

fn operation_return_type(left: &CqlType, right: &CqlType) -> CqlType {
    let size = operation_type_size(left).max(operation_type_size(right));
    if is_floating(left) || is_floating(right) {
        match size {
            0..=4 => CqlType::Float,
            5..=8 => CqlType::Double,
            _ => CqlType::Decimal,
        }
    } else {
        match size {
            0..=1 => CqlType::Tinyint,
            2 => CqlType::Smallint,
            3..=4 => CqlType::Int,
            5..=8 => CqlType::Bigint,
            _ => CqlType::Varint,
        }
    }
}

fn operation_type_size(cql_type: &CqlType) -> usize {
    match cql_type {
        CqlType::Tinyint => 1,
        CqlType::Smallint => 2,
        CqlType::Int | CqlType::Float => 4,
        CqlType::Bigint | CqlType::Double | CqlType::Counter => 8,
        CqlType::Varint | CqlType::Decimal => usize::MAX,
        _ => usize::MAX,
    }
}

fn is_floating(cql_type: &CqlType) -> bool {
    matches!(
        cql_type,
        CqlType::Float | CqlType::Double | CqlType::Decimal
    )
}

fn read_i8(cql_type: &CqlType, bytes: &[u8]) -> Result<i8, String> {
    Ok(read_i64(cql_type, bytes)? as i8)
}

fn read_i16(cql_type: &CqlType, bytes: &[u8]) -> Result<i16, String> {
    Ok(read_i64(cql_type, bytes)? as i16)
}

fn read_i32(cql_type: &CqlType, bytes: &[u8]) -> Result<i32, String> {
    Ok(read_i64(cql_type, bytes)? as i32)
}

fn read_i64(cql_type: &CqlType, bytes: &[u8]) -> Result<i64, String> {
    match cql_type {
        CqlType::Tinyint => {
            expect_len(cql_type, bytes, 1)?;
            Ok(bytes[0] as i8 as i64)
        }
        CqlType::Smallint => {
            expect_len(cql_type, bytes, 2)?;
            Ok(i16::from_be_bytes(bytes.try_into().unwrap()) as i64)
        }
        CqlType::Int => {
            expect_len(cql_type, bytes, 4)?;
            Ok(i32::from_be_bytes(bytes.try_into().unwrap()) as i64)
        }
        CqlType::Bigint | CqlType::Counter | CqlType::Timestamp | CqlType::Time => {
            expect_len(cql_type, bytes, 8)?;
            Ok(i64::from_be_bytes(bytes.try_into().unwrap()))
        }
        CqlType::Float => {
            expect_len(cql_type, bytes, 4)?;
            Ok(f32::from_be_bytes(bytes.try_into().unwrap()) as i64)
        }
        CqlType::Double => {
            expect_len(cql_type, bytes, 8)?;
            Ok(f64::from_be_bytes(bytes.try_into().unwrap()) as i64)
        }
        _ => Err(format!("unsupported numeric type {}", cql_type.cql_name())),
    }
}

fn read_f32(cql_type: &CqlType, bytes: &[u8]) -> Result<f32, String> {
    Ok(read_f64(cql_type, bytes)? as f32)
}

fn read_f64(cql_type: &CqlType, bytes: &[u8]) -> Result<f64, String> {
    match cql_type {
        CqlType::Float => {
            expect_len(cql_type, bytes, 4)?;
            Ok(f32::from_be_bytes(bytes.try_into().unwrap()) as f64)
        }
        CqlType::Double => {
            expect_len(cql_type, bytes, 8)?;
            Ok(f64::from_be_bytes(bytes.try_into().unwrap()))
        }
        _ => Ok(read_i64(cql_type, bytes)? as f64),
    }
}

fn read_bigint(cql_type: &CqlType, bytes: &[u8]) -> Result<BigInt, String> {
    match cql_type {
        CqlType::Varint => Ok(BigInt::from_signed_bytes_be(bytes)),
        CqlType::Tinyint
        | CqlType::Smallint
        | CqlType::Int
        | CqlType::Bigint
        | CqlType::Counter => Ok(BigInt::from(read_i64(cql_type, bytes)?)),
        _ => Err(format!(
            "unsupported varint operation type {}",
            cql_type.cql_name()
        )),
    }
}

fn read_decimal_value(cql_type: &CqlType, bytes: &[u8]) -> Result<DecimalValue, String> {
    match cql_type {
        CqlType::Decimal => DecimalValue::from_bytes(bytes),
        CqlType::Varint => Ok(DecimalValue::from_bigint(BigInt::from_signed_bytes_be(
            bytes,
        ))),
        CqlType::Tinyint
        | CqlType::Smallint
        | CqlType::Int
        | CqlType::Bigint
        | CqlType::Counter => Ok(DecimalValue::from_i64(read_i64(cql_type, bytes)?)),
        CqlType::Float => {
            expect_len(cql_type, bytes, 4)?;
            DecimalValue::from_f64(f32::from_be_bytes(bytes.try_into().unwrap()) as f64)
        }
        CqlType::Double => {
            expect_len(cql_type, bytes, 8)?;
            DecimalValue::from_f64(f64::from_be_bytes(bytes.try_into().unwrap()))
        }
        _ => Err(format!(
            "unsupported decimal operation type {}",
            cql_type.cql_name()
        )),
    }
}

fn expect_len(cql_type: &CqlType, bytes: &[u8], expected: usize) -> Result<(), String> {
    if bytes.len() == expected {
        Ok(())
    } else {
        Err(format!(
            "invalid {}: expected {expected} bytes, got {}",
            cql_type.cql_name(),
            bytes.len()
        ))
    }
}

fn eval_i8(op: Operation, left: i8, right: i8) -> Result<i8, String> {
    Ok(match op {
        Operation::Add => left.wrapping_add(right),
        Operation::Subtract => left.wrapping_sub(right),
        Operation::Multiply => left.wrapping_mul(right),
        Operation::Divide => left
            .checked_div(right)
            .ok_or_else(|| "division by zero".to_string())?,
        Operation::Modulo => left
            .checked_rem(right)
            .ok_or_else(|| "modulo by zero".to_string())?,
    })
}

fn eval_i16(op: Operation, left: i16, right: i16) -> Result<i16, String> {
    Ok(match op {
        Operation::Add => left.wrapping_add(right),
        Operation::Subtract => left.wrapping_sub(right),
        Operation::Multiply => left.wrapping_mul(right),
        Operation::Divide => left
            .checked_div(right)
            .ok_or_else(|| "division by zero".to_string())?,
        Operation::Modulo => left
            .checked_rem(right)
            .ok_or_else(|| "modulo by zero".to_string())?,
    })
}

fn eval_i32(op: Operation, left: i32, right: i32) -> Result<i32, String> {
    Ok(match op {
        Operation::Add => left.wrapping_add(right),
        Operation::Subtract => left.wrapping_sub(right),
        Operation::Multiply => left.wrapping_mul(right),
        Operation::Divide => left
            .checked_div(right)
            .ok_or_else(|| "division by zero".to_string())?,
        Operation::Modulo => left
            .checked_rem(right)
            .ok_or_else(|| "modulo by zero".to_string())?,
    })
}

fn eval_i64(op: Operation, left: i64, right: i64) -> Result<i64, String> {
    Ok(match op {
        Operation::Add => left.wrapping_add(right),
        Operation::Subtract => left.wrapping_sub(right),
        Operation::Multiply => left.wrapping_mul(right),
        Operation::Divide => left
            .checked_div(right)
            .ok_or_else(|| "division by zero".to_string())?,
        Operation::Modulo => left
            .checked_rem(right)
            .ok_or_else(|| "modulo by zero".to_string())?,
    })
}

fn eval_f32(op: Operation, left: f32, right: f32) -> f32 {
    match op {
        Operation::Add => left + right,
        Operation::Subtract => left - right,
        Operation::Multiply => left * right,
        Operation::Divide => left / right,
        Operation::Modulo => left % right,
    }
}

fn eval_f64(op: Operation, left: f64, right: f64) -> f64 {
    match op {
        Operation::Add => left + right,
        Operation::Subtract => left - right,
        Operation::Multiply => left * right,
        Operation::Divide => left / right,
        Operation::Modulo => left % right,
    }
}

#[derive(Clone, Copy)]
struct DurationParts {
    months: i32,
    days: i32,
    nanoseconds: i64,
}

const MILLIS_PER_DAY: i64 = 86_400_000;
const NANOS_PER_MILLI: i64 = 1_000_000;

fn decode_duration(bytes: &[u8]) -> Result<DurationParts, String> {
    match CqlValue::deserialize_value(&CqlType::Duration, bytes) {
        Ok(CqlValue::Duration {
            months,
            days,
            nanoseconds,
        }) => Ok(DurationParts {
            months,
            days,
            nanoseconds,
        }),
        Ok(_) => Err("duration decoded to unexpected value".to_string()),
        Err(err) => Err(format!("invalid duration bytes: {err}")),
    }
}

fn apply_duration(millis: i64, duration: DurationParts, op: Operation) -> Result<i64, String> {
    let sign = match op {
        Operation::Add => 1,
        Operation::Subtract => -1,
        _ => return Err("unsupported temporal operation".to_string()),
    };
    let months = duration
        .months
        .checked_mul(sign)
        .ok_or_else(|| "duration months overflow".to_string())?;
    let days = duration
        .days
        .checked_mul(sign)
        .ok_or_else(|| "duration days overflow".to_string())?;
    let nanos = duration
        .nanoseconds
        .checked_mul(sign as i64)
        .ok_or_else(|| "duration nanoseconds overflow".to_string())?;
    add_duration_parts(millis, months, days, nanos)
}

fn add_duration_parts(
    millis: i64,
    months: i32,
    days: i32,
    nanoseconds: i64,
) -> Result<i64, String> {
    let with_months = if months == 0 {
        millis
    } else {
        add_months_utc(millis, months)?
    };
    let day_millis = (days as i64)
        .checked_mul(MILLIS_PER_DAY)
        .ok_or_else(|| "duration days overflow milliseconds".to_string())?;
    with_months
        .checked_add(day_millis)
        .and_then(|value| value.checked_add(nanoseconds / NANOS_PER_MILLI))
        .ok_or_else(|| "duration operation overflow".to_string())
}

fn millis_to_date_bytes(millis: i64) -> Vec<u8> {
    let days = (millis / MILLIS_PER_DAY) as i32;
    days.wrapping_sub(i32::MIN).to_be_bytes().to_vec()
}

fn date_bytes_to_millis(bytes: &[u8]) -> Result<i64, String> {
    if bytes.len() != 4 {
        return Err("invalid date bytes".to_string());
    }
    let encoded_days = i32::from_be_bytes(bytes.try_into().unwrap());
    let days = encoded_days.wrapping_add(i32::MIN) as i64;
    Ok(days * MILLIS_PER_DAY)
}

fn add_months_utc(millis: i64, months: i32) -> Result<i64, String> {
    let date_time = UtcDateTime::from_millis(millis)?;
    let month_index = (date_time.year as i64)
        .checked_mul(12)
        .and_then(|value| value.checked_add(date_time.month as i64 - 1))
        .and_then(|value| value.checked_add(months as i64))
        .ok_or_else(|| "month arithmetic overflow".to_string())?;
    let year = month_index.div_euclid(12);
    let month = month_index.rem_euclid(12) as u32 + 1;
    let day = date_time.day.min(days_in_month(year, month));
    UtcDateTime {
        year,
        month,
        day,
        millis_of_day: date_time.millis_of_day,
    }
    .to_millis()
}

struct UtcDateTime {
    year: i64,
    month: u32,
    day: u32,
    millis_of_day: i64,
}

impl UtcDateTime {
    fn from_millis(millis: i64) -> Result<Self, String> {
        let days = millis.div_euclid(MILLIS_PER_DAY);
        let millis_of_day = millis.rem_euclid(MILLIS_PER_DAY);
        let (year, month, day) = civil_from_days(days)?;
        Ok(Self {
            year,
            month,
            day,
            millis_of_day,
        })
    }

    fn to_millis(&self) -> Result<i64, String> {
        days_from_civil(self.year, self.month, self.day)?
            .checked_mul(MILLIS_PER_DAY)
            .and_then(|value| value.checked_add(self.millis_of_day))
            .ok_or_else(|| "datetime overflow".to_string())
    }
}

fn days_from_civil(year: i64, month: u32, day: u32) -> Result<i64, String> {
    if !(1..=12).contains(&month) || day == 0 || day > days_in_month(year, month) {
        return Err("invalid UTC date".to_string());
    }
    let year = year - i64::from(month <= 2);
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let yoe = year - era * 400;
    let month = month as i64;
    let day = day as i64;
    let mp = month + if month > 2 { -3 } else { 9 };
    let doy = (153 * mp + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    Ok(era * 146_097 + doe - 719_468)
}

fn civil_from_days(days: i64) -> Result<(i64, u32, u32), String> {
    let days = days
        .checked_add(719_468)
        .ok_or_else(|| "date conversion overflow".to_string())?;
    let era = if days >= 0 { days } else { days - 146_096 } / 146_097;
    let doe = days - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let mut year = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = mp + if mp < 10 { 3 } else { -9 };
    year += i64::from(month <= 2);
    Ok((year, month as u32, day as u32))
}

fn days_in_month(year: i64, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap_year(year) => 29,
        2 => 28,
        _ => 0,
    }
}

fn is_leap_year(year: i64) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}

fn validate_text(cql_type: &CqlType, bytes: &[u8]) -> Result<(), String> {
    if *cql_type == CqlType::Ascii && !bytes.is_ascii() {
        return Err("invalid ascii bytes".to_string());
    }
    std::str::from_utf8(bytes)
        .map(|_| ())
        .map_err(|err| format!("invalid text bytes: {err}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use cassandra_types::bigint;
    use cassandra_types::vint::encode_vint;

    #[test]
    fn fixed_numeric_operations_resolve_with_java_names_and_return_types() {
        let registry = FunctionRegistry::with_builtins();

        let f = registry
            .resolve("_add", &[CqlType::Int, CqlType::Bigint])
            .unwrap();
        assert_eq!(f.return_type(), CqlType::Bigint);
        let left = 7i32.to_be_bytes();
        let right = 5i64.to_be_bytes();
        let result = f.execute(&[Some(&left), Some(&right)]).unwrap().unwrap();
        assert_eq!(i64::from_be_bytes(result.try_into().unwrap()), 12);

        let f = registry
            .resolve("_multiply", &[CqlType::Tinyint, CqlType::Smallint])
            .unwrap();
        assert_eq!(f.return_type(), CqlType::Smallint);
        let left = [4u8];
        let right = 9i16.to_be_bytes();
        let result = f.execute(&[Some(&left), Some(&right)]).unwrap().unwrap();
        assert_eq!(i16::from_be_bytes(result.try_into().unwrap()), 36);
    }

    #[test]
    fn floating_numeric_operations_promote_like_java_operationfcts() {
        let registry = FunctionRegistry::with_builtins();

        let f = registry
            .resolve("_divide", &[CqlType::Float, CqlType::Int])
            .unwrap();
        assert_eq!(f.return_type(), CqlType::Float);
        let left = 7.5f32.to_be_bytes();
        let right = 2i32.to_be_bytes();
        let result = f.execute(&[Some(&left), Some(&right)]).unwrap().unwrap();
        assert_eq!(f32::from_be_bytes(result.try_into().unwrap()), 3.75);

        let f = registry
            .resolve("_modulo", &[CqlType::Double, CqlType::Int])
            .unwrap();
        assert_eq!(f.return_type(), CqlType::Double);
        let left = 7.5f64.to_be_bytes();
        let right = 2i32.to_be_bytes();
        let result = f.execute(&[Some(&left), Some(&right)]).unwrap().unwrap();
        assert_eq!(f64::from_be_bytes(result.try_into().unwrap()), 1.5);
    }

    #[test]
    fn numeric_operations_wrap_and_negate_like_java_primitives() {
        let registry = FunctionRegistry::with_builtins();

        let f = registry
            .resolve("_add", &[CqlType::Int, CqlType::Int])
            .unwrap();
        let left = i32::MAX.to_be_bytes();
        let right = 1i32.to_be_bytes();
        let result = f.execute(&[Some(&left), Some(&right)]).unwrap().unwrap();
        assert_eq!(i32::from_be_bytes(result.try_into().unwrap()), i32::MIN);

        let f = registry.resolve("_negate", &[CqlType::Tinyint]).unwrap();
        assert_eq!(f.return_type(), CqlType::Tinyint);
        let result = f.execute(&[Some(&[0x80])]).unwrap().unwrap();
        assert_eq!(result[0] as i8, i8::MIN);
    }

    #[test]
    fn numeric_operation_null_propagates_and_divide_by_zero_errors() {
        let registry = FunctionRegistry::with_builtins();

        let f = registry
            .resolve("_add", &[CqlType::Int, CqlType::Int])
            .unwrap();
        assert_eq!(f.execute(&[None, Some(&1i32.to_be_bytes())]).unwrap(), None);

        let f = registry
            .resolve("_divide", &[CqlType::Int, CqlType::Int])
            .unwrap();
        let err = f
            .execute(&[Some(&1i32.to_be_bytes()), Some(&0i32.to_be_bytes())])
            .unwrap_err();
        assert!(err.contains("division by zero"));
    }

    #[test]
    fn varint_operations_use_arbitrary_precision_integer_arithmetic() {
        let registry = FunctionRegistry::with_builtins();
        let left = BigInt::parse_bytes(b"123456789012345678901234567890", 10)
            .unwrap()
            .to_signed_bytes_be();
        let right = 10i32.to_be_bytes();

        let add = registry
            .resolve("_add", &[CqlType::Varint, CqlType::Int])
            .unwrap();
        assert_eq!(add.return_type(), CqlType::Varint);
        let result = add.execute(&[Some(&left), Some(&right)]).unwrap().unwrap();
        assert_eq!(
            BigInt::from_signed_bytes_be(&result).to_string(),
            "123456789012345678901234567900"
        );

        let multiply = registry
            .resolve("_multiply", &[CqlType::Int, CqlType::Varint])
            .unwrap();
        let result = multiply
            .execute(&[Some(&right), Some(&left)])
            .unwrap()
            .unwrap();
        assert_eq!(
            BigInt::from_signed_bytes_be(&result).to_string(),
            "1234567890123456789012345678900"
        );
    }

    #[test]
    fn varint_divide_modulo_negate_and_null_semantics() {
        let registry = FunctionRegistry::with_builtins();
        let left = BigInt::from(22).to_signed_bytes_be();
        let right = BigInt::from(5).to_signed_bytes_be();

        let divide = registry
            .resolve("_divide", &[CqlType::Varint, CqlType::Varint])
            .unwrap();
        let result = divide
            .execute(&[Some(&left), Some(&right)])
            .unwrap()
            .unwrap();
        assert_eq!(BigInt::from_signed_bytes_be(&result), BigInt::from(4));
        assert_eq!(divide.execute(&[None, Some(&right)]).unwrap(), None);

        let modulo = registry
            .resolve("_modulo", &[CqlType::Varint, CqlType::Varint])
            .unwrap();
        let result = modulo
            .execute(&[Some(&left), Some(&right)])
            .unwrap()
            .unwrap();
        assert_eq!(BigInt::from_signed_bytes_be(&result), BigInt::from(2));
        assert_eq!(
            modulo.execute(&[Some(&left), Some(&BigInt::zero().to_signed_bytes_be())]),
            Err("modulo by zero".to_string())
        );

        let negate = registry.resolve("_negate", &[CqlType::Varint]).unwrap();
        let result = negate.execute(&[Some(&left)]).unwrap().unwrap();
        assert_eq!(BigInt::from_signed_bytes_be(&result), BigInt::from(-22));
    }

    #[test]
    fn decimal_operations_use_bigdecimal_style_arithmetic() {
        let registry = FunctionRegistry::with_builtins();
        let left = bigint::string_to_decimal("10.50").unwrap();
        let right = bigint::string_to_decimal("2.25").unwrap();

        let add = registry
            .resolve("_add", &[CqlType::Decimal, CqlType::Decimal])
            .unwrap();
        assert_eq!(add.return_type(), CqlType::Decimal);
        let result = add.execute(&[Some(&left), Some(&right)]).unwrap().unwrap();
        assert_eq!(bigint::decimal_to_string(&result).unwrap(), "12.75");

        let multiply = registry
            .resolve("_multiply", &[CqlType::Decimal, CqlType::Int])
            .unwrap();
        let result = multiply
            .execute(&[Some(&left), Some(&3i32.to_be_bytes())])
            .unwrap()
            .unwrap();
        assert_eq!(bigint::decimal_to_string(&result).unwrap(), "31.50");

        let modulo = registry
            .resolve("_modulo", &[CqlType::Decimal, CqlType::Decimal])
            .unwrap();
        let result = modulo
            .execute(&[Some(&left), Some(&right)])
            .unwrap()
            .unwrap();
        assert_eq!(bigint::decimal_to_string(&result).unwrap(), "1.50");
    }

    #[test]
    fn decimal_divide_negate_and_varint_double_promotion() {
        let registry = FunctionRegistry::with_builtins();
        let one = bigint::string_to_decimal("1").unwrap();
        let eight = bigint::string_to_decimal("8").unwrap();

        let divide = registry
            .resolve("_divide", &[CqlType::Decimal, CqlType::Decimal])
            .unwrap();
        let result = divide
            .execute(&[Some(&one), Some(&eight)])
            .unwrap()
            .unwrap();
        assert_eq!(bigint::decimal_to_string(&result).unwrap(), "0.125");

        let negate = registry.resolve("_negate", &[CqlType::Decimal]).unwrap();
        let result = negate.execute(&[Some(&eight)]).unwrap().unwrap();
        assert_eq!(bigint::decimal_to_string(&result).unwrap(), "-8");

        let add = registry
            .resolve("_add", &[CqlType::Varint, CqlType::Double])
            .unwrap();
        assert_eq!(add.return_type(), CqlType::Decimal);
        let varint = BigInt::from(2).to_signed_bytes_be();
        let double = 0.5f64.to_be_bytes();
        let result = add
            .execute(&[Some(&varint), Some(&double)])
            .unwrap()
            .unwrap();
        assert_eq!(bigint::decimal_to_string(&result).unwrap(), "2.5");
    }

    #[test]
    fn temporal_operation_adds_and_subtracts_duration_from_timestamp() {
        let registry = FunctionRegistry::with_builtins();
        let timestamp = utc_millis(2024, 1, 31, 12 * 60 * 60 * 1000).to_be_bytes();
        let duration = duration_bytes(1, 2, 3_000_000_000);

        let f = registry
            .resolve("_add", &[CqlType::Timestamp, CqlType::Duration])
            .unwrap();
        assert_eq!(f.return_type(), CqlType::Timestamp);
        let result = f
            .execute(&[Some(&timestamp), Some(&duration)])
            .unwrap()
            .unwrap();
        assert_eq!(
            i64::from_be_bytes(result.try_into().unwrap()),
            utc_millis(2024, 3, 2, 12 * 60 * 60 * 1000 + 3_000)
        );

        let f = registry
            .resolve("_substract", &[CqlType::Timestamp, CqlType::Duration])
            .unwrap();
        let result = f
            .execute(&[Some(&timestamp), Some(&duration)])
            .unwrap()
            .unwrap();
        assert_eq!(
            i64::from_be_bytes(result.try_into().unwrap()),
            utc_millis(2023, 12, 29, 12 * 60 * 60 * 1000 - 3_000)
        );
    }

    #[test]
    fn temporal_operation_adds_duration_to_date_with_day_precision() {
        let registry = FunctionRegistry::with_builtins();
        let date = millis_to_date_bytes(utc_millis(2024, 1, 31, 0));
        let duration = duration_bytes(1, 2, 0);

        let f = registry
            .resolve("_add", &[CqlType::Date, CqlType::Duration])
            .unwrap();
        assert_eq!(f.return_type(), CqlType::Date);
        let result = f.execute(&[Some(&date), Some(&duration)]).unwrap().unwrap();
        assert_eq!(result, millis_to_date_bytes(utc_millis(2024, 3, 2, 0)));

        let invalid = duration_bytes(0, 1, 1);
        let err = f.execute(&[Some(&date), Some(&invalid)]).unwrap_err();
        assert!(err.contains("day precision"));
    }

    #[test]
    fn string_operation_concatenates_text_and_ascii_like_java() {
        let registry = FunctionRegistry::with_builtins();

        let f = registry
            .resolve("_add", &[CqlType::Ascii, CqlType::Ascii])
            .unwrap();
        assert_eq!(f.return_type(), CqlType::Ascii);
        assert_eq!(
            f.execute(&[Some(b"ab"), Some(b"cd")]).unwrap(),
            Some(b"abcd".to_vec())
        );

        let f = registry
            .resolve("_add", &[CqlType::Varchar, CqlType::Ascii])
            .unwrap();
        assert_eq!(f.return_type(), CqlType::Varchar);
        assert_eq!(
            f.execute(&[Some("hola ".as_bytes()), Some(b"mundo")])
                .unwrap(),
            Some("hola mundo".as_bytes().to_vec())
        );

        let f = registry
            .resolve("_add", &[CqlType::Ascii, CqlType::Varchar])
            .unwrap();
        assert_eq!(f.return_type(), CqlType::Varchar);
        assert_eq!(
            f.execute(&[Some(b"hello "), Some("mundo".as_bytes())])
                .unwrap(),
            Some("hello mundo".as_bytes().to_vec())
        );
    }

    fn duration_bytes(months: i64, days: i64, nanoseconds: i64) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&encode_vint(months));
        bytes.extend_from_slice(&encode_vint(days));
        bytes.extend_from_slice(&encode_vint(nanoseconds));
        bytes
    }

    fn utc_millis(year: i64, month: u32, day: u32, millis_of_day: i64) -> i64 {
        days_from_civil(year, month, day).unwrap() * MILLIS_PER_DAY + millis_of_day
    }
}
