// Licensed under Apache License, Version 2.0.

//! Math, length, and JSON functions.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.cql3.functions.MathFcts`
//! - `org.apache.cassandra.cql3.functions.ToJsonFct`
//! - `org.apache.cassandra.cql3.functions.FromJsonFct`

use super::registry::{CqlFunction, FunctionRegistry};
use cassandra_types::bigint::{string_to_varint, varint_to_string};
use cassandra_types::{CqlType, CqlValue, VectorValue};
use num_bigint::BigInt;
use num_traits::{FromPrimitive, ToPrimitive, Zero};
use std::sync::Arc;

/// Register all math/length/JSON functions.
pub fn register_all(registry: &FunctionRegistry) {
    // Math functions for each numeric type
    registry.register(Arc::new(AbsTinyint));
    registry.register(Arc::new(AbsSmallint));
    registry.register(Arc::new(AbsInt));
    registry.register(Arc::new(AbsBigint));
    registry.register(Arc::new(AbsCounter));
    registry.register(Arc::new(AbsFloat));
    registry.register(Arc::new(AbsDouble));
    registry.register(Arc::new(AbsVarint));
    registry.register(Arc::new(AbsDecimal));
    registry.register(Arc::new(ExpTinyint));
    registry.register(Arc::new(ExpSmallint));
    registry.register(Arc::new(ExpInt));
    registry.register(Arc::new(ExpBigint));
    registry.register(Arc::new(ExpCounter));
    registry.register(Arc::new(ExpFloat));
    registry.register(Arc::new(ExpDouble));
    registry.register(Arc::new(LogTinyint));
    registry.register(Arc::new(LogSmallint));
    registry.register(Arc::new(LogInt));
    registry.register(Arc::new(LogBigint));
    registry.register(Arc::new(LogCounter));
    registry.register(Arc::new(LogFloat));
    registry.register(Arc::new(LogDouble));
    registry.register(Arc::new(Log10Tinyint));
    registry.register(Arc::new(Log10Smallint));
    registry.register(Arc::new(Log10Int));
    registry.register(Arc::new(Log10Bigint));
    registry.register(Arc::new(Log10Counter));
    registry.register(Arc::new(Log10Float));
    registry.register(Arc::new(Log10Double));
    registry.register(Arc::new(ExpVarint));
    registry.register(Arc::new(LogVarint));
    registry.register(Arc::new(Log10Varint));
    registry.register(Arc::new(ExpDecimal));
    registry.register(Arc::new(LogDecimal));
    registry.register(Arc::new(Log10Decimal));

    // Ceil/Floor/Round for float/double
    registry.register(Arc::new(CeilFloat));
    registry.register(Arc::new(CeilDouble));
    registry.register(Arc::new(FloorFloat));
    registry.register(Arc::new(FloorDouble));
    registry.register(Arc::new(RoundTinyint));
    registry.register(Arc::new(RoundSmallint));
    registry.register(Arc::new(RoundInt));
    registry.register(Arc::new(RoundBigint));
    registry.register(Arc::new(RoundCounter));
    registry.register(Arc::new(RoundFloat));
    registry.register(Arc::new(RoundDouble));
    registry.register(Arc::new(RoundVarint));
    registry.register(Arc::new(RoundDecimal));

    // Length
    registry.register(Arc::new(LengthText));
    registry.register(Arc::new(LengthBlob));
    for cql_type in native_octet_length_types() {
        registry.register(Arc::new(OctetLength { cql_type }));
    }

    // JSON
    registry.register(Arc::new(ToJsonFunction { name: "tojson" }));
    registry.register(Arc::new(ToJsonFunction { name: "to_json" }));
    registry.register(Arc::new(FromJsonFunction { name: "fromjson" }));
    registry.register(Arc::new(FromJsonFunction { name: "from_json" }));
}

pub(crate) fn resolve_with_return(
    name: &str,
    arg_types: &[CqlType],
    return_type: &CqlType,
) -> Option<Arc<dyn CqlFunction>> {
    if !matches!(name.to_ascii_lowercase().as_str(), "from_json" | "fromjson") {
        return None;
    }
    if !matches!(arg_types, [CqlType::Varchar] | [CqlType::Ascii]) {
        return None;
    }
    Some(Arc::new(TypedFromJsonFunction {
        name: if name.eq_ignore_ascii_case("from_json") {
            "from_json"
        } else {
            "fromjson"
        },
        return_type: return_type.clone(),
    }))
}

pub(crate) fn resolve_dynamic(name: &str, arg_types: &[CqlType]) -> Option<Arc<dyn CqlFunction>> {
    if !matches!(name.to_ascii_lowercase().as_str(), "to_json" | "tojson") {
        return None;
    }
    let [arg_type] = arg_types else {
        return None;
    };
    Some(Arc::new(TypedToJsonFunction {
        name: if name.eq_ignore_ascii_case("to_json") {
            "to_json"
        } else {
            "tojson"
        },
        arg_type: arg_type.clone(),
    }))
}

// ── Math: abs ─────────────────────────────────────────────────────────

macro_rules! define_integer_abs {
    ($name:ident, $cql_type:expr, $rust_type:ty) => {
        struct $name;

        impl CqlFunction for $name {
            fn name(&self) -> &str {
                "abs"
            }
            fn arg_types(&self) -> Vec<CqlType> {
                vec![$cql_type]
            }
            fn return_type(&self) -> CqlType {
                $cql_type
            }
            fn execute(&self, args: &[Option<&[u8]>]) -> Result<Option<Vec<u8>>, String> {
                match args.first().and_then(|a| *a) {
                    Some(bytes) => {
                        let arr: [u8; std::mem::size_of::<$rust_type>()] =
                            bytes.try_into().map_err(|_| "invalid bytes")?;
                        let v = <$rust_type>::from_be_bytes(arr);
                        let result = v.wrapping_abs();
                        Ok(Some(result.to_be_bytes().to_vec()))
                    }
                    None => Ok(None),
                }
            }
        }
    };
}

macro_rules! define_float_abs {
    ($name:ident, $cql_type:expr, $rust_type:ty) => {
        struct $name;

        impl CqlFunction for $name {
            fn name(&self) -> &str {
                "abs"
            }
            fn arg_types(&self) -> Vec<CqlType> {
                vec![$cql_type]
            }
            fn return_type(&self) -> CqlType {
                $cql_type
            }
            fn execute(&self, args: &[Option<&[u8]>]) -> Result<Option<Vec<u8>>, String> {
                match args.first().and_then(|a| *a) {
                    Some(bytes) => {
                        let arr: [u8; std::mem::size_of::<$rust_type>()] =
                            bytes.try_into().map_err(|_| "invalid bytes")?;
                        let v = <$rust_type>::from_be_bytes(arr);
                        let result = v.abs();
                        Ok(Some(result.to_be_bytes().to_vec()))
                    }
                    None => Ok(None),
                }
            }
        }
    };
}

define_integer_abs!(AbsTinyint, CqlType::Tinyint, i8);
define_integer_abs!(AbsSmallint, CqlType::Smallint, i16);
define_integer_abs!(AbsInt, CqlType::Int, i32);
define_integer_abs!(AbsBigint, CqlType::Bigint, i64);
define_integer_abs!(AbsCounter, CqlType::Counter, i64);
define_float_abs!(AbsFloat, CqlType::Float, f32);
define_float_abs!(AbsDouble, CqlType::Double, f64);

struct AbsVarint;

impl CqlFunction for AbsVarint {
    fn name(&self) -> &str {
        "abs"
    }
    fn arg_types(&self) -> Vec<CqlType> {
        vec![CqlType::Varint]
    }
    fn return_type(&self) -> CqlType {
        CqlType::Varint
    }
    fn execute(&self, args: &[Option<&[u8]>]) -> Result<Option<Vec<u8>>, String> {
        match args.first().and_then(|a| *a) {
            Some(bytes) => {
                let value = varint_to_string(bytes);
                let positive = value.strip_prefix('-').unwrap_or(&value);
                string_to_varint(positive)
                    .map(Some)
                    .map_err(|error| error.to_string())
            }
            None => Ok(None),
        }
    }
}

struct AbsDecimal;

impl CqlFunction for AbsDecimal {
    fn name(&self) -> &str {
        "abs"
    }
    fn arg_types(&self) -> Vec<CqlType> {
        vec![CqlType::Decimal]
    }
    fn return_type(&self) -> CqlType {
        CqlType::Decimal
    }
    fn execute(&self, args: &[Option<&[u8]>]) -> Result<Option<Vec<u8>>, String> {
        let Some(bytes) = args.first().and_then(|arg| *arg) else {
            return Ok(None);
        };
        Ok(Some(
            super::decimal_math::DecimalValue::from_bytes(bytes)?
                .abs()
                .to_bytes(),
        ))
    }
}

enum DecimalMathOp {
    Exp,
    Log,
    Log10,
}

fn execute_decimal_math(
    args: &[Option<&[u8]>],
    op: DecimalMathOp,
) -> Result<Option<Vec<u8>>, String> {
    let Some(bytes) = args.first().and_then(|arg| *arg) else {
        return Ok(None);
    };
    let value = crate::functions::decimal_math::DecimalValue::from_bytes(bytes)?;
    let result = match op {
        DecimalMathOp::Exp => value.exp(),
        DecimalMathOp::Log => value.log(),
        DecimalMathOp::Log10 => value.log10(),
    }?;
    Ok(Some(result.to_bytes()))
}

// ── Math: exp, log, log10 ─────────────────────────────────────────────

macro_rules! define_unary_float_math {
    ($name:ident, $function_name:literal, $cql_type:expr, $rust_type:ty, $op:ident) => {
        struct $name;

        impl CqlFunction for $name {
            fn name(&self) -> &str {
                $function_name
            }
            fn arg_types(&self) -> Vec<CqlType> {
                vec![$cql_type]
            }
            fn return_type(&self) -> CqlType {
                $cql_type
            }
            fn execute(&self, args: &[Option<&[u8]>]) -> Result<Option<Vec<u8>>, String> {
                match args.first().and_then(|a| *a) {
                    Some(bytes) => {
                        let arr: [u8; std::mem::size_of::<$rust_type>()] =
                            bytes.try_into().map_err(|_| "invalid bytes")?;
                        let v = <$rust_type>::from_be_bytes(arr);
                        let result = v.$op();
                        Ok(Some(result.to_be_bytes().to_vec()))
                    }
                    None => Ok(None),
                }
            }
        }
    };
}

macro_rules! define_unary_integer_math {
    ($name:ident, $function_name:literal, $cql_type:expr, $rust_type:ty, $op:ident) => {
        struct $name;

        impl CqlFunction for $name {
            fn name(&self) -> &str {
                $function_name
            }
            fn arg_types(&self) -> Vec<CqlType> {
                vec![$cql_type]
            }
            fn return_type(&self) -> CqlType {
                $cql_type
            }
            fn execute(&self, args: &[Option<&[u8]>]) -> Result<Option<Vec<u8>>, String> {
                match args.first().and_then(|a| *a) {
                    Some(bytes) => {
                        let arr: [u8; std::mem::size_of::<$rust_type>()] =
                            bytes.try_into().map_err(|_| "invalid bytes")?;
                        let v = <$rust_type>::from_be_bytes(arr) as f64;
                        let result = v.$op() as $rust_type;
                        Ok(Some(result.to_be_bytes().to_vec()))
                    }
                    None => Ok(None),
                }
            }
        }
    };
}

define_unary_integer_math!(ExpTinyint, "exp", CqlType::Tinyint, i8, exp);
define_unary_integer_math!(ExpSmallint, "exp", CqlType::Smallint, i16, exp);
define_unary_integer_math!(ExpInt, "exp", CqlType::Int, i32, exp);
define_unary_integer_math!(ExpBigint, "exp", CqlType::Bigint, i64, exp);
define_unary_integer_math!(ExpCounter, "exp", CqlType::Counter, i64, exp);
define_unary_float_math!(ExpFloat, "exp", CqlType::Float, f32, exp);
define_unary_float_math!(ExpDouble, "exp", CqlType::Double, f64, exp);

define_unary_integer_math!(LogTinyint, "log", CqlType::Tinyint, i8, ln);
define_unary_integer_math!(LogSmallint, "log", CqlType::Smallint, i16, ln);
define_unary_integer_math!(LogInt, "log", CqlType::Int, i32, ln);
define_unary_integer_math!(LogBigint, "log", CqlType::Bigint, i64, ln);
define_unary_integer_math!(LogCounter, "log", CqlType::Counter, i64, ln);
define_unary_float_math!(LogFloat, "log", CqlType::Float, f32, ln);
define_unary_float_math!(LogDouble, "log", CqlType::Double, f64, ln);

define_unary_integer_math!(Log10Tinyint, "log10", CqlType::Tinyint, i8, log10);
define_unary_integer_math!(Log10Smallint, "log10", CqlType::Smallint, i16, log10);
define_unary_integer_math!(Log10Int, "log10", CqlType::Int, i32, log10);
define_unary_integer_math!(Log10Bigint, "log10", CqlType::Bigint, i64, log10);
define_unary_integer_math!(Log10Counter, "log10", CqlType::Counter, i64, log10);
define_unary_float_math!(Log10Float, "log10", CqlType::Float, f32, log10);
define_unary_float_math!(Log10Double, "log10", CqlType::Double, f64, log10);

enum VarintMathOp {
    Exp,
    Log,
    Log10,
}

fn execute_varint_math(
    args: &[Option<&[u8]>],
    op: VarintMathOp,
) -> Result<Option<Vec<u8>>, String> {
    let Some(bytes) = args.first().and_then(|arg| *arg) else {
        return Ok(None);
    };
    let input = BigInt::from_signed_bytes_be(bytes);
    let value = input
        .to_f64()
        .ok_or_else(|| "varint value is outside supported math range".to_string())?;
    let result = match op {
        VarintMathOp::Exp => value.exp(),
        VarintMathOp::Log => {
            if input <= BigInt::zero() {
                return Err("Natural log of number zero or less".to_string());
            }
            value.ln()
        }
        VarintMathOp::Log10 => {
            if input <= BigInt::zero() {
                return Err("Log10 of number zero or less".to_string());
            }
            value.log10()
        }
    };
    if !result.is_finite() {
        return Err("varint math result is not finite".to_string());
    }
    let result = BigInt::from_f64(result.trunc())
        .ok_or_else(|| "varint math result is outside supported range".to_string())?;
    Ok(Some(result.to_signed_bytes_be()))
}

struct ExpVarint;

impl CqlFunction for ExpVarint {
    fn name(&self) -> &str {
        "exp"
    }
    fn arg_types(&self) -> Vec<CqlType> {
        vec![CqlType::Varint]
    }
    fn return_type(&self) -> CqlType {
        CqlType::Varint
    }
    fn execute(&self, args: &[Option<&[u8]>]) -> Result<Option<Vec<u8>>, String> {
        execute_varint_math(args, VarintMathOp::Exp)
    }
}

struct LogVarint;

impl CqlFunction for LogVarint {
    fn name(&self) -> &str {
        "log"
    }
    fn arg_types(&self) -> Vec<CqlType> {
        vec![CqlType::Varint]
    }
    fn return_type(&self) -> CqlType {
        CqlType::Varint
    }
    fn execute(&self, args: &[Option<&[u8]>]) -> Result<Option<Vec<u8>>, String> {
        execute_varint_math(args, VarintMathOp::Log)
    }
}

struct Log10Varint;

impl CqlFunction for Log10Varint {
    fn name(&self) -> &str {
        "log10"
    }
    fn arg_types(&self) -> Vec<CqlType> {
        vec![CqlType::Varint]
    }
    fn return_type(&self) -> CqlType {
        CqlType::Varint
    }
    fn execute(&self, args: &[Option<&[u8]>]) -> Result<Option<Vec<u8>>, String> {
        execute_varint_math(args, VarintMathOp::Log10)
    }
}

struct ExpDecimal;

impl CqlFunction for ExpDecimal {
    fn name(&self) -> &str {
        "exp"
    }
    fn arg_types(&self) -> Vec<CqlType> {
        vec![CqlType::Decimal]
    }
    fn return_type(&self) -> CqlType {
        CqlType::Decimal
    }
    fn execute(&self, args: &[Option<&[u8]>]) -> Result<Option<Vec<u8>>, String> {
        execute_decimal_math(args, DecimalMathOp::Exp)
    }
}

struct LogDecimal;

impl CqlFunction for LogDecimal {
    fn name(&self) -> &str {
        "log"
    }
    fn arg_types(&self) -> Vec<CqlType> {
        vec![CqlType::Decimal]
    }
    fn return_type(&self) -> CqlType {
        CqlType::Decimal
    }
    fn execute(&self, args: &[Option<&[u8]>]) -> Result<Option<Vec<u8>>, String> {
        execute_decimal_math(args, DecimalMathOp::Log)
    }
}

struct Log10Decimal;

impl CqlFunction for Log10Decimal {
    fn name(&self) -> &str {
        "log10"
    }
    fn arg_types(&self) -> Vec<CqlType> {
        vec![CqlType::Decimal]
    }
    fn return_type(&self) -> CqlType {
        CqlType::Decimal
    }
    fn execute(&self, args: &[Option<&[u8]>]) -> Result<Option<Vec<u8>>, String> {
        execute_decimal_math(args, DecimalMathOp::Log10)
    }
}

// ── Math: ceil, floor, round ──────────────────────────────────────────

macro_rules! define_float_math {
    ($name:ident, $cql_type:expr, $rust_type:ty, $op:ident) => {
        struct $name;

        impl CqlFunction for $name {
            fn name(&self) -> &str {
                stringify!($op)
            }
            fn arg_types(&self) -> Vec<CqlType> {
                vec![$cql_type]
            }
            fn return_type(&self) -> CqlType {
                $cql_type
            }
            fn execute(&self, args: &[Option<&[u8]>]) -> Result<Option<Vec<u8>>, String> {
                match args.first().and_then(|a| *a) {
                    Some(bytes) => {
                        let arr: [u8; std::mem::size_of::<$rust_type>()] =
                            bytes.try_into().map_err(|_| "invalid bytes")?;
                        let v = <$rust_type>::from_be_bytes(arr);
                        let result = v.$op();
                        Ok(Some(result.to_be_bytes().to_vec()))
                    }
                    None => Ok(None),
                }
            }
        }
    };
}

define_float_math!(CeilFloat, CqlType::Float, f32, ceil);
define_float_math!(CeilDouble, CqlType::Double, f64, ceil);
define_float_math!(FloorFloat, CqlType::Float, f32, floor);
define_float_math!(FloorDouble, CqlType::Double, f64, floor);

macro_rules! define_integer_round {
    ($name:ident, $cql_type:expr, $rust_type:ty) => {
        struct $name;

        impl CqlFunction for $name {
            fn name(&self) -> &str {
                "round"
            }
            fn arg_types(&self) -> Vec<CqlType> {
                vec![$cql_type]
            }
            fn return_type(&self) -> CqlType {
                $cql_type
            }
            fn execute(&self, args: &[Option<&[u8]>]) -> Result<Option<Vec<u8>>, String> {
                match args.first().and_then(|a| *a) {
                    Some(bytes) => {
                        let arr: [u8; std::mem::size_of::<$rust_type>()] =
                            bytes.try_into().map_err(|_| "invalid bytes")?;
                        Ok(Some(
                            <$rust_type>::from_be_bytes(arr).to_be_bytes().to_vec(),
                        ))
                    }
                    None => Ok(None),
                }
            }
        }
    };
}

define_integer_round!(RoundTinyint, CqlType::Tinyint, i8);
define_integer_round!(RoundSmallint, CqlType::Smallint, i16);
define_integer_round!(RoundInt, CqlType::Int, i32);
define_integer_round!(RoundBigint, CqlType::Bigint, i64);
define_integer_round!(RoundCounter, CqlType::Counter, i64);
define_float_math!(RoundFloat, CqlType::Float, f32, round);
define_float_math!(RoundDouble, CqlType::Double, f64, round);

struct RoundVarint;

impl CqlFunction for RoundVarint {
    fn name(&self) -> &str {
        "round"
    }
    fn arg_types(&self) -> Vec<CqlType> {
        vec![CqlType::Varint]
    }
    fn return_type(&self) -> CqlType {
        CqlType::Varint
    }
    fn execute(&self, args: &[Option<&[u8]>]) -> Result<Option<Vec<u8>>, String> {
        Ok(args.first().and_then(|a| *a).map(|bytes| bytes.to_vec()))
    }
}

struct RoundDecimal;

impl CqlFunction for RoundDecimal {
    fn name(&self) -> &str {
        "round"
    }
    fn arg_types(&self) -> Vec<CqlType> {
        vec![CqlType::Decimal]
    }
    fn return_type(&self) -> CqlType {
        CqlType::Decimal
    }
    fn execute(&self, args: &[Option<&[u8]>]) -> Result<Option<Vec<u8>>, String> {
        let Some(bytes) = args.first().and_then(|arg| *arg) else {
            return Ok(None);
        };
        Ok(Some(
            super::decimal_math::DecimalValue::from_bytes(bytes)?
                .round_half_up_zero_scale()
                .to_bytes(),
        ))
    }
}

// ── Length ─────────────────────────────────────────────────────────────

fn native_octet_length_types() -> Vec<CqlType> {
    vec![
        CqlType::Ascii,
        CqlType::Bigint,
        CqlType::Blob,
        CqlType::Boolean,
        CqlType::Counter,
        CqlType::Decimal,
        CqlType::Double,
        CqlType::Float,
        CqlType::Int,
        CqlType::Timestamp,
        CqlType::Uuid,
        CqlType::Varchar,
        CqlType::Varint,
        CqlType::Timeuuid,
        CqlType::Inet,
        CqlType::Date,
        CqlType::Time,
        CqlType::Smallint,
        CqlType::Tinyint,
        CqlType::Duration,
    ]
}

struct LengthText;

impl CqlFunction for LengthText {
    fn name(&self) -> &str {
        "length"
    }
    fn arg_types(&self) -> Vec<CqlType> {
        vec![CqlType::Varchar]
    }
    fn return_type(&self) -> CqlType {
        CqlType::Int
    }
    fn execute(&self, args: &[Option<&[u8]>]) -> Result<Option<Vec<u8>>, String> {
        match args.first().and_then(|a| *a) {
            Some(bytes) => {
                let s = std::str::from_utf8(bytes).map_err(|_| "invalid utf8")?;
                Ok(Some(
                    (s.encode_utf16().count() as i32).to_be_bytes().to_vec(),
                ))
            }
            None => Ok(None),
        }
    }
}

struct LengthBlob;

impl CqlFunction for LengthBlob {
    fn name(&self) -> &str {
        "length"
    }
    fn arg_types(&self) -> Vec<CqlType> {
        vec![CqlType::Blob]
    }
    fn return_type(&self) -> CqlType {
        CqlType::Int
    }
    fn execute(&self, args: &[Option<&[u8]>]) -> Result<Option<Vec<u8>>, String> {
        match args.first().and_then(|a| *a) {
            Some(bytes) => Ok(Some((bytes.len() as i32).to_be_bytes().to_vec())),
            None => Ok(None),
        }
    }
}

struct OctetLength {
    cql_type: CqlType,
}

impl CqlFunction for OctetLength {
    fn name(&self) -> &str {
        "octet_length"
    }
    fn arg_types(&self) -> Vec<CqlType> {
        vec![self.cql_type.clone()]
    }
    fn return_type(&self) -> CqlType {
        CqlType::Int
    }
    fn execute(&self, args: &[Option<&[u8]>]) -> Result<Option<Vec<u8>>, String> {
        match args.first().and_then(|a| *a) {
            Some(bytes) => Ok(Some((bytes.len() as i32).to_be_bytes().to_vec())),
            None => Ok(None),
        }
    }
}

// ── toJson ────────────────────────────────────────────────────────────

struct ToJsonFunction {
    name: &'static str,
}

struct TypedToJsonFunction {
    name: &'static str,
    arg_type: CqlType,
}

impl CqlFunction for ToJsonFunction {
    fn name(&self) -> &str {
        self.name
    }
    fn arg_types(&self) -> Vec<CqlType> {
        vec![] // accepts any type
    }
    fn return_type(&self) -> CqlType {
        CqlType::Varchar
    }
    fn execute(&self, args: &[Option<&[u8]>]) -> Result<Option<Vec<u8>>, String> {
        // Without source type context, function dispatch passes raw value bytes.
        // Text values are emitted as JSON strings; non-UTF-8 values are emitted
        // as Cassandra blob literals.
        match args.first().and_then(|a| *a) {
            Some(bytes) => {
                if let Ok(s) = std::str::from_utf8(bytes) {
                    serde_json::to_vec(s)
                        .map(Some)
                        .map_err(|e| format!("json encode error: {e}"))
                } else {
                    let blob_literal = format!("0x{}", hex::encode(bytes));
                    serde_json::to_vec(&blob_literal)
                        .map(Some)
                        .map_err(|e| format!("json encode error: {e}"))
                }
            }
            None => Ok(Some(b"null".to_vec())),
        }
    }
}

impl CqlFunction for TypedToJsonFunction {
    fn name(&self) -> &str {
        self.name
    }
    fn arg_types(&self) -> Vec<CqlType> {
        vec![self.arg_type.clone()]
    }
    fn return_type(&self) -> CqlType {
        CqlType::Varchar
    }
    fn execute(&self, args: &[Option<&[u8]>]) -> Result<Option<Vec<u8>>, String> {
        let Some(bytes) = args.first().and_then(|arg| *arg) else {
            return Ok(Some(b"null".to_vec()));
        };
        let value = CqlValue::deserialize_value(&self.arg_type, bytes).map_err(|err| {
            format!(
                "invalid {} value for to_json: {err}",
                self.arg_type.cql_name()
            )
        })?;
        serde_json::to_vec(&cql_value_to_json(value))
            .map(Some)
            .map_err(|err| format!("json encode error: {err}"))
    }
}

// ── fromJson ──────────────────────────────────────────────────────────

struct FromJsonFunction {
    name: &'static str,
}

struct TypedFromJsonFunction {
    name: &'static str,
    return_type: CqlType,
}

impl CqlFunction for FromJsonFunction {
    fn name(&self) -> &str {
        self.name
    }
    fn arg_types(&self) -> Vec<CqlType> {
        vec![CqlType::Varchar]
    }
    fn return_type(&self) -> CqlType {
        CqlType::Blob // actual type determined by context
    }
    fn execute(&self, args: &[Option<&[u8]>]) -> Result<Option<Vec<u8>>, String> {
        // Without target type context, numeric values use Cassandra's widest
        // primitive encodings and strings return their UTF-8 payload.
        match args.first().and_then(|a| *a) {
            Some(bytes) => {
                let s = std::str::from_utf8(bytes).map_err(|_| "invalid utf8")?;
                let trimmed = s.trim();
                if trimmed == "null" {
                    Ok(None)
                } else if trimmed.starts_with('"') {
                    let inner: String = serde_json::from_str(trimmed)
                        .map_err(|e| format!("invalid json string: {e}"))?;
                    if let Some(hex) = inner.strip_prefix("0x") {
                        hex::decode(hex)
                            .map(Some)
                            .map_err(|e| format!("invalid blob literal: {e}"))
                    } else {
                        Ok(Some(inner.into_bytes()))
                    }
                } else if let Ok(v) = trimmed.parse::<i64>() {
                    Ok(Some(v.to_be_bytes().to_vec()))
                } else if let Ok(v) = trimmed.parse::<f64>() {
                    Ok(Some(v.to_be_bytes().to_vec()))
                } else if trimmed == "true" {
                    Ok(Some(vec![1]))
                } else if trimmed == "false" {
                    Ok(Some(vec![0]))
                } else {
                    // Return as-is (e.g. for arrays/objects)
                    Ok(Some(bytes.to_vec()))
                }
            }
            None => Ok(None),
        }
    }
}

impl CqlFunction for TypedFromJsonFunction {
    fn name(&self) -> &str {
        self.name
    }
    fn arg_types(&self) -> Vec<CqlType> {
        vec![CqlType::Varchar]
    }
    fn return_type(&self) -> CqlType {
        self.return_type.clone()
    }
    fn execute(&self, args: &[Option<&[u8]>]) -> Result<Option<Vec<u8>>, String> {
        let Some(bytes) = args.first().and_then(|arg| *arg) else {
            return Ok(None);
        };
        let json = std::str::from_utf8(bytes).map_err(|_| "invalid utf8 JSON argument")?;
        let value = serde_json::from_str::<serde_json::Value>(json)
            .map_err(|err| format!("Could not decode JSON string '{json}': {err}"))?;
        json_value_to_bytes(&value, &self.return_type)
    }
}

fn json_value_to_bytes(
    value: &serde_json::Value,
    target: &CqlType,
) -> Result<Option<Vec<u8>>, String> {
    if value.is_null() {
        return Ok(None);
    }
    match target {
        CqlType::Reversed(inner) => json_value_to_bytes(value, inner),
        CqlType::Ascii => Ok(Some(json_string(value)?.as_bytes().to_vec())),
        CqlType::Varchar => Ok(Some(json_string(value)?.into_bytes())),
        CqlType::Boolean => Ok(Some(vec![json_bool(value)? as u8])),
        CqlType::Tinyint => Ok(Some((json_i64(value)? as i8).to_be_bytes().to_vec())),
        CqlType::Smallint => Ok(Some((json_i64(value)? as i16).to_be_bytes().to_vec())),
        CqlType::Int => Ok(Some((json_i64(value)? as i32).to_be_bytes().to_vec())),
        CqlType::Bigint | CqlType::Counter | CqlType::Timestamp | CqlType::Time => {
            Ok(Some(json_i64(value)?.to_be_bytes().to_vec()))
        }
        CqlType::Float => Ok(Some((json_f64(value)? as f32).to_be_bytes().to_vec())),
        CqlType::Double => Ok(Some(json_f64(value)?.to_be_bytes().to_vec())),
        CqlType::Varint => Ok(Some(
            num_bigint::BigInt::parse_bytes(json_number_text(value)?.as_bytes(), 10)
                .ok_or_else(|| format!("invalid varint JSON value {value}"))?
                .to_signed_bytes_be(),
        )),
        CqlType::Decimal => Ok(Some(
            super::decimal_math::DecimalValue::from_string(&json_number_text(value)?)?.to_bytes(),
        )),
        CqlType::Blob => {
            let text = json_string(value)?;
            let hex = text
                .strip_prefix("0x")
                .ok_or_else(|| format!("blob JSON value must be a 0x literal, got {text}"))?;
            hex::decode(hex)
                .map(Some)
                .map_err(|err| format!("invalid blob JSON value {text}: {err}"))
        }
        CqlType::Uuid | CqlType::Timeuuid => Ok(Some(
            uuid::Uuid::parse_str(&json_string(value)?)
                .map_err(|err| format!("invalid uuid JSON value: {err}"))?
                .into_bytes()
                .to_vec(),
        )),
        CqlType::Inet => Ok(Some(
            json_string(value)?
                .parse::<std::net::IpAddr>()
                .map_err(|err| format!("invalid inet JSON value: {err}"))?
                .octets()
                .to_vec(),
        )),
        CqlType::Date => Ok(Some((json_i64(value)? as u32).to_be_bytes().to_vec())),
        CqlType::List(inner, _) | CqlType::Set(inner, _) => {
            let array = value
                .as_array()
                .ok_or_else(|| format!("expected JSON array for {}", target.cql_name()))?;
            let mut out = Vec::new();
            push_len(&mut out, array.len());
            for item in array {
                push_optional_json_value(&mut out, item, inner)?;
            }
            Ok(Some(out))
        }
        CqlType::Map(key_type, value_type, _) => {
            let object = value
                .as_object()
                .ok_or_else(|| format!("expected JSON object for {}", target.cql_name()))?;
            let mut out = Vec::new();
            push_len(&mut out, object.len());
            for (key, item) in object {
                let key_value = serde_json::Value::String(key.clone());
                push_optional_json_value(&mut out, &key_value, key_type)?;
                push_optional_json_value(&mut out, item, value_type)?;
            }
            Ok(Some(out))
        }
        CqlType::Tuple(field_types) => {
            let array = value
                .as_array()
                .ok_or_else(|| format!("expected JSON array for {}", target.cql_name()))?;
            if array.len() != field_types.len() {
                return Err(format!(
                    "tuple JSON value for {} has {} fields, expected {}",
                    target.cql_name(),
                    array.len(),
                    field_types.len()
                ));
            }
            let mut out = Vec::new();
            for (item, field_type) in array.iter().zip(field_types) {
                push_optional_json_value(&mut out, item, field_type)?;
            }
            Ok(Some(out))
        }
        CqlType::Udt {
            field_names,
            field_types,
            ..
        } => {
            let object = value
                .as_object()
                .ok_or_else(|| format!("expected JSON object for {}", target.cql_name()))?;
            let mut out = Vec::new();
            for (field_name, field_type) in field_names.iter().zip(field_types) {
                match object.get(field_name) {
                    Some(item) => push_optional_json_value(&mut out, item, field_type)?,
                    None => out.extend_from_slice(&(-1i32).to_be_bytes()),
                }
            }
            Ok(Some(out))
        }
        CqlType::Vector(inner, dimensions) if matches!(inner.as_ref(), CqlType::Float) => {
            let array = value
                .as_array()
                .ok_or_else(|| format!("expected JSON array for {}", target.cql_name()))?;
            if array.len() != *dimensions as usize {
                return Err(format!(
                    "vector JSON value for {} has {} elements, expected {}",
                    target.cql_name(),
                    array.len(),
                    dimensions
                ));
            }
            let values = array
                .iter()
                .map(|item| Ok(json_f64(item)? as f32))
                .collect::<Result<Vec<_>, String>>()?;
            Ok(Some(VectorValue::new(values).serialize()))
        }
        CqlType::Empty => Ok(Some(Vec::new())),
        _ => Err(format!(
            "from_json does not support target type {}",
            target.cql_name()
        )),
    }
}

fn cql_value_to_json(value: CqlValue) -> serde_json::Value {
    match value {
        CqlValue::Null => serde_json::Value::Null,
        CqlValue::Ascii(value) | CqlValue::Varchar(value) => serde_json::Value::String(value),
        CqlValue::Int(value) => serde_json::json!(value),
        CqlValue::Smallint(value) => serde_json::json!(value),
        CqlValue::Tinyint(value) => serde_json::json!(value),
        CqlValue::Bigint(value)
        | CqlValue::Counter(value)
        | CqlValue::Timestamp(value)
        | CqlValue::Time(value) => serde_json::json!(value),
        CqlValue::Boolean(value) => serde_json::json!(value),
        CqlValue::Float(value) => serde_json::Number::from_f64(value as f64)
            .map(serde_json::Value::Number)
            .unwrap_or_else(|| serde_json::Value::String(value.to_string())),
        CqlValue::Double(value) => serde_json::Number::from_f64(value)
            .map(serde_json::Value::Number)
            .unwrap_or_else(|| serde_json::Value::String(value.to_string())),
        CqlValue::Blob(bytes) | CqlValue::Varint(bytes) => {
            serde_json::Value::String(format!("0x{}", hex::encode(bytes)))
        }
        CqlValue::Decimal { unscaled, scale } => serde_json::Value::String(format!(
            "{}e-{}",
            num_bigint::BigInt::from_signed_bytes_be(&unscaled),
            scale
        )),
        CqlValue::Uuid(bytes) | CqlValue::Timeuuid(bytes) => {
            serde_json::Value::String(uuid::Uuid::from_bytes(bytes).to_string())
        }
        CqlValue::Inet(addr) => serde_json::Value::String(addr.to_string()),
        CqlValue::Date(value) => serde_json::json!(value),
        CqlValue::Duration {
            months,
            days,
            nanoseconds,
        } => serde_json::Value::String(format!("{months}mo{days}d{nanoseconds}ns")),
        CqlValue::Empty => serde_json::Value::String(String::new()),
        CqlValue::List(values) | CqlValue::Set(values) => {
            serde_json::Value::Array(values.into_iter().map(cql_value_to_json).collect())
        }
        CqlValue::Map(entries) => {
            let mut object = serde_json::Map::new();
            for (key, value) in entries {
                object.insert(json_object_key(key), cql_value_to_json(value));
            }
            serde_json::Value::Object(object)
        }
        CqlValue::Tuple(values) => serde_json::Value::Array(
            values
                .into_iter()
                .map(|value| {
                    value
                        .map(cql_value_to_json)
                        .unwrap_or(serde_json::Value::Null)
                })
                .collect(),
        ),
        CqlValue::Udt(fields) => {
            let mut object = serde_json::Map::new();
            for (name, value) in fields {
                object.insert(
                    name,
                    value
                        .map(cql_value_to_json)
                        .unwrap_or(serde_json::Value::Null),
                );
            }
            serde_json::Value::Object(object)
        }
        CqlValue::Vector(vector) => serde_json::Value::Array(
            vector
                .values
                .into_iter()
                .map(|value| {
                    serde_json::Number::from_f64(value as f64)
                        .map(serde_json::Value::Number)
                        .unwrap_or_else(|| serde_json::Value::String(value.to_string()))
                })
                .collect(),
        ),
    }
}

fn json_object_key(value: CqlValue) -> String {
    match cql_value_to_json(value) {
        serde_json::Value::String(value) => value,
        other => other.to_string(),
    }
}

trait IpAddrOctets {
    fn octets(self) -> Vec<u8>;
}

impl IpAddrOctets for std::net::IpAddr {
    fn octets(self) -> Vec<u8> {
        match self {
            std::net::IpAddr::V4(addr) => addr.octets().to_vec(),
            std::net::IpAddr::V6(addr) => addr.octets().to_vec(),
        }
    }
}

fn push_optional_json_value(
    out: &mut Vec<u8>,
    value: &serde_json::Value,
    target: &CqlType,
) -> Result<(), String> {
    match json_value_to_bytes(value, target)? {
        Some(bytes) => {
            out.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
            out.extend_from_slice(&bytes);
        }
        None => out.extend_from_slice(&(-1i32).to_be_bytes()),
    }
    Ok(())
}

fn push_len(out: &mut Vec<u8>, len: usize) {
    out.extend_from_slice(&(len as i32).to_be_bytes());
}

fn json_string(value: &serde_json::Value) -> Result<String, String> {
    value
        .as_str()
        .map(ToString::to_string)
        .ok_or_else(|| format!("expected JSON string, got {value}"))
}

fn json_bool(value: &serde_json::Value) -> Result<bool, String> {
    value
        .as_bool()
        .ok_or_else(|| format!("expected JSON boolean, got {value}"))
}

fn json_i64(value: &serde_json::Value) -> Result<i64, String> {
    value
        .as_i64()
        .or_else(|| value.as_str().and_then(|text| text.parse::<i64>().ok()))
        .ok_or_else(|| format!("expected JSON integer, got {value}"))
}

fn json_f64(value: &serde_json::Value) -> Result<f64, String> {
    value
        .as_f64()
        .or_else(|| value.as_str().and_then(|text| text.parse::<f64>().ok()))
        .ok_or_else(|| format!("expected JSON number, got {value}"))
}

fn json_number_text(value: &serde_json::Value) -> Result<String, String> {
    match value {
        serde_json::Value::Number(number) => Ok(number.to_string()),
        serde_json::Value::String(text) => Ok(text.clone()),
        _ => Err(format!("expected JSON number, got {value}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cassandra_types::bigint::decimal_to_string;

    #[test]
    fn abs_int() {
        let f = AbsInt;
        let bytes = (-42i32).to_be_bytes();
        let result = f.execute(&[Some(&bytes)]).unwrap().unwrap();
        assert_eq!(i32::from_be_bytes(result.try_into().unwrap()), 42);
    }

    #[test]
    fn abs_integral_overloads_match_java_overflow_behavior() {
        let tiny = AbsTinyint;
        let bytes = i8::MIN.to_be_bytes();
        let result = tiny.execute(&[Some(&bytes)]).unwrap().unwrap();
        assert_eq!(i8::from_be_bytes(result.try_into().unwrap()), i8::MIN);

        let small = AbsSmallint;
        let bytes = (-42i16).to_be_bytes();
        let result = small.execute(&[Some(&bytes)]).unwrap().unwrap();
        assert_eq!(i16::from_be_bytes(result.try_into().unwrap()), 42);

        let counter = AbsCounter;
        let bytes = (-42i64).to_be_bytes();
        let result = counter.execute(&[Some(&bytes)]).unwrap().unwrap();
        assert_eq!(i64::from_be_bytes(result.try_into().unwrap()), 42);
    }

    #[test]
    fn abs_varint_uses_arbitrary_precision_encoding() {
        let f = AbsVarint;
        let bytes = string_to_varint("-123456789012345678901234567890").unwrap();
        let result = f.execute(&[Some(&bytes)]).unwrap().unwrap();
        assert_eq!(varint_to_string(&result), "123456789012345678901234567890");
    }

    #[test]
    fn abs_double() {
        let f = AbsDouble;
        let bytes = (-std::f64::consts::PI).to_be_bytes();
        let result = f.execute(&[Some(&bytes)]).unwrap().unwrap();
        let v = f64::from_be_bytes(result.try_into().unwrap());
        assert!((v - std::f64::consts::PI).abs() < 1e-10);
    }

    #[test]
    fn abs_decimal_uses_big_decimal_sign_semantics() {
        let f = AbsDecimal;
        let bytes = cassandra_types::bigint::string_to_decimal("-123.450").unwrap();
        let result = f.execute(&[Some(&bytes)]).unwrap().unwrap();
        assert_eq!(decimal_to_string(&result).unwrap(), "123.450");
    }

    #[test]
    fn ceil_float() {
        let f = CeilFloat;
        let bytes = 2.3f32.to_be_bytes();
        let result = f.execute(&[Some(&bytes)]).unwrap().unwrap();
        let v = f32::from_be_bytes(result.try_into().unwrap());
        assert_eq!(v, 3.0);
    }

    #[test]
    fn floor_double() {
        let f = FloorDouble;
        let bytes = 2.7f64.to_be_bytes();
        let result = f.execute(&[Some(&bytes)]).unwrap().unwrap();
        let v = f64::from_be_bytes(result.try_into().unwrap());
        assert_eq!(v, 2.0);
    }

    #[test]
    fn round_float() {
        let f = RoundFloat;
        let bytes = 2.5f32.to_be_bytes();
        let result = f.execute(&[Some(&bytes)]).unwrap().unwrap();
        let v = f32::from_be_bytes(result.try_into().unwrap());
        assert_eq!(v, 3.0);
    }

    #[test]
    fn round_integral_overloads_return_input_type() {
        let tiny = RoundTinyint;
        let bytes = (-7i8).to_be_bytes();
        let result = tiny.execute(&[Some(&bytes)]).unwrap().unwrap();
        assert_eq!(i8::from_be_bytes(result.try_into().unwrap()), -7);

        let small = RoundSmallint;
        let bytes = (-7i16).to_be_bytes();
        let result = small.execute(&[Some(&bytes)]).unwrap().unwrap();
        assert_eq!(i16::from_be_bytes(result.try_into().unwrap()), -7);

        let int = RoundInt;
        let bytes = (-7i32).to_be_bytes();
        let result = int.execute(&[Some(&bytes)]).unwrap().unwrap();
        assert_eq!(i32::from_be_bytes(result.try_into().unwrap()), -7);

        let bigint = RoundBigint;
        let bytes = (-7i64).to_be_bytes();
        let result = bigint.execute(&[Some(&bytes)]).unwrap().unwrap();
        assert_eq!(i64::from_be_bytes(result.try_into().unwrap()), -7);

        let counter = RoundCounter;
        let bytes = 11i64.to_be_bytes();
        let result = counter.execute(&[Some(&bytes)]).unwrap().unwrap();
        assert_eq!(i64::from_be_bytes(result.try_into().unwrap()), 11);
    }

    #[test]
    fn round_varint_returns_input_encoding() {
        let f = RoundVarint;
        let bytes = string_to_varint("-123456789012345678901234567890").unwrap();
        let result = f.execute(&[Some(&bytes)]).unwrap().unwrap();
        assert_eq!(result, bytes);
    }

    #[test]
    fn round_decimal_uses_big_decimal_half_up_zero_scale() {
        let f = RoundDecimal;
        for (input, expected) in [("2.5", "3"), ("-2.5", "-3"), ("2.4", "2")] {
            let bytes = cassandra_types::bigint::string_to_decimal(input).unwrap();
            let result = f.execute(&[Some(&bytes)]).unwrap().unwrap();
            assert_eq!(decimal_to_string(&result).unwrap(), expected);
        }
    }

    #[test]
    fn exp_log_log10_decimal_match_java_return_type_for_identity_values() {
        let registry = FunctionRegistry::with_builtins();
        for (name, input, expected) in
            [("exp", "0", "1"), ("log", "1", "0"), ("log10", "1000", "3")]
        {
            let function = registry.resolve(name, &[CqlType::Decimal]).unwrap();
            assert_eq!(function.return_type(), CqlType::Decimal);
            let source = cassandra_types::bigint::string_to_decimal(input).unwrap();
            let result = function.execute(&[Some(&source)]).unwrap().unwrap();
            assert_eq!(decimal_to_string(&result).unwrap(), expected);
        }
    }

    #[test]
    fn decimal_logs_reject_zero_or_negative_like_java() {
        let registry = FunctionRegistry::with_builtins();
        for name in ["log", "log10"] {
            let function = registry.resolve(name, &[CqlType::Decimal]).unwrap();
            for input in ["0", "-1"] {
                let source = cassandra_types::bigint::string_to_decimal(input).unwrap();
                let err = function.execute(&[Some(&source)]).unwrap_err();
                if name == "log" {
                    assert_eq!(err, "Natural log of number zero or less");
                } else {
                    assert_eq!(err, "Log10 of number zero or less");
                }
            }
        }
    }

    #[test]
    fn builtins_resolve_numeric_mathfcts_overloads() {
        let registry = FunctionRegistry::with_builtins();
        for (name, cql_type) in [
            ("abs", CqlType::Tinyint),
            ("abs", CqlType::Smallint),
            ("abs", CqlType::Counter),
            ("abs", CqlType::Varint),
            ("abs", CqlType::Decimal),
            ("round", CqlType::Tinyint),
            ("round", CqlType::Smallint),
            ("round", CqlType::Int),
            ("round", CqlType::Bigint),
            ("round", CqlType::Counter),
            ("round", CqlType::Varint),
            ("round", CqlType::Decimal),
            ("exp", CqlType::Tinyint),
            ("exp", CqlType::Varint),
            ("exp", CqlType::Decimal),
            ("log", CqlType::Smallint),
            ("log", CqlType::Varint),
            ("log", CqlType::Decimal),
            ("log10", CqlType::Counter),
            ("log10", CqlType::Varint),
            ("log10", CqlType::Decimal),
        ] {
            let function = registry
                .resolve(name, std::slice::from_ref(&cql_type))
                .unwrap_or_else(|| panic!("expected {name}({})", cql_type.cql_name()));
            assert_eq!(function.return_type(), cql_type);
        }
    }

    #[test]
    fn exp_log_log10_integer_math_matches_java_casts() {
        let exp = ExpInt;
        let input = 2i32.to_be_bytes();
        let result = exp.execute(&[Some(&input)]).unwrap().unwrap();
        assert_eq!(
            i32::from_be_bytes(result.try_into().unwrap()),
            2f64.exp() as i32
        );

        let log = LogInt;
        let input = 100i32.to_be_bytes();
        let result = log.execute(&[Some(&input)]).unwrap().unwrap();
        assert_eq!(
            i32::from_be_bytes(result.try_into().unwrap()),
            100f64.ln() as i32
        );

        let log10 = Log10Int;
        let input = 1000i32.to_be_bytes();
        let result = log10.execute(&[Some(&input)]).unwrap().unwrap();
        assert_eq!(i32::from_be_bytes(result.try_into().unwrap()), 3);
    }

    #[test]
    fn exp_log_log10_varint_math_matches_java_truncating_return_type() {
        let exp = ExpVarint;
        let input = string_to_varint("2").unwrap();
        let result = exp.execute(&[Some(&input)]).unwrap().unwrap();
        assert_eq!(varint_to_string(&result), "7");

        let log = LogVarint;
        let input = string_to_varint("100").unwrap();
        let result = log.execute(&[Some(&input)]).unwrap().unwrap();
        assert_eq!(varint_to_string(&result), "4");

        let log10 = Log10Varint;
        let input = string_to_varint("1000").unwrap();
        let result = log10.execute(&[Some(&input)]).unwrap().unwrap();
        assert_eq!(varint_to_string(&result), "3");
    }

    #[test]
    fn log_varint_rejects_zero_or_negative_like_java() {
        let log = LogVarint;
        let zero = string_to_varint("0").unwrap();
        assert_eq!(
            log.execute(&[Some(&zero)]).unwrap_err(),
            "Natural log of number zero or less"
        );

        let log10 = Log10Varint;
        let negative = string_to_varint("-1").unwrap();
        assert_eq!(
            log10.execute(&[Some(&negative)]).unwrap_err(),
            "Log10 of number zero or less"
        );
    }

    #[test]
    fn exp_log_log10_float_math_preserves_float_type() {
        let exp = ExpFloat;
        let input = 2.0f32.to_be_bytes();
        let result = exp.execute(&[Some(&input)]).unwrap().unwrap();
        let value = f32::from_be_bytes(result.try_into().unwrap());
        assert!((value - 2.0f32.exp()).abs() < 0.000001);

        let log = LogDouble;
        let input = std::f64::consts::E.to_be_bytes();
        let result = log.execute(&[Some(&input)]).unwrap().unwrap();
        let value = f64::from_be_bytes(result.try_into().unwrap());
        assert!((value - 1.0).abs() < 0.000000000001);

        let log10 = Log10Double;
        let input = 1000.0f64.to_be_bytes();
        let result = log10.execute(&[Some(&input)]).unwrap().unwrap();
        let value = f64::from_be_bytes(result.try_into().unwrap());
        assert!((value - 3.0).abs() < 0.000000000001);
    }

    #[test]
    fn length_text() {
        let f = LengthText;
        let bytes = b"hello";
        let result = f.execute(&[Some(bytes.as_slice())]).unwrap().unwrap();
        assert_eq!(i32::from_be_bytes(result.try_into().unwrap()), 5);
    }

    #[test]
    fn length_text_matches_java_utf16_code_units() {
        let f = LengthText;
        let bytes = "a😀b".as_bytes();
        let result = f.execute(&[Some(bytes)]).unwrap().unwrap();
        assert_eq!(i32::from_be_bytes(result.try_into().unwrap()), 4);
    }

    #[test]
    fn length_blob() {
        let f = LengthBlob;
        let bytes = vec![1, 2, 3, 4];
        let result = f.execute(&[Some(&bytes)]).unwrap().unwrap();
        assert_eq!(i32::from_be_bytes(result.try_into().unwrap()), 4);
    }

    #[test]
    fn octet_length_counts_raw_serialized_bytes() {
        let text = OctetLength {
            cql_type: CqlType::Varchar,
        };
        let bytes = "a😀b".as_bytes();
        let result = text.execute(&[Some(bytes)]).unwrap().unwrap();
        assert_eq!(i32::from_be_bytes(result.try_into().unwrap()), 6);

        let int = OctetLength {
            cql_type: CqlType::Int,
        };
        let bytes = 10i32.to_be_bytes();
        let result = int.execute(&[Some(&bytes)]).unwrap().unwrap();
        assert_eq!(i32::from_be_bytes(result.try_into().unwrap()), 4);
    }

    #[test]
    fn builtins_resolve_octet_length_for_native_types() {
        let registry = FunctionRegistry::with_builtins();
        for cql_type in native_octet_length_types() {
            let function = registry
                .resolve("octet_length", std::slice::from_ref(&cql_type))
                .unwrap_or_else(|| panic!("expected octet_length({})", cql_type.cql_name()));
            assert_eq!(function.return_type(), CqlType::Int);
        }
    }

    #[test]
    fn to_json_text() {
        let f = ToJsonFunction { name: "tojson" };
        let bytes = b"hello \"cql\"\n";
        let result = f.execute(&[Some(bytes.as_slice())]).unwrap().unwrap();
        assert_eq!(
            String::from_utf8(result).unwrap(),
            "\"hello \\\"cql\\\"\\n\""
        );
    }

    #[test]
    fn to_json_blob() {
        let f = ToJsonFunction { name: "tojson" };
        let bytes = [0, 159, 255];
        let result = f.execute(&[Some(bytes.as_slice())]).unwrap().unwrap();
        assert_eq!(String::from_utf8(result).unwrap(), "\"0x009fff\"");
    }

    #[test]
    fn to_json_null() {
        let f = ToJsonFunction { name: "tojson" };
        let result = f.execute(&[None]).unwrap().unwrap();
        assert_eq!(String::from_utf8(result).unwrap(), "null");
    }

    #[test]
    fn from_json_integer() {
        let f = FromJsonFunction { name: "fromjson" };
        let bytes = b"42";
        let result = f.execute(&[Some(bytes.as_slice())]).unwrap().unwrap();
        let v = i64::from_be_bytes(result.try_into().unwrap());
        assert_eq!(v, 42);
    }

    #[test]
    fn from_json_null() {
        let f = FromJsonFunction { name: "fromjson" };
        let bytes = b"null";
        let result = f.execute(&[Some(bytes.as_slice())]).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn from_json_string() {
        let f = FromJsonFunction { name: "fromjson" };
        let bytes = b"\"hello \\\"cql\\\"\\n\"";
        let result = f.execute(&[Some(bytes.as_slice())]).unwrap().unwrap();
        assert_eq!(String::from_utf8(result).unwrap(), "hello \"cql\"\n");
    }

    #[test]
    fn from_json_blob_literal() {
        let f = FromJsonFunction { name: "fromjson" };
        let bytes = b"\"0x009fff\"";
        let result = f.execute(&[Some(bytes.as_slice())]).unwrap().unwrap();
        assert_eq!(result, vec![0, 159, 255]);
    }

    #[test]
    fn json_functions_resolve_java_and_legacy_names() {
        let registry = FunctionRegistry::with_builtins();
        for name in ["to_json", "tojson"] {
            let function = registry.resolve(name, &[CqlType::Varchar]).unwrap();
            assert_eq!(function.name(), name);
            assert_eq!(function.return_type(), CqlType::Varchar);
        }
        for name in ["from_json", "fromjson"] {
            let function = registry.resolve(name, &[CqlType::Varchar]).unwrap();
            assert_eq!(function.name(), name);
            assert_eq!(function.return_type(), CqlType::Blob);
        }
    }

    #[test]
    fn to_json_resolves_typed_function_for_non_text_values() {
        let registry = FunctionRegistry::with_builtins();
        let int_json = registry.resolve("to_json", &[CqlType::Int]).unwrap();
        assert_eq!(int_json.arg_types(), vec![CqlType::Int]);
        assert_eq!(
            String::from_utf8(
                int_json
                    .execute(&[Some(&42i32.to_be_bytes())])
                    .unwrap()
                    .unwrap()
            )
            .unwrap(),
            "42"
        );

        let list_type = CqlType::List(Box::new(CqlType::Int), false);
        let list_json = registry
            .resolve("tojson", std::slice::from_ref(&list_type))
            .unwrap();
        let list = CqlValue::List(vec![CqlValue::Int(1), CqlValue::Int(2)]).serialize_value();
        assert_eq!(
            String::from_utf8(list_json.execute(&[Some(&list)]).unwrap().unwrap()).unwrap(),
            "[1,2]"
        );
    }

    #[test]
    fn from_json_resolves_with_receiver_return_type() {
        let registry = FunctionRegistry::with_builtins();
        let function = registry
            .resolve_with_return("from_json", &[CqlType::Varchar], &CqlType::Int)
            .unwrap();
        assert_eq!(function.return_type(), CqlType::Int);
        assert_eq!(
            function.execute(&[Some(b"42")]).unwrap(),
            Some(42i32.to_be_bytes().to_vec())
        );

        let list_type = CqlType::List(Box::new(CqlType::Int), false);
        let function = registry
            .resolve_with_return("fromjson", &[CqlType::Varchar], &list_type)
            .unwrap();
        assert_eq!(function.return_type(), list_type);
        let bytes = function.execute(&[Some(b"[1,2,3]")]).unwrap().unwrap();
        assert_eq!(
            CqlValue::deserialize_value(&function.return_type(), &bytes).unwrap(),
            CqlValue::List(vec![CqlValue::Int(1), CqlValue::Int(2), CqlValue::Int(3)])
        );
    }

    #[test]
    fn from_json_typed_supports_maps_tuples_and_nulls() {
        let registry = FunctionRegistry::with_builtins();
        let map_type = CqlType::Map(
            Box::new(CqlType::Varchar),
            Box::new(CqlType::Tuple(vec![CqlType::Int, CqlType::Varchar])),
            false,
        );
        let function = registry
            .resolve_with_return("from_json", &[CqlType::Varchar], &map_type)
            .unwrap();
        let bytes = function
            .execute(&[Some(br#"{"a":[7,"seven"],"b":null}"#)])
            .unwrap()
            .unwrap();
        assert_eq!(
            CqlValue::deserialize_value(&function.return_type(), &bytes).unwrap(),
            CqlValue::Map(vec![
                (
                    CqlValue::Varchar("a".to_string()),
                    CqlValue::Tuple(vec![
                        Some(CqlValue::Int(7)),
                        Some(CqlValue::Varchar("seven".to_string()))
                    ])
                ),
                (
                    CqlValue::Varchar("b".to_string()),
                    CqlValue::Tuple(vec![None, None])
                )
            ])
        );
    }
}
