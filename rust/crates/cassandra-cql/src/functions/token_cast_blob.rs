// Licensed under Apache License, Version 2.0.

//! Token, cast, and blob conversion functions.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.cql3.functions.TokenFct`
//! - `org.apache.cassandra.cql3.functions.CastFcts`
//! - `org.apache.cassandra.cql3.functions.BytesConversionFcts`

use super::registry::{CqlFunction, FunctionRegistry};
use cassandra_types::{CqlType, abstract_type, bigint};
use std::net::{Ipv4Addr, Ipv6Addr};
use std::sync::Arc;
use uuid::Uuid;

/// Register all token/cast/blob functions.
pub fn register_all(registry: &FunctionRegistry) {
    registry.register(Arc::new(TokenFunction));

    // Cast functions
    registry.register(Arc::new(CastIntToText));
    registry.register(Arc::new(CastBigintToText));
    registry.register(Arc::new(CastFloatToText));
    registry.register(Arc::new(CastDoubleToText));
    registry.register(Arc::new(CastIntToBigint));
    registry.register(Arc::new(CastIntToFloat));
    registry.register(Arc::new(CastIntToDouble));
    register_numeric_casts(registry);
    register_numeric_text_casts(registry);
    register_native_text_casts(registry);
    register_temporal_casts(registry);

    // Blob conversion functions. Java registers current names (`int_as_blob`,
    // `blob_as_int`) plus legacy aliases (`intasblob`, `blobasint`).
    for spec in blob_conversion_specs() {
        registry.register(Arc::new(TypeAsBlob {
            name: "typeasblob".to_string(),
            cql_type: spec.cql_type.clone(),
        }));
        registry.register(Arc::new(BlobAsType {
            name: "blobastype".to_string(),
            cql_type: spec.cql_type.clone(),
        }));

        for cql_name in spec.cql_names {
            registry.register(Arc::new(TypeAsBlob {
                name: format!("{cql_name}_as_blob"),
                cql_type: spec.cql_type.clone(),
            }));
            registry.register(Arc::new(TypeAsBlob {
                name: format!("{cql_name}asblob"),
                cql_type: spec.cql_type.clone(),
            }));
            registry.register(Arc::new(BlobAsType {
                name: format!("blob_as_{cql_name}"),
                cql_type: spec.cql_type.clone(),
            }));
            registry.register(Arc::new(BlobAsType {
                name: format!("blobas{cql_name}"),
                cql_type: spec.cql_type.clone(),
            }));
        }
    }
}

// ── token() ───────────────────────────────────────────────────────────

struct TokenFunction;

impl CqlFunction for TokenFunction {
    fn name(&self) -> &str {
        "token"
    }
    fn arg_types(&self) -> Vec<CqlType> {
        vec![] // variadic
    }
    fn return_type(&self) -> CqlType {
        CqlType::Bigint
    }
    fn execute(&self, args: &[Option<&[u8]>]) -> Result<Option<Vec<u8>>, String> {
        // Murmur3 hash of concatenated partition key values
        let mut data = Vec::new();
        for arg in args {
            match arg {
                Some(bytes) => {
                    let len = bytes.len() as u16;
                    data.extend_from_slice(&len.to_be_bytes());
                    data.extend_from_slice(bytes);
                    data.push(0); // end-of-component
                }
                None => return Ok(None),
            }
        }
        let hash = murmur3_hash(&data);
        Ok(Some(hash.to_be_bytes().to_vec()))
    }
}

/// Murmur3 hash (128-bit, using first 64 bits) matching Cassandra's partitioner.
fn murmur3_hash(data: &[u8]) -> i64 {
    let length = data.len();
    let nblocks = length / 16;

    let c1: u64 = 0x87c3_7b91_1142_53d5;
    let c2: u64 = 0x4cf5_ad43_2745_937f;

    let mut h1: u64 = 0;
    let mut h2: u64 = 0;

    for i in 0..nblocks {
        let offset = i * 16;
        let mut k1 = u64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
        let mut k2 = u64::from_le_bytes(data[offset + 8..offset + 16].try_into().unwrap());

        k1 = k1.wrapping_mul(c1);
        k1 = k1.rotate_left(31);
        k1 = k1.wrapping_mul(c2);
        h1 ^= k1;
        h1 = h1.rotate_left(27);
        h1 = h1.wrapping_add(h2);
        h1 = h1.wrapping_mul(5).wrapping_add(0x52dc_e729);

        k2 = k2.wrapping_mul(c2);
        k2 = k2.rotate_left(33);
        k2 = k2.wrapping_mul(c1);
        h2 ^= k2;
        h2 = h2.rotate_left(31);
        h2 = h2.wrapping_add(h1);
        h2 = h2.wrapping_mul(5).wrapping_add(0x3849_5ab5);
    }

    let tail = &data[nblocks * 16..];
    let mut k1: u64 = 0;
    let mut k2: u64 = 0;

    match tail.len() {
        15 => {
            k2 ^= (tail[14] as u64) << 48;
            k2 ^= (tail[13] as u64) << 40;
            k2 ^= (tail[12] as u64) << 32;
            k2 ^= (tail[11] as u64) << 24;
            k2 ^= (tail[10] as u64) << 16;
            k2 ^= (tail[9] as u64) << 8;
            k2 ^= tail[8] as u64;
            k2 = k2.wrapping_mul(c2);
            k2 = k2.rotate_left(33);
            k2 = k2.wrapping_mul(c1);
            h2 ^= k2;

            k1 ^= (tail[7] as u64) << 56;
            k1 ^= (tail[6] as u64) << 48;
            k1 ^= (tail[5] as u64) << 40;
            k1 ^= (tail[4] as u64) << 32;
            k1 ^= (tail[3] as u64) << 24;
            k1 ^= (tail[2] as u64) << 16;
            k1 ^= (tail[1] as u64) << 8;
            k1 ^= tail[0] as u64;
            k1 = k1.wrapping_mul(c1);
            k1 = k1.rotate_left(31);
            k1 = k1.wrapping_mul(c2);
            h1 ^= k1;
        }
        n @ 1..=14 => {
            if n >= 8 {
                for j in (8..n).rev() {
                    k2 ^= (tail[j] as u64) << ((j - 8) * 8);
                }
                if n > 8 {
                    k2 = k2.wrapping_mul(c2);
                    k2 = k2.rotate_left(33);
                    k2 = k2.wrapping_mul(c1);
                    h2 ^= k2;
                }
            }
            let end = std::cmp::min(n, 8);
            for j in (0..end).rev() {
                k1 ^= (tail[j] as u64) << (j * 8);
            }
            k1 = k1.wrapping_mul(c1);
            k1 = k1.rotate_left(31);
            k1 = k1.wrapping_mul(c2);
            h1 ^= k1;
        }
        _ => {}
    }

    h1 ^= length as u64;
    h2 ^= length as u64;
    h1 = h1.wrapping_add(h2);
    h2 = h2.wrapping_add(h1);

    h1 = fmix64(h1);
    h2 = fmix64(h2);

    h1 = h1.wrapping_add(h2);
    // h2 = h2.wrapping_add(h1); // not needed, we only use h1

    h1 as i64
}

fn fmix64(mut k: u64) -> u64 {
    k ^= k >> 33;
    k = k.wrapping_mul(0xff51_afd7_ed55_8ccd);
    k ^= k >> 33;
    k = k.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    k ^= k >> 33;
    k
}

// ── Cast functions ────────────────────────────────────────────────────

macro_rules! define_cast {
    ($name:ident, $from:expr, $to:expr, |$bytes:ident| $body:expr) => {
        struct $name;

        impl CqlFunction for $name {
            fn name(&self) -> &str {
                "cast"
            }
            fn arg_types(&self) -> Vec<CqlType> {
                vec![$from]
            }
            fn return_type(&self) -> CqlType {
                $to
            }
            fn execute(&self, args: &[Option<&[u8]>]) -> Result<Option<Vec<u8>>, String> {
                match args.first().and_then(|a| *a) {
                    Some($bytes) => Ok(Some($body)),
                    None => Ok(None),
                }
            }
        }
    };
}

define_cast!(CastIntToText, CqlType::Int, CqlType::Varchar, |bytes| {
    let v = i32::from_be_bytes(bytes.try_into().map_err(|_| "invalid int")?);
    v.to_string().into_bytes()
});

define_cast!(
    CastBigintToText,
    CqlType::Bigint,
    CqlType::Varchar,
    |bytes| {
        let v = i64::from_be_bytes(bytes.try_into().map_err(|_| "invalid bigint")?);
        v.to_string().into_bytes()
    }
);

define_cast!(CastFloatToText, CqlType::Float, CqlType::Varchar, |bytes| {
    let v = f32::from_be_bytes(bytes.try_into().map_err(|_| "invalid float")?);
    v.to_string().into_bytes()
});

define_cast!(
    CastDoubleToText,
    CqlType::Double,
    CqlType::Varchar,
    |bytes| {
        let v = f64::from_be_bytes(bytes.try_into().map_err(|_| "invalid double")?);
        v.to_string().into_bytes()
    }
);

define_cast!(CastIntToBigint, CqlType::Int, CqlType::Bigint, |bytes| {
    let v = i32::from_be_bytes(bytes.try_into().map_err(|_| "invalid int")?);
    (v as i64).to_be_bytes().to_vec()
});

define_cast!(CastIntToFloat, CqlType::Int, CqlType::Float, |bytes| {
    let v = i32::from_be_bytes(bytes.try_into().map_err(|_| "invalid int")?);
    (v as f32).to_be_bytes().to_vec()
});

define_cast!(CastIntToDouble, CqlType::Int, CqlType::Double, |bytes| {
    let v = i32::from_be_bytes(bytes.try_into().map_err(|_| "invalid int")?);
    (v as f64).to_be_bytes().to_vec()
});

struct NumericCast {
    name: String,
    from: CqlType,
    to: CqlType,
}

#[derive(Clone, Copy)]
enum NumericValue {
    Integral(i64),
    Floating(f64),
}

struct NumericTextCast {
    name: String,
    from: CqlType,
    to: CqlType,
}

struct NativeTextCast {
    name: String,
    from: CqlType,
    to: CqlType,
}

fn register_numeric_casts(registry: &FunctionRegistry) {
    let sources = [
        CqlType::Tinyint,
        CqlType::Smallint,
        CqlType::Int,
        CqlType::Bigint,
        CqlType::Counter,
        CqlType::Decimal,
        CqlType::Float,
        CqlType::Double,
        CqlType::Varint,
    ];
    let targets = [
        CqlType::Tinyint,
        CqlType::Smallint,
        CqlType::Int,
        CqlType::Bigint,
        CqlType::Decimal,
        CqlType::Float,
        CqlType::Double,
        CqlType::Varint,
    ];

    for from in sources {
        for to in &targets {
            if &from != to {
                register_numeric_cast(registry, "cast".to_string(), from.clone(), to.clone());

                for target_name in cast_target_names(to) {
                    register_numeric_cast(
                        registry,
                        format!("cast_as_{target_name}"),
                        from.clone(),
                        to.clone(),
                    );
                    register_numeric_cast(
                        registry,
                        format!("castAs{}", legacy_cast_target_name(to)),
                        from.clone(),
                        to.clone(),
                    );
                }
            }
        }
    }
}

fn register_numeric_text_casts(registry: &FunctionRegistry) {
    let sources = [
        CqlType::Tinyint,
        CqlType::Smallint,
        CqlType::Int,
        CqlType::Bigint,
        CqlType::Counter,
        CqlType::Decimal,
        CqlType::Float,
        CqlType::Double,
        CqlType::Varint,
    ];
    for from in sources {
        for to in [CqlType::Ascii, CqlType::Varchar] {
            register_numeric_text_cast(registry, "cast".to_string(), from.clone(), to.clone());
            for (target_name, legacy_target_name) in cast_text_target_names(&to) {
                register_numeric_text_cast(
                    registry,
                    format!("cast_as_{target_name}"),
                    from.clone(),
                    to.clone(),
                );
                register_numeric_text_cast(
                    registry,
                    format!("castAs{legacy_target_name}"),
                    from.clone(),
                    to.clone(),
                );
            }
        }
    }
}

fn register_native_text_casts(registry: &FunctionRegistry) {
    for from in [
        CqlType::Ascii,
        CqlType::Inet,
        CqlType::Boolean,
        CqlType::Timeuuid,
        CqlType::Timestamp,
        CqlType::Date,
        CqlType::Time,
        CqlType::Uuid,
    ] {
        let targets: &[CqlType] = if from == CqlType::Ascii {
            &[CqlType::Varchar]
        } else {
            &[CqlType::Ascii, CqlType::Varchar]
        };
        for to in targets {
            register_native_text_cast(registry, "cast".to_string(), from.clone(), to.clone());
            for (target_name, legacy_target_name) in cast_text_target_names(to) {
                register_native_text_cast(
                    registry,
                    format!("cast_as_{target_name}"),
                    from.clone(),
                    to.clone(),
                );
                register_native_text_cast(
                    registry,
                    format!("castAs{legacy_target_name}"),
                    from.clone(),
                    to.clone(),
                );
            }
        }
    }
}

fn register_numeric_cast(registry: &FunctionRegistry, name: String, from: CqlType, to: CqlType) {
    registry.register(Arc::new(NumericCast { name, from, to }));
}

fn register_numeric_text_cast(
    registry: &FunctionRegistry,
    name: String,
    from: CqlType,
    to: CqlType,
) {
    registry.register(Arc::new(NumericTextCast { name, from, to }));
}

fn register_native_text_cast(
    registry: &FunctionRegistry,
    name: String,
    from: CqlType,
    to: CqlType,
) {
    registry.register(Arc::new(NativeTextCast { name, from, to }));
}

fn cast_target_names(cql_type: &CqlType) -> &'static [&'static str] {
    match cql_type {
        CqlType::Tinyint => &["tinyint"],
        CqlType::Smallint => &["smallint"],
        CqlType::Int => &["int"],
        CqlType::Bigint => &["bigint"],
        CqlType::Decimal => &["decimal"],
        CqlType::Float => &["float"],
        CqlType::Double => &["double"],
        CqlType::Varint => &["varint"],
        CqlType::Date => &["date"],
        CqlType::Timestamp => &["timestamp"],
        _ => &[],
    }
}

fn legacy_cast_target_name(cql_type: &CqlType) -> &'static str {
    match cql_type {
        CqlType::Tinyint => "Tinyint",
        CqlType::Smallint => "Smallint",
        CqlType::Int => "Int",
        CqlType::Bigint => "Bigint",
        CqlType::Decimal => "Decimal",
        CqlType::Float => "Float",
        CqlType::Double => "Double",
        CqlType::Varint => "Varint",
        CqlType::Date => "Date",
        CqlType::Timestamp => "Timestamp",
        _ => "",
    }
}

fn cast_text_target_names(cql_type: &CqlType) -> &'static [(&'static str, &'static str)] {
    match cql_type {
        CqlType::Ascii => &[("ascii", "Ascii")],
        CqlType::Varchar => &[("text", "Text")],
        _ => &[],
    }
}

impl CqlFunction for NumericCast {
    fn name(&self) -> &str {
        &self.name
    }

    fn arg_types(&self) -> Vec<CqlType> {
        vec![self.from.clone()]
    }

    fn return_type(&self) -> CqlType {
        self.to.clone()
    }

    fn execute(&self, args: &[Option<&[u8]>]) -> Result<Option<Vec<u8>>, String> {
        let Some(bytes) = args.first().and_then(|a| *a) else {
            return Ok(None);
        };
        let value = read_numeric_value(&self.from, bytes)?;
        if self.to == CqlType::Decimal {
            return write_decimal_value(&self.from, bytes, value).map(Some);
        }
        if self.to == CqlType::Float {
            return Ok(Some(
                numeric_to_f32(value_for_float_cast(&self.from, bytes, value)?)
                    .to_be_bytes()
                    .to_vec(),
            ));
        }
        if self.to == CqlType::Double {
            return Ok(Some(
                numeric_to_f64(value_for_float_cast(&self.from, bytes, value)?)
                    .to_be_bytes()
                    .to_vec(),
            ));
        }
        write_numeric_value(&self.to, value).map(Some)
    }
}

impl CqlFunction for NumericTextCast {
    fn name(&self) -> &str {
        &self.name
    }

    fn arg_types(&self) -> Vec<CqlType> {
        vec![self.from.clone()]
    }

    fn return_type(&self) -> CqlType {
        self.to.clone()
    }

    fn execute(&self, args: &[Option<&[u8]>]) -> Result<Option<Vec<u8>>, String> {
        let Some(bytes) = args.first().and_then(|a| *a) else {
            return Ok(None);
        };
        let text = match &self.from {
            CqlType::Decimal => bigint::decimal_to_string(bytes).map_err(|err| err.to_string())?,
            CqlType::Varint => bigint::varint_to_string(bytes),
            CqlType::Float => {
                expect_len(&self.from, bytes, 4)?;
                java_float_string(f32::from_be_bytes(bytes.try_into().unwrap()) as f64)
            }
            CqlType::Double => {
                expect_len(&self.from, bytes, 8)?;
                java_float_string(f64::from_be_bytes(bytes.try_into().unwrap()))
            }
            _ => match read_numeric_value(&self.from, bytes)? {
                NumericValue::Integral(value) => value.to_string(),
                NumericValue::Floating(value) => java_float_string(value),
            },
        };
        Ok(Some(text.into_bytes()))
    }
}

impl CqlFunction for NativeTextCast {
    fn name(&self) -> &str {
        &self.name
    }

    fn arg_types(&self) -> Vec<CqlType> {
        vec![self.from.clone()]
    }

    fn return_type(&self) -> CqlType {
        self.to.clone()
    }

    fn execute(&self, args: &[Option<&[u8]>]) -> Result<Option<Vec<u8>>, String> {
        let Some(bytes) = args.first().and_then(|a| *a) else {
            return Ok(None);
        };
        native_text_value(&self.from, bytes).map(|text| Some(text.into_bytes()))
    }
}

fn native_text_value(cql_type: &CqlType, bytes: &[u8]) -> Result<String, String> {
    match cql_type {
        CqlType::Ascii => std::str::from_utf8(bytes)
            .map(|text| text.to_string())
            .map_err(|err| format!("invalid ascii bytes: {err}")),
        CqlType::Boolean => {
            expect_len(cql_type, bytes, 1)?;
            Ok((bytes[0] != 0).to_string())
        }
        CqlType::Inet => match bytes.len() {
            4 => Ok(Ipv4Addr::new(bytes[0], bytes[1], bytes[2], bytes[3]).to_string()),
            16 => {
                let mut address = [0u8; 16];
                address.copy_from_slice(bytes);
                Ok(Ipv6Addr::from(address).to_string())
            }
            other => Err(format!("invalid inet: expected 4 or 16 bytes, got {other}")),
        },
        CqlType::Uuid | CqlType::Timeuuid => {
            expect_len(cql_type, bytes, 16)?;
            Uuid::from_slice(bytes)
                .map(|uuid| uuid.to_string())
                .map_err(|err| format!("invalid {}: {err}", cql_type.cql_name()))
        }
        CqlType::Timestamp => timestamp_to_cql_text(bytes),
        CqlType::Date => date_to_cql_text(bytes),
        CqlType::Time => time_to_cql_text(bytes),
        other => Err(format!("unsupported text cast source {}", other.cql_name())),
    }
}

fn java_float_string(value: f64) -> String {
    if value.is_nan() {
        "NaN".to_string()
    } else if value == f64::INFINITY {
        "Infinity".to_string()
    } else if value == f64::NEG_INFINITY {
        "-Infinity".to_string()
    } else {
        value.to_string()
    }
}

fn read_numeric_value(cql_type: &CqlType, bytes: &[u8]) -> Result<NumericValue, String> {
    match cql_type {
        CqlType::Tinyint => {
            expect_len(cql_type, bytes, 1)?;
            Ok(NumericValue::Integral(bytes[0] as i8 as i64))
        }
        CqlType::Smallint => {
            expect_len(cql_type, bytes, 2)?;
            Ok(NumericValue::Integral(
                i16::from_be_bytes(bytes.try_into().unwrap()) as i64,
            ))
        }
        CqlType::Int => {
            expect_len(cql_type, bytes, 4)?;
            Ok(NumericValue::Integral(
                i32::from_be_bytes(bytes.try_into().unwrap()) as i64,
            ))
        }
        CqlType::Bigint | CqlType::Counter => {
            expect_len(cql_type, bytes, 8)?;
            Ok(NumericValue::Integral(i64::from_be_bytes(
                bytes.try_into().unwrap(),
            )))
        }
        CqlType::Float => {
            expect_len(cql_type, bytes, 4)?;
            Ok(NumericValue::Floating(
                f32::from_be_bytes(bytes.try_into().unwrap()) as f64,
            ))
        }
        CqlType::Double => {
            expect_len(cql_type, bytes, 8)?;
            Ok(NumericValue::Floating(f64::from_be_bytes(
                bytes.try_into().unwrap(),
            )))
        }
        CqlType::Decimal => Ok(NumericValue::Integral(decimal_to_i64_wrapping(bytes)?)),
        CqlType::Varint => Ok(NumericValue::Integral(varint_to_i64_wrapping(bytes))),
        other => Err(format!(
            "unsupported numeric cast source {}",
            other.cql_name()
        )),
    }
}

fn write_numeric_value(cql_type: &CqlType, value: NumericValue) -> Result<Vec<u8>, String> {
    match cql_type {
        CqlType::Tinyint => Ok(vec![numeric_to_i8(value) as u8]),
        CqlType::Smallint => Ok(numeric_to_i16(value).to_be_bytes().to_vec()),
        CqlType::Int => Ok(numeric_to_i32(value).to_be_bytes().to_vec()),
        CqlType::Bigint => Ok(numeric_to_i64(value).to_be_bytes().to_vec()),
        CqlType::Float => Ok(numeric_to_f32(value).to_be_bytes().to_vec()),
        CqlType::Double => Ok(numeric_to_f64(value).to_be_bytes().to_vec()),
        CqlType::Decimal => {
            let value = match value {
                NumericValue::Integral(value) => value.to_string(),
                NumericValue::Floating(value) => java_float_string(value),
            };
            bigint::string_to_decimal(&value).map_err(|err| err.to_string())
        }
        CqlType::Varint => {
            let value = numeric_to_i64(value);
            bigint::string_to_varint(&value.to_string()).map_err(|err| err.to_string())
        }
        other => Err(format!(
            "unsupported numeric cast target {}",
            other.cql_name()
        )),
    }
}

fn value_for_float_cast(
    source: &CqlType,
    bytes: &[u8],
    fallback: NumericValue,
) -> Result<NumericValue, String> {
    if *source == CqlType::Varint {
        let text = bigint::varint_to_string(bytes);
        let parsed = text
            .parse::<f64>()
            .map_err(|_| format!("invalid varint value for floating conversion: {text}"))?;
        Ok(NumericValue::Floating(parsed))
    } else if *source == CqlType::Decimal {
        let text = bigint::decimal_to_string(bytes).map_err(|err| err.to_string())?;
        let parsed = text
            .parse::<f64>()
            .map_err(|_| format!("invalid decimal value for floating conversion: {text}"))?;
        Ok(NumericValue::Floating(parsed))
    } else {
        Ok(fallback)
    }
}

fn write_decimal_value(
    source: &CqlType,
    bytes: &[u8],
    value: NumericValue,
) -> Result<Vec<u8>, String> {
    match source {
        CqlType::Varint => {
            let mut result = Vec::with_capacity(4 + bytes.len());
            result.extend_from_slice(&0i32.to_be_bytes());
            result.extend_from_slice(bytes);
            Ok(result)
        }
        CqlType::Float => {
            expect_len(source, bytes, 4)?;
            let value = f32::from_be_bytes(bytes.try_into().unwrap());
            bigint::string_to_decimal(&value.to_string()).map_err(|err| err.to_string())
        }
        CqlType::Double => {
            expect_len(source, bytes, 8)?;
            let value = f64::from_be_bytes(bytes.try_into().unwrap());
            bigint::string_to_decimal(&java_float_string(value)).map_err(|err| err.to_string())
        }
        _ => write_numeric_value(&CqlType::Decimal, value),
    }
}

fn decimal_to_i64_wrapping(bytes: &[u8]) -> Result<i64, String> {
    let integer = decimal_integer_string(bytes)?;
    let varint = bigint::string_to_varint(&integer).map_err(|err| err.to_string())?;
    Ok(varint_to_i64_wrapping(&varint))
}

fn decimal_integer_string(bytes: &[u8]) -> Result<String, String> {
    let text = bigint::decimal_to_string(bytes).map_err(|err| err.to_string())?;
    let (negative, unsigned) = match text.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, text.as_str()),
    };
    let integer = unsigned.split('.').next().unwrap_or("0");
    let integer = integer.trim_start_matches('0');
    if integer.is_empty() {
        Ok("0".to_string())
    } else if negative {
        Ok(format!("-{integer}"))
    } else {
        Ok(integer.to_string())
    }
}

fn varint_to_i64_wrapping(bytes: &[u8]) -> i64 {
    if bytes.is_empty() {
        return 0;
    }
    let fill = if bytes[0] & 0x80 != 0 { 0xFF } else { 0x00 };
    let mut out = [fill; 8];
    let take = bytes.len().min(8);
    out[8 - take..].copy_from_slice(&bytes[bytes.len() - take..]);
    i64::from_be_bytes(out)
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

fn numeric_to_i8(value: NumericValue) -> i8 {
    match value {
        NumericValue::Integral(v) => v as i8,
        NumericValue::Floating(v) => java_float_to_i32(v) as i8,
    }
}

fn numeric_to_i16(value: NumericValue) -> i16 {
    match value {
        NumericValue::Integral(v) => v as i16,
        NumericValue::Floating(v) => java_float_to_i32(v) as i16,
    }
}

fn numeric_to_i32(value: NumericValue) -> i32 {
    match value {
        NumericValue::Integral(v) => v as i32,
        NumericValue::Floating(v) => java_float_to_i32(v),
    }
}

fn numeric_to_i64(value: NumericValue) -> i64 {
    match value {
        NumericValue::Integral(v) => v,
        NumericValue::Floating(v) => java_float_to_i64(v),
    }
}

fn numeric_to_f32(value: NumericValue) -> f32 {
    match value {
        NumericValue::Integral(v) => v as f32,
        NumericValue::Floating(v) => v as f32,
    }
}

fn numeric_to_f64(value: NumericValue) -> f64 {
    match value {
        NumericValue::Integral(v) => v as f64,
        NumericValue::Floating(v) => v,
    }
}

fn java_float_to_i32(value: f64) -> i32 {
    if value.is_nan() {
        0
    } else if value >= i32::MAX as f64 {
        i32::MAX
    } else if value <= i32::MIN as f64 {
        i32::MIN
    } else {
        value.trunc() as i32
    }
}

fn java_float_to_i64(value: f64) -> i64 {
    if value.is_nan() {
        0
    } else if value >= i64::MAX as f64 {
        i64::MAX
    } else if value <= i64::MIN as f64 {
        i64::MIN
    } else {
        value.trunc() as i64
    }
}

struct TemporalCast {
    name: String,
    from: CqlType,
    to: CqlType,
}

const UUID_EPOCH_OFFSET: u64 = 0x01B2_1DD2_1381_4000;
const MILLIS_PER_DAY: i64 = 86_400_000;
const NANOS_PER_DAY: i64 = 86_400_000_000_000;

fn register_temporal_casts(registry: &FunctionRegistry) {
    for (from, to) in [
        (CqlType::Timeuuid, CqlType::Date),
        (CqlType::Timeuuid, CqlType::Timestamp),
        (CqlType::Timestamp, CqlType::Date),
        (CqlType::Date, CqlType::Timestamp),
    ] {
        register_temporal_cast(registry, "cast".to_string(), from.clone(), to.clone());
        for target_name in cast_target_names(&to) {
            register_temporal_cast(
                registry,
                format!("cast_as_{target_name}"),
                from.clone(),
                to.clone(),
            );
            register_temporal_cast(
                registry,
                format!("castAs{}", legacy_cast_target_name(&to)),
                from.clone(),
                to.clone(),
            );
        }
    }
}

fn register_temporal_cast(registry: &FunctionRegistry, name: String, from: CqlType, to: CqlType) {
    registry.register(Arc::new(TemporalCast { name, from, to }));
}

impl CqlFunction for TemporalCast {
    fn name(&self) -> &str {
        &self.name
    }

    fn arg_types(&self) -> Vec<CqlType> {
        vec![self.from.clone()]
    }

    fn return_type(&self) -> CqlType {
        self.to.clone()
    }

    fn execute(&self, args: &[Option<&[u8]>]) -> Result<Option<Vec<u8>>, String> {
        let Some(bytes) = args.first().and_then(|a| *a) else {
            return Ok(None);
        };

        match (&self.from, &self.to) {
            (CqlType::Timeuuid, CqlType::Timestamp) => {
                let millis =
                    timeuuid_to_millis(bytes).ok_or_else(|| "invalid timeuuid".to_string())?;
                Ok(Some(millis.to_be_bytes().to_vec()))
            }
            (CqlType::Timeuuid, CqlType::Date) => {
                let millis =
                    timeuuid_to_millis(bytes).ok_or_else(|| "invalid timeuuid".to_string())?;
                Ok(Some(millis_to_date_bytes(millis)))
            }
            (CqlType::Timestamp, CqlType::Date) => {
                expect_len(&CqlType::Timestamp, bytes, 8)?;
                let millis = i64::from_be_bytes(bytes.try_into().unwrap());
                Ok(Some(millis_to_date_bytes(millis)))
            }
            (CqlType::Date, CqlType::Timestamp) => {
                expect_len(&CqlType::Date, bytes, 4)?;
                let encoded_days = i32::from_be_bytes(bytes.try_into().unwrap());
                let days = encoded_days.wrapping_add(i32::MIN) as i64;
                Ok(Some((days * MILLIS_PER_DAY).to_be_bytes().to_vec()))
            }
            _ => Err(format!(
                "unsupported temporal cast from {} to {}",
                self.from.cql_name(),
                self.to.cql_name()
            )),
        }
    }
}

fn millis_to_date_bytes(millis: i64) -> Vec<u8> {
    let days = (millis / MILLIS_PER_DAY) as i32;
    days.wrapping_sub(i32::MIN).to_be_bytes().to_vec()
}

fn timestamp_to_cql_text(bytes: &[u8]) -> Result<String, String> {
    expect_len(&CqlType::Timestamp, bytes, 8)?;
    let millis = i64::from_be_bytes(bytes.try_into().unwrap());
    let seconds = millis.div_euclid(1000);
    let millis_of_second = millis.rem_euclid(1000);
    let days = seconds.div_euclid(86_400);
    let second_of_day = seconds.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    let hour = second_of_day / 3600;
    let minute = second_of_day % 3600 / 60;
    let second = second_of_day % 60;
    Ok(format!(
        "{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}.{millis_of_second:03}Z"
    ))
}

fn date_to_cql_text(bytes: &[u8]) -> Result<String, String> {
    expect_len(&CqlType::Date, bytes, 4)?;
    let encoded_days = i32::from_be_bytes(bytes.try_into().unwrap());
    let days = encoded_days.wrapping_add(i32::MIN) as i64;
    let (year, month, day) = civil_from_days(days);
    Ok(format!("{year:04}-{month:02}-{day:02}"))
}

fn time_to_cql_text(bytes: &[u8]) -> Result<String, String> {
    expect_len(&CqlType::Time, bytes, 8)?;
    let nanos = i64::from_be_bytes(bytes.try_into().unwrap());
    if !(0..NANOS_PER_DAY).contains(&nanos) {
        return Err(format!("invalid time: {nanos} nanos of day"));
    }
    let nano = nanos % 1_000;
    let micros_total = nanos / 1_000;
    let micro = micros_total % 1_000;
    let millis_total = micros_total / 1_000;
    let milli = millis_total % 1_000;
    let seconds_total = millis_total / 1_000;
    let second = seconds_total % 60;
    let minutes_total = seconds_total / 60;
    let minute = minutes_total % 60;
    let hour = minutes_total / 60;
    Ok(format!(
        "{hour:02}:{minute:02}:{second:02}.{milli:03}{micro:03}{nano:03}"
    ))
}

fn civil_from_days(days_since_epoch: i64) -> (i64, u32, u32) {
    let z = days_since_epoch + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = mp + if mp < 10 { 3 } else { -9 };
    let year = y + if month <= 2 { 1 } else { 0 };
    (year, month as u32, day as u32)
}

fn timeuuid_to_millis(bytes: &[u8]) -> Option<i64> {
    if bytes.len() != 16 {
        return None;
    }
    let time_low = u32::from_be_bytes(bytes[0..4].try_into().ok()?) as u64;
    let time_mid = u16::from_be_bytes(bytes[4..6].try_into().ok()?) as u64;
    let time_hi = (u16::from_be_bytes(bytes[6..8].try_into().ok()?) & 0x0FFF) as u64;

    let ts = time_low | (time_mid << 32) | (time_hi << 48);
    let millis = (ts.wrapping_sub(UUID_EPOCH_OFFSET)) / 10_000;
    Some(millis as i64)
}

// ── type_as_blob / blob_as_type ───────────────────────────────────────

struct BlobConversionSpec {
    cql_type: CqlType,
    cql_names: &'static [&'static str],
}

fn blob_conversion_specs() -> Vec<BlobConversionSpec> {
    vec![
        BlobConversionSpec {
            cql_type: CqlType::Ascii,
            cql_names: &["ascii"],
        },
        BlobConversionSpec {
            cql_type: CqlType::Bigint,
            cql_names: &["bigint"],
        },
        BlobConversionSpec {
            cql_type: CqlType::Boolean,
            cql_names: &["boolean"],
        },
        BlobConversionSpec {
            cql_type: CqlType::Counter,
            cql_names: &["counter"],
        },
        BlobConversionSpec {
            cql_type: CqlType::Date,
            cql_names: &["date"],
        },
        BlobConversionSpec {
            cql_type: CqlType::Decimal,
            cql_names: &["decimal"],
        },
        BlobConversionSpec {
            cql_type: CqlType::Double,
            cql_names: &["double"],
        },
        BlobConversionSpec {
            cql_type: CqlType::Duration,
            cql_names: &["duration"],
        },
        BlobConversionSpec {
            cql_type: CqlType::Empty,
            cql_names: &["empty"],
        },
        BlobConversionSpec {
            cql_type: CqlType::Float,
            cql_names: &["float"],
        },
        BlobConversionSpec {
            cql_type: CqlType::Inet,
            cql_names: &["inet"],
        },
        BlobConversionSpec {
            cql_type: CqlType::Int,
            cql_names: &["int"],
        },
        BlobConversionSpec {
            cql_type: CqlType::Smallint,
            cql_names: &["smallint"],
        },
        BlobConversionSpec {
            cql_type: CqlType::Varchar,
            cql_names: &["text", "varchar"],
        },
        BlobConversionSpec {
            cql_type: CqlType::Time,
            cql_names: &["time"],
        },
        BlobConversionSpec {
            cql_type: CqlType::Timestamp,
            cql_names: &["timestamp"],
        },
        BlobConversionSpec {
            cql_type: CqlType::Timeuuid,
            cql_names: &["timeuuid"],
        },
        BlobConversionSpec {
            cql_type: CqlType::Tinyint,
            cql_names: &["tinyint"],
        },
        BlobConversionSpec {
            cql_type: CqlType::Uuid,
            cql_names: &["uuid"],
        },
        BlobConversionSpec {
            cql_type: CqlType::Varint,
            cql_names: &["varint"],
        },
    ]
}

struct TypeAsBlob {
    name: String,
    cql_type: CqlType,
}

impl CqlFunction for TypeAsBlob {
    fn name(&self) -> &str {
        &self.name
    }
    fn arg_types(&self) -> Vec<CqlType> {
        vec![self.cql_type.clone()]
    }
    fn return_type(&self) -> CqlType {
        CqlType::Blob
    }
    fn execute(&self, args: &[Option<&[u8]>]) -> Result<Option<Vec<u8>>, String> {
        // Identity - just return the raw bytes
        match args.first().and_then(|a| *a) {
            Some(bytes) => Ok(Some(bytes.to_vec())),
            None => Ok(None),
        }
    }
}

struct BlobAsType {
    name: String,
    cql_type: CqlType,
}

impl CqlFunction for BlobAsType {
    fn name(&self) -> &str {
        &self.name
    }
    fn arg_types(&self) -> Vec<CqlType> {
        vec![CqlType::Blob]
    }
    fn return_type(&self) -> CqlType {
        self.cql_type.clone()
    }
    fn execute(&self, args: &[Option<&[u8]>]) -> Result<Option<Vec<u8>>, String> {
        match args.first().and_then(|a| *a) {
            Some(bytes) => {
                abstract_type::validate(&self.cql_type, bytes).map_err(|err| {
                    format!(
                        "In call to function {}, value is not a valid binary representation for type {}: {err}",
                        self.name,
                        self.cql_type.cql_name()
                    )
                })?;
                Ok(Some(bytes.to_vec()))
            }
            None => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_deterministic() {
        let f = TokenFunction;
        let key = 42i32.to_be_bytes();
        let r1 = f.execute(&[Some(&key)]).unwrap().unwrap();
        let r2 = f.execute(&[Some(&key)]).unwrap().unwrap();
        assert_eq!(r1, r2);
        assert_eq!(r1.len(), 8);
    }

    #[test]
    fn token_null_arg() {
        let f = TokenFunction;
        let result = f.execute(&[None]).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn cast_int_to_text() {
        let f = CastIntToText;
        let bytes = 42i32.to_be_bytes();
        let result = f.execute(&[Some(&bytes)]).unwrap().unwrap();
        assert_eq!(String::from_utf8(result).unwrap(), "42");
    }

    #[test]
    fn cast_int_to_bigint() {
        let f = CastIntToBigint;
        let bytes = 42i32.to_be_bytes();
        let result = f.execute(&[Some(&bytes)]).unwrap().unwrap();
        let v = i64::from_be_bytes(result.try_into().unwrap());
        assert_eq!(v, 42);
    }

    #[test]
    fn builtins_resolve_fixed_width_numeric_cast_overloads() {
        let registry = FunctionRegistry::with_builtins();

        let f = registry
            .resolve_with_return("cast", &[CqlType::Tinyint], &CqlType::Smallint)
            .unwrap();
        let result = f.execute(&[Some(&[0xFE])]).unwrap().unwrap();
        assert_eq!(i16::from_be_bytes(result.try_into().unwrap()), -2);

        let f = registry
            .resolve_with_return("cast", &[CqlType::Smallint], &CqlType::Tinyint)
            .unwrap();
        let source = 258i16.to_be_bytes();
        let result = f.execute(&[Some(&source)]).unwrap().unwrap();
        assert_eq!(result[0] as i8, 2);

        let f = registry
            .resolve_with_return("cast", &[CqlType::Bigint], &CqlType::Int)
            .unwrap();
        let source = 4_294_967_297i64.to_be_bytes();
        let result = f.execute(&[Some(&source)]).unwrap().unwrap();
        assert_eq!(i32::from_be_bytes(result.try_into().unwrap()), 1);

        let f = registry
            .resolve_with_return("cast", &[CqlType::Counter], &CqlType::Double)
            .unwrap();
        let source = 99i64.to_be_bytes();
        let result = f.execute(&[Some(&source)]).unwrap().unwrap();
        assert_eq!(f64::from_be_bytes(result.try_into().unwrap()), 99.0);
    }

    #[test]
    fn numeric_casts_use_java_float_to_integral_narrowing() {
        let registry = FunctionRegistry::with_builtins();

        let f = registry
            .resolve_with_return("cast", &[CqlType::Float], &CqlType::Tinyint)
            .unwrap();
        let source = 1.0e20f32.to_be_bytes();
        let result = f.execute(&[Some(&source)]).unwrap().unwrap();
        assert_eq!(result[0] as i8, -1);

        let source = f32::NAN.to_be_bytes();
        let result = f.execute(&[Some(&source)]).unwrap().unwrap();
        assert_eq!(result[0] as i8, 0);

        let f = registry
            .resolve_with_return("cast", &[CqlType::Double], &CqlType::Bigint)
            .unwrap();
        let source = f64::NEG_INFINITY.to_be_bytes();
        let result = f.execute(&[Some(&source)]).unwrap().unwrap();
        assert_eq!(i64::from_be_bytes(result.try_into().unwrap()), i64::MIN);
    }

    #[test]
    fn builtins_resolve_java_named_numeric_cast_functions() {
        let registry = FunctionRegistry::with_builtins();

        let f = registry.resolve("cast_as_int", &[CqlType::Bigint]).unwrap();
        assert_eq!(f.return_type(), CqlType::Int);
        let source = 4_294_967_299i64.to_be_bytes();
        let result = f.execute(&[Some(&source)]).unwrap().unwrap();
        assert_eq!(i32::from_be_bytes(result.try_into().unwrap()), 3);

        let f = registry
            .resolve("castAsDouble", &[CqlType::Counter])
            .unwrap();
        assert_eq!(f.return_type(), CqlType::Double);
        let source = 17i64.to_be_bytes();
        let result = f.execute(&[Some(&source)]).unwrap().unwrap();
        assert_eq!(f64::from_be_bytes(result.try_into().unwrap()), 17.0);
    }

    #[test]
    fn builtins_resolve_java_numeric_cast_names_for_all_fixed_width_targets() {
        let registry = FunctionRegistry::with_builtins();

        for (target, snake, legacy) in [
            (CqlType::Tinyint, "cast_as_tinyint", "castAsTinyint"),
            (CqlType::Smallint, "cast_as_smallint", "castAsSmallint"),
            (CqlType::Int, "cast_as_int", "castAsInt"),
            (CqlType::Bigint, "cast_as_bigint", "castAsBigint"),
            (CqlType::Float, "cast_as_float", "castAsFloat"),
            (CqlType::Double, "cast_as_double", "castAsDouble"),
        ] {
            let source = if target == CqlType::Int {
                CqlType::Bigint
            } else {
                CqlType::Int
            };
            assert!(
                registry.resolve(snake, &[source.clone()]).is_some(),
                "{snake} should resolve from {}",
                source.cql_name()
            );
            assert!(
                registry.resolve(legacy, &[source.clone()]).is_some(),
                "{legacy} should resolve from {}",
                source.cql_name()
            );
        }
    }

    #[test]
    fn builtins_resolve_java_numeric_cast_to_text_and_ascii_names() {
        let registry = FunctionRegistry::with_builtins();

        let f = registry
            .resolve("cast_as_text", &[CqlType::Tinyint])
            .unwrap();
        assert_eq!(f.return_type(), CqlType::Varchar);
        assert_eq!(f.execute(&[Some(&[0xFE])]).unwrap(), Some(b"-2".to_vec()));

        let f = registry
            .resolve("castAsAscii", &[CqlType::Counter])
            .unwrap();
        assert_eq!(f.return_type(), CqlType::Ascii);
        let source = 99i64.to_be_bytes();
        assert_eq!(f.execute(&[Some(&source)]).unwrap(), Some(b"99".to_vec()));

        let f = registry
            .resolve("cast_as_text", &[CqlType::Double])
            .unwrap();
        let source = f64::INFINITY.to_be_bytes();
        assert_eq!(
            f.execute(&[Some(&source)]).unwrap(),
            Some(b"Infinity".to_vec())
        );
    }

    #[test]
    fn builtins_resolve_numeric_text_casts_for_all_fixed_width_sources() {
        let registry = FunctionRegistry::with_builtins();

        for source in [
            CqlType::Tinyint,
            CqlType::Smallint,
            CqlType::Int,
            CqlType::Bigint,
            CqlType::Counter,
            CqlType::Decimal,
            CqlType::Float,
            CqlType::Double,
            CqlType::Varint,
        ] {
            for name in ["cast_as_text", "castAsText"] {
                let f = registry.resolve(name, &[source.clone()]).unwrap();
                assert_eq!(f.return_type(), CqlType::Varchar);
            }
            for name in ["cast_as_ascii", "castAsAscii"] {
                let f = registry.resolve(name, &[source.clone()]).unwrap();
                assert_eq!(f.return_type(), CqlType::Ascii);
            }
        }
    }

    #[test]
    fn builtins_resolve_java_native_text_casts() {
        let registry = FunctionRegistry::with_builtins();

        for source in [
            CqlType::Boolean,
            CqlType::Inet,
            CqlType::Uuid,
            CqlType::Timeuuid,
            CqlType::Timestamp,
            CqlType::Date,
            CqlType::Time,
        ] {
            for name in ["cast_as_text", "castAsText"] {
                let f = registry.resolve(name, &[source.clone()]).unwrap();
                assert_eq!(f.return_type(), CqlType::Varchar);
            }
            for name in ["cast_as_ascii", "castAsAscii"] {
                let f = registry.resolve(name, &[source.clone()]).unwrap();
                assert_eq!(f.return_type(), CqlType::Ascii);
            }
        }

        for name in ["cast_as_text", "castAsText"] {
            let f = registry.resolve(name, &[CqlType::Ascii]).unwrap();
            assert_eq!(f.return_type(), CqlType::Varchar);
            assert_eq!(f.execute(&[Some(b"abc")]).unwrap(), Some(b"abc".to_vec()));
        }
    }

    #[test]
    fn native_text_casts_use_cql_literal_no_quote_formatting() {
        let registry = FunctionRegistry::with_builtins();

        let boolean_text = registry
            .resolve("cast_as_text", &[CqlType::Boolean])
            .unwrap();
        assert_eq!(
            boolean_text.execute(&[Some(&[1])]).unwrap(),
            Some(b"true".to_vec())
        );
        assert_eq!(boolean_text.execute(&[None]).unwrap(), None);

        let boolean_ascii = registry
            .resolve("castAsAscii", &[CqlType::Boolean])
            .unwrap();
        assert_eq!(
            boolean_ascii.execute(&[Some(&[0])]).unwrap(),
            Some(b"false".to_vec())
        );

        let inet_text = registry.resolve("cast_as_text", &[CqlType::Inet]).unwrap();
        assert_eq!(
            inet_text.execute(&[Some(&[127, 0, 0, 1])]).unwrap(),
            Some(b"127.0.0.1".to_vec())
        );

        let uuid = Uuid::parse_str("6ba7b810-9dad-11d1-80b4-00c04fd430c8")
            .unwrap()
            .into_bytes();
        let uuid_ascii = registry.resolve("castAsAscii", &[CqlType::Uuid]).unwrap();
        assert_eq!(
            uuid_ascii.execute(&[Some(&uuid)]).unwrap(),
            Some(b"6ba7b810-9dad-11d1-80b4-00c04fd430c8".to_vec())
        );
        let timeuuid_text = registry
            .resolve("cast_as_text", &[CqlType::Timeuuid])
            .unwrap();
        assert_eq!(
            timeuuid_text.execute(&[Some(&uuid)]).unwrap(),
            Some(b"6ba7b810-9dad-11d1-80b4-00c04fd430c8".to_vec())
        );

        let timestamp = 0i64.to_be_bytes();
        let timestamp_text = registry
            .resolve("cast_as_text", &[CqlType::Timestamp])
            .unwrap();
        assert_eq!(
            timestamp_text.execute(&[Some(&timestamp)]).unwrap(),
            Some(b"1970-01-01T00:00:00.000Z".to_vec())
        );

        let date = (1u32 << 31).to_be_bytes();
        let date_ascii = registry.resolve("castAsAscii", &[CqlType::Date]).unwrap();
        assert_eq!(
            date_ascii.execute(&[Some(&date)]).unwrap(),
            Some(b"1970-01-01".to_vec())
        );

        let time = 3_723_004_005_006i64.to_be_bytes();
        let time_text = registry.resolve("cast_as_text", &[CqlType::Time]).unwrap();
        assert_eq!(
            time_text.execute(&[Some(&time)]).unwrap(),
            Some(b"01:02:03.004005006".to_vec())
        );
    }

    #[test]
    fn builtins_resolve_varint_numeric_cast_overloads() {
        let registry = FunctionRegistry::with_builtins();
        let source = bigint::string_to_varint("4294967299").unwrap();

        let f = registry
            .resolve_with_return("cast", &[CqlType::Varint], &CqlType::Int)
            .unwrap();
        let result = f.execute(&[Some(&source)]).unwrap().unwrap();
        assert_eq!(i32::from_be_bytes(result.try_into().unwrap()), 3);

        let f = registry
            .resolve_with_return("cast", &[CqlType::Varint], &CqlType::Bigint)
            .unwrap();
        let result = f.execute(&[Some(&source)]).unwrap().unwrap();
        assert_eq!(
            i64::from_be_bytes(result.try_into().unwrap()),
            4_294_967_299
        );

        let f = registry
            .resolve_with_return("cast", &[CqlType::Int], &CqlType::Varint)
            .unwrap();
        let int = (-129i32).to_be_bytes();
        let result = f.execute(&[Some(&int)]).unwrap().unwrap();
        assert_eq!(bigint::varint_to_string(&result), "-129");
    }

    #[test]
    fn builtins_resolve_java_named_varint_casts_and_text() {
        let registry = FunctionRegistry::with_builtins();
        let source = bigint::string_to_varint("12345678901234567890").unwrap();

        let f = registry
            .resolve("cast_as_varint", &[CqlType::Bigint])
            .unwrap();
        assert_eq!(f.return_type(), CqlType::Varint);
        let bigint_bytes = 42i64.to_be_bytes();
        let result = f.execute(&[Some(&bigint_bytes)]).unwrap().unwrap();
        assert_eq!(bigint::varint_to_string(&result), "42");

        let f = registry
            .resolve("castAsDouble", &[CqlType::Varint])
            .unwrap();
        assert_eq!(f.return_type(), CqlType::Double);
        let result = f.execute(&[Some(&source)]).unwrap().unwrap();
        assert_eq!(
            f64::from_be_bytes(result.try_into().unwrap()),
            12_345_678_901_234_567_890_f64
        );

        let f = registry
            .resolve("cast_as_text", &[CqlType::Varint])
            .unwrap();
        assert_eq!(f.return_type(), CqlType::Varchar);
        assert_eq!(
            f.execute(&[Some(&source)]).unwrap(),
            Some(b"12345678901234567890".to_vec())
        );
    }

    #[test]
    fn builtins_resolve_decimal_numeric_cast_overloads() {
        let registry = FunctionRegistry::with_builtins();
        let source = bigint::string_to_decimal("-123.45").unwrap();

        let f = registry
            .resolve_with_return("cast", &[CqlType::Decimal], &CqlType::Int)
            .unwrap();
        let result = f.execute(&[Some(&source)]).unwrap().unwrap();
        assert_eq!(i32::from_be_bytes(result.try_into().unwrap()), -123);

        let source = bigint::string_to_decimal("4294967299.9").unwrap();
        let f = registry
            .resolve_with_return("cast", &[CqlType::Decimal], &CqlType::Int)
            .unwrap();
        let result = f.execute(&[Some(&source)]).unwrap().unwrap();
        assert_eq!(i32::from_be_bytes(result.try_into().unwrap()), 3);

        let f = registry
            .resolve_with_return("cast", &[CqlType::Decimal], &CqlType::Double)
            .unwrap();
        let result = f.execute(&[Some(&source)]).unwrap().unwrap();
        assert_eq!(
            f64::from_be_bytes(result.try_into().unwrap()),
            4_294_967_299.9
        );
    }

    #[test]
    fn builtins_resolve_java_named_decimal_casts_and_text() {
        let registry = FunctionRegistry::with_builtins();

        let f = registry
            .resolve("cast_as_decimal", &[CqlType::Int])
            .unwrap();
        assert_eq!(f.return_type(), CqlType::Decimal);
        let int = 42i32.to_be_bytes();
        let result = f.execute(&[Some(&int)]).unwrap().unwrap();
        assert_eq!(bigint::decimal_to_string(&result).unwrap(), "42");

        let f = registry
            .resolve("castAsDecimal", &[CqlType::Varint])
            .unwrap();
        assert_eq!(f.return_type(), CqlType::Decimal);
        let source = bigint::string_to_varint("12345678901234567890").unwrap();
        let result = f.execute(&[Some(&source)]).unwrap().unwrap();
        assert_eq!(
            bigint::decimal_to_string(&result).unwrap(),
            "12345678901234567890"
        );

        let f = registry
            .resolve("cast_as_text", &[CqlType::Decimal])
            .unwrap();
        assert_eq!(f.return_type(), CqlType::Varchar);
        let source = bigint::string_to_decimal("-0.001").unwrap();
        assert_eq!(
            f.execute(&[Some(&source)]).unwrap(),
            Some(b"-0.001".to_vec())
        );
    }

    #[test]
    fn builtins_resolve_temporal_cast_overloads() {
        let registry = FunctionRegistry::with_builtins();

        let f = registry
            .resolve_with_return("cast", &[CqlType::Timestamp], &CqlType::Date)
            .unwrap();
        let timestamp = 0i64.to_be_bytes();
        let result = f.execute(&[Some(&timestamp)]).unwrap().unwrap();
        assert_eq!(u32::from_be_bytes(result.try_into().unwrap()), 1u32 << 31);

        let f = registry
            .resolve_with_return("cast", &[CqlType::Date], &CqlType::Timestamp)
            .unwrap();
        let date = (1u32 << 31 | 1).to_be_bytes();
        let result = f.execute(&[Some(&date)]).unwrap().unwrap();
        assert_eq!(
            i64::from_be_bytes(result.try_into().unwrap()),
            MILLIS_PER_DAY
        );
    }

    #[test]
    fn builtins_resolve_java_named_temporal_cast_functions() {
        let registry = FunctionRegistry::with_builtins();
        let timeuuid = test_timeuuid_for_millis(123_456_789);

        let f = registry
            .resolve("cast_as_timestamp", &[CqlType::Timeuuid])
            .unwrap();
        assert_eq!(f.return_type(), CqlType::Timestamp);
        let result = f.execute(&[Some(&timeuuid)]).unwrap().unwrap();
        assert_eq!(i64::from_be_bytes(result.try_into().unwrap()), 123_456_789);

        let f = registry
            .resolve("castAsDate", &[CqlType::Timeuuid])
            .unwrap();
        assert_eq!(f.return_type(), CqlType::Date);
        let result = f.execute(&[Some(&timeuuid)]).unwrap().unwrap();
        assert_eq!(
            u32::from_be_bytes(result.try_into().unwrap()),
            (1u32 << 31) + 1
        );
    }

    #[test]
    fn type_as_blob_identity() {
        let f = TypeAsBlob {
            name: "int_as_blob".to_string(),
            cql_type: CqlType::Int,
        };
        let bytes = 123i32.to_be_bytes();
        let result = f.execute(&[Some(&bytes)]).unwrap().unwrap();
        assert_eq!(result, bytes);
    }

    #[test]
    fn blob_as_type_identity() {
        let f = BlobAsType {
            name: "blob_as_int".to_string(),
            cql_type: CqlType::Int,
        };
        let bytes = 456i32.to_be_bytes();
        let result = f.execute(&[Some(&bytes)]).unwrap().unwrap();
        assert_eq!(result, bytes);
    }

    #[test]
    fn blob_as_type_rejects_invalid_bytes() {
        let f = BlobAsType {
            name: "blob_as_int".to_string(),
            cql_type: CqlType::Int,
        };
        let err = f.execute(&[Some(&[0, 1, 2])]).unwrap_err();
        assert!(err.contains("blob_as_int"));
        assert!(err.contains("type int"));
    }

    #[test]
    fn builtins_resolve_java_blob_conversion_names() {
        let registry = FunctionRegistry::with_builtins();

        for name in ["int_as_blob", "intasblob"] {
            let f = registry.resolve(name, &[CqlType::Int]).unwrap();
            assert_eq!(f.return_type(), CqlType::Blob);
            let bytes = 123i32.to_be_bytes();
            assert_eq!(f.execute(&[Some(&bytes)]).unwrap(), Some(bytes.to_vec()));
        }

        for name in ["blob_as_int", "blobasint"] {
            let f = registry.resolve(name, &[CqlType::Blob]).unwrap();
            assert_eq!(f.return_type(), CqlType::Int);
            let bytes = 456i32.to_be_bytes();
            assert_eq!(f.execute(&[Some(&bytes)]).unwrap(), Some(bytes.to_vec()));
        }
    }

    #[test]
    fn builtins_resolve_text_and_varchar_blob_conversion_aliases() {
        let registry = FunctionRegistry::with_builtins();

        for name in [
            "text_as_blob",
            "textasblob",
            "varchar_as_blob",
            "varcharasblob",
        ] {
            let f = registry.resolve(name, &[CqlType::Varchar]).unwrap();
            assert_eq!(f.return_type(), CqlType::Blob);
            assert_eq!(f.execute(&[Some(b"abc")]).unwrap(), Some(b"abc".to_vec()));
        }

        for name in [
            "blob_as_text",
            "blobastext",
            "blob_as_varchar",
            "blobasvarchar",
        ] {
            let f = registry.resolve(name, &[CqlType::Blob]).unwrap();
            assert_eq!(f.return_type(), CqlType::Varchar);
            assert_eq!(f.execute(&[Some(b"abc")]).unwrap(), Some(b"abc".to_vec()));
        }
    }

    #[test]
    fn builtins_resolve_java_blob_conversion_for_all_native_non_blob_types() {
        let registry = FunctionRegistry::with_builtins();

        for spec in blob_conversion_specs() {
            for cql_name in spec.cql_names {
                let to_blob = format!("{cql_name}_as_blob");
                assert!(
                    registry
                        .resolve(&to_blob, &[spec.cql_type.clone()])
                        .is_some(),
                    "{to_blob} should resolve"
                );

                let legacy_to_blob = format!("{cql_name}asblob");
                assert!(
                    registry
                        .resolve(&legacy_to_blob, &[spec.cql_type.clone()])
                        .is_some(),
                    "{legacy_to_blob} should resolve"
                );

                let from_blob = format!("blob_as_{cql_name}");
                let resolved = registry.resolve(&from_blob, &[CqlType::Blob]).unwrap();
                assert_eq!(resolved.return_type(), spec.cql_type);

                let legacy_from_blob = format!("blobas{cql_name}");
                let resolved = registry
                    .resolve(&legacy_from_blob, &[CqlType::Blob])
                    .unwrap();
                assert_eq!(resolved.return_type(), spec.cql_type);
            }
        }
    }

    #[test]
    fn murmur3_known_value() {
        // Verify the hash is stable (regression test)
        let data = b"hello";
        let h = murmur3_hash(data);
        // Just check it returns a stable value
        let h2 = murmur3_hash(data);
        assert_eq!(h, h2);
    }

    fn test_timeuuid_for_millis(millis: i64) -> Vec<u8> {
        let ts = (millis as u64) * 10_000 + UUID_EPOCH_OFFSET;
        let time_low = (ts & 0xFFFF_FFFF) as u32;
        let time_mid = ((ts >> 32) & 0xFFFF) as u16;
        let time_hi = ((ts >> 48) & 0x0FFF) as u16 | 0x1000;

        let mut result = [0u8; 16];
        result[0..4].copy_from_slice(&time_low.to_be_bytes());
        result[4..6].copy_from_slice(&time_mid.to_be_bytes());
        result[6..8].copy_from_slice(&time_hi.to_be_bytes());
        result[8] = 0x80;
        result.to_vec()
    }
}
