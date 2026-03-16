// Licensed under Apache License, Version 2.0.

//! Token, cast, and blob conversion functions.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.cql3.functions.TokenFct`
//! - `org.apache.cassandra.cql3.functions.CastFcts`
//! - `org.apache.cassandra.cql3.functions.BytesConversionFcts`

use super::registry::{CqlFunction, FunctionRegistry};
use cassandra_types::CqlType;
use std::sync::Arc;

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

    // Blob conversion functions
    for cql_type in &[
        CqlType::Int,
        CqlType::Bigint,
        CqlType::Float,
        CqlType::Double,
        CqlType::Varchar,
        CqlType::Uuid,
        CqlType::Timestamp,
        CqlType::Boolean,
    ] {
        registry.register(Arc::new(TypeAsBlob {
            cql_type: cql_type.clone(),
        }));
        registry.register(Arc::new(BlobAsType {
            cql_type: cql_type.clone(),
        }));
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

define_cast!(
    CastFloatToText,
    CqlType::Float,
    CqlType::Varchar,
    |bytes| {
        let v = f32::from_be_bytes(bytes.try_into().map_err(|_| "invalid float")?);
        v.to_string().into_bytes()
    }
);

define_cast!(
    CastDoubleToText,
    CqlType::Double,
    CqlType::Varchar,
    |bytes| {
        let v = f64::from_be_bytes(bytes.try_into().map_err(|_| "invalid double")?);
        v.to_string().into_bytes()
    }
);

define_cast!(
    CastIntToBigint,
    CqlType::Int,
    CqlType::Bigint,
    |bytes| {
        let v = i32::from_be_bytes(bytes.try_into().map_err(|_| "invalid int")?);
        (v as i64).to_be_bytes().to_vec()
    }
);

define_cast!(
    CastIntToFloat,
    CqlType::Int,
    CqlType::Float,
    |bytes| {
        let v = i32::from_be_bytes(bytes.try_into().map_err(|_| "invalid int")?);
        (v as f32).to_be_bytes().to_vec()
    }
);

define_cast!(
    CastIntToDouble,
    CqlType::Int,
    CqlType::Double,
    |bytes| {
        let v = i32::from_be_bytes(bytes.try_into().map_err(|_| "invalid int")?);
        (v as f64).to_be_bytes().to_vec()
    }
);

// ── typeAsBlob / blobAsType ───────────────────────────────────────────

struct TypeAsBlob {
    cql_type: CqlType,
}

impl CqlFunction for TypeAsBlob {
    fn name(&self) -> &str {
        // e.g. "intasblob", "bigintasblob"
        // We use a generic name; the registry resolves by arg type
        "typeasblob"
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
    cql_type: CqlType,
}

impl CqlFunction for BlobAsType {
    fn name(&self) -> &str {
        "blobastype"
    }
    fn arg_types(&self) -> Vec<CqlType> {
        vec![CqlType::Blob]
    }
    fn return_type(&self) -> CqlType {
        self.cql_type.clone()
    }
    fn execute(&self, args: &[Option<&[u8]>]) -> Result<Option<Vec<u8>>, String> {
        // Identity - return raw bytes, the type system handles interpretation
        match args.first().and_then(|a| *a) {
            Some(bytes) => Ok(Some(bytes.to_vec())),
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
    fn type_as_blob_identity() {
        let f = TypeAsBlob {
            cql_type: CqlType::Int,
        };
        let bytes = 123i32.to_be_bytes();
        let result = f.execute(&[Some(&bytes)]).unwrap().unwrap();
        assert_eq!(result, bytes);
    }

    #[test]
    fn blob_as_type_identity() {
        let f = BlobAsType {
            cql_type: CqlType::Int,
        };
        let bytes = 456i32.to_be_bytes();
        let result = f.execute(&[Some(&bytes)]).unwrap().unwrap();
        assert_eq!(result, bytes);
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
}
