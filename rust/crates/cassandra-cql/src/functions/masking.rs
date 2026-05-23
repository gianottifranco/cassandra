// Licensed under Apache License, Version 2.0.

//! CQL data masking functions.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.cql3.functions.MaskingFcts`

use super::registry::{CqlFunction, FunctionRegistry};
use cassandra_types::{CqlType, CqlValue, VectorValue};
use md2::Md2;
use md5::Md5;
use sha1::Sha1;
use sha2::{Digest, Sha224, Sha256, Sha384, Sha512, Sha512_224, Sha512_256};
use sha3::{Sha3_224, Sha3_256, Sha3_384, Sha3_512};
use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;

/// Register all masking functions.
pub fn register_all(registry: &FunctionRegistry) {
    for cql_type in native_mask_types() {
        registry.register(Arc::new(MaskDefault {
            cql_type: cql_type.clone(),
        }));
        registry.register(Arc::new(MaskNull {
            cql_type: cql_type.clone(),
        }));
        registry.register(Arc::new(MaskReplace {
            cql_type: cql_type.clone(),
        }));
        registry.register(Arc::new(MaskHash {
            cql_type: cql_type.clone(),
            has_algorithm: false,
        }));
        registry.register(Arc::new(MaskHash {
            cql_type,
            has_algorithm: true,
        }));
    }

    for cql_type in [CqlType::Ascii, CqlType::Varchar] {
        registry.register(Arc::new(PartialMask {
            name: "mask_inner",
            cql_type: cql_type.clone(),
            kind: PartialMaskKind::Inner,
            has_padding: false,
        }));
        registry.register(Arc::new(PartialMask {
            name: "mask_inner",
            cql_type: cql_type.clone(),
            kind: PartialMaskKind::Inner,
            has_padding: true,
        }));
        registry.register(Arc::new(PartialMask {
            name: "mask_outer",
            cql_type: cql_type.clone(),
            kind: PartialMaskKind::Outer,
            has_padding: false,
        }));
        registry.register(Arc::new(PartialMask {
            name: "mask_outer",
            cql_type,
            kind: PartialMaskKind::Outer,
            has_padding: true,
        }));
    }
}

pub(crate) fn resolve_dynamic(name: &str, arg_types: &[CqlType]) -> Option<Arc<dyn CqlFunction>> {
    match name.to_ascii_lowercase().as_str() {
        "mask_default" if arg_types.len() == 1 => {
            if masked_default_value(&arg_types[0]).is_ok() {
                Some(Arc::new(MaskDefault {
                    cql_type: arg_types[0].clone(),
                }))
            } else {
                None
            }
        }
        "mask_null" if arg_types.len() == 1 => Some(Arc::new(MaskNull {
            cql_type: arg_types[0].clone(),
        })),
        "mask_replace" if arg_types.len() == 2 && arg_types[0] == arg_types[1] => {
            Some(Arc::new(MaskReplace {
                cql_type: arg_types[0].clone(),
            }))
        }
        "mask_hash"
            if arg_types.len() == 1
                || (arg_types.len() == 2
                    && matches!(arg_types[1], CqlType::Varchar | CqlType::Ascii)) =>
        {
            Some(Arc::new(MaskHash {
                cql_type: arg_types[0].clone(),
                has_algorithm: arg_types.len() == 2,
            }))
        }
        _ => None,
    }
}

// ── mask_default ──────────────────────────────────────────────────────

/// Returns a type-specific default value: empty string for text, 0 for int, etc.
struct MaskDefault {
    cql_type: CqlType,
}

impl CqlFunction for MaskDefault {
    fn name(&self) -> &str {
        "mask_default"
    }
    fn arg_types(&self) -> Vec<CqlType> {
        vec![self.cql_type.clone()]
    }
    fn return_type(&self) -> CqlType {
        self.cql_type.clone()
    }
    fn execute(&self, _args: &[Option<&[u8]>]) -> Result<Option<Vec<u8>>, String> {
        Ok(Some(masked_default_value(&self.cql_type)?))
    }
}

// ── mask_null ─────────────────────────────────────────────────────────

/// Always returns NULL regardless of input.
struct MaskNull {
    cql_type: CqlType,
}

impl CqlFunction for MaskNull {
    fn name(&self) -> &str {
        "mask_null"
    }
    fn arg_types(&self) -> Vec<CqlType> {
        vec![self.cql_type.clone()]
    }
    fn return_type(&self) -> CqlType {
        self.cql_type.clone()
    }
    fn execute(&self, _args: &[Option<&[u8]>]) -> Result<Option<Vec<u8>>, String> {
        Ok(None)
    }
}

// ── mask_inner / mask_outer ───────────────────────────────────────────

#[derive(Clone, Copy)]
enum PartialMaskKind {
    Inner,
    Outer,
}

struct PartialMask {
    name: &'static str,
    cql_type: CqlType,
    kind: PartialMaskKind,
    has_padding: bool,
}

impl CqlFunction for PartialMask {
    fn name(&self) -> &str {
        self.name
    }
    fn arg_types(&self) -> Vec<CqlType> {
        let mut args = vec![self.cql_type.clone(), CqlType::Int, CqlType::Int];
        if self.has_padding {
            args.push(CqlType::Varchar);
        }
        args
    }
    fn return_type(&self) -> CqlType {
        self.cql_type.clone()
    }
    fn execute(&self, args: &[Option<&[u8]>]) -> Result<Option<Vec<u8>>, String> {
        let val = match args.first().and_then(|a| *a) {
            Some(b) => b,
            None => return Ok(None),
        };
        let s = std::str::from_utf8(val).map_err(|_| "invalid UTF-8")?;
        if s.is_empty() {
            return Ok(Some(val.to_vec()));
        }
        let begin = args
            .get(1)
            .and_then(|a| *a)
            .map(read_i32)
            .transpose()?
            .unwrap_or(0) as isize;
        let end = args
            .get(2)
            .and_then(|a| *a)
            .map(read_i32)
            .transpose()?
            .unwrap_or(0) as isize;
        let padding = match args.get(3).and_then(|a| *a) {
            Some(bytes) => single_padding_char(bytes)?,
            None => '*',
        };

        let chars: Vec<char> = s.chars().collect();
        let mut result = String::with_capacity(s.len());
        let end_index = chars.len() as isize - 1 - end;
        for (idx, ch) in chars.into_iter().enumerate() {
            let idx = idx as isize;
            let in_middle = idx >= begin && idx <= end_index;
            let should_mask = match self.kind {
                PartialMaskKind::Inner => in_middle,
                PartialMaskKind::Outer => !in_middle,
            };
            result.push(if should_mask { padding } else { ch });
        }
        Ok(Some(result.into_bytes()))
    }
}

// ── mask_hash ─────────────────────────────────────────────────────────

/// Replaces the value with a digest. Java defaults to SHA-256 and returns blob.
struct MaskHash {
    cql_type: CqlType,
    has_algorithm: bool,
}

impl CqlFunction for MaskHash {
    fn name(&self) -> &str {
        "mask_hash"
    }
    fn arg_types(&self) -> Vec<CqlType> {
        let mut args = vec![self.cql_type.clone()];
        if self.has_algorithm {
            args.push(CqlType::Varchar);
        }
        args
    }
    fn return_type(&self) -> CqlType {
        CqlType::Blob
    }
    fn execute(&self, args: &[Option<&[u8]>]) -> Result<Option<Vec<u8>>, String> {
        let Some(value) = args.first().and_then(|arg| *arg) else {
            return Ok(None);
        };
        let algorithm = args
            .get(1)
            .and_then(|arg| *arg)
            .map(|bytes| {
                std::str::from_utf8(bytes)
                    .map(|text| text.to_string())
                    .map_err(|_| "invalid UTF-8 hash algorithm".to_string())
            })
            .transpose()?
            .unwrap_or_else(|| "SHA-256".to_string());
        hash_value(&algorithm, value).map(Some)
    }
}

// ── mask_replace ──────────────────────────────────────────────────────

/// Replaces the entire value with a given replacement string.
/// `mask_replace(val, replacement)`
struct MaskReplace {
    cql_type: CqlType,
}

impl CqlFunction for MaskReplace {
    fn name(&self) -> &str {
        "mask_replace"
    }
    fn arg_types(&self) -> Vec<CqlType> {
        vec![self.cql_type.clone(), self.cql_type.clone()]
    }
    fn return_type(&self) -> CqlType {
        self.cql_type.clone()
    }
    fn execute(&self, args: &[Option<&[u8]>]) -> Result<Option<Vec<u8>>, String> {
        match args.get(1).and_then(|a| *a) {
            Some(replacement) => Ok(Some(replacement.to_vec())),
            None => Ok(None),
        }
    }
}

fn native_mask_types() -> Vec<CqlType> {
    vec![
        CqlType::Ascii,
        CqlType::Varchar,
        CqlType::Boolean,
        CqlType::Tinyint,
        CqlType::Smallint,
        CqlType::Int,
        CqlType::Bigint,
        CqlType::Counter,
        CqlType::Float,
        CqlType::Double,
        CqlType::Varint,
        CqlType::Decimal,
        CqlType::Blob,
        CqlType::Timestamp,
        CqlType::Date,
        CqlType::Time,
        CqlType::Uuid,
        CqlType::Timeuuid,
        CqlType::Inet,
        CqlType::Duration,
    ]
}

fn masked_default_value(cql_type: &CqlType) -> Result<Vec<u8>, String> {
    let value = match cql_type {
        CqlType::Ascii => CqlValue::Ascii("****".to_string()),
        CqlType::Varchar => CqlValue::Varchar("****".to_string()),
        CqlType::Boolean => CqlValue::Boolean(false),
        CqlType::Tinyint => CqlValue::Tinyint(0),
        CqlType::Smallint => CqlValue::Smallint(0),
        CqlType::Int => CqlValue::Int(0),
        CqlType::Bigint => CqlValue::Bigint(0),
        CqlType::Counter => CqlValue::Counter(0),
        CqlType::Float => CqlValue::Float(0.0),
        CqlType::Double => CqlValue::Double(0.0),
        CqlType::Varint => CqlValue::Varint(vec![0]),
        CqlType::Decimal => CqlValue::Decimal {
            scale: 0,
            unscaled: vec![0],
        },
        CqlType::Blob => CqlValue::Blob(Vec::new()),
        CqlType::Timestamp => CqlValue::Timestamp(0),
        CqlType::Date => CqlValue::Date(1u32 << 31),
        CqlType::Time => CqlValue::Time(0),
        CqlType::Uuid => CqlValue::Uuid([0; 16]),
        CqlType::Timeuuid => CqlValue::Timeuuid(min_timeuuid_at_unix_epoch()),
        CqlType::Inet => CqlValue::Inet(IpAddr::V4(Ipv4Addr::UNSPECIFIED)),
        CqlType::Duration => CqlValue::Duration {
            months: 0,
            days: 0,
            nanoseconds: 0,
        },
        CqlType::List(_, _) => return Ok(0i32.to_be_bytes().to_vec()),
        CqlType::Set(_, _) => return Ok(0i32.to_be_bytes().to_vec()),
        CqlType::Map(_, _, _) => return Ok(0i32.to_be_bytes().to_vec()),
        CqlType::Tuple(field_types) => return serialize_default_fields(field_types),
        CqlType::Udt { field_types, .. } => return serialize_default_fields(field_types),
        CqlType::Vector(inner, dimensions) if matches!(inner.as_ref(), CqlType::Float) => {
            CqlValue::Vector(VectorValue::new(vec![0.0; *dimensions as usize]))
        }
        CqlType::Reversed(inner) => return masked_default_value(inner),
        other => {
            return Err(format!(
                "mask_default is not implemented for {}",
                other.cql_name()
            ));
        }
    };
    Ok(value.serialize_value())
}

fn serialize_default_fields(field_types: &[CqlType]) -> Result<Vec<u8>, String> {
    let mut result = Vec::new();
    for field_type in field_types {
        let value = masked_default_value(field_type)?;
        result.extend_from_slice(&(value.len() as i32).to_be_bytes());
        result.extend_from_slice(&value);
    }
    Ok(result)
}

fn min_timeuuid_at_unix_epoch() -> [u8; 16] {
    const UUID_EPOCH_OFFSET: u64 = 0x01B2_1DD2_1381_4000;
    let time_low = (UUID_EPOCH_OFFSET & 0xFFFF_FFFF) as u32;
    let time_mid = ((UUID_EPOCH_OFFSET >> 32) & 0xFFFF) as u16;
    let time_hi = ((UUID_EPOCH_OFFSET >> 48) & 0x0FFF) as u16 | 0x1000;

    let mut result = [0u8; 16];
    result[0..4].copy_from_slice(&time_low.to_be_bytes());
    result[4..6].copy_from_slice(&time_mid.to_be_bytes());
    result[6..8].copy_from_slice(&time_hi.to_be_bytes());
    result[8] = 0x80;
    result
}

fn read_i32(bytes: &[u8]) -> Result<i32, String> {
    let bytes: [u8; 4] = bytes
        .try_into()
        .map_err(|_| format!("invalid int bytes: got {}", bytes.len()))?;
    Ok(i32::from_be_bytes(bytes))
}

fn single_padding_char(bytes: &[u8]) -> Result<char, String> {
    let value = std::str::from_utf8(bytes).map_err(|_| "invalid UTF-8 padding")?;
    let mut chars = value.chars();
    let Some(ch) = chars.next() else {
        return Err("padding argument should be single-character".to_string());
    };
    if chars.next().is_some() {
        return Err(format!(
            "padding argument should be single-character, got {} characters",
            value.chars().count()
        ));
    }
    Ok(ch)
}

fn hash_value(algorithm: &str, value: &[u8]) -> Result<Vec<u8>, String> {
    let canonical = algorithm
        .chars()
        .filter(|ch| *ch != '-' && *ch != '_' && *ch != '/')
        .flat_map(char::to_uppercase)
        .collect::<String>();
    match canonical.as_str() {
        "MD2" => Ok(Md2::digest(value).to_vec()),
        "MD5" => Ok(Md5::digest(value).to_vec()),
        "SHA" | "SHA1" => Ok(Sha1::digest(value).to_vec()),
        "SHA224" => Ok(Sha224::digest(value).to_vec()),
        "SHA256" => Ok(Sha256::digest(value).to_vec()),
        "SHA384" => Ok(Sha384::digest(value).to_vec()),
        "SHA512" => Ok(Sha512::digest(value).to_vec()),
        "SHA512224" => Ok(Sha512_224::digest(value).to_vec()),
        "SHA512256" => Ok(Sha512_256::digest(value).to_vec()),
        "SHA3224" => Ok(Sha3_224::digest(value).to_vec()),
        "SHA3256" => Ok(Sha3_256::digest(value).to_vec()),
        "SHA3384" => Ok(Sha3_384::digest(value).to_vec()),
        "SHA3512" => Ok(Sha3_512::digest(value).to_vec()),
        _ => Err(format!("Hash algorithm not found: {algorithm}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mask_default_text() {
        let f = MaskDefault {
            cql_type: CqlType::Varchar,
        };
        let val = Some(b"hello".as_slice());
        let result = f.execute(&[val]).unwrap();
        assert_eq!(result, Some(b"****".to_vec()));
    }

    #[test]
    fn mask_default_int() {
        let f = MaskDefault {
            cql_type: CqlType::Int,
        };
        let val = 42i32.to_be_bytes();
        let result = f.execute(&[Some(&val)]).unwrap();
        assert_eq!(result, Some(0i32.to_be_bytes().to_vec()));
    }

    #[test]
    fn mask_null_returns_none() {
        let f = MaskNull {
            cql_type: CqlType::Varchar,
        };
        let val = Some(b"hello".as_slice());
        let result = f.execute(&[val]).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn mask_inner_java_signature_masks_middle_range() {
        let f = PartialMask {
            name: "mask_inner",
            cql_type: CqlType::Varchar,
            kind: PartialMaskKind::Inner,
            has_padding: false,
        };
        let val = b"hello";
        let begin = 1i32.to_be_bytes();
        let end = 1i32.to_be_bytes();
        let result = f
            .execute(&[Some(val.as_slice()), Some(&begin), Some(&end)])
            .unwrap();
        assert_eq!(result, Some(b"h***o".to_vec()));
    }

    #[test]
    fn mask_inner_accepts_single_character_padding() {
        let f = PartialMask {
            name: "mask_inner",
            cql_type: CqlType::Varchar,
            kind: PartialMaskKind::Inner,
            has_padding: true,
        };
        let val = b"ab";
        let begin = 0i32.to_be_bytes();
        let end = 0i32.to_be_bytes();
        let pad = b"#";
        let result = f
            .execute(&[Some(val.as_slice()), Some(&begin), Some(&end), Some(pad)])
            .unwrap();
        assert_eq!(result, Some(b"##".to_vec()));
    }

    #[test]
    fn mask_outer_java_signature_masks_outer_range() {
        let f = PartialMask {
            name: "mask_outer",
            cql_type: CqlType::Varchar,
            kind: PartialMaskKind::Outer,
            has_padding: false,
        };
        let val = b"hello";
        let begin = 1i32.to_be_bytes();
        let end = 1i32.to_be_bytes();
        let result = f
            .execute(&[Some(val.as_slice()), Some(&begin), Some(&end)])
            .unwrap();
        assert_eq!(result, Some(b"*ell*".to_vec()));
    }

    #[test]
    fn partial_masks_use_java_unclamped_boundaries() {
        let inner = PartialMask {
            name: "mask_inner",
            cql_type: CqlType::Varchar,
            kind: PartialMaskKind::Inner,
            has_padding: false,
        };
        let outer = PartialMask {
            name: "mask_outer",
            cql_type: CqlType::Varchar,
            kind: PartialMaskKind::Outer,
            has_padding: false,
        };
        let value = b"hello";

        let begin = 0i32.to_be_bytes();
        let end_past_len = 10i32.to_be_bytes();
        assert_eq!(
            inner
                .execute(&[Some(value.as_slice()), Some(&begin), Some(&end_past_len)])
                .unwrap(),
            Some(b"hello".to_vec())
        );
        assert_eq!(
            outer
                .execute(&[Some(value.as_slice()), Some(&begin), Some(&end_past_len)])
                .unwrap(),
            Some(b"*****".to_vec())
        );

        let negative_begin = (-1i32).to_be_bytes();
        let end = 1i32.to_be_bytes();
        assert_eq!(
            inner
                .execute(&[Some(value.as_slice()), Some(&negative_begin), Some(&end)])
                .unwrap(),
            Some(b"****o".to_vec())
        );
        assert_eq!(
            outer
                .execute(&[Some(value.as_slice()), Some(&negative_begin), Some(&end)])
                .unwrap(),
            Some(b"hell*".to_vec())
        );
    }

    #[test]
    fn mask_replace_basic() {
        let f = MaskReplace {
            cql_type: CqlType::Varchar,
        };
        let val = b"secret";
        let replacement = b"REDACTED";
        let result = f
            .execute(&[Some(val.as_slice()), Some(replacement.as_slice())])
            .unwrap();
        assert_eq!(result, Some(b"REDACTED".to_vec()));
    }

    #[test]
    fn mask_replace_returns_replacement_even_for_null_input() {
        let f = MaskReplace {
            cql_type: CqlType::Varchar,
        };
        let result = f.execute(&[None, Some(b"REDACTED".as_slice())]).unwrap();
        assert_eq!(result, Some(b"REDACTED".to_vec()));
    }

    #[test]
    fn masking_registry_resolves_native_return_types() {
        let registry = FunctionRegistry::new();
        register_all(&registry);

        let default_int = registry.resolve("mask_default", &[CqlType::Int]).unwrap();
        assert_eq!(default_int.return_type(), CqlType::Int);
        assert_eq!(
            default_int.execute(&[Some(&42i32.to_be_bytes())]).unwrap(),
            Some(0i32.to_be_bytes().to_vec())
        );

        let null_uuid = registry.resolve("mask_null", &[CqlType::Uuid]).unwrap();
        assert_eq!(null_uuid.return_type(), CqlType::Uuid);
        assert_eq!(null_uuid.execute(&[Some(&[1u8; 16])]).unwrap(), None);

        let replace_bigint = registry
            .resolve("mask_replace", &[CqlType::Bigint, CqlType::Bigint])
            .unwrap();
        assert_eq!(replace_bigint.return_type(), CqlType::Bigint);
    }

    #[test]
    fn mask_hash_defaults_to_sha256_blob() {
        let registry = FunctionRegistry::new();
        register_all(&registry);
        let hash = registry.resolve("mask_hash", &[CqlType::Varchar]).unwrap();
        assert_eq!(hash.return_type(), CqlType::Blob);

        let result = hash.execute(&[Some(b"secret")]).unwrap().unwrap();
        assert_eq!(
            hex::encode(result),
            "2bb80d537b1da3e38bd30361aa855686bde0eacd7162fef6a25fe97bf527a25b"
        );
    }

    #[test]
    fn mask_hash_accepts_supported_algorithm_argument() {
        let registry = FunctionRegistry::new();
        register_all(&registry);
        let hash = registry
            .resolve("mask_hash", &[CqlType::Varchar, CqlType::Varchar])
            .unwrap();

        let result = hash
            .execute(&[Some(b"secret"), Some(b"SHA-512")])
            .unwrap()
            .unwrap();
        assert_eq!(result.len(), 64);
    }

    #[test]
    fn mask_hash_accepts_jdk_standard_digest_catalog() {
        let registry = FunctionRegistry::new();
        register_all(&registry);
        let hash = registry
            .resolve("mask_hash", &[CqlType::Varchar, CqlType::Varchar])
            .unwrap();

        for (algorithm, expected_hex) in [
            ("MD5", "5ebe2294ecd0e0f08eab7690d2a6ee69"),
            ("SHA-1", "e5e9fa1ba31ecd1ae84f75caaa474f3a663f05f4"),
            (
                "SHA3-256",
                "f5a5207a8729b1f709cb710311751eb2fc8acad5a1fb8ac991b736e69b6529a3",
            ),
            (
                "SHA-512/256",
                "4407cbbbec93609705d83608b3790da3837b3a619dc73346d10b6133dceb5eb7",
            ),
        ] {
            let result = hash
                .execute(&[Some(b"secret"), Some(algorithm.as_bytes())])
                .unwrap()
                .unwrap();
            assert_eq!(hex::encode(result), expected_hex, "{algorithm}");
        }
    }

    #[test]
    fn mask_hash_treats_null_algorithm_as_default() {
        let registry = FunctionRegistry::new();
        register_all(&registry);
        let hash = registry
            .resolve("mask_hash", &[CqlType::Varchar, CqlType::Varchar])
            .unwrap();

        let explicit_null = hash.execute(&[Some(b"secret"), None]).unwrap();
        let default = registry
            .resolve("mask_hash", &[CqlType::Varchar])
            .unwrap()
            .execute(&[Some(b"secret")])
            .unwrap();
        assert_eq!(explicit_null, default);
    }

    #[test]
    fn mask_hash_rejects_unknown_algorithm() {
        let registry = FunctionRegistry::new();
        register_all(&registry);
        let hash = registry
            .resolve("mask_hash", &[CqlType::Varchar, CqlType::Varchar])
            .unwrap();

        let err = hash
            .execute(&[Some(b"secret"), Some(b"unknown-algorithm")])
            .unwrap_err();
        assert!(err.contains("Hash algorithm not found"));
    }

    #[test]
    fn mask_default_resolves_complex_collection_defaults() {
        let registry = FunctionRegistry::new();
        register_all(&registry);
        let list_type = CqlType::List(Box::new(CqlType::Int), false);
        let mask_default = registry
            .resolve("mask_default", std::slice::from_ref(&list_type))
            .unwrap();

        assert_eq!(mask_default.return_type(), list_type);
        let result = mask_default.execute(&[Some(&[])]).unwrap().unwrap();
        assert_eq!(result, 0i32.to_be_bytes().to_vec());
        assert_eq!(
            CqlValue::deserialize_value(&list_type, &result).unwrap(),
            CqlValue::List(Vec::new())
        );
    }

    #[test]
    fn mask_default_resolves_tuple_and_udt_field_defaults() {
        let registry = FunctionRegistry::new();
        register_all(&registry);
        let tuple_type = CqlType::Tuple(vec![CqlType::Int, CqlType::Varchar]);
        let mask_default = registry
            .resolve("mask_default", std::slice::from_ref(&tuple_type))
            .unwrap();
        let result = mask_default.execute(&[Some(&[])]).unwrap().unwrap();

        let mut expected = Vec::new();
        expected.extend_from_slice(&4i32.to_be_bytes());
        expected.extend_from_slice(&0i32.to_be_bytes());
        expected.extend_from_slice(&4i32.to_be_bytes());
        expected.extend_from_slice(b"****");
        assert_eq!(result, expected);
        assert_eq!(
            CqlValue::deserialize_value(&tuple_type, &result).unwrap(),
            CqlValue::Tuple(vec![
                Some(CqlValue::Int(0)),
                Some(CqlValue::Varchar("****".into()))
            ])
        );

        let udt_type = CqlType::Udt {
            keyspace: "ks".to_string(),
            name: "profile".to_string(),
            field_names: vec!["age".to_string(), "name".to_string()],
            field_types: vec![CqlType::Int, CqlType::Varchar],
            is_multi_cell: false,
        };
        let udt_default = registry
            .resolve("mask_default", std::slice::from_ref(&udt_type))
            .unwrap();
        assert_eq!(
            udt_default.execute(&[Some(&[])]).unwrap().unwrap(),
            expected
        );
    }

    #[test]
    fn mask_default_resolves_vector_defaults() {
        let registry = FunctionRegistry::new();
        register_all(&registry);
        let vector_type = CqlType::Vector(Box::new(CqlType::Float), 3);
        let mask_default = registry
            .resolve("mask_default", std::slice::from_ref(&vector_type))
            .unwrap();
        let result = mask_default.execute(&[Some(&[])]).unwrap().unwrap();

        assert_eq!(result, vec![0; 12]);
        assert_eq!(
            CqlValue::deserialize_value(&vector_type, &result).unwrap(),
            CqlValue::Vector(VectorValue::new(vec![0.0, 0.0, 0.0]))
        );
    }

    #[test]
    fn register_all_adds_functions() {
        let registry = FunctionRegistry::new();
        register_all(&registry);
        assert!(registry.resolve_by_name("mask_default").is_some());
        assert!(registry.resolve_by_name("mask_null").is_some());
        assert!(registry.resolve_by_name("mask_inner").is_some());
        assert!(registry.resolve_by_name("mask_outer").is_some());
        assert!(registry.resolve_by_name("mask_replace").is_some());
        assert!(registry.resolve_by_name("mask_hash").is_some());
    }
}
