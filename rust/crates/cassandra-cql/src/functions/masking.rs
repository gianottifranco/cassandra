// Licensed under Apache License, Version 2.0.

//! CQL data masking functions.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.cql3.functions.MaskingFcts`

use super::registry::{CqlFunction, FunctionRegistry};
use cassandra_types::CqlType;
use std::sync::Arc;

/// Register all masking functions.
pub fn register_all(registry: &FunctionRegistry) {
    registry.register(Arc::new(MaskDefault));
    registry.register(Arc::new(MaskNull));
    registry.register(Arc::new(MaskInner));
    registry.register(Arc::new(MaskOuter));
    registry.register(Arc::new(MaskReplace));
}

// ── mask_default ──────────────────────────────────────────────────────

/// Returns a type-specific default value: empty string for text, 0 for int, etc.
struct MaskDefault;

impl CqlFunction for MaskDefault {
    fn name(&self) -> &str {
        "mask_default"
    }
    fn arg_types(&self) -> Vec<CqlType> {
        // Accepts any single argument (polymorphic).
        vec![]
    }
    fn return_type(&self) -> CqlType {
        CqlType::Varchar
    }
    fn execute(&self, args: &[Option<&[u8]>]) -> Result<Option<Vec<u8>>, String> {
        match args.first().and_then(|a| *a) {
            Some(bytes) => {
                // If input looks like a 4-byte int, return 0 as i32.
                // If 8 bytes, return 0 as i64. Otherwise return empty string.
                match bytes.len() {
                    4 => Ok(Some(0i32.to_be_bytes().to_vec())),
                    8 => Ok(Some(0i64.to_be_bytes().to_vec())),
                    _ => Ok(Some(Vec::new())),
                }
            }
            None => Ok(Some(Vec::new())),
        }
    }
}

// ── mask_null ─────────────────────────────────────────────────────────

/// Always returns NULL regardless of input.
struct MaskNull;

impl CqlFunction for MaskNull {
    fn name(&self) -> &str {
        "mask_null"
    }
    fn arg_types(&self) -> Vec<CqlType> {
        vec![]
    }
    fn return_type(&self) -> CqlType {
        CqlType::Varchar
    }
    fn execute(&self, _args: &[Option<&[u8]>]) -> Result<Option<Vec<u8>>, String> {
        Ok(None)
    }
}

// ── mask_inner ────────────────────────────────────────────────────────

/// Masks inner characters, keeping the first and last character visible.
/// `mask_inner(val, padding_char)` where padding_char is a single-byte char.
struct MaskInner;

impl CqlFunction for MaskInner {
    fn name(&self) -> &str {
        "mask_inner"
    }
    fn arg_types(&self) -> Vec<CqlType> {
        vec![CqlType::Varchar, CqlType::Varchar]
    }
    fn return_type(&self) -> CqlType {
        CqlType::Varchar
    }
    fn execute(&self, args: &[Option<&[u8]>]) -> Result<Option<Vec<u8>>, String> {
        let val = match args.first().and_then(|a| *a) {
            Some(b) => b,
            None => return Ok(None),
        };
        let pad_char = match args.get(1).and_then(|a| *a) {
            Some(b) if !b.is_empty() => b[0],
            _ => b'*',
        };

        let s = std::str::from_utf8(val).map_err(|_| "invalid UTF-8")?;
        let chars: Vec<char> = s.chars().collect();
        if chars.len() <= 2 {
            return Ok(Some(val.to_vec()));
        }

        let mut result = String::with_capacity(s.len());
        result.push(chars[0]);
        for _ in 1..chars.len() - 1 {
            result.push(pad_char as char);
        }
        result.push(chars[chars.len() - 1]);
        Ok(Some(result.into_bytes()))
    }
}

// ── mask_outer ────────────────────────────────────────────────────────

/// Masks outer characters, keeping the inner characters visible.
/// `mask_outer(val, padding_char)` where padding_char is a single-byte char.
struct MaskOuter;

impl CqlFunction for MaskOuter {
    fn name(&self) -> &str {
        "mask_outer"
    }
    fn arg_types(&self) -> Vec<CqlType> {
        vec![CqlType::Varchar, CqlType::Varchar]
    }
    fn return_type(&self) -> CqlType {
        CqlType::Varchar
    }
    fn execute(&self, args: &[Option<&[u8]>]) -> Result<Option<Vec<u8>>, String> {
        let val = match args.first().and_then(|a| *a) {
            Some(b) => b,
            None => return Ok(None),
        };
        let pad_char = match args.get(1).and_then(|a| *a) {
            Some(b) if !b.is_empty() => b[0],
            _ => b'*',
        };

        let s = std::str::from_utf8(val).map_err(|_| "invalid UTF-8")?;
        let chars: Vec<char> = s.chars().collect();
        if chars.len() <= 2 {
            // All characters are "outer"
            let masked: String = chars.iter().map(|_| pad_char as char).collect();
            return Ok(Some(masked.into_bytes()));
        }

        let mut result = String::with_capacity(s.len());
        result.push(pad_char as char);
        for &c in &chars[1..chars.len() - 1] {
            result.push(c);
        }
        result.push(pad_char as char);
        Ok(Some(result.into_bytes()))
    }
}

// ── mask_replace ──────────────────────────────────────────────────────

/// Replaces the entire value with a given replacement string.
/// `mask_replace(val, replacement)`
struct MaskReplace;

impl CqlFunction for MaskReplace {
    fn name(&self) -> &str {
        "mask_replace"
    }
    fn arg_types(&self) -> Vec<CqlType> {
        vec![CqlType::Varchar, CqlType::Varchar]
    }
    fn return_type(&self) -> CqlType {
        CqlType::Varchar
    }
    fn execute(&self, args: &[Option<&[u8]>]) -> Result<Option<Vec<u8>>, String> {
        // If the input is null, return null.
        if args.first().and_then(|a| *a).is_none() {
            return Ok(None);
        }
        // Return the replacement value (second arg), or empty if not provided.
        match args.get(1).and_then(|a| *a) {
            Some(replacement) => Ok(Some(replacement.to_vec())),
            None => Ok(Some(Vec::new())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mask_default_text() {
        let f = MaskDefault;
        let val = Some(b"hello".as_slice());
        let result = f.execute(&[val]).unwrap();
        assert_eq!(result, Some(Vec::new()));
    }

    #[test]
    fn mask_default_int() {
        let f = MaskDefault;
        let val = 42i32.to_be_bytes();
        let result = f.execute(&[Some(&val)]).unwrap();
        assert_eq!(result, Some(0i32.to_be_bytes().to_vec()));
    }

    #[test]
    fn mask_null_returns_none() {
        let f = MaskNull;
        let val = Some(b"hello".as_slice());
        let result = f.execute(&[val]).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn mask_inner_basic() {
        let f = MaskInner;
        let val = b"hello";
        let pad = b"*";
        let result = f
            .execute(&[Some(val.as_slice()), Some(pad.as_slice())])
            .unwrap();
        assert_eq!(result, Some(b"h***o".to_vec()));
    }

    #[test]
    fn mask_inner_short() {
        let f = MaskInner;
        let val = b"ab";
        let pad = b"*";
        let result = f
            .execute(&[Some(val.as_slice()), Some(pad.as_slice())])
            .unwrap();
        assert_eq!(result, Some(b"ab".to_vec()));
    }

    #[test]
    fn mask_outer_basic() {
        let f = MaskOuter;
        let val = b"hello";
        let pad = b"*";
        let result = f
            .execute(&[Some(val.as_slice()), Some(pad.as_slice())])
            .unwrap();
        assert_eq!(result, Some(b"*ell*".to_vec()));
    }

    #[test]
    fn mask_replace_basic() {
        let f = MaskReplace;
        let val = b"secret";
        let replacement = b"REDACTED";
        let result = f
            .execute(&[Some(val.as_slice()), Some(replacement.as_slice())])
            .unwrap();
        assert_eq!(result, Some(b"REDACTED".to_vec()));
    }

    #[test]
    fn mask_replace_null_input() {
        let f = MaskReplace;
        let result = f.execute(&[None, Some(b"REDACTED".as_slice())]).unwrap();
        assert!(result.is_none());
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
    }
}
