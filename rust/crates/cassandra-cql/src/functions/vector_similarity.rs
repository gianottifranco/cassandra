// Licensed under Apache License, Version 2.0.

//! CQL vector similarity functions.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.cql3.functions.VectorFcts`
//!
//! Implements `similarity_cosine`, `similarity_euclidean`, and
//! `similarity_dot_product` as CQL scalar functions. Each takes two
//! serialized vector arguments (big-endian f32 arrays) and returns a float.

use super::registry::{CqlFunction, FunctionRegistry};
use cassandra_types::CqlType;
use std::sync::Arc;

/// Register all vector similarity functions.
pub fn register_all(registry: &FunctionRegistry) {
    registry.register(Arc::new(SimilarityCosine));
    registry.register(Arc::new(SimilarityEuclidean));
    registry.register(Arc::new(SimilarityDotProduct));
}

/// Deserialize two vector args from big-endian f32 byte arrays and compute
/// their similarity using the given function.
fn execute_similarity(
    args: &[Option<&[u8]>],
    sim_fn: fn(&cassandra_types::vector::VectorValue, &cassandra_types::vector::VectorValue) -> f32,
) -> Result<Option<Vec<u8>>, String> {
    let a_bytes = match args.first().and_then(|a| *a) {
        Some(b) => b,
        None => return Ok(None),
    };
    let b_bytes = match args.get(1).and_then(|a| *a) {
        Some(b) => b,
        None => return Ok(None),
    };

    if a_bytes.len() % 4 != 0 {
        return Err("first vector argument has invalid byte length".to_string());
    }
    if b_bytes.len() % 4 != 0 {
        return Err("second vector argument has invalid byte length".to_string());
    }

    let dims_a = (a_bytes.len() / 4) as u32;
    let dims_b = (b_bytes.len() / 4) as u32;

    let vec_a = cassandra_types::vector::VectorValue::deserialize(a_bytes, dims_a)
        .map_err(|e| e.to_string())?;
    let vec_b = cassandra_types::vector::VectorValue::deserialize(b_bytes, dims_b)
        .map_err(|e| e.to_string())?;

    if dims_a != dims_b {
        return Err(format!(
            "vector dimension mismatch: {} vs {}",
            dims_a, dims_b
        ));
    }

    let result = sim_fn(&vec_a, &vec_b);
    Ok(Some(result.to_be_bytes().to_vec()))
}

// ── similarity_cosine ────────────────────────────────────────────────────

struct SimilarityCosine;

impl CqlFunction for SimilarityCosine {
    fn name(&self) -> &str {
        "similarity_cosine"
    }
    fn arg_types(&self) -> Vec<CqlType> {
        vec![CqlType::Blob, CqlType::Blob]
    }
    fn return_type(&self) -> CqlType {
        CqlType::Float
    }
    fn execute(&self, args: &[Option<&[u8]>]) -> Result<Option<Vec<u8>>, String> {
        execute_similarity(args, cassandra_types::vector::cosine_similarity)
    }
}

// ── similarity_euclidean ─────────────────────────────────────────────────

struct SimilarityEuclidean;

impl CqlFunction for SimilarityEuclidean {
    fn name(&self) -> &str {
        "similarity_euclidean"
    }
    fn arg_types(&self) -> Vec<CqlType> {
        vec![CqlType::Blob, CqlType::Blob]
    }
    fn return_type(&self) -> CqlType {
        CqlType::Float
    }
    fn execute(&self, args: &[Option<&[u8]>]) -> Result<Option<Vec<u8>>, String> {
        execute_similarity(args, cassandra_types::vector::euclidean_distance)
    }
}

// ── similarity_dot_product ───────────────────────────────────────────────

struct SimilarityDotProduct;

impl CqlFunction for SimilarityDotProduct {
    fn name(&self) -> &str {
        "similarity_dot_product"
    }
    fn arg_types(&self) -> Vec<CqlType> {
        vec![CqlType::Blob, CqlType::Blob]
    }
    fn return_type(&self) -> CqlType {
        CqlType::Float
    }
    fn execute(&self, args: &[Option<&[u8]>]) -> Result<Option<Vec<u8>>, String> {
        execute_similarity(args, cassandra_types::vector::dot_product)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cassandra_types::vector::VectorValue;

    fn serialize_vec(vals: &[f32]) -> Vec<u8> {
        VectorValue::new(vals.to_vec()).serialize()
    }

    #[test]
    fn cosine_identical_vectors() {
        let f = SimilarityCosine;
        let v = serialize_vec(&[1.0, 0.0, 0.0]);
        let result = f.execute(&[Some(&v), Some(&v)]).unwrap().unwrap();
        let score = f32::from_be_bytes(result.try_into().unwrap());
        assert!((score - 1.0).abs() < 1e-6);
    }

    #[test]
    fn cosine_orthogonal_vectors() {
        let f = SimilarityCosine;
        let a = serialize_vec(&[1.0, 0.0]);
        let b = serialize_vec(&[0.0, 1.0]);
        let result = f.execute(&[Some(&a), Some(&b)]).unwrap().unwrap();
        let score = f32::from_be_bytes(result.try_into().unwrap());
        assert!(score.abs() < 1e-6);
    }

    #[test]
    fn euclidean_same_point() {
        let f = SimilarityEuclidean;
        let v = serialize_vec(&[1.0, 2.0, 3.0]);
        let result = f.execute(&[Some(&v), Some(&v)]).unwrap().unwrap();
        let dist = f32::from_be_bytes(result.try_into().unwrap());
        assert!(dist.abs() < 1e-6);
    }

    #[test]
    fn euclidean_known_distance() {
        let f = SimilarityEuclidean;
        let a = serialize_vec(&[0.0, 0.0]);
        let b = serialize_vec(&[3.0, 4.0]);
        let result = f.execute(&[Some(&a), Some(&b)]).unwrap().unwrap();
        let dist = f32::from_be_bytes(result.try_into().unwrap());
        assert!((dist - 5.0).abs() < 1e-6);
    }

    #[test]
    fn dot_product_basic() {
        let f = SimilarityDotProduct;
        let a = serialize_vec(&[1.0, 2.0, 3.0]);
        let b = serialize_vec(&[4.0, 5.0, 6.0]);
        let result = f.execute(&[Some(&a), Some(&b)]).unwrap().unwrap();
        let dp = f32::from_be_bytes(result.try_into().unwrap());
        // 1*4 + 2*5 + 3*6 = 32
        assert!((dp - 32.0).abs() < 1e-6);
    }

    #[test]
    fn null_arg_returns_none() {
        let f = SimilarityCosine;
        let v = serialize_vec(&[1.0, 0.0]);
        assert!(f.execute(&[None, Some(&v)]).unwrap().is_none());
        assert!(f.execute(&[Some(&v), None]).unwrap().is_none());
    }

    #[test]
    fn dimension_mismatch_error() {
        let f = SimilarityCosine;
        let a = serialize_vec(&[1.0, 0.0]);
        let b = serialize_vec(&[1.0, 0.0, 0.0]);
        let result = f.execute(&[Some(&a), Some(&b)]);
        assert!(result.is_err());
    }

    #[test]
    fn invalid_byte_length_error() {
        let f = SimilarityCosine;
        let bad = vec![1, 2, 3]; // not a multiple of 4
        let good = serialize_vec(&[1.0]);
        assert!(f.execute(&[Some(&bad), Some(&good)]).is_err());
    }
}
