// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

//! Vector type support for CQL `vector<T, n>`.
//!
//! ## Java Oracle
//!
//! `org.apache.cassandra.db.marshal.VectorType`
//! `org.apache.cassandra.cql3.functions.types.VectorType` (driver side)
//!
//! ## Design
//!
//! A CQL vector is a fixed-dimension array of a scalar subtype (typically
//! `float`).  Used primarily for vector similarity search (ANN) in
//! combination with SAI indexes.
//!
//! ## Serialization
//!
//! IEEE 754 float array, big-endian (Java-compatible):
//! - Each element is 4 bytes (f32) in big-endian byte order.
//! - Total serialized size = `dimensions * 4`.
//!
//! ## Similarity Functions
//!
//! Three similarity/distance functions are provided:
//! - **Cosine similarity** — `cos(a, b) = dot(a,b) / (||a|| * ||b||)`
//! - **Euclidean distance** — `d(a, b) = sqrt(sum((a_i - b_i)^2))`
//! - **Dot product** — `dot(a, b) = sum(a_i * b_i)`

use std::fmt;

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use serde::{Deserialize, Serialize};

/// A CQL vector value — a fixed-dimension array of f32.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VectorValue {
    /// The vector components.
    pub values: Vec<f32>,
}

impl VectorValue {
    /// Create a new vector from components.
    pub fn new(values: Vec<f32>) -> Self {
        Self { values }
    }

    /// Number of dimensions.
    pub fn dimensions(&self) -> u32 {
        self.values.len() as u32
    }

    /// Serialize to big-endian IEEE 754 byte array (Java-compatible).
    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(self.values.len() * 4);
        for &v in &self.values {
            buf.write_f32::<BigEndian>(v).unwrap();
        }
        buf
    }

    /// Deserialize from big-endian IEEE 754 byte array.
    pub fn deserialize(data: &[u8], dimensions: u32) -> Result<Self, VectorError> {
        let expected_len = dimensions as usize * 4;
        if data.len() != expected_len {
            return Err(VectorError::DimensionMismatch {
                expected: dimensions,
                got_bytes: data.len(),
            });
        }

        let mut cursor = std::io::Cursor::new(data);
        let mut values = Vec::with_capacity(dimensions as usize);
        for _ in 0..dimensions {
            let v = cursor
                .read_f32::<BigEndian>()
                .map_err(|e| VectorError::DeserializationError(e.to_string()))?;
            values.push(v);
        }

        Ok(Self { values })
    }

    /// Euclidean (L2) norm of the vector.
    pub fn norm(&self) -> f32 {
        self.values.iter().map(|v| v * v).sum::<f32>().sqrt()
    }
}

impl fmt::Display for VectorValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[")?;
        for (i, v) in self.values.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{v}")?;
        }
        write!(f, "]")
    }
}

// ─── Similarity Functions ──────────────────────────────────────────────────

/// Compute the dot product of two vectors.
///
/// # Panics
/// Panics if vectors have different dimensions.
pub fn dot_product(a: &VectorValue, b: &VectorValue) -> f32 {
    assert_eq!(
        a.dimensions(),
        b.dimensions(),
        "Vectors must have equal dimensions for dot product"
    );
    a.values
        .iter()
        .zip(b.values.iter())
        .map(|(x, y)| x * y)
        .sum()
}

/// Compute the cosine similarity between two vectors.
///
/// Returns a value in `[-1, 1]`.  Returns `0.0` if either vector has zero norm.
pub fn cosine_similarity(a: &VectorValue, b: &VectorValue) -> f32 {
    let dot = dot_product(a, b);
    let norm_a = a.norm();
    let norm_b = b.norm();

    if norm_a == 0.0 || norm_b == 0.0 {
        return 0.0;
    }

    dot / (norm_a * norm_b)
}

/// Compute the Euclidean (L2) distance between two vectors.
pub fn euclidean_distance(a: &VectorValue, b: &VectorValue) -> f32 {
    assert_eq!(
        a.dimensions(),
        b.dimensions(),
        "Vectors must have equal dimensions for euclidean distance"
    );
    a.values
        .iter()
        .zip(b.values.iter())
        .map(|(x, y)| (x - y).powi(2))
        .sum::<f32>()
        .sqrt()
}

/// Vector type errors.
#[derive(Debug, thiserror::Error)]
pub enum VectorError {
    #[error("Dimension mismatch: expected {expected} dims ({} bytes), got {got_bytes} bytes",
            expected * 4)]
    DimensionMismatch { expected: u32, got_bytes: usize },

    #[error("Deserialization error: {0}")]
    DeserializationError(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vector_creation() {
        let v = VectorValue::new(vec![1.0, 2.0, 3.0]);
        assert_eq!(v.dimensions(), 3);
    }

    #[test]
    fn serialize_round_trip() {
        let v = VectorValue::new(vec![1.0, -2.5, 3.14, 0.0]);
        let bytes = v.serialize();
        assert_eq!(bytes.len(), 16); // 4 floats * 4 bytes

        let decoded = VectorValue::deserialize(&bytes, 4).unwrap();
        assert_eq!(decoded.values.len(), 4);
        assert!((decoded.values[0] - 1.0).abs() < f32::EPSILON);
        assert!((decoded.values[1] - (-2.5)).abs() < f32::EPSILON);
        assert!((decoded.values[2] - 3.14).abs() < 0.001);
        assert!((decoded.values[3] - 0.0).abs() < f32::EPSILON);
    }

    #[test]
    fn deserialize_wrong_dimensions() {
        let v = VectorValue::new(vec![1.0, 2.0]);
        let bytes = v.serialize();
        let result = VectorValue::deserialize(&bytes, 3);
        assert!(result.is_err());
    }

    #[test]
    fn dot_product_basic() {
        let a = VectorValue::new(vec![1.0, 0.0, 0.0]);
        let b = VectorValue::new(vec![0.0, 1.0, 0.0]);
        assert!((dot_product(&a, &b) - 0.0).abs() < f32::EPSILON);

        let c = VectorValue::new(vec![1.0, 2.0, 3.0]);
        let d = VectorValue::new(vec![4.0, 5.0, 6.0]);
        // 1*4 + 2*5 + 3*6 = 32
        assert!((dot_product(&c, &d) - 32.0).abs() < f32::EPSILON);
    }

    #[test]
    fn cosine_similarity_identical() {
        let a = VectorValue::new(vec![1.0, 2.0, 3.0]);
        let similarity = cosine_similarity(&a, &a);
        assert!((similarity - 1.0).abs() < 0.0001);
    }

    #[test]
    fn cosine_similarity_orthogonal() {
        let a = VectorValue::new(vec![1.0, 0.0]);
        let b = VectorValue::new(vec![0.0, 1.0]);
        let similarity = cosine_similarity(&a, &b);
        assert!(similarity.abs() < f32::EPSILON);
    }

    #[test]
    fn cosine_similarity_opposite() {
        let a = VectorValue::new(vec![1.0, 0.0]);
        let b = VectorValue::new(vec![-1.0, 0.0]);
        let similarity = cosine_similarity(&a, &b);
        assert!((similarity - (-1.0)).abs() < 0.0001);
    }

    #[test]
    fn cosine_zero_vector() {
        let a = VectorValue::new(vec![0.0, 0.0]);
        let b = VectorValue::new(vec![1.0, 2.0]);
        assert!((cosine_similarity(&a, &b) - 0.0).abs() < f32::EPSILON);
    }

    #[test]
    fn euclidean_distance_same_point() {
        let a = VectorValue::new(vec![1.0, 2.0, 3.0]);
        assert!((euclidean_distance(&a, &a) - 0.0).abs() < f32::EPSILON);
    }

    #[test]
    fn euclidean_distance_known() {
        let a = VectorValue::new(vec![0.0, 0.0]);
        let b = VectorValue::new(vec![3.0, 4.0]);
        assert!((euclidean_distance(&a, &b) - 5.0).abs() < 0.0001);
    }

    #[test]
    fn norm() {
        let v = VectorValue::new(vec![3.0, 4.0]);
        assert!((v.norm() - 5.0).abs() < 0.0001);
    }

    #[test]
    fn display() {
        let v = VectorValue::new(vec![1.0, 2.5]);
        let s = format!("{v}");
        assert_eq!(s, "[1, 2.5]");
    }

    #[test]
    fn empty_vector() {
        let v = VectorValue::new(vec![]);
        assert_eq!(v.dimensions(), 0);
        let bytes = v.serialize();
        let decoded = VectorValue::deserialize(&bytes, 0).unwrap();
        assert_eq!(decoded.values.len(), 0);
    }
}
