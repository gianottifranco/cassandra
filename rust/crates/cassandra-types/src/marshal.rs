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

//! Marshal error types for the CQL type system.
//!
//! Defines [`MarshalError`] and [`MarshalResult`] used by serialization,
//! deserialization, and validation code throughout the marshal framework.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.serializers.MarshalException`
//! - `org.apache.cassandra.db.marshal.MarshalException`

use std::fmt;

/// Errors that can occur during CQL value marshalling/unmarshalling.
///
/// These correspond to the various `MarshalException` subtypes in Java.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MarshalError {
    /// The bytes are not valid for this type.
    InvalidData {
        type_name: String,
        reason: String,
    },
    /// The actual type does not match the expected type.
    TypeMismatch { expected: String, actual: String },
    /// The byte buffer has the wrong size for this type.
    InvalidSize { expected: usize, actual: usize },
    /// A UTF-8 encoding error occurred.
    Utf8Error,
    /// A numeric overflow occurred during conversion.
    Overflow,
    /// The type is not supported in this context.
    UnsupportedType(String),
}

impl fmt::Display for MarshalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidData { type_name, reason } => {
                write!(f, "invalid data for type '{}': {}", type_name, reason)
            }
            Self::TypeMismatch { expected, actual } => {
                write!(f, "type mismatch: expected '{}', got '{}'", expected, actual)
            }
            Self::InvalidSize { expected, actual } => {
                write!(
                    f,
                    "invalid size: expected {} bytes, got {}",
                    expected, actual
                )
            }
            Self::Utf8Error => write!(f, "invalid UTF-8 encoding"),
            Self::Overflow => write!(f, "numeric overflow"),
            Self::UnsupportedType(name) => write!(f, "unsupported type: {}", name),
        }
    }
}

impl std::error::Error for MarshalError {}

/// Convenience alias for `Result<T, MarshalError>`.
pub type MarshalResult<T> = Result<T, MarshalError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_invalid_data() {
        let e = MarshalError::InvalidData {
            type_name: "int".into(),
            reason: "wrong length".into(),
        };
        assert!(e.to_string().contains("int"));
        assert!(e.to_string().contains("wrong length"));
    }

    #[test]
    fn display_type_mismatch() {
        let e = MarshalError::TypeMismatch {
            expected: "text".into(),
            actual: "int".into(),
        };
        assert!(e.to_string().contains("text"));
        assert!(e.to_string().contains("int"));
    }

    #[test]
    fn display_all_variants() {
        let variants: &[MarshalError] = &[
            MarshalError::InvalidData { type_name: "t".into(), reason: "r".into() },
            MarshalError::TypeMismatch { expected: "e".into(), actual: "a".into() },
            MarshalError::InvalidSize { expected: 4, actual: 2 },
            MarshalError::Utf8Error,
            MarshalError::Overflow,
            MarshalError::UnsupportedType("vector".into()),
        ];
        for v in variants {
            // All variants must produce non-empty display strings
            assert!(!v.to_string().is_empty());
        }
    }
}
