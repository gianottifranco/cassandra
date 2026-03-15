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

//! Error types for the Cassandra Rust implementation.
//!
//! Maps to error codes from the CQL native protocol specification.
//! Java oracle: `org.apache.cassandra.exceptions.*`

use thiserror::Error;

/// Result type alias used throughout the Cassandra crates.
pub type CassandraResult<T> = Result<T, CassandraError>;

/// Top-level error type for the Cassandra Rust implementation.
///
/// Error variants correspond to CQL protocol error codes where applicable.
/// See: CQL native protocol spec, section 9 (Error codes).
#[derive(Debug, Error)]
pub enum CassandraError {
    // --- Protocol errors (0x0000 - 0x00FF) ---
    /// Server error (0x0000).
    #[error("Server error: {0}")]
    ServerError(String),

    /// Protocol error (0x000A).
    #[error("Protocol error: {0}")]
    ProtocolError(String),

    /// Authentication error (0x0100).
    #[error("Authentication error: {0}")]
    AuthenticationError(String),

    // --- Query errors (0x1000 - 0x1FFF) ---
    /// Unavailable (0x1000).
    #[error("Cannot achieve consistency level {consistency}: required {required}, alive {alive}")]
    Unavailable {
        consistency: String,
        required: i32,
        alive: i32,
    },

    /// Coordinator overloaded (0x1001).
    #[error("Coordinator overloaded")]
    Overloaded,

    /// Coordinator node is bootstrapping (0x1002).
    #[error("Coordinator node is bootstrapping")]
    IsBootstrapping,

    /// Truncate error (0x1003).
    #[error("Truncation error: {0}")]
    TruncateError(String),

    /// Write timeout (0x1100).
    #[error("Write timeout: consistency={consistency}, received={received}, required={block_for}")]
    WriteTimeout {
        consistency: String,
        received: i32,
        block_for: i32,
        write_type: String,
    },

    /// Read timeout (0x1200).
    #[error("Read timeout: consistency={consistency}, received={received}, required={block_for}")]
    ReadTimeout {
        consistency: String,
        received: i32,
        block_for: i32,
        data_present: bool,
    },

    // --- Syntax/Validation errors (0x2000 - 0x2FFF) ---
    /// Syntax error (0x2000).
    #[error("Syntax error: {0}")]
    SyntaxError(String),

    /// Unauthorized (0x2100).
    #[error("Unauthorized: {0}")]
    Unauthorized(String),

    /// Invalid query (0x2200).
    #[error("Invalid query: {0}")]
    InvalidQuery(String),

    /// Configuration error (0x2300).
    #[error("Configuration error: {0}")]
    ConfigError(String),

    /// Already exists (0x2400).
    #[error("Already exists: {ks}.{table}")]
    AlreadyExists { ks: String, table: String },

    /// Unprepared statement (0x2500).
    #[error("Unprepared statement: {0:?}")]
    Unprepared(Vec<u8>),

    // --- Internal (non-protocol) errors ---
    /// Internal error not mapped to a protocol error code.
    #[error("Internal error: {0}")]
    Internal(String),

    /// I/O error.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

impl CassandraError {
    /// Returns the CQL native protocol error code for this error.
    ///
    /// Returns `None` for internal errors that don't map to protocol codes.
    pub fn error_code(&self) -> Option<i32> {
        match self {
            Self::ServerError(_) => Some(0x0000),
            Self::ProtocolError(_) => Some(0x000A),
            Self::AuthenticationError(_) => Some(0x0100),
            Self::Unavailable { .. } => Some(0x1000),
            Self::Overloaded => Some(0x1001),
            Self::IsBootstrapping => Some(0x1002),
            Self::TruncateError(_) => Some(0x1003),
            Self::WriteTimeout { .. } => Some(0x1100),
            Self::ReadTimeout { .. } => Some(0x1200),
            Self::SyntaxError(_) => Some(0x2000),
            Self::Unauthorized(_) => Some(0x2100),
            Self::InvalidQuery(_) => Some(0x2200),
            Self::ConfigError(_) => Some(0x2300),
            Self::AlreadyExists { .. } => Some(0x2400),
            Self::Unprepared(_) => Some(0x2500),
            Self::Internal(_) | Self::Io(_) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_codes_match_protocol_spec() {
        assert_eq!(
            CassandraError::ServerError("x".into()).error_code(),
            Some(0x0000)
        );
        assert_eq!(
            CassandraError::Unavailable {
                consistency: "QUORUM".into(),
                required: 2,
                alive: 1,
            }
            .error_code(),
            Some(0x1000)
        );
        assert_eq!(
            CassandraError::SyntaxError("x".into()).error_code(),
            Some(0x2000)
        );
        assert_eq!(CassandraError::Internal("x".into()).error_code(), None);
    }

    #[test]
    fn io_error_converts() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
        let err: CassandraError = io_err.into();
        assert!(matches!(err, CassandraError::Io(_)));
        assert!(err.error_code().is_none());
    }
}
