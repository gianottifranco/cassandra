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

/// Cassandra exception categories mirroring the Java exception hierarchy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CassandraExceptionCategory {
    Transport,
    Authentication,
    RequestValidation,
    RequestExecution,
    Server,
    Internal,
}

/// Java Cassandra exception kind represented by [`CassandraError`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CassandraExceptionKind {
    ServerError,
    ProtocolException,
    AuthenticationException,
    UnavailableException,
    OverloadedException,
    IsBootstrappingException,
    TruncateException,
    WriteTimeoutException,
    ReadTimeoutException,
    ReadFailureException,
    FunctionExecutionException,
    WriteFailureException,
    CDCWriteException,
    SyntaxException,
    UnauthorizedException,
    InvalidRequestException,
    ConfigurationException,
    AlreadyExistsException,
    PreparedQueryNotFoundException,
    InternalError,
    IoError,
}

impl CassandraExceptionKind {
    /// Java exception class name that best matches this error kind.
    pub fn java_class_name(self) -> &'static str {
        match self {
            Self::ServerError => "org.apache.cassandra.exceptions.ServerError",
            Self::ProtocolException => "org.apache.cassandra.transport.ProtocolException",
            Self::AuthenticationException => {
                "org.apache.cassandra.exceptions.AuthenticationException"
            }
            Self::UnavailableException => "org.apache.cassandra.exceptions.UnavailableException",
            Self::OverloadedException => "org.apache.cassandra.exceptions.OverloadedException",
            Self::IsBootstrappingException => {
                "org.apache.cassandra.exceptions.IsBootstrappingException"
            }
            Self::TruncateException => "org.apache.cassandra.exceptions.TruncateException",
            Self::WriteTimeoutException => "org.apache.cassandra.exceptions.WriteTimeoutException",
            Self::ReadTimeoutException => "org.apache.cassandra.exceptions.ReadTimeoutException",
            Self::ReadFailureException => "org.apache.cassandra.exceptions.ReadFailureException",
            Self::FunctionExecutionException => {
                "org.apache.cassandra.exceptions.FunctionExecutionException"
            }
            Self::WriteFailureException => "org.apache.cassandra.exceptions.WriteFailureException",
            Self::CDCWriteException => "org.apache.cassandra.exceptions.CDCWriteException",
            Self::SyntaxException => "org.apache.cassandra.exceptions.SyntaxException",
            Self::UnauthorizedException => "org.apache.cassandra.exceptions.UnauthorizedException",
            Self::InvalidRequestException => {
                "org.apache.cassandra.exceptions.InvalidRequestException"
            }
            Self::ConfigurationException => {
                "org.apache.cassandra.exceptions.ConfigurationException"
            }
            Self::AlreadyExistsException => {
                "org.apache.cassandra.exceptions.AlreadyExistsException"
            }
            Self::PreparedQueryNotFoundException => {
                "org.apache.cassandra.exceptions.PreparedQueryNotFoundException"
            }
            Self::InternalError => "org.apache.cassandra.exceptions.InternalError",
            Self::IoError => "java.io.IOException",
        }
    }

    /// High-level Java exception family.
    pub fn category(self) -> CassandraExceptionCategory {
        match self {
            Self::ProtocolException => CassandraExceptionCategory::Transport,
            Self::AuthenticationException => CassandraExceptionCategory::Authentication,
            Self::SyntaxException
            | Self::UnauthorizedException
            | Self::InvalidRequestException
            | Self::ConfigurationException
            | Self::AlreadyExistsException
            | Self::PreparedQueryNotFoundException => CassandraExceptionCategory::RequestValidation,
            Self::UnavailableException
            | Self::OverloadedException
            | Self::IsBootstrappingException
            | Self::TruncateException
            | Self::WriteTimeoutException
            | Self::ReadTimeoutException
            | Self::ReadFailureException
            | Self::FunctionExecutionException
            | Self::WriteFailureException
            | Self::CDCWriteException => CassandraExceptionCategory::RequestExecution,
            Self::ServerError => CassandraExceptionCategory::Server,
            Self::InternalError | Self::IoError => CassandraExceptionCategory::Internal,
        }
    }
}

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

    /// Read failure (0x1300).
    #[error(
        "Read failure: consistency={consistency}, received={received}, required={block_for}, failures={num_failures}"
    )]
    ReadFailure {
        consistency: String,
        received: i32,
        block_for: i32,
        num_failures: i32,
        data_present: bool,
    },

    /// Function failure (0x1400).
    #[error("Function failure: {keyspace}.{function}({arg_types:?})")]
    FunctionFailure {
        keyspace: String,
        function: String,
        arg_types: Vec<String>,
    },

    /// Write failure (0x1500).
    #[error(
        "Write failure: consistency={consistency}, received={received}, required={block_for}, failures={num_failures}"
    )]
    WriteFailure {
        consistency: String,
        received: i32,
        block_for: i32,
        num_failures: i32,
        write_type: String,
    },

    /// CDC write failure (0x1600).
    #[error("CDC write failure")]
    CDCWriteFailure,

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
    /// Java Cassandra exception kind represented by this error.
    pub fn exception_kind(&self) -> CassandraExceptionKind {
        match self {
            Self::ServerError(_) => CassandraExceptionKind::ServerError,
            Self::ProtocolError(_) => CassandraExceptionKind::ProtocolException,
            Self::AuthenticationError(_) => CassandraExceptionKind::AuthenticationException,
            Self::Unavailable { .. } => CassandraExceptionKind::UnavailableException,
            Self::Overloaded => CassandraExceptionKind::OverloadedException,
            Self::IsBootstrapping => CassandraExceptionKind::IsBootstrappingException,
            Self::TruncateError(_) => CassandraExceptionKind::TruncateException,
            Self::WriteTimeout { .. } => CassandraExceptionKind::WriteTimeoutException,
            Self::ReadTimeout { .. } => CassandraExceptionKind::ReadTimeoutException,
            Self::ReadFailure { .. } => CassandraExceptionKind::ReadFailureException,
            Self::FunctionFailure { .. } => CassandraExceptionKind::FunctionExecutionException,
            Self::WriteFailure { .. } => CassandraExceptionKind::WriteFailureException,
            Self::CDCWriteFailure => CassandraExceptionKind::CDCWriteException,
            Self::SyntaxError(_) => CassandraExceptionKind::SyntaxException,
            Self::Unauthorized(_) => CassandraExceptionKind::UnauthorizedException,
            Self::InvalidQuery(_) => CassandraExceptionKind::InvalidRequestException,
            Self::ConfigError(_) => CassandraExceptionKind::ConfigurationException,
            Self::AlreadyExists { .. } => CassandraExceptionKind::AlreadyExistsException,
            Self::Unprepared(_) => CassandraExceptionKind::PreparedQueryNotFoundException,
            Self::Internal(_) => CassandraExceptionKind::InternalError,
            Self::Io(_) => CassandraExceptionKind::IoError,
        }
    }

    /// High-level Java exception family for this error.
    pub fn exception_category(&self) -> CassandraExceptionCategory {
        self.exception_kind().category()
    }

    /// Java exception class name that best matches this error.
    pub fn java_class_name(&self) -> &'static str {
        self.exception_kind().java_class_name()
    }

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
            Self::ReadFailure { .. } => Some(0x1300),
            Self::FunctionFailure { .. } => Some(0x1400),
            Self::WriteFailure { .. } => Some(0x1500),
            Self::CDCWriteFailure => Some(0x1600),
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
        assert_eq!(
            CassandraError::ReadFailure {
                consistency: "QUORUM".into(),
                received: 2,
                block_for: 3,
                num_failures: 1,
                data_present: false,
            }
            .error_code(),
            Some(0x1300)
        );
        assert_eq!(
            CassandraError::FunctionFailure {
                keyspace: "ks".into(),
                function: "fn".into(),
                arg_types: vec!["int".into()],
            }
            .error_code(),
            Some(0x1400)
        );
        assert_eq!(
            CassandraError::WriteFailure {
                consistency: "QUORUM".into(),
                received: 2,
                block_for: 3,
                num_failures: 1,
                write_type: "SIMPLE".into(),
            }
            .error_code(),
            Some(0x1500)
        );
        assert_eq!(CassandraError::CDCWriteFailure.error_code(), Some(0x1600));
    }

    #[test]
    fn exception_kinds_map_to_java_hierarchy() {
        let cases = vec![
            (
                CassandraError::ProtocolError("bad frame".into()),
                CassandraExceptionKind::ProtocolException,
                CassandraExceptionCategory::Transport,
                "org.apache.cassandra.transport.ProtocolException",
            ),
            (
                CassandraError::AuthenticationError("bad credentials".into()),
                CassandraExceptionKind::AuthenticationException,
                CassandraExceptionCategory::Authentication,
                "org.apache.cassandra.exceptions.AuthenticationException",
            ),
            (
                CassandraError::InvalidQuery("bad query".into()),
                CassandraExceptionKind::InvalidRequestException,
                CassandraExceptionCategory::RequestValidation,
                "org.apache.cassandra.exceptions.InvalidRequestException",
            ),
            (
                CassandraError::WriteTimeout {
                    consistency: "QUORUM".into(),
                    received: 1,
                    block_for: 2,
                    write_type: "SIMPLE".into(),
                },
                CassandraExceptionKind::WriteTimeoutException,
                CassandraExceptionCategory::RequestExecution,
                "org.apache.cassandra.exceptions.WriteTimeoutException",
            ),
            (
                CassandraError::ServerError("boom".into()),
                CassandraExceptionKind::ServerError,
                CassandraExceptionCategory::Server,
                "org.apache.cassandra.exceptions.ServerError",
            ),
            (
                CassandraError::Internal("bug".into()),
                CassandraExceptionKind::InternalError,
                CassandraExceptionCategory::Internal,
                "org.apache.cassandra.exceptions.InternalError",
            ),
        ];

        for (err, kind, category, java_class_name) in cases {
            assert_eq!(err.exception_kind(), kind);
            assert_eq!(err.exception_category(), category);
            assert_eq!(err.java_class_name(), java_class_name);
        }
    }

    #[test]
    fn all_protocol_errors_have_java_exception_names() {
        let errors = vec![
            CassandraError::ServerError("x".into()),
            CassandraError::ProtocolError("x".into()),
            CassandraError::AuthenticationError("x".into()),
            CassandraError::Unavailable {
                consistency: "ONE".into(),
                required: 1,
                alive: 0,
            },
            CassandraError::Overloaded,
            CassandraError::IsBootstrapping,
            CassandraError::TruncateError("x".into()),
            CassandraError::WriteTimeout {
                consistency: "ONE".into(),
                received: 0,
                block_for: 1,
                write_type: "SIMPLE".into(),
            },
            CassandraError::ReadTimeout {
                consistency: "ONE".into(),
                received: 0,
                block_for: 1,
                data_present: false,
            },
            CassandraError::ReadFailure {
                consistency: "ONE".into(),
                received: 0,
                block_for: 1,
                num_failures: 1,
                data_present: false,
            },
            CassandraError::FunctionFailure {
                keyspace: "ks".into(),
                function: "f".into(),
                arg_types: Vec::new(),
            },
            CassandraError::WriteFailure {
                consistency: "ONE".into(),
                received: 0,
                block_for: 1,
                num_failures: 1,
                write_type: "SIMPLE".into(),
            },
            CassandraError::CDCWriteFailure,
            CassandraError::SyntaxError("x".into()),
            CassandraError::Unauthorized("x".into()),
            CassandraError::InvalidQuery("x".into()),
            CassandraError::ConfigError("x".into()),
            CassandraError::AlreadyExists {
                ks: "ks".into(),
                table: "tbl".into(),
            },
            CassandraError::Unprepared(vec![1, 2, 3]),
        ];

        for err in errors {
            assert!(err.error_code().is_some(), "{err:?}");
            assert!(err.java_class_name().starts_with("org.apache.cassandra."));
        }
    }

    #[test]
    fn io_error_converts() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
        let err: CassandraError = io_err.into();
        assert!(matches!(err, CassandraError::Io(_)));
        assert!(err.error_code().is_none());
    }
}
