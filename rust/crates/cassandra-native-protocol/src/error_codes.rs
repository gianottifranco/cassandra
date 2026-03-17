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

//! Mapping from CassandraError to native protocol error responses.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.transport.messages.ErrorMessage`
//! - `org.apache.cassandra.exceptions.ExceptionCode`

use crate::message::{ErrorDetail, ErrorMessage};
use crate::types::Consistency;
use cassandra_common::CassandraError;

/// Convert a CassandraError into a protocol ErrorMessage.
///
/// This ensures the wire error codes, messages, and detail fields match
/// exactly what Java Cassandra would produce.
pub fn error_to_message(err: &CassandraError) -> ErrorMessage {
    match err {
        CassandraError::ServerError(msg) => ErrorMessage {
            code: 0x0000,
            message: msg.clone(),
            detail: ErrorDetail::None,
        },
        CassandraError::ProtocolError(msg) => ErrorMessage {
            code: 0x000A,
            message: msg.clone(),
            detail: ErrorDetail::None,
        },
        CassandraError::AuthenticationError(msg) => ErrorMessage {
            code: 0x0100,
            message: msg.clone(),
            detail: ErrorDetail::None,
        },
        CassandraError::Unavailable {
            consistency,
            required,
            alive,
        } => ErrorMessage {
            code: 0x1000,
            message: format!(
                "Cannot achieve consistency level {}: not enough replicas alive ({}/{})",
                consistency, alive, required
            ),
            detail: ErrorDetail::Unavailable {
                consistency: Consistency::from_name(consistency).unwrap_or(Consistency::One),
                required: *required,
                alive: *alive,
            },
        },
        CassandraError::Overloaded => ErrorMessage {
            code: 0x1001,
            message: "Coordinator node is overloaded".to_string(),
            detail: ErrorDetail::None,
        },
        CassandraError::IsBootstrapping => ErrorMessage {
            code: 0x1002,
            message: "Coordinator node is bootstrapping".to_string(),
            detail: ErrorDetail::None,
        },
        CassandraError::TruncateError(msg) => ErrorMessage {
            code: 0x1003,
            message: msg.clone(),
            detail: ErrorDetail::None,
        },
        CassandraError::WriteTimeout {
            consistency,
            received,
            block_for,
            write_type,
        } => ErrorMessage {
            code: 0x1100,
            message: format!(
                "Operation timed out - received only {} responses, required {} for {}",
                received, block_for, consistency
            ),
            detail: ErrorDetail::WriteTimeout {
                consistency: Consistency::from_name(consistency).unwrap_or(Consistency::One),
                received: *received,
                block_for: *block_for,
                write_type: write_type.clone(),
            },
        },
        CassandraError::ReadTimeout {
            consistency,
            received,
            block_for,
            data_present,
        } => ErrorMessage {
            code: 0x1200,
            message: format!(
                "Operation timed out - received only {} responses, required {} for {}",
                received, block_for, consistency
            ),
            detail: ErrorDetail::ReadTimeout {
                consistency: Consistency::from_name(consistency).unwrap_or(Consistency::One),
                received: *received,
                block_for: *block_for,
                data_present: *data_present,
            },
        },
        CassandraError::ReadFailure {
            consistency,
            received,
            block_for,
            num_failures,
            data_present,
        } => ErrorMessage {
            code: 0x1300,
            message: format!(
                "Operation failed - received {} responses and {} failures for {}",
                received, num_failures, consistency
            ),
            detail: ErrorDetail::ReadFailure {
                consistency: Consistency::from_name(consistency).unwrap_or(Consistency::One),
                received: *received,
                block_for: *block_for,
                num_failures: *num_failures,
                data_present: *data_present,
            },
        },
        CassandraError::FunctionFailure {
            keyspace,
            function,
            arg_types,
        } => ErrorMessage {
            code: 0x1400,
            message: format!(
                "Execution of function {}.{}({}) failed",
                keyspace,
                function,
                arg_types.join(", ")
            ),
            detail: ErrorDetail::FunctionFailure {
                keyspace: keyspace.clone(),
                function: function.clone(),
                arg_types: arg_types.clone(),
            },
        },
        CassandraError::WriteFailure {
            consistency,
            received,
            block_for,
            num_failures,
            write_type,
        } => ErrorMessage {
            code: 0x1500,
            message: format!(
                "Operation failed - received {} responses and {} failures for {}",
                received, num_failures, consistency
            ),
            detail: ErrorDetail::WriteFailure {
                consistency: Consistency::from_name(consistency).unwrap_or(Consistency::One),
                received: *received,
                block_for: *block_for,
                num_failures: *num_failures,
                write_type: write_type.clone(),
            },
        },
        CassandraError::CDCWriteFailure => ErrorMessage {
            code: 0x1600,
            message: "CDC write failure".to_string(),
            detail: ErrorDetail::None,
        },
        CassandraError::SyntaxError(msg) => ErrorMessage {
            code: 0x2000,
            message: msg.clone(),
            detail: ErrorDetail::None,
        },
        CassandraError::Unauthorized(msg) => ErrorMessage {
            code: 0x2100,
            message: msg.clone(),
            detail: ErrorDetail::None,
        },
        CassandraError::InvalidQuery(msg) => ErrorMessage {
            code: 0x2200,
            message: msg.clone(),
            detail: ErrorDetail::None,
        },
        CassandraError::ConfigError(msg) => ErrorMessage {
            code: 0x2300,
            message: msg.clone(),
            detail: ErrorDetail::None,
        },
        CassandraError::AlreadyExists { ks, table } => ErrorMessage {
            code: 0x2400,
            message: format!("Cannot add already existing {}.{}", ks, table),
            detail: ErrorDetail::AlreadyExists {
                keyspace: ks.clone(),
                table: table.clone(),
            },
        },
        CassandraError::Unprepared(id) => ErrorMessage {
            code: 0x2500,
            message: "Prepared query not found".to_string(),
            detail: ErrorDetail::Unprepared { id: id.clone() },
        },
        CassandraError::Internal(msg) => ErrorMessage {
            code: 0x0000,
            message: format!("Server error: {}", msg),
            detail: ErrorDetail::None,
        },
        CassandraError::Io(e) => ErrorMessage {
            code: 0x0000,
            message: format!("Server I/O error: {}", e),
            detail: ErrorDetail::None,
        },
    }
}

impl Consistency {
    /// Parse a consistency name (as stored in CassandraError) to wire value.
    pub fn from_name(name: &str) -> Option<Consistency> {
        match name.to_uppercase().as_str() {
            "ANY" => Some(Consistency::Any),
            "ONE" => Some(Consistency::One),
            "TWO" => Some(Consistency::Two),
            "THREE" => Some(Consistency::Three),
            "QUORUM" => Some(Consistency::Quorum),
            "ALL" => Some(Consistency::All),
            "LOCAL_QUORUM" => Some(Consistency::LocalQuorum),
            "EACH_QUORUM" => Some(Consistency::EachQuorum),
            "SERIAL" => Some(Consistency::Serial),
            "LOCAL_SERIAL" => Some(Consistency::LocalSerial),
            "LOCAL_ONE" => Some(Consistency::LocalOne),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn syntax_error_mapping() {
        let err = CassandraError::SyntaxError("unterminated string".to_string());
        let msg = error_to_message(&err);
        assert_eq!(msg.code, 0x2000);
        assert_eq!(msg.message, "unterminated string");
    }

    #[test]
    fn unavailable_mapping() {
        let err = CassandraError::Unavailable {
            consistency: "QUORUM".to_string(),
            required: 2,
            alive: 1,
        };
        let msg = error_to_message(&err);
        assert_eq!(msg.code, 0x1000);
        match &msg.detail {
            ErrorDetail::Unavailable {
                consistency,
                required,
                alive,
            } => {
                assert_eq!(*consistency, Consistency::Quorum);
                assert_eq!(*required, 2);
                assert_eq!(*alive, 1);
            }
            _ => panic!("expected Unavailable detail"),
        }
    }

    #[test]
    fn already_exists_mapping() {
        let err = CassandraError::AlreadyExists {
            ks: "test_ks".to_string(),
            table: "users".to_string(),
        };
        let msg = error_to_message(&err);
        assert_eq!(msg.code, 0x2400);
    }

    #[test]
    fn unprepared_mapping() {
        let err = CassandraError::Unprepared(vec![1, 2, 3, 4]);
        let msg = error_to_message(&err);
        assert_eq!(msg.code, 0x2500);
    }

    #[test]
    fn all_error_codes_match_protocol() {
        // Verify each error variant maps to its expected protocol code.
        let cases: Vec<(CassandraError, i32)> = vec![
            (CassandraError::ServerError("x".into()), 0x0000),
            (CassandraError::ProtocolError("x".into()), 0x000A),
            (CassandraError::AuthenticationError("x".into()), 0x0100),
            (CassandraError::Overloaded, 0x1001),
            (CassandraError::IsBootstrapping, 0x1002),
            (CassandraError::TruncateError("x".into()), 0x1003),
            (
                CassandraError::ReadFailure {
                    consistency: "ONE".into(),
                    received: 0,
                    block_for: 1,
                    num_failures: 1,
                    data_present: false,
                },
                0x1300,
            ),
            (
                CassandraError::FunctionFailure {
                    keyspace: "ks".into(),
                    function: "f".into(),
                    arg_types: vec![],
                },
                0x1400,
            ),
            (
                CassandraError::WriteFailure {
                    consistency: "ONE".into(),
                    received: 0,
                    block_for: 1,
                    num_failures: 1,
                    write_type: "SIMPLE".into(),
                },
                0x1500,
            ),
            (CassandraError::CDCWriteFailure, 0x1600),
            (CassandraError::SyntaxError("x".into()), 0x2000),
            (CassandraError::Unauthorized("x".into()), 0x2100),
            (CassandraError::InvalidQuery("x".into()), 0x2200),
            (CassandraError::ConfigError("x".into()), 0x2300),
        ];

        for (err, expected_code) in cases {
            let msg = error_to_message(&err);
            assert_eq!(msg.code, expected_code, "code mismatch for {:?}", err);
        }
    }

    #[test]
    fn read_failure_mapping() {
        let err = CassandraError::ReadFailure {
            consistency: "QUORUM".to_string(),
            received: 1,
            block_for: 2,
            num_failures: 1,
            data_present: false,
        };
        let msg = error_to_message(&err);
        assert_eq!(msg.code, 0x1300);
        match &msg.detail {
            ErrorDetail::ReadFailure {
                consistency,
                received,
                block_for,
                num_failures,
                data_present,
            } => {
                assert_eq!(*consistency, Consistency::Quorum);
                assert_eq!(*received, 1);
                assert_eq!(*block_for, 2);
                assert_eq!(*num_failures, 1);
                assert!(!*data_present);
            }
            _ => panic!("expected ReadFailure detail"),
        }
    }

    #[test]
    fn write_failure_mapping() {
        let err = CassandraError::WriteFailure {
            consistency: "ALL".to_string(),
            received: 2,
            block_for: 3,
            num_failures: 1,
            write_type: "SIMPLE".to_string(),
        };
        let msg = error_to_message(&err);
        assert_eq!(msg.code, 0x1500);
        match &msg.detail {
            ErrorDetail::WriteFailure {
                consistency,
                received,
                block_for,
                num_failures,
                write_type,
            } => {
                assert_eq!(*consistency, Consistency::All);
                assert_eq!(*received, 2);
                assert_eq!(*block_for, 3);
                assert_eq!(*num_failures, 1);
                assert_eq!(write_type, "SIMPLE");
            }
            _ => panic!("expected WriteFailure detail"),
        }
    }

    #[test]
    fn function_failure_mapping() {
        let err = CassandraError::FunctionFailure {
            keyspace: "ks".to_string(),
            function: "my_func".to_string(),
            arg_types: vec!["int".to_string(), "text".to_string()],
        };
        let msg = error_to_message(&err);
        assert_eq!(msg.code, 0x1400);
        match &msg.detail {
            ErrorDetail::FunctionFailure {
                keyspace,
                function,
                arg_types,
            } => {
                assert_eq!(keyspace, "ks");
                assert_eq!(function, "my_func");
                assert_eq!(arg_types, &vec!["int".to_string(), "text".to_string()]);
            }
            _ => panic!("expected FunctionFailure detail"),
        }
    }

    #[test]
    fn cdc_write_failure_mapping() {
        let err = CassandraError::CDCWriteFailure;
        let msg = error_to_message(&err);
        assert_eq!(msg.code, 0x1600);
        assert!(matches!(msg.detail, ErrorDetail::None));
    }

    #[test]
    fn consistency_from_name() {
        assert_eq!(Consistency::from_name("QUORUM"), Some(Consistency::Quorum));
        assert_eq!(
            Consistency::from_name("local_quorum"),
            Some(Consistency::LocalQuorum)
        );
        assert_eq!(Consistency::from_name("INVALID"), None);
    }
}
