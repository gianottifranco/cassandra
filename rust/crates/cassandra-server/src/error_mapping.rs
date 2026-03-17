// Licensed under Apache License, Version 2.0.

//! Mapping from `ExecutorError` and `WriteError` to `CassandraError` and
//! protocol error frames.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.cql3.QueryProcessor` (error handling paths)
//! - `org.apache.cassandra.transport.messages.ErrorMessage`

use cassandra_common::CassandraError;
use cassandra_coordinator::WriteError;
use cassandra_native_protocol::error_codes;
use cassandra_native_protocol::frame::Frame;
use cassandra_native_protocol::message::Message;
use cassandra_native_protocol::response;

use crate::executor::ExecutorError;

impl From<ExecutorError> for CassandraError {
    fn from(err: ExecutorError) -> Self {
        match err {
            ExecutorError::InvalidQuery(msg) => CassandraError::InvalidQuery(msg),
            ExecutorError::KeyspaceNotFound(ks) => {
                CassandraError::InvalidQuery(format!("Keyspace '{}' not found", ks))
            }
            ExecutorError::TableNotFound(ks, table) => {
                CassandraError::InvalidQuery(format!("Table '{}.{}' not found", ks, table))
            }
            ExecutorError::StorageError(msg) => CassandraError::ServerError(msg),
            ExecutorError::SchemaError(msg) => CassandraError::ConfigError(msg),
        }
    }
}

/// Convert an `ExecutorError` to a protocol error frame using proper error codes.
pub fn executor_error_to_error_frame(
    err: ExecutorError,
    version: u8,
    stream_id: i16,
) -> Frame {
    let cassandra_err: CassandraError = err.into();
    let error_msg = error_codes::error_to_message(&cassandra_err);
    response::encode_response(
        &Message::Error(error_msg),
        version,
        stream_id,
    )
}

// ─── WriteError → CassandraError (WU-03) ────────────────────────

/// Convert a `WriteError` to a `CassandraError`.
///
/// Maps all `WriteError` variants to protocol-level error types:
/// - Timeout → WriteTimeout (0x1100)
/// - Unavailable → Unavailable (0x1000)
/// - WriteFailure → WriteFailure (0x1500) with failure_map
/// - Overloaded → Overloaded (0x1001)
/// - IsBootstrapping → IsBootstrapping (0x1002)
/// - TruncateInProgress → TruncateError (0x1003)
/// - SchemaDisagreement → InvalidQuery (0x2200)
/// - MutationTooLarge → InvalidQuery (0x2200)
/// - Internal → ServerError (0x0000)
pub fn write_error_to_cassandra_error(err: WriteError) -> CassandraError {
    match err {
        WriteError::Timeout {
            cl,
            write_type,
            required,
            received,
            block_for: _,
        } => CassandraError::WriteTimeout {
            consistency: cl.to_string(),
            received: received as i32,
            block_for: required as i32,
            write_type: write_type.protocol_name().to_string(),
        },
        WriteError::Unavailable {
            cl,
            required,
            alive,
        } => CassandraError::Unavailable {
            consistency: cl.to_string(),
            required: required as i32,
            alive: alive as i32,
        },
        WriteError::WriteFailure {
            cl,
            write_type,
            required,
            received,
            block_for: _,
            num_failures,
            failure_map: _,
        } => CassandraError::WriteFailure {
            consistency: cl.to_string(),
            received: received as i32,
            block_for: required as i32,
            num_failures: num_failures as i32,
            write_type: write_type.protocol_name().to_string(),
        },
        WriteError::Overloaded => CassandraError::Overloaded,
        WriteError::IsBootstrapping => CassandraError::IsBootstrapping,
        WriteError::TruncateInProgress => {
            CassandraError::TruncateError("Cannot write during truncation".to_string())
        }
        WriteError::SchemaDisagreement(msg) => CassandraError::InvalidQuery(msg),
        WriteError::MutationTooLarge { size, limit } => CassandraError::InvalidQuery(
            format!("Mutation of {} bytes exceeds limit of {} bytes", size, limit),
        ),
        WriteError::Internal(msg) => CassandraError::ServerError(msg),
    }
}

/// Convert a `WriteError` to a protocol error frame (WU-03).
///
/// Similar to `executor_error_to_error_frame()` but for write-path errors.
pub fn write_error_to_error_frame(
    err: WriteError,
    version: u8,
    stream_id: i16,
) -> Frame {
    let cassandra_err = write_error_to_cassandra_error(err);
    let error_msg = error_codes::error_to_message(&cassandra_err);
    response::encode_response(
        &Message::Error(error_msg),
        version,
        stream_id,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use cassandra_coordinator::{ConsistencyLevel, WriteType};
    use cassandra_native_protocol::frame::Opcode;
    use cassandra_native_protocol::types;
    use std::collections::HashMap;

    #[test]
    fn invalid_query_maps_to_invalid_query() {
        let err = ExecutorError::InvalidQuery("bad query".into());
        let ce: CassandraError = err.into();
        assert!(matches!(ce, CassandraError::InvalidQuery(_)));
        assert_eq!(ce.error_code(), Some(0x2200));
    }

    #[test]
    fn keyspace_not_found_maps_to_invalid_query() {
        let err = ExecutorError::KeyspaceNotFound("missing_ks".into());
        let ce: CassandraError = err.into();
        assert!(matches!(ce, CassandraError::InvalidQuery(_)));
        assert_eq!(ce.error_code(), Some(0x2200));
    }

    #[test]
    fn table_not_found_maps_to_invalid_query() {
        let err = ExecutorError::TableNotFound("ks".into(), "tbl".into());
        let ce: CassandraError = err.into();
        assert!(matches!(ce, CassandraError::InvalidQuery(_)));
        assert_eq!(ce.error_code(), Some(0x2200));
    }

    #[test]
    fn storage_error_maps_to_server_error() {
        let err = ExecutorError::StorageError("disk full".into());
        let ce: CassandraError = err.into();
        assert!(matches!(ce, CassandraError::ServerError(_)));
        assert_eq!(ce.error_code(), Some(0x0000));
    }

    #[test]
    fn schema_error_maps_to_config_error() {
        let err = ExecutorError::SchemaError("bad schema".into());
        let ce: CassandraError = err.into();
        assert!(matches!(ce, CassandraError::ConfigError(_)));
        assert_eq!(ce.error_code(), Some(0x2300));
    }

    #[test]
    fn executor_error_to_frame() {
        let err = ExecutorError::InvalidQuery("test error".into());
        let frame = executor_error_to_error_frame(err, 4, 1);
        assert_eq!(frame.header.opcode, Opcode::Error);
        assert_eq!(frame.header.stream_id, 1);

        let mut body: &[u8] = &frame.body;
        let code = types::read_int(&mut body).unwrap();
        assert_eq!(code, 0x2200);
    }

    #[test]
    fn storage_error_to_frame() {
        let err = ExecutorError::StorageError("io failure".into());
        let frame = executor_error_to_error_frame(err, 4, 5);

        let mut body: &[u8] = &frame.body;
        let code = types::read_int(&mut body).unwrap();
        assert_eq!(code, 0x0000); // SERVER_ERROR
    }

    // ── WU-03: WriteError → CassandraError Tests ──────────────────

    #[test]
    fn write_timeout_maps_to_write_timeout() {
        let err = WriteError::Timeout {
            cl: ConsistencyLevel::Quorum,
            write_type: WriteType::Simple,
            required: 2,
            received: 1,
            block_for: 2,
        };
        let ce = write_error_to_cassandra_error(err);
        assert_eq!(ce.error_code(), Some(0x1100));
        match ce {
            CassandraError::WriteTimeout {
                consistency,
                received,
                block_for,
                write_type,
            } => {
                assert_eq!(consistency, "QUORUM");
                assert_eq!(received, 1);
                assert_eq!(block_for, 2);
                assert_eq!(write_type, "SIMPLE");
            }
            other => panic!("Expected WriteTimeout, got {other:?}"),
        }
    }

    #[test]
    fn write_unavailable_maps_to_unavailable() {
        let err = WriteError::Unavailable {
            cl: ConsistencyLevel::All,
            required: 3,
            alive: 1,
        };
        let ce = write_error_to_cassandra_error(err);
        assert_eq!(ce.error_code(), Some(0x1000));
    }

    #[test]
    fn write_failure_maps_to_write_failure() {
        let err = WriteError::WriteFailure {
            cl: ConsistencyLevel::Quorum,
            write_type: WriteType::Batch,
            required: 2,
            received: 1,
            block_for: 2,
            num_failures: 1,
            failure_map: HashMap::new(),
        };
        let ce = write_error_to_cassandra_error(err);
        assert_eq!(ce.error_code(), Some(0x1500));
    }

    #[test]
    fn write_overloaded_maps_to_overloaded() {
        let ce = write_error_to_cassandra_error(WriteError::Overloaded);
        assert_eq!(ce.error_code(), Some(0x1001));
    }

    #[test]
    fn write_is_bootstrapping_maps_to_is_bootstrapping() {
        let ce = write_error_to_cassandra_error(WriteError::IsBootstrapping);
        assert_eq!(ce.error_code(), Some(0x1002));
    }

    #[test]
    fn write_truncate_in_progress_maps_to_truncate_error() {
        let ce = write_error_to_cassandra_error(WriteError::TruncateInProgress);
        assert_eq!(ce.error_code(), Some(0x1003));
    }

    #[test]
    fn write_schema_disagreement_maps_to_invalid_query() {
        let ce = write_error_to_cassandra_error(
            WriteError::SchemaDisagreement("table being altered".into()),
        );
        assert_eq!(ce.error_code(), Some(0x2200));
    }

    #[test]
    fn write_mutation_too_large_maps_to_invalid_query() {
        let ce = write_error_to_cassandra_error(WriteError::MutationTooLarge {
            size: 100,
            limit: 50,
        });
        assert_eq!(ce.error_code(), Some(0x2200));
    }

    #[test]
    fn write_internal_maps_to_server_error() {
        let ce = write_error_to_cassandra_error(WriteError::Internal("unexpected".into()));
        assert_eq!(ce.error_code(), Some(0x0000));
    }

    #[test]
    fn write_error_to_frame() {
        let err = WriteError::Timeout {
            cl: ConsistencyLevel::Quorum,
            write_type: WriteType::Simple,
            required: 2,
            received: 1,
            block_for: 2,
        };
        let frame = write_error_to_error_frame(err, 4, 3);
        assert_eq!(frame.header.opcode, Opcode::Error);
        assert_eq!(frame.header.stream_id, 3);

        let mut body: &[u8] = &frame.body;
        let code = types::read_int(&mut body).unwrap();
        assert_eq!(code, 0x1100); // WRITE_TIMEOUT
    }
}
