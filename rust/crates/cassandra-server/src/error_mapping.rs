// Licensed under Apache License, Version 2.0.

//! Mapping from `ExecutorError` and `WriteError` to `CassandraError` and
//! protocol error frames.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.cql3.QueryProcessor` (error handling paths)
//! - `org.apache.cassandra.transport.messages.ErrorMessage`

use cassandra_common::CassandraError;
use cassandra_coordinator::{ReadError, WriteError};
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

/// Convert any `CassandraError` to a protocol error frame.
///
/// Shared helper used by `executor_error_to_error_frame`, `write_error_to_error_frame`,
/// and `read_error_to_error_frame`.
pub fn cassandra_error_to_error_frame(err: CassandraError, version: u8, stream_id: i16) -> Frame {
    let error_msg = error_codes::error_to_message(&err);
    response::encode_response(&Message::Error(error_msg), version, stream_id)
}

/// Convert an `ExecutorError` to a protocol error frame using proper error codes.
pub fn executor_error_to_error_frame(err: ExecutorError, version: u8, stream_id: i16) -> Frame {
    cassandra_error_to_error_frame(err.into(), version, stream_id)
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
        WriteError::MutationTooLarge { size, limit } => CassandraError::InvalidQuery(format!(
            "Mutation of {} bytes exceeds limit of {} bytes",
            size, limit
        )),
        WriteError::Internal(msg) => CassandraError::ServerError(msg),
    }
}

/// Convert a `WriteError` to a protocol error frame (WU-03).
pub fn write_error_to_error_frame(err: WriteError, version: u8, stream_id: i16) -> Frame {
    cassandra_error_to_error_frame(write_error_to_cassandra_error(err), version, stream_id)
}

// ─── ReadError → CassandraError ──────────────────────────────────

/// Convert a `ReadError` to a `CassandraError`.
///
/// Maps all `ReadError` variants to protocol-level error types:
/// - Timeout → ReadTimeout (0x1200)
/// - ReadFailure → ReadFailure (0x1300)
/// - Unavailable → Unavailable (0x1000)
/// - TombstoneOverwhelming → ReadFailure (0x1300)
/// - DigestMismatch → ServerError (0x0000)
/// - QueryCancelled → ServerError (0x0000)
/// - CoordinatorBehind → ServerError (0x0000)
/// - Internal → ServerError (0x0000)
pub fn read_error_to_cassandra_error(err: ReadError) -> CassandraError {
    match err {
        ReadError::Timeout {
            cl,
            required,
            received,
            data_present,
        } => CassandraError::ReadTimeout {
            consistency: cl.to_string(),
            received: received as i32,
            block_for: required as i32,
            data_present,
        },
        ReadError::Unavailable {
            cl,
            required,
            alive,
        } => CassandraError::Unavailable {
            consistency: cl.to_string(),
            required: required as i32,
            alive: alive as i32,
        },
        ReadError::ReadFailure {
            cl,
            required,
            received,
            num_failures,
            data_present,
            failure_map: _,
        } => CassandraError::ReadFailure {
            consistency: cl.to_string(),
            received: received as i32,
            block_for: required as i32,
            num_failures: num_failures as i32,
            data_present,
        },
        ReadError::TombstoneOverwhelming { count, threshold } => {
            CassandraError::ServerError(format!(
                "Scanned over {count} tombstones during query (limit: {threshold}); query aborted"
            ))
        }
        ReadError::DigestMismatch { .. } => {
            CassandraError::ServerError("Digest mismatch during read".to_string())
        }
        ReadError::QueryCancelled => CassandraError::ServerError("Query cancelled".to_string()),
        ReadError::CoordinatorBehind(msg) => {
            CassandraError::ServerError(format!("Coordinator behind: {msg}"))
        }
        ReadError::Internal(msg) => CassandraError::ServerError(msg),
    }
}

/// Convert a `ReadError` to a protocol error frame.
pub fn read_error_to_error_frame(err: ReadError, version: u8, stream_id: i16) -> Frame {
    cassandra_error_to_error_frame(read_error_to_cassandra_error(err), version, stream_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use cassandra_coordinator::{ConsistencyLevel, ReadError, WriteType};
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
        let ce = write_error_to_cassandra_error(WriteError::SchemaDisagreement(
            "table being altered".into(),
        ));
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

    // ── ReadError → CassandraError Tests ────────────────────────────

    #[test]
    fn read_timeout_maps_to_read_timeout() {
        let err = ReadError::Timeout {
            cl: ConsistencyLevel::Quorum,
            required: 2,
            received: 1,
            data_present: false,
        };
        let ce = read_error_to_cassandra_error(err);
        assert_eq!(ce.error_code(), Some(0x1200));
        match ce {
            CassandraError::ReadTimeout {
                consistency,
                received,
                block_for,
                data_present,
            } => {
                assert_eq!(consistency, "QUORUM");
                assert_eq!(received, 1);
                assert_eq!(block_for, 2);
                assert!(!data_present);
            }
            other => panic!("Expected ReadTimeout, got {other:?}"),
        }
    }

    #[test]
    fn read_unavailable_maps_to_unavailable() {
        let err = ReadError::Unavailable {
            cl: ConsistencyLevel::All,
            required: 3,
            alive: 1,
        };
        let ce = read_error_to_cassandra_error(err);
        assert_eq!(ce.error_code(), Some(0x1000));
    }

    #[test]
    fn read_failure_maps_to_read_failure() {
        let err = ReadError::ReadFailure {
            cl: ConsistencyLevel::Quorum,
            required: 2,
            received: 1,
            num_failures: 1,
            data_present: true,
            failure_map: HashMap::new(),
        };
        let ce = read_error_to_cassandra_error(err);
        assert_eq!(ce.error_code(), Some(0x1300));
    }

    #[test]
    fn tombstone_overwhelming_maps_to_server_error() {
        let err = ReadError::TombstoneOverwhelming {
            count: 200_000,
            threshold: 100_000,
        };
        let ce = read_error_to_cassandra_error(err);
        assert_eq!(ce.error_code(), Some(0x0000));
        match ce {
            CassandraError::ServerError(msg) => {
                assert!(msg.contains("200000"));
                assert!(msg.contains("100000"));
            }
            other => panic!("Expected ServerError, got {other:?}"),
        }
    }

    #[test]
    fn digest_mismatch_maps_to_server_error() {
        let err = ReadError::DigestMismatch {
            replicas_mismatched: 2,
        };
        let ce = read_error_to_cassandra_error(err);
        assert_eq!(ce.error_code(), Some(0x0000));
    }

    #[test]
    fn query_cancelled_maps_to_server_error() {
        let ce = read_error_to_cassandra_error(ReadError::QueryCancelled);
        assert_eq!(ce.error_code(), Some(0x0000));
    }

    #[test]
    fn coordinator_behind_maps_to_server_error() {
        let ce = read_error_to_cassandra_error(ReadError::CoordinatorBehind("stale".into()));
        assert_eq!(ce.error_code(), Some(0x0000));
    }

    #[test]
    fn read_internal_maps_to_server_error() {
        let ce = read_error_to_cassandra_error(ReadError::Internal("unexpected".into()));
        assert_eq!(ce.error_code(), Some(0x0000));
    }

    #[test]
    fn read_error_to_frame_test() {
        let err = ReadError::Timeout {
            cl: ConsistencyLevel::Quorum,
            required: 2,
            received: 1,
            data_present: false,
        };
        let frame = read_error_to_error_frame(err, 4, 7);
        assert_eq!(frame.header.opcode, Opcode::Error);
        assert_eq!(frame.header.stream_id, 7);

        let mut body: &[u8] = &frame.body;
        let code = types::read_int(&mut body).unwrap();
        assert_eq!(code, 0x1200); // READ_TIMEOUT
    }
}
