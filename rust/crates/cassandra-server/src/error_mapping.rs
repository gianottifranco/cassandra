// Licensed under Apache License, Version 2.0.

//! Mapping from `ExecutorError` to `CassandraError` and protocol error frames.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.cql3.QueryProcessor` (error handling paths)

use cassandra_common::CassandraError;
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

#[cfg(test)]
mod tests {
    use super::*;
    use cassandra_native_protocol::frame::Opcode;
    use cassandra_native_protocol::types;

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
}
