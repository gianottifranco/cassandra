// Licensed under Apache License, Version 2.0.

//! Higher-level query options wrapping protocol `QueryParams`.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.cql3.QueryOptions`

use cassandra_native_protocol::message::{QueryParams, query_flags};
use cassandra_native_protocol::types::Consistency;

/// CQL-semantic query options built from protocol-level `QueryParams`.
#[derive(Debug, Clone)]
pub struct QueryOptions {
    pub consistency: Consistency,
    pub serial_consistency: Option<Consistency>,
    pub page_size: Option<i32>,
    pub paging_state: Option<Vec<u8>>,
    pub timestamp: Option<i64>,
    pub now_in_seconds: Option<i32>,
    pub keyspace: Option<String>,
    pub skip_metadata: bool,
    pub values: Vec<Option<Vec<u8>>>,
    pub protocol_version: u8,
}

impl QueryOptions {
    /// Create options for internal system queries (no paging, LOCAL_ONE).
    pub fn for_internal_calls() -> Self {
        Self {
            consistency: Consistency::LocalOne,
            serial_consistency: None,
            page_size: None,
            paging_state: None,
            timestamp: None,
            now_in_seconds: None,
            keyspace: None,
            skip_metadata: false,
            values: Vec::new(),
            protocol_version: 4,
        }
    }

    /// Builder: set consistency level.
    pub fn with_consistency(mut self, c: Consistency) -> Self {
        self.consistency = c;
        self
    }

    /// Builder: set page size.
    pub fn with_page_size(mut self, size: i32) -> Self {
        self.page_size = Some(size);
        self
    }

    /// Builder: set paging state.
    pub fn with_paging_state(mut self, state: Vec<u8>) -> Self {
        self.paging_state = Some(state);
        self
    }

    /// Builder: set timestamp.
    pub fn with_timestamp(mut self, ts: i64) -> Self {
        self.timestamp = Some(ts);
        self
    }

    /// Builder: set keyspace.
    pub fn with_keyspace(mut self, ks: String) -> Self {
        self.keyspace = Some(ks);
        self
    }

    /// Builder: set values.
    pub fn with_values(mut self, values: Vec<Option<Vec<u8>>>) -> Self {
        self.values = values;
        self
    }

    /// Builder: set protocol version.
    pub fn with_protocol_version(mut self, version: u8) -> Self {
        self.protocol_version = version;
        self
    }
}

impl From<&QueryParams> for QueryOptions {
    fn from(params: &QueryParams) -> Self {
        Self {
            consistency: params.consistency,
            serial_consistency: params.serial_consistency,
            page_size: params.page_size,
            paging_state: params.paging_state.clone(),
            timestamp: params.timestamp,
            now_in_seconds: params.now_in_seconds,
            keyspace: params.keyspace.clone(),
            skip_metadata: params.flags & query_flags::SKIP_METADATA != 0,
            values: params.values.clone(),
            protocol_version: 4, // Default; caller should override.
        }
    }
}

impl From<QueryParams> for QueryOptions {
    fn from(params: QueryParams) -> Self {
        Self::from(&params)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_default_query_params() {
        let params = QueryParams::default();
        let opts = QueryOptions::from(&params);
        assert_eq!(opts.consistency, Consistency::One);
        assert!(opts.serial_consistency.is_none());
        assert!(opts.page_size.is_none());
        assert!(opts.paging_state.is_none());
        assert!(opts.timestamp.is_none());
        assert!(!opts.skip_metadata);
        assert!(opts.values.is_empty());
    }

    #[test]
    fn from_query_params_with_flags() {
        let params = QueryParams {
            consistency: Consistency::Quorum,
            flags: query_flags::SKIP_METADATA | query_flags::PAGE_SIZE,
            page_size: Some(100),
            serial_consistency: Some(Consistency::Serial),
            timestamp: Some(12345),
            keyspace: Some("test_ks".to_string()),
            ..Default::default()
        };
        let opts = QueryOptions::from(&params);
        assert_eq!(opts.consistency, Consistency::Quorum);
        assert!(opts.skip_metadata);
        assert_eq!(opts.page_size, Some(100));
        assert_eq!(opts.serial_consistency, Some(Consistency::Serial));
        assert_eq!(opts.timestamp, Some(12345));
        assert_eq!(opts.keyspace.as_deref(), Some("test_ks"));
    }

    #[test]
    fn for_internal_calls() {
        let opts = QueryOptions::for_internal_calls();
        assert_eq!(opts.consistency, Consistency::LocalOne);
        assert!(!opts.skip_metadata);
        assert!(opts.page_size.is_none());
        assert!(opts.values.is_empty());
    }

    #[test]
    fn builder_pattern() {
        let opts = QueryOptions::for_internal_calls()
            .with_consistency(Consistency::All)
            .with_page_size(50)
            .with_timestamp(999)
            .with_keyspace("my_ks".to_string())
            .with_values(vec![Some(vec![1, 2, 3])])
            .with_protocol_version(5);

        assert_eq!(opts.consistency, Consistency::All);
        assert_eq!(opts.page_size, Some(50));
        assert_eq!(opts.timestamp, Some(999));
        assert_eq!(opts.keyspace.as_deref(), Some("my_ks"));
        assert_eq!(opts.values.len(), 1);
        assert_eq!(opts.protocol_version, 5);
    }

    #[test]
    fn with_paging_state() {
        let opts = QueryOptions::for_internal_calls().with_paging_state(vec![0xDE, 0xAD]);
        assert_eq!(opts.paging_state, Some(vec![0xDE, 0xAD]));
    }
}
