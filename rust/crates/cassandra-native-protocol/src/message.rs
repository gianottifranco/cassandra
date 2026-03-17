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

//! CQL protocol message definitions.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.transport.Message`
//! - `org.apache.cassandra.transport.messages.*`

use crate::types::Consistency;
use std::collections::HashMap;

/// Parsed request/response message.
#[derive(Debug, Clone)]
pub enum Message {
    // ── Requests (client → server) ──
    Startup(StartupMessage),
    Options,
    Query(QueryMessage),
    Prepare(PrepareMessage),
    Execute(ExecuteMessage),
    Batch(BatchMessage),
    Register(RegisterMessage),
    AuthResponse(AuthResponseMessage),

    // ── Responses (server → client) ──
    Ready,
    Authenticate(AuthenticateMessage),
    Supported(SupportedMessage),
    Result(ResultMessage),
    Event(EventMessage),
    Error(ErrorMessage),
    AuthChallenge(Vec<u8>),
    AuthSuccess(Option<Vec<u8>>),
}

// ─── Request message payloads ───────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct StartupMessage {
    pub options: HashMap<String, String>,
}

impl StartupMessage {
    pub fn cql_version(&self) -> Option<&str> {
        self.options.get("CQL_VERSION").map(|s| s.as_str())
    }

    pub fn compression(&self) -> Option<&str> {
        self.options.get("COMPRESSION").map(|s| s.as_str())
    }
}

#[derive(Debug, Clone)]
pub struct QueryMessage {
    pub query: String,
    pub params: QueryParams,
}

#[derive(Debug, Clone)]
pub struct QueryParams {
    pub consistency: Consistency,
    pub flags: u8,
    pub values: Vec<Option<Vec<u8>>>,
    pub page_size: Option<i32>,
    pub paging_state: Option<Vec<u8>>,
    pub serial_consistency: Option<Consistency>,
    pub timestamp: Option<i64>,
    pub keyspace: Option<String>,
    pub now_in_seconds: Option<i32>,
}

impl Default for QueryParams {
    fn default() -> Self {
        Self {
            consistency: Consistency::One,
            flags: 0,
            values: Vec::new(),
            page_size: None,
            paging_state: None,
            serial_consistency: None,
            timestamp: None,
            keyspace: None,
            now_in_seconds: None,
        }
    }
}

/// Query parameter flags.
pub mod query_flags {
    pub const VALUES: u8 = 0x01;
    pub const SKIP_METADATA: u8 = 0x02;
    pub const PAGE_SIZE: u8 = 0x04;
    pub const PAGING_STATE: u8 = 0x08;
    pub const SERIAL_CONSISTENCY: u8 = 0x10;
    pub const TIMESTAMP: u8 = 0x20;
    pub const NAMES_FOR_VALUES: u8 = 0x40;
    pub const KEYSPACE: u8 = 0x80; // v5+
}

#[derive(Debug, Clone)]
pub struct PrepareMessage {
    pub query: String,
    pub keyspace: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ExecuteMessage {
    pub id: Vec<u8>,
    pub result_metadata_id: Option<Vec<u8>>,
    pub params: QueryParams,
}

#[derive(Debug, Clone)]
pub struct BatchMessage {
    pub batch_type: BatchType,
    pub queries: Vec<BatchQuery>,
    pub consistency: Consistency,
    pub serial_consistency: Option<Consistency>,
    pub timestamp: Option<i64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BatchType {
    Logged = 0,
    Unlogged = 1,
    Counter = 2,
}

#[derive(Debug, Clone)]
pub struct BatchQuery {
    pub is_prepared: bool,
    /// Either the query string or the prepared statement ID.
    pub query_or_id: Vec<u8>,
    pub values: Vec<Option<Vec<u8>>>,
}

#[derive(Debug, Clone)]
pub struct RegisterMessage {
    pub event_types: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct AuthResponseMessage {
    pub token: Option<Vec<u8>>,
}

// ─── Response message payloads ──────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct AuthenticateMessage {
    pub authenticator: String,
}

#[derive(Debug, Clone)]
pub struct SupportedMessage {
    pub options: HashMap<String, Vec<String>>,
}

/// Result message variants (opcode 0x08).
#[derive(Debug, Clone)]
pub enum ResultMessage {
    /// Void result (successful mutation with no return).
    Void,
    /// Row data result.
    Rows(RowsResult),
    /// SET_KEYSPACE result.
    SetKeyspace(String),
    /// PREPARED result.
    Prepared(PreparedResult),
    /// SCHEMA_CHANGE result.
    SchemaChange(SchemaChange),
}

/// Result kind IDs from protocol spec.
impl ResultMessage {
    pub fn kind_id(&self) -> i32 {
        match self {
            ResultMessage::Void => 0x0001,
            ResultMessage::Rows(_) => 0x0002,
            ResultMessage::SetKeyspace(_) => 0x0003,
            ResultMessage::Prepared(_) => 0x0004,
            ResultMessage::SchemaChange(_) => 0x0005,
        }
    }
}

#[derive(Debug, Clone)]
pub struct RowsResult {
    pub metadata: RowsMetadata,
    pub rows_count: i32,
    pub rows: Vec<Vec<Option<Vec<u8>>>>,
}

#[derive(Debug, Clone)]
pub struct RowsMetadata {
    pub flags: i32,
    pub columns_count: i32,
    pub paging_state: Option<Vec<u8>>,
    pub new_metadata_id: Option<Vec<u8>>,
    pub global_table_spec: Option<(String, String)>,
    pub col_specs: Vec<ColumnSpec>,
}

/// Rows metadata flags.
pub mod rows_flags {
    pub const GLOBAL_TABLES_SPEC: i32 = 0x0001;
    pub const HAS_MORE_PAGES: i32 = 0x0002;
    pub const NO_METADATA: i32 = 0x0004;
    pub const METADATA_CHANGED: i32 = 0x0008;
}

#[derive(Debug, Clone)]
pub struct ColumnSpec {
    pub ksname: Option<String>,
    pub tablename: Option<String>,
    pub name: String,
    pub col_type: ColumnType,
}

/// Protocol-level column type descriptor.
#[derive(Debug, Clone)]
pub enum ColumnType {
    Custom(String),
    Ascii,
    Bigint,
    Blob,
    Boolean,
    Counter,
    Decimal,
    Double,
    Float,
    Int,
    Timestamp,
    Uuid,
    Varchar,
    Varint,
    Timeuuid,
    Inet,
    Date,
    Time,
    Smallint,
    Tinyint,
    Duration,
    List(Box<ColumnType>),
    Map(Box<ColumnType>, Box<ColumnType>),
    Set(Box<ColumnType>),
    Udt {
        ks: String,
        name: String,
        fields: Vec<(String, ColumnType)>,
    },
    Tuple(Vec<ColumnType>),
    /// Fixed-dimension vector: `vector<T, n>`.
    Vector(Box<ColumnType>, u32),
}

impl ColumnType {
    /// Protocol option ID for this type.
    pub fn id(&self) -> u16 {
        match self {
            ColumnType::Custom(_) => 0x0000,
            ColumnType::Ascii => 0x0001,
            ColumnType::Bigint => 0x0002,
            ColumnType::Blob => 0x0003,
            ColumnType::Boolean => 0x0004,
            ColumnType::Counter => 0x0005,
            ColumnType::Decimal => 0x0006,
            ColumnType::Double => 0x0007,
            ColumnType::Float => 0x0008,
            ColumnType::Int => 0x0009,
            ColumnType::Timestamp => 0x000B,
            ColumnType::Uuid => 0x000C,
            ColumnType::Varchar => 0x000D,
            ColumnType::Varint => 0x000E,
            ColumnType::Timeuuid => 0x000F,
            ColumnType::Inet => 0x0010,
            ColumnType::Date => 0x0011,
            ColumnType::Time => 0x0012,
            ColumnType::Smallint => 0x0013,
            ColumnType::Tinyint => 0x0014,
            ColumnType::Duration => 0x0015,
            ColumnType::List(_) => 0x0020,
            ColumnType::Map(_, _) => 0x0021,
            ColumnType::Set(_) => 0x0022,
            ColumnType::Udt { .. } => 0x0030,
            ColumnType::Tuple(_) => 0x0031,
            ColumnType::Vector(_, _) => 0x0032,
        }
    }

    /// Convert from CqlType to protocol ColumnType.
    pub fn from_cql_type(ct: &cassandra_types::CqlType) -> Self {
        use cassandra_types::CqlType;
        match ct {
            CqlType::Ascii => ColumnType::Ascii,
            CqlType::Bigint => ColumnType::Bigint,
            CqlType::Blob => ColumnType::Blob,
            CqlType::Boolean => ColumnType::Boolean,
            CqlType::Counter => ColumnType::Counter,
            CqlType::Decimal => ColumnType::Decimal,
            CqlType::Double => ColumnType::Double,
            CqlType::Float => ColumnType::Float,
            CqlType::Int => ColumnType::Int,
            CqlType::Timestamp => ColumnType::Timestamp,
            CqlType::Uuid => ColumnType::Uuid,
            CqlType::Varchar => ColumnType::Varchar,
            CqlType::Varint => ColumnType::Varint,
            CqlType::Timeuuid => ColumnType::Timeuuid,
            CqlType::Inet => ColumnType::Inet,
            CqlType::Date => ColumnType::Date,
            CqlType::Time => ColumnType::Time,
            CqlType::Smallint => ColumnType::Smallint,
            CqlType::Tinyint => ColumnType::Tinyint,
            CqlType::Duration => ColumnType::Duration,
            CqlType::Empty => ColumnType::Blob,
            CqlType::List(inner, _) => ColumnType::List(Box::new(Self::from_cql_type(inner))),
            CqlType::Set(inner, _) => ColumnType::Set(Box::new(Self::from_cql_type(inner))),
            CqlType::Map(k, v, _) => ColumnType::Map(
                Box::new(Self::from_cql_type(k)),
                Box::new(Self::from_cql_type(v)),
            ),
            CqlType::Tuple(types) => {
                ColumnType::Tuple(types.iter().map(Self::from_cql_type).collect())
            }
            CqlType::Udt {
                keyspace,
                name,
                field_names,
                field_types,
                ..
            } => ColumnType::Udt {
                ks: keyspace.clone(),
                name: name.clone(),
                fields: field_names
                    .iter()
                    .zip(field_types.iter())
                    .map(|(n, t)| (n.clone(), Self::from_cql_type(t)))
                    .collect(),
            },
            CqlType::Reversed(inner) => Self::from_cql_type(inner),
            CqlType::Vector(inner, dims) => {
                ColumnType::Vector(Box::new(Self::from_cql_type(inner)), *dims)
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct PreparedResult {
    pub id: Vec<u8>,
    pub result_metadata_id: Option<Vec<u8>>,
    pub bind_metadata: RowsMetadata,
    pub result_metadata: RowsMetadata,
}

#[derive(Debug, Clone)]
pub struct SchemaChange {
    pub change_type: String,
    pub target: String,
    pub keyspace: String,
    pub name: Option<String>,
    pub arg_types: Option<Vec<String>>,
}

/// Event message types.
#[derive(Debug, Clone)]
pub enum EventMessage {
    TopologyChange {
        change: String,
        addr: (std::net::IpAddr, u32),
    },
    StatusChange {
        change: String,
        addr: (std::net::IpAddr, u32),
    },
    SchemaChange(SchemaChange),
}

/// Error response.
#[derive(Debug, Clone)]
pub struct ErrorMessage {
    pub code: i32,
    pub message: String,
    pub detail: ErrorDetail,
}

/// Error-specific additional fields.
#[derive(Debug, Clone)]
pub enum ErrorDetail {
    None,
    Unavailable {
        consistency: Consistency,
        required: i32,
        alive: i32,
    },
    WriteTimeout {
        consistency: Consistency,
        received: i32,
        block_for: i32,
        write_type: String,
    },
    ReadTimeout {
        consistency: Consistency,
        received: i32,
        block_for: i32,
        data_present: bool,
    },
    ReadFailure {
        consistency: Consistency,
        received: i32,
        block_for: i32,
        num_failures: i32,
        data_present: bool,
    },
    WriteFailure {
        consistency: Consistency,
        received: i32,
        block_for: i32,
        num_failures: i32,
        write_type: String,
    },
    FunctionFailure {
        keyspace: String,
        function: String,
        arg_types: Vec<String>,
    },
    AlreadyExists {
        keyspace: String,
        table: String,
    },
    Unprepared {
        id: Vec<u8>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn result_kind_ids() {
        assert_eq!(ResultMessage::Void.kind_id(), 0x0001);
        assert_eq!(ResultMessage::SetKeyspace("x".into()).kind_id(), 0x0003);
    }

    #[test]
    fn column_type_ids() {
        assert_eq!(ColumnType::Ascii.id(), 0x0001);
        assert_eq!(ColumnType::Int.id(), 0x0009);
        assert_eq!(ColumnType::Varchar.id(), 0x000D);
        assert_eq!(ColumnType::List(Box::new(ColumnType::Int)).id(), 0x0020);
    }

    #[test]
    fn column_type_from_cql_type() {
        use cassandra_types::CqlType;
        let ct = ColumnType::from_cql_type(&CqlType::Int);
        assert_eq!(ct.id(), 0x0009);

        let list_ct = ColumnType::from_cql_type(&CqlType::List(Box::new(CqlType::Varchar), false));
        assert_eq!(list_ct.id(), 0x0020);
    }
}
