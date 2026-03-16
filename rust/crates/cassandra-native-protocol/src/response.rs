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

//! Response message encoders (server → client).
//!
//! ## Java Oracle
//! - `org.apache.cassandra.transport.messages.ResultMessage`
//! - `org.apache.cassandra.transport.messages.ErrorMessage`

use crate::frame::{self, Frame, Opcode};
use crate::message::*;
use crate::types;
use bytes::BytesMut;

/// Encode a response Message into a Frame.
pub fn encode_response(msg: &Message, version: u8, stream_id: i16) -> Frame {
    let mut body = BytesMut::with_capacity(256);

    let opcode = match msg {
        Message::Ready => {
            // Empty body.
            Opcode::Ready
        }
        Message::Authenticate(a) => {
            types::write_string(&mut body, &a.authenticator);
            Opcode::Authenticate
        }
        Message::Supported(s) => {
            types::write_string_multimap(&mut body, &s.options);
            Opcode::Supported
        }
        Message::Result(r) => {
            encode_result(r, &mut body);
            Opcode::Result
        }
        Message::Error(e) => {
            encode_error(e, &mut body);
            Opcode::Error
        }
        Message::Event(ev) => {
            encode_event(ev, &mut body);
            Opcode::Event
        }
        Message::AuthChallenge(token) => {
            types::write_bytes_opt(&mut body, Some(token));
            Opcode::AuthChallenge
        }
        Message::AuthSuccess(token) => {
            types::write_bytes_opt(&mut body, token.as_deref());
            Opcode::AuthSuccess
        }
        _ => {
            // Request messages should not be encoded as responses.
            // Return an ERROR frame for this case.
            types::write_int(&mut body, 0x0000); // SERVER_ERROR
            types::write_string(
                &mut body,
                "Internal: attempted to encode request as response",
            );
            Opcode::Error
        }
    };

    frame::response_frame(version, stream_id, opcode, 0, body.freeze())
}

fn encode_result(result: &ResultMessage, buf: &mut BytesMut) {
    types::write_int(buf, result.kind_id());

    match result {
        ResultMessage::Void => {}
        ResultMessage::Rows(rows) => {
            encode_rows_metadata(&rows.metadata, buf);
            types::write_int(buf, rows.rows_count);
            for row in &rows.rows {
                for cell in row {
                    types::write_bytes_opt(buf, cell.as_deref());
                }
            }
        }
        ResultMessage::SetKeyspace(ks) => {
            types::write_string(buf, ks);
        }
        ResultMessage::Prepared(p) => {
            types::write_short_bytes(buf, &p.id);
            // v4: bind_metadata then result_metadata
            encode_rows_metadata(&p.bind_metadata, buf);
            encode_rows_metadata(&p.result_metadata, buf);
        }
        ResultMessage::SchemaChange(sc) => {
            encode_schema_change(sc, buf);
        }
    }
}

fn encode_rows_metadata(meta: &RowsMetadata, buf: &mut BytesMut) {
    types::write_int(buf, meta.flags);
    types::write_int(buf, meta.columns_count);

    if meta.flags & rows_flags::HAS_MORE_PAGES != 0 {
        types::write_bytes_opt(buf, meta.paging_state.as_deref());
    }

    if meta.flags & rows_flags::NO_METADATA != 0 {
        return;
    }

    if meta.flags & rows_flags::GLOBAL_TABLES_SPEC != 0 {
        if let Some((ks, tbl)) = &meta.global_table_spec {
            types::write_string(buf, ks);
            types::write_string(buf, tbl);
        }
    }

    for spec in &meta.col_specs {
        if meta.flags & rows_flags::GLOBAL_TABLES_SPEC == 0 {
            if let Some(ks) = &spec.ksname {
                types::write_string(buf, ks);
            }
            if let Some(tbl) = &spec.tablename {
                types::write_string(buf, tbl);
            }
        }
        types::write_string(buf, &spec.name);
        encode_column_type(&spec.col_type, buf);
    }
}

fn encode_column_type(ct: &ColumnType, buf: &mut BytesMut) {
    types::write_short(buf, ct.id());
    match ct {
        ColumnType::Custom(name) => {
            types::write_string(buf, name);
        }
        ColumnType::List(inner) | ColumnType::Set(inner) => {
            encode_column_type(inner, buf);
        }
        ColumnType::Map(key, val) => {
            encode_column_type(key, buf);
            encode_column_type(val, buf);
        }
        ColumnType::Udt { ks, name, fields } => {
            types::write_string(buf, ks);
            types::write_string(buf, name);
            types::write_short(buf, fields.len() as u16);
            for (fname, ftype) in fields {
                types::write_string(buf, fname);
                encode_column_type(ftype, buf);
            }
        }
        ColumnType::Tuple(types_vec) => {
            types::write_short(buf, types_vec.len() as u16);
            for t in types_vec {
                encode_column_type(t, buf);
            }
        }
        _ => {} // Simple types need only the ID, already written.
    }
}

fn encode_schema_change(sc: &SchemaChange, buf: &mut BytesMut) {
    types::write_string(buf, &sc.change_type);
    types::write_string(buf, &sc.target);
    types::write_string(buf, &sc.keyspace);
    match &sc.target.as_str() {
        &"KEYSPACE" => {} // No additional fields.
        _ => {
            if let Some(name) = &sc.name {
                types::write_string(buf, name);
            }
            if let Some(args) = &sc.arg_types {
                types::write_string_list(buf, args);
            }
        }
    }
}

fn encode_error(err: &ErrorMessage, buf: &mut BytesMut) {
    types::write_int(buf, err.code);
    types::write_string(buf, &err.message);

    match &err.detail {
        ErrorDetail::None => {}
        ErrorDetail::Unavailable {
            consistency,
            required,
            alive,
        } => {
            types::write_consistency(buf, *consistency);
            types::write_int(buf, *required);
            types::write_int(buf, *alive);
        }
        ErrorDetail::WriteTimeout {
            consistency,
            received,
            block_for,
            write_type,
        } => {
            types::write_consistency(buf, *consistency);
            types::write_int(buf, *received);
            types::write_int(buf, *block_for);
            types::write_string(buf, write_type);
        }
        ErrorDetail::ReadTimeout {
            consistency,
            received,
            block_for,
            data_present,
        } => {
            types::write_consistency(buf, *consistency);
            types::write_int(buf, *received);
            types::write_int(buf, *block_for);
            types::write_byte(buf, if *data_present { 1 } else { 0 });
        }
        ErrorDetail::AlreadyExists { keyspace, table } => {
            types::write_string(buf, keyspace);
            types::write_string(buf, table);
        }
        ErrorDetail::Unprepared { id } => {
            types::write_short_bytes(buf, id);
        }
    }
}

fn encode_event(event: &EventMessage, buf: &mut BytesMut) {
    match event {
        EventMessage::TopologyChange { change, addr } => {
            types::write_string(buf, "TOPOLOGY_CHANGE");
            types::write_string(buf, change);
            types::write_inet(buf, addr.0, addr.1);
        }
        EventMessage::StatusChange { change, addr } => {
            types::write_string(buf, "STATUS_CHANGE");
            types::write_string(buf, change);
            types::write_inet(buf, addr.0, addr.1);
        }
        EventMessage::SchemaChange(sc) => {
            types::write_string(buf, "SCHEMA_CHANGE");
            encode_schema_change(sc, buf);
        }
    }
}

// ─── Helper constructors ────────────────────────────────────────────────────

/// Build a READY response frame.
pub fn ready_frame(version: u8, stream_id: i16) -> Frame {
    encode_response(&Message::Ready, version, stream_id)
}

/// Build a SUPPORTED response frame.
pub fn supported_frame(version: u8, stream_id: i16) -> Frame {
    let mut options = std::collections::HashMap::new();
    options.insert("CQL_VERSION".to_string(), vec!["3.4.7".to_string()]);
    options.insert(
        "COMPRESSION".to_string(),
        vec!["lz4".to_string(), "snappy".to_string()],
    );
    options.insert(
        "PROTOCOL_VERSIONS".to_string(),
        vec!["4/v4".to_string(), "5/v5-beta".to_string()],
    );
    encode_response(
        &Message::Supported(SupportedMessage { options }),
        version,
        stream_id,
    )
}

/// Build an ERROR response frame.
pub fn error_frame(version: u8, stream_id: i16, code: i32, message: &str) -> Frame {
    encode_response(
        &Message::Error(ErrorMessage {
            code,
            message: message.to_string(),
            detail: ErrorDetail::None,
        }),
        version,
        stream_id,
    )
}

/// Build a RESULT:Void response frame.
pub fn void_result_frame(version: u8, stream_id: i16) -> Frame {
    encode_response(&Message::Result(ResultMessage::Void), version, stream_id)
}

/// Build a RESULT:SetKeyspace response frame.
pub fn set_keyspace_frame(version: u8, stream_id: i16, ks: &str) -> Frame {
    encode_response(
        &Message::Result(ResultMessage::SetKeyspace(ks.to_string())),
        version,
        stream_id,
    )
}

/// Build a RESULT:SchemaChange CREATED response.
pub fn schema_change_frame(
    version: u8,
    stream_id: i16,
    change_type: &str,
    target: &str,
    keyspace: &str,
    name: Option<&str>,
) -> Frame {
    encode_response(
        &Message::Result(ResultMessage::SchemaChange(SchemaChange {
            change_type: change_type.to_string(),
            target: target.to_string(),
            keyspace: keyspace.to_string(),
            name: name.map(|s| s.to_string()),
            arg_types: None,
        })),
        version,
        stream_id,
    )
}

/// Build a Rows result with column specs and cell data.
pub fn rows_result_frame(
    version: u8,
    stream_id: i16,
    keyspace: &str,
    table: &str,
    col_specs: Vec<ColumnSpec>,
    rows: Vec<Vec<Option<Vec<u8>>>>,
) -> Frame {
    let rows_count = rows.len() as i32;
    let columns_count = col_specs.len() as i32;
    let metadata = RowsMetadata {
        flags: rows_flags::GLOBAL_TABLES_SPEC,
        columns_count,
        paging_state: None,
        new_metadata_id: None,
        global_table_spec: Some((keyspace.to_string(), table.to_string())),
        col_specs,
    };
    encode_response(
        &Message::Result(ResultMessage::Rows(RowsResult {
            metadata,
            rows_count,
            rows,
        })),
        version,
        stream_id,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_ready() {
        let frame = ready_frame(0x04, 0);
        assert_eq!(frame.header.opcode, Opcode::Ready);
        assert!(frame.header.is_response());
        assert!(frame.body.is_empty());
    }

    #[test]
    fn encode_error() {
        let frame = error_frame(0x04, 1, 0x2000, "Syntax error");
        assert_eq!(frame.header.opcode, Opcode::Error);
        let mut body: &[u8] = &frame.body;
        let code = types::read_int(&mut body).unwrap();
        let msg = types::read_string(&mut body).unwrap();
        assert_eq!(code, 0x2000);
        assert_eq!(msg, "Syntax error");
    }

    #[test]
    fn encode_void_result() {
        let frame = void_result_frame(0x04, 0);
        assert_eq!(frame.header.opcode, Opcode::Result);
        let mut body: &[u8] = &frame.body;
        let kind = types::read_int(&mut body).unwrap();
        assert_eq!(kind, 0x0001); // Void
    }

    #[test]
    fn encode_set_keyspace() {
        let frame = set_keyspace_frame(0x04, 0, "test_ks");
        let mut body: &[u8] = &frame.body;
        let kind = types::read_int(&mut body).unwrap();
        assert_eq!(kind, 0x0003); // SetKeyspace
        let ks = types::read_string(&mut body).unwrap();
        assert_eq!(ks, "test_ks");
    }

    #[test]
    fn encode_supported() {
        let frame = supported_frame(0x04, 0);
        assert_eq!(frame.header.opcode, Opcode::Supported);
        let mut body: &[u8] = &frame.body;
        let map = types::read_string_multimap(&mut body).unwrap();
        assert!(map.contains_key("CQL_VERSION"));
        assert!(map.contains_key("COMPRESSION"));
    }

    #[test]
    fn encode_rows_result() {
        let specs = vec![
            ColumnSpec {
                ksname: None,
                tablename: None,
                name: "id".to_string(),
                col_type: ColumnType::Uuid,
            },
            ColumnSpec {
                ksname: None,
                tablename: None,
                name: "name".to_string(),
                col_type: ColumnType::Varchar,
            },
        ];
        let rows = vec![vec![Some(vec![0u8; 16]), Some(b"Alice".to_vec())]];
        let frame = rows_result_frame(0x04, 0, "ks", "users", specs, rows);
        assert_eq!(frame.header.opcode, Opcode::Result);

        // Verify kind is Rows (0x0002)
        let mut body: &[u8] = &frame.body;
        let kind = types::read_int(&mut body).unwrap();
        assert_eq!(kind, 0x0002);
    }

    #[test]
    fn encode_schema_change_keyspace() {
        let frame = schema_change_frame(0x04, 0, "CREATED", "KEYSPACE", "test_ks", None);
        assert_eq!(frame.header.opcode, Opcode::Result);
        let mut body: &[u8] = &frame.body;
        let kind = types::read_int(&mut body).unwrap();
        assert_eq!(kind, 0x0005); // SchemaChange
    }

    #[test]
    fn encode_schema_change_table() {
        let frame = schema_change_frame(0x04, 0, "CREATED", "TABLE", "ks", Some("users"));
        assert_eq!(frame.header.opcode, Opcode::Result);
    }
}
