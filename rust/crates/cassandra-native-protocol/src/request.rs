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

//! Request message decoders (client → server).
//!
//! ## Java Oracle
//! - `org.apache.cassandra.transport.messages.StartupMessage`
//! - `org.apache.cassandra.transport.messages.QueryMessage`
//! - `org.apache.cassandra.transport.messages.PrepareMessage`
//! - `org.apache.cassandra.transport.messages.ExecuteMessage`
//! - `org.apache.cassandra.transport.messages.BatchMessage`

use crate::frame::{Frame, Opcode, flags};
use crate::message::*;
use crate::types;
use std::collections::HashMap;
use std::io;

/// Decode a request frame's body into a Message.
///
/// If the frame has the CUSTOM_PAYLOAD flag set, the bytes-map is read from
/// the start of the body before the opcode-specific payload. The custom
/// payload is then attached to Query, Prepare, Execute, and Batch messages.
pub fn decode_request(frame: &Frame) -> io::Result<Message> {
    let mut body: &[u8] = &frame.body;

    // Read custom payload prefix if the frame flag is set.
    let custom_payload = if frame.header.flags & flags::CUSTOM_PAYLOAD != 0 {
        Some(types::read_bytes_map(&mut body)?)
    } else {
        None
    };

    let version = frame.header.protocol_version();
    let msg = match frame.header.opcode {
        Opcode::Startup => decode_startup(&mut body),
        Opcode::Options => Ok(Message::Options),
        Opcode::Query => decode_query(&mut body, version, custom_payload),
        Opcode::Prepare => decode_prepare(&mut body, version, custom_payload),
        Opcode::Execute => decode_execute(&mut body, version, custom_payload),
        Opcode::Batch => decode_batch(&mut body, version, custom_payload),
        Opcode::Register => decode_register(&mut body),
        Opcode::AuthResponse => decode_auth_response(&mut body),
        other => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unexpected request opcode: {:?}", other),
        )),
    }?;

    if !body.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "trailing bytes in {:?} request body: {} bytes",
                frame.header.opcode,
                body.len()
            ),
        ));
    }

    Ok(msg)
}

fn decode_startup(body: &mut &[u8]) -> io::Result<Message> {
    let options = types::read_string_map(body)?;
    Ok(Message::Startup(StartupMessage { options }))
}

fn decode_query(
    body: &mut &[u8],
    version: u8,
    custom_payload: Option<HashMap<String, Vec<u8>>>,
) -> io::Result<Message> {
    let query = types::read_long_string(body)?;
    let params = decode_query_params(body, version)?;
    Ok(Message::Query(QueryMessage {
        query,
        params,
        custom_payload,
    }))
}

fn decode_prepare(
    body: &mut &[u8],
    version: u8,
    custom_payload: Option<HashMap<String, Vec<u8>>>,
) -> io::Result<Message> {
    let query = types::read_long_string(body)?;
    let keyspace = if version >= 5 {
        let flags = types::read_int(body)?;
        if flags & 0x01 != 0 {
            Some(types::read_string(body)?)
        } else {
            None
        }
    } else {
        None
    };
    Ok(Message::Prepare(PrepareMessage {
        query,
        keyspace,
        custom_payload,
    }))
}

fn decode_execute(
    body: &mut &[u8],
    version: u8,
    custom_payload: Option<HashMap<String, Vec<u8>>>,
) -> io::Result<Message> {
    let id = types::read_short_bytes(body)?;
    let result_metadata_id = if version >= 5 {
        Some(types::read_short_bytes(body)?)
    } else {
        None
    };
    let params = decode_query_params(body, version)?;
    Ok(Message::Execute(ExecuteMessage {
        id,
        result_metadata_id,
        params,
        custom_payload,
    }))
}

fn decode_batch(
    body: &mut &[u8],
    version: u8,
    custom_payload: Option<HashMap<String, Vec<u8>>>,
) -> io::Result<Message> {
    let type_byte = types::read_byte(body)?;
    let batch_type = match type_byte {
        0 => BatchType::Logged,
        1 => BatchType::Unlogged,
        2 => BatchType::Counter,
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unknown batch type: {}", type_byte),
            ));
        }
    };

    let n = types::read_short(body)? as usize;
    let mut queries = Vec::with_capacity(n);
    for _ in 0..n {
        let kind = types::read_byte(body)?;
        let (is_prepared, query_or_id) = match kind {
            0 => (false, types::read_long_string(body)?.into_bytes()),
            1 => (true, types::read_short_bytes(body)?),
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "invalid query kind in BATCH message: expected 0 or 1, got {}",
                        kind
                    ),
                ));
            }
        };
        let n_values = types::read_short(body)? as usize;
        let mut values = Vec::with_capacity(n_values);
        for _ in 0..n_values {
            values.push(types::read_bytes(body)?);
        }
        queries.push(BatchQuery {
            is_prepared,
            query_or_id,
            values,
        });
    }

    let consistency = types::read_consistency(body)?;
    let flags = if version >= 5 {
        types::read_int(body)? as u32
    } else {
        types::read_byte(body)? as u32
    };

    let serial_consistency = if flags & query_flags::SERIAL_CONSISTENCY != 0 {
        Some(types::read_consistency(body)?)
    } else {
        None
    };

    let timestamp = if flags & query_flags::TIMESTAMP != 0 {
        Some(types::read_long(body)?)
    } else {
        None
    };

    let keyspace = if version >= 5 && flags & query_flags::KEYSPACE != 0 {
        Some(types::read_string(body)?)
    } else {
        None
    };

    let now_in_seconds = if version >= 5 && flags & query_flags::NOW_IN_SECONDS != 0 {
        Some(types::read_int(body)?)
    } else {
        None
    };

    Ok(Message::Batch(BatchMessage {
        batch_type,
        queries,
        consistency,
        serial_consistency,
        timestamp,
        keyspace,
        now_in_seconds,
        custom_payload,
    }))
}

fn decode_register(body: &mut &[u8]) -> io::Result<Message> {
    let event_types = types::read_string_list(body)?;
    Ok(Message::Register(RegisterMessage { event_types }))
}

fn decode_auth_response(body: &mut &[u8]) -> io::Result<Message> {
    let token = types::read_bytes(body)?;
    Ok(Message::AuthResponse(AuthResponseMessage { token }))
}

/// Decode query parameters (shared between QUERY and EXECUTE).
pub fn decode_query_params(body: &mut &[u8], version: u8) -> io::Result<QueryParams> {
    let consistency = types::read_consistency(body)?;
    let flags = if version >= 5 {
        types::read_int(body)? as u32
    } else {
        types::read_byte(body)? as u32
    };

    let (values, value_names) = if flags & query_flags::VALUES != 0 {
        let n = types::read_short(body)? as usize;
        let mut vals = Vec::with_capacity(n);
        if flags & query_flags::NAMES_FOR_VALUES != 0 {
            let mut names = Vec::with_capacity(n);
            for _ in 0..n {
                names.push(types::read_string(body)?);
                vals.push(types::read_bytes(body)?);
            }
            (vals, Some(names))
        } else {
            for _ in 0..n {
                vals.push(types::read_bytes(body)?);
            }
            (vals, None)
        }
    } else {
        (Vec::new(), None)
    };

    let page_size = if flags & query_flags::PAGE_SIZE != 0 {
        Some(types::read_int(body)?)
    } else {
        None
    };

    let paging_state = if flags & query_flags::PAGING_STATE != 0 {
        types::read_bytes(body)?
    } else {
        None
    };

    let serial_consistency = if flags & query_flags::SERIAL_CONSISTENCY != 0 {
        Some(types::read_consistency(body)?)
    } else {
        None
    };

    let timestamp = if flags & query_flags::TIMESTAMP != 0 {
        Some(types::read_long(body)?)
    } else {
        None
    };

    let keyspace = if version >= 5 && flags & query_flags::KEYSPACE != 0 {
        Some(types::read_string(body)?)
    } else {
        None
    };

    let now_in_seconds = if version >= 5 && flags & query_flags::NOW_IN_SECONDS != 0 {
        Some(types::read_int(body)?)
    } else {
        None
    };

    Ok(QueryParams {
        consistency,
        flags,
        values,
        value_names,
        page_size,
        paging_state,
        serial_consistency,
        timestamp,
        keyspace,
        now_in_seconds,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frame::{FrameHeader, PROTOCOL_V4};
    use bytes::{Bytes, BytesMut};

    fn make_frame(opcode: Opcode, body: &[u8]) -> Frame {
        Frame {
            header: FrameHeader {
                version: PROTOCOL_V4,
                flags: 0,
                stream_id: 0,
                opcode,
                length: body.len() as u32,
            },
            body: Bytes::copy_from_slice(body),
        }
    }

    #[test]
    fn decode_startup_message() {
        let mut buf = BytesMut::new();
        let mut opts = std::collections::HashMap::new();
        opts.insert("CQL_VERSION".to_string(), "3.4.7".to_string());
        types::write_string_map(&mut buf, &opts);

        let frame = make_frame(Opcode::Startup, &buf);
        let msg = decode_request(&frame).unwrap();
        match msg {
            Message::Startup(s) => {
                assert_eq!(s.cql_version(), Some("3.4.7"));
            }
            _ => panic!("expected Startup"),
        }
    }

    #[test]
    fn decode_options_message() {
        let frame = make_frame(Opcode::Options, &[]);
        let msg = decode_request(&frame).unwrap();
        assert!(matches!(msg, Message::Options));
    }

    #[test]
    fn decode_query_message() {
        let mut buf = BytesMut::new();
        types::write_long_string(&mut buf, "SELECT * FROM system.local");
        // consistency ONE, flags 0
        types::write_consistency(&mut buf, crate::types::Consistency::One);
        types::write_byte(&mut buf, 0); // flags

        let frame = make_frame(Opcode::Query, &buf);
        let msg = decode_request(&frame).unwrap();
        match msg {
            Message::Query(q) => {
                assert_eq!(q.query, "SELECT * FROM system.local");
                assert_eq!(q.params.consistency, crate::types::Consistency::One);
            }
            _ => panic!("expected Query"),
        }
    }

    #[test]
    fn decode_query_rejects_trailing_body_bytes() {
        let mut buf = BytesMut::new();
        types::write_long_string(&mut buf, "SELECT * FROM system.local");
        types::write_consistency(&mut buf, crate::types::Consistency::One);
        types::write_byte(&mut buf, 0);
        types::write_byte(&mut buf, 0xCA);

        let frame = make_frame(Opcode::Query, &buf);
        let err = decode_request(&frame).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("trailing bytes"));
        assert!(err.to_string().contains("Query"));
    }

    #[test]
    fn decode_query_named_values() {
        let mut buf = BytesMut::new();
        types::write_long_string(&mut buf, "SELECT * FROM ks.t WHERE id = :id");
        types::write_consistency(&mut buf, crate::types::Consistency::One);
        types::write_byte(
            &mut buf,
            (query_flags::VALUES | query_flags::NAMES_FOR_VALUES) as u8,
        );
        types::write_short(&mut buf, 1);
        types::write_string(&mut buf, "id");
        types::write_bytes_opt(&mut buf, Some(&7i32.to_be_bytes()));

        let frame = make_frame(Opcode::Query, &buf);
        let msg = decode_request(&frame).unwrap();
        match msg {
            Message::Query(q) => {
                assert_eq!(q.params.value_names, Some(vec!["id".to_string()]));
                assert_eq!(q.params.values, vec![Some(7i32.to_be_bytes().to_vec())]);
            }
            _ => panic!("expected Query"),
        }
    }

    #[test]
    fn decode_prepare_message() {
        let mut buf = BytesMut::new();
        types::write_long_string(&mut buf, "INSERT INTO ks.t (id) VALUES (?)");

        let frame = make_frame(Opcode::Prepare, &buf);
        let msg = decode_request(&frame).unwrap();
        match msg {
            Message::Prepare(p) => {
                assert_eq!(p.query, "INSERT INTO ks.t (id) VALUES (?)");
                assert!(p.keyspace.is_none());
            }
            _ => panic!("expected Prepare"),
        }
    }

    #[test]
    fn decode_prepare_v5_keyspace() {
        use crate::frame::PROTOCOL_V5;

        let mut buf = BytesMut::new();
        types::write_long_string(&mut buf, "SELECT * FROM t");
        types::write_int(&mut buf, 0x01);
        types::write_string(&mut buf, "ks1");

        let frame = Frame {
            header: FrameHeader {
                version: PROTOCOL_V5,
                flags: 0,
                stream_id: 0,
                opcode: Opcode::Prepare,
                length: buf.len() as u32,
            },
            body: Bytes::from(buf.to_vec()),
        };
        let msg = decode_request(&frame).unwrap();
        match msg {
            Message::Prepare(p) => {
                assert_eq!(p.query, "SELECT * FROM t");
                assert_eq!(p.keyspace.as_deref(), Some("ks1"));
            }
            _ => panic!("expected Prepare"),
        }
    }

    #[test]
    fn decode_prepare_with_custom_payload() {
        use crate::frame::flags as frame_flags;

        let mut buf = BytesMut::new();
        types::write_short(&mut buf, 1);
        types::write_string(&mut buf, "trace");
        types::write_bytes_opt(&mut buf, Some(&[0x02]));
        types::write_long_string(&mut buf, "SELECT * FROM system.local");

        let frame = Frame {
            header: FrameHeader {
                version: PROTOCOL_V4,
                flags: frame_flags::CUSTOM_PAYLOAD,
                stream_id: 0,
                opcode: Opcode::Prepare,
                length: buf.len() as u32,
            },
            body: Bytes::from(buf.to_vec()),
        };
        let msg = decode_request(&frame).unwrap();
        match msg {
            Message::Prepare(p) => {
                assert_eq!(p.query, "SELECT * FROM system.local");
                let payload = p.custom_payload.unwrap();
                assert_eq!(payload.get("trace"), Some(&vec![0x02]));
            }
            _ => panic!("expected Prepare"),
        }
    }

    #[test]
    fn decode_register_message() {
        let mut buf = BytesMut::new();
        types::write_string_list(
            &mut buf,
            &[
                "TOPOLOGY_CHANGE".to_string(),
                "STATUS_CHANGE".to_string(),
                "SCHEMA_CHANGE".to_string(),
            ],
        );
        let frame = make_frame(Opcode::Register, &buf);
        let msg = decode_request(&frame).unwrap();
        match msg {
            Message::Register(r) => {
                assert_eq!(r.event_types.len(), 3);
            }
            _ => panic!("expected Register"),
        }
    }

    #[test]
    fn decode_auth_response_message() {
        let mut buf = BytesMut::new();
        types::write_bytes_opt(&mut buf, Some(b"\0user\0pass"));
        let frame = make_frame(Opcode::AuthResponse, &buf);
        let msg = decode_request(&frame).unwrap();
        match msg {
            Message::AuthResponse(a) => {
                assert_eq!(a.token, Some(b"\0user\0pass".to_vec()));
            }
            _ => panic!("expected AuthResponse"),
        }
    }

    // ── WU-01: now_in_seconds for v5 ───────────────────────────────────

    #[test]
    fn decode_query_params_v5_now_in_seconds() {
        use crate::frame::PROTOCOL_V5;

        let mut buf = BytesMut::new();
        types::write_long_string(&mut buf, "SELECT 1");
        types::write_consistency(&mut buf, crate::types::Consistency::One);
        types::write_int(&mut buf, query_flags::NOW_IN_SECONDS as i32);
        types::write_int(&mut buf, 12345);

        let frame = Frame {
            header: FrameHeader {
                version: PROTOCOL_V5,
                flags: 0,
                stream_id: 0,
                opcode: Opcode::Query,
                length: buf.len() as u32,
            },
            body: Bytes::from(buf.to_vec()),
        };
        let msg = decode_request(&frame).unwrap();
        match msg {
            Message::Query(q) => {
                assert_eq!(q.params.now_in_seconds, Some(12345));
            }
            _ => panic!("expected Query"),
        }
    }

    #[test]
    fn decode_query_params_v5_keyspace_and_now_in_seconds() {
        use crate::frame::PROTOCOL_V5;

        let mut buf = BytesMut::new();
        types::write_long_string(&mut buf, "SELECT * FROM t");
        types::write_consistency(&mut buf, crate::types::Consistency::LocalQuorum);
        types::write_int(
            &mut buf,
            (query_flags::KEYSPACE | query_flags::NOW_IN_SECONDS) as i32,
        );
        types::write_string(&mut buf, "ks1");
        types::write_int(&mut buf, 77);

        let frame = Frame {
            header: FrameHeader {
                version: PROTOCOL_V5,
                flags: 0,
                stream_id: 0,
                opcode: Opcode::Query,
                length: buf.len() as u32,
            },
            body: Bytes::from(buf.to_vec()),
        };
        let msg = decode_request(&frame).unwrap();
        match msg {
            Message::Query(q) => {
                assert_eq!(
                    q.params.flags,
                    query_flags::KEYSPACE | query_flags::NOW_IN_SECONDS
                );
                assert_eq!(q.params.consistency, crate::types::Consistency::LocalQuorum);
                assert_eq!(q.params.keyspace.as_deref(), Some("ks1"));
                assert_eq!(q.params.now_in_seconds, Some(77));
            }
            _ => panic!("expected Query"),
        }
    }

    #[test]
    fn decode_query_params_v5_rejects_unflagged_trailing_now_in_seconds() {
        use crate::frame::PROTOCOL_V5;

        let mut buf = BytesMut::new();
        types::write_long_string(&mut buf, "SELECT 1");
        types::write_consistency(&mut buf, crate::types::Consistency::One);
        types::write_int(&mut buf, 0);
        types::write_int(&mut buf, 12345);

        let frame = Frame {
            header: FrameHeader {
                version: PROTOCOL_V5,
                flags: 0,
                stream_id: 0,
                opcode: Opcode::Query,
                length: buf.len() as u32,
            },
            body: Bytes::from(buf.to_vec()),
        };
        let err = decode_request(&frame).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("trailing bytes"));
    }

    #[test]
    fn decode_execute_params_v5_keyspace_and_now_in_seconds() {
        use crate::frame::PROTOCOL_V5;

        let mut buf = BytesMut::new();
        types::write_short_bytes(&mut buf, &[0x11; 16]);
        types::write_short_bytes(&mut buf, &[0x22; 16]);
        types::write_consistency(&mut buf, crate::types::Consistency::LocalQuorum);
        types::write_int(
            &mut buf,
            (query_flags::KEYSPACE | query_flags::NOW_IN_SECONDS) as i32,
        );
        types::write_string(&mut buf, "ks1");
        types::write_int(&mut buf, 321);

        let frame = Frame {
            header: FrameHeader {
                version: PROTOCOL_V5,
                flags: 0,
                stream_id: 0,
                opcode: Opcode::Execute,
                length: buf.len() as u32,
            },
            body: Bytes::from(buf.to_vec()),
        };
        let msg = decode_request(&frame).unwrap();
        match msg {
            Message::Execute(execute) => {
                assert_eq!(execute.id, vec![0x11; 16]);
                assert_eq!(execute.result_metadata_id, Some(vec![0x22; 16]));
                assert_eq!(
                    execute.params.consistency,
                    crate::types::Consistency::LocalQuorum
                );
                assert_eq!(execute.params.keyspace.as_deref(), Some("ks1"));
                assert_eq!(execute.params.now_in_seconds, Some(321));
            }
            _ => panic!("expected Execute"),
        }
    }

    #[test]
    fn decode_batch_v5_keyspace_and_now_in_seconds() {
        use crate::frame::PROTOCOL_V5;

        let mut buf = BytesMut::new();
        types::write_byte(&mut buf, 0); // logged
        types::write_short(&mut buf, 1); // query count
        types::write_byte(&mut buf, 0); // raw CQL query
        types::write_long_string(&mut buf, "INSERT INTO t (id) VALUES (?)");
        types::write_short(&mut buf, 0); // values count
        types::write_consistency(&mut buf, crate::types::Consistency::LocalQuorum);
        types::write_int(
            &mut buf,
            (query_flags::KEYSPACE | query_flags::NOW_IN_SECONDS) as i32,
        );
        types::write_string(&mut buf, "ks1");
        types::write_int(&mut buf, 123);

        let frame = Frame {
            header: FrameHeader {
                version: PROTOCOL_V5,
                flags: 0,
                stream_id: 0,
                opcode: Opcode::Batch,
                length: buf.len() as u32,
            },
            body: Bytes::from(buf.to_vec()),
        };
        let msg = decode_request(&frame).unwrap();
        match msg {
            Message::Batch(batch) => {
                assert_eq!(batch.queries.len(), 1);
                assert_eq!(batch.consistency, crate::types::Consistency::LocalQuorum);
                assert_eq!(batch.keyspace.as_deref(), Some("ks1"));
                assert_eq!(batch.now_in_seconds, Some(123));
            }
            _ => panic!("expected Batch"),
        }
    }

    #[test]
    fn decode_batch_v5_rejects_unflagged_trailing_options() {
        use crate::frame::PROTOCOL_V5;

        let mut buf = BytesMut::new();
        types::write_byte(&mut buf, 0); // logged
        types::write_short(&mut buf, 1); // query count
        types::write_byte(&mut buf, 0); // raw CQL query
        types::write_long_string(&mut buf, "INSERT INTO t (id) VALUES (?)");
        types::write_short(&mut buf, 0); // values count
        types::write_consistency(&mut buf, crate::types::Consistency::One);
        types::write_int(&mut buf, 0); // v5 flags: none
        types::write_string(&mut buf, "ks1");
        types::write_int(&mut buf, 123);

        let frame = Frame {
            header: FrameHeader {
                version: PROTOCOL_V5,
                flags: 0,
                stream_id: 0,
                opcode: Opcode::Batch,
                length: buf.len() as u32,
            },
            body: Bytes::from(buf.to_vec()),
        };
        let err = decode_request(&frame).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("trailing bytes"));
    }

    #[test]
    fn decode_batch_rejects_unknown_query_kind() {
        let mut buf = BytesMut::new();
        types::write_byte(&mut buf, 0); // logged
        types::write_short(&mut buf, 1); // query count
        types::write_byte(&mut buf, 2); // invalid query kind

        let frame = make_frame(Opcode::Batch, &buf);
        let err = decode_request(&frame).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("invalid query kind"));
    }

    #[test]
    fn decode_query_params_v4_no_now_in_seconds() {
        let mut buf = BytesMut::new();
        types::write_long_string(&mut buf, "SELECT 1");
        types::write_consistency(&mut buf, crate::types::Consistency::One);
        types::write_byte(&mut buf, 0); // flags: none

        let frame = make_frame(Opcode::Query, &buf);
        let msg = decode_request(&frame).unwrap();
        match msg {
            Message::Query(q) => {
                assert!(q.params.now_in_seconds.is_none());
            }
            _ => panic!("expected Query"),
        }
    }

    // ── WU-02: Custom payload decoding ──────────────────────────────────

    #[test]
    fn decode_query_with_custom_payload() {
        use crate::frame::flags as frame_flags;

        // Build the body: custom payload bytes-map prefix + query body.
        let mut buf = BytesMut::new();

        // Custom payload: 1 entry { "trace" => [0x01] }
        types::write_short(&mut buf, 1); // n entries
        types::write_string(&mut buf, "trace");
        types::write_bytes_opt(&mut buf, Some(&[0x01]));

        // Now the actual query body.
        types::write_long_string(&mut buf, "SELECT 1");
        types::write_consistency(&mut buf, crate::types::Consistency::One);
        types::write_byte(&mut buf, 0); // query flags

        let frame = Frame {
            header: FrameHeader {
                version: PROTOCOL_V4,
                flags: frame_flags::CUSTOM_PAYLOAD,
                stream_id: 0,
                opcode: Opcode::Query,
                length: buf.len() as u32,
            },
            body: Bytes::from(buf.to_vec()),
        };
        let msg = decode_request(&frame).unwrap();
        match msg {
            Message::Query(q) => {
                assert!(q.custom_payload.is_some());
                let payload = q.custom_payload.unwrap();
                assert_eq!(payload.get("trace"), Some(&vec![0x01]));
                assert_eq!(q.query, "SELECT 1");
            }
            _ => panic!("expected Query"),
        }
    }

    #[test]
    fn decode_query_without_custom_payload() {
        let mut buf = BytesMut::new();
        types::write_long_string(&mut buf, "SELECT 1");
        types::write_consistency(&mut buf, crate::types::Consistency::One);
        types::write_byte(&mut buf, 0);

        let frame = make_frame(Opcode::Query, &buf);
        let msg = decode_request(&frame).unwrap();
        match msg {
            Message::Query(q) => {
                assert!(q.custom_payload.is_none());
            }
            _ => panic!("expected Query"),
        }
    }
}
