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

use crate::frame::{Frame, Opcode};
use crate::message::*;
use crate::types;
use std::io;

/// Decode a request frame's body into a Message.
pub fn decode_request(frame: &Frame) -> io::Result<Message> {
    let mut body: &[u8] = &frame.body;
    match frame.header.opcode {
        Opcode::Startup => decode_startup(&mut body),
        Opcode::Options => Ok(Message::Options),
        Opcode::Query => decode_query(&mut body, frame.header.protocol_version()),
        Opcode::Prepare => decode_prepare(&mut body, frame.header.protocol_version()),
        Opcode::Execute => decode_execute(&mut body, frame.header.protocol_version()),
        Opcode::Batch => decode_batch(&mut body, frame.header.protocol_version()),
        Opcode::Register => decode_register(&mut body),
        Opcode::AuthResponse => decode_auth_response(&mut body),
        other => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unexpected request opcode: {:?}", other),
        )),
    }
}

fn decode_startup(body: &mut &[u8]) -> io::Result<Message> {
    let options = types::read_string_map(body)?;
    Ok(Message::Startup(StartupMessage { options }))
}

fn decode_query(body: &mut &[u8], version: u8) -> io::Result<Message> {
    let query = types::read_long_string(body)?;
    let params = decode_query_params(body, version)?;
    Ok(Message::Query(QueryMessage { query, params }))
}

fn decode_prepare(body: &mut &[u8], version: u8) -> io::Result<Message> {
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
    Ok(Message::Prepare(PrepareMessage { query, keyspace }))
}

fn decode_execute(body: &mut &[u8], version: u8) -> io::Result<Message> {
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
    }))
}

fn decode_batch(body: &mut &[u8], version: u8) -> io::Result<Message> {
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
        let is_prepared = kind == 1;
        let query_or_id = if is_prepared {
            types::read_short_bytes(body)?
        } else {
            types::read_long_string(body)?.into_bytes()
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
    let flags = if !body.is_empty() {
        types::read_byte(body)?
    } else {
        0
    };

    let serial_consistency = if flags & 0x10 != 0 {
        Some(types::read_consistency(body)?)
    } else {
        None
    };

    let timestamp = if flags & 0x20 != 0 {
        Some(types::read_long(body)?)
    } else {
        None
    };

    // v5 keyspace flag ignored for now.
    let _ = version;

    Ok(Message::Batch(BatchMessage {
        batch_type,
        queries,
        consistency,
        serial_consistency,
        timestamp,
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
    let flags = types::read_byte(body)?;

    let values = if flags & query_flags::VALUES != 0 {
        let n = types::read_short(body)? as usize;
        let mut vals = Vec::with_capacity(n);
        if flags & query_flags::NAMES_FOR_VALUES != 0 {
            for _ in 0..n {
                let _name = types::read_string(body)?;
                vals.push(types::read_bytes(body)?);
            }
        } else {
            for _ in 0..n {
                vals.push(types::read_bytes(body)?);
            }
        }
        vals
    } else {
        Vec::new()
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

    Ok(QueryParams {
        consistency,
        flags,
        values,
        page_size,
        paging_state,
        serial_consistency,
        timestamp,
        keyspace,
        now_in_seconds: None, // v5 beta field, not standard
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
}
