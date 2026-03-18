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

//! Connection lifecycle state machine for the CQL native protocol.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.transport.ServerConnection`
//! - `org.apache.cassandra.transport.Server`
//! - `org.apache.cassandra.transport.Message.Dispatcher`
//!
//! ## Connection lifecycle
//!
//! ```text
//! New ─→ STARTUP ─→ (AUTHENTICATE ─→ AUTH_RESPONSE ─→)* ─→ READY
//!     ↘ OPTIONS → SUPPORTED (stays in New)
//! ```

use crate::auth::{AuthResult, Authenticator};
use crate::frame::{Frame, PROTOCOL_V4, PROTOCOL_V5, flags};
use crate::message::*;
use crate::request;
use crate::response;
use std::collections::HashSet;
use std::io;

#[cfg(any(feature = "compression-lz4", feature = "compression-snappy"))]
use crate::compress::Compression;

/// Connection state in the protocol lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionState {
    /// Waiting for STARTUP or OPTIONS.
    New,
    /// Waiting for AUTH_RESPONSE after sending AUTHENTICATE.
    Authenticating,
    /// Fully initialized, accepting queries.
    Ready,
}

/// Per-connection context tracked by the server.
pub struct ConnectionContext {
    /// Current lifecycle state.
    pub state: ConnectionState,
    /// Negotiated protocol version (4 or 5).
    pub protocol_version: u8,
    /// Negotiated compression algorithm (if any).
    #[cfg(any(feature = "compression-lz4", feature = "compression-snappy"))]
    pub compression: Option<Compression>,
    /// Event types this connection is registered for.
    pub registered_events: HashSet<String>,
    /// Optional keyspace set via USE or per-query.
    pub keyspace: Option<String>,
    /// Authenticated user, if any.
    pub authenticated_user: Option<String>,
}

impl Default for ConnectionContext {
    fn default() -> Self {
        Self {
            state: ConnectionState::New,
            protocol_version: PROTOCOL_V4,
            #[cfg(any(feature = "compression-lz4", feature = "compression-snappy"))]
            compression: None,
            registered_events: HashSet::new(),
            keyspace: None,
            authenticated_user: None,
        }
    }
}

impl ConnectionContext {
    pub fn new() -> Self {
        Self::default()
    }

    /// Process a single request frame and produce a response frame.
    ///
    /// This handles connection lifecycle messages (STARTUP, OPTIONS,
    /// AUTH_RESPONSE, REGISTER). Query/Execute/Prepare/Batch are returned
    /// as `Ok(None)` so the caller can route them to the executor.
    pub fn process_lifecycle(
        &mut self,
        frame: &Frame,
        authenticator: &dyn Authenticator,
    ) -> io::Result<Option<Frame>> {
        let version = frame.header.protocol_version();
        let stream_id = frame.header.stream_id;

        // Version negotiation: reject unsupported versions.
        // Per spec, the error response uses the highest supported version (v5)
        // in the frame header so the client can downgrade.
        if version != 4 && version != 5 {
            let err = response::error_frame(
                PROTOCOL_V5,
                stream_id,
                0x000A, // PROTOCOL_ERROR
                &format!(
                    "Invalid or unsupported protocol version ({}); supported versions are 4 and 5",
                    version
                ),
            );
            return Ok(Some(err));
        }

        // v5 is a beta protocol: the USE_BETA flag must be set on the frame.
        if version == 5 && (frame.header.flags & flags::USE_BETA == 0) {
            let err = response::error_frame(
                PROTOCOL_V5,
                stream_id,
                0x000A, // PROTOCOL_ERROR
                "Beta version of the protocol used (5/v5-beta), but USE_BETA flag is not set",
            );
            return Ok(Some(err));
        }

        self.protocol_version = version;

        let msg = request::decode_request(frame)?;

        match (&self.state, &msg) {
            // ── New state ──────────────────────────────────────────
            (ConnectionState::New, Message::Options) => {
                Ok(Some(response::supported_frame(version, stream_id)))
            }
            (ConnectionState::New, Message::Startup(startup)) => {
                // Negotiate compression.
                #[cfg(any(feature = "compression-lz4", feature = "compression-snappy"))]
                if let Some(comp_name) = startup.compression() {
                    match Compression::from_name(comp_name) {
                        Some(c) => self.compression = Some(c),
                        None => {
                            return Ok(Some(response::error_frame(
                                version,
                                stream_id,
                                0x000A,
                                &format!("Unknown compression algorithm: {}", comp_name),
                            )));
                        }
                    }
                }

                if authenticator.requires_auth() {
                    self.state = ConnectionState::Authenticating;
                    let resp = response::encode_response(
                        &Message::Authenticate(AuthenticateMessage {
                            authenticator: authenticator.class_name().to_string(),
                        }),
                        version,
                        stream_id,
                    );
                    Ok(Some(resp))
                } else {
                    self.state = ConnectionState::Ready;
                    Ok(Some(response::ready_frame(version, stream_id)))
                }
            }

            // ── Authenticating state ───────────────────────────────
            (ConnectionState::Authenticating, Message::AuthResponse(auth_resp)) => {
                match authenticator.authenticate(auth_resp.token.as_deref()) {
                    Ok(AuthResult::Success(user, final_token)) => {
                        self.state = ConnectionState::Ready;
                        if let Some(u) = user {
                            self.authenticated_user = Some(u);
                        }
                        Ok(Some(response::encode_response(
                            &Message::AuthSuccess(final_token),
                            version,
                            stream_id,
                        )))
                    }
                    Ok(AuthResult::Challenge(challenge)) => Ok(Some(response::encode_response(
                        &Message::AuthChallenge(challenge),
                        version,
                        stream_id,
                    ))),
                    Err(e) => {
                        Ok(Some(response::error_frame(
                            version, stream_id, 0x0100, // BAD_CREDENTIALS
                            &e,
                        )))
                    }
                }
            }

            // ── Ready state ────────────────────────────────────────
            (ConnectionState::Ready, Message::Register(reg)) => {
                for event_type in &reg.event_types {
                    self.registered_events.insert(event_type.clone());
                }
                Ok(Some(response::ready_frame(version, stream_id)))
            }

            // Query/Prepare/Execute/Batch → pass through to executor.
            (ConnectionState::Ready, Message::Query(_))
            | (ConnectionState::Ready, Message::Prepare(_))
            | (ConnectionState::Ready, Message::Execute(_))
            | (ConnectionState::Ready, Message::Batch(_)) => {
                Ok(None) // Caller handles these.
            }

            // Wrong state.
            (state, _) => {
                let err_msg = format!("Received {:?} in state {:?}", frame.header.opcode, state);
                Ok(Some(response::error_frame(
                    version, stream_id, 0x000A, &err_msg,
                )))
            }
        }
    }

    /// Build a response frame with optional tracing ID, warnings, and custom payload.
    pub fn wrap_response(
        &self,
        mut frame: Frame,
        tracing_id: Option<uuid::Uuid>,
        warnings: &[String],
        custom_payload: Option<&std::collections::HashMap<String, Vec<u8>>>,
    ) -> Frame {
        use crate::types;
        use bytes::BytesMut;

        let mut extra_flags: u8 = 0;
        let mut prefix = BytesMut::new();

        // Tracing ID (16-byte UUID prepended to body).
        if let Some(tid) = tracing_id {
            extra_flags |= flags::TRACING;
            prefix.extend_from_slice(tid.as_bytes());
        }

        // Warnings (string list prepended to body).
        if !warnings.is_empty() {
            extra_flags |= flags::WARNING;
            types::write_short(&mut prefix, warnings.len() as u16);
            for w in warnings {
                types::write_string(&mut prefix, w);
            }
        }

        // Custom payload (bytes map prepended to body).
        if let Some(payload) = custom_payload {
            extra_flags |= flags::CUSTOM_PAYLOAD;
            types::write_short(&mut prefix, payload.len() as u16);
            for (k, v) in payload {
                types::write_string(&mut prefix, k);
                types::write_bytes_opt(&mut prefix, Some(v));
            }
        }

        if extra_flags != 0 {
            let mut new_body = BytesMut::with_capacity(prefix.len() + frame.body.len());
            new_body.extend_from_slice(&prefix);
            new_body.extend_from_slice(&frame.body);

            frame.header.flags |= extra_flags;
            frame.header.length = new_body.len() as u32;
            frame.body = new_body.freeze();
        }

        frame
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::{AllowAllAuthenticator, PasswordAuthenticator};
    use crate::frame::{FrameHeader, Opcode, PROTOCOL_V4, PROTOCOL_V5, RESPONSE_FLAG};
    use crate::types;
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

    fn startup_body() -> Vec<u8> {
        let mut buf = BytesMut::new();
        let mut opts = std::collections::HashMap::new();
        opts.insert("CQL_VERSION".to_string(), "3.4.7".to_string());
        types::write_string_map(&mut buf, &opts);
        buf.to_vec()
    }

    #[test]
    fn lifecycle_options_in_new_state() {
        let mut ctx = ConnectionContext::new();
        let frame = make_frame(Opcode::Options, &[]);
        let resp = ctx
            .process_lifecycle(&frame, &AllowAllAuthenticator)
            .unwrap();
        assert!(resp.is_some());
        assert_eq!(resp.unwrap().header.opcode, Opcode::Supported);
        assert_eq!(ctx.state, ConnectionState::New); // Should stay in New.
    }

    #[test]
    fn lifecycle_startup_no_auth() {
        let mut ctx = ConnectionContext::new();
        let body = startup_body();
        let frame = make_frame(Opcode::Startup, &body);
        let resp = ctx
            .process_lifecycle(&frame, &AllowAllAuthenticator)
            .unwrap();
        assert!(resp.is_some());
        assert_eq!(resp.unwrap().header.opcode, Opcode::Ready);
        assert_eq!(ctx.state, ConnectionState::Ready);
    }

    #[test]
    fn lifecycle_startup_with_auth() {
        let mut ctx = ConnectionContext::new();
        let body = startup_body();
        let frame = make_frame(Opcode::Startup, &body);
        let resp = ctx
            .process_lifecycle(&frame, &PasswordAuthenticator)
            .unwrap();
        assert!(resp.is_some());
        assert_eq!(resp.unwrap().header.opcode, Opcode::Authenticate);
        assert_eq!(ctx.state, ConnectionState::Authenticating);

        // Now send AUTH_RESPONSE.
        let mut auth_body = BytesMut::new();
        types::write_bytes_opt(&mut auth_body, Some(b"\0cassandra\0cassandra"));
        let auth_frame = make_frame(Opcode::AuthResponse, &auth_body);
        let resp2 = ctx
            .process_lifecycle(&auth_frame, &PasswordAuthenticator)
            .unwrap();
        assert!(resp2.is_some());
        assert_eq!(resp2.unwrap().header.opcode, Opcode::AuthSuccess);
        assert_eq!(ctx.state, ConnectionState::Ready);
    }

    #[test]
    fn lifecycle_query_passthrough_in_ready() {
        let mut ctx = ConnectionContext::new();
        ctx.state = ConnectionState::Ready;

        let mut body = BytesMut::new();
        types::write_long_string(&mut body, "SELECT * FROM t");
        types::write_consistency(&mut body, crate::types::Consistency::One);
        types::write_byte(&mut body, 0);
        let frame = make_frame(Opcode::Query, &body);

        let resp = ctx
            .process_lifecycle(&frame, &AllowAllAuthenticator)
            .unwrap();
        assert!(resp.is_none()); // Should be None → caller handles.
    }

    #[test]
    fn lifecycle_register_events() {
        let mut ctx = ConnectionContext::new();
        ctx.state = ConnectionState::Ready;

        let mut body = BytesMut::new();
        types::write_string_list(
            &mut body,
            &[
                "TOPOLOGY_CHANGE".to_string(),
                "STATUS_CHANGE".to_string(),
                "SCHEMA_CHANGE".to_string(),
            ],
        );
        let frame = make_frame(Opcode::Register, &body);
        let resp = ctx
            .process_lifecycle(&frame, &AllowAllAuthenticator)
            .unwrap();
        assert!(resp.is_some());
        assert_eq!(ctx.registered_events.len(), 3);
        assert!(ctx.registered_events.contains("TOPOLOGY_CHANGE"));
    }

    #[test]
    fn lifecycle_wrong_state() {
        let mut ctx = ConnectionContext::new();
        // Sending a QUERY in New state should error.
        let mut body = BytesMut::new();
        types::write_long_string(&mut body, "SELECT 1");
        types::write_consistency(&mut body, crate::types::Consistency::One);
        types::write_byte(&mut body, 0);
        let frame = make_frame(Opcode::Query, &body);
        let resp = ctx
            .process_lifecycle(&frame, &AllowAllAuthenticator)
            .unwrap();
        assert!(resp.is_some());
        assert_eq!(resp.unwrap().header.opcode, Opcode::Error);
    }

    #[test]
    fn lifecycle_unsupported_version() {
        let mut ctx = ConnectionContext::new();
        let frame = Frame {
            header: FrameHeader {
                version: 0x03, // v3 not supported
                flags: 0,
                stream_id: 0,
                opcode: Opcode::Startup,
                length: 0,
            },
            body: Bytes::new(),
        };
        let resp = ctx
            .process_lifecycle(&frame, &AllowAllAuthenticator)
            .unwrap();
        assert!(resp.is_some());
        assert_eq!(resp.unwrap().header.opcode, Opcode::Error);
    }

    #[test]
    fn wrap_response_with_tracing() {
        let ctx = ConnectionContext::new();
        let frame = response::ready_frame(PROTOCOL_V4, 0);
        let tid = uuid::Uuid::from_bytes([1u8; 16]);
        let wrapped = ctx.wrap_response(frame, Some(tid), &[], None);
        assert_ne!(wrapped.header.flags & flags::TRACING, 0);
        // Body should start with the 16-byte UUID.
        assert!(wrapped.body.len() >= 16);
        assert_eq!(&wrapped.body[..16], &[1u8; 16]);
    }

    #[test]
    fn wrap_response_with_warnings() {
        let ctx = ConnectionContext::new();
        let frame = response::ready_frame(PROTOCOL_V4, 0);
        let warnings = vec!["test warning".to_string()];
        let wrapped = ctx.wrap_response(frame, None, &warnings, None);
        assert_ne!(wrapped.header.flags & flags::WARNING, 0);
    }

    // ── WU-01: v5 beta flag validation ──────────────────────────────────

    #[test]
    fn lifecycle_v5_without_beta_flag_rejected() {
        let mut ctx = ConnectionContext::new();
        let body = startup_body();
        let frame = Frame {
            header: FrameHeader {
                version: PROTOCOL_V5,
                flags: 0, // USE_BETA not set
                stream_id: 0,
                opcode: Opcode::Startup,
                length: body.len() as u32,
            },
            body: Bytes::copy_from_slice(&body),
        };
        let resp = ctx
            .process_lifecycle(&frame, &AllowAllAuthenticator)
            .unwrap();
        assert!(resp.is_some());
        let resp_frame = resp.unwrap();
        assert_eq!(resp_frame.header.opcode, Opcode::Error);
        // Should mention USE_BETA in the error.
        let mut resp_body: &[u8] = &resp_frame.body;
        let _code = types::read_int(&mut resp_body).unwrap();
        let msg = types::read_string(&mut resp_body).unwrap();
        assert!(
            msg.contains("USE_BETA"),
            "error message should mention USE_BETA: {}",
            msg
        );
    }

    #[test]
    fn lifecycle_v5_with_beta_flag_accepted() {
        let mut ctx = ConnectionContext::new();
        let body = startup_body();
        let frame = Frame {
            header: FrameHeader {
                version: PROTOCOL_V5,
                flags: flags::USE_BETA, // USE_BETA set
                stream_id: 0,
                opcode: Opcode::Startup,
                length: body.len() as u32,
            },
            body: Bytes::copy_from_slice(&body),
        };
        let resp = ctx
            .process_lifecycle(&frame, &AllowAllAuthenticator)
            .unwrap();
        assert!(resp.is_some());
        assert_eq!(resp.unwrap().header.opcode, Opcode::Ready);
        assert_eq!(ctx.state, ConnectionState::Ready);
        assert_eq!(ctx.protocol_version, 5);
    }

    // ── WU-03: Version negotiation error format ─────────────────────────

    #[test]
    fn lifecycle_v6_negotiation_produces_v5_error_frame() {
        let mut ctx = ConnectionContext::new();
        let frame = Frame {
            header: FrameHeader {
                version: 0x06, // v6 not supported
                flags: 0,
                stream_id: 42,
                opcode: Opcode::Startup,
                length: 0,
            },
            body: Bytes::new(),
        };
        let resp = ctx
            .process_lifecycle(&frame, &AllowAllAuthenticator)
            .unwrap();
        assert!(resp.is_some());
        let resp_frame = resp.unwrap();
        assert_eq!(resp_frame.header.opcode, Opcode::Error);
        // The response frame version should be PROTOCOL_V5 | RESPONSE_FLAG.
        assert_eq!(
            resp_frame.header.version,
            PROTOCOL_V5 | RESPONSE_FLAG,
            "error response should use highest supported version (v5)"
        );
        assert_eq!(resp_frame.header.stream_id, 42);
    }

    #[test]
    fn lifecycle_v3_negotiation_produces_v5_error_frame() {
        let mut ctx = ConnectionContext::new();
        let frame = Frame {
            header: FrameHeader {
                version: 0x03, // v3 not supported
                flags: 0,
                stream_id: 7,
                opcode: Opcode::Startup,
                length: 0,
            },
            body: Bytes::new(),
        };
        let resp = ctx
            .process_lifecycle(&frame, &AllowAllAuthenticator)
            .unwrap();
        assert!(resp.is_some());
        let resp_frame = resp.unwrap();
        assert_eq!(resp_frame.header.opcode, Opcode::Error);
        // Must use v5 (highest supported), not the rejected v3.
        assert_eq!(
            resp_frame.header.version,
            PROTOCOL_V5 | RESPONSE_FLAG,
            "error response should use v5, not the rejected version"
        );
    }
}
