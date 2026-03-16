// Licensed under Apache License, Version 2.0.

//! Native Protocol TCP Server.
//!
//! Listens for incoming CQL client connections (typically port 9042).

use bytes::{Buf, BytesMut};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tracing::{debug, error, info};

use cassandra_native_protocol::auth::{AuthResult, Authenticator as NativeAuthenticator};
use cassandra_native_protocol::connection::ConnectionContext;
use cassandra_native_protocol::frame::{self, Frame, FrameCodec};
use cassandra_security::audit::{AuditEvent, AuditEventType, AuditLogger, AuditStatus};
use cassandra_security::auth::{Authenticator as SecAuthenticator, Credentials};
use cassandra_security::fql::{FqlLogger, FqlRecord};
use cassandra_security::tls::ReloadableTlsAcceptor;
use tokio_util::codec::{Decoder, Encoder};

use crate::executor::{QueryExecutor, QueryResult};

pub struct ServerConfig {
    pub listen_address: String,
    pub client_encryption_enabled: bool,
    // TODO: load client_encryption_options from config
}

pub struct NativeServer {
    config: ServerConfig,
    executor: Arc<QueryExecutor>,
    authenticator: Arc<dyn NativeAuthenticator>,
    tls_acceptor: Option<ReloadableTlsAcceptor>,
    fql_logger: Arc<FqlLogger>,
    audit_logger: Arc<dyn AuditLogger>,
}

impl NativeServer {
    pub fn new(
        config: ServerConfig,
        executor: Arc<QueryExecutor>,
        authenticator: Arc<dyn NativeAuthenticator>,
        tls_acceptor: Option<ReloadableTlsAcceptor>,
        fql_logger: Arc<FqlLogger>,
        audit_logger: Arc<dyn AuditLogger>,
    ) -> Self {
        Self {
            config,
            executor,
            authenticator,
            tls_acceptor,
            fql_logger,
            audit_logger,
        }
    }

    pub async fn run(self: Arc<Self>) -> anyhow::Result<()> {
        let listener = TcpListener::bind(&self.config.listen_address).await?;
        info!(
            "Native protocol server listening on {}",
            self.config.listen_address
        );

        if self.config.client_encryption_enabled && self.tls_acceptor.is_none() {
            tracing::warn!("Client encryption is enabled but no TLS acceptor was provided!");
        }

        loop {
            let (stream, addr) = match listener.accept().await {
                Ok(res) => res,
                Err(e) => {
                    error!("Error accepting connection: {}", e);
                    continue;
                }
            };

            debug!("Accepted connection from {}", addr);
            let server = Arc::clone(&self);

            tokio::spawn(async move {
                if let Some(ref tls_acceptor) = server.tls_acceptor {
                    let acceptor = tls_acceptor.acceptor();
                    match acceptor.accept(stream).await {
                        Ok(tls_stream) => {
                            if let Err(e) = server.handle_connection(tls_stream).await {
                                debug!("Connection {} error: {}", addr, e);
                            }
                        }
                        Err(e) => {
                            error!("TLS handshake failed for {}: {}", addr, e);
                        }
                    }
                } else {
                    if let Err(e) = server.handle_connection(stream).await {
                        debug!("Connection {} error: {}", addr, e);
                    }
                }
            });
        }
    }

    async fn handle_connection<S>(&self, mut stream: S) -> anyhow::Result<()>
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        let mut ctx = ConnectionContext::new();
        let mut buffer = BytesMut::with_capacity(8192);

        loop {
            let n = stream.read_buf(&mut buffer).await?;
            if n == 0 {
                return Ok(()); // Client closed connection
            }

            // Parse as many frames as we can from the buffer
            while let Some(frame) = self.decode_frame(&mut buffer)? {
                let response_frame = {
                    let mut resp_opt =
                        ctx.process_lifecycle(&frame, self.authenticator.as_ref())?;
                    if resp_opt.is_none() {
                        // Protocol lifecycle didn't handle it, must be a query.
                        resp_opt = self.handle_query(&mut ctx, &frame).await?;
                    }
                    resp_opt
                };

                if let Some(mut r_frame) = response_frame {
                    r_frame = ctx.wrap_response(r_frame, None, &[], None);

                    let mut out_buf = bytes::BytesMut::new();
                    let mut codec = FrameCodec;
                    codec.encode(r_frame, &mut out_buf)?;
                    stream.write_all(&out_buf).await?;
                }
            }
        }
    }

    fn decode_frame(&self, buffer: &mut BytesMut) -> anyhow::Result<Option<Frame>> {
        let mut codec = FrameCodec;
        Ok(codec.decode(buffer)?)
    }

    async fn handle_query(
        &self,
        ctx: &mut ConnectionContext,
        frame: &Frame,
    ) -> anyhow::Result<Option<Frame>> {
        use cassandra_native_protocol::message::*;
        use cassandra_native_protocol::request;
        use cassandra_native_protocol::response;

        let msg = request::decode_request(frame)?;
        let version = ctx.protocol_version;
        let stream_id = frame.header.stream_id;

        match msg {
            Message::Query(q) => {
                let cql = q.query;
                let user_str = ctx
                    .authenticated_user
                    .clone()
                    .unwrap_or_else(|| "anonymous".to_string());

                // 1. FQL Logging
                if self.fql_logger.is_enabled() {
                    let record = FqlRecord {
                        timestamp_micros: std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap()
                            .as_micros() as i64,
                        consistency_level: 1, // mapping ONE for now
                        query: cql.clone(),
                        bind_values: vec![], // No bind values in simple Query
                    };
                    if let Err(e) = self.fql_logger.log_query(&record) {
                        tracing::warn!("Failed to append to FQL log: {}", e);
                    }
                }

                // Parse the CQL
                let stmt = match cassandra_cql::parser::parse(&cql) {
                    Ok(s) => s,
                    Err(e) => {
                        return Ok(Some(response::error_frame(
                            version,
                            stream_id,
                            0x2000,
                            &e.to_string(), // SYNTAX_ERROR
                        )));
                    }
                };

                // Plan the query (requires schema snapshot)
                // TODO: For now, executor reads schema directly, but we should pass it.
                // We'll parse assuming active keyspace.
                let schema_catalog = self.executor.catalog();
                let schema = schema_catalog.read().snapshot();

                let plan =
                    match cassandra_cql::planner::plan(&stmt, &schema, ctx.keyspace.as_deref()) {
                        Ok(p) => p,
                        Err(e) => {
                            return Ok(Some(response::error_frame(
                                version,
                                stream_id,
                                0x2200,
                                &e.to_string(), // INVALID
                            )));
                        }
                    };

                // Execute the query
                let result = self
                    .executor
                    .execute(&plan, ctx.authenticated_user.as_deref());

                // 2. Audit Logging
                if self.audit_logger.is_enabled() {
                    let status = if result.is_ok() {
                        AuditStatus::Success
                    } else {
                        AuditStatus::Failure
                    };
                    let mut event = AuditEvent::now(AuditEventType::Query, &user_str, "127.0.0.1");
                    event.query = Some(cql.clone());
                    event.keyspace = ctx.keyspace.clone();
                    event.status = status;
                    self.audit_logger.log(&event);
                }

                match result {
                    Ok(QueryResult::Void) => Ok(Some(response::encode_response(
                        &Message::Result(cassandra_native_protocol::message::ResultMessage::Void),
                        version,
                        stream_id,
                    ))),
                    Ok(QueryResult::SetKeyspace(ks)) => {
                        ctx.keyspace = Some(ks.clone());
                        Ok(Some(response::encode_response(
                            &Message::Result(
                                cassandra_native_protocol::message::ResultMessage::SetKeyspace(ks),
                            ),
                            version,
                            stream_id,
                        )))
                    }
                    Ok(QueryResult::SchemaChange {
                        change_type,
                        target,
                        keyspace,
                        name,
                    }) => {
                        Ok(Some(response::encode_response(
                            &Message::Result(
                                cassandra_native_protocol::message::ResultMessage::SchemaChange(
                                    cassandra_native_protocol::message::SchemaChange {
                                        change_type,
                                        target,
                                        keyspace,
                                        name,
                                        arg_types: None, // Used for functions/aggregates, omit for now
                                    },
                                ),
                            ),
                            version,
                            stream_id,
                        )))
                    }
                    Ok(QueryResult::Rows {
                        columns: _,
                        rows: _,
                    }) => {
                        // TODO: Map to actual Row results
                        // For now we just return a stub result because `Rows` isn't fully mapped to Message::ResultRows yet.
                        Ok(Some(response::encode_response(
                            &Message::Result(
                                cassandra_native_protocol::message::ResultMessage::Void,
                            ),
                            version,
                            stream_id,
                        )))
                    }
                    Err(e) => {
                        Ok(Some(response::error_frame(
                            version,
                            stream_id,
                            0x2200,
                            &e.to_string(), // INVALID
                        )))
                    }
                }
            }
            _ => Ok(Some(response::error_frame(
                version,
                stream_id,
                0x000A,
                "Message not supported in this state",
            ))),
        }
    }
}

pub struct NativeAuthWrapper {
    inner: Arc<dyn SecAuthenticator>,
}

impl NativeAuthWrapper {
    pub fn new(inner: Arc<dyn SecAuthenticator>) -> Self {
        Self { inner }
    }
}

impl NativeAuthenticator for NativeAuthWrapper {
    fn class_name(&self) -> &str {
        self.inner.name()
    }

    fn requires_auth(&self) -> bool {
        self.inner.require_authentication()
    }

    fn authenticate(&self, token: Option<&[u8]>) -> Result<AuthResult, String> {
        let token = token.ok_or("No authentication token provided")?;

        let parts: Vec<&[u8]> = token.splitn(3, |&b| b == 0).collect();
        if parts.len() < 3 {
            return Err("Invalid PLAIN credentials format".to_string());
        }

        let username = std::str::from_utf8(parts[1]).map_err(|_| "Invalid UTF-8 in username")?;
        let password = std::str::from_utf8(parts[2]).map_err(|_| "Invalid UTF-8 in password")?;

        let creds = Credentials {
            username: username.to_string(),
            password: password.to_string(),
            source_address: None,
        };

        match self.inner.authenticate(&creds) {
            Ok(_) => Ok(AuthResult::Success(Some(username.to_string()), None)),
            Err(e) => Err(e.to_string()),
        }
    }
}
