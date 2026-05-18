// Licensed under Apache License, Version 2.0.

//! Native Protocol TCP Server.
//!
//! Listens for incoming CQL client connections (typically port 9042).
//! Integrates connection resource limits, backpressure, metrics, and
//! graceful shutdown via `tokio::select!` + `CancellationToken`.

use bytes::BytesMut;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tracing::{debug, error, info, warn};

use cassandra_native_protocol::auth::{
    AuthResult, Authenticator as NativeAuthenticator, parse_plain_credentials,
};
use cassandra_native_protocol::connection::ConnectionContext;
use cassandra_native_protocol::frame::{Frame, FrameCodec};
use cassandra_security::audit::{AuditEvent, AuditEventType, AuditLogger, AuditStatus};
use cassandra_security::auth::{Authenticator as SecAuthenticator, Credentials};
use cassandra_security::fql::{FqlLogger, FqlRecord};
use cassandra_security::tls::ReloadableTlsAcceptor;
use tokio_util::codec::{Decoder, Encoder};

use cassandra_cql::prepared::PreparedCache;

use crate::client_state::ClientState;
use crate::executor::{QueryExecutor, QueryResult};
use crate::query_processor::QueryProcessor;
use crate::resource_limits::ResourceLimits;
use crate::shutdown::ShutdownCoordinator;
use crate::transport_metrics::TransportMetrics;

pub struct ServerConfig {
    pub listen_address: String,
    pub client_encryption_enabled: bool,
}

pub struct NativeServer {
    config: ServerConfig,
    executor: Arc<QueryExecutor>,
    query_processor: Arc<QueryProcessor>,
    prepared_cache: Arc<PreparedCache>,
    authenticator: Arc<dyn NativeAuthenticator>,
    tls_acceptor: Option<ReloadableTlsAcceptor>,
    fql_logger: Arc<FqlLogger>,
    audit_logger: Arc<dyn AuditLogger>,
    resource_limits: Arc<ResourceLimits>,
    metrics: Arc<TransportMetrics>,
    shutdown: Arc<ShutdownCoordinator>,
}

impl NativeServer {
    pub fn new(
        config: ServerConfig,
        executor: Arc<QueryExecutor>,
        authenticator: Arc<dyn NativeAuthenticator>,
        tls_acceptor: Option<ReloadableTlsAcceptor>,
        fql_logger: Arc<FqlLogger>,
        audit_logger: Arc<dyn AuditLogger>,
        resource_limits: Arc<ResourceLimits>,
        metrics: Arc<TransportMetrics>,
        shutdown: Arc<ShutdownCoordinator>,
    ) -> Self {
        let prepared_cache = Arc::new(PreparedCache::new());
        let catalog = executor.catalog();
        let query_processor = Arc::new(QueryProcessor::new(
            Arc::clone(&executor),
            Arc::clone(&prepared_cache),
            catalog,
        ));
        Self {
            config,
            executor,
            query_processor,
            prepared_cache,
            authenticator,
            tls_acceptor,
            fql_logger,
            audit_logger,
            resource_limits,
            metrics,
            shutdown,
        }
    }

    /// Returns a reference to the transport metrics.
    pub fn metrics(&self) -> &TransportMetrics {
        &self.metrics
    }

    pub async fn run(self: Arc<Self>) -> anyhow::Result<()> {
        let listener = TcpListener::bind(&self.config.listen_address).await?;
        info!(
            "Native protocol server listening on {}",
            self.config.listen_address
        );

        if self.config.client_encryption_enabled && self.tls_acceptor.is_none() {
            warn!("Client encryption is enabled but no TLS acceptor was provided!");
        }

        let cancel = self.shutdown.token();

        loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    info!("Accept loop shutting down");
                    return Ok(());
                }
                result = listener.accept() => {
                    let (stream, addr) = match result {
                        Ok(res) => res,
                        Err(e) => {
                            error!("Error accepting connection: {}", e);
                            continue;
                        }
                    };

                    // Acquire connection permit
                    let permit = match self.resource_limits.try_acquire_connection(addr.ip()) {
                        Some(p) => p,
                        None => {
                            self.metrics.connection_rejected();
                            warn!(%addr, "Connection rejected — resource limit reached");
                            drop(stream);
                            continue;
                        }
                    };

                    self.metrics.connection_accepted(addr.ip());
                    debug!("Accepted connection from {}", addr);
                    let server = Arc::clone(&self);

                    tokio::spawn(async move {
                        let _permit = permit; // RAII — released on drop
                        if let Some(ref tls_acceptor) = server.tls_acceptor {
                            let acceptor = tls_acceptor.acceptor();
                            match acceptor.accept(stream).await {
                                Ok(tls_stream) => {
                                    if let Err(e) = server.handle_connection(tls_stream, addr).await {
                                        debug!("Connection {} error: {}", addr, e);
                                    }
                                }
                                Err(e) => {
                                    error!("TLS handshake failed for {}: {}", addr, e);
                                }
                            }
                        } else if let Err(e) = server.handle_connection(stream, addr).await {
                            debug!("Connection {} error: {}", addr, e);
                        }
                        server.metrics.connection_closed(addr.ip());
                    });
                }
            }
        }
    }

    async fn handle_connection<S>(&self, mut stream: S, addr: SocketAddr) -> anyhow::Result<()>
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        let mut ctx = ConnectionContext::new();
        let mut client_state = ClientState::new(addr);
        let mut buffer = BytesMut::with_capacity(8192);
        let cancel = self.shutdown.token();

        loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    debug!(%addr, "Connection closing due to shutdown");
                    return Ok(());
                }
                result = stream.read_buf(&mut buffer) => {
                    let n = result?;
                    if n == 0 {
                        return Ok(()); // Client closed connection
                    }
                    self.metrics.record_bytes_in(n as u64);

                    // Parse as many frames as we can from the buffer
                    while let Some(frame) = self.decode_frame(&mut buffer)? {
                        let _guard = self.shutdown.track_request();
                        client_state.increment_request_count();
                        self.metrics.request_processed(Some(&addr.ip()));

                        let request_tracing = frame.header.flags & cassandra_native_protocol::frame::flags::TRACING != 0;
                        let trace_session = if request_tracing {
                            Some(cassandra_coordinator::tracing::TraceSession::new())
                        } else {
                            None
                        };

                        let response_frame = {
                            let mut resp_opt =
                                ctx.process_lifecycle(&frame, self.authenticator.as_ref())?;
                            if resp_opt.is_none() {
                                // Protocol lifecycle didn't handle it, must be a query.
                                if let Some(ref session) = trace_session {
                                    session.trace("server", "Dispatching query to executor");
                                }
                                resp_opt = self.handle_query(&mut ctx, &mut client_state, &frame).await?;
                                if let Some(ref session) = trace_session {
                                    session.trace("server", "Query execution completed");
                                }
                            }
                            resp_opt
                        };

                        if let Some(mut r_frame) = response_frame {
                            let tracing_id = trace_session.as_ref().map(|s| s.session_id);
                            r_frame = ctx.wrap_response(r_frame, tracing_id, &[], None);

                            let mut out_buf = bytes::BytesMut::new();
                            let mut codec = FrameCodec;
                            codec.encode(r_frame, &mut out_buf)?;
                            self.metrics.record_bytes_out(out_buf.len() as u64);
                            stream.write_all(&out_buf).await?;
                        }
                    }
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
        client_state: &mut ClientState,
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
                        consistency_level: 1,
                        query: cql.clone(),
                        bind_values: vec![],
                    };
                    if let Err(e) = self.fql_logger.log_query(&record) {
                        tracing::warn!("Failed to append to FQL log: {}", e);
                    }
                }

                // Execute via QueryProcessor
                let result = self.query_processor.process_query(
                    &cql,
                    &q.params,
                    ctx.authenticated_user.as_deref(),
                    ctx.keyspace.as_deref(),
                );

                // 2. Audit Logging
                if self.audit_logger.is_enabled() {
                    let status = if result.is_ok() {
                        AuditStatus::Success
                    } else {
                        AuditStatus::Failure
                    };
                    let source = client_state.remote_address().to_string();
                    let mut event = AuditEvent::now(AuditEventType::Query, &user_str, &source);
                    event.query = Some(cql.clone());
                    event.keyspace = ctx.keyspace.clone();
                    event.status = status;
                    self.audit_logger.log(&event);
                }

                match result {
                    Ok(qr) => Ok(Some(self.encode_query_result(
                        qr,
                        ctx,
                        client_state,
                        version,
                        stream_id,
                    ))),
                    Err(e) => Ok(Some(self.cassandra_error_to_frame(e, version, stream_id))),
                }
            }
            Message::Prepare(p) => {
                let keyspace = p.keyspace.or_else(|| ctx.keyspace.clone());
                match self
                    .query_processor
                    .process_prepare(&p.query, keyspace.as_deref())
                {
                    Ok(prepared_result) => Ok(Some(response::encode_response(
                        &Message::Result(ResultMessage::Prepared(prepared_result)),
                        version,
                        stream_id,
                    ))),
                    Err(e) => Ok(Some(self.cassandra_error_to_frame(e, version, stream_id))),
                }
            }
            Message::Execute(e) => {
                match self.query_processor.process_execute(
                    &e.id,
                    &e.params,
                    ctx.authenticated_user.as_deref(),
                    ctx.keyspace.as_deref(),
                ) {
                    Ok(exec_result) => {
                        if exec_result.metadata_changed {
                            debug!(
                                "METADATA_CHANGED detected for execute; new_metadata_id present={}",
                                exec_result.new_metadata_id.is_some()
                            );
                        }
                        Ok(Some(self.encode_query_result_with_metadata_change(
                            exec_result.result,
                            ctx,
                            client_state,
                            version,
                            stream_id,
                            exec_result.metadata_changed,
                            exec_result.new_metadata_id,
                        )))
                    }
                    Err(err) => Ok(Some(self.cassandra_error_to_frame(err, version, stream_id))),
                }
            }
            Message::Batch(b) => {
                match self.query_processor.process_batch(
                    &b,
                    ctx.authenticated_user.as_deref(),
                    ctx.keyspace.as_deref(),
                ) {
                    Ok(qr) => Ok(Some(self.encode_query_result(
                        qr,
                        ctx,
                        client_state,
                        version,
                        stream_id,
                    ))),
                    Err(err) => Ok(Some(self.cassandra_error_to_frame(err, version, stream_id))),
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

    /// Encode a `QueryResult` into a protocol response frame.
    fn encode_query_result(
        &self,
        result: QueryResult,
        ctx: &mut ConnectionContext,
        client_state: &mut ClientState,
        version: u8,
        stream_id: i16,
    ) -> Frame {
        self.encode_query_result_with_metadata_change(
            result,
            ctx,
            client_state,
            version,
            stream_id,
            false,
            None,
        )
    }

    /// Encode a `QueryResult` into a protocol response frame with execute-time
    /// result metadata change information.
    fn encode_query_result_with_metadata_change(
        &self,
        result: QueryResult,
        ctx: &mut ConnectionContext,
        client_state: &mut ClientState,
        version: u8,
        stream_id: i16,
        metadata_changed: bool,
        new_metadata_id: Option<Vec<u8>>,
    ) -> Frame {
        use cassandra_native_protocol::message::*;
        use cassandra_native_protocol::response;

        match result {
            QueryResult::Void => {
                response::encode_response(&Message::Result(ResultMessage::Void), version, stream_id)
            }
            QueryResult::SetKeyspace(ks) => {
                ctx.keyspace = Some(ks.clone());
                client_state.set_keyspace(ks.clone());
                response::encode_response(
                    &Message::Result(ResultMessage::SetKeyspace(ks)),
                    version,
                    stream_id,
                )
            }
            QueryResult::SchemaChange {
                change_type,
                target,
                keyspace,
                name,
            } => response::encode_response(
                &Message::Result(ResultMessage::SchemaChange(SchemaChange {
                    change_type,
                    target,
                    keyspace,
                    name,
                    arg_types: None,
                })),
                version,
                stream_id,
            ),
            QueryResult::Rows {
                columns,
                rows,
                paging_state,
                warnings: _warnings,
            } => {
                // Convert ResultColumn → ColumnSpec
                let col_specs: Vec<ColumnSpec> = columns
                    .iter()
                    .map(|rc| ColumnSpec {
                        ksname: Some(rc.keyspace.clone()),
                        tablename: Some(rc.table.clone()),
                        name: rc.name.clone(),
                        col_type: ColumnType::from_cql_type(&rc.cql_type),
                    })
                    .collect();

                // Detect global table spec optimization
                let global_spec = if !col_specs.is_empty() {
                    let first_ks = col_specs[0].ksname.as_deref();
                    let first_tbl = col_specs[0].tablename.as_deref();
                    if col_specs.iter().all(|s| {
                        s.ksname.as_deref() == first_ks && s.tablename.as_deref() == first_tbl
                    }) {
                        first_ks
                            .and_then(|ks| first_tbl.map(|tbl| (ks.to_string(), tbl.to_string())))
                    } else {
                        None
                    }
                } else {
                    None
                };

                let mut flags = 0i32;
                if global_spec.is_some() {
                    flags |= rows_flags::GLOBAL_TABLES_SPEC;
                }
                // WU-06: Set HAS_MORE_PAGES flag when paging state is present
                if paging_state.is_some() {
                    flags |= rows_flags::HAS_MORE_PAGES;
                }
                if metadata_changed {
                    flags |= rows_flags::METADATA_CHANGED;
                }

                let metadata = RowsMetadata {
                    flags,
                    columns_count: col_specs.len() as i32,
                    paging_state,
                    new_metadata_id,
                    global_table_spec: global_spec,
                    col_specs,
                };

                let rows_count = rows.len() as i32;
                response::encode_response(
                    &Message::Result(ResultMessage::Rows(RowsResult {
                        metadata,
                        rows_count,
                        rows,
                    })),
                    version,
                    stream_id,
                )
            }
        }
    }

    /// Convert a `CassandraError` to a protocol error frame with proper error code.
    fn cassandra_error_to_frame(
        &self,
        err: cassandra_common::CassandraError,
        version: u8,
        stream_id: i16,
    ) -> Frame {
        use cassandra_native_protocol::error_codes;
        use cassandra_native_protocol::message::Message;
        use cassandra_native_protocol::response;

        let error_msg = error_codes::error_to_message(&err);
        response::encode_response(&Message::Error(error_msg), version, stream_id)
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
        match self.inner.name() {
            "AllowAllAuthenticator" => "org.apache.cassandra.auth.AllowAllAuthenticator",
            "PasswordAuthenticator" => "org.apache.cassandra.auth.PasswordAuthenticator",
            other => other,
        }
    }

    fn requires_auth(&self) -> bool {
        self.inner.require_authentication()
    }

    fn authenticate(&self, token: Option<&[u8]>) -> Result<AuthResult, String> {
        let token = token.ok_or("No authentication token provided")?;
        let parsed = parse_plain_credentials(token)?;

        let creds = Credentials {
            username: parsed.username,
            password: parsed.password,
            source_address: None,
        };

        match self.inner.authenticate(&creds) {
            Ok(user) => Ok(AuthResult::Success(Some(user.role_name), None)),
            Err(e) => Err(e.to_string()),
        }
    }
}

#[cfg(test)]
mod native_auth_wrapper_tests {
    use super::*;
    use cassandra_security::auth::PasswordAuthenticator as SecurityPasswordAuthenticator;
    use cassandra_security::roles::InMemoryRoleManager;

    #[test]
    fn native_auth_wrapper_reports_java_password_class_name() {
        let auth = NativeAuthWrapper::new(Arc::new(SecurityPasswordAuthenticator::new(
            InMemoryRoleManager::new(),
        )));

        assert_eq!(
            auth.class_name(),
            "org.apache.cassandra.auth.PasswordAuthenticator"
        );
    }

    #[test]
    fn native_auth_wrapper_verifies_plain_credentials() {
        let auth = NativeAuthWrapper::new(Arc::new(SecurityPasswordAuthenticator::new(
            InMemoryRoleManager::new(),
        )));

        let result = auth.authenticate(Some(b"\0cassandra\0cassandra")).unwrap();
        assert!(matches!(
            result,
            AuthResult::Success(Some(ref user), None) if user == "cassandra"
        ));
        assert!(auth.authenticate(Some(b"\0cassandra\0wrong")).is_err());
    }
}
