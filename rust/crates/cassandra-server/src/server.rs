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

use cassandra_native_protocol::auth::{AuthResult, Authenticator as NativeAuthenticator};
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
                            let lifecycle_resp =
                                ctx.process_lifecycle(&frame, self.authenticator.as_ref())?;
                            if lifecycle_resp.is_none() {
                                // Protocol lifecycle didn't handle it, must be a query.
                                if let Some(ref session) = trace_session {
                                    session.trace("server", "Dispatching query to executor");
                                }
                                let resp_opt = self.handle_query(&mut ctx, &mut client_state, &frame).await?;
                                if let Some(ref session) = trace_session {
                                    session.trace("server", "Query execution completed");
                                }
                                resp_opt
                            } else {
                                lifecycle_resp.map(|frame| (frame, Vec::new()))
                            }
                        };

                        if let Some((mut r_frame, warnings)) = response_frame {
                            let tracing_id = trace_session.as_ref().map(|s| s.session_id);
                            r_frame = ctx.wrap_response(r_frame, tracing_id, &warnings, None);

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
    ) -> anyhow::Result<Option<(Frame, Vec<String>)>> {
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
                    Err(e) => Ok(Some((
                        self.cassandra_error_to_frame(e, version, stream_id),
                        Vec::new(),
                    ))),
                }
            }
            Message::Prepare(p) => {
                let keyspace = p.keyspace.or_else(|| ctx.keyspace.clone());
                match self
                    .query_processor
                    .process_prepare(&p.query, keyspace.as_deref())
                {
                    Ok(prepared_result) => Ok(Some((
                        response::encode_response(
                            &Message::Result(ResultMessage::Prepared(prepared_result)),
                            version,
                            stream_id,
                        ),
                        Vec::new(),
                    ))),
                    Err(e) => Ok(Some((
                        self.cassandra_error_to_frame(e, version, stream_id),
                        Vec::new(),
                    ))),
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
                        // TODO: propagate metadata_changed and new_metadata_id into the
                        // response frame flags once the protocol encoder supports it.
                        Ok(Some(self.encode_query_result(
                            exec_result.result,
                            ctx,
                            client_state,
                            version,
                            stream_id,
                        )))
                    }
                    Err(err) => Ok(Some((
                        self.cassandra_error_to_frame(err, version, stream_id),
                        Vec::new(),
                    ))),
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
                    Err(err) => Ok(Some((
                        self.cassandra_error_to_frame(err, version, stream_id),
                        Vec::new(),
                    ))),
                }
            }
            _ => Ok(Some((
                response::error_frame(
                    version,
                    stream_id,
                    0x000A,
                    "Message not supported in this state",
                ),
                Vec::new(),
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
    ) -> (Frame, Vec<String>) {
        use cassandra_native_protocol::message::*;
        use cassandra_native_protocol::response;

        match result {
            QueryResult::Void => (
                response::encode_response(
                    &Message::Result(ResultMessage::Void),
                    version,
                    stream_id,
                ),
                Vec::new(),
            ),
            QueryResult::SetKeyspace(ks) => {
                ctx.keyspace = Some(ks.clone());
                client_state.set_keyspace(ks.clone());
                (
                    response::encode_response(
                        &Message::Result(ResultMessage::SetKeyspace(ks)),
                        version,
                        stream_id,
                    ),
                    Vec::new(),
                )
            }
            QueryResult::SchemaChange {
                change_type,
                target,
                keyspace,
                name,
            } => (
                response::encode_response(
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
                Vec::new(),
            ),
            QueryResult::Rows {
                columns,
                rows,
                paging_state,
                warnings,
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
                if paging_state.is_some() {
                    flags |= rows_flags::HAS_MORE_PAGES;
                }

                let metadata = RowsMetadata {
                    flags,
                    columns_count: col_specs.len() as i32,
                    paging_state,
                    new_metadata_id: None,
                    global_table_spec: global_spec,
                    col_specs,
                };

                let rows_count = rows.len() as i32;
                (
                    response::encode_response(
                        &Message::Result(ResultMessage::Rows(RowsResult {
                            metadata,
                            rows_count,
                            rows,
                        })),
                        version,
                        stream_id,
                    ),
                    warnings,
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::time::Duration;

    use cassandra_native_protocol::message::{ColumnType, rows_flags};
    use cassandra_native_protocol::types;
    use cassandra_schema::{KeyspaceMetadata, KeyspaceParams, SchemaCatalog};
    use cassandra_security::audit::NoOpAuditLogger;
    use cassandra_security::fql::FqlLogger;
    use cassandra_security::{AllowAllAuthorizer, InMemoryRoleManager};
    use cassandra_storage::engine::{EngineConfig, StorageEngine};
    use parking_lot::RwLock;
    use tempfile::tempdir;

    fn make_server() -> (NativeServer, tempfile::TempDir) {
        let tmp = tempdir().unwrap();
        let engine = Arc::new(
            StorageEngine::open(EngineConfig {
                data_directories: vec![tmp.path().join("data")],
                ..EngineConfig::default()
            })
            .unwrap(),
        );
        let catalog = Arc::new(RwLock::new(
            SchemaCatalog::new()
                .with_keyspace(KeyspaceMetadata::new("ks", KeyspaceParams::default())),
        ));
        let executor = Arc::new(QueryExecutor::new(
            engine,
            catalog,
            Arc::new(InMemoryRoleManager::new()),
            Arc::new(AllowAllAuthorizer),
        ));
        let server = NativeServer::new(
            ServerConfig {
                listen_address: "127.0.0.1:9042".to_string(),
                client_encryption_enabled: false,
            },
            executor,
            Arc::new(cassandra_native_protocol::auth::AllowAllAuthenticator),
            None,
            Arc::new(FqlLogger::new(tmp.path().join("fql"), 1, false).unwrap()),
            Arc::new(NoOpAuditLogger),
            Arc::new(ResourceLimits::new(-1, -1, 1024 * 1024)),
            Arc::new(TransportMetrics::new()),
            Arc::new(ShutdownCoordinator::new(Duration::from_secs(1))),
        );
        (server, tmp)
    }

    #[test]
    fn encode_query_result_sets_has_more_pages_and_returns_warnings() {
        let (server, _tmp) = make_server();
        let mut ctx = ConnectionContext::new();
        let mut client_state = ClientState::new("127.0.0.1:9042".parse().unwrap());

        let (frame, warnings) = server.encode_query_result(
            QueryResult::Rows {
                columns: vec![crate::executor::ResultColumn {
                    keyspace: "ks".to_string(),
                    table: "tbl".to_string(),
                    name: "ck".to_string(),
                    cql_type: cassandra_types::CqlType::Varchar,
                }],
                rows: vec![vec![Some(b"c".to_vec())]],
                paging_state: Some(vec![1, 2, 3, 4]),
                warnings: vec!["tombstone warning".to_string()],
            },
            &mut ctx,
            &mut client_state,
            4,
            7,
        );

        assert_eq!(warnings, vec!["tombstone warning".to_string()]);

        let mut body: &[u8] = &frame.body;
        let kind = types::read_int(&mut body).unwrap();
        assert_eq!(kind, 0x0002);
        let flags = types::read_int(&mut body).unwrap();
        assert_ne!(flags & rows_flags::HAS_MORE_PAGES, 0);
        let columns_count = types::read_int(&mut body).unwrap();
        assert_eq!(columns_count, 1);
        let paging_state = types::read_bytes(&mut body).unwrap();
        assert_eq!(paging_state, Some(vec![1, 2, 3, 4]));
        let ks = types::read_string(&mut body).unwrap();
        let table = types::read_string(&mut body).unwrap();
        assert_eq!(ks, "ks");
        assert_eq!(table, "tbl");
        let name = types::read_string(&mut body).unwrap();
        assert_eq!(name, "ck");
        let col_type_id = types::read_short(&mut body).unwrap();
        assert_eq!(col_type_id, ColumnType::Varchar.id());
    }
}
