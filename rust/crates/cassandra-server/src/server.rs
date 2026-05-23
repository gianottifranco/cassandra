// Licensed under Apache License, Version 2.0.

//! Native Protocol TCP Server.
//!
//! Listens for incoming CQL client connections (typically port 9042).
//! Integrates connection resource limits, backpressure, metrics, and
//! graceful shutdown via `tokio::select!` + `CancellationToken`.

use bytes::{Bytes, BytesMut};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tracing::{debug, error, info, warn};

use cassandra_cluster_metadata::tcm::listeners::{
    ChangeListener, ListenerRegistry, MetadataChangeEvent,
};
use cassandra_cluster_metadata::{NodeId, NodeState, Transformation};
use cassandra_native_protocol::auth::{
    AuthResult, Authenticator as NativeAuthenticator, parse_plain_credentials,
};
use cassandra_native_protocol::connection::ConnectionContext;
use cassandra_native_protocol::event_dispatcher::EventDispatcher;
use cassandra_native_protocol::frame::{
    Frame, FrameCodec, FrameHeader, Opcode, PROTOCOL_V4, flags,
};
use cassandra_native_protocol::message::EventMessage;
use cassandra_security::audit::{AuditEvent, AuditEventType, AuditLogger, AuditStatus};
use cassandra_security::auth::{Authenticator as SecAuthenticator, Credentials};
use cassandra_security::fql::{FqlLogger, FqlRecord};
use cassandra_security::tls::ReloadableTlsAcceptor;
use parking_lot::RwLock;
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
    pub max_frame_size: u64,
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
    event_dispatcher: Arc<EventDispatcher>,
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
        let event_dispatcher = Arc::new(EventDispatcher::new());
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
            event_dispatcher,
        }
    }

    /// Returns a reference to the transport metrics.
    pub fn metrics(&self) -> &TransportMetrics {
        &self.metrics
    }

    /// Returns the server-wide native-protocol event dispatcher.
    pub fn event_dispatcher(&self) -> &Arc<EventDispatcher> {
        &self.event_dispatcher
    }

    /// Register a TCM topology listener that republishes metadata changes as
    /// native-protocol push events for clients subscribed through REGISTER.
    pub fn register_topology_event_listener(
        &self,
        registry: &ListenerRegistry,
    ) -> Arc<NativeProtocolTopologyListener> {
        let listener = Arc::new(NativeProtocolTopologyListener::new(Arc::clone(
            &self.event_dispatcher,
        )));
        registry.register(listener.clone());
        listener
    }

    /// Publish a native protocol STATUS_CHANGE event.
    pub fn publish_status_change(&self, change: impl Into<String>, addr: SocketAddr) {
        self.event_dispatcher.dispatch(
            EventMessage::StatusChange {
                change: change.into(),
                addr: (addr.ip(), addr.port() as u32),
            },
            PROTOCOL_V4,
        );
    }

    /// Publish a native protocol TOPOLOGY_CHANGE event.
    pub fn publish_topology_change(&self, change: impl Into<String>, addr: SocketAddr) {
        self.event_dispatcher.dispatch(
            EventMessage::TopologyChange {
                change: change.into(),
                addr: (addr.ip(), addr.port() as u32),
            },
            PROTOCOL_V4,
        );
    }

    pub async fn run(self: Arc<Self>) -> anyhow::Result<()> {
        let listener = TcpListener::bind(&self.config.listen_address).await?;
        let listen_addr = listener.local_addr()?;
        info!("Native protocol server listening on {}", listen_addr);
        self.publish_status_change("UP", listen_addr);

        if self.config.client_encryption_enabled && self.tls_acceptor.is_none() {
            warn!("Client encryption is enabled but no TLS acceptor was provided!");
        }

        let cancel = self.shutdown.token();

        loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    info!("Accept loop shutting down");
                    self.publish_status_change("DOWN", listen_addr);
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

    async fn handle_connection<S>(&self, stream: S, addr: SocketAddr) -> anyhow::Result<()>
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        let (mut reader, mut writer) = tokio::io::split(stream);
        let mut ctx = ConnectionContext::new();
        let mut client_state = ClientState::new(addr);
        let mut buffer = BytesMut::with_capacity(8192);
        let cancel = self.shutdown.token();
        let mut event_rx = self.event_dispatcher.subscribe();

        loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    debug!(%addr, "Connection closing due to shutdown");
                    return Ok(());
                }
                event = event_rx.recv() => {
                    let event = match event {
                        Ok(event) => event,
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                            warn!(%addr, skipped, "Native protocol client lagged event stream");
                            continue;
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => return Ok(()),
                    };
                    if ctx.registered_events.contains(&event.event_type) {
                        let mut out_buf = bytes::BytesMut::new();
                        let mut codec = FrameCodec;
                        let event_frame =
                            self.encode_outgoing_frame(&ctx, event.frame_for_version(ctx.protocol_version))?;
                        codec.encode(event_frame, &mut out_buf)?;
                        self.metrics.record_bytes_out(out_buf.len() as u64);
                        writer.write_all(&out_buf).await?;
                    }
                }
                result = reader.read_buf(&mut buffer) => {
                    let n = result?;
                    if n == 0 {
                        return Ok(()); // Client closed connection
                    }
                    self.metrics.record_bytes_in(n as u64);

                    // Parse as many frames as we can from the buffer
                    if let Some(error_frame) = self.reject_unknown_opcode_frame(&buffer) {
                        let mut out_buf = bytes::BytesMut::new();
                        let mut codec = FrameCodec;
                        codec.encode(error_frame, &mut out_buf)?;
                        self.metrics.record_bytes_out(out_buf.len() as u64);
                        writer.write_all(&out_buf).await?;
                        return Ok(());
                    }
                    if let Some(error_frame) = self.reject_oversized_frame(&buffer) {
                        let mut out_buf = bytes::BytesMut::new();
                        let mut codec = FrameCodec;
                        codec.encode(error_frame, &mut out_buf)?;
                        self.metrics.record_bytes_out(out_buf.len() as u64);
                        writer.write_all(&out_buf).await?;
                        return Ok(());
                    }
                    while let Some(frame) = self.decode_frame(&mut buffer)? {
                        let frame = match self.decode_incoming_frame(&ctx, frame) {
                            Ok(frame) => frame,
                            Err(error_frame) => {
                                let mut out_buf = bytes::BytesMut::new();
                                let mut codec = FrameCodec;
                                codec.encode(error_frame, &mut out_buf)?;
                                self.metrics.record_bytes_out(out_buf.len() as u64);
                                writer.write_all(&out_buf).await?;
                                continue;
                            }
                        };
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
                            r_frame = self.encode_outgoing_frame(&ctx, r_frame)?;

                            let mut out_buf = bytes::BytesMut::new();
                            let mut codec = FrameCodec;
                            codec.encode(r_frame, &mut out_buf)?;
                            self.metrics.record_bytes_out(out_buf.len() as u64);
                            writer.write_all(&out_buf).await?;
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

    fn reject_unknown_opcode_frame(&self, buffer: &BytesMut) -> Option<Frame> {
        if buffer.len() < FrameHeader::SIZE {
            return None;
        }

        let opcode = buffer[4];
        if Opcode::from_u8(opcode).is_ok() {
            return None;
        }

        let raw_version = buffer[0];
        let protocol_version = raw_version & 0x7F;
        let response_version = if protocol_version == 4 || protocol_version == 5 {
            protocol_version
        } else {
            cassandra_native_protocol::frame::PROTOCOL_V5
        };
        let stream_id = i16::from_be_bytes([buffer[2], buffer[3]]);

        Some(cassandra_native_protocol::response::error_frame(
            response_version,
            stream_id,
            0x000A,
            &format!("Unknown opcode: 0x{opcode:02X}"),
        ))
    }

    fn reject_oversized_frame(&self, buffer: &BytesMut) -> Option<Frame> {
        if buffer.len() < FrameHeader::SIZE {
            return None;
        }
        let header = match FrameHeader::decode(&buffer[..FrameHeader::SIZE]) {
            Ok(header) => header,
            Err(_) => return None,
        };
        if u64::from(header.length) <= self.config.max_frame_size {
            return None;
        }

        Some(cassandra_native_protocol::response::error_frame(
            header.protocol_version(),
            header.stream_id,
            0x000A,
            &format!(
                "Frame payload length {} exceeds configured native_transport_max_frame_size {}",
                header.length, self.config.max_frame_size
            ),
        ))
    }

    fn decode_incoming_frame(
        &self,
        ctx: &ConnectionContext,
        mut frame: Frame,
    ) -> Result<Frame, Frame> {
        if frame.header.flags & flags::COMPRESSION == 0 {
            return Ok(frame);
        }

        let Some(compression) = ctx.compression else {
            return Err(cassandra_native_protocol::response::error_frame(
                frame.header.protocol_version(),
                frame.header.stream_id,
                0x000A,
                "Compressed frame received before compression was negotiated",
            ));
        };

        match compression.decompress(&frame.body) {
            Ok(body) => {
                frame.header.flags &= !flags::COMPRESSION;
                frame.header.length = body.len() as u32;
                frame.body = Bytes::from(body);
                Ok(frame)
            }
            Err(e) => Err(cassandra_native_protocol::response::error_frame(
                frame.header.protocol_version(),
                frame.header.stream_id,
                0x000A,
                &format!("Invalid compressed frame body: {e}"),
            )),
        }
    }

    fn encode_outgoing_frame(
        &self,
        ctx: &ConnectionContext,
        mut frame: Frame,
    ) -> anyhow::Result<Frame> {
        let Some(compression) = ctx.compression else {
            return Ok(frame);
        };

        let body = compression.compress(&frame.body)?;
        frame.header.flags |= flags::COMPRESSION;
        frame.header.length = body.len() as u32;
        frame.body = Bytes::from(body);
        Ok(frame)
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

        let version = ctx.protocol_version;
        let stream_id = frame.header.stream_id;
        let msg = match request::decode_request(frame) {
            Ok(msg) => msg,
            Err(e) => {
                return Ok(Some(response::error_frame(
                    version,
                    stream_id,
                    0x000A,
                    &format!("Invalid request body: {e}"),
                )));
            }
        };

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
                    e.result_metadata_id.as_deref(),
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
                            exec_result.omit_result_metadata,
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
            false,
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
        omit_result_metadata: bool,
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
            } => {
                let schema_change = SchemaChange {
                    change_type,
                    target,
                    keyspace,
                    name,
                    arg_types: None,
                };
                self.event_dispatcher
                    .dispatch(EventMessage::SchemaChange(schema_change.clone()), version);
                response::encode_response(
                    &Message::Result(ResultMessage::SchemaChange(schema_change)),
                    version,
                    stream_id,
                )
            }
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
                if omit_result_metadata && !metadata_changed {
                    flags |= rows_flags::NO_METADATA;
                }

                let metadata = RowsMetadata {
                    flags,
                    columns_count: col_specs.len() as i32,
                    paging_state,
                    new_metadata_id,
                    global_table_spec: if flags & rows_flags::NO_METADATA == 0 {
                        global_spec
                    } else {
                        None
                    },
                    col_specs: if flags & rows_flags::NO_METADATA == 0 {
                        col_specs
                    } else {
                        Vec::new()
                    },
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

/// Bridges TCM metadata change notifications to native protocol server-push events.
///
/// Java Cassandra emits topology/status events from storage service and gossip
/// change points. The Rust rewrite's source of truth for membership changes is
/// TCM, so this listener maps committed topology transformations into the
/// protocol-level events consumed by registered clients.
pub struct NativeProtocolTopologyListener {
    dispatcher: Arc<EventDispatcher>,
    endpoints: RwLock<HashMap<NodeId, SocketAddr>>,
}

impl NativeProtocolTopologyListener {
    pub fn new(dispatcher: Arc<EventDispatcher>) -> Self {
        Self {
            dispatcher,
            endpoints: RwLock::new(HashMap::new()),
        }
    }

    /// Seed or update the endpoint cache for transformations that only carry a
    /// node id, such as state changes and unregister events.
    pub fn remember_node_endpoint(&self, node_id: NodeId, addr: SocketAddr) {
        self.endpoints.write().insert(node_id, addr);
    }

    fn dispatch_topology(&self, change: &str, addr: SocketAddr) {
        self.dispatcher.dispatch(
            EventMessage::TopologyChange {
                change: change.to_string(),
                addr: (addr.ip(), addr.port() as u32),
            },
            PROTOCOL_V4,
        );
    }

    fn dispatch_status(&self, change: &str, addr: SocketAddr) {
        self.dispatcher.dispatch(
            EventMessage::StatusChange {
                change: change.to_string(),
                addr: (addr.ip(), addr.port() as u32),
            },
            PROTOCOL_V4,
        );
    }

    fn endpoint_for(&self, node_id: &NodeId) -> Option<SocketAddr> {
        self.endpoints.read().get(node_id).copied()
    }
}

impl ChangeListener for NativeProtocolTopologyListener {
    fn on_change(&self, event: &MetadataChangeEvent) {
        let MetadataChangeEvent::TopologyChanged { transformation, .. } = event else {
            return;
        };

        match transformation {
            Transformation::Register {
                node_id, endpoint, ..
            } => {
                let addr = endpoint.addr();
                self.remember_node_endpoint(*node_id, addr);
                self.dispatch_topology("NEW_NODE", addr);
            }
            Transformation::Unregister { node_id } => {
                if let Some(addr) = self.endpoints.write().remove(node_id) {
                    self.dispatch_topology("REMOVED_NODE", addr);
                }
            }
            Transformation::AssignTokens { node_id, .. } => {
                if let Some(addr) = self.endpoint_for(node_id) {
                    self.dispatch_topology("MOVED_NODE", addr);
                }
            }
            Transformation::UpdateNodeState { node_id, state } => {
                let Some(addr) = self.endpoint_for(node_id) else {
                    return;
                };
                match state {
                    NodeState::Normal => self.dispatch_status("UP", addr),
                    NodeState::Dead | NodeState::Left => self.dispatch_status("DOWN", addr),
                    NodeState::Joining
                    | NodeState::Leaving
                    | NodeState::Moving
                    | NodeState::Replacing => {}
                }
            }
            Transformation::SchemaChange { .. }
            | Transformation::LockRanges { .. }
            | Transformation::UnlockRanges { .. }
            | Transformation::ForceSnapshot => {}
        }
    }

    fn name(&self) -> &str {
        "NativeProtocolTopologyListener"
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
    use bytes::{Bytes, BytesMut};
    use cassandra_cluster_metadata::tcm::listeners::{ListenerRegistry, MetadataChangeEvent};
    use cassandra_cluster_metadata::{Endpoint, NodeId, NodeState, Transformation};
    use cassandra_native_protocol::auth::AllowAllAuthenticator;
    use cassandra_native_protocol::compress::Compression;
    use cassandra_native_protocol::frame::{
        FrameHeader, Opcode, PROTOCOL_V4, PROTOCOL_V5, RESPONSE_FLAG, flags,
    };
    use cassandra_native_protocol::message::{EventMessage, SchemaChange};
    use cassandra_native_protocol::types;
    use cassandra_schema::SchemaCatalog;
    use cassandra_security::audit::NoOpAuditLogger;
    use cassandra_security::auth::PasswordAuthenticator as SecurityPasswordAuthenticator;
    use cassandra_security::authz::AllowAllAuthorizer;
    use cassandra_security::roles::InMemoryRoleManager;
    use cassandra_storage::commitlog::CommitLogConfig;
    use cassandra_storage::engine::{EngineConfig, StorageEngine};
    use parking_lot::RwLock;
    use tempfile::TempDir;
    use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
    use tokio_util::codec::{Decoder, Encoder};
    use uuid::Uuid;

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

    fn request_frame_with_flags(
        opcode: Opcode,
        version: u8,
        flags: u8,
        stream_id: i16,
        body: Vec<u8>,
    ) -> Frame {
        Frame {
            header: FrameHeader {
                version,
                flags,
                stream_id,
                opcode,
                length: body.len() as u32,
            },
            body: Bytes::from(body),
        }
    }

    fn request_frame(opcode: Opcode, stream_id: i16, body: Vec<u8>) -> Frame {
        request_frame_with_flags(opcode, PROTOCOL_V4, 0, stream_id, body)
    }

    fn startup_frame() -> Frame {
        startup_frame_for(PROTOCOL_V4, 0)
    }

    fn startup_frame_for(version: u8, frame_flags: u8) -> Frame {
        let mut opts = std::collections::HashMap::new();
        opts.insert("CQL_VERSION".to_string(), "3.4.7".to_string());
        startup_frame_with_options(version, frame_flags, opts)
    }

    fn startup_frame_with_options(
        version: u8,
        frame_flags: u8,
        opts: std::collections::HashMap<String, String>,
    ) -> Frame {
        let mut body = BytesMut::new();
        types::write_string_map(&mut body, &opts);
        request_frame_with_flags(Opcode::Startup, version, frame_flags, 1, body.to_vec())
    }

    fn register_frame(events: &[&str]) -> Frame {
        register_frame_for(events, PROTOCOL_V4, 0)
    }

    fn register_frame_for(events: &[&str], version: u8, frame_flags: u8) -> Frame {
        let mut body = BytesMut::new();
        let events: Vec<String> = events.iter().map(|event| (*event).to_string()).collect();
        types::write_string_list(&mut body, &events);
        request_frame_with_flags(Opcode::Register, version, frame_flags, 2, body.to_vec())
    }

    fn query_frame_for(cql: &str, version: u8, frame_flags: u8, stream_id: i16) -> Frame {
        let mut body = BytesMut::new();
        types::write_long_string(&mut body, cql);
        types::write_consistency(
            &mut body,
            cassandra_native_protocol::types::Consistency::One,
        );
        if version >= PROTOCOL_V5 {
            types::write_int(&mut body, 0);
        } else {
            types::write_byte(&mut body, 0);
        }
        request_frame_with_flags(
            Opcode::Query,
            version,
            frame_flags,
            stream_id,
            body.to_vec(),
        )
    }

    fn prepare_frame_for(cql: &str, version: u8, frame_flags: u8, stream_id: i16) -> Frame {
        let mut body = BytesMut::new();
        types::write_long_string(&mut body, cql);
        if version >= PROTOCOL_V5 {
            types::write_int(&mut body, 0);
        }
        request_frame_with_flags(
            Opcode::Prepare,
            version,
            frame_flags,
            stream_id,
            body.to_vec(),
        )
    }

    fn prepare_frame_for_keyspace(
        cql: &str,
        keyspace: &str,
        version: u8,
        frame_flags: u8,
        stream_id: i16,
    ) -> Frame {
        let mut body = BytesMut::new();
        types::write_long_string(&mut body, cql);
        if version >= PROTOCOL_V5 {
            types::write_int(&mut body, 0x01);
            types::write_string(&mut body, keyspace);
        }
        request_frame_with_flags(
            Opcode::Prepare,
            version,
            frame_flags,
            stream_id,
            body.to_vec(),
        )
    }

    fn execute_frame_for(
        id: &[u8],
        result_metadata_id: &[u8],
        version: u8,
        frame_flags: u8,
        stream_id: i16,
    ) -> Frame {
        let mut body = BytesMut::new();
        types::write_short_bytes(&mut body, id);
        if version >= PROTOCOL_V5 {
            types::write_short_bytes(&mut body, result_metadata_id);
        }
        types::write_consistency(
            &mut body,
            cassandra_native_protocol::types::Consistency::One,
        );
        if version >= PROTOCOL_V5 {
            types::write_int(&mut body, 0);
        } else {
            types::write_byte(&mut body, 0);
        }
        request_frame_with_flags(
            Opcode::Execute,
            version,
            frame_flags,
            stream_id,
            body.to_vec(),
        )
    }

    fn batch_frame_for_keyspace(
        cql: &str,
        keyspace: &str,
        version: u8,
        frame_flags: u8,
        stream_id: i16,
    ) -> Frame {
        let mut body = BytesMut::new();
        types::write_byte(&mut body, 0); // logged
        types::write_short(&mut body, 1);
        types::write_byte(&mut body, 0); // raw CQL query
        types::write_long_string(&mut body, cql);
        types::write_short(&mut body, 0);
        types::write_consistency(
            &mut body,
            cassandra_native_protocol::types::Consistency::One,
        );
        if version >= PROTOCOL_V5 {
            types::write_int(
                &mut body,
                cassandra_native_protocol::message::query_flags::KEYSPACE as i32,
            );
            types::write_string(&mut body, keyspace);
        } else {
            types::write_byte(&mut body, 0);
        }
        request_frame_with_flags(
            Opcode::Batch,
            version,
            frame_flags,
            stream_id,
            body.to_vec(),
        )
    }

    fn result_kind(frame: &Frame) -> i32 {
        let mut body: &[u8] = &frame.body;
        types::read_int(&mut body).unwrap()
    }

    fn compressed_frame(mut frame: Frame, compression: Compression) -> Frame {
        let compressed = compression.compress(&frame.body).unwrap();
        frame.header.flags |= flags::COMPRESSION;
        frame.header.length = compressed.len() as u32;
        frame.body = Bytes::from(compressed);
        frame
    }

    fn decompressed_body(frame: &Frame, compression: Compression) -> Vec<u8> {
        if frame.header.flags & flags::COMPRESSION == 0 {
            frame.body.to_vec()
        } else {
            compression.decompress(&frame.body).unwrap()
        }
    }

    fn node_id(n: u128) -> NodeId {
        NodeId::from_uuid(Uuid::from_u128(n))
    }

    fn endpoint(port: u16) -> Endpoint {
        Endpoint::new(format!("127.0.0.1:{port}").parse().expect("valid endpoint"))
    }

    fn topology_event(node_id: NodeId, transformation: Transformation) -> MetadataChangeEvent {
        MetadataChangeEvent::TopologyChanged {
            epoch: cassandra_cluster_metadata::Epoch::FIRST,
            node_id,
            transformation,
        }
    }

    async fn recv_event(
        rx: &mut tokio::sync::broadcast::Receiver<
            cassandra_native_protocol::event_dispatcher::EventFrame,
        >,
    ) -> (String, String, SocketAddr) {
        let event = rx.recv().await.unwrap();
        let mut body: &[u8] = &event.frame.body;
        let event_type = types::read_string(&mut body).unwrap();
        let change = types::read_string(&mut body).unwrap();
        let (ip, port) = types::read_inet(&mut body).unwrap();
        (event_type, change, SocketAddr::new(ip, port as u16))
    }

    async fn write_frame<W>(writer: &mut W, frame: Frame)
    where
        W: AsyncWrite + Unpin,
    {
        let mut out = BytesMut::new();
        let mut codec = FrameCodec;
        codec.encode(frame, &mut out).unwrap();
        writer.write_all(&out).await.unwrap();
    }

    async fn read_frame<R>(reader: &mut R, buf: &mut BytesMut) -> Frame
    where
        R: AsyncRead + Unpin,
    {
        loop {
            let mut codec = FrameCodec;
            if let Some(frame) = codec.decode(buf).unwrap() {
                return frame;
            }
            let n = reader.read_buf(buf).await.unwrap();
            assert!(n > 0, "connection closed before frame was received");
        }
    }

    fn test_native_server() -> (Arc<NativeServer>, TempDir) {
        test_native_server_with_max_frame_size(256 * 1024 * 1024)
    }

    fn test_native_server_with_max_frame_size(max_frame_size: u64) -> (Arc<NativeServer>, TempDir) {
        let temp = TempDir::new().unwrap();
        let engine = Arc::new(
            StorageEngine::open(EngineConfig {
                data_directories: vec![temp.path().join("data")],
                commitlog: CommitLogConfig {
                    directory: temp.path().join("commitlog"),
                    ..CommitLogConfig::default()
                },
                ..EngineConfig::default()
            })
            .unwrap(),
        );
        let catalog = Arc::new(RwLock::new(SchemaCatalog::new()));
        let executor = Arc::new(QueryExecutor::new(
            engine,
            catalog,
            Arc::new(InMemoryRoleManager::new()),
            Arc::new(AllowAllAuthorizer),
        ));
        let fql_logger = Arc::new(FqlLogger::new(temp.path().join("fql"), 1, false).unwrap());
        let server = Arc::new(NativeServer::new(
            ServerConfig {
                listen_address: "127.0.0.1:0".to_string(),
                client_encryption_enabled: false,
                max_frame_size,
            },
            executor,
            Arc::new(AllowAllAuthenticator),
            None,
            fql_logger,
            Arc::new(NoOpAuditLogger),
            Arc::new(ResourceLimits::new(-1, -1, 1024 * 1024)),
            Arc::new(TransportMetrics::new()),
            Arc::new(ShutdownCoordinator::new(std::time::Duration::from_secs(1))),
        ));
        (server, temp)
    }

    #[tokio::test]
    async fn native_server_registers_tcm_topology_listener_for_event_push() {
        let (server, _temp) = test_native_server();
        let registry = ListenerRegistry::new();
        let mut rx = server.event_dispatcher().subscribe();
        let listener = server.register_topology_event_listener(&registry);
        let id = node_id(1);
        let ep = endpoint(9042);

        assert_eq!(registry.listener_count(), 1);

        registry.notify(&topology_event(
            id,
            Transformation::Register {
                node_id: id,
                endpoint: ep,
                dc: "dc1".to_string(),
                rack: "rack1".to_string(),
            },
        ));

        let (event_type, change, addr) = recv_event(&mut rx).await;
        assert_eq!(event_type, "TOPOLOGY_CHANGE");
        assert_eq!(change, "NEW_NODE");
        assert_eq!(addr, ep.addr());
        assert_eq!(server.event_dispatcher().dispatched_count(), 1);
        assert_eq!(listener.name(), "NativeProtocolTopologyListener");
    }

    #[tokio::test]
    async fn native_topology_listener_maps_tcm_state_and_removal_events() {
        let dispatcher = Arc::new(EventDispatcher::new());
        let listener = NativeProtocolTopologyListener::new(Arc::clone(&dispatcher));
        let mut rx = dispatcher.subscribe();
        let id = node_id(2);
        let ep = endpoint(9043);

        listener.on_change(&topology_event(
            id,
            Transformation::Register {
                node_id: id,
                endpoint: ep,
                dc: "dc1".to_string(),
                rack: "rack1".to_string(),
            },
        ));
        let (_, change, _) = recv_event(&mut rx).await;
        assert_eq!(change, "NEW_NODE");

        listener.on_change(&topology_event(
            id,
            Transformation::UpdateNodeState {
                node_id: id,
                state: NodeState::Dead,
            },
        ));
        let (event_type, change, addr) = recv_event(&mut rx).await;
        assert_eq!(event_type, "STATUS_CHANGE");
        assert_eq!(change, "DOWN");
        assert_eq!(addr, ep.addr());

        listener.on_change(&topology_event(
            id,
            Transformation::Unregister { node_id: id },
        ));
        let (event_type, change, addr) = recv_event(&mut rx).await;
        assert_eq!(event_type, "TOPOLOGY_CHANGE");
        assert_eq!(change, "REMOVED_NODE");
        assert_eq!(addr, ep.addr());
    }

    #[tokio::test]
    async fn native_server_rejects_frames_over_configured_size() {
        let (server, _temp) = test_native_server_with_max_frame_size(16);
        let (mut client, server_side) = tokio::io::duplex(8192);
        let server_task = {
            let server = Arc::clone(&server);
            tokio::spawn(async move {
                server
                    .handle_connection(server_side, "127.0.0.1:9042".parse().unwrap())
                    .await
                    .unwrap();
            })
        };
        let mut incoming = BytesMut::new();

        let mut oversized_header = BytesMut::new();
        FrameHeader {
            version: PROTOCOL_V4,
            flags: 0,
            stream_id: 99,
            opcode: Opcode::Query,
            length: 17,
        }
        .encode(&mut oversized_header);
        client.write_all(&oversized_header).await.unwrap();

        let error = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            read_frame(&mut client, &mut incoming),
        )
        .await
        .unwrap();
        assert_eq!(error.header.opcode, Opcode::Error);
        assert_eq!(error.header.stream_id, 99);

        let mut body: &[u8] = &error.body;
        assert_eq!(types::read_int(&mut body).unwrap(), 0x000A);
        let message = types::read_string(&mut body).unwrap();
        assert!(message.contains("native_transport_max_frame_size"));

        server_task.await.unwrap();
    }

    #[tokio::test]
    async fn native_server_returns_protocol_error_for_unknown_opcode() {
        let (server, _temp) = test_native_server();
        let (mut client, server_side) = tokio::io::duplex(8192);
        let server_task = {
            let server = Arc::clone(&server);
            tokio::spawn(async move {
                server
                    .handle_connection(server_side, "127.0.0.1:9042".parse().unwrap())
                    .await
                    .unwrap();
            })
        };
        let mut incoming = BytesMut::new();

        let stream_id = 77i16;
        let mut unknown_opcode_frame = BytesMut::new();
        unknown_opcode_frame.extend_from_slice(&[PROTOCOL_V4, 0]);
        unknown_opcode_frame.extend_from_slice(&stream_id.to_be_bytes());
        unknown_opcode_frame.extend_from_slice(&[0x7F, 0, 0, 0, 0]);
        client.write_all(&unknown_opcode_frame).await.unwrap();

        let error = read_frame(&mut client, &mut incoming).await;
        assert_eq!(error.header.opcode, Opcode::Error);
        assert_eq!(error.header.stream_id, stream_id);

        let mut body: &[u8] = &error.body;
        assert_eq!(types::read_int(&mut body).unwrap(), 0x000A);
        let message = types::read_string(&mut body).unwrap();
        assert!(message.contains("Unknown opcode"));
        assert!(message.contains("0x7F"));

        server_task.await.unwrap();
    }

    #[tokio::test]
    async fn native_server_applies_negotiated_compression() {
        for compression in [Compression::Snappy, Compression::Lz4] {
            let (server, _temp) = test_native_server();
            let (mut client, server_side) = tokio::io::duplex(8192);
            let server_task = {
                let server = Arc::clone(&server);
                tokio::spawn(async move {
                    server
                        .handle_connection(server_side, "127.0.0.1:9042".parse().unwrap())
                        .await
                        .unwrap();
                })
            };
            let mut incoming = BytesMut::new();

            let mut opts = std::collections::HashMap::new();
            opts.insert("CQL_VERSION".to_string(), "3.4.7".to_string());
            opts.insert("COMPRESSION".to_string(), compression.name().to_string());
            write_frame(
                &mut client,
                startup_frame_with_options(PROTOCOL_V4, 0, opts),
            )
            .await;

            let ready = read_frame(&mut client, &mut incoming).await;
            assert_eq!(ready.header.opcode, Opcode::Ready);
            assert_ne!(ready.header.flags & flags::COMPRESSION, 0);
            assert!(decompressed_body(&ready, compression).is_empty());

            write_frame(
                &mut client,
                compressed_frame(register_frame(&["SCHEMA_CHANGE"]), compression),
            )
            .await;

            let ready = read_frame(&mut client, &mut incoming).await;
            assert_eq!(ready.header.opcode, Opcode::Ready);
            assert_ne!(ready.header.flags & flags::COMPRESSION, 0);
            assert!(decompressed_body(&ready, compression).is_empty());

            server.event_dispatcher().dispatch(
                EventMessage::SchemaChange(SchemaChange {
                    change_type: "CREATED".to_string(),
                    target: "TABLE".to_string(),
                    keyspace: "ks".to_string(),
                    name: Some(format!("{}_events", compression.name())),
                    arg_types: None,
                }),
                PROTOCOL_V4,
            );

            let event = tokio::time::timeout(
                std::time::Duration::from_secs(1),
                read_frame(&mut client, &mut incoming),
            )
            .await
            .unwrap();
            assert_eq!(event.header.opcode, Opcode::Event);
            assert_ne!(event.header.flags & flags::COMPRESSION, 0);

            let body = decompressed_body(&event, compression);
            let mut body: &[u8] = &body;
            assert_eq!(types::read_string(&mut body).unwrap(), "SCHEMA_CHANGE");
            assert_eq!(types::read_string(&mut body).unwrap(), "CREATED");
            assert_eq!(types::read_string(&mut body).unwrap(), "TABLE");
            assert_eq!(types::read_string(&mut body).unwrap(), "ks");
            assert_eq!(
                types::read_string(&mut body).unwrap(),
                format!("{}_events", compression.name())
            );

            drop(client);
            server_task.await.unwrap();
        }
    }

    #[tokio::test]
    async fn native_server_rejects_compressed_frame_before_negotiation() {
        let (server, _temp) = test_native_server();
        let (mut client, server_side) = tokio::io::duplex(8192);
        let server_task = {
            let server = Arc::clone(&server);
            tokio::spawn(async move {
                server
                    .handle_connection(server_side, "127.0.0.1:9042".parse().unwrap())
                    .await
                    .unwrap();
            })
        };
        let mut incoming = BytesMut::new();

        write_frame(
            &mut client,
            compressed_frame(startup_frame(), Compression::Snappy),
        )
        .await;

        let error = read_frame(&mut client, &mut incoming).await;
        assert_eq!(error.header.opcode, Opcode::Error);
        assert_eq!(error.header.stream_id, 1);
        assert_eq!(error.header.flags & flags::COMPRESSION, 0);

        let mut body: &[u8] = &error.body;
        assert_eq!(types::read_int(&mut body).unwrap(), 0x000A);
        let message = types::read_string(&mut body).unwrap();
        assert!(message.contains("before compression was negotiated"));

        drop(client);
        server_task.await.unwrap();
    }

    #[tokio::test]
    async fn native_server_rejects_startup_without_cql_version_but_keeps_connection_open() {
        let (server, _temp) = test_native_server();
        let (mut client, server_side) = tokio::io::duplex(8192);
        let server_task = {
            let server = Arc::clone(&server);
            tokio::spawn(async move {
                server
                    .handle_connection(server_side, "127.0.0.1:9042".parse().unwrap())
                    .await
                    .unwrap();
            })
        };
        let mut incoming = BytesMut::new();

        write_frame(
            &mut client,
            startup_frame_with_options(PROTOCOL_V4, 0, std::collections::HashMap::new()),
        )
        .await;

        let error = read_frame(&mut client, &mut incoming).await;
        assert_eq!(error.header.opcode, Opcode::Error);
        let mut body: &[u8] = &error.body;
        assert_eq!(types::read_int(&mut body).unwrap(), 0x000A);
        let message = types::read_string(&mut body).unwrap();
        assert!(message.contains("CQL_VERSION"));

        write_frame(&mut client, startup_frame()).await;
        let ready = read_frame(&mut client, &mut incoming).await;
        assert_eq!(ready.header.opcode, Opcode::Ready);

        drop(client);
        server_task.await.unwrap();
    }

    #[tokio::test]
    async fn native_server_rejects_response_only_request_flag_but_keeps_connection_open() {
        let (server, _temp) = test_native_server();
        let (mut client, server_side) = tokio::io::duplex(8192);
        let server_task = {
            let server = Arc::clone(&server);
            tokio::spawn(async move {
                server
                    .handle_connection(server_side, "127.0.0.1:9042".parse().unwrap())
                    .await
                    .unwrap();
            })
        };
        let mut incoming = BytesMut::new();

        write_frame(
            &mut client,
            request_frame_with_flags(Opcode::Options, PROTOCOL_V4, flags::WARNING, 1, Vec::new()),
        )
        .await;

        let error = read_frame(&mut client, &mut incoming).await;
        assert_eq!(error.header.opcode, Opcode::Error);
        assert_eq!(error.header.stream_id, 1);
        let mut body: &[u8] = &error.body;
        assert_eq!(types::read_int(&mut body).unwrap(), 0x000A);
        let message = types::read_string(&mut body).unwrap();
        assert!(message.contains("WARNING flag"));

        write_frame(&mut client, startup_frame()).await;
        let ready = read_frame(&mut client, &mut incoming).await;
        assert_eq!(ready.header.opcode, Opcode::Ready);

        drop(client);
        server_task.await.unwrap();
    }

    #[tokio::test]
    async fn native_server_returns_protocol_error_for_malformed_query_body() {
        let (server, _temp) = test_native_server();
        let (mut client, server_side) = tokio::io::duplex(8192);
        let server_task = {
            let server = Arc::clone(&server);
            tokio::spawn(async move {
                server
                    .handle_connection(server_side, "127.0.0.1:9042".parse().unwrap())
                    .await
                    .unwrap();
            })
        };
        let mut incoming = BytesMut::new();

        write_frame(&mut client, startup_frame()).await;
        let ready = read_frame(&mut client, &mut incoming).await;
        assert_eq!(ready.header.opcode, Opcode::Ready);

        let mut query_body = BytesMut::new();
        types::write_long_string(&mut query_body, "SELECT * FROM system.local");
        types::write_consistency(
            &mut query_body,
            cassandra_native_protocol::types::Consistency::One,
        );
        types::write_byte(&mut query_body, 0);
        types::write_byte(&mut query_body, 0xCA);
        write_frame(
            &mut client,
            request_frame(Opcode::Query, 3, query_body.to_vec()),
        )
        .await;

        let error = read_frame(&mut client, &mut incoming).await;
        assert_eq!(error.header.opcode, Opcode::Error);
        assert_eq!(error.header.stream_id, 3);
        let mut body: &[u8] = &error.body;
        assert_eq!(types::read_int(&mut body).unwrap(), 0x000A);
        let message = types::read_string(&mut body).unwrap();
        assert!(message.contains("Invalid request body"));
        assert!(message.contains("trailing bytes"));

        write_frame(&mut client, register_frame(&["SCHEMA_CHANGE"])).await;
        let ready = read_frame(&mut client, &mut incoming).await;
        assert_eq!(ready.header.opcode, Opcode::Ready);

        drop(client);
        server_task.await.unwrap();
    }

    #[tokio::test]
    async fn native_server_supports_v5_prepare_execute_driver_sequence() {
        let (server, _temp) = test_native_server();
        let (mut client, server_side) = tokio::io::duplex(8192);
        let server_task = {
            let server = Arc::clone(&server);
            tokio::spawn(async move {
                server
                    .handle_connection(server_side, "127.0.0.1:9042".parse().unwrap())
                    .await
                    .unwrap();
            })
        };
        let mut incoming = BytesMut::new();

        write_frame(
            &mut client,
            request_frame_with_flags(Opcode::Options, PROTOCOL_V5, flags::USE_BETA, 1, Vec::new()),
        )
        .await;
        let supported = read_frame(&mut client, &mut incoming).await;
        assert_eq!(supported.header.version, PROTOCOL_V5 | RESPONSE_FLAG);
        assert_eq!(supported.header.opcode, Opcode::Supported);

        write_frame(&mut client, startup_frame_for(PROTOCOL_V5, flags::USE_BETA)).await;
        let ready = read_frame(&mut client, &mut incoming).await;
        assert_eq!(ready.header.version, PROTOCOL_V5 | RESPONSE_FLAG);
        assert_eq!(ready.header.opcode, Opcode::Ready);

        let setup = [
            (
                3,
                "CREATE KEYSPACE drv WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}",
                0x0005,
            ),
            (
                4,
                "CREATE TABLE drv.events (id int PRIMARY KEY, name text)",
                0x0005,
            ),
            (
                5,
                "INSERT INTO drv.events (id, name) VALUES (7, 'prepared')",
                0x0001,
            ),
        ];
        for (stream_id, cql, expected_kind) in setup {
            write_frame(
                &mut client,
                query_frame_for(cql, PROTOCOL_V5, flags::USE_BETA, stream_id),
            )
            .await;
            let response = read_frame(&mut client, &mut incoming).await;
            assert_eq!(response.header.version, PROTOCOL_V5 | RESPONSE_FLAG);
            assert_eq!(response.header.opcode, Opcode::Result);
            assert_eq!(result_kind(&response), expected_kind);
        }

        write_frame(
            &mut client,
            prepare_frame_for_keyspace(
                "SELECT name FROM events",
                "drv",
                PROTOCOL_V5,
                flags::USE_BETA,
                6,
            ),
        )
        .await;
        let prepared = read_frame(&mut client, &mut incoming).await;
        assert_eq!(prepared.header.version, PROTOCOL_V5 | RESPONSE_FLAG);
        assert_eq!(prepared.header.opcode, Opcode::Result);
        let mut body: &[u8] = &prepared.body;
        assert_eq!(types::read_int(&mut body).unwrap(), 0x0004);
        let prepared_id = types::read_short_bytes(&mut body).unwrap();
        let result_metadata_id = types::read_short_bytes(&mut body).unwrap();
        assert!(!prepared_id.is_empty());
        assert!(!result_metadata_id.is_empty());

        write_frame(
            &mut client,
            execute_frame_for(
                &prepared_id,
                &result_metadata_id,
                PROTOCOL_V5,
                flags::USE_BETA,
                7,
            ),
        )
        .await;
        let rows = read_frame(&mut client, &mut incoming).await;
        assert_eq!(rows.header.version, PROTOCOL_V5 | RESPONSE_FLAG);
        assert_eq!(rows.header.opcode, Opcode::Result);

        let mut body: &[u8] = &rows.body;
        assert_eq!(types::read_int(&mut body).unwrap(), 0x0002);
        let metadata_flags = types::read_int(&mut body).unwrap();
        assert_ne!(
            metadata_flags & cassandra_native_protocol::message::rows_flags::NO_METADATA,
            0
        );
        assert_eq!(types::read_int(&mut body).unwrap(), 1);
        assert_eq!(types::read_int(&mut body).unwrap(), 1);
        assert_eq!(
            types::read_bytes(&mut body).unwrap(),
            Some(b"prepared".to_vec())
        );

        drop(client);
        server_task.await.unwrap();
    }

    #[tokio::test]
    async fn native_server_supports_v5_batch_request_keyspace() {
        let (server, _temp) = test_native_server();
        let (mut client, server_side) = tokio::io::duplex(8192);
        let server_task = {
            let server = Arc::clone(&server);
            tokio::spawn(async move {
                server
                    .handle_connection(server_side, "127.0.0.1:9042".parse().unwrap())
                    .await
                    .unwrap();
            })
        };
        let mut incoming = BytesMut::new();

        write_frame(&mut client, startup_frame_for(PROTOCOL_V5, flags::USE_BETA)).await;
        let ready = read_frame(&mut client, &mut incoming).await;
        assert_eq!(ready.header.version, PROTOCOL_V5 | RESPONSE_FLAG);
        assert_eq!(ready.header.opcode, Opcode::Ready);

        let setup = [
            (
                3,
                "CREATE KEYSPACE batchks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}",
                0x0005,
            ),
            (
                4,
                "CREATE TABLE batchks.events (id int PRIMARY KEY, name text)",
                0x0005,
            ),
        ];
        for (stream_id, cql, expected_kind) in setup {
            write_frame(
                &mut client,
                query_frame_for(cql, PROTOCOL_V5, flags::USE_BETA, stream_id),
            )
            .await;
            let response = read_frame(&mut client, &mut incoming).await;
            assert_eq!(response.header.version, PROTOCOL_V5 | RESPONSE_FLAG);
            assert_eq!(response.header.opcode, Opcode::Result);
            assert_eq!(result_kind(&response), expected_kind);
        }

        write_frame(
            &mut client,
            batch_frame_for_keyspace(
                "INSERT INTO events (id, name) VALUES (8, 'batched')",
                "batchks",
                PROTOCOL_V5,
                flags::USE_BETA,
                5,
            ),
        )
        .await;
        let batch_result = read_frame(&mut client, &mut incoming).await;
        assert_eq!(batch_result.header.version, PROTOCOL_V5 | RESPONSE_FLAG);
        assert_eq!(batch_result.header.opcode, Opcode::Result);
        assert_eq!(result_kind(&batch_result), 0x0001);

        write_frame(
            &mut client,
            query_frame_for(
                "SELECT name FROM batchks.events WHERE id = 8",
                PROTOCOL_V5,
                flags::USE_BETA,
                6,
            ),
        )
        .await;
        let rows = read_frame(&mut client, &mut incoming).await;
        assert_eq!(rows.header.version, PROTOCOL_V5 | RESPONSE_FLAG);
        assert_eq!(rows.header.opcode, Opcode::Result);

        let mut body: &[u8] = &rows.body;
        assert_eq!(types::read_int(&mut body).unwrap(), 0x0002);
        let metadata_flags = types::read_int(&mut body).unwrap();
        let column_count = types::read_int(&mut body).unwrap();
        assert_eq!(column_count, 1);
        if metadata_flags & cassandra_native_protocol::message::rows_flags::GLOBAL_TABLES_SPEC != 0
        {
            assert_eq!(types::read_string(&mut body).unwrap(), "batchks");
            assert_eq!(types::read_string(&mut body).unwrap(), "events");
        }
        for _ in 0..column_count {
            if metadata_flags & cassandra_native_protocol::message::rows_flags::GLOBAL_TABLES_SPEC
                == 0
            {
                let _ks = types::read_string(&mut body).unwrap();
                let _table = types::read_string(&mut body).unwrap();
            }
            let _name = types::read_string(&mut body).unwrap();
            let _type = types::read_short(&mut body).unwrap();
        }
        assert_eq!(types::read_int(&mut body).unwrap(), 1);
        assert_eq!(
            types::read_bytes(&mut body).unwrap(),
            Some(b"batched".to_vec())
        );

        drop(client);
        server_task.await.unwrap();
    }

    fn decode_status_event(frame: &Frame) -> (String, String) {
        assert_eq!(frame.header.opcode, Opcode::Event);
        assert_eq!(frame.header.stream_id, -1);
        let mut body: &[u8] = &frame.body;
        let event_type = types::read_string(&mut body).unwrap();
        let change = types::read_string(&mut body).unwrap();
        let _addr = types::read_inet(&mut body).unwrap();
        (event_type, change)
    }

    #[tokio::test]
    async fn native_server_run_publishes_status_change_events() {
        let (server, _temp) = test_native_server();
        let mut events = server.event_dispatcher().subscribe();
        let server_task = {
            let server = Arc::clone(&server);
            tokio::spawn(async move {
                server.run().await.unwrap();
            })
        };

        let up = tokio::time::timeout(std::time::Duration::from_secs(1), events.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(up.event_type, "STATUS_CHANGE");
        assert_eq!(
            decode_status_event(&up.frame),
            ("STATUS_CHANGE".to_string(), "UP".to_string())
        );

        server.shutdown.signal_shutdown();

        let down = tokio::time::timeout(std::time::Duration::from_secs(1), events.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(down.event_type, "STATUS_CHANGE");
        assert_eq!(
            decode_status_event(&down.frame),
            ("STATUS_CHANGE".to_string(), "DOWN".to_string())
        );

        server_task.await.unwrap();
    }

    #[tokio::test]
    async fn native_server_pushes_registered_schema_change_events() {
        let (server, _temp) = test_native_server();
        let (mut client, server_side) = tokio::io::duplex(8192);
        let server_task = {
            let server = Arc::clone(&server);
            tokio::spawn(async move {
                server
                    .handle_connection(server_side, "127.0.0.1:9042".parse().unwrap())
                    .await
                    .unwrap();
            })
        };
        let mut incoming = BytesMut::new();

        write_frame(&mut client, startup_frame()).await;
        let ready = read_frame(&mut client, &mut incoming).await;
        assert_eq!(ready.header.opcode, Opcode::Ready);

        write_frame(&mut client, register_frame(&["SCHEMA_CHANGE"])).await;
        let ready = read_frame(&mut client, &mut incoming).await;
        assert_eq!(ready.header.opcode, Opcode::Ready);

        server.event_dispatcher().dispatch(
            EventMessage::StatusChange {
                change: "UP".to_string(),
                addr: ("127.0.0.1".parse().unwrap(), 9042),
            },
            PROTOCOL_V4,
        );
        server.event_dispatcher().dispatch(
            EventMessage::SchemaChange(SchemaChange {
                change_type: "CREATED".to_string(),
                target: "TABLE".to_string(),
                keyspace: "ks".to_string(),
                name: Some("events".to_string()),
                arg_types: None,
            }),
            PROTOCOL_V4,
        );

        let event = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            read_frame(&mut client, &mut incoming),
        )
        .await
        .unwrap();
        assert_eq!(event.header.version, PROTOCOL_V4 | RESPONSE_FLAG);
        assert_eq!(event.header.opcode, Opcode::Event);
        assert_eq!(event.header.stream_id, -1);

        let mut body: &[u8] = &event.body;
        assert_eq!(types::read_string(&mut body).unwrap(), "SCHEMA_CHANGE");
        assert_eq!(types::read_string(&mut body).unwrap(), "CREATED");
        assert_eq!(types::read_string(&mut body).unwrap(), "TABLE");
        assert_eq!(types::read_string(&mut body).unwrap(), "ks");
        assert_eq!(types::read_string(&mut body).unwrap(), "events");

        drop(client);
        server_task.await.unwrap();
    }

    #[tokio::test]
    async fn native_server_push_events_use_negotiated_v5_protocol_version() {
        let (server, _temp) = test_native_server();
        let (mut client, server_side) = tokio::io::duplex(8192);
        let server_task = {
            let server = Arc::clone(&server);
            tokio::spawn(async move {
                server
                    .handle_connection(server_side, "127.0.0.1:9042".parse().unwrap())
                    .await
                    .unwrap();
            })
        };
        let mut incoming = BytesMut::new();

        write_frame(&mut client, startup_frame_for(PROTOCOL_V5, flags::USE_BETA)).await;
        let ready = read_frame(&mut client, &mut incoming).await;
        assert_eq!(ready.header.version, PROTOCOL_V5 | RESPONSE_FLAG);
        assert_eq!(ready.header.opcode, Opcode::Ready);

        write_frame(
            &mut client,
            register_frame_for(&["SCHEMA_CHANGE"], PROTOCOL_V5, flags::USE_BETA),
        )
        .await;
        let ready = read_frame(&mut client, &mut incoming).await;
        assert_eq!(ready.header.version, PROTOCOL_V5 | RESPONSE_FLAG);
        assert_eq!(ready.header.opcode, Opcode::Ready);

        server.event_dispatcher().dispatch(
            EventMessage::SchemaChange(SchemaChange {
                change_type: "CREATED".to_string(),
                target: "TABLE".to_string(),
                keyspace: "ks".to_string(),
                name: Some("v5_events".to_string()),
                arg_types: None,
            }),
            PROTOCOL_V4,
        );

        let event = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            read_frame(&mut client, &mut incoming),
        )
        .await
        .unwrap();
        assert_eq!(event.header.version, PROTOCOL_V5 | RESPONSE_FLAG);
        assert_eq!(event.header.opcode, Opcode::Event);
        assert_eq!(event.header.stream_id, -1);

        let mut body: &[u8] = &event.body;
        assert_eq!(types::read_string(&mut body).unwrap(), "SCHEMA_CHANGE");
        assert_eq!(types::read_string(&mut body).unwrap(), "CREATED");
        assert_eq!(types::read_string(&mut body).unwrap(), "TABLE");
        assert_eq!(types::read_string(&mut body).unwrap(), "ks");
        assert_eq!(types::read_string(&mut body).unwrap(), "v5_events");

        drop(client);
        server_task.await.unwrap();
    }
}
