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

//! Messaging service: central hub for internode communication.
//!
//! Provides:
//! - Verb handler registration
//! - Send/receive with connection pooling
//! - Request/response correlation
//! - Backpressure via in-flight limits
//! - Per-verb metrics
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.net.MessagingService`
//! - `org.apache.cassandra.net.OutboundConnections`

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use dashmap::DashMap;
use futures_util::{SinkExt, StreamExt};
use parking_lot::RwLock;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio_util::codec::{FramedRead, FramedWrite};
use tracing::{debug, error, info, warn};

use cassandra_security::tls::ReloadableTlsAcceptor;
use rustls::pki_types::ServerName;
use tokio_rustls::TlsConnector;

use crate::frame::{Message, MessageCodec};
use crate::metrics::MessagingMetrics;
use crate::verb::Verb;

/// Handler function type for incoming messages.
pub type MessageHandler = Arc<dyn Fn(Message) -> Option<Message> + Send + Sync>;

/// Maximum in-flight messages per endpoint (backpressure).
pub const DEFAULT_MAX_INFLIGHT: usize = 1024;

/// Connection timeout.
pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Pending response entry.
struct PendingResponse {
    #[allow(dead_code)]
    sender: oneshot::Sender<Message>,
    sent_at: std::time::Instant,
}

/// Helper trait for streams that support both reading and writing asynchronously.
pub trait AsyncStream: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send {}
impl<T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send> AsyncStream for T {}

/// The messaging service: manages all internode communication.
///
/// Register handlers for verbs, then send messages to other nodes.
/// The service manages connection pooling and response correlation.
pub struct MessagingService {
    /// Local listen address.
    listen_addr: SocketAddr,
    /// Verb handlers.
    handlers: RwLock<HashMap<Verb, MessageHandler>>,
    /// Pending responses keyed by message ID.
    pending_responses: DashMap<u64, PendingResponse>,
    /// Next message ID counter.
    next_message_id: AtomicU64,
    /// Max in-flight per endpoint.
    max_inflight: usize,
    /// Per-endpoint in-flight counters (for backpressure).
    inflight_per_endpoint: DashMap<SocketAddr, usize>,
    pub metrics: Arc<MessagingMetrics>,
    /// Optional TLS acceptor for incoming connections.
    tls_acceptor: Option<ReloadableTlsAcceptor>,
    /// Optional TLS connector for outgoing connections.
    tls_connector: Option<TlsConnector>,
}

impl MessagingService {
    /// Create a new messaging service.
    pub fn new(listen_addr: SocketAddr) -> Self {
        Self {
            listen_addr,
            handlers: RwLock::new(HashMap::new()),
            pending_responses: DashMap::new(),
            next_message_id: AtomicU64::new(1),
            max_inflight: DEFAULT_MAX_INFLIGHT,
            inflight_per_endpoint: DashMap::new(),
            metrics: Arc::new(MessagingMetrics::new()),
            tls_acceptor: None,
            tls_connector: None,
        }
    }

    /// Create with a custom max-inflight limit.
    pub fn with_max_inflight(listen_addr: SocketAddr, max_inflight: usize) -> Self {
        let mut svc = Self::new(listen_addr);
        svc.max_inflight = max_inflight;
        svc
    }

    /// Configure TLS for the messaging service.
    pub fn with_tls(mut self, acceptor: ReloadableTlsAcceptor, connector: TlsConnector) -> Self {
        self.tls_acceptor = Some(acceptor);
        self.tls_connector = Some(connector);
        self
    }

    /// Get the current in-flight count for an endpoint.
    fn inflight_count(&self, endpoint: &SocketAddr) -> usize {
        self.inflight_per_endpoint
            .get(endpoint)
            .map(|v| *v)
            .unwrap_or(0)
    }

    /// Increment the in-flight count for an endpoint.
    fn increment_inflight(&self, endpoint: &SocketAddr) {
        self.inflight_per_endpoint
            .entry(*endpoint)
            .and_modify(|c| *c += 1)
            .or_insert(1);
    }

    /// Decrement the in-flight count for an endpoint.
    fn decrement_inflight(&self, endpoint: &SocketAddr) {
        if let Some(mut entry) = self.inflight_per_endpoint.get_mut(endpoint) {
            *entry = entry.saturating_sub(1);
        }
    }

    /// Register a handler for a verb.
    pub fn register_handler(&self, verb: Verb, handler: MessageHandler) {
        self.handlers.write().insert(verb, handler);
    }

    /// Generate the next message ID.
    pub fn next_id(&self) -> u64 {
        self.next_message_id.fetch_add(1, Ordering::SeqCst)
    }

    /// Send a message to a remote endpoint (fire-and-forget).
    ///
    /// Returns immediately after queuing. Use `send_and_wait` for
    /// request/response patterns.
    ///
    /// Returns `Err(MessagingError::Backpressure)` if the endpoint has
    /// more than `max_inflight` outstanding requests.
    pub async fn send(&self, endpoint: SocketAddr, msg: Message) -> Result<(), MessagingError> {
        // Check backpressure
        let inflight = self.inflight_count(&endpoint);
        if inflight >= self.max_inflight {
            self.metrics.verb(msg.header.verb).record_dropped();
            return Err(MessagingError::Backpressure);
        }
        self.increment_inflight(&endpoint);

        self.metrics.verb(msg.header.verb).record_sent();

        let result = async {
            let tcp_stream = tokio::time::timeout(CONNECT_TIMEOUT, TcpStream::connect(endpoint))
                .await
                .map_err(|_| MessagingError::Timeout)?
                .map_err(MessagingError::Io)?;

            let stream: Box<dyn AsyncStream> = if let Some(ref tls) = self.tls_connector {
                let domain = ServerName::try_from(endpoint.ip().to_string()).map_err(|_| {
                    MessagingError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "Invalid IP for ServerName",
                    ))
                })?;
                let tls_stream = tls
                    .connect(domain, tcp_stream)
                    .await
                    .map_err(MessagingError::Io)?;
                Box::new(tls_stream)
            } else {
                Box::new(tcp_stream)
            };

            let mut framed = FramedWrite::new(stream, MessageCodec);
            framed.send(msg).await.map_err(MessagingError::Io)?;
            Ok(())
        }
        .await;

        self.decrement_inflight(&endpoint);
        result
    }

    /// Send a message and wait for the response.
    ///
    /// Correlates request and response by message ID. Times out after
    /// the given duration.
    pub async fn send_and_wait(
        &self,
        endpoint: SocketAddr,
        msg: Message,
        timeout: Duration,
    ) -> Result<Message, MessagingError> {
        let msg_id = msg.header.message_id;
        let verb = msg.header.verb;

        self.metrics.verb(verb).record_sent();

        // Set up response channel
        let (tx, _rx) = oneshot::channel();
        self.pending_responses.insert(
            msg_id,
            PendingResponse {
                sender: tx,
                sent_at: std::time::Instant::now(),
            },
        );

        // Connect and send
        let tcp_stream = tokio::time::timeout(CONNECT_TIMEOUT, TcpStream::connect(endpoint))
            .await
            .map_err(|_| MessagingError::Timeout)??;

        let stream: Box<dyn AsyncStream> = if let Some(ref tls) = self.tls_connector {
            let domain = ServerName::try_from(endpoint.ip().to_string()).map_err(|_| {
                MessagingError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "Invalid IP for ServerName",
                ))
            })?;
            let tls_stream = tls
                .connect(domain, tcp_stream)
                .await
                .map_err(MessagingError::Io)?;
            Box::new(tls_stream)
        } else {
            Box::new(tcp_stream)
        };

        let (read_half, write_half) = tokio::io::split(stream);
        let mut writer = FramedWrite::new(write_half, MessageCodec);
        let mut reader = FramedRead::new(read_half, MessageCodec);

        writer.send(msg).await?;

        // Wait for response with timeout
        let response = tokio::time::timeout(timeout, async {
            if let Some(result) = reader.next().await {
                match result {
                    Ok(response_msg) => {
                        // Record metrics
                        let elapsed = self
                            .pending_responses
                            .remove(&msg_id)
                            .map(|(_, pr)| pr.sent_at.elapsed().as_micros() as u64)
                            .unwrap_or(0);

                        self.metrics.verb(verb).record_completed(elapsed);
                        Ok(response_msg)
                    }
                    Err(e) => Err(MessagingError::Io(e)),
                }
            } else {
                Err(MessagingError::ConnectionClosed)
            }
        })
        .await;

        match response {
            Ok(result) => result,
            Err(_) => {
                self.pending_responses.remove(&msg_id);
                self.metrics.verb(verb).record_dropped();
                Err(MessagingError::Timeout)
            }
        }
    }

    /// Handle an incoming message by dispatching to the registered handler.
    pub fn dispatch(&self, msg: Message) -> Option<Message> {
        self.metrics.verb(msg.header.verb).record_received();

        let handlers = self.handlers.read();
        if let Some(handler) = handlers.get(&msg.header.verb) {
            handler(msg)
        } else {
            warn!(verb = %msg.header.verb, "No handler registered");
            None
        }
    }

    /// Start listening for incoming connections.
    ///
    /// Spawns a background task that accepts connections and dispatches
    /// messages to registered handlers.
    pub async fn start_listener(self: Arc<Self>) -> Result<(), std::io::Error> {
        let listener = TcpListener::bind(self.listen_addr).await?;
        info!(addr = %self.listen_addr, "Messaging service listening");

        let svc = Arc::clone(&self);
        tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((stream, peer)) => {
                        debug!(peer = %peer, "Accepted internode connection");
                        svc.metrics
                            .connections_active
                            .fetch_add(1, Ordering::Relaxed);
                        svc.metrics
                            .connections_created
                            .fetch_add(1, Ordering::Relaxed);

                        let svc2 = Arc::clone(&svc);
                        tokio::spawn(async move {
                            if let Some(ref tls) = svc2.tls_acceptor {
                                let acceptor = tls.acceptor();
                                match acceptor.accept(stream).await {
                                    Ok(tls_stream) => {
                                        if let Err(e) = svc2.handle_connection(tls_stream).await {
                                            debug!(peer = %peer, error = %e, "TLS Connection ended(err)");
                                        }
                                    }
                                    Err(e) => {
                                        error!(peer = %peer, error = %e, "TLS Handshake failed");
                                    }
                                }
                            } else if let Err(e) = svc2.handle_connection(stream).await {
                                debug!(peer = %peer, error = %e, "Connection ended");
                            }
                            svc2.metrics
                                .connections_active
                                .fetch_sub(1, Ordering::Relaxed);
                        });
                    }
                    Err(e) => {
                        error!(error = %e, "Failed to accept connection");
                    }
                }
            }
        });

        Ok(())
    }

    /// Handle a single inbound connection (dispatch messages until stream closes).
    pub async fn dispatch_on_stream<S>(&self, stream: S) -> Result<(), std::io::Error>
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
    {
        self.handle_connection(stream).await
    }

    /// Handle a single inbound connection.
    async fn handle_connection<S>(&self, stream: S) -> Result<(), std::io::Error>
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
    {
        let (read_half, write_half) = tokio::io::split(stream);
        let mut reader = FramedRead::new(read_half, MessageCodec);
        let mut writer = FramedWrite::new(write_half, MessageCodec);

        while let Some(result) = reader.next().await {
            match result {
                Ok(msg) => {
                    if let Some(response) = self.dispatch(msg) {
                        writer.send(response).await?;
                    }
                }
                Err(e) => {
                    return Err(e);
                }
            }
        }

        Ok(())
    }

    /// Get the listen address.
    pub fn listen_addr(&self) -> SocketAddr {
        self.listen_addr
    }

    /// Get the metrics.
    pub fn messaging_metrics(&self) -> Arc<MessagingMetrics> {
        Arc::clone(&self.metrics)
    }
}

/// Errors from the messaging service.
#[derive(Debug, thiserror::Error)]
pub enum MessagingError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Request timed out")]
    Timeout,

    #[error("Connection closed")]
    ConnectionClosed,

    #[error("Backpressure: too many in-flight requests")]
    Backpressure,

    #[error("Version negotiation failed")]
    VersionMismatch,
}

// ─────────────────────────────────────────────────────────────────────────────
// Connection Pool
// ─────────────────────────────────────────────────────────────────────────────

/// Per-endpoint connection pool with version tracking.
///
/// Caches TCP connections to remote endpoints, avoiding the overhead
/// of establishing a new connection for every message.
///
/// ## Java Oracle
///
/// `org.apache.cassandra.net.OutboundConnections`
pub struct ConnectionPool {
    /// Cached connections: endpoint → (stream, negotiated_version).
    connections: DashMap<SocketAddr, ConnectionInfo>,
    /// Maximum connections per endpoint.
    max_connections_per_endpoint: usize,
}

/// Info about a cached connection.
struct ConnectionInfo {
    /// Negotiated protocol version.
    negotiated_version: i32,
}

impl ConnectionPool {
    /// Create a new connection pool.
    pub fn new(max_connections_per_endpoint: usize) -> Self {
        Self {
            connections: DashMap::new(),
            max_connections_per_endpoint,
        }
    }

    /// Record a connection to an endpoint with a negotiated version.
    pub fn record_connection(&self, endpoint: SocketAddr, version: i32) {
        self.connections.insert(
            endpoint,
            ConnectionInfo {
                negotiated_version: version,
            },
        );
    }

    /// Get the negotiated version for an endpoint (if connected).
    pub fn negotiated_version(&self, endpoint: &SocketAddr) -> Option<i32> {
        self.connections.get(endpoint).map(|c| c.negotiated_version)
    }

    /// Remove a connection (on disconnect or error).
    pub fn remove(&self, endpoint: &SocketAddr) {
        self.connections.remove(endpoint);
    }

    /// Number of tracked connections.
    pub fn connection_count(&self) -> usize {
        self.connections.len()
    }

    /// Whether we have a connection to the endpoint.
    pub fn is_connected(&self, endpoint: &SocketAddr) -> bool {
        self.connections.contains_key(endpoint)
    }

    /// Maximum connections per endpoint.
    pub fn max_per_endpoint(&self) -> usize {
        self.max_connections_per_endpoint
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Per-Verb Timeout Configuration
// ─────────────────────────────────────────────────────────────────────────────

/// Per-verb timeout configuration.
///
/// Different message types have different latency expectations:
/// - Reads/writes: 5s default
/// - Gossip: 1s
/// - Streaming: 2h (long transfers)
/// - TCM: 30s (consensus)
///
/// ## Java Oracle
///
/// `org.apache.cassandra.config.DatabaseDescriptor.getRpcTimeout()` and friends
pub struct VerbTimeoutConfig {
    defaults: HashMap<Verb, Duration>,
}

impl VerbTimeoutConfig {
    /// Create with Cassandra-standard defaults.
    pub fn with_defaults() -> Self {
        let mut defaults = HashMap::new();
        // Data path
        defaults.insert(Verb::Mutation, Duration::from_secs(5));
        defaults.insert(Verb::ReadData, Duration::from_secs(5));
        defaults.insert(Verb::ReadDigest, Duration::from_secs(5));
        defaults.insert(Verb::ReadRepair, Duration::from_secs(5));
        defaults.insert(Verb::Hint, Duration::from_secs(10));
        defaults.insert(Verb::BatchStore, Duration::from_secs(5));
        // Gossip
        defaults.insert(Verb::GossipDigestSyn, Duration::from_secs(1));
        defaults.insert(Verb::GossipDigestAck, Duration::from_secs(1));
        // Schema
        defaults.insert(Verb::SchemaPush, Duration::from_secs(30));
        defaults.insert(Verb::SchemaPull, Duration::from_secs(30));
        // Streaming
        defaults.insert(Verb::StreamInit, Duration::from_secs(7200));
        defaults.insert(Verb::StreamData, Duration::from_secs(7200));
        defaults.insert(Verb::StreamComplete, Duration::from_secs(60));
        // TCM
        defaults.insert(Verb::TcmCommit, Duration::from_secs(30));
        defaults.insert(Verb::TcmFetch, Duration::from_secs(30));
        defaults.insert(Verb::TcmNotify, Duration::from_secs(10));
        // Ping
        defaults.insert(Verb::Ping, Duration::from_secs(1));
        // Repair
        defaults.insert(Verb::RepairRequest, Duration::from_secs(3600));
        defaults.insert(Verb::MerkleTreeRequest, Duration::from_secs(3600));
        Self { defaults }
    }

    /// Get the timeout for a verb.
    pub fn timeout_for(&self, verb: &Verb) -> Duration {
        self.defaults
            .get(verb)
            .copied()
            .unwrap_or(Duration::from_secs(10))
    }

    /// Override the timeout for a specific verb.
    pub fn set_timeout(&mut self, verb: Verb, timeout: Duration) {
        self.defaults.insert(verb, timeout);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn messaging_service_creation() {
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let svc = MessagingService::new(addr);

        assert_eq!(svc.max_inflight, DEFAULT_MAX_INFLIGHT);
    }

    #[test]
    fn register_and_dispatch() {
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let svc = MessagingService::new(addr);

        svc.register_handler(
            Verb::Ping,
            Arc::new(|msg| {
                Some(Message::response(
                    msg.header.message_id,
                    Verb::Pong,
                    Vec::new(),
                ))
            }),
        );

        let ping = Message::request(Verb::Ping, 1, Vec::new());
        let response = svc.dispatch(ping);
        assert!(response.is_some());
        let resp = response.unwrap();
        assert_eq!(resp.header.verb, Verb::Pong);
        assert!(resp.is_response());
    }

    #[test]
    fn dispatch_unregistered_verb() {
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let svc = MessagingService::new(addr);

        let msg = Message::request(Verb::Mutation, 1, Vec::new());
        let response = svc.dispatch(msg);
        assert!(response.is_none());
    }

    #[test]
    fn message_id_generation() {
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let svc = MessagingService::new(addr);

        let id1 = svc.next_id();
        let id2 = svc.next_id();
        assert_ne!(id1, id2);
        assert!(id2 > id1);
    }

    #[tokio::test]
    async fn listener_and_send() {
        let listener_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let svc = Arc::new(MessagingService::new(listener_addr));

        // Register a ping handler
        svc.register_handler(
            Verb::Ping,
            Arc::new(|msg| {
                Some(Message::response(
                    msg.header.message_id,
                    Verb::Pong,
                    b"pong".to_vec(),
                ))
            }),
        );

        // Start listener on a random port
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let actual_addr = listener.local_addr().unwrap();

        let svc2 = Arc::clone(&svc);
        tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((stream, _)) => {
                        let svc3 = Arc::clone(&svc2);
                        tokio::spawn(async move {
                            let _ = svc3.handle_connection(stream).await;
                        });
                    }
                    Err(_) => break,
                }
            }
        });

        // Give listener time to start
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Send a ping
        let client_svc = MessagingService::new("127.0.0.1:0".parse().unwrap());
        let ping = Message::request(Verb::Ping, client_svc.next_id(), b"ping".to_vec());

        let response = client_svc
            .send_and_wait(actual_addr, ping, Duration::from_secs(2))
            .await;

        assert!(response.is_ok(), "Expected Ok, got {:?}", response.err());
        let resp = response.unwrap();
        assert_eq!(resp.header.verb, Verb::Pong);
        assert_eq!(resp.payload, b"pong");
    }

    // ── Connection Pool Tests ───────────────────────────────────────────

    #[test]
    fn connection_pool_create_and_query() {
        let pool = ConnectionPool::new(4);
        let addr: SocketAddr = "127.0.0.1:7001".parse().unwrap();

        assert!(!pool.is_connected(&addr));
        assert_eq!(pool.connection_count(), 0);

        pool.record_connection(addr, 14);
        assert!(pool.is_connected(&addr));
        assert_eq!(pool.connection_count(), 1);
        assert_eq!(pool.negotiated_version(&addr), Some(14));
    }

    #[test]
    fn connection_pool_remove() {
        let pool = ConnectionPool::new(4);
        let addr: SocketAddr = "127.0.0.1:7001".parse().unwrap();

        pool.record_connection(addr, 14);
        pool.remove(&addr);
        assert!(!pool.is_connected(&addr));
        assert_eq!(pool.connection_count(), 0);
    }

    // ── Per-Verb Timeout Tests ──────────────────────────────────────────

    #[test]
    fn verb_timeout_defaults() {
        let config = VerbTimeoutConfig::with_defaults();
        assert_eq!(config.timeout_for(&Verb::Mutation), Duration::from_secs(5));
        assert_eq!(config.timeout_for(&Verb::Ping), Duration::from_secs(1));
        assert_eq!(
            config.timeout_for(&Verb::StreamInit),
            Duration::from_secs(7200)
        );
        assert_eq!(
            config.timeout_for(&Verb::TcmCommit),
            Duration::from_secs(30)
        );
    }

    #[test]
    fn verb_timeout_override() {
        let mut config = VerbTimeoutConfig::with_defaults();
        config.set_timeout(Verb::Mutation, Duration::from_secs(10));
        assert_eq!(config.timeout_for(&Verb::Mutation), Duration::from_secs(10));
    }

    #[test]
    fn verb_timeout_unknown_fallback() {
        let config = VerbTimeoutConfig::with_defaults();
        // Verb not explicitly configured should get 10s default
        assert_eq!(
            config.timeout_for(&Verb::BatchRemove),
            Duration::from_secs(10)
        );
    }
}
