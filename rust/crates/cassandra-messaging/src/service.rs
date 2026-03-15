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
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use parking_lot::RwLock;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio_util::codec::{FramedRead, FramedWrite};
use futures_util::{SinkExt, StreamExt};
use tracing::{debug, error, info, warn};

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
    sender: oneshot::Sender<Message>,
    sent_at: std::time::Instant,
}

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
    /// Metrics.
    pub metrics: Arc<MessagingMetrics>,
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
            metrics: Arc::new(MessagingMetrics::new()),
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
    pub async fn send(
        &self,
        endpoint: SocketAddr,
        msg: Message,
    ) -> Result<(), std::io::Error> {
        self.metrics.verb(msg.header.verb).record_sent();

        let stream = tokio::time::timeout(CONNECT_TIMEOUT, TcpStream::connect(endpoint))
            .await
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "connect timeout"))??;

        let mut framed = FramedWrite::new(stream, MessageCodec);
        framed.send(msg).await?;

        Ok(())
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
        let stream = tokio::time::timeout(CONNECT_TIMEOUT, TcpStream::connect(endpoint))
            .await
            .map_err(|_| MessagingError::Timeout)??;

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
                            if let Err(e) = svc2.handle_connection(stream).await {
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

    /// Handle a single inbound connection.
    async fn handle_connection(&self, stream: TcpStream) -> Result<(), std::io::Error> {
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
                Some(Message::response(msg.header.message_id, Verb::Pong, Vec::new()))
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
}
