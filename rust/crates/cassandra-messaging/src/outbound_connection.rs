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

//! Persistent outbound connection for a single channel.
//!
//! Manages a single TCP connection to a remote peer, with automatic
//! reconnection, heartbeats, and message draining from an outbound queue.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.net.OutboundConnection`
//! - `org.apache.cassandra.net.OutboundConnectionInitiator`

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_util::{FutureExt, SinkExt};
use tokio::net::TcpStream;
use tokio::sync::Notify;
use tokio_util::codec::FramedWrite;
use tracing::{debug, warn};

use crate::connection_type::ConnectionType;
use crate::frame::{Message, MessageCodec};
use crate::handshake;
use crate::outbound_queue::{OutboundMessageQueue, QueueProducer};

/// Connection state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionState {
    Disconnected,
    Connecting,
    Connected,
    Closed,
}

/// Reconnect backoff parameters.
const INITIAL_BACKOFF: Duration = Duration::from_millis(100);
const MAX_BACKOFF: Duration = Duration::from_secs(30);
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(10);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Metrics for a single outbound connection.
#[derive(Debug, Default)]
pub struct OutboundConnectionMetrics {
    pub bytes_sent: AtomicU64,
    pub messages_sent: AtomicU64,
    pub reconnects: AtomicU64,
}

/// Configuration for an outbound connection.
#[derive(Debug, Clone)]
pub struct OutboundConnectionConfig {
    pub remote: SocketAddr,
    pub local_addr: SocketAddr,
    pub connection_type: ConnectionType,
    pub compression: bool,
    pub crc_framing: bool,
}

/// A persistent outbound connection for one channel type.
///
/// Owns an outbound queue and drains it in a background tokio task.
/// Reconnects with exponential backoff on failure.
pub struct OutboundConnection {
    config: OutboundConnectionConfig,
    producer: QueueProducer,
    queue: parking_lot::Mutex<Option<OutboundMessageQueue>>,
    close_notify: Arc<Notify>,
    pub metrics: Arc<OutboundConnectionMetrics>,
}

impl OutboundConnection {
    /// Create a new outbound connection.
    ///
    /// Returns the connection handle. Call `spawn_run()` to start the
    /// background drain loop.
    pub fn new(config: OutboundConnectionConfig) -> Self {
        let (queue, producer) = OutboundMessageQueue::new();
        Self {
            config,
            producer,
            queue: parking_lot::Mutex::new(Some(queue)),
            close_notify: Arc::new(Notify::new()),
            metrics: Arc::new(OutboundConnectionMetrics::default()),
        }
    }

    /// Enqueue a message for sending.
    pub fn enqueue(&self, msg: Message, expires_at: Instant) -> bool {
        self.producer.enqueue(msg, expires_at)
    }

    /// Spawn the background drain task.
    ///
    /// Takes ownership of the internal queue. Can only be called once.
    /// The task connects to the remote peer, performs a handshake, then
    /// continuously drains the queue and writes messages. On disconnect,
    /// it reconnects with exponential backoff.
    pub fn spawn_run(&self) -> Option<tokio::task::JoinHandle<()>> {
        let mut queue = self.queue.lock().take()?;
        let config = self.config.clone();
        let close_notify = Arc::clone(&self.close_notify);
        let metrics = Arc::clone(&self.metrics);

        Some(tokio::spawn(async move {
            let mut backoff = INITIAL_BACKOFF;

            loop {
                // Check for close signal
                if close_notify.notified().now_or_never().is_some() {
                    debug!(remote = %config.remote, "Outbound connection closed");
                    return;
                }

                debug!(remote = %config.remote, conn_type = %config.connection_type, "Connecting");

                let connect_result = tokio::time::timeout(
                    CONNECT_TIMEOUT,
                    TcpStream::connect(config.remote),
                )
                .await;

                let mut stream = match connect_result {
                    Ok(Ok(s)) => s,
                    Ok(Err(e)) => {
                        warn!(remote = %config.remote, error = %e, "Connect failed");
                        metrics.reconnects.fetch_add(1, Ordering::Relaxed);
                        tokio::time::sleep(backoff).await;
                        backoff = (backoff * 2).min(MAX_BACKOFF);
                        continue;
                    }
                    Err(_) => {
                        warn!(remote = %config.remote, "Connect timeout");
                        metrics.reconnects.fetch_add(1, Ordering::Relaxed);
                        tokio::time::sleep(backoff).await;
                        backoff = (backoff * 2).min(MAX_BACKOFF);
                        continue;
                    }
                };

                // Perform handshake
                let hs_result = handshake::perform_outbound_handshake(
                    &mut stream,
                    config.connection_type,
                    config.compression,
                    config.crc_framing,
                    config.local_addr,
                )
                .await;

                if let Err(e) = hs_result {
                    warn!(remote = %config.remote, error = %e, "Handshake failed");
                    metrics.reconnects.fetch_add(1, Ordering::Relaxed);
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(MAX_BACKOFF);
                    continue;
                }

                // Connected — reset backoff
                backoff = INITIAL_BACKOFF;
                debug!(remote = %config.remote, "Connected");

                let (_read, write) = tokio::io::split(stream);
                let mut writer = FramedWrite::new(write, MessageCodec);
                let mut last_activity = Instant::now();

                // Drain loop
                loop {
                    if close_notify.notified().now_or_never().is_some() {
                        return;
                    }

                    if let Some(queued) = queue.poll_next() {
                        let msg_len = queued.message.payload.len() as u64;
                        if let Err(e) = writer.send(queued.message).await {
                            warn!(remote = %config.remote, error = %e, "Write failed");
                            break; // reconnect
                        }
                        metrics.bytes_sent.fetch_add(msg_len, Ordering::Relaxed);
                        metrics.messages_sent.fetch_add(1, Ordering::Relaxed);
                        last_activity = Instant::now();
                    } else if last_activity.elapsed() >= HEARTBEAT_INTERVAL {
                        // Send ping heartbeat
                        let ping = Message::request(
                            crate::verb::Verb::Ping,
                            0,
                            Vec::new(),
                        );
                        if let Err(e) = writer.send(ping).await {
                            warn!(remote = %config.remote, error = %e, "Heartbeat failed");
                            break;
                        }
                        last_activity = Instant::now();
                    } else {
                        // Brief sleep to avoid busy-wait
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                }

                metrics.reconnects.fetch_add(1, Ordering::Relaxed);
            }
        }))
    }

    /// Signal the connection to close.
    pub fn close(&self) {
        self.close_notify.notify_waiters();
    }

    /// Get a clone of the producer for enqueueing messages.
    pub fn producer(&self) -> QueueProducer {
        self.producer.clone()
    }

    /// Get the connection configuration.
    pub fn config(&self) -> &OutboundConnectionConfig {
        &self.config
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::verb::Verb;

    #[test]
    fn connection_state_values() {
        assert_ne!(ConnectionState::Disconnected, ConnectionState::Connected);
        assert_ne!(ConnectionState::Connecting, ConnectionState::Closed);
    }

    #[test]
    fn outbound_connection_enqueue() {
        let config = OutboundConnectionConfig {
            remote: "127.0.0.1:7000".parse().unwrap(),
            local_addr: "127.0.0.1:0".parse().unwrap(),
            connection_type: ConnectionType::Small,
            compression: false,
            crc_framing: true,
        };

        let conn = OutboundConnection::new(config);
        let msg = Message::request(Verb::Mutation, 1, b"test".to_vec());
        let expires = Instant::now() + Duration::from_secs(10);

        assert!(conn.enqueue(msg, expires));
    }

    #[test]
    fn outbound_connection_close() {
        let config = OutboundConnectionConfig {
            remote: "127.0.0.1:7000".parse().unwrap(),
            local_addr: "127.0.0.1:0".parse().unwrap(),
            connection_type: ConnectionType::Urgent,
            compression: false,
            crc_framing: false,
        };

        let conn = OutboundConnection::new(config);
        conn.close(); // Should not panic
    }

    #[test]
    fn metrics_default() {
        let metrics = OutboundConnectionMetrics::default();
        assert_eq!(metrics.bytes_sent.load(Ordering::Relaxed), 0);
        assert_eq!(metrics.messages_sent.load(Ordering::Relaxed), 0);
        assert_eq!(metrics.reconnects.load(Ordering::Relaxed), 0);
    }
}
