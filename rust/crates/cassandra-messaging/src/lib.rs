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

//! # cassandra-messaging
//!
//! Inter-node messaging protocol for Cassandra Rust.
//!
//! Provides framed TCP transport with length-prefixed messages, verb-based
//! routing, per-verb metrics, and a messaging service with handler registration
//! and request/response correlation.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.net.MessagingService`
//! - `org.apache.cassandra.net.Verb`
//! - `org.apache.cassandra.net.Message`
//!
//! ## Architecture
//!
//! - [`verb`] — message type enum with request/response pairing
//! - [`frame`] — wire-format codec (length-prefixed, Tokio Decoder/Encoder)
//! - [`metrics`] — per-verb counters and latency tracking
//! - [`service`] — central messaging hub with handler dispatch

pub mod connection_type;
pub mod crc;
pub mod forwarding;
pub mod frame;
pub mod frame_codec;
pub mod handshake;
pub mod metrics;
pub mod outbound_connection;
pub mod outbound_connections;
pub mod outbound_queue;
pub mod resource_limits;
pub mod service;
pub mod verb;

pub use frame::{
    CURRENT_MESSAGING_VERSION, MIN_MESSAGING_VERSION, Message, MessageCodec, MessageHeader,
    VersionNegotiation,
};
pub use metrics::{MessagingMetrics, VerbMetrics, VerbMetricsSnapshot};
pub use service::{
    ConnectionPool, MessageHandler, MessagingError, MessagingService, VerbTimeoutConfig,
};
pub use verb::Verb;
