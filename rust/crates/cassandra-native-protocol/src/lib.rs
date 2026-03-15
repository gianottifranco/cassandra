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

//! # cassandra-native-protocol
//!
//! CQL binary protocol v4/v5 frame codec, message types, and connection lifecycle.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.transport`
//! - `org.apache.cassandra.transport.messages`
//!
//! ## Architecture
//!
//! The protocol layer is structured as:
//! 1. **types** – primitive read/write for protocol data types
//! 2. **frame** – frame header + body with `tokio_util::codec` integration
//! 3. **message** – parsed message types (requests + responses)
//! 4. **request** – decoders for client→server messages
//! 5. **response** – encoders for server→client messages
//! 6. **compress** – LZ4/Snappy frame body compression
//! 7. **auth** – authenticator trait and implementations
//! 8. **error_codes** – CassandraError → protocol error mapping

pub mod types;
pub mod frame;
pub mod message;
pub mod request;
pub mod response;
pub mod error_codes;
pub mod auth;

#[cfg(any(feature = "compression-lz4", feature = "compression-snappy"))]
pub mod compress;
