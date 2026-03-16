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

//! # cassandra-streaming
//!
//! SSTable streaming for bootstrap, decommission, rebuild, repair.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.streaming`
//! - `org.apache.cassandra.db.streaming`
//!
//! ## Architecture
//!
//! - [`session`] — bidirectional streaming session with state machine
//! - [`plan`] — describes what ranges to stream between which nodes
//! - [`transfer`] — chunk-based file transfer with checksums and rate limiting
//! - [`manager`] — singleton managing all active stream sessions
//! - [`metrics`] — atomic counters for streaming progress
//! - [`snapshot`] — snapshot reference management for outgoing transfers

pub mod session;
pub mod plan;
pub mod transfer;
pub mod manager;
pub mod metrics;
pub mod snapshot;

pub use session::{StreamSession, StreamSessionState, StreamSessionId};
pub use plan::{StreamPlan, StreamRequest, StreamOperation};
pub use transfer::{
    StreamTransfer, TransferState, ChunkChecksum, ChecksumAlgorithm,
    StreamRateLimiter, StreamRetryPolicy, DataChunk,
};
pub use manager::StreamManager;
pub use metrics::StreamingMetrics;
pub use snapshot::{SnapshotTransferRef, SnapshotManager};
