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

//! # cassandra-coordinator
//!
//! Read/write coordination with consistency level enforcement,
//! hinted handoff, batch logging, and request tracing.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.service.StorageProxy` — coordinator entry points
//! - `org.apache.cassandra.db.ConsistencyLevel` — CL definitions
//! - `org.apache.cassandra.hints` — hinted handoff
//! - `org.apache.cassandra.batchlog` — batch log
//!
//! ## Architecture
//!
//! - [`consistency`] — CL enum and block_for calculations
//! - [`write`] — write coordination with replica fan-out
//! - [`read`] — read coordination with digest comparison
//! - [`hints`] — hinted handoff storage
//! - [`batch`] — distributed batch safety
//! - [`tracing`] — per-request tracing sessions

pub mod consistency;
pub mod write;
pub mod read;
pub mod hints;
pub mod batch;
pub mod tracing;
pub mod paxos;

pub use consistency::ConsistencyLevel;
pub use write::{WriteCoordinator, WriteError, WriteResult, CoordinatedMutation};
pub use read::{ReadCoordinator, ReadError, ReadResult, CoordinatedRead};
pub use hints::HintStore;
pub use batch::BatchLogManager;
pub use self::tracing::TraceSession;
pub use paxos::{Ballot, PaxosState, PaxosCoordinator, PaxosReplica, CasResult};
