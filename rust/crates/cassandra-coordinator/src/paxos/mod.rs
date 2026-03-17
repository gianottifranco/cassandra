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

//! # Paxos / Lightweight Transactions (LWT)
//!
//! Single-decree Paxos implementation for CAS operations.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.service.paxos.PaxosState`
//! - `org.apache.cassandra.service.paxos.Commit`
//! - `org.apache.cassandra.service.StorageProxy.cas()`
//!
//! ## Design
//!
//! Each partition key gets an independent Paxos instance (single-decree).
//! The coordinator drives prepare → propose → commit across a quorum of
//! replicas.  Serial consistency (SERIAL / LOCAL_SERIAL) is enforced by
//! requiring quorum participation in the Paxos round before evaluating
//! the IF condition and applying the mutation.
//!
//! ## Modules
//!
//! - [`ballot`] — Ballot type with total ordering
//! - [`state`] — Per-partition Paxos state machine
//! - [`messages`] — Inter-node Paxos message types
//! - [`coordinator`] — CAS coordinator orchestrating the full round

pub mod ballot;
pub mod cleanup;
pub mod contention;
pub mod coordinator;
pub mod messages;
pub mod repair;
pub mod state;
pub mod storage;

pub use ballot::Ballot;
pub use cleanup::PaxosCleanup;
pub use contention::{ConstantBackoff, ContentionStrategy, ExponentialBackoff};
pub use coordinator::{
    CasResult, PaxosConfig, PaxosCoordinator, PaxosCoordinatorError, PaxosReplica,
};
pub use messages::{PaxosAccept, PaxosCommit, PaxosPrepare, PaxosPromise, PaxosPropose};
pub use repair::PaxosRepair;
pub use state::{PaxosState, Proposal};
pub use storage::PaxosStorage;
