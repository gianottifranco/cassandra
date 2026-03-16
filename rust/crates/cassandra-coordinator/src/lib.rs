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
//! - [`write_response_handler`] — async ack collector for write CL enforcement
//! - [`read`] — read coordination with digest comparison
//! - [`hints`] — hinted handoff storage and lifecycle
//! - [`batch`] — batch coordinator with batchlog protocol
//! - [`tracing`] — per-request tracing sessions

pub mod consistency;
pub mod write;
pub mod write_response_handler;
pub mod read;
pub mod hints;
pub mod hint_segment;
pub mod batch;
pub mod tracing;
pub mod paxos;
pub mod counter;
pub mod consensus;

pub use consistency::ConsistencyLevel;
pub use write::{
    WriteCoordinator, WriteError, WriteResult, WriteType, WritePlan,
    CoordinatedMutation, MutationKind, MutationRow, CellMutation,
    CollectionOp, TombstoneMarker, RangeTombstone, WriteMetrics,
    DatacenterWritePlan, DcReplicaPlan, ViewFanoutResult, WriteGuardrails,
    ViewFanoutMetrics,
};
pub use write_response_handler::{WriteResponseHandler, RequestFailureReason};
pub use read::{
    ReadCoordinator, ReadError, ReadResult, CoordinatedRead,
    ReadMetrics, ReadCommand, SinglePartitionReadCommand, PartitionRangeReadCommand,
    ReadLimits, ColumnFilter, ClusteringSlice, DataRange,
    PagingState, PageSizeControl,
    SpeculativeRetryPolicy, ReadExecutionPlan, ReadExecutorType,
    DataResolver, DigestResolver, DigestMismatch, ResolvedData,
    DataResponse, Digest, ReadResponse, PartitionResult,
    TombstoneThresholds, TombstoneTracker,
    ShortReadProtection, ShortReadRetry,
    ReadRepairHandler, ReadRepairMutation, ReadRepairStrategy,
};
pub use hints::{HintStore, HintedHandoffManager, HintConfig, HintMetrics, Hint};
pub use hint_segment::{HintSegmentWriter, HintSegmentReader, HintSegmentManager};
pub use batch::{
    BatchLogManager, BatchCoordinator, BatchType, BatchEntry,
    BatchGuardrails, BatchLogMetrics,
};
pub use self::tracing::TraceSession;
pub use paxos::{Ballot, PaxosState, PaxosCoordinator, PaxosReplica, CasResult, PaxosConfig};
pub use counter::{CounterCoordinator, CounterReplica};
pub use consensus::ConsensusRouter;
