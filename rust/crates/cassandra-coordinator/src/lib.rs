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

pub mod batch;
pub mod consensus;
pub mod consistency;
pub mod counter;
pub mod hint_segment;
pub mod hints;
pub mod paxos;
pub mod read;
pub mod tracing;
pub mod write;
pub mod write_response_handler;

pub use self::tracing::TraceSession;
pub use batch::{
    BatchCoordinator, BatchEntry, BatchGuardrails, BatchLogManager, BatchLogMetrics, BatchType,
};
pub use consensus::ConsensusRouter;
pub use consistency::ConsistencyLevel;
pub use counter::{CounterCoordinator, CounterReplica};
pub use hint_segment::{HintSegmentManager, HintSegmentReader, HintSegmentWriter};
pub use hints::{Hint, HintConfig, HintMetrics, HintStore, HintedHandoffManager};
pub use paxos::{Ballot, CasResult, PaxosConfig, PaxosCoordinator, PaxosReplica, PaxosState};
pub use read::{
    ClusteringSlice, ColumnFilter, CoordinatedRead, DataRange, DataResolver, DataResponse, Digest,
    DigestMismatch, DigestResolver, PageSizeControl, PagingState, PartitionRangeReadCommand,
    PartitionResult, ReadCommand, ReadCoordinator, ReadError, ReadExecutionPlan, ReadExecutorType,
    ReadLimits, ReadMetrics, ReadRepairHandler, ReadRepairMutation, ReadRepairStrategy,
    ReadResponse, ReadResult, ResolvedData, ShortReadProtection, ShortReadRetry,
    SinglePartitionReadCommand, SpeculativeRetryPolicy, TombstoneThresholds, TombstoneTracker,
};
pub use write::{
    CellMutation, CollectionOp, CoordinatedMutation, DatacenterWritePlan, DcReplicaPlan,
    MutationKind, MutationRow, RangeTombstone, TombstoneMarker, ViewFanoutMetrics,
    ViewFanoutResult, WriteCoordinator, WriteError, WriteGuardrails, WriteMetrics, WritePlan,
    WriteResult, WriteType,
};
pub use write_response_handler::{RequestFailureReason, WriteResponseHandler};
