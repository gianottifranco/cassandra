// Licensed under Apache License, Version 2.0.

//! # cassandra-repair
//!
//! Incremental and full repair, Merkle tree exchange, anti-compaction.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.repair`
//! - `org.apache.cassandra.db.repair`
//! - `org.apache.cassandra.utils.MerkleTree`
//!
//! ## Architecture
//!
//! - [`merkle`] — Merkle tree for range-based anti-entropy
//! - [`coordinator`] — drives full and incremental repair
//! - [`session`] — per-range repair session between coordinator and replicas
//! - [`metrics`] — atomic counters for repair progress

pub mod anti_compaction;
pub mod consistent_coordinator;
pub mod consistent_local;
pub mod coordinator;
pub mod history;
pub mod job;
pub mod merkle;
pub mod messages;
pub mod metrics;
pub mod options;
pub mod repair_config;
pub mod session;
pub mod sync_task;
pub mod validator;
pub mod virtual_tables;

pub use anti_compaction::{AntiCompactionRequest, AntiCompactionResult, RepairedState};
pub use consistent_coordinator::{CoordinatorAction, CoordinatorSession};
pub use consistent_local::{LocalSession, LocalSessionStore, PendingRepairTracker};
pub use coordinator::{RepairCoordinator, RepairError, RepairType};
pub use history::{LoggingRepairHistoryTracker, RepairHistoryTracker};
pub use job::{DiffResult, RepairJobResult, SyncTaskDescriptor};
pub use merkle::MerkleTree;
pub use messages::{ConsistentSessionState, RepairMessage};
pub use options::{RepairOption, RepairParallelism, RepairRange};
pub use repair_config::{MerkleTreeResponseSpec, RepairConfig, RetrySpec};
pub use sync_task::{LocalSyncTask, RemoteSyncTask, StreamingRepairTask, SyncResult};
pub use validator::{hash_partition_data, validate, validate_from_hashes};
pub use virtual_tables::{InMemoryRepairHistory, RepairHistoryEntry};
