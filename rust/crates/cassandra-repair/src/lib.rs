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

pub mod coordinator;
pub mod history;
pub mod merkle;
pub mod metrics;
pub mod session;

pub use coordinator::{RepairCoordinator, RepairError, RepairType};
pub use history::{LoggingRepairHistoryTracker, RepairHistoryTracker};
pub use merkle::MerkleTree;
