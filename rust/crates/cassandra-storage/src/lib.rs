// Licensed under Apache License, Version 2.0.

//! # Cassandra Storage Engine
//!
//! Complete storage engine for the Rust rewrite of Apache Cassandra.
//! Provides commit log (WAL), memtables, SSTables, compaction, CDC,
//! snapshots, and incremental backups.
//!
//! ## Module Status
//!
//! | Module     | Status       | Java Oracle                        |
//! |------------|-------------|-------------------------------------|
//! | commitlog  | Functional  | `o.a.c.db.commitlog.CommitLog`      |
//! | memtable   | Functional  | `o.a.c.db.Memtable`                 |
//! | sstable    | Functional  | `o.a.c.io.sstable.format.*`         |
//! | compaction | Functional  | `o.a.c.db.compaction.*`             |
//! | engine     | Functional  | `o.a.c.db.ColumnFamilyStore`        |
//! | cdc        | Functional  | `o.a.c.db.commitlog.CommitLog(CDC)` |
//! | backup     | Functional  | `o.a.c.db.Keyspace.snapshot()`      |
//! | counter    | Functional  | `o.a.c.db.CounterMutation`          |
//! | index      | Functional  | `o.a.c.index.*`                     |
//!
//! ## Features
//!
//! | Feature      | Description                               |
//! |--------------|-------------------------------------------|
//! | `cdc`        | Change Data Capture                       |
//! | `ucs`        | Unified Compaction Strategy (experimental) |
//! | `triggers`   | User-defined triggers                     |
//! | `udfs`       | User-defined functions                    |
//! | `mat-views`  | Materialized views                        |

pub mod backup;
pub mod cdc;
pub mod commitlog;
pub mod compaction;
pub mod engine;
pub mod memtable;
pub mod sstable;

// Feature-gated re-exports
pub use engine::{EngineConfig, EngineStats, StorageEngine};
pub use commitlog::{CommitLog, CommitLogConfig, Mutation};
pub use sstable::{SSTableDescriptor, SSTableReader, SSTableWriter, BtiReader, BtiWriter};
pub use sstable::format::SSTableFormat;
pub use compaction::CompactionStrategyType;
pub use memtable::MemtableType;

// ─── Modules with stubs (from original crate) ─────────────────────────────

/// Counter mutation support.
pub mod counter;

/// Secondary index support.
pub mod index;

/// Materialized view support (feature-gated).
#[cfg(feature = "materialized-views")]
pub mod materialized_views;

/// Trigger support (feature-gated).
#[cfg(feature = "triggers")]
pub mod triggers;

/// UDF support (feature-gated).
#[cfg(feature = "udfs")]
pub mod udf {
    //! User-defined function operations.
    //! ## Java Oracle: `org.apache.cassandra.cql3.functions.*`
}
