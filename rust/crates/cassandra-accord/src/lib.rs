// Licensed under Apache License, Version 2.0.

//! Accord distributed transaction protocol integration.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.service.accord.AccordService`
//! - `org.apache.cassandra.service.paxos.consensus.*`
//!
//! ## Architecture
//! This crate provides the integration point between Cassandra's coordinator
//! and the Accord distributed transaction protocol. It defines the message
//! structures, the Accord Node interface, and configuration for consensus
//! migration.

pub mod command_store;
pub mod error;
pub mod executor;
pub mod journal;
pub mod migration;
pub mod service;
pub mod task;
pub mod topology;
pub mod types;

pub use command_store::CommandStore;
pub use error::{AccordError, AccordResult};
pub use executor::AccordExecutor;
pub use journal::AccordJournal;
pub use migration::{KeyMigrationState, TableMigrationState};
pub use service::{AccordConfig, AccordService, AccordTxnId};
pub use task::{AccordTask, TaskState};
pub use topology::AccordTopology;
pub use types::{CommandStatus, Keys, Timestamp, Txn, TxnId};
