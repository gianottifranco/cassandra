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

pub mod service;

pub use service::{AccordConfig, AccordService, AccordTxnId};
