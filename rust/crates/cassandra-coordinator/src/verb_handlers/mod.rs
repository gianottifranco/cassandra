// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.
// SPDX-License-Identifier: Apache-2.0

//! Replica-side verb handlers for internode messages.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.db.MutationVerbHandler`
//! - `org.apache.cassandra.db.ReadCommandVerbHandler`
//! - `org.apache.cassandra.hints.HintVerbHandler`
//! - `org.apache.cassandra.batchlog.BatchStoreVerbHandler`
//! - `org.apache.cassandra.batchlog.BatchRemoveVerbHandler`
//! - `org.apache.cassandra.service.reads.repair.ReadRepairHandler`
//!
//! ## Architecture
//!
//! Each handler receives a `Message` from the messaging service, deserializes
//! the payload, processes it locally, and returns an optional response message.
//! Handlers are registered with `MessagingService::register_handler()`.

pub mod batch_handler;
pub mod hint_handler;
pub mod mutation_handler;
pub mod read_handler;
pub mod read_repair_handler;
pub mod registration;

pub use batch_handler::{BatchRemoveVerbHandler, BatchStoreVerbHandler};
pub use hint_handler::HintVerbHandler;
pub use mutation_handler::MutationVerbHandler;
pub use read_handler::{ReadDataVerbHandler, ReadDigestVerbHandler};
pub use read_repair_handler::ReadRepairVerbHandler;
pub use registration::register_all_verb_handlers;
