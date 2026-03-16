// Licensed under Apache License, Version 2.0.

//! Configuration loading and validation for Cassandra Rust.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.config.Config`
//! - `org.apache.cassandra.config.DatabaseDescriptor`

pub mod config;
pub mod guardrails;
pub mod loader;
pub mod partition_denylist;
pub mod units;
pub mod validation;

pub use config::CassandraConfig;
pub use guardrails::{GuardrailAction, GuardrailsConfig, ThresholdGuardrail};
pub use loader::{load_config, load_config_from_str};
pub use partition_denylist::PartitionDenylist;
pub use units::{DataSize, Duration};
