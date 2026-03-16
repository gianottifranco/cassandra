// Licensed under Apache License, Version 2.0.

//! Configuration loading and validation for Cassandra Rust.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.config.Config`
//! - `org.apache.cassandra.config.DatabaseDescriptor`

pub mod units;
pub mod config;
pub mod validation;
pub mod loader;
pub mod guardrails;
pub mod partition_denylist;

pub use config::CassandraConfig;
pub use loader::{load_config, load_config_from_str};
pub use units::{DataSize, Duration};
pub use guardrails::{GuardrailsConfig, GuardrailAction, ThresholdGuardrail};
pub use partition_denylist::PartitionDenylist;
