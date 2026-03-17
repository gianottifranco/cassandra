// Licensed under Apache License, Version 2.0.

//! Configuration loading and validation for Cassandra Rust.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.config.Config`
//! - `org.apache.cassandra.config.DatabaseDescriptor`

pub mod config;
pub mod database_descriptor;
pub mod guardrails;
pub mod hot_reload;
pub mod loader;
pub mod partition_denylist;
pub mod properties;
pub mod sai_options;
pub mod units;
pub mod validation;

pub use config::CassandraConfig;
pub use database_descriptor::{ConfigSnapshot, DatabaseDescriptor};
pub use guardrails::{
    EnableFlagGuardrail, Guardrail, GuardrailAction, GuardrailRegistry, GuardrailViolation,
    GuardrailsConfig, PasswordPolicyGuardrail, ThresholdGuardrail, ValuesGuardrail,
};
pub use hot_reload::{ConfigWatcher, ConfigWatcherError};
pub use loader::{load_config, load_config_from_str};
pub use partition_denylist::PartitionDenylist;
pub use properties::apply_overrides;
pub use sai_options::StorageAttachedIndexOptions;
pub use units::{DataRateSpec, DataSize, Duration};
