// Licensed under Apache License, Version 2.0.

//! Index metadata.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.schema.IndexMetadata`

use std::collections::HashMap;
use serde::{Deserialize, Serialize};

/// The type of secondary index.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum IndexKind {
    /// Legacy, key-based index (e.g. 2i).
    Keys,
    /// Custom index (e.g. SASI, SAI).
    Custom,
}

/// Metadata for a secondary index.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexMetadata {
    /// ID of the index (typically generated from name).
    pub id: String,
    /// Name of the index.
    pub name: String,
    /// Kind of the index (Keys or Custom).
    pub kind: IndexKind,
    /// Specific options (e.g., target column, class_name for custom indexes).
    #[serde(default)]
    pub options: HashMap<String, String>,
}

impl IndexMetadata {
    pub fn new(id: String, name: String, kind: IndexKind, options: HashMap<String, String>) -> Self {
        Self {
            id,
            name,
            kind,
            options,
        }
    }

    /// Helper to get the target column if specified in options.
    pub fn target_column(&self) -> Option<&String> {
        self.options.get("target")
    }
}
