// Licensed under Apache License, Version 2.0.

//! Index metadata.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.schema.IndexMetadata`

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// The type of secondary index.
///
/// ## Java Oracle
/// - `org.apache.cassandra.schema.IndexMetadata.Kind`
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum IndexKind {
    /// Legacy, key-based index (e.g. 2i).
    Keys,
    /// Composites index (multi-column key index).
    Composites,
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
    /// Kind of the index (Keys, Composites, or Custom).
    pub kind: IndexKind,
    /// Specific options (e.g., target column, class_name for custom indexes).
    #[serde(default)]
    pub options: HashMap<String, String>,
}

impl IndexMetadata {
    pub fn new(
        id: String,
        name: String,
        kind: IndexKind,
        options: HashMap<String, String>,
    ) -> Self {
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

    /// Returns `true` if this is a legacy key-based index.
    pub fn is_keys(&self) -> bool {
        self.kind == IndexKind::Keys
    }

    /// Returns `true` if this is a composites index.
    pub fn is_composites(&self) -> bool {
        self.kind == IndexKind::Composites
    }

    /// Returns `true` if this is a custom index (SAI, SASI, etc.).
    pub fn is_custom(&self) -> bool {
        self.kind == IndexKind::Custom
    }

    /// Extract the `class_name` from options, if present.
    pub fn index_class_name(&self) -> Option<&String> {
        self.options.get("class_name")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serde_round_trip_keys() {
        let idx = IndexMetadata::new(
            "id1".into(),
            "my_idx".into(),
            IndexKind::Keys,
            HashMap::new(),
        );
        let json = serde_json::to_string(&idx).unwrap();
        let back: IndexMetadata = serde_json::from_str(&json).unwrap();
        assert_eq!(idx, back);
        assert!(back.is_keys());
        assert!(!back.is_custom());
        assert!(!back.is_composites());
    }

    #[test]
    fn serde_round_trip_composites() {
        let idx = IndexMetadata::new(
            "id2".into(),
            "comp_idx".into(),
            IndexKind::Composites,
            HashMap::new(),
        );
        let json = serde_json::to_string(&idx).unwrap();
        assert!(json.contains("COMPOSITES"));
        let back: IndexMetadata = serde_json::from_str(&json).unwrap();
        assert_eq!(idx, back);
        assert!(back.is_composites());
    }

    #[test]
    fn serde_round_trip_custom() {
        let mut opts = HashMap::new();
        opts.insert(
            "class_name".into(),
            "org.apache.cassandra.index.sai.StorageAttachedIndex".into(),
        );
        opts.insert("target".into(), "col1".into());
        let idx = IndexMetadata::new("id3".into(), "sai_idx".into(), IndexKind::Custom, opts);
        let json = serde_json::to_string(&idx).unwrap();
        let back: IndexMetadata = serde_json::from_str(&json).unwrap();
        assert_eq!(idx, back);
        assert!(back.is_custom());
        assert_eq!(
            back.index_class_name().unwrap(),
            "org.apache.cassandra.index.sai.StorageAttachedIndex"
        );
        assert_eq!(back.target_column().unwrap(), "col1");
    }
}
