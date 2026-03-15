// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

//! Golden fixture loader.
//!
//! Provides utilities for loading pre-generated golden fixtures from the
//! `diff-tests/golden/` directory for offline comparison tests.

use serde::de::DeserializeOwned;
use std::path::{Path, PathBuf};

/// Returns the path to the golden fixtures directory.
///
/// Resolves relative to the workspace root (assumes this crate is at
/// `rust/crates/cassandra-diff-tests/`).
pub fn golden_dir() -> PathBuf {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    // Navigate from crates/cassandra-diff-tests/ to diff-tests/golden/
    manifest_dir
        .parent() // crates/
        .and_then(|p| p.parent()) // rust/
        .map(|p| p.join("diff-tests").join("golden"))
        .unwrap_or_else(|| PathBuf::from("diff-tests/golden"))
}

/// Load a JSON golden fixture file.
pub fn load_json<T: DeserializeOwned>(relative_path: &str) -> Result<T, GoldenError> {
    let path = golden_dir().join(relative_path);
    load_json_from_path(&path)
}

/// Load a JSON golden fixture from an absolute path.
pub fn load_json_from_path<T: DeserializeOwned>(path: &Path) -> Result<T, GoldenError> {
    let content = std::fs::read_to_string(path).map_err(|e| GoldenError::Io {
        path: path.to_path_buf(),
        source: e,
    })?;
    serde_json::from_str(&content).map_err(|e| GoldenError::Parse {
        path: path.to_path_buf(),
        source: e,
    })
}

/// Load a binary golden fixture file.
pub fn load_binary(relative_path: &str) -> Result<Vec<u8>, GoldenError> {
    let path = golden_dir().join(relative_path);
    std::fs::read(&path).map_err(|e| GoldenError::Io { path, source: e })
}

/// List all golden fixture files in a subdirectory.
pub fn list_fixtures(subdir: &str) -> Result<Vec<PathBuf>, GoldenError> {
    let dir = golden_dir().join(subdir);
    if !dir.exists() {
        return Ok(vec![]);
    }
    let mut files = Vec::new();
    for entry in std::fs::read_dir(&dir).map_err(|e| GoldenError::Io {
        path: dir.clone(),
        source: e,
    })? {
        let entry = entry.map_err(|e| GoldenError::Io {
            path: dir.clone(),
            source: e,
        })?;
        if entry.file_type().map(|t| t.is_file()).unwrap_or(false) {
            files.push(entry.path());
        }
    }
    files.sort();
    Ok(files)
}

/// Errors that can occur while loading golden fixtures.
#[derive(Debug)]
pub enum GoldenError {
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
    Parse {
        path: PathBuf,
        source: serde_json::Error,
    },
}

impl std::fmt::Display for GoldenError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GoldenError::Io { path, source } => {
                write!(f, "IO error reading {:?}: {}", path, source)
            }
            GoldenError::Parse { path, source } => {
                write!(f, "Parse error in {:?}: {}", path, source)
            }
        }
    }
}

impl std::error::Error for GoldenError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn golden_dir_resolves() {
        let dir = golden_dir();
        assert!(
            dir.ends_with("diff-tests/golden"),
            "Expected path ending with diff-tests/golden, got {:?}",
            dir
        );
    }

    #[test]
    fn load_error_codes_json() {
        use crate::comparators::error::ErrorCodeEntry;
        use std::collections::HashMap;

        let result: Result<HashMap<String, ErrorCodeEntry>, _> =
            load_json("errors/protocol_error_codes.json");
        assert!(
            result.is_ok(),
            "Failed to load error codes: {:?}",
            result.err()
        );

        let map = result.unwrap();
        assert!(map.contains_key("0x0000"), "Missing SERVER_ERROR");
        assert!(map.contains_key("0x2000"), "Missing SYNTAX_ERROR");
        assert_eq!(map.len(), 20, "Expected 20 error codes");
    }

    #[test]
    fn load_type_serialization_reference() {
        let result: Result<serde_json::Value, _> = load_json("types/serialization_reference.json");
        assert!(
            result.is_ok(),
            "Failed to load type reference: {:?}",
            result.err()
        );

        let value = result.unwrap();
        let types = value.get("types").expect("Missing 'types' key");
        assert!(types.get("ascii").is_some(), "Missing ascii type");
        assert!(types.get("int").is_some(), "Missing int type");
    }

    #[test]
    fn list_stubs_fixtures() {
        let fixtures = list_fixtures("stubs").unwrap();
        assert!(
            fixtures.len() >= 3,
            "Expected at least 3 stub manifests, got {}",
            fixtures.len()
        );
    }

    #[test]
    fn load_nonexistent_returns_error() {
        let result: Result<serde_json::Value, _> = load_json("nonexistent/file.json");
        assert!(result.is_err());
    }
}
