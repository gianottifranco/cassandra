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

//! Workspace-level integration tests.
//!
//! These tests verify structural properties of the workspace that are
//! valuable even before any business logic is implemented.
//!
//! Located in cassandra-common as integration tests since virtual
//! workspaces cannot host integration tests directly.

use std::path::Path;

/// Returns the workspace root (rust/) from CARGO_MANIFEST_DIR.
/// CARGO_MANIFEST_DIR = rust/crates/cassandra-common
fn workspace_root() -> &'static Path {
    // CARGO_MANIFEST_DIR = .../rust/crates/cassandra-common
    // parent = .../rust/crates
    // parent = .../rust
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
}

/// Returns the repository root (cassandra/) from CARGO_MANIFEST_DIR.
fn repo_root() -> &'static Path {
    workspace_root().parent().unwrap()
}

/// Verify that all expected crate directories exist in the workspace.
#[test]
fn all_crates_exist() {
    let expected_crates = [
        "cassandra-common",
        "cassandra-config",
        "cassandra-types",
        "cassandra-schema",
        "cassandra-native-protocol",
        "cassandra-cql",
        "cassandra-storage",
        "cassandra-cluster-metadata",
        "cassandra-messaging",
        "cassandra-coordinator",
        "cassandra-repair",
        "cassandra-streaming",
        "cassandra-security",
        "cassandra-admin",
        "cassandra-server",
        "cassandra-tools",
    ];

    for crate_name in &expected_crates {
        let crate_dir = workspace_root().join("crates").join(crate_name);
        assert!(
            crate_dir.exists(),
            "Expected crate directory missing: {}",
            crate_dir.display()
        );

        let cargo_toml = crate_dir.join("Cargo.toml");
        assert!(
            cargo_toml.exists(),
            "Missing Cargo.toml in crate: {}",
            crate_name
        );
    }
}

/// Verify that the documentation directory structure exists.
#[test]
fn documentation_exists() {
    let expected_files = [
        "docs/rewrite/charter.md",
        "docs/rewrite/feature_matrix.yaml",
        "docs/rewrite/adrs/001-compatibility-criteria.md",
        "docs/rewrite/adrs/002-unsafe-policy.md",
        "docs/rewrite/adrs/003-feature-flags.md",
        "docs/rewrite/adrs/004-on-disk-formats.md",
        "docs/rewrite/adrs/005-runtime-concurrency.md",
        "docs/rewrite/adrs/006-java-oracle-policy.md",
    ];

    for file in &expected_files {
        let path = repo_root().join(file);
        assert!(
            path.exists(),
            "Expected documentation file missing: {}",
            file
        );
    }
}

/// Verify that the feature matrix YAML can be parsed and contains expected domains.
#[test]
fn feature_matrix_parseable() {
    let matrix_path = repo_root().join("docs/rewrite/feature_matrix.yaml");
    let content = std::fs::read_to_string(&matrix_path)
        .unwrap_or_else(|e| panic!("Cannot read feature_matrix.yaml: {}", e));

    // Verify key domains are present
    let expected_domains = [
        "Common Utilities",
        "Configuration",
        "Type System",
        "Schema",
        "Native Protocol",
        "CQL Language",
        "Storage Engine",
        "Cluster Metadata",
        "Inter-node Messaging",
        "Coordinator Path",
        "Repair & Anti-Entropy",
        "Streaming",
        "Security",
        "Admin & Tooling",
    ];

    for domain in &expected_domains {
        assert!(
            content.contains(domain),
            "Feature matrix missing domain: {}",
            domain
        );
    }

    // Verify baseline info
    assert!(content.contains("076c6f11364645bbb43360f013bee6f50a099185"));
    assert!(content.contains("2026-03-15"));
}

/// Verify that no unsafe code exists in the workspace (Phase 1 baseline).
///
/// Allowlisted paths:
/// - `cassandra-io/src/util/mmap_rebufferer.rs` — memory-mapped file I/O
/// - `cassandra-io/src/util/channel_proxy.rs` — file descriptor aliasing
/// - `cassandra-io/src/util/rebufferer.rs` — memory-mapped buffer management
///
/// These use `unsafe` for legitimate memory-mapped I/O via the `memmap2` crate.
/// See ADR-002 (unsafe-policy) for the project's unsafe code policy.
#[test]
fn no_unsafe_in_phase1() {
    let crates_dir = workspace_root().join("crates");

    // Files that legitimately need unsafe:
    // - cassandra-io/src/util/*: memory-mapped I/O via memmap2 crate
    // - cassandra-config/src/properties.rs: std::env::set_var is unsafe in Rust 2024
    let unsafe_allowlist: &[&str] = &[
        "cassandra-io/src/util/mmap_rebufferer.rs",
        "cassandra-io/src/util/channel_proxy.rs",
        "cassandra-io/src/util/rebufferer.rs",
        "cassandra-config/src/properties.rs",
    ];

    for entry in std::fs::read_dir(&crates_dir).unwrap() {
        let entry = entry.unwrap();
        if !entry.file_type().unwrap().is_dir() {
            continue;
        }
        let src_dir = entry.path().join("src");
        if !src_dir.exists() {
            continue;
        }
        check_no_unsafe_recursive(&src_dir, &crates_dir, unsafe_allowlist);
    }
}

fn check_no_unsafe_recursive(dir: &Path, crates_dir: &Path, allowlist: &[&str]) {
    for entry in std::fs::read_dir(dir).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        if path.is_dir() {
            check_no_unsafe_recursive(&path, crates_dir, allowlist);
        } else if path.extension().is_some_and(|ext| ext == "rs") {
            // Check if this file is in the allowlist
            let relative = path
                .strip_prefix(crates_dir)
                .unwrap_or(&path)
                .to_string_lossy();
            let is_allowed = allowlist.iter().any(|allowed| relative.ends_with(allowed));
            if is_allowed {
                continue;
            }

            let content = std::fs::read_to_string(&path).unwrap();
            // Look for `unsafe` keyword not in comments
            for (line_num, line) in content.lines().enumerate() {
                let trimmed = line.trim();
                if trimmed.starts_with("//") {
                    continue;
                }
                assert!(
                    !trimmed.contains("unsafe "),
                    "Unexpected unsafe code at {}:{}: {}",
                    path.display(),
                    line_num + 1,
                    trimmed,
                );
            }
        }
    }
}

/// Verify version constants are consistent.
#[test]
fn version_consistency() {
    use cassandra_common::version;

    assert_eq!(version::JAVA_BASELINE_BRANCH, "trunk");
    assert_eq!(
        version::JAVA_BASELINE_COMMIT,
        "076c6f11364645bbb43360f013bee6f50a099185"
    );
    assert_eq!(version::JAVA_BASELINE_DATE, "2026-03-15");
    assert!(version::SUPPORTED_PROTOCOL_VERSIONS.contains(&4));
    assert!(version::SUPPORTED_PROTOCOL_VERSIONS.contains(&5));
}

/// Verify error codes match CQL protocol specification.
#[test]
fn error_code_compliance() {
    use cassandra_common::error::CassandraError;

    // Verify a selection of error codes against the CQL native protocol spec
    let test_cases: Vec<(CassandraError, Option<i32>)> = vec![
        (CassandraError::ServerError("".into()), Some(0x0000)),
        (CassandraError::ProtocolError("".into()), Some(0x000A)),
        (CassandraError::AuthenticationError("".into()), Some(0x0100)),
        (
            CassandraError::Unavailable {
                consistency: "QUORUM".into(),
                required: 2,
                alive: 1,
            },
            Some(0x1000),
        ),
        (CassandraError::Overloaded, Some(0x1001)),
        (CassandraError::IsBootstrapping, Some(0x1002)),
        (CassandraError::TruncateError("".into()), Some(0x1003)),
        (
            CassandraError::WriteTimeout {
                consistency: "ONE".into(),
                received: 0,
                block_for: 1,
                write_type: "SIMPLE".into(),
            },
            Some(0x1100),
        ),
        (
            CassandraError::ReadTimeout {
                consistency: "ONE".into(),
                received: 0,
                block_for: 1,
                data_present: false,
            },
            Some(0x1200),
        ),
        (CassandraError::SyntaxError("".into()), Some(0x2000)),
        (CassandraError::Unauthorized("".into()), Some(0x2100)),
        (CassandraError::InvalidQuery("".into()), Some(0x2200)),
        (CassandraError::ConfigError("".into()), Some(0x2300)),
        (
            CassandraError::AlreadyExists {
                ks: "ks".into(),
                table: "tb".into(),
            },
            Some(0x2400),
        ),
        (CassandraError::Unprepared(vec![]), Some(0x2500)),
        (CassandraError::Internal("".into()), None),
    ];

    for (err, expected_code) in test_cases {
        assert_eq!(
            err.error_code(),
            expected_code,
            "Error code mismatch for {:?}",
            err
        );
    }
}

/// Verify each crate has the Apache license header in its lib.rs or main.rs.
#[test]
fn all_crates_have_license_header() {
    let crates_dir = workspace_root().join("crates");

    for entry in std::fs::read_dir(&crates_dir).unwrap() {
        let entry = entry.unwrap();
        if !entry.file_type().unwrap().is_dir() {
            continue;
        }
        let crate_name = entry.file_name();
        let src_dir = entry.path().join("src");

        let main_file = if src_dir.join("lib.rs").exists() {
            src_dir.join("lib.rs")
        } else if src_dir.join("main.rs").exists() {
            src_dir.join("main.rs")
        } else {
            panic!("No lib.rs or main.rs in {:?}", crate_name);
        };

        let content = std::fs::read_to_string(&main_file).unwrap();
        assert!(
            content.contains("Apache License, Version 2.0"),
            "Missing Apache license header in {:?}",
            crate_name,
        );
    }
}
