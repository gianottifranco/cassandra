// Licensed under Apache License, Version 2.0.

//! Upgrade/migration version compatibility matrix.
//!
//! Defines which Java Cassandra versions can migrate to Rust, what
//! format conversions are needed, and which combinations are explicitly
//! unsupported.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.utils.CassandraVersion`
//! - Upgrade dtest matrix in `cassandra-dtest`

use serde::{Deserialize, Serialize};
use std::fmt;

// ─── Version Types ────────────────────────────────────────────────────────

/// Semantic version of a Cassandra release.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct CassandraVersion {
    pub major: u32,
    pub minor: u32,
    pub patch: u32,
    pub variant: VersionVariant,
}

/// Whether the version is the original Java or the Rust rewrite.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum VersionVariant {
    Java,
    Rust,
}

impl fmt::Display for CassandraVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let suffix = match self.variant {
            VersionVariant::Java => "",
            VersionVariant::Rust => "-rust",
        };
        write!(f, "{}.{}.{}{}", self.major, self.minor, self.patch, suffix)
    }
}

impl CassandraVersion {
    pub fn java(major: u32, minor: u32, patch: u32) -> Self {
        Self {
            major,
            minor,
            patch,
            variant: VersionVariant::Java,
        }
    }

    pub fn rust(major: u32, minor: u32, patch: u32) -> Self {
        Self {
            major,
            minor,
            patch,
            variant: VersionVariant::Rust,
        }
    }
}

// ─── Compatibility Level ──────────────────────────────────────────────────

/// Level of compatibility between two versions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompatibilityLevel {
    /// Direct migration supported; formats are compatible or auto-converted.
    FullySupported,
    /// Migration requires explicit SSTable and/or schema conversion step.
    RequiresConversion { steps: Vec<String> },
    /// Migration path exists but is experimental / not fully validated.
    Experimental { reason: String },
    /// Migration is NOT supported between these versions.
    Unsupported { reason: String },
}

impl CompatibilityLevel {
    pub fn is_supported(&self) -> bool {
        !matches!(self, CompatibilityLevel::Unsupported { .. })
    }
}

// ─── Upgrade Check ────────────────────────────────────────────────────────

/// Result of a pre-migration compatibility check.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpgradeCheckResult {
    pub source: CassandraVersion,
    pub target: CassandraVersion,
    pub compatibility: CompatibilityLevel,
    pub sstable_format: SSTableFormatCompat,
    pub schema_compatible: bool,
    pub mixed_cluster_supported: bool,
    pub warnings: Vec<String>,
    pub required_prechecks: Vec<String>,
}

/// SSTable format compatibility detail.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SSTableFormatCompat {
    pub source_format: String,
    pub target_format: String,
    pub conversion_needed: bool,
    pub converter_available: bool,
}

// ─── Matrix ───────────────────────────────────────────────────────────────

/// Check upgrade compatibility between two Cassandra versions.
///
/// This is the primary entry point for pre-migration validation.
pub fn check_upgrade_path(
    source: &CassandraVersion,
    target: &CassandraVersion,
) -> UpgradeCheckResult {
    let (compatibility, sstable_compat, warnings, prechecks) = evaluate_path(source, target);

    UpgradeCheckResult {
        source: source.clone(),
        target: target.clone(),
        compatibility,
        sstable_format: sstable_compat,
        schema_compatible: true, // CQL schema is always exportable/importable
        mixed_cluster_supported: false, // Per ADR-014
        warnings,
        required_prechecks: prechecks,
    }
}

fn evaluate_path(
    source: &CassandraVersion,
    target: &CassandraVersion,
) -> (
    CompatibilityLevel,
    SSTableFormatCompat,
    Vec<String>,
    Vec<String>,
) {
    // Only Java → Rust migration paths are relevant.
    if target.variant != VersionVariant::Rust {
        return (
            CompatibilityLevel::Unsupported {
                reason: "Target must be Rust variant".into(),
            },
            SSTableFormatCompat {
                source_format: "big".into(),
                target_format: "big".into(),
                conversion_needed: false,
                converter_available: false,
            },
            vec![],
            vec![],
        );
    }

    if source.variant != VersionVariant::Java {
        return (
            CompatibilityLevel::Unsupported {
                reason: "Source must be Java variant for Java→Rust migration".into(),
            },
            SSTableFormatCompat {
                source_format: "unknown".into(),
                target_format: "rust-native".into(),
                conversion_needed: false,
                converter_available: false,
            },
            vec![],
            vec![],
        );
    }

    match (source.major, source.minor) {
        // Cassandra 4.0.x → Rust: requires SSTable conversion
        (4, 0) => (
            CompatibilityLevel::RequiresConversion {
                steps: vec![
                    "Export schema via DESCRIBE SCHEMA".into(),
                    "Take full snapshot of all keyspaces".into(),
                    "Convert SSTables using cassandra-migration sstable-import".into(),
                    "Apply schema to Rust cluster".into(),
                    "Import converted SSTables".into(),
                ],
            },
            SSTableFormatCompat {
                source_format: "big-ma".into(),
                target_format: "rust-native".into(),
                conversion_needed: true,
                converter_available: true,
            },
            vec![
                "Java 4.0 uses big-ma SSTable format; conversion is required".into(),
                "Ensure no compactions are running during snapshot".into(),
            ],
            vec![
                "Verify all nodes are UN (Up/Normal)".into(),
                "Disable auto-compaction on Java cluster".into(),
                "Take full snapshot".into(),
                "Enable FQL for shadow traffic validation".into(),
            ],
        ),

        // Cassandra 4.1.x → Rust: standard path
        (4, 1) => (
            CompatibilityLevel::RequiresConversion {
                steps: vec![
                    "Export schema via DESCRIBE SCHEMA".into(),
                    "Take full snapshot".into(),
                    "Convert SSTables (big format)".into(),
                    "Apply schema to Rust cluster".into(),
                    "Import converted SSTables".into(),
                ],
            },
            SSTableFormatCompat {
                source_format: "big-nb".into(),
                target_format: "rust-native".into(),
                conversion_needed: true,
                converter_available: true,
            },
            vec![],
            vec![
                "Verify all nodes are UN".into(),
                "Take full snapshot".into(),
            ],
        ),

        // Cassandra 5.0.x → Rust: big or bti format
        (5, 0) => (
            CompatibilityLevel::RequiresConversion {
                steps: vec![
                    "Export schema via DESCRIBE SCHEMA".into(),
                    "Determine SSTable format (big or bti)".into(),
                    "Take full snapshot".into(),
                    "Convert SSTables".into(),
                    "Apply schema to Rust cluster".into(),
                    "Import converted SSTables".into(),
                ],
            },
            SSTableFormatCompat {
                source_format: "big-nb or bti".into(),
                target_format: "rust-native".into(),
                conversion_needed: true,
                converter_available: true,
            },
            vec![
                "5.0 may use either big or bti format; check sstable_format in cassandra.yaml"
                    .into(),
            ],
            vec![
                "Verify all nodes are UN".into(),
                "Identify SSTable format in use".into(),
                "Take full snapshot".into(),
            ],
        ),

        // Cassandra 3.x → NOT directly supported
        (3, _) | (2, _) | (1, _) => (
            CompatibilityLevel::Unsupported {
                reason: format!(
                    "Cassandra {}.x → Rust migration not supported. Upgrade to 4.0+ first.",
                    source.major
                ),
            },
            SSTableFormatCompat {
                source_format: "big-la/lb".into(),
                target_format: "rust-native".into(),
                conversion_needed: true,
                converter_available: false,
            },
            vec!["Upgrade Java cluster to 4.0+ before migrating to Rust".into()],
            vec![],
        ),

        // Unknown versions
        _ => (
            CompatibilityLevel::Experimental {
                reason: format!(
                    "Version {}.{} not in known matrix; manual validation required",
                    source.major, source.minor
                ),
            },
            SSTableFormatCompat {
                source_format: "unknown".into(),
                target_format: "rust-native".into(),
                conversion_needed: true,
                converter_available: false,
            },
            vec!["Unknown source version; proceed with caution".into()],
            vec!["Manual SSTable format identification required".into()],
        ),
    }
}

/// Return all known supported migration paths.
pub fn supported_paths() -> Vec<UpgradeCheckResult> {
    let rust_target = CassandraVersion::rust(0, 1, 0);
    let sources = [
        CassandraVersion::java(4, 0, 0),
        CassandraVersion::java(4, 1, 0),
        CassandraVersion::java(5, 0, 0),
    ];

    sources
        .iter()
        .map(|src| check_upgrade_path(src, &rust_target))
        .collect()
}

/// Return explicitly unsupported migration paths.
pub fn unsupported_paths() -> Vec<UpgradeCheckResult> {
    let rust_target = CassandraVersion::rust(0, 1, 0);
    let sources = [
        CassandraVersion::java(3, 0, 0),
        CassandraVersion::java(3, 11, 0),
        CassandraVersion::java(2, 2, 0),
    ];

    sources
        .iter()
        .map(|src| check_upgrade_path(src, &rust_target))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn java_4_0_to_rust_requires_conversion() {
        let src = CassandraVersion::java(4, 0, 13);
        let tgt = CassandraVersion::rust(0, 1, 0);
        let result = check_upgrade_path(&src, &tgt);
        assert!(result.compatibility.is_supported());
        assert!(result.sstable_format.conversion_needed);
        assert!(!result.mixed_cluster_supported);
        match &result.compatibility {
            CompatibilityLevel::RequiresConversion { steps } => {
                assert!(!steps.is_empty());
            }
            other => panic!("Expected RequiresConversion, got {:?}", other),
        }
    }

    #[test]
    fn java_5_0_to_rust_supported() {
        let src = CassandraVersion::java(5, 0, 0);
        let tgt = CassandraVersion::rust(0, 1, 0);
        let result = check_upgrade_path(&src, &tgt);
        assert!(result.compatibility.is_supported());
    }

    #[test]
    fn java_3_x_to_rust_unsupported() {
        let src = CassandraVersion::java(3, 11, 14);
        let tgt = CassandraVersion::rust(0, 1, 0);
        let result = check_upgrade_path(&src, &tgt);
        assert!(!result.compatibility.is_supported());
    }

    #[test]
    fn rust_to_rust_unsupported() {
        let src = CassandraVersion::rust(0, 1, 0);
        let tgt = CassandraVersion::rust(0, 2, 0);
        let result = check_upgrade_path(&src, &tgt);
        assert!(!result.compatibility.is_supported());
    }

    #[test]
    fn mixed_cluster_always_false() {
        for path in supported_paths() {
            assert!(
                !path.mixed_cluster_supported,
                "Mixed cluster should never be supported for {}",
                path.source,
            );
        }
    }

    #[test]
    fn supported_paths_not_empty() {
        let paths = supported_paths();
        assert!(paths.len() >= 3);
        for p in &paths {
            assert!(p.compatibility.is_supported());
        }
    }

    #[test]
    fn unsupported_paths_all_unsupported() {
        for p in unsupported_paths() {
            assert!(!p.compatibility.is_supported());
        }
    }

    #[test]
    fn version_display() {
        let v = CassandraVersion::java(4, 1, 3);
        assert_eq!(v.to_string(), "4.1.3");
        let v = CassandraVersion::rust(0, 1, 0);
        assert_eq!(v.to_string(), "0.1.0-rust");
    }
}
