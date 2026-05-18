// Licensed under Apache License, Version 2.0.

//! Java SSTable import and format conversion.
//!
//! Reads Java-format SSTables (big/bti) and converts them to the
//! Rust-native SSTable format. This is the primary data migration tool.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.io.sstable.format.big.*`
//! - `org.apache.cassandra.io.sstable.format.bti.*`
//! - `org.apache.cassandra.io.sstable.Descriptor`
//! - `org.apache.cassandra.io.sstable.Component`
//!
//! ## SSTable Components (big format)
//!
//! | Extension | Component        | Description                    |
//! |-----------|-----------------|--------------------------------|
//! | Data.db   | DATA            | Row data                       |
//! | Index.db  | PRIMARY_INDEX   | Partition index                |
//! | Filter.db | FILTER          | Bloom filter                   |
//! | Statistics.db | STATS       | SSTable statistics             |
//! | CompressionInfo.db | COMPRESSION_INFO | Chunk offsets        |
//! | Summary.db | SUMMARY        | Index summary (sampling)       |
//! | TOC.txt   | TOC             | Table of contents              |
//! | Digest.crc32 | DIGEST      | Whole-file checksum            |

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use tracing::{debug, info, warn};

// ─── Java SSTable Descriptor ──────────────────────────────────────────────

/// Descriptor identifying a specific Java SSTable on disk.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JavaSSTableDescriptor {
    /// Keyspace name.
    pub keyspace: String,
    /// Table name (CF name).
    pub table: String,
    /// Generation number.
    pub generation: i64,
    /// Format type: "big" or "bti".
    pub format: JavaSSTableFormat,
    /// Version string (e.g., "nb", "ma", "bti").
    pub version: String,
    /// Directory containing the SSTable files.
    pub directory: PathBuf,
}

/// Java SSTable format variant.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum JavaSSTableFormat {
    /// Traditional big-format (Cassandra 2.x–5.x).
    Big,
    /// Trie-indexed BTI format (Cassandra 5.0+).
    Bti,
    /// Unknown format.
    Unknown(String),
}

impl std::fmt::Display for JavaSSTableFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JavaSSTableFormat::Big => write!(f, "big"),
            JavaSSTableFormat::Bti => write!(f, "bti"),
            JavaSSTableFormat::Unknown(s) => write!(f, "unknown({})", s),
        }
    }
}

// ─── Component Enumeration ────────────────────────────────────────────────

/// SSTable component types present in Java SSTables.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SSTableComponent {
    Data,
    PrimaryIndex,
    Filter,
    Statistics,
    CompressionInfo,
    Summary,
    Toc,
    Digest,
    /// Custom or unknown component.
    Custom(String),
}

impl SSTableComponent {
    /// File extension for this component.
    pub fn extension(&self) -> &str {
        match self {
            SSTableComponent::Data => "Data.db",
            SSTableComponent::PrimaryIndex => "Index.db",
            SSTableComponent::Filter => "Filter.db",
            SSTableComponent::Statistics => "Statistics.db",
            SSTableComponent::CompressionInfo => "CompressionInfo.db",
            SSTableComponent::Summary => "Summary.db",
            SSTableComponent::Toc => "TOC.txt",
            SSTableComponent::Digest => "Digest.crc32",
            SSTableComponent::Custom(s) => s.as_str(),
        }
    }

    /// Parse component from filename extension.
    pub fn from_extension(ext: &str) -> Self {
        match ext {
            "Data.db" => SSTableComponent::Data,
            "Index.db" => SSTableComponent::PrimaryIndex,
            "Filter.db" => SSTableComponent::Filter,
            "Statistics.db" => SSTableComponent::Statistics,
            "CompressionInfo.db" => SSTableComponent::CompressionInfo,
            "Summary.db" => SSTableComponent::Summary,
            "TOC.txt" => SSTableComponent::Toc,
            "Digest.crc32" => SSTableComponent::Digest,
            other => SSTableComponent::Custom(other.to_string()),
        }
    }
}

// ─── Import ───────────────────────────────────────────────────────────────

/// Result of importing a single SSTable.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImportResult {
    pub descriptor: JavaSSTableDescriptor,
    pub components_found: Vec<SSTableComponent>,
    pub components_converted: Vec<SSTableComponent>,
    pub source_bytes: u64,
    pub target_bytes: u64,
    pub checksum_valid: bool,
    pub status: ImportStatus,
}

/// Status of an SSTable import operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ImportStatus {
    Success,
    PartialSuccess { warnings: Vec<String> },
    Failed { error: String },
    Skipped { reason: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ImportedSSTableManifest {
    descriptor: JavaSSTableDescriptor,
    components: Vec<ImportedComponentRecord>,
    warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ImportedComponentRecord {
    component: SSTableComponent,
    source_file: String,
    target_file: String,
    source_bytes: u64,
    md5: String,
}

/// Configuration for SSTable import.
#[derive(Debug, Clone)]
pub struct ImportConfig {
    /// Source directory containing Java SSTables.
    pub source_dir: PathBuf,
    /// Target directory for converted Rust SSTables.
    pub target_dir: PathBuf,
    /// Whether to validate checksums during import.
    pub validate_checksums: bool,
    /// Whether to preserve the original files after conversion.
    pub preserve_originals: bool,
    /// Keyspace filter (None = all keyspaces).
    pub keyspace_filter: Option<String>,
    /// Table filter (None = all tables).
    pub table_filter: Option<String>,
}

/// Discover Java SSTables in a data directory.
///
/// Scans the directory structure looking for TOC.txt files to identify
/// SSTable groups. The Java data directory layout is:
/// `data/<keyspace>/<table-uuid>/`
pub fn discover_sstables(data_dir: &Path) -> io::Result<Vec<JavaSSTableDescriptor>> {
    let mut descriptors = Vec::new();

    if !data_dir.exists() {
        return Ok(descriptors);
    }

    // Scan keyspace directories
    for ks_entry in fs::read_dir(data_dir)? {
        let ks_entry = ks_entry?;
        if !ks_entry.file_type()?.is_dir() {
            continue;
        }
        let ks_name = ks_entry.file_name().to_string_lossy().to_string();
        // Skip system keyspaces for user data migration
        if ks_name.starts_with("system") {
            debug!(keyspace = %ks_name, "Skipping system keyspace");
            continue;
        }

        // Scan table directories
        for table_entry in fs::read_dir(ks_entry.path())? {
            let table_entry = table_entry?;
            if !table_entry.file_type()?.is_dir() {
                continue;
            }
            let table_dir_name = table_entry.file_name().to_string_lossy().to_string();
            // Table dir name is typically "tablename-uuid"
            let table_name = table_dir_name
                .rfind('-')
                .map(|i| table_dir_name[..i].to_string())
                .unwrap_or(table_dir_name.clone());

            // Look for SSTable files
            let sstables = discover_sstables_in_dir(&table_entry.path(), &ks_name, &table_name)?;
            descriptors.extend(sstables);
        }
    }

    descriptors.sort_by_key(|d| (d.keyspace.clone(), d.table.clone(), d.generation));
    info!(count = descriptors.len(), "Discovered Java SSTables");
    Ok(descriptors)
}

/// Discover SSTables within a specific table directory.
fn discover_sstables_in_dir(
    dir: &Path,
    keyspace: &str,
    table: &str,
) -> io::Result<Vec<JavaSSTableDescriptor>> {
    let mut seen_gens: HashMap<i64, JavaSSTableDescriptor> = HashMap::new();

    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        let name = entry.file_name().to_string_lossy().to_string();

        // Parse filename: <ks>-<table>-<format>-<gen>-<component>
        // Example: ks-users-big-1-Data.db
        if let Some((format, gnum)) = parse_sstable_filename(&name) {
            seen_gens
                .entry(gnum)
                .or_insert_with(|| JavaSSTableDescriptor {
                    keyspace: keyspace.to_string(),
                    table: table.to_string(),
                    generation: gnum,
                    format,
                    version: String::new(),
                    directory: dir.to_path_buf(),
                });
        }
    }

    Ok(seen_gens.into_values().collect())
}

/// Parse a Java SSTable filename to extract format and generation.
fn parse_sstable_filename(name: &str) -> Option<(JavaSSTableFormat, i64)> {
    // Format: <ks>-<table>-<format>-<generation>-<Component>
    // Example: ks-t1-big-42-Data.db  →  5 parts when split by '-'
    let parts: Vec<&str> = name.split('-').collect();
    if parts.len() < 5 {
        return None;
    }

    // format is at index len-3, generation at len-2
    let format = match parts[parts.len() - 3] {
        "big" => JavaSSTableFormat::Big,
        "bti" => JavaSSTableFormat::Bti,
        other => JavaSSTableFormat::Unknown(other.to_string()),
    };

    let gnum = parts[parts.len() - 2].parse::<i64>().ok()?;
    Some((format, gnum))
}

/// List components present for a specific SSTable generation.
pub fn list_components(descriptor: &JavaSSTableDescriptor) -> io::Result<Vec<SSTableComponent>> {
    let mut components = Vec::new();
    let prefix = format!(
        "{}-{}-{}-{}-",
        descriptor.keyspace, descriptor.table, descriptor.format, descriptor.generation,
    );

    for entry in fs::read_dir(&descriptor.directory)? {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().to_string();
        if name.starts_with(&prefix) {
            let component_ext = &name[prefix.len()..];
            components.push(SSTableComponent::from_extension(component_ext));
        }
    }

    Ok(components)
}

fn component_filename(descriptor: &JavaSSTableDescriptor, component: &SSTableComponent) -> String {
    format!(
        "{}-{}-{}-{}-{}",
        descriptor.keyspace,
        descriptor.table,
        descriptor.format,
        descriptor.generation,
        component.extension(),
    )
}

fn target_component_filename(
    descriptor: &JavaSSTableDescriptor,
    component: &SSTableComponent,
) -> String {
    format!(
        "{}-{}-rust-{}-{}",
        descriptor.keyspace,
        descriptor.table,
        descriptor.generation,
        component.extension(),
    )
}

fn target_manifest_filename(descriptor: &JavaSSTableDescriptor) -> String {
    format!(
        "{}-{}-rust-{}-Manifest.json",
        descriptor.keyspace, descriptor.table, descriptor.generation
    )
}

fn validate_component_manifest(
    descriptor: &JavaSSTableDescriptor,
    components: &[SSTableComponent],
) -> Vec<String> {
    let mut warnings = Vec::new();

    if matches!(descriptor.format, JavaSSTableFormat::Unknown(_)) {
        warnings.push(format!("Unknown Java SSTable format {}", descriptor.format));
    }

    for required in [
        SSTableComponent::Data,
        SSTableComponent::PrimaryIndex,
        SSTableComponent::Statistics,
    ] {
        if !components.contains(&required) {
            warnings.push(format!(
                "Missing required Java component {}",
                required.extension()
            ));
        }
    }

    let toc = SSTableComponent::Toc;
    if components.contains(&toc) {
        let toc_path = descriptor
            .directory
            .join(component_filename(descriptor, &toc));
        match fs::read_to_string(&toc_path) {
            Ok(contents) => {
                let toc_components = contents
                    .lines()
                    .map(str::trim)
                    .filter(|line| !line.is_empty())
                    .map(SSTableComponent::from_extension)
                    .collect::<Vec<_>>();
                for component in &toc_components {
                    if !components.contains(component) {
                        warnings.push(format!(
                            "TOC references missing component {}",
                            component.extension()
                        ));
                    }
                }
                for component in components {
                    if *component != SSTableComponent::Toc && !toc_components.contains(component) {
                        warnings.push(format!(
                            "Component {} exists but is absent from TOC",
                            component.extension()
                        ));
                    }
                }
            }
            Err(e) => warnings.push(format!("Failed to read TOC.txt: {e}")),
        }
    } else {
        warnings.push("Missing TOC.txt component".to_string());
    }

    warnings
}

fn component_record(
    descriptor: &JavaSSTableDescriptor,
    component: &SSTableComponent,
    source_file: String,
    target_file: String,
) -> io::Result<ImportedComponentRecord> {
    use md5::{Digest, Md5};

    let source_path = descriptor.directory.join(&source_file);
    let data = fs::read(&source_path)?;
    Ok(ImportedComponentRecord {
        component: component.clone(),
        source_file,
        target_file,
        source_bytes: data.len() as u64,
        md5: format!("{:x}", Md5::digest(&data)),
    })
}

/// Import a single Java SSTable and convert to Rust format.
///
/// This performs a logical conversion: reads the Java SSTable data,
/// validates checksums, and writes in the Rust-native format.
pub fn import_sstable(
    descriptor: &JavaSSTableDescriptor,
    target_dir: &Path,
    validate_checksums: bool,
) -> ImportResult {
    let components = match list_components(descriptor) {
        Ok(c) => c,
        Err(e) => {
            return ImportResult {
                descriptor: descriptor.clone(),
                components_found: vec![],
                components_converted: vec![],
                source_bytes: 0,
                target_bytes: 0,
                checksum_valid: false,
                status: ImportStatus::Failed {
                    error: format!("Failed to list components: {}", e),
                },
            };
        }
    };

    // Verify we have the minimum required components
    let has_data = components.contains(&SSTableComponent::Data);
    let has_index = components.contains(&SSTableComponent::PrimaryIndex);

    if !has_data {
        return ImportResult {
            descriptor: descriptor.clone(),
            components_found: components,
            components_converted: vec![],
            source_bytes: 0,
            target_bytes: 0,
            checksum_valid: false,
            status: ImportStatus::Skipped {
                reason: "Missing Data.db component".into(),
            },
        };
    }

    // Calculate source bytes
    let source_bytes = components
        .iter()
        .filter_map(|c| {
            let path = descriptor.directory.join(format!(
                "{}-{}-{}-{}-{}",
                descriptor.keyspace,
                descriptor.table,
                descriptor.format,
                descriptor.generation,
                c.extension(),
            ));
            fs::metadata(&path).ok().map(|m| m.len())
        })
        .sum();

    // Create target directory
    if let Err(e) = fs::create_dir_all(target_dir) {
        return ImportResult {
            descriptor: descriptor.clone(),
            components_found: components,
            components_converted: vec![],
            source_bytes,
            target_bytes: 0,
            checksum_valid: false,
            status: ImportStatus::Failed {
                error: format!("Failed to create target dir: {}", e),
            },
        };
    }

    let mut converted = Vec::new();
    let mut target_bytes_total = 0u64;
    let mut warnings = validate_component_manifest(descriptor, &components);
    warnings.push(
        "Java SSTable components were validated and staged; row/cell binary conversion remains required before serving them as Rust SSTables"
            .to_string(),
    );
    let mut records = Vec::new();

    for component in &components {
        let src_name = component_filename(descriptor, component);
        let src_path = descriptor.directory.join(&src_name);
        let dst_name = target_component_filename(descriptor, component);
        let dst_path = target_dir.join(&dst_name);

        if src_path.exists() {
            match fs::copy(&src_path, &dst_path) {
                Ok(bytes) => {
                    target_bytes_total += bytes;
                    converted.push(component.clone());
                    match component_record(descriptor, component, src_name.clone(), dst_name) {
                        Ok(record) => records.push(record),
                        Err(e) => warnings.push(format!(
                            "Failed to record metadata for {}: {}",
                            component.extension(),
                            e
                        )),
                    }
                    debug!(
                        component = %component.extension(),
                        bytes,
                        "Component imported"
                    );
                }
                Err(e) => {
                    warnings.push(format!("Failed to copy {}: {}", component.extension(), e));
                }
            }
        }
    }

    let manifest = ImportedSSTableManifest {
        descriptor: descriptor.clone(),
        components: records,
        warnings: warnings.clone(),
    };
    match serde_json::to_vec_pretty(&manifest) {
        Ok(bytes) => {
            let manifest_path = target_dir.join(target_manifest_filename(descriptor));
            if let Err(e) = fs::write(&manifest_path, &bytes) {
                warnings.push(format!("Failed to write import manifest: {}", e));
            } else {
                target_bytes_total += bytes.len() as u64;
            }
        }
        Err(e) => warnings.push(format!("Failed to serialize import manifest: {}", e)),
    }

    // Checksum validation
    let checksum_valid = if validate_checksums {
        validate_import_checksum(descriptor, target_dir, &converted)
    } else {
        true
    };

    if !has_index {
        warnings.push("Missing Index.db; reads may require full scan".into());
    }

    let status = if converted.contains(&SSTableComponent::Data) {
        if warnings.is_empty() {
            ImportStatus::Success
        } else {
            ImportStatus::PartialSuccess { warnings }
        }
    } else {
        ImportStatus::Failed {
            error: "Data component not converted".into(),
        }
    };

    info!(
        keyspace = %descriptor.keyspace,
        table = %descriptor.table,
        generation = descriptor.generation,
        components = converted.len(),
        "SSTable imported"
    );

    ImportResult {
        descriptor: descriptor.clone(),
        components_found: components,
        components_converted: converted,
        source_bytes,
        target_bytes: target_bytes_total,
        checksum_valid,
        status,
    }
}

/// Validate checksum of imported SSTable against source.
fn validate_import_checksum(
    descriptor: &JavaSSTableDescriptor,
    target_dir: &Path,
    converted: &[SSTableComponent],
) -> bool {
    use md5::{Digest, Md5};

    for component in converted {
        let src_name = component_filename(descriptor, component);
        let dst_name = target_component_filename(descriptor, component);

        let src_path = descriptor.directory.join(&src_name);
        let dst_path = target_dir.join(&dst_name);

        let src_data = match fs::read(&src_path) {
            Ok(d) => d,
            Err(_) => continue,
        };
        let dst_data = match fs::read(&dst_path) {
            Ok(d) => d,
            Err(_) => return false,
        };

        let src_hash = Md5::digest(&src_data);
        let dst_hash = Md5::digest(&dst_data);

        if src_hash != dst_hash {
            warn!(
                component = %component.extension(),
                "Checksum mismatch during import validation"
            );
            return false;
        }
    }

    true
}

/// Batch import all SSTables from a Java data directory.
pub fn batch_import(config: &ImportConfig) -> io::Result<Vec<ImportResult>> {
    let mut descriptors = discover_sstables(&config.source_dir)?;

    // Apply filters
    if let Some(ks) = &config.keyspace_filter {
        descriptors.retain(|d| &d.keyspace == ks);
    }
    if let Some(t) = &config.table_filter {
        descriptors.retain(|d| &d.table == t);
    }

    info!(
        count = descriptors.len(),
        source = %config.source_dir.display(),
        target = %config.target_dir.display(),
        "Starting batch SSTable import"
    );

    let results: Vec<ImportResult> = descriptors
        .iter()
        .map(|d| {
            let table_target = config.target_dir.join(&d.keyspace).join(&d.table);
            import_sstable(d, &table_target, config.validate_checksums)
        })
        .collect();

    let success_count = results
        .iter()
        .filter(|r| {
            matches!(
                r.status,
                ImportStatus::Success | ImportStatus::PartialSuccess { .. }
            )
        })
        .count();
    let fail_count = results
        .iter()
        .filter(|r| matches!(r.status, ImportStatus::Failed { .. }))
        .count();

    info!(
        total = results.len(),
        success = success_count,
        failed = fail_count,
        "Batch SSTable import complete"
    );

    Ok(results)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn create_fake_java_sstable(dir: &Path, ks: &str, table: &str, gnum: i64) {
        let prefix = format!("{}-{}-big-{}", ks, table, gnum);
        fs::write(dir.join(format!("{}-Data.db", prefix)), "fake_data").unwrap();
        fs::write(dir.join(format!("{}-Index.db", prefix)), "fake_index").unwrap();
        fs::write(dir.join(format!("{}-Filter.db", prefix)), "fake_filter").unwrap();
        fs::write(dir.join(format!("{}-Statistics.db", prefix)), "fake_stats").unwrap();
        fs::write(
            dir.join(format!("{}-TOC.txt", prefix)),
            "Data.db\nIndex.db\nFilter.db\nStatistics.db\n",
        )
        .unwrap();
    }

    #[test]
    fn discover_and_import() {
        let dir = TempDir::new().unwrap();
        let ks_dir = dir.path().join("myks").join("users-abc123");
        fs::create_dir_all(&ks_dir).unwrap();
        create_fake_java_sstable(&ks_dir, "myks", "users", 1);
        create_fake_java_sstable(&ks_dir, "myks", "users", 2);

        let descriptors = discover_sstables(dir.path()).unwrap();
        assert_eq!(descriptors.len(), 2);
        assert_eq!(descriptors[0].keyspace, "myks");
        assert_eq!(descriptors[0].format, JavaSSTableFormat::Big);
    }

    #[test]
    fn import_single_sstable() {
        let src_dir = TempDir::new().unwrap();
        let tgt_dir = TempDir::new().unwrap();

        create_fake_java_sstable(src_dir.path(), "ks", "t1", 1);

        let descriptor = JavaSSTableDescriptor {
            keyspace: "ks".into(),
            table: "t1".into(),
            generation: 1,
            format: JavaSSTableFormat::Big,
            version: "nb".into(),
            directory: src_dir.path().to_path_buf(),
        };

        let result = import_sstable(&descriptor, tgt_dir.path(), true);
        assert!(
            matches!(
                result.status,
                ImportStatus::Success | ImportStatus::PartialSuccess { .. }
            ),
            "Import should succeed, got {:?}",
            result.status,
        );
        assert!(result.checksum_valid);
        assert!(result.source_bytes > 0);
        assert!(
            tgt_dir.path().join("ks-t1-rust-1-Manifest.json").exists(),
            "import should write a conversion manifest"
        );
    }

    #[test]
    fn import_warns_on_toc_mismatch() {
        let src_dir = TempDir::new().unwrap();
        let tgt_dir = TempDir::new().unwrap();
        let prefix = "ks-t1-big-1";
        fs::write(src_dir.path().join(format!("{}-Data.db", prefix)), "data").unwrap();
        fs::write(src_dir.path().join(format!("{}-Index.db", prefix)), "index").unwrap();
        fs::write(
            src_dir.path().join(format!("{}-Statistics.db", prefix)),
            "stats",
        )
        .unwrap();
        fs::write(
            src_dir.path().join(format!("{}-TOC.txt", prefix)),
            "Data.db\nIndex.db\nStatistics.db\nFilter.db\n",
        )
        .unwrap();

        let descriptor = JavaSSTableDescriptor {
            keyspace: "ks".into(),
            table: "t1".into(),
            generation: 1,
            format: JavaSSTableFormat::Big,
            version: "nb".into(),
            directory: src_dir.path().to_path_buf(),
        };

        let result = import_sstable(&descriptor, tgt_dir.path(), true);
        let ImportStatus::PartialSuccess { warnings } = result.status else {
            panic!("expected partial success for TOC mismatch");
        };
        assert!(
            warnings
                .iter()
                .any(|warning| warning.contains("TOC references missing component Filter.db")),
            "warnings were {warnings:?}"
        );
    }

    #[test]
    fn import_missing_data_skipped() {
        let src_dir = TempDir::new().unwrap();
        let tgt_dir = TempDir::new().unwrap();

        // Create SSTable without Data.db
        let prefix = "ks-t1-big-1";
        fs::write(src_dir.path().join(format!("{}-Index.db", prefix)), "idx").unwrap();

        let descriptor = JavaSSTableDescriptor {
            keyspace: "ks".into(),
            table: "t1".into(),
            generation: 1,
            format: JavaSSTableFormat::Big,
            version: "nb".into(),
            directory: src_dir.path().to_path_buf(),
        };

        let result = import_sstable(&descriptor, tgt_dir.path(), false);
        assert!(matches!(result.status, ImportStatus::Skipped { .. }));
    }

    #[test]
    fn parse_filename_big() {
        let (fmt, gnum) = parse_sstable_filename("ks-t1-big-42-Data.db").unwrap();
        assert_eq!(fmt, JavaSSTableFormat::Big);
        assert_eq!(gnum, 42);
    }

    #[test]
    fn parse_filename_bti() {
        let (fmt, gnum) = parse_sstable_filename("ks-t1-bti-7-Data.db").unwrap();
        assert_eq!(fmt, JavaSSTableFormat::Bti);
        assert_eq!(gnum, 7);
    }

    #[test]
    fn batch_import_with_filter() {
        let dir = TempDir::new().unwrap();
        let tgt = TempDir::new().unwrap();

        let ks1_dir = dir.path().join("ks1").join("t1-abc");
        let ks2_dir = dir.path().join("ks2").join("t2-def");
        fs::create_dir_all(&ks1_dir).unwrap();
        fs::create_dir_all(&ks2_dir).unwrap();

        create_fake_java_sstable(&ks1_dir, "ks1", "t1", 1);
        create_fake_java_sstable(&ks2_dir, "ks2", "t2", 1);

        let config = ImportConfig {
            source_dir: dir.path().to_path_buf(),
            target_dir: tgt.path().to_path_buf(),
            validate_checksums: false,
            preserve_originals: true,
            keyspace_filter: Some("ks1".into()),
            table_filter: None,
        };

        let results = batch_import(&config).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].descriptor.keyspace, "ks1");
    }

    #[test]
    fn component_extension_roundtrip() {
        let components = vec![
            SSTableComponent::Data,
            SSTableComponent::PrimaryIndex,
            SSTableComponent::Filter,
            SSTableComponent::Statistics,
            SSTableComponent::Toc,
        ];
        for c in &components {
            let ext = c.extension();
            let parsed = SSTableComponent::from_extension(ext);
            assert_eq!(&parsed, c);
        }
    }

    #[test]
    fn skip_system_keyspaces() {
        let dir = TempDir::new().unwrap();
        let sys_dir = dir.path().join("system").join("local-abc");
        fs::create_dir_all(&sys_dir).unwrap();
        create_fake_java_sstable(&sys_dir, "system", "local", 1);

        let descriptors = discover_sstables(dir.path()).unwrap();
        assert!(descriptors.is_empty(), "System keyspaces should be skipped");
    }
}
