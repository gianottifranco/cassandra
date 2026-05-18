// Licensed under Apache License, Version 2.0.

//! SSTable verification tool: checks integrity of all SSTable components.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.tools.SSTableVerifier`
//!
//! Verifies magic bytes, CRC checksums, index ordering, bloom filter
//! integrity, and statistics validity for each SSTable component.

use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::path::Path;

use byteorder::{BigEndian, ReadBytesExt};

use cassandra_storage::sstable::{
    bloom::BloomFilter,
    format::{
        Component, DATA_MAGIC, DATA_VERSION, FILTER_MAGIC, INDEX_MAGIC, SSTableDescriptor,
        SSTableFormat,
    },
    metadata::MetadataSerializer,
};

/// Severity levels for verification findings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Severity {
    Info,
    Warning,
    Error,
}

impl std::fmt::Display for Severity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Severity::Info => write!(f, "INFO"),
            Severity::Warning => write!(f, "WARN"),
            Severity::Error => write!(f, "ERROR"),
        }
    }
}

struct Finding {
    severity: Severity,
    component: String,
    message: String,
}

fn parse_descriptor(file: &str) -> Option<SSTableDescriptor> {
    let path = Path::new(file);
    let file_name = path.file_name()?.to_string_lossy();
    let parts: Vec<&str> = file_name.split('-').collect();
    if parts.len() < 5 {
        return None;
    }

    let ks = parts[0];
    let tbl = parts[1];
    let fmt_str = parts[2];
    let gen_str = parts[3];

    let generation = gen_str.parse::<u64>().ok()?;
    let dir = path.parent().unwrap_or_else(|| Path::new("."));

    let mut desc = SSTableDescriptor::new(dir, ks, tbl, generation);
    if fmt_str == "bti" {
        desc.format = SSTableFormat::Bti;
    }
    Some(desc)
}

/// Run SSTable verification on the given file path.
pub fn run(file: &str) {
    let path = Path::new(file);
    if !path.exists() {
        eprintln!("Error: file not found: {}", file);
        return;
    }

    let desc = match parse_descriptor(file) {
        Some(d) => d,
        None => {
            eprintln!("Error: invalid SSTable filename format: {}", file);
            return;
        }
    };

    println!("Verifying SSTable: {}", desc.file_prefix());
    println!("Directory: {}", desc.directory.display());
    println!();

    let mut findings: Vec<Finding> = Vec::new();
    let mut errors = 0u32;
    let mut warnings = 0u32;

    // 1. Verify Data.db
    verify_data(&desc, &mut findings);

    // 2. Verify Index.db
    verify_index(&desc, &mut findings);

    // 3. Verify Filter.db
    verify_filter(&desc, &mut findings);

    // 4. Verify Statistics.db
    verify_statistics(&desc, &mut findings);

    // 5. Check component completeness
    for comp in desc.expected_components() {
        let comp_path = desc.component_path(*comp);
        if !comp_path.exists() {
            findings.push(Finding {
                severity: Severity::Warning,
                component: format!("{:?}", comp),
                message: format!("missing component file: {}", comp_path.display()),
            });
        }
    }

    // Print results
    println!("Verification results:");
    println!("{:-<60}", "");
    for finding in &findings {
        let marker = match finding.severity {
            Severity::Info => "  ",
            Severity::Warning => "! ",
            Severity::Error => "X ",
        };
        println!(
            "[{}] {} {}: {}",
            finding.severity, marker, finding.component, finding.message
        );
        match finding.severity {
            Severity::Error => errors += 1,
            Severity::Warning => warnings += 1,
            Severity::Info => {}
        }
    }

    println!("{:-<60}", "");
    println!(
        "Summary: {} error(s), {} warning(s), {} finding(s) total",
        errors,
        warnings,
        findings.len()
    );

    if errors == 0 {
        println!("SSTable verification PASSED.");
    } else {
        println!("SSTable verification FAILED.");
    }
}

fn verify_data(desc: &SSTableDescriptor, findings: &mut Vec<Finding>) {
    let data_path = desc.component_path(Component::Data);
    if !data_path.exists() {
        findings.push(Finding {
            severity: Severity::Error,
            component: "Data.db".to_string(),
            message: "file not found".to_string(),
        });
        return;
    }

    let mut file = match File::open(&data_path) {
        Ok(f) => BufReader::new(f),
        Err(e) => {
            findings.push(Finding {
                severity: Severity::Error,
                component: "Data.db".to_string(),
                message: format!("failed to open: {}", e),
            });
            return;
        }
    };

    // Verify magic bytes
    let mut magic = [0u8; 4];
    if let Err(e) = file.read_exact(&mut magic) {
        findings.push(Finding {
            severity: Severity::Error,
            component: "Data.db".to_string(),
            message: format!("failed to read magic bytes: {}", e),
        });
        return;
    }

    if magic != DATA_MAGIC {
        findings.push(Finding {
            severity: Severity::Error,
            component: "Data.db".to_string(),
            message: format!(
                "invalid magic bytes: expected {:?}, got {:?}",
                DATA_MAGIC, magic
            ),
        });
    } else {
        findings.push(Finding {
            severity: Severity::Info,
            component: "Data.db".to_string(),
            message: "magic bytes OK".to_string(),
        });
    }

    // Verify version byte
    match file.read_u8() {
        Ok(version) => {
            if version != DATA_VERSION {
                findings.push(Finding {
                    severity: Severity::Error,
                    component: "Data.db".to_string(),
                    message: format!(
                        "unsupported version: expected {}, got {}",
                        DATA_VERSION, version
                    ),
                });
            } else {
                findings.push(Finding {
                    severity: Severity::Info,
                    component: "Data.db".to_string(),
                    message: format!("version {} OK", version),
                });
            }
        }
        Err(e) => {
            findings.push(Finding {
                severity: Severity::Error,
                component: "Data.db".to_string(),
                message: format!("failed to read version: {}", e),
            });
            return;
        }
    }

    // Verify CRC32
    let file_size = match std::fs::metadata(&data_path) {
        Ok(m) => m.len(),
        Err(_) => return,
    };

    if file_size < 9 {
        // 4 magic + 1 version + 4 CRC minimum
        findings.push(Finding {
            severity: Severity::Error,
            component: "Data.db".to_string(),
            message: format!("file too small: {} bytes", file_size),
        });
        return;
    }

    // Read all data bytes (excluding the trailing CRC32)
    let data_len = (file_size - 4) as usize;
    if let Err(e) = file.seek(SeekFrom::Start(0)) {
        findings.push(Finding {
            severity: Severity::Error,
            component: "Data.db".to_string(),
            message: format!("failed to seek: {}", e),
        });
        return;
    }

    let mut data_bytes = vec![0u8; data_len];
    if let Err(e) = file.read_exact(&mut data_bytes) {
        findings.push(Finding {
            severity: Severity::Error,
            component: "Data.db".to_string(),
            message: format!("failed to read data for CRC: {}", e),
        });
        return;
    }

    let computed_crc = crc32fast::hash(&data_bytes);

    match file.read_u32::<BigEndian>() {
        Ok(stored_crc) => {
            if computed_crc != stored_crc {
                findings.push(Finding {
                    severity: Severity::Error,
                    component: "Data.db".to_string(),
                    message: format!(
                        "CRC mismatch: computed {:#010x}, stored {:#010x}",
                        computed_crc, stored_crc
                    ),
                });
            } else {
                findings.push(Finding {
                    severity: Severity::Info,
                    component: "Data.db".to_string(),
                    message: format!("CRC32 OK ({:#010x})", stored_crc),
                });
            }
        }
        Err(e) => {
            findings.push(Finding {
                severity: Severity::Error,
                component: "Data.db".to_string(),
                message: format!("failed to read stored CRC: {}", e),
            });
        }
    }
}

fn verify_index(desc: &SSTableDescriptor, findings: &mut Vec<Finding>) {
    let index_path = desc.component_path(Component::Index);
    if !index_path.exists() {
        findings.push(Finding {
            severity: Severity::Error,
            component: "Index.db".to_string(),
            message: "file not found".to_string(),
        });
        return;
    }

    let mut file = match File::open(&index_path) {
        Ok(f) => BufReader::new(f),
        Err(e) => {
            findings.push(Finding {
                severity: Severity::Error,
                component: "Index.db".to_string(),
                message: format!("failed to open: {}", e),
            });
            return;
        }
    };

    // Verify magic bytes
    let mut magic = [0u8; 4];
    if let Err(e) = file.read_exact(&mut magic) {
        findings.push(Finding {
            severity: Severity::Error,
            component: "Index.db".to_string(),
            message: format!("failed to read magic: {}", e),
        });
        return;
    }

    if magic != INDEX_MAGIC {
        findings.push(Finding {
            severity: Severity::Error,
            component: "Index.db".to_string(),
            message: format!(
                "invalid magic bytes: expected {:?}, got {:?}",
                INDEX_MAGIC, magic
            ),
        });
        return;
    }

    findings.push(Finding {
        severity: Severity::Info,
        component: "Index.db".to_string(),
        message: "magic bytes OK".to_string(),
    });

    // Verify keys are sorted
    let mut prev_key: Option<Vec<u8>> = None;
    let mut entry_count: u64 = 0;
    let mut sorted = true;

    loop {
        let pk_len = match file.read_u32::<BigEndian>() {
            Ok(l) => l,
            Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => {
                findings.push(Finding {
                    severity: Severity::Error,
                    component: "Index.db".to_string(),
                    message: format!("failed to read entry at position {}: {}", entry_count, e),
                });
                return;
            }
        };

        let mut pk = vec![0u8; pk_len as usize];
        if let Err(e) = file.read_exact(&mut pk) {
            findings.push(Finding {
                severity: Severity::Error,
                component: "Index.db".to_string(),
                message: format!("failed to read key at entry {}: {}", entry_count, e),
            });
            return;
        }

        // Read offset (u64)
        if let Err(e) = file.read_u64::<BigEndian>() {
            findings.push(Finding {
                severity: Severity::Error,
                component: "Index.db".to_string(),
                message: format!("failed to read offset at entry {}: {}", entry_count, e),
            });
            return;
        }

        if let Some(ref prev) = prev_key {
            if pk <= *prev {
                sorted = false;
                findings.push(Finding {
                    severity: Severity::Error,
                    component: "Index.db".to_string(),
                    message: format!(
                        "keys not sorted at entry {}: {:?} <= {:?}",
                        entry_count,
                        String::from_utf8_lossy(&pk),
                        String::from_utf8_lossy(prev)
                    ),
                });
            }
        }

        prev_key = Some(pk);
        entry_count += 1;
    }

    if sorted {
        findings.push(Finding {
            severity: Severity::Info,
            component: "Index.db".to_string(),
            message: format!("{} entries, sort order OK", entry_count),
        });
    }
}

fn verify_filter(desc: &SSTableDescriptor, findings: &mut Vec<Finding>) {
    let filter_path = desc.component_path(Component::Filter);
    if !filter_path.exists() {
        findings.push(Finding {
            severity: Severity::Error,
            component: "Filter.db".to_string(),
            message: "file not found".to_string(),
        });
        return;
    }

    let mut file = match File::open(&filter_path) {
        Ok(f) => BufReader::new(f),
        Err(e) => {
            findings.push(Finding {
                severity: Severity::Error,
                component: "Filter.db".to_string(),
                message: format!("failed to open: {}", e),
            });
            return;
        }
    };

    // Verify magic bytes
    let mut magic = [0u8; 4];
    if let Err(e) = file.read_exact(&mut magic) {
        findings.push(Finding {
            severity: Severity::Error,
            component: "Filter.db".to_string(),
            message: format!("failed to read magic: {}", e),
        });
        return;
    }

    if magic != FILTER_MAGIC {
        findings.push(Finding {
            severity: Severity::Error,
            component: "Filter.db".to_string(),
            message: format!(
                "invalid magic bytes: expected {:?}, got {:?}",
                FILTER_MAGIC, magic
            ),
        });
        return;
    }

    findings.push(Finding {
        severity: Severity::Info,
        component: "Filter.db".to_string(),
        message: "magic bytes OK".to_string(),
    });

    // Verify bloom filter can be deserialized
    match BloomFilter::deserialize(&mut file) {
        Ok(_bf) => {
            findings.push(Finding {
                severity: Severity::Info,
                component: "Filter.db".to_string(),
                message: "bloom filter deserialized OK".to_string(),
            });
        }
        Err(e) => {
            findings.push(Finding {
                severity: Severity::Error,
                component: "Filter.db".to_string(),
                message: format!("failed to deserialize bloom filter: {}", e),
            });
        }
    }
}

fn verify_statistics(desc: &SSTableDescriptor, findings: &mut Vec<Finding>) {
    let stats_path = desc.component_path(Component::Statistics);
    if !stats_path.exists() {
        findings.push(Finding {
            severity: Severity::Warning,
            component: "Statistics.db".to_string(),
            message: "file not found (optional component)".to_string(),
        });
        return;
    }

    match std::fs::read(&stats_path) {
        Ok(contents) => match MetadataSerializer::deserialize_auto(&contents) {
            Ok(metadata) => {
                findings.push(Finding {
                    severity: Severity::Info,
                    component: "Statistics.db".to_string(),
                    message: "valid metadata".to_string(),
                });

                if metadata.min_timestamp > metadata.max_timestamp {
                    findings.push(Finding {
                        severity: Severity::Warning,
                        component: "Statistics.db".to_string(),
                        message: "min timestamp exceeds max timestamp".to_string(),
                    });
                }
                if metadata.row_count < metadata.partition_count {
                    findings.push(Finding {
                        severity: Severity::Warning,
                        component: "Statistics.db".to_string(),
                        message: "row count is lower than partition count".to_string(),
                    });
                }
            }
            Err(e) => {
                findings.push(Finding {
                    severity: Severity::Error,
                    component: "Statistics.db".to_string(),
                    message: format!("invalid metadata: {}", e),
                });
            }
        },
        Err(e) => {
            findings.push(Finding {
                severity: Severity::Error,
                component: "Statistics.db".to_string(),
                message: format!("failed to read: {}", e),
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cassandra_storage::sstable::metadata::SSTableMetadata;

    #[test]
    fn verify_statistics_accepts_binary_metadata() {
        let dir = tempfile::tempdir().unwrap();
        let desc = SSTableDescriptor::new(dir.path(), "ks", "tbl", 7);
        let metadata = SSTableMetadata {
            partition_count: 2,
            row_count: 4,
            cell_count: 8,
            min_timestamp: 10,
            max_timestamp: 20,
            data_size: 128,
            index_size: 32,
            min_partition_key: b"a".to_vec(),
            max_partition_key: b"z".to_vec(),
        };
        std::fs::write(
            desc.component_path(Component::Statistics),
            MetadataSerializer::serialize(&metadata),
        )
        .unwrap();

        let mut findings = Vec::new();
        verify_statistics(&desc, &mut findings);
        assert!(findings.iter().any(|finding| {
            finding.severity == Severity::Info
                && finding.component == "Statistics.db"
                && finding.message == "valid metadata"
        }));
        assert!(
            !findings
                .iter()
                .any(|finding| finding.severity == Severity::Error)
        );
    }

    #[test]
    fn verify_statistics_accepts_legacy_json_metadata() {
        let dir = tempfile::tempdir().unwrap();
        let desc = SSTableDescriptor::new(dir.path(), "ks", "tbl", 8);
        std::fs::write(
            desc.component_path(Component::Statistics),
            serde_json::json!({
                "partition_count": 2,
                "row_count": 4,
                "cell_count": 8,
                "min_timestamp": 10,
                "max_timestamp": 20,
                "data_size": 128,
                "index_size": 32
            })
            .to_string(),
        )
        .unwrap();

        let mut findings = Vec::new();
        verify_statistics(&desc, &mut findings);
        assert!(findings.iter().any(|finding| {
            finding.severity == Severity::Info && finding.message == "valid metadata"
        }));
        assert!(
            !findings
                .iter()
                .any(|finding| finding.severity == Severity::Error)
        );
    }
}
