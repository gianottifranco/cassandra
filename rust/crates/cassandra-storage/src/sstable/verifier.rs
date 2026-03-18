// Licensed under Apache License, Version 2.0.

//! SSTable verifier: validates on-disk SSTable component integrity.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.io.sstable.format.SSTableReader`
//! - `org.apache.cassandra.db.compaction.Verifier`

use std::fs::{self, File};
use std::io::{self, BufReader, Read};

use byteorder::{BigEndian, ReadBytesExt};
use crc32fast::Hasher;

use super::bloom::BloomFilter;
use super::format::*;

/// Severity of a verification issue.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Severity {
    Error,
    Warning,
    Info,
}

/// A single issue found during verification.
#[derive(Debug, Clone)]
pub struct VerificationIssue {
    pub severity: Severity,
    pub component: Component,
    pub message: String,
}

/// Result of verifying an SSTable.
#[derive(Debug)]
pub struct VerificationResult {
    pub issues: Vec<VerificationIssue>,
}

impl VerificationResult {
    /// Returns true if no Error-severity issues were found.
    pub fn is_valid(&self) -> bool {
        !self.issues.iter().any(|i| i.severity == Severity::Error)
    }
}

/// Verifies SSTable component integrity.
pub struct SSTableVerifier;

impl SSTableVerifier {
    /// Verify all components of an SSTable.
    pub fn verify(descriptor: &SSTableDescriptor) -> VerificationResult {
        let mut issues = Vec::new();

        Self::verify_data(descriptor, &mut issues);
        Self::verify_index(descriptor, &mut issues);
        Self::verify_filter(descriptor, &mut issues);
        Self::verify_statistics(descriptor, &mut issues);
        Self::verify_toc(descriptor, &mut issues);

        VerificationResult { issues }
    }

    fn verify_data(desc: &SSTableDescriptor, issues: &mut Vec<VerificationIssue>) {
        let path = desc.component_path(Component::Data);
        let data = match fs::read(&path) {
            Ok(d) => d,
            Err(_) => {
                issues.push(VerificationIssue {
                    severity: Severity::Error,
                    component: Component::Data,
                    message: "Data.db file not found or unreadable".into(),
                });
                return;
            }
        };

        if data.len() < 5 {
            issues.push(VerificationIssue {
                severity: Severity::Error,
                component: Component::Data,
                message: "Data.db too small for header".into(),
            });
            return;
        }

        // Check magic bytes
        if data[..4] != DATA_MAGIC {
            issues.push(VerificationIssue {
                severity: Severity::Error,
                component: Component::Data,
                message: "Data.db magic bytes mismatch".into(),
            });
        }

        // Check version
        if data[4] != DATA_VERSION {
            issues.push(VerificationIssue {
                severity: Severity::Error,
                component: Component::Data,
                message: format!(
                    "Data.db version {} does not match expected {}",
                    data[4], DATA_VERSION
                ),
            });
        }

        // Verify CRC: last 4 bytes are the stored CRC, rest is payload
        if data.len() < 9 {
            // header(5) + crc(4) minimum
            issues.push(VerificationIssue {
                severity: Severity::Error,
                component: Component::Data,
                message: "Data.db too small for CRC".into(),
            });
            return;
        }

        let payload = &data[..data.len() - 4];
        let stored_crc_bytes = &data[data.len() - 4..];
        let stored_crc = u32::from_be_bytes([
            stored_crc_bytes[0],
            stored_crc_bytes[1],
            stored_crc_bytes[2],
            stored_crc_bytes[3],
        ]);

        let mut hasher = Hasher::new();
        hasher.update(payload);
        let computed_crc = hasher.finalize();

        if stored_crc != computed_crc {
            issues.push(VerificationIssue {
                severity: Severity::Error,
                component: Component::Data,
                message: format!(
                    "Data.db CRC mismatch: stored={stored_crc:#010x}, \
                     computed={computed_crc:#010x}"
                ),
            });
        }

        // Validate partition structure in the payload (after header, before CRC)
        Self::verify_data_structure(&data[5..data.len() - 4], issues);
    }

    fn verify_data_structure(data: &[u8], issues: &mut Vec<VerificationIssue>) {
        let mut cursor = io::Cursor::new(data);
        let len = data.len() as u64;

        while cursor.position() < len {
            // Read partition key length
            let pk_len = match cursor.read_u32::<BigEndian>() {
                Ok(l) => l,
                Err(_) => break,
            };

            if pk_len == 0 {
                break;
            }

            // Skip partition key bytes
            let new_pos = cursor.position() + pk_len as u64;
            if new_pos > len {
                issues.push(VerificationIssue {
                    severity: Severity::Error,
                    component: Component::Data,
                    message: "Data.db partition key extends beyond file".into(),
                });
                return;
            }
            cursor.set_position(new_pos);

            // Read markers until END_OF_PARTITION
            loop {
                let marker = match cursor.read_u8() {
                    Ok(m) => m,
                    Err(_) => {
                        issues.push(VerificationIssue {
                            severity: Severity::Error,
                            component: Component::Data,
                            message: "Data.db unexpected EOF reading marker".into(),
                        });
                        return;
                    }
                };

                if marker == END_OF_PARTITION {
                    break;
                }

                if marker != ROW_MARKER {
                    issues.push(VerificationIssue {
                        severity: Severity::Error,
                        component: Component::Data,
                        message: format!("Data.db invalid marker byte: {marker:#04x}"),
                    });
                    return;
                }

                // Skip row data: we just need to validate markers are correct.
                // Use read_row_from_reader to parse (and thus validate) the row.
                if let Err(e) = super::reader::read_row_from_reader(&mut cursor) {
                    issues.push(VerificationIssue {
                        severity: Severity::Error,
                        component: Component::Data,
                        message: format!("Data.db row parse error: {e}"),
                    });
                    return;
                }
            }
        }
    }

    fn verify_index(desc: &SSTableDescriptor, issues: &mut Vec<VerificationIssue>) {
        let path = desc.component_path(Component::Index);
        let mut reader = match File::open(&path) {
            Ok(f) => BufReader::new(f),
            Err(_) => {
                issues.push(VerificationIssue {
                    severity: Severity::Error,
                    component: Component::Index,
                    message: "Index.db file not found or unreadable".into(),
                });
                return;
            }
        };

        // Check magic
        let mut magic = [0u8; 4];
        if reader.read_exact(&mut magic).is_err() {
            issues.push(VerificationIssue {
                severity: Severity::Error,
                component: Component::Index,
                message: "Index.db too small for magic".into(),
            });
            return;
        }

        if magic != INDEX_MAGIC {
            issues.push(VerificationIssue {
                severity: Severity::Error,
                component: Component::Index,
                message: "Index.db magic bytes mismatch".into(),
            });
        }

        // Read entries, check sorted keys and offsets within data range
        let data_path = desc.component_path(Component::Data);
        let data_size = fs::metadata(&data_path).map(|m| m.len()).unwrap_or(0);

        let mut prev_key: Option<Vec<u8>> = None;
        loop {
            let pk_len = match reader.read_u32::<BigEndian>() {
                Ok(l) => l,
                Err(ref e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(_) => {
                    issues.push(VerificationIssue {
                        severity: Severity::Error,
                        component: Component::Index,
                        message: "Index.db read error".into(),
                    });
                    return;
                }
            };

            let mut pk = vec![0u8; pk_len as usize];
            if reader.read_exact(&mut pk).is_err() {
                issues.push(VerificationIssue {
                    severity: Severity::Error,
                    component: Component::Index,
                    message: "Index.db truncated entry".into(),
                });
                return;
            }

            let offset = match reader.read_u64::<BigEndian>() {
                Ok(o) => o,
                Err(_) => {
                    issues.push(VerificationIssue {
                        severity: Severity::Error,
                        component: Component::Index,
                        message: "Index.db truncated offset".into(),
                    });
                    return;
                }
            };

            // Check offset within data file range
            if data_size > 0 && offset >= data_size {
                issues.push(VerificationIssue {
                    severity: Severity::Error,
                    component: Component::Index,
                    message: format!("Index.db offset {offset} exceeds Data.db size {data_size}"),
                });
            }

            // Check sorted order
            if let Some(ref prev) = prev_key {
                if pk <= *prev {
                    issues.push(VerificationIssue {
                        severity: Severity::Error,
                        component: Component::Index,
                        message: "Index.db keys not sorted".into(),
                    });
                }
            }
            prev_key = Some(pk);
        }
    }

    fn verify_filter(desc: &SSTableDescriptor, issues: &mut Vec<VerificationIssue>) {
        let path = desc.component_path(Component::Filter);
        let mut reader = match File::open(&path) {
            Ok(f) => BufReader::new(f),
            Err(_) => {
                issues.push(VerificationIssue {
                    severity: Severity::Error,
                    component: Component::Filter,
                    message: "Filter.db file not found or unreadable".into(),
                });
                return;
            }
        };

        let mut magic = [0u8; 4];
        if reader.read_exact(&mut magic).is_err() {
            issues.push(VerificationIssue {
                severity: Severity::Error,
                component: Component::Filter,
                message: "Filter.db too small for magic".into(),
            });
            return;
        }

        if magic != FILTER_MAGIC {
            issues.push(VerificationIssue {
                severity: Severity::Error,
                component: Component::Filter,
                message: "Filter.db magic bytes mismatch".into(),
            });
        }

        if BloomFilter::deserialize(&mut reader).is_err() {
            issues.push(VerificationIssue {
                severity: Severity::Error,
                component: Component::Filter,
                message: "Filter.db bloom filter not deserializable".into(),
            });
        }
    }

    fn verify_statistics(desc: &SSTableDescriptor, issues: &mut Vec<VerificationIssue>) {
        let path = desc.component_path(Component::Statistics);
        let data = match fs::read_to_string(&path) {
            Ok(d) => d,
            Err(_) => {
                issues.push(VerificationIssue {
                    severity: Severity::Error,
                    component: Component::Statistics,
                    message: "Statistics.db not found or unreadable".into(),
                });
                return;
            }
        };

        if serde_json::from_str::<serde_json::Value>(&data).is_err() {
            issues.push(VerificationIssue {
                severity: Severity::Error,
                component: Component::Statistics,
                message: "Statistics.db is not valid JSON".into(),
            });
        }
    }

    fn verify_toc(desc: &SSTableDescriptor, issues: &mut Vec<VerificationIssue>) {
        let path = desc.component_path(Component::Toc);
        let data = match fs::read_to_string(&path) {
            Ok(d) => d,
            Err(_) => {
                issues.push(VerificationIssue {
                    severity: Severity::Warning,
                    component: Component::Toc,
                    message: "TOC.txt not found or unreadable".into(),
                });
                return;
            }
        };

        for line in data.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            let file_path = desc.directory.join(line);
            if !file_path.exists() {
                issues.push(VerificationIssue {
                    severity: Severity::Warning,
                    component: Component::Toc,
                    message: format!("TOC.txt lists missing file: {line}"),
                });
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memtable::partition::{Cell, PartitionData, Row};
    use crate::sstable::writer::SSTableWriter;
    use tempfile::TempDir;

    fn sample_partitions() -> Vec<(Vec<u8>, PartitionData)> {
        let mut partitions = Vec::new();
        for i in 0..3u8 {
            let mut pd = PartitionData::new();
            pd.apply_row(Row {
                clustering_key: vec![i],
                cells: vec![Cell {
                    column: "c".to_string(),
                    value: Some(vec![i]),
                    timestamp: 1000 + i as i64,
                    ttl: 0,
                    local_deletion_time: None,
                    is_tombstone: false,
                }],
                is_tombstone: false,
                local_deletion_time: None,
            });
            partitions.push((vec![i], pd));
        }
        partitions
    }

    #[test]
    fn verify_valid_sstable_no_issues() {
        let dir = TempDir::new().unwrap();
        let desc = SSTableDescriptor::new(dir.path(), "ks", "t1", 1);
        SSTableWriter::new(desc.clone())
            .write(&sample_partitions())
            .unwrap();

        let result = SSTableVerifier::verify(&desc);
        let errors: Vec<_> = result
            .issues
            .iter()
            .filter(|i| i.severity == Severity::Error)
            .collect();
        assert!(errors.is_empty(), "Expected no errors, got: {errors:?}");
        assert!(result.is_valid());
    }

    #[test]
    fn verify_corrupted_crc() {
        let dir = TempDir::new().unwrap();
        let desc = SSTableDescriptor::new(dir.path(), "ks", "t1", 1);
        SSTableWriter::new(desc.clone())
            .write(&sample_partitions())
            .unwrap();

        // Corrupt the last 4 bytes (CRC) of Data.db
        let data_path = desc.component_path(Component::Data);
        let mut data = fs::read(&data_path).unwrap();
        let len = data.len();
        data[len - 1] ^= 0xFF;
        data[len - 2] ^= 0xFF;
        fs::write(&data_path, &data).unwrap();

        let result = SSTableVerifier::verify(&desc);
        assert!(!result.is_valid());
        assert!(result.issues.iter().any(|i| i.severity == Severity::Error
            && i.component == Component::Data
            && i.message.contains("CRC")));
    }

    #[test]
    fn verify_missing_component() {
        let dir = TempDir::new().unwrap();
        let desc = SSTableDescriptor::new(dir.path(), "ks", "t1", 1);
        SSTableWriter::new(desc.clone())
            .write(&sample_partitions())
            .unwrap();

        // Remove Filter.db
        let filter_path = desc.component_path(Component::Filter);
        fs::remove_file(&filter_path).unwrap();

        let result = SSTableVerifier::verify(&desc);
        assert!(!result.is_valid());
        assert!(
            result
                .issues
                .iter()
                .any(|i| i.severity == Severity::Error && i.component == Component::Filter)
        );
    }
}
