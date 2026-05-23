// Licensed under Apache License, Version 2.0.

//! Full Query Logging (FQL).
//!
//! ## Java Oracle
//! - `org.apache.cassandra.fql.FullQueryLogger`
//! - `org.apache.cassandra.fql.FullQueryLoggerOptions`
//!
//! ## Design
//! Binary log format with length-prefixed records. Each record contains:
//! - version byte (1)
//! - timestamp (8 bytes, epoch micros)
//! - consistency level (2 bytes)
//! - query text (length-prefixed UTF-8)
//! - number of bind values (4 bytes)
//! - bind values (each length-prefixed)
//!
//! Feature-gated behind `fql` feature flag.

use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use serde::{Deserialize, Serialize};

// ─── FQL Record ────────────────────────────────────────────────────────────

/// A single FQL record.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FqlRecord {
    pub timestamp_micros: i64,
    pub consistency_level: u16,
    pub query: String,
    pub bind_values: Vec<Vec<u8>>,
}

const FQL_VERSION: u8 = 1;

impl FqlRecord {
    /// Serialize this record to binary format.
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();

        // Version
        buf.push(FQL_VERSION);

        // Timestamp
        buf.extend_from_slice(&self.timestamp_micros.to_be_bytes());

        // Consistency level
        buf.extend_from_slice(&self.consistency_level.to_be_bytes());

        // Query
        let query_bytes = self.query.as_bytes();
        buf.extend_from_slice(&(query_bytes.len() as u32).to_be_bytes());
        buf.extend_from_slice(query_bytes);

        // Bind values count
        buf.extend_from_slice(&(self.bind_values.len() as u32).to_be_bytes());

        // Bind values
        for val in &self.bind_values {
            buf.extend_from_slice(&(val.len() as u32).to_be_bytes());
            buf.extend_from_slice(val);
        }

        // Prepend total record length
        let record_len = buf.len() as u32;
        let mut result = Vec::with_capacity(4 + buf.len());
        result.extend_from_slice(&record_len.to_be_bytes());
        result.extend(buf);
        result
    }

    /// Deserialize a record from binary data.
    pub fn decode(data: &[u8]) -> Result<(Self, usize), FqlError> {
        if data.len() < 4 {
            return Err(FqlError::CorruptedRecord(
                "too short for length prefix".into(),
            ));
        }

        let record_len = u32::from_be_bytes([data[0], data[1], data[2], data[3]]) as usize;
        if data.len() < 4 + record_len {
            return Err(FqlError::CorruptedRecord("truncated record".into()));
        }

        let mut pos = 4;

        // Version
        let version = data[pos];
        pos += 1;
        if version != FQL_VERSION {
            return Err(FqlError::UnsupportedVersion(version));
        }

        // Timestamp
        let timestamp = i64::from_be_bytes([
            data[pos],
            data[pos + 1],
            data[pos + 2],
            data[pos + 3],
            data[pos + 4],
            data[pos + 5],
            data[pos + 6],
            data[pos + 7],
        ]);
        pos += 8;

        // Consistency level
        let consistency = u16::from_be_bytes([data[pos], data[pos + 1]]);
        pos += 2;

        // Query
        let query_len =
            u32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]) as usize;
        pos += 4;
        if pos + query_len > 4 + record_len {
            return Err(FqlError::CorruptedRecord("query length overflow".into()));
        }
        let query = String::from_utf8(data[pos..pos + query_len].to_vec())
            .map_err(|_| FqlError::CorruptedRecord("invalid UTF-8 in query".into()))?;
        pos += query_len;

        // Bind values count
        let bind_count =
            u32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]) as usize;
        pos += 4;

        // Bind values
        let mut bind_values = Vec::with_capacity(bind_count);
        for _ in 0..bind_count {
            if pos + 4 > 4 + record_len {
                return Err(FqlError::CorruptedRecord(
                    "bind value length overflow".into(),
                ));
            }
            let val_len =
                u32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]])
                    as usize;
            pos += 4;
            if pos + val_len > 4 + record_len {
                return Err(FqlError::CorruptedRecord("bind value data overflow".into()));
            }
            bind_values.push(data[pos..pos + val_len].to_vec());
            pos += val_len;
        }

        Ok((
            FqlRecord {
                timestamp_micros: timestamp,
                consistency_level: consistency,
                query,
                bind_values,
            },
            4 + record_len,
        ))
    }
}

// ─── Errors ────────────────────────────────────────────────────────────────

#[derive(Debug, thiserror::Error)]
pub enum FqlError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    #[error("corrupted FQL record: {0}")]
    CorruptedRecord(String),
    #[error("unsupported FQL version: {0}")]
    UnsupportedVersion(u8),
}

// ─── FQL Logger ────────────────────────────────────────────────────────────

/// Full Query Logger — writes binary FQL records to rolling files.
pub struct FqlLogger {
    log_dir: PathBuf,
    max_file_size: u64,
    enabled: bool,
}

impl FqlLogger {
    pub fn new(log_dir: PathBuf, max_file_size_mb: u64, enabled: bool) -> Result<Self, FqlError> {
        if enabled {
            fs::create_dir_all(&log_dir)?;
        }
        Ok(Self {
            log_dir,
            max_file_size: max_file_size_mb * 1024 * 1024,
            enabled,
        })
    }

    fn current_log_path(&self) -> PathBuf {
        self.log_dir.join("fql.bin")
    }

    /// Log a query record.
    pub fn log_query(&self, record: &FqlRecord) -> Result<(), FqlError> {
        if !self.enabled {
            return Ok(());
        }

        let path = self.current_log_path();

        // Check if rotation is needed
        if let Ok(meta) = fs::metadata(&path) {
            if meta.len() >= self.max_file_size {
                self.rotate()?;
            }
        }

        let data = record.encode();
        let mut file = OpenOptions::new().create(true).append(true).open(&path)?;
        file.write_all(&data)?;
        Ok(())
    }

    fn rotate(&self) -> Result<(), FqlError> {
        let current = self.current_log_path();
        if !current.exists() {
            return Ok(());
        }
        let ts = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let rotated = self.log_dir.join(format!("fql-{}.bin", ts));
        fs::rename(&current, &rotated)?;
        Ok(())
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled
    }
}

// ─── FQL Reader ────────────────────────────────────────────────────────────

/// Reads FQL binary log files.
pub struct FqlReader;

impl FqlReader {
    /// Read all records from an FQL log file.
    pub fn read_all(path: &Path) -> Result<Vec<FqlRecord>, FqlError> {
        let data = fs::read(path)?;
        let mut records = Vec::new();
        let mut pos = 0;

        while pos < data.len() {
            let (record, consumed) = FqlRecord::decode(&data[pos..])?;
            records.push(record);
            pos += consumed;
        }

        Ok(records)
    }
}

// ─── FQL Options ───────────────────────────────────────────────────────────

/// Configuration for FQL.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FqlOptions {
    pub enabled: bool,
    pub log_dir: String,
    pub max_log_size_mb: u64,
    pub block: bool,
    pub max_queue_weight: u64,
}

impl Default for FqlOptions {
    fn default() -> Self {
        Self {
            enabled: false,
            log_dir: "logs/fql".into(),
            max_log_size_mb: 100,
            block: true,
            max_queue_weight: 256 * 1024 * 1024, // 256 MiB
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn sample_record() -> FqlRecord {
        FqlRecord {
            timestamp_micros: 1_700_000_000_000_000,
            consistency_level: 1, // ONE
            query: "SELECT * FROM ks.users WHERE id = ?".into(),
            bind_values: vec![b"user-123".to_vec()],
        }
    }

    #[test]
    fn encode_decode_roundtrip() {
        let record = sample_record();
        let encoded = record.encode();
        let (decoded, consumed) = FqlRecord::decode(&encoded).unwrap();
        assert_eq!(decoded, record);
        assert_eq!(consumed, encoded.len());
    }

    #[test]
    fn encode_decode_no_bindings() {
        let record = FqlRecord {
            timestamp_micros: 123456789,
            consistency_level: 0,
            query: "SELECT 1".into(),
            bind_values: vec![],
        };
        let encoded = record.encode();
        let (decoded, _) = FqlRecord::decode(&encoded).unwrap();
        assert_eq!(decoded, record);
    }

    #[test]
    fn encode_decode_multiple_bindings() {
        let record = FqlRecord {
            timestamp_micros: 999,
            consistency_level: 7,
            query: "INSERT INTO t (a,b,c) VALUES (?,?,?)".into(),
            bind_values: vec![b"val1".to_vec(), b"val2".to_vec(), vec![0x00, 0x01, 0x02]],
        };
        let encoded = record.encode();
        let (decoded, _) = FqlRecord::decode(&encoded).unwrap();
        assert_eq!(decoded, record);
    }

    #[test]
    fn decode_truncated_fails() {
        let result = FqlRecord::decode(&[0x00, 0x00]);
        assert!(result.is_err());
    }

    #[test]
    fn fql_logger_write_and_read() {
        let dir = TempDir::new().unwrap();
        let logger = FqlLogger::new(dir.path().to_path_buf(), 10, true).unwrap();

        let record = sample_record();
        logger.log_query(&record).unwrap();

        let records = FqlReader::read_all(&dir.path().join("fql.bin")).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0], record);
    }

    #[test]
    fn fql_logger_multiple_records() {
        let dir = TempDir::new().unwrap();
        let logger = FqlLogger::new(dir.path().to_path_buf(), 10, true).unwrap();

        for i in 0..5 {
            let record = FqlRecord {
                timestamp_micros: i,
                consistency_level: 1,
                query: format!("SELECT {}", i),
                bind_values: vec![],
            };
            logger.log_query(&record).unwrap();
        }

        let records = FqlReader::read_all(&dir.path().join("fql.bin")).unwrap();
        assert_eq!(records.len(), 5);
    }

    #[test]
    fn fql_logger_disabled() {
        let dir = TempDir::new().unwrap();
        let logger = FqlLogger::new(dir.path().to_path_buf(), 10, false).unwrap();
        let record = sample_record();
        logger.log_query(&record).unwrap();
        assert!(!dir.path().join("fql.bin").exists());
    }
}
