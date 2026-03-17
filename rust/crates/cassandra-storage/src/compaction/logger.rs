// Licensed under Apache License, Version 2.0.

//! Structured JSON-lines compaction event logging.
//!
//! Writes compaction lifecycle events (started, progress, completed, failed,
//! cancelled) as newline-delimited JSON for post-hoc analysis and tooling.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.db.compaction.CompactionManager` (logging aspects)
//! - `org.apache.cassandra.tools.nodetool.CompactionHistory`

use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::compaction::errors::CompactionType;
use crate::sstable::format::SSTableId;

// ─── CompactionEvent ────────────────────────────────────────────────────────

/// A structured compaction lifecycle event.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum CompactionEvent {
    Started {
        id: Uuid,
        compaction_type: CompactionType,
        sstable_ids: Vec<SSTableId>,
        timestamp_ms: u64,
    },
    Progress {
        id: Uuid,
        bytes_processed: u64,
        total_bytes: u64,
        timestamp_ms: u64,
    },
    Completed {
        id: Uuid,
        input_sstables: usize,
        output_sstables: usize,
        bytes_read: u64,
        bytes_written: u64,
        duration_ms: u64,
        timestamp_ms: u64,
    },
    Failed {
        id: Uuid,
        error: String,
        timestamp_ms: u64,
    },
    Cancelled {
        id: Uuid,
        timestamp_ms: u64,
    },
}

// ─── CompactionLogger ───────────────────────────────────────────────────────

/// Logger that writes compaction events to a JSONL file and/or `tracing`.
pub struct CompactionLogger {
    log_path: Option<PathBuf>,
    use_tracing: bool,
}

impl CompactionLogger {
    /// Create a new compaction logger.
    pub fn new(log_path: Option<PathBuf>, use_tracing: bool) -> Self {
        Self {
            log_path,
            use_tracing,
        }
    }

    /// Log a compaction event.
    ///
    /// Appends a JSON line to the log file (if configured) and emits a
    /// `tracing::info!` event (if enabled).
    pub fn log_event(&self, event: &CompactionEvent) -> std::io::Result<()> {
        if let Some(ref path) = self.log_path {
            let json = serde_json::to_string(event).map_err(|e| {
                std::io::Error::new(std::io::ErrorKind::Other, format!("JSON error: {e}"))
            })?;
            let mut file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)?;
            writeln!(file, "{}", json)?;
        }

        if self.use_tracing {
            match event {
                CompactionEvent::Started {
                    id,
                    compaction_type,
                    sstable_ids,
                    timestamp_ms,
                } => {
                    tracing::info!(
                        compaction_id = %id,
                        compaction_type = ?compaction_type,
                        sstable_count = sstable_ids.len(),
                        timestamp_ms = timestamp_ms,
                        "compaction started"
                    );
                }
                CompactionEvent::Progress {
                    id,
                    bytes_processed,
                    total_bytes,
                    timestamp_ms,
                } => {
                    tracing::info!(
                        compaction_id = %id,
                        bytes_processed = bytes_processed,
                        total_bytes = total_bytes,
                        timestamp_ms = timestamp_ms,
                        "compaction progress"
                    );
                }
                CompactionEvent::Completed {
                    id,
                    input_sstables,
                    output_sstables,
                    bytes_read,
                    bytes_written,
                    duration_ms,
                    timestamp_ms,
                } => {
                    tracing::info!(
                        compaction_id = %id,
                        input_sstables = input_sstables,
                        output_sstables = output_sstables,
                        bytes_read = bytes_read,
                        bytes_written = bytes_written,
                        duration_ms = duration_ms,
                        timestamp_ms = timestamp_ms,
                        "compaction completed"
                    );
                }
                CompactionEvent::Failed {
                    id,
                    error,
                    timestamp_ms,
                } => {
                    tracing::info!(
                        compaction_id = %id,
                        error = %error,
                        timestamp_ms = timestamp_ms,
                        "compaction failed"
                    );
                }
                CompactionEvent::Cancelled { id, timestamp_ms } => {
                    tracing::info!(
                        compaction_id = %id,
                        timestamp_ms = timestamp_ms,
                        "compaction cancelled"
                    );
                }
            }
        }

        Ok(())
    }

    /// Read all compaction events from a JSONL file.
    pub fn read_events(path: &Path) -> std::io::Result<Vec<CompactionEvent>> {
        let contents = std::fs::read_to_string(path)?;
        let mut events = Vec::new();
        for line in contents.lines() {
            if line.trim().is_empty() {
                continue;
            }
            let event: CompactionEvent = serde_json::from_str(line).map_err(|e| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("failed to parse event: {e}"),
                )
            })?;
            events.push(event);
        }
        Ok(events)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn now_ms() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
    }

    #[test]
    fn json_roundtrip_started() {
        let event = CompactionEvent::Started {
            id: Uuid::new_v4(),
            compaction_type: CompactionType::Compaction,
            sstable_ids: vec![1, 2, 3],
            timestamp_ms: now_ms(),
        };
        let json = serde_json::to_string(&event).unwrap();
        let back: CompactionEvent = serde_json::from_str(&json).unwrap();
        assert!(matches!(back, CompactionEvent::Started { .. }));
    }

    #[test]
    fn json_roundtrip_progress() {
        let event = CompactionEvent::Progress {
            id: Uuid::new_v4(),
            bytes_processed: 500,
            total_bytes: 1000,
            timestamp_ms: now_ms(),
        };
        let json = serde_json::to_string(&event).unwrap();
        let back: CompactionEvent = serde_json::from_str(&json).unwrap();
        assert!(matches!(back, CompactionEvent::Progress { .. }));
    }

    #[test]
    fn json_roundtrip_completed() {
        let event = CompactionEvent::Completed {
            id: Uuid::new_v4(),
            input_sstables: 3,
            output_sstables: 1,
            bytes_read: 3000,
            bytes_written: 1000,
            duration_ms: 250,
            timestamp_ms: now_ms(),
        };
        let json = serde_json::to_string(&event).unwrap();
        let back: CompactionEvent = serde_json::from_str(&json).unwrap();
        assert!(matches!(back, CompactionEvent::Completed { .. }));
    }

    #[test]
    fn json_roundtrip_failed() {
        let event = CompactionEvent::Failed {
            id: Uuid::new_v4(),
            error: "disk full".into(),
            timestamp_ms: now_ms(),
        };
        let json = serde_json::to_string(&event).unwrap();
        let back: CompactionEvent = serde_json::from_str(&json).unwrap();
        assert!(matches!(back, CompactionEvent::Failed { .. }));
    }

    #[test]
    fn json_roundtrip_cancelled() {
        let event = CompactionEvent::Cancelled {
            id: Uuid::new_v4(),
            timestamp_ms: now_ms(),
        };
        let json = serde_json::to_string(&event).unwrap();
        let back: CompactionEvent = serde_json::from_str(&json).unwrap();
        assert!(matches!(back, CompactionEvent::Cancelled { .. }));
    }

    #[test]
    fn file_writes_valid_jsonl() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("compaction.log");
        let logger = CompactionLogger::new(Some(log_path.clone()), false);

        let id = Uuid::new_v4();
        let ts = now_ms();

        logger
            .log_event(&CompactionEvent::Started {
                id,
                compaction_type: CompactionType::Compaction,
                sstable_ids: vec![1, 2],
                timestamp_ms: ts,
            })
            .unwrap();

        logger
            .log_event(&CompactionEvent::Progress {
                id,
                bytes_processed: 500,
                total_bytes: 1000,
                timestamp_ms: ts + 100,
            })
            .unwrap();

        logger
            .log_event(&CompactionEvent::Completed {
                id,
                input_sstables: 2,
                output_sstables: 1,
                bytes_read: 1000,
                bytes_written: 800,
                duration_ms: 200,
                timestamp_ms: ts + 200,
            })
            .unwrap();

        // Read back and verify.
        let events = CompactionLogger::read_events(&log_path).unwrap();
        assert_eq!(events.len(), 3);
        assert!(matches!(events[0], CompactionEvent::Started { .. }));
        assert!(matches!(events[1], CompactionEvent::Progress { .. }));
        assert!(matches!(events[2], CompactionEvent::Completed { .. }));
    }

    #[test]
    fn read_events_parses_all_variants() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("all_events.log");
        let logger = CompactionLogger::new(Some(log_path.clone()), false);

        let id = Uuid::new_v4();
        let ts = now_ms();

        let events = vec![
            CompactionEvent::Started {
                id,
                compaction_type: CompactionType::AntiCompaction,
                sstable_ids: vec![10],
                timestamp_ms: ts,
            },
            CompactionEvent::Progress {
                id,
                bytes_processed: 100,
                total_bytes: 200,
                timestamp_ms: ts + 50,
            },
            CompactionEvent::Failed {
                id,
                error: "checksum mismatch".into(),
                timestamp_ms: ts + 100,
            },
            CompactionEvent::Cancelled {
                id: Uuid::new_v4(),
                timestamp_ms: ts + 150,
            },
        ];

        for event in &events {
            logger.log_event(event).unwrap();
        }

        let read_back = CompactionLogger::read_events(&log_path).unwrap();
        assert_eq!(read_back.len(), 4);
        assert!(matches!(read_back[0], CompactionEvent::Started { .. }));
        assert!(matches!(read_back[1], CompactionEvent::Progress { .. }));
        assert!(matches!(read_back[2], CompactionEvent::Failed { .. }));
        assert!(matches!(read_back[3], CompactionEvent::Cancelled { .. }));
    }
}
