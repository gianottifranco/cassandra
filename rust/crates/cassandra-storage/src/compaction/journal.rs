// Licensed under Apache License, Version 2.0.

//! WAL-style journal for SSTable mutations.
//!
//! The journal records every SSTable addition, removal, and replacement as a
//! JSON-line entry with a CRC32 checksum.  On startup the journal can be
//! replayed to reconstruct the set of live SSTables, skipping any records
//! whose checksum does not match (partial writes, bit-flips).
//!
//! ## Java Oracle
//! - `org.apache.cassandra.db.lifecycle.LogFile`
//! - `org.apache.cassandra.db.lifecycle.LogRecord`
//! - `org.apache.cassandra.db.lifecycle.LogTransaction`

use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::sstable::format::SSTableId;

// ─── Journal Entry ───────────────────────────────────────────────────────────

/// A single mutation event in the SSTable journal.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum JournalEntry {
    /// A new SSTable was flushed or compacted into existence.
    Add { sstable_id: SSTableId },
    /// An SSTable was removed (e.g. after compaction).
    Remove { sstable_id: SSTableId },
    /// Atomic compaction replacement: remove old SSTables, add new ones.
    Replace {
        removed: Vec<SSTableId>,
        added: Vec<SSTableId>,
    },
    /// Full checkpoint of the active SSTable set (enables log truncation).
    Checkpoint { active_sstables: Vec<SSTableId> },
}

// ─── Journal Record ──────────────────────────────────────────────────────────

/// A journal record wraps a [`JournalEntry`] with a sequence number and CRC32.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JournalRecord {
    pub sequence: u64,
    pub entry: JournalEntry,
    /// CRC32 computed over the JSON serialization of [`entry`](JournalRecord::entry).
    pub crc32: u32,
}

// ─── Journal ─────────────────────────────────────────────────────────────────

/// Append-only journal file for SSTable mutations.
pub struct Journal {
    #[allow(dead_code)]
    path: PathBuf,
    next_sequence: u64,
    writer: Option<BufWriter<File>>,
}

impl Journal {
    /// Creates a new journal file at `path`, truncating any existing content.
    pub fn create(path: &Path) -> io::Result<Self> {
        let file = File::create(path)?;
        Ok(Self {
            path: path.to_path_buf(),
            next_sequence: 1,
            writer: Some(BufWriter::new(file)),
        })
    }

    /// Opens an existing journal file for appending.
    ///
    /// Reads through the file to determine the next sequence number.
    pub fn open(path: &Path) -> io::Result<Self> {
        // First pass: find the highest sequence number.
        let mut next_sequence = 1u64;
        {
            let file = File::open(path)?;
            let reader = BufReader::new(file);
            for line in reader.lines() {
                let line = line?;
                if line.trim().is_empty() {
                    continue;
                }
                if let Ok(record) = serde_json::from_str::<JournalRecord>(&line) {
                    if record.sequence >= next_sequence {
                        next_sequence = record.sequence + 1;
                    }
                }
            }
        }

        // Re-open for appending.
        let file = OpenOptions::new().append(true).open(path)?;

        Ok(Self {
            path: path.to_path_buf(),
            next_sequence,
            writer: Some(BufWriter::new(file)),
        })
    }

    /// Appends a journal entry and returns its sequence number.
    ///
    /// The entry is serialized to JSON, a CRC32 is computed over those bytes,
    /// and the full [`JournalRecord`] is written as a single JSON line.
    pub fn append(&mut self, entry: JournalEntry) -> io::Result<u64> {
        let writer = self
            .writer
            .as_mut()
            .ok_or_else(|| io::Error::other("journal not open for writing"))?;

        let entry_json = serde_json::to_string(&entry)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let crc = crc32fast::hash(entry_json.as_bytes());

        let seq = self.next_sequence;
        self.next_sequence += 1;

        let record = JournalRecord {
            sequence: seq,
            entry,
            crc32: crc,
        };

        let record_json = serde_json::to_string(&record)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        writeln!(writer, "{}", record_json)?;
        writer.flush()?;

        Ok(seq)
    }

    /// Replays all valid entries from the journal file at `path`.
    ///
    /// Records with an invalid CRC32 or malformed JSON are skipped with a
    /// warning log.
    pub fn replay(path: &Path) -> io::Result<Vec<JournalEntry>> {
        let file = File::open(path)?;
        let reader = BufReader::new(file);
        let mut entries = Vec::new();

        for (line_no, line) in reader.lines().enumerate() {
            let line = match line {
                Ok(l) => l,
                Err(e) => {
                    tracing::warn!(line = line_no + 1, error = %e, "skipping unreadable journal line");
                    continue;
                }
            };

            if line.trim().is_empty() {
                continue;
            }

            let record: JournalRecord = match serde_json::from_str(&line) {
                Ok(r) => r,
                Err(e) => {
                    tracing::warn!(line = line_no + 1, error = %e, "skipping malformed journal record");
                    continue;
                }
            };

            // Verify CRC32.
            let entry_json = match serde_json::to_string(&record.entry) {
                Ok(j) => j,
                Err(e) => {
                    tracing::warn!(
                        line = line_no + 1,
                        sequence = record.sequence,
                        error = %e,
                        "skipping journal record with unserializable entry"
                    );
                    continue;
                }
            };

            let computed_crc = crc32fast::hash(entry_json.as_bytes());
            if computed_crc != record.crc32 {
                tracing::warn!(
                    line = line_no + 1,
                    sequence = record.sequence,
                    expected_crc = record.crc32,
                    computed_crc = computed_crc,
                    "skipping journal record with CRC mismatch"
                );
                continue;
            }

            entries.push(record.entry);
        }

        Ok(entries)
    }

    /// Deletes the journal file.
    pub fn cleanup(path: &Path) -> io::Result<()> {
        fs::remove_file(path)
    }

    /// Flushes the write buffer and syncs the file to disk.
    pub fn sync(&mut self) -> io::Result<()> {
        if let Some(writer) = self.writer.as_mut() {
            writer.flush()?;
            writer.get_ref().sync_all()?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_and_replay_entries_in_order() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.journal");

        {
            let mut journal = Journal::create(&path).unwrap();
            journal.append(JournalEntry::Add { sstable_id: 1 }).unwrap();
            journal.append(JournalEntry::Add { sstable_id: 2 }).unwrap();
            journal
                .append(JournalEntry::Remove { sstable_id: 1 })
                .unwrap();
            journal
                .append(JournalEntry::Replace {
                    removed: vec![2],
                    added: vec![3, 4],
                })
                .unwrap();
            journal
                .append(JournalEntry::Checkpoint {
                    active_sstables: vec![3, 4],
                })
                .unwrap();
        }

        let entries = Journal::replay(&path).unwrap();
        assert_eq!(entries.len(), 5);
        assert_eq!(entries[0], JournalEntry::Add { sstable_id: 1 });
        assert_eq!(entries[1], JournalEntry::Add { sstable_id: 2 });
        assert_eq!(entries[2], JournalEntry::Remove { sstable_id: 1 });
        assert_eq!(
            entries[3],
            JournalEntry::Replace {
                removed: vec![2],
                added: vec![3, 4],
            }
        );
        assert_eq!(
            entries[4],
            JournalEntry::Checkpoint {
                active_sstables: vec![3, 4],
            }
        );
    }

    #[test]
    fn crc_corruption_detection() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("corrupt.journal");

        // Write two valid entries.
        {
            let mut journal = Journal::create(&path).unwrap();
            journal.append(JournalEntry::Add { sstable_id: 1 }).unwrap();
            journal.append(JournalEntry::Add { sstable_id: 2 }).unwrap();
        }

        // Corrupt the CRC of the first record by flipping a byte.
        {
            let content = fs::read_to_string(&path).unwrap();
            let mut lines: Vec<String> = content.lines().map(String::from).collect();
            assert!(lines.len() >= 2);

            // Parse the first record, corrupt its crc32, and write back.
            let mut record: JournalRecord = serde_json::from_str(&lines[0]).unwrap();
            record.crc32 = record.crc32.wrapping_add(1);
            lines[0] = serde_json::to_string(&record).unwrap();

            let corrupted = lines.join("\n") + "\n";
            fs::write(&path, corrupted).unwrap();
        }

        // Replay should skip the corrupt first record.
        let entries = Journal::replay(&path).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0], JournalEntry::Add { sstable_id: 2 });
    }

    #[test]
    fn partial_write_handling() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("partial.journal");

        // Write one valid entry.
        {
            let mut journal = Journal::create(&path).unwrap();
            journal.append(JournalEntry::Add { sstable_id: 1 }).unwrap();
        }

        // Append a truncated line (simulating a crash mid-write).
        {
            let mut file = OpenOptions::new().append(true).open(&path).unwrap();
            write!(file, "{{\"sequence\":2,\"entry\":{{\"Add\":{{\"sst").unwrap();
        }

        // Replay should return only the first valid entry.
        let entries = Journal::replay(&path).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0], JournalEntry::Add { sstable_id: 1 });
    }

    #[test]
    fn cleanup_deletes_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cleanup.journal");

        Journal::create(&path).unwrap();
        assert!(path.exists());

        Journal::cleanup(&path).unwrap();
        assert!(!path.exists());
    }

    #[test]
    fn sequence_numbers_increment() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("seq.journal");

        let mut journal = Journal::create(&path).unwrap();
        let seq1 = journal.append(JournalEntry::Add { sstable_id: 1 }).unwrap();
        let seq2 = journal.append(JournalEntry::Add { sstable_id: 2 }).unwrap();
        let seq3 = journal.append(JournalEntry::Add { sstable_id: 3 }).unwrap();

        assert_eq!(seq1, 1);
        assert_eq!(seq2, 2);
        assert_eq!(seq3, 3);
    }

    #[test]
    fn open_existing_continues_sequence() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("reopen.journal");

        // Write two entries.
        {
            let mut journal = Journal::create(&path).unwrap();
            journal.append(JournalEntry::Add { sstable_id: 1 }).unwrap();
            journal.append(JournalEntry::Add { sstable_id: 2 }).unwrap();
        }

        // Re-open and continue.
        {
            let mut journal = Journal::open(&path).unwrap();
            let seq = journal.append(JournalEntry::Add { sstable_id: 3 }).unwrap();
            assert_eq!(seq, 3);
        }

        // All three entries should be present.
        let entries = Journal::replay(&path).unwrap();
        assert_eq!(entries.len(), 3);
    }

    #[test]
    fn sync_flushes_to_disk() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sync.journal");

        let mut journal = Journal::create(&path).unwrap();
        journal.append(JournalEntry::Add { sstable_id: 1 }).unwrap();
        journal.sync().unwrap();

        // File should be readable and contain the entry.
        let entries = Journal::replay(&path).unwrap();
        assert_eq!(entries.len(), 1);
    }
}
