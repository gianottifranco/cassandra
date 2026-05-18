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

//! Replayable journal primitives.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.journal.Journal`
//! - `org.apache.cassandra.journal.RecordPointer`
//! - `org.apache.cassandra.journal.Segments`
//! - `org.apache.cassandra.journal.InMemoryIndex`

use std::collections::{BTreeMap, HashMap};
use std::fs::{self, File};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

const SEGMENT_MAGIC: &[u8; 4] = b"CJR1";

/// Stable address of a journal record inside a logical segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RecordPointer {
    pub segment_id: u64,
    pub position: u64,
}

impl RecordPointer {
    pub fn new(segment_id: u64, position: u64) -> Self {
        Self {
            segment_id,
            position,
        }
    }
}

/// One replayable journal record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JournalRecord {
    pub pointer: RecordPointer,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

/// In-memory segmented journal with key lookup and ordered replay.
#[derive(Debug, Clone)]
pub struct SegmentedJournal {
    segment_capacity_bytes: u64,
    current_segment_id: u64,
    current_position: u64,
    records: Vec<JournalRecord>,
    by_pointer: BTreeMap<RecordPointer, usize>,
    by_key: HashMap<Vec<u8>, Vec<RecordPointer>>,
}

impl SegmentedJournal {
    pub fn new(segment_capacity_bytes: u64) -> Self {
        Self {
            segment_capacity_bytes: segment_capacity_bytes.max(1),
            current_segment_id: 0,
            current_position: 0,
            records: Vec::new(),
            by_pointer: BTreeMap::new(),
            by_key: HashMap::new(),
        }
    }

    pub fn append(&mut self, key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> RecordPointer {
        let key = key.into();
        let value = value.into();
        let width = record_width(&key, &value);

        if self.current_position > 0
            && self.current_position.saturating_add(width) > self.segment_capacity_bytes
        {
            self.current_segment_id += 1;
            self.current_position = 0;
        }

        let pointer = RecordPointer::new(self.current_segment_id, self.current_position);
        let record = JournalRecord {
            pointer,
            key: key.clone(),
            value,
        };
        let index = self.records.len();
        self.records.push(record);
        self.by_pointer.insert(pointer, index);
        self.by_key.entry(key).or_default().push(pointer);
        self.current_position = self.current_position.saturating_add(width);
        pointer
    }

    pub fn read(&self, pointer: RecordPointer) -> Option<&JournalRecord> {
        self.by_pointer
            .get(&pointer)
            .and_then(|index| self.records.get(*index))
    }

    pub fn records_for_key(&self, key: &[u8]) -> Vec<&JournalRecord> {
        self.by_key
            .get(key)
            .into_iter()
            .flatten()
            .filter_map(|pointer| self.read(*pointer))
            .collect()
    }

    pub fn latest(&self, key: &[u8]) -> Option<&JournalRecord> {
        self.by_key
            .get(key)
            .and_then(|pointers| pointers.last())
            .and_then(|pointer| self.read(*pointer))
    }

    pub fn replay(&self) -> Vec<&JournalRecord> {
        self.by_pointer
            .keys()
            .filter_map(|pointer| self.read(*pointer))
            .collect()
    }

    pub fn truncate_before(&mut self, min_pointer: RecordPointer) -> usize {
        let before = self.records.len();
        self.records.retain(|record| record.pointer >= min_pointer);
        self.rebuild_indexes();
        before - self.records.len()
    }

    pub fn len(&self) -> usize {
        self.records.len()
    }

    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    pub fn segment_count(&self) -> usize {
        self.segment_ids().len()
    }

    pub fn segment_ids(&self) -> Vec<u64> {
        let mut ids = self
            .records
            .iter()
            .map(|record| record.pointer.segment_id)
            .collect::<Vec<_>>();
        ids.sort_unstable();
        ids.dedup();
        ids
    }

    pub fn current_segment_id(&self) -> u64 {
        self.current_segment_id
    }

    /// Write one deterministic segment file per logical journal segment.
    pub fn write_segments(&self, dir: &Path) -> io::Result<Vec<PathBuf>> {
        fs::create_dir_all(dir)?;
        let mut paths = Vec::new();

        for segment_id in self.segment_ids() {
            let path = dir.join(segment_filename(segment_id));
            let mut writer = BufWriter::new(File::create(&path)?);
            writer.write_all(SEGMENT_MAGIC)?;
            writer.write_u64::<BigEndian>(segment_id)?;
            for record in self
                .records
                .iter()
                .filter(|record| record.pointer.segment_id == segment_id)
            {
                writer.write_u64::<BigEndian>(record.pointer.position)?;
                writer.write_u32::<BigEndian>(record.key.len() as u32)?;
                writer.write_u32::<BigEndian>(record.value.len() as u32)?;
                writer.write_all(&record.key)?;
                writer.write_all(&record.value)?;
            }
            writer.flush()?;
            paths.push(path);
        }

        Ok(paths)
    }

    /// Load a journal from deterministic segment files previously written by
    /// [`write_segments`].
    pub fn load_segments(dir: &Path, segment_capacity_bytes: u64) -> io::Result<Self> {
        let mut paths = fs::read_dir(dir)?
            .filter_map(|entry| entry.ok().map(|entry| entry.path()))
            .filter(|path| path.extension().and_then(|ext| ext.to_str()) == Some("seg"))
            .collect::<Vec<_>>();
        paths.sort();

        let mut journal = Self::new(segment_capacity_bytes);
        journal.records.clear();
        for path in paths {
            journal.read_segment_file(&path)?;
        }
        journal.rebuild_indexes();
        journal.restore_tail_position();
        Ok(journal)
    }

    fn rebuild_indexes(&mut self) {
        self.by_pointer.clear();
        self.by_key.clear();

        for (index, record) in self.records.iter().enumerate() {
            self.by_pointer.insert(record.pointer, index);
            self.by_key
                .entry(record.key.clone())
                .or_default()
                .push(record.pointer);
        }
    }

    fn read_segment_file(&mut self, path: &Path) -> io::Result<()> {
        let mut reader = BufReader::new(File::open(path)?);
        let mut magic = [0u8; 4];
        reader.read_exact(&mut magic)?;
        if &magic != SEGMENT_MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid journal segment magic in {}", path.display()),
            ));
        }
        let segment_id = reader.read_u64::<BigEndian>()?;

        loop {
            let position = match reader.read_u64::<BigEndian>() {
                Ok(position) => position,
                Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(err) => return Err(err),
            };
            let key_len = reader.read_u32::<BigEndian>()? as usize;
            let value_len = reader.read_u32::<BigEndian>()? as usize;
            let mut key = vec![0u8; key_len];
            let mut value = vec![0u8; value_len];
            reader.read_exact(&mut key)?;
            reader.read_exact(&mut value)?;
            self.records.push(JournalRecord {
                pointer: RecordPointer::new(segment_id, position),
                key,
                value,
            });
        }

        Ok(())
    }

    fn restore_tail_position(&mut self) {
        if let Some(last) = self.records.iter().map(|record| record.pointer).max() {
            self.current_segment_id = last.segment_id;
            self.current_position = self
                .read(last)
                .map(|record| record.pointer.position + record_width(&record.key, &record.value))
                .unwrap_or_default();
        }
    }
}

fn record_width(key: &[u8], value: &[u8]) -> u64 {
    (key.len() as u64).saturating_add(value.len() as u64).max(1)
}

fn segment_filename(segment_id: u64) -> String {
    format!("journal-{segment_id:020}.seg")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn appends_and_replays_in_pointer_order() {
        let mut journal = SegmentedJournal::new(64);
        let first = journal.append(b"k1".to_vec(), b"v1".to_vec());
        let second = journal.append(b"k2".to_vec(), b"v2".to_vec());

        assert!(first < second);
        assert_eq!(journal.read(first).unwrap().value, b"v1");
        assert_eq!(
            journal
                .replay()
                .into_iter()
                .map(|record| record.key.as_slice())
                .collect::<Vec<_>>(),
            vec![b"k1".as_slice(), b"k2".as_slice()]
        );
    }

    #[test]
    fn rolls_segments_when_capacity_is_crossed() {
        let mut journal = SegmentedJournal::new(5);
        let first = journal.append(b"k".to_vec(), b"1111".to_vec());
        let second = journal.append(b"k".to_vec(), b"2222".to_vec());

        assert_eq!(first, RecordPointer::new(0, 0));
        assert_eq!(second, RecordPointer::new(1, 0));
        assert_eq!(journal.segment_ids(), vec![0, 1]);
        assert_eq!(journal.segment_count(), 2);
    }

    #[test]
    fn indexes_all_records_for_key_and_latest_record() {
        let mut journal = SegmentedJournal::new(64);
        journal.append(b"k1".to_vec(), b"old".to_vec());
        journal.append(b"k2".to_vec(), b"other".to_vec());
        journal.append(b"k1".to_vec(), b"new".to_vec());

        let records = journal.records_for_key(b"k1");
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].value, b"old");
        assert_eq!(journal.latest(b"k1").unwrap().value, b"new");
    }

    #[test]
    fn truncates_before_pointer_and_rebuilds_indexes() {
        let mut journal = SegmentedJournal::new(5);
        let first = journal.append(b"k1".to_vec(), b"111".to_vec());
        let second = journal.append(b"k1".to_vec(), b"222".to_vec());
        let third = journal.append(b"k2".to_vec(), b"333".to_vec());

        assert_eq!(journal.truncate_before(third), 2);
        assert!(journal.read(first).is_none());
        assert!(journal.read(second).is_none());
        assert_eq!(journal.latest(b"k1"), None);
        assert_eq!(journal.read(third).unwrap().value, b"333");
        assert_eq!(journal.len(), 1);
    }

    #[test]
    fn writes_and_loads_durable_segment_files() {
        let dir = tempfile::tempdir().unwrap();
        let mut journal = SegmentedJournal::new(5);
        let first = journal.append(b"k1".to_vec(), b"111".to_vec());
        let second = journal.append(b"k2".to_vec(), b"222".to_vec());
        let third = journal.append(b"k1".to_vec(), b"333".to_vec());

        let paths = journal.write_segments(dir.path()).unwrap();
        assert_eq!(paths.len(), 3);
        assert!(paths.iter().all(|path| path.exists()));

        let loaded = SegmentedJournal::load_segments(dir.path(), 5).unwrap();
        assert_eq!(loaded.segment_ids(), vec![0, 1, 2]);
        assert_eq!(loaded.read(first).unwrap().value, b"111");
        assert_eq!(loaded.read(second).unwrap().value, b"222");
        assert_eq!(loaded.latest(b"k1").unwrap().pointer, third);
        assert_eq!(
            loaded
                .replay()
                .into_iter()
                .map(|record| record.pointer)
                .collect::<Vec<_>>(),
            vec![first, second, third]
        );
    }
}
