// Licensed under Apache License, Version 2.0.

//! Partition and row structures for the memtable.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.db.rows.Row`
//! - `org.apache.cassandra.db.rows.Cell`
//! - `org.apache.cassandra.db.partitions.PartitionUpdate`

use std::collections::BTreeMap;

/// A single cell within a row.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Cell {
    pub column: String,
    pub value: Option<Vec<u8>>,
    pub timestamp: i64,
    /// Time-to-live in seconds; 0 means no TTL.
    pub ttl: i32,
    /// Local deletion time (seconds since epoch) for TTL expiration.
    pub local_deletion_time: Option<i32>,
    /// If true, this cell is a tombstone (column deletion).
    pub is_tombstone: bool,
}

impl Cell {
    /// Returns true if this cell should be considered live at the given time.
    pub fn is_live_at(&self, now_seconds: i32) -> bool {
        if self.is_tombstone {
            return false;
        }
        if self.ttl > 0 {
            if let Some(ldt) = self.local_deletion_time {
                return now_seconds < ldt;
            }
        }
        true
    }

    /// Merge: higher timestamp wins. If equal timestamp, tombstone wins.
    pub fn merge_with(&self, other: &Cell) -> Cell {
        if other.timestamp > self.timestamp {
            other.clone()
        } else if other.timestamp == self.timestamp && other.is_tombstone && !self.is_tombstone {
            other.clone()
        } else {
            self.clone()
        }
    }
}

/// A row in a partition, identified by its clustering key.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Row {
    pub clustering_key: Vec<u8>,
    pub cells: Vec<Cell>,
    /// Row-level tombstone.
    pub is_tombstone: bool,
    pub local_deletion_time: Option<i32>,
}

impl Row {
    /// Merge another row into this one. Cell-level last-write-wins.
    pub fn merge_with(&mut self, other: &Row) {
        // Row-level tombstone: later one wins
        if other.is_tombstone && !self.is_tombstone {
            self.is_tombstone = true;
            self.local_deletion_time = other.local_deletion_time;
        }

        // Merge cells by column name
        for other_cell in &other.cells {
            if let Some(existing) = self.cells.iter_mut().find(|c| c.column == other_cell.column) {
                *existing = existing.merge_with(other_cell);
            } else {
                self.cells.push(other_cell.clone());
            }
        }
    }
}

/// All rows within a single partition, keyed by clustering key bytes.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PartitionData {
    pub rows: BTreeMap<Vec<u8>, Row>,
    /// Partition-level tombstone timestamp, if any.
    pub tombstone_timestamp: Option<i64>,
    pub tombstone_local_deletion_time: Option<i32>,
}

impl PartitionData {
    pub fn new() -> Self {
        Self {
            rows: BTreeMap::new(),
            tombstone_timestamp: None,
            tombstone_local_deletion_time: None,
        }
    }

    /// Apply a row to this partition (insert or merge).
    pub fn apply_row(&mut self, row: Row) {
        let ck = row.clustering_key.clone();
        self.rows
            .entry(ck)
            .and_modify(|existing| existing.merge_with(&row))
            .or_insert(row);
    }

    /// Set a partition-level tombstone.
    pub fn set_tombstone(&mut self, timestamp: i64, local_deletion_time: i32) {
        match self.tombstone_timestamp {
            Some(existing_ts) if existing_ts >= timestamp => {}
            _ => {
                self.tombstone_timestamp = Some(timestamp);
                self.tombstone_local_deletion_time = Some(local_deletion_time);
            }
        }
    }

    /// Returns all live rows at the given time, after filtering tombstones and TTLs.
    pub fn live_rows(&self, now_seconds: i32) -> Vec<&Row> {
        self.rows
            .values()
            .filter(|row| {
                if row.is_tombstone {
                    return false;
                }
                // Check if partition tombstone supersedes
                if let Some(pt_ts) = self.tombstone_timestamp {
                    if let Some(pt_ldt) = self.tombstone_local_deletion_time {
                        if now_seconds >= pt_ldt {
                            // Check if any cell is newer than the tombstone
                            return row.cells.iter().any(|c| c.timestamp > pt_ts);
                        }
                    }
                }
                true
            })
            .collect()
    }
}

impl Default for PartitionData {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cell_merge_newer_wins() {
        let c1 = Cell {
            column: "name".into(),
            value: Some(b"old".to_vec()),
            timestamp: 100,
            ttl: 0,
            local_deletion_time: None,
            is_tombstone: false,
        };
        let c2 = Cell {
            column: "name".into(),
            value: Some(b"new".to_vec()),
            timestamp: 200,
            ttl: 0,
            local_deletion_time: None,
            is_tombstone: false,
        };
        let merged = c1.merge_with(&c2);
        assert_eq!(merged.value.as_deref(), Some(b"new".as_slice()));
        assert_eq!(merged.timestamp, 200);
    }

    #[test]
    fn cell_merge_tombstone_wins_on_tie() {
        let c1 = Cell {
            column: "name".into(),
            value: Some(b"val".to_vec()),
            timestamp: 100,
            ttl: 0,
            local_deletion_time: None,
            is_tombstone: false,
        };
        let c2 = Cell {
            column: "name".into(),
            value: None,
            timestamp: 100,
            ttl: 0,
            local_deletion_time: Some(100),
            is_tombstone: true,
        };
        let merged = c1.merge_with(&c2);
        assert!(merged.is_tombstone);
    }

    #[test]
    fn cell_ttl_expiration() {
        let cell = Cell {
            column: "x".into(),
            value: Some(b"v".to_vec()),
            timestamp: 100,
            ttl: 60,
            local_deletion_time: Some(160), // expires at 160
            is_tombstone: false,
        };
        assert!(cell.is_live_at(150)); // before expiry
        assert!(!cell.is_live_at(160)); // at expiry
        assert!(!cell.is_live_at(200)); // after expiry
    }

    #[test]
    fn row_merge() {
        let mut r1 = Row {
            clustering_key: b"ck".to_vec(),
            cells: vec![Cell {
                column: "a".into(),
                value: Some(b"1".to_vec()),
                timestamp: 100,
                ttl: 0,
                local_deletion_time: None,
                is_tombstone: false,
            }],
            is_tombstone: false,
            local_deletion_time: None,
        };
        let r2 = Row {
            clustering_key: b"ck".to_vec(),
            cells: vec![
                Cell {
                    column: "a".into(),
                    value: Some(b"2".to_vec()),
                    timestamp: 200,
                    ttl: 0,
                    local_deletion_time: None,
                    is_tombstone: false,
                },
                Cell {
                    column: "b".into(),
                    value: Some(b"3".to_vec()),
                    timestamp: 200,
                    ttl: 0,
                    local_deletion_time: None,
                    is_tombstone: false,
                },
            ],
            is_tombstone: false,
            local_deletion_time: None,
        };

        r1.merge_with(&r2);
        assert_eq!(r1.cells.len(), 2);
        let cell_a = r1.cells.iter().find(|c| c.column == "a").unwrap();
        assert_eq!(cell_a.value.as_deref(), Some(b"2".as_slice()));
    }

    #[test]
    fn partition_apply_and_merge() {
        let mut pd = PartitionData::new();

        pd.apply_row(Row {
            clustering_key: b"ck1".to_vec(),
            cells: vec![Cell {
                column: "x".into(),
                value: Some(b"old".to_vec()),
                timestamp: 100,
                ttl: 0,
                local_deletion_time: None,
                is_tombstone: false,
            }],
            is_tombstone: false,
            local_deletion_time: None,
        });

        pd.apply_row(Row {
            clustering_key: b"ck1".to_vec(),
            cells: vec![Cell {
                column: "x".into(),
                value: Some(b"new".to_vec()),
                timestamp: 200,
                ttl: 0,
                local_deletion_time: None,
                is_tombstone: false,
            }],
            is_tombstone: false,
            local_deletion_time: None,
        });

        assert_eq!(pd.rows.len(), 1);
        let row = pd.rows.get(&b"ck1".to_vec()).unwrap();
        assert_eq!(row.cells[0].value.as_deref(), Some(b"new".as_slice()));
    }

    #[test]
    fn partition_tombstone() {
        let mut pd = PartitionData::new();
        pd.apply_row(Row {
            clustering_key: b"ck1".to_vec(),
            cells: vec![Cell {
                column: "x".into(),
                value: Some(b"v".to_vec()),
                timestamp: 100,
                ttl: 0,
                local_deletion_time: None,
                is_tombstone: false,
            }],
            is_tombstone: false,
            local_deletion_time: None,
        });

        pd.set_tombstone(200, 200);
        let live = pd.live_rows(300);
        assert!(live.is_empty()); // all deleted by partition tombstone
    }
}
