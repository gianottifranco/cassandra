// Licensed under Apache License, Version 2.0.

//! Rich cell model with proper tombstone and TTL semantics.
//!
//! ## Java Oracle
//! `org.apache.cassandra.db.rows.Cell`
//! `org.apache.cassandra.db.rows.BufferCell`

use cassandra_common::tombstone::DeletionTime;
use cassandra_common::ttl::NO_TTL;

use crate::memtable::partition::Cell;

/// Path within a complex column (collection element identity).
///
/// For maps: the serialized key bytes.
/// For sets: the serialized element bytes.
/// For lists: the timeuuid bytes.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CellPath(pub Vec<u8>);

/// Rich cell data with proper Cassandra semantics.
///
/// Distinct from `memtable::partition::Cell` — this type uses `DeletionTime`
/// and tracks TTL properly. Provides `From<Cell>` for backward compatibility.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CellData {
    /// Column name.
    pub column: String,
    /// Cell value; `None` for tombstones.
    pub value: Option<Vec<u8>>,
    /// Write timestamp in microseconds.
    pub timestamp: i64,
    /// TTL in seconds; `NO_TTL` (0) means no expiration.
    pub ttl: i32,
    /// Local deletion time (seconds since epoch) for tombstones/expiring cells.
    pub local_deletion_time: i32,
    /// Optional path for complex column cells (collections).
    pub path: Option<CellPath>,
}

impl CellData {
    /// Returns `true` if this cell is a tombstone.
    #[inline]
    pub fn is_tombstone(&self) -> bool {
        self.value.is_none() && self.local_deletion_time != i32::MAX
    }

    /// Returns `true` if this cell has a TTL.
    #[inline]
    pub const fn is_expiring(&self) -> bool {
        self.ttl != NO_TTL
    }

    /// Returns `true` if this cell is live at the given time.
    pub fn is_live(&self, now_in_seconds: i32) -> bool {
        if self.is_tombstone() {
            return false;
        }
        if self.is_expiring() {
            return now_in_seconds < self.local_deletion_time;
        }
        true
    }

    /// Returns the `DeletionTime` for this cell if it's a tombstone.
    pub fn deletion_time(&self) -> DeletionTime {
        if self.is_tombstone() {
            DeletionTime::new(self.timestamp, self.local_deletion_time)
        } else {
            DeletionTime::LIVE
        }
    }

    /// Merge two cells: higher timestamp wins. On tie, tombstone wins.
    #[allow(clippy::if_same_then_else)]
    pub fn merge_with(&self, other: &CellData) -> CellData {
        if other.timestamp > self.timestamp {
            other.clone()
        } else if other.timestamp == self.timestamp && other.is_tombstone() && !self.is_tombstone()
        {
            other.clone()
        } else {
            self.clone()
        }
    }
}

/// Convert from the simplified `memtable::partition::Cell`.
impl From<&Cell> for CellData {
    fn from(cell: &Cell) -> Self {
        Self {
            column: cell.column.clone(),
            value: cell.value.clone(),
            timestamp: cell.timestamp,
            ttl: cell.ttl,
            local_deletion_time: cell.local_deletion_time.unwrap_or(i32::MAX),
            path: None,
        }
    }
}

/// Convert back to the simplified cell.
impl From<&CellData> for Cell {
    fn from(cell: &CellData) -> Self {
        Self {
            column: cell.column.clone(),
            value: cell.value.clone(),
            timestamp: cell.timestamp,
            ttl: cell.ttl,
            local_deletion_time: if cell.local_deletion_time == i32::MAX {
                None
            } else {
                Some(cell.local_deletion_time)
            },
            is_tombstone: cell.is_tombstone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn live_cell(col: &str, val: &[u8], ts: i64) -> CellData {
        CellData {
            column: col.to_string(),
            value: Some(val.to_vec()),
            timestamp: ts,
            ttl: NO_TTL,
            local_deletion_time: i32::MAX,
            path: None,
        }
    }

    fn tombstone_cell(col: &str, ts: i64, ldt: i32) -> CellData {
        CellData {
            column: col.to_string(),
            value: None,
            timestamp: ts,
            ttl: NO_TTL,
            local_deletion_time: ldt,
            path: None,
        }
    }

    #[test]
    fn live_cell_is_live() {
        let c = live_cell("name", b"alice", 100);
        assert!(c.is_live(0));
        assert!(!c.is_tombstone());
        assert!(!c.is_expiring());
    }

    #[test]
    fn tombstone_not_live() {
        let c = tombstone_cell("name", 100, 100);
        assert!(!c.is_live(0));
        assert!(c.is_tombstone());
    }

    #[test]
    fn expiring_cell() {
        let c = CellData {
            column: "x".to_string(),
            value: Some(b"v".to_vec()),
            timestamp: 100,
            ttl: 60,
            local_deletion_time: 160,
            path: None,
        };
        assert!(c.is_expiring());
        assert!(c.is_live(159));
        assert!(!c.is_live(160));
    }

    #[test]
    fn merge_newer_wins() {
        let a = live_cell("x", b"old", 100);
        let b = live_cell("x", b"new", 200);
        let merged = a.merge_with(&b);
        assert_eq!(merged.value.as_deref(), Some(b"new".as_slice()));
    }

    #[test]
    fn merge_tombstone_wins_on_tie() {
        let a = live_cell("x", b"val", 100);
        let b = tombstone_cell("x", 100, 100);
        let merged = a.merge_with(&b);
        assert!(merged.is_tombstone());
    }

    #[test]
    fn from_simple_cell_roundtrip() {
        let simple = Cell {
            column: "col".to_string(),
            value: Some(b"val".to_vec()),
            timestamp: 1000,
            ttl: 60,
            local_deletion_time: Some(1060),
            is_tombstone: false,
        };
        let rich = CellData::from(&simple);
        assert_eq!(rich.column, "col");
        assert_eq!(rich.timestamp, 1000);
        assert_eq!(rich.ttl, 60);
        assert_eq!(rich.local_deletion_time, 1060);

        let back = Cell::from(&rich);
        assert_eq!(back.column, "col");
        assert_eq!(back.timestamp, 1000);
        assert_eq!(back.local_deletion_time, Some(1060));
    }

    #[test]
    fn cell_path_ordering() {
        let a = CellPath(vec![1, 2]);
        let b = CellPath(vec![1, 3]);
        assert!(a < b);
    }
}
