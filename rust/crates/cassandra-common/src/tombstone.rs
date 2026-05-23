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

//! Deletion markers (tombstones) for Cassandra's MVCC model.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.db.DeletionTime`
//! - `org.apache.cassandra.db.RangeTombstone`
//! - `org.apache.cassandra.db.MutableDeletionInfo`

use std::fmt;

use byteorder::{BigEndian, ByteOrder};

use crate::timestamp::{NO_TIMESTAMP, Timestamp};
use crate::ttl::{LocalDeletionTime, NO_DELETION_TIME};

/// A point-in-time deletion marker.
///
/// Represents a tombstone with:
/// - `marked_for_delete_at`: the timestamp at which the deletion was performed
///   (microseconds). Cells with timestamps ≤ this are considered deleted.
/// - `local_deletion_time`: when this tombstone becomes eligible for GC
///   (seconds since epoch).
///
/// The sentinel `DeletionTime::LIVE` indicates "not deleted".
///
/// ## Java Oracle
///
/// `org.apache.cassandra.db.DeletionTime`
#[derive(Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct DeletionTime {
    /// Timestamp (µs) at which the delete was issued.
    pub marked_for_delete_at: i64,
    /// Local deletion time (seconds since epoch) for GC grace.
    pub local_deletion_time: i32,
}

impl DeletionTime {
    /// Sentinel: this entity is live (not deleted).
    /// Matches Java's `DeletionTime.LIVE`.
    pub const LIVE: Self = Self {
        marked_for_delete_at: NO_TIMESTAMP,
        local_deletion_time: NO_DELETION_TIME,
    };

    /// Create a new deletion time.
    #[inline]
    pub const fn new(marked_for_delete_at: i64, local_deletion_time: i32) -> Self {
        Self {
            marked_for_delete_at,
            local_deletion_time,
        }
    }

    /// Returns `true` if this represents a live (non-tombstoned) state.
    #[inline]
    pub const fn is_live(&self) -> bool {
        self.marked_for_delete_at == NO_TIMESTAMP && self.local_deletion_time == NO_DELETION_TIME
    }

    /// Returns the timestamp as a `Timestamp` value.
    #[inline]
    pub const fn timestamp(&self) -> Timestamp {
        Timestamp(self.marked_for_delete_at)
    }

    /// Returns the local deletion time wrapper.
    #[inline]
    pub const fn local_deletion(&self) -> LocalDeletionTime {
        LocalDeletionTime(self.local_deletion_time)
    }

    /// Returns `true` if this deletion supersedes `other`.
    ///
    /// A deletion supersedes another if its timestamp is strictly greater.
    /// On tie, the one with the later local deletion time wins.
    #[inline]
    pub fn supersedes(&self, other: &DeletionTime) -> bool {
        self.marked_for_delete_at > other.marked_for_delete_at
            || (self.marked_for_delete_at == other.marked_for_delete_at
                && self.local_deletion_time > other.local_deletion_time)
    }

    /// Returns `true` if this tombstone has passed the GC grace period
    /// given the current time in seconds since epoch.
    #[inline]
    pub fn is_purgeable(&self, gc_before_seconds: i32) -> bool {
        !self.is_live() && self.local_deletion_time < gc_before_seconds
    }

    /// Serialized size: 8 bytes (timestamp) + 4 bytes (local deletion time) = 12.
    pub const SERIALIZED_SIZE: usize = 12;

    /// Serialize to a 12-byte buffer (big-endian).
    pub fn serialize(&self, buf: &mut [u8; 12]) {
        BigEndian::write_i64(&mut buf[0..8], self.marked_for_delete_at);
        BigEndian::write_i32(&mut buf[8..12], self.local_deletion_time);
    }

    /// Deserialize from a 12-byte buffer.
    pub fn deserialize(buf: &[u8; 12]) -> Self {
        Self {
            marked_for_delete_at: BigEndian::read_i64(&buf[0..8]),
            local_deletion_time: BigEndian::read_i32(&buf[8..12]),
        }
    }
}

impl Ord for DeletionTime {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.marked_for_delete_at
            .cmp(&other.marked_for_delete_at)
            .then_with(|| self.local_deletion_time.cmp(&other.local_deletion_time))
    }
}

impl PartialOrd for DeletionTime {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl fmt::Debug for DeletionTime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_live() {
            write!(f, "DeletionTime(LIVE)")
        } else {
            write!(
                f,
                "DeletionTime(marked={}µs, ldt={}s)",
                self.marked_for_delete_at, self.local_deletion_time
            )
        }
    }
}

impl fmt::Display for DeletionTime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_live() {
            write!(f, "LIVE")
        } else {
            write!(
                f,
                "deletedAt={}, localDeletion={}",
                self.marked_for_delete_at, self.local_deletion_time
            )
        }
    }
}

impl Default for DeletionTime {
    fn default() -> Self {
        Self::LIVE
    }
}

/// A range tombstone covering a contiguous range of clustering keys.
///
/// ## Java Oracle
///
/// `org.apache.cassandra.db.RangeTombstone`
///
/// In this implementation, the clustering bounds are stored as raw byte
/// vectors. Full integration with the clustering key comparator happens
/// at the storage layer.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RangeTombstone {
    /// Inclusive start bound (serialized clustering prefix).
    pub start: Vec<u8>,
    /// Inclusive end bound (serialized clustering prefix).
    pub end: Vec<u8>,
    /// The deletion marker for this range.
    pub deletion: DeletionTime,
}

impl RangeTombstone {
    /// Create a new range tombstone.
    pub fn new(start: Vec<u8>, end: Vec<u8>, deletion: DeletionTime) -> Self {
        Self {
            start,
            end,
            deletion,
        }
    }

    /// Returns `true` if the underlying deletion is live (should not happen
    /// in practice, but defensive).
    pub fn is_live(&self) -> bool {
        self.deletion.is_live()
    }

    /// Returns `true` if this tombstone covers the serialized clustering key.
    #[inline]
    pub fn covers(&self, clustering_key: &[u8]) -> bool {
        clustering_key >= self.start.as_slice() && clustering_key <= self.end.as_slice()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn live_sentinel() {
        let dt = DeletionTime::LIVE;
        assert!(dt.is_live());
        assert_eq!(dt.marked_for_delete_at, i64::MIN);
        assert_eq!(dt.local_deletion_time, i32::MAX);
    }

    #[test]
    fn default_is_live() {
        let dt = DeletionTime::default();
        assert!(dt.is_live());
    }

    #[test]
    fn non_live() {
        let dt = DeletionTime::new(1_000_000, 1_700_000_000);
        assert!(!dt.is_live());
    }

    #[test]
    fn supersedes() {
        let a = DeletionTime::new(100, 1000);
        let b = DeletionTime::new(200, 1000);
        assert!(b.supersedes(&a));
        assert!(!a.supersedes(&b));
    }

    #[test]
    fn supersedes_tiebreak_by_ldt() {
        let a = DeletionTime::new(100, 1000);
        let b = DeletionTime::new(100, 1001);
        assert!(b.supersedes(&a));
        assert!(!a.supersedes(&b));
    }

    #[test]
    fn purgeable() {
        let dt = DeletionTime::new(100, 1_000);
        assert!(dt.is_purgeable(1_001));
        assert!(!dt.is_purgeable(1_000));
        assert!(!dt.is_purgeable(999));
    }

    #[test]
    fn live_never_purgeable() {
        assert!(!DeletionTime::LIVE.is_purgeable(i32::MAX));
    }

    #[test]
    fn serialize_round_trip() {
        let dt = DeletionTime::new(1_234_567_890_000, 1_700_000_000);
        let mut buf = [0u8; 12];
        dt.serialize(&mut buf);
        assert_eq!(DeletionTime::deserialize(&buf), dt);
    }

    #[test]
    fn ordering() {
        let a = DeletionTime::new(100, 1000);
        let b = DeletionTime::new(200, 500);
        let c = DeletionTime::LIVE;
        assert!(c < a); // LIVE has marked_for_delete_at = i64::MIN
        assert!(a < b);
    }

    #[test]
    fn range_tombstone_basic() {
        let rt = RangeTombstone::new(
            vec![0x01, 0x02],
            vec![0x03, 0x04],
            DeletionTime::new(500, 1_700_000_000),
        );
        assert!(!rt.is_live());
        assert_eq!(rt.start, vec![0x01, 0x02]);
        assert_eq!(rt.end, vec![0x03, 0x04]);
    }
}
