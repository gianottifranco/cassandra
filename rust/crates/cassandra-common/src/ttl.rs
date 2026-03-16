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

//! Time-To-Live (TTL) and expiration for Cassandra cells.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.db.LivenessInfo` (TTL_UNSET, NO_TTL)
//! - `org.apache.cassandra.db.ExpiringCell`
//! - `org.apache.cassandra.db.BufferExpiringCell`

use std::fmt;

use byteorder::{BigEndian, ByteOrder};

/// Sentinel: no TTL set on this cell / row.
/// Matches Java's `LivenessInfo.NO_TTL = 0`.
pub const NO_TTL: i32 = 0;

/// Sentinel value for "no local deletion time" (cell is live).
/// Matches Java's `Cell.NO_DELETION_TIME = Integer.MAX_VALUE`.
pub const NO_DELETION_TIME: i32 = i32::MAX;

/// Maximum allowed TTL in seconds (20 years).
/// Matches Java's `ExpirationDateOverflowHandling.MAX_TTL`.
pub const MAX_TTL: i32 = 20 * 365 * 24 * 60 * 60; // ~630,720,000

/// Time-To-Live in seconds.
///
/// A cell with a TTL will expire after `ttl` seconds from its write timestamp.
/// `Ttl(0)` means "no TTL" (the cell lives forever unless explicitly deleted).
#[derive(
    Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub struct Ttl(pub i32);

impl Ttl {
    /// No TTL — cell lives forever.
    pub const NONE: Self = Self(NO_TTL);

    /// Create from seconds.
    #[inline]
    pub const fn from_seconds(s: i32) -> Self {
        Self(s)
    }

    /// Get the TTL in seconds.
    #[inline]
    pub const fn as_seconds(self) -> i32 {
        self.0
    }

    /// Returns `true` if a TTL is actually set (> 0).
    #[inline]
    pub const fn is_set(self) -> bool {
        self.0 > 0
    }

    /// Returns `true` if the TTL value is within the allowed range.
    #[inline]
    pub const fn is_valid(self) -> bool {
        self.0 >= 0 && self.0 <= MAX_TTL
    }
}

impl fmt::Debug for Ttl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if *self == Self::NONE {
            write!(f, "Ttl(NONE)")
        } else {
            write!(f, "Ttl({}s)", self.0)
        }
    }
}

impl fmt::Display for Ttl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if *self == Self::NONE {
            write!(f, "<no ttl>")
        } else {
            write!(f, "{}s", self.0)
        }
    }
}

/// Local deletion time in seconds since Unix epoch.
///
/// This records when a deletion (tombstone, TTL expiration) becomes effective
/// for the purposes of garbage collection. `NO_DELETION_TIME` (i32::MAX)
/// indicates the cell is live.
///
/// ## Java Oracle
///
/// - `org.apache.cassandra.db.DeletionTime.localDeletionTime`
#[derive(
    Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub struct LocalDeletionTime(pub i32);

impl LocalDeletionTime {
    /// Sentinel: cell is live (not deleted, not expired).
    pub const LIVE: Self = Self(NO_DELETION_TIME);

    /// Create from a Unix epoch seconds value.
    #[inline]
    pub const fn from_epoch_seconds(s: i32) -> Self {
        Self(s)
    }

    /// Raw seconds-since-epoch value.
    #[inline]
    pub const fn as_epoch_seconds(self) -> i32 {
        self.0
    }

    /// Returns `true` if this represents a live (non-deleted) cell.
    #[inline]
    pub const fn is_live(self) -> bool {
        self.0 == NO_DELETION_TIME
    }

    /// Serialize to 4 bytes, big-endian.
    #[inline]
    pub fn serialize(self, buf: &mut [u8; 4]) {
        BigEndian::write_i32(buf, self.0);
    }

    /// Deserialize from 4 bytes, big-endian.
    #[inline]
    pub fn deserialize(buf: &[u8; 4]) -> Self {
        Self(BigEndian::read_i32(buf))
    }

    /// Serialized size in bytes.
    pub const SERIALIZED_SIZE: usize = 4;
}

impl fmt::Debug for LocalDeletionTime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_live() {
            write!(f, "LocalDeletionTime(LIVE)")
        } else {
            write!(f, "LocalDeletionTime({})", self.0)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ttl_none() {
        assert!(!Ttl::NONE.is_set());
        assert_eq!(Ttl::NONE.as_seconds(), 0);
        assert!(Ttl::NONE.is_valid());
    }

    #[test]
    fn ttl_set() {
        let ttl = Ttl::from_seconds(3600);
        assert!(ttl.is_set());
        assert!(ttl.is_valid());
        assert_eq!(ttl.as_seconds(), 3600);
    }

    #[test]
    fn ttl_over_max() {
        let ttl = Ttl::from_seconds(MAX_TTL + 1);
        assert!(!ttl.is_valid());
    }

    #[test]
    fn ttl_negative_invalid() {
        let ttl = Ttl::from_seconds(-1);
        assert!(!ttl.is_valid());
    }

    #[test]
    fn local_deletion_time_live() {
        assert!(LocalDeletionTime::LIVE.is_live());
        assert_eq!(LocalDeletionTime::LIVE.as_epoch_seconds(), i32::MAX);
    }

    #[test]
    fn local_deletion_time_deleted() {
        let ldt = LocalDeletionTime::from_epoch_seconds(1_700_000_000);
        assert!(!ldt.is_live());
    }

    #[test]
    fn local_deletion_time_round_trip() {
        let ldt = LocalDeletionTime::from_epoch_seconds(1_700_000_000);
        let mut buf = [0u8; 4];
        ldt.serialize(&mut buf);
        assert_eq!(LocalDeletionTime::deserialize(&buf), ldt);
    }

    #[test]
    fn ttl_ordering() {
        assert!(Ttl::NONE < Ttl::from_seconds(1));
        assert!(Ttl::from_seconds(100) < Ttl::from_seconds(200));
    }
}
