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

//! Cassandra wall-clock timestamps in microseconds since Unix epoch.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.db.rows.Cell` (timestamp field)
//! - `org.apache.cassandra.utils.FBUtilities.timestampMicros()`
//! - Constants from `org.apache.cassandra.db.LivenessInfo`

use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};

use byteorder::{BigEndian, ByteOrder};

/// Sentinel value indicating "no timestamp" / "not set".
/// Matches Java's `LivenessInfo.NO_TIMESTAMP = Long.MIN_VALUE`.
pub const NO_TIMESTAMP: i64 = i64::MIN;

/// A Cassandra timestamp in microseconds since the Unix epoch.
///
/// Cassandra uses microsecond-resolution wall-clock timestamps for cell-level
/// conflict resolution (last-write-wins). This type wraps the raw `i64` value
/// and provides ordering, serialization, and convenience methods.
#[derive(
    Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub struct Timestamp(pub i64);

impl Timestamp {
    /// Sentinel: no timestamp assigned.
    pub const NONE: Self = Self(NO_TIMESTAMP);

    /// Create a timestamp from a raw microsecond value.
    #[inline]
    pub const fn from_micros(us: i64) -> Self {
        Self(us)
    }

    /// Returns the raw microsecond value.
    #[inline]
    pub const fn as_micros(self) -> i64 {
        self.0
    }

    /// Returns `true` if this is a real timestamp (not the sentinel).
    #[inline]
    pub const fn is_set(self) -> bool {
        self.0 != NO_TIMESTAMP
    }

    /// Returns the current wall-clock time as a Cassandra timestamp.
    pub fn now() -> Self {
        let duration = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock before Unix epoch");
        Self(duration.as_micros() as i64)
    }

    /// Serialize to 8 bytes, big-endian (matching Java DataOutput.writeLong).
    #[inline]
    pub fn serialize(self, buf: &mut [u8; 8]) {
        BigEndian::write_i64(buf, self.0);
    }

    /// Deserialize from 8 bytes, big-endian.
    #[inline]
    pub fn deserialize(buf: &[u8; 8]) -> Self {
        Self(BigEndian::read_i64(buf))
    }

    /// Size in bytes when serialized.
    pub const SERIALIZED_SIZE: usize = 8;
}

impl fmt::Debug for Timestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if *self == Self::NONE {
            write!(f, "Timestamp(NONE)")
        } else {
            write!(f, "Timestamp({}µs)", self.0)
        }
    }
}

impl fmt::Display for Timestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if *self == Self::NONE {
            write!(f, "<no timestamp>")
        } else {
            write!(f, "{}", self.0)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn none_sentinel() {
        assert_eq!(Timestamp::NONE.as_micros(), i64::MIN);
        assert!(!Timestamp::NONE.is_set());
    }

    #[test]
    fn real_timestamp_is_set() {
        let ts = Timestamp::from_micros(1_000_000);
        assert!(ts.is_set());
        assert_eq!(ts.as_micros(), 1_000_000);
    }

    #[test]
    fn now_is_positive() {
        let ts = Timestamp::now();
        assert!(ts.as_micros() > 0, "current time should be positive");
    }

    #[test]
    fn serialize_round_trip() {
        let ts = Timestamp::from_micros(1_710_000_000_000_000);
        let mut buf = [0u8; 8];
        ts.serialize(&mut buf);
        let deserialized = Timestamp::deserialize(&buf);
        assert_eq!(ts, deserialized);
    }

    #[test]
    fn serialize_none_round_trip() {
        let ts = Timestamp::NONE;
        let mut buf = [0u8; 8];
        ts.serialize(&mut buf);
        let deserialized = Timestamp::deserialize(&buf);
        assert_eq!(ts, deserialized);
    }

    #[test]
    fn ordering() {
        let a = Timestamp::from_micros(100);
        let b = Timestamp::from_micros(200);
        let none = Timestamp::NONE;
        assert!(none < a);
        assert!(a < b);
    }

    /// Golden test: big-endian encoding of known value.
    /// Java: DataOutputStream.writeLong(1000000L) produces these bytes.
    #[test]
    fn golden_serialization() {
        let ts = Timestamp::from_micros(1_000_000);
        let mut buf = [0u8; 8];
        ts.serialize(&mut buf);
        assert_eq!(buf, [0x00, 0x00, 0x00, 0x00, 0x00, 0x0F, 0x42, 0x40]);
    }
}
