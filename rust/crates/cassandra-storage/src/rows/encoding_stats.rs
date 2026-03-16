// Licensed under Apache License, Version 2.0.

//! Encoding statistics for delta-encoding in SSTable serialization.
//!
//! ## Java Oracle
//! `org.apache.cassandra.db.rows.EncodingStats`

use cassandra_common::timestamp::NO_TIMESTAMP;
use cassandra_common::ttl::{NO_DELETION_TIME, NO_TTL};

/// Statistics tracking minimum values for delta-encoding in SSTables.
///
/// During SSTable serialization, values are delta-encoded against these
/// minimums to reduce storage size.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EncodingStats {
    /// Minimum timestamp seen (microseconds).
    pub min_timestamp: i64,
    /// Minimum local deletion time seen (seconds since epoch).
    pub min_local_deletion_time: i32,
    /// Minimum TTL seen (seconds).
    pub min_ttl: i32,
}

impl EncodingStats {
    /// No stats collected yet.
    pub const NONE: Self = Self {
        min_timestamp: NO_TIMESTAMP,
        min_local_deletion_time: NO_DELETION_TIME,
        min_ttl: NO_TTL,
    };

    /// Create new encoding stats.
    pub const fn new(min_timestamp: i64, min_local_deletion_time: i32, min_ttl: i32) -> Self {
        Self {
            min_timestamp,
            min_local_deletion_time,
            min_ttl,
        }
    }

    /// Accumulate a timestamp.
    pub fn update_timestamp(&mut self, timestamp: i64) {
        if timestamp != NO_TIMESTAMP
            && (self.min_timestamp == NO_TIMESTAMP || timestamp < self.min_timestamp)
        {
            self.min_timestamp = timestamp;
        }
    }

    /// Accumulate a local deletion time.
    pub fn update_local_deletion_time(&mut self, ldt: i32) {
        if ldt != NO_DELETION_TIME
            && (self.min_local_deletion_time == NO_DELETION_TIME
                || ldt < self.min_local_deletion_time)
        {
            self.min_local_deletion_time = ldt;
        }
    }

    /// Accumulate a TTL.
    pub fn update_ttl(&mut self, ttl: i32) {
        if ttl != NO_TTL && (self.min_ttl == NO_TTL || ttl < self.min_ttl) {
            self.min_ttl = ttl;
        }
    }

    /// Merge another set of stats into this one.
    pub fn merge_with(&mut self, other: &EncodingStats) {
        self.update_timestamp(other.min_timestamp);
        self.update_local_deletion_time(other.min_local_deletion_time);
        self.update_ttl(other.min_ttl);
    }
}

impl Default for EncodingStats {
    fn default() -> Self {
        Self::NONE
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn none_sentinel() {
        let s = EncodingStats::NONE;
        assert_eq!(s.min_timestamp, NO_TIMESTAMP);
        assert_eq!(s.min_local_deletion_time, NO_DELETION_TIME);
        assert_eq!(s.min_ttl, NO_TTL);
    }

    #[test]
    fn accumulate_timestamps() {
        let mut s = EncodingStats::NONE;
        s.update_timestamp(200);
        assert_eq!(s.min_timestamp, 200);
        s.update_timestamp(100);
        assert_eq!(s.min_timestamp, 100);
        s.update_timestamp(300);
        assert_eq!(s.min_timestamp, 100); // min unchanged
    }

    #[test]
    fn accumulate_ldt() {
        let mut s = EncodingStats::NONE;
        s.update_local_deletion_time(500);
        assert_eq!(s.min_local_deletion_time, 500);
        s.update_local_deletion_time(300);
        assert_eq!(s.min_local_deletion_time, 300);
    }

    #[test]
    fn accumulate_ttl() {
        let mut s = EncodingStats::NONE;
        s.update_ttl(60);
        assert_eq!(s.min_ttl, 60);
        s.update_ttl(30);
        assert_eq!(s.min_ttl, 30);
    }

    #[test]
    fn ignore_sentinel_values() {
        let mut s = EncodingStats::new(100, 500, 30);
        s.update_timestamp(NO_TIMESTAMP);
        assert_eq!(s.min_timestamp, 100);
        s.update_local_deletion_time(NO_DELETION_TIME);
        assert_eq!(s.min_local_deletion_time, 500);
        s.update_ttl(NO_TTL);
        assert_eq!(s.min_ttl, 30);
    }

    #[test]
    fn merge() {
        let mut a = EncodingStats::new(200, 600, 60);
        let b = EncodingStats::new(100, 500, 30);
        a.merge_with(&b);
        assert_eq!(a.min_timestamp, 100);
        assert_eq!(a.min_local_deletion_time, 500);
        assert_eq!(a.min_ttl, 30);
    }

    #[test]
    fn merge_with_none() {
        let mut a = EncodingStats::new(100, 500, 30);
        a.merge_with(&EncodingStats::NONE);
        assert_eq!(a.min_timestamp, 100);
        assert_eq!(a.min_local_deletion_time, 500);
        assert_eq!(a.min_ttl, 30);
    }
}
