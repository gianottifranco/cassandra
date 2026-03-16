// Licensed under Apache License, Version 2.0.

//! Row primary-key liveness info.
//!
//! ## Java Oracle
//! `org.apache.cassandra.db.LivenessInfo`

use cassandra_common::timestamp::NO_TIMESTAMP;
use cassandra_common::ttl::{NO_DELETION_TIME, NO_TTL};

/// Liveness information for a row's primary key.
///
/// A row is considered live if its `LivenessInfo` has a real timestamp
/// (not `NO_TIMESTAMP`) and hasn't expired. This is distinct from
/// individual cell liveness.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct LivenessInfo {
    /// Write timestamp in microseconds.
    pub timestamp: i64,
    /// TTL in seconds; `NO_TTL` (0) means no expiration.
    pub ttl: i32,
    /// When this row expires (seconds since epoch); `NO_DELETION_TIME` if no TTL.
    pub local_deletion_time: i32,
}

impl LivenessInfo {
    /// Sentinel for "no liveness" (row has no PK liveness marker).
    pub const EMPTY: Self = Self {
        timestamp: NO_TIMESTAMP,
        ttl: NO_TTL,
        local_deletion_time: NO_DELETION_TIME,
    };

    /// Create a non-expiring liveness info.
    pub const fn create(timestamp: i64) -> Self {
        Self {
            timestamp,
            ttl: NO_TTL,
            local_deletion_time: NO_DELETION_TIME,
        }
    }

    /// Create an expiring liveness info.
    pub const fn expiring(timestamp: i64, ttl: i32, local_deletion_time: i32) -> Self {
        Self {
            timestamp,
            ttl,
            local_deletion_time,
        }
    }

    /// Returns `true` if this is the empty sentinel (no liveness).
    #[inline]
    pub const fn is_empty(&self) -> bool {
        self.timestamp == NO_TIMESTAMP
    }

    /// Returns `true` if this row is live at the given time.
    #[inline]
    pub fn is_live(&self, now_in_seconds: i32) -> bool {
        !self.is_empty() && !self.is_expired(now_in_seconds)
    }

    /// Returns `true` if this liveness info has a TTL set.
    #[inline]
    pub const fn is_expiring(&self) -> bool {
        self.ttl != NO_TTL
    }

    /// Returns `true` if the TTL has expired at the given time.
    #[inline]
    pub fn is_expired(&self, now_in_seconds: i32) -> bool {
        self.is_expiring() && now_in_seconds >= self.local_deletion_time
    }

    /// Returns `true` if this liveness supersedes `other`.
    ///
    /// Higher timestamp wins. On tie, longer TTL wins. On further tie,
    /// later local deletion time wins.
    pub fn supersedes(&self, other: &LivenessInfo) -> bool {
        if self.timestamp != other.timestamp {
            return self.timestamp > other.timestamp;
        }
        if self.is_expiring() != other.is_expiring() {
            // Non-expiring supersedes expiring at same timestamp
            return !self.is_expiring();
        }
        if self.ttl != other.ttl {
            return self.ttl > other.ttl;
        }
        self.local_deletion_time > other.local_deletion_time
    }

    /// Merge two liveness infos, keeping the superseding one.
    pub fn merge(a: LivenessInfo, b: LivenessInfo) -> LivenessInfo {
        if a.is_empty() {
            return b;
        }
        if b.is_empty() {
            return a;
        }
        if a.supersedes(&b) { a } else { b }
    }
}

impl Default for LivenessInfo {
    fn default() -> Self {
        Self::EMPTY
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_is_not_live() {
        assert!(LivenessInfo::EMPTY.is_empty());
        assert!(!LivenessInfo::EMPTY.is_live(0));
    }

    #[test]
    fn non_expiring_is_always_live() {
        let li = LivenessInfo::create(1000);
        assert!(!li.is_empty());
        assert!(li.is_live(0));
        assert!(li.is_live(i32::MAX - 1));
        assert!(!li.is_expiring());
    }

    #[test]
    fn expiring_liveness() {
        let li = LivenessInfo::expiring(1000, 60, 160);
        assert!(li.is_expiring());
        assert!(li.is_live(159)); // before expiry
        assert!(!li.is_live(160)); // at expiry
        assert!(!li.is_live(200)); // after expiry
    }

    #[test]
    fn supersedes_higher_timestamp_wins() {
        let a = LivenessInfo::create(100);
        let b = LivenessInfo::create(200);
        assert!(b.supersedes(&a));
        assert!(!a.supersedes(&b));
    }

    #[test]
    fn supersedes_non_expiring_wins_on_tie() {
        let a = LivenessInfo::create(100);
        let b = LivenessInfo::expiring(100, 60, 160);
        assert!(a.supersedes(&b));
        assert!(!b.supersedes(&a));
    }

    #[test]
    fn merge_keeps_superseding() {
        let a = LivenessInfo::create(100);
        let b = LivenessInfo::create(200);
        assert_eq!(LivenessInfo::merge(a, b), b);
        assert_eq!(LivenessInfo::merge(b, a), b);
    }

    #[test]
    fn merge_with_empty() {
        let a = LivenessInfo::create(100);
        assert_eq!(LivenessInfo::merge(a, LivenessInfo::EMPTY), a);
        assert_eq!(LivenessInfo::merge(LivenessInfo::EMPTY, a), a);
    }

    #[test]
    fn default_is_empty() {
        assert_eq!(LivenessInfo::default(), LivenessInfo::EMPTY);
    }
}
