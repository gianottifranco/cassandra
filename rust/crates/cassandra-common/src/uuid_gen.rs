// Licensed under Apache License, Version 2.0.

//! Time-based UUID helpers inspired by Cassandra's `UUIDGen`.
//!
//! Cassandra uses version-1 UUIDs as `timeuuid` values. This module keeps the
//! conversion logic explicit so timestamp extraction and range-bound UUIDs match
//! the RFC 4122 byte layout used on the wire.

use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use rand::{RngCore, thread_rng};
use uuid::Uuid;

const UUID_EPOCH_OFFSET_100NS: u64 = 0x01B2_1DD2_1381_4000;
const HUNDRED_NANOS_PER_MICRO: u64 = 10;
const HUNDRED_NANOS_PER_MILLI: u64 = 10_000;

static LAST_100NS: AtomicU64 = AtomicU64::new(0);
static NODE_ID: OnceLock<[u8; 6]> = OnceLock::new();
static CLOCK_SEQ: OnceLock<u16> = OnceLock::new();

/// Generate a monotonic version-1 UUID for the current system time.
pub fn time_uuid() -> Uuid {
    time_uuid_from_unix_100ns(monotonic_now_100ns())
}

/// Generate a version-1 UUID for the supplied Unix timestamp in milliseconds.
pub fn time_uuid_from_millis(unix_millis: i64) -> Uuid {
    let unix_100ns = checked_nonnegative(unix_millis)
        .saturating_mul(HUNDRED_NANOS_PER_MILLI)
        .saturating_add(UUID_EPOCH_OFFSET_100NS);
    time_uuid_from_unix_100ns(unix_100ns)
}

/// Generate a version-1 UUID for the supplied Unix timestamp in microseconds.
pub fn time_uuid_from_micros(unix_micros: i64) -> Uuid {
    let unix_100ns = checked_nonnegative(unix_micros)
        .saturating_mul(HUNDRED_NANOS_PER_MICRO)
        .saturating_add(UUID_EPOCH_OFFSET_100NS);
    time_uuid_from_unix_100ns(unix_100ns)
}

/// Return the lower bound UUID for a millisecond timestamp.
///
/// This mirrors Cassandra's min-timeuuid semantics for query bounds.
pub fn min_time_uuid(unix_millis: i64) -> Uuid {
    let timestamp = checked_nonnegative(unix_millis)
        .saturating_mul(HUNDRED_NANOS_PER_MILLI)
        .saturating_add(UUID_EPOCH_OFFSET_100NS);
    build_time_uuid(timestamp, 0, [0; 6])
}

/// Return the upper bound UUID for a millisecond timestamp.
///
/// This uses the last 100ns tick within the millisecond plus maximal clock
/// sequence and node bytes, matching Cassandra's max-timeuuid range behavior.
pub fn max_time_uuid(unix_millis: i64) -> Uuid {
    let timestamp = checked_nonnegative(unix_millis)
        .saturating_mul(HUNDRED_NANOS_PER_MILLI)
        .saturating_add(HUNDRED_NANOS_PER_MILLI - 1)
        .saturating_add(UUID_EPOCH_OFFSET_100NS);
    build_time_uuid(timestamp, 0x3fff, [0xff; 6])
}

/// Return true when the UUID is a version-1/timeuuid value.
pub fn is_time_uuid(uuid: &Uuid) -> bool {
    uuid.as_bytes()[6] >> 4 == 1
}

/// Extract the Unix timestamp in milliseconds from a version-1 UUID.
pub fn unix_timestamp(uuid: &Uuid) -> Option<i64> {
    timestamp_100ns(uuid).map(|timestamp| {
        ((timestamp.saturating_sub(UUID_EPOCH_OFFSET_100NS)) / HUNDRED_NANOS_PER_MILLI) as i64
    })
}

/// Extract the Unix timestamp in microseconds from a version-1 UUID.
pub fn unix_timestamp_micros(uuid: &Uuid) -> Option<i64> {
    timestamp_100ns(uuid).map(|timestamp| {
        ((timestamp.saturating_sub(UUID_EPOCH_OFFSET_100NS)) / HUNDRED_NANOS_PER_MICRO) as i64
    })
}

/// Return the raw 16-byte representation for a newly generated timeuuid.
pub fn time_uuid_bytes() -> [u8; 16] {
    *time_uuid().as_bytes()
}

/// Return the raw 16-byte representation for a millisecond timestamp UUID.
pub fn time_uuid_bytes_from_millis(unix_millis: i64) -> [u8; 16] {
    *time_uuid_from_millis(unix_millis).as_bytes()
}

fn time_uuid_from_unix_100ns(timestamp: u64) -> Uuid {
    build_time_uuid(timestamp, clock_seq(), node_id())
}

fn build_time_uuid(timestamp_100ns: u64, clock_seq: u16, node: [u8; 6]) -> Uuid {
    let time_low = timestamp_100ns as u32;
    let time_mid = (timestamp_100ns >> 32) as u16;
    let time_hi = ((timestamp_100ns >> 48) as u16 & 0x0fff) | 0x1000;
    let clock_seq = clock_seq & 0x3fff;

    let mut bytes = [0u8; 16];
    bytes[0..4].copy_from_slice(&time_low.to_be_bytes());
    bytes[4..6].copy_from_slice(&time_mid.to_be_bytes());
    bytes[6..8].copy_from_slice(&time_hi.to_be_bytes());
    bytes[8] = 0x80 | ((clock_seq >> 8) as u8 & 0x3f);
    bytes[9] = clock_seq as u8;
    bytes[10..16].copy_from_slice(&node);
    Uuid::from_bytes(bytes)
}

fn timestamp_100ns(uuid: &Uuid) -> Option<u64> {
    if !is_time_uuid(uuid) {
        return None;
    }

    let bytes = uuid.as_bytes();
    let time_low = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as u64;
    let time_mid = u16::from_be_bytes([bytes[4], bytes[5]]) as u64;
    let time_hi = (u16::from_be_bytes([bytes[6], bytes[7]]) & 0x0fff) as u64;
    Some((time_hi << 48) | (time_mid << 32) | time_low)
}

fn monotonic_now_100ns() -> u64 {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let unix_100ns = now
        .as_secs()
        .saturating_mul(10_000_000)
        .saturating_add((now.subsec_nanos() / 100) as u64)
        .saturating_add(UUID_EPOCH_OFFSET_100NS);

    let mut observed = LAST_100NS.load(Ordering::Relaxed);
    loop {
        let candidate = unix_100ns.max(observed.saturating_add(1));
        match LAST_100NS.compare_exchange_weak(
            observed,
            candidate,
            Ordering::SeqCst,
            Ordering::Relaxed,
        ) {
            Ok(_) => return candidate,
            Err(current) => observed = current,
        }
    }
}

fn node_id() -> [u8; 6] {
    *NODE_ID.get_or_init(|| {
        let mut node = [0u8; 6];
        thread_rng().fill_bytes(&mut node);
        node[0] |= 0x01;
        node
    })
}

fn clock_seq() -> u16 {
    *CLOCK_SEQ.get_or_init(|| (thread_rng().next_u32() as u16) & 0x3fff)
}

fn checked_nonnegative(value: i64) -> u64 {
    u64::try_from(value).unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_uuid_is_version_one_and_monotonic() {
        let first = time_uuid();
        let second = time_uuid();

        assert!(is_time_uuid(&first));
        assert!(is_time_uuid(&second));
        assert!(unix_timestamp_micros(&second) >= unix_timestamp_micros(&first));
    }

    #[test]
    fn millis_roundtrip_preserves_timestamp() {
        let uuid = time_uuid_from_millis(1_700_000_000_123);
        assert_eq!(unix_timestamp(&uuid), Some(1_700_000_000_123));
        assert_eq!(uuid.as_bytes()[6] >> 4, 1);
        assert_eq!(uuid.as_bytes()[8] & 0xc0, 0x80);
    }

    #[test]
    fn micros_roundtrip_preserves_timestamp() {
        let uuid = time_uuid_from_micros(1_700_000_000_123_456);
        assert_eq!(unix_timestamp_micros(&uuid), Some(1_700_000_000_123_456));
        assert_eq!(unix_timestamp(&uuid), Some(1_700_000_000_123));
    }

    #[test]
    fn min_and_max_time_uuid_bound_same_millisecond() {
        let millis = 1_700_000_000_123;
        let min = min_time_uuid(millis);
        let max = max_time_uuid(millis);

        assert_eq!(unix_timestamp(&min), Some(millis));
        assert_eq!(unix_timestamp(&max), Some(millis));
        assert!(min.as_bytes() < max.as_bytes());
        assert_eq!(min.as_bytes()[8] & 0x3f, 0);
        assert_eq!(min.as_bytes()[9], 0);
        assert_eq!(max.as_bytes()[8] & 0x3f, 0x3f);
        assert_eq!(max.as_bytes()[9], 0xff);
    }

    #[test]
    fn non_time_uuid_has_no_timestamp() {
        let uuid = Uuid::new_v4();
        assert!(!is_time_uuid(&uuid));
        assert_eq!(unix_timestamp(&uuid), None);
        assert_eq!(unix_timestamp_micros(&uuid), None);
    }

    #[test]
    fn byte_helpers_return_uuid_wire_format() {
        let bytes = time_uuid_bytes_from_millis(42);
        let uuid = Uuid::from_bytes(bytes);
        assert_eq!(unix_timestamp(&uuid), Some(42));
        assert_eq!(time_uuid_bytes().len(), 16);
    }
}
