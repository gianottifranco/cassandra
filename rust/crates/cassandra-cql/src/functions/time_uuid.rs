// Licensed under Apache License, Version 2.0.

//! Time and UUID functions.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.cql3.functions.TimeFcts`
//! - `org.apache.cassandra.cql3.functions.UuidFcts`

use super::registry::{CqlFunction, FunctionRegistry};
use cassandra_types::CqlType;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

/// Register all time/uuid functions into the registry.
pub fn register_all(registry: &FunctionRegistry) {
    registry.register(Arc::new(NowFunction));
    registry.register(Arc::new(CurrentTimestampFunction));
    registry.register(Arc::new(CurrentDateFunction));
    registry.register(Arc::new(CurrentTimeFunction));
    registry.register(Arc::new(UuidFunction));
    registry.register(Arc::new(ToTimestampFromTimeuuid));
    registry.register(Arc::new(ToDateFromTimestamp));
    registry.register(Arc::new(ToUnixTimestampFromTimeuuid));
    registry.register(Arc::new(ToUnixTimestampFromTimestamp));
    registry.register(Arc::new(MinTimeuuidFunction));
    registry.register(Arc::new(MaxTimeuuidFunction));
}

fn current_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

/// UUID v1 epoch offset: Oct 15, 1582 to Jan 1, 1970 in 100ns intervals.
const UUID_EPOCH_OFFSET: u64 = 0x01B2_1DD2_1381_4000;

fn millis_to_uuid_timestamp(millis: i64) -> u64 {
    (millis as u64) * 10_000 + UUID_EPOCH_OFFSET
}

fn make_timeuuid(millis: i64) -> [u8; 16] {
    let ts = millis_to_uuid_timestamp(millis);
    let time_low = (ts & 0xFFFF_FFFF) as u32;
    let time_mid = ((ts >> 32) & 0xFFFF) as u16;
    let time_hi = ((ts >> 48) & 0x0FFF) as u16 | 0x1000; // version 1

    let mut bytes = [0u8; 16];
    bytes[0..4].copy_from_slice(&time_low.to_be_bytes());
    bytes[4..6].copy_from_slice(&time_mid.to_be_bytes());
    bytes[6..8].copy_from_slice(&time_hi.to_be_bytes());
    // clock_seq + node: random for uniqueness
    let rand_bytes: u64 = rand_u64();
    bytes[8] = (rand_bytes >> 56) as u8 & 0x3F | 0x80; // variant
    bytes[9] = (rand_bytes >> 48) as u8;
    bytes[10..16].copy_from_slice(&rand_bytes.to_be_bytes()[2..8]);
    bytes
}

fn rand_u64() -> u64 {
    // Simple random using system time + pointer address for uniqueness
    let t = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    // Mix time with a stack address for entropy
    let stack_val: u64 = 0;
    let addr = &stack_val as *const u64 as u64;
    (t as u64).wrapping_mul(6364136223846793005).wrapping_add(addr)
}

fn timeuuid_to_millis(bytes: &[u8]) -> Option<i64> {
    if bytes.len() != 16 {
        return None;
    }
    let time_low = u32::from_be_bytes(bytes[0..4].try_into().ok()?) as u64;
    let time_mid = u16::from_be_bytes(bytes[4..6].try_into().ok()?) as u64;
    let time_hi = (u16::from_be_bytes(bytes[6..8].try_into().ok()?) & 0x0FFF) as u64;

    let ts = time_low | (time_mid << 32) | (time_hi << 48);
    let millis = (ts.wrapping_sub(UUID_EPOCH_OFFSET)) / 10_000;
    Some(millis as i64)
}

// ── now() → timeuuid ──────────────────────────────────────────────────

struct NowFunction;

impl CqlFunction for NowFunction {
    fn name(&self) -> &str {
        "now"
    }
    fn arg_types(&self) -> Vec<CqlType> {
        vec![]
    }
    fn return_type(&self) -> CqlType {
        CqlType::Timeuuid
    }
    fn execute(&self, _args: &[Option<&[u8]>]) -> Result<Option<Vec<u8>>, String> {
        let millis = current_millis();
        Ok(Some(make_timeuuid(millis).to_vec()))
    }
}

// ── currentTimestamp() → timestamp ────────────────────────────────────

struct CurrentTimestampFunction;

impl CqlFunction for CurrentTimestampFunction {
    fn name(&self) -> &str {
        "currenttimestamp"
    }
    fn arg_types(&self) -> Vec<CqlType> {
        vec![]
    }
    fn return_type(&self) -> CqlType {
        CqlType::Timestamp
    }
    fn execute(&self, _args: &[Option<&[u8]>]) -> Result<Option<Vec<u8>>, String> {
        Ok(Some(current_millis().to_be_bytes().to_vec()))
    }
}

// ── currentDate() → date ─────────────────────────────────────────────

struct CurrentDateFunction;

impl CqlFunction for CurrentDateFunction {
    fn name(&self) -> &str {
        "currentdate"
    }
    fn arg_types(&self) -> Vec<CqlType> {
        vec![]
    }
    fn return_type(&self) -> CqlType {
        CqlType::Date
    }
    fn execute(&self, _args: &[Option<&[u8]>]) -> Result<Option<Vec<u8>>, String> {
        // CQL date: days since epoch (1970-01-01) + 2^31 offset
        let millis = current_millis();
        let days = (millis / 86_400_000) as u32 + (1u32 << 31);
        Ok(Some(days.to_be_bytes().to_vec()))
    }
}

// ── currentTime() → time ─────────────────────────────────────────────

struct CurrentTimeFunction;

impl CqlFunction for CurrentTimeFunction {
    fn name(&self) -> &str {
        "currenttime"
    }
    fn arg_types(&self) -> Vec<CqlType> {
        vec![]
    }
    fn return_type(&self) -> CqlType {
        CqlType::Time
    }
    fn execute(&self, _args: &[Option<&[u8]>]) -> Result<Option<Vec<u8>>, String> {
        // CQL time: nanoseconds since midnight
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        let nanos_today = now.as_nanos() % 86_400_000_000_000;
        Ok(Some((nanos_today as i64).to_be_bytes().to_vec()))
    }
}

// ── uuid() → uuid v4 ─────────────────────────────────────────────────

struct UuidFunction;

impl CqlFunction for UuidFunction {
    fn name(&self) -> &str {
        "uuid"
    }
    fn arg_types(&self) -> Vec<CqlType> {
        vec![]
    }
    fn return_type(&self) -> CqlType {
        CqlType::Uuid
    }
    fn execute(&self, _args: &[Option<&[u8]>]) -> Result<Option<Vec<u8>>, String> {
        let id = uuid::Uuid::new_v4();
        Ok(Some(id.as_bytes().to_vec()))
    }
}

// ── toTimestamp(timeuuid) → timestamp ─────────────────────────────────

struct ToTimestampFromTimeuuid;

impl CqlFunction for ToTimestampFromTimeuuid {
    fn name(&self) -> &str {
        "totimestamp"
    }
    fn arg_types(&self) -> Vec<CqlType> {
        vec![CqlType::Timeuuid]
    }
    fn return_type(&self) -> CqlType {
        CqlType::Timestamp
    }
    fn execute(&self, args: &[Option<&[u8]>]) -> Result<Option<Vec<u8>>, String> {
        match args.first().and_then(|a| *a) {
            Some(bytes) => {
                let millis =
                    timeuuid_to_millis(bytes).ok_or_else(|| "invalid timeuuid".to_string())?;
                Ok(Some(millis.to_be_bytes().to_vec()))
            }
            None => Ok(None),
        }
    }
}

// ── toDate(timestamp) → date ─────────────────────────────────────────

struct ToDateFromTimestamp;

impl CqlFunction for ToDateFromTimestamp {
    fn name(&self) -> &str {
        "todate"
    }
    fn arg_types(&self) -> Vec<CqlType> {
        vec![CqlType::Timestamp]
    }
    fn return_type(&self) -> CqlType {
        CqlType::Date
    }
    fn execute(&self, args: &[Option<&[u8]>]) -> Result<Option<Vec<u8>>, String> {
        match args.first().and_then(|a| *a) {
            Some(bytes) if bytes.len() == 8 => {
                let millis = i64::from_be_bytes(bytes.try_into().unwrap());
                let days = (millis / 86_400_000) as u32 + (1u32 << 31);
                Ok(Some(days.to_be_bytes().to_vec()))
            }
            Some(_) => Err("invalid timestamp bytes".to_string()),
            None => Ok(None),
        }
    }
}

// ── toUnixTimestamp(timeuuid) → bigint ────────────────────────────────

struct ToUnixTimestampFromTimeuuid;

impl CqlFunction for ToUnixTimestampFromTimeuuid {
    fn name(&self) -> &str {
        "tounixtimestamp"
    }
    fn arg_types(&self) -> Vec<CqlType> {
        vec![CqlType::Timeuuid]
    }
    fn return_type(&self) -> CqlType {
        CqlType::Bigint
    }
    fn execute(&self, args: &[Option<&[u8]>]) -> Result<Option<Vec<u8>>, String> {
        match args.first().and_then(|a| *a) {
            Some(bytes) => {
                let millis =
                    timeuuid_to_millis(bytes).ok_or_else(|| "invalid timeuuid".to_string())?;
                Ok(Some(millis.to_be_bytes().to_vec()))
            }
            None => Ok(None),
        }
    }
}

// ── toUnixTimestamp(timestamp) → bigint ───────────────────────────────

struct ToUnixTimestampFromTimestamp;

impl CqlFunction for ToUnixTimestampFromTimestamp {
    fn name(&self) -> &str {
        "tounixtimestamp"
    }
    fn arg_types(&self) -> Vec<CqlType> {
        vec![CqlType::Timestamp]
    }
    fn return_type(&self) -> CqlType {
        CqlType::Bigint
    }
    fn execute(&self, args: &[Option<&[u8]>]) -> Result<Option<Vec<u8>>, String> {
        match args.first().and_then(|a| *a) {
            Some(bytes) if bytes.len() == 8 => Ok(Some(bytes.to_vec())),
            Some(_) => Err("invalid timestamp bytes".to_string()),
            None => Ok(None),
        }
    }
}

// ── minTimeuuid(timestamp) → timeuuid ─────────────────────────────────

struct MinTimeuuidFunction;

impl CqlFunction for MinTimeuuidFunction {
    fn name(&self) -> &str {
        "mintimeuuid"
    }
    fn arg_types(&self) -> Vec<CqlType> {
        vec![CqlType::Timestamp]
    }
    fn return_type(&self) -> CqlType {
        CqlType::Timeuuid
    }
    fn execute(&self, args: &[Option<&[u8]>]) -> Result<Option<Vec<u8>>, String> {
        match args.first().and_then(|a| *a) {
            Some(bytes) if bytes.len() == 8 => {
                let millis = i64::from_be_bytes(bytes.try_into().unwrap());
                let ts = millis_to_uuid_timestamp(millis);
                let time_low = (ts & 0xFFFF_FFFF) as u32;
                let time_mid = ((ts >> 32) & 0xFFFF) as u16;
                let time_hi = ((ts >> 48) & 0x0FFF) as u16 | 0x1000;

                let mut result = [0u8; 16];
                result[0..4].copy_from_slice(&time_low.to_be_bytes());
                result[4..6].copy_from_slice(&time_mid.to_be_bytes());
                result[6..8].copy_from_slice(&time_hi.to_be_bytes());
                result[8] = 0x80; // minimum variant
                // rest stays 0 (minimum)
                Ok(Some(result.to_vec()))
            }
            Some(_) => Err("invalid timestamp bytes".to_string()),
            None => Ok(None),
        }
    }
}

// ── maxTimeuuid(timestamp) → timeuuid ─────────────────────────────────

struct MaxTimeuuidFunction;

impl CqlFunction for MaxTimeuuidFunction {
    fn name(&self) -> &str {
        "maxtimeuuid"
    }
    fn arg_types(&self) -> Vec<CqlType> {
        vec![CqlType::Timestamp]
    }
    fn return_type(&self) -> CqlType {
        CqlType::Timeuuid
    }
    fn execute(&self, args: &[Option<&[u8]>]) -> Result<Option<Vec<u8>>, String> {
        match args.first().and_then(|a| *a) {
            Some(bytes) if bytes.len() == 8 => {
                let millis = i64::from_be_bytes(bytes.try_into().unwrap());
                let ts = millis_to_uuid_timestamp(millis);
                let time_low = (ts & 0xFFFF_FFFF) as u32;
                let time_mid = ((ts >> 32) & 0xFFFF) as u16;
                let time_hi = ((ts >> 48) & 0x0FFF) as u16 | 0x1000;

                let mut result = [0xFFu8; 16];
                result[0..4].copy_from_slice(&time_low.to_be_bytes());
                result[4..6].copy_from_slice(&time_mid.to_be_bytes());
                result[6..8].copy_from_slice(&time_hi.to_be_bytes());
                result[8] = 0xBF; // maximum variant
                Ok(Some(result.to_vec()))
            }
            Some(_) => Err("invalid timestamp bytes".to_string()),
            None => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn now_returns_timeuuid() {
        let f = NowFunction;
        let result = f.execute(&[]).unwrap().unwrap();
        assert_eq!(result.len(), 16);
        // Check version bits (should be 1)
        let version = (result[6] >> 4) & 0x0F;
        assert_eq!(version, 1);
    }

    #[test]
    fn uuid_returns_v4() {
        let f = UuidFunction;
        let result = f.execute(&[]).unwrap().unwrap();
        assert_eq!(result.len(), 16);
        let version = (result[6] >> 4) & 0x0F;
        assert_eq!(version, 4);
    }

    #[test]
    fn current_timestamp_returns_8_bytes() {
        let f = CurrentTimestampFunction;
        let result = f.execute(&[]).unwrap().unwrap();
        assert_eq!(result.len(), 8);
        let millis = i64::from_be_bytes(result.try_into().unwrap());
        assert!(millis > 0);
    }

    #[test]
    fn to_timestamp_roundtrip() {
        let now_f = NowFunction;
        let timeuuid = now_f.execute(&[]).unwrap().unwrap();

        let to_ts = ToTimestampFromTimeuuid;
        let ts = to_ts
            .execute(&[Some(&timeuuid)])
            .unwrap()
            .unwrap();
        let millis = i64::from_be_bytes(ts.try_into().unwrap());
        let actual_millis = current_millis();
        // Should be within 1 second
        assert!((millis - actual_millis).unsigned_abs() < 1000);
    }

    #[test]
    fn min_max_timeuuid() {
        let ts_bytes = 1000i64.to_be_bytes();
        let min_f = MinTimeuuidFunction;
        let max_f = MaxTimeuuidFunction;

        let min_uuid = min_f.execute(&[Some(&ts_bytes)]).unwrap().unwrap();
        let max_uuid = max_f.execute(&[Some(&ts_bytes)]).unwrap().unwrap();

        // Both should be valid timeuuids
        assert_eq!(min_uuid.len(), 16);
        assert_eq!(max_uuid.len(), 16);

        // Min should have minimum clock_seq/node, max should have maximum
        assert!(min_uuid[9..16].iter().all(|&b| b == 0));
        assert!(max_uuid[9..16].iter().all(|&b| b == 0xFF));
    }

    #[test]
    fn to_date_from_timestamp() {
        let f = ToDateFromTimestamp;
        // epoch timestamp = 0 → date should be 2^31
        let ts = 0i64.to_be_bytes();
        let result = f.execute(&[Some(&ts)]).unwrap().unwrap();
        let days = u32::from_be_bytes(result.try_into().unwrap());
        assert_eq!(days, 1u32 << 31);
    }

    #[test]
    fn null_propagation() {
        let f = ToTimestampFromTimeuuid;
        let result = f.execute(&[None]).unwrap();
        assert!(result.is_none());
    }
}
