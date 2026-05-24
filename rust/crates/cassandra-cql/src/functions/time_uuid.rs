// Licensed under Apache License, Version 2.0.

//! Time and UUID functions.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.cql3.functions.TimeFcts`
//! - `org.apache.cassandra.cql3.functions.UuidFcts`

use super::registry::{CqlFunction, FunctionRegistry};
use cassandra_types::{CqlType, CqlValue};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

/// Register all time/uuid functions into the registry.
pub fn register_all(registry: &FunctionRegistry) {
    registry.register(Arc::new(NowFunction));
    registry.register(Arc::new(CurrentTimeuuidFunction {
        name: "current_timeuuid",
    }));
    registry.register(Arc::new(CurrentTimeuuidFunction {
        name: "currenttimeuuid",
    }));
    registry.register(Arc::new(CurrentTimestampFunction));
    registry.register(Arc::new(CurrentTimestampSnakeFunction));
    registry.register(Arc::new(CurrentDateFunction));
    registry.register(Arc::new(CurrentDateSnakeFunction));
    registry.register(Arc::new(CurrentTimeFunction));
    registry.register(Arc::new(CurrentTimeSnakeFunction));
    registry.register(Arc::new(UuidFunction));
    registry.register(Arc::new(ToTimestampFromTimeuuid));
    registry.register(Arc::new(TemporalConversionFunction {
        name: "to_timestamp",
        from: CqlType::Timeuuid,
        to: CqlType::Timestamp,
    }));
    registry.register(Arc::new(TemporalConversionFunction {
        name: "to_timestamp",
        from: CqlType::Date,
        to: CqlType::Timestamp,
    }));
    registry.register(Arc::new(TemporalConversionFunction {
        name: "totimestamp",
        from: CqlType::Date,
        to: CqlType::Timestamp,
    }));
    registry.register(Arc::new(ToDateFromTimestamp));
    registry.register(Arc::new(TemporalConversionFunction {
        name: "to_date",
        from: CqlType::Timestamp,
        to: CqlType::Date,
    }));
    registry.register(Arc::new(TemporalConversionFunction {
        name: "to_date",
        from: CqlType::Timeuuid,
        to: CqlType::Date,
    }));
    registry.register(Arc::new(TemporalConversionFunction {
        name: "todate",
        from: CqlType::Timeuuid,
        to: CqlType::Date,
    }));
    registry.register(Arc::new(ToUnixTimestampFromTimeuuid));
    registry.register(Arc::new(TemporalConversionFunction {
        name: "to_unix_timestamp",
        from: CqlType::Timeuuid,
        to: CqlType::Bigint,
    }));
    registry.register(Arc::new(ToUnixTimestampFromTimestamp));
    registry.register(Arc::new(TemporalConversionFunction {
        name: "to_unix_timestamp",
        from: CqlType::Timestamp,
        to: CqlType::Bigint,
    }));
    registry.register(Arc::new(TemporalConversionFunction {
        name: "to_unix_timestamp",
        from: CqlType::Date,
        to: CqlType::Bigint,
    }));
    registry.register(Arc::new(TemporalConversionFunction {
        name: "tounixtimestamp",
        from: CqlType::Date,
        to: CqlType::Bigint,
    }));
    registry.register(Arc::new(MinTimeuuidFunction));
    registry.register(Arc::new(TimeuuidBoundaryFunction {
        name: "min_timeuuid",
        maximum: false,
    }));
    registry.register(Arc::new(MaxTimeuuidFunction));
    registry.register(Arc::new(TimeuuidBoundaryFunction {
        name: "max_timeuuid",
        maximum: true,
    }));
    register_floor_functions(registry);
}

fn current_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

/// UUID v1 epoch offset: Oct 15, 1582 to Jan 1, 1970 in 100ns intervals.
const UUID_EPOCH_OFFSET: u64 = 0x01B2_1DD2_1381_4000;
const MILLIS_PER_DAY: i64 = 86_400_000;
const NANOS_PER_MILLI: i64 = 1_000_000;
const NANOS_PER_DAY: i64 = 86_400_000_000_000;

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
    (t as u64)
        .wrapping_mul(6364136223846793005)
        .wrapping_add(addr)
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

fn millis_to_date_bytes(millis: i64) -> Vec<u8> {
    let days = (millis / MILLIS_PER_DAY) as i32;
    days.wrapping_sub(i32::MIN).to_be_bytes().to_vec()
}

fn date_bytes_to_millis(bytes: &[u8]) -> Result<i64, String> {
    if bytes.len() != 4 {
        return Err("invalid date bytes".to_string());
    }
    let encoded_days = i32::from_be_bytes(bytes.try_into().unwrap());
    let days = encoded_days.wrapping_add(i32::MIN) as i64;
    Ok(days * MILLIS_PER_DAY)
}

fn add_months_utc(millis: i64, months: i32) -> Result<i64, String> {
    let date_time = UtcDateTime::from_millis(millis)?;
    let month_index = date_time
        .year
        .checked_mul(12)
        .and_then(|v| v.checked_add(date_time.month as i64 - 1))
        .and_then(|v| v.checked_add(months as i64))
        .ok_or_else(|| "month arithmetic overflow".to_string())?;
    let year = month_index.div_euclid(12);
    let month = month_index.rem_euclid(12) as u32 + 1;
    let day = date_time.day.min(days_in_month(year, month));
    UtcDateTime {
        year,
        month,
        day,
        millis_of_day: date_time.millis_of_day,
    }
    .to_millis()
}

struct UtcDateTime {
    year: i64,
    month: u32,
    day: u32,
    millis_of_day: i64,
}

impl UtcDateTime {
    fn from_millis(millis: i64) -> Result<Self, String> {
        let days = millis.div_euclid(MILLIS_PER_DAY);
        let millis_of_day = millis.rem_euclid(MILLIS_PER_DAY);
        let (year, month, day) = civil_from_days(days)?;
        Ok(Self {
            year,
            month,
            day,
            millis_of_day,
        })
    }

    fn to_millis(&self) -> Result<i64, String> {
        days_from_civil(self.year, self.month, self.day)?
            .checked_mul(MILLIS_PER_DAY)
            .and_then(|v| v.checked_add(self.millis_of_day))
            .ok_or_else(|| "datetime overflow".to_string())
    }
}

fn days_from_civil(year: i64, month: u32, day: u32) -> Result<i64, String> {
    if !(1..=12).contains(&month) || day == 0 || day > days_in_month(year, month) {
        return Err("invalid UTC date".to_string());
    }
    let year = year - i64::from(month <= 2);
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let yoe = year - era * 400;
    let month = month as i64;
    let day = day as i64;
    let mp = month + if month > 2 { -3 } else { 9 };
    let doy = (153 * mp + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    Ok(era * 146_097 + doe - 719_468)
}

fn civil_from_days(days: i64) -> Result<(i64, u32, u32), String> {
    let days = days
        .checked_add(719_468)
        .ok_or_else(|| "date conversion overflow".to_string())?;
    let era = if days >= 0 { days } else { days - 146_096 } / 146_097;
    let doe = days - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let mut year = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = mp + if mp < 10 { 3 } else { -9 };
    year += i64::from(month <= 2);
    Ok((year, month as u32, day as u32))
}

fn days_in_month(year: i64, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap_year(year) => 29,
        2 => 28,
        _ => 0,
    }
}

fn is_leap_year(year: i64) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
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

struct CurrentTimeuuidFunction {
    name: &'static str,
}

impl CqlFunction for CurrentTimeuuidFunction {
    fn name(&self) -> &str {
        self.name
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

struct CurrentTimestampSnakeFunction;

impl CqlFunction for CurrentTimestampSnakeFunction {
    fn name(&self) -> &str {
        "current_timestamp"
    }
    fn arg_types(&self) -> Vec<CqlType> {
        vec![]
    }
    fn return_type(&self) -> CqlType {
        CqlType::Timestamp
    }
    fn execute(&self, args: &[Option<&[u8]>]) -> Result<Option<Vec<u8>>, String> {
        CurrentTimestampFunction.execute(args)
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
        Ok(Some(millis_to_date_bytes(millis)))
    }
}

struct CurrentDateSnakeFunction;

impl CqlFunction for CurrentDateSnakeFunction {
    fn name(&self) -> &str {
        "current_date"
    }
    fn arg_types(&self) -> Vec<CqlType> {
        vec![]
    }
    fn return_type(&self) -> CqlType {
        CqlType::Date
    }
    fn execute(&self, args: &[Option<&[u8]>]) -> Result<Option<Vec<u8>>, String> {
        CurrentDateFunction.execute(args)
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

struct CurrentTimeSnakeFunction;

impl CqlFunction for CurrentTimeSnakeFunction {
    fn name(&self) -> &str {
        "current_time"
    }
    fn arg_types(&self) -> Vec<CqlType> {
        vec![]
    }
    fn return_type(&self) -> CqlType {
        CqlType::Time
    }
    fn execute(&self, args: &[Option<&[u8]>]) -> Result<Option<Vec<u8>>, String> {
        CurrentTimeFunction.execute(args)
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

struct TemporalConversionFunction {
    name: &'static str,
    from: CqlType,
    to: CqlType,
}

impl CqlFunction for TemporalConversionFunction {
    fn name(&self) -> &str {
        self.name
    }
    fn arg_types(&self) -> Vec<CqlType> {
        vec![self.from.clone()]
    }
    fn return_type(&self) -> CqlType {
        self.to.clone()
    }
    fn execute(&self, args: &[Option<&[u8]>]) -> Result<Option<Vec<u8>>, String> {
        let Some(bytes) = args.first().and_then(|a| *a) else {
            return Ok(None);
        };

        match (&self.from, &self.to) {
            (CqlType::Timeuuid, CqlType::Timestamp) | (CqlType::Timeuuid, CqlType::Bigint) => {
                let millis =
                    timeuuid_to_millis(bytes).ok_or_else(|| "invalid timeuuid".to_string())?;
                Ok(Some(millis.to_be_bytes().to_vec()))
            }
            (CqlType::Timeuuid, CqlType::Date) => {
                let millis =
                    timeuuid_to_millis(bytes).ok_or_else(|| "invalid timeuuid".to_string())?;
                Ok(Some(millis_to_date_bytes(millis)))
            }
            (CqlType::Timestamp, CqlType::Date) => {
                if bytes.len() != 8 {
                    return Err("invalid timestamp bytes".to_string());
                }
                let millis = i64::from_be_bytes(bytes.try_into().unwrap());
                Ok(Some(millis_to_date_bytes(millis)))
            }
            (CqlType::Timestamp, CqlType::Bigint) => {
                if bytes.len() != 8 {
                    return Err("invalid timestamp bytes".to_string());
                }
                Ok(Some(bytes.to_vec()))
            }
            (CqlType::Date, CqlType::Timestamp) | (CqlType::Date, CqlType::Bigint) => {
                Ok(Some(date_bytes_to_millis(bytes)?.to_be_bytes().to_vec()))
            }
            _ => Err(format!(
                "unsupported temporal conversion from {} to {}",
                self.from.cql_name(),
                self.to.cql_name()
            )),
        }
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
                Ok(Some(millis_to_date_bytes(millis)))
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

struct TimeuuidBoundaryFunction {
    name: &'static str,
    maximum: bool,
}

impl CqlFunction for TimeuuidBoundaryFunction {
    fn name(&self) -> &str {
        self.name
    }
    fn arg_types(&self) -> Vec<CqlType> {
        vec![CqlType::Timestamp]
    }
    fn return_type(&self) -> CqlType {
        CqlType::Timeuuid
    }
    fn execute(&self, args: &[Option<&[u8]>]) -> Result<Option<Vec<u8>>, String> {
        if self.maximum {
            MaxTimeuuidFunction.execute(args)
        } else {
            MinTimeuuidFunction.execute(args)
        }
    }
}

struct FloorFunction {
    input: CqlType,
    start: Option<CqlType>,
    output: CqlType,
}

fn register_floor_functions(registry: &FunctionRegistry) {
    for (input, output, start) in [
        (CqlType::Timestamp, CqlType::Timestamp, None),
        (
            CqlType::Timestamp,
            CqlType::Timestamp,
            Some(CqlType::Timestamp),
        ),
        (CqlType::Timeuuid, CqlType::Timestamp, None),
        (
            CqlType::Timeuuid,
            CqlType::Timestamp,
            Some(CqlType::Timestamp),
        ),
        (CqlType::Date, CqlType::Date, None),
        (CqlType::Date, CqlType::Date, Some(CqlType::Date)),
        (CqlType::Time, CqlType::Time, None),
    ] {
        registry.register(Arc::new(FloorFunction {
            input: input.clone(),
            start,
            output,
        }));
    }
}

impl CqlFunction for FloorFunction {
    fn name(&self) -> &str {
        "floor"
    }

    fn arg_types(&self) -> Vec<CqlType> {
        let mut args = vec![self.input.clone(), CqlType::Duration];
        if let Some(start) = &self.start {
            args.push(start.clone());
        }
        args
    }

    fn return_type(&self) -> CqlType {
        self.output.clone()
    }

    fn execute(&self, args: &[Option<&[u8]>]) -> Result<Option<Vec<u8>>, String> {
        if args.iter().any(|arg| arg.is_none()) {
            return Ok(None);
        }
        let value = args.first().and_then(|arg| *arg).unwrap();
        let duration = decode_duration(args.get(1).and_then(|arg| *arg).unwrap())?;

        match &self.input {
            CqlType::Timestamp => {
                let time = read_i64(value, "timestamp")?;
                let start = match (self.start.as_ref(), args.get(2).and_then(|arg| *arg)) {
                    (Some(CqlType::Timestamp), Some(bytes)) => read_i64(bytes, "timestamp")?,
                    _ => 0,
                };
                Ok(Some(
                    floor_timestamp(time, duration, start)?
                        .to_be_bytes()
                        .to_vec(),
                ))
            }
            CqlType::Timeuuid => {
                let time =
                    timeuuid_to_millis(value).ok_or_else(|| "invalid timeuuid".to_string())?;
                let start = match (self.start.as_ref(), args.get(2).and_then(|arg| *arg)) {
                    (Some(CqlType::Timestamp), Some(bytes)) => read_i64(bytes, "timestamp")?,
                    _ => 0,
                };
                Ok(Some(
                    floor_timestamp(time, duration, start)?
                        .to_be_bytes()
                        .to_vec(),
                ))
            }
            CqlType::Date => {
                let time = date_bytes_to_millis(value)?;
                let start = match (self.start.as_ref(), args.get(2).and_then(|arg| *arg)) {
                    (Some(CqlType::Date), Some(bytes)) => date_bytes_to_millis(bytes)?,
                    _ => 0,
                };
                let floor = floor_date(time, duration, start)?;
                Ok(Some(millis_to_date_bytes(floor)))
            }
            CqlType::Time => {
                let time = read_i64(value, "time")?;
                Ok(Some(floor_time(time, duration)?.to_be_bytes().to_vec()))
            }
            _ => Err(format!("unsupported floor input {}", self.input.cql_name())),
        }
    }
}

#[derive(Clone, Copy)]
struct DurationParts {
    months: i32,
    days: i32,
    nanoseconds: i64,
}

fn decode_duration(bytes: &[u8]) -> Result<DurationParts, String> {
    match CqlValue::deserialize_value(&CqlType::Duration, bytes) {
        Ok(CqlValue::Duration {
            months,
            days,
            nanoseconds,
        }) => Ok(DurationParts {
            months,
            days,
            nanoseconds,
        }),
        Ok(_) => Err("duration decoded to unexpected value".to_string()),
        Err(err) => Err(format!("invalid duration bytes: {err}")),
    }
}

fn read_i64(bytes: &[u8], name: &str) -> Result<i64, String> {
    if bytes.len() != 8 {
        return Err(format!("invalid {name} bytes"));
    }
    Ok(i64::from_be_bytes(bytes.try_into().unwrap()))
}

fn floor_timestamp(time: i64, duration: DurationParts, start: i64) -> Result<i64, String> {
    validate_non_negative_duration(duration)?;
    if start > time {
        return Err("floor function starting time is greater than the provided time".to_string());
    }
    if duration.nanoseconds % NANOS_PER_MILLI != 0 {
        return Err(
            "floor cannot be computed for durations with precision below 1 millisecond".to_string(),
        );
    }
    let duration_millis = duration_millis_without_months(duration)?;
    if duration.months != 0 {
        let time_date = UtcDateTime::from_millis(time)?;
        let start_date = UtcDateTime::from_millis(start)?;
        let duration_in_months = (time_date.year - start_date.year)
            .checked_mul(12)
            .and_then(|v| v.checked_add(time_date.month as i64 - start_date.month as i64))
            .ok_or_else(|| "duration month calculation overflow".to_string())?;
        let mut multiplier = duration_in_months / duration.months as i64;
        let mut calendar_floor = add_months_utc(
            start,
            multiplier
                .checked_mul(duration.months as i64)
                .and_then(|v| i32::try_from(v).ok())
                .ok_or_else(|| "duration month multiplier overflow".to_string())?,
        )?;
        if duration.days == 0 && duration.nanoseconds == 0 {
            return Ok(calendar_floor);
        }
        let mut floor = calendar_floor
            .checked_add(
                multiplier
                    .checked_mul(duration_millis)
                    .ok_or_else(|| "duration multiplier overflow".to_string())?,
            )
            .ok_or_else(|| "duration floor overflow".to_string())?;
        while floor > time {
            multiplier -= 1;
            calendar_floor = add_months_utc(
                start,
                multiplier
                    .checked_mul(duration.months as i64)
                    .and_then(|v| i32::try_from(v).ok())
                    .ok_or_else(|| "duration month multiplier overflow".to_string())?,
            )?;
            floor = calendar_floor
                .checked_add(
                    multiplier
                        .checked_mul(duration_millis)
                        .ok_or_else(|| "duration multiplier overflow".to_string())?,
                )
                .ok_or_else(|| "duration floor overflow".to_string())?;
        }
        return Ok(floor.max(start));
    }
    if duration_millis == 0 {
        return Ok(time);
    }
    let delta = (time - start) % duration_millis;
    Ok(time - delta)
}

fn floor_date(time: i64, duration: DurationParts, start: i64) -> Result<i64, String> {
    validate_non_negative_duration(duration)?;
    if duration.nanoseconds != 0 {
        return Err("floor on date values requires day precision".to_string());
    }
    floor_timestamp(time, duration, start)
}

fn floor_time(time: i64, duration: DurationParts) -> Result<i64, String> {
    validate_non_negative_duration(duration)?;
    if duration.months != 0 || duration.days != 0 || duration.nanoseconds > NANOS_PER_DAY {
        return Err(
            "floor on time values requires a duration smaller than or equal to a day".to_string(),
        );
    }
    if duration.nanoseconds == 0 {
        return Ok(time);
    }
    let delta = time % duration.nanoseconds;
    Ok(time - delta)
}

fn duration_millis_without_months(duration: DurationParts) -> Result<i64, String> {
    let days = (duration.days as i64)
        .checked_mul(MILLIS_PER_DAY)
        .ok_or_else(|| "duration days overflow milliseconds".to_string())?;
    days.checked_add(duration.nanoseconds / NANOS_PER_MILLI)
        .ok_or_else(|| "duration overflow milliseconds".to_string())
}

fn validate_non_negative_duration(duration: DurationParts) -> Result<(), String> {
    if duration.months < 0 || duration.days < 0 || duration.nanoseconds < 0 {
        Err("negative durations are not supported by the floor function".to_string())
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cassandra_types::vint::encode_vint;

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
    fn builtins_resolve_current_time_java_names() {
        let registry = FunctionRegistry::with_builtins();

        for (name, return_type, len) in [
            ("current_timeuuid", CqlType::Timeuuid, 16usize),
            ("currenttimeuuid", CqlType::Timeuuid, 16),
            ("current_timestamp", CqlType::Timestamp, 8),
            ("currenttimestamp", CqlType::Timestamp, 8),
            ("current_date", CqlType::Date, 4),
            ("currentdate", CqlType::Date, 4),
            ("current_time", CqlType::Time, 8),
            ("currenttime", CqlType::Time, 8),
        ] {
            let f = registry.resolve(name, &[]).unwrap();
            assert_eq!(f.return_type(), return_type);
            assert_eq!(f.execute(&[]).unwrap().unwrap().len(), len);
        }
    }

    #[test]
    fn to_timestamp_roundtrip() {
        let now_f = NowFunction;
        let timeuuid = now_f.execute(&[]).unwrap().unwrap();

        let to_ts = ToTimestampFromTimeuuid;
        let ts = to_ts.execute(&[Some(&timeuuid)]).unwrap().unwrap();
        let millis = i64::from_be_bytes(ts.try_into().unwrap());
        let actual_millis = current_millis();
        // Should be within 1 second
        assert!((millis - actual_millis).unsigned_abs() < 1000);
    }

    #[test]
    fn builtins_resolve_time_conversion_java_names() {
        let registry = FunctionRegistry::with_builtins();
        let timeuuid = test_timeuuid_for_millis(123_456_789);

        for name in ["to_timestamp", "totimestamp"] {
            let f = registry.resolve(name, &[CqlType::Timeuuid]).unwrap();
            assert_eq!(f.return_type(), CqlType::Timestamp);
            let result = f.execute(&[Some(&timeuuid)]).unwrap().unwrap();
            assert_eq!(i64::from_be_bytes(result.try_into().unwrap()), 123_456_789);
        }

        for name in ["to_date", "todate"] {
            let f = registry.resolve(name, &[CqlType::Timeuuid]).unwrap();
            assert_eq!(f.return_type(), CqlType::Date);
            let result = f.execute(&[Some(&timeuuid)]).unwrap().unwrap();
            assert_eq!(
                u32::from_be_bytes(result.try_into().unwrap()),
                (1u32 << 31) + 1
            );
        }

        for name in ["to_unix_timestamp", "tounixtimestamp"] {
            let f = registry.resolve(name, &[CqlType::Timeuuid]).unwrap();
            assert_eq!(f.return_type(), CqlType::Bigint);
            let result = f.execute(&[Some(&timeuuid)]).unwrap().unwrap();
            assert_eq!(i64::from_be_bytes(result.try_into().unwrap()), 123_456_789);
        }
    }

    #[test]
    fn builtins_resolve_date_temporal_conversions() {
        let registry = FunctionRegistry::with_builtins();
        let date = ((1u32 << 31) + 2).to_be_bytes();

        for name in ["to_timestamp", "totimestamp"] {
            let f = registry.resolve(name, &[CqlType::Date]).unwrap();
            assert_eq!(f.return_type(), CqlType::Timestamp);
            let result = f.execute(&[Some(&date)]).unwrap().unwrap();
            assert_eq!(
                i64::from_be_bytes(result.try_into().unwrap()),
                2 * MILLIS_PER_DAY
            );
        }

        for name in ["to_unix_timestamp", "tounixtimestamp"] {
            let f = registry.resolve(name, &[CqlType::Date]).unwrap();
            assert_eq!(f.return_type(), CqlType::Bigint);
            let result = f.execute(&[Some(&date)]).unwrap().unwrap();
            assert_eq!(
                i64::from_be_bytes(result.try_into().unwrap()),
                2 * MILLIS_PER_DAY
            );
        }
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
    fn builtins_resolve_min_max_timeuuid_java_names() {
        let registry = FunctionRegistry::with_builtins();
        let ts_bytes = 1000i64.to_be_bytes();

        for name in ["min_timeuuid", "mintimeuuid"] {
            let f = registry.resolve(name, &[CqlType::Timestamp]).unwrap();
            assert_eq!(f.return_type(), CqlType::Timeuuid);
            let result = f.execute(&[Some(&ts_bytes)]).unwrap().unwrap();
            assert_eq!(result.len(), 16);
            assert!(result[9..16].iter().all(|&b| b == 0));
        }

        for name in ["max_timeuuid", "maxtimeuuid"] {
            let f = registry.resolve(name, &[CqlType::Timestamp]).unwrap();
            assert_eq!(f.return_type(), CqlType::Timeuuid);
            let result = f.execute(&[Some(&ts_bytes)]).unwrap().unwrap();
            assert_eq!(result.len(), 16);
            assert!(result[9..16].iter().all(|&b| b == 0xFF));
        }
    }

    #[test]
    fn floor_timestamp_duration_overloads_match_java_non_month_duration() {
        let registry = FunctionRegistry::with_builtins();
        let duration = duration_bytes(0, 0, 10_000_000_000);
        let timestamp = 12_345i64.to_be_bytes();

        let f = registry
            .resolve_with_return(
                "floor",
                &[CqlType::Timestamp, CqlType::Duration],
                &CqlType::Timestamp,
            )
            .unwrap();
        let result = f
            .execute(&[Some(&timestamp), Some(&duration)])
            .unwrap()
            .unwrap();
        assert_eq!(i64::from_be_bytes(result.try_into().unwrap()), 10_000);

        let start = 3_000i64.to_be_bytes();
        let f = registry
            .resolve_with_return(
                "floor",
                &[CqlType::Timestamp, CqlType::Duration, CqlType::Timestamp],
                &CqlType::Timestamp,
            )
            .unwrap();
        let result = f
            .execute(&[Some(&timestamp), Some(&duration), Some(&start)])
            .unwrap()
            .unwrap();
        assert_eq!(i64::from_be_bytes(result.try_into().unwrap()), 3_000);
    }

    #[test]
    fn floor_timeuuid_returns_timestamp_like_java() {
        let registry = FunctionRegistry::with_builtins();
        let duration = duration_bytes(0, 0, 10_000_000_000);
        let timeuuid = test_timeuuid_for_millis(12_345);

        let f = registry
            .resolve_with_return(
                "floor",
                &[CqlType::Timeuuid, CqlType::Duration],
                &CqlType::Timestamp,
            )
            .unwrap();
        let result = f
            .execute(&[Some(&timeuuid), Some(&duration)])
            .unwrap()
            .unwrap();
        assert_eq!(i64::from_be_bytes(result.try_into().unwrap()), 10_000);
    }

    #[test]
    fn floor_date_and_time_overloads_match_java_non_month_duration() {
        let registry = FunctionRegistry::with_builtins();
        let day_duration = duration_bytes(0, 2, 0);
        let date = ((1u32 << 31) + 5).to_be_bytes();

        let f = registry
            .resolve_with_return("floor", &[CqlType::Date, CqlType::Duration], &CqlType::Date)
            .unwrap();
        let result = f
            .execute(&[Some(&date), Some(&day_duration)])
            .unwrap()
            .unwrap();
        assert_eq!(
            u32::from_be_bytes(result.try_into().unwrap()),
            (1u32 << 31) + 4
        );

        let time_duration = duration_bytes(0, 0, 1_000_000_000);
        let time = 12_345_678_901i64.to_be_bytes();
        let f = registry
            .resolve_with_return("floor", &[CqlType::Time, CqlType::Duration], &CqlType::Time)
            .unwrap();
        let result = f
            .execute(&[Some(&time), Some(&time_duration)])
            .unwrap()
            .unwrap();
        assert_eq!(
            i64::from_be_bytes(result.try_into().unwrap()),
            12_000_000_000
        );
    }

    #[test]
    fn floor_timestamp_month_duration_matches_java_calendar_strategy() {
        let registry = FunctionRegistry::with_builtins();
        let duration = duration_bytes(1, 0, 0);
        let timestamp = utc_millis(2024, 5, 20, 0).to_be_bytes();
        let start = utc_millis(2024, 1, 15, 0).to_be_bytes();

        let f = registry
            .resolve_with_return(
                "floor",
                &[CqlType::Timestamp, CqlType::Duration, CqlType::Timestamp],
                &CqlType::Timestamp,
            )
            .unwrap();
        let result = f
            .execute(&[Some(&timestamp), Some(&duration), Some(&start)])
            .unwrap()
            .unwrap();
        assert_eq!(
            i64::from_be_bytes(result.try_into().unwrap()),
            utc_millis(2024, 5, 15, 0)
        );
    }

    #[test]
    fn floor_timestamp_month_day_duration_backs_off_when_estimate_overshoots() {
        let registry = FunctionRegistry::with_builtins();
        let duration = duration_bytes(1, 10, 0);
        let timestamp = utc_millis(2024, 4, 20, 0).to_be_bytes();
        let start = utc_millis(2024, 1, 1, 0).to_be_bytes();

        let f = registry
            .resolve_with_return(
                "floor",
                &[CqlType::Timestamp, CqlType::Duration, CqlType::Timestamp],
                &CqlType::Timestamp,
            )
            .unwrap();
        let result = f
            .execute(&[Some(&timestamp), Some(&duration), Some(&start)])
            .unwrap()
            .unwrap();
        assert_eq!(
            i64::from_be_bytes(result.try_into().unwrap()),
            utc_millis(2024, 3, 21, 0)
        );
    }

    #[test]
    fn floor_date_month_duration_uses_utc_calendar_months() {
        let registry = FunctionRegistry::with_builtins();
        let duration = duration_bytes(1, 0, 0);
        let date = millis_to_date_bytes(utc_millis(2024, 5, 20, 0));

        let f = registry
            .resolve_with_return("floor", &[CqlType::Date, CqlType::Duration], &CqlType::Date)
            .unwrap();
        let result = f.execute(&[Some(&date), Some(&duration)]).unwrap().unwrap();
        assert_eq!(result, millis_to_date_bytes(utc_millis(2024, 5, 1, 0)));
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

    fn test_timeuuid_for_millis(millis: i64) -> Vec<u8> {
        let ts = millis_to_uuid_timestamp(millis);
        let time_low = (ts & 0xFFFF_FFFF) as u32;
        let time_mid = ((ts >> 32) & 0xFFFF) as u16;
        let time_hi = ((ts >> 48) & 0x0FFF) as u16 | 0x1000;

        let mut result = [0u8; 16];
        result[0..4].copy_from_slice(&time_low.to_be_bytes());
        result[4..6].copy_from_slice(&time_mid.to_be_bytes());
        result[6..8].copy_from_slice(&time_hi.to_be_bytes());
        result[8] = 0x80;
        result.to_vec()
    }

    fn duration_bytes(months: i64, days: i64, nanoseconds: i64) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&encode_vint(months));
        bytes.extend_from_slice(&encode_vint(days));
        bytes.extend_from_slice(&encode_vint(nanoseconds));
        bytes
    }

    fn utc_millis(year: i64, month: u32, day: u32, millis_of_day: i64) -> i64 {
        days_from_civil(year, month, day).unwrap() * MILLIS_PER_DAY + millis_of_day
    }
}
