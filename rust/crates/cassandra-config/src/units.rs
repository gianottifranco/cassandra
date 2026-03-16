// Licensed under Apache License, Version 2.0.

//! Data size and duration parsers for `cassandra.yaml` values.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.config.DataStorageSpec`
//! - `org.apache.cassandra.config.DurationSpec`

use serde::{Deserialize, Deserializer, Serialize};
use std::fmt;
use std::str::FromStr;

/// A data size in bytes, parsed from strings like "128MiB", "1024KiB".
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
pub struct DataSize(pub u64);

impl DataSize {
    pub const ZERO: Self = Self(0);
    pub fn bytes(self) -> u64 {
        self.0
    }
    pub fn kibibytes(self) -> u64 {
        self.0 / 1024
    }
    pub fn mebibytes(self) -> u64 {
        self.0 / (1024 * 1024)
    }
    pub fn from_kibibytes(kib: u64) -> Self {
        Self(kib * 1024)
    }
    pub fn from_mebibytes(mib: u64) -> Self {
        Self(mib * 1024 * 1024)
    }
    pub fn from_gibibytes(gib: u64) -> Self {
        Self(gib * 1024 * 1024 * 1024)
    }
}

impl FromStr for DataSize {
    type Err = ParseUnitError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.trim();
        if s.is_empty() {
            return Err(ParseUnitError(format!("empty data size")));
        }
        // Try suffixed formats
        let lower = s.to_lowercase();
        if let Some(num) = lower.strip_suffix("tib") {
            let v: u64 = num
                .trim()
                .parse()
                .map_err(|_| ParseUnitError(format!("invalid number: {}", num)))?;
            return Ok(Self(v * 1024 * 1024 * 1024 * 1024));
        }
        if let Some(num) = lower.strip_suffix("gib") {
            let v: u64 = num
                .trim()
                .parse()
                .map_err(|_| ParseUnitError(format!("invalid number: {}", num)))?;
            return Ok(Self::from_gibibytes(v));
        }
        if let Some(num) = lower.strip_suffix("mib") {
            let v: u64 = num
                .trim()
                .parse()
                .map_err(|_| ParseUnitError(format!("invalid number: {}", num)))?;
            return Ok(Self::from_mebibytes(v));
        }
        if let Some(num) = lower.strip_suffix("kib") {
            let v: u64 = num
                .trim()
                .parse()
                .map_err(|_| ParseUnitError(format!("invalid number: {}", num)))?;
            return Ok(Self::from_kibibytes(v));
        }
        // Plain number = bytes
        let v: u64 = s
            .parse()
            .map_err(|_| ParseUnitError(format!("invalid data size: {}", s)))?;
        Ok(Self(v))
    }
}

impl<'de> Deserialize<'de> for DataSize {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = StringOrNumber::deserialize(deserializer)?;
        match s {
            StringOrNumber::Str(s) => s.parse().map_err(serde::de::Error::custom),
            StringOrNumber::Num(n) => Ok(DataSize(n)),
        }
    }
}

impl fmt::Display for DataSize {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.0 == 0 {
            return write!(f, "0");
        }
        if self.0 % (1024 * 1024 * 1024) == 0 {
            write!(f, "{}GiB", self.0 / (1024 * 1024 * 1024))
        } else if self.0 % (1024 * 1024) == 0 {
            write!(f, "{}MiB", self.0 / (1024 * 1024))
        } else if self.0 % 1024 == 0 {
            write!(f, "{}KiB", self.0 / 1024)
        } else {
            write!(f, "{}B", self.0)
        }
    }
}

/// A duration in milliseconds, parsed from strings like "10000ms", "3h", "30s".
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
pub struct Duration(pub u64);

impl Duration {
    pub const ZERO: Self = Self(0);
    pub fn millis(self) -> u64 {
        self.0
    }
    pub fn seconds(self) -> u64 {
        self.0 / 1000
    }
    pub fn from_millis(ms: u64) -> Self {
        Self(ms)
    }
    pub fn from_seconds(s: u64) -> Self {
        Self(s * 1000)
    }
    pub fn from_minutes(m: u64) -> Self {
        Self(m * 60 * 1000)
    }
    pub fn from_hours(h: u64) -> Self {
        Self(h * 3600 * 1000)
    }
}

impl FromStr for Duration {
    type Err = ParseUnitError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.trim();
        if s.is_empty() {
            return Err(ParseUnitError("empty duration".into()));
        }
        let lower = s.to_lowercase();
        if let Some(num) = lower.strip_suffix("ms") {
            let v: u64 = num
                .trim()
                .parse()
                .map_err(|_| ParseUnitError(format!("invalid: {}", num)))?;
            return Ok(Self::from_millis(v));
        }
        if let Some(num) = lower.strip_suffix('h') {
            let v: u64 = num
                .trim()
                .parse()
                .map_err(|_| ParseUnitError(format!("invalid: {}", num)))?;
            return Ok(Self::from_hours(v));
        }
        if let Some(num) = lower.strip_suffix('m') {
            let v: u64 = num
                .trim()
                .parse()
                .map_err(|_| ParseUnitError(format!("invalid: {}", num)))?;
            return Ok(Self::from_minutes(v));
        }
        if let Some(num) = lower.strip_suffix('s') {
            let v: u64 = num
                .trim()
                .parse()
                .map_err(|_| ParseUnitError(format!("invalid: {}", num)))?;
            return Ok(Self::from_seconds(v));
        }
        // Plain number = milliseconds
        let v: u64 = s
            .parse()
            .map_err(|_| ParseUnitError(format!("invalid duration: {}", s)))?;
        Ok(Self(v))
    }
}

impl<'de> Deserialize<'de> for Duration {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = StringOrNumber::deserialize(deserializer)?;
        match s {
            StringOrNumber::Str(s) => s.parse().map_err(serde::de::Error::custom),
            StringOrNumber::Num(n) => Ok(Duration(n)),
        }
    }
}

impl fmt::Display for Duration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.0 == 0 {
            write!(f, "0ms")
        } else if self.0 % 3_600_000 == 0 {
            write!(f, "{}h", self.0 / 3_600_000)
        } else if self.0 % 60_000 == 0 {
            write!(f, "{}m", self.0 / 60_000)
        } else if self.0 % 1_000 == 0 {
            write!(f, "{}s", self.0 / 1_000)
        } else {
            write!(f, "{}ms", self.0)
        }
    }
}

/// Helper for deserializing YAML values that can be string or number.
#[derive(Deserialize)]
#[serde(untagged)]
enum StringOrNumber {
    Str(String),
    Num(u64),
}

/// Error parsing a unit-suffixed value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseUnitError(pub String);

impl fmt::Display for ParseUnitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "unit parse error: {}", self.0)
    }
}
impl std::error::Error for ParseUnitError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn data_size_mib() {
        let ds: DataSize = "128MiB".parse().unwrap();
        assert_eq!(ds.bytes(), 128 * 1024 * 1024);
        assert_eq!(ds.mebibytes(), 128);
    }

    #[test]
    fn data_size_kib() {
        let ds: DataSize = "1024KiB".parse().unwrap();
        assert_eq!(ds.bytes(), 1024 * 1024);
    }

    #[test]
    fn data_size_plain() {
        let ds: DataSize = "4096".parse().unwrap();
        assert_eq!(ds.bytes(), 4096);
    }

    #[test]
    fn data_size_display() {
        assert_eq!(DataSize::from_mebibytes(128).to_string(), "128MiB");
        assert_eq!(DataSize::from_gibibytes(1).to_string(), "1GiB");
    }

    #[test]
    fn duration_ms() {
        let d: Duration = "10000ms".parse().unwrap();
        assert_eq!(d.millis(), 10000);
    }

    #[test]
    fn duration_hours() {
        let d: Duration = "3h".parse().unwrap();
        assert_eq!(d.millis(), 3 * 3600 * 1000);
    }

    #[test]
    fn duration_seconds() {
        let d: Duration = "30s".parse().unwrap();
        assert_eq!(d.seconds(), 30);
    }

    #[test]
    fn duration_display() {
        assert_eq!(Duration::from_hours(4).to_string(), "4h");
        assert_eq!(Duration::from_millis(500).to_string(), "500ms");
    }

    #[test]
    fn invalid_data_size() {
        assert!("not_a_size".parse::<DataSize>().is_err());
    }

    #[test]
    fn invalid_duration() {
        assert!("bad".parse::<Duration>().is_err());
    }
}
