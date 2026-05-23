// Licensed under Apache License, Version 2.0.

//! Common helpers inspired by Cassandra's `FBUtilities`.
//!
//! The Java class is a broad grab bag. This module intentionally starts with
//! helpers that are widely needed across the Rust port: wall-clock timestamps,
//! deterministic local address fallbacks, and human-readable byte sizes.

use std::net::{IpAddr, Ipv4Addr, SocketAddr, UdpSocket};
use std::time::{SystemTime, UNIX_EPOCH};

use thiserror::Error;

/// Errors returned by FBUtilities-style parsing helpers.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum FbUtilitiesError {
    /// The size string was empty.
    #[error("size string is empty")]
    EmptySize,
    /// The numeric part could not be parsed.
    #[error("invalid size number: {0}")]
    InvalidSizeNumber(String),
    /// The suffix is not a supported byte-size unit.
    #[error("invalid size unit: {0}")]
    InvalidSizeUnit(String),
    /// Multiplication would overflow `u64`.
    #[error("size overflows u64")]
    SizeOverflow,
}

/// Current Unix timestamp in microseconds.
pub fn timestamp_micros() -> i64 {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    duration.as_secs() as i64 * 1_000_000 + i64::from(duration.subsec_micros())
}

/// Current Unix timestamp in milliseconds.
pub fn timestamp_millis() -> i64 {
    timestamp_micros() / 1_000
}

/// Current Unix timestamp in seconds.
pub fn now_in_seconds() -> i32 {
    (timestamp_micros() / 1_000_000) as i32
}

/// Return the local address selected by the OS for outbound IPv4 traffic.
///
/// Falls back to loopback if the host has no usable route.
pub fn local_address() -> IpAddr {
    let socket = UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 0));
    if let Ok(socket) = socket {
        if socket.connect((Ipv4Addr::new(8, 8, 8, 8), 53)).is_ok() {
            if let Ok(SocketAddr::V4(addr)) = socket.local_addr() {
                return IpAddr::V4(*addr.ip());
            }
        }
    }
    IpAddr::V4(Ipv4Addr::LOCALHOST)
}

/// Return the broadcast/listen address used when no explicit config is present.
pub fn broadcast_address() -> IpAddr {
    local_address()
}

/// Format a byte count using binary units.
pub fn pretty_print_memory(bytes: u64) -> String {
    const UNITS: [&str; 6] = ["bytes", "KiB", "MiB", "GiB", "TiB", "PiB"];
    if bytes < 1024 {
        return format!("{bytes} bytes");
    }

    let mut value = bytes as f64;
    let mut unit = 0usize;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }

    if value >= 10.0 {
        format!("{value:.0} {}", UNITS[unit])
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}

/// Parse a byte size with optional binary or decimal suffix.
///
/// Supported suffixes: `B`, `K/KiB/KB`, `M/MiB/MB`, `G/GiB/GB`, `T/TiB/TB`,
/// `P/PiB/PB`. Bare numbers are bytes. Binary suffixes use powers of 1024,
/// matching Cassandra configuration sizes.
pub fn parse_human_readable_bytes(input: &str) -> Result<u64, FbUtilitiesError> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Err(FbUtilitiesError::EmptySize);
    }

    let split = trimmed
        .find(|ch: char| !(ch.is_ascii_digit() || ch == '.'))
        .unwrap_or(trimmed.len());
    let (number, suffix) = trimmed.split_at(split);
    if number.is_empty() {
        return Err(FbUtilitiesError::InvalidSizeNumber(trimmed.to_string()));
    }

    let value = number
        .parse::<f64>()
        .map_err(|_| FbUtilitiesError::InvalidSizeNumber(number.to_string()))?;
    if !value.is_finite() || value < 0.0 {
        return Err(FbUtilitiesError::InvalidSizeNumber(number.to_string()));
    }

    let multiplier = match suffix.trim().to_ascii_lowercase().as_str() {
        "" | "b" | "byte" | "bytes" => 1u64,
        "k" | "kb" | "kib" => 1024,
        "m" | "mb" | "mib" => 1024u64.pow(2),
        "g" | "gb" | "gib" => 1024u64.pow(3),
        "t" | "tb" | "tib" => 1024u64.pow(4),
        "p" | "pb" | "pib" => 1024u64.pow(5),
        other => return Err(FbUtilitiesError::InvalidSizeUnit(other.to_string())),
    };

    let bytes = value * multiplier as f64;
    if bytes > u64::MAX as f64 {
        return Err(FbUtilitiesError::SizeOverflow);
    }
    Ok(bytes as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timestamps_are_consistent() {
        let micros = timestamp_micros();
        let millis = timestamp_millis();
        let seconds = now_in_seconds();

        assert!(micros > 0);
        assert!(millis > 0);
        assert!(seconds > 0);
        assert!((micros / 1_000 - millis).abs() <= 1);
        assert!((millis / 1_000 - i64::from(seconds)).abs() <= 1);
    }

    #[test]
    fn local_addresses_have_fallback() {
        assert!(local_address().is_ipv4() || local_address().is_ipv6());
        assert!(broadcast_address().is_ipv4() || broadcast_address().is_ipv6());
    }

    #[test]
    fn pretty_prints_memory() {
        assert_eq!(pretty_print_memory(0), "0 bytes");
        assert_eq!(pretty_print_memory(1), "1 bytes");
        assert_eq!(pretty_print_memory(1024), "1.0 KiB");
        assert_eq!(pretty_print_memory(10 * 1024 * 1024), "10 MiB");
    }

    #[test]
    fn parses_human_readable_bytes() {
        assert_eq!(parse_human_readable_bytes("42").unwrap(), 42);
        assert_eq!(parse_human_readable_bytes("1 KiB").unwrap(), 1024);
        assert_eq!(parse_human_readable_bytes("1.5MiB").unwrap(), 1_572_864);
        assert_eq!(
            parse_human_readable_bytes("2 GB").unwrap(),
            2 * 1024 * 1024 * 1024
        );
    }

    #[test]
    fn rejects_invalid_sizes() {
        assert_eq!(
            parse_human_readable_bytes(""),
            Err(FbUtilitiesError::EmptySize)
        );
        assert_eq!(
            parse_human_readable_bytes("abc"),
            Err(FbUtilitiesError::InvalidSizeNumber("abc".to_string()))
        );
        assert_eq!(
            parse_human_readable_bytes("1XB"),
            Err(FbUtilitiesError::InvalidSizeUnit("xb".to_string()))
        );
    }
}
