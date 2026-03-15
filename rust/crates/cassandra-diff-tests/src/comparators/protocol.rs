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

//! Protocol frame comparator.
//!
//! Compares CQL native protocol frames byte-by-byte, with support for
//! ignoring fields that are expected to differ (e.g., timestamps, stream IDs).
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.transport.Frame`
//! - `org.apache.cassandra.transport.FrameEncoder`
//! - `org.apache.cassandra.transport.FrameDecoder`

use serde::{Deserialize, Serialize};

/// Parsed representation of a CQL native protocol frame header.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FrameHeader {
    pub version: u8,
    pub flags: u8,
    pub stream_id: i16,
    pub opcode: u8,
    pub body_length: u32,
}

/// Result of comparing two protocol frames.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FrameComparisonResult {
    pub headers_match: bool,
    pub bodies_match: bool,
    pub header_diffs: Vec<String>,
    pub body_diff_offset: Option<usize>,
    pub body_diff_detail: Option<String>,
}

impl FrameComparisonResult {
    pub fn is_match(&self) -> bool {
        self.headers_match && self.bodies_match
    }
}

/// Parse a frame header from raw bytes (9 bytes for v4/v5).
pub fn parse_header(data: &[u8]) -> Option<FrameHeader> {
    if data.len() < 9 {
        return None;
    }
    Some(FrameHeader {
        version: data[0],
        flags: data[1],
        stream_id: i16::from_be_bytes([data[2], data[3]]),
        opcode: data[4],
        body_length: u32::from_be_bytes([data[5], data[6], data[7], data[8]]),
    })
}

/// Options for frame comparison.
#[derive(Debug, Clone, Default)]
pub struct CompareOptions {
    /// Ignore stream ID differences (different connections use different IDs).
    pub ignore_stream_id: bool,
    /// Ignore body length differences (useful for partial comparisons).
    pub ignore_body_length: bool,
}

/// Compare two raw protocol frames.
///
/// Returns a detailed comparison result showing where frames differ.
pub fn compare_frames(
    java_frame: &[u8],
    rust_frame: &[u8],
    options: &CompareOptions,
) -> FrameComparisonResult {
    let java_header = parse_header(java_frame);
    let rust_header = parse_header(rust_frame);

    let mut header_diffs = Vec::new();

    match (&java_header, &rust_header) {
        (Some(jh), Some(rh)) => {
            if jh.version != rh.version {
                header_diffs.push(format!(
                    "version: java=0x{:02x} rust=0x{:02x}",
                    jh.version, rh.version
                ));
            }
            if jh.flags != rh.flags {
                header_diffs.push(format!(
                    "flags: java=0x{:02x} rust=0x{:02x}",
                    jh.flags, rh.flags
                ));
            }
            if !options.ignore_stream_id && jh.stream_id != rh.stream_id {
                header_diffs.push(format!(
                    "stream_id: java={} rust={}",
                    jh.stream_id, rh.stream_id
                ));
            }
            if jh.opcode != rh.opcode {
                header_diffs.push(format!(
                    "opcode: java=0x{:02x} rust=0x{:02x}",
                    jh.opcode, rh.opcode
                ));
            }
            if !options.ignore_body_length && jh.body_length != rh.body_length {
                header_diffs.push(format!(
                    "body_length: java={} rust={}",
                    jh.body_length, rh.body_length
                ));
            }
        }
        (None, _) => header_diffs.push("java frame too short for header".into()),
        (_, None) => header_diffs.push("rust frame too short for header".into()),
    }

    let headers_match = header_diffs.is_empty();

    // Compare bodies (bytes after header)
    let java_body = if java_frame.len() > 9 {
        &java_frame[9..]
    } else {
        &[]
    };
    let rust_body = if rust_frame.len() > 9 {
        &rust_frame[9..]
    } else {
        &[]
    };

    let (bodies_match, body_diff_offset, body_diff_detail) = compare_bodies(java_body, rust_body);

    FrameComparisonResult {
        headers_match,
        bodies_match,
        header_diffs,
        body_diff_offset,
        body_diff_detail,
    }
}

/// Compare two frame bodies byte-by-byte.
fn compare_bodies(java_body: &[u8], rust_body: &[u8]) -> (bool, Option<usize>, Option<String>) {
    if java_body == rust_body {
        return (true, None, None);
    }

    // Find first difference
    let min_len = java_body.len().min(rust_body.len());
    for i in 0..min_len {
        if java_body[i] != rust_body[i] {
            let context_start = i.saturating_sub(4);
            let context_end = (i + 5).min(min_len);
            let detail = format!(
                "First diff at offset {}. java[{}..{}]={} rust[{}..{}]={}",
                i,
                context_start,
                context_end,
                hex::encode(&java_body[context_start..context_end]),
                context_start,
                context_end,
                hex::encode(&rust_body[context_start..context_end]),
            );
            return (false, Some(i), Some(detail));
        }
    }

    // Length mismatch
    let detail = format!(
        "Bodies differ in length: java={} rust={}",
        java_body.len(),
        rust_body.len()
    );
    (false, Some(min_len), Some(detail))
}

/// Known CQL native protocol opcodes.
pub mod opcodes {
    pub const ERROR: u8 = 0x00;
    pub const STARTUP: u8 = 0x01;
    pub const READY: u8 = 0x02;
    pub const AUTHENTICATE: u8 = 0x03;
    pub const OPTIONS: u8 = 0x05;
    pub const SUPPORTED: u8 = 0x06;
    pub const QUERY: u8 = 0x07;
    pub const RESULT: u8 = 0x08;
    pub const PREPARE: u8 = 0x09;
    pub const EXECUTE: u8 = 0x0A;
    pub const REGISTER: u8 = 0x0B;
    pub const EVENT: u8 = 0x0C;
    pub const BATCH: u8 = 0x0D;
    pub const AUTH_CHALLENGE: u8 = 0x0E;
    pub const AUTH_RESPONSE: u8 = 0x0F;
    pub const AUTH_SUCCESS: u8 = 0x10;

    pub fn name(opcode: u8) -> &'static str {
        match opcode {
            ERROR => "ERROR",
            STARTUP => "STARTUP",
            READY => "READY",
            AUTHENTICATE => "AUTHENTICATE",
            OPTIONS => "OPTIONS",
            SUPPORTED => "SUPPORTED",
            QUERY => "QUERY",
            RESULT => "RESULT",
            PREPARE => "PREPARE",
            EXECUTE => "EXECUTE",
            REGISTER => "REGISTER",
            EVENT => "EVENT",
            BATCH => "BATCH",
            AUTH_CHALLENGE => "AUTH_CHALLENGE",
            AUTH_RESPONSE => "AUTH_RESPONSE",
            AUTH_SUCCESS => "AUTH_SUCCESS",
            _ => "UNKNOWN",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_valid_header() {
        // Protocol v4 response, no flags, stream 1, READY opcode, body length 0
        let data = [0x84, 0x00, 0x00, 0x01, 0x02, 0x00, 0x00, 0x00, 0x00];
        let header = parse_header(&data).unwrap();
        assert_eq!(header.version, 0x84);
        assert_eq!(header.flags, 0x00);
        assert_eq!(header.stream_id, 1);
        assert_eq!(header.opcode, opcodes::READY);
        assert_eq!(header.body_length, 0);
    }

    #[test]
    fn parse_short_data_returns_none() {
        let data = [0x84, 0x00, 0x00];
        assert!(parse_header(&data).is_none());
    }

    #[test]
    fn compare_identical_frames() {
        let frame = vec![0x84, 0x00, 0x00, 0x01, 0x02, 0x00, 0x00, 0x00, 0x00];
        let result = compare_frames(&frame, &frame, &CompareOptions::default());
        assert!(result.is_match());
    }

    #[test]
    fn compare_different_opcodes() {
        let java = vec![0x84, 0x00, 0x00, 0x01, 0x02, 0x00, 0x00, 0x00, 0x00];
        let rust = vec![0x84, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00];
        let result = compare_frames(&java, &rust, &CompareOptions::default());
        assert!(!result.is_match());
        assert!(!result.headers_match);
        assert!(result.header_diffs.iter().any(|d| d.contains("opcode")));
    }

    #[test]
    fn compare_ignoring_stream_id() {
        let java = vec![0x84, 0x00, 0x00, 0x01, 0x02, 0x00, 0x00, 0x00, 0x00];
        let rust = vec![0x84, 0x00, 0x00, 0x05, 0x02, 0x00, 0x00, 0x00, 0x00];
        let opts = CompareOptions {
            ignore_stream_id: true,
            ..Default::default()
        };
        let result = compare_frames(&java, &rust, &opts);
        assert!(result.is_match());
    }

    #[test]
    fn compare_different_bodies() {
        let mut java = vec![0x84, 0x00, 0x00, 0x01, 0x08, 0x00, 0x00, 0x00, 0x04];
        java.extend_from_slice(&[0x01, 0x02, 0x03, 0x04]);
        let mut rust = vec![0x84, 0x00, 0x00, 0x01, 0x08, 0x00, 0x00, 0x00, 0x04];
        rust.extend_from_slice(&[0x01, 0x02, 0xFF, 0x04]);
        let result = compare_frames(&java, &rust, &CompareOptions::default());
        assert!(!result.bodies_match);
        assert_eq!(result.body_diff_offset, Some(2));
    }

    #[test]
    fn opcode_names() {
        assert_eq!(opcodes::name(opcodes::ERROR), "ERROR");
        assert_eq!(opcodes::name(opcodes::STARTUP), "STARTUP");
        assert_eq!(opcodes::name(opcodes::QUERY), "QUERY");
        assert_eq!(opcodes::name(opcodes::RESULT), "RESULT");
        assert_eq!(opcodes::name(0xFF), "UNKNOWN");
    }
}
