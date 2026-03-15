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

//! Error code comparator.
//!
//! Compares Cassandra error codes and messages between Java and Rust
//! implementations, ensuring protocol-level error compatibility.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.exceptions.ExceptionCode`
//! - `org.apache.cassandra.transport.messages.ErrorMessage`

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// A Cassandra protocol error code with metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErrorCodeEntry {
    pub name: String,
    pub description: String,
}

/// Result of comparing error responses.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorComparisonResult {
    /// Whether the error codes match.
    pub codes_match: bool,
    /// Whether the error messages are semantically equivalent.
    pub messages_match: bool,
    /// Details of any mismatches.
    pub details: Vec<String>,
}

impl ErrorComparisonResult {
    pub fn is_match(&self) -> bool {
        self.codes_match && self.messages_match
    }
}

/// Parse error code from a protocol ERROR frame body.
///
/// The body starts with a 4-byte big-endian error code, followed by
/// a [string] (2-byte length + UTF-8 bytes) error message.
pub fn parse_error_body(body: &[u8]) -> Option<(i32, String)> {
    if body.len() < 6 {
        return None;
    }

    let code = i32::from_be_bytes([body[0], body[1], body[2], body[3]]);
    let msg_len = u16::from_be_bytes([body[4], body[5]]) as usize;

    if body.len() < 6 + msg_len {
        return None;
    }

    let message = String::from_utf8_lossy(&body[6..6 + msg_len]).to_string();
    Some((code, message))
}

/// Compare two error responses.
///
/// Error codes must match exactly. Error messages are compared with
/// tolerance for minor wording differences (but the error category
/// must be the same).
pub fn compare_errors(
    java_code: i32,
    java_message: &str,
    rust_code: i32,
    rust_message: &str,
) -> ErrorComparisonResult {
    let mut details = Vec::new();

    let codes_match = java_code == rust_code;
    if !codes_match {
        details.push(format!(
            "Error code mismatch: java=0x{:04x} rust=0x{:04x}",
            java_code, rust_code
        ));
    }

    // For messages, we check that they're semantically similar.
    // Exact match is ideal, but we tolerate minor differences in wording
    // as long as the error category is preserved.
    let messages_match = java_message == rust_message;
    if !messages_match {
        details.push(format!(
            "Error message mismatch:\n  java: \"{}\"\n  rust: \"{}\"",
            java_message, rust_message
        ));
    }

    ErrorComparisonResult {
        codes_match,
        messages_match,
        details,
    }
}

/// Load the canonical error code map from the golden fixtures.
pub fn load_error_code_map(
    json_content: &str,
) -> Result<HashMap<String, ErrorCodeEntry>, serde_json::Error> {
    serde_json::from_str(json_content)
}

/// All known Cassandra protocol error codes.
///
/// Java oracle: `org.apache.cassandra.exceptions.ExceptionCode`
pub mod error_codes {
    pub const SERVER_ERROR: i32 = 0x0000;
    pub const PROTOCOL_ERROR: i32 = 0x000A;
    pub const BAD_CREDENTIALS: i32 = 0x0100;
    pub const UNAVAILABLE: i32 = 0x1000;
    pub const OVERLOADED: i32 = 0x1001;
    pub const IS_BOOTSTRAPPING: i32 = 0x1002;
    pub const TRUNCATE_ERROR: i32 = 0x1003;
    pub const WRITE_TIMEOUT: i32 = 0x1100;
    pub const READ_TIMEOUT: i32 = 0x1200;
    pub const READ_FAILURE: i32 = 0x1300;
    pub const FUNCTION_FAILURE: i32 = 0x1400;
    pub const WRITE_FAILURE: i32 = 0x1500;
    pub const CDC_WRITE_FAILURE: i32 = 0x1600;
    pub const CAS_WRITE_UNKNOWN: i32 = 0x1700;
    pub const SYNTAX_ERROR: i32 = 0x2000;
    pub const UNAUTHORIZED: i32 = 0x2100;
    pub const INVALID: i32 = 0x2200;
    pub const CONFIG_ERROR: i32 = 0x2300;
    pub const ALREADY_EXISTS: i32 = 0x2400;
    pub const UNPREPARED: i32 = 0x2500;

    pub fn name(code: i32) -> &'static str {
        match code {
            SERVER_ERROR => "SERVER_ERROR",
            PROTOCOL_ERROR => "PROTOCOL_ERROR",
            BAD_CREDENTIALS => "BAD_CREDENTIALS",
            UNAVAILABLE => "UNAVAILABLE",
            OVERLOADED => "OVERLOADED",
            IS_BOOTSTRAPPING => "IS_BOOTSTRAPPING",
            TRUNCATE_ERROR => "TRUNCATE_ERROR",
            WRITE_TIMEOUT => "WRITE_TIMEOUT",
            READ_TIMEOUT => "READ_TIMEOUT",
            READ_FAILURE => "READ_FAILURE",
            FUNCTION_FAILURE => "FUNCTION_FAILURE",
            WRITE_FAILURE => "WRITE_FAILURE",
            CDC_WRITE_FAILURE => "CDC_WRITE_FAILURE",
            CAS_WRITE_UNKNOWN => "CAS_WRITE_UNKNOWN",
            SYNTAX_ERROR => "SYNTAX_ERROR",
            UNAUTHORIZED => "UNAUTHORIZED",
            INVALID => "INVALID",
            CONFIG_ERROR => "CONFIG_ERROR",
            ALREADY_EXISTS => "ALREADY_EXISTS",
            UNPREPARED => "UNPREPARED",
            _ => "UNKNOWN",
        }
    }

    /// All defined error codes as a slice for iteration.
    pub const ALL: &[i32] = &[
        SERVER_ERROR,
        PROTOCOL_ERROR,
        BAD_CREDENTIALS,
        UNAVAILABLE,
        OVERLOADED,
        IS_BOOTSTRAPPING,
        TRUNCATE_ERROR,
        WRITE_TIMEOUT,
        READ_TIMEOUT,
        READ_FAILURE,
        FUNCTION_FAILURE,
        WRITE_FAILURE,
        CDC_WRITE_FAILURE,
        CAS_WRITE_UNKNOWN,
        SYNTAX_ERROR,
        UNAUTHORIZED,
        INVALID,
        CONFIG_ERROR,
        ALREADY_EXISTS,
        UNPREPARED,
    ];
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_error_body_valid() {
        // error_code = 0x2000 (SYNTAX_ERROR), message = "bad"
        let body = [
            0x00, 0x00, 0x20, 0x00, // error code
            0x00, 0x03, // message length
            b'b', b'a', b'd', // message
        ];
        let (code, msg) = parse_error_body(&body).unwrap();
        assert_eq!(code, error_codes::SYNTAX_ERROR);
        assert_eq!(msg, "bad");
    }

    #[test]
    fn parse_error_body_too_short() {
        let body = [0x00, 0x00];
        assert!(parse_error_body(&body).is_none());
    }

    #[test]
    fn compare_matching_errors() {
        let result = compare_errors(
            error_codes::SYNTAX_ERROR,
            "line 1:0 no viable alternative",
            error_codes::SYNTAX_ERROR,
            "line 1:0 no viable alternative",
        );
        assert!(result.is_match());
    }

    #[test]
    fn compare_different_error_codes() {
        let result = compare_errors(
            error_codes::SYNTAX_ERROR,
            "error msg",
            error_codes::INVALID,
            "error msg",
        );
        assert!(!result.codes_match);
        assert!(result.messages_match);
    }

    #[test]
    fn all_error_codes_have_names() {
        for &code in error_codes::ALL {
            let name = error_codes::name(code);
            assert_ne!(name, "UNKNOWN", "Error code 0x{:04x} is UNKNOWN", code);
        }
    }

    #[test]
    fn error_code_count() {
        // Verify we have all 20 defined error codes.
        assert_eq!(error_codes::ALL.len(), 20);
    }
}
