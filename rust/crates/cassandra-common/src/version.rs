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

//! Version information for the Cassandra Rust implementation.
//!
//! Tracks both the Rust crate version and the Java baseline version
//! that this implementation targets for compatibility.

/// The Java Cassandra baseline that this Rust implementation targets.
pub const JAVA_BASELINE_BRANCH: &str = "trunk";

/// The exact commit hash of the frozen Java baseline.
pub const JAVA_BASELINE_COMMIT: &str = "076c6f11364645bbb43360f013bee6f50a099185";

/// The date the baseline was frozen.
pub const JAVA_BASELINE_DATE: &str = "2026-03-15";

/// CQL native protocol versions supported.
pub const SUPPORTED_PROTOCOL_VERSIONS: &[i32] = &[4, 5];

/// Returns a user-facing version string.
pub fn version_string() -> String {
    format!(
        "cassandra-rust {} (baseline: {}@{})",
        env!("CARGO_PKG_VERSION"),
        JAVA_BASELINE_BRANCH,
        &JAVA_BASELINE_COMMIT[..12],
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_string_format() {
        let v = version_string();
        assert!(v.starts_with("cassandra-rust 0.1.0"));
        assert!(v.contains("trunk"));
        assert!(v.contains("076c6f113646"));
    }

    #[test]
    fn protocol_versions() {
        assert!(SUPPORTED_PROTOCOL_VERSIONS.contains(&4));
        assert!(SUPPORTED_PROTOCOL_VERSIONS.contains(&5));
    }
}
