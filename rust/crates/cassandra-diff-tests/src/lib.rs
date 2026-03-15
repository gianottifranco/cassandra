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

//! # cassandra-diff-tests
//!
//! Differential testing framework for the Cassandra Java-to-Rust rewrite.
//!
//! This crate provides:
//! - **Comparators** for verifying behavioral equivalence between Java and Rust
//!   implementations across protocol frames, query results, error codes, and more.
//! - **Golden test infrastructure** for offline comparison against pre-generated
//!   fixtures from the Java oracle.
//! - **Fuzz strategies** (proptest) for property-based testing of protocol codecs
//!   and CQL type serialization.
//!
//! ## Architecture
//!
//! ```text
//! ┌────────────────────────────────┐
//! │     Java Oracle (Docker)       │
//! │  Generates golden fixtures     │
//! └─────────────┬──────────────────┘
//!               │ JSON + binary
//!               ▼
//! ┌────────────────────────────────┐
//! │   golden/ directory (VCS)      │
//! │  protocol/ types/ errors/ ...  │
//! └─────────────┬──────────────────┘
//!               │
//!               ▼
//! ┌────────────────────────────────┐
//! │   cassandra-diff-tests         │
//! │  ├── comparators/              │
//! │  ├── golden.rs                 │
//! │  └── fuzz.rs                   │
//! └────────────────────────────────┘
//! ```
//!
//! ## Java Oracle References
//!
//! - `org.apache.cassandra.transport.Frame` (protocol frames)
//! - `org.apache.cassandra.transport.messages.*` (message types)
//! - `org.apache.cassandra.db.marshal.*` (type system)
//! - `org.apache.cassandra.exceptions.*` (error codes)

pub mod comparators;
pub mod fuzz;
pub mod golden;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crate_compiles() {
        // Smoke test: the diff-tests crate compiles and links correctly.
        assert!(true);
    }

    #[test]
    fn golden_dir_exists() {
        // Verify the golden fixtures directory is accessible from this crate.
        let golden_dir = golden::golden_dir();
        assert!(
            golden_dir.exists(),
            "Golden fixtures directory not found at {:?}",
            golden_dir
        );
    }
}
