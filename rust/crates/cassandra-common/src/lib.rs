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

//! # cassandra-common
//!
//! Shared utilities, error types, byte-buffer abstractions, and concurrency
//! primitives used across all Cassandra Rust crates.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.utils`
//! - `org.apache.cassandra.concurrent`
//! - `org.apache.cassandra.exceptions`
//! - `org.apache.cassandra.serializers`

pub mod error;
pub mod murmur3;
pub mod partitioner;
pub mod timestamp;
pub mod token;
pub mod tombstone;
pub mod ttl;
pub mod version;

/// Re-export commonly used types.
pub use error::{CassandraError, CassandraResult};
pub use partitioner::{Murmur3Partitioner, Partitioner, create_partitioner};
pub use timestamp::Timestamp;
pub use token::Token;
pub use tombstone::{DeletionTime, RangeTombstone};
pub use ttl::{LocalDeletionTime, Ttl};

/// Crate version, matching the workspace version.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

// TODO(phase-3+): Add modules:
// - bloom: Bloom filter implementation
// - concurrent: Thread pool abstractions
// - bytes: ByteBuf / ByteComparable abstractions
// - compression: LZ4, Snappy, Zstd wrappers
// - merkle: Merkle tree for anti-entropy repair

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_is_set() {
        assert!(!VERSION.is_empty());
        assert_eq!(VERSION, "0.1.0");
    }

    #[test]
    fn error_display() {
        let err = CassandraError::Internal("test error".into());
        assert!(err.to_string().contains("test error"));
    }
}
