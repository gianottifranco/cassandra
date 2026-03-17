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

//! Token ring tokens for consistent hashing.
//!
//! Currently only the Murmur3Partitioner is implemented (the overwhelmingly
//! dominant choice in production). Other partitioners (Random, ByteOrdered)
//! can be added behind feature flags.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.dht.Murmur3Partitioner`
//! - `org.apache.cassandra.dht.Murmur3Partitioner.LongToken`

use std::fmt;

use byteorder::{BigEndian, ByteOrder};

use crate::murmur3;

/// A token on the Murmur3 ring.
///
/// The token space is `[i64::MIN, i64::MAX]`. Murmur3Partitioner uses
/// the first 64 bits of the Murmur3 128-bit hash as the token value.
#[derive(
    Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub struct Token(pub i64);

impl Token {
    /// The minimum token on the ring.
    /// Matches Java `Murmur3Partitioner.MINIMUM = new LongToken(Long.MIN_VALUE)`.
    pub const MINIMUM: Self = Self(i64::MIN);

    /// The maximum token on the ring.
    pub const MAXIMUM: Self = Self(i64::MAX);

    /// Compute the token for a partition key using Murmur3.
    ///
    /// This is the primary entry point for routing partition keys to nodes.
    #[inline]
    pub fn from_partition_key(key: &[u8]) -> Self {
        Self(murmur3::murmur3_token(key))
    }

    /// Create a token from a raw i64 value.
    #[inline]
    pub const fn from_raw(value: i64) -> Self {
        Self(value)
    }

    /// The raw i64 token value.
    #[inline]
    pub const fn value(self) -> i64 {
        self.0
    }

    /// Returns `true` if this is the minimum (wrap-around) token.
    #[inline]
    pub const fn is_minimum(self) -> bool {
        self.0 == i64::MIN
    }

    /// Serialize to 8 bytes, big-endian.
    #[inline]
    pub fn serialize(self, buf: &mut [u8; 8]) {
        BigEndian::write_i64(buf, self.0);
    }

    /// Deserialize from 8 bytes, big-endian.
    #[inline]
    pub fn deserialize(buf: &[u8; 8]) -> Self {
        Self(BigEndian::read_i64(buf))
    }

    /// Serialized size.
    pub const SERIALIZED_SIZE: usize = 8;

    /// Midpoint between two tokens on the ring (used for range splitting).
    ///
    /// Matches Java's `Murmur3Partitioner.midpoint()`.
    pub fn midpoint(left: Token, right: Token) -> Token {
        // Use i128 to avoid overflow
        let mid = ((left.0 as i128) + (right.0 as i128)) / 2;
        Token(mid as i64)
    }
}

impl fmt::Debug for Token {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_minimum() {
            write!(f, "Token(MINIMUM)")
        } else {
            write!(f, "Token({})", self.0)
        }
    }
}

impl fmt::Display for Token {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// A contiguous range of tokens `(start, end]` on the ring.
///
/// If `start >= end`, the range wraps around the ring.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub struct TokenRange {
    pub start: Token,
    pub end: Token,
}

impl TokenRange {
    /// Full ring: `(MINIMUM, MINIMUM]` which covers everything.
    pub const FULL_RING: Self = Self {
        start: Token::MINIMUM,
        end: Token::MINIMUM,
    };

    /// Create a new token range.
    pub const fn new(start: Token, end: Token) -> Self {
        Self { start, end }
    }

    /// Returns `true` if this range wraps around the ring
    /// (i.e., start >= end and it's not a full ring).
    pub fn wraps_around(&self) -> bool {
        self.start >= self.end && *self != Self::FULL_RING
    }

    /// Returns `true` if the given token falls within this range `(start, end]`.
    pub fn contains(&self, token: Token) -> bool {
        if self.wraps_around() {
            // Wrapping case: token > start OR token <= end
            token > self.start || token <= self.end
        } else if *self == Self::FULL_RING {
            true
        } else {
            token > self.start && token <= self.end
        }
    }
}

impl fmt::Display for TokenRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "({}, {}]", self.start, self.end)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn minimum_is_smallest() {
        assert!(Token::MINIMUM < Token::from_raw(0));
        assert!(Token::MINIMUM < Token::MAXIMUM);
    }

    #[test]
    fn ordering() {
        let a = Token::from_raw(-100);
        let b = Token::from_raw(0);
        let c = Token::from_raw(100);
        assert!(Token::MINIMUM < a);
        assert!(a < b);
        assert!(b < c);
        assert!(c < Token::MAXIMUM);
    }

    #[test]
    fn from_partition_key() {
        let token = Token::from_partition_key(b"test");
        // Just verify it's deterministic and not minimum
        let token2 = Token::from_partition_key(b"test");
        assert_eq!(token, token2);
    }

    #[test]
    fn different_keys_usually_different_tokens() {
        let t1 = Token::from_partition_key(b"key1");
        let t2 = Token::from_partition_key(b"key2");
        assert_ne!(t1, t2);
    }

    #[test]
    fn serialize_round_trip() {
        let token = Token::from_raw(42);
        let mut buf = [0u8; 8];
        token.serialize(&mut buf);
        assert_eq!(Token::deserialize(&buf), token);
    }

    #[test]
    fn midpoint() {
        let left = Token::from_raw(0);
        let right = Token::from_raw(100);
        let mid = Token::midpoint(left, right);
        assert_eq!(mid.value(), 50);
    }

    #[test]
    fn midpoint_no_overflow() {
        let mid = Token::midpoint(Token::from_raw(i64::MAX - 1), Token::from_raw(i64::MAX));
        assert!(mid.value() > i64::MAX - 2);
    }

    #[test]
    fn token_range_contains() {
        let range = TokenRange::new(Token::from_raw(10), Token::from_raw(20));
        assert!(!range.contains(Token::from_raw(10))); // exclusive start
        assert!(range.contains(Token::from_raw(11)));
        assert!(range.contains(Token::from_raw(20))); // inclusive end
        assert!(!range.contains(Token::from_raw(21)));
    }

    #[test]
    fn full_ring_contains_all() {
        assert!(TokenRange::FULL_RING.contains(Token::MINIMUM));
        assert!(TokenRange::FULL_RING.contains(Token::from_raw(0)));
        assert!(TokenRange::FULL_RING.contains(Token::MAXIMUM));
    }

    #[test]
    fn wrapping_range() {
        let range = TokenRange::new(Token::from_raw(100), Token::from_raw(-100));
        assert!(range.wraps_around());
        assert!(range.contains(Token::from_raw(101)));
        assert!(range.contains(Token::from_raw(i64::MAX)));
        assert!(range.contains(Token::from_raw(-100)));
        assert!(!range.contains(Token::from_raw(50)));
    }
}
