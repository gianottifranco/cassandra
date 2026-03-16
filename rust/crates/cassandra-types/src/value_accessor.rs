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

//! `ValueAccessor` trait for generic read access over serialized CQL bytes.
//!
//! Provides a uniform interface over `&[u8]` and `bytes::Bytes` so that
//! type-system code can operate without copying buffers unnecessarily.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.db.marshal.ValueAccessor`
//! - `org.apache.cassandra.db.marshal.ByteArrayAccessor`
//! - `org.apache.cassandra.db.marshal.ByteBufferAccessor`

use byteorder::{BigEndian, ByteOrder};
use bytes::Bytes;
use std::cmp::Ordering;

/// Generic read-only access to a CQL serialized value buffer.
///
/// Implementations may operate over raw `&[u8]` slices or `Bytes` objects.
/// All multi-byte reads are big-endian, matching the CQL native protocol.
pub trait ValueAccessor {
    /// Total number of bytes in the buffer.
    fn size(&self) -> usize;

    /// Returns `true` if the buffer is empty.
    fn is_empty(&self) -> bool {
        self.size() == 0
    }

    /// Read a single byte at `offset`.
    fn read_byte(&self, offset: usize) -> u8;

    /// Read a big-endian `i16` at `offset`.
    fn read_i16(&self, offset: usize) -> i16;

    /// Read a big-endian `i32` at `offset`.
    fn read_i32(&self, offset: usize) -> i32;

    /// Read a big-endian `i64` at `offset`.
    fn read_i64(&self, offset: usize) -> i64;

    /// Read a big-endian `f32` at `offset`.
    fn read_f32(&self, offset: usize) -> f32;

    /// Read a big-endian `f64` at `offset`.
    fn read_f64(&self, offset: usize) -> f64;

    /// Return a slice `[start..end]` of the underlying bytes.
    fn slice(&self, start: usize, end: usize) -> &[u8];

    /// Copy the entire buffer into a `Vec<u8>`.
    fn to_vec(&self) -> Vec<u8>;

    /// Lexicographic byte comparison with another accessor.
    fn compare(&self, other: &dyn ValueAccessor) -> Ordering {
        self.slice(0, self.size()).cmp(other.slice(0, other.size()))
    }
}

// ── impl for &[u8] ──────────────────────────────────────────────────────────

impl ValueAccessor for &[u8] {
    fn size(&self) -> usize {
        self.len()
    }

    fn read_byte(&self, offset: usize) -> u8 {
        self[offset]
    }

    fn read_i16(&self, offset: usize) -> i16 {
        BigEndian::read_i16(&self[offset..])
    }

    fn read_i32(&self, offset: usize) -> i32 {
        BigEndian::read_i32(&self[offset..])
    }

    fn read_i64(&self, offset: usize) -> i64 {
        BigEndian::read_i64(&self[offset..])
    }

    fn read_f32(&self, offset: usize) -> f32 {
        BigEndian::read_f32(&self[offset..])
    }

    fn read_f64(&self, offset: usize) -> f64 {
        BigEndian::read_f64(&self[offset..])
    }

    fn slice(&self, start: usize, end: usize) -> &[u8] {
        &self[start..end]
    }

    fn to_vec(&self) -> Vec<u8> {
        (*self).to_vec()
    }
}

// ── impl for bytes::Bytes ───────────────────────────────────────────────────

impl ValueAccessor for Bytes {
    fn size(&self) -> usize {
        self.len()
    }

    fn read_byte(&self, offset: usize) -> u8 {
        self[offset]
    }

    fn read_i16(&self, offset: usize) -> i16 {
        BigEndian::read_i16(&self[offset..])
    }

    fn read_i32(&self, offset: usize) -> i32 {
        BigEndian::read_i32(&self[offset..])
    }

    fn read_i64(&self, offset: usize) -> i64 {
        BigEndian::read_i64(&self[offset..])
    }

    fn read_f32(&self, offset: usize) -> f32 {
        BigEndian::read_f32(&self[offset..])
    }

    fn read_f64(&self, offset: usize) -> f64 {
        BigEndian::read_f64(&self[offset..])
    }

    fn slice(&self, start: usize, end: usize) -> &[u8] {
        &self[start..end]
    }

    fn to_vec(&self) -> Vec<u8> {
        self.as_ref().to_vec()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slice_read_i32() {
        let data: &[u8] = &[0x00, 0x00, 0x00, 0x2A]; // 42
        assert_eq!(data.read_i32(0), 42);
    }

    #[test]
    fn slice_read_i64() {
        let v: i64 = 1_710_000_000_000;
        let mut buf = vec![0u8; 8];
        BigEndian::write_i64(&mut buf, v);
        let data: &[u8] = &buf;
        assert_eq!(data.read_i64(0), v);
    }

    #[test]
    fn slice_size_empty() {
        let data: &[u8] = &[];
        assert_eq!(data.size(), 0);
        assert!(data.is_empty());
    }

    #[test]
    fn bytes_read_i32() {
        let b = Bytes::from_static(&[0x00, 0x00, 0x00, 0x2A]);
        assert_eq!(b.read_i32(0), 42);
    }

    #[test]
    fn compare_ordering() {
        let a: &[u8] = &[0x00, 0x00, 0x00, 0x01];
        let b: &[u8] = &[0x00, 0x00, 0x00, 0x02];
        // &a: &&[u8] coerces to &dyn ValueAccessor since &[u8]: ValueAccessor
        assert_eq!(a.compare(&b), Ordering::Less);
        assert_eq!(b.compare(&a), Ordering::Greater);
        assert_eq!(a.compare(&a), Ordering::Equal);
    }
}
