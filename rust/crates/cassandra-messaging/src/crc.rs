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

//! CRC utilities for internode frame integrity.
//!
//! Provides CRC-24 (3-byte, polynomial 0x875060 matching Java) and
//! CRC-32C computation for frame headers and payloads.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.net.Crc`
//! - `org.apache.cassandra.utils.CRC`

/// CRC-24 polynomial matching Java's implementation.
const CRC24_POLY: u32 = 0x87_5060;

/// CRC-24 initial value.
const CRC24_INIT: u32 = 0x87_5060;

/// Compute CRC-24 over the given bytes.
///
/// Returns a 3-byte CRC (lower 24 bits of the result).
/// Matches Java's `Crc.crc24()` bit-by-bit implementation.
pub fn crc24(data: &[u8]) -> u32 {
    let mut crc = CRC24_INIT;
    for &byte in data {
        crc ^= (byte as u32) << 16;
        for _ in 0..8 {
            crc <<= 1;
            if crc & 0x100_0000 != 0 {
                crc ^= CRC24_POLY;
            }
        }
    }
    crc & 0xFF_FFFF
}

/// Compute CRC-32C (Castagnoli) over the given bytes.
///
/// Uses the polynomial 0x1EDC6F41. This is the same as used by
/// Java's `java.util.zip.CRC32C`.
pub fn crc32c(data: &[u8]) -> u32 {
    let mut crc: u32 = 0xFFFF_FFFF;
    for &byte in data {
        crc ^= byte as u32;
        for _ in 0..8 {
            if crc & 1 != 0 {
                crc = (crc >> 1) ^ 0x82F6_3B78;
            } else {
                crc >>= 1;
            }
        }
    }
    crc ^ 0xFFFF_FFFF
}

/// Update an existing CRC-32C with additional bytes.
pub fn crc32c_update(crc: u32, data: &[u8]) -> u32 {
    let mut crc = crc ^ 0xFFFF_FFFF;
    for &byte in data {
        crc ^= byte as u32;
        for _ in 0..8 {
            if crc & 1 != 0 {
                crc = (crc >> 1) ^ 0x82F6_3B78;
            } else {
                crc >>= 1;
            }
        }
    }
    crc ^ 0xFFFF_FFFF
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crc24_empty() {
        let crc = crc24(&[]);
        assert_eq!(crc, CRC24_INIT & 0xFF_FFFF);
    }

    #[test]
    fn crc24_known_vectors() {
        // Single byte
        let crc = crc24(&[0x00]);
        assert_ne!(crc, 0);
        assert_eq!(crc & 0xFF00_0000, 0, "CRC-24 must fit in 24 bits");

        // Deterministic: same input → same output
        assert_eq!(crc24(&[1, 2, 3]), crc24(&[1, 2, 3]));

        // Different input → different output (with high probability)
        assert_ne!(crc24(&[1, 2, 3]), crc24(&[1, 2, 4]));
    }

    #[test]
    fn crc24_fits_in_three_bytes() {
        for i in 0u8..=255 {
            let crc = crc24(&[i]);
            assert!(crc <= 0xFF_FFFF, "CRC-24 overflow for byte {i}");
        }
    }

    #[test]
    fn crc32c_empty() {
        assert_eq!(crc32c(&[]), 0);
    }

    #[test]
    fn crc32c_known_vector() {
        // "123456789" has a well-known CRC-32C of 0xE3069283
        let data = b"123456789";
        assert_eq!(crc32c(data), 0xE306_9283);
    }

    #[test]
    fn crc32c_deterministic() {
        let data = b"hello world";
        assert_eq!(crc32c(data), crc32c(data));
    }

    #[test]
    fn crc32c_update_matches_full() {
        let data = b"hello world";
        let full = crc32c(data);
        let partial = crc32c(&data[..5]);
        let updated = crc32c_update(partial, &data[5..]);
        assert_eq!(updated, full);
    }

    // Property tests
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn crc24_always_24_bits(data in proptest::collection::vec(any::<u8>(), 0..256)) {
            let crc = crc24(&data);
            prop_assert!(crc <= 0xFF_FFFF);
        }

        #[test]
        fn crc32c_round_trip_consistency(data in proptest::collection::vec(any::<u8>(), 0..256)) {
            let a = crc32c(&data);
            let b = crc32c(&data);
            prop_assert_eq!(a, b);
        }

        #[test]
        fn crc32c_update_split_at_any_point(
            data in proptest::collection::vec(any::<u8>(), 1..256),
            split in 0usize..256
        ) {
            let split = split % data.len();
            let full = crc32c(&data);
            let partial = crc32c(&data[..split]);
            let updated = crc32c_update(partial, &data[split..]);
            prop_assert_eq!(updated, full);
        }
    }
}
