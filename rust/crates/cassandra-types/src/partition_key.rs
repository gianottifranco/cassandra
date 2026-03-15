// Licensed under Apache License, Version 2.0.

//! Composite partition key encoding/decoding.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.db.marshal.CompositeType`
//!
//! ## Wire Format
//! Multi-column partition keys are encoded as:
//!   [2-byte length][value bytes][0x00 EOC]  for each component

use byteorder::{BigEndian, ByteOrder};

/// Encode a composite partition key from individual components.
pub fn encode_composite(components: &[&[u8]]) -> Vec<u8> {
    if components.len() == 1 {
        return components[0].to_vec();
    }
    let mut result = Vec::new();
    for component in components {
        let mut len_buf = [0u8; 2];
        BigEndian::write_u16(&mut len_buf, component.len() as u16);
        result.extend_from_slice(&len_buf);
        result.extend_from_slice(component);
        result.push(0x00); // end-of-component byte
    }
    result
}

/// Decode a composite partition key into individual components.
/// Returns `None` if the data doesn't look like a composite key.
pub fn decode_composite(data: &[u8], expected_components: usize) -> Option<Vec<Vec<u8>>> {
    if expected_components <= 1 {
        return Some(vec![data.to_vec()]);
    }
    let mut result = Vec::with_capacity(expected_components);
    let mut offset = 0;
    while offset < data.len() && result.len() < expected_components {
        if offset + 2 > data.len() { return None; }
        let len = BigEndian::read_u16(&data[offset..offset + 2]) as usize;
        offset += 2;
        if offset + len > data.len() { return None; }
        result.push(data[offset..offset + len].to_vec());
        offset += len;
        if offset < data.len() {
            offset += 1; // skip EOC byte
        }
    }
    if result.len() == expected_components { Some(result) } else { None }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_component_passthrough() {
        let data = vec![0x01, 0x02, 0x03];
        let encoded = encode_composite(&[&data]);
        assert_eq!(encoded, data);
    }

    #[test]
    fn multi_component_round_trip() {
        let c1 = vec![0x00, 0x00, 0x00, 0x01]; // int 1
        let c2 = b"hello".to_vec();
        let encoded = encode_composite(&[&c1, &c2]);
        let decoded = decode_composite(&encoded, 2).unwrap();
        assert_eq!(decoded, vec![c1, c2]);
    }

    #[test]
    fn three_components() {
        let c1 = 42i32.to_be_bytes().to_vec();
        let c2 = b"abc".to_vec();
        let c3 = 100i64.to_be_bytes().to_vec();
        let encoded = encode_composite(&[&c1, &c2, &c3]);
        let decoded = decode_composite(&encoded, 3).unwrap();
        assert_eq!(decoded, vec![c1, c2, c3]);
    }

    #[test]
    fn wrong_component_count_fails() {
        let c1 = 42i32.to_be_bytes().to_vec();
        let encoded = encode_composite(&[&c1]);
        assert!(decode_composite(&encoded, 2).is_none() || decode_composite(&encoded, 2).unwrap().len() != 2);
    }
}
