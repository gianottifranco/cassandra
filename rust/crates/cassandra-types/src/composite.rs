// Licensed under Apache License, Version 2.0.

//! Legacy CompositeType wire format.
//!
//! CompositeType was used in Thrift-era schemas and for composite partition
//! keys.  Each component is encoded as `[u16 length][bytes][u8 eoc]` where
//! `eoc` (end-of-component) is typically 0x00 for normal, 0x01 for end-of-range
//! upper bound, and 0xFF for end-of-range lower bound.
//!
//! This is distinct from the Tuple wire format which uses i32 length prefixes
//! and no EOC bytes.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.db.marshal.CompositeType`

use byteorder::{BigEndian, ByteOrder};

/// End-of-component marker values.
pub const EOC_NONE: u8 = 0x00;
pub const EOC_END: u8 = 0x01;
pub const EOC_START: u8 = 0xFF;

/// A single component in a composite value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Component {
    pub data: Vec<u8>,
    pub eoc: u8,
}

/// Serialize a sequence of components into the legacy CompositeType wire format.
///
/// Wire format per component: `[u16 big-endian length][bytes][u8 eoc]`
pub fn serialize_composite(components: &[Component]) -> Vec<u8> {
    let mut buf = Vec::new();
    for c in components {
        let mut len_bytes = [0u8; 2];
        BigEndian::write_u16(&mut len_bytes, c.data.len() as u16);
        buf.extend_from_slice(&len_bytes);
        buf.extend_from_slice(&c.data);
        buf.push(c.eoc);
    }
    buf
}

/// Deserialize the legacy CompositeType wire format into components.
///
/// Returns an error string if the data is truncated.
pub fn deserialize_composite(data: &[u8]) -> Result<Vec<Component>, &'static str> {
    let mut pos = 0;
    let mut components = Vec::new();
    while pos < data.len() {
        if pos + 2 > data.len() {
            return Err("truncated component length");
        }
        let len = BigEndian::read_u16(&data[pos..]) as usize;
        pos += 2;
        if pos + len + 1 > data.len() {
            return Err("truncated component data or missing EOC");
        }
        let component_data = data[pos..pos + len].to_vec();
        pos += len;
        let eoc = data[pos];
        pos += 1;
        components.push(Component {
            data: component_data,
            eoc,
        });
    }
    Ok(components)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_single() {
        let components = vec![Component {
            data: vec![0x01, 0x02, 0x03],
            eoc: EOC_NONE,
        }];
        let bytes = serialize_composite(&components);
        assert_eq!(bytes, vec![0x00, 0x03, 0x01, 0x02, 0x03, 0x00]);
        let back = deserialize_composite(&bytes).unwrap();
        assert_eq!(back, components);
    }

    #[test]
    fn roundtrip_multiple() {
        let components = vec![
            Component {
                data: b"abc".to_vec(),
                eoc: EOC_NONE,
            },
            Component {
                data: vec![0xFF],
                eoc: EOC_END,
            },
        ];
        let bytes = serialize_composite(&components);
        let back = deserialize_composite(&bytes).unwrap();
        assert_eq!(back, components);
    }

    #[test]
    fn empty_components() {
        let components: Vec<Component> = vec![];
        let bytes = serialize_composite(&components);
        assert!(bytes.is_empty());
        let back = deserialize_composite(&bytes).unwrap();
        assert!(back.is_empty());
    }

    #[test]
    fn zero_length_component() {
        let components = vec![Component {
            data: vec![],
            eoc: EOC_START,
        }];
        let bytes = serialize_composite(&components);
        // [0x00, 0x00] len=0, no data, [0xFF] eoc
        assert_eq!(bytes, vec![0x00, 0x00, 0xFF]);
        let back = deserialize_composite(&bytes).unwrap();
        assert_eq!(back, components);
    }

    #[test]
    fn truncated_data_error() {
        // Only 2 bytes of length, no data
        assert!(deserialize_composite(&[0x00, 0x05, 0x01]).is_err());
    }

    #[test]
    fn eoc_values() {
        let c = Component {
            data: vec![1],
            eoc: EOC_END,
        };
        let bytes = serialize_composite(&[c]);
        // Last byte should be EOC_END (0x01)
        assert_eq!(*bytes.last().unwrap(), 0x01);
    }
}
