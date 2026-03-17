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

//! CQL native protocol primitive data types.
//!
//! Implements reading/writing of the fundamental wire types defined in the
//! CQL binary protocol specification (section 3).
//!
//! ## Java Oracle
//! - `org.apache.cassandra.transport.CBUtil`
//! - `org.apache.cassandra.transport.DataType`

use byteorder::{BigEndian, ReadBytesExt};
use bytes::{BufMut, BytesMut};
use std::collections::HashMap;
use std::io::{self, Read};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

/// Read a protocol [int] (4-byte signed big-endian).
pub fn read_int(cursor: &mut &[u8]) -> io::Result<i32> {
    cursor.read_i32::<BigEndian>()
}

/// Read a protocol [long] (8-byte signed big-endian).
pub fn read_long(cursor: &mut &[u8]) -> io::Result<i64> {
    cursor.read_i64::<BigEndian>()
}

/// Read a protocol [short] (2-byte unsigned big-endian).
pub fn read_short(cursor: &mut &[u8]) -> io::Result<u16> {
    cursor.read_u16::<BigEndian>()
}

/// Read a protocol [byte] (single unsigned byte).
pub fn read_byte(cursor: &mut &[u8]) -> io::Result<u8> {
    cursor.read_u8()
}

/// Read a protocol [string] (short-length-prefixed UTF-8).
pub fn read_string(cursor: &mut &[u8]) -> io::Result<String> {
    let len = read_short(cursor)? as usize;
    let mut buf = vec![0u8; len];
    cursor.read_exact(&mut buf)?;
    String::from_utf8(buf).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

/// Read a protocol [long string] (int-length-prefixed UTF-8).
pub fn read_long_string(cursor: &mut &[u8]) -> io::Result<String> {
    let len = read_int(cursor)? as usize;
    let mut buf = vec![0u8; len];
    cursor.read_exact(&mut buf)?;
    String::from_utf8(buf).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

/// Read a protocol [bytes] (int-length-prefixed byte sequence, -1 for null).
pub fn read_bytes(cursor: &mut &[u8]) -> io::Result<Option<Vec<u8>>> {
    let len = read_int(cursor)?;
    if len < 0 {
        return Ok(None);
    }
    let len = len as usize;
    let mut buf = vec![0u8; len];
    cursor.read_exact(&mut buf)?;
    Ok(Some(buf))
}

/// Read a protocol [short bytes] (short-length-prefixed byte sequence).
pub fn read_short_bytes(cursor: &mut &[u8]) -> io::Result<Vec<u8>> {
    let len = read_short(cursor)? as usize;
    let mut buf = vec![0u8; len];
    cursor.read_exact(&mut buf)?;
    Ok(buf)
}

/// Read a protocol [uuid] (16 bytes).
pub fn read_uuid(cursor: &mut &[u8]) -> io::Result<[u8; 16]> {
    let mut buf = [0u8; 16];
    cursor.read_exact(&mut buf)?;
    Ok(buf)
}

/// Read a protocol [string list].
pub fn read_string_list(cursor: &mut &[u8]) -> io::Result<Vec<String>> {
    let n = read_short(cursor)? as usize;
    let mut list = Vec::with_capacity(n);
    for _ in 0..n {
        list.push(read_string(cursor)?);
    }
    Ok(list)
}

/// Read a protocol [string map].
pub fn read_string_map(cursor: &mut &[u8]) -> io::Result<HashMap<String, String>> {
    let n = read_short(cursor)? as usize;
    let mut map = HashMap::with_capacity(n);
    for _ in 0..n {
        let key = read_string(cursor)?;
        let value = read_string(cursor)?;
        map.insert(key, value);
    }
    Ok(map)
}

/// Read a protocol [bytes map] (short-length-prefixed map of string → bytes).
/// Used for custom payloads in the CQL native protocol.
pub fn read_bytes_map(cursor: &mut &[u8]) -> io::Result<HashMap<String, Vec<u8>>> {
    let n = read_short(cursor)? as usize;
    let mut map = HashMap::with_capacity(n);
    for _ in 0..n {
        let key = read_string(cursor)?;
        let value = read_bytes(cursor)?.unwrap_or_default();
        map.insert(key, value);
    }
    Ok(map)
}

/// Read a protocol [string multimap].
pub fn read_string_multimap(cursor: &mut &[u8]) -> io::Result<HashMap<String, Vec<String>>> {
    let n = read_short(cursor)? as usize;
    let mut map = HashMap::with_capacity(n);
    for _ in 0..n {
        let key = read_string(cursor)?;
        let values = read_string_list(cursor)?;
        map.insert(key, values);
    }
    Ok(map)
}

/// Read a protocol [inet] (1-byte size + address bytes + 4-byte port).
pub fn read_inet(cursor: &mut &[u8]) -> io::Result<(IpAddr, u32)> {
    let size = read_byte(cursor)?;
    let addr = match size {
        4 => {
            let mut b = [0u8; 4];
            cursor.read_exact(&mut b)?;
            IpAddr::V4(Ipv4Addr::from(b))
        }
        16 => {
            let mut b = [0u8; 16];
            cursor.read_exact(&mut b)?;
            IpAddr::V6(Ipv6Addr::from(b))
        }
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid inet size: {}", size),
            ));
        }
    };
    let port = read_int(cursor)? as u32;
    Ok((addr, port))
}

/// CQL consistency level as wire value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u16)]
pub enum Consistency {
    Any = 0x0000,
    One = 0x0001,
    Two = 0x0002,
    Three = 0x0003,
    Quorum = 0x0004,
    All = 0x0005,
    LocalQuorum = 0x0006,
    EachQuorum = 0x0007,
    Serial = 0x0008,
    LocalSerial = 0x0009,
    LocalOne = 0x000A,
}

impl Consistency {
    pub fn from_u16(v: u16) -> io::Result<Self> {
        match v {
            0x0000 => Ok(Consistency::Any),
            0x0001 => Ok(Consistency::One),
            0x0002 => Ok(Consistency::Two),
            0x0003 => Ok(Consistency::Three),
            0x0004 => Ok(Consistency::Quorum),
            0x0005 => Ok(Consistency::All),
            0x0006 => Ok(Consistency::LocalQuorum),
            0x0007 => Ok(Consistency::EachQuorum),
            0x0008 => Ok(Consistency::Serial),
            0x0009 => Ok(Consistency::LocalSerial),
            0x000A => Ok(Consistency::LocalOne),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unknown consistency: 0x{:04X}", v),
            )),
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            Consistency::Any => "ANY",
            Consistency::One => "ONE",
            Consistency::Two => "TWO",
            Consistency::Three => "THREE",
            Consistency::Quorum => "QUORUM",
            Consistency::All => "ALL",
            Consistency::LocalQuorum => "LOCAL_QUORUM",
            Consistency::EachQuorum => "EACH_QUORUM",
            Consistency::Serial => "SERIAL",
            Consistency::LocalSerial => "LOCAL_SERIAL",
            Consistency::LocalOne => "LOCAL_ONE",
        }
    }
}

/// Read a consistency level from the wire.
pub fn read_consistency(cursor: &mut &[u8]) -> io::Result<Consistency> {
    let v = read_short(cursor)?;
    Consistency::from_u16(v)
}

// ─── Write helpers ──────────────────────────────────────────────────────────

/// Write a protocol [int].
pub fn write_int(buf: &mut BytesMut, v: i32) {
    buf.put_i32(v);
}

/// Write a protocol [long].
pub fn write_long(buf: &mut BytesMut, v: i64) {
    buf.put_i64(v);
}

/// Write a protocol [short].
pub fn write_short(buf: &mut BytesMut, v: u16) {
    buf.put_u16(v);
}

/// Write a protocol [byte].
pub fn write_byte(buf: &mut BytesMut, v: u8) {
    buf.put_u8(v);
}

/// Write a protocol [string].
pub fn write_string(buf: &mut BytesMut, s: &str) {
    write_short(buf, s.len() as u16);
    buf.extend_from_slice(s.as_bytes());
}

/// Write a protocol [long string].
pub fn write_long_string(buf: &mut BytesMut, s: &str) {
    write_int(buf, s.len() as i32);
    buf.extend_from_slice(s.as_bytes());
}

/// Write a protocol [bytes] (None → -1 length).
pub fn write_bytes_opt(buf: &mut BytesMut, data: Option<&[u8]>) {
    match data {
        Some(d) => {
            write_int(buf, d.len() as i32);
            buf.extend_from_slice(d);
        }
        None => write_int(buf, -1),
    }
}

/// Write a protocol [short bytes].
pub fn write_short_bytes(buf: &mut BytesMut, data: &[u8]) {
    write_short(buf, data.len() as u16);
    buf.extend_from_slice(data);
}

/// Write a protocol [uuid].
pub fn write_uuid(buf: &mut BytesMut, uuid: &[u8; 16]) {
    buf.extend_from_slice(uuid);
}

/// Write a protocol [string list].
pub fn write_string_list(buf: &mut BytesMut, list: &[String]) {
    write_short(buf, list.len() as u16);
    for s in list {
        write_string(buf, s);
    }
}

/// Write a protocol [string map].
pub fn write_string_map(buf: &mut BytesMut, map: &HashMap<String, String>) {
    write_short(buf, map.len() as u16);
    for (k, v) in map {
        write_string(buf, k);
        write_string(buf, v);
    }
}

/// Write a protocol [string multimap].
pub fn write_string_multimap(buf: &mut BytesMut, map: &HashMap<String, Vec<String>>) {
    write_short(buf, map.len() as u16);
    for (k, vs) in map {
        write_string(buf, k);
        write_string_list(buf, vs);
    }
}

/// Write a consistency level.
pub fn write_consistency(buf: &mut BytesMut, c: Consistency) {
    write_short(buf, c as u16);
}

/// Write a protocol [inet].
pub fn write_inet(buf: &mut BytesMut, addr: IpAddr, port: u32) {
    match addr {
        IpAddr::V4(v4) => {
            write_byte(buf, 4);
            buf.extend_from_slice(&v4.octets());
        }
        IpAddr::V6(v6) => {
            write_byte(buf, 16);
            buf.extend_from_slice(&v6.octets());
        }
    }
    write_int(buf, port as i32);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_int() {
        let mut buf = BytesMut::new();
        write_int(&mut buf, 42);
        let mut cursor: &[u8] = &buf;
        assert_eq!(read_int(&mut cursor).unwrap(), 42);
    }

    #[test]
    fn roundtrip_string() {
        let mut buf = BytesMut::new();
        write_string(&mut buf, "hello");
        let mut cursor: &[u8] = &buf;
        assert_eq!(read_string(&mut cursor).unwrap(), "hello");
    }

    #[test]
    fn roundtrip_bytes_some() {
        let mut buf = BytesMut::new();
        write_bytes_opt(&mut buf, Some(&[0xDE, 0xAD]));
        let mut cursor: &[u8] = &buf;
        assert_eq!(read_bytes(&mut cursor).unwrap(), Some(vec![0xDE, 0xAD]));
    }

    #[test]
    fn roundtrip_bytes_none() {
        let mut buf = BytesMut::new();
        write_bytes_opt(&mut buf, None);
        let mut cursor: &[u8] = &buf;
        assert_eq!(read_bytes(&mut cursor).unwrap(), None);
    }

    #[test]
    fn roundtrip_string_map() {
        let mut orig = HashMap::new();
        orig.insert("CQL_VERSION".to_string(), "3.4.7".to_string());
        let mut buf = BytesMut::new();
        write_string_map(&mut buf, &orig);
        let mut cursor: &[u8] = &buf;
        let decoded = read_string_map(&mut cursor).unwrap();
        assert_eq!(decoded, orig);
    }

    #[test]
    fn roundtrip_consistency() {
        let mut buf = BytesMut::new();
        write_consistency(&mut buf, Consistency::Quorum);
        let mut cursor: &[u8] = &buf;
        assert_eq!(read_consistency(&mut cursor).unwrap(), Consistency::Quorum);
    }

    #[test]
    fn roundtrip_uuid() {
        let uuid = [1u8; 16];
        let mut buf = BytesMut::new();
        write_uuid(&mut buf, &uuid);
        let mut cursor: &[u8] = &buf;
        assert_eq!(read_uuid(&mut cursor).unwrap(), uuid);
    }

    #[test]
    fn consistency_names() {
        assert_eq!(Consistency::One.name(), "ONE");
        assert_eq!(Consistency::LocalQuorum.name(), "LOCAL_QUORUM");
    }

    #[test]
    fn golden_int_wire_format() {
        let mut buf = BytesMut::new();
        write_int(&mut buf, 256);
        assert_eq!(&buf[..], &[0x00, 0x00, 0x01, 0x00]);
    }

    #[test]
    fn golden_string_wire_format() {
        let mut buf = BytesMut::new();
        write_string(&mut buf, "CQL");
        // 0x0003 length + "CQL"
        assert_eq!(&buf[..], &[0x00, 0x03, 0x43, 0x51, 0x4C]);
    }
}
