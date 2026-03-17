// Licensed under Apache License, Version 2.0.

//! On-disk serialization of range tombstones and partition deletions in Data.db.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.db.rows.RangeTombstoneMarker`
//! - `org.apache.cassandra.db.DeletionTime`
//!
//! ## Wire Format
//!
//! **Range tombstone:**
//! ```text
//! [0x02][start_len: u32][start][end_len: u32][end][marked_for_delete_at: i64][ldt: i32]
//! ```
//!
//! **Partition deletion:**
//! ```text
//! [0x03][marked_for_delete_at: i64][ldt: i32]
//! ```

use std::io::{self, Read, Write};

use byteorder::{BigEndian, ReadBytesExt};
use crc32fast::Hasher;

use cassandra_common::tombstone::{DeletionTime, RangeTombstone};

/// Marker byte for a range tombstone in Data.db.
pub const RANGE_TOMBSTONE_MARKER: u8 = 0x02;

/// Marker byte for a partition-level deletion in Data.db.
pub const PARTITION_DELETION_MARKER: u8 = 0x03;

/// Write a range tombstone to `writer`, updating `crc`. Returns bytes written.
pub fn write_range_tombstone<W: Write>(
    writer: &mut W,
    rt: &RangeTombstone,
    crc: &mut Hasher,
) -> io::Result<u64> {
    let mut written: u64 = 0;

    // Marker
    write_and_hash(writer, &[RANGE_TOMBSTONE_MARKER], crc)?;
    written += 1;

    // Start bound
    written += write_bytes_and_hash(writer, &rt.start, crc)?;

    // End bound
    written += write_bytes_and_hash(writer, &rt.end, crc)?;

    // Deletion time
    let ts_bytes = rt.deletion.marked_for_delete_at.to_be_bytes();
    write_and_hash(writer, &ts_bytes, crc)?;
    written += 8;

    let ldt_bytes = rt.deletion.local_deletion_time.to_be_bytes();
    write_and_hash(writer, &ldt_bytes, crc)?;
    written += 4;

    Ok(written)
}

/// Write a partition-level deletion to `writer`, updating `crc`. Returns bytes written.
pub fn write_partition_deletion<W: Write>(
    writer: &mut W,
    dt: &DeletionTime,
    crc: &mut Hasher,
) -> io::Result<u64> {
    let mut written: u64 = 0;

    // Marker
    write_and_hash(writer, &[PARTITION_DELETION_MARKER], crc)?;
    written += 1;

    // Deletion time
    let ts_bytes = dt.marked_for_delete_at.to_be_bytes();
    write_and_hash(writer, &ts_bytes, crc)?;
    written += 8;

    let ldt_bytes = dt.local_deletion_time.to_be_bytes();
    write_and_hash(writer, &ldt_bytes, crc)?;
    written += 4;

    Ok(written)
}

/// Read a range tombstone from `reader` (marker byte already consumed).
pub fn read_range_tombstone<R: Read>(reader: &mut R) -> io::Result<RangeTombstone> {
    let start = read_bytes(reader)?;
    let end = read_bytes(reader)?;
    let marked_for_delete_at = reader.read_i64::<BigEndian>()?;
    let local_deletion_time = reader.read_i32::<BigEndian>()?;

    Ok(RangeTombstone {
        start,
        end,
        deletion: DeletionTime::new(marked_for_delete_at, local_deletion_time),
    })
}

/// Read a partition deletion from `reader` (marker byte already consumed).
pub fn read_partition_deletion<R: Read>(reader: &mut R) -> io::Result<DeletionTime> {
    let marked_for_delete_at = reader.read_i64::<BigEndian>()?;
    let local_deletion_time = reader.read_i32::<BigEndian>()?;
    Ok(DeletionTime::new(marked_for_delete_at, local_deletion_time))
}

// ─── Internal helpers ──────────────────────────────────────────────────────

fn write_and_hash<W: Write>(w: &mut W, data: &[u8], crc: &mut Hasher) -> io::Result<()> {
    w.write_all(data)?;
    crc.update(data);
    Ok(())
}

fn write_bytes_and_hash<W: Write>(w: &mut W, data: &[u8], crc: &mut Hasher) -> io::Result<u64> {
    let len_bytes = (data.len() as u32).to_be_bytes();
    w.write_all(&len_bytes)?;
    crc.update(&len_bytes);
    w.write_all(data)?;
    crc.update(data);
    Ok(4 + data.len() as u64)
}

fn read_bytes<R: Read>(reader: &mut R) -> io::Result<Vec<u8>> {
    let len = reader.read_u32::<BigEndian>()? as usize;
    let mut buf = vec![0u8; len];
    reader.read_exact(&mut buf)?;
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn range_tombstone_round_trip() {
        let rt = RangeTombstone::new(
            b"start_key".to_vec(),
            b"end_key".to_vec(),
            DeletionTime::new(1_000_000, 1_700_000_000),
        );

        let mut buf = Vec::new();
        let mut crc = Hasher::new();
        let written = write_range_tombstone(&mut buf, &rt, &mut crc).unwrap();
        assert_eq!(written, buf.len() as u64);

        // Skip the marker byte, then read
        let mut cursor = Cursor::new(&buf[1..]);
        let decoded = read_range_tombstone(&mut cursor).unwrap();

        assert_eq!(decoded.start, rt.start);
        assert_eq!(decoded.end, rt.end);
        assert_eq!(decoded.deletion, rt.deletion);
    }

    #[test]
    fn partition_deletion_round_trip() {
        let dt = DeletionTime::new(500_000, 1_600_000_000);

        let mut buf = Vec::new();
        let mut crc = Hasher::new();
        let written = write_partition_deletion(&mut buf, &dt, &mut crc).unwrap();
        assert_eq!(written, buf.len() as u64);

        // Skip marker
        let mut cursor = Cursor::new(&buf[1..]);
        let decoded = read_partition_deletion(&mut cursor).unwrap();

        assert_eq!(decoded, dt);
    }

    #[test]
    fn range_tombstone_empty_keys() {
        let rt = RangeTombstone::new(
            Vec::new(),
            Vec::new(),
            DeletionTime::new(42, 100),
        );

        let mut buf = Vec::new();
        let mut crc = Hasher::new();
        write_range_tombstone(&mut buf, &rt, &mut crc).unwrap();

        let mut cursor = Cursor::new(&buf[1..]);
        let decoded = read_range_tombstone(&mut cursor).unwrap();

        assert!(decoded.start.is_empty());
        assert!(decoded.end.is_empty());
        assert_eq!(decoded.deletion.marked_for_delete_at, 42);
    }

    #[test]
    fn live_deletion_round_trip() {
        let dt = DeletionTime::LIVE;

        let mut buf = Vec::new();
        let mut crc = Hasher::new();
        write_partition_deletion(&mut buf, &dt, &mut crc).unwrap();

        let mut cursor = Cursor::new(&buf[1..]);
        let decoded = read_partition_deletion(&mut cursor).unwrap();

        assert!(decoded.is_live());
        assert_eq!(decoded, DeletionTime::LIVE);
    }

    #[test]
    fn crc_is_updated() {
        let rt = RangeTombstone::new(
            b"a".to_vec(),
            b"b".to_vec(),
            DeletionTime::new(1, 2),
        );

        let mut buf = Vec::new();
        let mut crc = Hasher::new();
        write_range_tombstone(&mut buf, &rt, &mut crc).unwrap();

        // CRC should be non-zero after writing data
        assert_ne!(crc.clone().finalize(), 0);
    }

    #[test]
    fn marker_bytes_correct() {
        let rt = RangeTombstone::new(
            b"s".to_vec(),
            b"e".to_vec(),
            DeletionTime::new(1, 2),
        );
        let mut buf = Vec::new();
        let mut crc = Hasher::new();
        write_range_tombstone(&mut buf, &rt, &mut crc).unwrap();
        assert_eq!(buf[0], RANGE_TOMBSTONE_MARKER);

        let dt = DeletionTime::new(1, 2);
        let mut buf2 = Vec::new();
        let mut crc2 = Hasher::new();
        write_partition_deletion(&mut buf2, &dt, &mut crc2).unwrap();
        assert_eq!(buf2[0], PARTITION_DELETION_MARKER);
    }
}
