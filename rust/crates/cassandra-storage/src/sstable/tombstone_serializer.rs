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
//! **Range tombstone marker:**
//! ```text
//! [0x04][marker_kind: u8][bound_kind: u8][bound_len: u32][bound]
//!       [marked_for_delete_at: i64][ldt: i32]
//!       [boundary_open_marked_for_delete_at: i64][boundary_open_ldt: i32]?
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

use crate::rows::unfiltered::{ClusteringBound, ClusteringBoundKind, RangeTombstoneMarker};

/// Marker byte for a range tombstone in Data.db.
pub const RANGE_TOMBSTONE_MARKER: u8 = 0x02;

/// Marker byte for a partition-level deletion in Data.db.
pub const PARTITION_DELETION_MARKER: u8 = 0x03;

/// Marker byte for an unfiltered range-tombstone bound marker in Data.db.
pub const RANGE_TOMBSTONE_BOUND_MARKER: u8 = 0x04;

const RT_MARKER_OPEN: u8 = 0;
const RT_MARKER_CLOSE: u8 = 1;
const RT_MARKER_BOUNDARY: u8 = 2;

const BOUND_INCLUSIVE_START: u8 = 0;
const BOUND_EXCLUSIVE_START: u8 = 1;
const BOUND_INCLUSIVE_END: u8 = 2;
const BOUND_EXCLUSIVE_END: u8 = 3;

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

/// Write an unfiltered range-tombstone marker to `writer`, updating `crc`.
///
/// This preserves the marker shape (`Open`, `Close`, or `Boundary`) and bound
/// inclusivity instead of collapsing it into a synthetic inclusive range.
pub fn write_range_tombstone_marker<W: Write>(
    writer: &mut W,
    marker: &RangeTombstoneMarker,
    crc: &mut Hasher,
) -> io::Result<u64> {
    let mut written: u64 = 0;

    write_and_hash(writer, &[RANGE_TOMBSTONE_BOUND_MARKER], crc)?;
    written += 1;

    match marker {
        RangeTombstoneMarker::Open { bound, deletion } => {
            write_and_hash(
                writer,
                &[RT_MARKER_OPEN, encode_bound_kind(bound.kind)],
                crc,
            )?;
            written += 2;
            written += write_bytes_and_hash(writer, &bound.values, crc)?;
            written += write_deletion_time(writer, *deletion, crc)?;
        }
        RangeTombstoneMarker::Close { bound, deletion } => {
            write_and_hash(
                writer,
                &[RT_MARKER_CLOSE, encode_bound_kind(bound.kind)],
                crc,
            )?;
            written += 2;
            written += write_bytes_and_hash(writer, &bound.values, crc)?;
            written += write_deletion_time(writer, *deletion, crc)?;
        }
        RangeTombstoneMarker::Boundary {
            bound,
            close_deletion,
            open_deletion,
        } => {
            write_and_hash(
                writer,
                &[RT_MARKER_BOUNDARY, encode_bound_kind(bound.kind)],
                crc,
            )?;
            written += 2;
            written += write_bytes_and_hash(writer, &bound.values, crc)?;
            written += write_deletion_time(writer, *close_deletion, crc)?;
            written += write_deletion_time(writer, *open_deletion, crc)?;
        }
    }

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

/// Read a range-tombstone marker from `reader` (marker byte already consumed).
pub fn read_range_tombstone_marker<R: Read>(reader: &mut R) -> io::Result<RangeTombstoneMarker> {
    let marker_kind = read_u8(reader)?;
    let bound_kind = decode_bound_kind(read_u8(reader)?)?;
    let bound = ClusteringBound {
        kind: bound_kind,
        values: read_bytes(reader)?,
    };
    let deletion = read_deletion_time(reader)?;

    match marker_kind {
        RT_MARKER_OPEN => Ok(RangeTombstoneMarker::Open { bound, deletion }),
        RT_MARKER_CLOSE => Ok(RangeTombstoneMarker::Close { bound, deletion }),
        RT_MARKER_BOUNDARY => Ok(RangeTombstoneMarker::Boundary {
            bound,
            close_deletion: deletion,
            open_deletion: read_deletion_time(reader)?,
        }),
        other => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid range tombstone marker kind: {other}"),
        )),
    }
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

fn write_deletion_time<W: Write>(
    writer: &mut W,
    deletion: DeletionTime,
    crc: &mut Hasher,
) -> io::Result<u64> {
    let ts_bytes = deletion.marked_for_delete_at.to_be_bytes();
    write_and_hash(writer, &ts_bytes, crc)?;
    let ldt_bytes = deletion.local_deletion_time.to_be_bytes();
    write_and_hash(writer, &ldt_bytes, crc)?;
    Ok(12)
}

fn read_u8<R: Read>(reader: &mut R) -> io::Result<u8> {
    let mut byte = [0u8; 1];
    reader.read_exact(&mut byte)?;
    Ok(byte[0])
}

fn read_bytes<R: Read>(reader: &mut R) -> io::Result<Vec<u8>> {
    let len = reader.read_u32::<BigEndian>()? as usize;
    let mut buf = vec![0u8; len];
    reader.read_exact(&mut buf)?;
    Ok(buf)
}

fn read_deletion_time<R: Read>(reader: &mut R) -> io::Result<DeletionTime> {
    let marked_for_delete_at = reader.read_i64::<BigEndian>()?;
    let local_deletion_time = reader.read_i32::<BigEndian>()?;
    Ok(DeletionTime::new(marked_for_delete_at, local_deletion_time))
}

fn encode_bound_kind(kind: ClusteringBoundKind) -> u8 {
    match kind {
        ClusteringBoundKind::InclusiveStart => BOUND_INCLUSIVE_START,
        ClusteringBoundKind::ExclusiveStart => BOUND_EXCLUSIVE_START,
        ClusteringBoundKind::InclusiveEnd => BOUND_INCLUSIVE_END,
        ClusteringBoundKind::ExclusiveEnd => BOUND_EXCLUSIVE_END,
    }
}

fn decode_bound_kind(kind: u8) -> io::Result<ClusteringBoundKind> {
    match kind {
        BOUND_INCLUSIVE_START => Ok(ClusteringBoundKind::InclusiveStart),
        BOUND_EXCLUSIVE_START => Ok(ClusteringBoundKind::ExclusiveStart),
        BOUND_INCLUSIVE_END => Ok(ClusteringBoundKind::InclusiveEnd),
        BOUND_EXCLUSIVE_END => Ok(ClusteringBoundKind::ExclusiveEnd),
        other => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid range tombstone bound kind: {other}"),
        )),
    }
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
    fn range_tombstone_open_marker_round_trip() {
        let marker = RangeTombstoneMarker::Open {
            bound: ClusteringBound {
                kind: ClusteringBoundKind::InclusiveStart,
                values: b"ck1".to_vec(),
            },
            deletion: DeletionTime::new(100, 10),
        };

        let mut buf = Vec::new();
        let mut crc = Hasher::new();
        let written = write_range_tombstone_marker(&mut buf, &marker, &mut crc).unwrap();
        assert_eq!(written, buf.len() as u64);
        assert_eq!(buf[0], RANGE_TOMBSTONE_BOUND_MARKER);

        let mut cursor = Cursor::new(&buf[1..]);
        let decoded = read_range_tombstone_marker(&mut cursor).unwrap();
        assert_eq!(decoded, marker);
    }

    #[test]
    fn range_tombstone_close_marker_round_trip() {
        let marker = RangeTombstoneMarker::Close {
            bound: ClusteringBound {
                kind: ClusteringBoundKind::ExclusiveEnd,
                values: b"ck9".to_vec(),
            },
            deletion: DeletionTime::new(200, 20),
        };

        let mut buf = Vec::new();
        let mut crc = Hasher::new();
        write_range_tombstone_marker(&mut buf, &marker, &mut crc).unwrap();

        let mut cursor = Cursor::new(&buf[1..]);
        let decoded = read_range_tombstone_marker(&mut cursor).unwrap();
        assert_eq!(decoded, marker);
    }

    #[test]
    fn range_tombstone_boundary_marker_round_trip() {
        let marker = RangeTombstoneMarker::Boundary {
            bound: ClusteringBound {
                kind: ClusteringBoundKind::InclusiveEnd,
                values: b"ck5".to_vec(),
            },
            close_deletion: DeletionTime::new(300, 30),
            open_deletion: DeletionTime::new(400, 40),
        };

        let mut buf = Vec::new();
        let mut crc = Hasher::new();
        write_range_tombstone_marker(&mut buf, &marker, &mut crc).unwrap();

        let mut cursor = Cursor::new(&buf[1..]);
        let decoded = read_range_tombstone_marker(&mut cursor).unwrap();
        assert_eq!(decoded, marker);
    }

    #[test]
    fn range_tombstone_marker_rejects_invalid_marker_kind() {
        let mut encoded = vec![9, BOUND_INCLUSIVE_START];
        encoded.extend_from_slice(&0u32.to_be_bytes());
        encoded.extend_from_slice(&DeletionTime::new(1, 2).marked_for_delete_at.to_be_bytes());
        encoded.extend_from_slice(&DeletionTime::new(1, 2).local_deletion_time.to_be_bytes());

        let mut cursor = Cursor::new(encoded);
        let err = read_range_tombstone_marker(&mut cursor).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn range_tombstone_empty_keys() {
        let rt = RangeTombstone::new(Vec::new(), Vec::new(), DeletionTime::new(42, 100));

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
        let rt = RangeTombstone::new(b"a".to_vec(), b"b".to_vec(), DeletionTime::new(1, 2));

        let mut buf = Vec::new();
        let mut crc = Hasher::new();
        write_range_tombstone(&mut buf, &rt, &mut crc).unwrap();

        // CRC should be non-zero after writing data
        assert_ne!(crc.clone().finalize(), 0);
    }

    #[test]
    fn marker_bytes_correct() {
        let rt = RangeTombstone::new(b"s".to_vec(), b"e".to_vec(), DeletionTime::new(1, 2));
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
