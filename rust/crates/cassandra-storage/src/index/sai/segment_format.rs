// Licensed under Apache License, Version 2.0.

//! SAI on-disk segment format for persisting index data alongside SSTables.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.index.sai.disk.v1.SAICodecUtils`
//! - `org.apache.cassandra.index.sai.disk.v1.segment.SegmentMetadata`
//!
//! ## Format
//!
//! ```text
//! Header:
//!   magic: [u8; 4]   = b"SAI1"
//!   version: u32      = 1
//!   term_count: u32
//!   row_count: u64
//!
//! Entries (repeated term_count times):
//!   term_len: u32
//!   term_bytes: [u8; term_len]
//!   posting_count: u32
//!   posting_locations (repeated posting_count times):
//!     pk_len: u32
//!     pk_bytes: [u8; pk_len]
//!     ck_len: u32
//!     ck_bytes: [u8; ck_len]
//! ```

use std::collections::BTreeMap;
use std::io::{self, Read, Write};
use std::path::Path;

use super::builder::SaiSegment;
use super::posting::PostingList;

const MAGIC: &[u8; 4] = b"SAI1";
const VERSION: u32 = 1;

/// Write a SAI segment to a binary file.
pub fn write_segment(path: &Path, segment: &SaiSegment) -> io::Result<()> {
    let mut file = std::fs::File::create(path)?;
    write_segment_to(&mut file, segment)
}

/// Write a SAI segment to any writer.
pub fn write_segment_to<W: Write>(w: &mut W, segment: &SaiSegment) -> io::Result<()> {
    // Header
    w.write_all(MAGIC)?;
    w.write_all(&VERSION.to_be_bytes())?;
    w.write_all(&(segment.terms.len() as u32).to_be_bytes())?;
    w.write_all(&segment.row_count.to_be_bytes())?;

    // Entries
    for (term, posting_list) in &segment.terms {
        w.write_all(&(term.len() as u32).to_be_bytes())?;
        w.write_all(term)?;

        let locations = posting_list.locations();
        w.write_all(&(locations.len() as u32).to_be_bytes())?;

        for loc in locations {
            w.write_all(&(loc.partition_key.len() as u32).to_be_bytes())?;
            w.write_all(&loc.partition_key)?;
            w.write_all(&(loc.clustering_key.len() as u32).to_be_bytes())?;
            w.write_all(&loc.clustering_key)?;
        }
    }

    Ok(())
}

/// Read a SAI segment from a binary file.
pub fn read_segment(path: &Path) -> io::Result<SaiSegment> {
    let mut file = std::fs::File::open(path)?;
    read_segment_from(&mut file)
}

/// Read a SAI segment from any reader.
pub fn read_segment_from<R: Read>(r: &mut R) -> io::Result<SaiSegment> {
    // Header
    let mut magic = [0u8; 4];
    r.read_exact(&mut magic)?;
    if &magic != MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid SAI segment magic: {:?}", magic),
        ));
    }

    let version = read_u32(r)?;
    if version != VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unsupported SAI segment version: {}", version),
        ));
    }

    let term_count = read_u32(r)?;
    let row_count = read_u64(r)?;

    // Entries
    let mut terms = BTreeMap::new();
    for _ in 0..term_count {
        let term_len = read_u32(r)? as usize;
        let mut term = vec![0u8; term_len];
        r.read_exact(&mut term)?;

        let posting_count = read_u32(r)?;
        let mut pl = PostingList::new();

        for _ in 0..posting_count {
            let pk_len = read_u32(r)? as usize;
            let mut pk = vec![0u8; pk_len];
            r.read_exact(&mut pk)?;

            let ck_len = read_u32(r)? as usize;
            let mut ck = vec![0u8; ck_len];
            r.read_exact(&mut ck)?;

            pl.add(pk, ck);
        }

        terms.insert(term, pl);
    }

    Ok(SaiSegment {
        sstable_generation: 0, // Not stored in segment file; caller sets it
        index_name: String::new(),
        column: String::new(),
        terms,
        row_count,
    })
}

fn read_u32<R: Read>(r: &mut R) -> io::Result<u32> {
    let mut buf = [0u8; 4];
    r.read_exact(&mut buf)?;
    Ok(u32::from_be_bytes(buf))
}

fn read_u64<R: Read>(r: &mut R) -> io::Result<u64> {
    let mut buf = [0u8; 8];
    r.read_exact(&mut buf)?;
    Ok(u64::from_be_bytes(buf))
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::builder::SaiSegmentBuilder;

    #[test]
    fn round_trip_in_memory() {
        let mut builder = SaiSegmentBuilder::new(42, "idx_age", "age");
        builder.add(b"term1".to_vec(), b"pk1".to_vec(), b"ck1".to_vec());
        builder.add(b"term1".to_vec(), b"pk2".to_vec(), b"ck2".to_vec());
        builder.add(b"term2".to_vec(), b"pk3".to_vec(), b"".to_vec());
        let segment = builder.build();

        let mut buf = Vec::new();
        write_segment_to(&mut buf, &segment).unwrap();

        let mut cursor = std::io::Cursor::new(&buf);
        let decoded = read_segment_from(&mut cursor).unwrap();

        assert_eq!(decoded.terms.len(), 2);
        assert_eq!(decoded.row_count, segment.row_count);
        assert_eq!(decoded.terms[b"term1".as_slice()].len(), 2);
        assert_eq!(decoded.terms[b"term2".as_slice()].len(), 1);
    }

    #[test]
    fn round_trip_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.sai");

        let mut builder = SaiSegmentBuilder::new(1, "idx", "col");
        builder.add(b"hello".to_vec(), b"pk".to_vec(), b"ck".to_vec());
        let segment = builder.build();

        write_segment(&path, &segment).unwrap();
        let decoded = read_segment(&path).unwrap();

        assert_eq!(decoded.terms.len(), 1);
        assert_eq!(decoded.terms[b"hello".as_slice()].len(), 1);
    }

    #[test]
    fn invalid_magic_rejected() {
        let bad_data = b"BAD1\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00";
        let mut cursor = std::io::Cursor::new(&bad_data[..]);
        let result = read_segment_from(&mut cursor);
        assert!(result.is_err());
    }

    #[test]
    fn empty_segment_round_trip() {
        let segment = SaiSegment {
            sstable_generation: 0,
            index_name: String::new(),
            column: String::new(),
            terms: BTreeMap::new(),
            row_count: 0,
        };

        let mut buf = Vec::new();
        write_segment_to(&mut buf, &segment).unwrap();

        let mut cursor = std::io::Cursor::new(&buf);
        let decoded = read_segment_from(&mut cursor).unwrap();
        assert!(decoded.terms.is_empty());
        assert_eq!(decoded.row_count, 0);
    }
}
