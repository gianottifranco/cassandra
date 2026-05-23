// Licensed under Apache License, Version 2.0.

#![allow(clippy::needless_range_loop)]

//! Partition-internal row index blocks.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.io.sstable.IndexInfo`
//! - `org.apache.cassandra.io.sstable.format.big.BigFormatPartitionWriter`
//! - `org.apache.cassandra.io.sstable.format.big.RowIndexEntry`

use std::io::{self, Cursor, Read, Write};

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use cassandra_io::util::varint::{read_unsigned_vint, read_vint, write_unsigned_vint, write_vint};
use cassandra_types::type_parser::parse_type;

const COLUMN_INDEX_MAGIC: [u8; 4] = *b"CIDX";
const COLUMN_INDEX_VERSION: u8 = 1;

/// Java `IndexInfo.Serializer.WIDTH_BASE`.
pub const JAVA_INDEX_INFO_WIDTH_BASE: i64 = 64 * 1024;

/// Cassandra's default `column_index_cache_size` from `Config`.
pub const JAVA_DEFAULT_COLUMN_INDEX_CACHE_SIZE: u32 = 2 * 1024;

/// Metadata for one partition-internal row index block.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexInfo {
    pub first_clustering: Vec<u8>,
    pub last_clustering: Vec<u8>,
    pub offset: u64,
    pub width: u64,
    pub row_count: u32,
}

impl IndexInfo {
    pub fn contains_clustering(&self, clustering: &[u8]) -> bool {
        self.first_clustering.as_slice() <= clustering
            && clustering <= self.last_clustering.as_slice()
    }
}

#[derive(Debug, Clone)]
struct PendingBlock {
    first_clustering: Vec<u8>,
    last_clustering: Vec<u8>,
    offset: u64,
    width: u64,
    row_count: u32,
}

impl PendingBlock {
    fn new(clustering: Vec<u8>, offset: u64, width: u64) -> Self {
        Self {
            first_clustering: clustering.clone(),
            last_clustering: clustering,
            offset,
            width,
            row_count: 1,
        }
    }

    fn add_row(&mut self, clustering: Vec<u8>, width: u64) {
        self.last_clustering = clustering;
        self.width += width;
        self.row_count += 1;
    }

    fn finish(self) -> IndexInfo {
        IndexInfo {
            first_clustering: self.first_clustering,
            last_clustering: self.last_clustering,
            offset: self.offset,
            width: self.width,
            row_count: self.row_count,
        }
    }
}

/// Builds `IndexInfo` blocks for rows inside one partition.
#[derive(Debug, Clone)]
pub struct ColumnIndexBuilder {
    block_size_threshold: u64,
    blocks: Vec<IndexInfo>,
    current: Option<PendingBlock>,
    next_offset: u64,
}

impl ColumnIndexBuilder {
    pub fn new(block_size_threshold: u64) -> Self {
        Self {
            block_size_threshold: block_size_threshold.max(1),
            blocks: Vec::new(),
            current: None,
            next_offset: 0,
        }
    }

    /// Add a row using the next sequential offset.
    pub fn add_row(&mut self, clustering: impl Into<Vec<u8>>, serialized_width: u64) {
        let offset = self.next_offset;
        self.add_row_at(clustering, offset, serialized_width);
        self.next_offset = offset.saturating_add(serialized_width);
    }

    /// Add a row at an explicit data-file-relative offset.
    pub fn add_row_at(
        &mut self,
        clustering: impl Into<Vec<u8>>,
        offset: u64,
        serialized_width: u64,
    ) {
        let clustering = clustering.into();
        let width = serialized_width.max(1);

        if self
            .current
            .as_ref()
            .is_some_and(|block| block.width >= self.block_size_threshold)
        {
            self.flush_current();
        }

        match self.current.as_mut() {
            Some(block) => block.add_row(clustering, width),
            None => self.current = Some(PendingBlock::new(clustering, offset, width)),
        }
    }

    pub fn finish(mut self) -> Vec<IndexInfo> {
        self.flush_current();
        self.blocks
    }

    pub fn is_empty(&self) -> bool {
        self.blocks.is_empty() && self.current.is_none()
    }

    pub fn block_count(&self) -> usize {
        self.blocks.len() + usize::from(self.current.is_some())
    }

    fn flush_current(&mut self) {
        if let Some(block) = self.current.take() {
            self.blocks.push(block.finish());
        }
    }
}

/// Find the index block that may contain the clustering key.
pub fn find_block<'a>(blocks: &'a [IndexInfo], clustering: &[u8]) -> Option<&'a IndexInfo> {
    blocks
        .binary_search_by(|block| {
            if clustering < block.first_clustering.as_slice() {
                std::cmp::Ordering::Greater
            } else if clustering > block.last_clustering.as_slice() {
                std::cmp::Ordering::Less
            } else {
                std::cmp::Ordering::Equal
            }
        })
        .ok()
        .map(|idx| &blocks[idx])
}

pub fn serialize_index_infos(blocks: &[IndexInfo]) -> io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    bytes.write_all(&COLUMN_INDEX_MAGIC)?;
    bytes.write_u8(COLUMN_INDEX_VERSION)?;
    bytes.write_u32::<BigEndian>(blocks.len() as u32)?;
    for block in blocks {
        write_bytes(&mut bytes, &block.first_clustering)?;
        write_bytes(&mut bytes, &block.last_clustering)?;
        bytes.write_u64::<BigEndian>(block.offset)?;
        bytes.write_u64::<BigEndian>(block.width)?;
        bytes.write_u32::<BigEndian>(block.row_count)?;
    }
    Ok(bytes)
}

pub fn deserialize_index_infos(data: &[u8]) -> io::Result<Vec<IndexInfo>> {
    let mut cursor = Cursor::new(data);
    let mut magic = [0u8; 4];
    cursor.read_exact(&mut magic)?;
    if magic != COLUMN_INDEX_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "column index magic mismatch",
        ));
    }
    let version = cursor.read_u8()?;
    if version != COLUMN_INDEX_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unsupported column index version: {version}"),
        ));
    }
    let count = cursor.read_u32::<BigEndian>()? as usize;
    let mut blocks = Vec::with_capacity(count);
    for _ in 0..count {
        let first_clustering = read_bytes(&mut cursor)?;
        let last_clustering = read_bytes(&mut cursor)?;
        let offset = cursor.read_u64::<BigEndian>()?;
        let width = cursor.read_u64::<BigEndian>()?;
        let row_count = cursor.read_u32::<BigEndian>()?;
        blocks.push(IndexInfo {
            first_clustering,
            last_clustering,
            offset,
            width,
            row_count,
        });
    }
    Ok(blocks)
}

/// Deletion time as serialized in Java Big-format `RowIndexEntry`.
///
/// Modern Java SSTables encode live deletion with a single `0x80` byte and
/// non-live deletion as a 12-byte big-endian timestamp/local-deletion-time
/// pair where the timestamp's sign bit is clear.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JavaBigDeletionTime {
    pub marked_for_delete_at: i64,
    pub local_deletion_time_unsigned: u32,
}

impl JavaBigDeletionTime {
    pub const LIVE: Self = Self {
        marked_for_delete_at: i64::MIN,
        local_deletion_time_unsigned: u32::MAX,
    };

    pub fn is_live(&self) -> bool {
        *self == Self::LIVE
    }

    fn serialized_size(&self) -> usize {
        if self.is_live() { 1 } else { 12 }
    }
}

/// Parsed Java Big-format primary-index row-index-entry kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JavaBigRowIndexEntryKind {
    Unindexed,
    Indexed,
    ShallowIndexed,
}

/// Java Big-format `RowIndexEntry` frame from `Index.db`.
///
/// The embedded `IndexInfo` records are schema-dependent because their
/// clustering prefixes require table comparator metadata. This parser validates
/// the Java frame and offset table, then exposes the raw `IndexInfo` slices so a
/// caller with schema metadata can decode those bodies separately.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JavaBigRowIndexEntry {
    pub data_file_position: u64,
    pub promoted_size: u32,
    pub kind: JavaBigRowIndexEntryKind,
    pub header_length: Option<u64>,
    pub deletion_time: Option<JavaBigDeletionTime>,
    pub column_index_count: u32,
    pub index_info_bytes: Vec<u8>,
    pub offsets: Vec<i32>,
}

impl JavaBigRowIndexEntry {
    pub fn is_indexed(&self) -> bool {
        self.kind != JavaBigRowIndexEntryKind::Unindexed
    }

    pub fn index_info_slice(&self, index: usize) -> Option<&[u8]> {
        let start = *self.offsets.get(index)? as usize;
        let end = self
            .offsets
            .get(index + 1)
            .map(|offset| *offset as usize)
            .unwrap_or(self.index_info_bytes.len());
        (start <= end && end <= self.index_info_bytes.len())
            .then_some(&self.index_info_bytes[start..end])
    }

    pub fn index_info_slices(&self) -> Vec<&[u8]> {
        (0..self.offsets.len())
            .filter_map(|idx| self.index_info_slice(idx))
            .collect()
    }
}

pub fn deserialize_java_big_row_index_entry(data: &[u8]) -> io::Result<JavaBigRowIndexEntry> {
    Ok(deserialize_java_big_row_index_entry_prefix_with_cache_size(
        data,
        JAVA_DEFAULT_COLUMN_INDEX_CACHE_SIZE,
    )?
    .0)
}

pub fn deserialize_java_big_row_index_entry_with_cache_size(
    data: &[u8],
    column_index_cache_size: u32,
) -> io::Result<JavaBigRowIndexEntry> {
    Ok(
        deserialize_java_big_row_index_entry_prefix_with_cache_size(data, column_index_cache_size)?
            .0,
    )
}

pub fn deserialize_java_big_row_index_entry_prefix(
    data: &[u8],
) -> io::Result<(JavaBigRowIndexEntry, usize)> {
    deserialize_java_big_row_index_entry_prefix_with_cache_size(
        data,
        JAVA_DEFAULT_COLUMN_INDEX_CACHE_SIZE,
    )
}

pub fn deserialize_java_big_row_index_entry_prefix_with_cache_size(
    data: &[u8],
    column_index_cache_size: u32,
) -> io::Result<(JavaBigRowIndexEntry, usize)> {
    let mut cursor = Cursor::new(data);
    let data_file_position = read_unsigned_vint(&mut cursor)?;
    let promoted_size = checked_u32(read_unsigned_vint(&mut cursor)?, "promoted index size")?;

    if promoted_size == 0 {
        let consumed = cursor.position() as usize;
        return Ok((
            JavaBigRowIndexEntry {
                data_file_position,
                promoted_size,
                kind: JavaBigRowIndexEntryKind::Unindexed,
                header_length: None,
                deletion_time: None,
                column_index_count: 0,
                index_info_bytes: Vec::new(),
                offsets: Vec::new(),
            },
            consumed,
        ));
    }

    let promoted_start = cursor.position() as usize;
    let promoted_end = promoted_start
        .checked_add(promoted_size as usize)
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "promoted index size overflows input position",
            )
        })?;
    if promoted_end > data.len() {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "promoted index size exceeds remaining row index entry bytes",
        ));
    }

    let header_length = read_unsigned_vint(&mut cursor)?;
    let deletion_time = read_java_big_deletion_time(&mut cursor)?;
    let column_index_count = checked_u32(read_unsigned_vint(&mut cursor)?, "column index count")?;
    if column_index_count <= 1 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "promoted Java Big row index entry must contain more than one IndexInfo",
        ));
    }

    let fields_end = cursor.position() as usize;
    let offset_table_bytes = checked_mul_usize(column_index_count as usize, 4, "offset table")?;
    if promoted_end < fields_end + offset_table_bytes {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "promoted index too small for Java Big offset table",
        ));
    }

    let index_info_end = promoted_end - offset_table_bytes;
    let mut index_info_bytes = vec![0u8; index_info_end - fields_end];
    cursor.read_exact(&mut index_info_bytes)?;

    let mut offsets = Vec::with_capacity(column_index_count as usize);
    for _ in 0..column_index_count {
        offsets.push(cursor.read_i32::<BigEndian>()?);
    }
    validate_java_big_offsets(&offsets, index_info_bytes.len())?;

    let kind = if promoted_size <= column_index_cache_size {
        JavaBigRowIndexEntryKind::Indexed
    } else {
        JavaBigRowIndexEntryKind::ShallowIndexed
    };

    Ok((
        JavaBigRowIndexEntry {
            data_file_position,
            promoted_size,
            kind,
            header_length: Some(header_length),
            deletion_time: Some(deletion_time),
            column_index_count,
            index_info_bytes,
            offsets,
        },
        promoted_end,
    ))
}

pub fn serialize_java_big_row_index_entry(entry: &JavaBigRowIndexEntry) -> io::Result<Vec<u8>> {
    let mut out = Vec::new();
    write_unsigned_vint(&mut out, entry.data_file_position)?;

    if !entry.is_indexed() {
        write_unsigned_vint(&mut out, 0)?;
        return Ok(out);
    }

    let header_length = entry.header_length.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "indexed Java Big row index entry missing header length",
        )
    })?;
    let deletion_time = entry.deletion_time.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "indexed Java Big row index entry missing deletion time",
        )
    })?;
    validate_java_big_offsets(&entry.offsets, entry.index_info_bytes.len())?;
    if entry.column_index_count as usize != entry.offsets.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "column index count does not match Java Big offset table length",
        ));
    }

    let fields_size = unsigned_vint_len(header_length)
        + deletion_time.serialized_size()
        + unsigned_vint_len(entry.column_index_count as u64);
    let promoted_size = checked_u32_usize(
        fields_size + entry.index_info_bytes.len() + entry.offsets.len() * 4,
        "promoted index size",
    )?;

    write_unsigned_vint(&mut out, promoted_size as u64)?;
    write_unsigned_vint(&mut out, header_length)?;
    write_java_big_deletion_time(&mut out, deletion_time)?;
    write_unsigned_vint(&mut out, entry.column_index_count as u64)?;
    out.write_all(&entry.index_info_bytes)?;
    for offset in &entry.offsets {
        out.write_i32::<BigEndian>(*offset)?;
    }
    Ok(out)
}

/// Java `IndexInfo` header fields that do not require clustering comparator metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JavaBigIndexInfoTail {
    pub offset: u64,
    pub width: u64,
    pub end_open_marker: Option<JavaBigDeletionTime>,
}

/// Schema-aware Java Big-format clustering prefix from an `IndexInfo` body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JavaBigClusteringPrefix {
    pub kind_ordinal: u8,
    pub values: Vec<Option<Vec<u8>>>,
}

/// Schema-aware Java Big-format `IndexInfo`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JavaBigIndexInfo {
    pub first_clustering: JavaBigClusteringPrefix,
    pub last_clustering: JavaBigClusteringPrefix,
    pub tail: JavaBigIndexInfoTail,
}

/// Decode a Java Big-format `IndexInfo` body using the table's clustering type specs.
pub fn read_java_big_index_info(
    data: &[u8],
    clustering_type_specs: &[String],
) -> io::Result<JavaBigIndexInfo> {
    let mut cursor = Cursor::new(data);
    let first_clustering = read_java_big_clustering_prefix(&mut cursor, clustering_type_specs)?;
    let last_clustering = read_java_big_clustering_prefix(&mut cursor, clustering_type_specs)?;
    let tail = read_java_big_index_info_tail(&mut cursor)?;
    if cursor.position() as usize != data.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "trailing bytes after Java Big IndexInfo",
        ));
    }
    Ok(JavaBigIndexInfo {
        first_clustering,
        last_clustering,
        tail,
    })
}

/// Encode a Java Big-format `IndexInfo` body using the table's clustering type specs.
pub fn write_java_big_index_info(
    writer: &mut impl Write,
    index_info: &JavaBigIndexInfo,
    clustering_type_specs: &[String],
) -> io::Result<()> {
    write_java_big_clustering_prefix(writer, &index_info.first_clustering, clustering_type_specs)?;
    write_java_big_clustering_prefix(writer, &index_info.last_clustering, clustering_type_specs)?;
    write_java_big_index_info_tail(writer, index_info.tail)
}

pub fn read_java_big_index_info_tail<R: Read>(reader: &mut R) -> io::Result<JavaBigIndexInfoTail> {
    let offset = read_unsigned_vint(reader)?;
    let width_delta = read_vint(reader)?;
    let width = JAVA_INDEX_INFO_WIDTH_BASE
        .checked_add(width_delta)
        .and_then(|value| u64::try_from(value).ok())
        .ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "invalid Java IndexInfo width")
        })?;
    let has_end_open_marker = reader.read_u8()? != 0;
    let end_open_marker = if has_end_open_marker {
        Some(read_java_big_deletion_time(reader)?)
    } else {
        None
    };

    Ok(JavaBigIndexInfoTail {
        offset,
        width,
        end_open_marker,
    })
}

pub fn write_java_big_index_info_tail<W: Write>(
    writer: &mut W,
    tail: JavaBigIndexInfoTail,
) -> io::Result<()> {
    write_unsigned_vint(writer, tail.offset)?;
    let width_delta = i64::try_from(tail.width).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "Java IndexInfo width too large",
        )
    })? - JAVA_INDEX_INFO_WIDTH_BASE;
    write_vint(writer, width_delta)?;
    writer.write_all(&[u8::from(tail.end_open_marker.is_some())])?;
    if let Some(deletion_time) = tail.end_open_marker {
        write_java_big_deletion_time(writer, deletion_time)?;
    }
    Ok(())
}

fn read_java_big_clustering_prefix<R: Read>(
    reader: &mut R,
    clustering_type_specs: &[String],
) -> io::Result<JavaBigClusteringPrefix> {
    let kind_ordinal = reader.read_u8()?;
    let size = reader.read_u16::<BigEndian>()? as usize;
    if size > clustering_type_specs.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Java Big clustering prefix has more values than clustering types",
        ));
    }

    let mut values = Vec::with_capacity(size);
    let mut offset = 0;
    while offset < size {
        let header = read_unsigned_vint(reader)?;
        let limit = size.min(offset + 32);
        for idx in offset..limit {
            let local_idx = idx - offset;
            if java_clustering_value_is_null(header, local_idx) {
                values.push(None);
            } else if java_clustering_value_is_empty(header, local_idx) {
                values.push(Some(Vec::new()));
            } else {
                values.push(Some(read_java_big_clustering_value(
                    reader,
                    &clustering_type_specs[idx],
                )?));
            }
        }
        offset = limit;
    }

    Ok(JavaBigClusteringPrefix {
        kind_ordinal,
        values,
    })
}

fn write_java_big_clustering_prefix<W: Write>(
    writer: &mut W,
    prefix: &JavaBigClusteringPrefix,
    clustering_type_specs: &[String],
) -> io::Result<()> {
    if prefix.values.len() > clustering_type_specs.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Java Big clustering prefix has more values than clustering types",
        ));
    }
    let size = u16::try_from(prefix.values.len()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "Java Big clustering prefix has too many values",
        )
    })?;
    writer.write_u8(prefix.kind_ordinal)?;
    writer.write_u16::<BigEndian>(size)?;

    let mut offset = 0;
    while offset < prefix.values.len() {
        let limit = prefix.values.len().min(offset + 32);
        let mut header = 0_u64;
        for idx in offset..limit {
            let local_idx = idx - offset;
            match &prefix.values[idx] {
                None => header |= 1_u64 << (local_idx * 2 + 1),
                Some(value) if value.is_empty() => header |= 1_u64 << (local_idx * 2),
                Some(_) => {}
            }
        }
        write_unsigned_vint(writer, header)?;
        for idx in offset..limit {
            if let Some(value) = &prefix.values[idx] {
                if !value.is_empty() {
                    write_java_big_clustering_value(writer, &clustering_type_specs[idx], value)?;
                }
            }
        }
        offset = limit;
    }
    Ok(())
}

fn read_java_big_clustering_value<R: Read>(reader: &mut R, type_spec: &str) -> io::Result<Vec<u8>> {
    if let Some(len) = java_clustering_fixed_size(type_spec)? {
        let mut value = vec![0u8; len];
        reader.read_exact(&mut value)?;
        Ok(value)
    } else {
        let len = checked_usize(
            read_unsigned_vint(reader)?,
            "Java Big clustering value length",
        )?;
        let mut value = vec![0u8; len];
        reader.read_exact(&mut value)?;
        Ok(value)
    }
}

fn write_java_big_clustering_value<W: Write>(
    writer: &mut W,
    type_spec: &str,
    value: &[u8],
) -> io::Result<()> {
    if let Some(len) = java_clustering_fixed_size(type_spec)? {
        if value.len() != len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("Java Big clustering value for {type_spec} must be {len} bytes"),
            ));
        }
        writer.write_all(value)
    } else {
        write_unsigned_vint(writer, value.len() as u64)?;
        writer.write_all(value)
    }
}

fn java_clustering_fixed_size(type_spec: &str) -> io::Result<Option<usize>> {
    parse_type(type_spec)
        .map(|ty| ty.unwrap_reversed().fixed_size())
        .map_err(|err| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("invalid Java Big clustering type '{type_spec}': {err}"),
            )
        })
}

fn java_clustering_value_is_null(header: u64, local_idx: usize) -> bool {
    (header & (1_u64 << (local_idx * 2 + 1))) != 0
}

fn java_clustering_value_is_empty(header: u64, local_idx: usize) -> bool {
    (header & (1_u64 << (local_idx * 2))) != 0
}

fn write_bytes<W: Write>(writer: &mut W, data: &[u8]) -> io::Result<()> {
    writer.write_u32::<BigEndian>(data.len() as u32)?;
    writer.write_all(data)
}

fn read_bytes<R: Read>(reader: &mut R) -> io::Result<Vec<u8>> {
    let len = reader.read_u32::<BigEndian>()? as usize;
    let mut bytes = vec![0u8; len];
    reader.read_exact(&mut bytes)?;
    Ok(bytes)
}

fn read_java_big_deletion_time<R: Read>(reader: &mut R) -> io::Result<JavaBigDeletionTime> {
    let first = reader.read_u8()?;
    if first == 0x80 {
        return Ok(JavaBigDeletionTime::LIVE);
    }
    if first & 0x80 != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid Java Big deletion-time flags",
        ));
    }

    let mut timestamp = [0u8; 8];
    timestamp[0] = first;
    reader.read_exact(&mut timestamp[1..])?;
    let marked_for_delete_at = i64::from_be_bytes(timestamp);
    let local_deletion_time_unsigned = reader.read_u32::<BigEndian>()?;
    Ok(JavaBigDeletionTime {
        marked_for_delete_at,
        local_deletion_time_unsigned,
    })
}

fn write_java_big_deletion_time<W: Write>(
    writer: &mut W,
    deletion_time: JavaBigDeletionTime,
) -> io::Result<()> {
    if deletion_time.is_live() {
        return writer.write_all(&[0x80]);
    }
    if deletion_time.marked_for_delete_at < 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Java Big deletion timestamp cannot set the live flag bit",
        ));
    }
    writer.write_i64::<BigEndian>(deletion_time.marked_for_delete_at)?;
    writer.write_u32::<BigEndian>(deletion_time.local_deletion_time_unsigned)
}

fn validate_java_big_offsets(offsets: &[i32], index_info_len: usize) -> io::Result<()> {
    let mut previous = None;
    for &offset in offsets {
        if offset < 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "negative Java Big IndexInfo offset",
            ));
        }
        let offset = offset as usize;
        if offset > index_info_len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Java Big IndexInfo offset exceeds serialized payload",
            ));
        }
        if previous.is_some_and(|prev| offset < prev) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Java Big IndexInfo offsets are not sorted",
            ));
        }
        previous = Some(offset);
    }
    Ok(())
}

fn checked_u32(value: u64, label: &str) -> io::Result<u32> {
    u32::try_from(value).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{label} does not fit in u32"),
        )
    })
}

fn checked_u32_usize(value: usize, label: &str) -> io::Result<u32> {
    u32::try_from(value).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{label} does not fit in u32"),
        )
    })
}

fn checked_usize(value: u64, label: &str) -> io::Result<usize> {
    usize::try_from(value).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{label} does not fit in usize"),
        )
    })
}

fn checked_mul_usize(left: usize, right: usize, label: &str) -> io::Result<usize> {
    left.checked_mul(right)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, format!("{label} size overflow")))
}

fn unsigned_vint_len(value: u64) -> usize {
    cassandra_io::util::varint::unsigned_vint_size(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_thresholded_blocks() {
        let mut builder = ColumnIndexBuilder::new(10);
        builder.add_row(b"ck1".to_vec(), 4);
        builder.add_row(b"ck2".to_vec(), 6);
        builder.add_row(b"ck3".to_vec(), 5);
        let blocks = builder.finish();

        assert_eq!(blocks.len(), 2);
        assert_eq!(blocks[0].first_clustering, b"ck1");
        assert_eq!(blocks[0].last_clustering, b"ck2");
        assert_eq!(blocks[0].offset, 0);
        assert_eq!(blocks[0].width, 10);
        assert_eq!(blocks[0].row_count, 2);
        assert_eq!(blocks[1].first_clustering, b"ck3");
        assert_eq!(blocks[1].offset, 10);
    }

    #[test]
    fn explicit_offsets_are_preserved() {
        let mut builder = ColumnIndexBuilder::new(1);
        builder.add_row_at(b"a".to_vec(), 42, 9);
        builder.add_row_at(b"b".to_vec(), 99, 1);
        let blocks = builder.finish();
        assert_eq!(blocks[0].offset, 42);
        assert_eq!(blocks[1].offset, 99);
    }

    #[test]
    fn finds_blocks_by_clustering_range() {
        let mut builder = ColumnIndexBuilder::new(5);
        for key in [b"a", b"b", b"c", b"d"] {
            builder.add_row(key.to_vec(), 5);
        }
        let blocks = builder.finish();
        assert!(find_block(&blocks, b"a").unwrap().contains_clustering(b"a"));
        assert!(find_block(&blocks, b"c").unwrap().contains_clustering(b"c"));
        assert!(find_block(&blocks, b"z").is_none());
    }

    #[test]
    fn serializes_index_info_blocks() {
        let mut builder = ColumnIndexBuilder::new(5);
        builder.add_row_at(b"a".to_vec(), 10, 5);
        builder.add_row_at(b"b".to_vec(), 15, 5);
        let blocks = builder.finish();

        let encoded = serialize_index_infos(&blocks).unwrap();
        assert_eq!(&encoded[..4], b"CIDX");
        assert_eq!(encoded[4], 1);
        let decoded = deserialize_index_infos(&encoded).unwrap();
        assert_eq!(decoded, blocks);

        let mut corrupt = encoded;
        corrupt[0] = b'X';
        assert!(deserialize_index_infos(&corrupt).is_err());
    }

    #[test]
    fn parses_unindexed_java_big_row_index_entry() {
        let entry = JavaBigRowIndexEntry {
            data_file_position: 128,
            promoted_size: 0,
            kind: JavaBigRowIndexEntryKind::Unindexed,
            header_length: None,
            deletion_time: None,
            column_index_count: 0,
            index_info_bytes: Vec::new(),
            offsets: Vec::new(),
        };

        let encoded = serialize_java_big_row_index_entry(&entry).unwrap();
        assert_eq!(&encoded, &[0x80, 0x80, 0x00]);

        let parsed = deserialize_java_big_row_index_entry(&encoded).unwrap();
        assert_eq!(parsed, entry);
        assert!(!parsed.is_indexed());
    }

    #[test]
    fn parses_indexed_java_big_row_index_entry_frame() {
        let entry = JavaBigRowIndexEntry {
            data_file_position: 4_096,
            promoted_size: 0,
            kind: JavaBigRowIndexEntryKind::Indexed,
            header_length: Some(9),
            deletion_time: Some(JavaBigDeletionTime::LIVE),
            column_index_count: 2,
            index_info_bytes: vec![0x10, 0x11, 0x12, 0x20, 0x21],
            offsets: vec![0, 3],
        };

        let encoded = serialize_java_big_row_index_entry(&entry).unwrap();
        let parsed = deserialize_java_big_row_index_entry_with_cache_size(&encoded, 64).unwrap();

        assert_eq!(parsed.data_file_position, 4_096);
        assert_eq!(parsed.kind, JavaBigRowIndexEntryKind::Indexed);
        assert_eq!(parsed.header_length, Some(9));
        assert_eq!(parsed.deletion_time, Some(JavaBigDeletionTime::LIVE));
        assert_eq!(parsed.column_index_count, 2);
        assert_eq!(parsed.index_info_slice(0), Some(&[0x10, 0x11, 0x12][..]));
        assert_eq!(parsed.index_info_slice(1), Some(&[0x20, 0x21][..]));
        assert_eq!(parsed.index_info_slices().len(), 2);
        assert_eq!(
            parsed.promoted_size,
            (1 + 1 + 1 + entry.index_info_bytes.len() + entry.offsets.len() * 4) as u32
        );
    }

    #[test]
    fn classifies_large_java_big_row_index_entry_as_shallow() {
        let entry = JavaBigRowIndexEntry {
            data_file_position: 7,
            promoted_size: 0,
            kind: JavaBigRowIndexEntryKind::ShallowIndexed,
            header_length: Some(300),
            deletion_time: Some(JavaBigDeletionTime {
                marked_for_delete_at: 1_234_567,
                local_deletion_time_unsigned: 1_700_000_000,
            }),
            column_index_count: 2,
            index_info_bytes: vec![0xaa; 24],
            offsets: vec![0, 12],
        };

        let encoded = serialize_java_big_row_index_entry(&entry).unwrap();
        let parsed = deserialize_java_big_row_index_entry_with_cache_size(&encoded, 8).unwrap();

        assert_eq!(parsed.kind, JavaBigRowIndexEntryKind::ShallowIndexed);
        assert_eq!(parsed.header_length, Some(300));
        assert_eq!(parsed.deletion_time, entry.deletion_time);
        assert_eq!(parsed.index_info_slice(0).unwrap().len(), 12);
        assert_eq!(parsed.index_info_slice(1).unwrap().len(), 12);
    }

    #[test]
    fn rejects_corrupt_java_big_offset_table() {
        let entry = JavaBigRowIndexEntry {
            data_file_position: 1,
            promoted_size: 0,
            kind: JavaBigRowIndexEntryKind::Indexed,
            header_length: Some(1),
            deletion_time: Some(JavaBigDeletionTime::LIVE),
            column_index_count: 2,
            index_info_bytes: vec![0x01, 0x02, 0x03],
            offsets: vec![2, 1],
        };

        assert!(serialize_java_big_row_index_entry(&entry).is_err());

        let mut encoded = Vec::new();
        write_unsigned_vint(&mut encoded, 1).unwrap();
        write_unsigned_vint(&mut encoded, 12).unwrap();
        write_unsigned_vint(&mut encoded, 1).unwrap();
        encoded.push(0x80);
        write_unsigned_vint(&mut encoded, 2).unwrap();
        encoded.extend_from_slice(&[0x01, 0x02, 0x03]);
        encoded.write_i32::<BigEndian>(2).unwrap();
        encoded.write_i32::<BigEndian>(1).unwrap();

        assert!(deserialize_java_big_row_index_entry(&encoded).is_err());
    }

    #[test]
    fn java_big_index_info_tail_uses_width_base_delta() {
        let tail = JavaBigIndexInfoTail {
            offset: 1_024,
            width: (JAVA_INDEX_INFO_WIDTH_BASE as u64) + 7,
            end_open_marker: Some(JavaBigDeletionTime {
                marked_for_delete_at: 99,
                local_deletion_time_unsigned: 123,
            }),
        };

        let mut encoded = Vec::new();
        write_java_big_index_info_tail(&mut encoded, tail).unwrap();
        let mut cursor = Cursor::new(encoded);
        let decoded = read_java_big_index_info_tail(&mut cursor).unwrap();
        assert_eq!(decoded, tail);
    }

    #[test]
    fn java_big_index_info_decodes_schema_aware_clustering_prefixes() {
        let clustering_types = vec!["Int32Type".to_string(), "UTF8Type".to_string()];
        let index_info = JavaBigIndexInfo {
            first_clustering: JavaBigClusteringPrefix {
                kind_ordinal: 0x20,
                values: vec![Some(1i32.to_be_bytes().to_vec()), Some(b"alpha".to_vec())],
            },
            last_clustering: JavaBigClusteringPrefix {
                kind_ordinal: 0x20,
                values: vec![Some(9i32.to_be_bytes().to_vec()), Some(b"omega".to_vec())],
            },
            tail: JavaBigIndexInfoTail {
                offset: 2_048,
                width: (JAVA_INDEX_INFO_WIDTH_BASE as u64) + 11,
                end_open_marker: None,
            },
        };

        let mut encoded = Vec::new();
        write_java_big_index_info(&mut encoded, &index_info, &clustering_types).unwrap();
        let decoded = read_java_big_index_info(&encoded, &clustering_types).unwrap();

        assert_eq!(decoded, index_info);
    }

    #[test]
    fn java_big_index_info_rejects_prefix_type_mismatch() {
        let clustering_types = vec!["Int32Type".to_string()];
        let index_info = JavaBigIndexInfo {
            first_clustering: JavaBigClusteringPrefix {
                kind_ordinal: 0x20,
                values: vec![Some(1i32.to_be_bytes().to_vec()), Some(b"extra".to_vec())],
            },
            last_clustering: JavaBigClusteringPrefix {
                kind_ordinal: 0x20,
                values: vec![Some(2i32.to_be_bytes().to_vec())],
            },
            tail: JavaBigIndexInfoTail {
                offset: 0,
                width: JAVA_INDEX_INFO_WIDTH_BASE as u64,
                end_open_marker: None,
            },
        };

        assert!(
            write_java_big_index_info(&mut Vec::new(), &index_info, &clustering_types).is_err()
        );
    }
}
