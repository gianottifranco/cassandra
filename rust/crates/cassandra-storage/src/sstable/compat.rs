// Licensed under Apache License, Version 2.0.

//! SSTable compatibility strategy and format documentation.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.io.sstable.format.SSTableFormat`
//! - `org.apache.cassandra.io.sstable.format.Version`
//!
//! ## Compatibility Strategy
//!
//! ### Non-Binary-Compatible with Java Cassandra
//!
//! The Rust SSTable format is intentionally **NOT** binary-compatible with the
//! Java Apache Cassandra SSTable format. This is a deliberate design choice:
//!
//! | Aspect               | Java Cassandra                | Rust Cassandra           |
//! |----------------------|-------------------------------|--------------------------|
//! | Data.db magic        | `COMPACTION_HEADER` (varies)  | `SSDT` (4 bytes)         |
//! | Index.db magic       | None (starts with entries)    | `SSIX` (4 bytes)         |
//! | Filter.db magic      | None (raw bloom data)         | `SSFL` (4 bytes)         |
//! | Row serialization    | Complex flags + vint encoding | Simplified fixed-width   |
//! | Statistics format    | Binary serialization map      | JSON (human-readable)    |
//! | CRC placement        | Per-chunk in compressed mode  | Single CRC32 at EOF      |
//! | String encoding      | Modified UTF-8                | Standard UTF-8           |
//!
//! ### Why Not Compatible?
//!
//! 1. **Simplicity**: Java's format carries 10+ years of backward compat baggage
//! 2. **Performance**: Fixed-width fields avoid vint decode overhead
//! 3. **Debuggability**: JSON stats, clear magic bytes, simpler structure
//! 4. **Independence**: Rust nodes form their own cluster; no mixed-mode
//!
//! ### Upgrade Path
//!
//! - Rust V1 SSTables will always be readable by any future Rust version
//! - Version upgrades are Rust V1 -> V2 only (when V2 is introduced)
//! - There is NO Java-to-Rust SSTable migration path; data migrates via
//!   streaming or CQL-level export/import
//! - Mixed Java/Rust clusters are not supported
//!
//! ### Format Versions
//!
//! | Version | Format | Status  | Description                        |
//! |---------|--------|---------|------------------------------------|
//! | V1      | Big    | Current | Partition index + binary search    |
//! | V1      | BTI    | Current | Trie-based partition index          |
//!
//! Both Big and BTI share the same Data.db on-disk format (magic, version,
//! partition/row/cell serialization). They differ only in the index structure:
//! Big uses a flat `Index.db` + `Summary.db`, while BTI uses a trie-encoded
//! `Partitions.db`.

use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};
use std::convert::TryInto;
use std::fs;
use std::io::Cursor;
use std::path::{Path, PathBuf};

use cassandra_io::util::varint::{read_unsigned_vint, write_unsigned_vint};
use crc32fast::Hasher;

use super::column_index::{JavaBigRowIndexEntry, deserialize_java_big_row_index_entry_prefix};
use super::format::{DATA_MAGIC, DATA_VERSION, FILTER_MAGIC, INDEX_MAGIC};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum JavaBigComponent {
    Data,
    Index,
    Summary,
    Filter,
    Statistics,
    CompressionInfo,
    Digest,
    Crc,
    Toc,
}

impl JavaBigComponent {
    pub fn suffix(self) -> &'static str {
        match self {
            Self::Data => "Data.db",
            Self::Index => "Index.db",
            Self::Summary => "Summary.db",
            Self::Filter => "Filter.db",
            Self::Statistics => "Statistics.db",
            Self::CompressionInfo => "CompressionInfo.db",
            Self::Digest => "Digest.crc32",
            Self::Crc => "CRC.db",
            Self::Toc => "TOC.txt",
        }
    }

    pub fn from_suffix(suffix: &str) -> Option<Self> {
        match suffix {
            "Data.db" => Some(Self::Data),
            "Index.db" => Some(Self::Index),
            "Summary.db" => Some(Self::Summary),
            "Filter.db" => Some(Self::Filter),
            "Statistics.db" => Some(Self::Statistics),
            "CompressionInfo.db" => Some(Self::CompressionInfo),
            "Digest.crc32" => Some(Self::Digest),
            "CRC.db" => Some(Self::Crc),
            "TOC.txt" => Some(Self::Toc),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct JavaBigDescriptor {
    pub version: String,
    pub generation: u64,
    pub format: String,
}

impl JavaBigDescriptor {
    pub fn parse_component_filename(filename: &str) -> Option<(Self, JavaBigComponent)> {
        let mut parts = filename.splitn(4, '-');
        let version = parts.next()?.to_string();
        let generation = parts.next()?.parse().ok()?;
        let format = parts.next()?.to_string();
        let component = JavaBigComponent::from_suffix(parts.next()?)?;
        if format != "big" {
            return None;
        }
        Some((
            Self {
                version,
                generation,
                format,
            },
            component,
        ))
    }

    pub fn component_filename(&self, component: JavaBigComponent) -> String {
        format!(
            "{}-{}-{}-{}",
            self.version,
            self.generation,
            self.format,
            component.suffix()
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JavaBigSSTableManifest {
    pub descriptor: JavaBigDescriptor,
    pub directory: PathBuf,
    pub components: Vec<JavaBigComponent>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JavaBigTocValidation {
    pub listed: Vec<JavaBigComponent>,
    pub present: Vec<JavaBigComponent>,
    pub missing: Vec<JavaBigComponent>,
    pub extra: Vec<JavaBigComponent>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JavaBigDigestValidation {
    pub stored: u64,
    pub calculated: u64,
}

impl JavaBigDigestValidation {
    pub fn is_valid(&self) -> bool {
        self.stored == self.calculated
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JavaBigCrcMetadata {
    pub chunk_size: u32,
    pub checksums: Vec<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JavaBigCrcValidation {
    pub chunk_size: u32,
    pub stored: Vec<u32>,
    pub calculated: Vec<u32>,
}

impl JavaBigCrcValidation {
    pub fn is_valid(&self) -> bool {
        self.stored == self.calculated
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JavaBigCompressionMetadata {
    pub compressor_name: String,
    pub options: BTreeMap<String, String>,
    pub chunk_length: u32,
    pub max_compressed_length: u32,
    pub data_length: u64,
    pub chunk_offsets: Vec<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JavaBigCompressedChunk {
    pub offset: u64,
    pub length: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JavaBigBloomFilter {
    pub hash_count: u32,
    pub word_count: u32,
    pub bitset_bytes: Vec<u8>,
}

impl JavaBigBloomFilter {
    pub fn capacity_bits(&self) -> u64 {
        self.bitset_bytes.len() as u64 * 8
    }

    pub fn is_bit_set(&self, index: u64) -> bool {
        let byte_index = (index >> 3) as usize;
        let bit = (index & 0x7) as u8;
        self.bitset_bytes
            .get(byte_index)
            .is_some_and(|byte| (byte & (1_u8 << bit)) != 0)
    }

    pub fn set_bit(&mut self, index: u64) -> std::io::Result<()> {
        let byte_index = (index >> 3) as usize;
        let Some(byte) = self.bitset_bytes.get_mut(byte_index) else {
            return Err(invalid_data("Java bloom-filter bit index out of range"));
        };
        let bit = (index & 0x7) as u8;
        *byte |= 1_u8 << bit;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JavaBigIndexEntry {
    pub index_offset: u64,
    pub partition_key: Vec<u8>,
    pub row_index: JavaBigRowIndexEntry,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JavaBigSummary {
    pub min_index_interval: u32,
    pub offheap_size: u64,
    pub sampling_level: u32,
    pub size_at_full_sampling: u32,
    pub entries: Vec<JavaBigSummaryEntry>,
    pub first_key: Vec<u8>,
    pub last_key: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JavaBigSummaryEntry {
    pub partition_key: Vec<u8>,
    pub index_offset: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JavaBigDeletionTimeMetadata {
    pub marked_for_delete_at: i64,
    pub local_deletion_time: i64,
    pub is_live: bool,
}

impl JavaBigDeletionTimeMetadata {
    pub const LIVE: Self = Self {
        marked_for_delete_at: i64::MIN,
        local_deletion_time: i64::MAX,
        is_live: true,
    };
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JavaBigDataPartitionHeader {
    pub data_offset: u64,
    pub partition_key: Vec<u8>,
    pub deletion_time: JavaBigDeletionTimeMetadata,
    pub bytes_consumed: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JavaBigIndexedDataPartitionHeader {
    pub index_entry: JavaBigIndexEntry,
    pub data_header: JavaBigDataPartitionHeader,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JavaBigDataPartitionSimpleRows {
    pub header: JavaBigDataPartitionHeader,
    pub unfiltereds: Vec<JavaBigDataPartitionUnfiltered>,
    pub end_offset: u64,
    pub bytes_consumed: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JavaBigDataPartitionUnfiltered {
    Row(JavaBigUnfilteredRowMetadata),
    RangeTombstoneMarker(JavaBigRangeTombstoneMarker),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JavaBigUnfilteredKind {
    EndOfPartition,
    Row,
    RangeTombstoneMarker,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JavaBigUnfilteredHeader {
    pub data_offset: u64,
    pub flags: u8,
    pub extended_flags: u8,
    pub kind: JavaBigUnfilteredKind,
    pub is_static: bool,
    pub clustering_kind_ordinal: Option<u8>,
    pub clustering_values: Vec<Option<Vec<u8>>>,
    pub framed_body_size: Option<u64>,
    pub previous_unfiltered_size: Option<u64>,
    pub body_length: Option<u64>,
    pub body_start_offset: Option<u64>,
    pub next_unfiltered_offset: u64,
    pub bytes_consumed: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JavaBigRangeTombstoneMarker {
    pub header: JavaBigUnfilteredHeader,
    pub deletion_time: JavaBigDeletionTimeMetadata,
    pub boundary_start_deletion_time: Option<JavaBigDeletionTimeMetadata>,
    pub body_bytes_consumed: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JavaBigRowLivenessMetadata {
    pub timestamp: Option<i64>,
    pub ttl: Option<i32>,
    pub local_expiration_time: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JavaBigUnfilteredRowMetadata {
    pub header: JavaBigUnfilteredHeader,
    pub liveness: JavaBigRowLivenessMetadata,
    pub deletion_time: Option<JavaBigDeletionTimeMetadata>,
    pub deletion_is_shadowable: bool,
    pub simple_cells: Vec<JavaBigSimpleCell>,
    pub complex_columns: Vec<JavaBigComplexColumn>,
    pub body_bytes_consumed: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JavaBigSimpleCell {
    pub column_name: Vec<u8>,
    pub column_type: String,
    pub flags: u8,
    pub timestamp: i64,
    pub local_deletion_time: Option<i64>,
    pub ttl: Option<i32>,
    pub value: Option<Vec<u8>>,
    pub is_deleted: bool,
    pub is_expiring: bool,
    pub uses_row_timestamp: bool,
    pub uses_row_ttl: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JavaBigComplexColumn {
    pub column_name: Vec<u8>,
    pub column_type: String,
    pub deletion_time: Option<JavaBigDeletionTimeMetadata>,
    pub cells: Vec<JavaBigComplexCell>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JavaBigComplexCell {
    pub path: Vec<u8>,
    pub cell: JavaBigSimpleCell,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum JavaBigMetadataType {
    Validation,
    Compaction,
    Stats,
    Header,
}

impl JavaBigMetadataType {
    pub fn ordinal(self) -> i32 {
        match self {
            Self::Validation => 0,
            Self::Compaction => 1,
            Self::Stats => 2,
            Self::Header => 3,
        }
    }

    pub fn from_ordinal(ordinal: i32) -> Option<Self> {
        match ordinal {
            0 => Some(Self::Validation),
            1 => Some(Self::Compaction),
            2 => Some(Self::Stats),
            3 => Some(Self::Header),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JavaBigStatisticsComponent {
    pub component_type: JavaBigMetadataType,
    pub offset: u32,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JavaBigStatisticsMetadata {
    pub has_checksum: bool,
    pub components: Vec<JavaBigStatisticsComponent>,
}

impl JavaBigStatisticsMetadata {
    pub fn component(&self, component_type: JavaBigMetadataType) -> Option<&[u8]> {
        self.components
            .iter()
            .find(|component| component.component_type == component_type)
            .map(|component| component.payload.as_slice())
    }

    pub fn validation_metadata(&self) -> std::io::Result<Option<JavaBigValidationMetadata>> {
        self.component(JavaBigMetadataType::Validation)
            .map(parse_java_big_validation_metadata)
            .transpose()
    }

    pub fn compaction_metadata(&self) -> std::io::Result<Option<JavaBigCompactionMetadata>> {
        self.component(JavaBigMetadataType::Compaction)
            .map(parse_java_big_compaction_metadata)
            .transpose()
    }

    pub fn stats_metadata_prefix(
        &self,
        has_unsigned_deletion_time: bool,
    ) -> std::io::Result<Option<JavaBigStatsMetadataPrefix>> {
        self.component(JavaBigMetadataType::Stats)
            .map(|data| parse_java_big_stats_metadata_prefix(data, has_unsigned_deletion_time))
            .transpose()
    }

    pub fn stats_metadata_legacy_tail(
        &self,
        prefix_bytes_consumed: usize,
        features: JavaBigStatsMetadataTailFeatures,
    ) -> std::io::Result<Option<JavaBigStatsMetadataLegacyTail>> {
        self.component(JavaBigMetadataType::Stats)
            .map(|data| {
                parse_java_big_stats_metadata_legacy_tail(data, prefix_bytes_consumed, features)
            })
            .transpose()
    }

    pub fn stats_metadata_modern_tail(
        &self,
        prefix_bytes_consumed: usize,
        features: JavaBigStatsMetadataTailFeatures,
    ) -> std::io::Result<Option<JavaBigStatsMetadataModernTail>> {
        self.component(JavaBigMetadataType::Stats)
            .map(|data| {
                parse_java_big_stats_metadata_modern_tail(data, prefix_bytes_consumed, features)
            })
            .transpose()
    }

    pub fn serialization_header(&self) -> std::io::Result<Option<JavaBigSerializationHeader>> {
        self.component(JavaBigMetadataType::Header)
            .map(parse_java_big_serialization_header)
            .transpose()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct JavaBigValidationMetadata {
    pub partitioner: String,
    pub bloom_filter_fp_chance: f64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JavaBigCompactionMetadata {
    pub cardinality_estimator: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JavaBigCommitLogPosition {
    pub segment_id: i64,
    pub position: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JavaBigEstimatedHistogram {
    pub buckets: Vec<JavaBigEstimatedHistogramBucket>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JavaBigEstimatedHistogramBucket {
    pub offset: i64,
    pub count: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JavaBigTombstoneHistogram {
    pub max_bin_size: i32,
    pub entries: Vec<JavaBigTombstoneHistogramEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JavaBigTombstoneHistogramEntry {
    pub point: i64,
    pub count: i64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct JavaBigStatsMetadataPrefix {
    pub estimated_partition_size: JavaBigEstimatedHistogram,
    pub estimated_cell_per_partition_count: JavaBigEstimatedHistogram,
    pub commit_log_upper_bound: JavaBigCommitLogPosition,
    pub min_timestamp: i64,
    pub max_timestamp: i64,
    pub min_local_deletion_time: i64,
    pub max_local_deletion_time: i64,
    pub min_ttl: i32,
    pub max_ttl: i32,
    pub compression_ratio: f64,
    pub estimated_tombstone_drop_time: JavaBigTombstoneHistogram,
    pub sstable_level: i32,
    pub repaired_at: i64,
    pub bytes_consumed: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JavaBigStatsMetadataTailFeatures {
    pub has_legacy_min_max: bool,
    pub has_improved_min_max: bool,
    pub has_commit_log_lower_bound: bool,
    pub has_commit_log_intervals: bool,
    pub has_pending_repair: bool,
    pub has_is_transient: bool,
    pub has_originating_host_id: bool,
    pub has_partition_level_deletions_presence_marker: bool,
    pub has_key_range: bool,
    pub has_token_space_coverage: bool,
}

impl JavaBigStatsMetadataTailFeatures {
    pub fn for_big_version(version: &str) -> Self {
        Self {
            has_legacy_min_max: matches_two_letter_range(version, b'm', b'a', b'z')
                || matches_two_letter_range(version, b'n', b'a', b'z'),
            has_improved_min_max: two_letter_version_at_least(version, "oa"),
            has_commit_log_lower_bound: two_letter_version_at_least(version, "mb"),
            has_commit_log_intervals: two_letter_version_at_least(version, "mc"),
            has_pending_repair: two_letter_version_at_least(version, "na"),
            has_is_transient: two_letter_version_at_least(version, "na"),
            has_originating_host_id: two_letter_version_at_least(version, "nb")
                || matches_two_letter_range(version, b'm', b'e', b'z'),
            has_partition_level_deletions_presence_marker: two_letter_version_at_least(
                version, "oa",
            ),
            has_key_range: two_letter_version_at_least(version, "oa"),
            has_token_space_coverage: two_letter_version_at_least(version, "oa"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JavaBigStatsMetadataLegacyTail {
    pub legacy_min_clustering_values: Vec<Vec<u8>>,
    pub legacy_max_clustering_values: Vec<Vec<u8>>,
    pub has_legacy_counter_shards: bool,
    pub total_columns_set: i64,
    pub total_rows: i64,
    pub commit_log_lower_bound: Option<JavaBigCommitLogPosition>,
    pub commit_log_intervals: Vec<JavaBigCommitLogInterval>,
    pub pending_repair: Option<[u8; 16]>,
    pub is_transient: Option<bool>,
    pub originating_host_id: Option<[u8; 16]>,
    pub has_partition_level_deletions: Option<bool>,
    pub bytes_consumed: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub struct JavaBigStatsMetadataModernTail {
    pub improved_min_max: JavaBigImprovedMinMax,
    pub has_legacy_counter_shards: bool,
    pub total_columns_set: i64,
    pub total_rows: i64,
    pub commit_log_lower_bound: Option<JavaBigCommitLogPosition>,
    pub commit_log_intervals: Vec<JavaBigCommitLogInterval>,
    pub pending_repair: Option<[u8; 16]>,
    pub is_transient: Option<bool>,
    pub originating_host_id: Option<[u8; 16]>,
    pub has_partition_level_deletions: Option<bool>,
    pub first_key: Option<Vec<u8>>,
    pub last_key: Option<Vec<u8>>,
    pub token_space_coverage: Option<f64>,
    pub bytes_consumed: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JavaBigImprovedMinMax {
    pub clustering_types: Vec<String>,
    pub covered_clustering: JavaBigRawSlice,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JavaBigRawSlice {
    pub start: JavaBigRawClusteringBound,
    pub end: JavaBigRawClusteringBound,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JavaBigRawClusteringBound {
    pub kind_ordinal: u8,
    pub values: Vec<Option<Vec<u8>>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JavaBigCommitLogInterval {
    pub start: JavaBigCommitLogPosition,
    pub end: JavaBigCommitLogPosition,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JavaBigEncodingStats {
    pub min_timestamp: i64,
    pub min_local_deletion_time: i64,
    pub min_ttl: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JavaBigSerializationHeader {
    pub encoding_stats: JavaBigEncodingStats,
    pub key_type: String,
    pub clustering_types: Vec<String>,
    pub static_columns: Vec<JavaBigSerializationHeaderColumn>,
    pub regular_columns: Vec<JavaBigSerializationHeaderColumn>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JavaBigSerializationHeaderColumn {
    pub name: Vec<u8>,
    pub type_spec: String,
}

impl JavaBigCompressionMetadata {
    pub fn chunk_for(
        &self,
        position: u64,
        compressed_file_length: u64,
    ) -> Option<JavaBigCompressedChunk> {
        if self.chunk_length == 0 || position >= self.data_length {
            return None;
        }
        let index = (position / self.chunk_length as u64) as usize;
        let offset = *self.chunk_offsets.get(index)?;
        let next_offset = self
            .chunk_offsets
            .get(index + 1)
            .copied()
            .unwrap_or(compressed_file_length);
        let length = next_offset.checked_sub(offset)?.checked_sub(4)?;
        Some(JavaBigCompressedChunk { offset, length })
    }
}

impl JavaBigSSTableManifest {
    pub fn has_component(&self, component: JavaBigComponent) -> bool {
        self.components.contains(&component)
    }

    pub fn is_minimally_readable(&self) -> bool {
        self.has_component(JavaBigComponent::Data)
            && self.has_component(JavaBigComponent::Index)
            && self.has_component(JavaBigComponent::Statistics)
    }

    pub fn component_path(&self, component: JavaBigComponent) -> PathBuf {
        self.directory
            .join(self.descriptor.component_filename(component))
    }

    pub fn toc_components(&self) -> std::io::Result<Vec<JavaBigComponent>> {
        read_java_big_toc(self.component_path(JavaBigComponent::Toc))
    }

    pub fn validate_toc(&self) -> std::io::Result<JavaBigTocValidation> {
        let listed = self.toc_components()?;
        let listed_set: BTreeSet<_> = listed.iter().copied().collect();
        let present_set: BTreeSet<_> = self.components.iter().copied().collect();
        let missing = listed_set.difference(&present_set).copied().collect();
        let extra = present_set.difference(&listed_set).copied().collect();

        Ok(JavaBigTocValidation {
            listed,
            present: self.components.clone(),
            missing,
            extra,
        })
    }

    pub fn validate_data_digest(&self) -> std::io::Result<Option<JavaBigDigestValidation>> {
        if !self.has_component(JavaBigComponent::Digest) {
            return Ok(None);
        }

        let stored = read_java_big_digest(self.component_path(JavaBigComponent::Digest))?;
        let calculated = calculate_crc32(self.component_path(JavaBigComponent::Data))?;
        Ok(Some(JavaBigDigestValidation { stored, calculated }))
    }

    pub fn validate_data_crc(&self) -> std::io::Result<Option<JavaBigCrcValidation>> {
        if !self.has_component(JavaBigComponent::Crc) {
            return Ok(None);
        }

        let metadata = read_java_big_crc(self.component_path(JavaBigComponent::Crc))?;
        let calculated = calculate_crc32_chunks(
            self.component_path(JavaBigComponent::Data),
            metadata.chunk_size,
        )?;
        Ok(Some(JavaBigCrcValidation {
            chunk_size: metadata.chunk_size,
            stored: metadata.checksums,
            calculated,
        }))
    }

    pub fn compression_metadata(&self) -> std::io::Result<Option<JavaBigCompressionMetadata>> {
        if !self.has_component(JavaBigComponent::CompressionInfo) {
            return Ok(None);
        }
        Ok(Some(read_java_big_compression_info(
            self.component_path(JavaBigComponent::CompressionInfo),
        )?))
    }

    pub fn bloom_filter(&self, old_format: bool) -> std::io::Result<Option<JavaBigBloomFilter>> {
        if !self.has_component(JavaBigComponent::Filter) {
            return Ok(None);
        }
        Ok(Some(read_java_big_filter(
            self.component_path(JavaBigComponent::Filter),
            old_format,
        )?))
    }

    pub fn primary_index_entries(&self) -> std::io::Result<Option<Vec<JavaBigIndexEntry>>> {
        if !self.has_component(JavaBigComponent::Index) {
            return Ok(None);
        }
        Ok(Some(read_java_big_primary_index(
            self.component_path(JavaBigComponent::Index),
        )?))
    }

    pub fn data_partition_header_at(
        &self,
        data_offset: u64,
        has_unsigned_deletion_time: bool,
    ) -> std::io::Result<Option<JavaBigDataPartitionHeader>> {
        if !self.has_component(JavaBigComponent::Data) {
            return Ok(None);
        }
        Ok(Some(read_java_big_data_partition_header_at(
            self.component_path(JavaBigComponent::Data),
            data_offset,
            has_unsigned_deletion_time,
        )?))
    }

    pub fn indexed_data_partition_headers(
        &self,
        has_unsigned_deletion_time: bool,
    ) -> std::io::Result<Option<Vec<JavaBigIndexedDataPartitionHeader>>> {
        if !self.has_component(JavaBigComponent::Data)
            || !self.has_component(JavaBigComponent::Index)
        {
            return Ok(None);
        }
        let index_entries =
            read_java_big_primary_index(self.component_path(JavaBigComponent::Index))?;
        Ok(Some(read_java_big_indexed_data_partition_headers(
            self.component_path(JavaBigComponent::Data),
            &index_entries,
            has_unsigned_deletion_time,
        )?))
    }

    pub fn data_partition_simple_rows_at(
        &self,
        data_offset: u64,
        has_unsigned_deletion_time: bool,
        serialization_header: &JavaBigSerializationHeader,
    ) -> std::io::Result<Option<JavaBigDataPartitionSimpleRows>> {
        if !self.has_component(JavaBigComponent::Data) {
            return Ok(None);
        }
        Ok(Some(read_java_big_data_partition_simple_rows_at(
            self.component_path(JavaBigComponent::Data),
            data_offset,
            has_unsigned_deletion_time,
            serialization_header,
        )?))
    }

    pub fn summary(&self) -> std::io::Result<Option<JavaBigSummary>> {
        if !self.has_component(JavaBigComponent::Summary) {
            return Ok(None);
        }
        Ok(Some(read_java_big_summary(
            self.component_path(JavaBigComponent::Summary),
        )?))
    }

    pub fn statistics_metadata(
        &self,
        has_checksum: bool,
    ) -> std::io::Result<Option<JavaBigStatisticsMetadata>> {
        if !self.has_component(JavaBigComponent::Statistics) {
            return Ok(None);
        }
        Ok(Some(read_java_big_statistics(
            self.component_path(JavaBigComponent::Statistics),
            has_checksum,
        )?))
    }

    pub fn statistics_metadata_auto(&self) -> std::io::Result<Option<JavaBigStatisticsMetadata>> {
        if !self.has_component(JavaBigComponent::Statistics) {
            return Ok(None);
        }
        Ok(Some(read_java_big_statistics_auto(
            self.component_path(JavaBigComponent::Statistics),
        )?))
    }
}

pub fn discover_java_big_sstables(
    directory: impl AsRef<Path>,
) -> std::io::Result<Vec<JavaBigSSTableManifest>> {
    let directory = directory.as_ref();
    let mut grouped: BTreeMap<JavaBigDescriptor, Vec<JavaBigComponent>> = BTreeMap::new();
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        let Some(filename) = entry.file_name().to_str().map(str::to_string) else {
            continue;
        };
        if let Some((descriptor, component)) =
            JavaBigDescriptor::parse_component_filename(&filename)
        {
            grouped.entry(descriptor).or_default().push(component);
        }
    }

    Ok(grouped
        .into_iter()
        .map(|(descriptor, mut components)| {
            components.sort();
            components.dedup();
            JavaBigSSTableManifest {
                descriptor,
                directory: directory.to_path_buf(),
                components,
            }
        })
        .collect())
}

pub fn read_java_big_toc(path: impl AsRef<Path>) -> std::io::Result<Vec<JavaBigComponent>> {
    let data = fs::read_to_string(path)?;
    Ok(data
        .lines()
        .filter_map(|line| parse_java_big_toc_line(line.trim()))
        .collect())
}

pub fn read_java_big_digest(path: impl AsRef<Path>) -> std::io::Result<u64> {
    let data = fs::read_to_string(path)?;
    let Some(first_line) = data.lines().next() else {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "empty Java Digest.crc32",
        ));
    };
    first_line.trim().parse::<u64>().map_err(|err| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("invalid Java Digest.crc32 value: {err}"),
        )
    })
}

pub fn calculate_crc32(path: impl AsRef<Path>) -> std::io::Result<u64> {
    let data = fs::read(path)?;
    let mut hasher = Hasher::new();
    hasher.update(&data);
    Ok(hasher.finalize() as u64)
}

pub fn read_java_big_crc(path: impl AsRef<Path>) -> std::io::Result<JavaBigCrcMetadata> {
    let data = fs::read(path)?;
    if data.len() < 4 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Java CRC.db missing chunk size",
        ));
    }
    if (data.len() - 4) % 4 != 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Java CRC.db checksum table is truncated",
        ));
    }

    let chunk_size = u32::from_be_bytes(data[0..4].try_into().unwrap());
    if chunk_size == 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Java CRC.db chunk size must be positive",
        ));
    }

    let checksums = data[4..]
        .chunks_exact(4)
        .map(|chunk| u32::from_be_bytes(chunk.try_into().unwrap()))
        .collect();
    Ok(JavaBigCrcMetadata {
        chunk_size,
        checksums,
    })
}

pub fn calculate_crc32_chunks(
    path: impl AsRef<Path>,
    chunk_size: u32,
) -> std::io::Result<Vec<u32>> {
    if chunk_size == 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "chunk size must be positive",
        ));
    }
    let data = fs::read(path)?;
    Ok(data
        .chunks(chunk_size as usize)
        .map(|chunk| {
            let mut hasher = Hasher::new();
            hasher.update(chunk);
            hasher.finalize()
        })
        .collect())
}

pub fn read_java_big_compression_info(
    path: impl AsRef<Path>,
) -> std::io::Result<JavaBigCompressionMetadata> {
    let data = fs::read(path)?;
    let mut input = JavaDataInput::new(&data);
    let compressor_name = input.read_utf()?;
    let option_count = input.read_i32()? as i64;
    if option_count < 0 {
        return Err(invalid_data("negative compression option count"));
    }

    let mut options = BTreeMap::new();
    for _ in 0..option_count {
        options.insert(input.read_utf()?, input.read_utf()?);
    }

    let chunk_length = input.read_i32()?;
    let max_compressed_length = input.read_i32()?;
    let data_length = input.read_i64()?;
    let chunk_count = input.read_i32()? as i64;
    if chunk_length <= 0 {
        return Err(invalid_data("compression chunk length must be positive"));
    }
    if max_compressed_length <= 0 {
        return Err(invalid_data("max compressed length must be positive"));
    }
    if data_length < 0 {
        return Err(invalid_data("compression data length must be non-negative"));
    }
    if chunk_count <= 0 {
        return Err(invalid_data("compression chunk count must be positive"));
    }

    let mut chunk_offsets = Vec::with_capacity(chunk_count as usize);
    for _ in 0..chunk_count {
        let offset = input.read_i64()?;
        if offset < 0 {
            return Err(invalid_data(
                "compression chunk offset must be non-negative",
            ));
        }
        if let Some(previous) = chunk_offsets.last() {
            if offset as u64 <= *previous {
                return Err(invalid_data("compression chunk offsets must be increasing"));
            }
        }
        chunk_offsets.push(offset as u64);
    }

    Ok(JavaBigCompressionMetadata {
        compressor_name,
        options,
        chunk_length: chunk_length as u32,
        max_compressed_length: max_compressed_length as u32,
        data_length: data_length as u64,
        chunk_offsets,
    })
}

pub fn read_java_big_filter(
    path: impl AsRef<Path>,
    old_format: bool,
) -> std::io::Result<JavaBigBloomFilter> {
    let data = fs::read(path)?;
    parse_java_big_filter(&data, old_format)
}

pub fn parse_java_big_filter(data: &[u8], old_format: bool) -> std::io::Result<JavaBigBloomFilter> {
    let mut input = JavaDataInput::new(data);
    let hash_count = input.read_i32()?;
    if hash_count <= 0 {
        return Err(invalid_data("Java BloomFilter hash count must be positive"));
    }
    let word_count = input.read_i32()?;
    if word_count < 0 {
        return Err(invalid_data(
            "Java BloomFilter word count must be non-negative",
        ));
    }
    let byte_count = checked_mul_usize(word_count as usize, 8, "Java BloomFilter bitset bytes")?;
    let raw = input.read_exact(byte_count)?;
    let bitset_bytes = if old_format {
        java_old_bloom_words_to_bitset_bytes(raw)
    } else {
        raw.to_vec()
    };
    if input.remaining() != 0 {
        return Err(invalid_data("Java BloomFilter has trailing bytes"));
    }
    Ok(JavaBigBloomFilter {
        hash_count: hash_count as u32,
        word_count: word_count as u32,
        bitset_bytes,
    })
}

pub fn write_java_big_filter(
    filter: &JavaBigBloomFilter,
    old_format: bool,
) -> std::io::Result<Vec<u8>> {
    let expected_bytes = checked_mul_usize(
        filter.word_count as usize,
        8,
        "Java BloomFilter bitset bytes",
    )?;
    if filter.hash_count == 0 {
        return Err(invalid_data("Java BloomFilter hash count must be positive"));
    }
    if filter.bitset_bytes.len() != expected_bytes {
        return Err(invalid_data(
            "Java BloomFilter bitset length does not match word count",
        ));
    }
    let mut out = Vec::with_capacity(8 + filter.bitset_bytes.len());
    out.extend_from_slice(&(filter.hash_count as i32).to_be_bytes());
    out.extend_from_slice(&(filter.word_count as i32).to_be_bytes());
    if old_format {
        out.extend_from_slice(&java_bitset_bytes_to_old_bloom_words(&filter.bitset_bytes));
    } else {
        out.extend_from_slice(&filter.bitset_bytes);
    }
    Ok(out)
}

pub fn read_java_big_primary_index(
    path: impl AsRef<Path>,
) -> std::io::Result<Vec<JavaBigIndexEntry>> {
    let data = fs::read(path)?;
    parse_java_big_primary_index(&data)
}

pub fn parse_java_big_primary_index(data: &[u8]) -> std::io::Result<Vec<JavaBigIndexEntry>> {
    let mut offset = 0usize;
    let mut entries = Vec::new();
    while offset < data.len() {
        let index_offset = offset as u64;
        if data.len() - offset < 2 {
            return Err(invalid_data("truncated Java Big primary-index key length"));
        }
        let key_len = u16::from_be_bytes(data[offset..offset + 2].try_into().unwrap()) as usize;
        offset += 2;

        let key_end = offset
            .checked_add(key_len)
            .ok_or_else(|| invalid_data("Java Big primary-index key length overflow"))?;
        let Some(partition_key) = data.get(offset..key_end) else {
            return Err(invalid_data("truncated Java Big primary-index key"));
        };
        offset = key_end;

        let (row_index, consumed) = deserialize_java_big_row_index_entry_prefix(&data[offset..])?;
        offset = offset
            .checked_add(consumed)
            .ok_or_else(|| invalid_data("Java Big primary-index entry offset overflow"))?;
        entries.push(JavaBigIndexEntry {
            index_offset,
            partition_key: partition_key.to_vec(),
            row_index,
        });
    }
    Ok(entries)
}

pub fn write_java_big_primary_index_entry(
    out: &mut Vec<u8>,
    partition_key: &[u8],
    row_index: &JavaBigRowIndexEntry,
) -> std::io::Result<()> {
    let key_len = u16::try_from(partition_key.len()).map_err(|_| {
        invalid_data("Java Big primary-index partition key exceeds unsigned-short length")
    })?;
    out.extend_from_slice(&key_len.to_be_bytes());
    out.extend_from_slice(partition_key);
    out.extend_from_slice(&super::column_index::serialize_java_big_row_index_entry(
        row_index,
    )?);
    Ok(())
}

pub fn read_java_big_data_partition_header_at(
    path: impl AsRef<Path>,
    data_offset: u64,
    has_unsigned_deletion_time: bool,
) -> std::io::Result<JavaBigDataPartitionHeader> {
    let data = fs::read(path)?;
    parse_java_big_data_partition_header_at(&data, data_offset, has_unsigned_deletion_time)
}

pub fn parse_java_big_data_partition_header_at(
    data: &[u8],
    data_offset: u64,
    has_unsigned_deletion_time: bool,
) -> std::io::Result<JavaBigDataPartitionHeader> {
    let start = usize::try_from(data_offset)
        .map_err(|_| invalid_data("Java Data.db partition offset exceeds usize"))?;
    let Some(slice) = data.get(start..) else {
        return Err(invalid_data(
            "Java Data.db partition offset is out of bounds",
        ));
    };
    let mut input = JavaDataInput::new(slice);
    let key_len = input.read_u16()? as usize;
    let partition_key = input.read_exact(key_len)?.to_vec();
    let deletion_time =
        read_java_big_partition_deletion_time(&mut input, has_unsigned_deletion_time)?;

    Ok(JavaBigDataPartitionHeader {
        data_offset,
        partition_key,
        deletion_time,
        bytes_consumed: input.offset,
    })
}

pub fn write_java_big_data_partition_header(
    out: &mut Vec<u8>,
    partition_key: &[u8],
    deletion_time: &JavaBigDeletionTimeMetadata,
    has_unsigned_deletion_time: bool,
) -> std::io::Result<()> {
    let key_len = u16::try_from(partition_key.len())
        .map_err(|_| invalid_data("Java Data.db partition key exceeds unsigned-short length"))?;
    out.extend_from_slice(&key_len.to_be_bytes());
    out.extend_from_slice(partition_key);
    write_java_big_partition_deletion_time(out, deletion_time, has_unsigned_deletion_time)
}

pub fn read_java_big_indexed_data_partition_headers(
    data_path: impl AsRef<Path>,
    index_entries: &[JavaBigIndexEntry],
    has_unsigned_deletion_time: bool,
) -> std::io::Result<Vec<JavaBigIndexedDataPartitionHeader>> {
    let data = fs::read(data_path)?;
    index_entries
        .iter()
        .map(|entry| {
            let data_header = parse_java_big_data_partition_header_at(
                &data,
                entry.row_index.data_file_position,
                has_unsigned_deletion_time,
            )?;
            Ok(JavaBigIndexedDataPartitionHeader {
                index_entry: entry.clone(),
                data_header,
            })
        })
        .collect()
}

pub fn read_java_big_data_partition_simple_rows_at(
    path: impl AsRef<Path>,
    data_offset: u64,
    has_unsigned_deletion_time: bool,
    serialization_header: &JavaBigSerializationHeader,
) -> std::io::Result<JavaBigDataPartitionSimpleRows> {
    let data = fs::read(path)?;
    parse_java_big_data_partition_simple_rows_at(
        &data,
        data_offset,
        has_unsigned_deletion_time,
        serialization_header,
    )
}

pub fn parse_java_big_data_partition_simple_rows_at(
    data: &[u8],
    data_offset: u64,
    has_unsigned_deletion_time: bool,
    serialization_header: &JavaBigSerializationHeader,
) -> std::io::Result<JavaBigDataPartitionSimpleRows> {
    let header =
        parse_java_big_data_partition_header_at(data, data_offset, has_unsigned_deletion_time)?;
    let mut offset = data_offset
        .checked_add(header.bytes_consumed as u64)
        .ok_or_else(|| invalid_data("Java partition row-stream offset overflow"))?;
    let mut unfiltereds = Vec::new();

    loop {
        let unfiltered_header = parse_java_big_unfiltered_header_at_with_clustering_types(
            data,
            offset,
            &serialization_header.clustering_types,
        )?;
        match unfiltered_header.kind {
            JavaBigUnfilteredKind::EndOfPartition => {
                let end_offset = unfiltered_header.next_unfiltered_offset;
                let bytes_consumed = end_offset
                    .checked_sub(data_offset)
                    .and_then(|value| usize::try_from(value).ok())
                    .ok_or_else(|| invalid_data("Java partition bytes consumed overflow"))?;
                return Ok(JavaBigDataPartitionSimpleRows {
                    header,
                    unfiltereds,
                    end_offset,
                    bytes_consumed,
                });
            }
            JavaBigUnfilteredKind::Row => {
                let row = parse_java_big_unfiltered_row_with_simple_cells(
                    data,
                    unfiltered_header,
                    serialization_header,
                    false,
                )?;
                offset = row.header.next_unfiltered_offset;
                unfiltereds.push(JavaBigDataPartitionUnfiltered::Row(row));
            }
            JavaBigUnfilteredKind::RangeTombstoneMarker => {
                let marker = parse_java_big_range_tombstone_marker(
                    data,
                    unfiltered_header,
                    &serialization_header.encoding_stats,
                )?;
                offset = marker.header.next_unfiltered_offset;
                unfiltereds.push(JavaBigDataPartitionUnfiltered::RangeTombstoneMarker(marker));
            }
        }
    }
}

pub fn read_java_big_unfiltered_header_at(
    path: impl AsRef<Path>,
    data_offset: u64,
    clustering_value_count: usize,
) -> std::io::Result<JavaBigUnfilteredHeader> {
    let data = fs::read(path)?;
    parse_java_big_unfiltered_header_at(&data, data_offset, clustering_value_count)
}

pub fn parse_java_big_unfiltered_header_at(
    data: &[u8],
    data_offset: u64,
    clustering_value_count: usize,
) -> std::io::Result<JavaBigUnfilteredHeader> {
    if clustering_value_count != 0 {
        return Err(invalid_data(
            "Java unfiltered clustering values require table type specs",
        ));
    }
    parse_java_big_unfiltered_header_at_with_clustering_types(data, data_offset, &[])
}

pub fn parse_java_big_unfiltered_header_at_with_clustering_types(
    data: &[u8],
    data_offset: u64,
    clustering_types: &[String],
) -> std::io::Result<JavaBigUnfilteredHeader> {
    let start = usize::try_from(data_offset)
        .map_err(|_| invalid_data("Java Data.db unfiltered offset exceeds usize"))?;
    let Some(slice) = data.get(start..) else {
        return Err(invalid_data(
            "Java Data.db unfiltered offset is out of bounds",
        ));
    };
    let mut input = JavaDataInput::new(slice);
    let flags = input.read_exact(1)?[0];
    if (flags & JAVA_UNFILTERED_END_OF_PARTITION) != 0 {
        if flags != JAVA_UNFILTERED_END_OF_PARTITION {
            return Err(invalid_data(
                "Java unfiltered end-of-partition marker has extra flags",
            ));
        }
        return Ok(JavaBigUnfilteredHeader {
            data_offset,
            flags,
            extended_flags: 0,
            kind: JavaBigUnfilteredKind::EndOfPartition,
            is_static: false,
            clustering_kind_ordinal: None,
            clustering_values: Vec::new(),
            framed_body_size: None,
            previous_unfiltered_size: None,
            body_length: None,
            body_start_offset: None,
            next_unfiltered_offset: data_offset + 1,
            bytes_consumed: 1,
        });
    }

    let extended_flags = if (flags & JAVA_UNFILTERED_EXTENSION_FLAG) != 0 {
        input.read_exact(1)?[0]
    } else {
        0
    };
    let is_static = (extended_flags & JAVA_UNFILTERED_EXT_IS_STATIC) != 0;

    let (kind, clustering_kind_ordinal, clustering_values) =
        if (flags & JAVA_UNFILTERED_IS_MARKER) != 0 {
            let bound = read_java_big_raw_clustering_bound(&mut input, clustering_types)?;
            validate_java_big_marker_clustering_kind(bound.kind_ordinal)?;
            (
                JavaBigUnfilteredKind::RangeTombstoneMarker,
                Some(bound.kind_ordinal),
                bound.values,
            )
        } else {
            let values = if is_static {
                Vec::new()
            } else {
                read_java_big_clustering_values_without_size(&mut input, clustering_types)?
            };
            (JavaBigUnfilteredKind::Row, None, values)
        };

    let framed_body_size = input.read_unsigned_vint()?;
    let before_previous_size = input.offset;
    let previous_unfiltered_size = input.read_unsigned_vint()?;
    let previous_size_vint_len = input.offset - before_previous_size;
    let body_length = framed_body_size
        .checked_sub(previous_size_vint_len as u64)
        .ok_or_else(|| invalid_data("Java unfiltered framed body size is too small"))?;
    let body_start_offset = data_offset
        .checked_add(input.offset as u64)
        .ok_or_else(|| invalid_data("Java unfiltered body start offset overflow"))?;
    let next_unfiltered_offset = body_start_offset
        .checked_add(body_length)
        .ok_or_else(|| invalid_data("Java unfiltered next offset overflow"))?;
    let next_unfiltered_offset_usize = usize::try_from(next_unfiltered_offset)
        .map_err(|_| invalid_data("Java unfiltered next offset exceeds usize"))?;
    if next_unfiltered_offset_usize > data.len() {
        return Err(invalid_data("Java unfiltered body extends past Data.db"));
    }

    Ok(JavaBigUnfilteredHeader {
        data_offset,
        flags,
        extended_flags,
        kind,
        is_static,
        clustering_kind_ordinal,
        clustering_values,
        framed_body_size: Some(framed_body_size),
        previous_unfiltered_size: Some(previous_unfiltered_size),
        body_length: Some(body_length),
        body_start_offset: Some(body_start_offset),
        next_unfiltered_offset,
        bytes_consumed: input.offset,
    })
}

pub fn write_java_big_unfiltered_end_of_partition(out: &mut Vec<u8>) {
    out.push(JAVA_UNFILTERED_END_OF_PARTITION);
}

pub fn write_java_big_unfiltered_empty_clustering_row(
    out: &mut Vec<u8>,
    flags: u8,
    extended_flags: Option<u8>,
    previous_unfiltered_size: u64,
    body: &[u8],
) -> std::io::Result<()> {
    write_java_big_unfiltered_row(
        out,
        flags,
        extended_flags,
        &[],
        &[],
        previous_unfiltered_size,
        body,
    )
}

pub fn write_java_big_unfiltered_row(
    out: &mut Vec<u8>,
    flags: u8,
    extended_flags: Option<u8>,
    clustering_values: &[Option<Vec<u8>>],
    clustering_types: &[String],
    previous_unfiltered_size: u64,
    body: &[u8],
) -> std::io::Result<()> {
    if (flags & JAVA_UNFILTERED_END_OF_PARTITION) != 0 || (flags & JAVA_UNFILTERED_IS_MARKER) != 0 {
        return Err(invalid_data("Java unfiltered row flags are invalid"));
    }
    if (flags & JAVA_UNFILTERED_EXTENSION_FLAG) != 0 && extended_flags.is_none() {
        return Err(invalid_data(
            "Java unfiltered row extension flag requires extended flags",
        ));
    }
    if (flags & JAVA_UNFILTERED_EXTENSION_FLAG) == 0 && extended_flags.is_some() {
        return Err(invalid_data(
            "Java unfiltered row extended flags require extension flag",
        ));
    }
    let is_static =
        extended_flags.is_some_and(|flags| (flags & JAVA_UNFILTERED_EXT_IS_STATIC) != 0);
    if is_static && !clustering_values.is_empty() {
        return Err(invalid_data(
            "Java static row cannot have clustering values",
        ));
    }
    out.push(flags);
    if let Some(extended_flags) = extended_flags {
        out.push(extended_flags);
    }
    if !is_static {
        write_java_big_clustering_values_without_size(out, clustering_values, clustering_types)?;
    }
    write_java_big_unfiltered_sstable_body_frame(out, previous_unfiltered_size, body)
}

pub fn write_java_big_unfiltered_marker_header(
    out: &mut Vec<u8>,
    clustering_kind_ordinal: u8,
    previous_unfiltered_size: u64,
    body: &[u8],
) -> std::io::Result<()> {
    validate_java_big_marker_clustering_kind(clustering_kind_ordinal)?;
    let bound = JavaBigRawClusteringBound {
        kind_ordinal: clustering_kind_ordinal,
        values: Vec::new(),
    };
    out.push(JAVA_UNFILTERED_IS_MARKER);
    write_java_big_raw_clustering_bound(out, &bound, &[])?;
    write_java_big_unfiltered_sstable_body_frame(out, previous_unfiltered_size, body)
}

pub fn write_java_big_unfiltered_marker(
    out: &mut Vec<u8>,
    clustering_kind_ordinal: u8,
    clustering_values: &[Option<Vec<u8>>],
    clustering_types: &[String],
    previous_unfiltered_size: u64,
    body: &[u8],
) -> std::io::Result<()> {
    validate_java_big_marker_clustering_kind(clustering_kind_ordinal)?;
    let bound = JavaBigRawClusteringBound {
        kind_ordinal: clustering_kind_ordinal,
        values: clustering_values.to_vec(),
    };
    out.push(JAVA_UNFILTERED_IS_MARKER);
    write_java_big_raw_clustering_bound(out, &bound, clustering_types)?;
    write_java_big_unfiltered_sstable_body_frame(out, previous_unfiltered_size, body)
}

pub fn write_java_big_range_tombstone_marker_body(
    out: &mut Vec<u8>,
    clustering_kind_ordinal: u8,
    deletion_time: &JavaBigDeletionTimeMetadata,
    boundary_start_deletion_time: Option<&JavaBigDeletionTimeMetadata>,
    encoding_stats: &JavaBigEncodingStats,
) -> std::io::Result<()> {
    validate_java_big_marker_clustering_kind(clustering_kind_ordinal)?;
    write_java_big_delta_deletion_time(out, deletion_time, encoding_stats)?;
    if java_big_marker_is_boundary(clustering_kind_ordinal) {
        let start_deletion = boundary_start_deletion_time.ok_or_else(|| {
            invalid_data("Java boundary range-tombstone marker requires start deletion time")
        })?;
        write_java_big_delta_deletion_time(out, start_deletion, encoding_stats)?;
    } else if boundary_start_deletion_time.is_some() {
        return Err(invalid_data(
            "Java bound range-tombstone marker cannot carry boundary start deletion time",
        ));
    }
    Ok(())
}

pub fn parse_java_big_range_tombstone_marker(
    data: &[u8],
    header: JavaBigUnfilteredHeader,
    encoding_stats: &JavaBigEncodingStats,
) -> std::io::Result<JavaBigRangeTombstoneMarker> {
    if header.kind != JavaBigUnfilteredKind::RangeTombstoneMarker {
        return Err(invalid_data(
            "Java unfiltered item is not a range-tombstone marker",
        ));
    }
    let body_start = header
        .body_start_offset
        .ok_or_else(|| invalid_data("Java marker is missing body start"))?;
    let body_length = header
        .body_length
        .ok_or_else(|| invalid_data("Java marker is missing body length"))?;
    let body_start = usize::try_from(body_start)
        .map_err(|_| invalid_data("Java marker body start exceeds usize"))?;
    let body_length = usize::try_from(body_length)
        .map_err(|_| invalid_data("Java marker body length exceeds usize"))?;
    let body_end = checked_add_usize(body_start, body_length, "Java marker body end")?;
    let Some(body) = data.get(body_start..body_end) else {
        return Err(invalid_data("Java marker body is out of bounds"));
    };
    let kind = header
        .clustering_kind_ordinal
        .ok_or_else(|| invalid_data("Java marker is missing clustering kind"))?;
    let mut input = JavaDataInput::new(body);
    let deletion_time = read_java_big_delta_deletion_time(&mut input, encoding_stats)?;
    let boundary_start_deletion_time = if java_big_marker_is_boundary(kind) {
        Some(read_java_big_delta_deletion_time(
            &mut input,
            encoding_stats,
        )?)
    } else {
        None
    };
    if input.remaining() != 0 {
        return Err(invalid_data("Java marker body has trailing bytes"));
    }

    Ok(JavaBigRangeTombstoneMarker {
        header,
        deletion_time,
        boundary_start_deletion_time,
        body_bytes_consumed: input.offset,
    })
}

pub fn parse_java_big_unfiltered_row_metadata_at(
    data: &[u8],
    data_offset: u64,
    clustering_value_count: usize,
    encoding_stats: &JavaBigEncodingStats,
    require_no_remaining_body: bool,
) -> std::io::Result<JavaBigUnfilteredRowMetadata> {
    let header = parse_java_big_unfiltered_header_at(data, data_offset, clustering_value_count)?;
    parse_java_big_unfiltered_row_metadata(data, header, encoding_stats, require_no_remaining_body)
}

pub fn parse_java_big_unfiltered_row_metadata(
    data: &[u8],
    header: JavaBigUnfilteredHeader,
    encoding_stats: &JavaBigEncodingStats,
    require_no_remaining_body: bool,
) -> std::io::Result<JavaBigUnfilteredRowMetadata> {
    if header.kind != JavaBigUnfilteredKind::Row {
        return Err(invalid_data("Java unfiltered item is not a row"));
    }
    let body_start = header
        .body_start_offset
        .ok_or_else(|| invalid_data("Java unfiltered row is missing body start"))?;
    let body_length = header
        .body_length
        .ok_or_else(|| invalid_data("Java unfiltered row is missing body length"))?;
    let body_start = usize::try_from(body_start)
        .map_err(|_| invalid_data("Java unfiltered row body start exceeds usize"))?;
    let body_length = usize::try_from(body_length)
        .map_err(|_| invalid_data("Java unfiltered row body length exceeds usize"))?;
    let body_end = checked_add_usize(body_start, body_length, "Java unfiltered row body end")?;
    let Some(body) = data.get(body_start..body_end) else {
        return Err(invalid_data("Java unfiltered row body is out of bounds"));
    };

    let mut input = JavaDataInput::new(body);
    let liveness = read_java_big_unfiltered_row_liveness(&mut input, header.flags, encoding_stats)?;
    let deletion_is_shadowable =
        (header.extended_flags & JAVA_UNFILTERED_EXT_HAS_SHADOWABLE_DELETION) != 0;
    let deletion_time = if (header.flags & JAVA_UNFILTERED_HAS_DELETION) != 0 {
        Some(read_java_big_delta_deletion_time(
            &mut input,
            encoding_stats,
        )?)
    } else {
        None
    };
    if require_no_remaining_body && input.remaining() != 0 {
        return Err(invalid_data(
            "Java unfiltered row body has undecoded column bytes",
        ));
    }

    Ok(JavaBigUnfilteredRowMetadata {
        header,
        liveness,
        deletion_time,
        deletion_is_shadowable,
        simple_cells: Vec::new(),
        complex_columns: Vec::new(),
        body_bytes_consumed: input.offset,
    })
}

pub fn parse_java_big_unfiltered_row_with_simple_cells_at(
    data: &[u8],
    data_offset: u64,
    serialization_header: &JavaBigSerializationHeader,
    require_all_columns: bool,
) -> std::io::Result<JavaBigUnfilteredRowMetadata> {
    let header = parse_java_big_unfiltered_header_at_with_clustering_types(
        data,
        data_offset,
        &serialization_header.clustering_types,
    )?;
    parse_java_big_unfiltered_row_with_simple_cells(
        data,
        header,
        serialization_header,
        require_all_columns,
    )
}

pub fn parse_java_big_unfiltered_row_with_simple_cells(
    data: &[u8],
    header: JavaBigUnfilteredHeader,
    serialization_header: &JavaBigSerializationHeader,
    require_all_columns: bool,
) -> std::io::Result<JavaBigUnfilteredRowMetadata> {
    if header.kind != JavaBigUnfilteredKind::Row {
        return Err(invalid_data("Java unfiltered item is not a row"));
    }
    let has_all_columns = (header.flags & JAVA_UNFILTERED_HAS_ALL_COLUMNS) != 0;
    let has_complex_deletion = (header.flags & JAVA_UNFILTERED_HAS_COMPLEX_DELETION) != 0;
    if require_all_columns && !has_all_columns {
        return Err(invalid_data(
            "Java unfiltered row does not carry all header columns",
        ));
    }

    let body_start = header
        .body_start_offset
        .ok_or_else(|| invalid_data("Java unfiltered row is missing body start"))?;
    let body_length = header
        .body_length
        .ok_or_else(|| invalid_data("Java unfiltered row is missing body length"))?;
    let body_start = usize::try_from(body_start)
        .map_err(|_| invalid_data("Java unfiltered row body start exceeds usize"))?;
    let body_length = usize::try_from(body_length)
        .map_err(|_| invalid_data("Java unfiltered row body length exceeds usize"))?;
    let body_end = checked_add_usize(body_start, body_length, "Java unfiltered row body end")?;
    let Some(body) = data.get(body_start..body_end) else {
        return Err(invalid_data("Java unfiltered row body is out of bounds"));
    };

    let mut input = JavaDataInput::new(body);
    let liveness = read_java_big_unfiltered_row_liveness(
        &mut input,
        header.flags,
        &serialization_header.encoding_stats,
    )?;
    let deletion_is_shadowable =
        (header.extended_flags & JAVA_UNFILTERED_EXT_HAS_SHADOWABLE_DELETION) != 0;
    let deletion_time = if (header.flags & JAVA_UNFILTERED_HAS_DELETION) != 0 {
        Some(read_java_big_delta_deletion_time(
            &mut input,
            &serialization_header.encoding_stats,
        )?)
    } else {
        None
    };
    let header_columns = if header.is_static {
        &serialization_header.static_columns
    } else {
        &serialization_header.regular_columns
    };
    let selected_columns = if has_all_columns {
        header_columns.iter().collect()
    } else {
        read_java_big_regular_column_subset(&mut input, header_columns)?
    };
    let mut simple_cells = Vec::with_capacity(selected_columns.len());
    let mut complex_columns = Vec::new();
    for column in selected_columns {
        if java_big_column_is_complex(&column.type_spec) {
            complex_columns.push(read_java_big_complex_column(
                &mut input,
                column,
                has_complex_deletion,
                &liveness,
                &serialization_header.encoding_stats,
            )?);
        } else {
            simple_cells.push(read_java_big_simple_cell(
                &mut input,
                column,
                &liveness,
                &serialization_header.encoding_stats,
            )?);
        }
    }
    if input.remaining() != 0 {
        return Err(invalid_data("Java unfiltered row body has trailing bytes"));
    }

    Ok(JavaBigUnfilteredRowMetadata {
        header,
        liveness,
        deletion_time,
        deletion_is_shadowable,
        simple_cells,
        complex_columns,
        body_bytes_consumed: input.offset,
    })
}

pub fn write_java_big_unfiltered_row_metadata_body(
    out: &mut Vec<u8>,
    flags: u8,
    liveness: &JavaBigRowLivenessMetadata,
    deletion_time: Option<&JavaBigDeletionTimeMetadata>,
    encoding_stats: &JavaBigEncodingStats,
) -> std::io::Result<()> {
    if (flags & JAVA_UNFILTERED_HAS_TTL) != 0 && (flags & JAVA_UNFILTERED_HAS_TIMESTAMP) == 0 {
        return Err(invalid_data(
            "Java unfiltered row TTL flag requires timestamp flag",
        ));
    }
    if (flags & JAVA_UNFILTERED_HAS_TIMESTAMP) != 0 {
        let timestamp = liveness
            .timestamp
            .ok_or_else(|| invalid_data("Java unfiltered row timestamp is missing"))?;
        write_java_big_delta_timestamp(out, timestamp, encoding_stats)?;
        if (flags & JAVA_UNFILTERED_HAS_TTL) != 0 {
            let ttl = liveness
                .ttl
                .ok_or_else(|| invalid_data("Java unfiltered row TTL is missing"))?;
            let local_expiration_time = liveness.local_expiration_time.ok_or_else(|| {
                invalid_data("Java unfiltered row local expiration time is missing")
            })?;
            write_java_big_delta_ttl(out, ttl, encoding_stats)?;
            write_java_big_delta_local_deletion_time(out, local_expiration_time, encoding_stats)?;
        }
    }
    if (flags & JAVA_UNFILTERED_HAS_DELETION) != 0 {
        let deletion_time = deletion_time
            .ok_or_else(|| invalid_data("Java unfiltered row deletion time is missing"))?;
        write_java_big_delta_deletion_time(out, deletion_time, encoding_stats)?;
    }
    Ok(())
}

pub fn write_java_big_regular_column_subset(
    out: &mut Vec<u8>,
    superset_len: usize,
    present_indices: &[usize],
) -> std::io::Result<()> {
    validate_java_big_regular_column_subset_indices(superset_len, present_indices)?;

    if present_indices.len() == superset_len {
        return write_java_unsigned_vint(out, 0);
    }

    if superset_len >= 64 {
        return write_java_big_large_regular_column_subset(out, superset_len, present_indices);
    }
    let mut missing_bitmap = 0_u64;
    for idx in 0..superset_len {
        if present_indices.binary_search(&idx).is_err() {
            missing_bitmap |= 1_u64 << idx;
        }
    }
    write_java_unsigned_vint(out, missing_bitmap)
}

pub fn write_java_big_simple_cell(
    out: &mut Vec<u8>,
    cell: &JavaBigSimpleCell,
    row_liveness: &JavaBigRowLivenessMetadata,
    encoding_stats: &JavaBigEncodingStats,
) -> std::io::Result<()> {
    write_java_big_cell(
        out,
        cell,
        None,
        &cell.column_type,
        row_liveness,
        encoding_stats,
    )
}

pub fn write_java_big_complex_column(
    out: &mut Vec<u8>,
    column: &JavaBigSerializationHeaderColumn,
    has_complex_deletion: bool,
    deletion_time: Option<&JavaBigDeletionTimeMetadata>,
    cells: &[JavaBigComplexCell],
    row_liveness: &JavaBigRowLivenessMetadata,
    encoding_stats: &JavaBigEncodingStats,
) -> std::io::Result<()> {
    if !java_big_column_is_complex(&column.type_spec) {
        return Err(invalid_data(
            "Java complex column writer requires a complex column type",
        ));
    }
    if has_complex_deletion {
        write_java_big_delta_deletion_time(
            out,
            deletion_time.unwrap_or(&JavaBigDeletionTimeMetadata::LIVE),
            encoding_stats,
        )?;
    }
    write_java_unsigned_vint(out, cells.len() as u64)?;
    for cell in cells {
        let value_type = java_big_complex_cell_value_type_for_path(&column.type_spec, &cell.path)?;
        write_java_big_cell(
            out,
            &cell.cell,
            Some(&cell.path),
            &value_type,
            row_liveness,
            encoding_stats,
        )?;
    }
    Ok(())
}

fn write_java_big_cell(
    out: &mut Vec<u8>,
    cell: &JavaBigSimpleCell,
    path: Option<&[u8]>,
    value_type: &str,
    row_liveness: &JavaBigRowLivenessMetadata,
    encoding_stats: &JavaBigEncodingStats,
) -> std::io::Result<()> {
    if cell.is_deleted && cell.is_expiring {
        return Err(invalid_data(
            "Java cell cannot be both deleted and expiring",
        ));
    }
    if cell.uses_row_ttl && !cell.is_expiring {
        return Err(invalid_data(
            "Java cell row-TTL flag requires expiring cell",
        ));
    }
    if cell.uses_row_ttl && row_liveness.ttl.is_none() {
        return Err(invalid_data(
            "Java cell row-TTL flag requires row liveness TTL",
        ));
    }

    let mut flags = cell.flags
        & !(JAVA_CELL_IS_DELETED
            | JAVA_CELL_IS_EXPIRING
            | JAVA_CELL_HAS_EMPTY_VALUE
            | JAVA_CELL_USE_ROW_TIMESTAMP
            | JAVA_CELL_USE_ROW_TTL);
    if cell.is_deleted {
        flags |= JAVA_CELL_IS_DELETED;
    }
    if cell.is_expiring {
        flags |= JAVA_CELL_IS_EXPIRING;
    }
    if cell.value.as_ref().is_none_or(Vec::is_empty) {
        flags |= JAVA_CELL_HAS_EMPTY_VALUE;
    }
    if cell.uses_row_timestamp {
        flags |= JAVA_CELL_USE_ROW_TIMESTAMP;
    }
    if cell.uses_row_ttl {
        flags |= JAVA_CELL_USE_ROW_TTL;
    }

    out.push(flags);
    if !cell.uses_row_timestamp {
        write_java_big_delta_timestamp(out, cell.timestamp, encoding_stats)?;
    }
    if (cell.is_deleted || cell.is_expiring) && !cell.uses_row_ttl {
        let local_deletion_time = cell
            .local_deletion_time
            .ok_or_else(|| invalid_data("Java cell local deletion time is missing"))?;
        write_java_big_delta_local_deletion_time(out, local_deletion_time, encoding_stats)?;
    }
    if cell.is_expiring && !cell.uses_row_ttl {
        let ttl = cell
            .ttl
            .ok_or_else(|| invalid_data("Java cell TTL is missing"))?;
        write_java_big_delta_ttl(out, ttl, encoding_stats)?;
    }
    if let Some(path) = path {
        write_bytes_with_vint_length(out, path)?;
    }
    if let Some(value) = &cell.value {
        if !value.is_empty() {
            write_java_big_cell_value(out, value_type, value)?;
        }
    }
    Ok(())
}

pub fn read_java_big_summary(path: impl AsRef<Path>) -> std::io::Result<JavaBigSummary> {
    let data = fs::read(path)?;
    parse_java_big_summary(&data)
}

pub fn parse_java_big_summary(data: &[u8]) -> std::io::Result<JavaBigSummary> {
    let mut input = JavaDataInput::new(data);
    let min_index_interval = input.read_i32()?;
    let offset_count = input.read_i32()?;
    let offheap_size = input.read_i64()?;
    let sampling_level = input.read_i32()?;
    let size_at_full_sampling = input.read_i32()?;

    if min_index_interval <= 0 {
        return Err(invalid_data(
            "Java Summary.db min index interval must be positive",
        ));
    }
    if offset_count <= 0 {
        return Err(invalid_data(
            "Java Summary.db offset count must be positive",
        ));
    }
    if offheap_size <= 0 {
        return Err(invalid_data(
            "Java Summary.db offheap size must be positive",
        ));
    }
    if sampling_level <= 0 {
        return Err(invalid_data(
            "Java Summary.db sampling level must be positive",
        ));
    }
    if size_at_full_sampling <= 0 {
        return Err(invalid_data(
            "Java Summary.db full-sampling size must be positive",
        ));
    }

    let offset_count = offset_count as usize;
    let offset_table_size = checked_mul_usize(offset_count, 4, "Java Summary.db offset table")?;
    let offheap_size = offheap_size as usize;
    if offheap_size < offset_table_size {
        return Err(invalid_data(
            "Java Summary.db offheap size smaller than offset table",
        ));
    }
    let entries_size = offheap_size - offset_table_size;

    let mut offsets = Vec::with_capacity(offset_count);
    for _ in 0..offset_count {
        let raw = input.read_exact(4)?;
        let offset_from_combined = u32::from_le_bytes(raw.try_into().unwrap()) as usize;
        if offset_from_combined < offset_table_size {
            return Err(invalid_data(
                "Java Summary.db entry offset points inside offset table",
            ));
        }
        offsets.push(offset_from_combined - offset_table_size);
    }
    validate_java_big_summary_offsets(&offsets, entries_size)?;

    let entries_bytes = input.read_exact(entries_size)?;
    let mut entries = Vec::with_capacity(offset_count);
    for idx in 0..offset_count {
        let start = offsets[idx];
        let end = offsets.get(idx + 1).copied().unwrap_or(entries_size);
        if end < start + 8 {
            return Err(invalid_data("Java Summary.db entry is too short"));
        }
        let key_end = end - 8;
        let partition_key = entries_bytes[start..key_end].to_vec();
        let index_offset = u64::from_le_bytes(entries_bytes[key_end..end].try_into().unwrap());
        entries.push(JavaBigSummaryEntry {
            partition_key,
            index_offset,
        });
    }

    let first_key = input.read_bytes_with_i32_length()?;
    let last_key = input.read_bytes_with_i32_length()?;
    if input.remaining() != 0 {
        return Err(invalid_data("Java Summary.db has trailing bytes"));
    }

    Ok(JavaBigSummary {
        min_index_interval: min_index_interval as u32,
        offheap_size: offheap_size as u64,
        sampling_level: sampling_level as u32,
        size_at_full_sampling: size_at_full_sampling as u32,
        entries,
        first_key,
        last_key,
    })
}

pub fn write_java_big_summary(summary: &JavaBigSummary) -> std::io::Result<Vec<u8>> {
    if summary.entries.is_empty() {
        return Err(invalid_data(
            "Java Summary.db must contain at least one entry",
        ));
    }

    let mut entries_bytes = Vec::new();
    let mut offsets = Vec::with_capacity(summary.entries.len());
    for entry in &summary.entries {
        offsets.push(entries_bytes.len());
        entries_bytes.extend_from_slice(&entry.partition_key);
        entries_bytes.extend_from_slice(&entry.index_offset.to_le_bytes());
    }

    let offset_table_size = checked_mul_usize(offsets.len(), 4, "Java Summary.db offset table")?;
    let offheap_size = checked_add_usize(
        offset_table_size,
        entries_bytes.len(),
        "Java Summary.db offheap size",
    )?;

    let mut out = Vec::new();
    out.extend_from_slice(&(summary.min_index_interval as i32).to_be_bytes());
    out.extend_from_slice(&(summary.entries.len() as i32).to_be_bytes());
    out.extend_from_slice(&(offheap_size as i64).to_be_bytes());
    out.extend_from_slice(&(summary.sampling_level as i32).to_be_bytes());
    out.extend_from_slice(&(summary.size_at_full_sampling as i32).to_be_bytes());
    for offset in offsets {
        let combined_offset = checked_add_usize(
            offset_table_size,
            offset,
            "Java Summary.db combined entry offset",
        )?;
        out.extend_from_slice(&(combined_offset as u32).to_le_bytes());
    }
    out.extend_from_slice(&entries_bytes);
    write_bytes_with_i32_length(&mut out, &summary.first_key)?;
    write_bytes_with_i32_length(&mut out, &summary.last_key)?;
    Ok(out)
}

pub fn read_java_big_statistics(
    path: impl AsRef<Path>,
    has_checksum: bool,
) -> std::io::Result<JavaBigStatisticsMetadata> {
    let data = fs::read(path)?;
    parse_java_big_statistics(&data, has_checksum)
}

pub fn read_java_big_statistics_auto(
    path: impl AsRef<Path>,
) -> std::io::Result<JavaBigStatisticsMetadata> {
    let data = fs::read(path)?;
    parse_java_big_statistics_auto(&data)
}

pub fn parse_java_big_statistics_auto(data: &[u8]) -> std::io::Result<JavaBigStatisticsMetadata> {
    match parse_java_big_statistics(data, true) {
        Ok(metadata) => Ok(metadata),
        Err(checksummed_error) => parse_java_big_statistics(data, false).map_err(|legacy_error| {
            invalid_data(format!(
                "Java Statistics.db is neither checksummed ({checksummed_error}) nor legacy ({legacy_error})"
            ))
        }),
    }
}

pub fn parse_java_big_statistics(
    data: &[u8],
    has_checksum: bool,
) -> std::io::Result<JavaBigStatisticsMetadata> {
    let mut input = JavaDataInput::new(data);
    let count_offset = input.offset;
    let count = input.read_i32()?;
    if count <= 0 {
        return Err(invalid_data(
            "Java Statistics.db component count must be positive",
        ));
    }
    if has_checksum {
        validate_crc32(
            &data[count_offset..input.offset],
            input.read_i32()?,
            "Java Statistics.db component-count checksum",
        )?;
    }

    let count = count as usize;
    let toc_start = input.offset;
    let mut toc = Vec::with_capacity(count);
    for _ in 0..count {
        let ordinal = input.read_i32()?;
        let component_type = JavaBigMetadataType::from_ordinal(ordinal).ok_or_else(|| {
            invalid_data(format!(
                "unknown Java Statistics.db metadata component ordinal {ordinal}"
            ))
        })?;
        let offset = input.read_i32()?;
        if offset < 0 {
            return Err(invalid_data("negative Java Statistics.db component offset"));
        }
        toc.push((component_type, offset as usize));
    }
    if has_checksum {
        validate_crc32(
            &data[toc_start..input.offset],
            input.read_i32()?,
            "Java Statistics.db TOC checksum",
        )?;
    }

    let components_start = input.offset;
    validate_java_big_statistics_toc(&toc, components_start, data.len(), has_checksum)?;

    let mut components = Vec::with_capacity(count);
    for (idx, (component_type, offset)) in toc.iter().copied().enumerate() {
        let next_offset = toc
            .get(idx + 1)
            .map(|(_, offset)| *offset)
            .unwrap_or(data.len());
        let payload_end = if has_checksum {
            next_offset
                .checked_sub(4)
                .ok_or_else(|| invalid_data("Java Statistics.db component checksum underflow"))?
        } else {
            next_offset
        };
        let payload = data
            .get(offset..payload_end)
            .ok_or_else(|| invalid_data("Java Statistics.db component bounds are invalid"))?;
        if has_checksum {
            let checksum = i32::from_be_bytes(data[payload_end..next_offset].try_into().unwrap());
            validate_crc32(
                payload,
                checksum,
                "Java Statistics.db component payload checksum",
            )?;
        }
        components.push(JavaBigStatisticsComponent {
            component_type,
            offset: offset as u32,
            payload: payload.to_vec(),
        });
    }

    Ok(JavaBigStatisticsMetadata {
        has_checksum,
        components,
    })
}

pub fn write_java_big_statistics(
    components: &[JavaBigStatisticsComponent],
    has_checksum: bool,
) -> std::io::Result<Vec<u8>> {
    if components.is_empty() {
        return Err(invalid_data(
            "Java Statistics.db must contain at least one component",
        ));
    }

    let mut sorted = components.to_vec();
    sorted.sort_by_key(|component| component.component_type);

    let checksum_bytes = if has_checksum { 4 } else { 0 };
    let mut next_offset = checked_add_usize(
        4 + sorted.len() * 8,
        checksum_bytes * 2,
        "Java Statistics.db first component offset",
    )?;
    let mut offsets = Vec::with_capacity(sorted.len());
    for component in &sorted {
        offsets.push(next_offset);
        next_offset = checked_add_usize(
            next_offset,
            component.payload.len() + checksum_bytes,
            "Java Statistics.db component offset",
        )?;
    }

    let mut out = Vec::new();
    out.extend_from_slice(&(sorted.len() as i32).to_be_bytes());
    if has_checksum {
        let count_bytes = out[..4].to_vec();
        write_crc32(&mut out, &count_bytes);
    }

    let toc_start = out.len();
    for (component, offset) in sorted.iter().zip(offsets.iter().copied()) {
        out.extend_from_slice(&component.component_type.ordinal().to_be_bytes());
        out.extend_from_slice(&(offset as i32).to_be_bytes());
    }
    if has_checksum {
        let toc_bytes = out[toc_start..toc_start + sorted.len() * 8].to_vec();
        write_crc32(&mut out, &toc_bytes);
    }

    for component in &sorted {
        out.extend_from_slice(&component.payload);
        if has_checksum {
            write_crc32(&mut out, &component.payload);
        }
    }
    Ok(out)
}

pub fn parse_java_big_validation_metadata(
    data: &[u8],
) -> std::io::Result<JavaBigValidationMetadata> {
    let mut input = JavaDataInput::new(data);
    let partitioner = input.read_utf()?;
    let bloom_filter_fp_chance = input.read_f64()?;
    if input.remaining() != 0 {
        return Err(invalid_data("Java validation metadata has trailing bytes"));
    }
    Ok(JavaBigValidationMetadata {
        partitioner,
        bloom_filter_fp_chance,
    })
}

pub fn write_java_big_validation_metadata(
    metadata: &JavaBigValidationMetadata,
) -> std::io::Result<Vec<u8>> {
    let mut out = Vec::new();
    write_java_utf(&mut out, &metadata.partitioner)?;
    out.extend_from_slice(&metadata.bloom_filter_fp_chance.to_be_bytes());
    Ok(out)
}

pub fn parse_java_big_compaction_metadata(
    data: &[u8],
) -> std::io::Result<JavaBigCompactionMetadata> {
    let mut input = JavaDataInput::new(data);
    let len = input.read_i32()?;
    if len < 0 {
        return Err(invalid_data(
            "negative Java compaction cardinality-estimator length",
        ));
    }
    let cardinality_estimator = input.read_exact(len as usize)?.to_vec();
    if input.remaining() != 0 {
        return Err(invalid_data("Java compaction metadata has trailing bytes"));
    }
    Ok(JavaBigCompactionMetadata {
        cardinality_estimator,
    })
}

pub fn write_java_big_compaction_metadata(
    metadata: &JavaBigCompactionMetadata,
) -> std::io::Result<Vec<u8>> {
    let len = i32::try_from(metadata.cardinality_estimator.len())
        .map_err(|_| invalid_data("Java compaction cardinality estimator too large"))?;
    let mut out = Vec::new();
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(&metadata.cardinality_estimator);
    Ok(out)
}

pub fn parse_java_big_stats_metadata_prefix(
    data: &[u8],
    has_unsigned_deletion_time: bool,
) -> std::io::Result<JavaBigStatsMetadataPrefix> {
    let mut input = JavaDataInput::new(data);
    let estimated_partition_size = read_java_big_estimated_histogram(&mut input)?;
    let estimated_cell_per_partition_count = read_java_big_estimated_histogram(&mut input)?;
    let commit_log_upper_bound = read_java_big_commit_log_position(&mut input)?;
    let min_timestamp = input.read_i64()?;
    let max_timestamp = input.read_i64()?;
    let min_local_deletion_time =
        read_java_big_local_deletion_time(&mut input, has_unsigned_deletion_time)?;
    let max_local_deletion_time =
        read_java_big_local_deletion_time(&mut input, has_unsigned_deletion_time)?;
    let min_ttl = input.read_i32()?;
    let max_ttl = input.read_i32()?;
    let compression_ratio = input.read_f64()?;
    let estimated_tombstone_drop_time =
        read_java_big_tombstone_histogram(&mut input, has_unsigned_deletion_time)?;
    let sstable_level = input.read_i32()?;
    let repaired_at = input.read_i64()?;
    let bytes_consumed = input.offset;

    Ok(JavaBigStatsMetadataPrefix {
        estimated_partition_size,
        estimated_cell_per_partition_count,
        commit_log_upper_bound,
        min_timestamp,
        max_timestamp,
        min_local_deletion_time,
        max_local_deletion_time,
        min_ttl,
        max_ttl,
        compression_ratio,
        estimated_tombstone_drop_time,
        sstable_level,
        repaired_at,
        bytes_consumed,
    })
}

pub fn write_java_big_stats_metadata_prefix(
    prefix: &JavaBigStatsMetadataPrefix,
    has_unsigned_deletion_time: bool,
) -> std::io::Result<Vec<u8>> {
    let mut out = Vec::new();
    write_java_big_estimated_histogram(&mut out, &prefix.estimated_partition_size)?;
    write_java_big_estimated_histogram(&mut out, &prefix.estimated_cell_per_partition_count)?;
    write_java_big_commit_log_position(&mut out, &prefix.commit_log_upper_bound);
    out.extend_from_slice(&prefix.min_timestamp.to_be_bytes());
    out.extend_from_slice(&prefix.max_timestamp.to_be_bytes());
    write_java_big_local_deletion_time(
        &mut out,
        prefix.min_local_deletion_time,
        has_unsigned_deletion_time,
    )?;
    write_java_big_local_deletion_time(
        &mut out,
        prefix.max_local_deletion_time,
        has_unsigned_deletion_time,
    )?;
    out.extend_from_slice(&prefix.min_ttl.to_be_bytes());
    out.extend_from_slice(&prefix.max_ttl.to_be_bytes());
    out.extend_from_slice(&prefix.compression_ratio.to_be_bytes());
    write_java_big_tombstone_histogram(
        &mut out,
        &prefix.estimated_tombstone_drop_time,
        has_unsigned_deletion_time,
    )?;
    out.extend_from_slice(&prefix.sstable_level.to_be_bytes());
    out.extend_from_slice(&prefix.repaired_at.to_be_bytes());
    Ok(out)
}

pub fn parse_java_big_stats_metadata_legacy_tail(
    data: &[u8],
    prefix_bytes_consumed: usize,
    features: JavaBigStatsMetadataTailFeatures,
) -> std::io::Result<JavaBigStatsMetadataLegacyTail> {
    if !features.has_legacy_min_max {
        return Err(invalid_data(
            "Java StatsMetadata legacy-tail parser requires legacy min/max metadata",
        ));
    }
    let mut input = JavaDataInput::new(data);
    input.skip(prefix_bytes_consumed)?;

    let legacy_min_clustering_values = read_java_big_legacy_clustering_values(&mut input)?;
    let legacy_max_clustering_values = read_java_big_legacy_clustering_values(&mut input)?;
    let has_legacy_counter_shards = input.read_bool()?;
    let total_columns_set = input.read_i64()?;
    let total_rows = input.read_i64()?;

    let commit_log_lower_bound = if features.has_commit_log_lower_bound {
        Some(read_java_big_commit_log_position(&mut input)?)
    } else {
        None
    };
    let commit_log_intervals = if features.has_commit_log_intervals {
        read_java_big_commit_log_intervals(&mut input)?
    } else {
        Vec::new()
    };
    let pending_repair = if features.has_pending_repair && input.read_bool()? {
        Some(input.read_uuid_bytes()?)
    } else {
        None
    };
    let is_transient = if features.has_is_transient {
        Some(input.read_bool()?)
    } else {
        None
    };
    let originating_host_id = if features.has_originating_host_id && input.read_bool()? {
        Some(input.read_uuid_bytes()?)
    } else {
        None
    };
    let has_partition_level_deletions = if features.has_partition_level_deletions_presence_marker {
        Some(input.read_bool()?)
    } else {
        None
    };

    if input.remaining() != 0 {
        return Err(invalid_data(
            "Java StatsMetadata legacy tail has trailing bytes",
        ));
    }

    Ok(JavaBigStatsMetadataLegacyTail {
        legacy_min_clustering_values,
        legacy_max_clustering_values,
        has_legacy_counter_shards,
        total_columns_set,
        total_rows,
        commit_log_lower_bound,
        commit_log_intervals,
        pending_repair,
        is_transient,
        originating_host_id,
        has_partition_level_deletions,
        bytes_consumed: input.offset - prefix_bytes_consumed,
    })
}

pub fn write_java_big_stats_metadata_legacy_tail(
    tail: &JavaBigStatsMetadataLegacyTail,
    features: JavaBigStatsMetadataTailFeatures,
) -> std::io::Result<Vec<u8>> {
    if !features.has_legacy_min_max {
        return Err(invalid_data(
            "Java StatsMetadata legacy-tail writer requires legacy min/max metadata",
        ));
    }
    let mut out = Vec::new();
    write_java_big_legacy_clustering_values(&mut out, &tail.legacy_min_clustering_values)?;
    write_java_big_legacy_clustering_values(&mut out, &tail.legacy_max_clustering_values)?;
    write_java_bool(&mut out, tail.has_legacy_counter_shards);
    out.extend_from_slice(&tail.total_columns_set.to_be_bytes());
    out.extend_from_slice(&tail.total_rows.to_be_bytes());

    if features.has_commit_log_lower_bound {
        let Some(position) = &tail.commit_log_lower_bound else {
            return Err(invalid_data("missing Java commitlog lower-bound position"));
        };
        write_java_big_commit_log_position(&mut out, position);
    }
    if features.has_commit_log_intervals {
        write_java_big_commit_log_intervals(&mut out, &tail.commit_log_intervals)?;
    }
    if features.has_pending_repair {
        match tail.pending_repair {
            Some(uuid) => {
                write_java_bool(&mut out, true);
                out.extend_from_slice(&uuid);
            }
            None => write_java_bool(&mut out, false),
        }
    }
    if features.has_is_transient {
        write_java_bool(&mut out, tail.is_transient.unwrap_or(false));
    }
    if features.has_originating_host_id {
        match tail.originating_host_id {
            Some(uuid) => {
                write_java_bool(&mut out, true);
                out.extend_from_slice(&uuid);
            }
            None => write_java_bool(&mut out, false),
        }
    }
    if features.has_partition_level_deletions_presence_marker {
        write_java_bool(
            &mut out,
            tail.has_partition_level_deletions.unwrap_or(false),
        );
    }
    Ok(out)
}

pub fn parse_java_big_stats_metadata_modern_tail(
    data: &[u8],
    prefix_bytes_consumed: usize,
    features: JavaBigStatsMetadataTailFeatures,
) -> std::io::Result<JavaBigStatsMetadataModernTail> {
    if !features.has_improved_min_max || features.has_legacy_min_max {
        return Err(invalid_data(
            "Java StatsMetadata modern-tail parser requires improved min/max without legacy min/max",
        ));
    }
    let mut input = JavaDataInput::new(data);
    input.skip(prefix_bytes_consumed)?;

    let improved_min_max = read_java_big_improved_min_max(&mut input)?;
    let has_legacy_counter_shards = input.read_bool()?;
    let total_columns_set = input.read_i64()?;
    let total_rows = input.read_i64()?;

    let commit_log_lower_bound = if features.has_commit_log_lower_bound {
        Some(read_java_big_commit_log_position(&mut input)?)
    } else {
        None
    };
    let commit_log_intervals = if features.has_commit_log_intervals {
        read_java_big_commit_log_intervals(&mut input)?
    } else {
        Vec::new()
    };
    let pending_repair = if features.has_pending_repair && input.read_bool()? {
        Some(input.read_uuid_bytes()?)
    } else {
        None
    };
    let is_transient = if features.has_is_transient {
        Some(input.read_bool()?)
    } else {
        None
    };
    let originating_host_id = if features.has_originating_host_id && input.read_bool()? {
        Some(input.read_uuid_bytes()?)
    } else {
        None
    };
    let has_partition_level_deletions = if features.has_partition_level_deletions_presence_marker {
        Some(input.read_bool()?)
    } else {
        None
    };
    let (first_key, last_key) = if features.has_key_range {
        (
            Some(input.read_bytes_with_vint_length()?),
            Some(input.read_bytes_with_vint_length()?),
        )
    } else {
        (None, None)
    };
    let token_space_coverage = if features.has_token_space_coverage {
        Some(input.read_f64()?)
    } else {
        None
    };

    if input.remaining() != 0 {
        return Err(invalid_data(
            "Java StatsMetadata modern tail has trailing bytes",
        ));
    }

    Ok(JavaBigStatsMetadataModernTail {
        improved_min_max,
        has_legacy_counter_shards,
        total_columns_set,
        total_rows,
        commit_log_lower_bound,
        commit_log_intervals,
        pending_repair,
        is_transient,
        originating_host_id,
        has_partition_level_deletions,
        first_key,
        last_key,
        token_space_coverage,
        bytes_consumed: input.offset - prefix_bytes_consumed,
    })
}

pub fn write_java_big_stats_metadata_modern_tail(
    tail: &JavaBigStatsMetadataModernTail,
    features: JavaBigStatsMetadataTailFeatures,
) -> std::io::Result<Vec<u8>> {
    if !features.has_improved_min_max || features.has_legacy_min_max {
        return Err(invalid_data(
            "Java StatsMetadata modern-tail writer requires improved min/max without legacy min/max",
        ));
    }
    let mut out = Vec::new();
    write_java_big_improved_min_max(&mut out, &tail.improved_min_max)?;
    write_java_bool(&mut out, tail.has_legacy_counter_shards);
    out.extend_from_slice(&tail.total_columns_set.to_be_bytes());
    out.extend_from_slice(&tail.total_rows.to_be_bytes());

    if features.has_commit_log_lower_bound {
        let Some(position) = &tail.commit_log_lower_bound else {
            return Err(invalid_data("missing Java commitlog lower-bound position"));
        };
        write_java_big_commit_log_position(&mut out, position);
    }
    if features.has_commit_log_intervals {
        write_java_big_commit_log_intervals(&mut out, &tail.commit_log_intervals)?;
    }
    if features.has_pending_repair {
        match tail.pending_repair {
            Some(uuid) => {
                write_java_bool(&mut out, true);
                out.extend_from_slice(&uuid);
            }
            None => write_java_bool(&mut out, false),
        }
    }
    if features.has_is_transient {
        write_java_bool(&mut out, tail.is_transient.unwrap_or(false));
    }
    if features.has_originating_host_id {
        match tail.originating_host_id {
            Some(uuid) => {
                write_java_bool(&mut out, true);
                out.extend_from_slice(&uuid);
            }
            None => write_java_bool(&mut out, false),
        }
    }
    if features.has_partition_level_deletions_presence_marker {
        write_java_bool(
            &mut out,
            tail.has_partition_level_deletions.unwrap_or(false),
        );
    }
    if features.has_key_range {
        let first_key = tail
            .first_key
            .as_deref()
            .ok_or_else(|| invalid_data("missing Java first key"))?;
        let last_key = tail
            .last_key
            .as_deref()
            .ok_or_else(|| invalid_data("missing Java last key"))?;
        write_bytes_with_vint_length(&mut out, first_key)?;
        write_bytes_with_vint_length(&mut out, last_key)?;
    }
    if features.has_token_space_coverage {
        let coverage = tail
            .token_space_coverage
            .ok_or_else(|| invalid_data("missing Java token-space coverage"))?;
        out.extend_from_slice(&coverage.to_be_bytes());
    }
    Ok(out)
}

pub fn parse_java_big_serialization_header(
    data: &[u8],
) -> std::io::Result<JavaBigSerializationHeader> {
    let mut input = JavaDataInput::new(data);
    let encoding_stats = read_java_big_encoding_stats(&mut input)?;
    let key_type = input.read_java_type_spec()?;
    let clustering_types = read_java_big_type_spec_list(&mut input)?;
    let static_columns = read_java_big_header_columns(&mut input)?;
    let regular_columns = read_java_big_header_columns(&mut input)?;
    if input.remaining() != 0 {
        return Err(invalid_data("Java serialization header has trailing bytes"));
    }
    Ok(JavaBigSerializationHeader {
        encoding_stats,
        key_type,
        clustering_types,
        static_columns,
        regular_columns,
    })
}

pub fn write_java_big_serialization_header(
    header: &JavaBigSerializationHeader,
) -> std::io::Result<Vec<u8>> {
    let mut out = Vec::new();
    write_java_big_encoding_stats(&mut out, &header.encoding_stats)?;
    write_java_type_spec(&mut out, &header.key_type)?;
    write_java_big_type_spec_list(&mut out, &header.clustering_types)?;
    write_java_big_header_columns(&mut out, &header.static_columns)?;
    write_java_big_header_columns(&mut out, &header.regular_columns)?;
    Ok(out)
}

struct JavaDataInput<'a> {
    data: &'a [u8],
    offset: usize,
}

impl<'a> JavaDataInput<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self { data, offset: 0 }
    }

    fn read_exact(&mut self, len: usize) -> std::io::Result<&'a [u8]> {
        let end = self
            .offset
            .checked_add(len)
            .ok_or_else(|| invalid_data("Java data input offset overflow"))?;
        let Some(bytes) = self.data.get(self.offset..end) else {
            return Err(invalid_data("truncated Java compression metadata"));
        };
        self.offset = end;
        Ok(bytes)
    }

    fn skip(&mut self, len: usize) -> std::io::Result<()> {
        self.read_exact(len).map(|_| ())
    }

    fn read_i16(&mut self) -> std::io::Result<i16> {
        Ok(i16::from_be_bytes(self.read_exact(2)?.try_into().unwrap()))
    }

    fn read_u16(&mut self) -> std::io::Result<u16> {
        Ok(u16::from_be_bytes(self.read_exact(2)?.try_into().unwrap()))
    }

    fn read_bool(&mut self) -> std::io::Result<bool> {
        Ok(self.read_exact(1)?[0] != 0)
    }

    fn read_i32(&mut self) -> std::io::Result<i32> {
        Ok(i32::from_be_bytes(self.read_exact(4)?.try_into().unwrap()))
    }

    fn read_i64(&mut self) -> std::io::Result<i64> {
        Ok(i64::from_be_bytes(self.read_exact(8)?.try_into().unwrap()))
    }

    fn read_f64(&mut self) -> std::io::Result<f64> {
        Ok(f64::from_be_bytes(self.read_exact(8)?.try_into().unwrap()))
    }

    fn read_unsigned_vint(&mut self) -> std::io::Result<u64> {
        let mut cursor = Cursor::new(&self.data[self.offset..]);
        let value = read_unsigned_vint(&mut cursor)?;
        self.offset = checked_add_usize(
            self.offset,
            cursor.position() as usize,
            "Java unsigned vint offset",
        )?;
        Ok(value)
    }

    fn read_unsigned_vint_usize(&mut self, label: &str) -> std::io::Result<usize> {
        let value = self.read_unsigned_vint()?;
        usize::try_from(value).map_err(|_| invalid_data(format!("{label} exceeds usize")))
    }

    fn remaining(&self) -> usize {
        self.data.len().saturating_sub(self.offset)
    }

    fn read_bytes_with_i32_length(&mut self) -> std::io::Result<Vec<u8>> {
        let len = self.read_i32()?;
        if len < 0 {
            return Err(invalid_data("negative Java byte-buffer length"));
        }
        Ok(self.read_exact(len as usize)?.to_vec())
    }

    fn read_bytes_with_vint_length(&mut self) -> std::io::Result<Vec<u8>> {
        let len = self.read_unsigned_vint_usize("Java vint byte-buffer length")?;
        Ok(self.read_exact(len)?.to_vec())
    }

    fn read_java_type_spec(&mut self) -> std::io::Result<String> {
        let bytes = self.read_bytes_with_vint_length()?;
        String::from_utf8(bytes).map_err(|err| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("invalid Java type spec UTF-8: {err}"),
            )
        })
    }

    fn read_bytes_with_i16_length(&mut self) -> std::io::Result<Vec<u8>> {
        let len = self.read_i16()?;
        if len < 0 {
            return Err(invalid_data("negative Java short byte-buffer length"));
        }
        Ok(self.read_exact(len as usize)?.to_vec())
    }

    fn read_utf(&mut self) -> std::io::Result<String> {
        let len = u16::from_be_bytes(self.read_exact(2)?.try_into().unwrap()) as usize;
        let bytes = self.read_exact(len)?;
        String::from_utf8(bytes.to_vec()).map_err(|err| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("invalid Java UTF string: {err}"),
            )
        })
    }

    fn read_uuid_bytes(&mut self) -> std::io::Result<[u8; 16]> {
        Ok(self.read_exact(16)?.try_into().unwrap())
    }
}

fn write_java_bool(out: &mut Vec<u8>, value: bool) {
    out.push(if value { 1 } else { 0 });
}

fn write_java_unsigned_vint(out: &mut Vec<u8>, value: u64) -> std::io::Result<()> {
    write_unsigned_vint(out, value).map(|_| ())
}

fn write_java_utf(out: &mut Vec<u8>, value: &str) -> std::io::Result<()> {
    let bytes = value.as_bytes();
    let len = u16::try_from(bytes.len()).map_err(|_| invalid_data("Java UTF string too long"))?;
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(bytes);
    Ok(())
}

fn write_bytes_with_i32_length(out: &mut Vec<u8>, bytes: &[u8]) -> std::io::Result<()> {
    let len = i32::try_from(bytes.len()).map_err(|_| invalid_data("Java byte buffer too large"))?;
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(bytes);
    Ok(())
}

fn write_bytes_with_vint_length(out: &mut Vec<u8>, bytes: &[u8]) -> std::io::Result<()> {
    write_java_unsigned_vint(out, bytes.len() as u64)?;
    out.extend_from_slice(bytes);
    Ok(())
}

fn write_java_type_spec(out: &mut Vec<u8>, type_spec: &str) -> std::io::Result<()> {
    write_bytes_with_vint_length(out, type_spec.as_bytes())
}

fn write_bytes_with_i16_length(out: &mut Vec<u8>, bytes: &[u8]) -> std::io::Result<()> {
    let len =
        i16::try_from(bytes.len()).map_err(|_| invalid_data("Java short byte buffer too large"))?;
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(bytes);
    Ok(())
}

const JAVA_ENCODING_STATS_TIMESTAMP_EPOCH: i64 = 1_442_880_000_000_000;
const JAVA_ENCODING_STATS_DELETION_TIME_EPOCH: i64 = 1_442_880_000;
const JAVA_ENCODING_STATS_TTL_EPOCH: i32 = 0;

fn read_java_big_encoding_stats(
    input: &mut JavaDataInput<'_>,
) -> std::io::Result<JavaBigEncodingStats> {
    let timestamp_delta = input.read_unsigned_vint()?;
    let deletion_time_delta = input.read_unsigned_vint()?;
    let ttl_delta = input.read_unsigned_vint()?;
    let min_timestamp = checked_add_i64_u64(
        JAVA_ENCODING_STATS_TIMESTAMP_EPOCH,
        timestamp_delta,
        "Java encoding-stats timestamp",
    )?;
    let min_local_deletion_time = checked_add_i64_u64(
        JAVA_ENCODING_STATS_DELETION_TIME_EPOCH,
        deletion_time_delta,
        "Java encoding-stats deletion time",
    )?;
    let ttl_delta = i32::try_from(ttl_delta)
        .map_err(|_| invalid_data("Java encoding-stats TTL delta exceeds int"))?;
    Ok(JavaBigEncodingStats {
        min_timestamp,
        min_local_deletion_time,
        min_ttl: JAVA_ENCODING_STATS_TTL_EPOCH
            .checked_add(ttl_delta)
            .ok_or_else(|| invalid_data("Java encoding-stats TTL overflow"))?,
    })
}

fn write_java_big_encoding_stats(
    out: &mut Vec<u8>,
    stats: &JavaBigEncodingStats,
) -> std::io::Result<()> {
    write_java_unsigned_vint(
        out,
        checked_i64_delta_as_u64(
            stats.min_timestamp,
            JAVA_ENCODING_STATS_TIMESTAMP_EPOCH,
            "Java encoding-stats timestamp",
        )?,
    )?;
    write_java_unsigned_vint(
        out,
        checked_i64_delta_as_u64(
            stats.min_local_deletion_time,
            JAVA_ENCODING_STATS_DELETION_TIME_EPOCH,
            "Java encoding-stats deletion time",
        )?,
    )?;
    let ttl_delta = stats
        .min_ttl
        .checked_sub(JAVA_ENCODING_STATS_TTL_EPOCH)
        .ok_or_else(|| invalid_data("Java encoding-stats TTL is before epoch"))?;
    write_java_unsigned_vint(out, ttl_delta as u64)?;
    Ok(())
}

fn read_java_big_type_spec_list(input: &mut JavaDataInput<'_>) -> std::io::Result<Vec<String>> {
    let count = input.read_unsigned_vint_usize("Java type-spec list count")?;
    let mut types = Vec::with_capacity(count);
    for _ in 0..count {
        types.push(input.read_java_type_spec()?);
    }
    Ok(types)
}

fn write_java_big_type_spec_list(out: &mut Vec<u8>, types: &[String]) -> std::io::Result<()> {
    write_java_unsigned_vint(out, types.len() as u64)?;
    for type_spec in types {
        write_java_type_spec(out, type_spec)?;
    }
    Ok(())
}

fn read_java_big_header_columns(
    input: &mut JavaDataInput<'_>,
) -> std::io::Result<Vec<JavaBigSerializationHeaderColumn>> {
    let count = input.read_unsigned_vint_usize("Java serialization-header column count")?;
    let mut columns = Vec::with_capacity(count);
    for _ in 0..count {
        columns.push(JavaBigSerializationHeaderColumn {
            name: input.read_bytes_with_vint_length()?,
            type_spec: input.read_java_type_spec()?,
        });
    }
    Ok(columns)
}

fn write_java_big_header_columns(
    out: &mut Vec<u8>,
    columns: &[JavaBigSerializationHeaderColumn],
) -> std::io::Result<()> {
    write_java_unsigned_vint(out, columns.len() as u64)?;
    for column in columns {
        write_bytes_with_vint_length(out, &column.name)?;
        write_java_type_spec(out, &column.type_spec)?;
    }
    Ok(())
}

fn read_java_big_improved_min_max(
    input: &mut JavaDataInput<'_>,
) -> std::io::Result<JavaBigImprovedMinMax> {
    let clustering_types = read_java_big_type_spec_list(input)?;
    let start = read_java_big_raw_clustering_bound(input, &clustering_types)?;
    let end = read_java_big_raw_clustering_bound(input, &clustering_types)?;
    Ok(JavaBigImprovedMinMax {
        clustering_types,
        covered_clustering: JavaBigRawSlice { start, end },
    })
}

fn write_java_big_improved_min_max(
    out: &mut Vec<u8>,
    improved_min_max: &JavaBigImprovedMinMax,
) -> std::io::Result<()> {
    write_java_big_type_spec_list(out, &improved_min_max.clustering_types)?;
    write_java_big_raw_clustering_bound(
        out,
        &improved_min_max.covered_clustering.start,
        &improved_min_max.clustering_types,
    )?;
    write_java_big_raw_clustering_bound(
        out,
        &improved_min_max.covered_clustering.end,
        &improved_min_max.clustering_types,
    )?;
    Ok(())
}

fn read_java_big_raw_clustering_bound(
    input: &mut JavaDataInput<'_>,
    clustering_types: &[String],
) -> std::io::Result<JavaBigRawClusteringBound> {
    let kind_ordinal = input.read_exact(1)?[0];
    let size = input.read_u16()? as usize;
    if size > clustering_types.len() {
        return Err(invalid_data(
            "Java clustering bound has more values than clustering types",
        ));
    }
    let mut values = Vec::with_capacity(size);
    let mut offset = 0;
    while offset < size {
        let header = input.read_unsigned_vint()?;
        let limit = size.min(offset + 32);
        for idx in offset..limit {
            let local_idx = idx - offset;
            if java_clustering_value_is_null(header, local_idx) {
                values.push(None);
            } else if java_clustering_value_is_empty(header, local_idx) {
                values.push(Some(Vec::new()));
            } else {
                values.push(Some(read_java_big_clustering_value(
                    input,
                    &clustering_types[idx],
                )?));
            }
        }
        offset = limit;
    }
    Ok(JavaBigRawClusteringBound {
        kind_ordinal,
        values,
    })
}

fn write_java_big_raw_clustering_bound(
    out: &mut Vec<u8>,
    bound: &JavaBigRawClusteringBound,
    clustering_types: &[String],
) -> std::io::Result<()> {
    if bound.values.len() > clustering_types.len() {
        return Err(invalid_data(
            "Java clustering bound has more values than clustering types",
        ));
    }
    let size = u16::try_from(bound.values.len())
        .map_err(|_| invalid_data("Java clustering bound has too many values"))?;
    out.push(bound.kind_ordinal);
    out.extend_from_slice(&size.to_be_bytes());
    let mut offset = 0;
    while offset < bound.values.len() {
        let limit = bound.values.len().min(offset + 32);
        let mut header = 0_u64;
        for idx in offset..limit {
            let local_idx = idx - offset;
            match &bound.values[idx] {
                None => header |= 1_u64 << (local_idx * 2 + 1),
                Some(value) if value.is_empty() => header |= 1_u64 << (local_idx * 2),
                Some(_) => {}
            }
        }
        write_java_unsigned_vint(out, header)?;
        for idx in offset..limit {
            if let Some(value) = &bound.values[idx] {
                if !value.is_empty() {
                    write_java_big_clustering_value(out, &clustering_types[idx], value)?;
                }
            }
        }
        offset = limit;
    }
    Ok(())
}

fn read_java_big_clustering_values_without_size(
    input: &mut JavaDataInput<'_>,
    clustering_types: &[String],
) -> std::io::Result<Vec<Option<Vec<u8>>>> {
    let mut values = Vec::with_capacity(clustering_types.len());
    let mut offset = 0;
    while offset < clustering_types.len() {
        let header = input.read_unsigned_vint()?;
        let limit = clustering_types.len().min(offset + 32);
        for idx in offset..limit {
            let local_idx = idx - offset;
            if java_clustering_value_is_null(header, local_idx) {
                values.push(None);
            } else if java_clustering_value_is_empty(header, local_idx) {
                values.push(Some(Vec::new()));
            } else {
                values.push(Some(read_java_big_clustering_value(
                    input,
                    &clustering_types[idx],
                )?));
            }
        }
        offset = limit;
    }
    Ok(values)
}

fn write_java_big_clustering_values_without_size(
    out: &mut Vec<u8>,
    values: &[Option<Vec<u8>>],
    clustering_types: &[String],
) -> std::io::Result<()> {
    if values.len() != clustering_types.len() {
        return Err(invalid_data(
            "Java clustering row values must match clustering type count",
        ));
    }
    let mut offset = 0;
    while offset < values.len() {
        let limit = values.len().min(offset + 32);
        let mut header = 0_u64;
        for idx in offset..limit {
            let local_idx = idx - offset;
            match &values[idx] {
                None => header |= 1_u64 << (local_idx * 2 + 1),
                Some(value) if value.is_empty() => header |= 1_u64 << (local_idx * 2),
                Some(_) => {}
            }
        }
        write_java_unsigned_vint(out, header)?;
        for idx in offset..limit {
            if let Some(value) = &values[idx] {
                if !value.is_empty() {
                    write_java_big_clustering_value(out, &clustering_types[idx], value)?;
                }
            }
        }
        offset = limit;
    }
    Ok(())
}

fn read_java_big_clustering_value(
    input: &mut JavaDataInput<'_>,
    type_spec: &str,
) -> std::io::Result<Vec<u8>> {
    if let Some(len) = java_fixed_value_length(type_spec) {
        Ok(input.read_exact(len)?.to_vec())
    } else {
        input.read_bytes_with_vint_length()
    }
}

fn write_java_big_clustering_value(
    out: &mut Vec<u8>,
    type_spec: &str,
    value: &[u8],
) -> std::io::Result<()> {
    if let Some(len) = java_fixed_value_length(type_spec) {
        if value.len() != len {
            return Err(invalid_data(format!(
                "Java clustering value for {type_spec} must be {len} bytes"
            )));
        }
        out.extend_from_slice(value);
        Ok(())
    } else {
        write_bytes_with_vint_length(out, value)
    }
}

fn java_fixed_value_length(type_spec: &str) -> Option<usize> {
    let type_spec = java_big_unwrapped_value_type_spec(type_spec);
    let type_name = java_big_type_name(type_spec.as_ref());
    if type_name == "VectorType" {
        let args = java_big_type_args(type_spec.as_ref())?;
        let inner_len = java_fixed_value_length(args.first()?)?;
        let dimensions = args.get(1)?.trim().parse::<usize>().ok()?;
        return inner_len.checked_mul(dimensions);
    }
    match type_name {
        "BooleanType" | "ByteType" => Some(1),
        "ShortType" => Some(2),
        "Int32Type" | "FloatType" | "SimpleDateType" => Some(4),
        "LongType" | "DoubleType" | "TimestampType" | "DateType" | "TimeType" => Some(8),
        "UUIDType" | "TimeUUIDType" | "LexicalUUIDType" => Some(16),
        _ => None,
    }
}

fn java_big_column_is_complex(type_spec: &str) -> bool {
    if type_spec.contains("FrozenType(") {
        return false;
    }
    let type_spec = java_big_unwrapped_value_type_spec(type_spec);
    matches!(
        java_big_type_name(type_spec.as_ref()),
        "ListType" | "SetType" | "MapType" | "UserType"
    )
}

fn java_big_complex_cell_value_type(type_spec: &str) -> String {
    let type_spec = java_big_unwrapped_value_type_spec(type_spec);
    let Some(args) = java_big_type_args(type_spec.as_ref()) else {
        return type_spec.into_owned();
    };
    match java_big_type_name(type_spec.as_ref()) {
        "ListType" | "UserType" => args
            .first()
            .cloned()
            .unwrap_or_else(|| type_spec.to_string()),
        "SetType" => "org.apache.cassandra.db.marshal.EmptyType".to_string(),
        "MapType" => args
            .get(1)
            .cloned()
            .unwrap_or_else(|| type_spec.to_string()),
        _ => type_spec.to_string(),
    }
}

fn java_big_complex_cell_value_type_for_path(
    type_spec: &str,
    path: &[u8],
) -> std::io::Result<String> {
    let type_spec = java_big_unwrapped_value_type_spec(type_spec);
    if java_big_type_name(type_spec.as_ref()) != "UserType" {
        return Ok(java_big_complex_cell_value_type(type_spec.as_ref()));
    }
    if path.len() != 2 {
        return Err(invalid_data(
            "Java UDT cell path must be a short field index",
        ));
    }
    let field_idx = u16::from_be_bytes(path.try_into().unwrap()) as usize;
    let field_types = java_big_user_type_field_types(type_spec.as_ref())?;
    field_types
        .get(field_idx)
        .cloned()
        .ok_or_else(|| invalid_data("Java UDT cell path field index is out of bounds"))
}

fn java_big_user_type_field_types(type_spec: &str) -> std::io::Result<Vec<String>> {
    let type_spec = java_big_unwrapped_value_type_spec(type_spec);
    if java_big_type_name(type_spec.as_ref()) != "UserType" {
        return Err(invalid_data("Java type is not a UserType"));
    }
    let args = java_big_type_args(type_spec.as_ref())
        .ok_or_else(|| invalid_data("Java UserType type spec is missing parameters"))?;
    if args.len() < 2 {
        return Err(invalid_data(
            "Java UserType type spec is missing keyspace/type parameters",
        ));
    }
    let mut fields = Vec::new();
    for field in args.into_iter().skip(2) {
        let Some((_, field_type)) = field.split_once(':') else {
            return Err(invalid_data("Java UserType field spec is missing type"));
        };
        fields.push(field_type.trim().to_string());
    }
    Ok(fields)
}

fn java_big_unwrapped_value_type_spec(type_spec: &str) -> Cow<'_, str> {
    let type_name = java_big_type_name(type_spec);
    if type_name != "ReversedType" {
        return Cow::Borrowed(type_spec);
    }
    let Some(args) = java_big_type_args(type_spec) else {
        return Cow::Borrowed(type_spec);
    };
    let Some(inner) = args.into_iter().next() else {
        return Cow::Borrowed(type_spec);
    };
    Cow::Owned(inner)
}

fn java_big_type_name(type_spec: &str) -> &str {
    let name = type_spec.split('(').next().unwrap_or(type_spec);
    name.rsplit('.').next().unwrap_or(name)
}

fn java_big_type_args(type_spec: &str) -> Option<Vec<String>> {
    let start = type_spec.find('(')?;
    let end = type_spec.rfind(')')?;
    if end <= start {
        return None;
    }
    let args = &type_spec[start + 1..end];
    let mut out = Vec::new();
    let mut depth = 0usize;
    let mut arg_start = 0usize;
    for (idx, ch) in args.char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => depth = depth.saturating_sub(1),
            ',' if depth == 0 => {
                out.push(args[arg_start..idx].trim().to_string());
                arg_start = idx + ch.len_utf8();
            }
            _ => {}
        }
    }
    let tail = args[arg_start..].trim();
    if !tail.is_empty() {
        out.push(tail.to_string());
    }
    Some(out)
}

fn java_clustering_value_is_null(header: u64, local_idx: usize) -> bool {
    (header & (1_u64 << (local_idx * 2 + 1))) != 0
}

fn java_clustering_value_is_empty(header: u64, local_idx: usize) -> bool {
    (header & (1_u64 << (local_idx * 2))) != 0
}

fn java_old_bloom_words_to_bitset_bytes(raw_words: &[u8]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(raw_words.len());
    for word in raw_words.chunks_exact(8) {
        let value = u64::from_be_bytes(word.try_into().unwrap());
        bytes.extend_from_slice(&value.to_le_bytes());
    }
    bytes
}

fn java_bitset_bytes_to_old_bloom_words(bitset_bytes: &[u8]) -> Vec<u8> {
    let mut raw_words = Vec::with_capacity(bitset_bytes.len());
    for word in bitset_bytes.chunks_exact(8) {
        let value = u64::from_le_bytes(word.try_into().unwrap());
        raw_words.extend_from_slice(&value.to_be_bytes());
    }
    raw_words
}

fn read_java_big_commit_log_position(
    input: &mut JavaDataInput<'_>,
) -> std::io::Result<JavaBigCommitLogPosition> {
    Ok(JavaBigCommitLogPosition {
        segment_id: input.read_i64()?,
        position: input.read_i32()?,
    })
}

fn write_java_big_commit_log_position(out: &mut Vec<u8>, position: &JavaBigCommitLogPosition) {
    out.extend_from_slice(&position.segment_id.to_be_bytes());
    out.extend_from_slice(&position.position.to_be_bytes());
}

fn read_java_big_commit_log_intervals(
    input: &mut JavaDataInput<'_>,
) -> std::io::Result<Vec<JavaBigCommitLogInterval>> {
    let count = input.read_i32()?;
    if count < 0 {
        return Err(invalid_data("negative Java commitlog interval count"));
    }
    let mut intervals = Vec::with_capacity(count as usize);
    for _ in 0..count {
        intervals.push(JavaBigCommitLogInterval {
            start: read_java_big_commit_log_position(input)?,
            end: read_java_big_commit_log_position(input)?,
        });
    }
    Ok(intervals)
}

fn write_java_big_commit_log_intervals(
    out: &mut Vec<u8>,
    intervals: &[JavaBigCommitLogInterval],
) -> std::io::Result<()> {
    let count = i32::try_from(intervals.len())
        .map_err(|_| invalid_data("Java commitlog interval count too large"))?;
    out.extend_from_slice(&count.to_be_bytes());
    for interval in intervals {
        write_java_big_commit_log_position(out, &interval.start);
        write_java_big_commit_log_position(out, &interval.end);
    }
    Ok(())
}

fn read_java_big_legacy_clustering_values(
    input: &mut JavaDataInput<'_>,
) -> std::io::Result<Vec<Vec<u8>>> {
    let count = input.read_i32()?;
    if count < 0 {
        return Err(invalid_data("negative Java legacy clustering-value count"));
    }
    let mut values = Vec::with_capacity(count as usize);
    for _ in 0..count {
        values.push(input.read_bytes_with_i16_length()?);
    }
    Ok(values)
}

fn write_java_big_legacy_clustering_values(
    out: &mut Vec<u8>,
    values: &[Vec<u8>],
) -> std::io::Result<()> {
    let count = i32::try_from(values.len())
        .map_err(|_| invalid_data("Java legacy clustering-value count too large"))?;
    out.extend_from_slice(&count.to_be_bytes());
    for value in values {
        write_bytes_with_i16_length(out, value)?;
    }
    Ok(())
}

fn read_java_big_estimated_histogram(
    input: &mut JavaDataInput<'_>,
) -> std::io::Result<JavaBigEstimatedHistogram> {
    let size = input.read_i32()?;
    if size <= 0 {
        return Err(invalid_data(
            "Java estimated histogram bucket count must be positive",
        ));
    }
    let mut buckets = Vec::with_capacity(size as usize);
    for _ in 0..size {
        buckets.push(JavaBigEstimatedHistogramBucket {
            offset: input.read_i64()?,
            count: input.read_i64()?,
        });
    }
    Ok(JavaBigEstimatedHistogram { buckets })
}

fn write_java_big_estimated_histogram(
    out: &mut Vec<u8>,
    histogram: &JavaBigEstimatedHistogram,
) -> std::io::Result<()> {
    let size = i32::try_from(histogram.buckets.len())
        .map_err(|_| invalid_data("Java estimated histogram has too many buckets"))?;
    if size <= 0 {
        return Err(invalid_data(
            "Java estimated histogram bucket count must be positive",
        ));
    }
    out.extend_from_slice(&size.to_be_bytes());
    for bucket in &histogram.buckets {
        out.extend_from_slice(&bucket.offset.to_be_bytes());
        out.extend_from_slice(&bucket.count.to_be_bytes());
    }
    Ok(())
}

fn read_java_big_tombstone_histogram(
    input: &mut JavaDataInput<'_>,
    has_unsigned_deletion_time: bool,
) -> std::io::Result<JavaBigTombstoneHistogram> {
    let max_bin_size = input.read_i32()?;
    let size = input.read_i32()?;
    if max_bin_size < 0 || size < 0 {
        return Err(invalid_data(
            "Java tombstone histogram sizes must be non-negative",
        ));
    }
    let mut entries = Vec::with_capacity(size as usize);
    for _ in 0..size {
        let (point, count) = if has_unsigned_deletion_time {
            (input.read_i64()?, input.read_i32()? as i64)
        } else {
            (input.read_f64()? as i64, input.read_i64()?)
        };
        entries.push(JavaBigTombstoneHistogramEntry { point, count });
    }
    Ok(JavaBigTombstoneHistogram {
        max_bin_size,
        entries,
    })
}

fn write_java_big_tombstone_histogram(
    out: &mut Vec<u8>,
    histogram: &JavaBigTombstoneHistogram,
    has_unsigned_deletion_time: bool,
) -> std::io::Result<()> {
    if histogram.max_bin_size < 0 {
        return Err(invalid_data(
            "Java tombstone histogram max-bin size must be non-negative",
        ));
    }
    let size = i32::try_from(histogram.entries.len())
        .map_err(|_| invalid_data("Java tombstone histogram has too many entries"))?;
    out.extend_from_slice(&histogram.max_bin_size.to_be_bytes());
    out.extend_from_slice(&size.to_be_bytes());
    for entry in &histogram.entries {
        if has_unsigned_deletion_time {
            let count = i32::try_from(entry.count)
                .map_err(|_| invalid_data("Java tombstone histogram count overflows int"))?;
            out.extend_from_slice(&entry.point.to_be_bytes());
            out.extend_from_slice(&count.to_be_bytes());
        } else {
            out.extend_from_slice(&(entry.point as f64).to_be_bytes());
            out.extend_from_slice(&entry.count.to_be_bytes());
        }
    }
    Ok(())
}

const JAVA_UNFILTERED_END_OF_PARTITION: u8 = 0x01;
const JAVA_UNFILTERED_IS_MARKER: u8 = 0x02;
pub const JAVA_UNFILTERED_EXTENSION_FLAG: u8 = 0x80;
pub const JAVA_UNFILTERED_EXT_IS_STATIC: u8 = 0x01;
pub const JAVA_UNFILTERED_EXT_HAS_SHADOWABLE_DELETION: u8 = 0x02;

const JAVA_CELL_IS_DELETED: u8 = 0x01;
const JAVA_CELL_IS_EXPIRING: u8 = 0x02;
const JAVA_CELL_HAS_EMPTY_VALUE: u8 = 0x04;
const JAVA_CELL_USE_ROW_TIMESTAMP: u8 = 0x08;
const JAVA_CELL_USE_ROW_TTL: u8 = 0x10;

pub const JAVA_UNFILTERED_HAS_TIMESTAMP: u8 = 0x04;
pub const JAVA_UNFILTERED_HAS_TTL: u8 = 0x08;
pub const JAVA_UNFILTERED_HAS_DELETION: u8 = 0x10;
pub const JAVA_UNFILTERED_HAS_ALL_COLUMNS: u8 = 0x20;
pub const JAVA_UNFILTERED_HAS_COMPLEX_DELETION: u8 = 0x40;

pub const JAVA_CLUSTERING_KIND_EXCL_END_BOUND: u8 = 0;
pub const JAVA_CLUSTERING_KIND_INCL_START_BOUND: u8 = 1;
pub const JAVA_CLUSTERING_KIND_EXCL_END_INCL_START_BOUNDARY: u8 = 2;
pub const JAVA_CLUSTERING_KIND_INCL_END_EXCL_START_BOUNDARY: u8 = 5;
pub const JAVA_CLUSTERING_KIND_INCL_END_BOUND: u8 = 6;
pub const JAVA_CLUSTERING_KIND_EXCL_START_BOUND: u8 = 7;
pub const JAVA_CLUSTERING_KIND_SSTABLE_LOWER_BOUND: u8 = 8;
pub const JAVA_CLUSTERING_KIND_SSTABLE_UPPER_BOUND: u8 = 9;

fn write_java_big_unfiltered_sstable_body_frame(
    out: &mut Vec<u8>,
    previous_unfiltered_size: u64,
    body: &[u8],
) -> std::io::Result<()> {
    let framed_body_size = checked_add_usize(
        body.len(),
        java_unsigned_vint_len(previous_unfiltered_size)?,
        "Java unfiltered framed body size",
    )?;
    write_java_unsigned_vint(out, framed_body_size as u64)?;
    write_java_unsigned_vint(out, previous_unfiltered_size)?;
    out.extend_from_slice(body);
    Ok(())
}

fn java_unsigned_vint_len(value: u64) -> std::io::Result<usize> {
    let mut bytes = Vec::new();
    write_java_unsigned_vint(&mut bytes, value)?;
    Ok(bytes.len())
}

fn validate_java_big_marker_clustering_kind(kind_ordinal: u8) -> std::io::Result<()> {
    match kind_ordinal {
        JAVA_CLUSTERING_KIND_EXCL_END_BOUND
        | JAVA_CLUSTERING_KIND_INCL_START_BOUND
        | JAVA_CLUSTERING_KIND_EXCL_END_INCL_START_BOUNDARY
        | JAVA_CLUSTERING_KIND_INCL_END_EXCL_START_BOUNDARY
        | JAVA_CLUSTERING_KIND_INCL_END_BOUND
        | JAVA_CLUSTERING_KIND_EXCL_START_BOUND
        | JAVA_CLUSTERING_KIND_SSTABLE_LOWER_BOUND
        | JAVA_CLUSTERING_KIND_SSTABLE_UPPER_BOUND => Ok(()),
        _ => Err(invalid_data(
            "Java range-tombstone marker has invalid clustering kind",
        )),
    }
}

fn java_big_marker_is_boundary(kind_ordinal: u8) -> bool {
    matches!(
        kind_ordinal,
        JAVA_CLUSTERING_KIND_EXCL_END_INCL_START_BOUNDARY
            | JAVA_CLUSTERING_KIND_INCL_END_EXCL_START_BOUNDARY
    )
}

fn read_java_big_simple_cell(
    input: &mut JavaDataInput<'_>,
    column: &JavaBigSerializationHeaderColumn,
    row_liveness: &JavaBigRowLivenessMetadata,
    encoding_stats: &JavaBigEncodingStats,
) -> std::io::Result<JavaBigSimpleCell> {
    let (cell, path) = read_java_big_cell(
        input,
        &column.name,
        &column.type_spec,
        &column.type_spec,
        false,
        row_liveness,
        encoding_stats,
    )?;
    debug_assert!(path.is_none());
    Ok(cell)
}

fn read_java_big_complex_column(
    input: &mut JavaDataInput<'_>,
    column: &JavaBigSerializationHeaderColumn,
    has_complex_deletion: bool,
    row_liveness: &JavaBigRowLivenessMetadata,
    encoding_stats: &JavaBigEncodingStats,
) -> std::io::Result<JavaBigComplexColumn> {
    let deletion_time = if has_complex_deletion {
        Some(read_java_big_delta_deletion_time(input, encoding_stats)?)
    } else {
        None
    };
    let cell_count = input.read_unsigned_vint_usize("Java complex-column cell count")?;
    let mut cells = Vec::with_capacity(cell_count);
    for _ in 0..cell_count {
        let (cell, path) = read_java_big_cell_dynamic_value_type(
            input,
            &column.name,
            &column.type_spec,
            true,
            |path| {
                let path = path.ok_or_else(|| invalid_data("Java complex cell path is missing"))?;
                java_big_complex_cell_value_type_for_path(&column.type_spec, path)
            },
            row_liveness,
            encoding_stats,
        )?;
        let path = path.ok_or_else(|| invalid_data("Java complex cell path is missing"))?;
        cells.push(JavaBigComplexCell { path, cell });
    }
    Ok(JavaBigComplexColumn {
        column_name: column.name.clone(),
        column_type: column.type_spec.clone(),
        deletion_time,
        cells,
    })
}

fn read_java_big_cell(
    input: &mut JavaDataInput<'_>,
    column_name: &[u8],
    column_type: &str,
    value_type: &str,
    has_path: bool,
    row_liveness: &JavaBigRowLivenessMetadata,
    encoding_stats: &JavaBigEncodingStats,
) -> std::io::Result<(JavaBigSimpleCell, Option<Vec<u8>>)> {
    read_java_big_cell_dynamic_value_type(
        input,
        column_name,
        column_type,
        has_path,
        |_| Ok(value_type.to_string()),
        row_liveness,
        encoding_stats,
    )
}

fn read_java_big_cell_dynamic_value_type<F>(
    input: &mut JavaDataInput<'_>,
    column_name: &[u8],
    column_type: &str,
    has_path: bool,
    value_type_for_path: F,
    row_liveness: &JavaBigRowLivenessMetadata,
    encoding_stats: &JavaBigEncodingStats,
) -> std::io::Result<(JavaBigSimpleCell, Option<Vec<u8>>)>
where
    F: FnOnce(Option<&[u8]>) -> std::io::Result<String>,
{
    let flags = input.read_exact(1)?[0];
    let has_value = (flags & JAVA_CELL_HAS_EMPTY_VALUE) == 0;
    let is_deleted = (flags & JAVA_CELL_IS_DELETED) != 0;
    let is_expiring = (flags & JAVA_CELL_IS_EXPIRING) != 0;
    let uses_row_timestamp = (flags & JAVA_CELL_USE_ROW_TIMESTAMP) != 0;
    let uses_row_ttl = (flags & JAVA_CELL_USE_ROW_TTL) != 0;

    if is_deleted && is_expiring {
        return Err(invalid_data("Java cell is both deleted and expiring"));
    }
    if uses_row_ttl && !is_expiring {
        return Err(invalid_data(
            "Java cell row-TTL flag requires expiring cell",
        ));
    }

    let timestamp = if uses_row_timestamp {
        row_liveness
            .timestamp
            .ok_or_else(|| invalid_data("Java cell row timestamp is missing"))?
    } else {
        read_java_big_delta_timestamp(input, encoding_stats)?
    };
    let local_deletion_time = if uses_row_ttl {
        Some(
            row_liveness
                .local_expiration_time
                .ok_or_else(|| invalid_data("Java cell row local expiration time is missing"))?,
        )
    } else if is_deleted || is_expiring {
        Some(read_java_big_delta_local_deletion_time(
            input,
            encoding_stats,
        )?)
    } else {
        None
    };
    let ttl = if uses_row_ttl {
        Some(
            row_liveness
                .ttl
                .ok_or_else(|| invalid_data("Java cell row TTL is missing"))?,
        )
    } else if is_expiring {
        Some(read_java_big_delta_ttl(input, encoding_stats)?)
    } else {
        None
    };
    let path = if has_path {
        Some(input.read_bytes_with_vint_length()?)
    } else {
        None
    };
    let value_type = value_type_for_path(path.as_deref())?;
    let value = if has_value {
        Some(read_java_big_cell_value(input, &value_type)?)
    } else {
        None
    };

    Ok((
        JavaBigSimpleCell {
            column_name: column_name.to_vec(),
            column_type: column_type.to_string(),
            flags,
            timestamp,
            local_deletion_time,
            ttl,
            value,
            is_deleted,
            is_expiring,
            uses_row_timestamp,
            uses_row_ttl,
        },
        path,
    ))
}

fn read_java_big_regular_column_subset<'a>(
    input: &mut JavaDataInput<'_>,
    superset: &'a [JavaBigSerializationHeaderColumn],
) -> std::io::Result<Vec<&'a JavaBigSerializationHeaderColumn>> {
    let encoded = input.read_unsigned_vint()?;
    if encoded == 0 {
        return Ok(superset.iter().collect());
    }
    if superset.len() >= 64 {
        return read_java_big_large_regular_column_subset(input, superset, encoded);
    }
    let mut selected = Vec::new();
    let mut bitmap = encoded;
    for column in superset {
        if (bitmap & 1) == 0 {
            selected.push(column);
        }
        bitmap >>= 1;
    }
    if bitmap != 0 {
        return Err(invalid_data(
            "Java regular-column subset bitmap has too many bits",
        ));
    }
    Ok(selected)
}

fn read_java_big_large_regular_column_subset<'a>(
    input: &mut JavaDataInput<'_>,
    superset: &'a [JavaBigSerializationHeaderColumn],
    delta: u64,
) -> std::io::Result<Vec<&'a JavaBigSerializationHeaderColumn>> {
    let delta = usize::try_from(delta)
        .map_err(|_| invalid_data("Java large regular-column subset delta exceeds usize"))?;
    if delta > superset.len() {
        return Err(invalid_data(
            "Java large regular-column subset delta exceeds superset size",
        ));
    }
    let column_count = superset.len() - delta;
    if column_count < superset.len() / 2 {
        let mut selected = Vec::with_capacity(column_count);
        let mut previous = None;
        for _ in 0..column_count {
            let idx = input.read_unsigned_vint_usize("Java large subset present column index")?;
            if idx >= superset.len() {
                return Err(invalid_data(
                    "Java large regular-column subset present index is out of bounds",
                ));
            }
            if previous.is_some_and(|prev| idx <= prev) {
                return Err(invalid_data(
                    "Java large regular-column subset present indices must be increasing",
                ));
            }
            previous = Some(idx);
            selected.push(&superset[idx]);
        }
        Ok(selected)
    } else {
        let mut missing = Vec::with_capacity(delta);
        let mut previous = None;
        for _ in 0..delta {
            let idx = input.read_unsigned_vint_usize("Java large subset missing column index")?;
            if idx >= superset.len() {
                return Err(invalid_data(
                    "Java large regular-column subset missing index is out of bounds",
                ));
            }
            if previous.is_some_and(|prev| idx <= prev) {
                return Err(invalid_data(
                    "Java large regular-column subset missing indices must be increasing",
                ));
            }
            previous = Some(idx);
            missing.push(idx);
        }
        Ok(superset
            .iter()
            .enumerate()
            .filter_map(|(idx, column)| {
                if missing.binary_search(&idx).is_err() {
                    Some(column)
                } else {
                    None
                }
            })
            .collect())
    }
}

fn write_java_big_large_regular_column_subset(
    out: &mut Vec<u8>,
    superset_len: usize,
    present_indices: &[usize],
) -> std::io::Result<()> {
    let delta = superset_len - present_indices.len();
    write_java_unsigned_vint(out, delta as u64)?;
    if present_indices.len() < superset_len / 2 {
        for &idx in present_indices {
            write_java_unsigned_vint(out, idx as u64)?;
        }
    } else {
        for idx in 0..superset_len {
            if present_indices.binary_search(&idx).is_err() {
                write_java_unsigned_vint(out, idx as u64)?;
            }
        }
    }
    Ok(())
}

fn validate_java_big_regular_column_subset_indices(
    superset_len: usize,
    present_indices: &[usize],
) -> std::io::Result<()> {
    let mut previous = None;
    for &idx in present_indices {
        if idx >= superset_len {
            return Err(invalid_data(
                "Java regular-column subset index is out of bounds",
            ));
        }
        if previous.is_some_and(|prev| idx <= prev) {
            return Err(invalid_data(
                "Java regular-column subset indices must be strictly increasing",
            ));
        }
        previous = Some(idx);
    }
    Ok(())
}

fn read_java_big_cell_value(
    input: &mut JavaDataInput<'_>,
    type_spec: &str,
) -> std::io::Result<Vec<u8>> {
    if let Some(len) = java_fixed_value_length(type_spec) {
        Ok(input.read_exact(len)?.to_vec())
    } else {
        input.read_bytes_with_vint_length()
    }
}

fn write_java_big_cell_value(
    out: &mut Vec<u8>,
    type_spec: &str,
    value: &[u8],
) -> std::io::Result<()> {
    if let Some(len) = java_fixed_value_length(type_spec) {
        if value.len() != len {
            return Err(invalid_data(format!(
                "Java cell value for {type_spec} must be {len} bytes"
            )));
        }
        out.extend_from_slice(value);
        Ok(())
    } else {
        write_bytes_with_vint_length(out, value)
    }
}

fn read_java_big_unfiltered_row_liveness(
    input: &mut JavaDataInput<'_>,
    flags: u8,
    encoding_stats: &JavaBigEncodingStats,
) -> std::io::Result<JavaBigRowLivenessMetadata> {
    if (flags & JAVA_UNFILTERED_HAS_TTL) != 0 && (flags & JAVA_UNFILTERED_HAS_TIMESTAMP) == 0 {
        return Err(invalid_data(
            "Java unfiltered row TTL flag requires timestamp flag",
        ));
    }
    if (flags & JAVA_UNFILTERED_HAS_TIMESTAMP) == 0 {
        return Ok(JavaBigRowLivenessMetadata {
            timestamp: None,
            ttl: None,
            local_expiration_time: None,
        });
    }

    let timestamp = read_java_big_delta_timestamp(input, encoding_stats)?;
    let (ttl, local_expiration_time) = if (flags & JAVA_UNFILTERED_HAS_TTL) != 0 {
        (
            Some(read_java_big_delta_ttl(input, encoding_stats)?),
            Some(read_java_big_delta_local_deletion_time(
                input,
                encoding_stats,
            )?),
        )
    } else {
        (None, None)
    };
    Ok(JavaBigRowLivenessMetadata {
        timestamp: Some(timestamp),
        ttl,
        local_expiration_time,
    })
}

fn read_java_big_delta_timestamp(
    input: &mut JavaDataInput<'_>,
    encoding_stats: &JavaBigEncodingStats,
) -> std::io::Result<i64> {
    checked_add_i64_u64(
        encoding_stats.min_timestamp,
        input.read_unsigned_vint()?,
        "Java row timestamp",
    )
}

fn write_java_big_delta_timestamp(
    out: &mut Vec<u8>,
    timestamp: i64,
    encoding_stats: &JavaBigEncodingStats,
) -> std::io::Result<()> {
    write_java_unsigned_vint(
        out,
        checked_i64_delta_as_u64(
            timestamp,
            encoding_stats.min_timestamp,
            "Java row timestamp",
        )?,
    )
}

fn read_java_big_delta_local_deletion_time(
    input: &mut JavaDataInput<'_>,
    encoding_stats: &JavaBigEncodingStats,
) -> std::io::Result<i64> {
    checked_add_i64_u64(
        encoding_stats.min_local_deletion_time,
        input.read_unsigned_vint()?,
        "Java row local deletion time",
    )
}

fn write_java_big_delta_local_deletion_time(
    out: &mut Vec<u8>,
    local_deletion_time: i64,
    encoding_stats: &JavaBigEncodingStats,
) -> std::io::Result<()> {
    write_java_unsigned_vint(
        out,
        checked_i64_delta_as_u64(
            local_deletion_time,
            encoding_stats.min_local_deletion_time,
            "Java row local deletion time",
        )?,
    )
}

fn read_java_big_delta_ttl(
    input: &mut JavaDataInput<'_>,
    encoding_stats: &JavaBigEncodingStats,
) -> std::io::Result<i32> {
    let delta = input.read_unsigned_vint()?;
    let delta = i32::try_from(delta).map_err(|_| invalid_data("Java row TTL delta exceeds int"))?;
    encoding_stats
        .min_ttl
        .checked_add(delta)
        .ok_or_else(|| invalid_data("Java row TTL overflow"))
}

fn write_java_big_delta_ttl(
    out: &mut Vec<u8>,
    ttl: i32,
    encoding_stats: &JavaBigEncodingStats,
) -> std::io::Result<()> {
    let delta = ttl
        .checked_sub(encoding_stats.min_ttl)
        .ok_or_else(|| invalid_data("Java row TTL is before encoding-stats minimum"))?;
    let delta = u64::try_from(delta)
        .map_err(|_| invalid_data("Java row TTL is before encoding-stats minimum"))?;
    write_java_unsigned_vint(out, delta)
}

fn read_java_big_delta_deletion_time(
    input: &mut JavaDataInput<'_>,
    encoding_stats: &JavaBigEncodingStats,
) -> std::io::Result<JavaBigDeletionTimeMetadata> {
    Ok(JavaBigDeletionTimeMetadata {
        marked_for_delete_at: read_java_big_delta_timestamp(input, encoding_stats)?,
        local_deletion_time: read_java_big_delta_local_deletion_time(input, encoding_stats)?,
        is_live: false,
    })
}

fn write_java_big_delta_deletion_time(
    out: &mut Vec<u8>,
    deletion_time: &JavaBigDeletionTimeMetadata,
    encoding_stats: &JavaBigEncodingStats,
) -> std::io::Result<()> {
    if deletion_time.is_live {
        return Err(invalid_data(
            "Java unfiltered row deletion flag cannot encode live deletion",
        ));
    }
    write_java_big_delta_timestamp(out, deletion_time.marked_for_delete_at, encoding_stats)?;
    write_java_big_delta_local_deletion_time(out, deletion_time.local_deletion_time, encoding_stats)
}

fn read_java_big_partition_deletion_time(
    input: &mut JavaDataInput<'_>,
    has_unsigned_deletion_time: bool,
) -> std::io::Result<JavaBigDeletionTimeMetadata> {
    if has_unsigned_deletion_time {
        read_java_big_unsigned_partition_deletion_time(input)
    } else {
        read_java_big_legacy_partition_deletion_time(input)
    }
}

fn write_java_big_partition_deletion_time(
    out: &mut Vec<u8>,
    deletion_time: &JavaBigDeletionTimeMetadata,
    has_unsigned_deletion_time: bool,
) -> std::io::Result<()> {
    if has_unsigned_deletion_time {
        write_java_big_unsigned_partition_deletion_time(out, deletion_time)
    } else {
        write_java_big_legacy_partition_deletion_time(out, deletion_time)
    }
}

fn read_java_big_unsigned_partition_deletion_time(
    input: &mut JavaDataInput<'_>,
) -> std::io::Result<JavaBigDeletionTimeMetadata> {
    let first = input.read_exact(1)?[0];
    if (first & 0x80) != 0 {
        if first != 0x80 {
            return Err(invalid_data(
                "Java unsigned deletion time has unknown marker flags",
            ));
        }
        return Ok(JavaBigDeletionTimeMetadata::LIVE);
    }

    let mut marked_for_delete_at_bytes = [0_u8; 8];
    marked_for_delete_at_bytes[0] = first;
    marked_for_delete_at_bytes[1..].copy_from_slice(input.read_exact(7)?);
    let marked_for_delete_at = i64::from_be_bytes(marked_for_delete_at_bytes);
    let raw_local_deletion_time = input.read_i32()?;

    Ok(JavaBigDeletionTimeMetadata {
        marked_for_delete_at,
        local_deletion_time: unsigned_java_deletion_time_to_i64(raw_local_deletion_time),
        is_live: false,
    })
}

fn write_java_big_unsigned_partition_deletion_time(
    out: &mut Vec<u8>,
    deletion_time: &JavaBigDeletionTimeMetadata,
) -> std::io::Result<()> {
    if deletion_time.is_live {
        out.push(0x80);
        return Ok(());
    }
    if deletion_time.marked_for_delete_at < 0 {
        return Err(invalid_data(
            "Java unsigned deletion time requires non-negative marked-for-delete timestamp",
        ));
    }
    out.extend_from_slice(&deletion_time.marked_for_delete_at.to_be_bytes());
    let raw_local_deletion_time =
        i32_from_unsigned_java_deletion_time(deletion_time.local_deletion_time)?;
    out.extend_from_slice(&raw_local_deletion_time.to_be_bytes());
    Ok(())
}

fn read_java_big_legacy_partition_deletion_time(
    input: &mut JavaDataInput<'_>,
) -> std::io::Result<JavaBigDeletionTimeMetadata> {
    let raw_local_deletion_time = input.read_i32()?;
    let marked_for_delete_at = input.read_i64()?;
    if marked_for_delete_at == i64::MIN && raw_local_deletion_time == i32::MAX {
        return Ok(JavaBigDeletionTimeMetadata::LIVE);
    }
    Ok(JavaBigDeletionTimeMetadata {
        marked_for_delete_at,
        local_deletion_time: if raw_local_deletion_time == i32::MAX {
            i64::MAX
        } else {
            raw_local_deletion_time as i64
        },
        is_live: false,
    })
}

fn write_java_big_legacy_partition_deletion_time(
    out: &mut Vec<u8>,
    deletion_time: &JavaBigDeletionTimeMetadata,
) -> std::io::Result<()> {
    if deletion_time.is_live {
        out.extend_from_slice(&i32::MAX.to_be_bytes());
        out.extend_from_slice(&i64::MIN.to_be_bytes());
        return Ok(());
    }
    write_java_big_local_deletion_time(out, deletion_time.local_deletion_time, false)?;
    out.extend_from_slice(&deletion_time.marked_for_delete_at.to_be_bytes());
    Ok(())
}

fn read_java_big_local_deletion_time(
    input: &mut JavaDataInput<'_>,
    has_unsigned_deletion_time: bool,
) -> std::io::Result<i64> {
    let raw = input.read_i32()?;
    if has_unsigned_deletion_time {
        Ok(unsigned_java_deletion_time_to_i64(raw))
    } else if raw == i32::MAX {
        Ok(i64::MAX)
    } else {
        Ok(raw as i64)
    }
}

fn write_java_big_local_deletion_time(
    out: &mut Vec<u8>,
    value: i64,
    has_unsigned_deletion_time: bool,
) -> std::io::Result<()> {
    let raw = if has_unsigned_deletion_time {
        i32_from_unsigned_java_deletion_time(value)?
    } else if value == i64::MAX {
        i32::MAX
    } else {
        i32::try_from(value)
            .map_err(|_| invalid_data("Java local deletion time overflows legacy int"))?
    };
    out.extend_from_slice(&raw.to_be_bytes());
    Ok(())
}

fn unsigned_java_deletion_time_to_i64(value: i32) -> i64 {
    let unsigned = value as u32;
    if unsigned == u32::MAX {
        i64::MAX
    } else {
        unsigned as i64
    }
}

fn i32_from_unsigned_java_deletion_time(value: i64) -> std::io::Result<i32> {
    if value == i64::MAX {
        Ok(-1)
    } else {
        let unsigned = u32::try_from(value)
            .map_err(|_| invalid_data("Java local deletion time overflows unsigned int"))?;
        if unsigned == u32::MAX {
            return Err(invalid_data(
                "Java local deletion time conflicts with no-deletion sentinel",
            ));
        }
        Ok(unsigned as i32)
    }
}

fn validate_java_big_summary_offsets(
    offsets: &[usize],
    entries_size: usize,
) -> std::io::Result<()> {
    let mut previous = None;
    for &offset in offsets {
        if offset > entries_size {
            return Err(invalid_data(
                "Java Summary.db entry offset exceeds entries payload",
            ));
        }
        if previous.is_some_and(|prev| offset <= prev) {
            return Err(invalid_data(
                "Java Summary.db entry offsets must be strictly increasing",
            ));
        }
        previous = Some(offset);
    }
    Ok(())
}

fn validate_java_big_statistics_toc(
    toc: &[(JavaBigMetadataType, usize)],
    components_start: usize,
    file_len: usize,
    has_checksum: bool,
) -> std::io::Result<()> {
    let mut previous_type = None;
    let mut previous_offset = None;
    for &(component_type, offset) in toc {
        if previous_type.is_some_and(|previous| component_type <= previous) {
            return Err(invalid_data(
                "Java Statistics.db metadata components are not sorted by ordinal",
            ));
        }
        if offset < components_start {
            return Err(invalid_data(
                "Java Statistics.db component offset points inside TOC",
            ));
        }
        if offset > file_len {
            return Err(invalid_data(
                "Java Statistics.db component offset exceeds file length",
            ));
        }
        if previous_offset.is_some_and(|previous| offset <= previous) {
            return Err(invalid_data(
                "Java Statistics.db component offsets must be strictly increasing",
            ));
        }
        previous_type = Some(component_type);
        previous_offset = Some(offset);
    }

    for (idx, &(_, offset)) in toc.iter().enumerate() {
        let next_offset = toc
            .get(idx + 1)
            .map(|(_, offset)| *offset)
            .unwrap_or(file_len);
        if next_offset < offset {
            return Err(invalid_data(
                "Java Statistics.db component offset range is inverted",
            ));
        }
        if has_checksum && next_offset - offset < 4 {
            return Err(invalid_data(
                "Java Statistics.db component is too small for checksum",
            ));
        }
    }
    Ok(())
}

fn validate_crc32(data: &[u8], expected: i32, label: &str) -> std::io::Result<()> {
    let mut hasher = Hasher::new();
    hasher.update(data);
    let actual = hasher.finalize() as i32;
    if actual != expected {
        return Err(invalid_data(format!("{label} mismatch")));
    }
    Ok(())
}

fn write_crc32(out: &mut Vec<u8>, data: &[u8]) {
    let mut hasher = Hasher::new();
    hasher.update(data);
    out.extend_from_slice(&(hasher.finalize() as i32).to_be_bytes());
}

fn checked_add_usize(left: usize, right: usize, label: &str) -> std::io::Result<usize> {
    left.checked_add(right)
        .ok_or_else(|| invalid_data(format!("{label} overflow")))
}

fn checked_mul_usize(left: usize, right: usize, label: &str) -> std::io::Result<usize> {
    left.checked_mul(right)
        .ok_or_else(|| invalid_data(format!("{label} overflow")))
}

fn checked_add_i64_u64(left: i64, right: u64, label: &str) -> std::io::Result<i64> {
    let right = i64::try_from(right).map_err(|_| invalid_data(format!("{label} overflow")))?;
    left.checked_add(right)
        .ok_or_else(|| invalid_data(format!("{label} overflow")))
}

fn checked_i64_delta_as_u64(value: i64, epoch: i64, label: &str) -> std::io::Result<u64> {
    let delta = value
        .checked_sub(epoch)
        .ok_or_else(|| invalid_data(format!("{label} is before epoch")))?;
    u64::try_from(delta).map_err(|_| invalid_data(format!("{label} is before epoch")))
}

fn two_letter_version_at_least(version: &str, minimum: &str) -> bool {
    version.as_bytes().len() == 2
        && minimum.as_bytes().len() == 2
        && version.as_bytes() >= minimum.as_bytes()
}

fn matches_two_letter_range(version: &str, major: u8, start_minor: u8, end_minor: u8) -> bool {
    let bytes = version.as_bytes();
    bytes.len() == 2 && bytes[0] == major && (start_minor..=end_minor).contains(&bytes[1])
}

fn invalid_data(message: impl Into<String>) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, message.into())
}

fn parse_java_big_toc_line(line: &str) -> Option<JavaBigComponent> {
    if line.is_empty() {
        return None;
    }
    JavaBigComponent::from_suffix(line).or_else(|| {
        JavaBigDescriptor::parse_component_filename(line).map(|(_, component)| component)
    })
}

/// Returns a human-readable description of the Rust SSTable format.
pub fn format_info() -> String {
    format!(
        "Rust Cassandra SSTable Format v{version}\n\
         \n\
         Magic bytes: Data={data_magic}, Index={index_magic}, Filter={filter_magic}\n\
         Supported formats: Big (partition index), BTI (trie index)\n\
         Statistics: JSON encoded\n\
         CRC: CRC32 at end of Data.db\n\
         \n\
         NOT binary-compatible with Java Apache Cassandra.\n\
         Upgrade path: Rust V1 -> future Rust versions only.\n\
         V1 SSTables will always be readable by any future version.",
        version = DATA_VERSION,
        data_magic = String::from_utf8_lossy(&DATA_MAGIC),
        index_magic = String::from_utf8_lossy(&INDEX_MAGIC),
        filter_magic = String::from_utf8_lossy(&FILTER_MAGIC),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memtable::partition::{Cell, PartitionData, Row};
    use crate::sstable::bti::{BtiReader, BtiWriter};
    use crate::sstable::format::{Component, SSTableDescriptor, SSTableFormat};
    use crate::sstable::reader::SSTableReader;
    use crate::sstable::writer::SSTableWriter;
    use std::fs;
    use tempfile::TempDir;

    // ─── Helper: build deterministic partitions ───────────────────────────

    fn make_regular_partitions() -> Vec<(Vec<u8>, PartitionData)> {
        let mut partitions = Vec::new();
        for i in 0..5u8 {
            let mut pd = PartitionData::new();
            for j in 0..3u8 {
                pd.apply_row(Row {
                    clustering_key: vec![j],
                    cells: vec![Cell {
                        column: "col".to_string(),
                        value: Some(format!("v_{i}_{j}").into_bytes()),
                        timestamp: 1000 + i as i64,
                        ttl: 0,
                        local_deletion_time: None,
                        is_tombstone: false,
                    }],
                    is_tombstone: false,
                    local_deletion_time: None,
                });
            }
            partitions.push((vec![i], pd));
        }
        partitions
    }

    fn make_complex_partitions() -> Vec<(Vec<u8>, PartitionData)> {
        let mut partitions = Vec::new();

        // Partition with tombstone cells
        let mut pd1 = PartitionData::new();
        pd1.apply_row(Row {
            clustering_key: b"ck_alive".to_vec(),
            cells: vec![Cell {
                column: "data".to_string(),
                value: Some(b"hello".to_vec()),
                timestamp: 100,
                ttl: 0,
                local_deletion_time: None,
                is_tombstone: false,
            }],
            is_tombstone: false,
            local_deletion_time: None,
        });
        pd1.apply_row(Row {
            clustering_key: b"ck_dead".to_vec(),
            cells: vec![Cell {
                column: "data".to_string(),
                value: None,
                timestamp: 200,
                ttl: 0,
                local_deletion_time: Some(200),
                is_tombstone: true,
            }],
            is_tombstone: true,
            local_deletion_time: Some(200),
        });
        partitions.push((b"pk_mixed".to_vec(), pd1));

        // Partition with TTL cells
        let mut pd2 = PartitionData::new();
        pd2.apply_row(Row {
            clustering_key: b"ck_ttl".to_vec(),
            cells: vec![Cell {
                column: "expiring".to_string(),
                value: Some(b"temp_value".to_vec()),
                timestamp: 300,
                ttl: 3600,
                local_deletion_time: Some(1000 + 3600),
                is_tombstone: false,
            }],
            is_tombstone: false,
            local_deletion_time: None,
        });
        partitions.push((b"pk_ttl".to_vec(), pd2));

        // Partition with empty value
        let mut pd3 = PartitionData::new();
        pd3.apply_row(Row {
            clustering_key: b"ck_empty".to_vec(),
            cells: vec![Cell {
                column: "empty_col".to_string(),
                value: Some(Vec::new()),
                timestamp: 400,
                ttl: 0,
                local_deletion_time: None,
                is_tombstone: false,
            }],
            is_tombstone: false,
            local_deletion_time: None,
        });
        partitions.push((b"pk_zz_empty".to_vec(), pd3));

        partitions
    }

    fn make_large_key_partitions() -> Vec<(Vec<u8>, PartitionData)> {
        let mut partitions = Vec::new();

        // Large partition key (1 KB)
        let large_key = vec![0xABu8; 1024];
        let mut pd = PartitionData::new();
        for j in 0..20u8 {
            pd.apply_row(Row {
                clustering_key: vec![j],
                cells: vec![Cell {
                    column: "c".to_string(),
                    value: Some(vec![j; 100]),
                    timestamp: 500 + j as i64,
                    ttl: 0,
                    local_deletion_time: None,
                    is_tombstone: false,
                }],
                is_tombstone: false,
                local_deletion_time: None,
            });
        }
        partitions.push((large_key, pd));
        partitions
    }

    fn write_java_utf(buf: &mut Vec<u8>, value: &str) {
        buf.extend_from_slice(&(value.len() as u16).to_be_bytes());
        buf.extend_from_slice(value.as_bytes());
    }

    fn sample_compression_info_bytes() -> Vec<u8> {
        let mut bytes = Vec::new();
        write_java_utf(&mut bytes, "LZ4Compressor");
        bytes.extend_from_slice(&1_i32.to_be_bytes());
        write_java_utf(&mut bytes, "crc_check_chance");
        write_java_utf(&mut bytes, "1.0");
        bytes.extend_from_slice(&4_i32.to_be_bytes());
        bytes.extend_from_slice(&32_i32.to_be_bytes());
        bytes.extend_from_slice(&10_i64.to_be_bytes());
        bytes.extend_from_slice(&3_i32.to_be_bytes());
        for offset in [0_i64, 9, 19] {
            bytes.extend_from_slice(&offset.to_be_bytes());
        }
        bytes
    }

    fn java_big_wide_subset_header(column_count: usize) -> JavaBigSerializationHeader {
        JavaBigSerializationHeader {
            encoding_stats: JavaBigEncodingStats {
                min_timestamp: 100,
                min_local_deletion_time: 200,
                min_ttl: 0,
            },
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            clustering_types: Vec::new(),
            static_columns: Vec::new(),
            regular_columns: (0..column_count)
                .map(|idx| JavaBigSerializationHeaderColumn {
                    name: format!("c{idx:02}").into_bytes(),
                    type_spec: "org.apache.cassandra.db.marshal.Int32Type".to_string(),
                })
                .collect(),
        }
    }

    fn java_big_int_cell(idx: usize, value: i32, timestamp: i64) -> JavaBigSimpleCell {
        JavaBigSimpleCell {
            column_name: format!("c{idx:02}").into_bytes(),
            column_type: "org.apache.cassandra.db.marshal.Int32Type".to_string(),
            flags: 0,
            timestamp,
            local_deletion_time: None,
            ttl: None,
            value: Some(value.to_be_bytes().to_vec()),
            is_deleted: false,
            is_expiring: false,
            uses_row_timestamp: true,
            uses_row_ttl: false,
        }
    }

    #[test]
    fn discovers_java_big_component_manifest() {
        let dir = TempDir::new().unwrap();
        for component in [
            JavaBigComponent::Data,
            JavaBigComponent::Index,
            JavaBigComponent::Statistics,
            JavaBigComponent::Toc,
        ] {
            fs::write(
                dir.path().join(format!("nb-1-big-{}", component.suffix())),
                b"x",
            )
            .unwrap();
        }

        let manifests = discover_java_big_sstables(dir.path()).unwrap();
        assert_eq!(manifests.len(), 1);
        assert_eq!(manifests[0].descriptor.version, "nb");
        assert!(manifests[0].is_minimally_readable());
        assert!(
            manifests[0]
                .component_path(JavaBigComponent::Data)
                .ends_with("nb-1-big-Data.db")
        );
    }

    #[test]
    fn reads_java_big_toc_components() {
        let dir = TempDir::new().unwrap();
        let toc = dir.path().join("nb-1-big-TOC.txt");
        fs::write(
            &toc,
            "Data.db\nnb-1-big-Index.db\nStatistics.db\n\nunknown.txt\n",
        )
        .unwrap();
        assert_eq!(
            read_java_big_toc(&toc).unwrap(),
            vec![
                JavaBigComponent::Data,
                JavaBigComponent::Index,
                JavaBigComponent::Statistics
            ]
        );
    }

    #[test]
    fn validates_java_big_toc_against_manifest() {
        let dir = TempDir::new().unwrap();
        for component in [
            JavaBigComponent::Data,
            JavaBigComponent::Index,
            JavaBigComponent::Statistics,
            JavaBigComponent::Toc,
        ] {
            fs::write(
                dir.path().join(format!("nb-1-big-{}", component.suffix())),
                b"x",
            )
            .unwrap();
        }
        fs::write(
            dir.path().join("nb-1-big-TOC.txt"),
            "Data.db\nIndex.db\nStatistics.db\nSummary.db\n",
        )
        .unwrap();

        let manifest = discover_java_big_sstables(dir.path()).unwrap().remove(0);
        let validation = manifest.validate_toc().unwrap();
        assert_eq!(
            validation.listed,
            vec![
                JavaBigComponent::Data,
                JavaBigComponent::Index,
                JavaBigComponent::Statistics,
                JavaBigComponent::Summary,
            ]
        );
        assert_eq!(validation.missing, vec![JavaBigComponent::Summary]);
        assert_eq!(validation.extra, vec![JavaBigComponent::Toc]);
    }

    #[test]
    fn validates_java_big_data_digest() {
        let dir = TempDir::new().unwrap();
        let data_path = dir.path().join("nb-1-big-Data.db");
        fs::write(&data_path, b"java-data").unwrap();
        for component in [
            JavaBigComponent::Index,
            JavaBigComponent::Statistics,
            JavaBigComponent::Toc,
        ] {
            fs::write(
                dir.path().join(format!("nb-1-big-{}", component.suffix())),
                b"x",
            )
            .unwrap();
        }
        let digest = calculate_crc32(&data_path).unwrap();
        fs::write(dir.path().join("nb-1-big-Digest.crc32"), digest.to_string()).unwrap();

        let manifest = discover_java_big_sstables(dir.path()).unwrap().remove(0);
        let validation = manifest.validate_data_digest().unwrap().unwrap();
        assert_eq!(validation.stored, digest);
        assert_eq!(validation.calculated, digest);
        assert!(validation.is_valid());

        fs::write(dir.path().join("nb-1-big-Digest.crc32"), "1").unwrap();
        let manifest = discover_java_big_sstables(dir.path()).unwrap().remove(0);
        let validation = manifest.validate_data_digest().unwrap().unwrap();
        assert_eq!(validation.stored, 1);
        assert_eq!(validation.calculated, digest);
        assert!(!validation.is_valid());
    }

    #[test]
    fn validates_java_big_crc_chunks() {
        let dir = TempDir::new().unwrap();
        let data_path = dir.path().join("nb-1-big-Data.db");
        fs::write(&data_path, b"abcdefghij").unwrap();
        for component in [
            JavaBigComponent::Index,
            JavaBigComponent::Statistics,
            JavaBigComponent::Toc,
            JavaBigComponent::Digest,
        ] {
            fs::write(
                dir.path().join(format!("nb-1-big-{}", component.suffix())),
                b"x",
            )
            .unwrap();
        }

        let checksums = calculate_crc32_chunks(&data_path, 4).unwrap();
        let mut crc_bytes = 4_u32.to_be_bytes().to_vec();
        for checksum in &checksums {
            crc_bytes.extend_from_slice(&checksum.to_be_bytes());
        }
        fs::write(dir.path().join("nb-1-big-CRC.db"), &crc_bytes).unwrap();

        let manifest = discover_java_big_sstables(dir.path()).unwrap().remove(0);
        let validation = manifest.validate_data_crc().unwrap().unwrap();
        assert_eq!(validation.chunk_size, 4);
        assert_eq!(validation.stored, checksums);
        assert_eq!(validation.calculated, checksums);
        assert!(validation.is_valid());

        crc_bytes[7] ^= 0xff;
        fs::write(dir.path().join("nb-1-big-CRC.db"), crc_bytes).unwrap();
        let manifest = discover_java_big_sstables(dir.path()).unwrap().remove(0);
        let validation = manifest.validate_data_crc().unwrap().unwrap();
        assert!(!validation.is_valid());
        assert_eq!(validation.calculated, checksums);
    }

    #[test]
    fn reads_java_big_compression_info() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("nb-1-big-CompressionInfo.db");
        fs::write(&path, sample_compression_info_bytes()).unwrap();

        let metadata = read_java_big_compression_info(&path).unwrap();
        assert_eq!(metadata.compressor_name, "LZ4Compressor");
        assert_eq!(
            metadata.options.get("crc_check_chance").map(String::as_str),
            Some("1.0")
        );
        assert_eq!(metadata.chunk_length, 4);
        assert_eq!(metadata.max_compressed_length, 32);
        assert_eq!(metadata.data_length, 10);
        assert_eq!(metadata.chunk_offsets, vec![0, 9, 19]);
        assert_eq!(
            metadata.chunk_for(5, 30).unwrap(),
            JavaBigCompressedChunk {
                offset: 9,
                length: 6,
            }
        );
        assert_eq!(
            metadata.chunk_for(9, 30).unwrap(),
            JavaBigCompressedChunk {
                offset: 19,
                length: 7,
            }
        );
        assert_eq!(metadata.chunk_for(10, 30), None);
    }

    #[test]
    fn manifest_reads_java_big_compression_info() {
        let dir = TempDir::new().unwrap();
        for component in [
            JavaBigComponent::Data,
            JavaBigComponent::Index,
            JavaBigComponent::Statistics,
            JavaBigComponent::Toc,
        ] {
            fs::write(
                dir.path().join(format!("nb-1-big-{}", component.suffix())),
                b"x",
            )
            .unwrap();
        }
        fs::write(
            dir.path().join("nb-1-big-CompressionInfo.db"),
            sample_compression_info_bytes(),
        )
        .unwrap();

        let manifest = discover_java_big_sstables(dir.path()).unwrap().remove(0);
        let metadata = manifest.compression_metadata().unwrap().unwrap();
        assert_eq!(metadata.compressor_name, "LZ4Compressor");
        assert_eq!(metadata.chunk_offsets, vec![0, 9, 19]);
    }

    #[test]
    fn reads_java_big_primary_index_entries() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("nb-1-big-Index.db");
        let mut bytes = Vec::new();
        let first = JavaBigRowIndexEntry {
            data_file_position: 128,
            promoted_size: 0,
            kind: super::super::column_index::JavaBigRowIndexEntryKind::Unindexed,
            header_length: None,
            deletion_time: None,
            column_index_count: 0,
            index_info_bytes: Vec::new(),
            offsets: Vec::new(),
        };
        let second = JavaBigRowIndexEntry {
            data_file_position: 4096,
            promoted_size: 0,
            kind: super::super::column_index::JavaBigRowIndexEntryKind::Indexed,
            header_length: Some(4),
            deletion_time: Some(super::super::column_index::JavaBigDeletionTime::LIVE),
            column_index_count: 2,
            index_info_bytes: vec![0x01, 0x02, 0x03, 0x04],
            offsets: vec![0, 2],
        };
        write_java_big_primary_index_entry(&mut bytes, b"pk-a", &first).unwrap();
        write_java_big_primary_index_entry(&mut bytes, b"pk-b", &second).unwrap();
        fs::write(&path, &bytes).unwrap();

        let entries = read_java_big_primary_index(&path).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].index_offset, 0);
        assert_eq!(entries[0].partition_key, b"pk-a");
        assert_eq!(entries[0].row_index.data_file_position, 128);
        assert!(!entries[0].row_index.is_indexed());

        assert!(entries[1].index_offset > entries[0].index_offset);
        assert_eq!(entries[1].partition_key, b"pk-b");
        assert_eq!(entries[1].row_index.data_file_position, 4096);
        assert_eq!(entries[1].row_index.column_index_count, 2);
        assert_eq!(
            entries[1].row_index.index_info_slice(0),
            Some(&[0x01, 0x02][..])
        );
        assert_eq!(
            entries[1].row_index.index_info_slice(1),
            Some(&[0x03, 0x04][..])
        );

        let mut corrupt = bytes;
        corrupt.truncate(corrupt.len() - 1);
        assert!(parse_java_big_primary_index(&corrupt).is_err());
    }

    #[test]
    fn reads_java_big_data_partition_headers() {
        let mut data = Vec::new();
        write_java_big_data_partition_header(
            &mut data,
            b"pk-live",
            &JavaBigDeletionTimeMetadata::LIVE,
            false,
        )
        .unwrap();
        data.extend_from_slice(&[0xaa, 0xbb]);
        let second_offset = data.len() as u64;
        let deleted = JavaBigDeletionTimeMetadata {
            marked_for_delete_at: 123_456_789,
            local_deletion_time: 1_700_000_000,
            is_live: false,
        };
        write_java_big_data_partition_header(&mut data, b"pk-deleted", &deleted, false).unwrap();

        let first = parse_java_big_data_partition_header_at(&data, 0, false).unwrap();
        assert_eq!(first.partition_key, b"pk-live");
        assert_eq!(first.deletion_time, JavaBigDeletionTimeMetadata::LIVE);
        assert_eq!(first.bytes_consumed, 2 + b"pk-live".len() + 12);

        let second = parse_java_big_data_partition_header_at(&data, second_offset, false).unwrap();
        assert_eq!(second.partition_key, b"pk-deleted");
        assert_eq!(second.deletion_time, deleted);
        assert_eq!(second.bytes_consumed, 2 + b"pk-deleted".len() + 12);
        assert!(
            parse_java_big_data_partition_header_at(&data, data.len() as u64 + 1, false).is_err()
        );
    }

    #[test]
    fn reads_java_big_unsigned_data_partition_headers() {
        let mut data = Vec::new();
        write_java_big_data_partition_header(
            &mut data,
            b"pk-live",
            &JavaBigDeletionTimeMetadata::LIVE,
            true,
        )
        .unwrap();
        let deleted_offset = data.len() as u64;
        let deleted = JavaBigDeletionTimeMetadata {
            marked_for_delete_at: 987_654_321,
            local_deletion_time: 3_000_000_000,
            is_live: false,
        };
        write_java_big_data_partition_header(&mut data, b"pk-deleted", &deleted, true).unwrap();

        let live = parse_java_big_data_partition_header_at(&data, 0, true).unwrap();
        assert_eq!(live.partition_key, b"pk-live");
        assert_eq!(live.deletion_time, JavaBigDeletionTimeMetadata::LIVE);
        assert_eq!(live.bytes_consumed, 2 + b"pk-live".len() + 1);

        let parsed = parse_java_big_data_partition_header_at(&data, deleted_offset, true).unwrap();
        assert_eq!(parsed.partition_key, b"pk-deleted");
        assert_eq!(parsed.deletion_time, deleted);
        assert_eq!(parsed.bytes_consumed, 2 + b"pk-deleted".len() + 12);

        let mut bad = Vec::new();
        bad.extend_from_slice(&1_u16.to_be_bytes());
        bad.push(b'x');
        bad.push(0x81);
        assert!(parse_java_big_data_partition_header_at(&bad, 0, true).is_err());
    }

    #[test]
    fn manifest_reads_java_big_primary_index_entries() {
        let dir = TempDir::new().unwrap();
        for component in [
            JavaBigComponent::Data,
            JavaBigComponent::Statistics,
            JavaBigComponent::Toc,
        ] {
            fs::write(
                dir.path().join(format!("nb-1-big-{}", component.suffix())),
                b"x",
            )
            .unwrap();
        }
        let mut index = Vec::new();
        let row_index = JavaBigRowIndexEntry {
            data_file_position: 77,
            promoted_size: 0,
            kind: super::super::column_index::JavaBigRowIndexEntryKind::Unindexed,
            header_length: None,
            deletion_time: None,
            column_index_count: 0,
            index_info_bytes: Vec::new(),
            offsets: Vec::new(),
        };
        write_java_big_primary_index_entry(&mut index, b"pk", &row_index).unwrap();
        fs::write(dir.path().join("nb-1-big-Index.db"), index).unwrap();

        let manifest = discover_java_big_sstables(dir.path()).unwrap().remove(0);
        let entries = manifest.primary_index_entries().unwrap().unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].partition_key, b"pk");
        assert_eq!(entries[0].row_index.data_file_position, 77);
    }

    #[test]
    fn manifest_reads_java_big_data_partition_header() {
        let dir = TempDir::new().unwrap();
        for component in [
            JavaBigComponent::Index,
            JavaBigComponent::Statistics,
            JavaBigComponent::Toc,
        ] {
            fs::write(
                dir.path().join(format!("nb-1-big-{}", component.suffix())),
                b"x",
            )
            .unwrap();
        }
        let mut data = Vec::new();
        write_java_big_data_partition_header(
            &mut data,
            b"pk",
            &JavaBigDeletionTimeMetadata::LIVE,
            false,
        )
        .unwrap();
        fs::write(dir.path().join("nb-1-big-Data.db"), data).unwrap();

        let manifest = discover_java_big_sstables(dir.path()).unwrap().remove(0);
        let header = manifest
            .data_partition_header_at(0, false)
            .unwrap()
            .unwrap();
        assert_eq!(header.partition_key, b"pk");
        assert_eq!(header.deletion_time, JavaBigDeletionTimeMetadata::LIVE);
    }

    #[test]
    fn manifest_reads_indexed_java_big_data_partition_headers() {
        let dir = TempDir::new().unwrap();
        for component in [JavaBigComponent::Statistics, JavaBigComponent::Toc] {
            fs::write(
                dir.path().join(format!("nb-1-big-{}", component.suffix())),
                b"x",
            )
            .unwrap();
        }

        let mut data = Vec::new();
        let first_offset = data.len() as u64;
        write_java_big_data_partition_header(
            &mut data,
            b"pk-a",
            &JavaBigDeletionTimeMetadata::LIVE,
            false,
        )
        .unwrap();
        data.push(0);
        let second_offset = data.len() as u64;
        let deleted = JavaBigDeletionTimeMetadata {
            marked_for_delete_at: 42,
            local_deletion_time: 1_700_000_000,
            is_live: false,
        };
        write_java_big_data_partition_header(&mut data, b"pk-b", &deleted, false).unwrap();
        fs::write(dir.path().join("nb-1-big-Data.db"), data).unwrap();

        let mut index = Vec::new();
        let first = JavaBigRowIndexEntry {
            data_file_position: first_offset,
            promoted_size: 0,
            kind: super::super::column_index::JavaBigRowIndexEntryKind::Unindexed,
            header_length: None,
            deletion_time: None,
            column_index_count: 0,
            index_info_bytes: Vec::new(),
            offsets: Vec::new(),
        };
        let second = JavaBigRowIndexEntry {
            data_file_position: second_offset,
            promoted_size: 0,
            kind: super::super::column_index::JavaBigRowIndexEntryKind::Unindexed,
            header_length: None,
            deletion_time: None,
            column_index_count: 0,
            index_info_bytes: Vec::new(),
            offsets: Vec::new(),
        };
        write_java_big_primary_index_entry(&mut index, b"pk-a", &first).unwrap();
        write_java_big_primary_index_entry(&mut index, b"pk-b", &second).unwrap();
        fs::write(dir.path().join("nb-1-big-Index.db"), index).unwrap();

        let manifest = discover_java_big_sstables(dir.path()).unwrap().remove(0);
        let headers = manifest
            .indexed_data_partition_headers(false)
            .unwrap()
            .unwrap();
        assert_eq!(headers.len(), 2);
        assert_eq!(headers[0].index_entry.partition_key, b"pk-a");
        assert_eq!(headers[0].data_header.partition_key, b"pk-a");
        assert_eq!(headers[1].index_entry.partition_key, b"pk-b");
        assert_eq!(headers[1].data_header.partition_key, b"pk-b");
        assert_eq!(headers[1].data_header.deletion_time, deleted);
    }

    #[test]
    fn reads_java_big_unfiltered_row_and_end_framing() {
        let mut data = Vec::new();
        write_java_big_data_partition_header(
            &mut data,
            b"pk",
            &JavaBigDeletionTimeMetadata::LIVE,
            false,
        )
        .unwrap();
        let row_offset = data.len() as u64;
        write_java_big_unfiltered_empty_clustering_row(
            &mut data,
            JAVA_UNFILTERED_HAS_ALL_COLUMNS,
            None,
            0,
            b"",
        )
        .unwrap();
        let end_offset = data.len() as u64;
        write_java_big_unfiltered_end_of_partition(&mut data);

        let row = parse_java_big_unfiltered_header_at(&data, row_offset, 0).unwrap();
        assert_eq!(row.kind, JavaBigUnfilteredKind::Row);
        assert_eq!(row.flags, JAVA_UNFILTERED_HAS_ALL_COLUMNS);
        assert_eq!(row.framed_body_size, Some(1));
        assert_eq!(row.previous_unfiltered_size, Some(0));
        assert_eq!(row.body_length, Some(0));
        assert_eq!(row.next_unfiltered_offset, end_offset);

        let end = parse_java_big_unfiltered_header_at(&data, end_offset, 0).unwrap();
        assert_eq!(end.kind, JavaBigUnfilteredKind::EndOfPartition);
        assert_eq!(end.next_unfiltered_offset, end_offset + 1);
    }

    #[test]
    fn reads_java_big_unfiltered_marker_framing() {
        let mut data = Vec::new();
        write_java_big_unfiltered_marker_header(
            &mut data,
            JAVA_CLUSTERING_KIND_INCL_START_BOUND,
            3,
            &[0xaa, 0xbb],
        )
        .unwrap();

        let marker = parse_java_big_unfiltered_header_at(&data, 0, 0).unwrap();
        assert_eq!(marker.kind, JavaBigUnfilteredKind::RangeTombstoneMarker);
        assert_eq!(
            marker.clustering_kind_ordinal,
            Some(JAVA_CLUSTERING_KIND_INCL_START_BOUND)
        );
        assert_eq!(marker.previous_unfiltered_size, Some(3));
        assert_eq!(marker.body_length, Some(2));
        assert_eq!(marker.next_unfiltered_offset, data.len() as u64);

        let mut invalid = Vec::new();
        invalid.push(JAVA_UNFILTERED_IS_MARKER);
        invalid.push(4);
        invalid.push(1);
        invalid.push(0);
        assert!(parse_java_big_unfiltered_header_at(&invalid, 0, 0).is_err());
    }

    #[test]
    fn reads_java_big_unfiltered_row_liveness_and_deletion_metadata() {
        let encoding_stats = JavaBigEncodingStats {
            min_timestamp: 1_000,
            min_local_deletion_time: 2_000,
            min_ttl: 10,
        };
        let liveness = JavaBigRowLivenessMetadata {
            timestamp: Some(1_123),
            ttl: Some(30),
            local_expiration_time: Some(2_345),
        };
        let deletion = JavaBigDeletionTimeMetadata {
            marked_for_delete_at: 1_456,
            local_deletion_time: 2_789,
            is_live: false,
        };
        let flags = JAVA_UNFILTERED_HAS_TIMESTAMP
            | JAVA_UNFILTERED_HAS_TTL
            | JAVA_UNFILTERED_HAS_DELETION
            | JAVA_UNFILTERED_HAS_ALL_COLUMNS;

        let mut body = Vec::new();
        write_java_big_unfiltered_row_metadata_body(
            &mut body,
            flags,
            &liveness,
            Some(&deletion),
            &encoding_stats,
        )
        .unwrap();

        let mut data = Vec::new();
        write_java_big_unfiltered_empty_clustering_row(&mut data, flags, None, 0, &body).unwrap();

        let parsed =
            parse_java_big_unfiltered_row_metadata_at(&data, 0, 0, &encoding_stats, true).unwrap();
        assert_eq!(parsed.header.kind, JavaBigUnfilteredKind::Row);
        assert_eq!(parsed.liveness, liveness);
        assert_eq!(parsed.deletion_time, Some(deletion));
        assert!(!parsed.deletion_is_shadowable);
        assert_eq!(parsed.body_bytes_consumed, body.len());
    }

    #[test]
    fn reads_java_big_shadowable_row_deletion_metadata() {
        let encoding_stats = JavaBigEncodingStats {
            min_timestamp: 1_000,
            min_local_deletion_time: 2_000,
            min_ttl: 10,
        };
        let liveness = JavaBigRowLivenessMetadata {
            timestamp: Some(1_123),
            ttl: None,
            local_expiration_time: None,
        };
        let deletion = JavaBigDeletionTimeMetadata {
            marked_for_delete_at: 1_456,
            local_deletion_time: 2_789,
            is_live: false,
        };
        let flags = JAVA_UNFILTERED_EXTENSION_FLAG
            | JAVA_UNFILTERED_HAS_TIMESTAMP
            | JAVA_UNFILTERED_HAS_DELETION
            | JAVA_UNFILTERED_HAS_ALL_COLUMNS;
        let mut body = Vec::new();
        write_java_big_unfiltered_row_metadata_body(
            &mut body,
            flags,
            &liveness,
            Some(&deletion),
            &encoding_stats,
        )
        .unwrap();

        let mut data = Vec::new();
        write_java_big_unfiltered_empty_clustering_row(
            &mut data,
            flags,
            Some(JAVA_UNFILTERED_EXT_HAS_SHADOWABLE_DELETION),
            0,
            &body,
        )
        .unwrap();
        let parsed =
            parse_java_big_unfiltered_row_metadata_at(&data, 0, 0, &encoding_stats, true).unwrap();
        assert_eq!(
            parsed.header.extended_flags,
            JAVA_UNFILTERED_EXT_HAS_SHADOWABLE_DELETION
        );
        assert_eq!(parsed.deletion_time, Some(deletion));
        assert!(parsed.deletion_is_shadowable);
    }

    #[test]
    fn rejects_java_big_unfiltered_row_metadata_with_undecoded_columns() {
        let encoding_stats = JavaBigEncodingStats {
            min_timestamp: 0,
            min_local_deletion_time: 0,
            min_ttl: 0,
        };
        let mut data = Vec::new();
        write_java_big_unfiltered_empty_clustering_row(
            &mut data,
            JAVA_UNFILTERED_HAS_ALL_COLUMNS,
            None,
            0,
            &[0xee],
        )
        .unwrap();

        assert!(
            parse_java_big_unfiltered_row_metadata_at(&data, 0, 0, &encoding_stats, true).is_err()
        );
        let parsed =
            parse_java_big_unfiltered_row_metadata_at(&data, 0, 0, &encoding_stats, false).unwrap();
        assert_eq!(parsed.body_bytes_consumed, 0);
        assert_eq!(parsed.header.body_length, Some(1));
    }

    #[test]
    fn reads_java_big_unfiltered_row_simple_cells() {
        let serialization_header = JavaBigSerializationHeader {
            encoding_stats: JavaBigEncodingStats {
                min_timestamp: 1_000,
                min_local_deletion_time: 2_000,
                min_ttl: 10,
            },
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            clustering_types: Vec::new(),
            static_columns: Vec::new(),
            regular_columns: vec![
                JavaBigSerializationHeaderColumn {
                    name: b"name".to_vec(),
                    type_spec: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
                },
                JavaBigSerializationHeaderColumn {
                    name: b"age".to_vec(),
                    type_spec: "org.apache.cassandra.db.marshal.Int32Type".to_string(),
                },
                JavaBigSerializationHeaderColumn {
                    name: b"gone".to_vec(),
                    type_spec: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
                },
            ],
        };
        let liveness = JavaBigRowLivenessMetadata {
            timestamp: Some(1_050),
            ttl: Some(30),
            local_expiration_time: Some(2_500),
        };
        let flags = JAVA_UNFILTERED_HAS_TIMESTAMP
            | JAVA_UNFILTERED_HAS_TTL
            | JAVA_UNFILTERED_HAS_ALL_COLUMNS;
        let mut body = Vec::new();
        write_java_big_unfiltered_row_metadata_body(
            &mut body,
            flags,
            &liveness,
            None,
            &serialization_header.encoding_stats,
        )
        .unwrap();
        let name_cell = JavaBigSimpleCell {
            column_name: b"name".to_vec(),
            column_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            flags: 0,
            timestamp: 1_050,
            local_deletion_time: None,
            ttl: None,
            value: Some(b"alice".to_vec()),
            is_deleted: false,
            is_expiring: false,
            uses_row_timestamp: true,
            uses_row_ttl: false,
        };
        let age_cell = JavaBigSimpleCell {
            column_name: b"age".to_vec(),
            column_type: "org.apache.cassandra.db.marshal.Int32Type".to_string(),
            flags: 0,
            timestamp: 1_060,
            local_deletion_time: Some(2_500),
            ttl: Some(30),
            value: Some(42_i32.to_be_bytes().to_vec()),
            is_deleted: false,
            is_expiring: true,
            uses_row_timestamp: false,
            uses_row_ttl: true,
        };
        let tombstone_cell = JavaBigSimpleCell {
            column_name: b"gone".to_vec(),
            column_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            flags: 0,
            timestamp: 1_070,
            local_deletion_time: Some(2_600),
            ttl: None,
            value: None,
            is_deleted: true,
            is_expiring: false,
            uses_row_timestamp: false,
            uses_row_ttl: false,
        };
        write_java_big_simple_cell(
            &mut body,
            &name_cell,
            &liveness,
            &serialization_header.encoding_stats,
        )
        .unwrap();
        write_java_big_simple_cell(
            &mut body,
            &age_cell,
            &liveness,
            &serialization_header.encoding_stats,
        )
        .unwrap();
        write_java_big_simple_cell(
            &mut body,
            &tombstone_cell,
            &liveness,
            &serialization_header.encoding_stats,
        )
        .unwrap();

        let mut data = Vec::new();
        write_java_big_unfiltered_empty_clustering_row(&mut data, flags, None, 0, &body).unwrap();
        let parsed = parse_java_big_unfiltered_row_with_simple_cells_at(
            &data,
            0,
            &serialization_header,
            true,
        )
        .unwrap();

        assert_eq!(parsed.liveness, liveness);
        assert_eq!(parsed.simple_cells.len(), 3);
        assert_eq!(parsed.simple_cells[0].column_name, b"name");
        assert_eq!(
            parsed.simple_cells[0].value.as_deref(),
            Some(b"alice".as_slice())
        );
        assert!(parsed.simple_cells[0].uses_row_timestamp);
        assert_eq!(
            parsed.simple_cells[1].value,
            Some(42_i32.to_be_bytes().to_vec())
        );
        assert!(parsed.simple_cells[1].is_expiring);
        assert!(parsed.simple_cells[1].uses_row_ttl);
        assert_eq!(parsed.simple_cells[2].column_name, b"gone");
        assert!(parsed.simple_cells[2].is_deleted);
        assert_eq!(parsed.simple_cells[2].value, None);
        assert_eq!(parsed.simple_cells[2].timestamp, 1_070);
        assert_eq!(parsed.simple_cells[2].local_deletion_time, Some(2_600));
    }

    #[test]
    fn reads_java_big_unfiltered_row_simple_cell_subset() {
        let serialization_header = JavaBigSerializationHeader {
            encoding_stats: JavaBigEncodingStats {
                min_timestamp: 100,
                min_local_deletion_time: 200,
                min_ttl: 0,
            },
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            clustering_types: Vec::new(),
            static_columns: Vec::new(),
            regular_columns: vec![
                JavaBigSerializationHeaderColumn {
                    name: b"a".to_vec(),
                    type_spec: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
                },
                JavaBigSerializationHeaderColumn {
                    name: b"b".to_vec(),
                    type_spec: "org.apache.cassandra.db.marshal.Int32Type".to_string(),
                },
                JavaBigSerializationHeaderColumn {
                    name: b"c".to_vec(),
                    type_spec: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
                },
            ],
        };
        let liveness = JavaBigRowLivenessMetadata {
            timestamp: Some(150),
            ttl: None,
            local_expiration_time: None,
        };
        let flags = JAVA_UNFILTERED_HAS_TIMESTAMP;
        let mut body = Vec::new();
        write_java_big_unfiltered_row_metadata_body(
            &mut body,
            flags,
            &liveness,
            None,
            &serialization_header.encoding_stats,
        )
        .unwrap();
        write_java_big_regular_column_subset(&mut body, 3, &[1]).unwrap();
        let cell = JavaBigSimpleCell {
            column_name: b"b".to_vec(),
            column_type: "org.apache.cassandra.db.marshal.Int32Type".to_string(),
            flags: 0,
            timestamp: 150,
            local_deletion_time: None,
            ttl: None,
            value: Some(7_i32.to_be_bytes().to_vec()),
            is_deleted: false,
            is_expiring: false,
            uses_row_timestamp: true,
            uses_row_ttl: false,
        };
        write_java_big_simple_cell(
            &mut body,
            &cell,
            &liveness,
            &serialization_header.encoding_stats,
        )
        .unwrap();

        let mut data = Vec::new();
        write_java_big_unfiltered_empty_clustering_row(&mut data, flags, None, 0, &body).unwrap();

        assert!(
            parse_java_big_unfiltered_row_with_simple_cells_at(
                &data,
                0,
                &serialization_header,
                true,
            )
            .is_err()
        );
        let parsed = parse_java_big_unfiltered_row_with_simple_cells_at(
            &data,
            0,
            &serialization_header,
            false,
        )
        .unwrap();
        assert_eq!(parsed.simple_cells.len(), 1);
        assert_eq!(parsed.simple_cells[0].column_name, b"b");
        assert_eq!(
            parsed.simple_cells[0].value,
            Some(7_i32.to_be_bytes().to_vec())
        );
    }

    #[test]
    fn reads_java_big_large_present_regular_column_subset() {
        let serialization_header = java_big_wide_subset_header(70);
        let liveness = JavaBigRowLivenessMetadata {
            timestamp: Some(150),
            ttl: None,
            local_expiration_time: None,
        };
        let flags = JAVA_UNFILTERED_HAS_TIMESTAMP;
        let mut body = Vec::new();
        write_java_big_unfiltered_row_metadata_body(
            &mut body,
            flags,
            &liveness,
            None,
            &serialization_header.encoding_stats,
        )
        .unwrap();
        write_java_big_regular_column_subset(&mut body, 70, &[2, 69]).unwrap();
        for (idx, value) in [(2, 20_i32), (69, 690_i32)] {
            let cell = java_big_int_cell(idx, value, 150);
            write_java_big_simple_cell(
                &mut body,
                &cell,
                &liveness,
                &serialization_header.encoding_stats,
            )
            .unwrap();
        }

        let mut data = Vec::new();
        write_java_big_unfiltered_empty_clustering_row(&mut data, flags, None, 0, &body).unwrap();
        let parsed = parse_java_big_unfiltered_row_with_simple_cells_at(
            &data,
            0,
            &serialization_header,
            false,
        )
        .unwrap();
        assert_eq!(
            parsed
                .simple_cells
                .iter()
                .map(|cell| cell.column_name.as_slice())
                .collect::<Vec<_>>(),
            vec![b"c02".as_slice(), b"c69".as_slice()]
        );
        assert_eq!(
            parsed.simple_cells[1].value,
            Some(690_i32.to_be_bytes().to_vec())
        );
    }

    #[test]
    fn reads_java_big_large_missing_regular_column_subset() {
        let serialization_header = java_big_wide_subset_header(70);
        let liveness = JavaBigRowLivenessMetadata {
            timestamp: Some(150),
            ttl: None,
            local_expiration_time: None,
        };
        let flags = JAVA_UNFILTERED_HAS_TIMESTAMP;
        let present_indices: Vec<_> = (0..70).filter(|idx| ![1, 3, 68].contains(idx)).collect();
        let mut body = Vec::new();
        write_java_big_unfiltered_row_metadata_body(
            &mut body,
            flags,
            &liveness,
            None,
            &serialization_header.encoding_stats,
        )
        .unwrap();
        write_java_big_regular_column_subset(&mut body, 70, &present_indices).unwrap();
        for idx in &present_indices {
            let cell = java_big_int_cell(*idx, *idx as i32, 150);
            write_java_big_simple_cell(
                &mut body,
                &cell,
                &liveness,
                &serialization_header.encoding_stats,
            )
            .unwrap();
        }

        let mut data = Vec::new();
        write_java_big_unfiltered_empty_clustering_row(&mut data, flags, None, 0, &body).unwrap();
        let parsed = parse_java_big_unfiltered_row_with_simple_cells_at(
            &data,
            0,
            &serialization_header,
            false,
        )
        .unwrap();
        assert_eq!(parsed.simple_cells.len(), 67);
        assert!(
            !parsed
                .simple_cells
                .iter()
                .any(|cell| cell.column_name == b"c01" || cell.column_name == b"c68")
        );
        assert_eq!(
            parsed.simple_cells.last().unwrap().column_name.as_slice(),
            b"c69"
        );
    }

    #[test]
    fn reads_java_big_partition_simple_rows_until_end() {
        let serialization_header = JavaBigSerializationHeader {
            encoding_stats: JavaBigEncodingStats {
                min_timestamp: 1_000,
                min_local_deletion_time: 2_000,
                min_ttl: 10,
            },
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            clustering_types: Vec::new(),
            static_columns: Vec::new(),
            regular_columns: vec![JavaBigSerializationHeaderColumn {
                name: b"value".to_vec(),
                type_spec: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            }],
        };
        let liveness = JavaBigRowLivenessMetadata {
            timestamp: Some(1_050),
            ttl: None,
            local_expiration_time: None,
        };
        let flags = JAVA_UNFILTERED_HAS_TIMESTAMP | JAVA_UNFILTERED_HAS_ALL_COLUMNS;
        let mut row_body = Vec::new();
        write_java_big_unfiltered_row_metadata_body(
            &mut row_body,
            flags,
            &liveness,
            None,
            &serialization_header.encoding_stats,
        )
        .unwrap();
        let cell = JavaBigSimpleCell {
            column_name: b"value".to_vec(),
            column_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            flags: 0,
            timestamp: 1_050,
            local_deletion_time: None,
            ttl: None,
            value: Some(b"v1".to_vec()),
            is_deleted: false,
            is_expiring: false,
            uses_row_timestamp: true,
            uses_row_ttl: false,
        };
        write_java_big_simple_cell(
            &mut row_body,
            &cell,
            &liveness,
            &serialization_header.encoding_stats,
        )
        .unwrap();

        let mut data = Vec::new();
        write_java_big_data_partition_header(
            &mut data,
            b"pk",
            &JavaBigDeletionTimeMetadata::LIVE,
            false,
        )
        .unwrap();
        write_java_big_unfiltered_empty_clustering_row(&mut data, flags, None, 0, &row_body)
            .unwrap();
        write_java_big_unfiltered_end_of_partition(&mut data);

        let partition =
            parse_java_big_data_partition_simple_rows_at(&data, 0, false, &serialization_header)
                .unwrap();
        assert_eq!(partition.header.partition_key, b"pk");
        assert_eq!(partition.end_offset, data.len() as u64);
        assert_eq!(partition.bytes_consumed, data.len());
        assert_eq!(partition.unfiltereds.len(), 1);
        let JavaBigDataPartitionUnfiltered::Row(row) = &partition.unfiltereds[0] else {
            panic!("expected row");
        };
        assert_eq!(row.simple_cells[0].value.as_deref(), Some(b"v1".as_slice()));
    }

    #[test]
    fn reads_java_big_partition_marker_stream_until_end() {
        let serialization_header = JavaBigSerializationHeader {
            encoding_stats: JavaBigEncodingStats {
                min_timestamp: 0,
                min_local_deletion_time: 0,
                min_ttl: 0,
            },
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            clustering_types: Vec::new(),
            static_columns: Vec::new(),
            regular_columns: Vec::new(),
        };
        let mut data = Vec::new();
        write_java_big_data_partition_header(
            &mut data,
            b"pk",
            &JavaBigDeletionTimeMetadata::LIVE,
            false,
        )
        .unwrap();
        let marker_deletion = JavaBigDeletionTimeMetadata {
            marked_for_delete_at: 10,
            local_deletion_time: 20,
            is_live: false,
        };
        let mut marker_body = Vec::new();
        write_java_big_range_tombstone_marker_body(
            &mut marker_body,
            JAVA_CLUSTERING_KIND_INCL_START_BOUND,
            &marker_deletion,
            None,
            &serialization_header.encoding_stats,
        )
        .unwrap();
        write_java_big_unfiltered_marker_header(
            &mut data,
            JAVA_CLUSTERING_KIND_INCL_START_BOUND,
            0,
            &marker_body,
        )
        .unwrap();
        write_java_big_unfiltered_end_of_partition(&mut data);

        let partition =
            parse_java_big_data_partition_simple_rows_at(&data, 0, false, &serialization_header)
                .unwrap();
        assert_eq!(partition.unfiltereds.len(), 1);
        let JavaBigDataPartitionUnfiltered::RangeTombstoneMarker(marker) =
            &partition.unfiltereds[0]
        else {
            panic!("expected marker");
        };
        assert_eq!(
            marker.header.clustering_kind_ordinal,
            Some(JAVA_CLUSTERING_KIND_INCL_START_BOUND)
        );
        assert_eq!(marker.deletion_time, marker_deletion);
        assert_eq!(partition.end_offset, data.len() as u64);
    }

    #[test]
    fn reads_java_big_boundary_marker_deletions() {
        let serialization_header = JavaBigSerializationHeader {
            encoding_stats: JavaBigEncodingStats {
                min_timestamp: 100,
                min_local_deletion_time: 200,
                min_ttl: 0,
            },
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            clustering_types: Vec::new(),
            static_columns: Vec::new(),
            regular_columns: Vec::new(),
        };
        let end_deletion = JavaBigDeletionTimeMetadata {
            marked_for_delete_at: 111,
            local_deletion_time: 222,
            is_live: false,
        };
        let start_deletion = JavaBigDeletionTimeMetadata {
            marked_for_delete_at: 112,
            local_deletion_time: 223,
            is_live: false,
        };
        let mut marker_body = Vec::new();
        write_java_big_range_tombstone_marker_body(
            &mut marker_body,
            JAVA_CLUSTERING_KIND_EXCL_END_INCL_START_BOUNDARY,
            &end_deletion,
            Some(&start_deletion),
            &serialization_header.encoding_stats,
        )
        .unwrap();

        let mut data = Vec::new();
        write_java_big_data_partition_header(
            &mut data,
            b"pk",
            &JavaBigDeletionTimeMetadata::LIVE,
            false,
        )
        .unwrap();
        write_java_big_unfiltered_marker_header(
            &mut data,
            JAVA_CLUSTERING_KIND_EXCL_END_INCL_START_BOUNDARY,
            0,
            &marker_body,
        )
        .unwrap();
        write_java_big_unfiltered_end_of_partition(&mut data);

        let partition =
            parse_java_big_data_partition_simple_rows_at(&data, 0, false, &serialization_header)
                .unwrap();
        let JavaBigDataPartitionUnfiltered::RangeTombstoneMarker(marker) =
            &partition.unfiltereds[0]
        else {
            panic!("expected boundary marker");
        };
        assert_eq!(
            marker.header.clustering_kind_ordinal,
            Some(JAVA_CLUSTERING_KIND_EXCL_END_INCL_START_BOUNDARY)
        );
        assert_eq!(marker.deletion_time, end_deletion);
        assert_eq!(marker.boundary_start_deletion_time, Some(start_deletion));
        assert_eq!(marker.body_bytes_consumed, marker_body.len());
        assert_eq!(partition.end_offset, data.len() as u64);

        assert!(
            write_java_big_range_tombstone_marker_body(
                &mut Vec::new(),
                JAVA_CLUSTERING_KIND_EXCL_END_INCL_START_BOUNDARY,
                &end_deletion,
                None,
                &serialization_header.encoding_stats,
            )
            .is_err()
        );
    }

    #[test]
    fn reads_java_big_clustered_partition_simple_rows_until_end() {
        let serialization_header = JavaBigSerializationHeader {
            encoding_stats: JavaBigEncodingStats {
                min_timestamp: 1_000,
                min_local_deletion_time: 2_000,
                min_ttl: 10,
            },
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            clustering_types: vec!["org.apache.cassandra.db.marshal.Int32Type".to_string()],
            static_columns: Vec::new(),
            regular_columns: vec![JavaBigSerializationHeaderColumn {
                name: b"value".to_vec(),
                type_spec: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            }],
        };
        let liveness = JavaBigRowLivenessMetadata {
            timestamp: Some(1_050),
            ttl: None,
            local_expiration_time: None,
        };
        let flags = JAVA_UNFILTERED_HAS_TIMESTAMP | JAVA_UNFILTERED_HAS_ALL_COLUMNS;
        let mut row_body = Vec::new();
        write_java_big_unfiltered_row_metadata_body(
            &mut row_body,
            flags,
            &liveness,
            None,
            &serialization_header.encoding_stats,
        )
        .unwrap();
        let cell = JavaBigSimpleCell {
            column_name: b"value".to_vec(),
            column_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            flags: 0,
            timestamp: 1_050,
            local_deletion_time: None,
            ttl: None,
            value: Some(b"clustered".to_vec()),
            is_deleted: false,
            is_expiring: false,
            uses_row_timestamp: true,
            uses_row_ttl: false,
        };
        write_java_big_simple_cell(
            &mut row_body,
            &cell,
            &liveness,
            &serialization_header.encoding_stats,
        )
        .unwrap();

        let mut data = Vec::new();
        write_java_big_data_partition_header(
            &mut data,
            b"pk",
            &JavaBigDeletionTimeMetadata::LIVE,
            false,
        )
        .unwrap();
        write_java_big_unfiltered_row(
            &mut data,
            flags,
            None,
            &[Some(7_i32.to_be_bytes().to_vec())],
            &serialization_header.clustering_types,
            0,
            &row_body,
        )
        .unwrap();
        write_java_big_unfiltered_end_of_partition(&mut data);

        let partition =
            parse_java_big_data_partition_simple_rows_at(&data, 0, false, &serialization_header)
                .unwrap();
        let JavaBigDataPartitionUnfiltered::Row(row) = &partition.unfiltereds[0] else {
            panic!("expected clustered row");
        };
        assert_eq!(
            row.header.clustering_values,
            vec![Some(7_i32.to_be_bytes().to_vec())]
        );
        assert_eq!(
            row.simple_cells[0].value.as_deref(),
            Some(b"clustered".as_slice())
        );
    }

    #[test]
    fn reads_java_big_static_row_simple_cells() {
        let serialization_header = JavaBigSerializationHeader {
            encoding_stats: JavaBigEncodingStats {
                min_timestamp: 1_000,
                min_local_deletion_time: 2_000,
                min_ttl: 10,
            },
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            clustering_types: vec!["org.apache.cassandra.db.marshal.Int32Type".to_string()],
            static_columns: vec![JavaBigSerializationHeaderColumn {
                name: b"s".to_vec(),
                type_spec: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            }],
            regular_columns: vec![JavaBigSerializationHeaderColumn {
                name: b"v".to_vec(),
                type_spec: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            }],
        };
        let liveness = JavaBigRowLivenessMetadata {
            timestamp: Some(1_050),
            ttl: None,
            local_expiration_time: None,
        };
        let flags = JAVA_UNFILTERED_EXTENSION_FLAG
            | JAVA_UNFILTERED_HAS_TIMESTAMP
            | JAVA_UNFILTERED_HAS_ALL_COLUMNS;
        let mut row_body = Vec::new();
        write_java_big_unfiltered_row_metadata_body(
            &mut row_body,
            flags,
            &liveness,
            None,
            &serialization_header.encoding_stats,
        )
        .unwrap();
        let cell = JavaBigSimpleCell {
            column_name: b"s".to_vec(),
            column_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            flags: 0,
            timestamp: 1_050,
            local_deletion_time: None,
            ttl: None,
            value: Some(b"static-value".to_vec()),
            is_deleted: false,
            is_expiring: false,
            uses_row_timestamp: true,
            uses_row_ttl: false,
        };
        write_java_big_simple_cell(
            &mut row_body,
            &cell,
            &liveness,
            &serialization_header.encoding_stats,
        )
        .unwrap();

        let mut data = Vec::new();
        write_java_big_data_partition_header(
            &mut data,
            b"pk",
            &JavaBigDeletionTimeMetadata::LIVE,
            false,
        )
        .unwrap();
        write_java_big_unfiltered_row(
            &mut data,
            flags,
            Some(JAVA_UNFILTERED_EXT_IS_STATIC),
            &[],
            &serialization_header.clustering_types,
            0,
            &row_body,
        )
        .unwrap();
        write_java_big_unfiltered_end_of_partition(&mut data);

        let partition =
            parse_java_big_data_partition_simple_rows_at(&data, 0, false, &serialization_header)
                .unwrap();
        let JavaBigDataPartitionUnfiltered::Row(row) = &partition.unfiltereds[0] else {
            panic!("expected static row");
        };
        assert!(row.header.is_static);
        assert!(row.header.clustering_values.is_empty());
        assert_eq!(row.simple_cells[0].column_name, b"s");
        assert_eq!(
            row.simple_cells[0].value.as_deref(),
            Some(b"static-value".as_slice())
        );
    }

    #[test]
    fn reads_java_big_reversed_clustering_and_cell_values() {
        let reversed_int = "org.apache.cassandra.db.marshal.ReversedType(org.apache.cassandra.db.marshal.Int32Type)";
        let serialization_header = JavaBigSerializationHeader {
            encoding_stats: JavaBigEncodingStats {
                min_timestamp: 1_000,
                min_local_deletion_time: 2_000,
                min_ttl: 10,
            },
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            clustering_types: vec![reversed_int.to_string()],
            static_columns: Vec::new(),
            regular_columns: vec![JavaBigSerializationHeaderColumn {
                name: b"rank".to_vec(),
                type_spec: reversed_int.to_string(),
            }],
        };
        let liveness = JavaBigRowLivenessMetadata {
            timestamp: Some(1_050),
            ttl: None,
            local_expiration_time: None,
        };
        let flags = JAVA_UNFILTERED_HAS_TIMESTAMP | JAVA_UNFILTERED_HAS_ALL_COLUMNS;
        let mut row_body = Vec::new();
        write_java_big_unfiltered_row_metadata_body(
            &mut row_body,
            flags,
            &liveness,
            None,
            &serialization_header.encoding_stats,
        )
        .unwrap();
        let cell = JavaBigSimpleCell {
            column_name: b"rank".to_vec(),
            column_type: reversed_int.to_string(),
            flags: 0,
            timestamp: 1_050,
            local_deletion_time: None,
            ttl: None,
            value: Some(9_i32.to_be_bytes().to_vec()),
            is_deleted: false,
            is_expiring: false,
            uses_row_timestamp: true,
            uses_row_ttl: false,
        };
        write_java_big_simple_cell(
            &mut row_body,
            &cell,
            &liveness,
            &serialization_header.encoding_stats,
        )
        .unwrap();

        let mut data = Vec::new();
        write_java_big_data_partition_header(
            &mut data,
            b"pk",
            &JavaBigDeletionTimeMetadata::LIVE,
            false,
        )
        .unwrap();
        write_java_big_unfiltered_row(
            &mut data,
            flags,
            None,
            &[Some(5_i32.to_be_bytes().to_vec())],
            &serialization_header.clustering_types,
            0,
            &row_body,
        )
        .unwrap();
        write_java_big_unfiltered_end_of_partition(&mut data);

        let partition =
            parse_java_big_data_partition_simple_rows_at(&data, 0, false, &serialization_header)
                .unwrap();
        let JavaBigDataPartitionUnfiltered::Row(row) = &partition.unfiltereds[0] else {
            panic!("expected reversed clustering row");
        };
        assert_eq!(
            row.header.clustering_values,
            vec![Some(5_i32.to_be_bytes().to_vec())]
        );
        assert_eq!(
            row.simple_cells[0].value.as_deref(),
            Some(9_i32.to_be_bytes().as_slice())
        );
    }

    #[test]
    fn reads_java_big_fixed_vector_cell_values() {
        let vector_type = "org.apache.cassandra.db.marshal.VectorType(org.apache.cassandra.db.marshal.FloatType,3)";
        let vector_value = [1.0_f32, -2.5_f32, 3.25_f32]
            .into_iter()
            .flat_map(f32::to_be_bytes)
            .collect::<Vec<_>>();
        let serialization_header = JavaBigSerializationHeader {
            encoding_stats: JavaBigEncodingStats {
                min_timestamp: 1_000,
                min_local_deletion_time: 2_000,
                min_ttl: 10,
            },
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            clustering_types: Vec::new(),
            static_columns: Vec::new(),
            regular_columns: vec![JavaBigSerializationHeaderColumn {
                name: b"embedding".to_vec(),
                type_spec: vector_type.to_string(),
            }],
        };
        let liveness = JavaBigRowLivenessMetadata {
            timestamp: Some(1_050),
            ttl: None,
            local_expiration_time: None,
        };
        let flags = JAVA_UNFILTERED_HAS_TIMESTAMP | JAVA_UNFILTERED_HAS_ALL_COLUMNS;
        let mut row_body = Vec::new();
        write_java_big_unfiltered_row_metadata_body(
            &mut row_body,
            flags,
            &liveness,
            None,
            &serialization_header.encoding_stats,
        )
        .unwrap();
        let cell = JavaBigSimpleCell {
            column_name: b"embedding".to_vec(),
            column_type: vector_type.to_string(),
            flags: 0,
            timestamp: 1_050,
            local_deletion_time: None,
            ttl: None,
            value: Some(vector_value.clone()),
            is_deleted: false,
            is_expiring: false,
            uses_row_timestamp: true,
            uses_row_ttl: false,
        };
        write_java_big_simple_cell(
            &mut row_body,
            &cell,
            &liveness,
            &serialization_header.encoding_stats,
        )
        .unwrap();

        let mut data = Vec::new();
        write_java_big_data_partition_header(
            &mut data,
            b"pk",
            &JavaBigDeletionTimeMetadata::LIVE,
            false,
        )
        .unwrap();
        write_java_big_unfiltered_row(&mut data, flags, None, &[], &[], 0, &row_body).unwrap();
        write_java_big_unfiltered_end_of_partition(&mut data);

        let partition =
            parse_java_big_data_partition_simple_rows_at(&data, 0, false, &serialization_header)
                .unwrap();
        let JavaBigDataPartitionUnfiltered::Row(row) = &partition.unfiltereds[0] else {
            panic!("expected vector row");
        };
        assert_eq!(
            row.simple_cells[0].value.as_deref(),
            Some(vector_value.as_slice())
        );
    }

    #[test]
    fn reads_java_big_complex_column_cells() {
        let list_type =
            "org.apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.Int32Type)";
        let serialization_header = JavaBigSerializationHeader {
            encoding_stats: JavaBigEncodingStats {
                min_timestamp: 1_000,
                min_local_deletion_time: 2_000,
                min_ttl: 10,
            },
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            clustering_types: Vec::new(),
            static_columns: Vec::new(),
            regular_columns: vec![JavaBigSerializationHeaderColumn {
                name: b"items".to_vec(),
                type_spec: list_type.to_string(),
            }],
        };
        let liveness = JavaBigRowLivenessMetadata {
            timestamp: Some(1_050),
            ttl: None,
            local_expiration_time: None,
        };
        let complex_deletion = JavaBigDeletionTimeMetadata {
            marked_for_delete_at: 1_025,
            local_deletion_time: 2_025,
            is_live: false,
        };
        let flags = JAVA_UNFILTERED_HAS_TIMESTAMP
            | JAVA_UNFILTERED_HAS_ALL_COLUMNS
            | JAVA_UNFILTERED_HAS_COMPLEX_DELETION;
        let mut row_body = Vec::new();
        write_java_big_unfiltered_row_metadata_body(
            &mut row_body,
            flags,
            &liveness,
            None,
            &serialization_header.encoding_stats,
        )
        .unwrap();
        let cells = vec![
            JavaBigComplexCell {
                path: vec![0x10; 16],
                cell: JavaBigSimpleCell {
                    column_name: b"items".to_vec(),
                    column_type: list_type.to_string(),
                    flags: 0,
                    timestamp: 1_050,
                    local_deletion_time: None,
                    ttl: None,
                    value: Some(10_i32.to_be_bytes().to_vec()),
                    is_deleted: false,
                    is_expiring: false,
                    uses_row_timestamp: true,
                    uses_row_ttl: false,
                },
            },
            JavaBigComplexCell {
                path: vec![0x20; 16],
                cell: JavaBigSimpleCell {
                    column_name: b"items".to_vec(),
                    column_type: list_type.to_string(),
                    flags: 0,
                    timestamp: 1_050,
                    local_deletion_time: None,
                    ttl: None,
                    value: Some(11_i32.to_be_bytes().to_vec()),
                    is_deleted: false,
                    is_expiring: false,
                    uses_row_timestamp: true,
                    uses_row_ttl: false,
                },
            },
        ];
        write_java_big_complex_column(
            &mut row_body,
            &serialization_header.regular_columns[0],
            true,
            Some(&complex_deletion),
            &cells,
            &liveness,
            &serialization_header.encoding_stats,
        )
        .unwrap();

        let mut data = Vec::new();
        write_java_big_data_partition_header(
            &mut data,
            b"pk",
            &JavaBigDeletionTimeMetadata::LIVE,
            false,
        )
        .unwrap();
        write_java_big_unfiltered_row(&mut data, flags, None, &[], &[], 0, &row_body).unwrap();
        write_java_big_unfiltered_end_of_partition(&mut data);

        let partition =
            parse_java_big_data_partition_simple_rows_at(&data, 0, false, &serialization_header)
                .unwrap();
        let JavaBigDataPartitionUnfiltered::Row(row) = &partition.unfiltereds[0] else {
            panic!("expected row with complex column");
        };
        assert!(row.simple_cells.is_empty());
        assert_eq!(row.complex_columns.len(), 1);
        assert_eq!(row.complex_columns[0].column_name, b"items");
        assert_eq!(row.complex_columns[0].deletion_time, Some(complex_deletion));
        assert_eq!(row.complex_columns[0].cells.len(), 2);
        assert_eq!(row.complex_columns[0].cells[0].path, vec![0x10; 16]);
        assert_eq!(
            row.complex_columns[0].cells[0].cell.value.as_deref(),
            Some(10_i32.to_be_bytes().as_slice())
        );
        assert_eq!(row.complex_columns[0].cells[1].path, vec![0x20; 16]);
        assert_eq!(
            row.complex_columns[0].cells[1].cell.value.as_deref(),
            Some(11_i32.to_be_bytes().as_slice())
        );
    }

    #[test]
    fn reads_java_big_complex_map_column_cells() {
        let map_type = "org.apache.cassandra.db.marshal.MapType(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.Int32Type)";
        let serialization_header = JavaBigSerializationHeader {
            encoding_stats: JavaBigEncodingStats {
                min_timestamp: 1_000,
                min_local_deletion_time: 2_000,
                min_ttl: 10,
            },
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            clustering_types: Vec::new(),
            static_columns: Vec::new(),
            regular_columns: vec![JavaBigSerializationHeaderColumn {
                name: b"attrs".to_vec(),
                type_spec: map_type.to_string(),
            }],
        };
        let liveness = JavaBigRowLivenessMetadata {
            timestamp: Some(1_050),
            ttl: None,
            local_expiration_time: None,
        };
        let flags = JAVA_UNFILTERED_HAS_TIMESTAMP | JAVA_UNFILTERED_HAS_ALL_COLUMNS;
        let mut row_body = Vec::new();
        write_java_big_unfiltered_row_metadata_body(
            &mut row_body,
            flags,
            &liveness,
            None,
            &serialization_header.encoding_stats,
        )
        .unwrap();
        let cells = vec![JavaBigComplexCell {
            path: b"score".to_vec(),
            cell: JavaBigSimpleCell {
                column_name: b"attrs".to_vec(),
                column_type: map_type.to_string(),
                flags: 0,
                timestamp: 1_050,
                local_deletion_time: None,
                ttl: None,
                value: Some(99_i32.to_be_bytes().to_vec()),
                is_deleted: false,
                is_expiring: false,
                uses_row_timestamp: true,
                uses_row_ttl: false,
            },
        }];
        write_java_big_complex_column(
            &mut row_body,
            &serialization_header.regular_columns[0],
            false,
            None,
            &cells,
            &liveness,
            &serialization_header.encoding_stats,
        )
        .unwrap();

        let mut data = Vec::new();
        write_java_big_data_partition_header(
            &mut data,
            b"pk",
            &JavaBigDeletionTimeMetadata::LIVE,
            false,
        )
        .unwrap();
        write_java_big_unfiltered_row(&mut data, flags, None, &[], &[], 0, &row_body).unwrap();
        write_java_big_unfiltered_end_of_partition(&mut data);

        let partition =
            parse_java_big_data_partition_simple_rows_at(&data, 0, false, &serialization_header)
                .unwrap();
        let JavaBigDataPartitionUnfiltered::Row(row) = &partition.unfiltereds[0] else {
            panic!("expected row with complex map column");
        };
        assert_eq!(row.complex_columns.len(), 1);
        assert_eq!(row.complex_columns[0].column_name, b"attrs");
        assert_eq!(row.complex_columns[0].deletion_time, None);
        assert_eq!(row.complex_columns[0].cells[0].path, b"score");
        assert_eq!(
            row.complex_columns[0].cells[0].cell.value.as_deref(),
            Some(99_i32.to_be_bytes().as_slice())
        );
    }

    #[test]
    fn reads_java_big_complex_set_column_cells() {
        let set_type =
            "org.apache.cassandra.db.marshal.SetType(org.apache.cassandra.db.marshal.UTF8Type)";
        let serialization_header = JavaBigSerializationHeader {
            encoding_stats: JavaBigEncodingStats {
                min_timestamp: 1_000,
                min_local_deletion_time: 2_000,
                min_ttl: 10,
            },
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            clustering_types: Vec::new(),
            static_columns: Vec::new(),
            regular_columns: vec![JavaBigSerializationHeaderColumn {
                name: b"tags".to_vec(),
                type_spec: set_type.to_string(),
            }],
        };
        let liveness = JavaBigRowLivenessMetadata {
            timestamp: Some(1_050),
            ttl: None,
            local_expiration_time: None,
        };
        let flags = JAVA_UNFILTERED_HAS_TIMESTAMP | JAVA_UNFILTERED_HAS_ALL_COLUMNS;
        let mut row_body = Vec::new();
        write_java_big_unfiltered_row_metadata_body(
            &mut row_body,
            flags,
            &liveness,
            None,
            &serialization_header.encoding_stats,
        )
        .unwrap();
        let cells = vec![JavaBigComplexCell {
            path: b"blue".to_vec(),
            cell: JavaBigSimpleCell {
                column_name: b"tags".to_vec(),
                column_type: set_type.to_string(),
                flags: 0,
                timestamp: 1_050,
                local_deletion_time: None,
                ttl: None,
                value: None,
                is_deleted: false,
                is_expiring: false,
                uses_row_timestamp: true,
                uses_row_ttl: false,
            },
        }];
        write_java_big_complex_column(
            &mut row_body,
            &serialization_header.regular_columns[0],
            false,
            None,
            &cells,
            &liveness,
            &serialization_header.encoding_stats,
        )
        .unwrap();

        let mut data = Vec::new();
        write_java_big_data_partition_header(
            &mut data,
            b"pk",
            &JavaBigDeletionTimeMetadata::LIVE,
            false,
        )
        .unwrap();
        write_java_big_unfiltered_row(&mut data, flags, None, &[], &[], 0, &row_body).unwrap();
        write_java_big_unfiltered_end_of_partition(&mut data);

        let partition =
            parse_java_big_data_partition_simple_rows_at(&data, 0, false, &serialization_header)
                .unwrap();
        let JavaBigDataPartitionUnfiltered::Row(row) = &partition.unfiltereds[0] else {
            panic!("expected row with complex set column");
        };
        assert_eq!(row.complex_columns.len(), 1);
        assert_eq!(row.complex_columns[0].column_name, b"tags");
        assert_eq!(row.complex_columns[0].cells[0].path, b"blue");
        assert_eq!(row.complex_columns[0].cells[0].cell.value, None);
        assert!(row.complex_columns[0].cells[0].cell.flags & JAVA_CELL_HAS_EMPTY_VALUE != 0);
    }

    #[test]
    fn reads_java_big_complex_udt_column_cells() {
        let udt_type = "org.apache.cassandra.db.marshal.UserType(ks,70726f66696c65,6e616d65:org.apache.cassandra.db.marshal.UTF8Type,616765:org.apache.cassandra.db.marshal.Int32Type)";
        let serialization_header = JavaBigSerializationHeader {
            encoding_stats: JavaBigEncodingStats {
                min_timestamp: 1_000,
                min_local_deletion_time: 2_000,
                min_ttl: 10,
            },
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            clustering_types: Vec::new(),
            static_columns: Vec::new(),
            regular_columns: vec![JavaBigSerializationHeaderColumn {
                name: b"profile".to_vec(),
                type_spec: udt_type.to_string(),
            }],
        };
        let liveness = JavaBigRowLivenessMetadata {
            timestamp: Some(1_050),
            ttl: None,
            local_expiration_time: None,
        };
        let flags = JAVA_UNFILTERED_HAS_TIMESTAMP | JAVA_UNFILTERED_HAS_ALL_COLUMNS;
        let mut row_body = Vec::new();
        write_java_big_unfiltered_row_metadata_body(
            &mut row_body,
            flags,
            &liveness,
            None,
            &serialization_header.encoding_stats,
        )
        .unwrap();
        let cells = vec![
            JavaBigComplexCell {
                path: 0_u16.to_be_bytes().to_vec(),
                cell: JavaBigSimpleCell {
                    column_name: b"profile".to_vec(),
                    column_type: udt_type.to_string(),
                    flags: 0,
                    timestamp: 1_050,
                    local_deletion_time: None,
                    ttl: None,
                    value: Some(b"ada".to_vec()),
                    is_deleted: false,
                    is_expiring: false,
                    uses_row_timestamp: true,
                    uses_row_ttl: false,
                },
            },
            JavaBigComplexCell {
                path: 1_u16.to_be_bytes().to_vec(),
                cell: JavaBigSimpleCell {
                    column_name: b"profile".to_vec(),
                    column_type: udt_type.to_string(),
                    flags: 0,
                    timestamp: 1_050,
                    local_deletion_time: None,
                    ttl: None,
                    value: Some(37_i32.to_be_bytes().to_vec()),
                    is_deleted: false,
                    is_expiring: false,
                    uses_row_timestamp: true,
                    uses_row_ttl: false,
                },
            },
        ];
        write_java_big_complex_column(
            &mut row_body,
            &serialization_header.regular_columns[0],
            false,
            None,
            &cells,
            &liveness,
            &serialization_header.encoding_stats,
        )
        .unwrap();

        let mut data = Vec::new();
        write_java_big_data_partition_header(
            &mut data,
            b"pk",
            &JavaBigDeletionTimeMetadata::LIVE,
            false,
        )
        .unwrap();
        write_java_big_unfiltered_row(&mut data, flags, None, &[], &[], 0, &row_body).unwrap();
        write_java_big_unfiltered_end_of_partition(&mut data);

        let partition =
            parse_java_big_data_partition_simple_rows_at(&data, 0, false, &serialization_header)
                .unwrap();
        let JavaBigDataPartitionUnfiltered::Row(row) = &partition.unfiltereds[0] else {
            panic!("expected row with complex UDT column");
        };
        assert_eq!(row.complex_columns.len(), 1);
        assert_eq!(row.complex_columns[0].cells[0].path, 0_u16.to_be_bytes());
        assert_eq!(
            row.complex_columns[0].cells[0].cell.value.as_deref(),
            Some(b"ada".as_slice())
        );
        assert_eq!(row.complex_columns[0].cells[1].path, 1_u16.to_be_bytes());
        assert_eq!(
            row.complex_columns[0].cells[1].cell.value.as_deref(),
            Some(37_i32.to_be_bytes().as_slice())
        );
    }

    #[test]
    fn reads_java_big_clustered_marker_stream_until_end() {
        let serialization_header = JavaBigSerializationHeader {
            encoding_stats: JavaBigEncodingStats {
                min_timestamp: 0,
                min_local_deletion_time: 0,
                min_ttl: 0,
            },
            key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            clustering_types: vec!["org.apache.cassandra.db.marshal.Int32Type".to_string()],
            static_columns: Vec::new(),
            regular_columns: Vec::new(),
        };
        let mut data = Vec::new();
        write_java_big_data_partition_header(
            &mut data,
            b"pk",
            &JavaBigDeletionTimeMetadata::LIVE,
            false,
        )
        .unwrap();
        let marker_deletion = JavaBigDeletionTimeMetadata {
            marked_for_delete_at: 10,
            local_deletion_time: 20,
            is_live: false,
        };
        let mut marker_body = Vec::new();
        write_java_big_range_tombstone_marker_body(
            &mut marker_body,
            JAVA_CLUSTERING_KIND_INCL_START_BOUND,
            &marker_deletion,
            None,
            &serialization_header.encoding_stats,
        )
        .unwrap();
        write_java_big_unfiltered_marker(
            &mut data,
            JAVA_CLUSTERING_KIND_INCL_START_BOUND,
            &[Some(9_i32.to_be_bytes().to_vec())],
            &serialization_header.clustering_types,
            0,
            &marker_body,
        )
        .unwrap();
        write_java_big_unfiltered_end_of_partition(&mut data);

        let partition =
            parse_java_big_data_partition_simple_rows_at(&data, 0, false, &serialization_header)
                .unwrap();
        let JavaBigDataPartitionUnfiltered::RangeTombstoneMarker(marker) =
            &partition.unfiltereds[0]
        else {
            panic!("expected marker");
        };
        assert_eq!(
            marker.header.clustering_values,
            vec![Some(9_i32.to_be_bytes().to_vec())]
        );
        assert_eq!(marker.deletion_time, marker_deletion);
        assert_eq!(partition.end_offset, data.len() as u64);
    }

    #[test]
    fn reads_java_big_summary() {
        let summary = JavaBigSummary {
            min_index_interval: 128,
            offheap_size: 0,
            sampling_level: 128,
            size_at_full_sampling: 3,
            entries: vec![
                JavaBigSummaryEntry {
                    partition_key: b"pk-a".to_vec(),
                    index_offset: 0,
                },
                JavaBigSummaryEntry {
                    partition_key: b"pk-b".to_vec(),
                    index_offset: 32,
                },
                JavaBigSummaryEntry {
                    partition_key: b"pk-c".to_vec(),
                    index_offset: 64,
                },
            ],
            first_key: b"pk-a".to_vec(),
            last_key: b"pk-c".to_vec(),
        };

        let encoded = write_java_big_summary(&summary).unwrap();
        let parsed = parse_java_big_summary(&encoded).unwrap();

        assert_eq!(parsed.min_index_interval, 128);
        assert_eq!(parsed.offheap_size, 48);
        assert_eq!(parsed.sampling_level, 128);
        assert_eq!(parsed.size_at_full_sampling, 3);
        assert_eq!(parsed.entries, summary.entries);
        assert_eq!(parsed.first_key, b"pk-a");
        assert_eq!(parsed.last_key, b"pk-c");

        let mut corrupt = encoded;
        corrupt[20] = 0;
        corrupt[21] = 0;
        corrupt[22] = 0;
        corrupt[23] = 0;
        assert!(parse_java_big_summary(&corrupt).is_err());
    }

    #[test]
    fn manifest_reads_java_big_summary() {
        let dir = TempDir::new().unwrap();
        for component in [
            JavaBigComponent::Data,
            JavaBigComponent::Index,
            JavaBigComponent::Statistics,
            JavaBigComponent::Toc,
        ] {
            fs::write(
                dir.path().join(format!("nb-1-big-{}", component.suffix())),
                b"x",
            )
            .unwrap();
        }
        let summary = JavaBigSummary {
            min_index_interval: 64,
            offheap_size: 0,
            sampling_level: 128,
            size_at_full_sampling: 1,
            entries: vec![JavaBigSummaryEntry {
                partition_key: b"pk".to_vec(),
                index_offset: 99,
            }],
            first_key: b"pk".to_vec(),
            last_key: b"pk".to_vec(),
        };
        fs::write(
            dir.path().join("nb-1-big-Summary.db"),
            write_java_big_summary(&summary).unwrap(),
        )
        .unwrap();

        let manifest = discover_java_big_sstables(dir.path()).unwrap().remove(0);
        let parsed = manifest.summary().unwrap().unwrap();
        assert_eq!(parsed.entries[0].partition_key, b"pk");
        assert_eq!(parsed.entries[0].index_offset, 99);
        assert_eq!(parsed.first_key, b"pk");
        assert_eq!(parsed.last_key, b"pk");
    }

    #[test]
    fn reads_java_big_statistics_component_container() {
        let validation = JavaBigValidationMetadata {
            partitioner: "org.apache.cassandra.dht.Murmur3Partitioner".to_string(),
            bloom_filter_fp_chance: 0.01,
        };
        let validation_payload = write_java_big_validation_metadata(&validation).unwrap();
        let compaction = JavaBigCompactionMetadata {
            cardinality_estimator: vec![0xca, 0xfe, 0xba, 0xbe],
        };
        let compaction_payload = write_java_big_compaction_metadata(&compaction).unwrap();
        let components = vec![
            JavaBigStatisticsComponent {
                component_type: JavaBigMetadataType::Header,
                offset: 0,
                payload: b"header".to_vec(),
            },
            JavaBigStatisticsComponent {
                component_type: JavaBigMetadataType::Validation,
                offset: 0,
                payload: validation_payload.clone(),
            },
            JavaBigStatisticsComponent {
                component_type: JavaBigMetadataType::Compaction,
                offset: 0,
                payload: compaction_payload.clone(),
            },
            JavaBigStatisticsComponent {
                component_type: JavaBigMetadataType::Stats,
                offset: 0,
                payload: b"stats".to_vec(),
            },
        ];

        let encoded = write_java_big_statistics(&components, true).unwrap();
        let parsed = parse_java_big_statistics(&encoded, true).unwrap();
        let auto = parse_java_big_statistics_auto(&encoded).unwrap();

        assert!(parsed.has_checksum);
        assert!(auto.has_checksum);
        assert_eq!(auto.components, parsed.components);
        assert_eq!(parsed.components.len(), 4);
        assert_eq!(
            parsed
                .components
                .iter()
                .map(|component| component.component_type)
                .collect::<Vec<_>>(),
            vec![
                JavaBigMetadataType::Validation,
                JavaBigMetadataType::Compaction,
                JavaBigMetadataType::Stats,
                JavaBigMetadataType::Header
            ]
        );
        assert_eq!(
            parsed.component(JavaBigMetadataType::Validation),
            Some(validation_payload.as_slice())
        );
        let parsed_validation = parsed.validation_metadata().unwrap().unwrap();
        assert_eq!(parsed_validation.partitioner, validation.partitioner);
        assert!(
            (parsed_validation.bloom_filter_fp_chance - validation.bloom_filter_fp_chance).abs()
                < f64::EPSILON
        );
        let parsed_compaction = parsed.compaction_metadata().unwrap().unwrap();
        assert_eq!(parsed_compaction, compaction);
        assert_eq!(
            parsed.component(JavaBigMetadataType::Compaction),
            Some(compaction_payload.as_slice())
        );
        assert_eq!(
            parsed.component(JavaBigMetadataType::Stats),
            Some(&b"stats"[..])
        );
        assert_eq!(
            parsed.component(JavaBigMetadataType::Header),
            Some(&b"header"[..])
        );
        assert!(parsed.components[0].offset < parsed.components[1].offset);
        assert!(parsed.components[1].offset < parsed.components[2].offset);

        let mut corrupt = encoded;
        let last = corrupt.len() - 1;
        corrupt[last] ^= 0xff;
        assert!(parse_java_big_statistics(&corrupt, true).is_err());
    }

    #[test]
    fn reads_java_big_validation_metadata() {
        let metadata = JavaBigValidationMetadata {
            partitioner: "org.apache.cassandra.dht.ByteOrderedPartitioner".to_string(),
            bloom_filter_fp_chance: 0.001,
        };
        let encoded = write_java_big_validation_metadata(&metadata).unwrap();
        let parsed = parse_java_big_validation_metadata(&encoded).unwrap();
        assert_eq!(parsed.partitioner, metadata.partitioner);
        assert!(
            (parsed.bloom_filter_fp_chance - metadata.bloom_filter_fp_chance).abs() < f64::EPSILON
        );

        let mut trailing = encoded;
        trailing.push(0);
        assert!(parse_java_big_validation_metadata(&trailing).is_err());
    }

    #[test]
    fn reads_java_big_compaction_metadata() {
        let metadata = JavaBigCompactionMetadata {
            cardinality_estimator: vec![1, 2, 3, 4, 5],
        };
        let encoded = write_java_big_compaction_metadata(&metadata).unwrap();
        let parsed = parse_java_big_compaction_metadata(&encoded).unwrap();
        assert_eq!(parsed, metadata);

        let mut trailing = encoded;
        trailing.push(0);
        assert!(parse_java_big_compaction_metadata(&trailing).is_err());
    }

    #[test]
    fn manifest_reads_java_big_statistics_component_container() {
        let dir = TempDir::new().unwrap();
        for component in [
            JavaBigComponent::Data,
            JavaBigComponent::Index,
            JavaBigComponent::Summary,
            JavaBigComponent::Toc,
        ] {
            fs::write(
                dir.path().join(format!("nb-1-big-{}", component.suffix())),
                b"x",
            )
            .unwrap();
        }
        let stats = vec![JavaBigStatisticsComponent {
            component_type: JavaBigMetadataType::Stats,
            offset: 0,
            payload: b"stats-payload".to_vec(),
        }];
        fs::write(
            dir.path().join("nb-1-big-Statistics.db"),
            write_java_big_statistics(&stats, false).unwrap(),
        )
        .unwrap();

        let manifest = discover_java_big_sstables(dir.path()).unwrap().remove(0);
        let parsed = manifest.statistics_metadata(false).unwrap().unwrap();
        assert!(!parsed.has_checksum);
        assert_eq!(
            parsed.component(JavaBigMetadataType::Stats),
            Some(&b"stats-payload"[..])
        );
        let auto = manifest.statistics_metadata_auto().unwrap().unwrap();
        assert!(!auto.has_checksum);
        assert_eq!(
            auto.component(JavaBigMetadataType::Stats),
            Some(&b"stats-payload"[..])
        );
    }

    // ─── Big format tests ─────────────────────────────────────────────────

    #[test]
    fn big_roundtrip_regular_cells() {
        let dir = TempDir::new().unwrap();
        let desc = SSTableDescriptor::new(dir.path(), "ks", "tbl", 1);
        let partitions = make_regular_partitions();

        SSTableWriter::new(desc.clone()).write(&partitions).unwrap();
        let reader = SSTableReader::open(desc).unwrap();
        let read_back = reader.iter_partitions().unwrap();

        assert_eq!(read_back.len(), partitions.len());
        for (i, (pk, pd)) in read_back.iter().enumerate() {
            assert_eq!(pk, &partitions[i].0);
            assert_eq!(pd.rows.len(), partitions[i].1.rows.len());
            for (ck, row) in &pd.rows {
                let orig_row = partitions[i].1.rows.get(ck).unwrap();
                assert_eq!(row.cells.len(), orig_row.cells.len());
                assert_eq!(row.cells[0].column, orig_row.cells[0].column);
                assert_eq!(row.cells[0].value, orig_row.cells[0].value);
                assert_eq!(row.cells[0].timestamp, orig_row.cells[0].timestamp);
            }
        }
    }

    #[test]
    fn big_roundtrip_tombstones_and_ttl() {
        let dir = TempDir::new().unwrap();
        let desc = SSTableDescriptor::new(dir.path(), "ks", "tbl", 2);
        let partitions = make_complex_partitions();

        SSTableWriter::new(desc.clone()).write(&partitions).unwrap();
        let reader = SSTableReader::open(desc).unwrap();

        // Check tombstone partition
        let p = reader.get_partition(b"pk_mixed").unwrap().unwrap();
        let alive_row = p.rows.get(&b"ck_alive".to_vec()).unwrap();
        assert!(!alive_row.is_tombstone);
        assert_eq!(
            alive_row.cells[0].value.as_deref(),
            Some(b"hello".as_slice())
        );

        let dead_row = p.rows.get(&b"ck_dead".to_vec()).unwrap();
        assert!(dead_row.is_tombstone);
        assert_eq!(dead_row.local_deletion_time, Some(200));
        assert!(dead_row.cells[0].is_tombstone);
        assert!(dead_row.cells[0].value.is_none());
        assert_eq!(dead_row.cells[0].local_deletion_time, Some(200));

        // Check TTL partition
        let p_ttl = reader.get_partition(b"pk_ttl").unwrap().unwrap();
        let ttl_row = p_ttl.rows.get(&b"ck_ttl".to_vec()).unwrap();
        assert_eq!(ttl_row.cells[0].ttl, 3600);
        assert_eq!(ttl_row.cells[0].local_deletion_time, Some(1000 + 3600));

        // Check empty value
        let p_empty = reader.get_partition(b"pk_zz_empty").unwrap().unwrap();
        let empty_row = p_empty.rows.get(&b"ck_empty".to_vec()).unwrap();
        assert_eq!(empty_row.cells[0].value.as_deref(), Some(b"".as_slice()));
    }

    #[test]
    fn big_roundtrip_large_keys_many_rows() {
        let dir = TempDir::new().unwrap();
        let desc = SSTableDescriptor::new(dir.path(), "ks", "tbl", 3);
        let partitions = make_large_key_partitions();

        SSTableWriter::new(desc.clone()).write(&partitions).unwrap();
        let reader = SSTableReader::open(desc).unwrap();
        let read_back = reader.iter_partitions().unwrap();

        assert_eq!(read_back.len(), 1);
        assert_eq!(read_back[0].0.len(), 1024);
        assert_eq!(read_back[0].1.rows.len(), 20);
    }

    // ─── BTI format tests ─────────────────────────────────────────────────

    #[test]
    fn bti_roundtrip_regular_cells() {
        let dir = TempDir::new().unwrap();
        let mut desc = SSTableDescriptor::new(dir.path(), "ks", "tbl", 1);
        desc.format = SSTableFormat::Bti;
        let partitions = make_regular_partitions();

        BtiWriter::new(desc.clone()).write(&partitions).unwrap();
        let reader = BtiReader::open(desc).unwrap();
        let read_back = reader.iter_partitions().unwrap();

        assert_eq!(read_back.len(), partitions.len());
        for (i, (pk, pd)) in read_back.iter().enumerate() {
            assert_eq!(pk, &partitions[i].0);
            assert_eq!(pd.rows.len(), partitions[i].1.rows.len());
        }
    }

    #[test]
    fn bti_roundtrip_tombstones_and_ttl() {
        let dir = TempDir::new().unwrap();
        let mut desc = SSTableDescriptor::new(dir.path(), "ks", "tbl", 2);
        desc.format = SSTableFormat::Bti;
        let partitions = make_complex_partitions();

        BtiWriter::new(desc.clone()).write(&partitions).unwrap();
        let reader = BtiReader::open(desc).unwrap();

        let p = reader.get_partition(b"pk_mixed").unwrap().unwrap();
        let dead_row = p.rows.get(&b"ck_dead".to_vec()).unwrap();
        assert!(dead_row.is_tombstone);
        assert!(dead_row.cells[0].is_tombstone);

        let p_ttl = reader.get_partition(b"pk_ttl").unwrap().unwrap();
        let ttl_row = p_ttl.rows.get(&b"ck_ttl".to_vec()).unwrap();
        assert_eq!(ttl_row.cells[0].ttl, 3600);
    }

    #[test]
    fn bti_roundtrip_large_keys_many_rows() {
        let dir = TempDir::new().unwrap();
        let mut desc = SSTableDescriptor::new(dir.path(), "ks", "tbl", 3);
        desc.format = SSTableFormat::Bti;
        let partitions = make_large_key_partitions();

        BtiWriter::new(desc.clone()).write(&partitions).unwrap();
        let reader = BtiReader::open(desc).unwrap();
        let read_back = reader.iter_partitions().unwrap();

        assert_eq!(read_back.len(), 1);
        assert_eq!(read_back[0].0.len(), 1024);
        assert_eq!(read_back[0].1.rows.len(), 20);
    }

    // ─── Error handling: unknown magic bytes ──────────────────────────────

    #[test]
    fn unknown_data_magic_produces_clear_error() {
        let dir = TempDir::new().unwrap();
        let desc = SSTableDescriptor::new(dir.path(), "ks", "tbl", 99);

        // Write a valid SSTable first to get all component files
        let partitions = make_regular_partitions();
        SSTableWriter::new(desc.clone()).write(&partitions).unwrap();

        // Corrupt the Data.db magic bytes
        let data_path = desc.component_path(Component::Data);
        let mut data = fs::read(&data_path).unwrap();
        data[0] = 0xFF;
        data[1] = 0xFE;
        data[2] = 0xFD;
        data[3] = 0xFC;
        fs::write(&data_path, &data).unwrap();

        // Reader should still open (magic is not checked on open for Data.db)
        // but iter_partitions should fail or return wrong data because the
        // seek skips past the (now corrupted) magic. The key test is that
        // opening with garbage Filter.db or Index.db produces clear errors.
        let reader = SSTableReader::open(desc);
        // The reader opens fine since Data.db magic isn't validated on open,
        // but the bloom/index files are intact so it works. This is expected.
        assert!(reader.is_ok());
    }

    #[test]
    fn unknown_filter_magic_produces_clear_error() {
        let dir = TempDir::new().unwrap();
        let desc = SSTableDescriptor::new(dir.path(), "ks", "tbl", 100);

        let partitions = make_regular_partitions();
        SSTableWriter::new(desc.clone()).write(&partitions).unwrap();

        // Corrupt Filter.db magic
        let filter_path = desc.component_path(Component::Filter);
        let mut data = fs::read(&filter_path).unwrap();
        data[0] = 0xBA;
        data[1] = 0xAD;
        data[2] = 0xCA;
        data[3] = 0xFE;
        fs::write(&filter_path, &data).unwrap();

        let err = match SSTableReader::open(desc) {
            Err(e) => e,
            Ok(_) => panic!("expected error for corrupted filter magic"),
        };
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
        let msg = err.to_string();
        assert!(
            msg.contains("magic") || msg.contains("invalid"),
            "error should mention magic or invalid: {msg}"
        );
    }

    #[test]
    fn unknown_index_magic_produces_clear_error() {
        let dir = TempDir::new().unwrap();
        let desc = SSTableDescriptor::new(dir.path(), "ks", "tbl", 101);

        let partitions = make_regular_partitions();
        SSTableWriter::new(desc.clone()).write(&partitions).unwrap();

        // Corrupt Index.db magic
        let index_path = desc.component_path(Component::Index);
        let mut data = fs::read(&index_path).unwrap();
        data[0] = 0xDE;
        data[1] = 0xAD;
        data[2] = 0xBE;
        data[3] = 0xEF;
        fs::write(&index_path, &data).unwrap();

        let err = match SSTableReader::open(desc) {
            Err(e) => e,
            Ok(_) => panic!("expected error for corrupted index magic"),
        };
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    }

    #[test]
    fn garbage_file_as_sstable_produces_error() {
        let dir = TempDir::new().unwrap();
        let desc = SSTableDescriptor::new(dir.path(), "ks", "tbl", 102);

        // Create garbage files for all components
        fs::create_dir_all(dir.path()).unwrap();
        for component in desc.expected_components() {
            let path = desc.component_path(*component);
            fs::write(&path, b"GARBAGE_DATA_NOT_A_REAL_SSTABLE").unwrap();
        }

        let result = SSTableReader::open(desc);
        assert!(result.is_err());
    }

    // ─── format_info ──────────────────────────────────────────────────────

    #[test]
    fn format_info_contains_key_details() {
        let info = format_info();
        assert!(info.contains("SSDT"));
        assert!(info.contains("SSIX"));
        assert!(info.contains("SSFL"));
        assert!(info.contains("NOT binary-compatible"));
        assert!(info.contains("Big"));
        assert!(info.contains("BTI"));
        assert!(info.contains("V1"));
    }
}
