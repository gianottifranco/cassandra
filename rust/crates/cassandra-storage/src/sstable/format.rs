// Licensed under Apache License, Version 2.0.

//! SSTable format constants, descriptor, and file naming.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.io.sstable.Descriptor`
//! - `org.apache.cassandra.io.sstable.Component`
//!
//! ## Formats
//!
//! | Format | Module  | Status       | Description                        |
//! |--------|---------|-------------|------------------------------------|
//! | Big    | writer  | Functional  | Classic partition index + data     |
//! | BTI    | bti     | Functional  | Trie-based partition index + data  |

use std::path::{Path, PathBuf};

/// Unique identifier for an SSTable.
pub type SSTableId = u64;

/// Component types of an SSTable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Component {
    Data,
    Index,
    Filter,
    Summary,
    Statistics,
    Toc,
    /// BTI format: trie-based partition index.
    Partitions,
    /// BTI format: row-level index.
    Rows,
    /// Digest of the data file (MD5/CRC).
    Digest,
    /// Compression info (chunk offsets).
    CompressionInfo,
}

impl Component {
    pub fn extension(&self) -> &'static str {
        match self {
            Component::Data => "Data.db",
            Component::Index => "Index.db",
            Component::Filter => "Filter.db",
            Component::Summary => "Summary.db",
            Component::Statistics => "Statistics.db",
            Component::Toc => "TOC.txt",
            Component::Partitions => "Partitions.db",
            Component::Rows => "Rows.db",
            Component::Digest => "Digest.crc32",
            Component::CompressionInfo => "CompressionInfo.db",
        }
    }

    /// Components for the Big format.
    pub fn big_components() -> &'static [Component] {
        &[
            Component::Data,
            Component::Index,
            Component::Filter,
            Component::Summary,
            Component::Statistics,
            Component::Toc,
        ]
    }

    /// Components for the BTI format.
    pub fn bti_components() -> &'static [Component] {
        &[
            Component::Data,
            Component::Partitions,
            Component::Filter,
            Component::Statistics,
            Component::Toc,
        ]
    }

    /// All possible component types.
    pub fn all() -> &'static [Component] {
        &[
            Component::Data,
            Component::Index,
            Component::Filter,
            Component::Summary,
            Component::Statistics,
            Component::Toc,
            Component::Partitions,
            Component::Rows,
            Component::Digest,
            Component::CompressionInfo,
        ]
    }
}

/// SSTable descriptor: identifies one SSTable on disk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SSTableDescriptor {
    /// Base directory.
    pub directory: PathBuf,
    /// Keyspace name.
    pub keyspace: String,
    /// Table name.
    pub table: String,
    /// Generation number.
    pub generation: SSTableId,
    /// Format identifier.
    pub format: SSTableFormat,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SSTableFormat {
    /// Classic big-format with binary-search partition index.
    Big,
    /// Block-based trie index format.
    Bti,
}

impl Default for SSTableFormat {
    fn default() -> Self {
        Self::Big
    }
}

impl SSTableDescriptor {
    pub fn new(dir: &Path, keyspace: &str, table: &str, generation: SSTableId) -> Self {
        Self {
            directory: dir.to_path_buf(),
            keyspace: keyspace.to_string(),
            table: table.to_string(),
            generation,
            format: SSTableFormat::Big,
        }
    }

    /// File prefix, varies by format.
    /// Big:  `{keyspace}-{table}-big-{generation}`
    /// BTI:  `{keyspace}-{table}-bti-{generation}`
    pub fn file_prefix(&self) -> String {
        let fmt_tag = match self.format {
            SSTableFormat::Big => "big",
            SSTableFormat::Bti => "bti",
        };
        format!("{}-{}-{fmt_tag}-{}", self.keyspace, self.table, self.generation)
    }

    /// Complete path for a component file.
    pub fn component_path(&self, component: Component) -> PathBuf {
        self.directory
            .join(format!("{}-{}", self.file_prefix(), component.extension()))
    }

    /// Components expected for this format.
    pub fn expected_components(&self) -> &'static [Component] {
        match self.format {
            SSTableFormat::Big => Component::big_components(),
            SSTableFormat::Bti => Component::bti_components(),
        }
    }

    /// Check if all expected component files exist.
    pub fn is_complete(&self) -> bool {
        self.expected_components()
            .iter()
            .all(|c| self.component_path(*c).exists())
    }
}

// ─── Data format constants ─────────────────────────────────────────────────

/// Magic bytes at the start of Data.db.
pub const DATA_MAGIC: [u8; 4] = *b"SSDT";
/// Data file format version.
pub const DATA_VERSION: u8 = 1;

/// Magic bytes at the start of Index.db.
pub const INDEX_MAGIC: [u8; 4] = *b"SSIX";

/// Magic bytes at the start of Filter.db.
pub const FILTER_MAGIC: [u8; 4] = *b"SSFL";

/// End-of-partition marker in Data.db.
pub const END_OF_PARTITION: u8 = 0x00;
/// Row marker in Data.db.
pub const ROW_MARKER: u8 = 0x01;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn descriptor_paths_big() {
        let desc = SSTableDescriptor::new(Path::new("/data"), "ks", "t1", 42);
        assert_eq!(
            desc.component_path(Component::Data),
            PathBuf::from("/data/ks-t1-big-42-Data.db")
        );
        assert_eq!(
            desc.component_path(Component::Filter),
            PathBuf::from("/data/ks-t1-big-42-Filter.db")
        );
        assert_eq!(desc.file_prefix(), "ks-t1-big-42");
    }

    #[test]
    fn descriptor_paths_bti() {
        let mut desc = SSTableDescriptor::new(Path::new("/data"), "ks", "t1", 42);
        desc.format = SSTableFormat::Bti;
        assert_eq!(desc.file_prefix(), "ks-t1-bti-42");
        assert_eq!(
            desc.component_path(Component::Partitions),
            PathBuf::from("/data/ks-t1-bti-42-Partitions.db")
        );
    }

    #[test]
    fn big_components_list() {
        assert_eq!(Component::big_components().len(), 6);
    }

    #[test]
    fn bti_components_list() {
        assert_eq!(Component::bti_components().len(), 5);
    }

    #[test]
    fn format_default_is_big() {
        assert_eq!(SSTableFormat::default(), SSTableFormat::Big);
    }
}
