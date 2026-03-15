// Licensed under Apache License, Version 2.0.

//! SSTable format constants, descriptor, and file naming.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.io.sstable.Descriptor`
//! - `org.apache.cassandra.io.sstable.Component`

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
        }
    }

    pub fn all() -> &'static [Component] {
        &[
            Component::Data,
            Component::Index,
            Component::Filter,
            Component::Summary,
            Component::Statistics,
            Component::Toc,
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
    /// Simplified big-format compatible.
    Big,
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

    /// File prefix: `{keyspace}-{table}-big-{generation}`
    pub fn file_prefix(&self) -> String {
        format!(
            "{}-{}-big-{}",
            self.keyspace, self.table, self.generation
        )
    }

    /// Complete path for a component file.
    pub fn component_path(&self, component: Component) -> PathBuf {
        self.directory
            .join(format!("{}-{}", self.file_prefix(), component.extension()))
    }

    /// Check if all component files exist.
    pub fn is_complete(&self) -> bool {
        Component::all()
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
    use std::path::Path;

    #[test]
    fn descriptor_paths() {
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
    fn all_components() {
        assert_eq!(Component::all().len(), 6);
    }
}
