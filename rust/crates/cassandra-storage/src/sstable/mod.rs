// Licensed under Apache License, Version 2.0.

//! SSTable on-disk format implementation.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.io.sstable.format.big.BigFormat`
//! - `org.apache.cassandra.io.sstable.format.bti.BtiFormat`
//! - `org.apache.cassandra.io.sstable.format.SSTableReader`
//! - `org.apache.cassandra.io.sstable.format.SSTableWriter`
//!
//! ## Formats
//!
//! | Format | Module  | Status       | Description                        |
//! |--------|---------|-------------|------------------------------------|
//! | Big    | writer  | Functional  | Partition index + binary search    |
//! | BTI    | bti     | Functional  | Trie-based partition index         |
//!
//! ## Components
//!
//! Each SSTable consists of multiple component files:
//! - `*-Data.db`       – serialized partitions + rows (both formats)
//! - `*-Index.db`      – partition key → offset (Big only)
//! - `*-Partitions.db` – trie-encoded partition index (BTI only)
//! - `*-Filter.db`     – Bloom filter for partition keys
//! - `*-Summary.db`    – sampled index for fast positioning (Big only)
//! - `*-Statistics.db` – table-level stats
//! - `*-TOC.txt`       – list of component files

pub mod bloom;
pub mod bti;
pub mod column_index;
pub mod compat;
pub mod filtered_scanner;
pub mod format;
pub mod key_cache;
pub mod metadata;
pub mod reader;
pub mod reverse_scanner;
pub mod rewriter;
pub mod scanner;
pub mod scrubber;
pub mod summary;
pub mod tombstone_serializer;
pub mod tracker;
pub mod upgrader;
pub mod verifier;
pub mod version;
pub mod writer;

pub use bti::{BtiReader, BtiWriter};
pub use format::{SSTableDescriptor, SSTableFormat, SSTableId};
pub use reader::SSTableReader;
pub use writer::SSTableWriter;
