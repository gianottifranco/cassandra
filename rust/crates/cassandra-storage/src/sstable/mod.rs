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
pub mod format;
pub mod reader;
pub mod writer;

pub use format::{SSTableDescriptor, SSTableFormat, SSTableId};
pub use reader::SSTableReader;
pub use writer::SSTableWriter;
pub use bti::{BtiReader, BtiWriter};
