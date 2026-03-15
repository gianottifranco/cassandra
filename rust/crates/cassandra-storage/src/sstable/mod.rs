// Licensed under Apache License, Version 2.0.

//! SSTable on-disk format implementation.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.io.sstable.format.big.BigFormat`
//! - `org.apache.cassandra.io.sstable.format.SSTableReader`
//! - `org.apache.cassandra.io.sstable.format.SSTableWriter`
//!
//! ## Format
//!
//! Each SSTable consists of multiple component files:
//! - `*-Data.db`     – serialized partitions + rows
//! - `*-Index.db`    – partition key → offset in Data.db
//! - `*-Filter.db`   – Bloom filter for partition keys
//! - `*-Summary.db`  – sampled index for fast positioning
//! - `*-Statistics.db` – table-level stats (row count, min/max timestamps, etc.)
//! - `*-TOC.txt`     – list of component files
//!
//! This is a simplified "big"-compatible format per ADR-004.

pub mod bloom;
pub mod format;
pub mod reader;
pub mod writer;

pub use format::{SSTableDescriptor, SSTableId};
pub use reader::SSTableReader;
pub use writer::SSTableWriter;
