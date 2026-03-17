// Licensed under Apache License, Version 2.0.

//! IO utilities for Cassandra: compression, mmap/rebufferers, buffer pools,
//! and checksummed readers/writers.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.io`

pub mod compress;
pub mod error;
pub mod util;
