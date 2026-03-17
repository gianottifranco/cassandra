// Licensed under Apache License, Version 2.0.

//! IO utility modules: buffer pools, rebufferers, data input/output,
//! disk optimization, and variable-length integer encoding.

pub mod buffer_pool;
pub mod channel_proxy;
pub mod checksummed_rebufferer;
pub mod checksummed_writer;
pub mod chunk_reader;
pub mod data_input;
pub mod data_integrity;
pub mod data_output;
pub mod disk_optimization;
pub mod file_handle;
pub mod mmap_rebufferer;
pub mod random_access_reader;
pub mod rebufferer;
pub mod sequential_writer;
pub mod sized_ints;
pub mod varint;
