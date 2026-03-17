// Licensed under Apache License, Version 2.0.

//! Cache subsystem: KeyCache, RowCache, CounterCache, ChunkCache.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.cache.ICache`
//! - `org.apache.cassandra.cache.AutoSavingCache`

pub mod chunk_cache;
pub mod counter_cache;
pub mod invalidation;
pub mod persistence;
pub mod row_cache;
pub mod service;
pub mod traits;

pub use invalidation::CacheInvalidationListener;
pub use service::CacheService;
pub use traits::{CacheStats, ICache};
