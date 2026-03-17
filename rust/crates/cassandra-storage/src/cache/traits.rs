// Licensed under Apache License, Version 2.0.

//! Generic cache trait and statistics.
//!
//! Java Oracle: `org.apache.cassandra.cache.ICache`

use serde::{Deserialize, Serialize};

/// Statistics for a cache instance.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CacheStats {
    pub hits: u64,
    pub misses: u64,
    pub size: usize,
    pub evictions: u64,
}

impl CacheStats {
    /// Returns the hit rate as a fraction in `[0.0, 1.0]`.
    ///
    /// Returns `0.0` when there have been no requests (both hits and misses are zero).
    pub fn hit_rate(&self) -> f64 {
        let total = self.hits + self.misses;
        if total == 0 {
            0.0
        } else {
            self.hits as f64 / total as f64
        }
    }
}

/// Generic cache interface mirroring `org.apache.cassandra.cache.ICache`.
pub trait ICache<K, V>: Send + Sync {
    /// Look up a value by key.
    fn get(&self, key: &K) -> Option<V>;

    /// Insert or update a key-value pair.
    fn put(&self, key: K, value: V);

    /// Remove a single entry.
    fn invalidate(&self, key: &K);

    /// Remove all entries.
    fn clear(&self);

    /// Return a snapshot of cache statistics.
    fn stats(&self) -> CacheStats;

    /// Return the number of entries currently in the cache.
    fn size(&self) -> usize;

    /// Return the name of this cache instance.
    fn name(&self) -> &str;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache_stats_default_values() {
        let stats = CacheStats::default();
        assert_eq!(stats.hits, 0);
        assert_eq!(stats.misses, 0);
        assert_eq!(stats.size, 0);
        assert_eq!(stats.evictions, 0);
    }

    #[test]
    fn test_cache_stats_hit_rate_all_hits() {
        let stats = CacheStats {
            hits: 100,
            misses: 0,
            size: 50,
            evictions: 0,
        };
        assert!((stats.hit_rate() - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_cache_stats_hit_rate_fifty_fifty() {
        let stats = CacheStats {
            hits: 50,
            misses: 50,
            size: 25,
            evictions: 0,
        };
        assert!((stats.hit_rate() - 0.5).abs() < f64::EPSILON);
    }

    #[test]
    fn test_cache_stats_hit_rate_no_requests() {
        let stats = CacheStats::default();
        assert!((stats.hit_rate() - 0.0).abs() < f64::EPSILON);
    }
}
