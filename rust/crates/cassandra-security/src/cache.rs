// Licensed under Apache License, Version 2.0.

//! Generic auth cache with TTL expiration and max entries.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.auth.AuthCache`

use std::hash::Hash;
use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;

type RefreshLoader<K, V> = dyn Fn(&K) -> Option<V> + Send + Sync;

/// Configuration for an auth cache.
#[derive(Debug, Clone)]
pub struct AuthCacheConfig {
    /// Maximum number of entries in the cache.
    pub max_entries: usize,
    /// Time-to-live for cache entries.
    pub validity_period: Duration,
    /// How often to refresh entries in the background.
    pub update_interval: Duration,
}

impl Default for AuthCacheConfig {
    fn default() -> Self {
        Self {
            max_entries: 1000,
            validity_period: Duration::from_secs(2000),
            update_interval: Duration::from_secs(2000),
        }
    }
}

/// A cached entry with its insertion time.
struct CacheEntry<V> {
    value: V,
    inserted_at: Instant,
}

/// Generic cache for auth data with TTL expiration and size limits.
///
/// Thread-safe via DashMap. Entries expire after `validity_period`.
pub struct AuthCache<K, V> {
    entries: DashMap<K, CacheEntry<V>>,
    config: AuthCacheConfig,
    name: String,
}

impl<K, V> AuthCache<K, V>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    pub fn new(name: impl Into<String>, config: AuthCacheConfig) -> Self {
        Self {
            entries: DashMap::with_capacity(config.max_entries),
            config,
            name: name.into(),
        }
    }

    /// Get a cached value, returning None if expired or missing.
    pub fn get(&self, key: &K) -> Option<V> {
        let entry = self.entries.get(key)?;
        if entry.inserted_at.elapsed() > self.config.validity_period {
            drop(entry);
            self.entries.remove(key);
            return None;
        }
        Some(entry.value.clone())
    }

    /// Get a value, loading it via `loader` if not cached or expired.
    pub fn get_or_load<F, E>(&self, key: &K, loader: F) -> Result<V, E>
    where
        F: FnOnce(&K) -> Result<V, E>,
    {
        if let Some(v) = self.get(key) {
            return Ok(v);
        }
        let value = loader(key)?;
        self.put(key.clone(), value.clone());
        Ok(value)
    }

    /// Insert or update a cache entry.
    pub fn put(&self, key: K, value: V) {
        // Evict oldest if at capacity
        if self.entries.len() >= self.config.max_entries {
            self.evict_oldest();
        }
        self.entries.insert(
            key,
            CacheEntry {
                value,
                inserted_at: Instant::now(),
            },
        );
    }

    /// Remove a specific entry (invalidation).
    pub fn invalidate(&self, key: &K) {
        self.entries.remove(key);
    }

    /// Clear all entries.
    pub fn invalidate_all(&self) {
        self.entries.clear();
    }

    /// Remove expired entries.
    pub fn evict_expired(&self) {
        let validity = self.config.validity_period;
        self.entries
            .retain(|_, entry| entry.inserted_at.elapsed() <= validity);
    }

    /// Number of entries currently cached.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    /// Return keys matching a predicate.
    pub fn entries_matching<F>(&self, predicate: F) -> Vec<K>
    where
        F: Fn(&K) -> bool,
    {
        self.entries
            .iter()
            .filter(|e| predicate(e.key()))
            .map(|e| e.key().clone())
            .collect()
    }

    fn evict_oldest(&self) {
        let mut oldest_key = None;
        let mut oldest_time = Instant::now();

        for entry in self.entries.iter() {
            if entry.value().inserted_at < oldest_time {
                oldest_time = entry.value().inserted_at;
                oldest_key = Some(entry.key().clone());
            }
        }

        if let Some(key) = oldest_key {
            self.entries.remove(&key);
        }
    }
}

/// A self-refreshing auth cache that can run background updates.
pub struct RefreshableAuthCache<K, V> {
    cache: Arc<AuthCache<K, V>>,
    loader: Arc<RefreshLoader<K, V>>,
}

impl<K, V> RefreshableAuthCache<K, V>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync + 'static,
{
    pub fn new(cache: Arc<AuthCache<K, V>>, loader: Arc<RefreshLoader<K, V>>) -> Self {
        Self { cache, loader }
    }

    /// Refresh all entries by re-loading their values.
    pub fn refresh_all(&self) {
        let keys: Vec<K> = self.cache.entries.iter().map(|e| e.key().clone()).collect();

        for key in keys {
            if let Some(value) = (self.loader)(&key) {
                self.cache.put(key, value);
            }
        }
    }

    pub fn cache(&self) -> &Arc<AuthCache<K, V>> {
        &self.cache
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn basic_put_and_get() {
        let cache = AuthCache::new(
            "test",
            AuthCacheConfig {
                max_entries: 100,
                validity_period: Duration::from_secs(60),
                ..Default::default()
            },
        );

        cache.put("key1".to_string(), 42);
        assert_eq!(cache.get(&"key1".to_string()), Some(42));
        assert_eq!(cache.get(&"key2".to_string()), None);
    }

    #[test]
    fn ttl_expiration() {
        let cache = AuthCache::new(
            "test",
            AuthCacheConfig {
                max_entries: 100,
                validity_period: Duration::from_millis(50),
                ..Default::default()
            },
        );

        cache.put("k".to_string(), 1);
        assert_eq!(cache.get(&"k".to_string()), Some(1));

        thread::sleep(Duration::from_millis(60));
        assert_eq!(cache.get(&"k".to_string()), None);
    }

    #[test]
    fn max_entries_eviction() {
        let cache = AuthCache::new(
            "test",
            AuthCacheConfig {
                max_entries: 2,
                validity_period: Duration::from_secs(60),
                ..Default::default()
            },
        );

        cache.put("a".to_string(), 1);
        cache.put("b".to_string(), 2);
        cache.put("c".to_string(), 3);

        assert_eq!(cache.len(), 2);
        // The oldest entry ("a") should have been evicted
        assert_eq!(cache.get(&"c".to_string()), Some(3));
    }

    #[test]
    fn invalidate() {
        let cache = AuthCache::new("test", AuthCacheConfig::default());
        cache.put("x".to_string(), 10);
        cache.invalidate(&"x".to_string());
        assert_eq!(cache.get(&"x".to_string()), None);
    }

    #[test]
    fn invalidate_all() {
        let cache = AuthCache::new("test", AuthCacheConfig::default());
        cache.put("a".to_string(), 1);
        cache.put("b".to_string(), 2);
        cache.invalidate_all();
        assert!(cache.is_empty());
    }

    #[test]
    fn get_or_load() {
        let cache = AuthCache::<String, i32>::new("test", AuthCacheConfig::default());
        let result = cache.get_or_load(&"key".to_string(), |_| Ok::<_, String>(99));
        assert_eq!(result.unwrap(), 99);
        // Should be cached now
        assert_eq!(cache.get(&"key".to_string()), Some(99));
    }

    #[test]
    fn evict_expired() {
        let cache = AuthCache::new(
            "test",
            AuthCacheConfig {
                max_entries: 100,
                validity_period: Duration::from_millis(50),
                ..Default::default()
            },
        );

        cache.put("old".to_string(), 1);
        thread::sleep(Duration::from_millis(60));
        cache.put("new".to_string(), 2);

        cache.evict_expired();
        assert_eq!(cache.len(), 1);
        assert_eq!(cache.get(&"new".to_string()), Some(2));
    }
}
