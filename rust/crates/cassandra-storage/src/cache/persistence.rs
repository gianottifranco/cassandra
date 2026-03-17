// Licensed under Apache License, Version 2.0.

//! Cache persistence: save/load caches to disk for warm restarts.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.cache.AutoSavingCache`
//!
//! Saves KeyCache and CounterCache as JSON files. Row cache and chunk cache
//! are NOT persisted (too large and/or too transient).

use std::fs;
use std::io;
use std::path::Path;

use serde::{Deserialize, Serialize};

use super::counter_cache::{CounterCache, CounterCacheKey};
use crate::sstable::key_cache::{KeyCache, KeyCacheKey};

/// Versioned wrapper for serialized cache files.
#[derive(Serialize, Deserialize)]
struct CacheFile<T> {
    version: u32,
    entry_count: usize,
    entries: Vec<T>,
}

/// Current cache file format version.
const CACHE_VERSION: u32 = 1;

/// Save the key cache to disk.
///
/// Writes a JSON file atomically (write to temp, then rename).
/// Recommended file name: `KeyCache-v1.json`.
pub fn save_key_cache(path: &Path, cache: &KeyCache) -> io::Result<()> {
    let entries = cache.entries();
    let file = CacheFile {
        version: CACHE_VERSION,
        entry_count: entries.len(),
        entries,
    };
    atomic_write_json(path, &file)
}

/// Load key cache entries from disk.
///
/// Returns the number of entries loaded. If the file does not exist,
/// returns `Ok(0)` (not an error — just no cache to load).
pub fn load_key_cache(path: &Path, cache: &KeyCache) -> io::Result<usize> {
    let data = match fs::read_to_string(path) {
        Ok(data) => data,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(0),
        Err(e) => return Err(e),
    };

    let file: CacheFile<(KeyCacheKey, u64)> = serde_json::from_str(&data).map_err(|e| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("corrupt key cache file: {e}"),
        )
    })?;

    if file.version != CACHE_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "unsupported key cache version: expected {CACHE_VERSION}, got {}",
                file.version
            ),
        ));
    }

    let count = file.entries.len();
    cache.load_entries(file.entries);
    Ok(count)
}

/// Save the counter cache to disk.
///
/// Writes a JSON file atomically (write to temp, then rename).
/// Recommended file name: `CounterCache-v1.json`.
pub fn save_counter_cache(path: &Path, cache: &CounterCache) -> io::Result<()> {
    let entries = cache.entries();
    let file = CacheFile {
        version: CACHE_VERSION,
        entry_count: entries.len(),
        entries,
    };
    atomic_write_json(path, &file)
}

/// Load counter cache entries from disk.
///
/// Returns the number of entries loaded. If the file does not exist,
/// returns `Ok(0)`.
pub fn load_counter_cache(path: &Path, cache: &CounterCache) -> io::Result<usize> {
    let data = match fs::read_to_string(path) {
        Ok(data) => data,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(0),
        Err(e) => return Err(e),
    };

    let file: CacheFile<(CounterCacheKey, Vec<u8>)> =
        serde_json::from_str(&data).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("corrupt counter cache file: {e}"),
            )
        })?;

    if file.version != CACHE_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "unsupported counter cache version: expected {CACHE_VERSION}, got {}",
                file.version
            ),
        ));
    }

    let count = file.entries.len();
    cache.load_entries(file.entries);
    Ok(count)
}

/// Write JSON to a temp file then rename for atomicity.
fn atomic_write_json<T: Serialize>(path: &Path, value: &T) -> io::Result<()> {
    let json = serde_json::to_string_pretty(value).map_err(|e| {
        io::Error::new(
            io::ErrorKind::Other,
            format!("failed to serialize cache: {e}"),
        )
    })?;

    let tmp_path = path.with_extension("tmp");
    fs::write(&tmp_path, json.as_bytes())?;
    fs::rename(&tmp_path, path)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::counter_cache::CounterCacheConfig;
    use crate::sstable::key_cache::KeyCacheConfig;

    #[test]
    fn key_cache_save_load_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("KeyCache-v1.json");

        let cache = KeyCache::new(KeyCacheConfig { max_entries: 100 });
        cache.put(1, b"pk1".to_vec(), 1000);
        cache.put(2, b"pk2".to_vec(), 2000);
        cache.put(1, b"pk3".to_vec(), 3000);

        save_key_cache(&path, &cache).unwrap();

        let cache2 = KeyCache::new(KeyCacheConfig { max_entries: 100 });
        let loaded = load_key_cache(&path, &cache2).unwrap();
        assert_eq!(loaded, 3);

        assert_eq!(cache2.get(1, b"pk1"), Some(1000));
        assert_eq!(cache2.get(2, b"pk2"), Some(2000));
        assert_eq!(cache2.get(1, b"pk3"), Some(3000));
    }

    #[test]
    fn counter_cache_save_load_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("CounterCache-v1.json");

        let cache = CounterCache::new(CounterCacheConfig { max_entries: 100 });
        cache.put(1, b"pk1".to_vec(), b"col1".to_vec(), b"val1".to_vec());
        cache.put(2, b"pk2".to_vec(), b"col2".to_vec(), b"val2".to_vec());

        save_counter_cache(&path, &cache).unwrap();

        let cache2 = CounterCache::new(CounterCacheConfig { max_entries: 100 });
        let loaded = load_counter_cache(&path, &cache2).unwrap();
        assert_eq!(loaded, 2);

        assert_eq!(cache2.get(1, b"pk1", b"col1"), Some(b"val1".to_vec()));
        assert_eq!(cache2.get(2, b"pk2", b"col2"), Some(b"val2".to_vec()));
    }

    #[test]
    fn load_missing_file_returns_zero() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nonexistent.json");

        let key_cache = KeyCache::new(KeyCacheConfig::default());
        assert_eq!(load_key_cache(&path, &key_cache).unwrap(), 0);

        let counter_cache = CounterCache::new(CounterCacheConfig::default());
        assert_eq!(load_counter_cache(&path, &counter_cache).unwrap(), 0);
    }

    #[test]
    fn load_corrupt_file_returns_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("corrupt.json");
        fs::write(&path, b"not valid json {{{}}}").unwrap();

        let key_cache = KeyCache::new(KeyCacheConfig::default());
        let result = load_key_cache(&path, &key_cache);
        assert!(result.is_err());

        let counter_cache = CounterCache::new(CounterCacheConfig::default());
        let result = load_counter_cache(&path, &counter_cache);
        assert!(result.is_err());
    }

    #[test]
    fn save_creates_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("KeyCache-v1.json");

        let cache = KeyCache::new(KeyCacheConfig { max_entries: 100 });
        cache.put(1, b"pk".to_vec(), 42);

        save_key_cache(&path, &cache).unwrap();

        assert!(path.exists());
        let data = fs::read_to_string(&path).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&data).unwrap();
        assert_eq!(parsed["version"], 1);
        assert_eq!(parsed["entry_count"], 1);
    }
}
