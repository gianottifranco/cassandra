// Licensed under Apache License, Version 2.0.

//! Prepared statement cache with schema-change invalidation.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.cql3.QueryProcessor`
//! - `org.apache.cassandra.cql3.statements.BatchStatement`

use std::sync::atomic::{AtomicU64, Ordering};
use dashmap::DashMap;
use md5::{Md5, Digest};
use crate::ast::Statement;
use crate::parser;

/// A prepared statement ready for execution.
#[derive(Debug, Clone)]
pub struct PreparedStatement {
    /// MD5 hash of the query text (16 bytes), used as the statement ID.
    pub id: [u8; 16],
    /// Original CQL query text.
    pub query: String,
    /// Parsed AST.
    pub statement: Statement,
    /// Schema version at preparation time.
    pub schema_version: u64,
    /// Bind variable count (positional).
    pub bind_count: usize,
    /// MD5 of the result column metadata, for detecting METADATA_CHANGED.
    pub result_metadata_id: Option<[u8; 16]>,
    /// Keyspace context at preparation time.
    pub keyspace: Option<String>,
}

/// Concurrent prepared statement cache.
///
/// Thread-safe: uses `DashMap` for lock-free concurrent reads.
/// Invalidation is version-based: when schema changes, all entries
/// with older schema versions are evicted.
pub struct PreparedCache {
    cache: DashMap<[u8; 16], PreparedStatement>,
    schema_version: AtomicU64,
    /// Cache hit counter for metrics.
    hits: AtomicU64,
    /// Cache miss counter for metrics.
    misses: AtomicU64,
}

impl PreparedCache {
    /// Create a new empty cache.
    pub fn new() -> Self {
        Self {
            cache: DashMap::new(),
            schema_version: AtomicU64::new(0),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    /// Compute the statement ID (MD5 of query text), matching Java behavior.
    pub fn compute_id(query: &str) -> [u8; 16] {
        let mut hasher = Md5::new();
        hasher.update(query.as_bytes());
        let result = hasher.finalize();
        let mut id = [0u8; 16];
        id.copy_from_slice(&result);
        id
    }

    /// Prepare a statement: parse, cache, and return the PreparedStatement.
    pub fn prepare(
        &self,
        query: &str,
        schema_version: u64,
    ) -> Result<PreparedStatement, String> {
        let id = Self::compute_id(query);

        // Check if already cached with current schema version.
        if let Some(existing) = self.cache.get(&id) {
            if existing.schema_version >= schema_version {
                self.hits.fetch_add(1, Ordering::Relaxed);
                return Ok(existing.clone());
            }
        }

        self.misses.fetch_add(1, Ordering::Relaxed);

        // Parse the query.
        let statement = parser::parse(query).map_err(|e| e.to_string())?;

        // Count bind markers.
        let bind_count = count_bind_markers(&statement);

        let prepared = PreparedStatement {
            id,
            query: query.to_string(),
            statement,
            schema_version,
            bind_count,
            result_metadata_id: None,
            keyspace: None,
        };

        self.cache.insert(id, prepared.clone());
        Ok(prepared)
    }

    /// Look up a prepared statement by its ID.
    pub fn get(&self, id: &[u8; 16]) -> Option<PreparedStatement> {
        self.cache.get(id).map(|entry| {
            self.hits.fetch_add(1, Ordering::Relaxed);
            entry.clone()
        })
    }

    /// Invalidate all entries with schema_version < new_version.
    ///
    /// This is called whenever a DDL statement changes the schema.
    pub fn invalidate_for_schema_change(&self, new_version: u64) {
        self.schema_version.store(new_version, Ordering::Release);
        self.cache.retain(|_, v| v.schema_version >= new_version);
        tracing::info!(
            new_version,
            remaining = self.cache.len(),
            "prepared cache invalidated for schema change"
        );
    }

    /// Number of cached statements.
    pub fn len(&self) -> usize {
        self.cache.len()
    }

    /// Whether the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.cache.is_empty()
    }

    /// Clear the entire cache.
    pub fn clear(&self) {
        self.cache.clear();
    }

    /// Get cache hit count.
    pub fn hit_count(&self) -> u64 {
        self.hits.load(Ordering::Relaxed)
    }

    /// Get cache miss count.
    pub fn miss_count(&self) -> u64 {
        self.misses.load(Ordering::Relaxed)
    }
}

impl Default for PreparedCache {
    fn default() -> Self {
        Self::new()
    }
}

/// Count positional bind markers in a statement.
fn count_bind_markers(stmt: &Statement) -> usize {
    use crate::ast::*;
    let mut count = 0;

    fn count_in_term(t: &Term, count: &mut usize) {
        match t {
            Term::BindMarker(_) => *count += 1,
            Term::FunctionCall(_, args) => {
                for a in args {
                    count_in_term(a, count);
                }
            }
            Term::CollectionLiteral(items) | Term::TupleLiteral(items) => {
                for item in items {
                    count_in_term(item, count);
                }
            }
            Term::MapLiteral(entries) => {
                for (k, v) in entries {
                    count_in_term(k, count);
                    count_in_term(v, count);
                }
            }
            Term::TypeHint(_, inner) => count_in_term(inner, count),
            Term::Literal(_) => {}
        }
    }

    fn count_in_relations(rels: &[Relation], count: &mut usize) {
        for r in rels {
            count_in_term(&r.value, count);
        }
    }

    match stmt {
        Statement::Select(s) => {
            count_in_relations(&s.where_clause, &mut count);
            if let Some(l) = &s.limit {
                count_in_term(l, &mut count);
            }
        }
        Statement::Insert(i) => {
            for v in &i.values {
                count_in_term(v, &mut count);
            }
        }
        Statement::Update(u) => {
            for a in &u.assignments {
                count_in_term(&a.value, &mut count);
            }
            count_in_relations(&u.where_clause, &mut count);
        }
        Statement::Delete(d) => {
            count_in_relations(&d.where_clause, &mut count);
        }
        Statement::Batch(b) => {
            for s in &b.statements {
                count += count_bind_markers(s);
            }
        }
        _ => {}
    }

    count
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compute_id_deterministic() {
        let id1 = PreparedCache::compute_id("SELECT * FROM users");
        let id2 = PreparedCache::compute_id("SELECT * FROM users");
        assert_eq!(id1, id2);
    }

    #[test]
    fn compute_id_different() {
        let id1 = PreparedCache::compute_id("SELECT * FROM users");
        let id2 = PreparedCache::compute_id("SELECT * FROM orders");
        assert_ne!(id1, id2);
    }

    #[test]
    fn prepare_and_get() {
        let cache = PreparedCache::new();
        let prepared = cache.prepare("SELECT * FROM t WHERE id = ?", 1).unwrap();
        assert_eq!(prepared.bind_count, 1);

        let got = cache.get(&prepared.id).unwrap();
        assert_eq!(got.query, "SELECT * FROM t WHERE id = ?");
    }

    #[test]
    fn prepare_cache_hit() {
        let cache = PreparedCache::new();
        cache.prepare("SELECT * FROM t1", 1).unwrap();
        cache.prepare("SELECT * FROM t1", 1).unwrap(); // Should be cache hit.
        assert_eq!(cache.hit_count(), 1);
        assert_eq!(cache.miss_count(), 1);
    }

    #[test]
    fn invalidate_evicts_old() {
        let cache = PreparedCache::new();
        cache.prepare("SELECT * FROM t1", 1).unwrap();
        cache.prepare("SELECT * FROM t2", 2).unwrap();
        assert_eq!(cache.len(), 2);

        cache.invalidate_for_schema_change(2);
        assert_eq!(cache.len(), 1); // v1 entry evicted.
    }

    #[test]
    fn invalidate_all() {
        let cache = PreparedCache::new();
        cache.prepare("SELECT * FROM t1", 1).unwrap();
        cache.prepare("SELECT * FROM t2", 2).unwrap();

        cache.invalidate_for_schema_change(100);
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn bind_marker_count() {
        let cache = PreparedCache::new();

        let p = cache.prepare("INSERT INTO t (a, b, c) VALUES (?, ?, ?)", 1).unwrap();
        assert_eq!(p.bind_count, 3);

        let p = cache.prepare("SELECT * FROM t", 1).unwrap();
        assert_eq!(p.bind_count, 0);
    }

    #[test]
    fn concurrent_access() {
        use std::sync::Arc;
        let cache = Arc::new(PreparedCache::new());
        let mut handles = Vec::new();

        for i in 0..10 {
            let cache = Arc::clone(&cache);
            handles.push(std::thread::spawn(move || {
                let query = format!("SELECT * FROM t{} WHERE id = ?", i);
                cache.prepare(&query, 1).unwrap();
            }));
        }

        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(cache.len(), 10);
    }

    #[test]
    fn clear() {
        let cache = PreparedCache::new();
        cache.prepare("SELECT * FROM t1", 1).unwrap();
        cache.clear();
        assert!(cache.is_empty());
    }

    #[test]
    fn schema_version_re_prepare() {
        let cache = PreparedCache::new();
        cache.prepare("SELECT * FROM t1", 1).unwrap();

        // Invalidate old version.
        cache.invalidate_for_schema_change(5);
        assert!(cache.is_empty());

        // Re-prepare with new schema version.
        let p = cache.prepare("SELECT * FROM t1", 5).unwrap();
        assert_eq!(p.schema_version, 5);
        assert_eq!(cache.len(), 1);
    }
}
