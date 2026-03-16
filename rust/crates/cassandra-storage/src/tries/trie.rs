// Licensed under Apache License, Version 2.0.

//! Trie trait and Cursor interface.
//!
//! ## Java Oracle
//! `org.apache.cassandra.db.tries.Trie`
//! `org.apache.cassandra.db.tries.Trie.Cursor`

/// A cursor for depth-first traversal of a trie.
///
/// The cursor starts before the first entry. Call `advance()` to move
/// to the next node.
pub trait Cursor<T> {
    /// Advance to the next node in depth-first order.
    /// Returns `true` if there is a next node, `false` if traversal is complete.
    fn advance(&mut self) -> bool;

    /// Current depth in the trie (0 = root).
    fn depth(&self) -> usize;

    /// The byte at the current depth that led to this node.
    fn incoming_byte(&self) -> Option<u8>;

    /// Content at the current node, if any.
    fn content(&self) -> Option<&T>;
}

/// A generic trie mapping byte-sequence keys to values.
pub trait Trie<T> {
    /// Get a value by key.
    fn get(&self, key: &[u8]) -> Option<&T>;

    /// Insert or update using a merge function.
    /// Returns the old value if it was replaced.
    fn apply<F>(&mut self, key: &[u8], merge_fn: F)
    where
        F: FnOnce(Option<&T>) -> T;

    /// Number of entries (keys with values).
    fn entry_count(&self) -> usize;

    /// Returns `true` if the trie has no entries.
    fn is_empty(&self) -> bool {
        self.entry_count() == 0
    }
}
