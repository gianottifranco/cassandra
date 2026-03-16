// Licensed under Apache License, Version 2.0.

//! In-memory trie implementation with cursor-based iteration.
//!
//! ## Java Oracle
//! `org.apache.cassandra.db.tries.InMemoryTrie`

use std::collections::BTreeMap;

use super::trie::{Cursor, Trie};

/// A node in the in-memory trie.
#[derive(Debug, Clone)]
struct TrieNode<T> {
    /// Content at this node (if this is a terminal for some key).
    content: Option<T>,
    /// Children keyed by the next byte. Uses BTreeMap for sorted iteration.
    /// For nodes with very few children, this is still efficient enough;
    /// Java uses inline arrays for small children counts, but BTreeMap
    /// keeps the code simple and provides sorted order naturally.
    children: BTreeMap<u8, TrieNode<T>>,
}

impl<T> TrieNode<T> {
    fn new() -> Self {
        Self {
            content: None,
            children: BTreeMap::new(),
        }
    }
}

impl<T> Default for TrieNode<T> {
    fn default() -> Self {
        Self::new()
    }
}

/// In-memory trie mapping byte-sequence keys to values.
///
/// Memory-efficient for keys with shared prefixes (like partition keys
/// in the same token range). Uses BTreeMap children for sorted iteration.
#[derive(Debug, Clone)]
pub struct InMemoryTrie<T> {
    root: TrieNode<T>,
    entry_count: usize,
}

impl<T> InMemoryTrie<T> {
    pub fn new() -> Self {
        Self {
            root: TrieNode::new(),
            entry_count: 0,
        }
    }

    /// Create a cursor for depth-first traversal.
    pub fn cursor(&self) -> InMemoryTrieCursor<'_, T> {
        InMemoryTrieCursor::new(&self.root)
    }

    /// Iterate all entries in sorted key order.
    pub fn iter(&self) -> Vec<(Vec<u8>, &T)> {
        let mut results = Vec::new();
        let mut key = Vec::new();
        Self::collect_entries(&self.root, &mut key, &mut results);
        results
    }

    fn collect_entries<'a>(
        node: &'a TrieNode<T>,
        key: &mut Vec<u8>,
        results: &mut Vec<(Vec<u8>, &'a T)>,
    ) {
        if let Some(ref content) = node.content {
            results.push((key.clone(), content));
        }
        for (&byte, child) in &node.children {
            key.push(byte);
            Self::collect_entries(child, key, results);
            key.pop();
        }
    }
}

impl<T> Default for InMemoryTrie<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Trie<T> for InMemoryTrie<T> {
    fn get(&self, key: &[u8]) -> Option<&T> {
        let mut node = &self.root;
        for &byte in key {
            match node.children.get(&byte) {
                Some(child) => node = child,
                None => return None,
            }
        }
        node.content.as_ref()
    }

    fn apply<F>(&mut self, key: &[u8], merge_fn: F)
    where
        F: FnOnce(Option<&T>) -> T,
    {
        let mut node = &mut self.root;
        for &byte in key {
            node = node.children.entry(byte).or_default();
        }
        let new_value = merge_fn(node.content.as_ref());
        if node.content.is_none() {
            self.entry_count += 1;
        }
        node.content = Some(new_value);
    }

    fn entry_count(&self) -> usize {
        self.entry_count
    }
}

/// Cursor-based depth-first traversal of an `InMemoryTrie`.
pub struct InMemoryTrieCursor<'a, T> {
    /// Stack of (node, child_iterator_state).
    /// Each entry is (node_ref, remaining children as a peekable iterator, incoming_byte).
    stack: Vec<CursorFrame<'a, T>>,
    started: bool,
}

struct CursorFrame<'a, T> {
    node: &'a TrieNode<T>,
    children_iter: std::collections::btree_map::Iter<'a, u8, TrieNode<T>>,
    incoming_byte: Option<u8>,
}

impl<'a, T> InMemoryTrieCursor<'a, T> {
    fn new(root: &'a TrieNode<T>) -> Self {
        Self {
            stack: vec![CursorFrame {
                node: root,
                children_iter: root.children.iter(),
                incoming_byte: None,
            }],
            started: false,
        }
    }
}

impl<'a, T> Cursor<T> for InMemoryTrieCursor<'a, T> {
    fn advance(&mut self) -> bool {
        if !self.started {
            self.started = true;
            // Start at root
            return !self.stack.is_empty();
        }

        // Try to go deeper (visit first child of current node)
        if let Some(frame) = self.stack.last_mut() {
            if let Some((&byte, child)) = frame.children_iter.next() {
                let new_frame = CursorFrame {
                    node: child,
                    children_iter: child.children.iter(),
                    incoming_byte: Some(byte),
                };
                self.stack.push(new_frame);
                return true;
            }
        }

        // No more children at current level; backtrack
        loop {
            self.stack.pop();
            if let Some(frame) = self.stack.last_mut() {
                if let Some((&byte, child)) = frame.children_iter.next() {
                    let new_frame = CursorFrame {
                        node: child,
                        children_iter: child.children.iter(),
                        incoming_byte: Some(byte),
                    };
                    self.stack.push(new_frame);
                    return true;
                }
            } else {
                return false; // Traversal complete
            }
        }
    }

    fn depth(&self) -> usize {
        if self.stack.is_empty() {
            0
        } else {
            self.stack.len() - 1
        }
    }

    fn incoming_byte(&self) -> Option<u8> {
        self.stack.last().and_then(|f| f.incoming_byte)
    }

    fn content(&self) -> Option<&T> {
        self.stack
            .last()
            .and_then(|f| f.node.content.as_ref())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_trie() {
        let trie: InMemoryTrie<i32> = InMemoryTrie::new();
        assert!(trie.is_empty());
        assert_eq!(trie.entry_count(), 0);
        assert!(trie.get(b"key").is_none());
    }

    #[test]
    fn insert_and_get() {
        let mut trie = InMemoryTrie::new();
        trie.apply(b"hello", |_| 42);
        trie.apply(b"world", |_| 99);

        assert_eq!(trie.get(b"hello"), Some(&42));
        assert_eq!(trie.get(b"world"), Some(&99));
        assert_eq!(trie.get(b"hell"), None);
        assert_eq!(trie.entry_count(), 2);
    }

    #[test]
    fn merge_existing() {
        let mut trie = InMemoryTrie::new();
        trie.apply(b"key", |_| 10);
        trie.apply(b"key", |existing| existing.unwrap() + 5);
        assert_eq!(trie.get(b"key"), Some(&15));
        assert_eq!(trie.entry_count(), 1); // no double-count
    }

    #[test]
    fn prefix_sharing() {
        let mut trie = InMemoryTrie::new();
        trie.apply(b"abc", |_| 1);
        trie.apply(b"abd", |_| 2);
        trie.apply(b"xyz", |_| 3);

        assert_eq!(trie.get(b"abc"), Some(&1));
        assert_eq!(trie.get(b"abd"), Some(&2));
        assert_eq!(trie.get(b"xyz"), Some(&3));
        assert_eq!(trie.get(b"ab"), None);
    }

    #[test]
    fn iter_sorted() {
        let mut trie = InMemoryTrie::new();
        trie.apply(b"c", |_| 3);
        trie.apply(b"a", |_| 1);
        trie.apply(b"b", |_| 2);

        let entries: Vec<_> = trie.iter();
        let keys: Vec<_> = entries.iter().map(|(k, _)| k.clone()).collect();
        assert_eq!(keys, vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
    }

    #[test]
    fn cursor_traversal() {
        let mut trie = InMemoryTrie::new();
        trie.apply(b"ab", |_| 1);
        trie.apply(b"ac", |_| 2);

        let mut cursor = trie.cursor();
        let mut contents = Vec::new();

        while cursor.advance() {
            if let Some(val) = cursor.content() {
                contents.push(*val);
            }
        }

        assert_eq!(contents, vec![1, 2]);
    }

    #[test]
    fn empty_key() {
        let mut trie = InMemoryTrie::new();
        trie.apply(b"", |_| 42);
        assert_eq!(trie.get(b""), Some(&42));
        assert_eq!(trie.entry_count(), 1);
    }

    #[test]
    fn long_key() {
        let mut trie = InMemoryTrie::new();
        let long_key = vec![0u8; 1000];
        trie.apply(&long_key, |_| 99);
        assert_eq!(trie.get(&long_key), Some(&99));
    }
}
