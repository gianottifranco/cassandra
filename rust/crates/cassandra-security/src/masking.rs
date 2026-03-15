// Licensed under Apache License, Version 2.0.

//! Dynamic Data Masking (DDM).
//!
//! ## Java Oracle
//! - `org.apache.cassandra.cql3.functions.masking.*`
//! - Cassandra 5.0+ feature
//!
//! Allows defining masking functions on columns so that unauthorized users
//! see masked values instead of the real data.

use serde::{Deserialize, Serialize};

// ─── MaskingFunction Trait ─────────────────────────────────────────────────

/// A data masking function applied to column values.
pub trait MaskingFunction: Send + Sync {
    /// Apply the mask to a raw CQL value.
    fn mask(&self, value: &[u8]) -> Vec<u8>;

    /// Name of this masking function for configuration/logging.
    fn name(&self) -> &str;
}

// ─── Built-in Masks ────────────────────────────────────────────────────────

/// Returns a default/null value (e.g., empty string, zero).
pub struct DefaultMask;

impl MaskingFunction for DefaultMask {
    fn mask(&self, _value: &[u8]) -> Vec<u8> {
        Vec::new()
    }

    fn name(&self) -> &str {
        "mask_default"
    }
}

/// Replaces all characters with a fixed replacement character.
pub struct ReplaceMask {
    pub replacement: u8,
}

impl Default for ReplaceMask {
    fn default() -> Self {
        Self { replacement: b'*' }
    }
}

impl MaskingFunction for ReplaceMask {
    fn mask(&self, value: &[u8]) -> Vec<u8> {
        vec![self.replacement; value.len()]
    }

    fn name(&self) -> &str {
        "mask_replace"
    }
}

/// Shows the first and last N characters, masks the rest.
pub struct PartialMask {
    pub show_first: usize,
    pub show_last: usize,
    pub mask_char: u8,
}

impl Default for PartialMask {
    fn default() -> Self {
        Self {
            show_first: 0,
            show_last: 4,
            mask_char: b'*',
        }
    }
}

impl MaskingFunction for PartialMask {
    fn mask(&self, value: &[u8]) -> Vec<u8> {
        let len = value.len();
        if len <= self.show_first + self.show_last {
            return vec![self.mask_char; len];
        }

        let mut result = Vec::with_capacity(len);
        for (i, &byte) in value.iter().enumerate() {
            if i < self.show_first || i >= len - self.show_last {
                result.push(byte);
            } else {
                result.push(self.mask_char);
            }
        }
        result
    }

    fn name(&self) -> &str {
        "mask_inner"
    }
}

/// Replaces the value with its SHA-256 hash (in hex bytes).
pub struct HashMask;

impl MaskingFunction for HashMask {
    fn mask(&self, value: &[u8]) -> Vec<u8> {
        // Simple hash using available primitives (not cryptographic-grade for DDM).
        // In production, use a proper SHA-256 from ring or sha2 crate.
        // For now, use a simple FNV-1a style hash as placeholder.
        // TODO: Replace with SHA-256 when sha2 crate is added.
        let mut hash: u64 = 0xcbf29ce484222325;
        for &byte in value {
            hash ^= byte as u64;
            hash = hash.wrapping_mul(0x100000001b3);
        }
        format!("{:016x}", hash).into_bytes()
    }

    fn name(&self) -> &str {
        "mask_hash"
    }
}

/// Returns NULL (empty) for any input.
pub struct NullMask;

impl MaskingFunction for NullMask {
    fn mask(&self, _value: &[u8]) -> Vec<u8> {
        Vec::new()
    }

    fn name(&self) -> &str {
        "mask_null"
    }
}

// ─── Column Masking Policy ─────────────────────────────────────────────────

/// Policy defining which columns in which tables use which masking functions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnMaskingConfig {
    pub keyspace: String,
    pub table: String,
    pub column: String,
    pub function_name: String,
    pub function_args: Vec<String>,
}

/// Registry of masking policies.
pub struct MaskingRegistry {
    policies: Vec<ColumnMaskingConfig>,
}

impl MaskingRegistry {
    pub fn new() -> Self {
        Self {
            policies: Vec::new(),
        }
    }

    pub fn add_policy(&mut self, config: ColumnMaskingConfig) {
        self.policies.push(config);
    }

    pub fn remove_policy(&mut self, keyspace: &str, table: &str, column: &str) {
        self.policies
            .retain(|p| p.keyspace != keyspace || p.table != table || p.column != column);
    }

    /// Get the masking function name for a specific column, if any.
    pub fn get_mask(
        &self,
        keyspace: &str,
        table: &str,
        column: &str,
    ) -> Option<&ColumnMaskingConfig> {
        self.policies
            .iter()
            .find(|p| p.keyspace == keyspace && p.table == table && p.column == column)
    }

    /// Apply masking to a value using the appropriate function.
    pub fn apply_mask(
        &self,
        keyspace: &str,
        table: &str,
        column: &str,
        value: &[u8],
    ) -> Option<Vec<u8>> {
        let config = self.get_mask(keyspace, table, column)?;
        let masked = match config.function_name.as_str() {
            "mask_default" => DefaultMask.mask(value),
            "mask_replace" => ReplaceMask::default().mask(value),
            "mask_inner" => PartialMask::default().mask(value),
            "mask_hash" => HashMask.mask(value),
            "mask_null" => NullMask.mask(value),
            _ => return None, // Unknown function, no masking
        };
        Some(masked)
    }
}

impl Default for MaskingRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_mask_returns_empty() {
        let mask = DefaultMask;
        assert!(mask.mask(b"secret").is_empty());
    }

    #[test]
    fn replace_mask_asterisks() {
        let mask = ReplaceMask::default();
        let result = mask.mask(b"hello");
        assert_eq!(result, b"*****");
    }

    #[test]
    fn replace_mask_custom_char() {
        let mask = ReplaceMask { replacement: b'X' };
        let result = mask.mask(b"ab");
        assert_eq!(result, b"XX");
    }

    #[test]
    fn partial_mask_show_last_4() {
        let mask = PartialMask {
            show_first: 0,
            show_last: 4,
            mask_char: b'*',
        };
        let result = mask.mask(b"4111111111111111");
        let expected = b"************1111";
        assert_eq!(result, expected.to_vec());
    }

    #[test]
    fn partial_mask_show_first_and_last() {
        let mask = PartialMask {
            show_first: 2,
            show_last: 2,
            mask_char: b'#',
        };
        let result = mask.mask(b"ABCDEFGH");
        assert_eq!(result, b"AB####GH".to_vec());
    }

    #[test]
    fn partial_mask_short_value() {
        let mask = PartialMask {
            show_first: 3,
            show_last: 3,
            mask_char: b'*',
        };
        let result = mask.mask(b"AB");
        assert_eq!(result, b"**".to_vec());
    }

    #[test]
    fn hash_mask_deterministic() {
        let mask = HashMask;
        let a = mask.mask(b"test");
        let b = mask.mask(b"test");
        assert_eq!(a, b);
        assert!(!a.is_empty());
    }

    #[test]
    fn hash_mask_different_inputs() {
        let mask = HashMask;
        let a = mask.mask(b"alice");
        let b = mask.mask(b"bob");
        assert_ne!(a, b);
    }

    #[test]
    fn null_mask_returns_empty() {
        let mask = NullMask;
        assert!(mask.mask(b"anything").is_empty());
    }

    #[test]
    fn masking_registry() {
        let mut reg = MaskingRegistry::new();
        reg.add_policy(ColumnMaskingConfig {
            keyspace: "ks".into(),
            table: "users".into(),
            column: "ssn".into(),
            function_name: "mask_inner".into(),
            function_args: vec![],
        });

        assert!(reg.get_mask("ks", "users", "ssn").is_some());
        assert!(reg.get_mask("ks", "users", "name").is_none());

        let masked = reg.apply_mask("ks", "users", "ssn", b"123-45-6789");
        assert!(masked.is_some());
        let masked = masked.unwrap();
        // Last 4 bytes should be preserved
        assert_eq!(&masked[masked.len() - 4..], b"6789");
    }

    #[test]
    fn masking_registry_remove() {
        let mut reg = MaskingRegistry::new();
        reg.add_policy(ColumnMaskingConfig {
            keyspace: "ks".into(),
            table: "t".into(),
            column: "c".into(),
            function_name: "mask_replace".into(),
            function_args: vec![],
        });
        assert!(reg.get_mask("ks", "t", "c").is_some());
        reg.remove_policy("ks", "t", "c");
        assert!(reg.get_mask("ks", "t", "c").is_none());
    }
}
