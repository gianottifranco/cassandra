// Licensed under Apache License, Version 2.0.

//! Transparent Data Encryption (TDE) configuration.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.config.TransparentDataEncryptionOptions`
//!
//! Config-only module: defines TDE settings parsed from YAML.
//! Actual encryption logic lives in [`super::crypto`] and [`super::encryption_context`].

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Recognised cipher algorithms for TDE.
pub const RECOGNIZED_CIPHERS: &[&str] = &[
    "AES/CBC/PKCS5Padding",
    "AES/CBC/NoPadding",
    "AES/CTR/NoPadding",
    "AES/GCM/NoPadding",
];

/// Key lengths in bits that are valid for AES.
pub const VALID_KEY_LENGTHS: &[u32] = &[128, 192, 256];

/// TDE configuration loaded from `cassandra.yaml`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransparentDataEncryptionOptions {
    /// Whether TDE is enabled.
    #[serde(default)]
    pub enabled: bool,

    /// Chunk length in KiB for encrypting data blocks.
    #[serde(default = "defaults::chunk_length_kb")]
    pub chunk_length_kb: u32,

    /// Cipher algorithm (e.g. "AES/CBC/PKCS5Padding").
    #[serde(default = "defaults::cipher")]
    pub cipher: String,

    /// Key alias to look up in the key provider.
    #[serde(default)]
    pub key_alias: Option<String>,

    /// Key length in bits (128, 192, or 256).
    #[serde(default = "defaults::key_length")]
    pub key_length: u32,

    /// Key provider configuration.
    #[serde(default)]
    pub key_provider: Option<KeyProviderConfig>,
}

/// Configuration for the key provider.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeyProviderConfig {
    /// Provider class/type name.
    pub class_name: String,

    /// Provider-specific parameters.
    #[serde(default)]
    pub parameters: HashMap<String, String>,
}

impl Default for TransparentDataEncryptionOptions {
    fn default() -> Self {
        Self {
            enabled: false,
            chunk_length_kb: defaults::chunk_length_kb(),
            cipher: defaults::cipher(),
            key_alias: None,
            key_length: defaults::key_length(),
            key_provider: None,
        }
    }
}

impl TransparentDataEncryptionOptions {
    /// Validate the TDE options, returning errors if invalid.
    pub fn validate(&self) -> Vec<String> {
        let mut errors = Vec::new();

        if self.enabled {
            if !RECOGNIZED_CIPHERS.contains(&self.cipher.as_str()) {
                errors.push(format!(
                    "unrecognized cipher '{}'; valid options: {:?}",
                    self.cipher, RECOGNIZED_CIPHERS
                ));
            }

            if !VALID_KEY_LENGTHS.contains(&self.key_length) {
                errors.push(format!(
                    "invalid key_length {}; must be one of {:?}",
                    self.key_length, VALID_KEY_LENGTHS
                ));
            }

            if self.chunk_length_kb == 0 {
                errors.push("chunk_length_kb must be > 0".to_string());
            }

            if self.chunk_length_kb > 1024 {
                errors.push("chunk_length_kb must be <= 1024".to_string());
            }

            if self.key_alias.is_none() {
                errors.push("key_alias is required when TDE is enabled".to_string());
            }

            if self.key_provider.is_none() {
                errors.push("key_provider is required when TDE is enabled".to_string());
            }
        }

        errors
    }

    /// Chunk length in bytes.
    pub fn chunk_length_bytes(&self) -> usize {
        self.chunk_length_kb as usize * 1024
    }
}

mod defaults {
    pub fn chunk_length_kb() -> u32 {
        64
    }

    pub fn cipher() -> String {
        "AES/CBC/PKCS5Padding".to_string()
    }

    pub fn key_length() -> u32 {
        128
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_disabled() {
        let opts = TransparentDataEncryptionOptions::default();
        assert!(!opts.enabled);
        assert_eq!(opts.chunk_length_kb, 64);
        assert_eq!(opts.cipher, "AES/CBC/PKCS5Padding");
    }

    #[test]
    fn disabled_validates() {
        let opts = TransparentDataEncryptionOptions::default();
        assert!(opts.validate().is_empty());
    }

    #[test]
    fn enabled_without_key_fails() {
        let mut opts = TransparentDataEncryptionOptions::default();
        opts.enabled = true;
        let errors = opts.validate();
        assert!(errors.iter().any(|e| e.contains("key_alias")));
        assert!(errors.iter().any(|e| e.contains("key_provider")));
    }

    #[test]
    fn unrecognized_cipher_fails() {
        let mut opts = TransparentDataEncryptionOptions::default();
        opts.enabled = true;
        opts.cipher = "DES/ECB/NoPadding".to_string();
        opts.key_alias = Some("test".to_string());
        opts.key_provider = Some(KeyProviderConfig {
            class_name: "FileKeyProvider".to_string(),
            parameters: HashMap::new(),
        });
        let errors = opts.validate();
        assert!(errors.iter().any(|e| e.contains("unrecognized cipher")));
    }

    #[test]
    fn valid_enabled_config() {
        let opts = TransparentDataEncryptionOptions {
            enabled: true,
            chunk_length_kb: 64,
            cipher: "AES/CBC/PKCS5Padding".to_string(),
            key_alias: Some("mykey".to_string()),
            key_length: 256,
            key_provider: Some(KeyProviderConfig {
                class_name: "FileKeyProvider".to_string(),
                parameters: HashMap::from([
                    ("keystore".to_string(), "/etc/keys/tde.keystore".to_string()),
                ]),
            }),
        };
        assert!(opts.validate().is_empty());
    }

    #[test]
    fn chunk_length_bytes() {
        let opts = TransparentDataEncryptionOptions::default();
        assert_eq!(opts.chunk_length_bytes(), 64 * 1024);
    }

    #[test]
    fn serde_roundtrip() {
        let opts = TransparentDataEncryptionOptions::default();
        let json = serde_json::to_string(&opts).unwrap();
        let opts2: TransparentDataEncryptionOptions = serde_json::from_str(&json).unwrap();
        assert_eq!(opts2.cipher, opts.cipher);
    }
}
