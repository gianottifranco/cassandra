// Licensed under Apache License, Version 2.0.

//! Crypto provider abstraction for encryption-at-rest.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.security.CipherFactory`
//! - `org.apache.cassandra.security.EncryptionUtils`
//!
//! Provides `CryptoProvider` (encrypt/decrypt/IV) and `KeyProvider` (key lookup)
//! traits with implementations for AES-CBC, NoOp, and file-based keys.

use crate::SecurityError;
use std::path::Path;

/// Trait for key management: look up encryption keys by alias.
pub trait KeyProvider: Send + Sync {
    /// Get the raw key bytes for the given alias.
    fn get_key(&self, alias: &str) -> Result<Vec<u8>, SecurityError>;

    /// List available key aliases.
    fn list_aliases(&self) -> Result<Vec<String>, SecurityError>;
}

/// Trait for symmetric encryption/decryption.
pub trait CryptoProvider: Send + Sync {
    /// Encrypt `plaintext` with the given `key` and `iv`.
    fn encrypt(&self, key: &[u8], iv: &[u8], plaintext: &[u8]) -> Result<Vec<u8>, SecurityError>;

    /// Decrypt `ciphertext` with the given `key` and `iv`.
    fn decrypt(&self, key: &[u8], iv: &[u8], ciphertext: &[u8]) -> Result<Vec<u8>, SecurityError>;

    /// Generate a random IV of the appropriate length for this provider.
    fn generate_iv(&self) -> Vec<u8>;

    /// The IV length in bytes.
    fn iv_length(&self) -> usize;

    /// A human-readable name for this provider.
    fn name(&self) -> &str;
}

// ─── AES-CBC Provider ────────────────────────────────────────────────────

/// AES-CBC with PKCS7 padding (maps Java's AES/CBC/PKCS5Padding).
///
/// Supports 128-bit and 256-bit keys.
pub struct AesCbcProvider;

impl CryptoProvider for AesCbcProvider {
    fn encrypt(&self, key: &[u8], iv: &[u8], plaintext: &[u8]) -> Result<Vec<u8>, SecurityError> {
        use aes::Aes128;
        use cbc::cipher::block_padding::Pkcs7;
        use cbc::cipher::BlockEncryptMut;
        use cbc::cipher::KeyIvInit;

        match key.len() {
            16 => {
                type Aes128CbcEnc = cbc::Encryptor<Aes128>;
                let encryptor = Aes128CbcEnc::new_from_slices(key, iv)
                    .map_err(|e| SecurityError::ConfigError(format!("AES init: {e}")))?;
                Ok(encryptor.encrypt_padded_vec_mut::<Pkcs7>(plaintext))
            }
            32 => {
                type Aes256CbcEnc = cbc::Encryptor<aes::Aes256>;
                let encryptor = Aes256CbcEnc::new_from_slices(key, iv)
                    .map_err(|e| SecurityError::ConfigError(format!("AES init: {e}")))?;
                Ok(encryptor.encrypt_padded_vec_mut::<Pkcs7>(plaintext))
            }
            other => Err(SecurityError::ConfigError(format!(
                "unsupported AES key length: {} bytes (need 16 or 32)",
                other
            ))),
        }
    }

    fn decrypt(&self, key: &[u8], iv: &[u8], ciphertext: &[u8]) -> Result<Vec<u8>, SecurityError> {
        use aes::Aes128;
        use cbc::cipher::block_padding::Pkcs7;
        use cbc::cipher::BlockDecryptMut;
        use cbc::cipher::KeyIvInit;

        match key.len() {
            16 => {
                type Aes128CbcDec = cbc::Decryptor<Aes128>;
                let decryptor = Aes128CbcDec::new_from_slices(key, iv)
                    .map_err(|e| SecurityError::ConfigError(format!("AES init: {e}")))?;
                decryptor
                    .decrypt_padded_vec_mut::<Pkcs7>(ciphertext)
                    .map_err(|e| SecurityError::ConfigError(format!("AES decrypt: {e}")))
            }
            32 => {
                type Aes256CbcDec = cbc::Decryptor<aes::Aes256>;
                let decryptor = Aes256CbcDec::new_from_slices(key, iv)
                    .map_err(|e| SecurityError::ConfigError(format!("AES init: {e}")))?;
                decryptor
                    .decrypt_padded_vec_mut::<Pkcs7>(ciphertext)
                    .map_err(|e| SecurityError::ConfigError(format!("AES decrypt: {e}")))
            }
            other => Err(SecurityError::ConfigError(format!(
                "unsupported AES key length: {} bytes",
                other
            ))),
        }
    }

    fn generate_iv(&self) -> Vec<u8> {
        use rand::RngCore;
        let mut iv = vec![0u8; 16];
        rand::thread_rng().fill_bytes(&mut iv);
        iv
    }

    fn iv_length(&self) -> usize {
        16
    }

    fn name(&self) -> &str {
        "AES/CBC/PKCS7"
    }
}

// ─── NoOp Provider ───────────────────────────────────────────────────────

/// Pass-through provider that does not encrypt. For testing / disabled TDE.
pub struct NoOpCryptoProvider;

impl CryptoProvider for NoOpCryptoProvider {
    fn encrypt(&self, _key: &[u8], _iv: &[u8], plaintext: &[u8]) -> Result<Vec<u8>, SecurityError> {
        Ok(plaintext.to_vec())
    }

    fn decrypt(&self, _key: &[u8], _iv: &[u8], ciphertext: &[u8]) -> Result<Vec<u8>, SecurityError> {
        Ok(ciphertext.to_vec())
    }

    fn generate_iv(&self) -> Vec<u8> {
        vec![0u8; 16]
    }

    fn iv_length(&self) -> usize {
        16
    }

    fn name(&self) -> &str {
        "NoOp"
    }
}

// ─── File Key Provider ───────────────────────────────────────────────────

/// Loads encryption keys from files on disk.
///
/// Supports raw binary key files and hex-encoded key files.
pub struct FileKeyProvider {
    keys_directory: std::path::PathBuf,
}

impl FileKeyProvider {
    pub fn new<P: AsRef<Path>>(keys_directory: P) -> Self {
        Self {
            keys_directory: keys_directory.as_ref().to_path_buf(),
        }
    }
}

impl KeyProvider for FileKeyProvider {
    fn get_key(&self, alias: &str) -> Result<Vec<u8>, SecurityError> {
        // Try raw key file first
        let raw_path = self.keys_directory.join(alias);
        if raw_path.exists() {
            let data = std::fs::read(&raw_path)
                .map_err(|e| SecurityError::ConfigError(format!("read key '{}': {}", alias, e)))?;
            return Ok(data);
        }

        // Try hex-encoded key file
        let hex_path = self.keys_directory.join(format!("{}.hex", alias));
        if hex_path.exists() {
            let hex_str = std::fs::read_to_string(&hex_path)
                .map_err(|e| SecurityError::ConfigError(format!("read key '{}': {}", alias, e)))?;
            let decoded = hex_decode(hex_str.trim())
                .map_err(|e| SecurityError::ConfigError(format!("hex decode '{}': {}", alias, e)))?;
            return Ok(decoded);
        }

        Err(SecurityError::ConfigError(format!(
            "key '{}' not found in {}",
            alias,
            self.keys_directory.display()
        )))
    }

    fn list_aliases(&self) -> Result<Vec<String>, SecurityError> {
        let entries = std::fs::read_dir(&self.keys_directory)
            .map_err(|e| SecurityError::ConfigError(format!("read keys dir: {}", e)))?;
        let mut aliases = Vec::new();
        for entry in entries {
            let entry = entry.map_err(|e| SecurityError::ConfigError(e.to_string()))?;
            if let Some(name) = entry.file_name().to_str() {
                let alias = name.strip_suffix(".hex").unwrap_or(name);
                aliases.push(alias.to_string());
            }
        }
        aliases.sort();
        aliases.dedup();
        Ok(aliases)
    }
}

fn hex_decode(s: &str) -> Result<Vec<u8>, String> {
    if s.len() % 2 != 0 {
        return Err("odd-length hex string".to_string());
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).map_err(|e| e.to_string()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn aes128_cbc_roundtrip() {
        let provider = AesCbcProvider;
        let key = [0x42u8; 16]; // 128-bit key
        let iv = provider.generate_iv();
        let plaintext = b"Hello, Cassandra TDE!";

        let ciphertext = provider.encrypt(&key, &iv, plaintext).unwrap();
        assert_ne!(&ciphertext, plaintext);

        let decrypted = provider.decrypt(&key, &iv, &ciphertext).unwrap();
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn aes256_cbc_roundtrip() {
        let provider = AesCbcProvider;
        let key = [0x42u8; 32]; // 256-bit key
        let iv = provider.generate_iv();
        let plaintext = b"Encryption at rest test data 256";

        let ciphertext = provider.encrypt(&key, &iv, plaintext).unwrap();
        let decrypted = provider.decrypt(&key, &iv, &ciphertext).unwrap();
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn aes_bad_key_length() {
        let provider = AesCbcProvider;
        let key = [0u8; 24]; // 192-bit — not supported by our impl
        let iv = vec![0u8; 16];
        assert!(provider.encrypt(&key, &iv, b"test").is_err());
    }

    #[test]
    fn noop_passthrough() {
        let provider = NoOpCryptoProvider;
        let data = b"plaintext data";
        let encrypted = provider.encrypt(&[], &[], data).unwrap();
        assert_eq!(encrypted, data);
        let decrypted = provider.decrypt(&[], &[], &encrypted).unwrap();
        assert_eq!(decrypted, data);
    }

    #[test]
    fn file_key_provider_raw() {
        let dir = tempfile::tempdir().unwrap();
        let key_data = [0xAA; 16];
        std::fs::write(dir.path().join("test_key"), key_data).unwrap();

        let provider = FileKeyProvider::new(dir.path());
        let key = provider.get_key("test_key").unwrap();
        assert_eq!(key, key_data);
    }

    #[test]
    fn file_key_provider_hex() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("mykey.hex"), "0102030405060708090a0b0c0d0e0f10").unwrap();

        let provider = FileKeyProvider::new(dir.path());
        let key = provider.get_key("mykey").unwrap();
        assert_eq!(key.len(), 16);
        assert_eq!(key[0], 0x01);
        assert_eq!(key[15], 0x10);
    }

    #[test]
    fn file_key_provider_not_found() {
        let dir = tempfile::tempdir().unwrap();
        let provider = FileKeyProvider::new(dir.path());
        assert!(provider.get_key("nonexistent").is_err());
    }

    #[test]
    fn list_aliases() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("key_a"), [0u8; 16]).unwrap();
        std::fs::write(dir.path().join("key_b.hex"), "00").unwrap();

        let provider = FileKeyProvider::new(dir.path());
        let aliases = provider.list_aliases().unwrap();
        assert!(aliases.contains(&"key_a".to_string()));
        assert!(aliases.contains(&"key_b".to_string()));
    }

    #[test]
    fn iv_length() {
        assert_eq!(AesCbcProvider.iv_length(), 16);
        assert_eq!(NoOpCryptoProvider.iv_length(), 16);
    }

    #[test]
    fn provider_names() {
        assert_eq!(AesCbcProvider.name(), "AES/CBC/PKCS7");
        assert_eq!(NoOpCryptoProvider.name(), "NoOp");
    }
}
