// Licensed under Apache License, Version 2.0.

//! Encrypted commit log segment support (stub).
//!
//! ## Java Oracle
//! - `org.apache.cassandra.db.commitlog.EncryptedSegment`
//!
//! Defines the hook points where a `StorageEncryptor` would be inserted
//! into the commitlog write path. The actual encryption is delegated to
//! `cassandra-security`'s `StorageEncryptor` trait.
//!
//! ## Status
//!
//! This is a stub. The `CommitLog` currently writes plaintext segments.
//! When TDE is enabled, each segment's data blocks should be passed through
//! `StorageEncryptor::encrypt_segment` before writing and
//! `StorageEncryptor::decrypt_segment` after reading during replay.

/// Marker trait indicating a segment type supports encryption.
///
/// In the future, this will wrap the segment writer to transparently
/// encrypt data blocks before they hit disk.
pub trait EncryptedSegmentWriter {
    /// Whether this writer is performing encryption.
    fn is_encrypted(&self) -> bool;
}

/// A no-op implementation for unencrypted segments (the current default).
pub struct PlainSegmentWriter;

impl EncryptedSegmentWriter for PlainSegmentWriter {
    fn is_encrypted(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plain_writer_not_encrypted() {
        let writer = PlainSegmentWriter;
        assert!(!writer.is_encrypted());
    }
}
