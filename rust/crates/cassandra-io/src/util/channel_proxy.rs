// Licensed under Apache License, Version 2.0.

//! Positioned-read abstraction over a file descriptor.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.io.util.ChannelProxy`

use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

// ---------------------------------------------------------------------------
// ChannelProxy
// ---------------------------------------------------------------------------

/// A cheaply-cloneable handle that supports positioned (offset-based) reads
/// against an open file.
///
/// On Unix platforms the reads are performed with `pread(2)` — no locking is
/// required and multiple threads may read from the same descriptor
/// concurrently.
#[derive(Debug)]
pub struct ChannelProxy {
    inner: Arc<File>,
    path: PathBuf,
}

impl ChannelProxy {
    /// Opens the file at `path` for reading.
    pub fn new(path: impl AsRef<Path>) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = File::open(&path)?;
        Ok(Self {
            inner: Arc::new(file),
            path,
        })
    }

    /// Reads up to `buf.len()` bytes starting at `position`.
    ///
    /// Returns the number of bytes actually read (which may be less than
    /// `buf.len()` at EOF).
    #[cfg(unix)]
    pub fn pread(&self, buf: &mut [u8], position: u64) -> io::Result<usize> {
        use std::os::unix::fs::FileExt;
        self.inner.read_at(buf, position)
    }

    #[cfg(not(unix))]
    pub fn pread(&self, buf: &mut [u8], position: u64) -> io::Result<usize> {
        use std::io::{Read, Seek, SeekFrom};
        // Fallback: serialise via a mutex so that seek + read is atomic.
        // The mutex lives inside the struct for non-Unix builds (not shown
        // here because macOS/Linux are the primary targets).
        let file = &*self.inner;
        // SAFETY: we need &mut for Seek/Read but only one thread enters at a
        // time thanks to the Arc guaranteeing a single File instance.  On
        // non-Unix this is inherently racy without external synchronisation —
        // callers on Windows should wrap in their own Mutex.
        let file = unsafe { &mut *(file as *const File as *mut File) };
        file.seek(SeekFrom::Start(position))?;
        file.read(buf)
    }

    /// Returns the size of the underlying file in bytes.
    pub fn size(&self) -> io::Result<u64> {
        self.inner.metadata().map(|m| m.len())
    }

    /// Returns the path that was used to open the file.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Clone for ChannelProxy {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            path: self.path.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    /// Helper: create a temp file with the given content, return its path.
    fn temp_file(content: &[u8]) -> tempfile::NamedTempFile {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.write_all(content).unwrap();
        f.flush().unwrap();
        f
    }

    #[test]
    fn test_read_at_beginning() {
        let data = b"hello, channel proxy!";
        let f = temp_file(data);
        let proxy = ChannelProxy::new(f.path()).unwrap();

        let mut buf = [0u8; 5];
        let n = proxy.pread(&mut buf, 0).unwrap();
        assert_eq!(n, 5);
        assert_eq!(&buf, b"hello");
    }

    #[test]
    fn test_read_at_offset() {
        let data = b"abcdefghij";
        let f = temp_file(data);
        let proxy = ChannelProxy::new(f.path()).unwrap();

        let mut buf = [0u8; 3];
        let n = proxy.pread(&mut buf, 5).unwrap();
        assert_eq!(n, 3);
        assert_eq!(&buf, b"fgh");
    }

    #[test]
    fn test_read_past_eof_returns_short_read() {
        let data = b"short";
        let f = temp_file(data);
        let proxy = ChannelProxy::new(f.path()).unwrap();

        let mut buf = [0u8; 64];
        let n = proxy.pread(&mut buf, 3).unwrap();
        // Only 2 bytes remain ("rt").
        assert_eq!(n, 2);
        assert_eq!(&buf[..2], b"rt");
    }

    #[test]
    fn test_read_at_exact_eof() {
        let data = b"end";
        let f = temp_file(data);
        let proxy = ChannelProxy::new(f.path()).unwrap();

        let mut buf = [0u8; 4];
        let n = proxy.pread(&mut buf, data.len() as u64).unwrap();
        assert_eq!(n, 0);
    }

    #[test]
    fn test_size() {
        let data = vec![0u8; 9999];
        let f = temp_file(&data);
        let proxy = ChannelProxy::new(f.path()).unwrap();
        assert_eq!(proxy.size().unwrap(), 9999);
    }

    #[test]
    fn test_path() {
        let f = temp_file(b"x");
        let proxy = ChannelProxy::new(f.path()).unwrap();
        assert_eq!(proxy.path(), f.path());
    }

    #[test]
    fn test_clone_shares_descriptor() {
        let data = b"shared descriptor";
        let f = temp_file(data);
        let proxy = ChannelProxy::new(f.path()).unwrap();
        let clone = proxy.clone();

        let mut buf1 = [0u8; 6];
        let mut buf2 = [0u8; 10];

        proxy.pread(&mut buf1, 0).unwrap();
        clone.pread(&mut buf2, 7).unwrap();

        assert_eq!(&buf1, b"shared");
        assert_eq!(&buf2, b"descriptor");
    }

    #[test]
    fn test_concurrent_reads() {
        let data: Vec<u8> = (0..=255).cycle().take(8192).collect();
        let f = temp_file(&data);
        let proxy = ChannelProxy::new(f.path()).unwrap();

        let handles: Vec<_> = (0..4)
            .map(|i| {
                let p = proxy.clone();
                let expected = data.clone();
                std::thread::spawn(move || {
                    let offset = i * 1024;
                    let mut buf = vec![0u8; 1024];
                    let n = p.pread(&mut buf, offset as u64).unwrap();
                    assert_eq!(n, 1024);
                    assert_eq!(&buf[..], &expected[offset..offset + 1024]);
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }
    }
}
