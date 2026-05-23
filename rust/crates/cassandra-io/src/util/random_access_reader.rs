// Licensed under Apache License, Version 2.0.

//! Buffered random-access reader backed by a [`Rebufferer`].
//!
//! Provides `std::io::Read` and `std::io::Seek` on top of chunk-oriented
//! rebuffering, plus Cassandra-specific typed reads (int, long, short, float,
//! double) in big-endian byte order.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.io.util.RandomAccessReader`

use std::io::{self, Read, Seek, SeekFrom};

use byteorder::{BigEndian, ReadBytesExt};

use crate::util::rebufferer::{BufferHolder, Rebufferer};

// ---------------------------------------------------------------------------
// RandomAccessReader
// ---------------------------------------------------------------------------

/// A seekable reader that fetches data through a [`Rebufferer`].
///
/// Internally caches the most recently fetched [`BufferHolder`] and serves
/// reads from it until the logical position leaves the cached range.
pub struct RandomAccessReader {
    rebufferer: Box<dyn Rebufferer>,
    position: u64,
    buffer: Option<BufferHolder>,
    file_length: u64,
}

impl RandomAccessReader {
    /// Creates a new reader.
    pub fn new(rebufferer: Box<dyn Rebufferer>) -> Self {
        let file_length = rebufferer.file_length();
        Self {
            rebufferer,
            position: 0,
            buffer: None,
            file_length,
        }
    }

    /// Returns the current logical position in the file.
    pub fn position(&self) -> u64 {
        self.position
    }

    /// Returns the total file length.
    pub fn file_length(&self) -> u64 {
        self.file_length
    }

    /// Reads exactly `buf.len()` bytes, returning an error on short read.
    pub fn read_fully(&mut self, buf: &mut [u8]) -> io::Result<()> {
        self.read_exact(buf)
    }

    /// Advances the position by `n` bytes without reading.
    pub fn skip_bytes(&mut self, n: u64) -> io::Result<()> {
        let target = self.position.saturating_add(n);
        if target > self.file_length {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "skip_bytes past end of file",
            ));
        }
        self.position = target;
        // Invalidate buffer if position left cached range.
        if let Some(ref buf) = self.buffer {
            if self.position < buf.offset()
                || self.position >= buf.offset() + buf.data().len() as u64
            {
                self.buffer = None;
            }
        }
        Ok(())
    }

    // -- Cassandra-specific typed reads (big-endian) ------------------------

    /// Reads a big-endian `i32`.
    pub fn read_int(&mut self) -> io::Result<i32> {
        self.read_i32::<BigEndian>()
    }

    /// Reads a big-endian `i64`.
    pub fn read_long(&mut self) -> io::Result<i64> {
        self.read_i64::<BigEndian>()
    }

    /// Reads a big-endian `i16`.
    pub fn read_short(&mut self) -> io::Result<i16> {
        self.read_i16::<BigEndian>()
    }

    /// Reads a big-endian `f32`.
    pub fn read_float(&mut self) -> io::Result<f32> {
        self.read_f32::<BigEndian>()
    }

    /// Reads a big-endian `f64`.
    pub fn read_double(&mut self) -> io::Result<f64> {
        self.read_f64::<BigEndian>()
    }

    // -- internal helpers ---------------------------------------------------

    /// Ensures `self.buffer` covers `self.position`. Returns `false` when
    /// at or past EOF.
    fn ensure_buffer(&mut self) -> io::Result<bool> {
        if self.position >= self.file_length {
            return Ok(false);
        }

        let need_rebuffer = match &self.buffer {
            Some(buf) => {
                let buf_end = buf.offset() + buf.data().len() as u64;
                self.position < buf.offset() || self.position >= buf_end
            }
            None => true,
        };

        if need_rebuffer {
            let holder = self
                .rebufferer
                .rebuffer(self.position)
                .map_err(|e| io::Error::other(e.to_string()))?;
            self.buffer = Some(holder);
        }

        Ok(true)
    }
}

impl Read for RandomAccessReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }

        let mut total = 0usize;

        while total < buf.len() {
            if !self.ensure_buffer()? {
                break; // EOF
            }

            let holder = self.buffer.as_ref().unwrap();
            let offset_in_buf = (self.position - holder.offset()) as usize;
            let available = holder.data().len() - offset_in_buf;
            let to_copy = available.min(buf.len() - total);

            buf[total..total + to_copy]
                .copy_from_slice(&holder.data()[offset_in_buf..offset_in_buf + to_copy]);

            self.position += to_copy as u64;
            total += to_copy;

            // If we consumed this buffer entirely, invalidate so next
            // iteration fetches the next chunk.
            if offset_in_buf + to_copy >= holder.data().len() {
                self.buffer = None;
            }
        }

        Ok(total)
    }
}

impl Seek for RandomAccessReader {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        let new_pos = match pos {
            SeekFrom::Start(abs) => abs as i64,
            SeekFrom::Current(rel) => self.position as i64 + rel,
            SeekFrom::End(rel) => self.file_length as i64 + rel,
        };

        if new_pos < 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "seek to negative position",
            ));
        }

        self.position = new_pos as u64;

        // Invalidate buffer if the new position is outside it.
        if let Some(ref buf) = self.buffer {
            let buf_end = buf.offset() + buf.data().len() as u64;
            if self.position < buf.offset() || self.position >= buf_end {
                self.buffer = None;
            }
        }

        Ok(self.position)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::IoError;
    use std::io::Write as IoWrite;
    use tempfile::NamedTempFile;

    // -----------------------------------------------------------------------
    // Minimal file-backed rebufferer for testing.
    // -----------------------------------------------------------------------

    struct FileChunkRebufferer {
        file: std::fs::File,
        chunk_size: usize,
        file_length: u64,
    }

    impl FileChunkRebufferer {
        fn new(path: &std::path::Path, chunk_size: usize) -> Self {
            let file = std::fs::File::open(path).unwrap();
            let file_length = file.metadata().unwrap().len();
            Self {
                file,
                chunk_size,
                file_length,
            }
        }
    }

    impl Rebufferer for FileChunkRebufferer {
        fn rebuffer(&self, position: u64) -> Result<BufferHolder, IoError> {
            use std::os::unix::fs::FileExt;
            let chunk_start = (position / self.chunk_size as u64) * self.chunk_size as u64;

            let remaining = (self.file_length - chunk_start) as usize;
            let to_read = remaining.min(self.chunk_size);
            let mut buf = vec![0u8; to_read];
            self.file.read_exact_at(&mut buf, chunk_start)?;

            Ok(BufferHolder::new(buf, chunk_start))
        }

        fn file_length(&self) -> u64 {
            self.file_length
        }
    }

    fn make_reader(data: &[u8], chunk_size: usize) -> (NamedTempFile, RandomAccessReader) {
        let mut tmp = NamedTempFile::new().unwrap();
        tmp.write_all(data).unwrap();
        tmp.flush().unwrap();

        let rebuf = Box::new(FileChunkRebufferer::new(tmp.path(), chunk_size));
        (tmp, RandomAccessReader::new(rebuf))
    }

    #[test]
    fn test_sequential_read() {
        let data = b"Hello, random access world!";
        let (_tmp, mut reader) = make_reader(data, 8);

        let mut out = vec![0u8; data.len()];
        reader.read_exact(&mut out).unwrap();
        assert_eq!(&out, data);
        assert_eq!(reader.position(), data.len() as u64);
    }

    #[test]
    fn test_seek_start_and_read() {
        let data = b"ABCDEFGHIJKLMNOP";
        let (_tmp, mut reader) = make_reader(data, 4);

        reader.seek(SeekFrom::Start(8)).unwrap();
        let mut buf = [0u8; 4];
        reader.read_exact(&mut buf).unwrap();
        assert_eq!(&buf, b"IJKL");
    }

    #[test]
    fn test_seek_current() {
        let data = b"0123456789";
        let (_tmp, mut reader) = make_reader(data, 4);

        reader.read_exact(&mut [0u8; 3]).unwrap(); // position = 3
        reader.seek(SeekFrom::Current(2)).unwrap(); // position = 5
        let mut buf = [0u8; 2];
        reader.read_exact(&mut buf).unwrap();
        assert_eq!(&buf, b"56");
    }

    #[test]
    fn test_seek_end() {
        let data = b"ABCDEFGH";
        let (_tmp, mut reader) = make_reader(data, 4);

        reader.seek(SeekFrom::End(-3)).unwrap();
        let mut buf = [0u8; 3];
        reader.read_exact(&mut buf).unwrap();
        assert_eq!(&buf, b"FGH");
    }

    #[test]
    fn test_cross_buffer_boundary_read() {
        let data: Vec<u8> = (0u8..32).collect();
        let (_tmp, mut reader) = make_reader(&data, 8);

        // Start in middle of chunk 1 (position 6), read 10 bytes across
        // the boundary into chunk 2.
        reader.seek(SeekFrom::Start(6)).unwrap();
        let mut buf = [0u8; 10];
        reader.read_exact(&mut buf).unwrap();
        assert_eq!(&buf, &data[6..16]);
    }

    #[test]
    fn test_read_beyond_eof_returns_zero() {
        let data = b"short";
        let (_tmp, mut reader) = make_reader(data, 16);

        reader.seek(SeekFrom::Start(data.len() as u64)).unwrap();
        let mut buf = [0u8; 4];
        let n = reader.read(&mut buf).unwrap();
        assert_eq!(n, 0);
    }

    #[test]
    fn test_read_int_and_long() {
        let mut data = Vec::new();
        use byteorder::WriteBytesExt;
        data.write_i32::<BigEndian>(42).unwrap();
        data.write_i64::<BigEndian>(-1_000_000).unwrap();

        let (_tmp, mut reader) = make_reader(&data, 64);

        assert_eq!(reader.read_int().unwrap(), 42);
        assert_eq!(reader.read_long().unwrap(), -1_000_000);
    }

    #[test]
    fn test_read_short_float_double() {
        let mut data = Vec::new();
        use byteorder::WriteBytesExt;
        data.write_i16::<BigEndian>(1234).unwrap();
        data.write_f32::<BigEndian>(std::f32::consts::PI).unwrap();
        data.write_f64::<BigEndian>(std::f64::consts::E).unwrap();

        let (_tmp, mut reader) = make_reader(&data, 64);

        assert_eq!(reader.read_short().unwrap(), 1234);
        assert!((reader.read_float().unwrap() - std::f32::consts::PI).abs() < 1e-5);
        assert!((reader.read_double().unwrap() - std::f64::consts::E).abs() < 1e-9);
    }

    #[test]
    fn test_skip_bytes() {
        let data = b"AABBCCDDEE";
        let (_tmp, mut reader) = make_reader(data, 4);

        reader.skip_bytes(4).unwrap();
        assert_eq!(reader.position(), 4);
        let mut buf = [0u8; 2];
        reader.read_exact(&mut buf).unwrap();
        assert_eq!(&buf, b"CC");
    }

    #[test]
    fn test_skip_bytes_past_eof_errors() {
        let data = b"tiny";
        let (_tmp, mut reader) = make_reader(data, 16);

        let result = reader.skip_bytes(100);
        assert!(result.is_err());
    }

    #[test]
    fn test_file_length() {
        let data = b"1234567890";
        let (_tmp, reader) = make_reader(data, 4);
        assert_eq!(reader.file_length(), 10);
    }

    #[test]
    fn test_read_fully() {
        let data = b"complete read test data";
        let (_tmp, mut reader) = make_reader(data, 8);

        let mut out = vec![0u8; data.len()];
        reader.read_fully(&mut out).unwrap();
        assert_eq!(&out, data);
    }
}
