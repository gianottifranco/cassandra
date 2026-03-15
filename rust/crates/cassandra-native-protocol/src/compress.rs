// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

//! Frame body compression (LZ4 and Snappy).
//!
//! ## Java Oracle
//! - `org.apache.cassandra.transport.Frame.Compressor`

/// Compression algorithm negotiated during STARTUP.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Compression {
    Lz4,
    Snappy,
}

impl Compression {
    /// Parse from the COMPRESSION option string.
    pub fn from_name(name: &str) -> Option<Self> {
        match name.to_lowercase().as_str() {
            "lz4" => Some(Compression::Lz4),
            "snappy" => Some(Compression::Snappy),
            _ => None,
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            Compression::Lz4 => "lz4",
            Compression::Snappy => "snappy",
        }
    }

    /// Compress a frame body.
    pub fn compress(&self, data: &[u8]) -> std::io::Result<Vec<u8>> {
        match self {
            #[cfg(feature = "compression-lz4")]
            Compression::Lz4 => {
                // CQL protocol: 4-byte BE uncompressed length + LZ4 compressed body.
                let compressed = lz4_flex::compress(data);
                let mut result = Vec::with_capacity(4 + compressed.len());
                result.extend_from_slice(&(data.len() as u32).to_be_bytes());
                result.extend_from_slice(&compressed);
                Ok(result)
            }
            #[cfg(feature = "compression-snappy")]
            Compression::Snappy => {
                let mut encoder = snap::raw::Encoder::new();
                encoder.compress_vec(data).map_err(|e| {
                    std::io::Error::new(std::io::ErrorKind::Other, e)
                })
            }
            #[allow(unreachable_patterns)]
            _ => Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                format!("compression {} not compiled in", self.name()),
            )),
        }
    }

    /// Decompress a frame body.
    pub fn decompress(&self, data: &[u8]) -> std::io::Result<Vec<u8>> {
        match self {
            #[cfg(feature = "compression-lz4")]
            Compression::Lz4 => {
                if data.len() < 4 {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "LZ4 header too short",
                    ));
                }
                let uncompressed_size =
                    u32::from_be_bytes([data[0], data[1], data[2], data[3]]) as usize;
                if uncompressed_size == 0 {
                    return Ok(Vec::new());
                }
                let compressed = &data[4..];
                lz4_flex::decompress(compressed, uncompressed_size).map_err(|e| {
                    std::io::Error::new(std::io::ErrorKind::InvalidData, e)
                })
            }
            #[cfg(feature = "compression-snappy")]
            Compression::Snappy => {
                let mut decoder = snap::raw::Decoder::new();
                decoder.decompress_vec(data).map_err(|e| {
                    std::io::Error::new(std::io::ErrorKind::InvalidData, e)
                })
            }
            #[allow(unreachable_patterns)]
            _ => Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                format!("decompression {} not compiled in", self.name()),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(feature = "compression-lz4")]
    #[test]
    fn lz4_roundtrip() {
        let data = b"Hello, Cassandra! This is a test of LZ4 compression.";
        let compressed = Compression::Lz4.compress(data).unwrap();
        let decompressed = Compression::Lz4.decompress(&compressed).unwrap();
        assert_eq!(decompressed, data);
    }

    #[cfg(feature = "compression-snappy")]
    #[test]
    fn snappy_roundtrip() {
        let data = b"Hello, Cassandra! This is a test of Snappy compression.";
        let compressed = Compression::Snappy.compress(data).unwrap();
        let decompressed = Compression::Snappy.decompress(&compressed).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn from_name() {
        assert_eq!(Compression::from_name("lz4"), Some(Compression::Lz4));
        assert_eq!(Compression::from_name("Snappy"), Some(Compression::Snappy));
        assert_eq!(Compression::from_name("gzip"), None);
    }

    #[cfg(feature = "compression-lz4")]
    #[test]
    fn lz4_empty() {
        let compressed = Compression::Lz4.compress(b"").unwrap();
        let decompressed = Compression::Lz4.decompress(&compressed).unwrap();
        assert!(decompressed.is_empty());
    }
}
