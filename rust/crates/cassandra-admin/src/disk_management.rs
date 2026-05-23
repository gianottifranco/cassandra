// Licensed under Apache License, Version 2.0.

//! Data-directory disk tracking and token-range boundaries.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.db.Disks`
//! - `org.apache.cassandra.db.Directories`
//! - `org.apache.cassandra.db.DiskBoundaries`

use std::fmt;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use cassandra_common::token::{Token, TokenRange};

/// Runtime view of one configured Cassandra data directory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataDirectory {
    pub path: PathBuf,
    pub total_bytes: Option<u64>,
    pub reserved_bytes: u64,
    pub used_bytes: u64,
}

impl DataDirectory {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            total_bytes: None,
            reserved_bytes: 0,
            used_bytes: 0,
        }
    }

    pub fn with_capacity(path: impl Into<PathBuf>, total_bytes: u64, reserved_bytes: u64) -> Self {
        Self {
            path: path.into(),
            total_bytes: Some(total_bytes),
            reserved_bytes,
            used_bytes: 0,
        }
    }

    pub fn refresh_usage(&mut self) -> io::Result<()> {
        self.used_bytes = directory_size(&self.path)?;
        Ok(())
    }

    pub fn available_bytes(&self) -> Option<u64> {
        let total = self.total_bytes?;
        Some(total.saturating_sub(self.reserved_bytes + self.used_bytes))
    }

    pub fn usage_fraction(&self) -> Option<f64> {
        let total = self.total_bytes?;
        if total == 0 {
            return Some(1.0);
        }
        Some((self.used_bytes + self.reserved_bytes) as f64 / total as f64)
    }

    pub fn can_accept(&self, estimated_bytes: u64) -> bool {
        self.available_bytes()
            .is_none_or(|available| available >= estimated_bytes)
    }
}

/// Assignment of a token range to a data directory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiskBoundary {
    pub range: TokenRange,
    pub directory: PathBuf,
}

impl DiskBoundary {
    pub fn new(range: TokenRange, directory: impl Into<PathBuf>) -> Self {
        Self {
            range,
            directory: directory.into(),
        }
    }
}

/// Maintains data-directory health and token range placement hints.
#[derive(Debug, Clone, Default)]
pub struct DiskBoundaryManager {
    directories: Vec<DataDirectory>,
    boundaries: Vec<DiskBoundary>,
}

impl DiskBoundaryManager {
    pub fn new(mut directories: Vec<DataDirectory>) -> Self {
        directories.sort_by(|a, b| a.path.cmp(&b.path));
        Self {
            directories,
            boundaries: Vec::new(),
        }
    }

    pub fn from_paths<I, P>(paths: I) -> io::Result<Self>
    where
        I: IntoIterator<Item = P>,
        P: Into<PathBuf>,
    {
        let mut directories = Vec::new();
        for path in paths {
            let path = path.into();
            fs::create_dir_all(&path)?;
            let mut directory = DataDirectory::new(path);
            directory.refresh_usage()?;
            directories.push(directory);
        }
        Ok(Self::new(directories))
    }

    pub fn directories(&self) -> &[DataDirectory] {
        &self.directories
    }

    pub fn boundaries(&self) -> &[DiskBoundary] {
        &self.boundaries
    }

    pub fn set_boundaries(&mut self, boundaries: Vec<DiskBoundary>) -> Result<(), DiskError> {
        for boundary in &boundaries {
            if !self.has_directory(&boundary.directory) {
                return Err(DiskError::UnknownDirectory(boundary.directory.clone()));
            }
        }
        self.boundaries = boundaries;
        self.boundaries
            .sort_by_key(|boundary| boundary.range.end.value());
        Ok(())
    }

    pub fn refresh_usage(&mut self) -> io::Result<()> {
        for directory in &mut self.directories {
            directory.refresh_usage()?;
        }
        Ok(())
    }

    pub fn directory_for_token(&self, token: Token) -> Option<&DataDirectory> {
        let boundary = self
            .boundaries
            .iter()
            .find(|boundary| boundary.range.contains(token))?;
        self.directory_by_path(&boundary.directory)
    }

    pub fn boundaries_for_directory(&self, path: &Path) -> Vec<&DiskBoundary> {
        self.boundaries
            .iter()
            .filter(|boundary| boundary.directory == path)
            .collect()
    }

    pub fn select_write_directory(&self, estimated_bytes: u64) -> Option<&DataDirectory> {
        self.directories
            .iter()
            .filter(|directory| directory.can_accept(estimated_bytes))
            .min_by(|a, b| {
                let a_score = a.usage_fraction().unwrap_or(0.0);
                let b_score = b.usage_fraction().unwrap_or(0.0);
                a_score
                    .partial_cmp(&b_score)
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then_with(|| a.used_bytes.cmp(&b.used_bytes))
                    .then_with(|| a.path.cmp(&b.path))
            })
    }

    fn has_directory(&self, path: &Path) -> bool {
        self.directories
            .iter()
            .any(|directory| directory.path == path)
    }

    fn directory_by_path(&self, path: &Path) -> Option<&DataDirectory> {
        self.directories
            .iter()
            .find(|directory| directory.path == path)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DiskError {
    UnknownDirectory(PathBuf),
}

impl fmt::Display for DiskError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownDirectory(path) => write!(f, "unknown data directory: {}", path.display()),
        }
    }
}

impl std::error::Error for DiskError {}

fn directory_size(path: &Path) -> io::Result<u64> {
    let metadata = fs::metadata(path)?;
    if metadata.is_file() {
        return Ok(metadata.len());
    }

    let mut total = 0u64;
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let entry_path = entry.path();
        let file_type = entry.file_type()?;
        if file_type.is_dir() {
            total = total.saturating_add(directory_size(&entry_path)?);
        } else if file_type.is_file() {
            total = total.saturating_add(entry.metadata()?.len());
        }
    }
    Ok(total)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn token(value: i64) -> Token {
        Token::from_raw(value)
    }

    fn range(start: i64, end: i64) -> TokenRange {
        TokenRange::new(token(start), token(end))
    }

    #[test]
    fn refresh_usage_counts_nested_files() {
        let dir = TempDir::new().unwrap();
        let nested = dir.path().join("ks").join("tbl");
        fs::create_dir_all(&nested).unwrap();
        fs::write(dir.path().join("CommitLog-1.log"), [0u8; 5]).unwrap();
        fs::write(nested.join("nb-1-big-Data.db"), [0u8; 7]).unwrap();

        let mut data_dir = DataDirectory::with_capacity(dir.path(), 100, 10);
        data_dir.refresh_usage().unwrap();

        assert_eq!(data_dir.used_bytes, 12);
        assert_eq!(data_dir.available_bytes(), Some(78));
    }

    #[test]
    fn token_boundaries_select_configured_directory() {
        let dir_a = TempDir::new().unwrap();
        let dir_b = TempDir::new().unwrap();
        let mut manager = DiskBoundaryManager::new(vec![
            DataDirectory::new(dir_a.path()),
            DataDirectory::new(dir_b.path()),
        ]);

        manager
            .set_boundaries(vec![
                DiskBoundary::new(range(-100, 0), dir_a.path()),
                DiskBoundary::new(range(0, 100), dir_b.path()),
            ])
            .unwrap();

        assert_eq!(
            manager.directory_for_token(token(-50)).unwrap().path,
            dir_a.path()
        );
        assert_eq!(
            manager.directory_for_token(token(50)).unwrap().path,
            dir_b.path()
        );
    }

    #[test]
    fn rejects_boundaries_for_unknown_directory() {
        let dir = TempDir::new().unwrap();
        let mut manager = DiskBoundaryManager::new(vec![DataDirectory::new(dir.path())]);

        let err = manager
            .set_boundaries(vec![DiskBoundary::new(range(0, 100), "/missing")])
            .unwrap_err();

        assert!(matches!(err, DiskError::UnknownDirectory(_)));
    }

    #[test]
    fn write_selection_prefers_lowest_usage_with_capacity() {
        let dir_a = TempDir::new().unwrap();
        let dir_b = TempDir::new().unwrap();
        let mut a = DataDirectory::with_capacity(dir_a.path(), 100, 0);
        a.used_bytes = 90;
        let mut b = DataDirectory::with_capacity(dir_b.path(), 100, 0);
        b.used_bytes = 20;
        let manager = DiskBoundaryManager::new(vec![a, b]);

        assert_eq!(
            manager.select_write_directory(50).unwrap().path,
            dir_b.path()
        );
        assert!(manager.select_write_directory(90).is_none());
    }
}
