// Licensed under Apache License, Version 2.0.

//! Persistent level assignments for Leveled Compaction Strategy (LCS).
//!
//! Tracks which SSTable belongs to which level and persists the mapping
//! to a JSON manifest file so it survives restarts.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.db.compaction.LeveledManifest`

use std::collections::{BTreeSet, HashMap};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::sstable::format::SSTableId;

/// A single SSTable-to-level assignment, used for serialization.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LevelAssignment {
    pub sstable_id: SSTableId,
    pub level: u32,
}

/// Serializable manifest data written to disk as JSON.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestData {
    pub assignments: Vec<LevelAssignment>,
    pub version: u64,
}

/// In-memory representation of SSTable level assignments.
///
/// Maintains a bidirectional mapping between SSTables and their levels,
/// and optionally persists the data to a JSON file.
#[derive(Debug)]
pub struct LeveledManifest {
    /// SSTable -> level mapping.
    levels: HashMap<SSTableId, u32>,
    /// Level -> set of SSTables at that level.
    by_level: HashMap<u32, BTreeSet<SSTableId>>,
    /// Monotonically increasing version, bumped on every mutation.
    version: u64,
    /// Optional path to the on-disk manifest file.
    manifest_path: Option<PathBuf>,
}

impl LeveledManifest {
    /// Create a new empty manifest without an on-disk path.
    pub fn new() -> Self {
        Self {
            levels: HashMap::new(),
            by_level: HashMap::new(),
            version: 0,
            manifest_path: None,
        }
    }

    /// Create a new empty manifest that will persist to the given path.
    pub fn with_path(path: PathBuf) -> Self {
        Self {
            levels: HashMap::new(),
            by_level: HashMap::new(),
            version: 0,
            manifest_path: Some(path),
        }
    }

    /// Add an SSTable at the given level. If it already exists, it is
    /// first removed from its old level before being placed in the new one.
    pub fn add(&mut self, sstable_id: SSTableId, level: u32) {
        // Remove from old level if present.
        if let Some(old_level) = self.levels.insert(sstable_id, level) {
            if let Some(set) = self.by_level.get_mut(&old_level) {
                set.remove(&sstable_id);
            }
        }
        self.by_level.entry(level).or_default().insert(sstable_id);
        self.version += 1;
    }

    /// Remove an SSTable from the manifest. Returns the level it was in,
    /// or `None` if it was not tracked.
    pub fn remove(&mut self, sstable_id: &SSTableId) -> Option<u32> {
        if let Some(level) = self.levels.remove(sstable_id) {
            if let Some(set) = self.by_level.get_mut(&level) {
                set.remove(sstable_id);
            }
            self.version += 1;
            Some(level)
        } else {
            None
        }
    }

    /// Promote (or demote) an SSTable to a new level. Returns an error
    /// if the SSTable is not currently tracked.
    pub fn promote(&mut self, sstable_id: &SSTableId, new_level: u32) -> Result<(), String> {
        let old_level = self
            .levels
            .get(sstable_id)
            .copied()
            .ok_or_else(|| format!("SSTable {} not found in manifest", sstable_id))?;

        if let Some(set) = self.by_level.get_mut(&old_level) {
            set.remove(sstable_id);
        }
        self.levels.insert(*sstable_id, new_level);
        self.by_level
            .entry(new_level)
            .or_default()
            .insert(*sstable_id);
        self.version += 1;
        Ok(())
    }

    /// Return the level of the given SSTable, or `None` if not tracked.
    pub fn level_of(&self, sstable_id: &SSTableId) -> Option<u32> {
        self.levels.get(sstable_id).copied()
    }

    /// Return a sorted list of SSTable IDs at the given level.
    pub fn sstables_in_level(&self, level: u32) -> Vec<SSTableId> {
        self.by_level
            .get(&level)
            .map(|set| set.iter().copied().collect())
            .unwrap_or_default()
    }

    /// Return the number of levels (highest non-empty level + 1), or 0
    /// if the manifest is empty.
    pub fn level_count(&self) -> u32 {
        self.by_level
            .iter()
            .filter(|(_, set)| !set.is_empty())
            .map(|(level, _)| *level + 1)
            .max()
            .unwrap_or(0)
    }

    /// Total number of SSTables tracked.
    pub fn total_sstables(&self) -> usize {
        self.levels.len()
    }

    /// Serialize the manifest to its on-disk path as JSON.
    ///
    /// Returns `Ok(())` if no path is configured (no-op).
    pub fn save(&self) -> std::io::Result<()> {
        let path = match &self.manifest_path {
            Some(p) => p,
            None => return Ok(()),
        };

        let data = ManifestData {
            assignments: self
                .levels
                .iter()
                .map(|(&sstable_id, &level)| LevelAssignment { sstable_id, level })
                .collect(),
            version: self.version,
        };

        let json = serde_json::to_string_pretty(&data).map_err(|e| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, e)
        })?;

        std::fs::write(path, json)
    }

    /// Load a manifest from a JSON file on disk.
    pub fn load(path: &Path) -> std::io::Result<Self> {
        let contents = std::fs::read_to_string(path)?;
        let data: ManifestData = serde_json::from_str(&contents).map_err(|e| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, e)
        })?;

        let mut manifest = Self::with_path(path.to_path_buf());
        manifest.version = data.version;

        for assignment in data.assignments {
            manifest
                .levels
                .insert(assignment.sstable_id, assignment.level);
            manifest
                .by_level
                .entry(assignment.level)
                .or_default()
                .insert(assignment.sstable_id);
        }

        Ok(manifest)
    }
}

impl Default for LeveledManifest {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn add_and_remove_sstable_tracking() {
        let mut manifest = LeveledManifest::new();

        manifest.add(1, 0);
        manifest.add(2, 0);
        manifest.add(3, 1);

        assert_eq!(manifest.level_of(&1), Some(0));
        assert_eq!(manifest.level_of(&2), Some(0));
        assert_eq!(manifest.level_of(&3), Some(1));
        assert_eq!(manifest.total_sstables(), 3);

        let old = manifest.remove(&2);
        assert_eq!(old, Some(0));
        assert_eq!(manifest.level_of(&2), None);
        assert_eq!(manifest.total_sstables(), 2);

        // Removing a non-existent SSTable returns None.
        assert_eq!(manifest.remove(&999), None);
    }

    #[test]
    fn promote_changes_level_correctly() {
        let mut manifest = LeveledManifest::new();

        manifest.add(10, 0);
        assert_eq!(manifest.level_of(&10), Some(0));

        manifest.promote(&10, 1).unwrap();
        assert_eq!(manifest.level_of(&10), Some(1));

        // Old level should no longer contain the SSTable.
        assert!(manifest.sstables_in_level(0).is_empty());
        assert_eq!(manifest.sstables_in_level(1), vec![10]);
    }

    #[test]
    fn promote_unknown_sstable_returns_error() {
        let mut manifest = LeveledManifest::new();
        let result = manifest.promote(&42, 1);
        assert!(result.is_err());
    }

    #[test]
    fn save_load_roundtrip_preserves_data() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("manifest.json");

        let mut original = LeveledManifest::with_path(path.clone());
        original.add(1, 0);
        original.add(2, 0);
        original.add(3, 1);
        original.add(4, 2);
        original.save().unwrap();

        let loaded = LeveledManifest::load(&path).unwrap();

        assert_eq!(loaded.level_of(&1), Some(0));
        assert_eq!(loaded.level_of(&2), Some(0));
        assert_eq!(loaded.level_of(&3), Some(1));
        assert_eq!(loaded.level_of(&4), Some(2));
        assert_eq!(loaded.total_sstables(), 4);
    }

    #[test]
    fn level_queries_return_correct_sets() {
        let mut manifest = LeveledManifest::new();

        manifest.add(1, 0);
        manifest.add(2, 0);
        manifest.add(3, 1);
        manifest.add(4, 1);
        manifest.add(5, 2);

        let l0 = manifest.sstables_in_level(0);
        assert_eq!(l0.len(), 2);
        assert!(l0.contains(&1));
        assert!(l0.contains(&2));

        let l1 = manifest.sstables_in_level(1);
        assert_eq!(l1.len(), 2);
        assert!(l1.contains(&3));
        assert!(l1.contains(&4));

        let l2 = manifest.sstables_in_level(2);
        assert_eq!(l2, vec![5]);

        // Empty level returns empty vec.
        assert!(manifest.sstables_in_level(99).is_empty());
    }

    #[test]
    fn level_count_reflects_highest_nonempty_level() {
        let mut manifest = LeveledManifest::new();

        assert_eq!(manifest.level_count(), 0);

        manifest.add(1, 0);
        assert_eq!(manifest.level_count(), 1);

        manifest.add(2, 3);
        assert_eq!(manifest.level_count(), 4); // highest level 3 → count = 4

        manifest.remove(&2);
        assert_eq!(manifest.level_count(), 1); // back to only level 0

        manifest.remove(&1);
        assert_eq!(manifest.level_count(), 0);
    }

    #[test]
    fn add_existing_sstable_moves_to_new_level() {
        let mut manifest = LeveledManifest::new();

        manifest.add(1, 0);
        assert_eq!(manifest.sstables_in_level(0), vec![1]);

        // Re-add at a different level.
        manifest.add(1, 2);
        assert!(manifest.sstables_in_level(0).is_empty());
        assert_eq!(manifest.sstables_in_level(2), vec![1]);
        assert_eq!(manifest.total_sstables(), 1);
    }
}
