// Licensed under Apache License, Version 2.0.

//! Snapshot and incremental backup management.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.db.Keyspace.snapshot()`
//! - `org.apache.cassandra.db.ColumnFamilyStore.snapshotWithoutMemtable()`
//! - `org.apache.cassandra.db.Directories`
//!
//! ## Architecture
//!
//! ### Snapshots
//! A named snapshot creates hard links to all SSTable component files
//! in a `snapshots/{name}` subdirectory. A manifest file lists all
//! components and the schema at snapshot time.
//!
//! ### Incremental Backups
//! On every SSTable flush, if enabled, hard links to the new SSTable
//! are created in a `backups/` subdirectory.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use tracing::{debug, info};

// ─── Snapshot ──────────────────────────────────────────────────────────────

/// Manifest describing a snapshot.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SnapshotManifest {
    /// Snapshot name.
    pub name: String,
    /// Timestamp when the snapshot was created (epoch millis).
    pub created_at: u64,
    /// Keyspace name.
    pub keyspace: String,
    /// Table name.
    pub table: String,
    /// List of SSTable component files in the snapshot.
    pub files: Vec<String>,
    /// Schema definition at snapshot time (CQL).
    pub schema: Option<String>,
}

/// Create a snapshot via hard links.
pub fn create_snapshot(
    name: &str,
    data_dir: &Path,
    keyspace: &str,
    table: &str,
    sstable_files: &[PathBuf],
    schema_cql: Option<&str>,
) -> io::Result<SnapshotManifest> {
    let snap_dir = data_dir
        .join("snapshots")
        .join(name);
    fs::create_dir_all(&snap_dir)?;

    let mut file_names = Vec::new();

    for src in sstable_files {
        if let Some(fname) = src.file_name() {
            let dest = snap_dir.join(fname);
            if !dest.exists() {
                // Try hard link, fall back to copy
                if fs::hard_link(src, &dest).is_err() {
                    fs::copy(src, &dest)?;
                }
                file_names.push(fname.to_string_lossy().to_string());
                debug!(file = %fname.to_string_lossy(), "Snapshot linked");
            }
        }
    }

    // Write schema if provided
    if let Some(cql) = schema_cql {
        let schema_path = snap_dir.join("schema.cql");
        fs::write(&schema_path, cql)?;
        file_names.push("schema.cql".to_string());
    }

    let manifest = SnapshotManifest {
        name: name.to_string(),
        created_at: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64,
        keyspace: keyspace.to_string(),
        table: table.to_string(),
        files: file_names,
        schema: schema_cql.map(|s| s.to_string()),
    };

    // Write manifest
    let manifest_path = snap_dir.join("manifest.json");
    let json = serde_json::to_vec_pretty(&manifest)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
    fs::write(&manifest_path, json)?;

    info!(
        name,
        files = manifest.files.len(),
        "Snapshot created"
    );

    Ok(manifest)
}

/// List all snapshots for a data directory.
pub fn list_snapshots(data_dir: &Path) -> io::Result<Vec<SnapshotManifest>> {
    let snap_root = data_dir.join("snapshots");
    if !snap_root.exists() {
        return Ok(Vec::new());
    }

    let mut manifests = Vec::new();
    for entry in fs::read_dir(&snap_root)? {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            let manifest_path = entry.path().join("manifest.json");
            if manifest_path.exists() {
                let data = fs::read_to_string(&manifest_path)?;
                if let Ok(manifest) = serde_json::from_str::<SnapshotManifest>(&data) {
                    manifests.push(manifest);
                }
            }
        }
    }

    manifests.sort_by_key(|m| m.created_at);
    Ok(manifests)
}

/// Delete a named snapshot.
pub fn delete_snapshot(data_dir: &Path, name: &str) -> io::Result<()> {
    let snap_dir = data_dir.join("snapshots").join(name);
    if snap_dir.exists() {
        fs::remove_dir_all(&snap_dir)?;
        info!(name, "Snapshot deleted");
    }
    Ok(())
}

/// Restore files from a snapshot back to the data directory.
pub fn restore_snapshot(
    data_dir: &Path,
    name: &str,
) -> io::Result<SnapshotManifest> {
    let snap_dir = data_dir.join("snapshots").join(name);
    let manifest_path = snap_dir.join("manifest.json");

    let data = fs::read_to_string(&manifest_path)?;
    let manifest: SnapshotManifest =
        serde_json::from_str(&data).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

    for file_name in &manifest.files {
        if file_name == "manifest.json" || file_name == "schema.cql" {
            continue;
        }
        let src = snap_dir.join(file_name);
        let dest = data_dir.join(file_name);
        if src.exists() && !dest.exists() {
            if fs::hard_link(&src, &dest).is_err() {
                fs::copy(&src, &dest)?;
            }
            debug!(file = %file_name, "Snapshot file restored");
        }
    }

    info!(name, files = manifest.files.len(), "Snapshot restored");
    Ok(manifest)
}

// ─── Incremental Backup ───────────────────────────────────────────────────

/// Configuration for incremental backups.
#[derive(Debug, Clone)]
pub struct IncrementalBackupConfig {
    pub enabled: bool,
    pub directory: PathBuf,
}

impl Default for IncrementalBackupConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            directory: PathBuf::from("data/backups"),
        }
    }
}

/// Hard-link newly flushed SSTable files to the backup directory.
/// Called automatically after each flush if incremental backup is enabled.
pub fn backup_sstable(
    backup_dir: &Path,
    sstable_files: &[PathBuf],
) -> io::Result<usize> {
    fs::create_dir_all(backup_dir)?;
    let mut linked = 0;

    for src in sstable_files {
        if let Some(fname) = src.file_name() {
            let dest = backup_dir.join(fname);
            if !dest.exists() {
                if fs::hard_link(src, &dest).is_err() {
                    fs::copy(src, &dest)?;
                }
                linked += 1;
            }
        }
    }

    if linked > 0 {
        debug!(linked, "SSTable files backed up incrementally");
    }

    Ok(linked)
}

/// List all backup files in the backup directory.
pub fn list_backup_files(backup_dir: &Path) -> io::Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    if backup_dir.exists() {
        for entry in fs::read_dir(backup_dir)? {
            let entry = entry?;
            if entry.file_type()?.is_file() {
                files.push(entry.path());
            }
        }
        files.sort();
    }
    Ok(files)
}

/// Clear the backup directory.
pub fn clear_backups(backup_dir: &Path) -> io::Result<u64> {
    let files = list_backup_files(backup_dir)?;
    let count = files.len() as u64;
    for file in files {
        fs::remove_file(&file)?;
    }
    if count > 0 {
        info!(count, "Backup files cleared");
    }
    Ok(count)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn snapshot_create_list_delete() {
        let dir = TempDir::new().unwrap();

        // Create some "SSTable" files
        let f1 = dir.path().join("ks-t1-big-1-Data.db");
        let f2 = dir.path().join("ks-t1-big-1-Index.db");
        fs::write(&f1, "data_content").unwrap();
        fs::write(&f2, "index_content").unwrap();

        // Snapshot
        let manifest = create_snapshot(
            "snap1",
            dir.path(),
            "ks",
            "t1",
            &[f1.clone(), f2.clone()],
            Some("CREATE TABLE t1 (id int PRIMARY KEY);"),
        )
        .unwrap();

        assert_eq!(manifest.name, "snap1");
        assert_eq!(manifest.files.len(), 3); // 2 SSTable files + schema.cql

        // List
        let snapshots = list_snapshots(dir.path()).unwrap();
        assert_eq!(snapshots.len(), 1);
        assert_eq!(snapshots[0].name, "snap1");

        // Delete
        delete_snapshot(dir.path(), "snap1").unwrap();
        let snapshots = list_snapshots(dir.path()).unwrap();
        assert!(snapshots.is_empty());
    }

    #[test]
    fn snapshot_restore() {
        let dir = TempDir::new().unwrap();

        let f1 = dir.path().join("testfile.db");
        fs::write(&f1, "content").unwrap();

        create_snapshot("snap2", dir.path(), "ks", "t1", &[f1.clone()], None).unwrap();

        // Remove original
        fs::remove_file(&f1).unwrap();
        assert!(!f1.exists());

        // Restore
        restore_snapshot(dir.path(), "snap2").unwrap();
        assert!(f1.exists());
        assert_eq!(fs::read_to_string(&f1).unwrap(), "content");
    }

    #[test]
    fn incremental_backup() {
        let dir = TempDir::new().unwrap();
        let backup_dir = dir.path().join("backups");

        let f1 = dir.path().join("data1.db");
        let f2 = dir.path().join("data2.db");
        fs::write(&f1, "d1").unwrap();
        fs::write(&f2, "d2").unwrap();

        let linked = backup_sstable(&backup_dir, &[f1, f2]).unwrap();
        assert_eq!(linked, 2);

        let files = list_backup_files(&backup_dir).unwrap();
        assert_eq!(files.len(), 2);

        let cleared = clear_backups(&backup_dir).unwrap();
        assert_eq!(cleared, 2);
    }

    #[test]
    fn list_snapshots_empty() {
        let dir = TempDir::new().unwrap();
        let snapshots = list_snapshots(dir.path()).unwrap();
        assert!(snapshots.is_empty());
    }
}
