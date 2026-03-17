// Licensed under Apache License, Version 2.0.

//! Rollback validation tests at every migration phase.
//!
//! Verifies that rollback produces correct data state at each point:
//! pre-cutover, mid-cutover, and post-cutover.

#[cfg(test)]
mod tests {
    use std::fs;
    use tempfile::TempDir;

    fn write_data(dir: &std::path::Path, prefix: &str, count: usize) {
        fs::create_dir_all(dir).unwrap();
        for i in 0..count {
            fs::write(
                dir.join(format!("{}-{}.db", prefix, i)),
                format!("data-{}-{}", prefix, i),
            )
            .unwrap();
        }
    }

    fn snapshot(data_dir: &std::path::Path, snap_dir: &std::path::Path) {
        fs::create_dir_all(snap_dir).unwrap();
        for entry in fs::read_dir(data_dir).unwrap() {
            let entry = entry.unwrap();
            if entry.file_type().unwrap().is_file() {
                fs::copy(entry.path(), snap_dir.join(entry.file_name())).unwrap();
            }
        }
    }

    fn restore(snap_dir: &std::path::Path, data_dir: &std::path::Path) {
        // Clear data dir
        for entry in fs::read_dir(data_dir).unwrap() {
            let entry = entry.unwrap();
            if entry.file_type().unwrap().is_file() {
                fs::remove_file(entry.path()).unwrap();
            }
        }
        // Restore from snapshot
        for entry in fs::read_dir(snap_dir).unwrap() {
            let entry = entry.unwrap();
            if entry.file_type().unwrap().is_file() {
                fs::copy(entry.path(), data_dir.join(entry.file_name())).unwrap();
            }
        }
    }

    fn count_files(dir: &std::path::Path) -> usize {
        fs::read_dir(dir)
            .unwrap()
            .filter(|e| {
                e.as_ref()
                    .map(|e| e.file_type().unwrap().is_file())
                    .unwrap_or(false)
            })
            .count()
    }

    #[test]
    fn rollback_pre_cutover() {
        let dir = TempDir::new().unwrap();
        let data = dir.path().join("data");
        let snap = dir.path().join("snap");

        // Pre-migration state
        write_data(&data, "pre", 5);
        snapshot(&data, &snap);

        // Attempt migration (write new data)
        write_data(&data, "post", 3);
        assert_eq!(count_files(&data), 8);

        // Rollback
        restore(&snap, &data);
        assert_eq!(
            count_files(&data),
            5,
            "Rollback should restore pre-migration state"
        );

        // Verify only pre-migration data
        for i in 0..5 {
            let content = fs::read_to_string(data.join(format!("pre-{}.db", i))).unwrap();
            assert_eq!(content, format!("data-pre-{}", i));
        }
        assert!(
            !data.join("post-0.db").exists(),
            "Post-migration data should be gone"
        );
    }

    #[test]
    fn rollback_mid_cutover() {
        let dir = TempDir::new().unwrap();
        let data = dir.path().join("data");
        let snap = dir.path().join("snap");

        write_data(&data, "initial", 10);
        snapshot(&data, &snap);

        // Partial cutover: modify some files
        write_data(&data, "cutover", 3);
        fs::remove_file(data.join("initial-0.db")).unwrap();
        assert_eq!(count_files(&data), 12); // 9 initial + 3 cutover

        // Rollback
        restore(&snap, &data);
        assert_eq!(
            count_files(&data),
            10,
            "Should have exactly 10 initial files"
        );
        assert!(
            data.join("initial-0.db").exists(),
            "Deleted file should be restored"
        );
    }

    #[test]
    fn rollback_post_cutover() {
        let dir = TempDir::new().unwrap();
        let data = dir.path().join("data");
        let snap = dir.path().join("snap");

        write_data(&data, "original", 5);
        snapshot(&data, &snap);

        // Full cutover: replace all data
        for entry in fs::read_dir(&data).unwrap() {
            fs::remove_file(entry.unwrap().path()).unwrap();
        }
        write_data(&data, "new-format", 5);

        // Rollback
        restore(&snap, &data);
        assert_eq!(count_files(&data), 5);
        assert!(data.join("original-0.db").exists());
        assert!(!data.join("new-format-0.db").exists());
    }

    #[test]
    fn incremental_backup_survives_rollback() {
        let dir = TempDir::new().unwrap();
        let data = dir.path().join("data");
        let backups = dir.path().join("backups");
        let snap = dir.path().join("snap");

        write_data(&data, "base", 3);
        snapshot(&data, &snap);

        // Incremental: new files go to backups
        write_data(&backups, "incr", 2);

        // Rollback data dir
        restore(&snap, &data);

        // Backups should still exist
        assert_eq!(
            count_files(&backups),
            2,
            "Incremental backups should survive rollback"
        );
        assert_eq!(count_files(&data), 3, "Data should be at snapshot point");
    }

    #[test]
    fn multiple_rollback_points() {
        let dir = TempDir::new().unwrap();
        let data = dir.path().join("data");
        let snap1 = dir.path().join("snap1");
        let snap2 = dir.path().join("snap2");

        write_data(&data, "v1", 3);
        snapshot(&data, &snap1);

        write_data(&data, "v2", 2);
        snapshot(&data, &snap2);

        write_data(&data, "v3", 1);
        assert_eq!(count_files(&data), 6);

        // Rollback to snap2
        restore(&snap2, &data);
        assert_eq!(count_files(&data), 5);

        // Rollback to snap1
        restore(&snap1, &data);
        assert_eq!(count_files(&data), 3);
    }

    #[test]
    fn data_content_integrity_after_rollback() {
        let dir = TempDir::new().unwrap();
        let data = dir.path().join("data");
        let snap = dir.path().join("snap");

        // Write specific content
        fs::create_dir_all(&data).unwrap();
        fs::write(data.join("users.db"), "alice,bob,charlie").unwrap();
        fs::write(data.join("orders.db"), "order1,order2").unwrap();

        snapshot(&data, &snap);

        // Corrupt data
        fs::write(data.join("users.db"), "CORRUPTED").unwrap();
        fs::remove_file(data.join("orders.db")).unwrap();

        // Rollback
        restore(&snap, &data);

        assert_eq!(
            fs::read_to_string(data.join("users.db")).unwrap(),
            "alice,bob,charlie"
        );
        assert_eq!(
            fs::read_to_string(data.join("orders.db")).unwrap(),
            "order1,order2"
        );
    }
}
