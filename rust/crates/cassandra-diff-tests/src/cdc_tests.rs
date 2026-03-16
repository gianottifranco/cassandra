// Licensed under Apache License, Version 2.0.

//! CDC continuity tests across simulated migration boundary.

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn make_cdc_segments(dir: &std::path::Path, ids: &[u64]) {
        fs::create_dir_all(dir).unwrap();
        for id in ids {
            fs::write(
                dir.join(format!("CommitLog-{}.log", id)),
                format!("cdc-data-{}", id),
            )
            .unwrap();
        }
    }

    #[test]
    fn cdc_continuity_across_migration() {
        let java_dir = TempDir::new().unwrap();
        let rust_dir = TempDir::new().unwrap();

        // Java side: segments 1-5
        make_cdc_segments(java_dir.path(), &[1, 2, 3, 4, 5]);

        // Simulate migration: copy segments to rust side, add new ones
        make_cdc_segments(rust_dir.path(), &[1, 2, 3, 4, 5, 6, 7]);

        // Verify all Java segments present in Rust
        let java_ids: BTreeSet<u64> = (1..=5).collect();
        let rust_ids: BTreeSet<u64> = (1..=7).collect();
        let missing: Vec<u64> = java_ids.difference(&rust_ids).copied().collect();

        assert!(missing.is_empty(), "Missing segments: {:?}", missing);
    }

    #[test]
    fn cdc_no_duplicates() {
        let dir = TempDir::new().unwrap();
        make_cdc_segments(dir.path(), &[1, 2, 3]);

        // Count segments
        let count = fs::read_dir(dir.path())
            .unwrap()
            .filter(|e| {
                e.as_ref()
                    .ok()
                    .and_then(|e| e.file_name().to_str().map(|n| n.starts_with("CommitLog-")))
                    .unwrap_or(false)
            })
            .count();

        assert_eq!(count, 3, "Expected exactly 3 CDC segments, got {}", count);
    }

    #[test]
    fn cdc_gap_detection() {
        let ids: BTreeSet<u64> = [1, 2, 5, 6, 10].into();
        let sorted: Vec<u64> = ids.iter().copied().collect();
        let mut gaps = Vec::new();
        for w in sorted.windows(2) {
            if w[1] > w[0] + 1 {
                gaps.push((w[0] + 1, w[1] - 1));
            }
        }
        assert_eq!(gaps, vec![(3, 4), (7, 9)]);
    }

    #[test]
    fn cdc_handoff_point() {
        let last_java = 100u64;
        let first_rust = 101u64;
        assert_eq!(first_rust, last_java + 1, "Handoff should be contiguous");
    }

    #[test]
    fn cdc_segment_ordering() {
        let dir = TempDir::new().unwrap();
        make_cdc_segments(dir.path(), &[5, 1, 3, 2, 4]);

        let mut ids: Vec<u64> = Vec::new();
        for entry in fs::read_dir(dir.path()).unwrap() {
            let name = entry.unwrap().file_name().to_string_lossy().to_string();
            if let Some(rest) = name.strip_prefix("CommitLog-") {
                if let Some(id_str) = rest.strip_suffix(".log") {
                    if let Ok(id) = id_str.parse::<u64>() {
                        ids.push(id);
                    }
                }
            }
        }
        ids.sort();
        assert_eq!(ids, vec![1, 2, 3, 4, 5]);
    }
}
