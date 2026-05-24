// Licensed under Apache License, Version 2.0.

//! `auditlogviewer` — inspect audit log JSONL files.
//!
//! Reads one or more audit log files (or directories containing `*.log` files)
//! and prints parsed events.

use cassandra_security::audit::AuditEvent;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};

fn print_usage() {
    eprintln!("Usage: cassandra-tools auditlogviewer [--json] <file-or-dir> [file-or-dir ...]");
}

fn collect_log_files(path: &Path, out: &mut Vec<PathBuf>) {
    if path.is_file() {
        out.push(path.to_path_buf());
        return;
    }
    if !path.is_dir() {
        eprintln!("Skipping '{}': not a file or directory", path.display());
        return;
    }

    let entries = match std::fs::read_dir(path) {
        Ok(entries) => entries,
        Err(e) => {
            eprintln!("Skipping '{}': {}", path.display(), e);
            return;
        }
    };

    for entry in entries.flatten() {
        let p = entry.path();
        if p.is_file() && p.extension().is_some_and(|ext| ext == "log") {
            out.push(p);
        }
    }
}

fn read_events(files: &[PathBuf]) -> Vec<AuditEvent> {
    let mut events = Vec::new();
    for file in files {
        let f = match File::open(file) {
            Ok(f) => f,
            Err(e) => {
                eprintln!("Skipping '{}': {}", file.display(), e);
                continue;
            }
        };
        let reader = BufReader::new(f);
        for (line_no, line) in reader.lines().enumerate() {
            let line = match line {
                Ok(line) => line,
                Err(e) => {
                    eprintln!("Skipping '{}:{}': {}", file.display(), line_no + 1, e);
                    continue;
                }
            };
            if line.trim().is_empty() {
                continue;
            }
            match serde_json::from_str::<AuditEvent>(&line) {
                Ok(event) => events.push(event),
                Err(e) => eprintln!(
                    "Skipping invalid JSON at '{}:{}': {}",
                    file.display(),
                    line_no + 1,
                    e
                ),
            }
        }
    }
    events
}

pub fn run(args: &[String]) {
    let mut json = false;
    let mut inputs = Vec::new();

    for arg in args {
        match arg.as_str() {
            "--json" => json = true,
            "-h" | "--help" => {
                print_usage();
                return;
            }
            _ => inputs.push(arg.clone()),
        }
    }

    if inputs.is_empty() {
        print_usage();
        return;
    }

    let mut files = Vec::new();
    for input in &inputs {
        collect_log_files(Path::new(input), &mut files);
    }
    files.sort();
    files.dedup();

    if files.is_empty() {
        eprintln!("No log files found.");
        return;
    }

    let events = read_events(&files);
    if json {
        for event in &events {
            match serde_json::to_string(event) {
                Ok(line) => println!("{}", line),
                Err(e) => eprintln!("Failed to serialize event: {}", e),
            }
        }
    } else {
        println!(
            "{:<14} {:<14} {:<8} {:<20} QUERY",
            "TIMESTAMP_MS", "TYPE", "STATUS", "USER"
        );
        println!("{}", "-".repeat(96));
        for event in &events {
            println!(
                "{:<14} {:<14} {:<8} {:<20} {}",
                event.timestamp,
                event.event_type,
                format!("{:?}", event.status),
                event.user,
                event.query.as_deref().unwrap_or("-")
            );
        }
    }

    println!("\n--- {} event(s) ---", events.len());
}

#[cfg(test)]
mod tests {
    use super::*;
    use cassandra_security::audit::{AuditEventType, AuditStatus};
    use std::io::Write;
    use tempfile::tempdir;

    #[test]
    fn collect_log_files_includes_log_extension() {
        let dir = tempdir().unwrap();
        let p1 = dir.path().join("audit.log");
        let p2 = dir.path().join("notes.txt");
        File::create(&p1).unwrap();
        File::create(&p2).unwrap();
        let mut files = Vec::new();
        collect_log_files(dir.path(), &mut files);
        assert_eq!(files.len(), 1);
        assert_eq!(files[0], p1);
    }

    #[test]
    fn read_events_parses_valid_json_lines() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("audit.log");
        let mut f = File::create(&path).unwrap();
        let event = AuditEvent {
            timestamp: 123,
            event_type: AuditEventType::Query,
            user: "alice".to_string(),
            source_address: "127.0.0.1".to_string(),
            keyspace: Some("ks".to_string()),
            table: Some("tbl".to_string()),
            query: Some("SELECT * FROM ks.tbl".to_string()),
            status: AuditStatus::Success,
        };
        writeln!(f, "{}", serde_json::to_string(&event).unwrap()).unwrap();
        writeln!(f, "{{invalid").unwrap();

        let events = read_events(&[path]);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].user, "alice");
    }
}
