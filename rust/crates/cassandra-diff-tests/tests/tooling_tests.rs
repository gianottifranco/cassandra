// Licensed under Apache License, Version 2.0.

//! Tooling E2E Tests
//!
//! Verifies that `bin/nodetool` and `rust/crates/cassandra-tools` execute
//! successfully and have compatible output formats without requiring JMX.

use std::path::PathBuf;
use std::process::Command;

fn get_cassandra_home() -> PathBuf {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("../../..");
    path.canonicalize().expect("failed to find cassandra home")
}

fn get_nodetool_path() -> PathBuf {
    let mut path = get_cassandra_home();
    path.push("bin/nodetool");
    path
}

fn get_cassandra_tools_path() -> PathBuf {
    let mut path = get_cassandra_home();
    // Assuming cargo build has run
    path.push("rust/target/debug/cassandra-tools");
    path
}

#[test]
fn test_nodetool_status_wrapper() {
    let nodetool = get_nodetool_path();
    if !nodetool.exists() {
        println!("Skipping nodetool wrapper test: bin/nodetool not found");
        return;
    }

    // Run `nodetool status` (connects to 127.0.0.1:9090 which might fail connection
    // but the wrapper should execute and format)
    let output = Command::new(&nodetool)
        .arg("status")
        .output()
        .expect("Failed to execute nodetool wrapper");

    let stdout = String::from_utf8_lossy(&output.stdout);
    
    // As long as the tool executed (even if it cannot connect to a live node because we don't start one in the test)
    // we verify the header is present, indicating wrapper success.
    assert!(stdout.contains("Datacenter: datacenter1"), "Wrapper failed to execute `status` correctly: {}", stdout);
}

#[test]
fn test_cassandra_tools_binary() {
    let tools_bin = get_cassandra_tools_path();
    if !tools_bin.exists() {
        println!("Skipping cassandra-tools binary test: target not found (run cargo build first)");
        return;
    }

    let output = Command::new(&tools_bin)
        .arg("info")
        .output()
        .expect("Failed to execute cassandra-tools");

    let stdout = String::from_utf8_lossy(&output.stdout);
    
    // Check if the 'cassandra-tools info' mock static output is present
    assert!(stdout.contains("Gossip active"), "cassandra-tools info output missing details: {}", stdout);
}

#[test]
fn test_sstabledump_wrapper_execution() {
    let mut sstabledump = get_cassandra_home();
    sstabledump.push("tools/bin/sstabledump");
    
    if !sstabledump.exists() {
        println!("Skipping sstabledump test: wrapper not found");
        return;
    }

    // Call it with a nonexistent file expecting an error format that matches the rust tool
    let output = Command::new(&sstabledump)
        .arg("nonexistent-file-Data.db")
        .output()
        .expect("Failed to execute sstabledump wrapper");
        
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("invalid SSTable filename format") || stderr.contains("not found"), 
            "sstabledump stderr mismatch: {}", stderr);
}

#[test]
fn test_sstablemetadata_wrapper_execution() {
    let mut sstablemetadata = get_cassandra_home();
    sstablemetadata.push("tools/bin/sstablemetadata");
    
    if !sstablemetadata.exists() {
        println!("Skipping sstablemetadata test: wrapper not found");
        return;
    }

    let output = Command::new(&sstablemetadata)
        .arg("nonexistent-file-Data.db")
        .output()
        .expect("Failed to execute sstablemetadata wrapper");
        
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("invalid SSTable filename format") || stderr.contains("not found"), 
            "sstablemetadata stderr mismatch: {}", stderr);
}
