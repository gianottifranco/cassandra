// Licensed under Apache License, Version 2.0.

//! Tooling E2E Tests
//!
//! Verifies that `bin/nodetool` and `rust/crates/cassandra-tools` execute
//! successfully and have compatible output formats without requiring JMX.

use std::path::PathBuf;
use std::process::Command;
use std::sync::mpsc;
use std::thread;
use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
};

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

const PROTOCOL_V4_REQUEST: u8 = 0x04;
const PROTOCOL_V4_RESPONSE: u8 = 0x84;
const OPCODE_READY: u8 = 0x02;
const OPCODE_AUTHENTICATE: u8 = 0x03;
const OPCODE_ERROR: u8 = 0x00;
const OPCODE_RESULT: u8 = 0x08;
const OPCODE_STARTUP: u8 = 0x01;
const OPCODE_PREPARE: u8 = 0x09;
const OPCODE_EXECUTE: u8 = 0x0A;
const OPCODE_AUTH_CHALLENGE: u8 = 0x0E;
const OPCODE_AUTH_RESPONSE: u8 = 0x0F;
const OPCODE_AUTH_SUCCESS: u8 = 0x10;
const RESULT_KIND_VOID: i32 = 0x0001;
const RESULT_KIND_PREPARED: i32 = 0x0004;
const ERROR_UNPREPARED: i32 = 0x2500;
const ROWS_FLAG_NO_METADATA: i32 = 0x0004;
const QUERY_FLAG_VALUES: u8 = 0x01;

#[derive(Debug)]
struct Frame {
    version: u8,
    stream_id: i16,
    opcode: u8,
    body: Vec<u8>,
}

fn read_u16_be(cursor: &mut &[u8]) -> u16 {
    let mut buf = [0u8; 2];
    buf.copy_from_slice(&cursor[..2]);
    *cursor = &cursor[2..];
    u16::from_be_bytes(buf)
}

fn read_i32_be(cursor: &mut &[u8]) -> i32 {
    let mut buf = [0u8; 4];
    buf.copy_from_slice(&cursor[..4]);
    *cursor = &cursor[4..];
    i32::from_be_bytes(buf)
}

fn read_bytes(cursor: &mut &[u8]) -> Option<Vec<u8>> {
    let len = read_i32_be(cursor);
    if len < 0 {
        return None;
    }
    let len = len as usize;
    let value = cursor[..len].to_vec();
    *cursor = &cursor[len..];
    Some(value)
}

fn read_short_bytes(cursor: &mut &[u8]) -> Vec<u8> {
    let len = read_u16_be(cursor) as usize;
    let value = cursor[..len].to_vec();
    *cursor = &cursor[len..];
    value
}

fn write_i32_be(out: &mut Vec<u8>, v: i32) {
    out.extend_from_slice(&v.to_be_bytes());
}

fn write_u16_be(out: &mut Vec<u8>, v: u16) {
    out.extend_from_slice(&v.to_be_bytes());
}

fn write_string(out: &mut Vec<u8>, s: &str) {
    write_u16_be(out, s.len() as u16);
    out.extend_from_slice(s.as_bytes());
}

fn write_short_bytes(out: &mut Vec<u8>, data: &[u8]) {
    write_u16_be(out, data.len() as u16);
    out.extend_from_slice(data);
}

fn write_bytes_opt(out: &mut Vec<u8>, data: Option<&[u8]>) {
    match data {
        Some(data) => {
            write_i32_be(out, data.len() as i32);
            out.extend_from_slice(data);
        }
        None => write_i32_be(out, -1),
    }
}

fn read_frame(stream: &mut TcpStream) -> std::io::Result<Frame> {
    let mut header = [0u8; 9];
    stream.read_exact(&mut header)?;
    let version = header[0];
    let stream_id = i16::from_be_bytes([header[2], header[3]]);
    let opcode = header[4];
    let length = u32::from_be_bytes([header[5], header[6], header[7], header[8]]) as usize;
    let mut body = vec![0u8; length];
    stream.read_exact(&mut body)?;
    Ok(Frame {
        version,
        stream_id,
        opcode,
        body,
    })
}

fn write_response_frame(
    stream: &mut TcpStream,
    stream_id: i16,
    opcode: u8,
    body: &[u8],
) -> std::io::Result<()> {
    let mut header = Vec::with_capacity(9);
    header.push(PROTOCOL_V4_RESPONSE);
    header.push(0);
    header.extend_from_slice(&stream_id.to_be_bytes());
    header.push(opcode);
    header.extend_from_slice(&(body.len() as u32).to_be_bytes());
    stream.write_all(&header)?;
    stream.write_all(body)?;
    Ok(())
}

fn prepared_result_body(prepared_id: &[u8]) -> Vec<u8> {
    let mut body = Vec::new();
    write_i32_be(&mut body, RESULT_KIND_PREPARED);
    write_short_bytes(&mut body, prepared_id);

    // bind metadata with two columns, both unknown type IDs -> should map to UNSET.
    write_i32_be(&mut body, 0); // flags
    write_i32_be(&mut body, 2); // columns_count
    for name in ["k", "v"] {
        write_string(&mut body, "ks");
        write_string(&mut body, "tbl");
        write_string(&mut body, name);
        write_u16_be(&mut body, 0x4242); // unknown option id
    }

    // result metadata: NO_METADATA + 0 columns
    write_i32_be(&mut body, ROWS_FLAG_NO_METADATA);
    write_i32_be(&mut body, 0);
    body
}

fn authenticate_body(authenticator: &str) -> Vec<u8> {
    let mut body = Vec::new();
    write_string(&mut body, authenticator);
    body
}

fn auth_challenge_body(token: &[u8]) -> Vec<u8> {
    let mut body = Vec::new();
    write_bytes_opt(&mut body, Some(token));
    body
}

fn auth_success_body(token: Option<&[u8]>) -> Vec<u8> {
    let mut body = Vec::new();
    write_bytes_opt(&mut body, token);
    body
}

fn unprepared_error_body(prepared_id: &[u8]) -> Vec<u8> {
    let mut body = Vec::new();
    write_i32_be(&mut body, ERROR_UNPREPARED);
    write_string(&mut body, "Prepared query not found");
    write_short_bytes(&mut body, prepared_id);
    body
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
    assert!(
        stdout.contains("Datacenter: datacenter1"),
        "Wrapper failed to execute `status` correctly: {}",
        stdout
    );
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
    assert!(
        stdout.contains("Gossip active"),
        "cassandra-tools info output missing details: {}",
        stdout
    );
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
    assert!(
        stderr.contains("invalid SSTable filename format") || stderr.contains("not found"),
        "sstabledump stderr mismatch: {}",
        stderr
    );
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
    assert!(
        stderr.contains("invalid SSTable filename format") || stderr.contains("not found"),
        "sstablemetadata stderr mismatch: {}",
        stderr
    );
}

#[test]
fn test_cassandra_stress_prepared_unknown_binds_emit_unset() {
    let tools_bin = get_cassandra_tools_path();
    if !tools_bin.exists() {
        println!("Skipping cassandra-tools stress test: target not found (run cargo build first)");
        return;
    }

    let listener = TcpListener::bind("127.0.0.1:0").expect("bind stub listener");
    let native_port = listener.local_addr().unwrap().port();
    let (tx, rx) = mpsc::channel::<Result<(), String>>();

    let server = thread::spawn(move || {
        let result = (|| -> Result<(), String> {
            let (mut socket, _) = listener.accept().map_err(|e| e.to_string())?;

            let startup = read_frame(&mut socket).map_err(|e| e.to_string())?;
            if startup.version != PROTOCOL_V4_REQUEST || startup.opcode != OPCODE_STARTUP {
                return Err(format!(
                    "expected STARTUP v4, got version=0x{:02X} opcode=0x{:02X}",
                    startup.version, startup.opcode
                ));
            }
            write_response_frame(&mut socket, startup.stream_id, OPCODE_READY, &[])
                .map_err(|e| e.to_string())?;

            let prepare = read_frame(&mut socket).map_err(|e| e.to_string())?;
            if prepare.opcode != OPCODE_PREPARE {
                return Err(format!(
                    "expected PREPARE, got opcode=0x{:02X}",
                    prepare.opcode
                ));
            }
            let prepared_id = b"stress-prepared-id";
            let prepared_body = prepared_result_body(prepared_id);
            write_response_frame(
                &mut socket,
                prepare.stream_id,
                OPCODE_RESULT,
                &prepared_body,
            )
            .map_err(|e| e.to_string())?;

            let execute = read_frame(&mut socket).map_err(|e| e.to_string())?;
            if execute.opcode != OPCODE_EXECUTE {
                return Err(format!(
                    "expected EXECUTE, got opcode=0x{:02X}",
                    execute.opcode
                ));
            }
            let mut cursor: &[u8] = &execute.body;
            let recv_id = read_short_bytes(&mut cursor);
            if recv_id != prepared_id {
                return Err(format!(
                    "prepared id mismatch: expected {:?} got {:?}",
                    prepared_id, recv_id
                ));
            }
            let _consistency = read_u16_be(&mut cursor);
            let flags = cursor[0];
            cursor = &cursor[1..];
            if flags & QUERY_FLAG_VALUES == 0 {
                return Err(format!("expected VALUES flag, got 0x{flags:02X}"));
            }
            let value_count = read_u16_be(&mut cursor);
            if value_count != 2 {
                return Err(format!("expected 2 bind values, got {}", value_count));
            }
            let v1 = read_i32_be(&mut cursor);
            let v2 = read_i32_be(&mut cursor);
            if v1 != -2 || v2 != -2 {
                return Err(format!("expected UNSET(-2,-2), got ({v1},{v2})"));
            }

            let mut void_body = Vec::new();
            write_i32_be(&mut void_body, RESULT_KIND_VOID);
            write_response_frame(&mut socket, execute.stream_id, OPCODE_RESULT, &void_body)
                .map_err(|e| e.to_string())?;

            Ok(())
        })();
        let _ = tx.send(result);
    });

    let output = Command::new(&tools_bin)
        .args([
            "--host",
            "127.0.0.1",
            "--port",
            "1",
            "cassandra-stress",
            "write",
            "--native-cql",
            "--prepared",
            "--native-port",
            &native_port.to_string(),
            "--query-write",
            "INSERT INTO ks.tbl (k, v) VALUES (?, ?)",
            "--ops",
            "1",
            "--concurrency",
            "1",
        ])
        .output()
        .expect("Failed to execute cassandra-tools stress");

    let stub_result = rx.recv().expect("stub result channel");
    server.join().expect("stub join");
    if let Err(err) = stub_result {
        panic!("native stub validation failed: {err}");
    }

    assert!(output.status.success(), "stress command should succeed");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("transport:      native-cql"));
    assert!(stdout.contains("prepared:       true"));
}

#[test]
fn test_cassandra_stress_native_auth_challenge_roundtrip() {
    let tools_bin = get_cassandra_tools_path();
    if !tools_bin.exists() {
        println!(
            "Skipping cassandra-tools stress auth test: target not found (run cargo build first)"
        );
        return;
    }

    let listener = TcpListener::bind("127.0.0.1:0").expect("bind stub listener");
    let native_port = listener.local_addr().unwrap().port();
    let (tx, rx) = mpsc::channel::<Result<(), String>>();

    let server = thread::spawn(move || {
        let result = (|| -> Result<(), String> {
            let (mut socket, _) = listener.accept().map_err(|e| e.to_string())?;

            let startup = read_frame(&mut socket).map_err(|e| e.to_string())?;
            if startup.version != PROTOCOL_V4_REQUEST || startup.opcode != OPCODE_STARTUP {
                return Err(format!(
                    "expected STARTUP v4, got version=0x{:02X} opcode=0x{:02X}",
                    startup.version, startup.opcode
                ));
            }

            let authenticate = authenticate_body("org.apache.cassandra.auth.PasswordAuthenticator");
            write_response_frame(
                &mut socket,
                startup.stream_id,
                OPCODE_AUTHENTICATE,
                &authenticate,
            )
            .map_err(|e| e.to_string())?;

            let first_auth = read_frame(&mut socket).map_err(|e| e.to_string())?;
            if first_auth.opcode != OPCODE_AUTH_RESPONSE {
                return Err(format!(
                    "expected AUTH_RESPONSE, got opcode=0x{:02X}",
                    first_auth.opcode
                ));
            }
            let mut first_auth_cursor: &[u8] = &first_auth.body;
            let first_token = read_bytes(&mut first_auth_cursor)
                .ok_or_else(|| "first auth token must be present".to_string())?;
            if first_token != b"\0alice\0secret".to_vec() {
                return Err(format!("unexpected first auth token: {:?}", first_token));
            }

            let challenge = auth_challenge_body(b"challenge");
            write_response_frame(
                &mut socket,
                first_auth.stream_id,
                OPCODE_AUTH_CHALLENGE,
                &challenge,
            )
            .map_err(|e| e.to_string())?;

            let second_auth = read_frame(&mut socket).map_err(|e| e.to_string())?;
            if second_auth.opcode != OPCODE_AUTH_RESPONSE {
                return Err(format!(
                    "expected second AUTH_RESPONSE, got opcode=0x{:02X}",
                    second_auth.opcode
                ));
            }
            let mut second_auth_cursor: &[u8] = &second_auth.body;
            let second_token = read_bytes(&mut second_auth_cursor)
                .ok_or_else(|| "second auth token must be present".to_string())?;
            if second_token != first_token {
                return Err(format!(
                    "auth token mismatch across challenge: {:?} vs {:?}",
                    first_token, second_token
                ));
            }

            let success = auth_success_body(None);
            write_response_frame(
                &mut socket,
                second_auth.stream_id,
                OPCODE_AUTH_SUCCESS,
                &success,
            )
            .map_err(|e| e.to_string())?;

            let prepare = read_frame(&mut socket).map_err(|e| e.to_string())?;
            if prepare.opcode != OPCODE_PREPARE {
                return Err(format!(
                    "expected PREPARE, got opcode=0x{:02X}",
                    prepare.opcode
                ));
            }
            let prepared_id = b"stress-auth-prepared-id";
            let prepared_body = prepared_result_body(prepared_id);
            write_response_frame(
                &mut socket,
                prepare.stream_id,
                OPCODE_RESULT,
                &prepared_body,
            )
            .map_err(|e| e.to_string())?;

            let execute = read_frame(&mut socket).map_err(|e| e.to_string())?;
            if execute.opcode != OPCODE_EXECUTE {
                return Err(format!(
                    "expected EXECUTE, got opcode=0x{:02X}",
                    execute.opcode
                ));
            }
            let mut cursor: &[u8] = &execute.body;
            let recv_id = read_short_bytes(&mut cursor);
            if recv_id != prepared_id {
                return Err(format!(
                    "prepared id mismatch: expected {:?} got {:?}",
                    prepared_id, recv_id
                ));
            }
            let _consistency = read_u16_be(&mut cursor);
            let flags = cursor[0];
            cursor = &cursor[1..];
            if flags & QUERY_FLAG_VALUES == 0 {
                return Err(format!("expected VALUES flag, got 0x{flags:02X}"));
            }
            let value_count = read_u16_be(&mut cursor);
            if value_count != 2 {
                return Err(format!("expected 2 bind values, got {}", value_count));
            }
            let v1 = read_i32_be(&mut cursor);
            let v2 = read_i32_be(&mut cursor);
            if v1 != -2 || v2 != -2 {
                return Err(format!("expected UNSET(-2,-2), got ({v1},{v2})"));
            }

            let mut void_body = Vec::new();
            write_i32_be(&mut void_body, RESULT_KIND_VOID);
            write_response_frame(&mut socket, execute.stream_id, OPCODE_RESULT, &void_body)
                .map_err(|e| e.to_string())?;

            Ok(())
        })();
        let _ = tx.send(result);
    });

    let output = Command::new(&tools_bin)
        .args([
            "--host",
            "127.0.0.1",
            "--port",
            "1",
            "cassandra-stress",
            "write",
            "--native-cql",
            "--prepared",
            "--username",
            "alice",
            "--password",
            "secret",
            "--native-port",
            &native_port.to_string(),
            "--query-write",
            "INSERT INTO ks.tbl (k, v) VALUES (?, ?)",
            "--ops",
            "1",
            "--concurrency",
            "1",
        ])
        .output()
        .expect("Failed to execute cassandra-tools stress auth");

    let stub_result = rx.recv().expect("stub result channel");
    server.join().expect("stub join");
    if let Err(err) = stub_result {
        panic!("native auth stub validation failed: {err}");
    }

    assert!(output.status.success(), "stress command should succeed");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("transport:      native-cql"));
    assert!(stdout.contains("prepared:       true"));
}

#[test]
fn test_cassandra_stress_reprepare_on_unprepared_error() {
    let tools_bin = get_cassandra_tools_path();
    if !tools_bin.exists() {
        println!(
            "Skipping cassandra-tools stress reprepare test: target not found (run cargo build first)"
        );
        return;
    }

    let listener = TcpListener::bind("127.0.0.1:0").expect("bind stub listener");
    let native_port = listener.local_addr().unwrap().port();
    let (tx, rx) = mpsc::channel::<Result<(), String>>();

    let server = thread::spawn(move || {
        let result = (|| -> Result<(), String> {
            let (mut socket, _) = listener.accept().map_err(|e| e.to_string())?;

            let startup = read_frame(&mut socket).map_err(|e| e.to_string())?;
            if startup.version != PROTOCOL_V4_REQUEST || startup.opcode != OPCODE_STARTUP {
                return Err(format!(
                    "expected STARTUP v4, got version=0x{:02X} opcode=0x{:02X}",
                    startup.version, startup.opcode
                ));
            }
            write_response_frame(&mut socket, startup.stream_id, OPCODE_READY, &[])
                .map_err(|e| e.to_string())?;

            let prepare_a = read_frame(&mut socket).map_err(|e| e.to_string())?;
            if prepare_a.opcode != OPCODE_PREPARE {
                return Err(format!(
                    "expected first PREPARE, got opcode=0x{:02X}",
                    prepare_a.opcode
                ));
            }
            let prepared_id_a = b"stress-prepared-a";
            let prepared_a = prepared_result_body(prepared_id_a);
            write_response_frame(&mut socket, prepare_a.stream_id, OPCODE_RESULT, &prepared_a)
                .map_err(|e| e.to_string())?;

            let execute_a = read_frame(&mut socket).map_err(|e| e.to_string())?;
            if execute_a.opcode != OPCODE_EXECUTE {
                return Err(format!(
                    "expected first EXECUTE, got opcode=0x{:02X}",
                    execute_a.opcode
                ));
            }
            let err_unprepared = unprepared_error_body(prepared_id_a);
            write_response_frame(
                &mut socket,
                execute_a.stream_id,
                OPCODE_ERROR,
                &err_unprepared,
            )
            .map_err(|e| e.to_string())?;

            let prepare_b = read_frame(&mut socket).map_err(|e| e.to_string())?;
            if prepare_b.opcode != OPCODE_PREPARE {
                return Err(format!(
                    "expected second PREPARE, got opcode=0x{:02X}",
                    prepare_b.opcode
                ));
            }
            let prepared_id_b = b"stress-prepared-b";
            let prepared_b = prepared_result_body(prepared_id_b);
            write_response_frame(&mut socket, prepare_b.stream_id, OPCODE_RESULT, &prepared_b)
                .map_err(|e| e.to_string())?;

            let execute_b = read_frame(&mut socket).map_err(|e| e.to_string())?;
            if execute_b.opcode != OPCODE_EXECUTE {
                return Err(format!(
                    "expected second EXECUTE, got opcode=0x{:02X}",
                    execute_b.opcode
                ));
            }
            let mut cursor: &[u8] = &execute_b.body;
            let recv_id = read_short_bytes(&mut cursor);
            if recv_id != prepared_id_b {
                return Err(format!(
                    "expected reprepared id {:?}, got {:?}",
                    prepared_id_b, recv_id
                ));
            }

            let mut void_body = Vec::new();
            write_i32_be(&mut void_body, RESULT_KIND_VOID);
            write_response_frame(&mut socket, execute_b.stream_id, OPCODE_RESULT, &void_body)
                .map_err(|e| e.to_string())?;
            Ok(())
        })();
        let _ = tx.send(result);
    });

    let output = Command::new(&tools_bin)
        .args([
            "--host",
            "127.0.0.1",
            "--port",
            "1",
            "cassandra-stress",
            "write",
            "--native-cql",
            "--prepared",
            "--native-port",
            &native_port.to_string(),
            "--query-write",
            "INSERT INTO ks.tbl (k, v) VALUES (?, ?)",
            "--ops",
            "1",
            "--concurrency",
            "1",
        ])
        .output()
        .expect("Failed to execute cassandra-tools stress reprepare");

    let stub_result = rx.recv().expect("stub result channel");
    server.join().expect("stub join");
    if let Err(err) = stub_result {
        panic!("native reprepare stub validation failed: {err}");
    }

    assert!(output.status.success(), "stress command should succeed");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("transport:      native-cql"));
    assert!(stdout.contains("prepared:       true"));
}
