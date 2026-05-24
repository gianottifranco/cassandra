// Licensed under Apache License, Version 2.0.

//! Lightweight Rust implementation of `cassandra-stress` style load generation.
//!
//! Supports two transports:
//! - `admin-http` (default): uses admin API endpoints
//! - `native-cql`: opens native protocol connections and sends CQL QUERY frames

use bytes::{Bytes, BytesMut};
use cassandra_native_protocol::frame::{Frame, FrameHeader, Opcode, PROTOCOL_V4};
use cassandra_native_protocol::message::{SUPPORTED_CQL_VERSION, query_flags};
use cassandra_native_protocol::types::{self, Consistency};
use cassandra_types::{CqlValue, VectorValue};
use reqwest::blocking::Client;
use serde_json::json;
use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{IpAddr, Ipv4Addr, TcpStream, ToSocketAddrs};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StressMode {
    Read,
    Write,
    Mixed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TransportMode {
    AdminHttp,
    NativeCql,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum KeyDistribution {
    Sequential,
    Uniform,
}

#[derive(Debug, Clone)]
struct StressConfig {
    mode: StressMode,
    transport: TransportMode,
    ops: u64,
    concurrency: usize,
    read_path: String,
    write_path: String,
    read_ratio: f64,
    keyspace: Option<String>,
    table: Option<String>,
    native_port: u16,
    query_read: Option<String>,
    query_write: Option<String>,
    value_size: Option<usize>,
    prepared: bool,
    consistency: Consistency,
    rate_ops_per_sec: Option<u64>,
    duration_secs: Option<Duration>,
    warmup_secs: Option<Duration>,
    warmup_ops: Option<u64>,
    keys: u64,
    key_dist: KeyDistribution,
    key_bind_index: usize,
    username: Option<String>,
    password: Option<String>,
}

impl Default for StressConfig {
    fn default() -> Self {
        Self {
            mode: StressMode::Read,
            transport: TransportMode::AdminHttp,
            ops: 1_000,
            concurrency: 8,
            read_path: "/health".to_string(),
            write_path: "/api/v1/operations/flush".to_string(),
            read_ratio: 0.7,
            keyspace: None,
            table: None,
            native_port: 9042,
            query_read: None,
            query_write: None,
            value_size: None,
            prepared: false,
            consistency: Consistency::One,
            rate_ops_per_sec: None,
            duration_secs: None,
            warmup_secs: None,
            warmup_ops: None,
            keys: 1000,
            key_dist: KeyDistribution::Sequential,
            key_bind_index: 0,
            username: None,
            password: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum BindValueKind {
    Ascii,
    Bigint,
    Blob,
    Boolean,
    Counter,
    Decimal,
    Duration,
    Double,
    Float,
    Int,
    Timestamp,
    Uuid,
    Varchar,
    Varint,
    Timeuuid,
    Inet,
    Date,
    Time,
    Smallint,
    Tinyint,
    List(Box<BindValueKind>),
    Map(Box<BindValueKind>, Box<BindValueKind>),
    Set(Box<BindValueKind>),
    Tuple(Vec<BindValueKind>),
    Udt(Vec<(String, BindValueKind)>),
    Vector(Box<BindValueKind>, u32),
    Custom(String),
    Unknown(u16),
}

#[derive(Debug, Clone)]
struct PreparedPlan {
    id: Vec<u8>,
    query: String,
    bind_kinds: Vec<BindValueKind>,
}

#[derive(Debug, Clone, PartialEq)]
enum BoundValue {
    Bytes(Vec<u8>),
    Unset,
}

#[derive(Debug, Clone)]
struct StressReport {
    total_ops: u64,
    executed_ops: u64,
    warmup_ops: u64,
    ok_ops: u64,
    failed_ops: u64,
    elapsed_secs: f64,
    measured_elapsed_secs: f64,
    throughput_ops: f64,
    avg_latency_ms: f64,
    p50_latency_ms: f64,
    p95_latency_ms: f64,
    p99_latency_ms: f64,
    sample_errors: Vec<String>,
}

fn usage() -> &'static str {
    "Usage:
  cassandra-tools cassandra-stress <read|write|mixed> [options]

Options:
  --ops <N> | n=<N>               Number of operations (default: 1000)
  --concurrency <N> | threads=<N> Worker threads (default: 8)
  --rate <OPS_PER_SEC> | rate=<N> Target total throughput (ops/s)
  --duration <DUR> | duration=<DUR> Stop after duration (e.g. 30s, 2m, 500ms)
  --warmup <DUR> | warmup=<DUR> Warmup period excluded from metrics
  --warmup-ops <N> | warmup-ops=<N> Warmup operation count excluded from metrics
  --keys <N> | keys=<N>           Key cardinality (default: 1000)
  --id-dist <sequential|uniform>  Key distribution (default: sequential)
  --key-bind-index <N>            Prepared bind position for key_id (default: 0)
  --value-size <BYTES> | value-size=<N> Value payload size for default writes
  --read-ratio <0..1>             Mixed-mode read ratio (default: 0.7)

  --transport <admin-http|native-cql>  Transport mode (default: admin-http)
  --native-cql                    Shorthand for --transport native-cql
  --native-port <PORT>            Native protocol port (default: 9042)
  --query-read <CQL>              Custom read query (native-cql only)
  --query-write <CQL>             Custom write query (native-cql only)
  --prepared                      Use PREPARE/EXECUTE in native-cql mode
  --consistency <LEVEL>           Native CQL consistency (default: ONE)
  --username <USER>               Native auth username (requires --password)
  --password <PASS>               Native auth password (requires --username)

  --read-path <PATH>              Read endpoint path (admin-http default: /health)
  --write-path <PATH>             Write endpoint path (admin-http default: /api/v1/operations/flush)
  --keyspace <KS>                 Optional keyspace (used for native write setup if no custom query)
  --table <TABLE>                 Optional table (used for native write setup if no custom query)

Examples:
  cassandra-tools cassandra-stress read --ops 5000 --concurrency 16
  cassandra-tools cassandra-stress write n=1000 --native-cql --keyspace ks --table kv
  cassandra-tools cassandra-stress mixed --transport native-cql --ops 20000 --read-ratio 0.8"
}

fn parse_u64(value: &str, field: &str) -> anyhow::Result<u64> {
    value
        .parse::<u64>()
        .map_err(|e| anyhow::anyhow!("invalid {} '{}': {}", field, value, e))
}

fn parse_usize(value: &str, field: &str) -> anyhow::Result<usize> {
    value
        .parse::<usize>()
        .map_err(|e| anyhow::anyhow!("invalid {} '{}': {}", field, value, e))
}

fn parse_u16(value: &str, field: &str) -> anyhow::Result<u16> {
    value
        .parse::<u16>()
        .map_err(|e| anyhow::anyhow!("invalid {} '{}': {}", field, value, e))
}

fn parse_f64(value: &str, field: &str) -> anyhow::Result<f64> {
    value
        .parse::<f64>()
        .map_err(|e| anyhow::anyhow!("invalid {} '{}': {}", field, value, e))
}

fn parse_duration(value: &str, field: &str) -> anyhow::Result<Duration> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        anyhow::bail!("invalid {} '{}': empty duration", field, value);
    }
    let lower = trimmed.to_ascii_lowercase();
    let (num_part, unit) = if let Some(num) = lower.strip_suffix("ms") {
        (num, "ms")
    } else if let Some(num) = lower.strip_suffix('s') {
        (num, "s")
    } else if let Some(num) = lower.strip_suffix('m') {
        (num, "m")
    } else if let Some(num) = lower.strip_suffix('h') {
        (num, "h")
    } else {
        (lower.as_str(), "s")
    };
    let n = num_part
        .parse::<u64>()
        .map_err(|e| anyhow::anyhow!("invalid {} '{}': {}", field, value, e))?;
    let duration = match unit {
        "ms" => Duration::from_millis(n),
        "s" => Duration::from_secs(n),
        "m" => Duration::from_secs(
            n.checked_mul(60)
                .ok_or_else(|| anyhow::anyhow!("{} '{}' overflows", field, value))?,
        ),
        "h" => Duration::from_secs(
            n.checked_mul(3600)
                .ok_or_else(|| anyhow::anyhow!("{} '{}' overflows", field, value))?,
        ),
        _ => unreachable!(),
    };
    Ok(duration)
}

fn format_duration(duration: Duration) -> String {
    let secs = duration.as_secs();
    if duration.subsec_nanos() == 0 {
        if secs >= 3600 && secs.is_multiple_of(3600) {
            return format!("{}h", secs / 3600);
        }
        if secs >= 60 && secs.is_multiple_of(60) {
            return format!("{}m", secs / 60);
        }
        if secs > 0 {
            return format!("{secs}s");
        }
    }
    format!("{}ms", duration.as_millis())
}

fn parse_consistency(value: &str) -> anyhow::Result<Consistency> {
    let normalized = value.trim().to_ascii_uppercase().replace('-', "_");
    let consistency = match normalized.as_str() {
        "ANY" => Consistency::Any,
        "ONE" => Consistency::One,
        "TWO" => Consistency::Two,
        "THREE" => Consistency::Three,
        "QUORUM" => Consistency::Quorum,
        "ALL" => Consistency::All,
        "LOCAL_QUORUM" => Consistency::LocalQuorum,
        "EACH_QUORUM" => Consistency::EachQuorum,
        "SERIAL" => Consistency::Serial,
        "LOCAL_SERIAL" => Consistency::LocalSerial,
        "LOCAL_ONE" => Consistency::LocalOne,
        _ => {
            anyhow::bail!(
                "invalid --consistency '{}'; expected one of ANY, ONE, TWO, THREE, QUORUM, ALL, LOCAL_QUORUM, EACH_QUORUM, SERIAL, LOCAL_SERIAL, LOCAL_ONE",
                value
            )
        }
    };
    Ok(consistency)
}

fn parse_key_distribution(value: &str) -> anyhow::Result<KeyDistribution> {
    match value.trim().to_ascii_lowercase().as_str() {
        "sequential" | "seq" => Ok(KeyDistribution::Sequential),
        "uniform" | "rand" | "random" => Ok(KeyDistribution::Uniform),
        _ => anyhow::bail!(
            "invalid --id-dist '{}'; expected sequential or uniform",
            value
        ),
    }
}

fn parse_args(args: &[String]) -> anyhow::Result<StressConfig> {
    if args.is_empty() {
        anyhow::bail!("{}", usage());
    }
    if args.iter().any(|arg| arg == "-h" || arg == "--help") {
        anyhow::bail!("{}", usage());
    }

    let mut cfg = StressConfig::default();
    cfg.mode = match args[0].as_str() {
        "read" => StressMode::Read,
        "write" => StressMode::Write,
        "mixed" => StressMode::Mixed,
        other => anyhow::bail!("unknown stress mode '{}'\n\n{}", other, usage()),
    };

    let mut i = 1usize;
    while i < args.len() {
        let arg = args[i].as_str();
        match arg {
            "--ops" => {
                i += 1;
                if i >= args.len() {
                    anyhow::bail!("--ops requires a value");
                }
                cfg.ops = parse_u64(&args[i], "ops")?;
            }
            "--concurrency" => {
                i += 1;
                if i >= args.len() {
                    anyhow::bail!("--concurrency requires a value");
                }
                cfg.concurrency = parse_usize(&args[i], "concurrency")?;
            }
            "--rate" => {
                i += 1;
                if i >= args.len() {
                    anyhow::bail!("--rate requires a value");
                }
                cfg.rate_ops_per_sec = Some(parse_u64(&args[i], "rate")?);
            }
            "--duration" => {
                i += 1;
                if i >= args.len() {
                    anyhow::bail!("--duration requires a value");
                }
                cfg.duration_secs = Some(parse_duration(&args[i], "duration")?);
            }
            "--warmup" => {
                i += 1;
                if i >= args.len() {
                    anyhow::bail!("--warmup requires a value");
                }
                cfg.warmup_secs = Some(parse_duration(&args[i], "warmup")?);
            }
            "--warmup-ops" => {
                i += 1;
                if i >= args.len() {
                    anyhow::bail!("--warmup-ops requires a value");
                }
                cfg.warmup_ops = Some(parse_u64(&args[i], "warmup-ops")?);
            }
            "--keys" => {
                i += 1;
                if i >= args.len() {
                    anyhow::bail!("--keys requires a value");
                }
                cfg.keys = parse_u64(&args[i], "keys")?;
            }
            "--id-dist" => {
                i += 1;
                if i >= args.len() {
                    anyhow::bail!("--id-dist requires a value");
                }
                cfg.key_dist = parse_key_distribution(&args[i])?;
            }
            "--key-bind-index" => {
                i += 1;
                if i >= args.len() {
                    anyhow::bail!("--key-bind-index requires a value");
                }
                cfg.key_bind_index = parse_usize(&args[i], "key-bind-index")?;
            }
            "--read-path" => {
                i += 1;
                if i >= args.len() {
                    anyhow::bail!("--read-path requires a value");
                }
                cfg.read_path = args[i].clone();
            }
            "--write-path" => {
                i += 1;
                if i >= args.len() {
                    anyhow::bail!("--write-path requires a value");
                }
                cfg.write_path = args[i].clone();
            }
            "--read-ratio" => {
                i += 1;
                if i >= args.len() {
                    anyhow::bail!("--read-ratio requires a value");
                }
                cfg.read_ratio = parse_f64(&args[i], "read-ratio")?;
            }
            "--transport" => {
                i += 1;
                if i >= args.len() {
                    anyhow::bail!("--transport requires a value");
                }
                cfg.transport = match args[i].as_str() {
                    "admin-http" => TransportMode::AdminHttp,
                    "native-cql" => TransportMode::NativeCql,
                    other => anyhow::bail!("invalid --transport '{}'", other),
                };
            }
            "--native-cql" => cfg.transport = TransportMode::NativeCql,
            "--native-port" => {
                i += 1;
                if i >= args.len() {
                    anyhow::bail!("--native-port requires a value");
                }
                cfg.native_port = parse_u16(&args[i], "native-port")?;
            }
            "--query-read" => {
                i += 1;
                if i >= args.len() {
                    anyhow::bail!("--query-read requires a value");
                }
                cfg.query_read = Some(args[i].clone());
            }
            "--query-write" => {
                i += 1;
                if i >= args.len() {
                    anyhow::bail!("--query-write requires a value");
                }
                cfg.query_write = Some(args[i].clone());
            }
            "--value-size" => {
                i += 1;
                if i >= args.len() {
                    anyhow::bail!("--value-size requires a value");
                }
                cfg.value_size = Some(parse_usize(&args[i], "value-size")?);
            }
            "--prepared" => cfg.prepared = true,
            "--consistency" => {
                i += 1;
                if i >= args.len() {
                    anyhow::bail!("--consistency requires a value");
                }
                cfg.consistency = parse_consistency(&args[i])?;
            }
            "--username" => {
                i += 1;
                if i >= args.len() {
                    anyhow::bail!("--username requires a value");
                }
                cfg.username = Some(args[i].clone());
            }
            "--password" => {
                i += 1;
                if i >= args.len() {
                    anyhow::bail!("--password requires a value");
                }
                cfg.password = Some(args[i].clone());
            }
            "--keyspace" => {
                i += 1;
                if i >= args.len() {
                    anyhow::bail!("--keyspace requires a value");
                }
                cfg.keyspace = Some(args[i].clone());
            }
            "--table" => {
                i += 1;
                if i >= args.len() {
                    anyhow::bail!("--table requires a value");
                }
                cfg.table = Some(args[i].clone());
            }
            _ if arg.starts_with("n=") => cfg.ops = parse_u64(&arg[2..], "ops")?,
            _ if arg.starts_with("threads=") => {
                cfg.concurrency = parse_usize(&arg[8..], "concurrency")?
            }
            _ if arg.starts_with("rate=") => {
                cfg.rate_ops_per_sec = Some(parse_u64(&arg[5..], "rate")?)
            }
            _ if arg.starts_with("duration=") => {
                cfg.duration_secs = Some(parse_duration(&arg[9..], "duration")?)
            }
            _ if arg.starts_with("warmup=") => {
                cfg.warmup_secs = Some(parse_duration(&arg[7..], "warmup")?)
            }
            _ if arg.starts_with("warmup-ops=") => {
                cfg.warmup_ops = Some(parse_u64(&arg[11..], "warmup-ops")?)
            }
            _ if arg.starts_with("keys=") => cfg.keys = parse_u64(&arg[5..], "keys")?,
            _ if arg.starts_with("id-dist=") => cfg.key_dist = parse_key_distribution(&arg[8..])?,
            _ if arg.starts_with("key-bind-index=") => {
                cfg.key_bind_index = parse_usize(&arg[15..], "key-bind-index")?
            }
            _ if arg.starts_with("value-size=") => {
                cfg.value_size = Some(parse_usize(&arg[11..], "value-size")?)
            }
            _ => anyhow::bail!("unknown argument '{}'\n\n{}", arg, usage()),
        }
        i += 1;
    }

    if cfg.ops == 0 {
        anyhow::bail!("ops must be > 0");
    }
    if cfg.concurrency == 0 {
        anyhow::bail!("concurrency must be > 0");
    }
    if cfg.rate_ops_per_sec == Some(0) {
        anyhow::bail!("rate must be > 0");
    }
    if cfg.duration_secs.is_some_and(|duration| duration.is_zero()) {
        anyhow::bail!("duration must be > 0");
    }
    if cfg.warmup_secs.is_some_and(|duration| duration.is_zero()) {
        anyhow::bail!("warmup must be > 0");
    }
    if cfg.warmup_ops == Some(0) {
        anyhow::bail!("warmup-ops must be > 0");
    }
    if cfg.keys == 0 {
        anyhow::bail!("keys must be > 0");
    }
    if cfg.value_size == Some(0) {
        anyhow::bail!("value-size must be > 0");
    }
    if !(0.0..=1.0).contains(&cfg.read_ratio) {
        anyhow::bail!("read-ratio must be between 0.0 and 1.0");
    }
    if cfg.username.is_some() ^ cfg.password.is_some() {
        anyhow::bail!("--username and --password must be provided together");
    }

    if cfg.transport == TransportMode::NativeCql
        && matches!(cfg.mode, StressMode::Write | StressMode::Mixed)
    {
        if cfg.keyspace.is_none() {
            cfg.keyspace = Some("stress_native".to_string());
        }
        if cfg.table.is_none() {
            cfg.table = Some("kv".to_string());
        }
    }

    Ok(cfg)
}

fn percentile(sorted_us: &[u64], p: f64) -> f64 {
    if sorted_us.is_empty() {
        return 0.0;
    }
    let idx = ((sorted_us.len() - 1) as f64 * p).round() as usize;
    sorted_us[idx] as f64 / 1_000.0
}

fn scheduled_offset_for_op(op_id: u64, rate_ops_per_sec: u64) -> Duration {
    let micros = op_id.saturating_mul(1_000_000) / rate_ops_per_sec;
    Duration::from_micros(micros)
}

fn maybe_pace_op(run_start: Instant, op_id: u64, rate_ops_per_sec: Option<u64>) {
    let Some(rate) = rate_ops_per_sec else {
        return;
    };
    let target = run_start + scheduled_offset_for_op(op_id, rate);
    let now = Instant::now();
    if target > now {
        thread::sleep(target.duration_since(now));
    }
}

fn duration_deadline(run_start: Instant, duration_secs: Option<Duration>) -> Option<Instant> {
    duration_secs.map(|duration| run_start + duration)
}

fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E3779B97F4A7C15);
    let mut z = x;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
    z ^ (z >> 31)
}

fn key_id_for(cfg: &StressConfig, op_id: u64) -> u64 {
    match cfg.key_dist {
        KeyDistribution::Sequential => op_id % cfg.keys,
        KeyDistribution::Uniform => splitmix64(op_id) % cfg.keys,
    }
}

fn execute_one_admin(
    client: &Client,
    base_url: &str,
    mode: StressMode,
    cfg: &StressConfig,
) -> anyhow::Result<()> {
    match mode {
        StressMode::Read => {
            let url = format!("{}{}", base_url, cfg.read_path);
            let resp = client.get(&url).send()?;
            if !resp.status().is_success() {
                anyhow::bail!("HTTP {} from {}", resp.status(), url);
            }
            Ok(())
        }
        StressMode::Write => {
            let url = format!("{}{}", base_url, cfg.write_path);
            let mut body = serde_json::Map::new();
            if let Some(ks) = &cfg.keyspace {
                body.insert("keyspace".to_string(), json!(ks));
            }
            if let Some(tbl) = &cfg.table {
                body.insert("table".to_string(), json!(tbl));
            }
            let resp = client.post(&url).json(&body).send()?;
            if !resp.status().is_success() {
                anyhow::bail!("HTTP {} from {}", resp.status(), url);
            }
            Ok(())
        }
        StressMode::Mixed => unreachable!("mixed is expanded at call-site"),
    }
}

fn connect_tcp(addr: &str) -> anyhow::Result<TcpStream> {
    let addrs: Vec<_> = addr.to_socket_addrs()?.collect();
    if addrs.is_empty() {
        anyhow::bail!("no socket addresses resolved for {}", addr);
    }
    let mut last_err = None;
    for socket in addrs {
        match TcpStream::connect_timeout(&socket, Duration::from_secs(3)) {
            Ok(stream) => return Ok(stream),
            Err(e) => last_err = Some(e),
        }
    }
    anyhow::bail!(
        "failed to connect to {}: {}",
        addr,
        last_err
            .map(|e| e.to_string())
            .unwrap_or_else(|| "unknown error".to_string())
    )
}

fn write_frame(stream: &mut TcpStream, frame: &Frame) -> anyhow::Result<()> {
    let mut header_buf = BytesMut::new();
    frame.header.encode(&mut header_buf);
    stream.write_all(&header_buf)?;
    stream.write_all(&frame.body)?;
    Ok(())
}

fn read_frame(stream: &mut TcpStream) -> anyhow::Result<Frame> {
    let mut header_bytes = [0u8; FrameHeader::SIZE];
    stream.read_exact(&mut header_bytes)?;
    let header = FrameHeader::decode(&header_bytes)?;
    let mut body = vec![0u8; header.length as usize];
    stream.read_exact(&mut body)?;
    Ok(Frame {
        header,
        body: Bytes::from(body),
    })
}

fn error_from_frame(frame: &Frame) -> String {
    let mut cursor: &[u8] = &frame.body;
    let code = types::read_int(&mut cursor).unwrap_or_default();
    let msg = types::read_string(&mut cursor).unwrap_or_else(|_| "unknown error".to_string());
    format!("native error 0x{code:04X}: {msg}")
}

#[derive(Debug, Clone)]
struct NativeError {
    code: i32,
    message: String,
    unprepared_id: Option<Vec<u8>>,
}

fn parse_native_error(frame: &Frame) -> anyhow::Result<NativeError> {
    if frame.header.opcode != Opcode::Error {
        anyhow::bail!("expected ERROR frame, got {:?}", frame.header.opcode);
    }
    let mut cursor: &[u8] = &frame.body;
    let code = types::read_int(&mut cursor)?;
    let message = types::read_string(&mut cursor)?;
    let unprepared_id = if code == 0x2500 {
        types::read_short_bytes(&mut cursor).ok()
    } else {
        None
    };
    Ok(NativeError {
        code,
        message,
        unprepared_id,
    })
}

fn startup_frame(stream_id: i16) -> Frame {
    let mut opts = HashMap::new();
    opts.insert("CQL_VERSION".to_string(), SUPPORTED_CQL_VERSION.to_string());
    let mut body = BytesMut::new();
    types::write_string_map(&mut body, &opts);
    Frame {
        header: FrameHeader {
            version: PROTOCOL_V4,
            flags: 0,
            stream_id,
            opcode: Opcode::Startup,
            length: body.len() as u32,
        },
        body: body.freeze(),
    }
}

fn query_frame(cql: &str, consistency: Consistency, stream_id: i16) -> Frame {
    let mut body = BytesMut::new();
    types::write_long_string(&mut body, cql);
    types::write_consistency(&mut body, consistency);
    types::write_byte(&mut body, 0);
    Frame {
        header: FrameHeader {
            version: PROTOCOL_V4,
            flags: 0,
            stream_id,
            opcode: Opcode::Query,
            length: body.len() as u32,
        },
        body: body.freeze(),
    }
}

fn prepare_frame(cql: &str, stream_id: i16) -> Frame {
    let mut body = BytesMut::new();
    types::write_long_string(&mut body, cql);
    Frame {
        header: FrameHeader {
            version: PROTOCOL_V4,
            flags: 0,
            stream_id,
            opcode: Opcode::Prepare,
            length: body.len() as u32,
        },
        body: body.freeze(),
    }
}

fn auth_response_frame(token: &[u8], stream_id: i16) -> Frame {
    let mut body = BytesMut::new();
    types::write_bytes_opt(&mut body, Some(token));
    Frame {
        header: FrameHeader {
            version: PROTOCOL_V4,
            flags: 0,
            stream_id,
            opcode: Opcode::AuthResponse,
            length: body.len() as u32,
        },
        body: body.freeze(),
    }
}

fn write_bound_value(buf: &mut BytesMut, value: &BoundValue) {
    match value {
        BoundValue::Bytes(bytes) => types::write_bytes_opt(buf, Some(bytes)),
        BoundValue::Unset => types::write_int(buf, -2),
    }
}

fn execute_frame(
    prepared_id: &[u8],
    values: &[BoundValue],
    consistency: Consistency,
    stream_id: i16,
) -> Frame {
    let mut body = BytesMut::new();
    types::write_short_bytes(&mut body, prepared_id);
    types::write_consistency(&mut body, consistency);
    let flags = if values.is_empty() {
        0
    } else {
        query_flags::VALUES as u8
    };
    types::write_byte(&mut body, flags);
    if !values.is_empty() {
        types::write_short(&mut body, values.len() as u16);
        for value in values {
            write_bound_value(&mut body, value);
        }
    }
    Frame {
        header: FrameHeader {
            version: PROTOCOL_V4,
            flags: 0,
            stream_id,
            opcode: Opcode::Execute,
            length: body.len() as u32,
        },
        body: body.freeze(),
    }
}

const ROWS_FLAG_GLOBAL_TABLES_SPEC: i32 = 0x0001;
const ROWS_FLAG_HAS_MORE_PAGES: i32 = 0x0002;
const ROWS_FLAG_NO_METADATA: i32 = 0x0004;
const ROWS_FLAG_METADATA_CHANGED: i32 = 0x0008;

fn decode_bind_value_kind(cursor: &mut &[u8]) -> anyhow::Result<BindValueKind> {
    let type_id = types::read_short(cursor)?;
    let kind = match type_id {
        0x0000 => {
            let custom = types::read_string(cursor)?;
            BindValueKind::Custom(custom)
        }
        0x0001 => BindValueKind::Ascii,
        0x0002 => BindValueKind::Bigint,
        0x0003 => BindValueKind::Blob,
        0x0004 => BindValueKind::Boolean,
        0x0005 => BindValueKind::Counter,
        0x0006 => BindValueKind::Decimal,
        0x0007 => BindValueKind::Double,
        0x0008 => BindValueKind::Float,
        0x0009 => BindValueKind::Int,
        0x000B => BindValueKind::Timestamp,
        0x000C => BindValueKind::Uuid,
        0x000D => BindValueKind::Varchar,
        0x000E => BindValueKind::Varint,
        0x000F => BindValueKind::Timeuuid,
        0x0010 => BindValueKind::Inet,
        0x0011 => BindValueKind::Date,
        0x0012 => BindValueKind::Time,
        0x0013 => BindValueKind::Smallint,
        0x0014 => BindValueKind::Tinyint,
        0x0015 => BindValueKind::Duration,
        0x0020 => {
            let inner = decode_bind_value_kind(cursor)?;
            BindValueKind::List(Box::new(inner))
        }
        0x0021 => {
            let key = decode_bind_value_kind(cursor)?;
            let val = decode_bind_value_kind(cursor)?;
            BindValueKind::Map(Box::new(key), Box::new(val))
        }
        0x0022 => {
            let inner = decode_bind_value_kind(cursor)?;
            BindValueKind::Set(Box::new(inner))
        }
        0x0030 => {
            let _ks = types::read_string(cursor)?;
            let _name = types::read_string(cursor)?;
            let fields = types::read_short(cursor)? as usize;
            let mut field_specs = Vec::with_capacity(fields);
            for _ in 0..fields {
                let field_name = types::read_string(cursor)?;
                let field_type = decode_bind_value_kind(cursor)?;
                field_specs.push((field_name, field_type));
            }
            BindValueKind::Udt(field_specs)
        }
        0x0031 => {
            let tuple_len = types::read_short(cursor)? as usize;
            let mut items = Vec::with_capacity(tuple_len);
            for _ in 0..tuple_len {
                items.push(decode_bind_value_kind(cursor)?);
            }
            BindValueKind::Tuple(items)
        }
        0x0032 => {
            let inner = decode_bind_value_kind(cursor)?;
            let dims = types::read_int(cursor)?;
            if dims <= 0 {
                BindValueKind::Unknown(type_id)
            } else {
                BindValueKind::Vector(Box::new(inner), dims as u32)
            }
        }
        other => BindValueKind::Unknown(other),
    };
    Ok(kind)
}

fn parse_rows_metadata_bind_kinds(cursor: &mut &[u8]) -> anyhow::Result<Vec<BindValueKind>> {
    let flags = types::read_int(cursor)?;
    let columns_count = read_non_negative_count(cursor, "rows columns_count")?;

    if flags & ROWS_FLAG_HAS_MORE_PAGES != 0 {
        let _paging = types::read_bytes(cursor)?;
    }
    if flags & ROWS_FLAG_METADATA_CHANGED != 0 {
        let _metadata_id = types::read_short_bytes(cursor)?;
    }
    if flags & ROWS_FLAG_NO_METADATA != 0 {
        return Ok(Vec::new());
    }

    let has_global_table_spec = flags & ROWS_FLAG_GLOBAL_TABLES_SPEC != 0;
    if has_global_table_spec {
        let _ks = types::read_string(cursor)?;
        let _tbl = types::read_string(cursor)?;
    }

    let mut bind_kinds = Vec::with_capacity(columns_count);
    for _ in 0..columns_count {
        if !has_global_table_spec {
            let _ks = types::read_string(cursor)?;
            let _tbl = types::read_string(cursor)?;
        }
        let _name = types::read_string(cursor)?;
        bind_kinds.push(decode_bind_value_kind(cursor)?);
    }
    Ok(bind_kinds)
}

fn parse_prepared_bind_metadata_bind_kinds(
    cursor: &mut &[u8],
) -> anyhow::Result<Vec<BindValueKind>> {
    let flags = types::read_int(cursor)?;
    let columns_count = read_non_negative_count(cursor, "prepared columns_count")?;

    if flags & ROWS_FLAG_HAS_MORE_PAGES != 0 {
        let _paging = types::read_bytes(cursor)?;
    }
    if flags & ROWS_FLAG_METADATA_CHANGED != 0 {
        let _metadata_id = types::read_short_bytes(cursor)?;
    }

    // PREPARED bind metadata includes partition key indexes before column specs.
    let pk_count = read_non_negative_count(cursor, "prepared pk_count")?;
    for _ in 0..pk_count {
        let _pk_index = types::read_short(cursor)?;
    }

    if flags & ROWS_FLAG_NO_METADATA != 0 {
        return Ok(Vec::new());
    }

    let has_global_table_spec = flags & ROWS_FLAG_GLOBAL_TABLES_SPEC != 0;
    if has_global_table_spec {
        let _ks = types::read_string(cursor)?;
        let _tbl = types::read_string(cursor)?;
    }

    let mut bind_kinds = Vec::with_capacity(columns_count);
    for _ in 0..columns_count {
        if !has_global_table_spec {
            let _ks = types::read_string(cursor)?;
            let _tbl = types::read_string(cursor)?;
        }
        let _name = types::read_string(cursor)?;
        bind_kinds.push(decode_bind_value_kind(cursor)?);
    }
    Ok(bind_kinds)
}

fn read_non_negative_count(cursor: &mut &[u8], label: &str) -> anyhow::Result<usize> {
    let raw = types::read_int(cursor)?;
    if raw < 0 {
        anyhow::bail!("{label} must be >= 0, got {raw}");
    }
    Ok(raw as usize)
}

fn parse_prepared_plan(frame: &Frame) -> anyhow::Result<PreparedPlan> {
    if frame.header.opcode == Opcode::Error {
        anyhow::bail!("{}", error_from_frame(frame));
    }
    if frame.header.opcode != Opcode::Result {
        anyhow::bail!(
            "unexpected PREPARE response opcode {:?}",
            frame.header.opcode
        );
    }
    let mut cursor: &[u8] = &frame.body;
    let kind = types::read_int(&mut cursor)?;
    if kind != 0x0004 {
        anyhow::bail!("unexpected PREPARE result kind {kind}");
    }
    let id = types::read_short_bytes(&mut cursor)?;
    let bind_kinds = parse_prepared_bind_metadata_bind_kinds(&mut cursor)?;
    let _result_metadata = parse_rows_metadata_bind_kinds(&mut cursor)?;
    Ok(PreparedPlan {
        id,
        query: String::new(),
        bind_kinds,
    })
}

fn parse_authenticator_name(frame: &Frame) -> Option<String> {
    if frame.header.opcode != Opcode::Authenticate {
        return None;
    }
    let mut cursor: &[u8] = &frame.body;
    types::read_string(&mut cursor).ok()
}

fn parse_auth_challenge(frame: &Frame) -> Option<Option<Vec<u8>>> {
    if frame.header.opcode != Opcode::AuthChallenge {
        return None;
    }
    let mut cursor: &[u8] = &frame.body;
    types::read_bytes(&mut cursor).ok()
}

fn sasl_plain_token(username: &str, password: &str) -> Vec<u8> {
    let mut token = Vec::with_capacity(username.len() + password.len() + 2);
    token.push(0);
    token.extend_from_slice(username.as_bytes());
    token.push(0);
    token.extend_from_slice(password.as_bytes());
    token
}

#[derive(Debug, Clone)]
struct NativeAuthCredentials {
    username: String,
    password: String,
}

fn initial_auth_token(auth: &NativeAuthCredentials) -> Vec<u8> {
    sasl_plain_token(&auth.username, &auth.password)
}

struct NativeSession {
    stream: TcpStream,
}

impl NativeSession {
    fn connect(addr: &str, auth: Option<NativeAuthCredentials>) -> anyhow::Result<Self> {
        let mut stream = connect_tcp(addr)?;
        stream.set_nodelay(true)?;
        stream.set_read_timeout(Some(Duration::from_secs(5)))?;
        stream.set_write_timeout(Some(Duration::from_secs(5)))?;

        let startup = startup_frame(1);
        write_frame(&mut stream, &startup)?;
        let response = read_frame(&mut stream)?;
        match response.header.opcode {
            Opcode::Ready => Ok(Self { stream }),
            Opcode::Authenticate => {
                let creds = auth.ok_or_else(|| {
                    let authenticator = parse_authenticator_name(&response)
                        .unwrap_or_else(|| "unknown authenticator".to_string());
                    anyhow::anyhow!(
                        "server requires native authentication ({authenticator}); pass --username and --password"
                    )
                })?;
                let token = initial_auth_token(&creds);
                let mut stream_id = 2i16;
                let max_rounds = 8usize;
                for _ in 0..max_rounds {
                    let auth_frame = auth_response_frame(&token, stream_id);
                    write_frame(&mut stream, &auth_frame)?;
                    let auth_response = read_frame(&mut stream)?;
                    match auth_response.header.opcode {
                        Opcode::AuthSuccess | Opcode::Ready => return Ok(Self { stream }),
                        Opcode::Error => anyhow::bail!("{}", error_from_frame(&auth_response)),
                        Opcode::AuthChallenge => {
                            let _challenge = parse_auth_challenge(&auth_response);
                            stream_id = stream_id.saturating_add(1);
                            continue;
                        }
                        other => anyhow::bail!("unexpected auth response opcode {:?}", other),
                    }
                }
                anyhow::bail!("authentication challenge sequence exceeded {max_rounds} rounds");
            }
            Opcode::Error => anyhow::bail!("{}", error_from_frame(&response)),
            other => anyhow::bail!("unexpected STARTUP response opcode {:?}", other),
        }
    }

    fn query(&mut self, cql: &str, consistency: Consistency) -> anyhow::Result<()> {
        let frame = query_frame(cql, consistency, 2);
        write_frame(&mut self.stream, &frame)?;
        let response = read_frame(&mut self.stream)?;
        match response.header.opcode {
            Opcode::Result => Ok(()),
            Opcode::Error => anyhow::bail!("{}", error_from_frame(&response)),
            other => anyhow::bail!("unexpected QUERY response opcode {:?}", other),
        }
    }

    fn prepare(&mut self, cql: &str) -> anyhow::Result<PreparedPlan> {
        let frame = prepare_frame(cql, 3);
        write_frame(&mut self.stream, &frame)?;
        let response = read_frame(&mut self.stream)?;
        let mut plan = parse_prepared_plan(&response)?;
        plan.query = cql.to_string();
        Ok(plan)
    }

    fn execute_once(
        &mut self,
        prepared_id: &[u8],
        values: &[BoundValue],
        consistency: Consistency,
    ) -> anyhow::Result<()> {
        let frame = execute_frame(prepared_id, values, consistency, 4);
        write_frame(&mut self.stream, &frame)?;
        let response = read_frame(&mut self.stream)?;
        match response.header.opcode {
            Opcode::Result => Ok(()),
            Opcode::Error => {
                let parsed = parse_native_error(&response)?;
                anyhow::bail!("native error 0x{:04X}: {}", parsed.code, parsed.message);
            }
            other => anyhow::bail!("unexpected EXECUTE response opcode {:?}", other),
        }
    }

    fn execute_with_reprepare(
        &mut self,
        plan: &mut PreparedPlan,
        values: &[BoundValue],
        consistency: Consistency,
    ) -> anyhow::Result<()> {
        let frame = execute_frame(&plan.id, values, consistency, 4);
        write_frame(&mut self.stream, &frame)?;
        let response = read_frame(&mut self.stream)?;
        match response.header.opcode {
            Opcode::Result => Ok(()),
            Opcode::Error => {
                let parsed = parse_native_error(&response)?;
                if parsed.code == 0x2500 {
                    // Retry once by re-preparing on this connection.
                    let refreshed = self.prepare(&plan.query)?;
                    let _unprepared_id = parsed.unprepared_id;
                    plan.id = refreshed.id;
                    plan.bind_kinds = refreshed.bind_kinds;
                    return self.execute_once(&plan.id, values, consistency);
                }
                anyhow::bail!("native error 0x{:04X}: {}", parsed.code, parsed.message);
            }
            other => anyhow::bail!("unexpected EXECUTE response opcode {:?}", other),
        }
    }
}

fn render_query_template(template: &str, op_id: u64, key_id: u64, value: Option<&str>) -> String {
    let mut rendered = template
        .replace("${id}", &op_id.to_string())
        .replace("${key}", &key_id.to_string())
        .replace("{id}", &op_id.to_string())
        .replace("{key}", &key_id.to_string());
    if let Some(value) = value {
        rendered = rendered
            .replace("${value}", value)
            .replace("{value}", value);
    }
    rendered
}

fn deterministic_value_payload(op_id: u64, size: usize) -> String {
    let seed = format!("{op_id:016x}");
    let mut payload = String::with_capacity(size);
    while payload.len() < size {
        let remain = size - payload.len();
        if remain >= seed.len() {
            payload.push_str(&seed);
        } else {
            payload.push_str(&seed[..remain]);
        }
    }
    payload
}

fn default_read_query(cfg: &StressConfig, op_id: u64, key_id: u64) -> String {
    if let Some(query) = &cfg.query_read {
        return render_query_template(query, op_id, key_id, None);
    }
    if let (Some(ks), Some(tbl)) = (&cfg.keyspace, &cfg.table) {
        return format!("SELECT v FROM {ks}.{tbl} WHERE k = {key_id}");
    }
    "SELECT key FROM system.local LIMIT 1".to_string()
}

fn default_write_query(cfg: &StressConfig, op_id: u64, key_id: u64) -> String {
    let payload = cfg
        .value_size
        .map(|size| deterministic_value_payload(op_id, size))
        .unwrap_or_else(|| format!("v{op_id}"));
    if let Some(query) = &cfg.query_write {
        return render_query_template(query, op_id, key_id, Some(&payload));
    }
    let ks = cfg.keyspace.as_deref().unwrap_or("stress_native");
    let tbl = cfg.table.as_deref().unwrap_or("kv");
    format!("INSERT INTO {ks}.{tbl} (k, v) VALUES ({key_id}, '{payload}')")
}

fn default_prepared_read_query(cfg: &StressConfig) -> String {
    if let Some(query) = &cfg.query_read {
        return query.clone();
    }
    if let (Some(ks), Some(tbl)) = (&cfg.keyspace, &cfg.table) {
        return format!("SELECT v FROM {ks}.{tbl} WHERE k = ?");
    }
    "SELECT key FROM system.local LIMIT 1".to_string()
}

fn default_prepared_write_query(cfg: &StressConfig) -> String {
    if let Some(query) = &cfg.query_write {
        return query.clone();
    }
    let ks = cfg.keyspace.as_deref().unwrap_or("stress_native");
    let tbl = cfg.table.as_deref().unwrap_or("kv");
    format!("INSERT INTO {ks}.{tbl} (k, v) VALUES (?, ?)")
}

fn bind_cql_value_for_kind(
    kind: &BindValueKind,
    op_id: u64,
    position: usize,
    depth: usize,
) -> anyhow::Result<CqlValue> {
    if depth > 8 {
        anyhow::bail!("bind type nesting too deep");
    }
    let pos = position as u64;
    let value = match kind {
        BindValueKind::Ascii => CqlValue::Ascii(format!("v{op_id}_{position}")),
        BindValueKind::Bigint => CqlValue::Bigint((op_id + pos) as i64),
        BindValueKind::Blob => CqlValue::Blob((op_id + pos).to_be_bytes().to_vec()),
        BindValueKind::Boolean => CqlValue::Boolean((op_id + pos).is_multiple_of(2)),
        BindValueKind::Counter => CqlValue::Counter((op_id + pos) as i64),
        BindValueKind::Decimal => CqlValue::Decimal {
            scale: 2,
            unscaled: ((op_id + pos) as i64).to_be_bytes().to_vec(),
        },
        BindValueKind::Duration => CqlValue::Duration {
            months: ((op_id + pos) % 12) as i32,
            days: ((op_id + pos) % 28) as i32,
            nanoseconds: ((op_id + pos) as i64) * 1_000,
        },
        BindValueKind::Double => CqlValue::Double((op_id + pos) as f64 + 0.25),
        BindValueKind::Float => CqlValue::Float((op_id + pos) as f32 + 0.25),
        BindValueKind::Int => CqlValue::Int((op_id % i32::MAX as u64) as i32),
        BindValueKind::Timestamp => CqlValue::Timestamp(1_700_000_000_000 + (op_id + pos) as i64),
        BindValueKind::Uuid => {
            let mut bytes = [0u8; 16];
            bytes[..8].copy_from_slice(&op_id.to_be_bytes());
            bytes[8..].copy_from_slice(&pos.to_be_bytes());
            CqlValue::Uuid(bytes)
        }
        BindValueKind::Varchar => CqlValue::Varchar(format!("v{op_id}_{position}")),
        BindValueKind::Varint => CqlValue::Varint((op_id + pos).to_be_bytes().to_vec()),
        BindValueKind::Timeuuid => {
            let mut bytes = [0u8; 16];
            bytes[..8].copy_from_slice(&(op_id + 0x1020).to_be_bytes());
            bytes[8..].copy_from_slice(&(pos + 0x3040).to_be_bytes());
            CqlValue::Timeuuid(bytes)
        }
        BindValueKind::Inet => CqlValue::Inet(IpAddr::V4(Ipv4Addr::new(
            127,
            0,
            0,
            1 + ((op_id + pos) % 200) as u8,
        ))),
        BindValueKind::Date => CqlValue::Date((1u32 << 31) + ((op_id + pos) % 4096) as u32),
        BindValueKind::Time => CqlValue::Time(((op_id + pos) as i64) * 1_000),
        BindValueKind::Smallint => CqlValue::Smallint(((op_id + pos) % i16::MAX as u64) as i16),
        BindValueKind::Tinyint => CqlValue::Tinyint(((op_id + pos) % i8::MAX as u64) as i8),
        BindValueKind::List(inner) => {
            let item = bind_cql_value_for_kind(inner, op_id, position + 1, depth + 1)?;
            CqlValue::List(vec![item])
        }
        BindValueKind::Set(inner) => {
            let item = bind_cql_value_for_kind(inner, op_id, position + 1, depth + 1)?;
            CqlValue::Set(vec![item])
        }
        BindValueKind::Map(key, value) => {
            let k = bind_cql_value_for_kind(key, op_id, position + 1, depth + 1)?;
            let v = bind_cql_value_for_kind(value, op_id, position + 2, depth + 1)?;
            CqlValue::Map(vec![(k, v)])
        }
        BindValueKind::Tuple(items) => {
            let tuple_values = items
                .iter()
                .enumerate()
                .map(|(idx, item)| {
                    bind_cql_value_for_kind(item, op_id, position + idx, depth + 1).map(Some)
                })
                .collect::<anyhow::Result<Vec<_>>>()?;
            CqlValue::Tuple(tuple_values)
        }
        BindValueKind::Udt(fields) => {
            let udt_values = fields
                .iter()
                .enumerate()
                .map(|(idx, (name, field_kind))| {
                    bind_cql_value_for_kind(field_kind, op_id, position + idx, depth + 1)
                        .map(|value| (name.clone(), Some(value)))
                })
                .collect::<anyhow::Result<Vec<_>>>()?;
            CqlValue::Udt(udt_values)
        }
        BindValueKind::Vector(inner, dims) => {
            let base = match inner.as_ref() {
                BindValueKind::Float => (op_id + pos) as f32 + 0.5,
                BindValueKind::Double => ((op_id + pos) as f64 + 0.5) as f32,
                BindValueKind::Int
                | BindValueKind::Bigint
                | BindValueKind::Smallint
                | BindValueKind::Tinyint => ((op_id + pos) % 1024) as f32,
                _ => anyhow::bail!("unsupported vector inner type"),
            };
            let values = (0..*dims).map(|d| base + d as f32).collect();
            CqlValue::Vector(VectorValue::new(values))
        }
        BindValueKind::Custom(name) => anyhow::bail!("custom bind type '{}'", name),
        BindValueKind::Unknown(id) => anyhow::bail!("unknown bind type 0x{id:04X}"),
    };
    Ok(value)
}

fn bind_value_for_kind(
    kind: &BindValueKind,
    op_id: u64,
    position: usize,
    value_size: Option<usize>,
) -> BoundValue {
    let maybe_text_override = if let Some(size) = value_size {
        match kind {
            BindValueKind::Ascii => Some(CqlValue::Ascii(deterministic_value_payload(op_id, size))),
            BindValueKind::Varchar => {
                Some(CqlValue::Varchar(deterministic_value_payload(op_id, size)))
            }
            _ => None,
        }
    } else {
        None
    };
    let value = if let Some(value) = maybe_text_override {
        Ok(value)
    } else {
        bind_cql_value_for_kind(kind, op_id, position, 0)
    };
    match value {
        Ok(value) => BoundValue::Bytes(value.serialize_value()),
        Err(_) => BoundValue::Unset,
    }
}

#[cfg(test)]
fn bind_values_for(kinds: &[BindValueKind], op_id: u64) -> Vec<BoundValue> {
    bind_values_for_key(kinds, op_id, op_id, 0, None)
}

fn bind_values_for_key(
    kinds: &[BindValueKind],
    op_id: u64,
    key_id: u64,
    key_bind_index: usize,
    value_size: Option<usize>,
) -> Vec<BoundValue> {
    kinds
        .iter()
        .enumerate()
        .map(|(position, kind)| {
            let seed = if position == key_bind_index {
                key_id
            } else {
                op_id
            };
            let value_size_for_position = if position == key_bind_index {
                None
            } else {
                value_size
            };
            bind_value_for_kind(kind, seed, position, value_size_for_position)
        })
        .collect()
}

fn ensure_native_write_schema(addr: &str, cfg: &StressConfig) -> anyhow::Result<()> {
    if cfg.transport != TransportMode::NativeCql || matches!(cfg.mode, StressMode::Read) {
        return Ok(());
    }
    if cfg.query_write.is_some() {
        return Ok(());
    }
    let ks = cfg.keyspace.as_deref().unwrap_or("stress_native");
    let tbl = cfg.table.as_deref().unwrap_or("kv");
    let mut session = NativeSession::connect(
        addr,
        cfg.username
            .as_ref()
            .zip(cfg.password.as_ref())
            .map(|(username, password)| NativeAuthCredentials {
                username: username.clone(),
                password: password.clone(),
            }),
    )?;
    session.query(
        &format!(
        "CREATE KEYSPACE IF NOT EXISTS {ks} WITH replication = {{'class':'SimpleStrategy','replication_factor':1}}"
    ),
        cfg.consistency,
    )?;
    session.query(
        &format!("CREATE TABLE IF NOT EXISTS {ks}.{tbl} (k bigint PRIMARY KEY, v text)"),
        cfg.consistency,
    )?;
    Ok(())
}

fn build_prepared_plans(
    session: &mut NativeSession,
    cfg: &StressConfig,
) -> anyhow::Result<(Option<PreparedPlan>, Option<PreparedPlan>)> {
    if cfg.transport != TransportMode::NativeCql || !cfg.prepared {
        return Ok((None, None));
    }

    let read_plan = match cfg.mode {
        StressMode::Read | StressMode::Mixed => {
            let query = default_prepared_read_query(cfg);
            Some(session.prepare(&query)?)
        }
        StressMode::Write => None,
    };

    let write_plan = match cfg.mode {
        StressMode::Write | StressMode::Mixed => {
            let query = default_prepared_write_query(cfg);
            Some(session.prepare(&query)?)
        }
        StressMode::Read => None,
    };

    Ok((read_plan, write_plan))
}

fn run_stress(target: &str, cfg: &StressConfig) -> StressReport {
    if let Err(err) = ensure_native_write_schema(target, cfg) {
        eprintln!("Warning: native schema setup failed: {}", err);
    }

    let next_op = Arc::new(AtomicU64::new(0));
    let executed_ops = Arc::new(AtomicU64::new(0));
    let ok_ops = Arc::new(AtomicU64::new(0));
    let failed_ops = Arc::new(AtomicU64::new(0));
    let latencies_us = Arc::new(Mutex::new(Vec::<u64>::with_capacity(cfg.ops as usize)));
    let errors = Arc::new(Mutex::new(Vec::<String>::new()));
    let first_measured_at = Arc::new(Mutex::new(None::<Instant>));
    let ratio_scaled = (cfg.read_ratio * 1000.0).round() as u64;

    let start = Instant::now();
    let deadline = duration_deadline(start, cfg.duration_secs);
    let warmup_deadline = duration_deadline(start, cfg.warmup_secs);
    let mut joins = Vec::with_capacity(cfg.concurrency);
    for _ in 0..cfg.concurrency {
        let target = target.to_string();
        let cfg = cfg.clone();
        let next_op = Arc::clone(&next_op);
        let executed_ops = Arc::clone(&executed_ops);
        let ok_ops = Arc::clone(&ok_ops);
        let failed_ops = Arc::clone(&failed_ops);
        let latencies_us = Arc::clone(&latencies_us);
        let errors = Arc::clone(&errors);
        let first_measured_at = Arc::clone(&first_measured_at);

        joins.push(thread::spawn(move || {
            let http_client = if cfg.transport == TransportMode::AdminHttp {
                Some(Client::new())
            } else {
                None
            };
            let mut native_session = if cfg.transport == TransportMode::NativeCql {
                NativeSession::connect(
                    &target,
                    cfg.username
                        .as_ref()
                        .zip(cfg.password.as_ref())
                        .map(|(username, password)| NativeAuthCredentials {
                            username: username.clone(),
                            password: password.clone(),
                        }),
                )
                .ok()
            } else {
                None
            };
            let mut prepared_read: Option<PreparedPlan> = None;
            let mut prepared_write: Option<PreparedPlan> = None;
            if let Some(session) = native_session.as_mut() {
                if let Ok((read_plan, write_plan)) = build_prepared_plans(session, &cfg) {
                    prepared_read = read_plan;
                    prepared_write = write_plan;
                }
            }
            let native_connect_err =
                if cfg.transport == TransportMode::NativeCql && native_session.is_none() {
                    Some(format!("failed to connect native CQL to {}", target))
                } else {
                    None
                };

            loop {
                if let Some(deadline) = deadline {
                    if Instant::now() >= deadline {
                        break;
                    }
                }

                let op_id = next_op.fetch_add(1, Ordering::Relaxed);
                if op_id >= cfg.ops {
                    break;
                }

                let mode = match cfg.mode {
                    StressMode::Mixed => {
                        if op_id % 1000 < ratio_scaled {
                            StressMode::Read
                        } else {
                            StressMode::Write
                        }
                    }
                    fixed => fixed,
                };
                let key_id = key_id_for(&cfg, op_id);

                maybe_pace_op(start, op_id, cfg.rate_ops_per_sec);
                if let Some(deadline) = deadline {
                    if Instant::now() >= deadline {
                        break;
                    }
                }
                let t0 = Instant::now();
                let result = match cfg.transport {
                    TransportMode::AdminHttp => {
                        let client = http_client.as_ref().expect("http client");
                        execute_one_admin(client, &target, mode, &cfg)
                    }
                    TransportMode::NativeCql => {
                        if let Some(session) = native_session.as_mut() {
                            if cfg.prepared {
                                match mode {
                                    StressMode::Read => {
                                        if let Some(plan) = prepared_read.as_mut() {
                                            let values = bind_values_for_key(
                                                &plan.bind_kinds,
                                                op_id,
                                                key_id,
                                                cfg.key_bind_index,
                                                cfg.value_size,
                                            );
                                            session.execute_with_reprepare(
                                                plan,
                                                &values,
                                                cfg.consistency,
                                            )
                                        } else {
                                            let query = default_read_query(&cfg, op_id, key_id);
                                            session.query(&query, cfg.consistency)
                                        }
                                    }
                                    StressMode::Write => {
                                        if let Some(plan) = prepared_write.as_mut() {
                                            let values = bind_values_for_key(
                                                &plan.bind_kinds,
                                                op_id,
                                                key_id,
                                                cfg.key_bind_index,
                                                cfg.value_size,
                                            );
                                            session.execute_with_reprepare(
                                                plan,
                                                &values,
                                                cfg.consistency,
                                            )
                                        } else {
                                            let query = default_write_query(&cfg, op_id, key_id);
                                            session.query(&query, cfg.consistency)
                                        }
                                    }
                                    StressMode::Mixed => unreachable!(),
                                }
                            } else {
                                let query = match mode {
                                    StressMode::Read => default_read_query(&cfg, op_id, key_id),
                                    StressMode::Write => default_write_query(&cfg, op_id, key_id),
                                    StressMode::Mixed => unreachable!(),
                                };
                                session.query(&query, cfg.consistency)
                            }
                        } else {
                            Err(anyhow::anyhow!(
                                "{}",
                                native_connect_err
                                    .clone()
                                    .unwrap_or_else(|| "native connection unavailable".to_string())
                            ))
                        }
                    }
                };
                let elapsed = t0.elapsed().as_micros() as u64;
                let executed_idx = executed_ops.fetch_add(1, Ordering::Relaxed);
                let measured = warmup_deadline
                    .map(|deadline| t0 >= deadline)
                    .unwrap_or(true)
                    && cfg
                        .warmup_ops
                        .map(|ops| executed_idx >= ops)
                        .unwrap_or(true);

                if measured {
                    let mut first = first_measured_at.lock().unwrap();
                    if first.is_none() {
                        *first = Some(t0);
                    }
                    latencies_us.lock().unwrap().push(elapsed);
                    if let Err(e) = result {
                        failed_ops.fetch_add(1, Ordering::Relaxed);
                        let mut errs = errors.lock().unwrap();
                        if errs.len() < 5 {
                            errs.push(e.to_string());
                        }
                    } else {
                        ok_ops.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }));
    }

    for join in joins {
        let _ = join.join();
    }
    let end = Instant::now();
    let elapsed_secs = end.duration_since(start).as_secs_f64();
    let measurement_start = *first_measured_at.lock().unwrap();
    let measured_elapsed_secs = measurement_start
        .map(|start| end.duration_since(start).as_secs_f64())
        .unwrap_or(0.0);

    let mut lat = latencies_us.lock().unwrap().clone();
    lat.sort_unstable();
    let sum_us: u128 = lat.iter().copied().map(u128::from).sum();
    let avg_ms = if lat.is_empty() {
        0.0
    } else {
        (sum_us as f64 / lat.len() as f64) / 1_000.0
    };
    let measured_ok = ok_ops.load(Ordering::Relaxed);
    let measured_failed = failed_ops.load(Ordering::Relaxed);
    let measured_total = measured_ok + measured_failed;
    let executed_total = executed_ops.load(Ordering::Relaxed);
    let warmup_ops = executed_total.saturating_sub(measured_total);
    let throughput_ops = if measured_elapsed_secs == 0.0 {
        0.0
    } else {
        measured_total as f64 / measured_elapsed_secs
    };

    StressReport {
        total_ops: measured_total,
        executed_ops: executed_total,
        warmup_ops,
        ok_ops: measured_ok,
        failed_ops: measured_failed,
        elapsed_secs,
        measured_elapsed_secs,
        throughput_ops,
        avg_latency_ms: avg_ms,
        p50_latency_ms: percentile(&lat, 0.50),
        p95_latency_ms: percentile(&lat, 0.95),
        p99_latency_ms: percentile(&lat, 0.99),
        sample_errors: errors.lock().unwrap().clone(),
    }
}

fn print_report(cfg: &StressConfig, report: &StressReport, target: &str) {
    println!("Cassandra Stress (Rust)");
    println!(
        "  transport:      {}",
        match cfg.transport {
            TransportMode::AdminHttp => "admin-http",
            TransportMode::NativeCql => "native-cql",
        }
    );
    println!("  target:         {}", target);
    println!("  mode:           {:?}", cfg.mode);
    if cfg.transport == TransportMode::NativeCql {
        println!("  prepared:       {}", cfg.prepared);
        println!("  consistency:    {}", cfg.consistency.name());
    }
    println!("  operations:     {}", report.total_ops);
    println!("  executed ops:   {}", report.executed_ops);
    println!("  target ops:     {}", cfg.ops);
    println!("  keys:           {}", cfg.keys);
    println!(
        "  id distribution: {}",
        match cfg.key_dist {
            KeyDistribution::Sequential => "sequential",
            KeyDistribution::Uniform => "uniform",
        }
    );
    println!("  key bind index: {}", cfg.key_bind_index);
    if let Some(value_size) = cfg.value_size {
        println!("  value size:     {} bytes", value_size);
    }
    println!("  concurrency:    {}", cfg.concurrency);
    if let Some(rate) = cfg.rate_ops_per_sec {
        println!("  target rate:    {} ops/s", rate);
    }
    if let Some(duration) = cfg.duration_secs {
        println!("  duration limit: {}", format_duration(duration));
    }
    if let Some(warmup) = cfg.warmup_secs {
        println!("  warmup:         {}", format_duration(warmup));
    }
    if let Some(warmup_ops) = cfg.warmup_ops {
        println!("  warmup target:  {} ops", warmup_ops);
    }
    println!("  warmup ops:     {}", report.warmup_ops);
    println!("  elapsed:        {:.3}s", report.elapsed_secs);
    println!("  measured:       {:.3}s", report.measured_elapsed_secs);
    println!("  throughput:     {:.2} ops/s", report.throughput_ops);
    println!("  success/fail:   {}/{}", report.ok_ops, report.failed_ops);
    println!("  latency avg:    {:.3} ms", report.avg_latency_ms);
    println!("  latency p50:    {:.3} ms", report.p50_latency_ms);
    println!("  latency p95:    {:.3} ms", report.p95_latency_ms);
    println!("  latency p99:    {:.3} ms", report.p99_latency_ms);

    if !report.sample_errors.is_empty() {
        println!("  sample errors:");
        for err in &report.sample_errors {
            println!("    - {}", err);
        }
    }
}

pub fn run(args: &[String], host: &str, admin_port: u16) {
    let cfg = match parse_args(args) {
        Ok(cfg) => cfg,
        Err(e) => {
            eprintln!("{}", e);
            return;
        }
    };

    let target = match cfg.transport {
        TransportMode::AdminHttp => format!("http://{}:{}", host, admin_port),
        TransportMode::NativeCql => format!("{}:{}", host, cfg.native_port),
    };
    let report = run_stress(&target, &cfg);
    print_report(&cfg, &report, &target);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::TcpListener;
    use std::thread;

    fn authenticate_frame(stream_id: i16, authenticator: &str) -> Frame {
        let mut body = BytesMut::new();
        types::write_string(&mut body, authenticator);
        Frame {
            header: FrameHeader {
                version: PROTOCOL_V4,
                flags: 0,
                stream_id,
                opcode: Opcode::Authenticate,
                length: body.len() as u32,
            },
            body: body.freeze(),
        }
    }

    fn auth_challenge_frame(stream_id: i16, token: Option<&[u8]>) -> Frame {
        let mut body = BytesMut::new();
        types::write_bytes_opt(&mut body, token);
        Frame {
            header: FrameHeader {
                version: PROTOCOL_V4,
                flags: 0,
                stream_id,
                opcode: Opcode::AuthChallenge,
                length: body.len() as u32,
            },
            body: body.freeze(),
        }
    }

    fn auth_success_frame(stream_id: i16, token: Option<&[u8]>) -> Frame {
        let mut body = BytesMut::new();
        types::write_bytes_opt(&mut body, token);
        Frame {
            header: FrameHeader {
                version: PROTOCOL_V4,
                flags: 0,
                stream_id,
                opcode: Opcode::AuthSuccess,
                length: body.len() as u32,
            },
            body: body.freeze(),
        }
    }

    fn ready_frame(stream_id: i16) -> Frame {
        Frame {
            header: FrameHeader {
                version: PROTOCOL_V4,
                flags: 0,
                stream_id,
                opcode: Opcode::Ready,
                length: 0,
            },
            body: Bytes::new(),
        }
    }

    fn result_void_frame(stream_id: i16) -> Frame {
        let mut body = BytesMut::new();
        types::write_int(&mut body, 0x0001);
        Frame {
            header: FrameHeader {
                version: PROTOCOL_V4,
                flags: 0,
                stream_id,
                opcode: Opcode::Result,
                length: body.len() as u32,
            },
            body: body.freeze(),
        }
    }

    fn error_unprepared_frame(stream_id: i16, prepared_id: &[u8]) -> Frame {
        let mut body = BytesMut::new();
        types::write_int(&mut body, 0x2500);
        types::write_string(&mut body, "Prepared query not found");
        types::write_short_bytes(&mut body, prepared_id);
        Frame {
            header: FrameHeader {
                version: PROTOCOL_V4,
                flags: 0,
                stream_id,
                opcode: Opcode::Error,
                length: body.len() as u32,
            },
            body: body.freeze(),
        }
    }

    fn prepared_result_frame(stream_id: i16, prepared_id: &[u8], bind_types: &[u16]) -> Frame {
        let mut body = BytesMut::new();
        types::write_int(&mut body, 0x0004);
        types::write_short_bytes(&mut body, prepared_id);
        body.extend_from_slice(&encode_test_prepared_bind_metadata(bind_types, &[]));
        body.extend_from_slice(&encode_test_rows_metadata(&[]));
        Frame {
            header: FrameHeader {
                version: PROTOCOL_V4,
                flags: 0,
                stream_id,
                opcode: Opcode::Result,
                length: body.len() as u32,
            },
            body: body.freeze(),
        }
    }

    fn read_auth_response_token(frame: &Frame) -> Option<Vec<u8>> {
        assert_eq!(frame.header.opcode, Opcode::AuthResponse);
        let mut cursor: &[u8] = &frame.body;
        types::read_bytes(&mut cursor).unwrap()
    }

    fn encode_test_rows_metadata(col_types: &[u16]) -> BytesMut {
        let mut body = BytesMut::new();
        types::write_int(&mut body, 0);
        types::write_int(&mut body, col_types.len() as i32);
        for (i, type_id) in col_types.iter().enumerate() {
            types::write_string(&mut body, "ks");
            types::write_string(&mut body, "tbl");
            types::write_string(&mut body, &format!("c{i}"));
            types::write_short(&mut body, *type_id);
        }
        body
    }

    fn encode_test_prepared_bind_metadata(col_types: &[u16], pk_indexes: &[u16]) -> BytesMut {
        let mut body = BytesMut::new();
        types::write_int(&mut body, 0);
        types::write_int(&mut body, col_types.len() as i32);
        types::write_int(&mut body, pk_indexes.len() as i32);
        for idx in pk_indexes {
            types::write_short(&mut body, *idx);
        }
        for (i, type_id) in col_types.iter().enumerate() {
            types::write_string(&mut body, "ks");
            types::write_string(&mut body, "tbl");
            types::write_string(&mut body, &format!("c{i}"));
            types::write_short(&mut body, *type_id);
        }
        body
    }

    fn encode_test_column_type(kind: &BindValueKind, out: &mut BytesMut) {
        match kind {
            BindValueKind::Ascii => types::write_short(out, 0x0001),
            BindValueKind::Bigint => types::write_short(out, 0x0002),
            BindValueKind::Blob => types::write_short(out, 0x0003),
            BindValueKind::Boolean => types::write_short(out, 0x0004),
            BindValueKind::Counter => types::write_short(out, 0x0005),
            BindValueKind::Decimal => types::write_short(out, 0x0006),
            BindValueKind::Double => types::write_short(out, 0x0007),
            BindValueKind::Float => types::write_short(out, 0x0008),
            BindValueKind::Int => types::write_short(out, 0x0009),
            BindValueKind::Timestamp => types::write_short(out, 0x000B),
            BindValueKind::Uuid => types::write_short(out, 0x000C),
            BindValueKind::Varchar => types::write_short(out, 0x000D),
            BindValueKind::Varint => types::write_short(out, 0x000E),
            BindValueKind::Timeuuid => types::write_short(out, 0x000F),
            BindValueKind::Inet => types::write_short(out, 0x0010),
            BindValueKind::Date => types::write_short(out, 0x0011),
            BindValueKind::Time => types::write_short(out, 0x0012),
            BindValueKind::Smallint => types::write_short(out, 0x0013),
            BindValueKind::Tinyint => types::write_short(out, 0x0014),
            BindValueKind::Duration => types::write_short(out, 0x0015),
            BindValueKind::List(inner) => {
                types::write_short(out, 0x0020);
                encode_test_column_type(inner, out);
            }
            BindValueKind::Map(key, value) => {
                types::write_short(out, 0x0021);
                encode_test_column_type(key, out);
                encode_test_column_type(value, out);
            }
            BindValueKind::Set(inner) => {
                types::write_short(out, 0x0022);
                encode_test_column_type(inner, out);
            }
            BindValueKind::Tuple(items) => {
                types::write_short(out, 0x0031);
                types::write_short(out, items.len() as u16);
                for item in items {
                    encode_test_column_type(item, out);
                }
            }
            BindValueKind::Udt(fields) => {
                types::write_short(out, 0x0030);
                types::write_string(out, "ks");
                types::write_string(out, "udt");
                types::write_short(out, fields.len() as u16);
                for (name, field_kind) in fields {
                    types::write_string(out, name);
                    encode_test_column_type(field_kind, out);
                }
            }
            BindValueKind::Vector(inner, dims) => {
                types::write_short(out, 0x0032);
                encode_test_column_type(inner, out);
                types::write_int(out, *dims as i32);
            }
            BindValueKind::Custom(name) => {
                types::write_short(out, 0x0000);
                types::write_string(out, name);
            }
            BindValueKind::Unknown(id) => types::write_short(out, *id),
        }
    }

    fn encode_test_prepared_bind_metadata_kinds(
        col_types: &[BindValueKind],
        pk_indexes: &[u16],
    ) -> BytesMut {
        let mut body = BytesMut::new();
        types::write_int(&mut body, 0);
        types::write_int(&mut body, col_types.len() as i32);
        types::write_int(&mut body, pk_indexes.len() as i32);
        for idx in pk_indexes {
            types::write_short(&mut body, *idx);
        }
        for (i, type_kind) in col_types.iter().enumerate() {
            types::write_string(&mut body, "ks");
            types::write_string(&mut body, "tbl");
            types::write_string(&mut body, &format!("c{i}"));
            encode_test_column_type(type_kind, &mut body);
        }
        body
    }

    #[test]
    fn parse_read_defaults() {
        let cfg = parse_args(&["read".to_string()]).unwrap();
        assert_eq!(cfg.mode, StressMode::Read);
        assert_eq!(cfg.transport, TransportMode::AdminHttp);
        assert_eq!(cfg.ops, 1000);
        assert_eq!(cfg.concurrency, 8);
        assert_eq!(cfg.rate_ops_per_sec, None);
        assert_eq!(cfg.duration_secs, None);
        assert_eq!(cfg.warmup_secs, None);
        assert_eq!(cfg.warmup_ops, None);
        assert_eq!(cfg.keys, 1000);
        assert_eq!(cfg.key_dist, KeyDistribution::Sequential);
        assert_eq!(cfg.key_bind_index, 0);
        assert_eq!(cfg.value_size, None);
        assert_eq!(cfg.read_path, "/health");
    }

    #[test]
    fn parse_write_with_legacy_flags() {
        let cfg = parse_args(&[
            "write".to_string(),
            "n=55".to_string(),
            "threads=4".to_string(),
            "rate=123".to_string(),
            "duration=7".to_string(),
            "warmup=2".to_string(),
            "warmup-ops=3".to_string(),
            "keys=500".to_string(),
            "id-dist=uniform".to_string(),
            "key-bind-index=2".to_string(),
            "value-size=64".to_string(),
            "--keyspace".to_string(),
            "ks".to_string(),
            "--table".to_string(),
            "tbl".to_string(),
        ])
        .unwrap();
        assert_eq!(cfg.mode, StressMode::Write);
        assert_eq!(cfg.ops, 55);
        assert_eq!(cfg.concurrency, 4);
        assert_eq!(cfg.rate_ops_per_sec, Some(123));
        assert_eq!(cfg.duration_secs, Some(Duration::from_secs(7)));
        assert_eq!(cfg.warmup_secs, Some(Duration::from_secs(2)));
        assert_eq!(cfg.warmup_ops, Some(3));
        assert_eq!(cfg.keys, 500);
        assert_eq!(cfg.key_dist, KeyDistribution::Uniform);
        assert_eq!(cfg.key_bind_index, 2);
        assert_eq!(cfg.value_size, Some(64));
        assert_eq!(cfg.keyspace.as_deref(), Some("ks"));
        assert_eq!(cfg.table.as_deref(), Some("tbl"));
    }

    #[test]
    fn parse_rate_rejects_zero() {
        let err = parse_args(&["read".to_string(), "--rate".to_string(), "0".to_string()])
            .unwrap_err()
            .to_string();
        assert!(err.contains("rate must be > 0"));
    }

    #[test]
    fn parse_duration_rejects_zero() {
        let err = parse_args(&[
            "read".to_string(),
            "--duration".to_string(),
            "0".to_string(),
        ])
        .unwrap_err()
        .to_string();
        assert!(err.contains("duration must be > 0"));
    }

    #[test]
    fn parse_warmup_rejects_zero() {
        let err = parse_args(&["read".to_string(), "--warmup".to_string(), "0".to_string()])
            .unwrap_err()
            .to_string();
        assert!(err.contains("warmup must be > 0"));
    }

    #[test]
    fn parse_duration_and_warmup_with_units() {
        let cfg = parse_args(&[
            "read".to_string(),
            "--duration".to_string(),
            "1500ms".to_string(),
            "--warmup".to_string(),
            "2m".to_string(),
        ])
        .unwrap();
        assert_eq!(cfg.duration_secs, Some(Duration::from_millis(1500)));
        assert_eq!(cfg.warmup_secs, Some(Duration::from_secs(120)));
    }

    #[test]
    fn parse_warmup_ops_rejects_zero() {
        let err = parse_args(&[
            "read".to_string(),
            "--warmup-ops".to_string(),
            "0".to_string(),
        ])
        .unwrap_err()
        .to_string();
        assert!(err.contains("warmup-ops must be > 0"));
    }

    #[test]
    fn parse_keys_rejects_zero() {
        let err = parse_args(&["read".to_string(), "--keys".to_string(), "0".to_string()])
            .unwrap_err()
            .to_string();
        assert!(err.contains("keys must be > 0"));
    }

    #[test]
    fn parse_id_dist_rejects_unknown() {
        let err = parse_args(&[
            "read".to_string(),
            "--id-dist".to_string(),
            "gaussian".to_string(),
        ])
        .unwrap_err()
        .to_string();
        assert!(err.contains("invalid --id-dist"));
    }

    #[test]
    fn parse_key_bind_index_rejects_non_numeric() {
        let err = parse_args(&[
            "read".to_string(),
            "--key-bind-index".to_string(),
            "x".to_string(),
        ])
        .unwrap_err()
        .to_string();
        assert!(err.contains("invalid key-bind-index"));
    }

    #[test]
    fn parse_value_size_rejects_zero() {
        let err = parse_args(&[
            "write".to_string(),
            "--value-size".to_string(),
            "0".to_string(),
        ])
        .unwrap_err()
        .to_string();
        assert!(err.contains("value-size must be > 0"));
    }

    #[test]
    fn parse_native_defaults_keyspace_table_for_write() {
        let cfg = parse_args(&[
            "write".to_string(),
            "--native-cql".to_string(),
            "--prepared".to_string(),
        ])
        .unwrap();
        assert_eq!(cfg.transport, TransportMode::NativeCql);
        assert!(cfg.prepared);
        assert_eq!(cfg.keyspace.as_deref(), Some("stress_native"));
        assert_eq!(cfg.table.as_deref(), Some("kv"));
    }

    #[test]
    fn parse_native_consistency() {
        let cfg = parse_args(&[
            "read".to_string(),
            "--native-cql".to_string(),
            "--consistency".to_string(),
            "local_quorum".to_string(),
        ])
        .unwrap();
        assert_eq!(cfg.transport, TransportMode::NativeCql);
        assert_eq!(cfg.consistency, Consistency::LocalQuorum);
    }

    #[test]
    fn parse_native_consistency_rejects_unknown() {
        let err = parse_args(&[
            "read".to_string(),
            "--native-cql".to_string(),
            "--consistency".to_string(),
            "not-a-level".to_string(),
        ])
        .unwrap_err()
        .to_string();
        assert!(err.contains("invalid --consistency"));
    }

    #[test]
    fn scheduled_offset_for_op_uses_global_rate() {
        assert_eq!(scheduled_offset_for_op(0, 100), Duration::from_micros(0));
        assert_eq!(
            scheduled_offset_for_op(1, 100),
            Duration::from_micros(10_000)
        );
        assert_eq!(
            scheduled_offset_for_op(10, 100),
            Duration::from_micros(100_000)
        );
    }

    #[test]
    fn key_id_for_supports_sequential_and_uniform() {
        let seq_cfg = StressConfig {
            keys: 10,
            key_dist: KeyDistribution::Sequential,
            ..StressConfig::default()
        };
        assert_eq!(key_id_for(&seq_cfg, 0), 0);
        assert_eq!(key_id_for(&seq_cfg, 15), 5);

        let uniform_cfg = StressConfig {
            keys: 10,
            key_dist: KeyDistribution::Uniform,
            ..StressConfig::default()
        };
        let a = key_id_for(&uniform_cfg, 1);
        let b = key_id_for(&uniform_cfg, 2);
        assert!(a < 10);
        assert!(b < 10);
        assert_ne!(a, b);
    }

    #[test]
    fn deterministic_value_payload_has_requested_size() {
        let payload = deterministic_value_payload(42, 33);
        assert_eq!(payload.len(), 33);
    }

    #[test]
    fn duration_deadline_builds_instant() {
        let start = Instant::now();
        let deadline = duration_deadline(start, Some(Duration::from_secs(2))).unwrap();
        assert!(deadline >= start + Duration::from_secs(2));
        assert!(duration_deadline(start, None).is_none());
    }

    #[test]
    fn format_duration_uses_compact_units() {
        assert_eq!(format_duration(Duration::from_millis(500)), "500ms");
        assert_eq!(format_duration(Duration::from_secs(2)), "2s");
        assert_eq!(format_duration(Duration::from_secs(120)), "2m");
        assert_eq!(format_duration(Duration::from_secs(7200)), "2h");
    }

    #[test]
    fn parse_native_auth_credentials() {
        let cfg = parse_args(&[
            "read".to_string(),
            "--native-cql".to_string(),
            "--username".to_string(),
            "alice".to_string(),
            "--password".to_string(),
            "secret".to_string(),
        ])
        .unwrap();
        assert_eq!(cfg.transport, TransportMode::NativeCql);
        assert_eq!(cfg.username.as_deref(), Some("alice"));
        assert_eq!(cfg.password.as_deref(), Some("secret"));
    }

    #[test]
    fn parse_native_auth_rejects_partial_credentials() {
        let err = parse_args(&[
            "read".to_string(),
            "--native-cql".to_string(),
            "--username".to_string(),
            "alice".to_string(),
        ])
        .unwrap_err()
        .to_string();
        assert!(err.contains("--username and --password must be provided together"));
    }

    #[test]
    fn sasl_plain_token_layout() {
        let token = sasl_plain_token("alice", "secret");
        assert_eq!(
            token,
            vec![
                0, b'a', b'l', b'i', b'c', b'e', 0, b's', b'e', b'c', b'r', b'e', b't'
            ]
        );
    }

    #[test]
    fn parse_auth_challenge_token() {
        let frame = auth_challenge_frame(1, Some(b"more-data"));
        let token = parse_auth_challenge(&frame).unwrap().unwrap();
        assert_eq!(token, b"more-data".to_vec());
    }

    #[test]
    fn parse_prepared_plan_reads_bind_types() {
        let mut body = BytesMut::new();
        types::write_int(&mut body, 0x0004);
        types::write_short_bytes(&mut body, b"prepared-id");
        body.extend_from_slice(&encode_test_prepared_bind_metadata(&[0x0002, 0x000D], &[0]));
        body.extend_from_slice(&encode_test_rows_metadata(&[]));
        let frame = Frame {
            header: FrameHeader {
                version: PROTOCOL_V4,
                flags: 0,
                stream_id: 5,
                opcode: Opcode::Result,
                length: body.len() as u32,
            },
            body: body.freeze(),
        };

        let plan = parse_prepared_plan(&frame).unwrap();
        assert_eq!(plan.id, b"prepared-id".to_vec());
        assert_eq!(
            plan.bind_kinds,
            vec![BindValueKind::Bigint, BindValueKind::Varchar]
        );
    }

    #[test]
    fn parse_prepared_plan_reads_composite_bind_types() {
        let mut body = BytesMut::new();
        types::write_int(&mut body, 0x0004);
        types::write_short_bytes(&mut body, b"prepared-id-composite");
        body.extend_from_slice(&encode_test_prepared_bind_metadata_kinds(
            &[
                BindValueKind::List(Box::new(BindValueKind::Int)),
                BindValueKind::Map(
                    Box::new(BindValueKind::Varchar),
                    Box::new(BindValueKind::Bigint),
                ),
                BindValueKind::Tuple(vec![BindValueKind::Ascii, BindValueKind::Uuid]),
                BindValueKind::Vector(Box::new(BindValueKind::Float), 3),
            ],
            &[0, 1],
        ));
        body.extend_from_slice(&encode_test_rows_metadata(&[]));
        let frame = Frame {
            header: FrameHeader {
                version: PROTOCOL_V4,
                flags: 0,
                stream_id: 5,
                opcode: Opcode::Result,
                length: body.len() as u32,
            },
            body: body.freeze(),
        };

        let plan = parse_prepared_plan(&frame).unwrap();
        assert_eq!(
            plan.bind_kinds,
            vec![
                BindValueKind::List(Box::new(BindValueKind::Int)),
                BindValueKind::Map(
                    Box::new(BindValueKind::Varchar),
                    Box::new(BindValueKind::Bigint)
                ),
                BindValueKind::Tuple(vec![BindValueKind::Ascii, BindValueKind::Uuid]),
                BindValueKind::Vector(Box::new(BindValueKind::Float), 3),
            ]
        );
    }

    #[test]
    fn parse_prepared_plan_rejects_negative_columns_count() {
        let mut body = BytesMut::new();
        types::write_int(&mut body, 0x0004);
        types::write_short_bytes(&mut body, b"prepared-negative-columns");
        types::write_int(&mut body, 0); // flags
        types::write_int(&mut body, -1); // columns_count (invalid)
        let frame = Frame {
            header: FrameHeader {
                version: PROTOCOL_V4,
                flags: 0,
                stream_id: 5,
                opcode: Opcode::Result,
                length: body.len() as u32,
            },
            body: body.freeze(),
        };
        let err = parse_prepared_plan(&frame).unwrap_err().to_string();
        assert!(err.contains("prepared columns_count must be >= 0"));
    }

    #[test]
    fn parse_prepared_plan_rejects_negative_pk_count() {
        let mut body = BytesMut::new();
        types::write_int(&mut body, 0x0004);
        types::write_short_bytes(&mut body, b"prepared-negative-pk");
        types::write_int(&mut body, 0); // flags
        types::write_int(&mut body, 1); // columns_count
        types::write_int(&mut body, -1); // pk_count (invalid)
        let frame = Frame {
            header: FrameHeader {
                version: PROTOCOL_V4,
                flags: 0,
                stream_id: 5,
                opcode: Opcode::Result,
                length: body.len() as u32,
            },
            body: body.freeze(),
        };
        let err = parse_prepared_plan(&frame).unwrap_err().to_string();
        assert!(err.contains("prepared pk_count must be >= 0"));
    }

    #[test]
    fn bind_values_for_uses_kind_serialization() {
        let values = bind_values_for(
            &[
                BindValueKind::Bigint,
                BindValueKind::Varchar,
                BindValueKind::Boolean,
            ],
            42,
        );
        assert_eq!(values.len(), 3);
        assert_eq!(
            values[0],
            BoundValue::Bytes(CqlValue::Bigint(42).serialize_value())
        );
        assert_eq!(
            values[1],
            BoundValue::Bytes(CqlValue::Varchar("v42_1".to_string()).serialize_value())
        );
        assert_eq!(
            values[2],
            BoundValue::Bytes(CqlValue::Boolean(true).serialize_value())
        );
    }

    #[test]
    fn bind_values_for_key_uses_key_id_for_first_bind() {
        let values = bind_values_for_key(
            &[BindValueKind::Bigint, BindValueKind::Bigint],
            42,
            7,
            0,
            None,
        );
        assert_eq!(values.len(), 2);
        assert_eq!(
            values[0],
            BoundValue::Bytes(CqlValue::Bigint(7).serialize_value())
        );
        assert_eq!(
            values[1],
            BoundValue::Bytes(CqlValue::Bigint(43).serialize_value())
        );
    }

    #[test]
    fn bind_values_for_key_respects_key_bind_index() {
        let values = bind_values_for_key(
            &[
                BindValueKind::Bigint,
                BindValueKind::Bigint,
                BindValueKind::Bigint,
            ],
            42,
            7,
            2,
            None,
        );
        assert_eq!(values.len(), 3);
        assert_eq!(
            values[0],
            BoundValue::Bytes(CqlValue::Bigint(42).serialize_value())
        );
        assert_eq!(
            values[1],
            BoundValue::Bytes(CqlValue::Bigint(43).serialize_value())
        );
        assert_eq!(
            values[2],
            BoundValue::Bytes(CqlValue::Bigint(9).serialize_value())
        );
    }

    #[test]
    fn bind_values_for_key_applies_value_size_to_non_key_text_binds() {
        let values = bind_values_for_key(
            &[BindValueKind::Varchar, BindValueKind::Varchar],
            42,
            7,
            0,
            Some(12),
        );
        assert_eq!(
            values[0],
            BoundValue::Bytes(CqlValue::Varchar("v7_0".to_string()).serialize_value())
        );
        assert_eq!(
            values[1],
            BoundValue::Bytes(
                CqlValue::Varchar(deterministic_value_payload(42, 12)).serialize_value()
            )
        );
    }

    #[test]
    fn default_write_query_uses_value_size_payload() {
        let cfg = StressConfig {
            keyspace: Some("ks".to_string()),
            table: Some("tbl".to_string()),
            value_size: Some(8),
            ..StressConfig::default()
        };
        let query = default_write_query(&cfg, 42, 7);
        assert!(query.contains("INSERT INTO ks.tbl"));
        assert!(query.contains("(7, '00000000')"));
    }

    #[test]
    fn default_write_query_custom_template_supports_value_placeholder() {
        let cfg = StressConfig {
            query_write: Some("INSERT INTO ks.tbl (k,v) VALUES ({key}, '{value}')".to_string()),
            value_size: Some(8),
            ..StressConfig::default()
        };
        let query = default_write_query(&cfg, 42, 7);
        assert_eq!(query, "INSERT INTO ks.tbl (k,v) VALUES (7, '00000000')");
    }

    #[test]
    fn render_query_template_supports_key_and_id_placeholders() {
        let rendered = render_query_template("k={key} id={id} ${key} ${id}", 42, 7, None);
        assert_eq!(rendered, "k=7 id=42 7 42");
    }

    #[test]
    fn render_query_template_supports_value_placeholders() {
        let rendered = render_query_template(
            "INSERT INTO ks.t (k,v) VALUES ({key}, '{value}') /* ${value} */",
            42,
            7,
            Some("abc"),
        );
        assert_eq!(
            rendered,
            "INSERT INTO ks.t (k,v) VALUES (7, 'abc') /* abc */"
        );
    }

    #[test]
    fn bind_values_for_supports_composite_types() {
        let values = bind_values_for(
            &[
                BindValueKind::List(Box::new(BindValueKind::Int)),
                BindValueKind::Map(
                    Box::new(BindValueKind::Varchar),
                    Box::new(BindValueKind::Bigint),
                ),
                BindValueKind::Tuple(vec![BindValueKind::Ascii, BindValueKind::Int]),
                BindValueKind::Udt(vec![
                    ("name".to_string(), BindValueKind::Varchar),
                    ("age".to_string(), BindValueKind::Int),
                ]),
                BindValueKind::Vector(Box::new(BindValueKind::Float), 3),
            ],
            7,
        );
        assert_eq!(values.len(), 5);
        assert_eq!(
            values[0],
            BoundValue::Bytes(CqlValue::List(vec![CqlValue::Int(7)]).serialize_value())
        );
        assert!(matches!(values[1], BoundValue::Bytes(_)));
        assert!(matches!(values[2], BoundValue::Bytes(_)));
        assert!(matches!(values[3], BoundValue::Bytes(_)));
        assert!(matches!(values[4], BoundValue::Bytes(_)));
    }

    #[test]
    fn bind_values_for_unknown_type_uses_unset() {
        let values = bind_values_for(&[BindValueKind::Unknown(0x7777)], 9);
        assert_eq!(values, vec![BoundValue::Unset]);
    }

    #[test]
    fn execute_frame_encodes_unset_value_marker() {
        let frame = execute_frame(b"pid", &[BoundValue::Unset], Consistency::Quorum, 4);
        let mut cursor: &[u8] = &frame.body;
        let prepared_id = types::read_short_bytes(&mut cursor).unwrap();
        assert_eq!(prepared_id, b"pid".to_vec());
        let consistency = types::read_consistency(&mut cursor).unwrap();
        assert_eq!(consistency, Consistency::Quorum);
        let flags = types::read_byte(&mut cursor).unwrap();
        assert_eq!(flags, query_flags::VALUES as u8);
        let count = types::read_short(&mut cursor).unwrap();
        assert_eq!(count, 1);
        let marker = types::read_int(&mut cursor).unwrap();
        assert_eq!(marker, -2);
    }

    #[test]
    fn query_frame_encodes_requested_consistency() {
        let frame = query_frame("SELECT key FROM system.local", Consistency::LocalOne, 2);
        let mut cursor: &[u8] = &frame.body;
        let query = types::read_long_string(&mut cursor).unwrap();
        assert_eq!(query, "SELECT key FROM system.local");
        let consistency = types::read_consistency(&mut cursor).unwrap();
        assert_eq!(consistency, Consistency::LocalOne);
    }

    #[test]
    fn native_connect_completes_auth_challenge_flow() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let expected_token = sasl_plain_token("alice", "secret");

        let server = thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();

            let startup = read_frame(&mut socket).unwrap();
            assert_eq!(startup.header.opcode, Opcode::Startup);

            let auth = authenticate_frame(1, "org.apache.cassandra.auth.PasswordAuthenticator");
            write_frame(&mut socket, &auth).unwrap();

            let first_response = read_frame(&mut socket).unwrap();
            assert_eq!(
                read_auth_response_token(&first_response).as_deref(),
                Some(expected_token.as_slice())
            );

            let challenge = auth_challenge_frame(2, Some(b"challenge"));
            write_frame(&mut socket, &challenge).unwrap();

            let second_response = read_frame(&mut socket).unwrap();
            assert_eq!(
                read_auth_response_token(&second_response).as_deref(),
                Some(expected_token.as_slice())
            );

            let success = auth_success_frame(3, None);
            write_frame(&mut socket, &success).unwrap();
        });

        let result = NativeSession::connect(
            &addr.to_string(),
            Some(NativeAuthCredentials {
                username: "alice".to_string(),
                password: "secret".to_string(),
            }),
        );
        assert!(result.is_ok(), "expected auth challenge flow to succeed");
        server.join().unwrap();
    }

    #[test]
    fn native_connect_requires_credentials_for_authenticate() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        let server = thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            let startup = read_frame(&mut socket).unwrap();
            assert_eq!(startup.header.opcode, Opcode::Startup);
            let auth = authenticate_frame(1, "org.apache.cassandra.auth.PasswordAuthenticator");
            write_frame(&mut socket, &auth).unwrap();
        });

        let err = match NativeSession::connect(&addr.to_string(), None) {
            Ok(_) => panic!("expected connect to fail without credentials"),
            Err(err) => err.to_string(),
        };
        assert!(err.contains("server requires native authentication"));
        assert!(err.contains("PasswordAuthenticator"));
        server.join().unwrap();
    }

    #[test]
    fn native_connect_fails_after_max_auth_challenges() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        let server = thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            let startup = read_frame(&mut socket).unwrap();
            assert_eq!(startup.header.opcode, Opcode::Startup);

            let auth = authenticate_frame(1, "org.apache.cassandra.auth.PasswordAuthenticator");
            write_frame(&mut socket, &auth).unwrap();

            for round in 0..8 {
                let response = read_frame(&mut socket).unwrap();
                assert_eq!(response.header.opcode, Opcode::AuthResponse);
                let challenge = auth_challenge_frame(2 + round as i16, Some(b"again"));
                write_frame(&mut socket, &challenge).unwrap();
            }
        });

        let err = match NativeSession::connect(
            &addr.to_string(),
            Some(NativeAuthCredentials {
                username: "alice".to_string(),
                password: "secret".to_string(),
            }),
        ) {
            Ok(_) => panic!("expected connect to fail after excessive auth challenges"),
            Err(err) => err.to_string(),
        };
        assert!(err.contains("authentication challenge sequence exceeded 8 rounds"));
        server.join().unwrap();
    }

    #[test]
    fn native_execute_reprepares_on_unprepared() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        let server = thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();

            let startup = read_frame(&mut socket).unwrap();
            assert_eq!(startup.header.opcode, Opcode::Startup);
            write_frame(&mut socket, &ready_frame(startup.header.stream_id)).unwrap();

            let first_prepare = read_frame(&mut socket).unwrap();
            assert_eq!(first_prepare.header.opcode, Opcode::Prepare);
            write_frame(
                &mut socket,
                &prepared_result_frame(first_prepare.header.stream_id, b"pid-a", &[0x0002]),
            )
            .unwrap();

            let first_execute = read_frame(&mut socket).unwrap();
            assert_eq!(first_execute.header.opcode, Opcode::Execute);
            write_frame(
                &mut socket,
                &error_unprepared_frame(first_execute.header.stream_id, b"pid-a"),
            )
            .unwrap();

            let second_prepare = read_frame(&mut socket).unwrap();
            assert_eq!(second_prepare.header.opcode, Opcode::Prepare);
            write_frame(
                &mut socket,
                &prepared_result_frame(second_prepare.header.stream_id, b"pid-b", &[0x0002]),
            )
            .unwrap();

            let second_execute = read_frame(&mut socket).unwrap();
            assert_eq!(second_execute.header.opcode, Opcode::Execute);
            let mut cursor: &[u8] = &second_execute.body;
            let prepared_id = types::read_short_bytes(&mut cursor).unwrap();
            assert_eq!(prepared_id, b"pid-b".to_vec());
            write_frame(
                &mut socket,
                &result_void_frame(second_execute.header.stream_id),
            )
            .unwrap();
        });

        let mut session = NativeSession::connect(&addr.to_string(), None).unwrap();
        let mut plan = session
            .prepare("INSERT INTO ks.tbl (k) VALUES (?)")
            .unwrap();
        assert_eq!(plan.id, b"pid-a".to_vec());
        let values = bind_values_for(&plan.bind_kinds, 11);
        session
            .execute_with_reprepare(&mut plan, &values, Consistency::One)
            .unwrap();
        assert_eq!(plan.id, b"pid-b".to_vec());

        server.join().unwrap();
    }

    #[test]
    fn parse_mixed_rejects_invalid_ratio() {
        let err = parse_args(&[
            "mixed".to_string(),
            "--read-ratio".to_string(),
            "1.5".to_string(),
        ])
        .unwrap_err()
        .to_string();
        assert!(err.contains("read-ratio"));
    }

    #[test]
    fn run_stress_tracks_failures_for_unreachable_admin_host() {
        let cfg = StressConfig {
            mode: StressMode::Read,
            ops: 20,
            concurrency: 4,
            ..StressConfig::default()
        };
        let report = run_stress("http://127.0.0.1:1", &cfg);
        assert_eq!(report.total_ops, 20);
        assert_eq!(report.executed_ops, 20);
        assert_eq!(report.warmup_ops, 0);
        assert_eq!(report.ok_ops + report.failed_ops, 20);
        assert_eq!(report.failed_ops, 20);
    }

    #[test]
    fn run_stress_tracks_failures_for_unreachable_native_host() {
        let cfg = StressConfig {
            mode: StressMode::Read,
            transport: TransportMode::NativeCql,
            ops: 10,
            concurrency: 2,
            ..StressConfig::default()
        };
        let report = run_stress("127.0.0.1:1", &cfg);
        assert_eq!(report.total_ops, 10);
        assert_eq!(report.executed_ops, 10);
        assert_eq!(report.warmup_ops, 0);
        assert_eq!(report.ok_ops + report.failed_ops, 10);
        assert_eq!(report.failed_ops, 10);
    }

    #[test]
    fn run_stress_stops_early_when_duration_limit_hits() {
        let cfg = StressConfig {
            mode: StressMode::Read,
            ops: 1_000,
            concurrency: 2,
            rate_ops_per_sec: Some(10),
            duration_secs: Some(Duration::from_secs(1)),
            ..StressConfig::default()
        };
        let report = run_stress("http://127.0.0.1:1", &cfg);
        assert!(report.total_ops > 0);
        assert!(report.total_ops < cfg.ops);
        assert_eq!(report.total_ops, report.executed_ops);
        assert_eq!(report.warmup_ops, 0);
        assert_eq!(report.ok_ops + report.failed_ops, report.total_ops);
    }

    #[test]
    fn run_stress_excludes_warmup_ops_from_metrics() {
        let cfg = StressConfig {
            mode: StressMode::Read,
            ops: 1_000,
            concurrency: 2,
            rate_ops_per_sec: Some(10),
            duration_secs: Some(Duration::from_secs(1)),
            warmup_secs: Some(Duration::from_secs(2)),
            ..StressConfig::default()
        };
        let report = run_stress("http://127.0.0.1:1", &cfg);
        assert!(report.executed_ops > 0);
        assert_eq!(report.total_ops, 0);
        assert_eq!(report.ok_ops, 0);
        assert_eq!(report.failed_ops, 0);
        assert_eq!(report.warmup_ops, report.executed_ops);
    }

    #[test]
    fn run_stress_excludes_warmup_target_ops_from_metrics() {
        let cfg = StressConfig {
            mode: StressMode::Read,
            ops: 1_000,
            concurrency: 2,
            rate_ops_per_sec: Some(10),
            duration_secs: Some(Duration::from_secs(1)),
            warmup_ops: Some(100),
            ..StressConfig::default()
        };
        let report = run_stress("http://127.0.0.1:1", &cfg);
        assert!(report.executed_ops > 0);
        assert_eq!(report.total_ops, 0);
        assert_eq!(report.ok_ops, 0);
        assert_eq!(report.failed_ops, 0);
        assert_eq!(report.warmup_ops, report.executed_ops);
    }
}
