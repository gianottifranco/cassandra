#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Golden Fixture Generator for Cassandra Differential Testing.

Connects to a running Cassandra Java oracle node, executes a suite of
CQL operations, and exports the observable results as golden fixtures
(JSON + binary) for offline comparison against the Rust implementation.

Usage:
    python3 generate_golden.py --host <cassandra-host> --output <dir>
    python3 generate_golden.py --host java-oracle --output /golden

Categories of fixtures generated:
    protocol/   - Native protocol frame metadata
    queries/    - CQL query result payloads
    errors/     - Error codes and messages
    types/      - Serialized CQL type values
    schema/     - System schema dumps
    tombstones/ - TTL and deletion behavior
    stubs/      - Manifests for future phases (SSTable, CommitLog, etc.)
"""

import argparse
import json
import os
import socket
import struct
import sys
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path

# Optional: cassandra-driver for CQL-level operations
try:
    from cassandra.cluster import Cluster
    from cassandra.query import SimpleStatement, ConsistencyLevel
    from cassandra import InvalidRequest, Unavailable, ReadTimeout, WriteTimeout
    HAS_DRIVER = True
except ImportError:
    HAS_DRIVER = False
    print("[golden-gen] WARNING: cassandra-driver not installed. "
          "Only raw protocol fixtures will be generated.", file=sys.stderr)


# ── Protocol Constants ────────────────────────────────────────────────────

PROTOCOL_V4_REQUEST = 0x04
PROTOCOL_V4_RESPONSE = 0x84
OPCODE_STARTUP = 0x01
OPCODE_OPTIONS = 0x05
OPCODE_QUERY = 0x07
OPCODE_ERROR = 0x00
OPCODE_SUPPORTED = 0x06
OPCODE_READY = 0x02


# ── Test Schemas ──────────────────────────────────────────────────────────

SETUP_CQL = [
    "CREATE KEYSPACE IF NOT EXISTS diff_test WITH replication = "
    "{'class': 'SimpleStrategy', 'replication_factor': 1}",

    """CREATE TABLE IF NOT EXISTS diff_test.all_types (
        pk int PRIMARY KEY,
        col_ascii ascii,
        col_bigint bigint,
        col_blob blob,
        col_boolean boolean,
        col_decimal decimal,
        col_double double,
        col_float float,
        col_int int,
        col_text text,
        col_timestamp timestamp,
        col_uuid uuid,
        col_varchar varchar,
        col_varint varint,
        col_timeuuid timeuuid,
        col_inet inet,
        col_smallint smallint,
        col_tinyint tinyint,
        col_list list<text>,
        col_set set<int>,
        col_map map<text, int>,
        col_tuple tuple<int, text, boolean>
    )""",

    """CREATE TABLE IF NOT EXISTS diff_test.simple_kv (
        key text PRIMARY KEY,
        value text,
        version int
    )""",

    """CREATE TABLE IF NOT EXISTS diff_test.ttl_test (
        pk int PRIMARY KEY,
        value text
    )""",

    """CREATE TABLE IF NOT EXISTS diff_test.counter_test (
        pk text PRIMARY KEY,
        cnt counter
    )""",

    """CREATE TABLE IF NOT EXISTS diff_test.clustering_test (
        pk int,
        ck1 text,
        ck2 int,
        value text,
        PRIMARY KEY (pk, ck1, ck2)
    ) WITH CLUSTERING ORDER BY (ck1 ASC, ck2 DESC)""",
]

INSERT_CQL = [
    ("INSERT INTO diff_test.all_types (pk, col_ascii, col_bigint, col_boolean, "
     "col_double, col_float, col_int, col_text, col_smallint, col_tinyint) "
     "VALUES (1, 'hello', 9223372036854775807, true, 3.14159, 2.71, 42, "
     "'world', 32000, 127)"),

    "INSERT INTO diff_test.simple_kv (key, value, version) VALUES ('k1', 'v1', 1)",
    "INSERT INTO diff_test.simple_kv (key, value, version) VALUES ('k2', 'v2', 2)",
    "INSERT INTO diff_test.simple_kv (key, value, version) VALUES ('k3', 'v3', 3)",

    # TTL insert
    "INSERT INTO diff_test.ttl_test (pk, value) VALUES (1, 'expires') USING TTL 86400",
    "INSERT INTO diff_test.ttl_test (pk, value) VALUES (2, 'no_ttl')",

    # Counter
    "UPDATE diff_test.counter_test SET cnt = cnt + 5 WHERE pk = 'c1'",
    "UPDATE diff_test.counter_test SET cnt = cnt + 3 WHERE pk = 'c1'",

    # Clustering
    "INSERT INTO diff_test.clustering_test (pk, ck1, ck2, value) VALUES (1, 'a', 1, 'a1')",
    "INSERT INTO diff_test.clustering_test (pk, ck1, ck2, value) VALUES (1, 'a', 2, 'a2')",
    "INSERT INTO diff_test.clustering_test (pk, ck1, ck2, value) VALUES (1, 'b', 1, 'b1')",
    "INSERT INTO diff_test.clustering_test (pk, ck1, ck2, value) VALUES (1, 'b', 3, 'b3')",
]

QUERY_CQL = {
    "select_all_types": "SELECT * FROM diff_test.all_types WHERE pk = 1",
    "select_simple_kv_all": "SELECT * FROM diff_test.simple_kv",
    "select_clustering_ordered": "SELECT * FROM diff_test.clustering_test WHERE pk = 1",
    "select_clustering_reversed": "SELECT * FROM diff_test.clustering_test WHERE pk = 1 ORDER BY ck1 DESC",
    "select_clustering_slice": "SELECT * FROM diff_test.clustering_test WHERE pk = 1 AND ck1 = 'a'",
    "select_counter": "SELECT * FROM diff_test.counter_test WHERE pk = 'c1'",
    "select_ttl_writeTime": "SELECT pk, value, TTL(value), WRITETIME(value) FROM diff_test.ttl_test",
    "select_count": "SELECT COUNT(*) FROM diff_test.simple_kv",
    "select_system_local": "SELECT cluster_name, data_center, rack, release_version, "
                           "cql_version, native_protocol_version FROM system.local",
    "select_system_peers": "SELECT peer, data_center, rack, release_version FROM system.peers",
}

ERROR_CQL = {
    "syntax_error": "SELCT * FROM system.local",
    "invalid_keyspace": "SELECT * FROM nonexistent_ks.nonexistent_table",
    "type_mismatch": "INSERT INTO diff_test.simple_kv (key, value, version) VALUES ('k', 'v', 'not_an_int')",
}


# ── Raw Protocol Helpers ──────────────────────────────────────────────────

def build_frame(opcode: int, stream_id: int, body: bytes) -> bytes:
    """Build a CQL native protocol v4 request frame."""
    header = struct.pack(">BbhBI", PROTOCOL_V4_REQUEST, 0x00, stream_id, opcode, len(body))
    return header + body


def build_startup_body() -> bytes:
    """Build STARTUP message body (string map: CQL_VERSION -> 3.4.7)."""
    pairs = {"CQL_VERSION": "3.4.7"}
    body = struct.pack(">H", len(pairs))
    for k, v in pairs.items():
        kb = k.encode("utf-8")
        vb = v.encode("utf-8")
        body += struct.pack(">H", len(kb)) + kb
        body += struct.pack(">H", len(vb)) + vb
    return body


def build_options_body() -> bytes:
    """Build OPTIONS message body (empty)."""
    return b""


def build_query_body(query: str) -> bytes:
    """Build QUERY message body."""
    qb = query.encode("utf-8")
    body = struct.pack(">I", len(qb)) + qb
    # Query parameters: consistency=ONE, flags=0x00
    body += struct.pack(">Hb", 0x0001, 0x00)
    return body


def parse_response_header(data: bytes):
    """Parse a CQL native protocol response header."""
    version, flags, stream_id, opcode, length = struct.unpack(">BbhBI", data[:9])
    return {
        "version": version,
        "flags": flags,
        "stream_id": stream_id,
        "opcode": opcode,
        "body_length": length,
    }


def send_frame_and_receive(sock: socket.socket, frame: bytes):
    """Send a frame and receive the full response."""
    sock.sendall(frame)
    # Read response header
    header_data = b""
    while len(header_data) < 9:
        chunk = sock.recv(9 - len(header_data))
        if not chunk:
            raise ConnectionError("Connection closed")
        header_data += chunk

    header = parse_response_header(header_data)
    body_len = header["body_length"]

    body = b""
    while len(body) < body_len:
        chunk = sock.recv(min(body_len - len(body), 65536))
        if not chunk:
            raise ConnectionError("Connection closed")
        body += chunk

    return header, body, header_data + body


def raw_protocol_fixtures(host: str, port: int, output_dir: Path):
    """Generate raw protocol frame fixtures via TCP."""
    proto_dir = output_dir / "protocol"
    proto_dir.mkdir(parents=True, exist_ok=True)

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(30)
    sock.connect((host, port))

    try:
        # 1. OPTIONS request/response
        options_frame = build_frame(OPCODE_OPTIONS, 1, build_options_body())
        (proto_dir / "options_request.bin").write_bytes(options_frame)

        header, body, raw = send_frame_and_receive(sock, options_frame)
        (proto_dir / "options_response.bin").write_bytes(raw)
        (proto_dir / "options_response.json").write_text(json.dumps({
            "header": header,
            "body_hex": body.hex(),
            "description": "SUPPORTED response to OPTIONS request",
        }, indent=2))
        print(f"  [protocol] OPTIONS → opcode=0x{header['opcode']:02x}")

        # 2. STARTUP request/response
        startup_frame = build_frame(OPCODE_STARTUP, 2, build_startup_body())
        (proto_dir / "startup_request.bin").write_bytes(startup_frame)

        header, body, raw = send_frame_and_receive(sock, startup_frame)
        (proto_dir / "startup_response.bin").write_bytes(raw)
        (proto_dir / "startup_response.json").write_text(json.dumps({
            "header": header,
            "body_hex": body.hex(),
            "description": "READY response to STARTUP request",
        }, indent=2))
        print(f"  [protocol] STARTUP → opcode=0x{header['opcode']:02x}")

        # 3. QUERY request/response (system.local)
        query = "SELECT cluster_name, release_version FROM system.local"
        query_frame = build_frame(OPCODE_QUERY, 3, build_query_body(query))
        (proto_dir / "query_system_local_request.bin").write_bytes(query_frame)

        header, body, raw = send_frame_and_receive(sock, query_frame)
        (proto_dir / "query_system_local_response.bin").write_bytes(raw)
        (proto_dir / "query_system_local_response.json").write_text(json.dumps({
            "header": header,
            "body_hex": body.hex(),
            "query": query,
            "description": "RESULT response to QUERY on system.local",
        }, indent=2))
        print(f"  [protocol] QUERY '{query}' → opcode=0x{header['opcode']:02x}")

        # 4. Error-inducing QUERY
        bad_query = "SELCT * FROM system.local"
        error_frame = build_frame(OPCODE_QUERY, 4, build_query_body(bad_query))
        (proto_dir / "error_syntax_request.bin").write_bytes(error_frame)

        header, body, raw = send_frame_and_receive(sock, error_frame)
        (proto_dir / "error_syntax_response.bin").write_bytes(raw)

        error_code = struct.unpack(">i", body[:4])[0] if len(body) >= 4 else None
        (proto_dir / "error_syntax_response.json").write_text(json.dumps({
            "header": header,
            "body_hex": body.hex(),
            "error_code": error_code,
            "query": bad_query,
            "description": "ERROR response to invalid CQL syntax",
        }, indent=2))
        print(f"  [protocol] ERROR query → opcode=0x{header['opcode']:02x} code={error_code}")

    finally:
        sock.close()


# ── CQL-Level Fixtures (requires cassandra-driver) ────────────────────────

def serialize_row(row, columns):
    """Serialize a cassandra-driver row to a JSON-compatible dict."""
    result = {}
    for col in columns:
        val = getattr(row, col, None)
        if val is None:
            result[col] = None
        elif isinstance(val, bytes):
            result[col] = val.hex()
        elif isinstance(val, (datetime,)):
            result[col] = val.isoformat()
        elif isinstance(val, (set, frozenset)):
            result[col] = sorted(str(v) for v in val)
        elif isinstance(val, (list, tuple)):
            result[col] = [str(v) for v in val]
        elif isinstance(val, dict):
            result[col] = {str(k): str(v) for k, v in val.items()}
        else:
            result[col] = str(val) if not isinstance(val, (int, float, bool)) else val
    return result


def cql_fixtures(host: str, port: int, output_dir: Path):
    """Generate CQL-level golden fixtures."""
    if not HAS_DRIVER:
        print("[golden-gen] Skipping CQL fixtures (no cassandra-driver)")
        return

    cluster = Cluster([host], port=port)
    session = cluster.connect()

    try:
        # Setup test schema
        print("[golden-gen] Setting up test schema...")
        for cql in SETUP_CQL:
            try:
                session.execute(cql)
            except Exception as e:
                print(f"  [setup] Warning: {e}")

        # Insert test data
        print("[golden-gen] Inserting test data...")
        for cql in INSERT_CQL:
            try:
                session.execute(cql)
            except Exception as e:
                print(f"  [insert] Warning: {e}")

        # Query fixtures
        queries_dir = output_dir / "queries"
        queries_dir.mkdir(parents=True, exist_ok=True)

        print("[golden-gen] Generating query fixtures...")
        for name, cql in QUERY_CQL.items():
            try:
                result = session.execute(cql)
                columns = [col.column_name for col in result.column_names] if hasattr(result, 'column_names') else [c[0] for c in result.column_types] if hasattr(result, 'column_types') else []

                # Get column names from the result set
                if hasattr(result, '_current_rows') and len(result._current_rows) > 0:
                    row = result._current_rows[0]
                    columns = list(row._fields) if hasattr(row, '_fields') else []

                rows = []
                for row in result:
                    if hasattr(row, '_fields'):
                        columns = list(row._fields)
                    rows.append(serialize_row(row, columns))

                fixture = {
                    "query": cql,
                    "columns": columns,
                    "row_count": len(rows),
                    "rows": rows,
                    "generated_at": datetime.now(timezone.utc).isoformat(),
                }
                (queries_dir / f"{name}.json").write_text(json.dumps(fixture, indent=2))
                print(f"  [query] {name}: {len(rows)} rows")
            except Exception as e:
                print(f"  [query] {name}: ERROR - {e}")
                (queries_dir / f"{name}.json").write_text(json.dumps({
                    "query": cql,
                    "error": str(e),
                    "generated_at": datetime.now(timezone.utc).isoformat(),
                }, indent=2))

        # Error fixtures
        errors_dir = output_dir / "errors"
        errors_dir.mkdir(parents=True, exist_ok=True)

        print("[golden-gen] Generating error fixtures...")
        error_results = {}
        for name, cql in ERROR_CQL.items():
            try:
                session.execute(cql)
                error_results[name] = {"query": cql, "error": None, "message": "No error (unexpected)"}
            except Exception as e:
                error_results[name] = {
                    "query": cql,
                    "error_type": type(e).__name__,
                    "message": str(e),
                }
                print(f"  [error] {name}: {type(e).__name__}")

        (errors_dir / "error_codes.json").write_text(json.dumps(error_results, indent=2))

        # Generate canonical error code map from Cassandra protocol spec
        error_code_map = {
            "0x0000": {"name": "SERVER_ERROR", "description": "Server error"},
            "0x000A": {"name": "PROTOCOL_ERROR", "description": "Protocol error"},
            "0x0100": {"name": "BAD_CREDENTIALS", "description": "Bad credentials"},
            "0x1000": {"name": "UNAVAILABLE", "description": "Unavailable"},
            "0x1001": {"name": "OVERLOADED", "description": "Overloaded"},
            "0x1002": {"name": "IS_BOOTSTRAPPING", "description": "Is bootstrapping"},
            "0x1003": {"name": "TRUNCATE_ERROR", "description": "Truncate error"},
            "0x1100": {"name": "WRITE_TIMEOUT", "description": "Write timeout"},
            "0x1200": {"name": "READ_TIMEOUT", "description": "Read timeout"},
            "0x1300": {"name": "READ_FAILURE", "description": "Read failure"},
            "0x1400": {"name": "FUNCTION_FAILURE", "description": "Function failure"},
            "0x1500": {"name": "WRITE_FAILURE", "description": "Write failure"},
            "0x1600": {"name": "CDC_WRITE_FAILURE", "description": "CDC write failure"},
            "0x1700": {"name": "CAS_WRITE_UNKNOWN", "description": "CAS write unknown"},
            "0x2000": {"name": "SYNTAX_ERROR", "description": "Syntax error"},
            "0x2100": {"name": "UNAUTHORIZED", "description": "Unauthorized"},
            "0x2200": {"name": "INVALID", "description": "Invalid query"},
            "0x2300": {"name": "CONFIG_ERROR", "description": "Config error"},
            "0x2400": {"name": "ALREADY_EXISTS", "description": "Already exists"},
            "0x2500": {"name": "UNPREPARED", "description": "Unprepared"},
        }
        (errors_dir / "protocol_error_codes.json").write_text(json.dumps(error_code_map, indent=2))

        # Schema fixtures
        schema_dir = output_dir / "schema"
        schema_dir.mkdir(parents=True, exist_ok=True)

        print("[golden-gen] Generating schema fixtures...")
        schema_queries = {
            "keyspaces": "SELECT * FROM system_schema.keyspaces",
            "tables": "SELECT * FROM system_schema.tables WHERE keyspace_name = 'diff_test'",
            "columns": "SELECT * FROM system_schema.columns WHERE keyspace_name = 'diff_test'",
        }
        for name, cql in schema_queries.items():
            try:
                result = session.execute(cql)
                rows = []
                columns = []
                for row in result:
                    if hasattr(row, '_fields'):
                        columns = list(row._fields)
                    rows.append(serialize_row(row, columns))
                (schema_dir / f"{name}.json").write_text(json.dumps({
                    "query": cql,
                    "columns": columns,
                    "rows": rows,
                    "generated_at": datetime.now(timezone.utc).isoformat(),
                }, indent=2))
                print(f"  [schema] {name}: {len(rows)} rows")
            except Exception as e:
                print(f"  [schema] {name}: ERROR - {e}")

        # Tombstone / TTL fixtures
        tombstone_dir = output_dir / "tombstones"
        tombstone_dir.mkdir(parents=True, exist_ok=True)

        print("[golden-gen] Generating tombstone/TTL fixtures...")
        # Delete a row to create a tombstone
        session.execute("DELETE FROM diff_test.simple_kv WHERE key = 'k3'")
        tombstone_queries = {
            "after_delete": "SELECT * FROM diff_test.simple_kv",
            "ttl_check": "SELECT pk, value, TTL(value) FROM diff_test.ttl_test",
        }
        for name, cql in tombstone_queries.items():
            try:
                result = session.execute(cql)
                rows = []
                columns = []
                for row in result:
                    if hasattr(row, '_fields'):
                        columns = list(row._fields)
                    rows.append(serialize_row(row, columns))
                (tombstone_dir / f"{name}.json").write_text(json.dumps({
                    "query": cql,
                    "columns": columns,
                    "rows": rows,
                    "description": f"Result after tombstone/TTL operations: {name}",
                    "generated_at": datetime.now(timezone.utc).isoformat(),
                }, indent=2))
                print(f"  [tombstone] {name}: {len(rows)} rows")
            except Exception as e:
                print(f"  [tombstone] {name}: ERROR - {e}")

    finally:
        cluster.shutdown()


# ── Stub Fixtures (manifests for future phases) ───────────────────────────

def stub_fixtures(output_dir: Path):
    """Generate manifest stubs for SSTable, CommitLog, etc."""
    stubs_dir = output_dir / "stubs"
    stubs_dir.mkdir(parents=True, exist_ok=True)

    print("[golden-gen] Generating stub manifests...")

    # SSTable manifest
    (stubs_dir / "sstable_manifest.json").write_text(json.dumps({
        "description": "SSTable golden fixture manifest",
        "status": "pending",
        "phase_target": "Phase 4 (Storage Engine)",
        "expected_formats": ["nb (big-format)", "nc (BTI format)"],
        "expected_fixtures": [
            "simple_table_sstable.db",
            "clustering_table_sstable.db",
            "counter_table_sstable.db",
            "sstable_statistics.json",
            "compression_info.db",
            "index_summary.db",
            "bloom_filter.db",
        ],
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }, indent=2))

    # CommitLog manifest
    (stubs_dir / "commitlog_manifest.json").write_text(json.dumps({
        "description": "CommitLog golden fixture manifest",
        "status": "pending",
        "phase_target": "Phase 4 (Storage Engine)",
        "expected_fixtures": [
            "commitlog_segment.log",
            "commitlog_header.json",
            "commitlog_replay_expected.json",
        ],
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }, indent=2))

    # Hints manifest
    (stubs_dir / "hints_manifest.json").write_text(json.dumps({
        "description": "Hinted handoff golden fixture manifest",
        "status": "pending",
        "phase_target": "Phase 7 (Production Hardening)",
        "expected_fixtures": [
            "hint_file.hints",
            "hint_header.json",
            "hint_replay_expected.json",
        ],
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }, indent=2))

    # Topology scenarios
    (stubs_dir / "topology_scenarios.json").write_text(json.dumps({
        "description": "Topology golden fixture manifest",
        "status": "partial",
        "scenarios": [
            {
                "name": "single_node",
                "nodes": 1,
                "replication_factor": 1,
                "status": "available_via_docker_compose",
            },
            {
                "name": "three_node",
                "nodes": 3,
                "replication_factor": 3,
                "status": "available_via_docker_compose_three_node_profile",
            },
            {
                "name": "multi_dc",
                "nodes": 6,
                "replication_factor": {"dc1": 3, "dc2": 3},
                "status": "pending_phase_6",
            },
            {
                "name": "bootstrap",
                "description": "Adding a node to existing cluster",
                "status": "pending_phase_6",
            },
            {
                "name": "decommission",
                "description": "Removing a node from cluster",
                "status": "pending_phase_6",
            },
        ],
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }, indent=2))

    print("  [stubs] sstable_manifest.json")
    print("  [stubs] commitlog_manifest.json")
    print("  [stubs] hints_manifest.json")
    print("  [stubs] topology_scenarios.json")


# ── Main ──────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Generate golden fixtures from Cassandra Java oracle"
    )
    parser.add_argument("--host", default="127.0.0.1", help="Cassandra host")
    parser.add_argument("--port", type=int, default=9042, help="Native transport port")
    parser.add_argument("--output", default="./golden", help="Output directory")
    parser.add_argument("--skip-cql", action="store_true",
                        help="Skip CQL-level fixtures (only raw protocol + stubs)")
    args = parser.parse_args()

    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"[golden-gen] Generating golden fixtures → {output_dir}")
    print(f"[golden-gen] Oracle: {args.host}:{args.port}")

    # Metadata
    (output_dir / "metadata.json").write_text(json.dumps({
        "oracle_host": args.host,
        "oracle_port": args.port,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "generator_version": "1.0.0",
        "baseline_commit": "076c6f11364645bbb43360f013bee6f50a099185",
    }, indent=2))

    # Raw protocol fixtures
    try:
        print("\n[golden-gen] === Raw Protocol Fixtures ===")
        raw_protocol_fixtures(args.host, args.port, output_dir)
    except Exception as e:
        print(f"[golden-gen] ERROR generating protocol fixtures: {e}")
        traceback.print_exc()

    # CQL-level fixtures
    if not args.skip_cql:
        try:
            print("\n[golden-gen] === CQL Query Fixtures ===")
            cql_fixtures(args.host, args.port, output_dir)
        except Exception as e:
            print(f"[golden-gen] ERROR generating CQL fixtures: {e}")
            traceback.print_exc()

    # Stub manifests (always generated, no server needed)
    print("\n[golden-gen] === Stub Manifests ===")
    stub_fixtures(output_dir)

    print(f"\n[golden-gen] Done. Fixtures written to {output_dir}")


if __name__ == "__main__":
    main()
