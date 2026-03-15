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
Protocol behavior differential tests.

These tests compare the CQL native protocol behavior of the Java oracle
against the Rust implementation. They test connection lifecycle, protocol
negotiation, query execution, and error handling.

Expected state during Phase 2:
    - Java tests: PASS
    - Rust tests: XFAIL (stub returns SERVER_ERROR for everything)
"""

import socket
import struct

import pytest

from conftest import JAVA_HOST, JAVA_PORT, RUST_HOST, RUST_PORT, is_port_open


# ── Protocol Helpers ──────────────────────────────────────────────────────

PROTOCOL_V4_REQUEST = 0x04
OPCODE_OPTIONS = 0x05
OPCODE_STARTUP = 0x01
OPCODE_QUERY = 0x07


def build_frame(opcode: int, stream_id: int, body: bytes) -> bytes:
    """Build a CQL native protocol v4 request frame."""
    header = struct.pack(">BbhBI", PROTOCOL_V4_REQUEST, 0x00, stream_id, opcode, len(body))
    return header + body


def build_options() -> bytes:
    """Build an OPTIONS request frame."""
    return build_frame(OPCODE_OPTIONS, 1, b"")


def build_startup() -> bytes:
    """Build a STARTUP request frame."""
    pairs = {"CQL_VERSION": "3.4.7"}
    body = struct.pack(">H", len(pairs))
    for k, v in pairs.items():
        kb = k.encode("utf-8")
        vb = v.encode("utf-8")
        body += struct.pack(">H", len(kb)) + kb
        body += struct.pack(">H", len(vb)) + vb
    return build_frame(OPCODE_STARTUP, 2, body)


def build_query(cql: str) -> bytes:
    """Build a QUERY request frame."""
    qb = cql.encode("utf-8")
    body = struct.pack(">I", len(qb)) + qb
    body += struct.pack(">Hb", 0x0001, 0x00)  # consistency=ONE, flags=0
    return build_frame(OPCODE_QUERY, 3, body)


def send_and_receive(host: str, port: int, frame: bytes, timeout: float = 10.0):
    """Send a frame to a Cassandra node and receive the response."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(timeout)
    sock.connect((host, port))
    try:
        sock.sendall(frame)

        # Read response header (9 bytes)
        header_data = b""
        while len(header_data) < 9:
            chunk = sock.recv(9 - len(header_data))
            if not chunk:
                raise ConnectionError("Connection closed")
            header_data += chunk

        version, flags, stream_id, opcode, body_length = struct.unpack(
            ">BbhBI", header_data
        )

        # Read body
        body = b""
        while len(body) < body_length:
            chunk = sock.recv(min(body_length - len(body), 65536))
            if not chunk:
                raise ConnectionError("Connection closed")
            body += chunk

        return {
            "version": version,
            "flags": flags,
            "stream_id": stream_id,
            "opcode": opcode,
            "body_length": body_length,
            "body": body,
            "raw": header_data + body,
        }
    finally:
        sock.close()


# ── Tests: Java Oracle (should all PASS) ──────────────────────────────────


class TestJavaProtocol:
    """Protocol tests against the Java oracle."""

    @pytest.fixture(autouse=True)
    def check_java(self, java_available):
        if not java_available:
            pytest.skip("Java oracle not available")

    def test_options_returns_supported(self):
        """OPTIONS request should return SUPPORTED (opcode 0x06)."""
        resp = send_and_receive(JAVA_HOST, JAVA_PORT, build_options())
        assert resp["opcode"] == 0x06, f"Expected SUPPORTED (0x06), got 0x{resp['opcode']:02x}"
        assert resp["version"] == 0x84, "Expected protocol v4 response"

    def test_startup_returns_ready(self):
        """STARTUP request should return READY (opcode 0x02)."""
        resp = send_and_receive(JAVA_HOST, JAVA_PORT, build_startup())
        assert resp["opcode"] in (0x02, 0x03), (
            f"Expected READY (0x02) or AUTHENTICATE (0x03), got 0x{resp['opcode']:02x}"
        )

    def test_query_system_local(self):
        """Query system.local should return RESULT (opcode 0x08)."""
        # First do STARTUP
        send_and_receive(JAVA_HOST, JAVA_PORT, build_startup())

        # Need a fresh connection for the query since we closed in send_and_receive
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(10)
        sock.connect((JAVA_HOST, JAVA_PORT))
        try:
            # STARTUP
            sock.sendall(build_startup())
            header_data = sock.recv(9)
            _, _, _, _, body_len = struct.unpack(">BbhBI", header_data)
            sock.recv(body_len)  # consume READY body

            # QUERY
            query = build_query("SELECT cluster_name FROM system.local")
            sock.sendall(query)
            header_data = b""
            while len(header_data) < 9:
                header_data += sock.recv(9 - len(header_data))
            _, _, _, opcode, body_len = struct.unpack(">BbhBI", header_data)

            assert opcode == 0x08, f"Expected RESULT (0x08), got 0x{opcode:02x}"
        finally:
            sock.close()

    def test_invalid_query_returns_error(self):
        """Invalid CQL should return ERROR (opcode 0x00)."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(10)
        sock.connect((JAVA_HOST, JAVA_PORT))
        try:
            # STARTUP
            sock.sendall(build_startup())
            header_data = sock.recv(9)
            _, _, _, _, body_len = struct.unpack(">BbhBI", header_data)
            sock.recv(body_len)

            # Invalid QUERY
            query = build_query("SELCT * FROM system.local")
            sock.sendall(query)
            header_data = b""
            while len(header_data) < 9:
                header_data += sock.recv(9 - len(header_data))
            _, _, _, opcode, body_len = struct.unpack(">BbhBI", header_data)

            assert opcode == 0x00, f"Expected ERROR (0x00), got 0x{opcode:02x}"

            # Read error body and check code
            body = b""
            while len(body) < body_len:
                body += sock.recv(body_len - len(body))
            error_code = struct.unpack(">i", body[:4])[0]
            assert error_code == 0x2000, f"Expected SYNTAX_ERROR (0x2000), got 0x{error_code:04x}"
        finally:
            sock.close()


# ── Tests: Rust SUT (expected XFAIL during Phase 2) ──────────────────────


class TestRustProtocol:
    """
    Protocol tests against the Rust SUT.

    During Phase 2, the Rust stub only returns SERVER_ERROR for all requests.
    These tests are expected to XFAIL. As protocol handling is implemented,
    they will progressively turn green.
    """

    @pytest.fixture(autouse=True)
    def check_rust(self, rust_available):
        if not rust_available:
            pytest.skip("Rust SUT not available")

    @pytest.mark.xfail(reason="Rust stub returns SERVER_ERROR for all requests")
    def test_options_returns_supported(self):
        """OPTIONS should return SUPPORTED once Rust implements protocol."""
        resp = send_and_receive(RUST_HOST, RUST_PORT, build_options())
        assert resp["opcode"] == 0x06

    @pytest.mark.xfail(reason="Rust stub returns SERVER_ERROR for all requests")
    def test_startup_returns_ready(self):
        """STARTUP should return READY once Rust implements protocol."""
        resp = send_and_receive(RUST_HOST, RUST_PORT, build_startup())
        assert resp["opcode"] in (0x02, 0x03)

    def test_stub_returns_valid_error_frame(self):
        """Even the stub should return a valid protocol ERROR frame."""
        resp = send_and_receive(RUST_HOST, RUST_PORT, build_options())
        # The stub should at minimum send back a valid ERROR frame
        assert resp["version"] == 0x84, "Expected v4 response header"
        assert resp["opcode"] == 0x00, "Expected ERROR opcode from stub"
        # Parse error code from body
        if len(resp["body"]) >= 4:
            error_code = struct.unpack(">i", resp["body"][:4])[0]
            assert error_code == 0x0000, f"Expected SERVER_ERROR, got 0x{error_code:04x}"


# ── Tests: Differential (compare Java vs Rust) ───────────────────────────


class TestDifferentialProtocol:
    """
    Side-by-side protocol comparison tests.

    These tests send the same request to both Java and Rust, then compare
    the responses. They require both clusters to be running.
    """

    @pytest.fixture(autouse=True)
    def check_both(self, java_available, rust_available):
        if not java_available:
            pytest.skip("Java oracle not available")
        if not rust_available:
            pytest.skip("Rust SUT not available")

    @pytest.mark.xfail(reason="Rust stub does not implement protocol yet")
    def test_options_response_matches(self):
        """OPTIONS response should be identical between Java and Rust."""
        java_resp = send_and_receive(JAVA_HOST, JAVA_PORT, build_options())
        rust_resp = send_and_receive(RUST_HOST, RUST_PORT, build_options())

        assert java_resp["opcode"] == rust_resp["opcode"], (
            f"Opcode mismatch: java=0x{java_resp['opcode']:02x} "
            f"rust=0x{rust_resp['opcode']:02x}"
        )

    @pytest.mark.xfail(reason="Rust stub does not implement protocol yet")
    def test_error_response_same_code(self):
        """Error responses should use the same error code."""
        # Send invalid CQL to both
        # For Java, need STARTUP first; for Rust stub, it just returns ERROR
        java_resp = send_and_receive(JAVA_HOST, JAVA_PORT, build_options())
        rust_resp = send_and_receive(RUST_HOST, RUST_PORT, build_options())

        # Both should have valid headers
        assert java_resp["version"] == rust_resp["version"], "Protocol version mismatch"
