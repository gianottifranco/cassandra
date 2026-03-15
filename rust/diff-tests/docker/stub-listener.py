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
Minimal TCP stub listener for the Cassandra Rust node.

Accepts connections on port 9042 and responds to any incoming data with a
Cassandra native protocol SERVER_ERROR frame. This allows the diff-test
harness to verify network connectivity and basic protocol error handling
before the real Rust protocol engine is implemented.

Protocol v4 ERROR response format:
  [header: 9 bytes]
    version:  0x84 (response, protocol v4)
    flags:    0x00
    stream:   <from request>
    opcode:   0x00 (ERROR)
    length:   <body length, 4 bytes big-endian>
  [body]
    error_code: 0x0000_0000 (SERVER_ERROR, 4 bytes big-endian)
    message:    [short] "Rust implementation not yet available"
"""

import socket
import struct
import sys
import threading
import signal
import os

LISTEN_HOST = "0.0.0.0"
LISTEN_PORT = int(os.environ.get("CASSANDRA_NATIVE_PORT", "9042"))

# Cassandra protocol constants
PROTOCOL_V4_RESPONSE = 0x84
OPCODE_ERROR = 0x00
ERROR_SERVER_ERROR = 0x0000

ERROR_MESSAGE = "Rust implementation not yet available"


def build_error_frame(stream_id: int) -> bytes:
    """Build a CQL native protocol v4 ERROR frame."""
    # Body: error_code (4 bytes) + message ([short] = 2-byte len + UTF-8 bytes)
    msg_bytes = ERROR_MESSAGE.encode("utf-8")
    body = struct.pack(">i", ERROR_SERVER_ERROR) + struct.pack(">H", len(msg_bytes)) + msg_bytes

    # Header: version (1) + flags (1) + stream (2) + opcode (1) + length (4)
    header = struct.pack(
        ">BbhBI",
        PROTOCOL_V4_RESPONSE,
        0x00,  # flags
        stream_id,
        OPCODE_ERROR,
        len(body),
    )
    return header + body


def handle_client(conn: socket.socket, addr: tuple):
    """Handle a single client connection."""
    try:
        while True:
            # Read protocol header (9 bytes for v4/v5)
            header_data = b""
            while len(header_data) < 9:
                chunk = conn.recv(9 - len(header_data))
                if not chunk:
                    return
                header_data += chunk

            # Parse stream ID from header
            _version, _flags, stream_id, _opcode, body_length = struct.unpack(
                ">BbhBI", header_data
            )

            # Read and discard body
            remaining = body_length
            while remaining > 0:
                chunk = conn.recv(min(remaining, 4096))
                if not chunk:
                    return
                remaining -= len(chunk)

            # Send ERROR response
            response = build_error_frame(stream_id)
            conn.sendall(response)

            print(f"[stub] {addr} stream={stream_id} opcode=0x{_opcode:02x} → ERROR", flush=True)
    except (ConnectionResetError, BrokenPipeError, OSError):
        pass
    finally:
        conn.close()


def main():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((LISTEN_HOST, LISTEN_PORT))
    server.listen(64)

    print(f"[stub] Cassandra Rust stub listening on {LISTEN_HOST}:{LISTEN_PORT}", flush=True)
    print(f"[stub] All requests will receive SERVER_ERROR response", flush=True)

    # Graceful shutdown
    def shutdown(signum, frame):
        print("\n[stub] Shutting down...", flush=True)
        server.close()
        sys.exit(0)

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    while True:
        try:
            conn, addr = server.accept()
            print(f"[stub] Connection from {addr}", flush=True)
            t = threading.Thread(target=handle_client, args=(conn, addr), daemon=True)
            t.start()
        except OSError:
            break


if __name__ == "__main__":
    main()
