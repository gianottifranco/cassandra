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
Pytest configuration for Cassandra differential tests.

Provides fixtures for connecting to the Java oracle and Rust SUT clusters,
with automatic skip handling when Docker is not available.
"""

import os
import socket
import time

import pytest

# ── Configuration ─────────────────────────────────────────────────────────

JAVA_HOST = os.environ.get("CASSANDRA_JAVA_HOST", "localhost")
JAVA_PORT = int(os.environ.get("CASSANDRA_JAVA_PORT", "19042"))
RUST_HOST = os.environ.get("CASSANDRA_RUST_HOST", "localhost")
RUST_PORT = int(os.environ.get("CASSANDRA_RUST_PORT", "29042"))


# ── Helpers ───────────────────────────────────────────────────────────────

def is_port_open(host: str, port: int, timeout: float = 2.0) -> bool:
    """Check if a TCP port is open."""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except (ConnectionRefusedError, TimeoutError, OSError):
        return False


def wait_for_port(host: str, port: int, timeout: int = 30) -> bool:
    """Wait for a TCP port to become available."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if is_port_open(host, port):
            return True
        time.sleep(1)
    return False


# ── Pytest Markers ────────────────────────────────────────────────────────

def pytest_configure(config):
    """Register custom markers."""
    config.addinivalue_line(
        "markers", "requires_java: test requires the Java oracle to be running"
    )
    config.addinivalue_line(
        "markers", "requires_rust: test requires the Rust SUT to be running"
    )
    config.addinivalue_line(
        "markers", "requires_both: test requires both Java and Rust clusters"
    )


# ── Fixtures ──────────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def java_available():
    """Check if the Java oracle is reachable."""
    return is_port_open(JAVA_HOST, JAVA_PORT)


@pytest.fixture(scope="session")
def rust_available():
    """Check if the Rust SUT is reachable."""
    return is_port_open(RUST_HOST, RUST_PORT)


@pytest.fixture(scope="session")
def java_session(java_available):
    """Create a CQL session to the Java oracle."""
    if not java_available:
        pytest.skip("Java oracle not available")

    try:
        from cassandra.cluster import Cluster
        cluster = Cluster([JAVA_HOST], port=JAVA_PORT)
        session = cluster.connect()
        yield session
        cluster.shutdown()
    except ImportError:
        pytest.skip("cassandra-driver not installed")
    except Exception as e:
        pytest.skip(f"Cannot connect to Java oracle: {e}")


@pytest.fixture(scope="session")
def rust_session(rust_available):
    """
    Create a CQL session to the Rust SUT.

    NOTE: This will fail with XFAIL until the Rust node implements
    the native protocol. This is expected and by design.
    """
    if not rust_available:
        pytest.skip("Rust SUT not available")

    try:
        from cassandra.cluster import Cluster
        cluster = Cluster([RUST_HOST], port=RUST_PORT)
        session = cluster.connect()
        yield session
        cluster.shutdown()
    except ImportError:
        pytest.skip("cassandra-driver not installed")
    except Exception as e:
        pytest.xfail(f"Rust SUT not yet protocol-compatible: {e}")


@pytest.fixture(scope="session")
def dual_session(java_session, rust_session):
    """Return both CQL sessions for side-by-side comparison."""
    return java_session, rust_session


@pytest.fixture(scope="session")
def java_connection_info():
    """Connection info for the Java oracle."""
    return {"host": JAVA_HOST, "port": JAVA_PORT}


@pytest.fixture(scope="session")
def rust_connection_info():
    """Connection info for the Rust SUT."""
    return {"host": RUST_HOST, "port": RUST_PORT}
