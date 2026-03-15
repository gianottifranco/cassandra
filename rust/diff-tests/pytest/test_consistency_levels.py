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
Consistency level differential tests.

Tests that consistency level semantics are correctly implemented
on both Java and Rust clusters.

Expected state during Phase 2: all XFAIL on Rust side.
"""

import pytest


class TestJavaConsistencyLevels:
    """Consistency level tests against Java oracle."""

    @pytest.fixture(autouse=True)
    def setup(self, java_session, java_available):
        if not java_available:
            pytest.skip("Java oracle not available")
        try:
            java_session.execute(
                "CREATE KEYSPACE IF NOT EXISTS diff_cl_test "
                "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}"
            )
            java_session.execute(
                "CREATE TABLE IF NOT EXISTS diff_cl_test.data ("
                "  pk int PRIMARY KEY, value text"
                ")"
            )
        except Exception:
            pass

    def test_cl_one_on_single_node(self, java_session):
        """CL=ONE should satisfy on a single-node cluster."""
        from cassandra.query import SimpleStatement, ConsistencyLevel
        stmt = SimpleStatement(
            "INSERT INTO diff_cl_test.data (pk, value) VALUES (1, 'test')",
            consistency_level=ConsistencyLevel.ONE,
        )
        java_session.execute(stmt)

        stmt = SimpleStatement(
            "SELECT * FROM diff_cl_test.data WHERE pk = 1",
            consistency_level=ConsistencyLevel.ONE,
        )
        result = java_session.execute(stmt)
        assert len(list(result)) == 1

    def test_cl_all_on_single_node(self, java_session):
        """CL=ALL should satisfy since RF=1 and we have 1 node."""
        from cassandra.query import SimpleStatement, ConsistencyLevel
        stmt = SimpleStatement(
            "INSERT INTO diff_cl_test.data (pk, value) VALUES (2, 'all')",
            consistency_level=ConsistencyLevel.ALL,
        )
        java_session.execute(stmt)

        stmt = SimpleStatement(
            "SELECT * FROM diff_cl_test.data WHERE pk = 2",
            consistency_level=ConsistencyLevel.ALL,
        )
        result = java_session.execute(stmt)
        assert len(list(result)) == 1

    def test_cl_quorum_on_single_node(self, java_session):
        """CL=QUORUM should satisfy since RF=1 and quorum(1)=1."""
        from cassandra.query import SimpleStatement, ConsistencyLevel
        stmt = SimpleStatement(
            "INSERT INTO diff_cl_test.data (pk, value) VALUES (3, 'quorum')",
            consistency_level=ConsistencyLevel.QUORUM,
        )
        java_session.execute(stmt)


class TestRustConsistencyLevels:
    """Consistency level tests against Rust SUT. All XFAIL."""

    @pytest.fixture(autouse=True)
    def check_rust(self, rust_available):
        if not rust_available:
            pytest.skip("Rust SUT not available")

    @pytest.mark.xfail(reason="Rust stub does not handle CL yet")
    def test_cl_one_on_single_node(self, rust_session):
        """CL=ONE should work on Rust single-node cluster."""
        from cassandra.query import SimpleStatement, ConsistencyLevel
        stmt = SimpleStatement(
            "SELECT * FROM system.local",
            consistency_level=ConsistencyLevel.ONE,
        )
        result = rust_session.execute(stmt)
        assert len(list(result)) >= 1
