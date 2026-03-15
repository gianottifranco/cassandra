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
Read/write path differential tests.

Tests that INSERT/SELECT/UPDATE/DELETE operations produce identical results
on both Java and Rust clusters across all CQL types.

Expected state during Phase 2: all XFAIL on Rust side.
"""

import pytest


SETUP_CQL = [
    "CREATE KEYSPACE IF NOT EXISTS diff_rw_test WITH replication = "
    "{'class': 'SimpleStrategy', 'replication_factor': 1}",
    """CREATE TABLE IF NOT EXISTS diff_rw_test.all_types (
        pk int PRIMARY KEY,
        col_text text,
        col_int int,
        col_bigint bigint,
        col_boolean boolean,
        col_double double,
        col_float float
    )""",
    """CREATE TABLE IF NOT EXISTS diff_rw_test.ttl_test (
        pk int PRIMARY KEY,
        value text
    )""",
    """CREATE TABLE IF NOT EXISTS diff_rw_test.clustering (
        pk int,
        ck text,
        value text,
        PRIMARY KEY (pk, ck)
    )""",
]


class TestJavaReadWrite:
    """Read/write path tests against Java oracle."""

    @pytest.fixture(autouse=True)
    def setup_schema(self, java_session, java_available):
        if not java_available:
            pytest.skip("Java oracle not available")
        for cql in SETUP_CQL:
            try:
                java_session.execute(cql)
            except Exception:
                pass

    def test_insert_and_select(self, java_session):
        """INSERT followed by SELECT should return the inserted data."""
        java_session.execute(
            "INSERT INTO diff_rw_test.all_types "
            "(pk, col_text, col_int, col_bigint, col_boolean) "
            "VALUES (1, 'hello', 42, 9223372036854775807, true)"
        )
        result = java_session.execute(
            "SELECT * FROM diff_rw_test.all_types WHERE pk = 1"
        )
        rows = list(result)
        assert len(rows) == 1
        assert rows[0].col_text == "hello"
        assert rows[0].col_int == 42
        assert rows[0].col_boolean is True

    def test_delete_creates_tombstone(self, java_session):
        """DELETE should make the row invisible."""
        java_session.execute(
            "INSERT INTO diff_rw_test.all_types (pk, col_text) VALUES (99, 'deleteme')"
        )
        java_session.execute("DELETE FROM diff_rw_test.all_types WHERE pk = 99")
        result = java_session.execute(
            "SELECT * FROM diff_rw_test.all_types WHERE pk = 99"
        )
        assert len(list(result)) == 0

    def test_ttl_insert(self, java_session):
        """INSERT with TTL should set a TTL on the row."""
        java_session.execute(
            "INSERT INTO diff_rw_test.ttl_test (pk, value) VALUES (1, 'expires') USING TTL 86400"
        )
        result = java_session.execute(
            "SELECT pk, value, TTL(value) FROM diff_rw_test.ttl_test WHERE pk = 1"
        )
        rows = list(result)
        assert len(rows) == 1
        ttl = rows[0][2]  # TTL(value)
        assert ttl is not None
        assert ttl > 0
        assert ttl <= 86400

    def test_clustering_order(self, java_session):
        """Rows with clustering columns should be returned in clustering order."""
        java_session.execute("TRUNCATE diff_rw_test.clustering")
        java_session.execute(
            "INSERT INTO diff_rw_test.clustering (pk, ck, value) VALUES (1, 'c', 'third')"
        )
        java_session.execute(
            "INSERT INTO diff_rw_test.clustering (pk, ck, value) VALUES (1, 'a', 'first')"
        )
        java_session.execute(
            "INSERT INTO diff_rw_test.clustering (pk, ck, value) VALUES (1, 'b', 'second')"
        )
        result = java_session.execute(
            "SELECT ck, value FROM diff_rw_test.clustering WHERE pk = 1"
        )
        rows = list(result)
        assert len(rows) == 3
        assert [r.ck for r in rows] == ["a", "b", "c"]


class TestRustReadWrite:
    """
    Read/write path tests against Rust SUT.

    All expected to XFAIL during Phase 2.
    """

    @pytest.fixture(autouse=True)
    def check_rust(self, rust_available):
        if not rust_available:
            pytest.skip("Rust SUT not available")

    @pytest.mark.xfail(reason="Rust stub does not implement storage yet")
    def test_insert_and_select(self, rust_session):
        """INSERT/SELECT should work once Rust implements storage."""
        for cql in SETUP_CQL:
            rust_session.execute(cql)
        rust_session.execute(
            "INSERT INTO diff_rw_test.all_types (pk, col_text) VALUES (1, 'hello')"
        )
        result = rust_session.execute(
            "SELECT * FROM diff_rw_test.all_types WHERE pk = 1"
        )
        assert len(list(result)) == 1


class TestDifferentialReadWrite:
    """Side-by-side read/write comparison tests."""

    @pytest.fixture(autouse=True)
    def check_both(self, java_available, rust_available):
        if not java_available:
            pytest.skip("Java oracle not available")
        if not rust_available:
            pytest.skip("Rust SUT not available")

    @pytest.mark.xfail(reason="Rust stub does not implement CQL yet")
    def test_select_results_match(self, dual_session):
        """Same SELECT should return identical results on both clusters."""
        java_session, rust_session = dual_session

        # Setup identical schemas on both
        for cql in SETUP_CQL:
            java_session.execute(cql)
            rust_session.execute(cql)

        # Insert same data
        insert = ("INSERT INTO diff_rw_test.all_types "
                  "(pk, col_text, col_int) VALUES (1, 'test', 42)")
        java_session.execute(insert)
        rust_session.execute(insert)

        # Compare results
        query = "SELECT pk, col_text, col_int FROM diff_rw_test.all_types WHERE pk = 1"
        java_rows = list(java_session.execute(query))
        rust_rows = list(rust_session.execute(query))

        assert len(java_rows) == len(rust_rows), (
            f"Row count mismatch: java={len(java_rows)} rust={len(rust_rows)}"
        )
        for j, r in zip(java_rows, rust_rows):
            assert j.pk == r.pk
            assert j.col_text == r.col_text
            assert j.col_int == r.col_int
