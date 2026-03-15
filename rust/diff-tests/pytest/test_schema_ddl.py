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
Schema DDL differential tests.

Tests that CREATE/ALTER/DROP operations produce the same schema metadata
on both Java and Rust clusters.

Expected state during Phase 2: all XFAIL on Rust side.
"""

import pytest


class TestJavaSchema:
    """Schema tests against Java oracle."""

    @pytest.fixture(autouse=True)
    def check_java(self, java_available):
        if not java_available:
            pytest.skip("Java oracle not available")

    def test_create_keyspace(self, java_session):
        """CREATE KEYSPACE should succeed and be visible in system_schema."""
        java_session.execute(
            "CREATE KEYSPACE IF NOT EXISTS diff_schema_test "
            "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}"
        )
        result = java_session.execute(
            "SELECT keyspace_name FROM system_schema.keyspaces "
            "WHERE keyspace_name = 'diff_schema_test'"
        )
        rows = list(result)
        assert len(rows) == 1
        assert rows[0].keyspace_name == "diff_schema_test"

    def test_create_table(self, java_session):
        """CREATE TABLE should succeed with proper column metadata."""
        java_session.execute(
            "CREATE TABLE IF NOT EXISTS diff_schema_test.test_table ("
            "  pk int PRIMARY KEY,"
            "  col_text text,"
            "  col_int int"
            ")"
        )
        result = java_session.execute(
            "SELECT column_name, type FROM system_schema.columns "
            "WHERE keyspace_name = 'diff_schema_test' AND table_name = 'test_table'"
        )
        columns = {row.column_name: row.type for row in result}
        assert "pk" in columns
        assert "col_text" in columns
        assert "col_int" in columns

    def test_drop_table(self, java_session):
        """DROP TABLE should succeed."""
        java_session.execute(
            "CREATE TABLE IF NOT EXISTS diff_schema_test.drop_me ("
            "  pk int PRIMARY KEY"
            ")"
        )
        java_session.execute("DROP TABLE IF EXISTS diff_schema_test.drop_me")
        result = java_session.execute(
            "SELECT table_name FROM system_schema.tables "
            "WHERE keyspace_name = 'diff_schema_test' AND table_name = 'drop_me'"
        )
        assert len(list(result)) == 0


class TestRustSchema:
    """
    Schema tests against Rust SUT.

    All expected to XFAIL during Phase 2.
    """

    @pytest.fixture(autouse=True)
    def check_rust(self, rust_available):
        if not rust_available:
            pytest.skip("Rust SUT not available")

    @pytest.mark.xfail(reason="Rust stub does not implement CQL yet")
    def test_create_keyspace(self, rust_session):
        """CREATE KEYSPACE should work on Rust."""
        rust_session.execute(
            "CREATE KEYSPACE IF NOT EXISTS diff_schema_test "
            "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}"
        )

    @pytest.mark.xfail(reason="Rust stub does not implement CQL yet")
    def test_create_table(self, rust_session):
        """CREATE TABLE should work on Rust."""
        rust_session.execute(
            "CREATE TABLE IF NOT EXISTS diff_schema_test.test_table ("
            "  pk int PRIMARY KEY"
            ")"
        )


class TestDifferentialSchema:
    """Side-by-side schema comparison tests."""

    @pytest.fixture(autouse=True)
    def check_both(self, java_available, rust_available):
        if not java_available:
            pytest.skip("Java oracle not available")
        if not rust_available:
            pytest.skip("Rust SUT not available")

    @pytest.mark.xfail(reason="Rust stub does not implement CQL yet")
    def test_system_schema_keyspaces_match(self, dual_session):
        """system_schema.keyspaces should return same system keyspaces."""
        java_session, rust_session = dual_session

        java_ks = {row.keyspace_name for row in
                   java_session.execute("SELECT keyspace_name FROM system_schema.keyspaces")}
        rust_ks = {row.keyspace_name for row in
                   rust_session.execute("SELECT keyspace_name FROM system_schema.keyspaces")}

        # Both should have at least the system keyspaces
        system_ks = {"system", "system_schema", "system_auth", "system_distributed"}
        assert system_ks.issubset(java_ks), f"Java missing system keyspaces: {system_ks - java_ks}"
        assert system_ks.issubset(rust_ks), f"Rust missing system keyspaces: {system_ks - rust_ks}"
