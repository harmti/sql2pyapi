"""Test that generated code maps columns by name, not position.

Validates the fix for column order independence: when production table
column order differs from schema file order (e.g., after ALTER TABLE
ADD COLUMN), the generated code should still map values correctly
using cursor.description column names.
"""

import asyncio
import subprocess
import sys
from pathlib import Path
from unittest.mock import AsyncMock

import psycopg
import pytest

TESTS_ROOT_DIR = Path(__file__).parent.parent
FIXTURES_DIR = TESTS_ROOT_DIR / "fixtures"
PROJECT_ROOT = TESTS_ROOT_DIR.parent


def run_cli_tool(functions_sql, output_py, schema_sql=None):
    cmd = [
        sys.executable,
        "-m",
        "sql2pyapi.cli",
        str(functions_sql),
        str(output_py),
    ]
    if schema_sql:
        cmd.extend(["--schema-file", str(schema_sql)])
    return subprocess.run(cmd, capture_output=True, text=True, cwd=PROJECT_ROOT, check=False)


def test_column_order_independence_simple(tmp_path):
    """Test that a simple SETOF table function works when cursor columns are reordered."""
    schema_sql = tmp_path / "schema.sql"
    schema_sql.write_text(
        """
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    status TEXT NOT NULL
);
"""
    )

    functions_sql = tmp_path / "functions.sql"
    functions_sql.write_text(
        """
-- Get all users
CREATE OR REPLACE FUNCTION get_all_users()
RETURNS SETOF users
LANGUAGE sql AS $$ SELECT * FROM users; $$;
"""
    )

    output_py = tmp_path / "api.py"
    result = run_cli_tool(functions_sql, output_py, schema_sql)
    assert result.returncode == 0, f"CLI failed: {result.stderr}"

    generated_code = output_py.read_text()
    test_module = {}
    exec(generated_code, test_module)

    User = test_module["User"]
    get_all_users = test_module["get_all_users"]

    async def test_reordered_columns():
        mock_conn = AsyncMock(spec=psycopg.AsyncConnection)
        mock_cursor = AsyncMock(spec=psycopg.AsyncCursor)

        # Cursor returns columns in DIFFERENT order than schema definition
        # Schema order: id, name, status
        # Cursor order: status, id, name (as if columns were reordered)
        mock_cursor.description = [("status",), ("id",), ("name",)]
        mock_cursor.fetchall.return_value = [
            ("active", 1, "Alice"),
            ("inactive", 2, "Bob"),
        ]

        mock_conn.cursor.return_value.__aenter__.return_value = mock_cursor
        mock_conn.cursor.return_value.__aexit__.return_value = None

        results = await get_all_users(mock_conn)

        assert len(results) == 2

        # Verify fields are mapped by NAME, not position
        assert results[0].id == 1
        assert results[0].name == "Alice"
        assert results[0].status == "active"

        assert results[1].id == 2
        assert results[1].name == "Bob"
        assert results[1].status == "inactive"

    asyncio.run(test_reordered_columns())


def test_column_order_independence_single_row(tmp_path):
    """Test single row return with reordered cursor columns."""
    schema_sql = tmp_path / "schema.sql"
    schema_sql.write_text(
        """
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    status TEXT NOT NULL
);
"""
    )

    functions_sql = tmp_path / "functions.sql"
    functions_sql.write_text(
        """
-- Get user by id
CREATE OR REPLACE FUNCTION get_user_by_id(p_id INTEGER)
RETURNS users
LANGUAGE sql AS $$ SELECT * FROM users WHERE id = p_id; $$;
"""
    )

    output_py = tmp_path / "api.py"
    result = run_cli_tool(functions_sql, output_py, schema_sql)
    assert result.returncode == 0, f"CLI failed: {result.stderr}"

    generated_code = output_py.read_text()
    test_module = {}
    exec(generated_code, test_module)

    User = test_module["User"]
    get_user_by_id = test_module["get_user_by_id"]

    async def test_reordered_columns():
        mock_conn = AsyncMock(spec=psycopg.AsyncConnection)
        mock_cursor = AsyncMock(spec=psycopg.AsyncCursor)

        # Cursor returns columns in different order
        mock_cursor.description = [("name",), ("status",), ("id",)]
        mock_cursor.fetchone.return_value = ("Alice", "active", 1)

        mock_conn.cursor.return_value.__aenter__.return_value = mock_cursor
        mock_conn.cursor.return_value.__aexit__.return_value = None

        result = await get_user_by_id(mock_conn, id=1)

        assert result is not None
        assert result.id == 1
        assert result.name == "Alice"
        assert result.status == "active"

    asyncio.run(test_reordered_columns())


def test_column_order_independence_returns_table(tmp_path):
    """Test RETURNS TABLE function with reordered cursor columns."""
    functions_sql = tmp_path / "functions.sql"
    functions_sql.write_text(
        """
-- Search items
CREATE OR REPLACE FUNCTION search_items(p_query TEXT)
RETURNS TABLE(item_id INTEGER, item_name TEXT, score NUMERIC)
LANGUAGE sql AS $$ SELECT 1, 'test', 1.0; $$;
"""
    )

    output_py = tmp_path / "api.py"
    result = run_cli_tool(functions_sql, output_py)
    assert result.returncode == 0, f"CLI failed: {result.stderr}"

    generated_code = output_py.read_text()
    test_module = {}
    exec(generated_code, test_module)

    SearchItemsResult = test_module["SearchItemsResult"]
    search_items = test_module["search_items"]

    async def test_reordered_columns():
        mock_conn = AsyncMock(spec=psycopg.AsyncConnection)
        mock_cursor = AsyncMock(spec=psycopg.AsyncCursor)

        # Cursor returns columns in different order
        mock_cursor.description = [("score",), ("item_id",), ("item_name",)]
        mock_cursor.fetchall.return_value = [
            (9.5, 42, "Widget"),
            (7.2, 13, "Gadget"),
        ]

        mock_conn.cursor.return_value.__aenter__.return_value = mock_cursor
        mock_conn.cursor.return_value.__aexit__.return_value = None

        results = await search_items(mock_conn, query="test")

        assert len(results) == 2
        assert results[0].item_id == 42
        assert results[0].item_name == "Widget"
        assert results[0].score == 9.5
        assert results[1].item_id == 13
        assert results[1].item_name == "Gadget"

    asyncio.run(test_reordered_columns())


def test_column_order_independence_with_enum(tmp_path):
    """Test enum column handling with reordered cursor columns."""
    schema_sql = tmp_path / "schema.sql"
    schema_sql.write_text(
        """
CREATE TYPE user_role AS ENUM ('admin', 'member', 'guest');

CREATE TABLE app_users (
    id INTEGER PRIMARY KEY,
    username TEXT NOT NULL,
    role user_role NOT NULL
);
"""
    )

    functions_sql = tmp_path / "functions.sql"
    functions_sql.write_text(
        """
-- Get user
CREATE OR REPLACE FUNCTION get_app_user(p_id INTEGER)
RETURNS app_users
LANGUAGE sql AS $$ SELECT * FROM app_users WHERE id = p_id; $$;
"""
    )

    output_py = tmp_path / "api.py"
    result = run_cli_tool(functions_sql, output_py, schema_sql)
    assert result.returncode == 0, f"CLI failed: {result.stderr}"

    generated_code = output_py.read_text()
    test_module = {}
    exec(generated_code, test_module)

    AppUser = test_module["AppUser"]
    UserRole = test_module["UserRole"]
    get_app_user = test_module["get_app_user"]

    async def test_reordered_columns():
        mock_conn = AsyncMock(spec=psycopg.AsyncConnection)
        mock_cursor = AsyncMock(spec=psycopg.AsyncCursor)

        # Cursor returns columns in different order: role, id, username
        mock_cursor.description = [("role",), ("id",), ("username",)]
        mock_cursor.fetchone.return_value = ("admin", 1, "alice")

        mock_conn.cursor.return_value.__aenter__.return_value = mock_cursor
        mock_conn.cursor.return_value.__aexit__.return_value = None

        result = await get_app_user(mock_conn, id=1)

        assert result is not None
        assert result.id == 1
        assert result.username == "alice"
        # Enum should be properly converted
        assert result.role == UserRole.ADMIN or result.role == "admin"

    asyncio.run(test_reordered_columns())


def test_scalar_returns_unaffected(tmp_path):
    """Verify scalar returns (row[0]) still work correctly - they don't use column names."""
    functions_sql = tmp_path / "functions.sql"
    functions_sql.write_text(
        """
-- Get count
CREATE OR REPLACE FUNCTION get_count()
RETURNS INTEGER
LANGUAGE sql AS $$ SELECT 42; $$;
"""
    )

    output_py = tmp_path / "api.py"
    result = run_cli_tool(functions_sql, output_py)
    assert result.returncode == 0, f"CLI failed: {result.stderr}"

    generated_code = output_py.read_text()
    test_module = {}
    exec(generated_code, test_module)

    get_count = test_module["get_count"]

    async def test_scalar():
        mock_conn = AsyncMock(spec=psycopg.AsyncConnection)
        mock_cursor = AsyncMock(spec=psycopg.AsyncCursor)

        mock_cursor.fetchone.return_value = (42,)
        mock_conn.cursor.return_value.__aenter__.return_value = mock_cursor
        mock_conn.cursor.return_value.__aexit__.return_value = None

        result = await get_count(mock_conn)
        assert result == 42

    asyncio.run(test_scalar())
