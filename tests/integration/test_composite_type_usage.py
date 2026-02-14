"""Integration test for actual composite type usage.

This test verifies that composite types can be properly used at runtime,
not just generated correctly.
"""

import ast
import asyncio
import subprocess
import sys
from pathlib import Path
from unittest.mock import AsyncMock

import psycopg
import pytest


# Define paths
TESTS_ROOT_DIR = Path(__file__).parent.parent
PROJECT_ROOT = TESTS_ROOT_DIR.parent


def run_cli_tool(functions_sql: Path, output_py: Path, schema_sql: Path | None = None, verbose: bool = False):
    """Helper function to run the CLI tool as a subprocess."""
    cmd = [
        sys.executable,
        "-m",
        "sql2pyapi.cli",
        str(functions_sql),
        str(output_py),
    ]
    if schema_sql:
        cmd.extend(["--schema-file", str(schema_sql)])
    if verbose:
        cmd.append("-v")

    result = subprocess.run(cmd, capture_output=True, text=True, cwd=PROJECT_ROOT, check=False)

    if result.returncode != 0:
        print("CLI Error STDOUT:", result.stdout)
        print("CLI Error STDERR:", result.stderr)

    return result


def test_composite_type_runtime_usage(tmp_path):
    """Test that composite types can be properly used at runtime."""

    # Create test schema with a composite type
    schema_sql_content = """
-- Simple table for testing
CREATE TABLE IF NOT EXISTS devices (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    status TEXT NOT NULL,
    location TEXT,
    last_ping TIMESTAMP
);

-- Composite type that includes device info
CREATE TYPE device_status AS (
    device_id INTEGER,
    device_name TEXT,
    is_online BOOLEAN,
    last_seen TIMESTAMP,
    error_count INTEGER
);

-- Another composite type that references a table
CREATE TYPE device_with_stats AS (
    device devices,
    total_errors INTEGER,
    uptime_percentage NUMERIC(5,2)
);
"""

    # Create test functions
    function_sql_content = """
-- Function returning a simple composite type
CREATE OR REPLACE FUNCTION get_device_status(p_device_id INTEGER)
RETURNS device_status
LANGUAGE plpgsql
AS $$
DECLARE
    result device_status;
BEGIN
    SELECT
        d.id,
        d.name,
        d.status = 'online',
        d.last_ping,
        COALESCE((SELECT COUNT(*) FROM logs WHERE device_id = d.id AND level = 'ERROR'), 0)
    INTO result
    FROM devices d
    WHERE d.id = p_device_id;

    RETURN result;
END;
$$;

-- Function returning SETOF composite type
CREATE OR REPLACE FUNCTION get_all_device_statuses()
RETURNS SETOF device_status
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    SELECT
        d.id,
        d.name,
        d.status = 'online',
        d.last_ping,
        COALESCE((SELECT COUNT(*) FROM logs WHERE device_id = d.id AND level = 'ERROR'), 0)::INTEGER
    FROM devices d
    ORDER BY d.id;
END;
$$;

-- Function returning composite type with table reference
CREATE OR REPLACE FUNCTION get_device_with_stats(p_device_id INTEGER)
RETURNS device_with_stats
LANGUAGE plpgsql
AS $$
DECLARE
    result device_with_stats;
BEGIN
    SELECT d.*, 42, 99.5
    INTO result.device, result.total_errors, result.uptime_percentage
    FROM devices d
    WHERE d.id = p_device_id;

    RETURN result;
END;
$$;
"""

    # Write test files
    schema_sql_path = tmp_path / "schema.sql"
    function_sql_path = tmp_path / "functions.sql"
    output_py_path = tmp_path / "api.py"

    schema_sql_path.write_text(schema_sql_content)
    function_sql_path.write_text(function_sql_content)

    # Run the generator
    result = run_cli_tool(function_sql_path, output_py_path, schema_sql_path, verbose=True)
    assert result.returncode == 0, f"CLI failed: {result.stderr}"

    # Read the generated file
    generated_code = output_py_path.read_text()
    print("Generated code:")
    print(generated_code)

    # Parse to verify structure
    tree = ast.parse(generated_code)

    # Find all generated classes
    generated_classes = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef):
            generated_classes[node.name] = node

    # Verify expected dataclasses exist
    assert "DeviceStatus" in generated_classes, "DeviceStatus dataclass not generated"
    assert "Device" in generated_classes, "Device dataclass not generated"
    # Note: The generator converts to singular form
    assert "DeviceWithStat" in generated_classes, "DeviceWithStat dataclass not generated"

    # Now test runtime usage by executing the generated code
    # Create a test module from the generated code
    test_module = {}
    exec(generated_code, test_module)

    # Get the generated classes and functions
    DeviceStatus = test_module["DeviceStatus"]
    Device = test_module["Device"]
    DeviceWithStat = test_module["DeviceWithStat"]  # Note: singular form
    get_device_status = test_module["get_device_status"]
    get_all_device_statuses = test_module["get_all_device_statuses"]
    get_device_with_stats = test_module["get_device_with_stats"]

    # Test 1: Verify we can create instances of the dataclasses
    # This is where the "missing positional arguments" error would occur
    from datetime import datetime

    # Create a DeviceStatus instance - this tests if initialization works
    device_status = DeviceStatus(
        device_id=1, device_name="Test Device", is_online=True, last_seen=datetime.now(), error_count=5
    )
    assert device_status.device_id == 1
    assert device_status.device_name == "Test Device"

    # Test 2: Mock database calls and verify the functions work
    async def test_functions():
        # Mock connection and cursor
        mock_conn = AsyncMock(spec=psycopg.AsyncConnection)
        mock_cursor = AsyncMock(spec=psycopg.AsyncCursor)

        # Test get_device_status
        mock_cursor.description = [("device_id",), ("device_name",), ("is_online",), ("last_seen",), ("error_count",)]
        mock_cursor.fetchone.return_value = (1, "Device 1", True, datetime.now(), 3)
        mock_conn.cursor.return_value.__aenter__.return_value = mock_cursor
        mock_conn.cursor.return_value.__aexit__.return_value = None

        result = await get_device_status(mock_conn, 1)
        assert result is not None
        assert isinstance(result, DeviceStatus)
        assert result.device_id == 1
        assert result.device_name == "Device 1"
        assert result.is_online is True
        assert result.error_count == 3

        # Test get_all_device_statuses
        mock_cursor.fetchall.return_value = [
            (1, "Device 1", True, datetime.now(), 3),
            (2, "Device 2", False, datetime.now(), 0),
        ]

        results = await get_all_device_statuses(mock_conn)
        assert len(results) == 2
        assert all(isinstance(r, DeviceStatus) for r in results)
        assert results[0].device_id == 1
        assert results[1].device_id == 2

        # Test get_device_with_stats (composite with table reference)
        # This is the tricky part - the Device might have many fields
        # Let's assume Device has the fields from the devices table
        mock_cursor.description = [("device",), ("total_errors",), ("uptime_percentage",)]
        mock_cursor.fetchone.return_value = (
            # First element is the device tuple (all device fields)
            (1, "Device 1", "online", "Room A", datetime.now()),
            42,  # total_errors
            99.5,  # uptime_percentage
        )

        result = await get_device_with_stats(mock_conn, 1)
        assert result is not None
        assert isinstance(result, DeviceWithStat)  # Note: singular form
        assert isinstance(result.device, Device)
        assert result.device.id == 1
        assert result.device.name == "Device 1"
        assert result.total_errors == 42
        assert result.uptime_percentage == 99.5

    # Run the async test
    asyncio.run(test_functions())

    print("All runtime tests passed!")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
