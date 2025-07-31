"""Integration test for nested composite type string parsing bug fix.

This test verifies that nested composite types are properly parsed when
psycopg returns them as string representations instead of tuples.
"""

import asyncio
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from unittest.mock import AsyncMock
from uuid import uuid4

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


def test_nested_composite_string_parsing(tmp_path):
    """Test that nested composite types are properly parsed from string representations."""

    # Create schema that matches the bug report scenario
    schema_sql_content = """
-- Table that represents metering_points
CREATE TABLE IF NOT EXISTS metering_points (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    location_id UUID,
    ean TEXT,
    type TEXT,
    source TEXT,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Composite type definition that contains nested metering_points table
CREATE TYPE metering_point_upsert_result AS (
    metering_point metering_points,  -- This nested composite type should be parsed from strings
    was_created BOOLEAN
);
"""

    # Function that returns the composite type
    function_sql_content = """
-- Function returning the composite type
CREATE OR REPLACE FUNCTION upsert_metering_point_from_discovery(
    p_location_id UUID,
    p_ean TEXT,
    p_type TEXT DEFAULT 'electric',
    p_source TEXT DEFAULT 'discovery'
)
RETURNS metering_point_upsert_result
LANGUAGE plpgsql
AS $$
DECLARE
    result metering_point_upsert_result;
    existing_point metering_points;
    was_created BOOLEAN := FALSE;
BEGIN
    -- Try to find existing point
    SELECT * INTO existing_point
    FROM metering_points
    WHERE ean = p_ean;

    IF existing_point IS NULL THEN
        -- Create new point
        INSERT INTO metering_points (location_id, ean, type, source)
        VALUES (p_location_id, p_ean, p_type, p_source)
        RETURNING * INTO existing_point;
        was_created := TRUE;
    END IF;

    -- Build result
    result.metering_point := existing_point;
    result.was_created := was_created;

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

    # Read and execute the generated code
    generated_code = output_py_path.read_text()

    # Execute the generated code to get the classes
    test_module = {}
    exec(generated_code, test_module)

    # Get the generated classes
    MeteringPoint = test_module["MeteringPoint"]
    MeteringPointUpsertResult = test_module["MeteringPointUpsertResult"]
    upsert_metering_point_from_discovery = test_module["upsert_metering_point_from_discovery"]

    # Test with mocked database - simulate both scenarios
    async def test_function():
        mock_conn = AsyncMock(spec=psycopg.AsyncConnection)
        mock_cursor = AsyncMock(spec=psycopg.AsyncCursor)

        test_id = uuid4()
        location_id = uuid4()

        # Test Case 1: Database returns proper nested tuple (should work)
        print("=== Test Case 1: Database returns proper nested tuple ===")
        mock_cursor.fetchone.return_value = (
            # metering_point as a tuple (should be parsed as MeteringPoint)
            (test_id, location_id, "123456789", "electric", "discovery", datetime.now(), datetime.now()),
            True,  # was_created
        )

        mock_conn.cursor.return_value.__aenter__.return_value = mock_cursor
        mock_conn.cursor.return_value.__aexit__.return_value = None

        result = await upsert_metering_point_from_discovery(mock_conn, location_id=location_id, ean="123456789")

        assert isinstance(result, MeteringPointUpsertResult)
        assert isinstance(result.metering_point, MeteringPoint)
        assert result.metering_point.id == test_id
        assert result.metering_point.ean == "123456789"
        assert result.was_created is True
        print("✓ Tuple representation works correctly")

        # Test Case 2: Database returns string representation (the bug scenario)
        print("\n=== Test Case 2: Database returns string representation (BUG FIX) ===")

        # Format string similar to how PostgreSQL composite types are represented
        composite_string = f"({test_id},{location_id},123456789,electric,discovery,2025-07-25 14:28:41.496775,2025-07-25 14:28:41.496792)"

        mock_cursor.fetchone.return_value = (
            composite_string,  # metering_point as a string representation
            True,  # was_created
        )

        result = await upsert_metering_point_from_discovery(mock_conn, location_id=location_id, ean="123456789")

        # Verify the result is properly parsed
        assert isinstance(result, MeteringPointUpsertResult)
        assert isinstance(result.metering_point, MeteringPoint), (
            f"Expected MeteringPoint, got {type(result.metering_point)}"
        )
        assert result.metering_point.ean == "123456789"
        assert result.was_created is True
        # The ID will be a string since it's parsed from the string representation
        assert str(result.metering_point.id) == str(test_id)
        print("✓ String representation is correctly parsed into MeteringPoint instance")

        # Test Case 3: Test with None value
        print("\n=== Test Case 3: Database returns None for nested composite ===")
        mock_cursor.fetchone.return_value = (
            None,  # metering_point is None
            False,  # was_created
        )

        result = await upsert_metering_point_from_discovery(mock_conn, location_id=location_id, ean="nonexistent")

        assert isinstance(result, MeteringPointUpsertResult)
        assert result.metering_point is None
        assert result.was_created is False
        print("✓ None values are handled correctly")

    # Run the async test
    asyncio.run(test_function())

    print("\n✅ All nested composite string parsing tests passed!")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
