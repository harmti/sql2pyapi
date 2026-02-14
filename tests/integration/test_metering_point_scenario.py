"""Test for MeteringPoint-like scenario with complex nested composite types."""

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


def test_metering_point_scenario(tmp_path):
    """Test the MeteringPoint scenario with 20+ fields in nested composite types."""

    # Create schema that mimics the MeteringPoint scenario
    schema_sql_content = """
-- Large table with many fields (like metering_points)
CREATE TABLE IF NOT EXISTS metering_points (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    meter_number TEXT NOT NULL,
    installation_date DATE NOT NULL,
    location_address TEXT,
    location_city TEXT,
    location_postal_code TEXT,
    location_country TEXT DEFAULT 'US',
    meter_type TEXT NOT NULL,
    manufacturer TEXT,
    model TEXT,
    serial_number TEXT UNIQUE,
    firmware_version TEXT,
    hardware_version TEXT,
    max_capacity NUMERIC(10,2),
    unit_of_measure TEXT DEFAULT 'kWh',
    accuracy_class TEXT,
    certification_date DATE,
    certification_expiry DATE,
    owner_name TEXT,
    owner_contact TEXT,
    maintenance_schedule TEXT,
    last_maintenance_date DATE,
    next_maintenance_date DATE,
    status TEXT NOT NULL DEFAULT 'active',
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Composite type that includes the metering point
CREATE TYPE metering_point_result AS (
    metering_point metering_points,
    validation_status TEXT,
    error_message TEXT,
    processed_at TIMESTAMP
);

-- Another composite that nests the first one
CREATE TYPE metering_point_batch_result AS (
    batch_id UUID,
    results metering_point_result[],
    total_processed INTEGER,
    total_errors INTEGER
);
"""

    # Create functions
    function_sql_content = """
-- Function that returns a single metering point
CREATE OR REPLACE FUNCTION get_metering_point(p_id UUID)
RETURNS metering_points
LANGUAGE sql
AS $$
    SELECT * FROM metering_points WHERE id = p_id;
$$;

-- Function that processes and returns a metering point result
CREATE OR REPLACE FUNCTION process_metering_point(
    p_meter_number TEXT,
    p_installation_date DATE,
    p_location_address TEXT DEFAULT NULL,
    p_meter_type TEXT DEFAULT 'standard'
)
RETURNS metering_point_result
LANGUAGE plpgsql
AS $$
DECLARE
    result metering_point_result;
    new_point metering_points;
BEGIN
    -- Insert or update the metering point
    INSERT INTO metering_points (
        meter_number, installation_date, location_address, meter_type
    ) VALUES (
        p_meter_number, p_installation_date, p_location_address, p_meter_type
    )
    ON CONFLICT (serial_number) DO UPDATE SET
        updated_at = NOW()
    RETURNING * INTO new_point;

    -- Build the result
    result.metering_point := new_point;
    result.validation_status := 'valid';
    result.error_message := NULL;
    result.processed_at := NOW();

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
    print("Generated code snippet:")
    print(generated_code[:1500])  # Print first part

    # Execute the generated code to get the classes
    test_module = {}
    exec(generated_code, test_module)

    # Get the generated classes
    MeteringPoint = test_module["MeteringPoint"]
    MeteringPointResult = test_module["MeteringPointResult"]
    test_module["get_metering_point"]
    process_metering_point = test_module["process_metering_point"]

    # Test 1: Create a MeteringPoint instance with all fields
    # This is where the "missing 20 required positional arguments" error would occur
    test_id = uuid4()
    metering_point = MeteringPoint(
        id=test_id,
        meter_number="MP-12345",
        installation_date=datetime.now().date(),
        location_address="123 Main St",
        location_city="Anytown",
        location_postal_code="12345",
        location_country="US",
        meter_type="smart",
        manufacturer="ACME",
        model="SM-1000",
        serial_number="SN-123456",
        firmware_version="1.2.3",
        hardware_version="2.0",
        max_capacity=100.00,
        unit_of_measure="kWh",
        accuracy_class="0.5",
        certification_date=datetime.now().date(),
        certification_expiry=datetime.now().date(),
        owner_name="John Doe",
        owner_contact="john@example.com",
        maintenance_schedule="annual",
        last_maintenance_date=datetime.now().date(),
        next_maintenance_date=datetime.now().date(),
        status="active",
        created_at=datetime.now(),
        updated_at=datetime.now(),
    )

    # Verify all fields are set correctly
    assert metering_point.id == test_id
    assert metering_point.meter_number == "MP-12345"
    assert metering_point.meter_type == "smart"

    # Test 2: Test the async functions with mocked database
    async def test_functions():
        mock_conn = AsyncMock(spec=psycopg.AsyncConnection)
        mock_cursor = AsyncMock(spec=psycopg.AsyncCursor)

        # Test process_metering_point - returns nested composite
        # Mock the row data - this should be a tuple with nested tuple
        mock_cursor.description = [("metering_point",), ("validation_status",), ("error_message",), ("processed_at",)]
        mock_cursor.fetchone.return_value = (
            # First element is the nested metering_points tuple (26 fields)
            (
                test_id,
                "MP-12345",
                datetime.now().date(),
                "123 Main St",
                "Anytown",
                "12345",
                "US",
                "smart",
                "ACME",
                "SM-1000",
                "SN-123456",
                "1.2.3",
                "2.0",
                100.00,
                "kWh",
                "0.5",
                datetime.now().date(),
                datetime.now().date(),
                "John Doe",
                "john@example.com",
                "annual",
                datetime.now().date(),
                datetime.now().date(),
                "active",
                datetime.now(),
                datetime.now(),
            ),
            "valid",  # validation_status
            None,  # error_message
            datetime.now(),  # processed_at
        )

        mock_conn.cursor.return_value.__aenter__.return_value = mock_cursor
        mock_conn.cursor.return_value.__aexit__.return_value = None

        result = await process_metering_point(
            mock_conn, meter_number="MP-12345", installation_date=datetime.now().date()
        )

        # Verify the result
        assert result is not None
        assert isinstance(result, MeteringPointResult)
        assert isinstance(result.metering_point, MeteringPoint)
        assert result.metering_point.meter_number == "MP-12345"
        assert result.validation_status == "valid"
        assert result.error_message is None

    # Run the async test
    asyncio.run(test_functions())

    print("MeteringPoint scenario test passed!")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
