"""Test for composite type table reference bug.

This test reproduces the bug where composite types containing table references
generate Optional[Any] instead of the proper dataclass reference.

Bug report: sql2pyapi-bug-report.md
"""

import ast
import subprocess
import sys
from pathlib import Path


# Define paths relative to the main tests/ directory
TESTS_ROOT_DIR = Path(__file__).parent.parent  # Go up one level to tests/
PROJECT_ROOT = TESTS_ROOT_DIR.parent  # Go up one level from tests/ to project root


def run_cli_tool(functions_sql: Path, output_py: Path, schema_sql: Path | None = None, verbose: bool = False):
    """Helper function to run the CLI tool as a subprocess."""
    cmd = [
        sys.executable,  # Use the current Python executable
        "-m",
        "sql2pyapi.cli",  # Invoke the module's entry point
        str(functions_sql),
        str(output_py),
    ]
    if schema_sql:
        cmd.extend(["--schema-file", str(schema_sql)])
    if verbose:
        cmd.append("-v")

    # Run from the project root directory
    result = subprocess.run(cmd, capture_output=True, text=True, cwd=PROJECT_ROOT, check=False)

    if result.returncode != 0:
        print("CLI Error STDOUT:", result.stdout)
        print("CLI Error STDERR:", result.stderr)

    return result


def test_composite_type_with_table_reference_bug(tmp_path):
    """Test that composite types with table references generate proper dataclass references.

    This test reproduces the bug where:
    - A table 'metering_points' should generate a 'MeteringPoint' dataclass
    - A composite type with field 'metering_point metering_points' should reference
      Optional[MeteringPoint], not Optional[Any]
    """
    # Create test schema file
    schema_sql_content = """-- Test schema for composite type bug reproduction
CREATE TABLE IF NOT EXISTS metering_points (
    id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
    name TEXT NOT NULL,
    location TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Composite type that references the table above
-- The bug: metering_point field should map to Optional[MeteringPoint]
-- but instead maps to Optional[Any]
CREATE TYPE metering_point_upsert_result AS (
    metering_point metering_points,  -- Should map to MeteringPoint dataclass
    was_created BOOLEAN
);"""

    # Create test function file
    function_sql_content = """-- Functions for composite type table reference test

-- This function returns the table directly, which should generate MeteringPoint dataclass
CREATE OR REPLACE FUNCTION get_metering_point_by_id(p_id uuid)
RETURNS metering_points
LANGUAGE sql
AS $$
    SELECT * FROM metering_points WHERE id = p_id;
$$;

-- This function returns the composite type that contains a table reference
CREATE OR REPLACE FUNCTION upsert_metering_point(
    p_id uuid,
    p_name TEXT,
    p_location TEXT DEFAULT NULL
)
RETURNS metering_point_upsert_result
LANGUAGE plpgsql
AS $$
DECLARE
    result metering_point_upsert_result;
    existing_point metering_points;
BEGIN
    -- Try to find existing metering point
    SELECT * INTO existing_point
    FROM metering_points
    WHERE id = p_id;

    IF FOUND THEN
        result.metering_point := existing_point;
        result.was_created := FALSE;
    ELSE
        -- Insert new metering point
        INSERT INTO metering_points (id, name, location)
        VALUES (p_id, p_name, p_location)
        RETURNING * INTO result.metering_point;

        result.was_created := TRUE;
    END IF;

    RETURN result;
END;
$$;"""

    # Write test files
    schema_sql_path = tmp_path / "test_schema.sql"
    function_sql_path = tmp_path / "test_functions.sql"
    output_py_path = tmp_path / "test_api.py"

    schema_sql_path.write_text(schema_sql_content)
    function_sql_path.write_text(function_sql_content)

    # Run the generator tool
    result = run_cli_tool(function_sql_path, output_py_path, schema_sql_path, verbose=True)
    assert result.returncode == 0, f"CLI failed: {result.stderr}"

    # Read and parse the generated file
    assert output_py_path.is_file(), "Generated file was not created."
    actual_content = output_py_path.read_text()
    tree = ast.parse(actual_content)

    # Check that MeteringPoint dataclass is generated
    metering_point_class = None
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef) and node.name == "MeteringPoint":
            metering_point_class = node
            break

    assert metering_point_class is not None, "MeteringPoint dataclass was not generated"
    assert any(isinstance(d, ast.Name) and d.id == "dataclass" for d in metering_point_class.decorator_list), (
        "MeteringPoint class is not decorated with @dataclass"
    )

    # Check that MeteringPointUpsertResult dataclass is generated
    upsert_result_class = None
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef) and node.name == "MeteringPointUpsertResult":
            upsert_result_class = node
            break

    assert upsert_result_class is not None, "MeteringPointUpsertResult dataclass was not generated"
    assert any(isinstance(d, ast.Name) and d.id == "dataclass" for d in upsert_result_class.decorator_list), (
        "MeteringPointUpsertResult class is not decorated with @dataclass"
    )

    # Extract the fields from MeteringPointUpsertResult
    upsert_result_fields = {}
    for stmt in upsert_result_class.body:
        if isinstance(stmt, ast.AnnAssign) and hasattr(stmt.target, "id"):
            field_name = stmt.target.id
            field_type = ast.unparse(stmt.annotation)
            upsert_result_fields[field_name] = field_type

    # THIS IS THE KEY TEST: The bug is that metering_point field should be
    # Optional[MeteringPoint] but is currently Optional[Any]
    assert "metering_point" in upsert_result_fields, "metering_point field not found in MeteringPointUpsertResult"

    # EXPECTED: This assertion should pass after the bug is fixed
    # CURRENT BUG: This assertion will fail because it's currently Optional[Any]
    expected_type = "Optional[MeteringPoint]"
    actual_type = upsert_result_fields["metering_point"]

    assert actual_type == expected_type, (
        f"BUG CONFIRMED: metering_point field type is '{actual_type}', should be '{expected_type}'. "
        f"This confirms the bug where composite types with table references generate Optional[Any] "
        f"instead of proper dataclass references."
    )

    # Also verify the was_created field is correct
    assert upsert_result_fields["was_created"] == "Optional[bool]", (
        f"was_created field type is incorrect: {upsert_result_fields['was_created']}"
    )

    # Verify the functions are generated properly
    get_function = None
    upsert_function = None
    for node in ast.walk(tree):
        if isinstance(node, ast.AsyncFunctionDef):
            if node.name == "get_metering_point_by_id":
                get_function = node
            elif node.name == "upsert_metering_point":
                upsert_function = node

    assert get_function is not None, "get_metering_point_by_id function not generated"
    assert upsert_function is not None, "upsert_metering_point function not generated"

    # Check return types
    get_return_type = ast.unparse(get_function.returns) if get_function.returns else None
    upsert_return_type = ast.unparse(upsert_function.returns) if upsert_function.returns else None

    assert get_return_type == "Optional[MeteringPoint]", (
        f"get_metering_point_by_id return type is {get_return_type}, should be Optional[MeteringPoint]"
    )

    assert upsert_return_type == "Optional[MeteringPointUpsertResult]", (
        f"upsert_metering_point return type is {upsert_return_type}, should be Optional[MeteringPointUpsertResult]"
    )
