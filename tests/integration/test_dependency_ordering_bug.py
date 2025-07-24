"""Test for dependency ordering issue in class generation.

This test reproduces the bug where composite types that reference table types
are generated before the table types they depend on, causing NameError.

Bug report: Composite types should be generated after their dependencies.
"""

from pathlib import Path
import subprocess
import sys
import ast
import pytest

# Define paths relative to the main tests/ directory
TESTS_ROOT_DIR = Path(__file__).parent.parent  # Go up one level to tests/
PROJECT_ROOT = TESTS_ROOT_DIR.parent  # Go up one level from tests/ to project root


def run_cli_tool(functions_sql: Path, output_py: Path, schema_sql: Path = None, verbose: bool = False):
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


def extract_class_definitions_order(tree: ast.AST) -> list:
    """Extract class definitions in the order they appear in the file."""
    class_names = []
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef):
            class_names.append(node.name)
    return class_names


def test_dependency_ordering_issue_reproduction(tmp_path):
    """Test that reproduces the dependency ordering issue.
    
    The issue: When a composite type references a table type, sql2pyapi may generate
    the composite type class before the table type class, causing a NameError.
    
    This test verifies the problem exists and will serve as a regression test
    once the fix is implemented.
    """
    
    # Create test schema where composite type references table
    # This mimics the real-world scenario from the bug report
    schema_sql_content = """
-- Define composite type FIRST (before table) to trigger the ordering issue
CREATE TYPE metering_point_upsert_result AS (
    metering_point metering_points,  -- References table defined below
    was_created BOOLEAN
);

-- Define the referenced table AFTER the composite type
CREATE TABLE metering_points (
    id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
    name TEXT NOT NULL,
    location TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);
"""

    # Create test function that uses both types
    function_sql_content = """
-- Function that returns the table directly (ensures MeteringPoint class is needed)
CREATE OR REPLACE FUNCTION get_metering_point_by_id(p_id uuid)
RETURNS metering_points
LANGUAGE sql
AS $$
    SELECT * FROM metering_points WHERE id = p_id;
$$;

-- Function that returns the composite type (ensures MeteringPointUpsertResult class is needed)
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
    SELECT * INTO existing_point 
    FROM metering_points 
    WHERE id = p_id;
    
    IF FOUND THEN
        result.metering_point := existing_point;
        result.was_created := FALSE;
    ELSE
        INSERT INTO metering_points (id, name, location)
        VALUES (p_id, p_name, p_location)
        RETURNING * INTO result.metering_point;
        
        result.was_created := TRUE;
    END IF;
    
    RETURN result;
END;
$$;
"""

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

    # Extract class definitions in order
    class_order = extract_class_definitions_order(tree)
    
    # Find positions of the classes
    metering_point_pos = None
    metering_point_upsert_result_pos = None
    
    for i, class_name in enumerate(class_order):
        if class_name == 'MeteringPoint':
            metering_point_pos = i
        elif class_name == 'MeteringPointUpsertResult':
            metering_point_upsert_result_pos = i
    
    # Verify both classes exist
    assert metering_point_pos is not None, "MeteringPoint class not found"
    assert metering_point_upsert_result_pos is not None, "MeteringPointUpsertResult class not found"
    
    # Print current order for debugging
    print(f"Current class order: {class_order}")
    print(f"MeteringPoint position: {metering_point_pos}")
    print(f"MeteringPointUpsertResult position: {metering_point_upsert_result_pos}")
    
    # THE BUG: MeteringPointUpsertResult references MeteringPoint, so MeteringPoint should come first
    # This assertion will FAIL until the dependency ordering is fixed
    if metering_point_upsert_result_pos < metering_point_pos:
        # Try to import the generated module to see if it actually causes NameError
        try:
            # Write the content to a temporary Python file and try to compile it
            exec(compile(actual_content, output_py_path, 'exec'))
            pytest.fail("Expected NameError due to dependency ordering, but code executed successfully")
        except NameError as e:
            # This is the expected error due to the dependency ordering issue
            assert "MeteringPoint" in str(e), f"NameError should mention MeteringPoint: {e}"
            pytest.fail(
                f"BUG CONFIRMED: Dependency ordering issue detected. "
                f"MeteringPointUpsertResult (pos {metering_point_upsert_result_pos}) is defined before "
                f"MeteringPoint (pos {metering_point_pos}), causing NameError: {e}"
            )
    
    # If we reach here, the dependency ordering is correct
    assert metering_point_pos < metering_point_upsert_result_pos, \
        f"Dependency ordering issue: MeteringPoint (pos {metering_point_pos}) should come before " \
        f"MeteringPointUpsertResult (pos {metering_point_upsert_result_pos}) since the latter references the former"


def test_simple_dependency_chain(tmp_path):
    """Test a simple dependency chain to verify correct ordering."""
    
    schema_sql_content = """
-- Create a dependency chain: C depends on B depends on A
CREATE TABLE table_a (
    id uuid PRIMARY KEY,
    name TEXT
);

CREATE TABLE table_b (
    id uuid PRIMARY KEY,
    a_ref table_a
);

CREATE TYPE composite_c AS (
    b_ref table_b,
    extra_field TEXT
);
"""
    
    function_sql_content = """
CREATE OR REPLACE FUNCTION get_all()
RETURNS composite_c
LANGUAGE sql
AS $$
    SELECT ROW(ROW(uuid_generate_v4(), 'test'), 'extra')::composite_c;
$$;
"""

    # Write test files
    schema_sql_path = tmp_path / "test_schema.sql"
    function_sql_path = tmp_path / "test_functions.sql"
    output_py_path = tmp_path / "test_api.py"

    schema_sql_path.write_text(schema_sql_content)
    function_sql_path.write_text(function_sql_content)

    # Run the generator tool
    result = run_cli_tool(function_sql_path, output_py_path, schema_sql_path)
    assert result.returncode == 0, f"CLI failed: {result.stderr}"

    # Read and analyze the generated file
    actual_content = output_py_path.read_text()
    tree = ast.parse(actual_content)
    
    # Extract class order
    class_order = extract_class_definitions_order(tree)
    
    print(f"Class order for dependency chain: {class_order}")
    
    # Expected order should be: TableA, TableB, CompositeC
    # (dependencies should come before dependents)
    expected_classes = {'TableA', 'TableB', 'CompositeC'}
    actual_classes = set(class_order)
    
    # Verify all expected classes are present
    assert expected_classes.issubset(actual_classes), \
        f"Missing classes. Expected: {expected_classes}, Got: {actual_classes}"