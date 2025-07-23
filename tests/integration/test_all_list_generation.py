"""Test for __all__ list generation in generated modules.

This test verifies that sql2pyapi automatically generates __all__ lists
that include all dataclasses, enums, and functions.
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


def extract_all_list_from_ast(tree: ast.AST) -> list:
    """Extract the __all__ list from an AST, returning its contents as a list of strings."""
    for node in ast.walk(tree):
        if (isinstance(node, ast.Assign) and 
            len(node.targets) == 1 and 
            isinstance(node.targets[0], ast.Name) and 
            node.targets[0].id == '__all__'):
            
            # Extract the list contents
            if isinstance(node.value, ast.List):
                all_items = []
                for elt in node.value.elts:
                    if isinstance(elt, ast.Constant) and isinstance(elt.value, str):
                        all_items.append(elt.value)
                return all_items
    
    return []  # __all__ not found


def test_all_list_basic_generation(tmp_path):
    """Test that __all__ list is generated with basic components."""
    
    # Create test schema with table, enum, and composite type
    schema_sql_content = """
-- Test table
CREATE TABLE test_users (
    id uuid PRIMARY KEY,
    name TEXT NOT NULL,
    status user_status
);

-- Test enum
CREATE TYPE user_status AS ENUM ('active', 'inactive', 'pending');

-- Test composite type
CREATE TYPE user_result AS (
    user test_users,
    is_new BOOLEAN
);
"""

    # Create test functions
    function_sql_content = """
-- Function returning table directly
CREATE OR REPLACE FUNCTION get_user_by_id(p_id uuid)
RETURNS test_users
LANGUAGE sql
AS $$
    SELECT * FROM test_users WHERE id = p_id;
$$;

-- Function with enum parameter and return
CREATE OR REPLACE FUNCTION update_user_status(p_id uuid, p_status user_status)
RETURNS user_status
LANGUAGE sql
AS $$
    UPDATE test_users SET status = p_status WHERE id = p_id
    RETURNING status;
$$;

-- Function returning composite type
CREATE OR REPLACE FUNCTION create_user_result(p_id uuid, p_name TEXT)
RETURNS user_result
LANGUAGE plpgsql
AS $$
DECLARE
    result user_result;
BEGIN
    INSERT INTO test_users (id, name, status)
    VALUES (p_id, p_name, 'pending')
    RETURNING * INTO result.user;
    
    result.is_new := TRUE;
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
    result = run_cli_tool(function_sql_path, output_py_path, schema_sql_path)
    assert result.returncode == 0, f"CLI failed: {result.stderr}"

    # Read and parse the generated file
    assert output_py_path.is_file(), "Generated file was not created."
    actual_content = output_py_path.read_text()
    tree = ast.parse(actual_content)

    # Extract __all__ list
    all_list = extract_all_list_from_ast(tree)
    
    # Verify __all__ list exists and is not empty
    assert all_list, "No __all__ list found in generated code"
    
    # Expected items: dataclasses, enums, and functions
    expected_items = {
        # Dataclasses
        'TestUser',          # from test_users table
        'UserResult',        # from user_result composite type
        
        # Enums
        'UserStatus',        # from user_status enum
        
        # Functions  
        'get_user_by_id',
        'update_user_status', 
        'create_user_result'
    }
    
    # Verify all expected items are in __all__
    actual_items = set(all_list)
    assert expected_items.issubset(actual_items), \
        f"Missing items in __all__. Expected: {expected_items}, Got: {actual_items}"
    
    # Verify __all__ is sorted alphabetically
    assert all_list == sorted(all_list), f"__all__ list is not sorted: {all_list}"
    
    # Verify no duplicates
    assert len(all_list) == len(set(all_list)), f"__all__ list has duplicates: {all_list}"


def test_all_list_empty_case(tmp_path):
    """Test __all__ list handling when no functions are defined."""
    
    # Create empty function file
    function_sql_content = "-- No functions defined"
    
    # Write test files
    function_sql_path = tmp_path / "empty_functions.sql"
    output_py_path = tmp_path / "empty_api.py"

    function_sql_path.write_text(function_sql_content)

    # Run the generator tool
    result = run_cli_tool(function_sql_path, output_py_path)
    assert result.returncode == 0, f"CLI failed: {result.stderr}"

    # Read and parse the generated file
    assert output_py_path.is_file(), "Generated file was not created."
    actual_content = output_py_path.read_text()
    tree = ast.parse(actual_content)
    
    # Extract __all__ list
    all_list = extract_all_list_from_ast(tree)
    
    # With no functions/classes, __all__ should either be empty or not present
    # If present, it should be empty
    if all_list is not None:
        assert len(all_list) == 0, f"Expected empty __all__ list, got: {all_list}"


def test_all_list_format_in_generated_code(tmp_path):
    """Test that __all__ list has proper formatting in the generated code."""
    
    # Simple schema and function for testing format
    function_sql_content = """
CREATE OR REPLACE FUNCTION simple_test()
RETURNS INTEGER
LANGUAGE sql
AS $$
    SELECT 42;
$$;
"""

    # Write test files
    function_sql_path = tmp_path / "simple_functions.sql"
    output_py_path = tmp_path / "simple_api.py"

    function_sql_path.write_text(function_sql_content)

    # Run the generator tool
    result = run_cli_tool(function_sql_path, output_py_path)
    assert result.returncode == 0, f"CLI failed: {result.stderr}"

    # Read the generated file as text to check formatting
    actual_content = output_py_path.read_text()
    
    # Verify __all__ appears in the file
    assert "__all__ = [" in actual_content, "__all__ list not found in generated code"
    
    # Verify it's properly positioned (after imports, before functions/classes)
    lines = actual_content.split('\n')
    all_line_index = None
    import_line_index = None
    function_line_index = None
    
    for i, line in enumerate(lines):
        if line.startswith('from ') or line.startswith('import '):
            import_line_index = i
        elif line.startswith('__all__ = ['):
            all_line_index = i
        elif line.startswith('async def '):
            function_line_index = i
            break
    
    # Verify ordering: imports → __all__ → functions/classes
    if import_line_index is not None and all_line_index is not None:
        assert import_line_index < all_line_index, \
            "__all__ should come after imports"
    
    if all_line_index is not None and function_line_index is not None:
        assert all_line_index < function_line_index, \
            "__all__ should come before function definitions"