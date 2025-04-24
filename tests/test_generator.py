import pytest
from pathlib import Path
import subprocess
import sys

# Define paths relative to the test file location
TEST_DIR = Path(__file__).parent
FIXTURES_DIR = TEST_DIR / "fixtures"
EXPECTED_DIR = TEST_DIR / "expected"
PROJECT_ROOT = TEST_DIR.parent # Assumes tests/ is one level down from root


def run_cli_tool(functions_sql: Path, output_py: Path, schema_sql: Path = None):
    """Helper function to run the CLI tool as a subprocess."""
    cmd = [
        sys.executable, # Use the current Python executable
        "-m", 
        "sql_to_python_api.cli", # Invoke the module's entry point
        str(functions_sql),
        str(output_py),
    ]
    if schema_sql:
        cmd.extend(["--schema-file", str(schema_sql)])

    # Run from the project root directory
    result = subprocess.run(cmd, capture_output=True, text=True, cwd=PROJECT_ROOT, check=False)
    
    if result.returncode != 0:
        print("CLI Error STDOUT:", result.stdout)
        print("CLI Error STDERR:", result.stderr)
        result.check_returncode() # Raise CalledProcessError
        
    return result


def test_user_generation_with_schema(tmp_path):
    """Test generating the user API with a separate schema file."""
    functions_sql_path = FIXTURES_DIR / "users2.sql"
    schema_sql_path = FIXTURES_DIR / "schema2.sql"
    expected_output_path = EXPECTED_DIR / "users2_api.py"
    # Use pytest's tmp_path fixture for the output file
    actual_output_path = tmp_path / "users2_api.py" 

    # Run the generator tool
    run_cli_tool(functions_sql_path, actual_output_path, schema_sql_path)

    # Compare the generated file with the expected file
    assert actual_output_path.is_file(), "Generated file was not created."
    
    expected_content = expected_output_path.read_text()
    actual_content = actual_output_path.read_text()
    
    assert actual_content == expected_content, (
        f"Generated file content does not match expected content.\n"
        f"Expected ({expected_output_path}):\n{expected_content}\n"
        f"Actual ({actual_output_path}):\n{actual_content}"
    )
    # No need for manual cleanup, tmp_path handles it

def test_void_function_generation(tmp_path):
    """Test generating a function that returns void."""
    functions_sql_path = FIXTURES_DIR / "void_function.sql"
    expected_output_path = EXPECTED_DIR / "void_function_api.py"
    actual_output_path = tmp_path / "void_function_api.py"

    # Run the generator tool (no schema file needed)
    run_cli_tool(functions_sql_path, actual_output_path)

    # Compare the generated file with the expected file
    assert actual_output_path.is_file(), "Generated file was not created."
    
    expected_content = expected_output_path.read_text()
    actual_content = actual_output_path.read_text()
    
    assert actual_content == expected_content, (
        f"Generated file content does not match expected content.\n"
        f"Expected ({expected_output_path}):\n{expected_content}\n"
        f"Actual ({actual_output_path}):\n{actual_content}"
    )

def test_scalar_function_generation(tmp_path):
    """Test generating functions that return simple scalar types."""
    functions_sql_path = FIXTURES_DIR / "scalar_function.sql"
    expected_output_path = EXPECTED_DIR / "scalar_function_api.py"
    actual_output_path = tmp_path / "scalar_function_api.py"

    # Run the generator tool
    run_cli_tool(functions_sql_path, actual_output_path)

    # Compare the generated file with the expected file
    assert actual_output_path.is_file(), "Generated file was not created."
    
    expected_content = expected_output_path.read_text()
    actual_content = actual_output_path.read_text()
    
    assert actual_content == expected_content, (
        f"Generated file content does not match expected content.\n"
        f"Expected ({expected_output_path}):\n{expected_content}\n"
        f"Actual ({actual_output_path}):\n{actual_content}"
    )

def test_setof_scalar_function_generation(tmp_path):
    """Test generating a function that returns SETOF scalar."""
    functions_sql_path = FIXTURES_DIR / "setof_scalar_function.sql"
    expected_output_path = EXPECTED_DIR / "setof_scalar_function_api.py"
    actual_output_path = tmp_path / "setof_scalar_function_api.py"

    # Run the generator tool
    run_cli_tool(functions_sql_path, actual_output_path)

    # Compare the generated file with the expected file
    assert actual_output_path.is_file(), "Generated file was not created."
    
    expected_content = expected_output_path.read_text()
    actual_content = actual_output_path.read_text()
    
    assert actual_content == expected_content, (
        f"Generated file content does not match expected content.\n"
        f"Expected ({expected_output_path}):\n{expected_content}\n"
        f"Actual ({actual_output_path}):\n{actual_content}"
    )

def test_returns_table_function_generation(tmp_path):
    """Test generating a function that returns TABLE(...)."""
    functions_sql_path = FIXTURES_DIR / "returns_table_function.sql"
    expected_output_path = EXPECTED_DIR / "returns_table_function_api.py"
    actual_output_path = tmp_path / "returns_table_function_api.py"

    # Run the generator tool
    run_cli_tool(functions_sql_path, actual_output_path)

    # Compare the generated file with the expected file
    assert actual_output_path.is_file(), "Generated file was not created."
    
    expected_content = expected_output_path.read_text()
    actual_content = actual_output_path.read_text()
    
    assert actual_content == expected_content, (
        f"Generated file content does not match expected content.\n"
        f"Expected ({expected_output_path}):\n{expected_content}\n"
        f"Actual ({actual_output_path}):\n{actual_content}"
    )

def test_setof_missing_table_function_generation(tmp_path):
    """Test generating a function that returns SETOF table_name where the schema is missing."""
    functions_sql_path = FIXTURES_DIR / "setof_missing_table_function.sql"
    expected_output_path = EXPECTED_DIR / "setof_missing_table_function_api.py"
    actual_output_path = tmp_path / "setof_missing_table_function_api.py"

    # Run the generator tool (no schema file needed)
    run_cli_tool(functions_sql_path, actual_output_path)

    # Compare the generated file with the expected file
    assert actual_output_path.is_file(), "Generated file was not created."
    
    expected_content = expected_output_path.read_text()
    actual_content = actual_output_path.read_text()
    
    assert actual_content == expected_content, (
        f"Generated file content does not match expected content.\n"
        f"Expected ({expected_output_path}):\n{expected_content}\n"
        f"Actual ({actual_output_path}):\n{actual_content}"
    )

def test_returns_record_function_generation(tmp_path):
    """Test generating functions that return record or SETOF record."""
    functions_sql_path = FIXTURES_DIR / "returns_record_function.sql"
    expected_output_path = EXPECTED_DIR / "returns_record_function_api.py"
    actual_output_path = tmp_path / "returns_record_function_api.py"

    # Run the generator tool
    run_cli_tool(functions_sql_path, actual_output_path)

    # Compare the generated file with the expected file
    assert actual_output_path.is_file(), "Generated file was not created."

    expected_content = expected_output_path.read_text()
    actual_content = actual_output_path.read_text()

    assert actual_content == expected_content, (
        f"Generated file content does not match expected content.\n"
        f"Expected ({expected_output_path}):\n{expected_content}\n"
        f"Actual ({actual_output_path}):\n{actual_content}"
    )

def test_array_types_function_generation(tmp_path):
    """Test generating functions that take/return array types."""
    functions_sql_path = FIXTURES_DIR / "array_types_function.sql"
    expected_output_path = EXPECTED_DIR / "array_types_function_api.py"
    actual_output_path = tmp_path / "array_types_function_api.py"

    # Run the generator tool
    run_cli_tool(functions_sql_path, actual_output_path)

    # Compare the generated file with the expected file
    assert actual_output_path.is_file(), "Generated file was not created."
    
    expected_content = expected_output_path.read_text()
    actual_content = actual_output_path.read_text()
    
    assert actual_content == expected_content, (
        f"Generated file content does not match expected content.\n"
        f"Expected ({expected_output_path}):\n{expected_content}\n"
        f"Actual ({actual_output_path}):\n{actual_content}"
    )

def test_no_params_function_generation(tmp_path):
    """Test generating a function that takes no parameters."""
    functions_sql_path = FIXTURES_DIR / "no_params_function.sql"
    expected_output_path = EXPECTED_DIR / "no_params_function_api.py"
    actual_output_path = tmp_path / "no_params_function_api.py"

    # Run the generator tool
    run_cli_tool(functions_sql_path, actual_output_path)

    # Compare the generated file with the expected file
    assert actual_output_path.is_file(), "Generated file was not created."
    
    expected_content = expected_output_path.read_text()
    actual_content = actual_output_path.read_text()
    
    assert actual_content == expected_content, (
        f"Generated file content does not match expected content.\n"
        f"Expected ({expected_output_path}):\n{expected_content}\n"
        f"Actual ({actual_output_path}):\n{actual_content}"
    )

def test_multi_params_function_generation(tmp_path):
    """Test generating a function that takes multiple parameters."""
    functions_sql_path = FIXTURES_DIR / "multi_params_function.sql"
    expected_output_path = EXPECTED_DIR / "multi_params_function_api.py"
    actual_output_path = tmp_path / "multi_params_function_api.py"

    # Run the generator tool
    run_cli_tool(functions_sql_path, actual_output_path)

    # Compare the generated file with the expected file
    assert actual_output_path.is_file(), "Generated file was not created."

    expected_content = expected_output_path.read_text()
    actual_content = actual_output_path.read_text()

    assert actual_content == expected_content, (
        f"Generated file content does not match expected content.\n"
        f"Expected ({expected_output_path}):\n{expected_content}\n"
        f"Actual ({actual_output_path}):\n{actual_content}"
    )

def test_optional_params_function_generation(tmp_path):
    """Test generating a function with optional parameters (DEFAULT)."""
    functions_sql_path = FIXTURES_DIR / "optional_params_function.sql"
    expected_output_path = EXPECTED_DIR / "optional_params_function_api.py"
    actual_output_path = tmp_path / "optional_params_function_api.py"

    # Run the generator tool
    run_cli_tool(functions_sql_path, actual_output_path)

    # Compare the generated file with the expected file
    assert actual_output_path.is_file(), "Generated file was not created."
    
    expected_content = expected_output_path.read_text()
    actual_content = actual_output_path.read_text()
    
    assert actual_content == expected_content, (
        f"Generated file content does not match expected content.\n"
        f"Expected ({expected_output_path}):\n{expected_content}\n"
        f"Actual ({actual_output_path}):\n{actual_content}"
    )
