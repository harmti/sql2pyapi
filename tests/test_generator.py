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


def test_user_generation_with_schema():
    """Test generating the user API with a separate schema file."""
    functions_sql_path = FIXTURES_DIR / "users2.sql"
    schema_sql_path = FIXTURES_DIR / "schema2.sql"
    expected_output_path = EXPECTED_DIR / "users2_api.py"
    # Create a temporary output file for the test run
    # Note: pytest provides fixtures like `tmp_path` for better temporary file handling
    # Using a fixed name for now, but tmp_path is recommended for more robust tests.
    actual_output_path = TEST_DIR / "_temp_users2_api.py" 

    try:
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

    finally:
        # Clean up the temporary file
        if actual_output_path.exists():
            actual_output_path.unlink() 