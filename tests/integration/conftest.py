import pytest
from pathlib import Path
import subprocess
import sys

# Define paths relative to the main tests/ directory
TESTS_ROOT_DIR = Path(__file__).parent # This is tests/integration/
PROJECT_ROOT = TESTS_ROOT_DIR.parent.parent # Go up two levels to project root

@pytest.fixture
def run_cli_tool():
    """Fixture to provide a helper function for running the CLI tool."""
    def _run_cli(functions_sql: Path, output_py: Path, schema_sql: Path = None, verbose: bool = False, allow_missing_schemas: bool = False, no_helpers: bool = False):
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
            cmd.append("-v") # Add verbose flag if requested
        if allow_missing_schemas:
            cmd.append("--allow-missing-schemas")
        if no_helpers:
            cmd.append("--no-helpers")

        # Run from the project root directory
        result = subprocess.run(cmd, capture_output=True, text=True, cwd=PROJECT_ROOT, check=False)

        # Always print output if the input is table_col_comments.sql for debugging
        # Or if there was an error
        if "table_col_comments.sql" in str(functions_sql) or result.returncode != 0:
            print(f"--- CLI Output for {functions_sql.name} ---")
            print(f"Command: {' '.join(cmd)}")
            print("STDOUT:", result.stdout)
            print("STDERR:", result.stderr)
            print("--- End CLI Output ---")
        # elif result.returncode != 0: # Combined above
        #     # Keep these prints for actual errors in other tests
        #     print("CLI Error STDOUT:", result.stdout)
        #     print("CLI Error STDERR:", result.stderr)

        return result
    return _run_cli

# You might want to add fixtures for common paths too
@pytest.fixture(scope="session")
def fixtures_dir() -> Path:
    return Path(__file__).parent.parent / "fixtures"

@pytest.fixture(scope="session")
def expected_dir() -> Path:
    return Path(__file__).parent.parent / "expected"

@pytest.fixture(scope="session")
def project_root() -> Path:
    return Path(__file__).parent.parent.parent 