import ast
from pathlib import Path


# Define paths relative to the main tests/ directory
TESTS_ROOT_DIR = Path(__file__).parent.parent  # Go up one level to tests/
FIXTURES_DIR = TESTS_ROOT_DIR / "fixtures"
EXPECTED_DIR = TESTS_ROOT_DIR / "expected"


def test_no_helpers_flag(tmp_path, run_cli_tool):
    """Test that --no-helpers flag omits helper functions and imports."""
    functions_sql_path = FIXTURES_DIR / "scalar_function.sql"  # Use a simple fixture
    actual_output_path = tmp_path / "scalar_function_no_helpers_api.py"

    # Run the generator tool WITH the --no-helpers flag via the fixture
    result = run_cli_tool(functions_sql_path, actual_output_path, no_helpers=True)
    assert result.returncode == 0, f"CLI tool failed with --no-helpers: {result.stderr}"

    # --- Assertions ---
    assert actual_output_path.is_file(), "Generated file was not created."
    actual_content = actual_output_path.read_text()

    # 1. Check helper functions are NOT present
    assert "def get_optional(" not in actual_content, "get_optional helper function found but should be omitted."
    assert "def get_required(" not in actual_content, "get_required helper function found but should be omitted."
    assert "# ===== SECTION: RESULT HELPERS ====" not in actual_content, (
        "Helper section header found but should be omitted."
    )

    # 2. Check helper-specific imports are NOT present (unless needed otherwise)
    # Use AST to be more robust than simple string checking
    tree = ast.parse(actual_content)
    found_typevar = False
    found_sequence = False
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module == "typing":
            for alias in node.names:
                if alias.name == "TypeVar":
                    found_typevar = True
                if alias.name == "Sequence":
                    found_sequence = True

    assert not found_typevar, "TypeVar import found but should be omitted when helpers are excluded."
    assert not found_sequence, "Sequence import found but should be omitted when helpers are excluded."
