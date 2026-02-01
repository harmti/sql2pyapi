"""Test that a column named 'language' in RETURNS TABLE does not confuse the parser.

The LANGUAGE keyword is used in SQL function definitions (e.g., LANGUAGE sql, LANGUAGE plpgsql).
If a RETURNS TABLE has a column named 'language', the parser's regex should not mistake it
for the LANGUAGE keyword and prematurely terminate the return definition parsing.
"""

import ast
import subprocess
import sys
from pathlib import Path

TESTS_ROOT_DIR = Path(__file__).parent.parent
FIXTURES_DIR = TESTS_ROOT_DIR / "fixtures"
PROJECT_ROOT = TESTS_ROOT_DIR.parent


def run_cli_tool(functions_sql, output_py):
    cmd = [
        sys.executable,
        "-m",
        "sql2pyapi.cli",
        str(functions_sql),
        str(output_py),
    ]
    return subprocess.run(cmd, capture_output=True, text=True, cwd=PROJECT_ROOT, check=False)


def test_returns_table_with_language_column(tmp_path):
    """Column named 'language' in RETURNS TABLE should not conflict with LANGUAGE keyword."""
    functions_sql = FIXTURES_DIR / "returns_table_language_column.sql"
    output_py = tmp_path / "language_col_api.py"

    result = run_cli_tool(functions_sql, output_py)

    assert result.returncode == 0, f"CLI failed: {result.stderr}"
    assert output_py.is_file(), "Generated file was not created."

    content = output_py.read_text()
    tree = ast.parse(content)

    # Find the generated async function
    func_node = None
    for node in ast.walk(tree):
        if isinstance(node, ast.AsyncFunctionDef) and node.name == "get_collections":
            func_node = node
            break

    assert func_node is not None, "Function 'get_collections' not found in generated code"

    # Find the result dataclass - should have all 4 columns including 'language'
    dataclass_node = None
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef) and "Collection" in node.name or (
            isinstance(node, ast.ClassDef) and "GetCollections" in node.name
        ):
            dataclass_node = node
            break

    assert dataclass_node is not None, (
        f"Result dataclass not found. Generated content:\n{content}"
    )

    # Extract field names from the dataclass
    field_names = []
    for item in dataclass_node.body:
        if isinstance(item, ast.AnnAssign) and isinstance(item.target, ast.Name):
            field_names.append(item.target.id)

    assert "language" in field_names, (
        f"Column 'language' missing from dataclass fields. Found: {field_names}. "
        f"The parser likely confused the column name with the LANGUAGE keyword."
    )
    assert "status" in field_names, (
        f"Column 'status' missing from dataclass fields. Found: {field_names}"
    )
    assert "id" in field_names, f"Column 'id' missing from dataclass fields. Found: {field_names}"
    assert "name" in field_names, f"Column 'name' missing from dataclass fields. Found: {field_names}"
    assert len(field_names) == 4, (
        f"Expected 4 fields (id, name, language, status), got {len(field_names)}: {field_names}"
    )


def test_returns_table_with_language_column_plpgsql(tmp_path):
    """Same test but with LANGUAGE plpgsql and $$ body style."""
    sql_content = """\
CREATE OR REPLACE FUNCTION get_items_by_language(p_language text)
RETURNS TABLE(id uuid, title text, language text, status text)
AS $$
BEGIN
    RETURN QUERY
    SELECT i.id, i.title, i.language, i.status
    FROM items i
    WHERE i.language = p_language;
END;
$$ LANGUAGE plpgsql;
"""
    sql_file = tmp_path / "language_col_plpgsql.sql"
    sql_file.write_text(sql_content)
    output_py = tmp_path / "language_col_plpgsql_api.py"

    result = run_cli_tool(sql_file, output_py)

    assert result.returncode == 0, f"CLI failed: {result.stderr}"
    assert output_py.is_file(), "Generated file was not created."

    content = output_py.read_text()
    tree = ast.parse(content)

    # Find the generated async function
    func_node = None
    for node in ast.walk(tree):
        if isinstance(node, ast.AsyncFunctionDef) and node.name == "get_items_by_language":
            func_node = node
            break

    assert func_node is not None, (
        f"Function 'get_items_by_language' not found. Generated content:\n{content}"
    )

    # Find the result dataclass
    dataclass_node = None
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef):
            # Check field names to find the right dataclass
            fields = [
                item.target.id
                for item in node.body
                if isinstance(item, ast.AnnAssign) and isinstance(item.target, ast.Name)
            ]
            if "language" in fields or "title" in fields:
                dataclass_node = node
                break

    assert dataclass_node is not None, (
        f"Result dataclass not found. Generated content:\n{content}"
    )

    field_names = [
        item.target.id
        for item in dataclass_node.body
        if isinstance(item, ast.AnnAssign) and isinstance(item.target, ast.Name)
    ]

    assert "language" in field_names, (
        f"Column 'language' missing from dataclass fields. Found: {field_names}. "
        f"The parser likely confused the column name with the LANGUAGE keyword."
    )
    assert len(field_names) == 4, (
        f"Expected 4 fields (id, title, language, status), got {len(field_names)}: {field_names}"
    )
