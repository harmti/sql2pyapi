"""Tests for handling comments in type definitions."""

from pathlib import Path

# Import the public API
from sql2pyapi.parser import parse_sql

# Import test utilities
from tests.test_utils import find_function


def test_type_definition_with_comments():
    """Test that type definitions with inline comments are parsed correctly."""
    # Load the test SQL file
    test_file = Path(__file__).parent.parent / "fixtures" / "type_with_comments.sql"
    with open(test_file, encoding="utf-8") as f:
        sql_content = f.read()

    # Debug: Print the SQL content being parsed
    print("\n=== SQL Content ===")
    print(sql_content)

    # Find the type definition in the SQL content
    type_def_start = -1
    # Look for the type definition, accounting for potential comments before it
    for i, line in enumerate(sql_content.splitlines()):
        if "CREATE TYPE daily_consumption_summary AS (" in line:
            type_def_start = sql_content.find(line)
            break

    if type_def_start == -1:
        print("ERROR: Could not find type definition in SQL content")
        type_def = ""
    else:
        # Find the matching closing parenthesis
        depth = 0
        type_def_end = type_def_start
        for i in range(type_def_start, len(sql_content)):
            if sql_content[i] == "(":
                depth += 1
            elif sql_content[i] == ")":
                depth -= 1
                if depth == 0:
                    type_def_end = i + 1
                    break
        type_def = sql_content[type_def_start:type_def_end]
    print("\n=== Type Definition ===")
    print(type_def)

    # Parse the SQL
    functions, _, composite_types, _ = parse_sql(sql_content)

    # Debug output
    print("\n=== Parsed composite types ===")
    for type_name, columns in composite_types.items():
        print(f"Type: {type_name}")
        for i, col in enumerate(columns, 1):
            print(f"  {i}. {col.name}: {col.sql_type} (Python: {col.python_type})")

    # Verify the composite type was parsed correctly
    assert "daily_consumption_summary" in composite_types, (
        f"daily_consumption_summary not found in {list(composite_types.keys())}"
    )
    type_columns = composite_types["daily_consumption_summary"]

    # Debug output for the columns we found
    print("\n=== Parsed columns ===")
    for i, col in enumerate(type_columns, 1):
        print(f"  {i}. {col.name}: {col.sql_type} (Python: {col.python_type})")

    # Verify the number of columns
    assert len(type_columns) == 5, f"Expected 5 columns, got {len(type_columns)}: {[col.name for col in type_columns]}"

    # Verify each column and its type
    expected_columns = [
        ("day", "str"),
        ("location_id", "UUID"),
        ("location_name", "str"),
        ("quantity", "Decimal"),
        ("unittype", "str"),
    ]

    for (name, py_type), col in zip(expected_columns, type_columns, strict=False):
        assert col.name == name
        assert col.python_type == py_type

    # Verify the function that uses the type
    func = find_function(functions, "get_daily_consumption")
    assert func is not None
    assert func.returns_table
    assert func.return_columns == type_columns

    # Verify parameter parsing
    assert len(func.params) == 2
    assert func.params[0].name == "p_location_id"
    assert func.params[0].python_type == "UUID"
    assert func.params[1].name == "p_day"
    assert func.params[1].python_type == "date"
