"""Tests for simplified enum handling using the public API.

These tests verify that SQL enum types are correctly handled without requiring
manual .value access in client code.
"""

from pathlib import Path

from sql2pyapi.generator import generate_python_code

# Import the public API
from sql2pyapi.parser import parse_sql


# Import test utilities


def _get_fixture_path(filename):
    """Return the path to a fixture file."""
    root_dir = Path(__file__).parent.parent  # tests directory
    fixture_path = root_dir / "fixtures" / filename
    return str(fixture_path)


def test_enum_parameter_value_extraction():
    """Test that enum parameters automatically extract .value when passing to SQL."""
    # Test with the enum_handling fixture
    sql_fixture_path = _get_fixture_path("enum_handling.sql")
    with open(sql_fixture_path) as f:
        sql_content = f.read()

    # Parse the SQL
    functions, table_schema_imports, parsed_composite_types, parsed_enum_types = parse_sql(sql_content)

    # Debug - Print the parsed functions and their parameters
    for func in functions:
        if func.sql_name == "add_company_member":
            print(f"\nFunction: {func.sql_name}")
            for param in func.params:
                print(f"  Param: {param.name}, Python type: {param.python_type}")

    # Generate the Python code
    python_code = generate_python_code(
        functions, table_schema_imports, parsed_composite_types, parsed_enum_types, source_sql_file=sql_fixture_path
    )

    # Debug - Print a snippet of the generated code
    print("\nGenerated code snippet:")
    for line in python_code.split("\n")[:50]:
        if "add_company_member" in line:
            print(line)
        if "execute" in line and "add_company_member" in line:
            for i in range(5):
                print(python_code.split("\n")[python_code.split("\n").index(line) - 2 + i])

    # Verify that the add_company_member function converts the enum parameter
    assert "# Extract .value from enum parameters" in python_code
    assert "role_value = role.value if role is not None else None" in python_code

    # Check that _call_params_dict is populated correctly
    # All parameters are non-optional here.
    # SQL names: p_company_id, p_user_id, p_role
    # Python names: company_id, user_id, role
    assert "_sql_named_args_parts.append(f'p_company_id := %(company_id)s')" in python_code
    assert "_call_params_dict['company_id'] = company_id" in python_code

    assert "_sql_named_args_parts.append(f'p_user_id := %(user_id)s')" in python_code
    assert "_call_params_dict['user_id'] = user_id" in python_code

    assert "_sql_named_args_parts.append(f'p_role := %(role)s')" in python_code
    assert "_call_params_dict['role'] = role_value" in python_code  # Uses role_value for enums

    # Ensure the old style direct list or _call_values appends are not present
    assert "[company_id, user_id, role_value]" not in python_code
    assert "_call_values.append(company_id)" not in python_code
    assert "_call_values.append(user_id)" not in python_code
    assert "_call_values.append(role_value)" not in python_code


def test_enum_return_value_conversion():
    """Test that enum returns are automatically converted to enum objects."""
    # Test with the enum_handling fixture
    sql_fixture_path = _get_fixture_path("enum_handling.sql")
    with open(sql_fixture_path) as f:
        sql_content = f.read()

    # Parse the SQL
    functions, table_schema_imports, parsed_composite_types, parsed_enum_types = parse_sql(sql_content)

    # Generate the Python code
    python_code = generate_python_code(
        functions, table_schema_imports, parsed_composite_types, parsed_enum_types, source_sql_file=sql_fixture_path
    )

    # Verify that the get_user_role function returns an enum object
    assert "return CompanyRole(row[0])" in python_code


def test_enum_in_table_result_conversion():
    """Test that enums in table results are converted to enum objects."""
    # Test with the enum_handling fixture
    sql_fixture_path = _get_fixture_path("enum_handling.sql")
    with open(sql_fixture_path) as f:
        sql_content = f.read()

    # Parse the SQL
    functions, table_schema_imports, parsed_composite_types, parsed_enum_types = parse_sql(sql_content)

    # Debug - Print the parsed functions and their return columns
    for func in functions:
        if func.sql_name == "get_company_member":
            print(f"\nFunction: {func.sql_name}")
            print(f"  returns_table: {func.returns_table}")
            print(f"  returns_setof: {func.returns_setof}")
            print(f"  required_imports: {func.required_imports}")
            for col in func.return_columns:
                print(f"  Column: {col.name}, Python type: {col.python_type}")

    # Generate the Python code
    python_code = generate_python_code(
        functions, table_schema_imports, parsed_composite_types, parsed_enum_types, source_sql_file=sql_fixture_path
    )

    # Debug - Print a snippet of the generated code for the function
    print("\nGenerated code snippet for get_company_member:")
    in_function = False
    for line in python_code.split("\n"):
        if "async def get_company_member" in line:
            in_function = True
        if in_function:
            print(line)
        if in_function and line.strip() == "":
            in_function = False

    # Verify that enums in table results are converted in the helper function
    assert "# Inner helper function for efficient conversion" in python_code
    assert "role=CompanyRole(row[3]) if row[3] is not None else None" in python_code


def test_enum_in_list_table_result_conversion():
    """Test that enums in list table results are converted using an inline helper function."""
    # Test with the enum_handling fixture
    sql_fixture_path = _get_fixture_path("enum_handling.sql")
    with open(sql_fixture_path) as f:
        sql_content = f.read()

    # Parse the SQL
    functions, table_schema_imports, parsed_composite_types, parsed_enum_types = parse_sql(sql_content)

    # Debug - Print the parsed functions and their return columns
    for func in functions:
        if func.sql_name == "list_company_members":
            print(f"\nFunction: {func.sql_name}")
            print(f"  returns_table: {func.returns_table}")
            print(f"  returns_setof: {func.returns_setof}")
            print(f"  required_imports: {func.required_imports}")
            for col in func.return_columns:
                print(f"  Column: {col.name}, Python type: {col.python_type}")

    # Generate the Python code
    python_code = generate_python_code(
        functions, table_schema_imports, parsed_composite_types, parsed_enum_types, source_sql_file=sql_fixture_path
    )

    # Debug - Print a snippet of the generated code for the function
    print("\nGenerated code snippet for list_company_members:")
    in_function = False
    for line in python_code.split("\n"):
        if "async def list_company_members" in line:
            in_function = True
        if in_function:
            print(line)
        if in_function and line.strip() == "":
            in_function = False

    # Verify that we generate an inner helper function for list results
    assert "# Inner helper function for efficient conversion" in python_code
    assert "def create_listcompanymembersresult(row):" in python_code
    assert "role=CompanyRole(row[3]) if row[3] is not None else None" in python_code
    assert "return [create_listcompanymembersresult(row) for row in rows]" in python_code
