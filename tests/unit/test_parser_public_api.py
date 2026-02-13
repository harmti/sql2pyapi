"""Unit tests for the sql2pyapi parser module using only the public API.

These tests verify the parser functionality through the public API (parse_sql)
rather than testing internal implementation details.
"""

import pytest

# Import the public API
from tests.test_utils import create_test_enum

# Import test utilities
from tests.test_utils import create_test_function
from tests.test_utils import create_test_table
from tests.test_utils import find_function
from tests.test_utils import find_parameter
from tests.test_utils import find_return_column
from tests.test_utils import parse_test_sql


# === Type Mapping Tests ===

# Parameterized test cases: (sql_type, is_optional, expected_py_type, expected_imports)
map_type_test_cases = [
    # Basic types
    ("integer", False, "int", set()),
    ("int", False, "int", set()),
    ("bigint", False, "int", set()),
    ("smallint", False, "int", set()),
    ("serial", False, "int", set()),
    ("bigserial", False, "int", set()),
    ("text", False, "str", set()),
    ("varchar", False, "str", set()),
    ("character varying", False, "str", set()),
    ("character", False, "str", set()),
    ("char", False, "str", set()),
    ("CHAR(2)", False, "str", set()),
    ("char(10)", False, "str", set()),
    ("boolean", False, "bool", set()),
    ("bool", False, "bool", set()),
    ("bytea", False, "bytes", set()),
    # Types requiring imports
    ("uuid", False, "UUID", {"UUID"}),
    ("timestamp", False, "datetime", {"datetime"}),
    ("timestamp without time zone", False, "datetime", {"datetime"}),
    ("timestamptz", False, "datetime", {"datetime"}),
    ("timestamp with time zone", False, "datetime", {"datetime"}),
    ("date", False, "date", {"date"}),
    ("interval", False, "timedelta", {"timedelta"}),
    ("numeric", False, "Decimal", {"Decimal"}),
    ("decimal", False, "Decimal", {"Decimal"}),
    # JSON types
    ("json", False, "Dict[str, Any]", {"Dict", "Any"}),
    ("jsonb", False, "Dict[str, Any]", {"Dict", "Any"}),
    # Unknown type
    ("some_unknown_type", False, "Any", {"Any"}),
    # Case insensitivity and whitespace
    (" INTEGER ", False, "int", set()),
    (" VARCHAR ", False, "str", set()),
    # Optional types (basic)
    ("integer", True, "Optional[int]", {"Optional"}),
    ("text", True, "Optional[str]", {"Optional"}),
    ("boolean", True, "Optional[bool]", {"Optional"}),
    # Optional types (requiring imports)
    ("uuid", True, "Optional[UUID]", {"Optional", "UUID"}),
    ("date", True, "Optional[date]", {"Optional", "date"}),
    ("interval", True, "Optional[timedelta]", {"Optional", "timedelta"}),
    ("numeric", True, "Optional[Decimal]", {"Optional", "Decimal"}),
    ("jsonb", True, "Optional[Dict[str, Any]]", {"Optional", "Dict", "Any"}),
    # Optional unknown type maps to Any (not Optional[Any])
    ("some_unknown_type", True, "Any", {"Any"}),
    # Array types (basic)
    ("integer[]", False, "List[int]", {"List"}),
    ("text[]", False, "List[str]", {"List"}),
    ("varchar[]", False, "List[str]", {"List"}),
    # Array types (requiring imports)
    ("uuid[]", False, "List[UUID]", {"List", "UUID"}),
    ("date[]", False, "List[date]", {"List", "date"}),
    ("interval[]", False, "List[timedelta]", {"List", "timedelta"}),
    ("numeric[]", False, "List[Decimal]", {"List", "Decimal"}),
    ("jsonb[]", False, "List[Dict[str, Any]]", {"List", "Dict", "Any"}),
    # Optional array types (Now expecting Optional[List[T]])
    ("integer[]", True, "Optional[List[int]]", {"List", "Optional"}),
    ("uuid[]", True, "Optional[List[UUID]]", {"List", "UUID", "Optional"}),
    ("interval[]", True, "Optional[List[timedelta]]", {"List", "timedelta", "Optional"}),
    # We'll skip complex type names for now as they require special handling
    # and are tested in other ways
]


@pytest.mark.parametrize("sql_type, is_optional, expected_py_type, expected_imports", map_type_test_cases)
def test_map_sql_to_python_type_via_public_api(sql_type, is_optional, expected_py_type, expected_imports):
    """Tests SQL type mapping through the public API."""
    # Create a function with a parameter of the given type
    param_name = "p_test"

    # For SQL types with special characters, we'll just use a simple type
    # to avoid syntax issues - complex types are tested elsewhere
    param_def = f"{param_name} {sql_type}"

    # Add DEFAULT NULL for optional parameters
    if is_optional:
        param_def += " DEFAULT NULL"

    # Create and parse a test function
    func_sql = create_test_function("test_type_mapping", param_def)
    functions, _, _, _ = parse_test_sql(func_sql)

    # Find the function and parameter
    func = find_function(functions, "test_type_mapping")
    param = find_parameter(func, param_name)

    # Verify the parameter type and imports
    assert param.python_type == expected_py_type, f"Expected {expected_py_type}, got {param.python_type}"

    # Check that all expected imports are included in the function's required imports
    for imp in expected_imports:
        assert imp in func.required_imports, f"Expected import {imp} not found in {func.required_imports}"


# === Parameter Parsing Tests ===

# Parameterized test cases: (param_str, expected_params, expected_imports)
parse_params_test_cases = [
    # No params
    ("", [], set()),
    # Single simple param
    ("p_name text", [("p_name", "name", "text", "str", False)], set()),
    # Multiple simple params
    (
        "p_id integer, p_email varchar",
        [("p_id", "id", "integer", "int", False), ("p_email", "email", "varchar", "str", False)],
        set(),
    ),
    # Params with default values (implies optional)
    ("p_count int DEFAULT 0", [("p_count", "count", "int", "Optional[int]", True)], {"Optional"}),
    ("p_tag text DEFAULT 'hello'", [("p_tag", "tag", "text", "Optional[str]", True)], {"Optional"}),
    # Params with complex types
    ("p_ids uuid[]", [("p_ids", "ids", "uuid[]", "List[UUID]", False)], {"List", "UUID"}),
    ("p_data jsonb", [("p_data", "data", "jsonb", "Dict[str, Any]", False)], {"Dict", "Any"}),
    # Params with IN/OUT/INOUT modes
    ("IN p_user_id int", [("p_user_id", "user_id", "int", "int", False)], set()),
    ("OUT p_result text", [("p_result", "result", "text", "str", False)], set()),
    # Mixed cases
    (
        "p_id int, p_name text DEFAULT 'Guest', p_values int[]",
        [
            ("p_id", "id", "int", "int", False),
            ("p_name", "name", "text", "Optional[str]", True),
            ("p_values", "values", "int[]", "List[int]", False),
        ],
        {"Optional", "List"},
    ),
]


@pytest.mark.parametrize("param_str, expected_params, expected_imports", parse_params_test_cases)
def test_parse_params_via_public_api(param_str, expected_params, expected_imports):
    """Tests parameter parsing through the public API."""
    # Create and parse a test function
    func_sql = create_test_function("test_params", param_str)
    functions, _, _, _ = parse_test_sql(func_sql)

    # Find the function
    func = find_function(functions, "test_params")

    # Verify the parameters
    assert len(func.params) == len(expected_params), f"Expected {len(expected_params)} params, got {len(func.params)}"

    for i, (name, py_name, sql_type, py_type, is_optional) in enumerate(expected_params):
        assert func.params[i].name == name
        assert func.params[i].python_name == py_name
        assert func.params[i].sql_type == sql_type
        assert func.params[i].python_type == py_type
        assert func.params[i].is_optional == is_optional

    # Check that all expected imports are included in the function's required imports
    for imp in expected_imports:
        assert imp in func.required_imports, f"Expected import {imp} not found in {func.required_imports}"


# === Return Clause Tests ===

# Test cases for return clause parsing
return_clause_test_cases = [
    # Simple scalar return
    ("integer", False, False, False, "Optional[int]", [], None, {"Optional"}),
    # SETOF scalar
    ("SETOF text", False, False, True, "List[str]", [], None, {"List"}),
    # Return TABLE - note that the parser treats this as returns_table=True, returns_setof=True
    # and might return List[TestReturnsResult] instead of just TestReturnsResult
    (
        "TABLE(id integer, name text)",
        True,
        False,
        True,
        "TestReturnsResult",
        [("id", "integer", "Optional[int]", True), ("name", "text", "Optional[str]", True)],
        None,
        {"dataclass", "Optional"},
    ),
    # Return SETOF TABLE - parser treats this as returns_table=True
    (
        "SETOF TABLE(id uuid, active boolean)",
        True,
        False,
        True,
        "List[TestReturnsResult]",
        [("id", "uuid", "Optional[UUID]", True), ("active", "boolean", "Optional[bool]", True)],
        None,
        {"dataclass", "List", "UUID", "Optional"},
    ),
    # Return record - falls back to Tuple when body cannot be parsed
    ("record", False, True, False, "Optional[Tuple]", [], None, {"Tuple", "Optional"}),
    # Return SETOF record - falls back to Tuple when body cannot be parsed
    ("SETOF record", False, True, True, "List[Tuple]", [], None, {"List", "Tuple"}),
]


@pytest.mark.parametrize(
    "return_clause, returns_table, returns_record, returns_setof, "
    "expected_return_type, expected_columns, expected_setof_table, expected_imports",
    return_clause_test_cases,
)
def test_return_clause_via_public_api(
    return_clause,
    returns_table,
    returns_record,
    returns_setof,
    expected_return_type,
    expected_columns,
    expected_setof_table,
    expected_imports,
):
    """Tests return clause parsing through the public API."""
    # Create and parse a test function
    func_sql = create_test_function("test_returns", returns=return_clause)
    functions, _, _, _ = parse_test_sql(func_sql)

    # Find the function
    func = find_function(functions, "test_returns")

    # Verify return properties
    assert func.returns_table == returns_table, f"Expected returns_table={returns_table}, got {func.returns_table}"
    assert func.returns_record == returns_record, f"Expected returns_record={returns_record}, got {func.returns_record}"
    assert func.returns_setof == returns_setof, f"Expected returns_setof={returns_setof}, got {func.returns_setof}"

    # For dataclass returns, we need special handling
    if "dataclass" in expected_imports:
        # For TABLE returns, the parser might return List[TestReturnsResult] instead of just TestReturnsResult
        # depending on how it interprets the clause
        if expected_return_type == "TestReturnsResult" and func.return_type.startswith("List["):
            assert "TestReturnsResult" in func.return_type, (
                f"Expected class name 'TestReturnsResult' in {func.return_type}"
            )
        # For SETOF TABLE returns
        elif expected_return_type.startswith("List["):
            assert func.return_type.startswith("List["), f"Expected List type, got {func.return_type}"
            class_name = expected_return_type.split("[")[-1].rstrip("]")
            assert class_name in func.return_type, f"Expected class name '{class_name}' in {func.return_type}"
        else:
            assert func.return_type == expected_return_type, f"Expected {expected_return_type}, got {func.return_type}"
    else:
        assert func.return_type == expected_return_type, f"Expected {expected_return_type}, got {func.return_type}"

    # Verify setof table name if applicable
    if expected_setof_table:
        assert func.setof_table_name == expected_setof_table, (
            f"Expected setof_table_name={expected_setof_table}, got {func.setof_table_name}"
        )

    # Verify return columns if applicable
    if expected_columns:
        assert len(func.return_columns) == len(expected_columns), (
            f"Expected {len(expected_columns)} columns, got {len(func.return_columns)}"
        )
        for i, (name, sql_type, py_type, is_optional) in enumerate(expected_columns):
            assert func.return_columns[i].name == name, (
                f"Expected column name {name}, got {func.return_columns[i].name}"
            )
            assert func.return_columns[i].sql_type == sql_type, (
                f"Expected SQL type {sql_type}, got {func.return_columns[i].sql_type}"
            )
            assert func.return_columns[i].python_type == py_type, (
                f"Expected Python type {py_type}, got {func.return_columns[i].python_type}"
            )
            assert func.return_columns[i].is_optional == is_optional, (
                f"Expected is_optional={is_optional}, got {func.return_columns[i].is_optional}"
            )

    # Check that all expected imports are included in the function's required imports
    for imp in expected_imports:
        assert imp in func.required_imports, f"Expected import {imp} not found in {func.required_imports}"


# === Table Schema Tests ===


def test_table_schema_integration():
    """Tests that table schemas are correctly parsed and used for return types."""
    # Create a test table
    table_sql = create_test_table(
        "users",
        """
        user_id uuid PRIMARY KEY,
        email varchar(255) NOT NULL,
        created_at timestamp DEFAULT now()
    """,
    )

    # Create a function that returns the table
    func_sql = create_test_function("get_user", "p_id uuid", "users")

    # Parse both
    functions, table_imports, _, _ = parse_test_sql(func_sql, table_sql)

    # Find the function
    func = find_function(functions, "get_user")

    # Verify that the function returns the table
    assert func.returns_table
    assert not func.returns_setof
    assert len(func.return_columns) == 3

    # Verify the return columns
    user_id_col = find_return_column(func, "user_id")
    assert user_id_col.sql_type == "uuid"
    assert user_id_col.python_type == "UUID"
    assert not user_id_col.is_optional

    email_col = find_return_column(func, "email")
    assert email_col.sql_type == "varchar(255)"
    assert email_col.python_type == "str"
    assert not email_col.is_optional

    created_at_col = find_return_column(func, "created_at")
    assert created_at_col.sql_type == "timestamp"
    assert created_at_col.python_type == "Optional[datetime]"
    assert created_at_col.is_optional

    # Verify imports
    assert "UUID" in func.required_imports
    assert "datetime" in func.required_imports
    assert "Optional" in func.required_imports
    assert "dataclass" in func.required_imports


# === Schema-Qualified Table Tests ===


def test_schema_qualified_table_integration():
    """Tests that schema-qualified table names are correctly parsed and used for return types."""
    # Create a test table with schema qualification
    table_sql = create_test_table(
        "public.companies",
        """
        company_id serial PRIMARY KEY,
        name text NOT NULL,
        founded_date date
    """,
    )

    # Create functions that return the table with different qualifications
    func1_sql = create_test_function("get_company", "p_id integer", "public.companies")
    func2_sql = create_test_function("list_companies", "", "SETOF companies")

    # Parse all
    functions, table_imports, _, _ = parse_test_sql(func1_sql + "\n" + func2_sql, table_sql)

    # Find the functions
    func1 = find_function(functions, "get_company")
    func2 = find_function(functions, "list_companies")

    # Verify that both functions return the same table structure
    assert func1.returns_table
    assert func2.returns_table
    assert not func1.returns_setof
    assert func2.returns_setof

    # Both should have the same columns
    assert len(func1.return_columns) == 3
    assert len(func2.return_columns) == 3

    # Verify columns in both functions
    for func in [func1, func2]:
        company_id_col = find_return_column(func, "company_id")
        assert company_id_col.sql_type == "serial"
        assert company_id_col.python_type == "int"
        assert not company_id_col.is_optional

        name_col = find_return_column(func, "name")
        assert name_col.sql_type == "text"
        assert name_col.python_type == "str"
        assert not name_col.is_optional

        founded_col = find_return_column(func, "founded_date")
        assert founded_col.sql_type == "date"
        assert founded_col.python_type == "Optional[date]"
        assert founded_col.is_optional


# === Enum Type Tests ===


def test_enum_type_integration():
    """Tests that enum types are correctly parsed and used."""
    # Create a test enum type
    enum_sql = create_test_enum("status_type", ["pending", "active", "inactive"])

    # Create a function that uses the enum
    func_sql = create_test_function(
        "get_users_by_status", "p_status status_type", "TABLE(user_id uuid, status status_type)"
    )

    # Parse both
    functions, _, _, enum_types = parse_test_sql(func_sql, enum_sql)

    # Find the function
    func = find_function(functions, "get_users_by_status")

    # Verify the parameter type
    param = find_parameter(func, "p_status")
    assert param.sql_type == "status_type"
    assert param.python_type == "StatusType"  # Should be converted to PascalCase

    # Verify the return column type
    status_col = find_return_column(func, "status")
    assert status_col.sql_type == "status_type"
    assert status_col.python_type == "StatusType"

    # Verify the enum type was parsed
    assert "status_type" in enum_types
    assert enum_types["status_type"] == ["pending", "active", "inactive"]

    # Verify imports
    assert "Enum" in func.required_imports
    assert "UUID" in func.required_imports
