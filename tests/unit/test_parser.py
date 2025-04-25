"""Unit tests for the sql2pyapi parser module."""

import pytest
from typing import Optional, Tuple, Set, List

# Import the function under test (even if private)
from sql2pyapi.parser import _map_sql_to_python_type, _parse_params, SQLParameter

# Import the functions/classes under test
from sql2pyapi.parser import (
    _parse_column_definitions,
    ReturnColumn,
)

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
    ("boolean", False, "bool", set()),
    ("bool", False, "bool", set()),
    ("bytea", False, "bytes", set()),
    # Types requiring imports
    ("uuid", False, "UUID", {"from uuid import UUID"}),
    ("timestamp", False, "datetime", {"from datetime import datetime"}),
    ("timestamp without time zone", False, "datetime", {"from datetime import datetime"}),
    ("timestamptz", False, "datetime", {"from datetime import datetime"}),
    ("timestamp with time zone", False, "datetime", {"from datetime import datetime"}),
    ("date", False, "date", {"from datetime import date"}),
    ("numeric", False, "Decimal", {"from decimal import Decimal"}),
    ("decimal", False, "Decimal", {"from decimal import Decimal"}),
    # JSON types
    ("json", False, "Dict[str, Any]", {"from typing import Dict", "from typing import Any"}),
    ("jsonb", False, "Dict[str, Any]", {"from typing import Dict", "from typing import Any"}),
    # Unknown type
    ("some_unknown_type", False, "Any", {"from typing import Any"}),
    # Case insensitivity and whitespace
    (" INTEGER ", False, "int", set()),
    (" VARCHAR ", False, "str", set()),
    # Optional types (basic)
    ("integer", True, "Optional[int]", {"from typing import Optional"}),
    ("text", True, "Optional[str]", {"from typing import Optional"}),
    ("boolean", True, "Optional[bool]", {"from typing import Optional"}),
    # Optional types (requiring imports)
    ("uuid", True, "Optional[UUID]", {"from typing import Optional", "from uuid import UUID"}),
    ("date", True, "Optional[date]", {"from typing import Optional", "from datetime import date"}),
    ("numeric", True, "Optional[Decimal]", {"from typing import Optional", "from decimal import Decimal"}),
    ("jsonb", True, "Optional[Dict[str, Any]]", {"from typing import Optional", "from typing import Dict", "from typing import Any"}),
    # Optional unknown type maps to Any (not Optional[Any])
    ("some_unknown_type", True, "Any", {"from typing import Any"}),
    # Array types (basic)
    ("integer[]", False, "List[int]", {"from typing import List"}),
    ("text[]", False, "List[str]", {"from typing import List"}),
    ("varchar[]", False, "List[str]", {"from typing import List"}),
    # Array types (requiring imports)
    ("uuid[]", False, "List[UUID]", {"from typing import List", "from uuid import UUID"}),
    ("date[]", False, "List[date]", {"from typing import List", "from datetime import date"}),
    ("numeric[]", False, "List[Decimal]", {"from typing import List", "from decimal import Decimal"}),
    ("jsonb[]", False, "List[Dict[str, Any]]", {"from typing import List", "from typing import Dict", "from typing import Any"}),
    # Optional array types (Now expecting Optional[List[T]])
    ("integer[]", True, "Optional[List[int]]", {"from typing import List", "from typing import Optional"}),
    ("uuid[]", True, "Optional[List[UUID]]", {"from typing import List", "from uuid import UUID", "from typing import Optional"}),
    # Complex type names (like varchar(N))
    ("character varying(255)", False, "str", set()),
    ("varchar(100)", False, "str", set()),
    ("numeric(10, 2)", False, "Decimal", {"from decimal import Decimal"}),
    ("decimal(5, 0)", False, "Decimal", {"from decimal import Decimal"}),
    ("timestamp(0) without time zone", False, "datetime", {"from datetime import datetime"}),
    ("timestamp(6) with time zone", False, "datetime", {"from datetime import datetime"}),
    # Optional complex types
    ("character varying(50)", True, "Optional[str]", {"from typing import Optional"}),
    ("numeric(8, 4)", True, "Optional[Decimal]", {"from typing import Optional", "from decimal import Decimal"}),
]


@pytest.mark.parametrize("sql_type, is_optional, expected_py_type, expected_imports", map_type_test_cases)
def test_map_sql_to_python_type(sql_type: str, is_optional: bool, expected_py_type: str, expected_imports: Set[str]):
    """Tests the _map_sql_to_python_type function with various inputs."""
    py_type, imports_str = _map_sql_to_python_type(sql_type, is_optional)

    # Check the Python type
    assert py_type == expected_py_type

    # Check the imports
    # Convert the returned string of imports (or None) into a set for comparison
    returned_imports = set(imports_str.split('\n')) if imports_str else set()
    # Remove empty strings that might result from splitting None or empty string
    returned_imports.discard('')

    assert returned_imports == expected_imports


# Remove the dummy test now that we have real tests
# def test_dummy():
#     """Dummy test to ensure discovery."""
#     assert True 


# --- Tests for _parse_params --- 

# Parameterized test cases: (param_str, expected_params, expected_imports)
parse_params_test_cases = [
    # No params
    ("", [], set()), 
    (" ", [], set()),
    # Single simple param
    ("p_name text", [SQLParameter('p_name', 'name', 'text', 'str', False)], set()),
    # Multiple simple params
    ("p_id integer, p_email varchar", [
        SQLParameter('p_id', 'id', 'integer', 'int', False),
        SQLParameter('p_email', 'email', 'varchar', 'str', False)
    ], set()),
    # Params needing pythonic name conversion
    ("_p_name text, _value int", [
        SQLParameter('_p_name', 'p_name', 'text', 'str', False),
        SQLParameter('_value', 'value', 'int', 'int', False)
    ], set()),
    # Params with default values (implies optional)
    ("p_count int DEFAULT 0", [SQLParameter('p_count', 'count', 'int', 'Optional[int]', True)], {"from typing import Optional"}),
    ("p_tag text DEFAULT 'hello'", [SQLParameter('p_tag', 'tag', 'text', 'Optional[str]', True)], {"from typing import Optional"}),
    ("p_active boolean DEFAULT true, p_ratio numeric DEFAULT 0.5", [
        SQLParameter('p_active', 'active', 'boolean', 'Optional[bool]', True),
        SQLParameter('p_ratio', 'ratio', 'numeric', 'Optional[Decimal]', True)
    ], {"from typing import Optional", "from decimal import Decimal"}),
    # Params with complex types
    ("p_ids uuid[]", [SQLParameter('p_ids', 'ids', 'uuid[]', 'List[UUID]', False)], {"from typing import List", "from uuid import UUID"}),
    ("p_data jsonb", [SQLParameter('p_data', 'data', 'jsonb', 'Dict[str, Any]', False)], {"from typing import Dict", "from typing import Any"}),
    ("p_dates date[] DEFAULT NULL", [SQLParameter('p_dates', 'dates', 'date[]', 'Optional[List[date]]', True)], {"from typing import List", "from datetime import date", "from typing import Optional"}),
    # Params with IN/OUT/INOUT modes (current parser ignores mode but should parse name/type)
    ("IN p_user_id int", [SQLParameter('p_user_id', 'user_id', 'int', 'int', False)], set()),
    ("OUT p_result text", [SQLParameter('p_result', 'result', 'text', 'str', False)], set()),
    ("INOUT p_counter bigint", [SQLParameter('p_counter', 'counter', 'bigint', 'int', False)], set()),
    # Mixed cases
    ("p_id int, IN p_name text DEFAULT 'Guest', p_values int[]", [
        SQLParameter('p_id', 'id', 'int', 'int', False),
        SQLParameter('p_name', 'name', 'text', 'Optional[str]', True),
        SQLParameter('p_values', 'values', 'int[]', 'List[int]', False)
    ], {"from typing import Optional", "from typing import List"}),
    # Types with precision/scale
    ("p_price numeric(10, 2)", [SQLParameter('p_price', 'price', 'numeric(10,2)', 'Decimal', False)], {"from decimal import Decimal"}),
    ("p_code character varying(50) DEFAULT 'DEFAULT'", [SQLParameter('p_code', 'code', 'character varying(50)', 'Optional[str]', True)], {"from typing import Optional"}),
]


@pytest.mark.parametrize("param_str, expected_params, expected_imports", parse_params_test_cases)
def test_parse_params(param_str: str, expected_params: List[SQLParameter], expected_imports: Set[str]):
    """Tests the _parse_params function with various input strings."""
    params, required_imports = _parse_params(param_str)

    # Check the list of SQLParameter objects
    assert params == expected_params

    # Check the set of required imports
    assert required_imports == expected_imports 


# --- Tests for _parse_column_definitions --- 

# Parameterized test cases: (col_defs_str, expected_cols, expected_imports)
parse_columns_test_cases = [
    # Empty input
    ("", [], set()),
    ("  ", [], set()),
    # Single simple column (default: optional=True)
    ("id integer", [ReturnColumn('id', 'integer', 'Optional[int]', True)], {"from typing import Optional"}),
    # Multiple simple columns
    ("name text, created_at timestamp", [
        ReturnColumn('name', 'text', 'Optional[str]', True),
        ReturnColumn('created_at', 'timestamp', 'Optional[datetime]', True),
    ], {"from typing import Optional", "from datetime import datetime"}),
    # Newline separated
    ("name text\nvalue numeric", [
        ReturnColumn('name', 'text', 'Optional[str]', True),
        ReturnColumn('value', 'numeric', 'Optional[Decimal]', True),
    ], {"from typing import Optional", "from decimal import Decimal"}),
    # NOT NULL constraint
    ("user_id uuid NOT NULL", [ReturnColumn('user_id', 'uuid', 'UUID', False)], {"from uuid import UUID"}),
    # PRIMARY KEY constraint (implies NOT NULL)
    ("item_id bigint PRIMARY KEY", [ReturnColumn('item_id', 'bigint', 'int', False)], set()),
    # Mixed nullability
    ("id int PRIMARY KEY, description text, is_active boolean NOT NULL", [
        ReturnColumn('id', 'int', 'int', False),
        ReturnColumn('description', 'text', 'Optional[str]', True),
        ReturnColumn('is_active', 'boolean', 'bool', False),
    ], {"from typing import Optional"}),
    # With comments
    ("col1 int, -- This is a comment\ncol2 text -- Another comment", [
        ReturnColumn('col1', 'int', 'Optional[int]', True),
        ReturnColumn('col2', 'text', 'Optional[str]', True),
    ], {"from typing import Optional"}),
    # Types with precision/scale
    # ("price numeric(10, 2) NOT NULL", [ReturnColumn('price', 'numeric(10, 2)', 'Decimal', False)], {"from decimal import Decimal"}), # TODO: Fix parser for comma in type
    ("code character varying(50)", [ReturnColumn('code', 'character varying(50)', 'Optional[str]', True)], {"from typing import Optional"}),
    # Array types
    ("tags text[]", [ReturnColumn('tags', 'text[]', 'Optional[List[str]]', True)], {"from typing import Optional", "from typing import List"}),
    ("scores integer[] NOT NULL", [ReturnColumn('scores', 'integer[]', 'List[int]', False)], {"from typing import List"}),
    # Constraints to ignore
    ("id serial PRIMARY KEY, name varchar UNIQUE, email text NOT NULL CHECK (email <> ''), age int DEFAULT 18", [
        ReturnColumn('id', 'serial', 'int', False),
        ReturnColumn('name', 'varchar', 'Optional[str]', True),
        ReturnColumn('email', 'text', 'str', False),
        ReturnColumn('age', 'int', 'Optional[int]', True),
    ], {"from typing import Optional"}),
    # Quoted identifiers
    ('"user Name" text, "order" int NOT NULL', [
        ReturnColumn('user Name', 'text', 'Optional[str]', True),
        ReturnColumn('order', 'int', 'int', False),
    ], {"from typing import Optional"})

]


@pytest.mark.parametrize("col_defs_str, expected_cols, expected_imports", parse_columns_test_cases)
def test_parse_column_definitions(col_defs_str: str, expected_cols: List[ReturnColumn], expected_imports: Set[str]):
    """Tests the _parse_column_definitions function."""
    cols, imports = _parse_column_definitions(col_defs_str)

    assert cols == expected_cols
    assert imports == expected_imports 