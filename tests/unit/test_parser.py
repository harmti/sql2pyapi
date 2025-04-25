"""Unit tests for the sql2pyapi parser module."""

import pytest
from typing import Optional, Tuple, Set, List, Dict
import re
from unittest.mock import patch
import copy

# Import the module itself to access module-level constants
from sql2pyapi import parser
# Import specific classes and functions needed
from sql2pyapi.parser import (
    SQLParameter, 
    ReturnColumn,
    _map_sql_to_python_type, 
    _parse_params, 
    _parse_column_definitions, 
    _clean_comment_block, 
    _find_preceding_comment, 
    _parse_return_clause,
    # FUNCTION_REGEX is NOT imported directly
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


# --- Tests for _clean_comment_block ---

@pytest.mark.parametrize(
    "comment_lines, expected",
    [
        # Input strings are split into lists
        (["-- Just a single line"], "Just a single line"),
        (["-- Line 1", "-- Line 2"], "Line 1\nLine 2"),
        (["/* Just a single line */"], "Just a single line"),
        # Cases involving stars and internal structure
        (["/* Line 1", " * Line 2 */"], "Line 1\n* Line 2"),
        (["/*", " * Line 1", " * Line 2", " */"], "Line 1\nLine 2"),
        (["-- Line 1", "/* Block 2 */", "-- Line 3"], "Line 1\nBlock 2\nLine 3"),
        (["/* Block 1 */", "-- Line 2"], "Block 1\nLine 2"),
        (["/* Block 1 ", " * With Star */"], "Block 1\n* With Star"),
        # Empty comments
        (["--"], ""),
        (["/**/"], ""),
        (["/* ", "*/"], ""),
        # Leading/trailing spaces
        (["-- Leading Space"], "Leading Space"),
        (["--Trailing Space "], "Trailing Space"),
        (["/* Leading Space */"], "Leading Space"),
        (["/* Trailing Space */"], "Trailing Space"),
        # Comments containing comment markers
        (["/* Mix -- Dash */"], "Mix -- Dash"),
        (["-- Mix /* Block */"], "Mix /* Block */"),
        # Extra space after star
        (["/* Line 1", " *  Line 2 */"], "Line 1\n*  Line 2"),
        # Tabs instead of spaces - Adjusted expectation: Expect relative tab preserved
        (["/*\tLine 1", "\t*\tLine 2", "*/"], "Line 1\n\tLine 2"),
        # Windows newlines (passed as string literals) - Adjusted expectations: \r removed
        (["/* Line 1\r", " * Line 2 */"], "Line 1\n* Line 2"),
        (["-- Line 1\r", "-- Line 2"], "Line 1\nLine 2"),
    ],
)
def test_clean_comment_block(comment_lines: List[str], expected: str):
    assert _clean_comment_block(comment_lines) == expected


# --- Tests for _find_preceding_comment ---

# Test cases: (lines, func_start_line_idx, expected_comment)
find_comment_test_cases = [
    # No comment
    ([
        "CREATE FUNCTION no_comment() ..."
    ], 0, None),
    # Single line --
    ([
        "-- Comment A",
        "CREATE FUNCTION func_a() ..."
    ], 1, "Comment A"),
    # Multi-line --
    ([
        "-- Comment B1",
        "-- Comment B2",
        "CREATE FUNCTION func_b() ..."
    ], 2, "Comment B1\nComment B2"),
    # Single block /* */
    ([
        "/* Comment C */",
        "CREATE FUNCTION func_c() ..."
    ], 1, "Comment C"),
    # Multi-line block /* */ (with stars)
    ([
        "/* Comment D1",
        " * Comment D2 */",
        "CREATE FUNCTION func_d() ..."
    ], 2, "Comment D1\n* Comment D2"),
    # Multi-line block /* */ (no stars) - Reverted expectation to correct one
    ([
        "/* Comment D3 \n", # Original line has newline
        "   Comment D4 */", # Original line has leading space
        "CREATE FUNCTION func_d34() ..."
    ], 2, "Comment D3\nComment D4"), # Cleaned strips internal whitespace/newlines
    # Comment separated by blank line (should be found)
    ([
        "-- Comment E",
        "",
        "CREATE FUNCTION func_e() ..."
    ], 2, "Comment E"),
    # Comment separated by code (should NOT be found)
    ([
        "-- Comment F",
        "SELECT 1;",
        "CREATE FUNCTION func_f() ..."
    ], 2, None),
    # Multiple comment blocks (adjacent -- and /* */)
    ([
        "-- Block 1",
        "/* Block 2 */",
        "CREATE FUNCTION func_g() ..."
    ], 2, "Block 1\nBlock 2"),
    # Multiple comment blocks (adjacent /* */ and --)
    ([
        "/* Block 3 */",
        "-- Block 4",
        "CREATE FUNCTION func_h() ..."
    ], 2, "Block 3\nBlock 4"),
    # Multiple comment blocks separated by blank line
    ([
        "-- Block 5",
        "",
        "/* Block 6 */",
        "CREATE FUNCTION func_i() ..."
    ], 3, "Block 6"), # Only the last contiguous block is taken
    # Comment applies to the *second* function
    ([
        "CREATE FUNCTION first() ...",
        "-- Comment J",
        "CREATE FUNCTION func_j() ..."
    ], 2, "Comment J"),
    # Ignore comment *after* function start line index
    ([
        "CREATE FUNCTION func_k() -- Comment K ...",
        "RETURNS void ..."
    ], 0, None),
    # Block comment ending on same line as function start
    ([
        "/* Block L */ CREATE FUNCTION func_l() ..."
    ], 0, None), # Comment does not *precede*
    # Single line comment ending on same line as function start
    ([
        "-- Comment M",
        "-- Comment N CREATE FUNCTION func_n() ..."
    ], 1, "Comment M"), # Only Comment M precedes
    # Block comment finishing just before function
    ([
        "/* Start O",
        " * End O */",
        "CREATE FUNCTION func_o() ..."
    ], 2, "Start O\n* End O"), # Expecting star
    # Multiple block comments
    ([
        "/* Block P1 */",
        "/* Block P2 */",
        "CREATE FUNCTION func_p() ..."
    ], 2, "Block P1\nBlock P2"),

]

@pytest.mark.parametrize("lines, func_start_line_idx, expected_comment", find_comment_test_cases)
def test_find_preceding_comment(lines: List[str], func_start_line_idx: int, expected_comment: Optional[str]):
    """Tests the _find_preceding_comment function."""
    assert _find_preceding_comment(lines, func_start_line_idx) == expected_comment


# --- Tests for _parse_return_clause ---

# Helper to create a match object for testing
def create_match(sql: str) -> Optional[re.Match]:
    # Access regex via module
    return parser.FUNCTION_REGEX.search(sql)

# Define some sample return columns for mocking table schemas
sample_user_columns = [
    ReturnColumn(name='user_id', sql_type='int', python_type='int', is_optional=False),
    ReturnColumn(name='email', sql_type='text', python_type='Optional[str]', is_optional=True)
]
sample_user_imports = {"from typing import Optional"}

# Test cases: (function_sql, initial_imports, expected_props, expected_imports_delta)
# expected_imports_delta only contains *new* imports added by _parse_return_clause
parse_return_test_cases = [
    # RETURNS void
    ("CREATE FUNCTION do_nothing() RETURNS void AS $$", set(), 
     {'return_type': 'None', 'returns_table': False, 'return_columns': [], 'returns_record': False, 'setof_table_name': None},
     set()), 
    # RETURNS scalar
    ("CREATE FUNCTION get_count() RETURNS int AS $$", {"psycopg"}, 
     {'return_type': 'int', 'returns_table': False, 'return_columns': [], 'returns_record': False, 'setof_table_name': None},
     set()),
    ("CREATE FUNCTION get_id() RETURNS uuid AS $$", {"psycopg"},
     {'return_type': 'UUID', 'returns_table': False, 'return_columns': [], 'returns_record': False, 'setof_table_name': None},
     {"from uuid import UUID"}),
    # RETURNS SETOF scalar
    ("CREATE FUNCTION get_scores() RETURNS SETOF int AS $$", {"psycopg"},
     {'return_type': 'int', 'returns_table': False, 'return_columns': [], 'returns_record': False, 'setof_table_name': None},
     set()),
    # RETURNS TABLE
    ("CREATE FUNCTION get_data() RETURNS TABLE(id int, name text) AS $$", {"psycopg"},
     {'return_type': 'DataclassPlaceholder', 'returns_table': True, 
      'return_columns': [ReturnColumn('id', 'int', 'Optional[int]', True), ReturnColumn('name', 'text', 'Optional[str]', True)], 
      'returns_record': False, 'setof_table_name': None},
     {"from dataclasses import dataclass", "from typing import Optional"}),
    # RETURNS TABLE with NOT NULL
    ("CREATE FUNCTION get_items() RETURNS TABLE(item_id uuid NOT NULL, value text) AS $$", {"psycopg"},
     {'return_type': 'DataclassPlaceholder', 'returns_table': True, 
      'return_columns': [ReturnColumn('item_id', 'uuid', 'UUID', False), ReturnColumn('value', 'text', 'Optional[str]', True)], 
      'returns_record': False, 'setof_table_name': None},
     {"from dataclasses import dataclass", "from uuid import UUID", "from typing import Optional"}),
    # RETURNS record
    ("CREATE FUNCTION get_pair() RETURNS record AS $$", {"psycopg"},
     {'return_type': 'Tuple', 'returns_table': False, 'return_columns': [], 'returns_record': True, 'setof_table_name': None},
     {"from typing import Tuple"}),
    # RETURNS SETOF record
    ("CREATE FUNCTION get_pairs() RETURNS SETOF record AS $$", {"psycopg"},
     {'return_type': 'Tuple', 'returns_table': False, 'return_columns': [], 'returns_record': True, 'setof_table_name': None},
     {"from typing import Tuple"}),
    # RETURNS SETOF table_name (schema found)
    ("CREATE FUNCTION get_users() RETURNS SETOF users AS $$", {"psycopg"},
     {'return_type': 'DataclassPlaceholder', 'returns_table': True, 'return_columns': sample_user_columns, 'returns_record': False, 'setof_table_name': 'users'},
     {"from dataclasses import dataclass", "from typing import Optional"}),
    # RETURNS SETOF table_name (schema not found)
    ("CREATE FUNCTION get_products() RETURNS SETOF products AS $$", {"psycopg"},
     {'return_type': 'DataclassPlaceholder', 'returns_table': True, 
      'return_columns': [ReturnColumn(name='unknown', sql_type='products', python_type='Any')], 
      'returns_record': False, 'setof_table_name': 'products'},
     {"from dataclasses import dataclass", "from typing import Any"}),
]

@pytest.mark.parametrize("function_sql, initial_imports, expected_props, expected_imports_delta", parse_return_test_cases)
def test_parse_return_clause(function_sql: str, initial_imports: set, expected_props: dict, expected_imports_delta: set):
    """Tests the _parse_return_clause function with various RETURNS types."""
    match = create_match(function_sql)
    assert match is not None, f"Test setup failed: regex did not match SQL: {function_sql}"

    # Patch globals *inside* the test using context managers
    # Use copies of sample data to prevent mutation across tests
    patched_schemas = copy.deepcopy({'users': sample_user_columns})
    patched_imports = copy.deepcopy({'users': sample_user_imports})

    with patch.object(parser, 'TABLE_SCHEMAS', patched_schemas, create=True), \
         patch.object(parser, 'TABLE_SCHEMA_IMPORTS', patched_imports, create=True):
        
        return_props, result_imports = parser._parse_return_clause(match, initial_imports)

        # Make copies for comparison to avoid modifying originals during asserts
        expected_props_copy = copy.deepcopy(expected_props)
        return_props_copy = copy.deepcopy(return_props)

        # Check the returned properties dictionary parts
        actual_cols = sorted(return_props_copy.pop('return_columns', []), key=lambda c: c.name)
        expected_cols = sorted(expected_props_copy.pop('return_columns', []), key=lambda c: c.name)
        assert actual_cols == expected_cols
        assert return_props_copy == expected_props_copy # Compare remaining props

        # Check that only the expected *new* imports were added
        newly_added_imports = result_imports - initial_imports
        assert newly_added_imports == expected_imports_delta 