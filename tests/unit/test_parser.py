"""Unit tests for the sql2pyapi parser module."""

import pytest
from typing import Optional, Tuple, Set, List, Dict
import re
from unittest.mock import patch
import copy

# Import the module itself to access module-level constants/regex
from sql2pyapi import parser as parser_module # Alias to avoid name clash
# Import specific classes and the new top-level parse function / SQLParser class
from sql2pyapi.parser import (
    SQLParser, # Import the class
    parse_sql, # Import the top-level function if needed for other tests
    # Removed direct imports of private functions: 
    # _map_sql_to_python_type, _parse_params, _parse_column_definitions, 
    # _clean_comment_block, _find_preceding_comment, _parse_return_clause
)
# Import models from the new location
from sql2pyapi.sql_models import (
    SQLParameter,
    ReturnColumn,
    ParsedFunction, # Might be needed for asserting results of parse_sql
    TYPE_MAP, # Keep if tests assert against it directly
    PYTHON_IMPORTS # Keep if tests assert against it directly
)
# Import the new comment parser functions directly
from sql2pyapi.comment_parser import clean_comment_block, find_preceding_comment

# Define mock table schemas at module level (potentially used by other tests)
# These might need deepcopy if modified by tests
MOCK_TABLE_SCHEMAS = {
    "users": [
        ReturnColumn("id", "integer", "int", False),
        ReturnColumn("name", "text", "Optional[str]", True),
        ReturnColumn("created_at", "timestamp", "Optional[datetime]", True),
    ],
    "products": [
        ReturnColumn("product_id", "uuid", "UUID", False),
        ReturnColumn("description", "text", "Optional[str]", True),
        ReturnColumn("price", "numeric", "Decimal", False),
    ],
    "orders": [
        ReturnColumn("order_id", "bigint", "int", False),
        ReturnColumn("user_id", "integer", "int", False), # Foreign key
        ReturnColumn("order_date", "date", "date", False),
    ]
}

# Define mock table schemas needed for return clause tests *BEFORE* test cases list
# These might need deepcopy if modified by tests
MOCK_TABLE_SCHEMAS_FOR_RETURNS = {
    'users': [
        ReturnColumn(name='user_id', sql_type='uuid', python_type='UUID', is_optional=False),
        ReturnColumn(name='email', sql_type='character varying(255)', python_type='str', is_optional=False),
        ReturnColumn(name='created_at', sql_type='timestamp', python_type='Optional[datetime]', is_optional=True)
    ],
    'products': [ # Used for store.products tests too
        ReturnColumn(name='product_id', sql_type='serial', python_type='int', is_optional=False),
        ReturnColumn(name='name', sql_type='text', python_type='str', is_optional=False),
        ReturnColumn(name='description', sql_type='text', python_type='Optional[str]', is_optional=True),
        ReturnColumn(name='price', sql_type='numeric(10, 2)', python_type='Decimal', is_optional=False)
    ]
}
MOCK_TABLE_SCHEMA_IMPORTS_FOR_RETURNS = {
    'users': {"UUID", "Optional", "datetime"}, # email(str) needs no import
    'products': {"Optional", "Decimal"} # product_id(int), name(str), desc(str) need no import
}

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
    ("uuid", False, "UUID", {"UUID"}),
    ("timestamp", False, "datetime", {"datetime"}),
    ("timestamp without time zone", False, "datetime", {"datetime"}),
    ("timestamptz", False, "datetime", {"datetime"}),
    ("timestamp with time zone", False, "datetime", {"datetime"}),
    ("date", False, "date", {"date"}),
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
    ("numeric[]", False, "List[Decimal]", {"List", "Decimal"}),
    ("jsonb[]", False, "List[Dict[str, Any]]", {"List", "Dict", "Any"}),
    # Optional array types (Now expecting Optional[List[T]])
    ("integer[]", True, "Optional[List[int]]", {"List", "Optional"}),
    ("uuid[]", True, "Optional[List[UUID]]", {"List", "UUID", "Optional"}),
    # Complex type names (like varchar(N))
    ("character varying(255)", False, "str", set()),
    ("varchar(100)", False, "str", set()),
    ("numeric(10, 2)", False, "Decimal", {"Decimal"}),
    ("decimal(5, 0)", False, "Decimal", {"Decimal"}),
    ("timestamp(0) without time zone", False, "datetime", {"datetime"}),
    ("timestamp(6) with time zone", False, "datetime", {"datetime"}),
    # Optional complex types
    ("character varying(50)", True, "Optional[str]", {"Optional"}),
    ("numeric(8, 4)", True, "Optional[Decimal]", {"Optional", "Decimal"}),
]


@pytest.mark.parametrize("sql_type, is_optional, expected_py_type, expected_imports", map_type_test_cases)
def test_map_sql_to_python_type(sql_type: str, is_optional: bool, expected_py_type: str, expected_imports: Set[str]):
    """Tests the _map_sql_to_python_type method with various inputs."""
    parser_instance = SQLParser()
    py_type, imports = parser_instance._map_sql_to_python_type(sql_type, is_optional)
    assert py_type == expected_py_type
    assert imports == expected_imports

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
    ("p_count int DEFAULT 0", [SQLParameter('p_count', 'count', 'int', 'Optional[int]', True)], {"Optional"}),
    ("p_tag text DEFAULT 'hello'", [SQLParameter('p_tag', 'tag', 'text', 'Optional[str]', True)], {"Optional"}),
    ("p_active boolean DEFAULT true, p_ratio numeric DEFAULT 0.5", [
        SQLParameter('p_active', 'active', 'boolean', 'Optional[bool]', True),
        SQLParameter('p_ratio', 'ratio', 'numeric', 'Optional[Decimal]', True)
    ], {"Optional", "Decimal"}),
    # Params with complex types
    ("p_ids uuid[]", [SQLParameter('p_ids', 'ids', 'uuid[]', 'List[UUID]', False)], {"List", "UUID"}),
    ("p_data jsonb", [SQLParameter('p_data', 'data', 'jsonb', 'Dict[str, Any]', False)], {"Dict", "Any"}),
    ("p_dates date[] DEFAULT NULL", [SQLParameter('p_dates', 'dates', 'date[]', 'Optional[List[date]]', True)], {"List", "date", "Optional"}),
    # Params with IN/OUT/INOUT modes (current parser ignores mode but should parse name/type)
    ("IN p_user_id int", [SQLParameter('p_user_id', 'user_id', 'int', 'int', False)], set()),
    ("OUT p_result text", [SQLParameter('p_result', 'result', 'text', 'str', False)], set()),
    ("INOUT p_counter bigint", [SQLParameter('p_counter', 'counter', 'bigint', 'int', False)], set()),
    # Mixed cases
    ("p_id int, IN p_name text DEFAULT 'Guest', p_values int[]", [
        SQLParameter('p_id', 'id', 'int', 'int', False),
        SQLParameter('p_name', 'name', 'text', 'Optional[str]', True),
        SQLParameter('p_values', 'values', 'int[]', 'List[int]', False)
    ], {"Optional", "List"}),
    # Types with precision/scale
    ("p_price numeric(10, 2)", [SQLParameter('p_price', 'price', 'numeric(10,2)', 'Decimal', False)], {"Decimal"}),
    ("p_code character varying(50) DEFAULT 'DEFAULT'", [SQLParameter('p_code', 'code', 'character varying(50)', 'Optional[str]', True)], {"Optional"}),
]


@pytest.mark.parametrize("param_str, expected_params, expected_imports", parse_params_test_cases)
def test_parse_params(param_str: str, expected_params: List[SQLParameter], expected_imports: Set[str]):
    """Tests the _parse_params method with various input strings."""
    parser_instance = SQLParser()
    params, required_imports = parser_instance._parse_params(param_str)
    assert params == expected_params
    assert required_imports == expected_imports


# --- Tests for _parse_column_definitions ---

# Parameterized test cases: (col_defs_str, expected_cols, expected_imports)
parse_columns_test_cases = [
    # Empty input
    ("", [], set()),
    ("  ", [], set()),
    # Single simple column (default: optional=True) -> Expect Optional[T]
    ("id integer", [ReturnColumn('id', 'integer', 'Optional[int]', True)], {"Optional"}),
    # Multiple simple columns -> Expect Optional[T]
    ("name text, created_at timestamp", [
        ReturnColumn('name', 'text', 'Optional[str]', True),
        ReturnColumn('created_at', 'timestamp', 'Optional[datetime]', True),
    ], {"Optional", "datetime"}),
    # Newline separated -> Expect Optional[T]
    ("name text\nvalue numeric", [
        ReturnColumn('name', 'text', 'Optional[str]', True),
        ReturnColumn('value', 'numeric', 'Optional[Decimal]', True),
    ], {"Optional", "Decimal"}),
    # NOT NULL constraint
    ("user_id uuid NOT NULL", [ReturnColumn('user_id', 'uuid', 'UUID', False)], {"UUID"}),
    # PRIMARY KEY constraint (implies NOT NULL)
    ("item_id bigint PRIMARY KEY", [ReturnColumn('item_id', 'bigint', 'int', False)], set()),
    # Mixed nullability
    ("id int PRIMARY KEY, description text, is_active boolean NOT NULL", [
        ReturnColumn('id', 'int', 'int', False),
        ReturnColumn('description', 'text', 'Optional[str]', True),
        ReturnColumn('is_active', 'boolean', 'bool', False),
    ], {"Optional"}),
    # With comments (line comments should be removed)
    ("col1 int, -- This is a comment\ncol2 text -- Another comment", [
        ReturnColumn('col1', 'int', 'Optional[int]', True),
        ReturnColumn('col2', 'text', 'Optional[str]', True),
    ], {"Optional"}),
    # Types with precision/scale
    # ("price numeric(10, 2) NOT NULL", [ReturnColumn('price', 'numeric(10, 2)', 'Decimal', False)], {"Decimal"}), # Parser doesn't handle comma in type split well yet
    ("code character varying(50)", [ReturnColumn('code', 'character varying(50)', 'Optional[str]', True)], {"Optional"}),
    # Array types
    ("tags text[]", [ReturnColumn('tags', 'text[]', 'Optional[List[str]]', True)], {"Optional", "List"}),
    ("scores integer[] NOT NULL", [ReturnColumn('scores', 'integer[]', 'List[int]', False)], {"List"}),
    # Constraints to ignore (affecting optionality correctly)
    ("id serial PRIMARY KEY, name varchar UNIQUE, email text NOT NULL CHECK (email <> ''), age int DEFAULT 18", [
        ReturnColumn('id', 'serial', 'int', False),
        ReturnColumn('name', 'varchar', 'Optional[str]', True),
        ReturnColumn('email', 'text', 'str', False),
        ReturnColumn('age', 'int', 'Optional[int]', True),
    ], {"Optional"}),
    # Quoted identifiers -> Expect Optional[T]
    ('"user Name" text, "order" int NOT NULL', [
        ReturnColumn('user Name', 'text', 'Optional[str]', True),
        ReturnColumn('order', 'int', 'int', False),
    ], {"Optional"}),
    # Copied from previous test run failures, expecting Optional[T]
    ("id int", [ReturnColumn('id', 'int', 'Optional[int]', True)], {"Optional"}),
    ("name text NOT NULL", [ReturnColumn('name', 'text', 'str', False)], set()),
    ("product_id uuid PRIMARY KEY", [ReturnColumn('product_id', 'uuid', 'UUID', False)], {"UUID"}),
    ("col_a int, col_b varchar", [
        ReturnColumn('col_a', 'int', 'Optional[int]', True),
        ReturnColumn('col_b', 'varchar', 'Optional[str]', True)
    ], {"Optional"}),
    ("id serial PRIMARY KEY,\n  name text NOT NULL,\n  created_at timestamp", [
        ReturnColumn('id', 'serial', 'int', False),
        ReturnColumn('name', 'text', 'str', False),
        ReturnColumn('created_at', 'timestamp', 'Optional[datetime]', True)
    ], {"Optional", "datetime"}),
    ('"user ID" int, "data field" jsonb', [
        ReturnColumn('user ID', 'int', 'Optional[int]', True),
        ReturnColumn('data field', 'jsonb', 'Optional[Dict[str, Any]]', True)
    ], {"Optional", "Dict", "Any"}),
    ("status text DEFAULT 'pending' NOT NULL", [ReturnColumn('status', 'text', 'str', False)], set()),
    ("code varchar UNIQUE", [ReturnColumn('code', 'varchar', 'Optional[str]', True)], {"Optional"}),
    ("value int CHECK (value > 0)", [ReturnColumn('value', 'int', 'Optional[int]', True)], {"Optional"}),
    ("price numeric(10, 2) NOT NULL, tags text[]", [
        ReturnColumn('price', 'numeric(10, 2)', 'Decimal', False),
        ReturnColumn('tags', 'text[]', 'Optional[List[str]]', True)
    ], {"Decimal", "Optional", "List"}),
    # Empty input handled by initial check
    ("", [], set()),
    ("  ", [], set()),
    # Input with only comments (should parse to empty list now)
    ("-- id int", [], set()),
    ("/* name text */", [], set()),
    # Comment after type -> Expect Optional[T]
    ("id int -- primary key", [ReturnColumn('id', 'int', 'Optional[int]', True)], {"Optional"}),
]


@pytest.mark.parametrize("col_defs_str, expected_cols, expected_imports", parse_columns_test_cases)
def test_parse_column_definitions(col_defs_str: str, expected_cols: List[ReturnColumn], expected_imports: Set[str]):
    """Tests the _parse_column_definitions method with various input strings."""
    parser_instance = SQLParser()
    columns, required_imports = parser_instance._parse_column_definitions(col_defs_str)
    assert columns == expected_cols
    assert required_imports == expected_imports


# --- Tests for _clean_comment_block ---

# Parameterized test cases: (comment_lines, expected)
clean_comment_test_cases = [
    # Input strings are split into lists
    (["-- Just a single line"], "Just a single line"),
    (["-- Line 1", "-- Line 2"], "Line 1\nLine 2"),
    (["/* Just a single line */"], "Just a single line"),
    # Cases involving stars and internal structure
    (["/* Line 1", "Line 2 */"], "Line 1\nLine 2"),
    (["/*", "Line 1", "Line 2", "*/"], "Line 1\nLine 2"),
    (["-- Line 1", "/* Block 2 */", "-- Line 3"], "Line 1\nBlock 2\nLine 3"),
    (["/* Block 1 */", "-- Line 2"], "Block 1\nLine 2"),
    (["/* Block 1 ", "With Star */"], "Block 1\nWith Star"),
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
    (["/* Line 1", "Line 2 */"], "Line 1\nLine 2"),
    # Tabs instead of spaces - Dedent removes common leading whitespace (tab)
    (["/*\tLine 1", "\tLine 2", "*/"], "Line 1\nLine 2"),
    # Windows newlines (passed as string literals) - Dedent removes star
    (["/* Line 1\r", "Line 2 */"], "Line 1\nLine 2"),
    (["-- Line 1\r", "-- Line 2"], "Line 1\nLine 2"),
    (["/* Line 1\r", "Line 2 */"], "Line 1\nLine 2"),
    (["-- Line 1\r", "-- Line 2"], "Line 1\nLine 2"),
]


@pytest.mark.parametrize("comment_lines, expected", clean_comment_test_cases)
def test_clean_comment_block(comment_lines: List[str], expected: str):
    """Tests the clean_comment_block function from comment_parser."""
    # Call the imported function directly
    cleaned_comment = clean_comment_block(comment_lines)
    assert cleaned_comment == expected


# --- Tests for _find_preceding_comment ---

# Test cases: (lines, func_start_idx, expected_cleaned_comment)
find_comment_test_cases = [
    # No comment
    (["CREATE FUNCTION foo() ..."], 0, None),
    (["SELECT 1;", "CREATE FUNCTION foo() ..."], 1, None),
    (["", "CREATE FUNCTION foo() ..."], 1, None),
    # Single line comment (--)
    (["-- My func desc", "CREATE FUNCTION foo() ..."], 1, "My func desc"),
    # Multi-line comment (--)
    (["-- Line 1", "-- Line 2", "CREATE FUNCTION foo() ..."], 2, "Line 1\nLine 2"),
    # Single line block comment (/* */)
    (["/* My block comment */", "CREATE FUNCTION foo() ..."], 1, "My block comment"),
    # Multi-line block comment (/* */)
    (["/* Start", " * Middle", " End */", "CREATE FUNCTION foo() ..."], 3, "Start\nMiddle\nEnd"),
    # Comment with blank line separation
    (["-- Comment above", "", "CREATE FUNCTION foo() ..."], 2, None),
    (["/* Block above */", "", "CREATE FUNCTION foo() ..."], 2, None),
    # Comment immediately before
    (["SELECT 1;", "-- The real comment", "CREATE FUNCTION foo() ..."], 2, "The real comment"),
    # Indented comments
    (["  -- Indented dash", "CREATE FUNCTION foo() ..."], 1, "Indented dash"),
    (["  /* Indented block */", "CREATE FUNCTION foo() ..."], 1, "Indented block"),
    # Empty comment lines
    (["--", "CREATE FUNCTION foo() ..."], 1, ""),
    (["/**/", "CREATE FUNCTION foo() ..."], 1, ""),
    # Function at the beginning of the file
    (["-- Top comment", "CREATE FUNCTION foo() ..."], 1, "Top comment"),
    # Multiple comment blocks, only take the last one
    (["-- Old comment", "", "/* New comment */", "CREATE FUNCTION foo() ..."], 3, "New comment"),
    # Search stops at non-comment, non-blank line
    (["SELECT 1;", "-- This is the one", "SELECT 2;", "CREATE FUNCTION foo() ..."], 3, None),
]


@pytest.mark.parametrize("lines, func_start_line_idx, expected_comment", find_comment_test_cases)
def test_find_preceding_comment(lines: List[str], func_start_line_idx: int, expected_comment: Optional[str]):
    """Tests the find_preceding_comment function from comment_parser."""
    # Call the imported function directly
    comment = find_preceding_comment(lines, func_start_line_idx)
    assert comment == expected_comment


# --- Tests for _parse_return_clause --- 

# Helper function to create a mock re.Match object
def create_match(sql: str) -> Optional[re.Match]:
    # Access regex via the aliased module import
    return parser_module.FUNCTION_REGEX.search(sql)

# Parameterized test cases
# (sql_fragment, initial_imports, expected_props, expected_imports_delta)
# expected_props keys: 'return_type', 'returns_table', 'returns_record', 'returns_setof', 'return_columns', 'setof_table_name'
# expected_imports_delta: Only the *new* imports added by the return clause parser
parse_return_test_cases = [
    # Simple scalar return
    ("RETURNS integer LANGUAGE sql", set(), 
     {'return_type': 'int', 'returns_table': False, 'returns_record': False, 'returns_setof': False, 'return_columns': [], 'setof_table_name': None}, 
     set()),
    # Scalar requiring import
    ("RETURNS uuid AS $$", {"ParamImport"}, 
     {'return_type': 'UUID', 'returns_table': False, 'returns_record': False, 'returns_setof': False, 'return_columns': [], 'setof_table_name': None},
     {"UUID"}),
    # VOID return
    ("RETURNS void LANGUAGE plpgsql", set(),
     {'return_type': 'None', 'returns_table': False, 'returns_record': False, 'returns_setof': False, 'return_columns': [], 'setof_table_name': None},
     set()),
    # RECORD return
    ("RETURNS record AS $$", set(),
     {'return_type': 'Tuple', 'returns_table': False, 'returns_record': True, 'returns_setof': False, 'return_columns': [], 'setof_table_name': None},
     {"Tuple"}),
    # RETURNS TABLE
    ("RETURNS TABLE(id int, name text) LANGUAGE sql", {"Initial"}, 
     {'return_type': 'DataclassPlaceholder', 'returns_table': True, 'returns_record': False, 'returns_setof': False, 
      'return_columns': [ReturnColumn('id', 'int', 'Optional[int]', True), ReturnColumn('name', 'text', 'Optional[str]', True)], 
      'setof_table_name': None},
     {"dataclass", "Optional"}),
    # RETURNS TABLE with complex types and constraints
    ("RETURNS TABLE(user_id uuid NOT NULL, value numeric(5,2)) AS $$", set(),
     {'return_type': 'DataclassPlaceholder', 'returns_table': True, 'returns_record': False, 'returns_setof': False,
      'return_columns': [ReturnColumn('user_id', 'uuid', 'UUID', False), ReturnColumn('value', 'numeric(5, 2)', 'Optional[Decimal]', True)],
      'setof_table_name': None},
     {"dataclass", "UUID", "Optional", "Decimal"}),
    # SETOF scalar
    ("RETURNS SETOF text LANGUAGE plpgsql", set(),
     {'return_type': 'str', 'returns_table': False, 'returns_record': False, 'returns_setof': True, 'return_columns': [], 'setof_table_name': None},
     set()),
    # SETOF scalar requiring import
    ("RETURNS SETOF date AS $$", set(),
     {'return_type': 'date', 'returns_table': False, 'returns_record': False, 'returns_setof': True, 'return_columns': [], 'setof_table_name': None},
     {"date"}),
    # SETOF RECORD
    ("RETURNS SETOF record LANGUAGE sql", set(),
     {'return_type': 'Tuple', 'returns_table': False, 'returns_record': True, 'returns_setof': True, 'return_columns': [], 'setof_table_name': None},
     {"Tuple"}),
    # SETOF TABLE(...)
    ("RETURNS SETOF TABLE(col1 int, col2 uuid) AS $$", set(),
     {'return_type': 'DataclassPlaceholder', 'returns_table': True, 'returns_record': False, 'returns_setof': True,
      'return_columns': [ReturnColumn('col1', 'int', 'Optional[int]', True), ReturnColumn('col2', 'uuid', 'Optional[UUID]', True)],
      'setof_table_name': None},
     {"dataclass", "Optional", "UUID"}),
    # RETURNS table_name (using mock schema)
    ("RETURNS users LANGUAGE sql", set(),
     {'return_type': 'DataclassPlaceholder', 'returns_table': True, 'returns_record': False, 'returns_setof': False,
      'return_columns': MOCK_TABLE_SCHEMAS_FOR_RETURNS['users'],
      'setof_table_name': None},
     {"dataclass", "UUID", "Optional", "datetime"}),
    # RETURNS schema.table_name (using mock schema)
    ("RETURNS store.products AS $$", set(),
     {'return_type': 'DataclassPlaceholder', 'returns_table': True, 'returns_record': False, 'returns_setof': False,
      'return_columns': MOCK_TABLE_SCHEMAS_FOR_RETURNS['products'],
      'setof_table_name': None},
     {"dataclass", "Optional", "Decimal"}),
    # SETOF table_name (using mock schema)
    ("RETURNS SETOF users LANGUAGE sql", set(),
     {'return_type': 'DataclassPlaceholder', 'returns_table': True, 'returns_record': False, 'returns_setof': True,
      'return_columns': MOCK_TABLE_SCHEMAS_FOR_RETURNS['users'],
      'setof_table_name': 'users'},
     {"dataclass", "UUID", "Optional", "datetime"}),
    # SETOF schema.table_name (using mock schema)
    ("RETURNS SETOF store.products AS $$", set(),
     {'return_type': 'DataclassPlaceholder', 'returns_table': True, 'returns_record': False, 'returns_setof': True,
      'return_columns': MOCK_TABLE_SCHEMAS_FOR_RETURNS['products'],
      'setof_table_name': 'store.products'},
     {"dataclass", "Optional", "Decimal"}),
    # Unknown table name
    ("RETURNS non_existent_table LANGUAGE sql", set(),
     # Should map to Any scalar
     {'return_type': 'Any', 'returns_table': False, 'returns_record': False, 'returns_setof': False,
      'return_columns': [], 'setof_table_name': None},
     {"Any"}),
    # SETOF unknown table name (special case, treated as table returning Any)
    ("RETURNS SETOF widgets AS $$", set(),
     {'return_type': 'DataclassPlaceholder', 'returns_table': True, 'returns_record': False, 'returns_setof': True,
      'return_columns': [ReturnColumn(name='unknown', sql_type='widgets', python_type='Optional[Any]', is_optional=True)],
      'setof_table_name': 'widgets'},
     {"dataclass", "Any", "Optional"}),
    # Unknown type that looks like a table but isn't in schema
    ("RETURNS widgets AS $$", set(),
     # Should map to Any scalar
     {'return_type': 'Any', 'returns_table': False, 'returns_record': False, 'returns_setof': False,
      'return_columns': [], 'setof_table_name': None},
     {"Any"}),
]


@pytest.mark.parametrize("function_sql, initial_imports, expected_props, expected_imports_delta", parse_return_test_cases)
def test_parse_return_clause(function_sql: str, initial_imports: set, expected_props: dict, expected_imports_delta: set):
    """Tests the _parse_return_clause method with various RETURNS clauses."""
    # Create a mock match object
    # Need a minimal CREATE FUNCTION structure for the regex
    full_sql = f"CREATE FUNCTION test_func() {function_sql}"
    match = create_match(full_sql)
    assert match is not None, f"Regex failed to match test SQL: {full_sql}"

    # Instantiate parser and set up mock state
    parser_instance = SQLParser()
    # Deepcopy to avoid modifying module-level mocks if tests change state
    parser_instance.table_schemas = copy.deepcopy(MOCK_TABLE_SCHEMAS_FOR_RETURNS)
    parser_instance.table_schema_imports = copy.deepcopy(MOCK_TABLE_SCHEMA_IMPORTS_FOR_RETURNS)

    # Call the method on the instance
    initial_imports_copy = initial_imports.copy()
    returns_info, updated_imports = parser_instance._parse_return_clause(match, initial_imports_copy, "test_func")

    # Assert properties
    for key, expected_value in expected_props.items():
        assert returns_info.get(key) == expected_value, f"Mismatch for property '{key}'"

    # Assert imports: check only the *new* imports added
    added_imports = updated_imports - initial_imports
    assert added_imports == expected_imports_delta, "Mismatch in added imports"

# ... (Keep rest of the file, e.g., tests for parse_sql if any) ... 