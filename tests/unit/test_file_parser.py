"""Unit tests for the file parsing functionality in the parser module."""

import pytest
from pathlib import Path
from unittest import mock
import os

# Local imports
from sql2pyapi.parser import (
    parse_sql,
    ParsedFunction,
    SQLParameter,
    ReturnColumn
)
from sql2pyapi.errors import ParsingError

# Define the complex SQL content for the test
COMPLEX_FUNC_SQL = """
-- Function 1: Simple SELECT with basic params
-- No return type specified, should default? (Test this assumption?)
-- Actually, RETURNS void is expected if nothing else matches
CREATE OR REPLACE FUNCTION get_simple_data(p_id integer, p_name text DEFAULT 'default')
RETURNS void -- Let's be explicit for testing
LANGUAGE sql AS $$
    SELECT p_id, p_name; -- Body doesn't matter for parsing signature
$$;

/*
 * Function 2: Returns a known table type (users)
 * With a multi-line block comment.
 */
CREATE FUNCTION get_user_by_email(p_email varchar)
RETURNS users -- Assume 'users' table schema is defined elsewhere
LANGUAGE sql AS $$
    SELECT * FROM users WHERE email = p_email;
$$;

-- Function 3: Returns SETOF a known table type (products)
CREATE FUNCTION list_all_products()
RETURNS SETOF products -- Assume 'products' table schema is defined elsewhere
LANGUAGE plpgsql AS $$
BEGIN
    RETURN QUERY SELECT * FROM products;
END;
$$;

-- Function 4: Returns an explicit TABLE definition
-- Includes various types and nullability
CREATE FUNCTION get_order_summary(order_id bigint)
RETURNS TABLE(
    item_id uuid NOT NULL,
    description text, -- Nullable
    quantity integer NOT NULL,
    price numeric(10, 2)
)
LANGUAGE sql STABLE AS $$
    SELECT oi.item_uuid, p.description, oi.qty, p.price
    FROM order_items oi JOIN products p ON oi.product_id = p.product_id
    WHERE oi.order_ref = order_id;
$$;

-- Function 5: No preceding comment, returns scalar
CREATE FUNCTION calculate_total(price numeric, quantity int)
RETURNS numeric
LANGUAGE sql IMMUTABLE AS $$
    SELECT price * quantity;
$$;

-- Function 6: With INOUT parameter (should be parsed like IN)
CREATE FUNCTION update_counter(INOUT p_count bigint)
RETURNS bigint
LANGUAGE sql AS $$
    SELECT p_count + 1;
$$;

-- Function 7: Returns SETOF scalar (uuid)
CREATE OR REPLACE FUNCTION get_all_user_ids()
RETURNS SETOF uuid
AS $$ SELECT user_id FROM users; $$ LANGUAGE sql;

-- Function 8: Returns unknown table (should result in Any/dataclass placeholder)
CREATE FUNCTION get_widget(widget_id int)
RETURNS widgets -- Assume 'widgets' is NOT defined in schema
LANGUAGE sql AS $$ SELECT * FROM widgets_table WHERE id = widget_id; $$;

-- Function 9: Schema-qualified return type (public.orders)
CREATE FUNCTION get_order_details(p_order_id bigint)
RETURNS public.orders -- Assume 'public.orders' is defined
LANGUAGE sql AS $$ SELECT * FROM public.orders WHERE order_id = p_order_id; $$;

-- Function 10: SETOF schema-qualified return (public.orders)
CREATE FUNCTION list_recent_orders(days_back int)
RETURNS SETOF public.orders -- Assume 'public.orders' is defined
LANGUAGE sql AS $$ SELECT * FROM public.orders WHERE order_date > now() - (days_back * interval '1 day'); $$;
"""

# Define the schema SQL content for the test
SCHEMA_SQL = """
-- Define schemas used by the functions

CREATE TABLE users (
    user_id uuid PRIMARY KEY, -- Implicitly NOT NULL
    email character varying(255) UNIQUE NOT NULL,
    created_at timestamp DEFAULT now() -- Nullable
);

CREATE TABLE products (
   product_id serial PRIMARY KEY, -- Implicitly NOT NULL
   name text NOT NULL,
   description text, -- Nullable
   price numeric(10, 2) NOT NULL
);

-- Schema-qualified table
CREATE TABLE public.orders (
    order_id bigint PRIMARY KEY,
    user_id uuid REFERENCES users(user_id), -- Nullable FK
    order_date date NOT NULL,
    total_amount numeric(12, 2)
);

-- This table is intentionally NOT defined: widgets
"""

# Placeholder test
def test_placeholder():
    """A placeholder test to ensure pytest discovery."""
    assert True

# --- Test for parse_sql_file ---
# Rename test to reflect it tests parse_sql with file content
def test_parse_sql_with_complex_file_content(tmp_path):
    """
    Tests parse_sql using content read from a complex SQL file and schema file.
    Verifies parsing of various function definitions, comments, return types,
    and table schema integration.
    """
    # Create temporary files
    sql_file = tmp_path / "functions.sql"
    schema_file = tmp_path / "schema.sql"

    # Write content to files
    sql_file.write_text(COMPLEX_FUNC_SQL)
    schema_file.write_text(SCHEMA_SQL)

    # --- Read content from files ---
    sql_content = sql_file.read_text()
    schema_content = schema_file.read_text()

    # --- Call the function under test ---
    # Call parse_sql with the read content
    parsed_functions, table_imports, composite_types = parse_sql(sql_content, schema_content)

    # --- Assertions ---
    assert len(parsed_functions) == 10, f"Expected 10 functions, found {len(parsed_functions)}"

    # --- Verify individual functions ---

    # Function 1: get_simple_data
    f1 = next((f for f in parsed_functions if f.sql_name == 'get_simple_data'), None)
    assert f1 is not None
    assert f1.python_name == 'get_simple_data'
    assert f1.sql_comment == "Function 1: Simple SELECT with basic params\nNo return type specified, should default? (Test this assumption?)\nActually, RETURNS void is expected if nothing else matches"
    assert len(f1.params) == 2
    assert f1.params[0] == SQLParameter(name='p_id', python_name='id', sql_type='integer', python_type='int', is_optional=False)
    assert f1.params[1] == SQLParameter(name='p_name', python_name='name', sql_type='text', python_type='Optional[str]', is_optional=True)
    assert f1.return_type == 'None' # Explicitly returns void
    assert not f1.returns_table
    assert not f1.returns_setof
    assert 'Optional' in f1.required_imports

    # Function 2: get_user_by_email
    f2 = next((f for f in parsed_functions if f.sql_name == 'get_user_by_email'), None)
    assert f2 is not None
    assert f2.python_name == 'get_user_by_email'
    assert f2.sql_comment == "Function 2: Returns a known table type (users)\nWith a multi-line block comment."
    assert len(f2.params) == 1
    assert f2.params[0] == SQLParameter(name='p_email', python_name='email', sql_type='varchar', python_type='str', is_optional=False)
    assert f2.returns_table # Returns 'users' table
    assert not f2.returns_setof
    # Check return_type based on table schema (generator handles Optional[Dataclass])
    assert f2.return_type == 'Optional[GetUserByEmailResult]' # Expect generated name
    assert len(f2.return_columns) == 3 # From users schema
    assert f2.return_columns[0].name == 'user_id' and f2.return_columns[0].python_type == 'UUID' and not f2.return_columns[0].is_optional
    assert f2.return_columns[1].name == 'email' and f2.return_columns[1].python_type == 'str' and not f2.return_columns[1].is_optional # UNIQUE NOT NULL
    assert f2.return_columns[2].name == 'created_at' and f2.return_columns[2].python_type == 'Optional[datetime]' and f2.return_columns[2].is_optional
    assert 'dataclass' in f2.required_imports
    assert 'Optional' in f2.required_imports
    assert 'UUID' in f2.required_imports
    assert 'datetime' in f2.required_imports

    # Function 3: list_all_products
    f3 = next((f for f in parsed_functions if f.sql_name == 'list_all_products'), None)
    assert f3 is not None
    assert f3.python_name == 'list_all_products'
    assert f3.sql_comment == "Function 3: Returns SETOF a known table type (products)"
    assert len(f3.params) == 0
    assert f3.returns_table # Returns SETOF 'products'
    assert f3.returns_setof
    assert f3.setof_table_name == 'products'
    assert f3.return_type == 'List[Products]' # Expect generated name
    assert len(f3.return_columns) == 4 # From products schema
    assert f3.return_columns[0].name == 'product_id' and f3.return_columns[0].python_type == 'int' and not f3.return_columns[0].is_optional
    assert f3.return_columns[3].name == 'price' and f3.return_columns[3].python_type == 'Decimal' and not f3.return_columns[3].is_optional
    assert 'dataclass' in f3.required_imports
    assert 'List' in f3.required_imports
    assert 'Optional' in f3.required_imports # From description column
    assert 'Decimal' in f3.required_imports

    # Function 4: get_order_summary
    f4 = next((f for f in parsed_functions if f.sql_name == 'get_order_summary'), None)
    assert f4 is not None
    assert f4.python_name == 'get_order_summary'
    assert f4.sql_comment == "Function 4: Returns an explicit TABLE definition\nIncludes various types and nullability"
    assert len(f4.params) == 1
    assert f4.params[0] == SQLParameter(name='order_id', python_name='order_id', sql_type='bigint', python_type='int', is_optional=False)
    assert f4.returns_table # Explicit RETURNS TABLE
    assert f4.returns_setof # REVERTED: RETURNS TABLE implies SETOF
    assert f4.return_type == 'List[GetOrderSummaryResult]' # REVERTED: SETOF -> List
    assert len(f4.return_columns) == 4
    assert f4.return_columns[0] == ReturnColumn(name='item_id', sql_type='uuid', python_type='UUID', is_optional=False) # Keep SQL NOT NULL
    assert f4.return_columns[1] == ReturnColumn(name='description', sql_type='text', python_type='Optional[str]', is_optional=True) # Keep SQL Nullable
    assert f4.return_columns[2] == ReturnColumn(name='quantity', sql_type='integer', python_type='int', is_optional=False) # Keep SQL NOT NULL
    assert f4.return_columns[3] == ReturnColumn(name='price', sql_type='numeric(10, 2)', python_type='Optional[Decimal]', is_optional=True) # Keep SQL Nullable
    assert 'dataclass' in f4.required_imports
    assert 'Optional' in f4.required_imports
    assert 'UUID' in f4.required_imports
    assert 'Decimal' in f4.required_imports

    # Function 5: calculate_total
    f5 = next((f for f in parsed_functions if f.sql_name == 'calculate_total'), None)
    assert f5 is not None
    assert f5.python_name == 'calculate_total'
    assert f5.sql_comment == "Function 5: No preceding comment, returns scalar"
    assert len(f5.params) == 2
    assert f5.params[0] == SQLParameter(name='price', python_name='price', sql_type='numeric', python_type='Decimal', is_optional=False)
    assert f5.params[1] == SQLParameter(name='quantity', python_name='quantity', sql_type='int', python_type='int', is_optional=False)
    assert f5.return_type == 'Optional[Decimal]' # Scalar return wrapped in Optional
    assert not f5.returns_table
    assert not f5.returns_setof
    assert 'Decimal' in f5.required_imports
    assert 'Optional' in f5.required_imports

    # Function 6: update_counter
    f6 = next((f for f in parsed_functions if f.sql_name == 'update_counter'), None)
    assert f6 is not None
    assert f6.python_name == 'update_counter'
    assert f6.sql_comment == "Function 6: With INOUT parameter (should be parsed like IN)"
    assert len(f6.params) == 1
    assert f6.params[0] == SQLParameter(name='p_count', python_name='count', sql_type='bigint', python_type='int', is_optional=False) # INOUT treated as IN
    assert f6.return_type == 'Optional[int]' # Returns bigint -> Optional[int]
    assert not f6.returns_table
    assert not f6.returns_setof
    assert 'Optional' in f6.required_imports

    # Function 7: get_all_user_ids
    f7 = next((f for f in parsed_functions if f.sql_name == 'get_all_user_ids'), None)
    assert f7 is not None
    assert f7.python_name == 'get_all_user_ids'
    assert f7.sql_comment == "Function 7: Returns SETOF scalar (uuid)"
    assert len(f7.params) == 0
    assert f7.return_type == 'List[UUID]' # SETOF uuid -> List[UUID]
    assert not f7.returns_table
    assert not f7.returns_record
    assert f7.returns_setof
    assert 'List' in f7.required_imports
    assert 'UUID' in f7.required_imports

    # Function 8: get_widget (unknown table)
    f8 = next((f for f in parsed_functions if f.sql_name == 'get_widget'), None)
    assert f8 is not None
    assert f8.python_name == 'get_widget'
    assert f8.sql_comment == "Function 8: Returns unknown table (should result in Any/dataclass placeholder)"
    assert len(f8.params) == 1
    assert f8.params[0] == SQLParameter(name='widget_id', python_name='widget_id', sql_type='int', python_type='int', is_optional=False)
    assert f8.return_type == 'Optional[Any]' # Unknown table maps to Optional[Any]
    assert not f8.returns_table # Treated as scalar Any because table schema unknown
    assert not f8.returns_setof
    assert 'Any' in f8.required_imports
    assert 'Optional' in f8.required_imports

    # Function 9: get_order_details (schema-qualified)
    f9 = next((f for f in parsed_functions if f.sql_name == 'get_order_details'), None)
    assert f9 is not None
    assert f9.python_name == 'get_order_details'
    assert f9.sql_comment == "Function 9: Schema-qualified return type (public.orders)"
    assert len(f9.params) == 1
    assert f9.params[0] == SQLParameter(name='p_order_id', python_name='order_id', sql_type='bigint', python_type='int', is_optional=False)
    assert f9.returns_table # Returns 'public.orders' table
    assert not f9.returns_setof
    assert f9.return_type == 'Optional[GetOrderDetailsResult]' # Expect generated name
    assert len(f9.return_columns) == 4 # From public.orders schema
    assert f9.return_columns[0].name == 'order_id' and not f9.return_columns[0].is_optional
    assert f9.return_columns[1].name == 'user_id' and f9.return_columns[1].is_optional # FK allows null
    assert f9.return_columns[2].name == 'order_date' and not f9.return_columns[2].is_optional
    assert 'dataclass' in f9.required_imports
    assert 'Optional' in f9.required_imports
    assert 'UUID' in f9.required_imports # From user_id
    assert 'date' in f9.required_imports # From order_date
    assert 'Decimal' in f9.required_imports # From total_amount

    # Function 10: list_recent_orders (SETOF schema-qualified)
    f10 = next((f for f in parsed_functions if f.sql_name == 'list_recent_orders'), None)
    assert f10 is not None
    assert f10.python_name == 'list_recent_orders'
    assert f10.sql_comment == "Function 10: SETOF schema-qualified return (public.orders)"
    assert len(f10.params) == 1
    assert f10.params[0] == SQLParameter(name='days_back', python_name='days_back', sql_type='int', python_type='int', is_optional=False)
    assert f10.returns_table # Returns SETOF 'public.orders'
    assert f10.returns_setof
    assert f10.setof_table_name == 'public.orders'
    assert f10.return_type == 'List[Orders]' # Expect generated name based on table
    assert len(f10.return_columns) == 4 # From public.orders schema
    assert 'dataclass' in f10.required_imports
    assert 'List' in f10.required_imports
    assert 'Optional' in f10.required_imports # From user_id, total_amount
    assert 'UUID' in f10.required_imports
    assert 'date' in f10.required_imports
    assert 'Decimal' in f10.required_imports

    # --- Verify table_imports (contains imports needed for dataclasses) ---
    # This should contain imports derived ONLY from the CREATE TABLE statements
    assert 'users' in table_imports
    assert table_imports['users'] == {'UUID', 'Optional', 'datetime'} # email is varchar->str (no import), created_at is timestamp

    assert 'products' in table_imports
    assert table_imports['products'] == {'Optional', 'Decimal'} # name/desc are text (no import), price is numeric

    assert 'public.orders' in table_imports
    assert table_imports['public.orders'] == {'UUID', 'Optional', 'date', 'Decimal'} # user_id, order_date, total_amount nullable? No, user_id is, total_amount is.

    # Check normalized name 'orders' also points to the same imports
    assert 'orders' in table_imports
    assert table_imports['orders'] == table_imports['public.orders']

    assert 'widgets' not in table_imports # Was not defined

    # Verify total number of keys (normalized + qualified if different)
    assert len(table_imports) == 4
