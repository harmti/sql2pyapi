"""Test utilities for sql2pyapi tests.

This module provides helper functions to simplify testing through the public API.
"""

from typing import Any

from sql2pyapi import generate_python_code
from sql2pyapi.parser import parse_sql
from sql2pyapi.sql_models import ParsedFunction
from sql2pyapi.sql_models import ReturnColumn
from sql2pyapi.sql_models import SQLParameter


def create_test_function(
    name: str, params: str = "", returns: str = "void", body: str = "SELECT 1;", comment: str = ""
) -> str:
    """Create a SQL function definition for testing.

    Args:
        name: Function name
        params: Parameter string, e.g., "p_id integer, p_name text"
        returns: Return type, e.g., "integer", "TABLE(...)", "SETOF users"
        body: Function body
        comment: SQL comment to place before the function

    Returns:
        SQL function definition string
    """
    comment_str = f"-- {comment}\n" if comment else ""
    return f"""{comment_str}CREATE OR REPLACE FUNCTION {name}({params})
RETURNS {returns}
LANGUAGE sql AS $$
    {body}
$$;"""


def create_test_table(name: str, columns: str) -> str:
    """Create a SQL table definition for testing.

    Args:
        name: Table name
        columns: Column definitions, e.g., "id integer PRIMARY KEY, name text"

    Returns:
        SQL table definition string
    """
    return f"""CREATE TABLE {name} (
    {columns}
);"""


def create_test_enum(name: str, values: list[str]) -> str:
    """Create a SQL enum type definition for testing.

    Args:
        name: Enum type name
        values: List of enum values

    Returns:
        SQL enum type definition string
    """
    values_str = ", ".join([f"'{value}'" for value in values])
    return f"""CREATE TYPE {name} AS ENUM ({values_str});"""


def find_function(functions: list[ParsedFunction], name: str) -> ParsedFunction:
    """Find a function by name in a list of ParsedFunction objects.

    Args:
        functions: List of ParsedFunction objects
        name: Function name to find

    Returns:
        The found ParsedFunction object

    Raises:
        ValueError: If function not found
    """
    for func in functions:
        if func.sql_name == name:
            return func
    raise ValueError(f"Function '{name}' not found in parsed functions")


def find_parameter(function: ParsedFunction, param_name: str) -> SQLParameter:
    """Find a parameter by name in a ParsedFunction.

    Args:
        function: ParsedFunction object
        param_name: Parameter name to find (SQL name, not Python name)

    Returns:
        The found SQLParameter object

    Raises:
        ValueError: If parameter not found
    """
    for param in function.params:
        if param.name == param_name:
            return param
    raise ValueError(f"Parameter '{param_name}' not found in function '{function.sql_name}'")


def find_return_column(function: ParsedFunction, column_name: str) -> ReturnColumn:
    """Find a return column by name in a ParsedFunction.

    Args:
        function: ParsedFunction object
        column_name: Column name to find

    Returns:
        The found ReturnColumn object

    Raises:
        ValueError: If column not found or function doesn't return a table
    """
    if not function.returns_table:
        raise ValueError(f"Function '{function.sql_name}' does not return a table")

    for col in function.return_columns:
        if col.name == column_name:
            return col
    raise ValueError(f"Column '{column_name}' not found in function '{function.sql_name}' return columns")


def parse_test_sql(
    sql_content: str, schema_content: str | None = None
) -> tuple[list[ParsedFunction], dict[str, set[str]], dict[str, list[ReturnColumn]], dict[str, list[str]]]:
    """Parse SQL content using the public API.

    This is a thin wrapper around parse_sql to make tests more readable.

    Args:
        sql_content: SQL content to parse
        schema_content: Optional schema content

    Returns:
        Tuple of (parsed_functions, table_imports, composite_types, enum_types)
    """
    return parse_sql(sql_content, schema_content)


# --- Utilities for Integration Tests ---
import importlib.util
import os
import sys
from pathlib import Path

import psycopg  # type: ignore


# Ensure TEST_DB_CONN_STRING is available or defined
# It might be in a constants file or environment variable for a real setup.
# For now, define it here if not found elsewhere, but ideally it should be shared.
TEST_DB_CONN_STRING = os.environ.get(
    "TEST_DB_CONN_STRING",
    "postgresql://testuser:testpass@localhost:5433/testdb",  # Updated to match system test DSN
)


async def execute_sql_on_db(conn_str: str, sql_statements: list[str]):
    """Execute a list of SQL statements on the database."""
    async with await psycopg.AsyncConnection.connect(conn_str) as conn:
        async with conn.cursor() as cur:
            for stmt in sql_statements:
                if stmt.strip():  # Avoid executing empty statements
                    await cur.execute(stmt)
        await conn.commit()  # Commit changes


async def load_generated_api_module(tmp_path: Path, module_name: str, python_code: str) -> Any:
    """Dynamically loads a Python module from code string."""
    api_file = tmp_path / f"{module_name}.py"
    api_file.write_text(python_code)

    spec = importlib.util.spec_from_file_location(module_name, api_file)
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not create module spec for {module_name} at {api_file}")

    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module  # Add to sys.modules before execution
    try:
        spec.loader.exec_module(module)
    except Exception:
        print(f"Error executing generated module {module_name}:\n{python_code}")
        raise
    return module


async def setup_db_and_load_api(
    tmp_path: Path,
    sql_function_content: str,
    sql_schema_content: str | None = None,
    module_name: str = "temp_db_api",
    db_conn_string: str = TEST_DB_CONN_STRING,
) -> Any:
    """
    Sets up the database with schema and functions, then generates and loads the Python API.

    Args:
        tmp_path: Pytest tmp_path fixture for temporary file storage.
        sql_function_content: String containing SQL function definitions.
        sql_schema_content: Optional string with SQL schema (tables, types).
        module_name: Name for the generated Python module.
        db_conn_string: Connection string for the test database.

    Returns:
        The dynamically loaded Python module.
    """
    # 1. Execute SQL to set up DB (schema first, then functions)
    sql_to_execute = []
    if sql_schema_content:
        sql_to_execute.append(sql_schema_content)
    sql_to_execute.append(sql_function_content)
    await execute_sql_on_db(db_conn_string, sql_to_execute)

    # 2. Parse SQL and generate Python code
    functions, table_imports, composite_types, enum_types = parse_sql(
        sql_content=sql_function_content, schema_content=sql_schema_content
    )
    python_code = generate_python_code(functions, table_imports, composite_types, enum_types)

    # 3. Load the generated code as a module
    loaded_module = await load_generated_api_module(tmp_path, module_name, python_code)
    return loaded_module
