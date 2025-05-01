"""Test utilities for sql2pyapi tests.

This module provides helper functions to simplify testing through the public API.
"""

from typing import Dict, List, Optional, Set, Tuple, Any
from sql2pyapi.parser import parse_sql
from sql2pyapi.sql_models import ParsedFunction, SQLParameter, ReturnColumn


def create_test_function(name: str, params: str = "", returns: str = "void", 
                       body: str = "SELECT 1;", comment: str = "") -> str:
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


def create_test_enum(name: str, values: List[str]) -> str:
    """Create a SQL enum type definition for testing.
    
    Args:
        name: Enum type name
        values: List of enum values
        
    Returns:
        SQL enum type definition string
    """
    values_str = ", ".join([f"'{value}'" for value in values])
    return f"""CREATE TYPE {name} AS ENUM ({values_str});"""


def find_function(functions: List[ParsedFunction], name: str) -> ParsedFunction:
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


def parse_test_sql(sql_content: str, schema_content: Optional[str] = None) -> Tuple[List[ParsedFunction], Dict[str, Set[str]], Dict[str, List[ReturnColumn]], Dict[str, List[str]]]:
    """Parse SQL content using the public API.
    
    This is a thin wrapper around parse_sql to make tests more readable.
    
    Args:
        sql_content: SQL content to parse
        schema_content: Optional schema content
        
    Returns:
        Tuple of (parsed_functions, table_imports, composite_types, enum_types)
    """
    return parse_sql(sql_content, schema_content)
