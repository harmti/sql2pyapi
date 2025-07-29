"""Tests for enum type handling using the public API.

These tests verify that SQL enum types are correctly parsed and used in
various contexts through the public API.
"""

import pytest
from typing import List, Dict, Set, Optional

# Import the public API
from sql2pyapi.parser import parse_sql

# Import test utilities
from tests.test_utils import (
    create_test_enum,
    create_test_function,
    create_test_table,
    find_function,
    find_parameter,
    find_return_column,
    parse_test_sql
)


def test_basic_enum_parsing():
    """Test basic enum type parsing and usage."""
    # Create a test enum type
    enum_sql = create_test_enum("status_type", ["pending", "active", "inactive"])
    
    # Create a function that uses the enum as a parameter
    func_sql = create_test_function(
        "get_users_by_status", 
        "p_status status_type", 
        "integer"
    )
    
    # Parse both
    functions, _, _, enum_types = parse_test_sql(func_sql, enum_sql)
    
    # Verify the enum type was parsed
    assert "status_type" in enum_types
    assert enum_types["status_type"] == ["pending", "active", "inactive"]
    
    # Verify the parameter type
    func = find_function(functions, "get_users_by_status")
    param = find_parameter(func, "p_status")
    assert param.sql_type == "status_type"
    assert param.python_type == "StatusType"  # Should be converted to PascalCase
    
    # Verify imports
    assert "Enum" in func.required_imports


def test_schema_qualified_enum():
    """Test schema-qualified enum types."""
    # Create a schema-qualified enum type
    enum_sql = """CREATE TYPE public.color_type AS ENUM ('red', 'green', 'blue');"""
    
    # Create a function that uses the enum
    func_sql = create_test_function(
        "get_items_by_color", 
        "p_color public.color_type", 
        "integer"
    )
    
    # Parse both
    functions, _, _, enum_types = parse_test_sql(func_sql, enum_sql)
    
    # Verify the enum type was parsed
    assert "public.color_type" in enum_types
    assert enum_types["public.color_type"] == ["red", "green", "blue"]
    
    # Verify the parameter type
    func = find_function(functions, "get_items_by_color")
    param = find_parameter(func, "p_color")
    assert param.sql_type == "public.color_type"
    # The parser currently uses PascalCase with schema prefix
    assert param.python_type == "Public.colorType"
    
    # Verify imports
    assert "Enum" in func.required_imports


def test_enum_array():
    """Test enum array types."""
    # Create an enum type
    enum_sql = create_test_enum("tag_type", ["important", "urgent", "normal", "low"])
    
    # Create a function that uses the enum array
    func_sql = create_test_function(
        "get_items_by_tags", 
        "p_tags tag_type[]", 
        "integer"
    )
    
    # Parse both
    functions, _, _, enum_types = parse_test_sql(func_sql, enum_sql)
    
    # Verify the enum type was parsed
    assert "tag_type" in enum_types
    
    # Verify the parameter type
    func = find_function(functions, "get_items_by_tags")
    param = find_parameter(func, "p_tags")
    assert param.sql_type == "tag_type[]"
    # The parser now correctly treats enum arrays as List[EnumType]
    assert param.python_type == "List[TagType]"
    
    # Verify imports
    assert "List" in func.required_imports
    assert "Enum" in func.required_imports


def test_optional_enum():
    """Test optional enum types (with DEFAULT)."""
    # Create an enum type
    enum_sql = create_test_enum("priority_type", ["high", "medium", "low"])
    
    # Create a function that uses the enum with a default value
    func_sql = create_test_function(
        "get_tasks_by_priority", 
        "p_priority priority_type DEFAULT 'medium'", 
        "integer"
    )
    
    # Parse both
    functions, _, _, enum_types = parse_test_sql(func_sql, enum_sql)
    
    # Verify the parameter type
    func = find_function(functions, "get_tasks_by_priority")
    param = find_parameter(func, "p_priority")
    assert param.sql_type == "priority_type"
    assert param.python_type == "Optional[PriorityType]"  # Should be Optional[EnumType]
    assert param.is_optional == True
    
    # Verify imports
    assert "Enum" in func.required_imports
    assert "Optional" in func.required_imports


def test_enum_in_return_table():
    """Test enum types in table return columns."""
    # Create an enum type
    enum_sql = create_test_enum("status_type", ["pending", "active", "inactive"])
    
    # Create a function that returns a table with an enum column
    func_sql = create_test_function(
        "list_users_with_status", 
        "", 
        "TABLE(user_id integer, name text, status status_type)"
    )
    
    # Parse both
    functions, _, _, enum_types = parse_test_sql(func_sql, enum_sql)
    
    # Verify the function
    func = find_function(functions, "list_users_with_status")
    assert func.returns_table
    
    # Verify the enum column
    status_col = find_return_column(func, "status")
    assert status_col.sql_type == "status_type"
    # The parser currently doesn't make enum columns in TABLE returns Optional
    assert status_col.python_type == "StatusType"
    
    # Verify imports
    assert "Enum" in func.required_imports
    assert "dataclass" in func.required_imports


def test_enum_in_table_schema():
    """Test enum types in table schemas used for returns."""
    # Create an enum type
    enum_sql = create_test_enum("user_role", ["admin", "editor", "viewer"])
    
    # Create a table that uses the enum
    table_sql = create_test_table("users", """
        user_id integer PRIMARY KEY,
        name text NOT NULL,
        role user_role NOT NULL
    """)
    
    # Create a function that returns the table
    func_sql = create_test_function(
        "get_user", 
        "p_id integer", 
        "users"
    )
    
    # Parse all
    functions, table_imports, _, enum_types = parse_test_sql(func_sql, enum_sql + "\n" + table_sql)
    
    # Verify the function
    func = find_function(functions, "get_user")
    assert func.returns_table
    
    # Verify the enum column
    role_col = find_return_column(func, "role")
    assert role_col.sql_type == "user_role"
    assert role_col.python_type == "UserRole"  # NOT NULL column
    assert not role_col.is_optional
    
    # Verify imports
    assert "Enum" in func.required_imports
    assert "dataclass" in func.required_imports
