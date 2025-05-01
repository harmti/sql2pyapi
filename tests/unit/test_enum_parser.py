"""Unit tests for parsing SQL ENUM types in sql2pyapi."""

import pytest
from typing import List, Dict, Optional, Set

from sql2pyapi.parser import SQLParser, parse_sql
from sql2pyapi.sql_models import ReturnColumn, ParsedFunction


def test_parse_enum_type_definition():
    """Test that the parser correctly identifies and parses ENUM type definitions."""
    sql = """
    CREATE TYPE status_type AS ENUM (
        'pending',
        'active',
        'inactive',
        'deleted'
    );
    """
    
    parser = SQLParser()
    parser._parse_enum_types(sql)
    
    # Check that the enum type was parsed correctly
    assert "status_type" in parser.enum_types
    enum_values = parser.enum_types["status_type"]
    assert enum_values == ["pending", "active", "inactive", "deleted"]


def test_parse_multiple_enum_types():
    """Test parsing multiple ENUM type definitions."""
    sql = """
    CREATE TYPE status_type AS ENUM (
        'pending',
        'active',
        'inactive',
        'deleted'
    );
    
    CREATE TYPE user_role AS ENUM (
        'admin',
        'moderator',
        'user',
        'guest'
    );
    """
    
    parser = SQLParser()
    parser._parse_enum_types(sql)
    
    # Check that both enum types were parsed correctly
    assert "status_type" in parser.enum_types
    assert "user_role" in parser.enum_types
    
    assert parser.enum_types["status_type"] == ["pending", "active", "inactive", "deleted"]
    assert parser.enum_types["user_role"] == ["admin", "moderator", "user", "guest"]


def test_map_enum_to_python_type():
    """Test mapping SQL ENUM types to Python Enum types."""
    parser = SQLParser()
    
    # Setup enum types in the parser
    parser.enum_types = {
        "status_type": ["pending", "active", "inactive", "deleted"],
        "user_role": ["admin", "moderator", "user", "guest"]
    }
    
    # Test mapping of enum types
    py_type, imports = parser._map_sql_to_python_type("status_type")
    assert py_type == "StatusType"
    assert "Enum" in imports
    
    # Test with optional flag
    py_type, imports = parser._map_sql_to_python_type("user_role", is_optional=True)
    assert py_type == "Optional[UserRole]"
    assert "Enum" in imports
    assert "Optional" in imports


def test_parse_function_with_enum_parameter():
    """Test parsing a function that takes an ENUM parameter."""
    sql = """
    CREATE TYPE user_role AS ENUM (
        'admin',
        'moderator',
        'user',
        'guest'
    );
    
    CREATE OR REPLACE FUNCTION is_active_role(p_role user_role)
    RETURNS boolean
    LANGUAGE sql
    AS $$
        SELECT p_role IN ('admin', 'moderator');
    $$;
    """
    
    functions, table_schema_imports, composite_types, enum_types = parse_sql(sql)
    
    # Check that the function was parsed correctly
    assert len(functions) == 1
    func = functions[0]
    
    # Check parameter type
    assert len(func.params) == 1
    param = func.params[0]
    assert param.name == "p_role"
    assert param.sql_type == "user_role"
    assert param.python_type == "UserRole"
    
    # Check required imports
    assert "Enum" in func.required_imports


def test_parse_function_returning_enum():
    """Test parsing a function that returns an ENUM type."""
    sql = """
    CREATE TYPE status_type AS ENUM (
        'pending',
        'active',
        'inactive',
        'deleted'
    );
    
    CREATE OR REPLACE FUNCTION get_default_status()
    RETURNS status_type
    LANGUAGE sql
    AS $$
        SELECT 'active'::status_type;
    $$;
    """
    
    functions, table_schema_imports, composite_types, enum_types = parse_sql(sql)
    
    # Check that the function was parsed correctly
    assert len(functions) == 1
    func = functions[0]
    
    # Check return type
    assert func.return_type == "StatusType"
    assert func.returns_table is False
    
    # Check required imports
    assert "Enum" in func.required_imports


def test_parse_function_with_table_containing_enum():
    """Test parsing a function that returns a table with ENUM columns."""
    sql = """
    CREATE TYPE status_type AS ENUM (
        'pending',
        'active',
        'inactive',
        'deleted'
    );
    
    CREATE TYPE user_role AS ENUM (
        'admin',
        'moderator',
        'user',
        'guest'
    );
    
    CREATE OR REPLACE FUNCTION get_users_by_status(p_status status_type)
    RETURNS TABLE (
        user_id integer,
        username text,
        status status_type,
        role user_role
    )
    LANGUAGE sql
    AS $$
        SELECT 1, 'admin_user', 'active'::status_type, 'admin'::user_role;
    $$;
    """
    
    functions, table_schema_imports, composite_types, enum_types = parse_sql(sql)
    
    # Check that the function was parsed correctly
    assert len(functions) == 1
    func = functions[0]
    
    # Check parameter type
    assert len(func.params) == 1
    param = func.params[0]
    assert param.sql_type == "status_type"
    assert param.python_type == "StatusType"
    
    # Check return columns
    assert func.returns_table is True
    assert len(func.return_columns) == 4
    
    # Check enum columns in return table
    status_col = next(col for col in func.return_columns if col.name == "status")
    role_col = next(col for col in func.return_columns if col.name == "role")
    
    assert status_col.sql_type == "status_type"
    assert status_col.python_type == "StatusType"
    
    assert role_col.sql_type == "user_role"
    assert role_col.python_type == "UserRole"
    
    # Check required imports
    assert "Enum" in func.required_imports
