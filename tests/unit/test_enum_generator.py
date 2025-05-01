"""Unit tests for generating Python Enum classes from SQL ENUM types in sql2pyapi."""

import pytest
from typing import List, Dict, Optional, Set

from sql2pyapi.parser import SQLParser, parse_sql
from sql2pyapi.generator import generate_python_code
from sql2pyapi.sql_models import ReturnColumn, ParsedFunction


def test_generate_enum_class():
    """Test generating a Python Enum class from a SQL ENUM type."""
    # Parse a simple function that uses the enum
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
    
    # Generate Python code
    code = generate_python_code(functions, table_schema_imports, composite_types, 
                             parsed_enum_types=enum_types)
    
    # Check that the Enum class was generated correctly
    assert "from enum import Enum" in code
    assert "class StatusType(Enum):" in code
    assert "PENDING = 'pending'" in code
    assert "ACTIVE = 'active'" in code
    assert "INACTIVE = 'inactive'" in code
    assert "DELETED = 'deleted'" in code


def test_generate_multiple_enum_classes():
    """Test generating multiple Python Enum classes from SQL ENUM types."""
    # Parse a function that uses both enums
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
    
    # Generate Python code
    code = generate_python_code(functions, table_schema_imports, composite_types, 
                             parsed_enum_types=enum_types)
    
    # Check that both Enum classes were generated correctly
    assert "from enum import Enum" in code
    
    # Check StatusType enum
    assert "class StatusType(Enum):" in code
    assert "PENDING = 'pending'" in code
    assert "ACTIVE = 'active'" in code
    assert "INACTIVE = 'inactive'" in code
    assert "DELETED = 'deleted'" in code
    
    # Check UserRole enum
    assert "class UserRole(Enum):" in code
    assert "ADMIN = 'admin'" in code
    assert "MODERATOR = 'moderator'" in code
    assert "USER = 'user'" in code
    assert "GUEST = 'guest'" in code


def test_function_with_enum_parameter():
    """Test generating a function that takes an Enum parameter."""
    # Parse a function that takes an enum parameter
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
    
    # Generate Python code
    code = generate_python_code(functions, table_schema_imports, composite_types, 
                             parsed_enum_types=enum_types)
    
    # Check that the function signature includes the enum parameter
    assert "async def is_active_role(conn: AsyncConnection, role: UserRole) -> Optional[bool]:" in code


def test_function_returning_enum():
    """Test generating a function that returns an Enum type."""
    # Parse a function that returns an enum
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
    
    # Generate Python code
    code = generate_python_code(functions, table_schema_imports, composite_types, 
                             parsed_enum_types=enum_types)
    
    # Check that the function signature includes the enum return type
    assert "async def get_default_status(conn: AsyncConnection) -> StatusType:" in code


def test_function_with_table_containing_enum():
    """Test generating a function that returns a table with Enum columns."""
    # Parse a function that returns a table with enum columns
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
    
    # Generate Python code
    code = generate_python_code(functions, table_schema_imports, composite_types, 
                             parsed_enum_types=enum_types)
    
    # Check that the dataclass includes the enum fields
    assert "@dataclass" in code
    assert "class GetUsersByStatusResult:" in code
    assert "user_id: Optional[int]" in code
    assert "username: Optional[str]" in code
    assert "status: Optional[StatusType]" in code
    assert "role: Optional[UserRole]" in code
