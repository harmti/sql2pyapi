import pytest
from sql2pyapi.parser import parse_sql


def test_single_line_enum_parsing():
    """Test that ENUM types defined in a single line are correctly parsed."""
    sql = """
    -- Create an ENUM type for company roles for better type safety
    CREATE TYPE company_role AS ENUM ('owner', 'admin', 'member');
    """
    
    functions, columns, tables, enum_types = parse_sql(sql)
    
    # Check that the enum type was correctly parsed
    assert 'company_role' in enum_types
    assert enum_types['company_role'] == ['owner', 'admin', 'member']
    
    # Now let's test with a function that uses this enum
    sql_with_function = """
    -- Create an ENUM type for company roles for better type safety
    CREATE TYPE company_role AS ENUM ('owner', 'admin', 'member');
    
    -- Function that uses the enum type
    CREATE OR REPLACE FUNCTION get_role_description(role company_role)
    RETURNS text
    LANGUAGE sql
    AS $$
        SELECT CASE
            WHEN role = 'owner' THEN 'Company owner with full access'
            WHEN role = 'admin' THEN 'Administrator with management rights'
            WHEN role = 'member' THEN 'Regular team member'
            ELSE 'Unknown role'
        END;
    $$;
    """
    
    functions, columns, tables, enum_types = parse_sql(sql_with_function)
    
    # Check that the enum type was correctly parsed
    assert 'company_role' in enum_types
    assert enum_types['company_role'] == ['owner', 'admin', 'member']
    
    # Check that the function was correctly parsed with the enum parameter
    assert len(functions) == 1
    func = functions[0]
    assert func.sql_name == 'get_role_description'
    assert len(func.params) == 1
    assert func.params[0].sql_type == 'company_role'
    assert func.params[0].python_type == 'CompanyRole'
