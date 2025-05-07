import pytest
from sql2pyapi.parser import parse_sql, SQLParser
from sql2pyapi.generator import generate_python_code
from sql2pyapi.errors import MissingSchemaError

# SQL definitions based on the bug report (matches temp/ files closely)
SCHEMA_SQL = """
CREATE TABLE users (id UUID PRIMARY KEY); -- Minimal, for REFERENCES
CREATE TABLE companies (id UUID PRIMARY KEY); -- Minimal, for REFERENCES

CREATE TYPE company_role AS ENUM ('owner', 'admin', 'member');

CREATE TABLE company_members (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL, 
    company_id uuid NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
    role company_role NOT NULL DEFAULT 'member',
    status text NOT NULL DEFAULT 'active',
    invited_at timestamp with time zone DEFAULT now(),
    joined_at timestamp with time zone DEFAULT now(),
    invited_by_user_id uuid REFERENCES users(id) ON DELETE SET NULL,
    invitation_id uuid,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);

CREATE TYPE company_member_with_user AS (
    id UUID,
    email TEXT -- simplified for brevity
);
"""

FUNCTION_SQL_FAILS = """
CREATE OR REPLACE FUNCTION add_company_member(
    p_company_id UUID,
    p_user_id UUID,
    p_invited_by_user_id UUID,
    p_role company_role DEFAULT 'member',
    p_status TEXT DEFAULT 'pending',
    p_invitation_id UUID DEFAULT NULL
)
RETURNS company_members
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
AS $$
BEGIN
    INSERT INTO company_members (company_id, user_id, invited_by_user_id, role, status, invitation_id, invited_at)
    VALUES (p_company_id, p_user_id, p_invited_by_user_id, p_role, p_status, p_invitation_id, NOW())
    RETURNING *;
END;
$$;
"""

FUNCTION_SQL_WORKS_CUSTOM_TYPE = """
CREATE OR REPLACE FUNCTION list_company_members(
    p_company_id UUID
)
RETURNS SETOF company_member_with_user
LANGUAGE sql STABLE SECURITY DEFINER
AS $$
    SELECT cm.id, 'test@example.com'::TEXT
    FROM company_members cm
    WHERE cm.company_id = p_company_id;
$$;
"""

# Test that was failing due to CLI error message confusion, now understood.
# The CLI error "Failed to parse SQL" was misleading; the error came from the generator.
# parse_sql itself does NOT raise the error for this case.
def test_cli_reported_parsing_failure_for_table_row_type():
    """
    Tests that parse_sql itself does NOT raise MissingSchemaError for a function
    returning a table row type, even if that table isn't in composite_types.
    The error reported by CLI originates from the generator.
    """
    # parse_sql returns 4 values: functions, imports, composite_types, enums
    parsed_functions, _, _, _ = parse_sql( 
        FUNCTION_SQL_FAILS, 
        SCHEMA_SQL          
    )
    assert len(parsed_functions) == 1
    assert parsed_functions[0].sql_name == "add_company_member"
    # No MissingSchemaError expected from parse_sql


def test_generator_handles_direct_table_return_schema_correctly():
    """
    Tests that generate_python_code successfully generates code for a function
    returning a table row type, using func.return_columns when the table's schema
    is not initially in parsed_composite_types.
    """
    parser = SQLParser()
    parsed_funcs, imports_for_generator, composite_types_for_generator = parser.parse(
        FUNCTION_SQL_FAILS, 
        SCHEMA_SQL          
    )

    assert 'company_members' not in composite_types_for_generator

    enum_types_for_generator = parser.enum_types

    # The MissingSchemaError should no longer be raised.
    # We expect successful code generation.
    generated_code = generate_python_code(
        functions=parsed_funcs,
        table_schema_imports=imports_for_generator,
        parsed_composite_types=composite_types_for_generator,
        parsed_enum_types=enum_types_for_generator,
    )

    # print(f"Generated code for test_generator_handles_direct_table_return_schema_correctly:\n{generated_code}") # DEBUG PRINT
    assert generated_code is not None
    assert "@dataclass\nclass CompanyMember:" in generated_code # Singular form
    # Add more specific checks if necessary, e.g., for specific fields
    assert "id: UUID" in generated_code # Example field check
    assert "user_id: UUID" in generated_code
    assert "company_id: UUID" in generated_code
    assert "role: CompanyRole" in generated_code # Check for Enum type


def test_function_returning_custom_type_works_for_contrast():
    """
    Tests that a function returning a custom composite type (CREATE TYPE) works through parsing.
    """
    try:
        _ = parse_sql(sql_content=FUNCTION_SQL_WORKS_CUSTOM_TYPE, schema_content=SCHEMA_SQL)
        # If parse_sql succeeds, the test for this contrast case is considered passed.
        # We are not testing generation here, just that parsing doesn't fail for the working case.
        assert True # Explicitly mark success
    except MissingSchemaError as e:
        pytest.fail(f"parse_sql for custom type function failed unexpectedly: {e}")
    except Exception as e:
        pytest.fail(f"An unexpected error occurred during parse_sql for working case: {e}") 