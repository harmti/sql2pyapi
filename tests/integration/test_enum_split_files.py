"""
Integration test simulating the scenario where ENUM types are defined
in a schema file and used by functions in a separate functions file.
Tests the fix for the bug where enums were being overwritten.
"""

import pytest
from typing import Dict, List, Set, Optional

# Import the public API
from sql2pyapi.parser import parse_sql
from sql2pyapi.generator import generate_python_code
from sql2pyapi.sql_models import ParsedFunction


def test_enum_defined_in_schema_used_in_func():
    """
    Test case:
    - ENUM 'invitation_status' is defined in schema_content.
    - Function 'list_invitations' (in func_content) uses 'invitation_status'
      as a parameter and returns a table containing it.
    Verify:
    - The enum is parsed correctly.
    - The generated Python function uses 'InvitationStatus' type hint, not Any.
    - The generated dataclass for the return table uses 'InvitationStatus'.
    """
    schema_content = """
    CREATE TYPE invitation_status AS ENUM (
        'pending',
        'accepted',
        'rejected',
        'expired',
        'cancelled'
    );

    CREATE TABLE company_invitations (
        id uuid PRIMARY KEY,
        company_id uuid NOT NULL,
        email text NOT NULL,
        status invitation_status NOT NULL DEFAULT 'pending'
    );
    """

    func_content = """
    -- Function using the enum defined in the schema file
    CREATE OR REPLACE FUNCTION list_company_invitations(
        p_company_id uuid,
        p_status invitation_status DEFAULT NULL
    )
    RETURNS SETOF company_invitations
    LANGUAGE sql
    AS $$
        SELECT * FROM company_invitations
        WHERE company_id = p_company_id
        AND (p_status IS NULL OR status = p_status);
    $$;
    """

    # Parse using both schema and function content
    functions, table_imports, composite_types, enum_types = parse_sql(
        sql_content=func_content,
        schema_content=schema_content
    )

    # --- Verification --- 

    # 1. Verify enum was parsed
    assert 'invitation_status' in enum_types
    assert enum_types['invitation_status'] == [
        'pending', 'accepted', 'rejected', 'expired', 'cancelled'
    ]

    # 2. Verify function parameter type
    assert len(functions) == 1
    list_func: ParsedFunction = functions[0]
    assert list_func.sql_name == "list_company_invitations"
    assert len(list_func.params) == 2

    status_param = next((p for p in list_func.params if p.name == 'p_status'), None)
    assert status_param is not None
    assert status_param.sql_type == 'invitation_status'
    # Should be Optional[InvitationStatus] because of DEFAULT NULL
    assert status_param.python_type == 'Optional[InvitationStatus]'
    assert 'Enum' in list_func.required_imports
    assert 'Optional' in list_func.required_imports

    # 3. Verify function return type (SetOf Table -> List[Dataclass])
    assert list_func.returns_setof is True
    assert list_func.returns_table is True
    assert list_func.setof_table_name == 'company_invitations'
    assert list_func.return_type == 'List[CompanyInvitations]'
    assert 'List' in list_func.required_imports
    assert 'dataclass' in list_func.required_imports

    # 4. Generate Python code
    python_code = generate_python_code(
        functions, table_imports, composite_types, enum_types
    )

    # 5. Verify generated Enum class
    assert "from enum import Enum" in python_code
    assert "class InvitationStatus(Enum):" in python_code
    assert "PENDING = 'pending'" in python_code
    assert "CANCELLED = 'cancelled'" in python_code

    # 6. Verify generated Dataclass
    assert "from dataclasses import dataclass" in python_code
    assert "@dataclass" in python_code
    assert "class CompanyInvitations:" in python_code
    # Check a non-enum field
    assert "email: str" in python_code # Non-null text field
    # Check the enum field (NOT NULL in table, so not Optional in dataclass)
    assert "status: InvitationStatus" in python_code

    # 7. Verify generated function signature
    expected_signature = (
        "async def list_company_invitations(conn: AsyncConnection, " 
        "company_id: UUID, " 
        "status: Optional[InvitationStatus] = None) -> List[CompanyInvitations]:"
    )
    assert expected_signature in python_code

    # 8. Verify enum parameter is used correctly in function body
    assert "status_value = status.value if status is not None else None" in python_code
    assert "params = [company_id, status_value]" in python_code 