"""Integration test for functions with both enum and string parameters."""

import pytest
from sql2pyapi.parser import parse_sql
from sql2pyapi.generator import generate_python_code
from tests.test_utils import create_test_enum, create_test_function, parse_test_sql

def test_enum_and_string_param_code_generation():
    """Test that only enum parameters extract .value, not string parameters."""
    # Create an enum type
    enum_sql = create_test_enum("status_type", ["pending", "active", "inactive"])

    # Create a function with both an enum and a string parameter
    func_sql = create_test_function(
        "add_company_member",
        "p_role status_type, p_status text",
        "integer"
    )

    # Parse both
    functions, _, _, enum_types = parse_test_sql(func_sql, enum_sql)

    # Generate the Python code
    python_code = generate_python_code(functions, {}, {}, enum_types)

    # Should extract .value only for the enum param (role), not for string (status)
    assert "role_value = role.value if role is not None else None" in python_code
    # Should NOT extract .value for the string param
    assert "status_value = status.value" not in python_code
    # Should use status as-is in the argument list
    assert "[role_value, status]" in python_code
