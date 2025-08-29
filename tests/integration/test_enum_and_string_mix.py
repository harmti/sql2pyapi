"""Integration test for functions with both enum and string parameters."""

from sql2pyapi.generator import generate_python_code
from tests.test_utils import create_test_enum
from tests.test_utils import create_test_function
from tests.test_utils import parse_test_sql


def test_enum_and_string_param_code_generation():
    """Test that only enum parameters extract .value, not string parameters."""
    # Create an enum type
    enum_sql = create_test_enum("status_type", ["pending", "active", "inactive"])

    # Create a function with both an enum and a string parameter
    func_sql = create_test_function("add_company_member", "p_role status_type, p_status text", "integer")

    # Parse both
    functions, _, _, enum_types = parse_test_sql(func_sql, enum_sql)

    # Generate the Python code
    python_code = generate_python_code(functions, {}, {}, enum_types)

    # Should extract .value only for the enum param (role), not for string (status)
    assert "role_value = role.value if role is not None else None" in python_code
    # Should NOT extract .value for the string param
    assert "status_value = status.value" not in python_code

    # Should use role_value for enum and status directly for string in _call_params_dict
    # SQL names: p_role, p_status
    # Python names: role, status
    assert "_sql_named_args_parts.append(f'p_role := %(role)s')" in python_code
    assert "_call_params_dict['role'] = role_value" in python_code

    assert "_sql_named_args_parts.append(f'p_status := %(status)s')" in python_code
    assert "_call_params_dict['status'] = status" in python_code

    # Ensure old list format and _call_values appends are not present
    assert "[role_value, status]" not in python_code
    assert "_call_values.append(role_value)" not in python_code
    assert "_call_values.append(status)" not in python_code


def test_setof_enum_return_type():
    """Test that SETOF enum functions correctly extract enum class name from List wrapper."""
    # Create an enum type
    enum_sql = create_test_enum("user_role", ["admin", "user", "guest"])

    # Create a SETOF enum function
    func_sql = """
    CREATE FUNCTION get_all_roles()
    RETURNS SETOF user_role
    AS $$ SELECT unnest(ARRAY['admin', 'user', 'guest']::user_role[]); $$ LANGUAGE SQL;
    """

    # Parse both
    functions, _, _, enum_types = parse_test_sql(func_sql, enum_sql)

    # Generate the Python code
    python_code = generate_python_code(functions, {}, {}, enum_types)

    # Check the return type annotation
    assert "-> List[UserRole]:" in python_code

    # Check that it correctly extracts enum class from List wrapper
    assert "return [UserRole(row[0]) for row in rows]" in python_code

    # Should NOT have List[UserRole] in the list comprehension
    assert "return [List[UserRole](row[0])" not in python_code


def test_composite_with_enum_fields():
    """Test that composite types with enum fields use type-aware parsing."""
    # Create enum and composite types
    enum_sql = create_test_enum("user_role", ["admin", "user", "guest"])

    composite_sql = """
    CREATE TYPE user_profile AS (
        id UUID,
        name TEXT,
        role user_role
    );
    """

    func_sql = """
    CREATE FUNCTION get_user_profile(p_user_id UUID)
    RETURNS user_profile
    AS $$ 
        SELECT ROW(p_user_id, 'John Doe', 'admin'::user_role)::user_profile;
    $$ LANGUAGE SQL;
    """

    # Parse all
    full_sql = enum_sql + "\n" + composite_sql + "\n" + func_sql
    functions, _, composite_types, enum_types = parse_test_sql(full_sql)

    # Generate the Python code
    python_code = generate_python_code(functions, {}, composite_types, enum_types)

    # Check that composite unpacking is used
    assert "parse_composite_with_types" in python_code or "_parse_composite_string_typed" in python_code

    # Check that UserRole enum is properly defined
    assert "class UserRole(Enum):" in python_code

    # Check that enum field is properly typed in the dataclass
    assert "role: Optional[UserRole]" in python_code


def test_setof_composite_with_enum_fields():
    """Test that SETOF composite types with enum fields generate helper functions."""
    # Create enum and composite types
    enum_sql = create_test_enum("user_role", ["admin", "user", "guest"])

    composite_sql = """
    CREATE TYPE user_profile AS (
        id UUID,
        name TEXT,
        role user_role
    );
    """

    func_sql = """
    CREATE FUNCTION get_all_profiles()
    RETURNS SETOF user_profile
    AS $$
        SELECT ROW(gen_random_uuid(), 'User ' || i, 
               CASE i % 3 
                   WHEN 0 THEN 'admin'::user_role
                   WHEN 1 THEN 'user'::user_role
                   ELSE 'guest'::user_role
               END)::user_profile
        FROM generate_series(1, 3) i;
    $$ LANGUAGE SQL;
    """

    # Parse all
    full_sql = enum_sql + "\n" + composite_sql + "\n" + func_sql
    functions, _, composite_types, enum_types = parse_test_sql(full_sql)

    # Generate the Python code
    python_code = generate_python_code(functions, {}, composite_types, enum_types)

    # Check that helper function is generated for SETOF composite
    assert "def create_userprofile(row):" in python_code

    # Check that enum conversion happens in the helper - should have UserRole in field_types
    assert "'UserRole'" in python_code or "UserRole" in python_code
    # Check for type-aware conversion function which handles enums
    assert "_convert_postgresql_value_typed" in python_code
