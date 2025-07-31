"""Integration tests for SQL ENUM type support in sql2pyapi."""

from pathlib import Path

from sql2pyapi.parser import parse_sql


def test_enum_types_integration():
    """Test end-to-end parsing for SQL ENUM types."""
    # Load the SQL fixture
    fixture_path = Path(__file__).parent.parent / "fixtures" / "enum_types.sql"
    with open(fixture_path) as f:
        sql_content = f.read()

    # Parse the SQL
    functions, table_schema_imports, composite_types, enum_types = parse_sql(sql_content)

    # Verify enum types were parsed correctly
    assert "status_type" in enum_types
    assert enum_types["status_type"] == ["pending", "active", "inactive", "deleted"]

    assert "user_role" in enum_types
    assert enum_types["user_role"] == ["admin", "moderator", "user", "guest"]

    # Verify functions were parsed correctly
    assert len(functions) == 3

    # Check get_default_status function
    get_default_status = next(f for f in functions if f.sql_name == "get_default_status")
    assert get_default_status.return_type == "StatusType"
    assert get_default_status.returns_enum_type

    # Check is_active_role function
    is_active_role = next(f for f in functions if f.sql_name == "is_active_role")
    assert len(is_active_role.params) == 1
    assert is_active_role.params[0].sql_type == "user_role"
    assert is_active_role.params[0].python_type == "UserRole"

    # Check get_users_by_status function
    get_users_by_status = next(f for f in functions if f.sql_name == "get_users_by_status")
    assert len(get_users_by_status.params) == 1
    assert get_users_by_status.params[0].sql_type == "status_type"
    assert get_users_by_status.params[0].python_type == "StatusType"

    assert get_users_by_status.returns_table
    assert len(get_users_by_status.return_columns) == 4

    # Check enum columns in return table
    status_col = next(col for col in get_users_by_status.return_columns if col.name == "status")
    role_col = next(col for col in get_users_by_status.return_columns if col.name == "role")

    assert status_col.sql_type == "status_type"
    assert status_col.python_type == "StatusType"

    assert role_col.sql_type == "user_role"
    assert role_col.python_type == "UserRole"
