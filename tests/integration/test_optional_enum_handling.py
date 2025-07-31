"""Integration test for Optional enum parameter handling in code generation."""

import textwrap

# Import the public API
from sql2pyapi.generator import generate_python_code

# Test utilities
from tests.test_utils import create_test_enum
from tests.test_utils import create_test_function
from tests.test_utils import parse_test_sql


def test_optional_enum_code_generation():
    """Test that Optional enum parameters correctly extract .value in generated code."""
    # Create an enum type
    enum_sql = create_test_enum("priority_type", ["high", "medium", "low"])

    # Create a function that uses the enum with a default value (making it Optional)
    func_sql = create_test_function("get_tasks_by_priority", "p_priority priority_type DEFAULT 'medium'", "integer")

    # Parse both
    functions, _, _, enum_types = parse_test_sql(func_sql, enum_sql)

    # Generate the Python code
    python_code = generate_python_code(functions, {}, {}, enum_types)

    # Verify that the code correctly extracts .value from the optional enum parameter
    assert "# Extract .value from enum parameters" in python_code
    # The parameter is renamed from p_priority to priority in the generated code
    assert "priority_value = priority.value if priority is not None else None" in python_code

    # Verify that the extracted value is used in the SQL query if provided
    # If 'priority' is None, it's omitted, DB uses DEFAULT 'medium'
    # If 'priority' is not None, it's included in _sql_named_args_parts and _call_params_dict
    textwrap.dedent("""
    if priority is not None:
        _sql_named_args_parts.append(f'p_priority := %(priority)s')
        _call_params_dict['priority'] = priority_value
    """).strip()
    # Normalize whitespace for comparison if necessary, or check key components
    # For simplicity, check for key lines within the generated code structure
    assert "if priority is not None:" in python_code
    # Check if the assignment lines are present within an if block related to 'priority'
    # This is a simplified check; a full AST parse would be more robust for structure.
    assert "_sql_named_args_parts.append(f'p_priority := %(priority)s')" in python_code
    assert "_call_params_dict['priority'] = priority_value" in python_code

    # Ensure old logic is not present
    assert "if priority is None:" not in python_code  # Old check for DEFAULT keyword
    assert "_sql_parts.append('DEFAULT')" not in python_code
    assert "_call_values.append(priority_value)" not in python_code


def test_direct_enum_and_optional_enum_parameters():
    """Test that both direct enum and Optional enum parameters extract .value."""
    # Create an enum type
    enum_sql = create_test_enum("status_type", ["active", "pending", "inactive"])

    # Create a function with both a required enum parameter and an optional enum parameter
    func_sql = create_test_function(
        "filter_items", "p_status status_type, p_optional_status status_type DEFAULT NULL", "integer"
    )

    # Parse both
    functions, _, _, enum_types = parse_test_sql(func_sql, enum_sql)

    # Generate the Python code
    python_code = generate_python_code(functions, {}, {}, enum_types)

    # Verify that both parameters extract .value
    # The parameters are renamed from p_status to status and p_optional_status to optional_status
    assert "status_value = status.value if status is not None else None" in python_code
    assert "optional_status_value = optional_status.value if optional_status is not None else None" in python_code

    # Verify that both extracted values are used in the SQL query
    # status (non-optional, SQL name p_status) should be directly assigned
    assert "_sql_named_args_parts.append(f'p_status := %(status)s')" in python_code
    assert "_call_params_dict['status'] = status_value" in python_code

    # optional_status (optional, SQL name p_optional_status, DEFAULT NULL)
    # If 'optional_status' (Python var) is None, it's omitted.
    # If 'optional_status' is not None, it's included.
    textwrap.dedent("""
    if optional_status is not None:
        _sql_named_args_parts.append(f'p_optional_status := %(optional_status)s')
        _call_params_dict['optional_status'] = optional_status_value
    """).strip()
    assert "if optional_status is not None:" in python_code
    assert "_sql_named_args_parts.append(f'p_optional_status := %(optional_status)s')" in python_code
    assert "_call_params_dict['optional_status'] = optional_status_value" in python_code

    # Ensure old logic for DEFAULT NULL (explicitly passing None) is not present
    assert "if optional_status is None:" not in python_code  # Old check
    assert "_sql_parts.append('%s')" not in python_code  # Part of old logic
    # This specific check might be too broad if _sql_parts is used elsewhere,
    # but in the context of how optional_status was handled, it should be gone.
    # A more targeted removal would be to check it wasn't in the old 'if optional_status is None:' block.
    assert "_call_values.append(None)" not in python_code  # Part of old logic
    assert "_call_values.append(optional_status_value)" not in python_code  # Old append style
