"""Integration test for Optional enum parameter handling in code generation."""

import pytest
from pathlib import Path

# Import the public API
from sql2pyapi.parser import parse_sql
from sql2pyapi.generator import generate_python_code

# Test utilities
from tests.test_utils import create_test_enum, create_test_function, parse_test_sql

def test_optional_enum_code_generation():
    """Test that Optional enum parameters correctly extract .value in generated code."""
    # Create an enum type
    enum_sql = create_test_enum("priority_type", ["high", "medium", "low"])
    
    # Create a function that uses the enum with a default value (making it Optional)
    func_sql = create_test_function(
        "get_tasks_by_priority", 
        "p_priority priority_type DEFAULT 'medium'", 
        "integer"
    )
    
    # Parse both
    functions, _, _, enum_types = parse_test_sql(func_sql, enum_sql)
    
    # Generate the Python code
    python_code = generate_python_code(functions, {}, {}, enum_types)
    
    # Verify that the code correctly extracts .value from the optional enum parameter
    assert "# Extract .value from enum parameters" in python_code
    # The parameter is renamed from p_priority to priority in the generated code
    assert "priority_value = priority.value if priority is not None else None" in python_code
    
    # Verify that the extracted value is used in the SQL query
    assert "[priority_value]" in python_code

def test_direct_enum_and_optional_enum_parameters():
    """Test that both direct enum and Optional enum parameters extract .value."""
    # Create an enum type
    enum_sql = create_test_enum("status_type", ["active", "pending", "inactive"])
    
    # Create a function with both a required enum parameter and an optional enum parameter
    func_sql = create_test_function(
        "filter_items", 
        "p_status status_type, p_optional_status status_type DEFAULT NULL", 
        "integer"
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
    assert "[status_value, optional_status_value]" in python_code
