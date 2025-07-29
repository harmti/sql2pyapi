"""Tests for composite type handling using the public API.

These tests verify that SQL composite types are correctly parsed and used in
various contexts through the public API.
"""

import pytest
from typing import List, Dict, Set, Optional

# Import the public API
from sql2pyapi.parser import parse_sql
from sql2pyapi import generate_python_code

# Import test utilities
from tests.test_utils import (
    create_test_function,
    create_test_table,
    find_function,
    find_parameter,
    find_return_column,
    parse_test_sql
)


def test_basic_composite_type():
    """Test basic composite type parsing and usage."""
    # Create a composite type
    type_sql = """
    CREATE TYPE address_type AS (
        street text,
        city text,
        state text,
        zip_code text
    );
    """
    
    # Create a function that uses the composite type as a parameter
    func_sql = create_test_function(
        "save_address", 
        "p_address address_type", 
        "integer"
    )
    
    # Parse both
    functions, _, composite_types, _ = parse_test_sql(func_sql, type_sql)
    
    # Verify the function was parsed
    func = find_function(functions, "save_address")
    param = find_parameter(func, "p_address")
    assert param.sql_type == "address_type"
    # The parser now correctly recognizes composite types
    assert param.python_type == "AddressType"


def test_schema_qualified_composite_type():
    """Test schema-qualified composite types."""
    # Create a schema-qualified composite type
    type_sql = """
    CREATE TYPE public.point_type AS (
        x numeric,
        y numeric
    );
    """
    
    # Create a function that uses the composite type
    func_sql = create_test_function(
        "calculate_distance", 
        "p_point1 public.point_type, p_point2 public.point_type", 
        "numeric"
    )
    
    # Parse both
    functions, _, composite_types, _ = parse_test_sql(func_sql, type_sql)
    
    # Verify the function was parsed
    func = find_function(functions, "calculate_distance")
    param1 = find_parameter(func, "p_point1")
    assert param1.sql_type == "public.point_type"
    # The parser now correctly recognizes schema-qualified composite types
    assert param1.python_type == "PointType"


def test_composite_type_array():
    """Test composite type arrays."""
    # Create a composite type
    type_sql = """
    CREATE TYPE contact_type AS (
        name text,
        email text,
        phone text
    );
    """
    
    # Create a function that uses the composite type array
    func_sql = create_test_function(
        "save_contacts", 
        "p_contacts contact_type[]", 
        "integer"
    )
    
    # Parse both
    functions, _, composite_types, _ = parse_test_sql(func_sql, type_sql)
    
    # Verify the function was parsed
    func = find_function(functions, "save_contacts")
    param = find_parameter(func, "p_contacts")
    assert param.sql_type == "contact_type[]"
    # The parser now correctly treats composite type arrays as List[CompositeType]
    assert param.python_type == "List[ContactType]"
    
    # Verify imports
    assert "List" in func.required_imports


def test_returning_composite_type():
    """Test returning a composite type."""
    # Create a composite type
    type_sql = """
    CREATE TYPE user_info_type AS (
        user_id integer,
        username text,
        email text,
        created_at timestamp
    );
    """
    
    # Create a function that returns the composite type
    func_sql = create_test_function(
        "get_user_info", 
        "p_id integer", 
        "user_info_type"
    )
    
    # Parse both
    functions, _, composite_types, _ = parse_test_sql(func_sql, type_sql)
    
    # Verify the function
    func = find_function(functions, "get_user_info")
    # The parser actually handles composite types well, creating a dataclass
    assert func.returns_table
    assert "UserInfoType" in func.return_type
    assert "dataclass" in func.required_imports
    
    # Verify the return columns
    assert len(func.return_columns) == 4
    assert func.return_columns[0].name == "user_id"
    assert func.return_columns[1].name == "username"
    assert func.return_columns[2].name == "email"
    assert func.return_columns[3].name == "created_at"


def test_composite_type_with_nested_types():
    """Test composite types that include other complex types."""
    # Create a composite type with various field types
    type_sql = """
    CREATE TYPE product_details_type AS (
        product_id uuid,
        name text,
        price numeric(10,2),
        tags text[],
        created_at timestamp,
        is_available boolean
    );
    """
    
    # Create a function that returns the composite type
    func_sql = create_test_function(
        "get_product_details", 
        "p_id uuid", 
        "product_details_type"
    )
    
    # Parse both
    functions, _, composite_types, _ = parse_test_sql(func_sql, type_sql)
    
    # Verify the function
    func = find_function(functions, "get_product_details")
    # The parser actually handles composite types well, creating a dataclass
    assert func.returns_table
    assert "ProductDetailsType" in func.return_type
    assert "dataclass" in func.required_imports
    
    # Verify the return columns and imports
    assert len(func.return_columns) >= 3  # At least the first few columns
    assert func.return_columns[0].name == "product_id"
    assert "UUID" in func.required_imports
    assert "Decimal" in func.required_imports


def test_widget_details_composite_type_all_fields_parsed():
    """Tests that a composite type with various SQL types has all its fields parsed correctly."""
    type_sql = """
    CREATE TYPE widget_details AS (
        widget_id UUID,
        name TEXT,
        description TEXT,
        stock_count INTEGER,
        last_ordered_date DATE,
        is_active BOOLEAN
    );
    """

    func_sql = create_test_function(
        "get_widget_details_by_id",
        "p_widget_id UUID",
        "widget_details"  # The function returns our custom type
    )

    functions, table_imports, composite_types, enum_types = parse_test_sql(func_sql, type_sql)

    func = find_function(functions, "get_widget_details_by_id")
    assert func is not None, "Function get_widget_details_by_id not found"

    assert func.returns_table, "Function should be marked as returning a table-like structure (dataclass)"
    assert "WidgetDetail" in func.return_type, f"Return type should be 'WidgetDetail', got {func.return_type}"
    assert "dataclass" in func.required_imports, "Dataclass import should be required"

    # Verify all six fields are present in the return_columns
    assert len(func.return_columns) == 6, f"Expected 6 return columns, got {len(func.return_columns)}"

    expected_fields = {
        "widget_id": "UUID",
        "name": "str",
        "description": "str",
        "stock_count": "int",
        "last_ordered_date": "date",
        "is_active": "bool"
    }
    
    expected_imports = {"UUID", "date"} # From uuid import UUID, from datetime import date

    actual_fields = {col.name: col.python_type for col in func.return_columns}

    for field_name, python_type in expected_fields.items():
        assert field_name in actual_fields, f"Field '{field_name}' missing in parsed return columns"
        # The actual python_type might be Optional[<type>], so we check if the expected type is a substring
        assert python_type in actual_fields[field_name], \
            f"Field '{field_name}' has type '{actual_fields[field_name]}', expected to contain '{python_type}'"

    for imp in expected_imports:
        assert imp in func.required_imports, f"Expected import '{imp}' not found in required_imports: {func.required_imports}"

    # Check the composite_types structure as well (if it's populated by parse_test_sql)
    # This part depends on how composite_types is structured and used.
    # For now, we focus on func.return_columns as that directly impacts dataclass generation.
    assert "widget_details" in composite_types
    widget_type_info_columns = composite_types["widget_details"] # Expecting a list of column objects
    assert isinstance(widget_type_info_columns, list), "composite_types['widget_details'] should be a list of column definitions"
    assert len(widget_type_info_columns) == 6, f"Expected 6 columns in composite_types['widget_details'], got {len(widget_type_info_columns)}"
    
    parsed_column_names_from_type = [col.name for col in widget_type_info_columns]
    for field_name in expected_fields.keys():
        assert field_name in parsed_column_names_from_type, f"Field '{field_name}' missing in parsed composite_types['widget_details'] columns"

    # Now, generate the full Python code and check the dataclass string
    # Correctly unpack results from parse_test_sql for clarity
    parsed_functions, parsed_table_imports, parsed_composite_types, parsed_enum_types = functions, table_imports, composite_types, enum_types

    generated_code = generate_python_code(
        functions=parsed_functions,
        table_schema_imports=parsed_table_imports if parsed_table_imports else {},
        parsed_composite_types=parsed_composite_types if parsed_composite_types else {},
        parsed_enum_types=parsed_enum_types if parsed_enum_types else {}
    )

    # print(f"--- GENERATED CODE ---\n{generated_code}\n--- END GENERATED CODE ---") # For debugging

    expected_dataclass_fields_in_string = [
        "widget_id: Optional[UUID]",
        "name: Optional[str]",
        "description: Optional[str]",
        "stock_count: Optional[int]",
        "last_ordered_date: Optional[date]",
        "is_active: Optional[bool]"
    ]

    # Rough check for the class and its fields
    # This is a bit brittle; AST parsing would be more robust but let's start here.
    assert "@dataclass" in generated_code
    assert "class WidgetDetail:" in generated_code

    # Extract the WidgetDetails class block (very roughly)
    class_block_lines = []
    in_class_block = False
    for line in generated_code.splitlines():
        stripped_line = line.strip()
        if "class WidgetDetail:" in stripped_line:
            in_class_block = True
            class_block_lines.append(stripped_line) # Include the class declaration itself for context
            continue
        if in_class_block:
            if not stripped_line: # An empty line might end the fields before methods start
                pass # continue collecting, might be just a space
            if line.startswith("def ") or line.startswith("    def ") or line.startswith("class ") or line.startswith("    @") :
                in_class_block = False # Stop if we hit a method or another class
                break
            if stripped_line: # Only add non-empty lines
                 class_block_lines.append(stripped_line)
    
    generated_dataclass_string = "\n".join(class_block_lines)
    # print(f"--- EXTRACTED DATACLASS ---\n{generated_dataclass_string}\n--- END EXTRACTED DATACLASS ---")

    for field_def in expected_dataclass_fields_in_string:
        assert field_def in generated_dataclass_string, \
            f"Expected field definition '{field_def}' not found in generated WidgetDetail dataclass:\n{generated_dataclass_string}"
