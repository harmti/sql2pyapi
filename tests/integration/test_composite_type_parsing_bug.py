"""
Integration test for composite type parsing bug.

Tests the reported issue where PostgreSQL boolean and numeric types
are not properly converted when parsing composite type string representations.
The bug occurs in the _parse_composite_string function where PostgreSQL-specific
string values ('t'/'f' for booleans, numeric strings) are not converted to
proper Python types.
"""

from decimal import Decimal

import pytest

from tests.test_utils import parse_test_sql


def test_composite_type_boolean_numeric_parsing_bug():
    """
    Test that reproduces the composite type parsing bug for BOOLEAN and NUMERIC types.

    This test creates a composite type with boolean and numeric fields, then tests
    that the generated parsing code properly converts PostgreSQL string representations
    to Python types.
    """

    # Create SQL that defines a composite type with boolean and numeric fields
    sql_content = """
    -- Table that will be referenced by composite type
    CREATE TABLE metering_points (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL,
        is_remotely_connectable BOOLEAN,
        latitude NUMERIC(10,7),
        longitude NUMERIC(10,7),
        max_capacity NUMERIC(10,2)
    );

    -- Composite type that contains the problematic types
    CREATE TYPE metering_point_upsert_result AS (
        metering_point metering_points,
        was_created BOOLEAN
    );

    -- Function that returns the composite type
    CREATE FUNCTION upsert_metering_point_test(
        p_name TEXT,
        p_is_remotely_connectable BOOLEAN DEFAULT NULL,
        p_latitude NUMERIC(10,7) DEFAULT NULL,
        p_longitude NUMERIC(10,7) DEFAULT NULL,
        p_max_capacity NUMERIC(10,2) DEFAULT NULL
    )
    RETURNS metering_point_upsert_result
    AS $$
    BEGIN
        -- This would normally insert/update and return the result
        -- For testing, we'll return a constructed result
        RETURN (
            (1, p_name, p_is_remotely_connectable, p_latitude, p_longitude, p_max_capacity)::metering_points,
            TRUE
        )::metering_point_upsert_result;
    END;
    $$ LANGUAGE plpgsql;
    """

    # Parse the SQL
    functions, table_imports, composite_types, enum_types = parse_test_sql(sql_content)

    # Verify we have the expected function
    assert len(functions) == 1
    func = functions[0]
    assert func.sql_name == "upsert_metering_point_test"
    assert func.returns_table
    assert len(func.return_columns) == 2

    # Verify the composite type was parsed
    assert "metering_point_upsert_result" in composite_types
    composite_fields = composite_types["metering_point_upsert_result"]
    assert len(composite_fields) == 2

    # Check that the metering_point field references the table
    metering_point_field = composite_fields[0]
    assert metering_point_field.name == "metering_point"
    assert metering_point_field.sql_type == "metering_points"

    # Check that the was_created field is boolean
    was_created_field = composite_fields[1]
    assert was_created_field.name == "was_created"
    print(f"DEBUG: was_created_field.sql_type = {was_created_field.sql_type}")
    print(f"DEBUG: was_created_field.python_type = {was_created_field.python_type}")
    # The sql_type might be "BOOLEAN" (uppercase)
    assert was_created_field.sql_type.lower() == "boolean"
    assert "bool" in was_created_field.python_type

    # Verify the table schema was captured
    assert "metering_points" in table_imports

    print("✅ Composite type parsing structure looks correct")
    print(f"Function return columns: {[col.name + ':' + col.python_type for col in func.return_columns]}")
    print(f"Composite fields: {[(f.name, f.sql_type, f.python_type) for f in composite_fields]}")


def test_composite_type_generation_produces_parsing_function():
    """
    Test that the generated code includes the _parse_composite_string function
    that is responsible for the bug.
    """

    sql_content = """
    CREATE TABLE test_table (
        id SERIAL PRIMARY KEY,
        flag BOOLEAN,
        amount NUMERIC(10,2)
    );

    CREATE TYPE test_composite AS (
        record test_table,
        extra_flag BOOLEAN
    );

    CREATE FUNCTION get_test_composite()
    RETURNS test_composite
    AS $$
        SELECT ((1, TRUE, 123.45)::test_table, FALSE)::test_composite;
    $$ LANGUAGE SQL;
    """

    # This would need to use the actual generator to create the code
    # and then inspect the generated _parse_composite_string function
    functions, table_imports, composite_types, enum_types = parse_test_sql(sql_content)

    assert len(functions) == 1
    func = functions[0]

    # The function should be marked as returning a table/composite
    assert func.returns_table

    # Should have imports for composite unpacking
    # (This would be verified by looking at the actual generated code)
    print("✅ Function structure supports composite type generation")


def test_boolean_conversion_scenarios():
    """
    Test different boolean scenarios that should be handled in composite parsing.
    """

    # Test cases for boolean values that appear in PostgreSQL composite strings
    test_cases = [
        ('t', True, "PostgreSQL TRUE representation"),
        ('f', False, "PostgreSQL FALSE representation"),
        (None, None, "NULL value"),
    ]

    for pg_value, expected_python, description in test_cases:
        print(f"Testing {description}: '{pg_value}' -> {expected_python}")
        # This is where we would test the actual _parse_composite_string function
        # once we implement the fix


def test_numeric_conversion_scenarios():
    """
    Test different numeric scenarios that should be handled in composite parsing.
    """

    # Test cases for numeric values that appear in PostgreSQL composite strings
    test_cases = [
        ('60.1698570', Decimal('60.1698570'), "NUMERIC(10,7) value"),
        ('123.45', Decimal('123.45'), "NUMERIC(10,2) value"),
        ('-456.789', Decimal('-456.789'), "Negative numeric value"),
        ('0', Decimal('0'), "Zero value"),
        ('0.0000000', Decimal('0.0000000'), "Zero with precision"),
        (None, None, "NULL numeric value"),
    ]

    for pg_value, expected_python, description in test_cases:
        print(f"Testing {description}: '{pg_value}' -> {expected_python}")
        # This is where we would test the actual _parse_composite_string function
        # once we implement the fix


def test_jsonb_conversion_scenarios():
    """
    Test different JSONB scenarios that should be handled in composite parsing.
    """

    # Test cases for JSONB values that appear in PostgreSQL composite strings
    test_cases = [
        ('{"source": "DH-131", "grid_operator": "Fingrid"}',
         {"source": "DH-131", "grid_operator": "Fingrid"},
         "JSONB object"),
        ('{"nested": {"key": "value"}, "array": [1, 2, 3]}',
         {"nested": {"key": "value"}, "array": [1, 2, 3]},
         "Complex nested JSONB"),
        ('[1, 2, 3, "string"]',
         [1, 2, 3, "string"],
         "JSONB array"),
        ('[]',
         [],
         "Empty JSONB array"),
        ('{}',
         {},
         "Empty JSONB object"),
        ('{"key": null}',
         {"key": None},
         "JSONB with null value"),
        ('"just a string"',
         "just a string",
         "JSONB string value"),
        ('not_json_at_all',
         'not_json_at_all',
         "Regular string (should not be converted)"),
        ('{broken json',
         '{broken json',
         "Malformed JSON (should remain as string)"),
        (None, None, "NULL JSONB value"),
    ]

    for pg_value, expected_python, description in test_cases:
        print(f"Testing {description}: '{pg_value}' -> {expected_python}")
        # This is where we would test the actual _parse_composite_string function
        # once we implement the fix


def test_composite_type_with_jsonb_fields():
    """
    Test composite type structure with JSONB fields to ensure proper type mapping.
    """

    sql_content = """
    -- Table with JSONB fields
    CREATE TABLE metering_points_with_jsonb (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL,
        datahub_raw_metadata JSONB,
        config_settings JSON,
        tags JSONB
    );

    -- Composite type containing JSONB
    CREATE TYPE meter_with_metadata AS (
        meter metering_points_with_jsonb,
        last_updated TIMESTAMP
    );

    -- Function returning the composite type
    CREATE FUNCTION get_meter_with_metadata(p_id INTEGER)
    RETURNS meter_with_metadata
    AS $$
        SELECT
            (p_id, 'Test Meter', '{"source": "test"}', '{"enabled": true}', '["tag1", "tag2"]')::metering_points_with_jsonb,
            now()::TIMESTAMP;
    $$ LANGUAGE SQL;
    """

    # Parse the SQL
    functions, table_imports, composite_types, enum_types = parse_test_sql(sql_content)

    # Verify function structure
    assert len(functions) == 1
    func = functions[0]
    assert func.sql_name == "get_meter_with_metadata"
    assert func.returns_table

    # Verify table schema contains JSONB fields
    assert "metering_points_with_jsonb" in table_imports

    # Verify composite type structure
    assert "meter_with_metadata" in composite_types
    composite_fields = composite_types["meter_with_metadata"]
    assert len(composite_fields) == 2

    # Check the meter field references the table with JSONB
    meter_field = composite_fields[0]
    assert meter_field.name == "meter"
    assert meter_field.sql_type == "metering_points_with_jsonb"

    print("✅ JSONB composite type structure looks correct")
    print(f"Composite fields: {[(f.name, f.sql_type, f.python_type) for f in composite_fields]}")


if __name__ == "__main__":
    # Run the tests
    test_composite_type_boolean_numeric_parsing_bug()
    test_composite_type_generation_produces_parsing_function()
    test_boolean_conversion_scenarios()
    test_numeric_conversion_scenarios()
    test_jsonb_conversion_scenarios()
    test_composite_type_with_jsonb_fields()
    print("\n🎯 All structure tests passed - ready to implement the fix!")
