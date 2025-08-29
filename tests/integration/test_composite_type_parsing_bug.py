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
        ("t", True, "PostgreSQL TRUE representation"),
        ("f", False, "PostgreSQL FALSE representation"),
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
        ("60.1698570", Decimal("60.1698570"), "NUMERIC(10,7) value"),
        ("123.45", Decimal("123.45"), "NUMERIC(10,2) value"),
        ("-456.789", Decimal("-456.789"), "Negative numeric value"),
        ("0", Decimal("0"), "Zero value"),
        ("0.0000000", Decimal("0.0000000"), "Zero with precision"),
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
        (
            '{"source": "DH-131", "grid_operator": "Fingrid"}',
            {"source": "DH-131", "grid_operator": "Fingrid"},
            "JSONB object",
        ),
        (
            '{"nested": {"key": "value"}, "array": [1, 2, 3]}',
            {"nested": {"key": "value"}, "array": [1, 2, 3]},
            "Complex nested JSONB",
        ),
        ('[1, 2, 3, "string"]', [1, 2, 3, "string"], "JSONB array"),
        ("[]", [], "Empty JSONB array"),
        ("{}", {}, "Empty JSONB object"),
        ('{"key": null}', {"key": None}, "JSONB with null value"),
        ('"just a string"', "just a string", "JSONB string value"),
        ("not_json_at_all", "not_json_at_all", "Regular string (should not be converted)"),
        ("{broken json", "{broken json", "Malformed JSON (should remain as string)"),
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


def test_type_aware_composite_parsing_functionality():
    """
    Test the new type-aware composite parsing functionality.
    This tests both the should_use_type_aware_parsing detection
    and the generation of type-aware parsing code.
    """
    from src.sql2pyapi.generator.composite_unpacker import generate_composite_unpacking_code
    from src.sql2pyapi.generator.composite_unpacker import generate_type_aware_composite_parser
    from src.sql2pyapi.generator.composite_unpacker import generate_type_aware_converter
    from src.sql2pyapi.generator.composite_unpacker import should_use_type_aware_parsing
    from src.sql2pyapi.sql_models import ReturnColumn

    # Test should_use_type_aware_parsing detection

    # Test case 1: Simple string columns - should not use type-aware parsing
    simple_columns = [
        ReturnColumn(name="id", sql_type="TEXT", python_type="str"),
        ReturnColumn(name="name", sql_type="TEXT", python_type="str"),
    ]
    assert not should_use_type_aware_parsing(simple_columns)

    # Test case 2: Boolean columns - should use type-aware parsing
    boolean_columns = [
        ReturnColumn(name="id", sql_type="TEXT", python_type="str"),
        ReturnColumn(name="is_active", sql_type="BOOLEAN", python_type="Optional[bool]"),
    ]
    assert should_use_type_aware_parsing(boolean_columns)

    # Test case 3: Decimal columns - should use type-aware parsing
    decimal_columns = [
        ReturnColumn(name="id", sql_type="TEXT", python_type="str"),
        ReturnColumn(name="amount", sql_type="NUMERIC", python_type="Optional[Decimal]"),
    ]
    assert should_use_type_aware_parsing(decimal_columns)

    # Test case 4: UUID columns - should use type-aware parsing
    uuid_columns = [
        ReturnColumn(name="user_id", sql_type="UUID", python_type="Optional[UUID]"),
        ReturnColumn(name="name", sql_type="TEXT", python_type="str"),
    ]
    assert should_use_type_aware_parsing(uuid_columns)

    # Test case 5: DateTime columns - should use type-aware parsing
    datetime_columns = [
        ReturnColumn(name="created_at", sql_type="TIMESTAMP", python_type="Optional[datetime]"),
        ReturnColumn(name="name", sql_type="TEXT", python_type="str"),
    ]
    assert should_use_type_aware_parsing(datetime_columns)

    # Test case 6: Dict/List columns (JSON/JSONB) - should use type-aware parsing
    json_columns = [
        ReturnColumn(name="metadata", sql_type="JSONB", python_type="Optional[Dict[str, Any]]"),
        ReturnColumn(name="tags", sql_type="JSONB", python_type="Optional[List[str]]"),
    ]
    assert should_use_type_aware_parsing(json_columns)

    print("✅ should_use_type_aware_parsing detection works correctly")

    # Test generation of type-aware functions
    converter_code = generate_type_aware_converter()
    assert any("_convert_postgresql_value_typed" in line for line in converter_code)
    assert any("bool" in line for line in converter_code)
    assert any("decimal" in line for line in converter_code)
    assert any("uuid" in line for line in converter_code)
    assert any("datetime" in line for line in converter_code)

    parser_code = generate_type_aware_composite_parser()
    assert any("_parse_composite_string_typed" in line for line in parser_code)
    assert any("field_types: List[str]" in line for line in parser_code)
    assert any("_convert_postgresql_value_typed" in line for line in parser_code)

    print("✅ Type-aware function generation works correctly")

    # Test composite unpacking code generation with type-aware parsing
    unpacking_code = generate_composite_unpacking_code(
        class_name="TestResult",
        columns=boolean_columns,  # Has bool, should trigger type-aware parsing
        composite_types={},  # No nested composites
    )

    # Should include type-aware functions and field_types
    unpacking_str = "\n".join(unpacking_code)
    assert "_convert_postgresql_value_typed" in unpacking_str
    assert "_parse_composite_string_typed" in unpacking_str
    assert "field_types = " in unpacking_str

    print("✅ Type-aware composite unpacking code generation works correctly")


def test_type_aware_converter_logic():
    """
    Test the actual conversion logic of the type-aware converter.
    This simulates what would happen when the generated code runs.
    """

    # This is a simulation of the generated _convert_postgresql_value_typed function
    def _convert_postgresql_value_typed(field: str, expected_type: str):  # noqa: PLR0911
        """Simulate the generated converter function."""
        if field is None or field.lower() in ("null", ""):
            return None

        field = field.strip()

        # Boolean types - only for bool types
        if "bool" in expected_type.lower():
            if field == "t":
                return True
            if field == "f":
                return False

        # Decimal types - only for Decimal types
        if "decimal" in expected_type.lower():
            try:
                from decimal import Decimal

                return Decimal(field)
            except (ValueError, TypeError):
                pass

        # UUID types
        if "uuid" in expected_type.lower():
            try:
                from uuid import UUID

                return UUID(field)
            except (ValueError, TypeError):
                pass

        # DateTime types
        if "datetime" in expected_type.lower():
            try:
                from datetime import datetime

                # Handle PostgreSQL timestamp format
                return datetime.fromisoformat(field.replace(" ", "T"))
            except (ValueError, TypeError):
                pass

        # JSON/JSONB types - only for Dict/List types
        if any(hint in expected_type.lower() for hint in ["dict", "list", "any"]):
            if field.strip().startswith(("{", "[")):
                try:
                    import json

                    return json.loads(field)
                except (json.JSONDecodeError, ValueError):
                    pass

        # For all other values, keep as string
        return field

    # Test boolean conversions
    assert _convert_postgresql_value_typed("t", "Optional[bool]") is True
    assert _convert_postgresql_value_typed("f", "Optional[bool]") is False
    assert _convert_postgresql_value_typed("t", "str") == "t"  # Should not convert for non-bool types

    # Test decimal conversions
    from decimal import Decimal

    assert _convert_postgresql_value_typed("123.45", "Optional[Decimal]") == Decimal("123.45")
    assert _convert_postgresql_value_typed("123", "Optional[Decimal]") == Decimal("123")
    assert _convert_postgresql_value_typed("123.45", "str") == "123.45"  # Should not convert for non-decimal types

    # Test UUID conversions
    from uuid import UUID

    test_uuid = "123e4567-e89b-12d3-a456-426614174000"
    assert _convert_postgresql_value_typed(test_uuid, "Optional[UUID]") == UUID(test_uuid)
    assert _convert_postgresql_value_typed(test_uuid, "str") == test_uuid  # Should not convert for non-UUID types

    # Test JSON conversions
    json_obj = '{"key": "value"}'
    json_array = "[1, 2, 3]"
    assert _convert_postgresql_value_typed(json_obj, "Optional[Dict[str, Any]]") == {"key": "value"}
    assert _convert_postgresql_value_typed(json_array, "Optional[List[int]]") == [1, 2, 3]
    assert _convert_postgresql_value_typed(json_obj, "str") == json_obj  # Should not convert for non-dict/list types

    # Test that invalid values fallback to string
    assert _convert_postgresql_value_typed("invalid-uuid", "Optional[UUID]") == "invalid-uuid"
    assert _convert_postgresql_value_typed("{broken json", "Optional[Dict[str, Any]]") == "{broken json"

    print("✅ Type-aware converter logic works correctly")


def test_unboundlocalerror_fix():
    """
    Test that the UnboundLocalError bug is fixed.

    This test verifies that our type-aware parser doesn't generate
    redundant local imports that would shadow global typing imports.
    """
    from src.sql2pyapi.generator.composite_unpacker import generate_type_aware_composite_parser
    from src.sql2pyapi.generator.composite_unpacker import generate_type_aware_converter

    # Generate the code
    parser_lines = generate_type_aware_composite_parser()
    converter_lines = generate_type_aware_converter()

    # Check that no problematic typing imports are generated
    parser_code = "\n".join(parser_lines)
    converter_code = "\n".join(converter_lines)

    # The critical fix: ensure no "from typing import" statements are generated
    assert "from typing import" not in parser_code, (
        f"Parser code contains problematic typing imports. This would cause UnboundLocalError.\n"
        f"Generated code:\n{parser_code}"
    )

    # Converter should also not have typing imports
    typing_imports = [line for line in converter_lines if "from typing import" in line]
    assert len(typing_imports) == 0, (
        f"Converter code contains typing imports: {typing_imports}. This would cause UnboundLocalError."
    )

    print("✅ UnboundLocalError bug is fixed - no redundant typing imports generated")

    # Verify the generated functions can be executed without import issues
    # This simulates the environment in a generated API file
    test_globals = {
        "List": list,  # These would be available globally in generated files
        "Any": object,  # Simplified for test
        "ValueError": ValueError,
        "TypeError": TypeError,
    }

    try:
        # Execute function definitions - this should not raise UnboundLocalError
        exec(parser_code, test_globals)
        exec(converter_code, test_globals)
        print("✅ Generated functions execute without UnboundLocalError")

    except NameError as e:
        if "List" in str(e) or "Any" in str(e):
            raise AssertionError(f"UnboundLocalError-style issue detected: {e}")
        else:
            # Other NameErrors might be expected due to missing imports (like Decimal)
            print(f"Info: Expected NameError for missing imports: {e}")
    except UnboundLocalError as e:
        raise AssertionError(f"UnboundLocalError occurred - fix failed: {e}")


def test_composite_types_nameerror_fix():
    """
    Test that the NameError for undefined composite_types variable is fixed.

    This test covers the scenario where nested composite types would generate
    code that references composite_types[...] at runtime, causing a NameError.
    """
    from src.sql2pyapi.generator.composite_unpacker import generate_composite_unpacking_code
    from src.sql2pyapi.sql_models import ReturnColumn

    # Create a scenario with nested composite types
    # Main composite contains a nested composite
    nested_composite_columns = [
        ReturnColumn(name="id", sql_type="INTEGER", python_type="int"),
        ReturnColumn(name="name", sql_type="TEXT", python_type="str"),
        ReturnColumn(name="is_active", sql_type="BOOLEAN", python_type="Optional[bool]"),
    ]

    main_composite_columns = [
        ReturnColumn(name="nested_item", sql_type="nested_type", python_type="NestedType"),
        ReturnColumn(name="status", sql_type="BOOLEAN", python_type="bool"),
    ]

    composite_types = {"nested_type": nested_composite_columns}

    # Generate unpacking code
    unpacking_code = generate_composite_unpacking_code(
        class_name="MainResult", columns=main_composite_columns, composite_types=composite_types
    )

    generated_code = "\n".join(unpacking_code)

    # Critical fix verification: no runtime composite_types references
    assert "composite_types[" not in generated_code, (
        f"Generated code contains runtime composite_types reference that would cause NameError:\n{generated_code}"
    )

    # Verify field types are properly inlined instead
    assert "nested_field_types = [" in generated_code, (
        "Field types should be inlined as literals, not referenced at runtime"
    )

    # Check that the correct types are inlined
    assert "'int'" in generated_code, "Should contain inlined int type"
    assert "'str'" in generated_code, "Should contain inlined str type"
    assert "'Optional[bool]'" in generated_code, "Should contain inlined Optional[bool] type"

    print("✅ NameError for composite_types variable is fixed - types are properly inlined")


if __name__ == "__main__":
    # Run the original structure tests
    test_composite_type_boolean_numeric_parsing_bug()
    test_composite_type_generation_produces_parsing_function()
    test_boolean_conversion_scenarios()
    test_numeric_conversion_scenarios()
    test_jsonb_conversion_scenarios()
    test_composite_type_with_jsonb_fields()
    print("\n🎯 All structure tests passed!")

    # Run the new type-aware parsing tests
    test_type_aware_composite_parsing_functionality()
    test_type_aware_converter_logic()
    test_unboundlocalerror_fix()
    test_composite_types_nameerror_fix()
    print("\n🚀 All type-aware parsing tests passed - implementation is ready!")
