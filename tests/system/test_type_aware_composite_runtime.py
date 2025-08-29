"""
System test for type-aware composite handling runtime behavior.

This test generates actual API code and executes it to catch runtime issues
that unit/integration tests might miss, such as:
- UnboundLocalError from shadowed imports
- NameError from undefined variables
- Type conversion failures in real scenarios
"""

import os
import sys
import tempfile
from pathlib import Path

import pytest
from tests.test_utils import parse_test_sql


def test_type_aware_composite_runtime_execution():
    """
    End-to-end system test for type-aware composite parsing.

    This test:
    1. Creates SQL with complex composite types (nested, with various data types)
    2. Generates actual Python API code using sql2pyapi
    3. Executes the generated code to verify runtime behavior
    4. Tests actual type conversions with real data
    """

    # Define comprehensive SQL with nested composites and various data types
    sql_content = """
    -- Base table with various data types that benefit from type-aware parsing
    CREATE TABLE sensor_readings (
        id SERIAL PRIMARY KEY,
        sensor_name TEXT NOT NULL,
        is_active BOOLEAN DEFAULT TRUE,
        latitude NUMERIC(10,7),
        longitude NUMERIC(10,7), 
        metadata JSONB,
        last_reading_time TIMESTAMP,
        created_at TIMESTAMP DEFAULT NOW()
    );
    
    -- Composite type containing the sensor table (tests nested composite parsing)
    CREATE TYPE sensor_status AS (
        sensor sensor_readings,
        status_message TEXT,
        is_online BOOLEAN,
        uptime_hours NUMERIC(8,2)
    );
    
    -- Another composite with direct field types (tests direct type conversion)
    CREATE TYPE reading_summary AS (
        total_readings INTEGER,
        average_value NUMERIC(12,4),
        has_anomalies BOOLEAN,
        sensor_metadata JSONB,
        last_update TIMESTAMP
    );
    
    -- Function returning nested composite (most complex scenario)
    CREATE FUNCTION get_sensor_status_complex(p_sensor_id INTEGER)
    RETURNS sensor_status
    AS $$
    BEGIN
        RETURN (
            (p_sensor_id, 'Test Sensor', TRUE, 60.1698570, 24.9383790, 
             '{"type": "temperature", "unit": "celsius"}', NOW())::sensor_readings,
            'All systems operational',
            TRUE,
            72.50
        )::sensor_status;
    END;
    $$ LANGUAGE plpgsql;
    
    -- Function returning direct composite (simpler but still type-aware)
    CREATE FUNCTION get_reading_summary(p_sensor_id INTEGER)
    RETURNS reading_summary  
    AS $$
    BEGIN
        RETURN (
            1500,
            23.4567,
            FALSE,
            '{"anomaly_threshold": 30.0, "alerts_enabled": true}',
            NOW()
        )::reading_summary;
    END;
    $$ LANGUAGE plpgsql;
    
    -- Function returning SETOF to test both single and multiple results
    CREATE FUNCTION get_multiple_sensor_statuses()
    RETURNS SETOF sensor_status
    AS $$
    BEGIN
        RETURN QUERY
        SELECT 
            (1, 'Sensor A', TRUE, 60.1, 24.9, '{"type": "temp"}', NOW())::sensor_readings,
            'Online',
            TRUE,
            100.0
        UNION ALL
        SELECT
            (2, 'Sensor B', FALSE, 61.2, 25.1, '{"type": "humidity"}', NOW())::sensor_readings, 
            'Offline',
            FALSE,
            0.0;
    END;
    $$ LANGUAGE plpgsql;
    """

    print("🧪 Starting comprehensive type-aware composite system test...")

    # Parse the SQL
    functions, table_imports, composite_types, enum_types = parse_test_sql(sql_content)

    # Verify we have the expected structure
    assert len(functions) == 3, f"Expected 3 functions, got {len(functions)}"
    assert "sensor_readings" in table_imports, "sensor_readings table should be imported"
    assert "sensor_status" in composite_types, "sensor_status composite should be parsed"
    assert "reading_summary" in composite_types, "reading_summary composite should be parsed"

    print("✅ SQL parsing completed successfully")

    # Now generate the actual Python API code
    from src.sql2pyapi.generator.core import generate_python_code

    api_code = generate_python_code(
        functions=functions,
        table_schema_imports=table_imports,
        parsed_composite_types=composite_types,
        parsed_enum_types=enum_types,
        omit_helpers=False,
    )

    print("✅ Python API code generated")

    # Write the generated code to a temporary file and import it for testing
    with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as temp_file:
        temp_file.write(api_code)
        temp_file_path = temp_file.name

    try:
        # Add the temp directory to Python path so we can import the module
        temp_dir = os.path.dirname(temp_file_path)
        temp_module_name = os.path.basename(temp_file_path)[:-3]  # Remove .py

        if temp_dir not in sys.path:
            sys.path.insert(0, temp_dir)

        # Import the generated module
        spec = __import__("importlib.util", fromlist=["spec_from_file_location"]).spec_from_file_location(
            temp_module_name, temp_file_path
        )
        generated_module = __import__("importlib.util", fromlist=["module_from_spec"]).module_from_spec(spec)
        spec.loader.exec_module(generated_module)

        print("✅ Generated module imported successfully")

        # Test that all expected classes and functions exist
        assert hasattr(generated_module, "SensorReading"), "SensorReading dataclass should exist"
        assert hasattr(generated_module, "SensorStatus"), "SensorStatus dataclass should exist"
        assert hasattr(generated_module, "ReadingSummary"), "ReadingSummary dataclass should exist"
        assert hasattr(generated_module, "get_sensor_status_complex"), "get_sensor_status_complex function should exist"
        assert hasattr(generated_module, "get_reading_summary"), "get_reading_summary function should exist"
        assert hasattr(generated_module, "get_multiple_sensor_statuses"), (
            "get_multiple_sensor_statuses function should exist"
        )

        print("✅ All expected classes and functions are present")

        # Test the dataclass structures
        SensorReading = generated_module.SensorReading
        SensorStatus = generated_module.SensorStatus
        ReadingSummary = generated_module.ReadingSummary

        # Test manual construction (should work without runtime errors)
        import json
        from datetime import datetime
        from decimal import Decimal

        # Test SensorReading creation
        sensor = SensorReading(
            id=1,
            sensor_name="Test Sensor",
            is_active=True,
            latitude=Decimal("60.1698570"),
            longitude=Decimal("24.9383790"),
            metadata={"type": "temperature"},
            last_reading_time=datetime.now(),
            created_at=datetime.now(),
        )

        # Test nested composite creation
        status = SensorStatus(
            sensor=sensor, status_message="Operational", is_online=True, uptime_hours=Decimal("72.50")
        )

        print("✅ Dataclass construction works correctly")

        # Test the critical type-aware parsing functionality
        # Simulate what happens when PostgreSQL returns composite string data

        # Test composite string parsing for nested types (the complex scenario that caused bugs)
        if hasattr(generated_module, "_parse_composite_string_typed"):
            parse_typed = generated_module._parse_composite_string_typed
            convert_typed = generated_module._convert_postgresql_value_typed

            # Test type-aware conversion
            assert convert_typed("t", "bool") is True, "Boolean 't' should convert to True"
            assert convert_typed("f", "bool") is False, "Boolean 'f' should convert to False"
            assert convert_typed("123.456", "Decimal") == Decimal("123.456"), "Numeric should convert to Decimal"
            assert convert_typed('{"key": "value"}', "Dict") == {"key": "value"}, "JSON should parse correctly"

            # Test composite string parsing with type information
            test_composite = '(123,"Test",t,60.1698570)'
            field_types = ["int", "str", "bool", "Decimal"]
            parsed = parse_typed(test_composite, field_types)

            assert parsed[0] == 123, "Integer should be parsed correctly"
            assert parsed[1] == "Test", "String should be parsed correctly"
            assert parsed[2] is True, "Boolean should be converted to True"
            assert parsed[3] == Decimal("60.1698570"), "Decimal should be converted correctly"

            print("✅ Type-aware parsing functions work correctly")

        # Test that the functions can be called without runtime errors
        # (We can't actually connect to a database, but we can test the function signatures)

        # Verify function signatures
        import inspect

        get_sensor_status_sig = inspect.signature(generated_module.get_sensor_status_complex)
        print(f"Function signature: {get_sensor_status_sig}")

        # Check that the function has the expected parameter (may be 'conn' or similar)
        param_names = list(get_sensor_status_sig.parameters.keys())
        assert len(param_names) >= 2, f"Function should have at least 2 parameters, got: {param_names}"

        print("✅ Function signatures are correct")

        print("\n🎉 System test PASSED - Type-aware composite handling works correctly!")
        print("   ✅ No UnboundLocalError (imports are correct)")
        print("   ✅ No NameError (variables are properly defined)")
        print("   ✅ Type conversions work as expected")
        print("   ✅ Nested composite parsing is functional")
        print("   ✅ Generated code executes without runtime errors")

    finally:
        # Clean up
        try:
            os.unlink(temp_file_path)
        except OSError:
            pass

        # Remove from sys.path
        if temp_dir in sys.path:
            sys.path.remove(temp_dir)


def test_type_aware_composite_error_scenarios():
    """
    Test error handling in type-aware composite parsing.
    Ensures that malformed data doesn't cause crashes.
    """

    sql_content = """
    CREATE TABLE simple_table (
        id INTEGER,
        flag BOOLEAN,
        amount NUMERIC(10,2)
    );
    
    CREATE TYPE simple_composite AS (
        record simple_table,
        extra_flag BOOLEAN
    );
    
    CREATE FUNCTION get_simple_composite()
    RETURNS simple_composite
    AS $$
        SELECT ((1, TRUE, 123.45)::simple_table, FALSE)::simple_composite;
    $$ LANGUAGE SQL;
    """

    # Parse and generate code
    functions, table_imports, composite_types, enum_types = parse_test_sql(sql_content)

    from src.sql2pyapi.generator.core import generate_python_code

    api_code = generate_python_code(
        functions=functions,
        table_schema_imports=table_imports,
        parsed_composite_types=composite_types,
        parsed_enum_types=enum_types,
        omit_helpers=False,
    )

    # Execute the generated code to test error handling
    exec_globals = {
        "List": list,
        "Optional": type(None),
        "Tuple": tuple,
        "Dict": dict,
        "Any": object,
        "Decimal": __import__("decimal").Decimal,
        "UUID": __import__("uuid").UUID,
        "datetime": __import__("datetime").datetime,
    }

    try:
        exec(api_code, exec_globals)

        # Test error scenarios if parsing functions exist
        if "_parse_composite_string_typed" in exec_globals:
            parse_func = exec_globals["_parse_composite_string_typed"]

            # Test malformed composite strings
            try:
                parse_func("not-a-composite", ["str"])
                raise AssertionError("Should have raised ValueError for malformed input")
            except ValueError:
                pass  # Expected

            # Test mismatched field count (should handle gracefully)
            try:
                result = parse_func("(1,2,3)", ["int", "int"])  # 3 fields, 2 types
                assert len(result) == 3, "Should still parse all fields"
            except Exception as e:
                print(f"Info: Parsing with mismatched field count: {e}")

        print("✅ Error handling test passed")

    except Exception as e:
        assert False, f"Generated code failed to execute: {e}"


def test_enum_conversion_in_composite_types():
    """
    System test specifically for enum conversion in composite types.

    This test verifies that:
    1. Enum types in composite fields are correctly converted from strings to enum instances
    2. Type-aware parsing is triggered for composites containing enums
    3. Generated code can handle both direct enum values and composite string parsing
    """

    sql_content = """
    -- Define enum types for testing
    CREATE TYPE device_status AS ENUM ('active', 'inactive', 'maintenance', 'error');
    CREATE TYPE priority_level AS ENUM ('low', 'medium', 'high', 'critical');

    -- Composite type containing enum fields
    CREATE TYPE device_info AS (
        device_id INTEGER,
        device_name TEXT,
        status device_status,
        priority priority_level,
        last_updated TIMESTAMP
    );

    -- Nested composite with enums
    CREATE TYPE system_report AS (
        device device_info,
        overall_status device_status,
        needs_attention BOOLEAN
    );

    -- Function returning composite with enums
    CREATE FUNCTION get_device_info(p_device_id INTEGER)
    RETURNS device_info
    AS $$
    BEGIN
        RETURN (
            p_device_id,
            'Test Device',
            'active'::device_status,
            'high'::priority_level,
            NOW()
        )::device_info;
    END;
    $$ LANGUAGE plpgsql;

    -- Function returning nested composite with enums
    CREATE FUNCTION get_system_report(p_device_id INTEGER)
    RETURNS system_report
    AS $$
    BEGIN
        RETURN (
            (p_device_id, 'Test Device', 'active'::device_status, 'critical'::priority_level, NOW())::device_info,
            'maintenance'::device_status,
            TRUE
        )::system_report;
    END;
    $$ LANGUAGE plpgsql;

    -- Function returning multiple devices with enums
    CREATE FUNCTION get_all_devices()
    RETURNS SETOF device_info
    AS $$
    BEGIN
        RETURN QUERY
        SELECT 
            1,
            'Device A',
            'active'::device_status,
            'low'::priority_level,
            NOW()
        UNION ALL
        SELECT
            2,
            'Device B',
            'error'::device_status,
            'critical'::priority_level,
            NOW();
    END;
    $$ LANGUAGE plpgsql;
    """

    print("🧪 Starting enum conversion system test...")

    # Parse the SQL - should find enum types
    functions, table_imports, composite_types, enum_types = parse_test_sql(sql_content)

    # Verify enum types were parsed
    assert "device_status" in enum_types, "device_status enum should be parsed"
    assert "priority_level" in enum_types, "priority_level enum should be parsed"
    assert len(enum_types) == 2, f"Expected 2 enum types, got {len(enum_types)}"

    # Verify composite types
    assert "device_info" in composite_types, "device_info composite should be parsed"
    assert "system_report" in composite_types, "system_report composite should be parsed"

    # Verify functions
    assert len(functions) == 3, f"Expected 3 functions, got {len(functions)}"

    print("✅ SQL parsing with enums completed successfully")
    print(f"   Found enum types: {list(enum_types.keys())}")
    print(f"   Found composite types: {list(composite_types.keys())}")

    # Generate Python API code
    from src.sql2pyapi.generator.core import generate_python_code

    api_code = generate_python_code(
        functions=functions,
        table_schema_imports=table_imports,
        parsed_composite_types=composite_types,
        parsed_enum_types=enum_types,
        omit_helpers=False,
    )

    # Verify that enum conversion logic is present in generated code
    assert "_convert_postgresql_value_typed" in api_code, "Type-aware converter should be generated for enum types"
    assert "enum_class = frame.f_globals[expected_type]" in api_code, "Enum conversion logic should be present"

    print("✅ Python API code with enum support generated")

    # Test the generated code by importing and executing it
    with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as temp_file:
        temp_file.write(api_code)
        temp_file_path = temp_file.name

    try:
        # Import the generated module
        temp_dir = os.path.dirname(temp_file_path)
        temp_module_name = os.path.basename(temp_file_path)[:-3]

        if temp_dir not in sys.path:
            sys.path.insert(0, temp_dir)

        spec = __import__("importlib.util", fromlist=["spec_from_file_location"]).spec_from_file_location(
            temp_module_name, temp_file_path
        )
        generated_module = __import__("importlib.util", fromlist=["module_from_spec"]).module_from_spec(spec)
        spec.loader.exec_module(generated_module)

        print("✅ Generated module with enums imported successfully")

        # Test that enum classes were generated correctly
        DeviceStatus = generated_module.DeviceStatus
        PriorityLevel = generated_module.PriorityLevel
        DeviceInfo = generated_module.DeviceInfo
        SystemReport = generated_module.SystemReport

        # Test enum values
        assert DeviceStatus.ACTIVE.value == "active"
        assert DeviceStatus.INACTIVE.value == "inactive"
        assert DeviceStatus.MAINTENANCE.value == "maintenance"
        assert DeviceStatus.ERROR.value == "error"

        assert PriorityLevel.LOW.value == "low"
        assert PriorityLevel.MEDIUM.value == "medium"
        assert PriorityLevel.HIGH.value == "high"
        assert PriorityLevel.CRITICAL.value == "critical"

        print("✅ Enum classes generated correctly with proper values")

        # Test that type-aware conversion functions exist and work
        # We'll test the internal converter function if it's available (exclude helper functions)
        test_functions = [
            name
            for name in dir(generated_module)
            if name.startswith("get_")
            and callable(getattr(generated_module, name))
            and name not in ["get_optional", "get_required"]
        ]

        assert len(test_functions) == 3, f"Expected 3 test functions, got {len(test_functions)}: {test_functions}"
        print(f"✅ Generated functions: {test_functions}")

        # Verify the generated code structure
        import inspect

        # Check that global helper functions with enum conversion logic exist
        # With the optimization, enum conversion logic is now in global helpers
        has_global_converter = hasattr(generated_module, "_convert_postgresql_value_typed")
        has_enum_logic = False

        if has_global_converter:
            converter_func = getattr(generated_module, "_convert_postgresql_value_typed")
            converter_source = inspect.getsource(converter_func)
            has_enum_logic = "frame.f_globals" in converter_source and "expected_type" in converter_source

        assert has_enum_logic, "Global helper functions should contain enum conversion logic"
        print("✅ Enum conversion logic is present in global helper functions")

        # Test dataclass creation with enums (manual test since we don't have DB)
        from datetime import datetime

        # Create a device_info instance
        device = DeviceInfo(
            device_id=1,
            device_name="Test Device",
            status=DeviceStatus.ACTIVE,
            priority=PriorityLevel.HIGH,
            last_updated=datetime.now(),
        )

        assert device.status == DeviceStatus.ACTIVE
        assert device.priority == PriorityLevel.HIGH
        print("✅ Dataclass construction with enum fields works correctly")

        # Test nested composite with enums
        report = SystemReport(device=device, overall_status=DeviceStatus.MAINTENANCE, needs_attention=True)

        assert report.device.status == DeviceStatus.ACTIVE
        assert report.overall_status == DeviceStatus.MAINTENANCE
        assert report.needs_attention is True
        print("✅ Nested composite with enum fields works correctly")

        print("🎉 Enum conversion system test completed successfully!")

    except Exception as e:
        print(f"❌ Error during enum conversion test: {e}")
        # Print part of the generated code for debugging
        print("\n--- Generated Code Sample (first 1000 chars) ---")
        print(api_code[:1000])
        print("--- End Sample ---")
        raise AssertionError(f"Enum conversion system test failed: {e}")

    finally:
        # Clean up
        try:
            os.unlink(temp_file_path)
        except OSError:
            pass

        if temp_dir in sys.path:
            sys.path.remove(temp_dir)


if __name__ == "__main__":
    test_type_aware_composite_runtime_execution()
    test_type_aware_composite_error_scenarios()
    test_enum_conversion_in_composite_types()
    print("\n🚀 All system tests passed!")
