"""
Integration test for UUID serialization in JSONB parameters.

This test ensures that JSONB parameters containing UUID values are properly
serialized using a custom JSON encoder, preventing TypeError exceptions.
"""
import pytest
import uuid
from datetime import datetime
from sql2pyapi.parser import parse_sql
from sql2pyapi.generator.core import generate_python_code


def test_uuid_jsonb_serialization_basic():
    """
    Test that functions with JSONB parameters generate code that can serialize UUIDs.
    """
    
    # SQL function that accepts JSONB parameter
    sql_code = """
    CREATE OR REPLACE FUNCTION create_async_process(
        p_context JSONB DEFAULT '{}'
    ) RETURNS INTEGER
    AS $$
        SELECT 1;
    $$ LANGUAGE SQL;
    """
    
    # Parse SQL and generate Python code
    functions, table_schemas, composite_types, enum_types = parse_sql(sql_code)
    generated_code = generate_python_code(functions, table_schemas, composite_types, enum_types)
    
    # Verify that DatabaseJSONEncoder is included
    assert "class DatabaseJSONEncoder(json.JSONEncoder):" in generated_code
    assert "if isinstance(obj, UUID):" in generated_code
    assert "return str(obj)" in generated_code
    
    # Verify that the encoder is used in the function
    assert "json.dumps(context, cls=DatabaseJSONEncoder)" in generated_code


def test_uuid_jsonb_serialization_with_datetime():
    """
    Test that the JSON encoder handles both UUIDs and datetime objects.
    """
    
    sql_code = """
    CREATE OR REPLACE FUNCTION update_process_metadata(
        p_metadata JSONB
    ) RETURNS VOID
    AS $$
        -- Function body
    $$ LANGUAGE SQL;
    """
    
    # Parse SQL and generate Python code
    functions, table_schemas, composite_types, enum_types = parse_sql(sql_code)
    generated_code = generate_python_code(functions, table_schemas, composite_types, enum_types)
    
    # Verify that both UUID and datetime handling are included
    assert "if isinstance(obj, UUID):" in generated_code
    assert "elif isinstance(obj, datetime):" in generated_code
    assert "return obj.isoformat()" in generated_code


def test_multiple_jsonb_parameters():
    """
    Test that functions with multiple JSONB parameters all use the custom encoder.
    """
    
    sql_code = """
    CREATE OR REPLACE FUNCTION complex_function(
        p_config JSONB,
        p_metadata JSONB DEFAULT NULL,
        p_user_id INTEGER
    ) RETURNS TEXT
    AS $$
        SELECT 'result';
    $$ LANGUAGE SQL;
    """
    
    # Parse SQL and generate Python code
    functions, table_schemas, composite_types, enum_types = parse_sql(sql_code)
    generated_code = generate_python_code(functions, table_schemas, composite_types, enum_types)
    
    # Verify encoder is defined once
    encoder_count = generated_code.count("class DatabaseJSONEncoder(json.JSONEncoder):")
    assert encoder_count == 1, f"Expected 1 encoder definition, got {encoder_count}"
    
    # Verify both JSONB parameters use the encoder
    assert "json.dumps(config, cls=DatabaseJSONEncoder)" in generated_code
    assert "json.dumps(metadata, cls=DatabaseJSONEncoder)" in generated_code


def test_no_jsonb_parameters_no_encoder():
    """
    Test that functions without JSONB parameters don't include the encoder.
    """
    
    sql_code = """
    CREATE OR REPLACE FUNCTION simple_function(
        p_user_id INTEGER,
        p_name TEXT
    ) RETURNS TEXT
    AS $$
        SELECT p_name;
    $$ LANGUAGE SQL;
    """
    
    # Parse SQL and generate Python code
    functions, table_schemas, composite_types, enum_types = parse_sql(sql_code)
    generated_code = generate_python_code(functions, table_schemas, composite_types, enum_types)
    
    # Verify no encoder is included
    assert "class DatabaseJSONEncoder" not in generated_code
    assert "json.dumps" not in generated_code


def test_json_and_jsonb_both_supported():
    """
    Test that both JSON and JSONB types use the custom encoder.
    """
    
    sql_code = """
    CREATE OR REPLACE FUNCTION mixed_json_function(
        p_json_data JSON,
        p_jsonb_data JSONB
    ) RETURNS INTEGER
    AS $$
        SELECT 1;
    $$ LANGUAGE SQL;
    """
    
    # Parse SQL and generate Python code
    functions, table_schemas, composite_types, enum_types = parse_sql(sql_code)
    generated_code = generate_python_code(functions, table_schemas, composite_types, enum_types)
    
    # Verify both parameters use the encoder
    assert "json.dumps(json_data, cls=DatabaseJSONEncoder)" in generated_code
    assert "json.dumps(jsonb_data, cls=DatabaseJSONEncoder)" in generated_code


def test_generated_code_syntax_valid():
    """
    Test that the generated code with UUID encoder is syntactically valid Python.
    """
    
    sql_code = """
    CREATE OR REPLACE FUNCTION test_function(
        p_context JSONB
    ) RETURNS INTEGER
    AS $$
        SELECT 1;
    $$ LANGUAGE SQL;
    """
    
    # Parse SQL and generate Python code
    functions, table_schemas, composite_types, enum_types = parse_sql(sql_code)
    generated_code = generate_python_code(functions, table_schemas, composite_types, enum_types)
    
    # Verify the code compiles without syntax errors
    try:
        compile(generated_code, '<generated>', 'exec')
    except SyntaxError as e:
        pytest.fail(f"Generated code has syntax errors: {e}")


def test_encoder_imports_included():
    """
    Test that all necessary imports are included when JSON encoder is added.
    """
    
    sql_code = """
    CREATE OR REPLACE FUNCTION test_function(
        p_data JSONB
    ) RETURNS INTEGER
    AS $$
        SELECT 1;
    $$ LANGUAGE SQL;
    """
    
    # Parse SQL and generate Python code
    functions, table_schemas, composite_types, enum_types = parse_sql(sql_code)
    generated_code = generate_python_code(functions, table_schemas, composite_types, enum_types)
    
    # Verify necessary imports are present
    assert "import json" in generated_code
    assert "from uuid import UUID" in generated_code
    assert "from datetime import datetime" in generated_code


def test_encoder_handles_nested_structures():
    """
    Test that the encoder description suggests it handles nested structures.
    This is more of a documentation test since the actual nested handling
    would need runtime testing with real data.
    """
    
    sql_code = """
    CREATE OR REPLACE FUNCTION test_function(
        p_nested_data JSONB
    ) RETURNS INTEGER
    AS $$
        SELECT 1;
    $$ LANGUAGE SQL;
    """
    
    # Parse SQL and generate Python code
    functions, table_schemas, composite_types, enum_types = parse_sql(sql_code)
    generated_code = generate_python_code(functions, table_schemas, composite_types, enum_types)
    
    # The encoder should be designed to handle nested structures
    # by calling super().default(obj) for unknown types
    assert "return super().default(obj)" in generated_code