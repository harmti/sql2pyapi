"""
Integration test for Bug Report 1: JSON Default Value Parsing Creates Phantom Columns

This test ensures that complex JSON default values in CREATE TABLE statements
are parsed correctly without creating phantom columns.
"""

from sql2pyapi.parser.parser import SQLParser


def test_json_default_value_parsing():
    """
    Test that complex JSON default values don't create phantom columns in dataclasses.

    Bug: sql2pyapi incorrectly parses complex JSON default values and creates phantom columns
    in dataclasses when the JSON contains certain keywords like "backoff".

    Root cause: The column fragment splitting logic only handled parentheses () but not
    quotes '' or curly braces {}, causing JSON strings with commas to be split incorrectly.
    """

    # SQL from the bug report - contains JSON with comma inside quotes
    schema_sql = """
    CREATE TABLE async_processes (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        retry_strategy JSONB DEFAULT '{"max_retries": 3, "backoff": "exponential"}'::jsonb
    );
    """

    # Parse the schema using parser directly to access table schemas
    parser = SQLParser()
    parser._parse_create_table(schema_sql)

    # Should have exactly 1 table
    assert len(parser.table_schemas) == 1, f"Expected 1 table, got {len(parser.table_schemas)}"

    # Get the table
    table_name = next(iter(parser.table_schemas.keys()))
    table_columns = parser.table_schemas[table_name]

    assert table_name == "async_processes", f"Expected table name 'async_processes', got '{table_name}'"

    # The table should have exactly 2 columns, not more
    assert len(table_columns) == 2, (
        f"Expected 2 columns, got {len(table_columns)}: {[col.name for col in table_columns]}"
    )

    # Check column names and types
    column_names = [col.name for col in table_columns]
    assert "id" in column_names, f"Missing 'id' column in {column_names}"
    assert "retry_strategy" in column_names, f"Missing 'retry_strategy' column in {column_names}"

    # Verify column details
    id_col = next(col for col in table_columns if col.name == "id")
    retry_col = next(col for col in table_columns if col.name == "retry_strategy")

    assert id_col.sql_type == "UUID", f"Expected UUID type for id, got {id_col.sql_type}"
    assert retry_col.sql_type == "JSONB", f"Expected JSONB type for retry_strategy, got {retry_col.sql_type}"

    # No phantom columns should be created from JSON parsing
    for col in table_columns:
        assert col.name in ["id", "retry_strategy"], f"Unexpected phantom column: {col.name}"


def test_json_default_value_parsing_multiple_nested():
    """
    Test more complex JSON default values with nested structures and multiple commas.
    """

    schema_sql = """
    CREATE TABLE complex_config (
        id SERIAL PRIMARY KEY,
        config JSONB DEFAULT '{"database": {"host": "localhost", "port": 5432}, "cache": {"redis": {"url": "redis://localhost", "timeout": 30}}}'::jsonb,
        metadata TEXT DEFAULT 'test'
    );
    """

    # Parse the schema
    parser = SQLParser()
    parser._parse_create_table(schema_sql)

    # Should have exactly 1 table with 3 columns
    assert len(parser.table_schemas) == 1
    table_name = next(iter(parser.table_schemas.keys()))
    table_columns = parser.table_schemas[table_name]

    assert table_name == "complex_config"
    assert len(table_columns) == 3, (
        f"Expected 3 columns, got {len(table_columns)}: {[col.name for col in table_columns]}"
    )

    column_names = [col.name for col in table_columns]
    assert set(column_names) == {"id", "config", "metadata"}, f"Unexpected columns: {column_names}"


def test_quoted_strings_with_commas():
    """
    Test that quoted strings containing commas don't cause column splitting issues.
    """

    schema_sql = """
    CREATE TABLE quoted_defaults (
        id SERIAL PRIMARY KEY,
        description TEXT DEFAULT 'This is a test, with commas, and more text',
        tags TEXT DEFAULT 'tag1,tag2,tag3'
    );
    """

    # Parse the schema
    parser = SQLParser()
    parser._parse_create_table(schema_sql)

    # Should have exactly 1 table with 3 columns
    assert len(parser.table_schemas) == 1
    table_name = next(iter(parser.table_schemas.keys()))
    table_columns = parser.table_schemas[table_name]

    assert table_name == "quoted_defaults"
    assert len(table_columns) == 3, (
        f"Expected 3 columns, got {len(table_columns)}: {[col.name for col in table_columns]}"
    )

    column_names = [col.name for col in table_columns]
    assert set(column_names) == {"id", "description", "tags"}, f"Unexpected columns: {column_names}"
