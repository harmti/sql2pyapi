"""Integration tests for INTERVAL type support.

Tests that INTERVAL types are properly mapped to Python timedelta
and that the generated code works correctly.
"""

import asyncio
from datetime import timedelta
from unittest.mock import AsyncMock

import psycopg

from tests.test_utils import create_test_function
from tests.test_utils import parse_test_sql


def test_interval_type_parameter():
    """Test function with INTERVAL parameter."""
    sql = create_test_function("delay_process", "p_retry_after INTERVAL", returns="TEXT")

    functions, _, _, _ = parse_test_sql(sql)
    func = functions[0]

    # Check parameter mapping
    assert len(func.params) == 1
    param = func.params[0]
    assert param.sql_type == "INTERVAL"
    assert param.python_type == "timedelta"
    assert "timedelta" in func.required_imports


def test_interval_type_return():
    """Test function returning INTERVAL."""
    sql = create_test_function("get_process_duration", "p_process_id UUID", returns="INTERVAL")

    functions, _, _, _ = parse_test_sql(sql)
    func = functions[0]

    # Check return type mapping
    assert func.return_type == "Optional[timedelta]"
    assert "timedelta" in func.required_imports
    assert "Optional" in func.required_imports


def test_interval_array_parameter():
    """Test function with INTERVAL array parameter."""
    sql = create_test_function("batch_delay_processes", "p_retry_intervals INTERVAL[]", returns="INTEGER")

    functions, _, _, _ = parse_test_sql(sql)
    func = functions[0]

    # Check parameter mapping
    assert len(func.params) == 1
    param = func.params[0]
    assert param.sql_type == "INTERVAL[]"
    assert param.python_type == "List[timedelta]"
    assert "timedelta" in func.required_imports
    assert "List" in func.required_imports


def test_interval_setof_return():
    """Test function returning SETOF INTERVAL."""
    sql = create_test_function("get_all_durations", "p_limit INTEGER DEFAULT 10", returns="SETOF INTERVAL")

    functions, _, _, _ = parse_test_sql(sql)
    func = functions[0]

    # Check return type mapping
    assert func.return_type == "List[timedelta]"
    assert func.returns_setof is True
    assert "timedelta" in func.required_imports
    assert "List" in func.required_imports


def test_interval_optional_parameter():
    """Test function with optional INTERVAL parameter."""
    sql = create_test_function("process_with_timeout", "p_timeout INTERVAL DEFAULT NULL", returns="BOOLEAN")

    functions, _, _, _ = parse_test_sql(sql)
    func = functions[0]

    # Check parameter mapping
    assert len(func.params) == 1
    param = func.params[0]
    assert param.sql_type == "INTERVAL"
    assert param.python_type == "Optional[timedelta]"
    assert param.is_optional is True
    assert "timedelta" in func.required_imports
    assert "Optional" in func.required_imports


def test_interval_code_generation():
    """Test that generated code includes proper imports and works with timedelta."""
    from sql2pyapi.generator.core import generate_python_code

    sql = """
    CREATE FUNCTION fail_async_process(
        p_process_id UUID,
        p_retry_after INTERVAL DEFAULT INTERVAL '1 hour'
    )
    RETURNS BOOLEAN
    AS $$
        -- Mock function body
        SELECT true;
    $$ LANGUAGE SQL;
    """

    functions, _, _, _ = parse_test_sql(sql)

    # Generate the Python code
    generated_code = generate_python_code(functions, {}, {}, {})

    # Check that timedelta is imported
    assert "from datetime import timedelta" in generated_code

    # Check that the function signature uses timedelta
    assert "retry_after: Optional[timedelta] = None" in generated_code


def test_interval_with_mock_database():
    """Test INTERVAL functionality with mocked database calls."""
    sql = """
    CREATE FUNCTION get_retry_interval(p_attempt INTEGER)
    RETURNS INTERVAL
    AS $$
        SELECT INTERVAL '5 minutes' * p_attempt;
    $$ LANGUAGE SQL;
    """

    # Parse and generate
    functions, _, _, _ = parse_test_sql(sql)
    from sql2pyapi.generator.core import generate_python_code

    generated_code = generate_python_code(functions, {}, {}, {})

    # Execute the generated code to get the function
    namespace = {}
    exec(generated_code, namespace)
    get_retry_interval = namespace["get_retry_interval"]

    async def test_execution():
        # Mock database connection and cursor
        mock_conn = AsyncMock(spec=psycopg.AsyncConnection)
        mock_cursor = AsyncMock(spec=psycopg.AsyncCursor)

        # Mock the return value as a timedelta
        expected_interval = timedelta(minutes=15)  # 5 minutes * 3 attempts
        mock_cursor.fetchone.return_value = (expected_interval,)

        mock_conn.cursor.return_value.__aenter__.return_value = mock_cursor
        mock_conn.cursor.return_value.__aexit__.return_value = None

        # Call the generated function
        result = await get_retry_interval(mock_conn, attempt=3)

        # Verify the result
        assert result == expected_interval
        assert isinstance(result, timedelta)

        # Verify the SQL call was made correctly
        mock_cursor.execute.assert_called_once()
        call_args = mock_cursor.execute.call_args[0]
        assert "get_retry_interval" in call_args[0]
        # Check that parameters are passed as expected (using named parameters)
        assert call_args[1] == {"attempt": 3}

    # Run the async test
    asyncio.run(test_execution())


if __name__ == "__main__":
    # Run individual tests for debugging
    test_interval_type_parameter()
    test_interval_type_return()
    test_interval_array_parameter()
    test_interval_setof_return()
    test_interval_optional_parameter()
    test_interval_code_generation()
    test_interval_with_mock_database()
    print("All INTERVAL type tests passed!")
