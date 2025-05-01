import os
import pytest
import logging
from io import StringIO
from pathlib import Path
from sql2pyapi.parser import parse_sql
from sql2pyapi.generator import generate_python_code


def test_non_qualified_table_no_warnings():
    """Test that non-schema-qualified table names don't generate warnings."""
    # Get the path to the test fixtures directory
    fixtures_dir = Path(__file__).parent.parent / "fixtures"
    sql_path = fixtures_dir / "non_qualified_table.sql"
    
    # Read the SQL file
    with open(sql_path, "r") as f:
        sql_content = f.read()
    
    # Set up logging capture
    log_capture = StringIO()
    handler = logging.StreamHandler(log_capture)
    logger = logging.getLogger()
    logger.addHandler(handler)
    logger.setLevel(logging.WARNING)
    
    try:
        # Parse the SQL
        parsed_functions, table_imports, composite_types, enum_types = parse_sql(sql_content)
        
        # Check the log output for warnings
        log_capture.seek(0)
        log_output = log_capture.read()
        assert "Unknown SQL type: users" not in log_output, \
            "Should not warn about unknown SQL type for table names"
        
        # Verify that we found the function
        assert len(parsed_functions) == 1
        
        # Find the get_user_by_clerk_id function
        function = parsed_functions[0]
        assert function.sql_name == "get_user_by_clerk_id"
        
        # Verify that it returns a SETOF users
        assert function.returns_table is True
        assert function.returns_setof is True
        assert function.setof_table_name == "users"
        
        # Generate Python code
        python_code = generate_python_code(parsed_functions, table_imports, composite_types, enum_types)
        
        # Verify that the generated code contains the correct class and return type
        assert "class User:" in python_code
        assert "async def get_user_by_clerk_id(conn: AsyncConnection, clerk_id: str) -> List[User]:" in python_code
    finally:
        # Clean up the logger
        logger.removeHandler(handler)
