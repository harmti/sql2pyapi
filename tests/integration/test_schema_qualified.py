import os
import pytest
from pathlib import Path
from sql2pyapi.parser import parse_sql
from sql2pyapi.generator import generate_python_code


def test_schema_qualified_table_names():
    """Test that schema-qualified table names are handled correctly."""
    # Get the path to the test fixtures directory
    fixtures_dir = Path(__file__).parent.parent / "fixtures"
    schema_qualified_sql_path = fixtures_dir / "schema_qualified.sql"
    
    # Read the SQL file
    with open(schema_qualified_sql_path, "r") as f:
        sql_content = f.read()
    
    # Parse the SQL
    parsed_functions, table_imports, composite_types = parse_sql(sql_content)
    
    # Verify that we found both functions
    assert len(parsed_functions) == 2
    
    # Find the list_companies function
    list_companies = next((f for f in parsed_functions if f.sql_name == "list_companies"), None)
    assert list_companies is not None
    
    # Verify that it returns a SETOF public.companies
    assert list_companies.returns_table is True
    assert list_companies.returns_setof is True
    assert list_companies.setof_table_name == "public.companies"
    
    # Generate Python code
    python_code = generate_python_code(parsed_functions, table_imports, composite_types)
    
    # Verify that the generated code contains the correct class and return type
    assert "class Company:" in python_code
    assert "async def list_companies(conn: AsyncConnection) -> List[Company]:" in python_code
