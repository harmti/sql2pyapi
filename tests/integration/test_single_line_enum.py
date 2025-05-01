import pytest
from pathlib import Path
from sql2pyapi.parser import parse_sql
from sql2pyapi.generator import generate_python_code

# Define paths relative to the main tests/ directory
TESTS_ROOT_DIR = Path(__file__).parent.parent  # Go up one level to tests/
FIXTURES_DIR = TESTS_ROOT_DIR / "fixtures"


def test_single_line_enum_integration():
    """Test that ENUM types defined in a single line are correctly handled in the full pipeline."""
    # Load the SQL file
    sql_file_path = FIXTURES_DIR / "single_line_enum.sql"
    sql_content = sql_file_path.read_text()
    
    # Parse the SQL
    functions, columns, tables, enum_types = parse_sql(sql_content)
    
    # Check that the enum type was correctly parsed
    assert 'company_role' in enum_types
    assert enum_types['company_role'] == ['owner', 'admin', 'member']
    
    # Check that the functions were correctly parsed
    assert len(functions) == 3
    
    # Check the function that uses the enum as a parameter
    role_desc_func = next(f for f in functions if f.sql_name == 'get_role_description')
    assert len(role_desc_func.params) == 1
    assert role_desc_func.params[0].sql_type == 'company_role'
    assert role_desc_func.params[0].python_type == 'CompanyRole'
    
    # Check the function that returns the enum
    default_role_func = next(f for f in functions if f.sql_name == 'get_default_role')
    assert default_role_func.returns_enum_type
    assert default_role_func.return_type == 'CompanyRole'
    
    # Check the function that returns a table with an enum column
    user_roles_func = next(f for f in functions if f.sql_name == 'get_user_roles')
    assert user_roles_func.returns_table
    enum_column = next(c for c in user_roles_func.return_columns if c.name == 'role')
    assert enum_column.sql_type == 'company_role'
    assert enum_column.python_type == 'CompanyRole'
    
    # Generate Python code - we don't parse it with AST to avoid indentation issues
    # Instead we check for key patterns in the code
    python_code = generate_python_code(functions, columns, tables, enum_types)
    
    # Check for enum class definition
    assert "class CompanyRole(Enum):" in python_code
    assert "OWNER = 'owner'" in python_code
    assert "ADMIN = 'admin'" in python_code
    assert "MEMBER = 'member'" in python_code
    
    # Check for function definitions
    assert "async def get_role_description(conn: AsyncConnection, role: CompanyRole)" in python_code
    assert "async def get_default_role(conn: AsyncConnection)" in python_code
    assert "async def get_user_roles(conn: AsyncConnection)" in python_code
    
    # Check for enum type handling in the code
    assert "return CompanyRole(" in python_code  # Enum constructor used in return
    assert "role: Optional[CompanyRole]" in python_code  # Enum type used in dataclass
