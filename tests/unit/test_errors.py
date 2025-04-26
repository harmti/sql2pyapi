import pytest
from sql2pyapi.errors import (
    SQL2PyAPIError,
    ParsingError,
    FunctionParsingError,
    TableParsingError,
    TypeMappingError,
    CodeGenerationError,
    ParameterError,
    ReturnTypeError
)


def test_base_error():
    """Test the base SQL2PyAPIError class."""
    error = SQL2PyAPIError("Base error message")
    assert str(error) == "Base error message"
    assert isinstance(error, Exception)


def test_parsing_error_basic():
    """Test ParsingError with basic message."""
    error = ParsingError("Failed to parse SQL")
    assert str(error) == "Failed to parse SQL"
    assert isinstance(error, SQL2PyAPIError)


def test_parsing_error_with_line_number():
    """Test ParsingError with line number."""
    error = ParsingError("Failed to parse SQL", line_number=42)
    assert str(error) == "Failed to parse SQL at line 42"
    assert error.line_number == 42


def test_parsing_error_with_file_name():
    """Test ParsingError with file name."""
    error = ParsingError("Failed to parse SQL", file_name="functions.sql")
    assert str(error) == "Failed to parse SQL in file 'functions.sql'"
    assert error.file_name == "functions.sql"


def test_parsing_error_with_snippet():
    """Test ParsingError with SQL snippet."""
    error = ParsingError("Failed to parse SQL", sql_snippet="CREATE FUNCTION foo()")
    assert str(error) == "Failed to parse SQL: CREATE FUNCTION foo()"
    assert error.sql_snippet == "CREATE FUNCTION foo()"


def test_parsing_error_with_all_details():
    """Test ParsingError with all details."""
    error = ParsingError(
        "Failed to parse SQL",
        sql_snippet="CREATE FUNCTION foo()",
        line_number=42,
        file_name="functions.sql"
    )
    assert str(error) == "Failed to parse SQL in file 'functions.sql' at line 42: CREATE FUNCTION foo()"


def test_parsing_error_truncates_long_snippets():
    """Test that ParsingError truncates long SQL snippets."""
    long_snippet = "SELECT " + "x, " * 100 + "y FROM table"
    error = ParsingError("Failed to parse SQL", sql_snippet=long_snippet)
    assert "..." in str(error)
    assert len(str(error)) < len("Failed to parse SQL: " + long_snippet)


def test_function_parsing_error():
    """Test FunctionParsingError."""
    error = FunctionParsingError(
        "Invalid function definition",
        sql_snippet="CREATE FUNCTION foo() RETURNS void",
        line_number=10
    )
    assert "Invalid function definition" in str(error)
    assert "line 10" in str(error)
    assert isinstance(error, ParsingError)


def test_table_parsing_error():
    """Test TableParsingError."""
    error = TableParsingError(
        "Invalid table definition",
        sql_snippet="CREATE TABLE users (id INT)",
        line_number=20
    )
    assert "Invalid table definition" in str(error)
    assert "line 20" in str(error)
    assert isinstance(error, ParsingError)


def test_type_mapping_error():
    """Test TypeMappingError."""
    error = TypeMappingError("custom_type")
    assert "Unable to map SQL type 'custom_type'" in str(error)
    assert error.sql_type == "custom_type"


def test_type_mapping_error_with_context():
    """Test TypeMappingError with context."""
    error = TypeMappingError("custom_type", context="get_user function")
    assert "in get_user function" in str(error)


def test_code_generation_error():
    """Test CodeGenerationError."""
    error = CodeGenerationError("Failed to generate code")
    assert str(error) == "Failed to generate code"


def test_code_generation_error_with_function_name():
    """Test CodeGenerationError with function name."""
    error = CodeGenerationError("Failed to generate code", function_name="get_user")
    assert "for function 'get_user'" in str(error)


def test_code_generation_error_with_return_type():
    """Test CodeGenerationError with return type."""
    error = CodeGenerationError(
        "Failed to generate code",
        function_name="get_user",
        return_type="TABLE"
    )
    assert "with return type 'TABLE'" in str(error)


def test_parameter_error():
    """Test ParameterError."""
    error = ParameterError("Invalid parameter")
    assert str(error) == "Invalid parameter"


def test_parameter_error_with_details():
    """Test ParameterError with all details."""
    error = ParameterError(
        "Invalid parameter",
        param_name="user_id",
        param_type="uuid",
        function_name="get_user"
    )
    assert "in function 'get_user'" in str(error)
    assert "for parameter 'user_id'" in str(error)
    assert "of type 'uuid'" in str(error)


def test_return_type_error():
    """Test ReturnTypeError."""
    error = ReturnTypeError("Invalid return type")
    assert str(error) == "Invalid return type"


def test_return_type_error_with_details():
    """Test ReturnTypeError with all details."""
    error = ReturnTypeError(
        "Invalid return type",
        return_type="SETOF custom_type",
        function_name="get_users"
    )
    assert "in function 'get_users'" in str(error)
    assert "for return type 'SETOF custom_type'" in str(error)
