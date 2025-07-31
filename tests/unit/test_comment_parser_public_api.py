"""Tests for comment parsing functionality using the public API.

These tests verify that SQL comments are correctly parsed and associated
with functions using only the public API.
"""


# Import the public API

# Import test utilities
from tests.test_utils import find_function
from tests.test_utils import parse_test_sql


def test_single_line_comments():
    """Test parsing of single-line comments before functions."""
    # Create test SQL with single-line comments
    sql = """
    -- This is a simple function
    -- It returns an integer
    CREATE FUNCTION get_answer()
    RETURNS integer
    LANGUAGE sql AS $$
        SELECT 42;
    $$;

    -- Another function with a single comment line
    CREATE FUNCTION get_greeting(p_name text)
    RETURNS text
    LANGUAGE sql AS $$
        SELECT 'Hello, ' || p_name;
    $$;
    """

    # Parse the SQL
    functions, _, _, _ = parse_test_sql(sql)

    # Verify comment parsing
    get_answer = find_function(functions, "get_answer")
    assert get_answer.sql_comment == "This is a simple function\nIt returns an integer"

    get_greeting = find_function(functions, "get_greeting")
    assert get_greeting.sql_comment == "Another function with a single comment line"


def test_block_comments():
    """Test parsing of block comments before functions."""
    # Create test SQL with block comments
    sql = """
    /*
     * This is a block comment
     * It spans multiple lines
     */
    CREATE FUNCTION multiply(p_a integer, p_b integer)
    RETURNS integer
    LANGUAGE sql AS $$
        SELECT p_a * p_b;
    $$;

    /* Single line block comment */
    CREATE FUNCTION divide(p_a integer, p_b integer)
    RETURNS numeric
    LANGUAGE sql AS $$
        SELECT p_a / p_b;
    $$;
    """

    # Parse the SQL
    functions, _, _, _ = parse_test_sql(sql)

    # Verify comment parsing
    multiply = find_function(functions, "multiply")
    assert multiply.sql_comment == "This is a block comment\nIt spans multiple lines"

    divide = find_function(functions, "divide")
    assert divide.sql_comment == "Single line block comment"


def test_mixed_comments():
    """Test parsing of mixed comment styles before functions."""
    # Create test SQL with mixed comment styles
    sql = """
    -- This is a line comment
    /* Followed by a block comment
     * With multiple lines
     */
    CREATE FUNCTION complex_function(p_input text)
    RETURNS text
    LANGUAGE sql AS $$
        SELECT p_input;
    $$;

    /* Block comment first */
    -- Then a line comment
    CREATE FUNCTION another_function()
    RETURNS void
    LANGUAGE sql AS $$
        -- This comment inside the function body should be ignored
        SELECT 1;
    $$;
    """

    # Parse the SQL
    functions, _, _, _ = parse_test_sql(sql)

    # Verify comment parsing
    complex_function = find_function(functions, "complex_function")
    assert complex_function.sql_comment == "This is a line comment\nFollowed by a block comment\nWith multiple lines"

    another_function = find_function(functions, "another_function")
    assert another_function.sql_comment == "Block comment first\nThen a line comment"


def test_no_comments():
    """Test functions without preceding comments."""
    # Create test SQL without comments
    sql = """
    CREATE FUNCTION no_comment_function()
    RETURNS void
    LANGUAGE sql AS $$
        SELECT 1;
    $$;

    -- This comment belongs to the next function
    CREATE FUNCTION with_comment_function()
    RETURNS void
    LANGUAGE sql AS $$
        SELECT 1;
    $$;
    """

    # Parse the SQL
    functions, _, _, _ = parse_test_sql(sql)

    # Verify comment parsing
    no_comment = find_function(functions, "no_comment_function")
    assert no_comment.sql_comment is None  # Parser returns None for no comment

    with_comment = find_function(functions, "with_comment_function")
    assert with_comment.sql_comment == "This comment belongs to the next function"


def test_comments_with_whitespace():
    """Test comment parsing with various whitespace patterns."""
    # Create test SQL with whitespace variations
    sql = """
    -- Comment with no space after --
    CREATE FUNCTION func1()
    RETURNS void
    LANGUAGE sql AS $$
        SELECT 1;
    $$;

    --Comment with no space after -- and no space before function
    CREATE FUNCTION func2()
    RETURNS void
    LANGUAGE sql AS $$
        SELECT 1;
    $$;

    -- Comment with trailing whitespace
    CREATE FUNCTION func3()
    RETURNS void
    LANGUAGE sql AS $$
        SELECT 1;
    $$;

    /*Block comment with no space*/
    CREATE FUNCTION func4()
    RETURNS void
    LANGUAGE sql AS $$
        SELECT 1;
    $$;
    """

    # Parse the SQL
    functions, _, _, _ = parse_test_sql(sql)

    # Verify comment parsing
    func1 = find_function(functions, "func1")
    assert func1.sql_comment == "Comment with no space after --"

    func2 = find_function(functions, "func2")
    assert func2.sql_comment == "Comment with no space after -- and no space before function"

    func3 = find_function(functions, "func3")
    assert func3.sql_comment == "Comment with trailing whitespace"

    func4 = find_function(functions, "func4")
    assert func4.sql_comment == "Block comment with no space"


def test_comment_with_special_characters():
    """Test comment parsing with special characters and formatting."""
    # Create test SQL with special characters in comments
    sql = """
    -- Function to calculate π (pi)
    -- Uses the formula: π ≈ 3.14159
    CREATE FUNCTION calculate_pi()
    RETURNS numeric
    LANGUAGE sql AS $$
        SELECT 3.14159;
    $$;

    /*
     * Function that handles SQL-specific characters:
     * - Handles quotes: ' and "
     * - Handles semicolons: ;
     * - Handles parentheses: () and brackets: []
     */
    CREATE FUNCTION special_chars_function(p_input text)
    RETURNS text
    LANGUAGE sql AS $$
        SELECT p_input;
    $$;
    """

    # Parse the SQL
    functions, _, _, _ = parse_test_sql(sql)

    # Verify comment parsing
    calculate_pi = find_function(functions, "calculate_pi")
    assert calculate_pi.sql_comment == "Function to calculate π (pi)\nUses the formula: π ≈ 3.14159"

    special_chars = find_function(functions, "special_chars_function")
    assert "Function that handles SQL-specific characters:" in special_chars.sql_comment
    assert "Handles quotes: ' and \"" in special_chars.sql_comment
    assert "Handles semicolons: ;" in special_chars.sql_comment
    assert "Handles parentheses: () and brackets: []" in special_chars.sql_comment
