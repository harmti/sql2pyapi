import pytest

from sql2pyapi import generate_python_code
from sql2pyapi import parse_sql


# Test for NUMERIC DEFAULT NULL parameters issue
FUNC_SQL_NUMERIC_DEFAULT_NULL = """
CREATE OR REPLACE FUNCTION test_location_function(
    p_name TEXT,
    p_latitude NUMERIC(10,7) DEFAULT NULL,
    p_longitude NUMERIC(10,7) DEFAULT NULL
)
RETURNS TABLE (
    name TEXT,
    latitude NUMERIC(10,7),
    longitude NUMERIC(10,7),
    status TEXT
)
AS $$
    SELECT
        p_name::TEXT as name,
        p_latitude as latitude,
        p_longitude as longitude,
        CASE
            WHEN p_latitude IS NULL OR p_longitude IS NULL THEN 'location_missing'
            ELSE 'location_provided'
        END as status;
$$ LANGUAGE SQL;

-- Another test with mixed parameter types
CREATE OR REPLACE FUNCTION test_mixed_defaults(
    p_product_id INTEGER,
    p_discount_rate NUMERIC(5,2) DEFAULT NULL,
    p_category TEXT DEFAULT 'general',
    p_priority INTEGER DEFAULT 1
)
RETURNS TABLE (
    product_id INTEGER,
    discount_rate NUMERIC(5,2),
    category TEXT,
    priority INTEGER,
    result_msg TEXT
)
AS $$
    SELECT
        p_product_id as product_id,
        p_discount_rate as discount_rate,
        p_category as category,
        p_priority as priority,
        CASE
            WHEN p_discount_rate IS NULL THEN 'no_discount_applied'
            ELSE 'discount_applied'
        END as result_msg;
$$ LANGUAGE SQL;
"""


def test_numeric_default_null_parsing_and_code_generation():
    """
    Tests that NUMERIC parameters with DEFAULT NULL are properly handled:
    1. They should be parsed as optional (Optional[Decimal])
    2. They should not be marked as required in the generated Python API
    3. Generated code should have correct function signatures
    """
    # 1. Parse and check parameter properties
    functions, _, _, _ = parse_sql(FUNC_SQL_NUMERIC_DEFAULT_NULL)
    assert len(functions) == 2

    location_func = next(f for f in functions if f.sql_name == "test_location_function")
    mixed_func = next(f for f in functions if f.sql_name == "test_mixed_defaults")

    # Check location function parameters
    assert len(location_func.params) == 3

    name_param = next(p for p in location_func.params if p.name == "p_name")
    lat_param = next(p for p in location_func.params if p.name == "p_latitude")
    lng_param = next(p for p in location_func.params if p.name == "p_longitude")

    # Name should be required (no DEFAULT)
    assert not name_param.is_optional
    assert not name_param.has_sql_default
    assert name_param.python_type == "str"

    # Latitude and longitude should be optional but with NULL defaults (not meaningful SQL defaults)
    assert lat_param.is_optional
    assert not lat_param.has_sql_default  # DEFAULT NULL is not a meaningful SQL default
    assert lat_param.python_type == "Optional[Decimal]"

    assert lng_param.is_optional
    assert not lng_param.has_sql_default  # DEFAULT NULL is not a meaningful SQL default
    assert lng_param.python_type == "Optional[Decimal]"

    # Check mixed function parameters
    assert len(mixed_func.params) == 4

    product_param = next(p for p in mixed_func.params if p.name == "p_product_id")
    discount_param = next(p for p in mixed_func.params if p.name == "p_discount_rate")
    category_param = next(p for p in mixed_func.params if p.name == "p_category")
    priority_param = next(p for p in mixed_func.params if p.name == "p_priority")

    # Product ID should be required
    assert not product_param.is_optional
    assert product_param.python_type == "int"

    # Discount rate should be optional with NULL default (not meaningful SQL default)
    assert discount_param.is_optional
    assert not discount_param.has_sql_default  # DEFAULT NULL is not meaningful
    assert discount_param.python_type == "Optional[Decimal]"

    # Category should be optional with non-NULL default
    assert category_param.is_optional
    assert category_param.has_sql_default  # DEFAULT 'general' is meaningful
    assert category_param.python_type == "Optional[str]"

    # Priority should be optional with non-NULL default
    assert priority_param.is_optional
    assert priority_param.has_sql_default  # DEFAULT 1 is meaningful
    assert priority_param.python_type == "Optional[int]"

    # 2. Generate Python code and verify signatures
    python_code = generate_python_code(functions, {}, {}, {})
    assert "def test_location_function" in python_code
    assert "def test_mixed_defaults" in python_code

    # Verify that NUMERIC DEFAULT NULL parameters are optional in the generated signatures
    assert "latitude: Optional[Decimal] = None" in python_code
    assert "longitude: Optional[Decimal] = None" in python_code
    assert "discount_rate: Optional[Decimal] = None" in python_code

    # Verify that required parameters don't have default values
    assert "name: str," in python_code or "name: str)" in python_code
    assert "product_id: int," in python_code or "product_id: int)" in python_code


def test_numeric_default_null_parameter_should_be_optional():
    """
    Specific test to verify the reported issue: NUMERIC DEFAULT NULL parameters
    should NOT be mandatory in the generated Python API.
    """
    # This simple SQL function mimics the reported issue
    test_sql = """
    CREATE OR REPLACE FUNCTION test_coordinates(
        p_name TEXT,
        p_latitude NUMERIC(10,7) DEFAULT NULL,
        p_longitude NUMERIC(10,7) DEFAULT NULL
    )
    RETURNS TEXT
    AS $$
        SELECT p_name || ' coordinates processed';
    $$ LANGUAGE SQL;
    """

    functions, _, _, _ = parse_sql(test_sql)
    assert len(functions) == 1

    func = functions[0]
    assert len(func.params) == 3

    # Find the NUMERIC parameters
    lat_param = next(p for p in func.params if p.name == "p_latitude")
    lng_param = next(p for p in func.params if p.name == "p_longitude")

    # Both should be optional due to DEFAULT NULL
    assert lat_param.is_optional, "p_latitude with DEFAULT NULL should be optional"
    assert lng_param.is_optional, "p_longitude with DEFAULT NULL should be optional"

    # Both should NOT have meaningful SQL defaults (DEFAULT NULL is not meaningful)
    assert not lat_param.has_sql_default, "p_latitude with DEFAULT NULL should not have meaningful SQL default"
    assert not lng_param.has_sql_default, "p_longitude with DEFAULT NULL should not have meaningful SQL default"

    # Both should have Optional[Decimal] type in Python
    assert lat_param.python_type == "Optional[Decimal]"
    assert lng_param.python_type == "Optional[Decimal]"

    # Generate code and verify it doesn't make these parameters required
    python_code = generate_python_code(functions, {}, {}, {})

    # The function signature should have default None values for these parameters
    # (This verifies they are not treated as required parameters)
    assert "latitude: Optional[Decimal] = None" in python_code
    assert "longitude: Optional[Decimal] = None" in python_code
