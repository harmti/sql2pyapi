"""
Unit tests for NUMERIC DEFAULT NULL parameter parsing.

Tests the fix for the issue where NUMERIC(precision,scale) DEFAULT NULL parameters
were incorrectly parsed as required due to comma-splitting issues in parameter parsing.
"""

import pytest

from sql2pyapi.parser.parameter_parser import parse_params


def test_numeric_with_parentheses_no_default():
    """Test that NUMERIC(10,7) without DEFAULT parses correctly."""
    param_str = "p_lat NUMERIC(10,7)"
    params, imports = parse_params(param_str, "test_function")

    assert len(params) == 1
    param = params[0]

    assert param.name == "p_lat"
    assert param.python_name == "lat"
    assert param.sql_type == "NUMERIC(10,7)"
    assert param.python_type == "Decimal"
    assert param.is_optional is False
    assert param.has_sql_default is False


def test_numeric_with_parentheses_default_null():
    """Test that NUMERIC(10,7) DEFAULT NULL parses correctly."""
    param_str = "p_lat NUMERIC(10,7) DEFAULT NULL"
    params, imports = parse_params(param_str, "test_function")

    assert len(params) == 1
    param = params[0]

    assert param.name == "p_lat"
    assert param.python_name == "lat"
    assert param.sql_type == "NUMERIC(10,7)"
    assert param.python_type == "Optional[Decimal]"
    assert param.is_optional is True
    assert param.has_sql_default is False  # DEFAULT NULL is not a meaningful SQL default


def test_numeric_with_parentheses_default_value():
    """Test that NUMERIC(10,7) DEFAULT 0.0 parses correctly."""
    param_str = "p_lat NUMERIC(10,7) DEFAULT 0.0"
    params, imports = parse_params(param_str, "test_function")

    assert len(params) == 1
    param = params[0]

    assert param.name == "p_lat"
    assert param.python_name == "lat"
    assert param.sql_type == "NUMERIC(10,7)"
    assert param.python_type == "Optional[Decimal]"
    assert param.is_optional is True
    assert param.has_sql_default is True  # DEFAULT 0.0 is a meaningful SQL default


def test_multiple_numeric_parameters_mixed_defaults():
    """Test multiple NUMERIC parameters with mixed default scenarios."""
    param_str = "p_id INTEGER, p_lat NUMERIC(10,7), p_lng NUMERIC(10,7) DEFAULT NULL, p_alt NUMERIC(5,2) DEFAULT 100.0"
    params, imports = parse_params(param_str, "test_function")

    assert len(params) == 4

    # p_id INTEGER - required
    id_param = params[0]
    assert id_param.name == "p_id"
    assert id_param.sql_type == "INTEGER"
    assert id_param.python_type == "int"
    assert id_param.is_optional is False
    assert id_param.has_sql_default is False

    # p_lat NUMERIC(10,7) - required
    lat_param = params[1]
    assert lat_param.name == "p_lat"
    assert lat_param.sql_type == "NUMERIC(10,7)"
    assert lat_param.python_type == "Decimal"
    assert lat_param.is_optional is False
    assert lat_param.has_sql_default is False

    # p_lng NUMERIC(10,7) DEFAULT NULL - optional with NULL default
    lng_param = params[2]
    assert lng_param.name == "p_lng"
    assert lng_param.sql_type == "NUMERIC(10,7)"
    assert lng_param.python_type == "Optional[Decimal]"
    assert lng_param.is_optional is True
    assert lng_param.has_sql_default is False

    # p_alt NUMERIC(5,2) DEFAULT 100.0 - optional with meaningful default
    alt_param = params[3]
    assert alt_param.name == "p_alt"
    assert alt_param.sql_type == "NUMERIC(5,2)"
    assert alt_param.python_type == "Optional[Decimal]"
    assert alt_param.is_optional is True
    assert alt_param.has_sql_default is True


def test_reported_bug_scenario():
    """Test the exact scenario reported in the bug: p_latitude NUMERIC(10,7) DEFAULT NULL, p_longitude NUMERIC(10,7) DEFAULT NULL."""
    param_str = "p_name TEXT, p_latitude NUMERIC(10,7) DEFAULT NULL, p_longitude NUMERIC(10,7) DEFAULT NULL"
    params, imports = parse_params(param_str, "test_function")

    assert len(params) == 3

    # p_name TEXT - required
    name_param = params[0]
    assert name_param.name == "p_name"
    assert name_param.sql_type == "TEXT"
    assert name_param.python_type == "str"
    assert name_param.is_optional is False

    # p_latitude NUMERIC(10,7) DEFAULT NULL - should be optional
    lat_param = params[1]
    assert lat_param.name == "p_latitude"
    assert lat_param.sql_type == "NUMERIC(10,7)"
    assert lat_param.python_type == "Optional[Decimal]"
    assert lat_param.is_optional is True, "p_latitude with DEFAULT NULL should be optional, not mandatory"
    assert lat_param.has_sql_default is False

    # p_longitude NUMERIC(10,7) DEFAULT NULL - should be optional
    lng_param = params[2]
    assert lng_param.name == "p_longitude"
    assert lng_param.sql_type == "NUMERIC(10,7)"
    assert lng_param.python_type == "Optional[Decimal]"
    assert lng_param.is_optional is True, "p_longitude with DEFAULT NULL should be optional, not mandatory"
    assert lng_param.has_sql_default is False


def test_smart_comma_split_function():
    """Test the internal _smart_comma_split function directly."""
    from sql2pyapi.parser.parameter_parser import _smart_comma_split

    # Test case that was failing before the fix
    param_str = "p_name TEXT, p_latitude NUMERIC(10,7) DEFAULT NULL, p_longitude NUMERIC(10,7) DEFAULT NULL"
    result = _smart_comma_split(param_str)

    expected = ["p_name TEXT", "p_latitude NUMERIC(10,7) DEFAULT NULL", "p_longitude NUMERIC(10,7) DEFAULT NULL"]

    assert result == expected

    # Test basic types with parentheses
    basic_param_str = "p_id INT, p_data NUMERIC(10,7), p_more DECIMAL(15,3) DEFAULT NULL"
    result2 = _smart_comma_split(basic_param_str)

    expected2 = ["p_id INT", "p_data NUMERIC(10,7)", "p_more DECIMAL(15,3) DEFAULT NULL"]

    assert result2 == expected2


def test_decimal_vs_numeric_consistency():
    """Test that both DECIMAL and NUMERIC with parentheses work the same way."""
    test_cases = [
        ("p_d1 DECIMAL(10,7) DEFAULT NULL", "DECIMAL(10,7)", "Optional[Decimal]", True, False),
        ("p_n1 NUMERIC(10,7) DEFAULT NULL", "NUMERIC(10,7)", "Optional[Decimal]", True, False),
        ("p_d2 DECIMAL(5,2) DEFAULT 99.99", "DECIMAL(5,2)", "Optional[Decimal]", True, True),
        ("p_n2 NUMERIC(5,2) DEFAULT 99.99", "NUMERIC(5,2)", "Optional[Decimal]", True, True),
    ]

    for param_str, expected_sql_type, expected_python_type, expected_optional, expected_has_default in test_cases:
        params, _ = parse_params(param_str, "test_function")
        assert len(params) == 1

        param = params[0]
        assert param.sql_type == expected_sql_type
        assert param.python_type == expected_python_type
        assert param.is_optional == expected_optional
        assert param.has_sql_default == expected_has_default
