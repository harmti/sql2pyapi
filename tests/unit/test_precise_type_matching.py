"""
Unit tests for precise type matching patterns in composite type parsing.

These tests validate that the regex patterns in Phase 1 improvements work correctly
and don't have false positives or negatives.
"""

import re
import pytest


def test_regex_patterns_compilation():
    """Test that all regex patterns compile without errors."""

    # These are the corrected patterns that should be in the implementation
    type_patterns = {
        "bool": re.compile(r"^(?:Optional\[bool\]|bool)$"),
        "int": re.compile(r"^(?:Optional\[int\]|int)$"),
        "float": re.compile(r"^(?:Optional\[float\]|float)$"),
        "decimal": re.compile(r"^(?:Optional\[(?:Decimal|decimal)\]|(?:Decimal|decimal))$"),
        "uuid": re.compile(r"^(?:Optional\[UUID\]|UUID)$"),
        "datetime": re.compile(r"^(?:Optional\[datetime\]|datetime)$"),
        "dict": re.compile(r"^(?:Optional\[(?:Dict|dict)(?:\[.*\])?\]|(?:Dict|dict)(?:\[.*\])?)$"),
        "list": re.compile(r"^(?:Optional\[(?:List|list)(?:\[.*\])?\]|(?:List|list)(?:\[.*\])?)$"),
        "any": re.compile(r"^(?:Optional\[(?:Any|any)\]|(?:Any|any))$"),
    }

    # All patterns should compile successfully
    for pattern_name, pattern in type_patterns.items():
        assert pattern is not None, f"Pattern {pattern_name} failed to compile"
        assert hasattr(pattern, "match"), f"Pattern {pattern_name} doesn't have match method"


def test_bool_type_matching():
    """Test boolean type pattern matching."""

    bool_pattern = re.compile(r"^(?:Optional\[bool\]|bool)$")

    # Should match
    assert bool_pattern.match("bool") is not None
    assert bool_pattern.match("Optional[bool]") is not None

    # Should NOT match (the old fragile approach would match these incorrectly)
    assert bool_pattern.match("MyBooleanWrapper") is None
    assert bool_pattern.match("boolean") is None
    assert bool_pattern.match("bool_field") is None
    assert bool_pattern.match("rebool") is None
    assert bool_pattern.match("Boolean") is None
    assert bool_pattern.match("Optional[Boolean]") is None


def test_int_type_matching():
    """Test integer type pattern matching."""

    int_pattern = re.compile(r"^(?:Optional\[int\]|int)$")

    # Should match
    assert int_pattern.match("int") is not None
    assert int_pattern.match("Optional[int]") is not None

    # Should NOT match
    assert int_pattern.match("integer") is None
    assert int_pattern.match("int32") is None
    assert int_pattern.match("bigint") is None
    assert int_pattern.match("MyIntWrapper") is None
    assert int_pattern.match("print") is None  # Contains 'int' but shouldn't match


def test_decimal_type_matching():
    """Test decimal type pattern matching."""

    decimal_pattern = re.compile(r"^(?:Optional\[(?:Decimal|decimal)\]|(?:Decimal|decimal))$")

    # Should match
    assert decimal_pattern.match("Decimal") is not None
    assert decimal_pattern.match("decimal") is not None
    assert decimal_pattern.match("Optional[Decimal]") is not None
    assert decimal_pattern.match("Optional[decimal]") is not None

    # Should NOT match
    assert decimal_pattern.match("DecimalField") is None
    assert decimal_pattern.match("MyDecimal") is None
    assert decimal_pattern.match("decimal_value") is None


def test_uuid_type_matching():
    """Test UUID type pattern matching."""

    uuid_pattern = re.compile(r"^(?:Optional\[UUID\]|UUID)$")

    # Should match
    assert uuid_pattern.match("UUID") is not None
    assert uuid_pattern.match("Optional[UUID]") is not None

    # Should NOT match
    assert uuid_pattern.match("uuid") is None  # lowercase
    assert uuid_pattern.match("UUIDField") is None
    assert uuid_pattern.match("MyUUID") is None


def test_dict_type_matching():
    """Test dict type pattern matching - should handle both bare Dict and parameterized Dict[K,V]."""

    dict_pattern = re.compile(r"^(?:Optional\[(?:Dict|dict)(?:\[.*\])?\]|(?:Dict|dict)(?:\[.*\])?)$")

    # Should match - bare types
    assert dict_pattern.match("Dict") is not None
    assert dict_pattern.match("dict") is not None
    assert dict_pattern.match("Optional[Dict]") is not None
    assert dict_pattern.match("Optional[dict]") is not None

    # Should match - parameterized types
    assert dict_pattern.match("Dict[str, int]") is not None
    assert dict_pattern.match("dict[str, int]") is not None
    assert dict_pattern.match("Optional[Dict[str, int]]") is not None
    assert dict_pattern.match("Dict[str, List[int]]") is not None  # nested generics

    # Should NOT match
    assert dict_pattern.match("dictionary") is None
    assert dict_pattern.match("DictField") is None
    assert dict_pattern.match("MyDict") is None


def test_list_type_matching():
    """Test list type pattern matching - should handle both bare List and parameterized List[T]."""

    list_pattern = re.compile(r"^(?:Optional\[(?:List|list)(?:\[.*\])?\]|(?:List|list)(?:\[.*\])?)$")

    # Should match - bare types
    assert list_pattern.match("List") is not None
    assert list_pattern.match("list") is not None
    assert list_pattern.match("Optional[List]") is not None
    assert list_pattern.match("Optional[list]") is not None

    # Should match - parameterized types
    assert list_pattern.match("List[int]") is not None
    assert list_pattern.match("list[str]") is not None
    assert list_pattern.match("Optional[List[int]]") is not None
    assert list_pattern.match("List[Dict[str, int]]") is not None  # nested generics

    # Should NOT match
    assert list_pattern.match("listing") is None
    assert list_pattern.match("ListField") is None
    assert list_pattern.match("MyList") is None


def test_datetime_type_matching():
    """Test datetime type pattern matching."""

    datetime_pattern = re.compile(r"^(?:Optional\[datetime\]|datetime)$")

    # Should match
    assert datetime_pattern.match("datetime") is not None
    assert datetime_pattern.match("Optional[datetime]") is not None

    # Should NOT match
    assert datetime_pattern.match("DateTime") is None  # uppercase
    assert datetime_pattern.match("datetime_field") is None
    assert datetime_pattern.match("DateTimeField") is None


def test_any_type_matching():
    """Test Any type pattern matching."""

    any_pattern = re.compile(r"^(?:Optional\[(?:Any|any)\]|(?:Any|any))$")

    # Should match
    assert any_pattern.match("Any") is not None
    assert any_pattern.match("any") is not None
    assert any_pattern.match("Optional[Any]") is not None
    assert any_pattern.match("Optional[any]") is not None

    # Should NOT match
    assert any_pattern.match("anything") is None
    assert any_pattern.match("AnyField") is None
    assert any_pattern.match("company") is None  # contains 'any' but shouldn't match


def test_false_positive_prevention():
    """Test that common false positives from the old substring matching are prevented."""

    # These would incorrectly match with old 'bool' in expected_type.lower() approach
    problematic_types = [
        "MyBooleanWrapper",
        "BooleanField",
        "UserBoolSettings",
        "rebool",
        "boolean",
    ]

    bool_pattern = re.compile(r"^(?:Optional\[bool\]|bool)$")

    for type_name in problematic_types:
        assert bool_pattern.match(type_name) is None, f"Pattern incorrectly matched '{type_name}'"


def test_generated_code_integration():
    """Test that the patterns work in the context of generated code."""

    # Simulate the _matches_type_pattern function from generated code
    TYPE_PATTERNS = {
        "bool": re.compile(r"^(?:Optional\[bool\]|bool)$"),
        "int": re.compile(r"^(?:Optional\[int\]|int)$"),
        "decimal": re.compile(r"^(?:Optional\[(?:Decimal|decimal)\]|(?:Decimal|decimal))$"),
        "dict": re.compile(r"^(?:Optional\[(?:Dict|dict)(?:\[.*\])?\]|(?:Dict|dict)(?:\[.*\])?)$"),
    }

    def _matches_type_pattern(expected_type: str, pattern_name: str) -> bool:
        return TYPE_PATTERNS[pattern_name].match(expected_type) is not None

    # Test realistic scenarios
    assert _matches_type_pattern("bool", "bool") is True
    assert _matches_type_pattern("Optional[bool]", "bool") is True
    assert _matches_type_pattern("BooleanField", "bool") is False

    assert _matches_type_pattern("int", "int") is True
    assert _matches_type_pattern("Optional[int]", "int") is True
    assert _matches_type_pattern("integer", "int") is False

    assert _matches_type_pattern("Decimal", "decimal") is True
    assert _matches_type_pattern("decimal", "decimal") is True
    assert _matches_type_pattern("Optional[Decimal]", "decimal") is True
    assert _matches_type_pattern("DecimalField", "decimal") is False

    # Test the specific case from the runtime test
    assert _matches_type_pattern("Dict", "dict") is True
    assert _matches_type_pattern("Dict[str, int]", "dict") is True
    assert _matches_type_pattern("Optional[Dict[str, Any]]", "dict") is True


def test_edge_cases():
    """Test edge cases and malformed type strings."""

    bool_pattern = re.compile(r"^(?:Optional\[bool\]|bool)$")

    # Edge cases that should not match
    edge_cases = [
        "",  # empty string
        "Optional[",  # malformed Optional
        "Optional[]",  # empty Optional
        "bool]",  # malformed closing bracket
        "[bool]",  # malformed opening bracket
        "Optional[bool",  # missing closing bracket
        "Optional[bool]]",  # extra closing bracket
        "bool bool",  # space in type
        "Optional[Optional[bool]]",  # double Optional (shouldn't happen but test anyway)
    ]

    for edge_case in edge_cases:
        assert bool_pattern.match(edge_case) is None, f"Pattern incorrectly matched edge case '{edge_case}'"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
