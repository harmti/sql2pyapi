"""Unit tests for Phase 2 enum registry improvements in composite unpacker."""

import pytest

from sql2pyapi.generator.composite_unpacker import (
    generate_enum_registration_section,
    generate_type_aware_converter,
)


def test_generate_enum_registration_section_empty():
    """Test enum registration section generation with no enums."""
    result = generate_enum_registration_section()
    assert result == []

    result = generate_enum_registration_section({})
    assert result == []


def test_generate_enum_registration_section_with_enums():
    """Test enum registration section generation with enums."""
    enum_types = {
        "user_role": ["admin", "moderator", "user", "guest"],
        "status_type": ["pending", "active", "inactive", "deleted"],
    }

    result = generate_enum_registration_section(enum_types)

    # Should have header comments
    assert result[0].startswith("# Register enum classes")
    assert result[1].startswith("# This eliminates")

    # Should have registration calls for both enums
    expected_calls = [
        "_ENUM_REGISTRY.register_enum('UserRole', UserRole)",
        "_ENUM_REGISTRY.register_enum('StatusType', StatusType)",
    ]

    # Extract the registration calls (skip header comments)
    registration_calls = [line for line in result if line.startswith("_ENUM_REGISTRY.register_enum")]

    assert len(registration_calls) == 2
    for expected_call in expected_calls:
        assert expected_call in registration_calls


def test_generate_type_aware_converter_without_enums():
    """Test type-aware converter generation without enums."""
    result = generate_type_aware_converter()

    code_text = "\n".join(result)

    # Should not contain enum registry code
    assert "_EnumRegistry" not in code_text
    assert "_ENUM_REGISTRY" not in code_text
    assert "# No enums detected" in code_text

    # Should still have basic type patterns
    assert "_TYPE_PATTERNS" in code_text
    assert "_convert_postgresql_value_typed" in code_text


def test_generate_type_aware_converter_with_enums():
    """Test type-aware converter generation with enums."""
    enum_types = {
        "user_role": ["admin", "moderator", "user", "guest"],
        "status_type": ["pending", "active", "inactive", "deleted"],
    }

    result = generate_type_aware_converter(enum_types)
    code_text = "\n".join(result)

    # Should contain enum registry code
    assert "class _EnumRegistry:" in code_text
    assert "_ENUM_REGISTRY = _EnumRegistry()" in code_text
    assert "def register_enum(self, enum_name: str, enum_class):" in code_text
    assert "def convert_enum_value(self, value: str, enum_name: str):" in code_text

    # Should have registry-based enum conversion
    assert "# Enum types - use registry-based conversion" in code_text
    assert "_ENUM_REGISTRY.convert_enum_value(field, expected_type)" in code_text

    # Should not contain fragile sys._getframe approach (actual code)
    assert "sys._getframe(1)" not in code_text
    assert "frame = sys._getframe" not in code_text
    assert "frame.f_globals" not in code_text

    # Should still have basic type patterns and conversion function
    assert "_TYPE_PATTERNS" in code_text
    assert "_convert_postgresql_value_typed" in code_text


def test_enum_registry_class_functionality():
    """Test the enum registry class functionality by executing generated code."""
    from enum import Enum

    # Create a test enum
    class TestUserRole(Enum):
        ADMIN = "admin"
        USER = "user"
        GUEST = "guest"

    enum_types = {"user_role": ["admin", "user", "guest"]}
    result = generate_type_aware_converter(enum_types)

    # Execute the generated code to test the registry
    code_text = "\n".join(result)

    # Create a local namespace and execute the code
    local_namespace = {"Enum": Enum}
    exec(code_text, local_namespace)

    # Get the registry and register our test enum
    registry = local_namespace["_ENUM_REGISTRY"]
    registry.register_enum("TestUserRole", TestUserRole)

    # Test enum conversion
    assert registry.convert_enum_value("ADMIN", "TestUserRole") == TestUserRole.ADMIN
    assert registry.convert_enum_value("admin", "TestUserRole") == TestUserRole.ADMIN  # case insensitive
    assert registry.convert_enum_value("user", "TestUserRole") == TestUserRole.USER

    # Test fallback for unknown enum
    assert registry.convert_enum_value("admin", "UnknownEnum") == "admin"

    # Test fallback for invalid value
    assert registry.convert_enum_value("invalid", "TestUserRole") == "invalid"


def test_type_aware_converter_enum_conversion():
    """Test the complete type-aware conversion with enum support."""
    from enum import Enum

    # Create test enums
    class UserRole(Enum):
        ADMIN = "admin"
        USER = "user"
        GUEST = "guest"

    enum_types = {"user_role": ["admin", "user", "guest"]}
    result = generate_type_aware_converter(enum_types)

    # Execute the generated code
    code_text = "\n".join(result)
    local_namespace = {"Enum": Enum}
    exec(code_text, local_namespace)

    # Register the enum and get the conversion function
    registry = local_namespace["_ENUM_REGISTRY"]
    registry.register_enum("UserRole", UserRole)
    convert_func = local_namespace["_convert_postgresql_value_typed"]

    # Test enum conversion
    assert convert_func("admin", "UserRole") == UserRole.ADMIN
    assert convert_func("user", "UserRole") == UserRole.USER

    # Test other type conversions still work
    assert convert_func("t", "bool") == True
    assert convert_func("f", "bool") == False
    assert convert_func("123", "int") == 123

    # Test fallback to string for unknown types
    assert convert_func("some_value", "UnknownType") == "some_value"


def test_registry_handles_edge_cases():
    """Test that the enum registry handles edge cases properly."""
    from enum import Enum

    # Create enum with mixed case values
    class StatusType(Enum):
        PENDING = "pending"
        ACTIVE = "active"
        IN_PROGRESS = "in_progress"

    enum_types = {"status_type": ["pending", "active", "in_progress"]}
    result = generate_type_aware_converter(enum_types)

    # Execute the generated code
    code_text = "\n".join(result)
    local_namespace = {"Enum": Enum}
    exec(code_text, local_namespace)

    registry = local_namespace["_ENUM_REGISTRY"]
    registry.register_enum("StatusType", StatusType)

    # Test case variations
    assert registry.convert_enum_value("PENDING", "StatusType") == StatusType.PENDING
    assert registry.convert_enum_value("pending", "StatusType") == StatusType.PENDING
    assert registry.convert_enum_value("ACTIVE", "StatusType") == StatusType.ACTIVE
    assert registry.convert_enum_value("active", "StatusType") == StatusType.ACTIVE

    # Test underscore handling
    assert registry.convert_enum_value("IN_PROGRESS", "StatusType") == StatusType.IN_PROGRESS
    assert registry.convert_enum_value("in_progress", "StatusType") == StatusType.IN_PROGRESS
