"""Helper module for handling nested composite type unpacking in generated code."""

from ..sql_models import ReturnColumn


def generate_postgresql_value_converter() -> list[str]:
    """
    Generates a helper function to convert PostgreSQL string representations to proper Python types.

    Returns:
        List of code lines for the converter function
    """
    return [
        "def _convert_postgresql_value(field: str):",
        '    """Convert PostgreSQL string representations to proper Python types."""',
        "    field = field.strip()",
        "    ",
        "    # Handle boolean representations - these are PostgreSQL specific",
        "    if field == 't':",
        "        return True",
        "    elif field == 'f':",
        "        return False",
        "    ",
        "    # Handle numeric representations only for values that contain a decimal point",
        "    # This is more conservative and avoids converting integer strings that might be IDs",
        "    if '.' in field and field.replace('.', '').replace('-', '').replace('+', '').isdigit():",
        "        # Only convert numbers with decimal points to Decimal",
        "        try:",
        "            from decimal import Decimal",
        "            return Decimal(field)",
        "        except (ValueError, TypeError):",
        "            # If Decimal conversion fails, keep as string",
        "            pass",
        "    ",
        "    # Handle JSON/JSONB representations",
        "    if field.strip().startswith(('{', '[')):",
        "        try:",
        "            import json",
        "            return json.loads(field)",
        "        except (json.JSONDecodeError, ValueError):",
        "            # If JSON parsing fails, keep as string",
        "            pass",
        "    ",
        "    # For all other values (including integer strings), keep as string",
        "    # This prevents converting IDs and other integer strings to Decimal",
        "    return field",
        "",
    ]


def generate_type_aware_converter(enum_types: dict[str, list[str]] = None) -> list[str]:
    """
    Generates a helper function to convert PostgreSQL values with type awareness.
    Uses precise regex patterns for reliable type matching and a known enum registry.

    Args:
        enum_types: Dictionary mapping enum names to their possible values

    Returns:
        List of code lines for the type-aware converter function
    """
    enum_types = enum_types or {}

    lines = [
        "# Module-level imports for type conversions",
        "import re",
        "from decimal import Decimal",
        "from uuid import UUID",
        "from datetime import datetime",
        "from typing import Any",
        "import json",
        "",
        "# Compiled regex patterns for precise type matching",
        "_TYPE_PATTERNS = {",
        "    'bool': re.compile(r'^(?:Optional\\[bool\\]|bool)$'),",
        "    'int': re.compile(r'^(?:Optional\\[int\\]|int)$'),",
        "    'float': re.compile(r'^(?:Optional\\[float\\]|float)$'),",
        "    'decimal': re.compile(r'^(?:Optional\\[(?:Decimal|decimal)\\]|(?:Decimal|decimal))$'),",
        "    'uuid': re.compile(r'^(?:Optional\\[UUID\\]|UUID)$'),",
        "    'datetime': re.compile(r'^(?:Optional\\[datetime\\]|datetime)$'),",
        "    'dict': re.compile(r'^(?:Optional\\[(?:Dict|dict)(?:\\[.*\\])?\\]|(?:Dict|dict)(?:\\[.*\\])?)$'),",
        "    'list': re.compile(r'^(?:Optional\\[(?:List|list)(?:\\[.*\\])?\\]|(?:List|list)(?:\\[.*\\])?)$'),",
        "    'any': re.compile(r'^(?:Optional\\[(?:Any|any)\\]|(?:Any|any))$'),",
        "}",
        "",
    ]

    # Add enum registry if there are enums
    if enum_types:
        lines.extend(
            [
                "# Known Enum Registry - eliminates fragile sys._getframe() approach",
                "class _EnumRegistry:",
                "    def __init__(self):",
                "        self.known_enums = {}",
                "    ",
                "    def register_enum(self, enum_name: str, enum_class):",
                '        """Register an enum class with the registry."""',
                "        self.known_enums[enum_name] = enum_class",
                "    ",
                "    def convert_enum_value(self, value: str, enum_name: str):",
                '        """Convert string value to enum instance if enum is registered."""',
                "        if enum_name in self.known_enums:",
                "            enum_class = self.known_enums[enum_name]",
                "            try:",
                "                # Try by name first (case insensitive)",
                "                return enum_class[value.upper()]",
                "            except (KeyError, AttributeError):",
                "                try:",
                "                    # Try by value",
                "                    return enum_class(value)",
                "                except (ValueError, TypeError):",
                "                    pass",
                "        return value",
                "",
                "# Global enum registry instance",
                "_ENUM_REGISTRY = _EnumRegistry()",
                "",
            ]
        )

        # Generate enum registration calls for all known enums
        lines.append("")
        for enum_name in enum_types.keys():
            from ..parser.utils import _to_singular_camel_case

            python_enum_name = _to_singular_camel_case(enum_name)
            lines.append(f"# Register {python_enum_name} enum (will be registered after import)")
    else:
        lines.extend(
            [
                "# No enums detected - enum registry not generated",
                "",
            ]
        )

    lines.extend(
        [
            "def _matches_type_pattern(expected_type: str, pattern_name: str) -> bool:",
            '    """Check if expected_type matches a specific type pattern."""',
            "    return _TYPE_PATTERNS[pattern_name].match(expected_type) is not None",
            "",
            "def _convert_postgresql_value_typed(field: str, expected_type: str) -> Any:",
            '    """Convert PostgreSQL string representations to proper Python types with type guidance."""',
            "    if field is None:",
            "        return None",
            "    # Only apply string-specific conversions if field is actually a string",
            "    if not isinstance(field, str):",
            "        return field",
            "    if field.lower() in ('null', ''):",
            "        return None",
            "    ",
            "    field = field.strip()",
            "    ",
            "    # Boolean types - precise matching",
            "    if _matches_type_pattern(expected_type, 'bool'):",
            "        if field == 't':",
            "            return True",
            "        if field == 'f':",
            "            return False",
            "    ",
            "    # Integer types - precise matching",
            "    if _matches_type_pattern(expected_type, 'int'):",
            "        try:",
            "            return int(field)",
            "        except (ValueError, TypeError) as e:",
            "            # Enhanced error context for debugging",
            "            if getattr(_convert_postgresql_value_typed, 'debug_mode', False):",
            "                print(f\"Int conversion failed: field='{field}', expected='{expected_type}', error={e}\")",
            "            pass",
            "    ",
            "    # Float types - precise matching",
            "    if _matches_type_pattern(expected_type, 'float'):",
            "        try:",
            "            return float(field)",
            "        except (ValueError, TypeError) as e:",
            "            # Enhanced error context for debugging",
            "            if getattr(_convert_postgresql_value_typed, 'debug_mode', False):",
            "                print(f\"Float conversion failed: field='{field}', expected='{expected_type}', error={e}\")",
            "            pass",
            "    ",
            "    # Decimal types - precise matching",
            "    if _matches_type_pattern(expected_type, 'decimal'):",
            "        try:",
            "            return Decimal(field)",
            "        except (ValueError, TypeError) as e:",
            "            # Enhanced error context for debugging",
            "            if getattr(_convert_postgresql_value_typed, 'debug_mode', False):",
            "                print(f\"Decimal conversion failed: field='{field}', expected='{expected_type}', error={e}\")",
            "            pass",
            "    ",
            "    # UUID types - precise matching",
            "    if _matches_type_pattern(expected_type, 'uuid'):",
            "        try:",
            "            return UUID(field)",
            "        except (ValueError, TypeError) as e:",
            "            # Enhanced error context for debugging",
            "            if getattr(_convert_postgresql_value_typed, 'debug_mode', False):",
            "                print(f\"UUID conversion failed: field='{field}', expected='{expected_type}', error={e}\")",
            "            pass",
            "    ",
            "    # DateTime types - precise matching",
            "    if _matches_type_pattern(expected_type, 'datetime'):",
            "        try:",
            "            # Handle PostgreSQL timestamp format",
            "            return datetime.fromisoformat(field.replace(' ', 'T'))",
            "        except (ValueError, TypeError) as e:",
            "            # Enhanced error context for debugging",
            "            if getattr(_convert_postgresql_value_typed, 'debug_mode', False):",
            "                print(f\"DateTime conversion failed: field='{field}', expected='{expected_type}', error={e}\")",
            "            pass",
            "    ",
            "    # JSON/JSONB types - precise matching for Dict/List/Any types",
            "    if (_matches_type_pattern(expected_type, 'dict') or ",
            "        _matches_type_pattern(expected_type, 'list') or ",
            "        _matches_type_pattern(expected_type, 'any')):",
            "        if field.strip().startswith(('{', '[')):",
            "            try:",
            "                return json.loads(field)",
            "            except (json.JSONDecodeError, ValueError) as e:",
            "                # Enhanced error context for debugging",
            "                if getattr(_convert_postgresql_value_typed, 'debug_mode', False):",
            "                    print(f\"JSON conversion failed: field='{field}', expected='{expected_type}', error={e}\")",
            "                pass",
            "    ",
        ]
    )

    # Add enum handling based on whether we have enum types
    if enum_types:
        lines.extend(
            [
                "    # Enum types - use registry-based conversion (replaces fragile sys._getframe approach)",
                "    # Enum types are typically PascalCase and don't contain common type hints",
                "    if (expected_type and ",
                "        expected_type[0].isupper() and ",
                "        not any(_matches_type_pattern(expected_type, pattern) for pattern in _TYPE_PATTERNS.keys()) and",
                "        not any(expected_type.lower().startswith(hint + '[') for hint in ['optional', 'list', 'dict'])):",
                "        # Use the registry-based enum conversion",
                "        converted_value = _ENUM_REGISTRY.convert_enum_value(field, expected_type)",
                "        if converted_value != field:  # Conversion succeeded",
                "            return converted_value",
                "    ",
            ]
        )
    else:
        lines.extend(
            [
                "    # No enum types detected - enum conversion skipped",
                "    ",
            ]
        )

    lines.extend(
        [
            "    # For all other values, keep as string",
            "    return field",
            "",
        ]
    )

    return lines


def generate_type_aware_composite_parser() -> list[str]:
    """
    Generates a helper function to parse PostgreSQL composite type strings with type awareness.

    Returns:
        List of code lines for the type-aware parser function
    """
    return [
        "def _parse_composite_string_typed(composite_str: str, field_types: List[str]) -> tuple:",
        '    """Parse a PostgreSQL composite type string representation with type awareness."""',
        "    if not composite_str or not composite_str.startswith('(') or not composite_str.endswith(')'):",
        "        raise ValueError(f'Invalid composite string format: {composite_str}')",
        "    ",
        "    # Remove outer parentheses",
        "    content = composite_str[1:-1]",
        "    if not content:",
        "        return ()",
        "    ",
        "    # Split by comma, but respect nested structures and quoted strings",
        "    fields = []",
        "    current_field = ''",
        "    paren_depth = 0",
        "    in_quotes = False",
        "    escape_next = False",
        "    ",
        "    for char in content:",
        "        if escape_next:",
        "            current_field += char",
        "            escape_next = False",
        "        elif char == '\\\\' and in_quotes:",
        "            current_field += char",
        "            escape_next = True",
        "        elif char == '\"':",
        "            current_field += char",
        "            in_quotes = not in_quotes",
        "        elif not in_quotes:",
        "            if char == '(':",
        "                paren_depth += 1",
        "                current_field += char",
        "            elif char == ')':",
        "                paren_depth -= 1",
        "                current_field += char",
        "            elif char == ',' and paren_depth == 0:",
        "                fields.append(current_field.strip())",
        "                current_field = ''",
        "            else:",
        "                current_field += char",
        "        else:",
        "            current_field += char",
        "    ",
        "    # Add the last field",
        "    if current_field:",
        "        fields.append(current_field.strip())",
        "    ",
        "    # Convert fields to proper Python types with type guidance",
        "    parsed_fields = []",
        "    for i, field in enumerate(fields):",
        "        field = field.strip()",
        "        if not field or field.lower() in ('null', ''):",
        "            parsed_fields.append(None)",
        "        elif field.startswith('\"') and field.endswith('\"'):",
        "            # Quoted string - remove quotes and handle escapes, then apply type conversion",
        "            unquoted_field = field[1:-1].replace('\\\\\"', '\"').replace('\\\\\\\\', '\\\\')",
        "            expected_type = field_types[i] if i < len(field_types) else 'str'",
        "            converted_field = _convert_postgresql_value_typed(unquoted_field, expected_type)",
        "            parsed_fields.append(converted_field)",
        "        else:",
        "            # Unquoted value - convert with type guidance",
        "            expected_type = field_types[i] if i < len(field_types) else 'str'",
        "            converted_field = _convert_postgresql_value_typed(field, expected_type)",
        "            parsed_fields.append(converted_field)",
        "    ",
        "    return tuple(parsed_fields)",
        "",
    ]


def generate_composite_string_parser() -> list[str]:
    """
    Generates a helper function to parse PostgreSQL composite type string representations.

    Returns:
        List of code lines for the parser function
    """
    return [
        "def _parse_composite_string(composite_str: str) -> tuple:",
        '    """Parse a PostgreSQL composite type string representation into a tuple."""',
        "    if not composite_str or not composite_str.startswith('(') or not composite_str.endswith(')'):",
        "        raise ValueError(f'Invalid composite string format: {composite_str}')",
        "    ",
        "    # Remove outer parentheses",
        "    content = composite_str[1:-1]",
        "    if not content:",
        "        return ()",
        "    ",
        "    # Split by comma, but respect nested structures and quoted strings",
        "    fields = []",
        "    current_field = ''",
        "    paren_depth = 0",
        "    in_quotes = False",
        "    escape_next = False",
        "    ",
        "    for char in content:",
        "        if escape_next:",
        "            current_field += char",
        "            escape_next = False",
        "        elif char == '\\\\' and in_quotes:",
        "            current_field += char",
        "            escape_next = True",
        "        elif char == '\"':",
        "            current_field += char",
        "            in_quotes = not in_quotes",
        "        elif not in_quotes:",
        "            if char == '(':",
        "                paren_depth += 1",
        "                current_field += char",
        "            elif char == ')':",
        "                paren_depth -= 1",
        "                current_field += char",
        "            elif char == ',' and paren_depth == 0:",
        "                fields.append(current_field.strip())",
        "                current_field = ''",
        "            else:",
        "                current_field += char",
        "        else:",
        "            current_field += char",
        "    ",
        "    # Add the last field",
        "    if current_field:",
        "        fields.append(current_field.strip())",
        "    ",
        "    # Convert fields to proper Python types",
        "    parsed_fields = []",
        "    for field in fields:",
        "        field = field.strip()",
        "        if not field or field.lower() in ('null', ''):",
        "            parsed_fields.append(None)",
        "        elif field.startswith('\"') and field.endswith('\"'):",
        "            # Quoted string - remove quotes and handle escapes",
        "            parsed_fields.append(field[1:-1].replace('\\\\\"', '\"').replace('\\\\\\\\', '\\\\'))",
        "        else:",
        "            # Unquoted value - convert PostgreSQL representations to proper Python types",
        "            converted_field = _convert_postgresql_value(field)",
        "            parsed_fields.append(converted_field)",
        "    ",
        "    return tuple(parsed_fields)",
        "",
    ]


def detect_nested_composites(
    columns: list[ReturnColumn], composite_types: dict[str, list[ReturnColumn]]
) -> dict[int, str]:
    """
    Detects which columns in a composite type are themselves composite types.

    Args:
        columns: List of columns in the composite type
        composite_types: Dictionary of all known composite types

    Returns:
        Dictionary mapping column index to the composite type name
    """
    nested_composites = {}

    for i, col in enumerate(columns):
        # Remove Optional[] wrapper if present
        python_type = col.python_type
        if python_type.startswith("Optional[") and python_type.endswith("]"):
            python_type = python_type[9:-1]

        # Check the SQL type name (might be lowercase or qualified)
        sql_type = col.sql_type
        found_type = None

        # Direct match
        if sql_type in composite_types:
            found_type = sql_type
        elif "." in sql_type:
            # Handle schema-qualified names
            unqualified = sql_type.split(".")[-1]
            if unqualified in composite_types:
                found_type = unqualified

        # Also check if the python_type matches any composite type names
        # This handles cases where python_type has already been converted to CamelCase
        if not found_type:
            for comp_type_name in composite_types:
                # Convert composite type name to CamelCase for comparison
                from ..parser.utils import _to_singular_camel_case

                camel_case_name = _to_singular_camel_case(comp_type_name)
                if python_type == camel_case_name:
                    found_type = comp_type_name
                    break

        if found_type:
            nested_composites[i] = found_type

    return nested_composites


def should_use_type_aware_parsing(
    columns: list[ReturnColumn], composite_types: dict[str, list[ReturnColumn]] | None = None
) -> bool:
    """
    Determines if type-aware parsing should be used based on column types.

    Type-aware parsing is beneficial when we have:
    - Boolean types (that would benefit from 't'/'f' conversion)
    - Decimal types (that would benefit from proper numeric conversion)
    - UUID, DateTime, or other specialized types
    - Enum types (that would benefit from string-to-enum conversion)
    - Nested composite types that contain any of the above

    Args:
        columns: List of columns in the composite type
        composite_types: Dictionary of all known composite types (for recursive checking)

    Returns:
        True if type-aware parsing would be beneficial
    """
    composite_types = composite_types or {}

    def _check_column_needs_type_aware_parsing(col: ReturnColumn, visited: set = None) -> bool:
        """Recursively check if a column needs type-aware parsing."""
        visited = visited or set()

        python_type = col.python_type
        python_type_lower = python_type.lower()

        # Remove Optional[] wrapper if present
        if python_type.startswith("Optional[") and python_type.endswith("]"):
            python_type = python_type[9:-1]
            python_type_lower = python_type.lower()

        # Check for types that benefit from type-aware parsing
        if any(type_hint in python_type_lower for type_hint in ["bool", "decimal", "uuid", "datetime", "dict", "list"]):
            return True

        # Check for enum types - enum types are typically PascalCase and don't contain common type hints
        # This ensures composite types with enum fields use type-aware parsing for enum conversion
        # Use exact matching to avoid false matches (e.g. 'any' in 'CompanyRole')
        exact_common_types = ["int", "str", "bool", "float", "decimal", "uuid", "datetime", "any", "dict", "list"]
        has_exact_match = python_type_lower in exact_common_types

        # Also check for generic type patterns like Optional[...], List[...], etc.
        has_generic_pattern = (
            python_type_lower.startswith(("optional[", "list[", "dict["))
            or "[" in python_type_lower  # Any generic type with brackets
        )

        has_common_hints = has_exact_match or has_generic_pattern

        if python_type and python_type[0].isupper() and not has_common_hints:
            # Check if this is a known composite type
            # Look for matching composite type by checking both snake_case and CamelCase forms
            composite_key = None
            for comp_name in composite_types:
                from ..parser.utils import _to_singular_camel_case

                if _to_singular_camel_case(comp_name) == python_type or comp_name == python_type:
                    composite_key = comp_name
                    break

            if composite_key and composite_key not in visited:
                # This is a known composite type - recursively check its fields
                visited.add(composite_key)
                composite_columns = composite_types[composite_key]
                for nested_col in composite_columns:
                    if _check_column_needs_type_aware_parsing(nested_col, visited):
                        return True
            else:
                # This looks like an enum type (not a known composite type)
                return True

        return False

    # Check all columns for type-aware parsing needs
    for col in columns:
        if _check_column_needs_type_aware_parsing(col):
            return True

    return False


def generate_composite_unpacking_code(
    class_name: str, columns: list[ReturnColumn], composite_types: dict[str, list[ReturnColumn]], indent: str = "    "
) -> list[str]:
    """
    Generates code to properly unpack a composite type with nested composites.

    Args:
        class_name: Name of the dataclass being created
        columns: List of columns in the composite type
        composite_types: Dictionary of all known composite types
        indent: Base indentation level

    Returns:
        List of code lines for unpacking the composite type
    """
    nested_composites = detect_nested_composites(columns, composite_types)
    use_type_aware = should_use_type_aware_parsing(columns, composite_types)

    if not nested_composites and not use_type_aware:
        # No nested composites and no type-aware parsing needed, use simple unpacking
        return [
            f"{indent}instance = {class_name}(*row)",
            f"{indent}# Check for 'empty' composite rows (all values are None) returned as a single tuple",
            f"{indent}if all(v is None for v in row):",
            f"{indent}    return None",
            f"{indent}return instance",
        ]

    # Use global helper functions instead of generating inline ones
    lines = []

    if nested_composites or use_type_aware:
        # We need to process the row field by field
        lines.append(f"{indent}# Process fields with type awareness and/or nested composite handling")
        if use_type_aware:
            # Add field types for type-aware parsing
            field_types = [col.python_type for col in columns]
            lines.append(f"{indent}field_types = {field_types!r}")

        lines.append(f"{indent}field_values = []")
        lines.append(f"{indent}for i, value in enumerate(row):")

        # Generate if-elif chain for each nested composite
        if nested_composites:
            first = True
            for col_idx, composite_type in nested_composites.items():
                col = columns[col_idx]
                # Get the Python class name for the composite type
                # Always convert to CamelCase class name
                from ..parser.utils import _to_singular_camel_case

                python_class_name = _to_singular_camel_case(composite_type)

                if first:
                    lines.append(f"{indent}    if i == {col_idx}:")
                    first = False
                else:
                    lines.append(f"{indent}    elif i == {col_idx}:")

                lines.append(f"{indent}        # Column '{col.name}' is a nested composite type")
                lines.append(f"{indent}        if value is None:")
                lines.append(f"{indent}            field_values.append(None)")
                lines.append(f"{indent}        elif isinstance(value, tuple):")
                lines.append(f"{indent}            # Recursively create nested dataclass")
                lines.append(f"{indent}            field_values.append({python_class_name}(*value))")
                lines.append(
                    f"{indent}        elif isinstance(value, str) and value.startswith('(') and value.endswith(')'):"
                )
                lines.append(f"{indent}            # Parse composite string representation")
                lines.append(f"{indent}            try:")
                if use_type_aware:
                    lines.append(f"{indent}                # Use type-aware parsing for nested composite")
                    # Get the field types for the nested composite at generation time
                    nested_field_types = [col.python_type for col in composite_types[composite_type]]
                    lines.append(f"{indent}                nested_field_types = {nested_field_types!r}")
                    lines.append(
                        f"{indent}                parsed_tuple = _parse_composite_string_typed(value, nested_field_types)"
                    )
                else:
                    lines.append(f"{indent}                parsed_tuple = _parse_composite_string(value)")
                lines.append(f"{indent}                field_values.append({python_class_name}(*parsed_tuple))")
                lines.append(f"{indent}            except (ValueError, TypeError) as e:")
                lines.append(
                    f"{indent}                raise ValueError(f'Failed to parse nested composite type {{{python_class_name}}} from string: {{value!r}}. Error: {{e}}')"
                )
                lines.append(f"{indent}        else:")
                lines.append(f"{indent}            # Already a dataclass instance or other value")
                lines.append(f"{indent}            field_values.append(value)")

            lines.append(f"{indent}    else:")
            lines.append(f"{indent}        # Regular field")
        else:
            lines.append(f"{indent}    # Regular field processing")

        if use_type_aware:
            lines.append(f"{indent}        # Apply type-aware conversion for regular fields")
            lines.append(
                f"{indent}        if isinstance(value, str) and value.startswith('(') and value.endswith(')'):"
            )
            lines.append(f"{indent}            # This might be a composite string that needs parsing")
            lines.append(f"{indent}            try:")
            lines.append(f"{indent}                parsed_tuple = _parse_composite_string_typed(value, field_types)")
            lines.append(
                f"{indent}                # If this succeeds, we had a composite string, use the parsed result"
            )
            lines.append(f"{indent}                if len(parsed_tuple) == len(field_types):")
            lines.append(f"{indent}                    # Replace the entire row with parsed values")
            lines.append(f"{indent}                    field_values = list(parsed_tuple)")
            lines.append(f"{indent}                    break")
            lines.append(f"{indent}                else:")
            lines.append(f"{indent}                    # Fallback to regular processing")
            lines.append(f"{indent}                    field_values.append(value)")
            lines.append(f"{indent}            except (ValueError, TypeError):")
            lines.append(f"{indent}                # Not a composite string, treat as regular field")
            lines.append(f"{indent}                expected_type = field_types[i] if i < len(field_types) else 'str'")
            lines.append(
                f"{indent}                field_values.append(_convert_postgresql_value_typed(value, expected_type))"
            )
            lines.append(f"{indent}        else:")
            lines.append(f"{indent}            # Regular field, apply type-aware conversion")
            lines.append(f"{indent}            expected_type = field_types[i] if i < len(field_types) else 'str'")
            lines.append(
                f"{indent}            field_values.append(_convert_postgresql_value_typed(value, expected_type))"
            )
        else:
            lines.append(f"{indent}        field_values.append(value)")

        lines.append(f"{indent}")
        lines.append(f"{indent}# Create the main dataclass instance")
        lines.append(f"{indent}instance = {class_name}(*field_values)")
        lines.append(f"{indent}")
        lines.append(f"{indent}# Check for 'empty' composite rows")
        lines.append(f"{indent}if all(v is None for v in field_values):")
        lines.append(f"{indent}    return None")
        lines.append(f"{indent}")
        lines.append(f"{indent}return instance")

    return lines


def generate_enum_registration_section(enum_types: dict[str, list[str]] = None) -> list[str]:
    """
    Generates enum registration calls to populate the enum registry.

    Args:
        enum_types: Dictionary mapping enum names to their possible values

    Returns:
        List of code lines for enum registration
    """
    enum_types = enum_types or {}

    if not enum_types:
        return []

    lines = [
        "# Register enum classes with the registry (Phase 2.2: Enum Discovery Enhancement)",
        "# This eliminates the need for fragile sys._getframe() lookups at runtime",
    ]

    for enum_name in enum_types.keys():
        from ..parser.utils import _to_singular_camel_case

        python_enum_name = _to_singular_camel_case(enum_name)
        lines.append(f"_ENUM_REGISTRY.register_enum('{python_enum_name}', {python_enum_name})")

    return lines


def generate_global_helper_functions(enum_types: dict[str, list[str]] = None) -> list[str]:
    """
    Generates global helper functions to be placed at module level.

    Args:
        enum_types: Dictionary mapping enum names to their possible values

    Returns:
        List of code lines for the global helper functions
    """
    enum_types = enum_types or {}
    lines = []

    # Add the type-aware converter function with enum support
    lines.extend(generate_type_aware_converter(enum_types))
    lines.append("")

    # Add the type-aware composite parser function
    lines.extend(generate_type_aware_composite_parser())
    lines.append("")

    # Add the basic composite parser function (still needed for backwards compatibility)
    lines.extend(generate_composite_string_parser())
    lines.append("")

    # Add the basic value converter function (still needed for backwards compatibility)
    lines.extend(generate_postgresql_value_converter())
    lines.append("")

    return lines


def needs_global_helpers(functions: list, composite_types: dict[str, list[ReturnColumn]]) -> bool:
    """
    Determines if any functions in the module need global helper functions.

    Args:
        functions: List of parsed functions
        composite_types: Dictionary of all known composite types

    Returns:
        True if global helpers are needed, False otherwise
    """
    # Check if any function has return columns that need special handling
    for func in functions:
        if hasattr(func, "return_columns") and func.return_columns:
            if needs_nested_unpacking(func.return_columns, composite_types):
                return True
    return False


def needs_nested_unpacking(columns: list[ReturnColumn], composite_types: dict[str, list[ReturnColumn]]) -> bool:
    """
    Checks if a composite type needs special handling for nested composites or type-aware parsing.

    Args:
        columns: List of columns in the composite type
        composite_types: Dictionary of all known composite types

    Returns:
        True if the type has nested composites or needs type-aware parsing, False otherwise
    """
    # Check for nested composites or type-aware parsing needs (enums, booleans, etc.)
    return bool(
        detect_nested_composites(columns, composite_types) or should_use_type_aware_parsing(columns, composite_types)
    )
