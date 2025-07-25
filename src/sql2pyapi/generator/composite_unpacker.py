"""Helper module for handling nested composite type unpacking in generated code."""

import re
from typing import List, Dict
from ..sql_models import ReturnColumn


def generate_composite_string_parser() -> List[str]:
    """
    Generates a helper function to parse PostgreSQL composite type string representations.
    
    Returns:
        List of code lines for the parser function
    """
    return [
        "def _parse_composite_string(composite_str: str) -> tuple:",
        "    \"\"\"Parse a PostgreSQL composite type string representation into a tuple.\"\"\"",
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
        "            # Unquoted value - keep as string for now",
        "            # The dataclass constructor will handle type conversion",
        "            parsed_fields.append(field)",
        "    ",
        "    return tuple(parsed_fields)",
        "",
    ]


def detect_nested_composites(columns: List[ReturnColumn], 
                           composite_types: Dict[str, List[ReturnColumn]]) -> Dict[int, str]:
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
        elif '.' in sql_type:
            # Handle schema-qualified names
            unqualified = sql_type.split('.')[-1]
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


def generate_composite_unpacking_code(class_name: str,
                                    columns: List[ReturnColumn],
                                    composite_types: Dict[str, List[ReturnColumn]],
                                    indent: str = "    ") -> List[str]:
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
    
    if not nested_composites:
        # No nested composites, use simple unpacking
        return [
            f"{indent}instance = {class_name}(*row)",
            f"{indent}# Check for 'empty' composite rows (all values are None) returned as a single tuple",
            f"{indent}if all(v is None for v in row):",
            f"{indent}    return None",
            f"{indent}return instance"
        ]
    
    # Generate the composite string parser function inline
    lines = []
    lines.append(f"{indent}# Helper function to parse composite string representations")
    parser_lines = generate_composite_string_parser()
    for parser_line in parser_lines:
        lines.append(f"{indent}{parser_line}")
    lines.append("")
    
    # Generate code for nested composite unpacking
    lines.append(f"{indent}# Handle nested composite types")
    lines.append(f"{indent}field_values = []")
    lines.append(f"{indent}for i, value in enumerate(row):")
    
    # Generate if-elif chain for each nested composite
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
        lines.append(f"{indent}        elif isinstance(value, str) and value.startswith('(') and value.endswith(')'):")
        lines.append(f"{indent}            # Parse composite string representation")
        lines.append(f"{indent}            try:")
        lines.append(f"{indent}                parsed_tuple = _parse_composite_string(value)")
        lines.append(f"{indent}                field_values.append({python_class_name}(*parsed_tuple))")
        lines.append(f"{indent}            except (ValueError, TypeError) as e:")
        lines.append(f"{indent}                raise ValueError(f'Failed to parse nested composite type {{{python_class_name}}} from string: {{value!r}}. Error: {{e}}')")
        lines.append(f"{indent}        else:")
        lines.append(f"{indent}            # Already a dataclass instance or other value")
        lines.append(f"{indent}            field_values.append(value)")
    
    lines.append(f"{indent}    else:")
    lines.append(f"{indent}        # Regular field")
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


def needs_nested_unpacking(columns: List[ReturnColumn],
                         composite_types: Dict[str, List[ReturnColumn]]) -> bool:
    """
    Checks if a composite type needs special handling for nested composites.
    
    Args:
        columns: List of columns in the composite type
        composite_types: Dictionary of all known composite types
        
    Returns:
        True if the type has nested composites, False otherwise
    """
    return bool(detect_nested_composites(columns, composite_types))