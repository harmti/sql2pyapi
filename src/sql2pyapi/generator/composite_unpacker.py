"""Helper module for handling nested composite type unpacking in generated code."""

from typing import List, Dict
from ..sql_models import ReturnColumn


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
    
    # Generate code for nested composite unpacking
    lines = []
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
        lines.append(f"{indent}        else:")
        lines.append(f"{indent}            # Already a dataclass instance")
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