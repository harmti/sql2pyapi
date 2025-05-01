# ===== SECTION: IMPORTS =====
import re

# ===== SECTION: FUNCTIONS =====

def sanitize_for_class_name(name: str) -> str:
    """
    Sanitizes a SQL table/type name for use as a Python class name.
    Handles schema-qualified names by removing the schema prefix.
    
    Args:
        name (str): The SQL table/type name
        
    Returns:
        str: A sanitized class name
    """
    # Remove schema prefix if present (e.g., 'public.users' -> 'users')
    if '.' in name:
        name = name.split('.')[-1]
    
    # Replace special characters with underscores
    sanitized = re.sub(r'[^a-zA-Z0-9_]', '_', name)
    
    # Ensure the name starts with a letter
    if sanitized and not sanitized[0].isalpha():
        sanitized = 'T_' + sanitized
    
    # Capitalize the name (CamelCase)
    parts = sanitized.split('_')
    sanitized = ''.join(p.capitalize() for p in parts if p)
    
    # Ensure we have a valid name
    if not sanitized:
        sanitized = 'Type'
    
    return sanitized


def generate_dataclass_name(sql_func_name: str, is_return: bool = False) -> str:
    """
    Generates a Pythonic class name based on the SQL function name.
    Handles schema-qualified names and ensures consistent naming for return types.
    
    Args:
        sql_func_name (str): The SQL function name, possibly schema-qualified
        is_return (bool): Whether this is for a return type (adds 'Result' suffix)
        
    Returns:
        str: A valid Python class name in PascalCase
    """
    # Remove schema prefix if present
    if '.' in sql_func_name:
        func_name = sql_func_name.split('.')[-1]
    else:
        func_name = sql_func_name
    
    # Split by underscore and convert to PascalCase
    parts = func_name.split('_')
    pascal_case = ''.join(part.capitalize() for part in parts if part)
    
    # Add 'Result' suffix for return types if requested
    if is_return:
        pascal_case += 'Result'
    
    return pascal_case
