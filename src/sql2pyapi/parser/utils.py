# ===== SECTION: IMPORTS =====
import re
import inflection

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


def _to_singular_camel_case(name: str) -> str:
    """
    Converts snake_case plural table names to SingularCamelCase for dataclass names.
    Handles schema-qualified names by removing the schema prefix.
    
    Args:
        name (str): The table name, typically plural (e.g., 'user_accounts' or 'public.users')
        
    Returns:
        str: A singular CamelCase name suitable for a dataclass (e.g., 'UserAccount')
        
    Examples:
        >>> _to_singular_camel_case('users')
        'User'
        >>> _to_singular_camel_case('order_items')
        'OrderItem'
        >>> _to_singular_camel_case('public.companies')
        'Company'
    """
    if not name:
        return "ResultRow" # Default for empty names, consistent with generator/utils
        
    # Handle schema-qualified names (e.g., 'public.companies')
    # Extract just the table name part
    table_name_part = name.split('.')[-1]
    
    # Use inflection library for better singularization
    singular_snake = inflection.singularize(table_name_part)
    # Convert snake_case to CamelCase
    camel_case_name = inflection.camelize(singular_snake)
    
    # Ensure the name starts with a letter, if not, prefix (optional, but good practice from sanitize_for_class_name)
    if camel_case_name and not camel_case_name[0].isalpha():
        camel_case_name = 'T_' + camel_case_name # Or some other suitable prefix
    if not camel_case_name:
        return "ResultRow" # Fallback if somehow empty after processing
        
    return camel_case_name
