# ===== SECTION: IMPORTS AND SETUP =====
# Standard library and third-party imports
import inflection  # Using inflection library for plural->singular

# Local imports
from ..sql_models import ParsedFunction, ReturnColumn, SQLParameter
from ..constants import *


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
        return "ResultRow"
        
    # Handle schema-qualified names (e.g., 'public.companies')
    # Extract just the table name part
    table_name = name.split('.')[-1]
    
    # Simple pluralization check (can be improved)
    # Use inflection library for better singularization
    singular_snake = inflection.singularize(table_name)
    # Convert snake_case to CamelCase
    return inflection.camelize(singular_snake)


