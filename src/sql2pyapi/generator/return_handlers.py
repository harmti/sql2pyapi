# ===== SECTION: IMPORTS AND SETUP =====
# Standard library and third-party imports
from typing import List, Dict, Tuple, Optional

# Local imports
from ..sql_models import ParsedFunction, ReturnColumn, SQLParameter
from ..constants import *
from .utils import _to_singular_camel_case


def _determine_return_type(func: ParsedFunction, custom_types: Dict[str, List[ReturnColumn]]) -> Tuple[str, Optional[str], set]:
    """
    Determines the Python return type hint and dataclass name for a SQL function.
    Now also returns the set of imports required for the return type.
    
    Args:
        func (ParsedFunction): The parsed SQL function definition
        custom_types (Dict[str, List[ReturnColumn]]): Dictionary of custom types fields
    
    Returns:
        Tuple[str, Optional[str], set]: A tuple containing:
            - The Python return type hint as a string (e.g., 'int', 'List[User]')
            - The dataclass name for table returns, or None for scalar/record returns
            - The set of imports required for the return type
    """
    # Since we've consolidated the return type determination in the parser,
    # this function is now much simpler
    current_imports = set()
    
    # Use the return_type_hint already determined by the parser
    return_type_hint = func.return_type_hint or func.return_type
    
    # Use the dataclass_name already determined by the parser
    final_dataclass_name = func.dataclass_name
    
    # Handle schema-qualified table names for dataclass names
    if final_dataclass_name and '.' in final_dataclass_name:
        # Convert schema.table_name to singular CamelCase (e.g., public.companies -> Company)
        final_dataclass_name = _to_singular_camel_case(final_dataclass_name)
        
        # Update the return type hint with the correct dataclass name
        if func.returns_setof:
            return_type_hint = f"List[{final_dataclass_name}]"
        else:
            return_type_hint = f"Optional[{final_dataclass_name}]"
    
    # If we have a dataclass name, ensure dataclass is imported
    if final_dataclass_name:
        current_imports.add(PYTHON_IMPORTS["dataclass"])
    
    # Extract base types from the return type hint for import collection
    if return_type_hint != "None":
        # Determine the base type within the hint for import purposes
        base_type_in_hint = return_type_hint.replace("Optional[", "").replace("List[", "").replace("]", "")
        
        # Add necessary imports based on the hint structure
        if "List[" in return_type_hint:
            current_imports.add(PYTHON_IMPORTS.get("List"))
        if "Optional[" in return_type_hint:
            current_imports.add(PYTHON_IMPORTS.get("Optional"))
        if "Tuple" in return_type_hint:
            current_imports.add(PYTHON_IMPORTS.get("Tuple"))
        if "Any" in return_type_hint:
            current_imports.add(PYTHON_IMPORTS.get("Any"))
            
        # Add import for the base type itself (int, str, UUID, etc.)
        import_stmt = PYTHON_IMPORTS.get(base_type_in_hint)
        if import_stmt:
            current_imports.add(import_stmt)
    # Also add any imports the parser collected
    for imp_name in func.required_imports:
        if imp_name in PYTHON_IMPORTS:
            current_imports.add(PYTHON_IMPORTS[imp_name])
    
    # Add imports for column types in return_columns if we have a dataclass
    if final_dataclass_name and func.return_columns:
        for col in func.return_columns:
            # Extract base type and add imports
            col_base_type = col.python_type.replace("Optional[", "").replace("List[", "").replace("]", "")
            if col_base_type in PYTHON_IMPORTS:
                current_imports.add(PYTHON_IMPORTS[col_base_type])
            if "Optional[" in col.python_type:
                current_imports.add(PYTHON_IMPORTS["Optional"])
            if "List[" in col.python_type:
                current_imports.add(PYTHON_IMPORTS["List"])


    # Clean up None entries from the set of collected imports
    required_imports = {imp for imp in current_imports if imp}
    
    return return_type_hint, final_dataclass_name, required_imports

