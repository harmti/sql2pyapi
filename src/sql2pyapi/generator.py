# ===== SECTION: IMPORTS AND SETUP =====
# Standard library and third-party imports
from typing import List, Dict, Tuple, Optional
import textwrap
import inflection  # Using inflection library for plural->singular
from pathlib import Path
import os

# Local imports
from .parser import ParsedFunction, ReturnColumn, SQLParameter
from .constants import *

# ===== SECTION: CONSTANTS AND CONFIGURATION =====
# Define PYTHON_IMPORTS locally as well for fallback cases
PYTHON_IMPORTS = {
    "Any": "from typing import Any",
    "List": "from typing import List",
    "Optional": "from typing import Optional",
    "Dict": "from typing import Dict",
    "Tuple": "from typing import Tuple",
    "UUID": "from uuid import UUID",
    "datetime": "from datetime import datetime",
    "date": "from datetime import date",
    "Decimal": "from decimal import Decimal",
    "dataclass": "from dataclasses import dataclass"
    # Add others if needed directly by generator logic, but prefer parser-provided imports
}

# ===== SECTION: UTILITY FUNCTIONS =====
# Helper functions for name transformation and code generation

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


# ===== SECTION: DATACLASS GENERATION =====
# Functions for generating Python dataclasses from SQL table definitions

def _generate_dataclass(class_name: str, columns: List[ReturnColumn], make_fields_optional: bool = False) -> str:
    """
    Generates a Python dataclass definition string based on SQL column definitions.
    
    Args:
        class_name (str): Name for the generated dataclass
        columns (List[ReturnColumn]): Column definitions extracted from SQL
        make_fields_optional (bool): Whether to make all fields Optional, regardless of
                                    their nullability in the database schema
    
    Returns:
        str: Python code for the dataclass definition as a string
    
    Notes:
        - If columns list is empty or only contains an 'unknown' column, a TODO comment
          will be generated instead of a complete dataclass
        - Column types are mapped from SQL to Python types by the parser
    """
    if not columns or (len(columns) == 1 and columns[0].name == "unknown"):
        # Handle case where schema wasn't found or columns couldn't be parsed
        sql_table_name = columns[0].sql_type if columns else "unknown_table"
        return f"""# TODO: Define dataclass for table '{sql_table_name}'
# @dataclass
# class {class_name}:
#     pass"""

    fields = []
    for col in columns:
        field_type = col.python_type
        # Wrap with Optional if needed based on column's optionality OR if forced for RETURNS TABLE
        if make_fields_optional and not field_type.startswith("Optional["):
            field_type = f"Optional[{field_type}]"
        elif col.is_optional and not field_type.startswith("Optional["):
            # This case should already be handled by _map_sql_to_python_type, but double-check
            # We might have removed Optional in mapping if is_optional was False initially
            # Re-add Optional if the parser determined it should be optional now
            field_type = f"Optional[{field_type}]"

        fields.append(f"    {col.name}: {field_type}")

    fields_str = "\n".join(fields)
    return f"""@dataclass
class {class_name}:
{fields_str}
"""


# ===== SECTION: FUNCTION GENERATION =====
# Functions for generating Python async functions from SQL function definitions

def _generate_parameter_list(func_params: List[SQLParameter]) -> Tuple[List[SQLParameter], str]:
    """
    Generates a sorted parameter list and parameter string for a Python function.
    
    Args:
        func_params (List[SQLParameter]): The parameters from the parsed SQL function
        
    Returns:
        Tuple[List[SQLParameter], str]: A tuple containing:
            - The sorted parameters list (required params first, then optional)
            - The formatted parameter string for the Python function signature
    """
    # Sort parameters: non-optional first, then optional
    non_optional_params = [p for p in func_params if not p.is_optional]
    optional_params = [p for p in func_params if p.is_optional]
    sorted_params = non_optional_params + optional_params

    # Build the parameter list string for the Python function signature
    params_list_py = ["conn: AsyncConnection"]
    for p in sorted_params:
        params_list_py.append(f"{p.python_name}: {p.python_type}{' = None' if p.is_optional else ''}")
    params_str_py = ", ".join(params_list_py)
    
    return sorted_params, params_str_py


def _generate_docstring(func: ParsedFunction) -> str:
    """
    Generates a properly formatted docstring for a Python function.
    
    Args:
        func (ParsedFunction): The parsed SQL function definition
        
    Returns:
        str: The formatted docstring with proper indentation
    
    Notes:
        - Uses the SQL comment if available, otherwise generates a default docstring
        - Handles both single-line and multi-line docstrings with proper indentation
    """
    docstring_lines = []
    if func.sql_comment:
        comment_lines = func.sql_comment.strip().splitlines()
        if len(comment_lines) == 1:
            # Single line docstring
            docstring_lines.append(f'    """{comment_lines[0]}"""')
        else:
            # Multi-line docstring
            docstring_lines.append(f'    """{comment_lines[0]}')  # First line on same line as opening quotes
            # Indent subsequent lines relative to the function body (4 spaces)
            for line in comment_lines[1:]:
                docstring_lines.append(f"    {line}")  # Add 4 spaces for base indentation
            docstring_lines.append('    """')  # Closing quotes on new line, indented
    else:
        # Default docstring
        docstring_lines.append(f'    """Call PostgreSQL function {func.sql_name}()."""')

    return "\n".join(docstring_lines)


def _generate_function_body(func: ParsedFunction, final_dataclass_name: Optional[str], sql_args_placeholders: str, python_args_list: str) -> List[str]:
    """
    Generates the body of a Python async function based on the SQL function's return type.
    
    Args:
        func (ParsedFunction): The parsed SQL function definition
        final_dataclass_name (Optional[str]): The name of the dataclass for table returns
        sql_args_placeholders (str): Placeholders for SQL query parameters
        python_args_list (str): Python arguments list for the execute call
        
    Returns:
        List[str]: Lines of code for the function body
    
    Notes:
        - Handles different return types: void, scalar, record, table, setof
        - Implements proper NULL handling for both None rows and composite NULL rows
        - Uses constants from constants.py for consistent code generation
    """
    body_lines = []
    
    # Common setup for all function types
    body_lines.append("async with conn.cursor() as cur:")
    body_lines.append(
        f'    await cur.execute("SELECT * FROM {func.sql_name}({sql_args_placeholders})", {python_args_list})'
    )
    
    # Handle different return types
    if not func.returns_table and func.return_type == "None":
        # Void function - simplest case, just execute and return None
        body_lines.append("    return None")
    elif func.returns_setof:
        # Handle SETOF returns (multiple rows)
        body_lines.extend(_generate_setof_return_body(func, final_dataclass_name))
    else:
        # Handle single row returns
        body_lines.extend(_generate_single_row_return_body(func, final_dataclass_name))
        
    return body_lines


def _generate_setof_return_body(func: ParsedFunction, final_dataclass_name: Optional[str]) -> List[str]:
    """
    Generates code for handling SETOF returns (multiple rows).
    
    Args:
        func (ParsedFunction): The parsed SQL function definition
        final_dataclass_name (Optional[str]): The name of the dataclass for table returns
        
    Returns:
        List[str]: Lines of code for handling SETOF returns
    """
    body_lines = []
    body_lines.append("    rows = await cur.fetchall()")
    
    if func.returns_table:
        # SETOF table_name or RETURNS TABLE
        body_lines.append(f"    # Ensure dataclass '{final_dataclass_name}' is defined above.")
        body_lines.append("    if not rows:")
        body_lines.append("        return []")
        body_lines.append("    colnames = [desc[0] for desc in cur.description]")
        body_lines.append("    processed_rows = [")
        body_lines.append("        dict(zip(colnames, r)) if not isinstance(r, dict) else r")
        body_lines.append("        for r in rows")
        body_lines.append("    ]")
        body_lines.append(f"    return [{final_dataclass_name}(**row_dict) for row_dict in processed_rows]")
    elif func.returns_record:
        # SETOF record
        body_lines.append("    # Return list of tuples for SETOF record")
        body_lines.append("    return rows")
    else:
        # SETOF scalar
        body_lines.append("    # Assuming SETOF returns list of single-element tuples for scalars")
        body_lines.append("    return [row[0] for row in rows if row]")
        
    return body_lines


def _generate_single_row_return_body(func: ParsedFunction, final_dataclass_name: Optional[str]) -> List[str]:
    """
    Generates code for handling single row returns.
    
    Args:
        func (ParsedFunction): The parsed SQL function definition
        final_dataclass_name (Optional[str]): The name of the dataclass for table returns
        
    Returns:
        List[str]: Lines of code for handling single row returns
    """
    body_lines = []
    body_lines.append("    row = await cur.fetchone()")
    body_lines.append("    if row is None:")
    body_lines.append("        return None")
    
    # All row processing happens after the None check
    if func.returns_table:
        # Table/composite type return
        body_lines.append(f"    # Ensure dataclass '{final_dataclass_name}' is defined above.")
        body_lines.append("    colnames = [desc[0] for desc in cur.description]")
        body_lines.append("    row_dict = dict(zip(colnames, row)) if not isinstance(row, dict) else row")
        # Handle PostgreSQL composite type returns with all NULL values
        body_lines.append("    # Check for 'empty' composite rows (all values are None)")
        body_lines.append("    if all(value is None for value in row_dict.values()):")
        body_lines.append("        return None")
        body_lines.append(f"    return {final_dataclass_name}(**row_dict)")
    elif func.returns_record:
        # Record return
        body_lines.append("    # Return tuple for record type")
        body_lines.append("    return row")
    else:
        # Scalar return
        body_lines.append("    if isinstance(row, dict):")
        body_lines.append(f"        # Assumes the key is the function name for dict rows")
        body_lines.append(f"        return row[{repr(func.sql_name)}]")
        body_lines.append("    else:")
        body_lines.append("        # Fallback for tuple-like rows (index 0)")
        body_lines.append("        return row[0]")
        
    return body_lines


def _determine_return_type(func: ParsedFunction, class_name_map: Dict[str, str]) -> Tuple[str, Optional[str]]:
    """
    Determines the Python return type hint and dataclass name for a SQL function.
    
    Args:
        func (ParsedFunction): The parsed SQL function definition
        class_name_map (Dict[str, str]): Mapping of SQL table names to Python class names
        
    Returns:
        Tuple[str, Optional[str]]: A tuple containing:
            - The Python return type hint as a string (e.g., 'int', 'List[User]')
            - The dataclass name for table returns, or None for scalar/record returns
    """
    return_type_hint = "None"  # Default
    final_dataclass_name = None

    if func.returns_table:
        if func.setof_table_name:
            # Case: RETURNS SETOF table_name (schema potentially found)
            # Get the standardized class name from the map
            final_dataclass_name = class_name_map.get(
                func.setof_table_name, _to_singular_camel_case(func.setof_table_name)
            )
        else:
            # Case: Explicit RETURNS TABLE(...)
            # Generate a unique name based on function name
            # Apply CamelCase directly and append Result suffix
            final_dataclass_name = inflection.camelize(func.python_name) + "Result"

        # Ensure the name is stored in the map for the function generation pass
        # This handles both SETOF table_name and explicit RETURNS TABLE cases
        func_key = f"_func_{func.sql_name}" 
        class_name_map[func_key] = final_dataclass_name 

        return_type_hint = final_dataclass_name
        if func.returns_setof:
            return_type_hint = f"List[{final_dataclass_name}]"
        else:
            return_type_hint = f"Optional[{final_dataclass_name}]"
    elif func.return_type != "None":
        # Parser already determined Optional/List wrapping for scalar/record types
        return_type_hint = func.return_type
        
    return return_type_hint, final_dataclass_name


def _generate_function(func: ParsedFunction, class_name_map: Dict[str, str]) -> str:
    """
    Generates a Python async function string from a parsed SQL function definition.
    
    This is the core code generation function that creates Python wrapper functions
    for PostgreSQL functions. It handles different return types (scalar, record, table),
    parameter ordering, docstring generation, and proper NULL handling.
    
    Args:
        func (ParsedFunction): The parsed SQL function definition
        class_name_map (Dict[str, str]): Mapping of SQL table names to Python class names
    
    Returns:
        str: Python code for the async function as a string
    
    Notes:
        - Parameters are sorted with required parameters first, then optional ones
        - Return type is determined based on the SQL function's return type
        - Special handling is implemented for different PostgreSQL return styles
        - NULL handling is carefully implemented for both None rows and composite NULL rows
    """
    
    # Generate the parameter list and signature
    sorted_params, params_str_py = _generate_parameter_list(func.params)

    # Determine return type hint and dataclass name
    return_type_hint, final_dataclass_name = _determine_return_type(func, class_name_map)

    sql_args_placeholders = ", ".join(["%s"] * len(func.params))
    # Use sorted params for the execute call arguments list
    python_args_list = "[" + ", ".join([p.python_name for p in sorted_params]) + "]"

    # Generate the docstring
    docstring = _generate_docstring(func)

    # Generate the function body based on the return type
    body_lines = _generate_function_body(func, final_dataclass_name, sql_args_placeholders, python_args_list)

    indented_body = textwrap.indent("\n".join(body_lines), prefix="    ")

    # --- Assemble the function ---
    # Note: Docstring is now pre-formatted with indentation
    func_def = f"""
async def {func.python_name}({params_str_py}) -> {return_type_hint}:
{docstring}
{indented_body}
"""
    return func_def


# ===== SECTION: MAIN CODE GENERATION =====
# Main entry point for generating the complete Python module

def generate_python_code(
    functions: List[ParsedFunction],
    table_schema_imports: Dict[str, set],  # Accept the schema imports
    source_sql_file: str = "",
) -> str:
    """
    Generates the full Python module code as a string.
    
    This is the main entry point for code generation. It processes all parsed SQL functions
    and generates a complete Python module with imports, dataclass definitions, and
    async function implementations.
    
    Args:
        functions (List[ParsedFunction]): List of parsed SQL function definitions
        table_schema_imports (Dict[str, set]): Imports needed for table schemas
        source_sql_file (str, optional): Name of the source SQL file for header comment
    
    Returns:
        str: Complete Python module code as a string
    
    Notes:
        - Imports are automatically collected based on types used
        - Dataclasses are generated for table returns and complex types
        - Each SQL function gets a corresponding async Python function
        - The output follows a consistent structure: imports → dataclasses → functions
    """

    all_imports = set()
    dataclass_defs = {}
    generated_functions = []

    # Base typing imports like Optional, List etc are added by the parser as needed

    table_to_class_name_map: Dict[str, str] = {}
    # Rename dataclass_field_imports -> imports_per_dataclass
    imports_per_dataclass: Dict[str, set] = {}

    # --- First pass: Identify and prepare unique dataclasses ---
    processed_tables = set()
    for func in functions:
        # Update all_imports with requirements from this function FIRST
        # Convert import names to full import statements
        for import_name in func.required_imports:
            if import_name in PYTHON_IMPORTS:
                all_imports.add(PYTHON_IMPORTS[import_name])
            else:
                # If we don't have a mapping, just add the name as-is (for debugging)
                all_imports.add(import_name)

        target_dataclass_name = None
        table_key = None  # Key for tracking processed tables (normalized name)

        if func.returns_table:
            all_imports.add(PYTHON_IMPORTS["dataclass"])  # Needed if any table is returned
            if func.setof_table_name:
                table_key = func.setof_table_name  # Already normalized by parser
                target_dataclass_name = _to_singular_camel_case(table_key)
                table_to_class_name_map[table_key] = target_dataclass_name

                if table_key not in processed_tables:
                    processed_tables.add(table_key)
                    # Get imports FOR THE FIELDS using the PASSED dictionary
                    schema_imports = table_schema_imports.get(table_key, set())
                    # Convert import names to full import statements
                    converted_imports = set()
                    for import_name in schema_imports:
                        if import_name in PYTHON_IMPORTS:
                            converted_imports.add(PYTHON_IMPORTS[import_name])
                        else:
                            # If we don't have a mapping, just add the name as-is (for debugging)
                            converted_imports.add(import_name)
                    imports_per_dataclass[target_dataclass_name] = converted_imports  # Use new name

                    if func.return_columns and func.return_columns[0].name != "unknown":
                        dataclass_defs[target_dataclass_name] = _generate_dataclass(
                            target_dataclass_name, func.return_columns,
                            make_fields_optional=False # SETOF table uses schema directly (optionality from ReturnColumn)
                        )
                    else:  # Schema not found by parser
                        placeholder_cols = [ReturnColumn(name="unknown", sql_type=table_key, python_type="Any")]
                        dataclass_defs[target_dataclass_name] = _generate_dataclass(
                            target_dataclass_name, placeholder_cols,
                            make_fields_optional=True # Make placeholder optional
                        )
                        # Add Any import if placeholder used
                        any_import = PYTHON_IMPORTS.get("Any")  # Use local PYTHON_IMPORTS
                        if any_import:
                            # Ensure the set exists before adding to it
                            if target_dataclass_name not in imports_per_dataclass: # Use new name
                                imports_per_dataclass[target_dataclass_name] = set() # Use new name
                            imports_per_dataclass[target_dataclass_name].add(any_import) # Use new name

            else:  # Explicit RETURNS TABLE(...)
                # Generate a unique name based on function name
                # Apply CamelCase directly and append Result suffix
                target_dataclass_name = inflection.camelize(func.python_name) + "Result"

                # Always store the class name mapping for the function generation pass
                func_key = f"_func_{func.sql_name}"
                table_to_class_name_map[func_key] = target_dataclass_name

                # Generate the dataclass definition only if it hasn't been created yet
                if target_dataclass_name not in dataclass_defs:
                    dataclass_defs[target_dataclass_name] = _generate_dataclass(
                        target_dataclass_name, func.return_columns,
                        make_fields_optional=True # Explicit RETURNS TABLE cols default to Optional
                    )
                
                # Crucially, always associate the required imports for this function's 
                # specific return type with the dataclass name, even if the def was skipped.
                # This ensures the imports are collected correctly later.
                # Convert import names to full import statements
                converted_imports = set()
                for import_name in func.required_imports:
                    if import_name in PYTHON_IMPORTS:
                        converted_imports.add(PYTHON_IMPORTS[import_name])
                    else:
                        # If we don't have a mapping, just add the name as-is (for debugging)
                        converted_imports.add(import_name)
                imports_per_dataclass[target_dataclass_name] = converted_imports # Use new name

    # Add all necessary field type imports from generated dataclasses
    # Now this loop should not encounter a NameError
    # print(f\"DEBUG: Before iterating imports_per_dataclass: {imports_per_dataclass}\") # <--- REMOVED DEBUG LINE
    # Iterate directly over the dictionary items
    for _, imports_set in imports_per_dataclass.items(): # Use original dict
        all_imports.update(imports_set)

    # --- Second pass: Generate functions ---
    # Restore the function generation loop
    for func in functions:
        generated_functions.append(_generate_function(func, table_to_class_name_map))

    # --- Assemble code ---
    all_imports.discard(None)

    # Consolidate typing imports for better readability
    # Define standard imports that should always be present if used
    standard_imports_order = [
        "from typing import List, Optional, Tuple, Dict, Any",
        "from uuid import UUID",
        "from datetime import date, datetime",
        "from decimal import Decimal",
        "from psycopg import AsyncConnection",
        "from dataclasses import dataclass",
    ]

    # Filter the standard imports based on what's actually in all_imports
    present_standard_imports = []
    temp_all_imports = all_imports.copy()

    for std_imp in standard_imports_order:
        # Check if any part of the standard import line matches an import in the set
        # Example: Check if 'from typing import List' is needed if 'from typing import Optional' is also standard
        import_parts = std_imp.split('import')
        module = import_parts[0].replace('from', '').strip()
        names = [name.strip() for name in import_parts[1].split(',')] if len(import_parts) > 1 else []

        needed = False
        imports_to_remove = set()
        for imp_in_set in temp_all_imports:
            if imp_in_set.startswith(f"from {module} import"):
                # Check if this standard import line covers one needed in the set
                imp_names_in_set = [name.strip() for name in imp_in_set.split('import')[1].split(',')]
                if any(name in imp_names_in_set for name in names):
                    needed = True
                    imports_to_remove.add(imp_in_set) # Mark for removal if covered by consolidated line
            elif std_imp == imp_in_set: # Handle direct match like 'import psycopg'
                needed = True
                imports_to_remove.add(imp_in_set)

        if needed:
            present_standard_imports.append(std_imp)
            # Remove the specific imports that are now covered by the standard line
            # This logic might need refinement for complex cases
            # A simpler approach might be to just check if keywords like 'List', 'Optional' exist
            # For now, let's stick to adding the standard line if *any* part is needed
            temp_all_imports -= imports_to_remove # Imperfect removal, might leave unused specific imports

    # Collect any remaining non-standard imports (e.g., custom types)
    # This is still imperfect; we might add standard imports unnecessarily if a part matches.
    # A better approach would be to parse all required names (List, Optional, UUID, etc.)
    # and then build the standard import lines based ONLY on the names present.
    # For now, prioritizing inclusion over perfect minimalism.
    other_imports = sorted(list(temp_all_imports))

    # Combine standard and other imports
    import_statements = present_standard_imports + other_imports

    # --- Generate Header ---
    source_filename = os.path.basename(source_sql_file) if source_sql_file else "input.sql"
    header_lines = [
        "# -*- coding: utf-8 -*-\n",
        f"# Auto-generated by sql2pyapi from {source_filename}",
    ]
    header = "\n".join(header_lines)

    # --- Generate Dataclasses and Functions (without section headers) ---
    stripped_dataclasses = [dataclass_defs[name].strip() for name in sorted(dataclass_defs.keys())]
    stripped_functions = [func.strip() for func in generated_functions]

    # Combine definitions with minimal spacing
    code_body = "\n\n".join(stripped_dataclasses + stripped_functions)

    # Assemble final string
    final_str = header
    if import_statements:
        final_str += "\n\n" + "\n".join(import_statements)
    if code_body:
        final_str += "\n\n" + code_body

    return final_str.strip() + "\n" # Ensure single trailing newline
