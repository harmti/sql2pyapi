# ===== SECTION: IMPORTS AND SETUP =====
# Standard library and third-party imports
from typing import List, Dict, Tuple, Optional
import textwrap
import inflection  # Using inflection library for plural->singular
from pathlib import Path
import os
import logging

# Local imports
from .sql_models import ParsedFunction, ReturnColumn, SQLParameter
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
        # If columns exist (parser couldn't map), use the SQL type from the dummy column.
        # If columns is empty (generator added placeholder), try to guess SQL name from class name.
        if columns:
             sql_table_name_guess = columns[0].sql_type
        else:
             # Attempt to convert CamelCase class_name back to snake_case for the comment
             # REVISED: Pluralize the snake_case name for the comment to match original table likely name
             singular_snake = inflection.underscore(class_name) 
             sql_table_name_guess = inflection.pluralize(singular_snake) # Convert 'item' back to 'items'
             # If it was an ad-hoc Result class, remove _result suffix (apply before pluralizing? No, class_name is Item)
             # if sql_table_name_guess.endswith("_result"):
             #      sql_table_name_guess = sql_table_name_guess[:-7] 
        
        # Ensure the guessed name is not empty, fallback if needed
        if not sql_table_name_guess:
             sql_table_name_guess = "unknown_table"
             
        return f"""# TODO: Define dataclass for table '{sql_table_name_guess}'
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
        # Handle single row returns (scalar, record, or single table row)
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
        # Covers SETOF table_name, SETOF custom_type_name, SETOF TABLE(...)
        body_lines.append(f"    # Ensure dataclass '{final_dataclass_name}' is defined above.")
        body_lines.append("    if not rows:")
        body_lines.append("        return []")
        # Logic for SETOF custom_type (expect list of tuples)
        # Logic for SETOF table_name / TABLE(...) (expect list of Row/dict or tuples)
        # Use tuple unpacking, assuming it works for list of tuples from custom types
        # This MIGHT break for SETOF table_name if list of dicts is returned.
        # Ensure we use the singular form of the class name in the list comprehension
        singular_class_name = final_dataclass_name
        # If it's a table name, make sure it's in singular form
        if func.returns_table and func.setof_table_name:
            singular_class_name = _to_singular_camel_case(func.setof_table_name)
            
        body_lines.append(f"    # Expecting list of tuples for SETOF composite type {singular_class_name}")
        body_lines.append(f"    try:")
        body_lines.append(f"        return [{singular_class_name}(*r) for r in rows]")
        body_lines.append(f"    except TypeError as e:")
        body_lines.append(f"        # Tuple unpacking failed. This often happens if the DB connection")
        body_lines.append(f"        # is configured with a dict-like row factory (e.g., DictRow).")
        body_lines.append(f"        # This generated code expects the default tuple row factory.")
        body_lines.append(f"        raise TypeError(")
        body_lines.append(f"            f\"Failed to map SETOF results to dataclass list for {singular_class_name}. \"")
        body_lines.append(f"            f\"Check DB connection: Default tuple row_factory expected. Error: {{e}}\"")
        body_lines.append(f"        )")

    elif func.returns_record:
        # SETOF RECORD -> List[Tuple]
        body_lines.append("    # Return list of tuples for SETOF record")
        body_lines.append("    return rows")
    else:
        # SETOF scalar -> List[scalar_type]
        body_lines.append("    # Assuming SETOF returns list of single-element tuples for scalars")
        # Filter out potential None rows if the outer list itself shouldn't be Optional
        body_lines.append("    return [row[0] for row in rows if row]")

    return body_lines


def _generate_single_row_return_body(func: ParsedFunction, final_dataclass_name: Optional[str]) -> List[str]:
    """
    Generates code for handling single-row returns (scalar, record, or table).

    Args:
        func (ParsedFunction): The parsed SQL function definition
        final_dataclass_name (Optional[str]): The name of the dataclass for table returns

    Returns:
        List[str]: Lines of code for handling single-row returns
    """
    body_lines = []
    body_lines.append("    row = await cur.fetchone()")
    body_lines.append("    if row is None:")
    # If returns_table is true BUT returns_setof is false, the hint is Optional[Dataclass],
    # so we should return None here, not [].
    body_lines.append(f"        return None") 

    if func.returns_table:
        # Handle single row table/composite type returns -> Hint is Optional[Dataclass]
        # Ensure we use the singular form of the class name
        singular_class_name = final_dataclass_name
        # If it's a table name, make sure it's in singular form
        if func.returns_table and func.returns_sql_type_name:
            singular_class_name = _to_singular_camel_case(func.returns_sql_type_name)
            
        body_lines.append(f"    # Ensure dataclass '{singular_class_name}' is defined above.")
        body_lines.append(f"    # Expecting simple tuple return for composite type {singular_class_name}")
        body_lines.append(f"    try:")
        body_lines.append(f"        instance = {singular_class_name}(*row)")
        body_lines.append(f"        # Check for 'empty' composite rows (all values are None) returned as a single tuple")
        body_lines.append(f"        # Note: This check might be DB-driver specific for NULL composites")
        body_lines.append(f"        if all(v is None for v in row):")
        # Return None if the single row represents a NULL composite (consistency with Optional hint)
        body_lines.append(f"             return None") 
        body_lines.append(f"        return instance") # Return the single instance, not a list
        body_lines.append(f"    except TypeError as e:")
        body_lines.append(f"        # Tuple unpacking failed. This often happens if the DB connection")
        body_lines.append(f"        # is configured with a dict-like row factory (e.g., DictRow).")
        body_lines.append(f"        # This generated code expects the default tuple row factory.")
        body_lines.append(f"        raise TypeError(")
        body_lines.append(f"            f\"Failed to map single row result to dataclass {singular_class_name}. \"")
        body_lines.append(f"            f\"Check DB connection: Default tuple row_factory expected. Row: {{row!r}}. Error: {{e}}\"")
        body_lines.append(f"        )")

    elif func.returns_record:
        # RECORD -> Optional[Tuple] (Hint determined previously)
        body_lines.append("    # Return tuple for record type")
        body_lines.append("    return row")
    else:
        # Scalar type -> Optional[basic_type] (Hint determined previously)
        # Remove check for dict row - assume tuple factory provides tuple even for single col
        body_lines.append("    # Expecting a tuple even for scalar returns, access first element.")
        body_lines.append("    return row[0]")

    return body_lines


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


def _generate_function(func: ParsedFunction) -> str:
    """
    Generates a Python async function string from a parsed SQL function definition.
    
    This is the core code generation function that creates Python wrapper functions
    for PostgreSQL functions. It handles different return types (scalar, record, table),
    parameter ordering, docstring generation, and proper NULL handling.
    
    Args:
        func (ParsedFunction): The parsed SQL function definition
    
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

    # Get the return type hint and dataclass name from the parsed function
    # These were already determined by the parser and potentially refined by _determine_return_type
    return_type_hint, final_dataclass_name, _ = _determine_return_type(func, {})

    sql_args_placeholders = ", ".join(["%s"] * len(func.params))
    # Use sorted params for the execute call arguments list
    python_args_list = "[" + ", ".join([p.python_name for p in sorted_params]) + "]"

    # Generate the docstring
    docstring = _generate_docstring(func)

    # Generate the function body based on the return type
    body_lines = _generate_function_body(func, final_dataclass_name, sql_args_placeholders, python_args_list)

    indented_body = textwrap.indent("\n".join(body_lines), prefix="    ")

    # Ensure we use the correct class name in the return type hint for both
    # schema-qualified and non-schema-qualified table names
    if func.returns_table:
        # Handle SETOF table returns
        if func.returns_setof and func.setof_table_name:
            # Convert the table name to a singular class name
            singular_name = _to_singular_camel_case(func.setof_table_name)
            return_type_hint = f"List[{singular_name}]"
        # Handle single table returns
        elif func.returns_sql_type_name:
            # Convert the table name to a singular class name
            singular_name = _to_singular_camel_case(func.returns_sql_type_name)
            return_type_hint = f"Optional[{singular_name}]"
        # Handle ad-hoc RETURNS TABLE
        elif final_dataclass_name:
            if func.returns_setof:
                return_type_hint = f"List[{final_dataclass_name}]"
            else:
                return_type_hint = f"Optional[{final_dataclass_name}]"

    # --- Assemble the function ---
    # Note: Docstring is now pre-formatted with indentation
    func_def = (
        f"async def {func.python_name}({params_str_py}) -> {return_type_hint}:\n"
        f"{docstring}\n"
        f"{indented_body}\n"
    )
    return func_def


# ===== SECTION: MAIN CODE GENERATION =====
# Main entry point for generating the complete Python module

def generate_python_code(
    functions: List[ParsedFunction],
    table_schema_imports: Dict[str, set],  # Accept the schema imports
    parsed_composite_types: Dict[str, List[ReturnColumn]], # Accept composite types
    source_sql_file: str = "",
    omit_helpers: bool = False,
) -> str:
    """
    Generates the full Python module code as a string.
    
    Args:
        functions (List[ParsedFunction]): List of parsed SQL function definitions
        table_schema_imports (Dict[str, set]): Imports needed for table schemas
        parsed_composite_types (Dict[str, List[ReturnColumn]]): Parsed composite type definitions
        source_sql_file (str, optional): Name of the source SQL file for header comment
        omit_helpers (bool, optional): Whether to omit helper functions from the final output
    
    Returns:
        str: Complete Python module code as a string
    
    Notes:
        - Imports are automatically collected based on types used
        - Dataclasses are generated for table returns and complex types
        - Each SQL function gets a corresponding async Python function
        - The output follows a consistent structure: imports → dataclasses → functions
    """

    logging.debug(f"Generating code with composite_types: {parsed_composite_types}") # Added debug log

    current_imports = set()
    # Add logging if potentially used by error handling
    # Remove unnecessary logging import
    # REMOVED: Unconditional addition of "import logging"
    dataclass_defs = {}
    generated_functions = []

    # Base typing imports like Optional, List etc are added by the parser as needed

    # Use the composite types passed from the parser
    current_custom_types = parsed_composite_types.copy()
    function_defs = []

    # --- First pass: Determine return types and required imports, potentially create ad-hoc types ---
    for func in functions:
        # Get the return type hint, dataclass name, and imports from the function
        # This uses the information already determined by the parser
        return_type_hint, determined_dataclass_name, type_imports = _determine_return_type(func, current_custom_types)
        current_imports.update(type_imports) # Add imports specific to the return type

        # If we have a dataclass name, ensure its definition exists in current_custom_types
        if determined_dataclass_name:
            # For ad-hoc dataclasses (those ending with 'Result')
            if determined_dataclass_name.endswith("Result") and determined_dataclass_name not in current_custom_types:
                if func.return_columns:
                    # Create placeholder entry if missing. The actual generation happens later.
                    logging.debug(f"Creating placeholder for ad-hoc dataclass: {determined_dataclass_name}")
                    # Store the columns needed to generate it later
                    current_custom_types[determined_dataclass_name] = func.return_columns
                    # Add dataclass import generally if any ad-hoc is needed
                    current_imports.add(PYTHON_IMPORTS["dataclass"])
                else:
                    logging.warning(f"Function {func.sql_name} needs ad-hoc dataclass {determined_dataclass_name} but has no return columns.")
            
            # For non-ad-hoc dataclasses that are missing (e.g., SETOF table_name where table schema wasn't found)
            elif determined_dataclass_name not in current_custom_types:
                logging.warning(f"Schema for type '{determined_dataclass_name}' (likely from function '{func.sql_name}') not found. Generating placeholder dataclass.")
                # Add entry with empty columns list to trigger placeholder generation
                current_custom_types[determined_dataclass_name] = []

        # Update current_imports with requirements from function parameters
        # (Parser should have added base type imports like UUID, Decimal to func.required_imports)
        for imp_name in func.required_imports:
            if imp_name in PYTHON_IMPORTS:
                current_imports.add(PYTHON_IMPORTS[imp_name])
            else:
                # If we don't have a mapping, just add the name as-is (for debugging)
                current_imports.add(imp_name)

    # --- Generate Dataclasses section --- 
    dataclasses_section_list = []
    processed_dataclass_names = set()
    # Now add the generated dataclasses to the output in the correct order
    # Iterate through the custom types collected (from CREATE TYPE and ad-hoc RETURNS TABLE)
    for type_name, columns in current_custom_types.items():
         # Determine the final class name (handle potential internal names for ad-hoc)
         if type_name.endswith("Result"):
              class_name = type_name # Use the name directly (e.g., GetUserDataResult)
         elif '.' in type_name: # Schema-qualified table name
              class_name = _to_singular_camel_case(type_name) # Convert SQL name (e.g., public.companies -> Company)
         else: # Non-schema-qualified table name
              class_name = _to_singular_camel_case(type_name) # Convert SQL name (e.g., users -> User)

         if class_name in processed_dataclass_names:
              continue # Avoid duplicates
         processed_dataclass_names.add(class_name)

         # Determine if fields should be optional (True for ad-hoc RETURNS TABLE)
         make_fields_optional = type_name.endswith("Result")

         logging.debug(f"Generating dataclass '{class_name}' ({type_name}) with columns: {columns}") # DEBUG LOG
         dataclass_code = _generate_dataclass(
             class_name, columns, make_fields_optional
         )
         dataclasses_section_list.append(dataclass_code)

         # Collect imports needed *by this specific dataclass's fields*
         # This logic seems better placed within _generate_dataclass or requires passing imports back
         # For now, let's re-collect imports based on columns here
         dataclass_imports = set()
         for col in columns:
             base_type = col.python_type.replace("Optional[", "").replace("List[", "").replace("]", "")
             if base_type in PYTHON_IMPORTS: dataclass_imports.add(PYTHON_IMPORTS[base_type])
             if col.python_type.startswith("Optional["): 
                 dataclass_imports.add(PYTHON_IMPORTS["Optional"])
                 logging.debug(f"Added Optional import for column {col.name}: {PYTHON_IMPORTS['Optional']}") # Added debug log
             if col.python_type.startswith("List["): dataclass_imports.add(PYTHON_IMPORTS["List"])
         # Ensure dataclass itself is imported if we generated one
         if dataclass_code and not dataclass_code.startswith("# TODO"):
             dataclass_imports.add(PYTHON_IMPORTS["dataclass"])
         current_imports.update(dataclass_imports)


    dataclasses_section = "\n\n".join(dataclasses_section_list)

    # --- Second pass: Generate functions ---
    # Restore the function generation loop
    for func in functions:
        logging.info(f"Attempting to generate function: {func.sql_name}") # DEBUG LOG
        generated_functions.append(_generate_function(func))

    # --- Add imports needed for helper functions ---
    # Ensure TypeVar and Sequence are imported if helpers are generated
    # REMOVED UNCONDITIONAL ADDITION
    # Optional and List should already be included if used elsewhere

    # --- Restore Import Calculation Logic ---
    # ... existing code ...

    # --- Assemble code --- REVISED
    current_imports.discard(None)
    # DEBUG: Print the final set of all collected imports before formatting
    # print(f"[GENERATOR DEBUG] Final current_imports before formatting: {current_imports}")
    logging.debug(f"[DEBUG] Imports BEFORE consolidation: {current_imports}") # Added debug log

    # Consolidate typing imports for better readability
    # Define standard imports that should always be present if used
    standard_imports_order = [
        "from typing import List, Optional, Tuple, Dict, Any", # Base typing imports
        "from typing import TypeVar, Sequence", # Helper function typing imports
        "from uuid import UUID",
        "from datetime import date, datetime",
        "from decimal import Decimal",
        "from psycopg import AsyncConnection",
        "from dataclasses import dataclass",
    ]

    # Filter the standard imports based on what's actually in current_imports
    # AND conditionally add helper imports
    present_standard_imports = []
    needed_typing_names = set() # Track specific names needed from typing
    needed_other_modules = set() # Track other modules needed

    # Collect all needed names/modules from current_imports
    for imp_in_set in current_imports:
        if imp_in_set.startswith("from typing import"):
            names = {name.strip() for name in imp_in_set.split('import')[1].split(',')}
            needed_typing_names.update(names)
        elif imp_in_set.startswith("from"):
            module = imp_in_set.split('import')[0].replace('from', '').strip()
            needed_other_modules.add(module)
        # Handle direct imports like 'import psycopg' if needed later

    # Conditionally add helper function types if helpers are included
    if not omit_helpers:
        needed_typing_names.update(["TypeVar", "Sequence"])
        # Ensure List/Optional are added if needed by helpers, even if not elsewhere
        needed_typing_names.update(["List", "Optional"])


    # Build the final list of standard import lines
    for std_imp in standard_imports_order:
        import_parts = std_imp.split('import')
        module = import_parts[0].replace('from', '').strip()
        names_in_line = {name.strip() for name in import_parts[1].split(',')} if len(import_parts) > 1 else set()

        # Special handling for the TypeVar/Sequence import line
        is_helper_import_line = "TypeVar" in names_in_line or "Sequence" in names_in_line
        if is_helper_import_line and omit_helpers:
            continue # Skip this line entirely if helpers are omitted

        if module == 'typing':
            # Include the typing line if any of its names are needed
            if any(name in needed_typing_names for name in names_in_line):
                present_standard_imports.append(std_imp)
                # Optionally remove covered names from needed_typing_names for stricter checks later
                # needed_typing_names -= names_in_line
        elif module in needed_other_modules:
            # Include other standard lines if the module was required
            present_standard_imports.append(std_imp)
            # Optionally remove covered module
            # needed_other_modules.remove(module)

    # Collect any remaining non-standard imports (this logic might need refinement)
    # For now, we assume standard imports cover everything needed, which might be too broad.
    # A more robust approach would track *all* required symbols and generate minimal imports.
    other_imports = [] # Assume standard imports cover all for now

    # Combine standard and other imports
    import_statements = present_standard_imports + other_imports

    logging.debug(f"[DEBUG] Imports AFTER consolidation: {import_statements}") # Added debug log

    # --- Define Helper Functions Code ---
    # Conditionally define helper code
    helper_functions_code = """
# ===== SECTION: RESULT HELPERS =====
# REMOVED redundant import line

T = TypeVar('T')

def get_optional(result: Optional[List[T]] | Optional[T]) -> Optional[T]:
    \"\"\"\\
    Safely retrieves an optional single result.

    Handles cases where the input is:
    - None
    - An empty list
    - A list with one item
    - A single item (non-list, non-None)

    Returns the item if exactly one is found, otherwise None.
    \"\"\"
    if result is None:
        return None
    # Check if it's a list/tuple but not string/bytes
    if isinstance(result, Sequence) and not isinstance(result, (str, bytes)):
        if len(result) == 1:
            return result[0]
        else: # Empty list or list with more than one item
            return None
    else: # It's already a single item
        return result

def get_required(result: Optional[List[T]] | Optional[T]) -> T:
    \"\"\"\\
    Retrieves a required single result, raising an error if none or multiple are found.

    Handles cases where the input is:
    - None
    - An empty list
    - A list with one item
    - A single item (non-list, non-None)

    Returns the item if exactly one is found.
    Raises ValueError otherwise.
    \"\"\"
    item = get_optional(result)
    if item is None:
         # Improved error message
         input_repr = repr(result)
         if len(input_repr) > 80: # Truncate long inputs
             input_repr = input_repr[:77] + '...'
         raise ValueError(f"Expected exactly one result, but got none or multiple. Input was: {input_repr}")
    return item
""" if not omit_helpers else ""

    # --- Generate Header ---
    source_filename = os.path.basename(source_sql_file) if source_sql_file else "input.sql"
    header_lines = [
        "# -*- coding: utf-8 -*-",
        f"# Auto-generated by sql2pyapi from {source_filename}",
        "#",
        "# IMPORTANT: This code expects the database connection to use the default",
        "# psycopg tuple row factory. It will raise errors if used with",
        "# dictionary-based row factories (like DictRow).",
    ]
    header = "\n".join(header_lines)

    # --- Generate Dataclasses and Functions (without section headers) ---
    # Filter out empty strings before joining
    non_empty_dataclasses = [dc for dc in [dataclasses_section.strip()] if dc]
    non_empty_functions = [func.strip() for func in generated_functions if func.strip()]
    
    # Combine definitions with minimal spacing, handling empty sections
    code_parts = non_empty_dataclasses + non_empty_functions
    code_body = "\n\n".join(code_parts)

    # --- Assemble final code --- REVISED
    final_parts = [header]
    if import_statements:
        # Delete the incorrect block that re-adds helper imports
        # (Lines defining needed_helper_imports, import_set, update, and recalculating final_imports from import_set)
        
        # Keep only the correct logic using the pre-calculated import_statements:
        final_imports = sorted(list(import_statements))
        final_parts.append("\n".join(final_imports))
    
    # Add Helpers (only if not omitted and code is non-empty)
    if helper_functions_code:
        final_parts.append(helper_functions_code)

    # Prepare dataclass and function blocks
    dataclass_block = ""
    function_block = ""
    non_empty_dataclasses = [dc for dc in [dataclasses_section.strip()] if dc]
    non_empty_functions = [func.strip() for func in generated_functions if func.strip()]

    if non_empty_dataclasses:
        dataclass_block = "\n\n".join(non_empty_dataclasses)
    if non_empty_functions:
        function_block = "\n\n".join(non_empty_functions)

    # Add Dataclasses (if any)
    if dataclass_block:
        final_parts.append(dataclass_block)

    # Add Functions (if any)
    if function_block:
        final_parts.append(function_block)

    # Join parts with two newlines, add trailing newline
    # Ensure empty parts don't create extra newlines by filtering them out
    final_code = "\n\n".join(part for part in final_parts if part) + "\n"

    return final_code

# ===== SECTION: FILE WRITING =====
# Function to write the generated code to a file
