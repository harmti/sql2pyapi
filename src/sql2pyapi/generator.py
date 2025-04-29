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
        body_lines.append(f"    # Expecting list of tuples for SETOF composite type {final_dataclass_name}")
        body_lines.append(f"    try:")
        body_lines.append(f"        return [{final_dataclass_name}(*r) for r in rows]")
        body_lines.append(f"    except TypeError as e:")
        body_lines.append(f"        # Tuple unpacking failed. This often happens if the DB connection")
        body_lines.append(f"        # is configured with a dict-like row factory (e.g., DictRow).")
        body_lines.append(f"        # This generated code expects the default tuple row factory.")
        body_lines.append(f"        raise TypeError(")
        body_lines.append(f"            f\"Failed to map SETOF results to dataclass list for {final_dataclass_name}. \"")
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
    body_lines.append(f"        return {'[]' if func.returns_table else 'None'}")

    if func.returns_table:
        # Handle single row table/composite type returns -> Hint is List[Dataclass]
        body_lines.append(f"    # Ensure dataclass '{final_dataclass_name}' is defined above.")
        body_lines.append(f"    # Expecting simple tuple return for composite type {final_dataclass_name}")
        body_lines.append(f"    try:")
        body_lines.append(f"        instance = {final_dataclass_name}(*row)")
        body_lines.append(f"        # Check for 'empty' composite rows (all values are None) returned as a single tuple")
        body_lines.append(f"        # Note: This check might be DB-driver specific for NULL composites")
        body_lines.append(f"        if all(v is None for v in row):")
        body_lines.append(f"             return [] # Return empty list if the single row represents a NULL composite")
        body_lines.append(f"        return [instance] # Return list with one item")
        body_lines.append(f"    except TypeError as e:")
        body_lines.append(f"        # Tuple unpacking failed. This often happens if the DB connection")
        body_lines.append(f"        # is configured with a dict-like row factory (e.g., DictRow).")
        body_lines.append(f"        # This generated code expects the default tuple row factory.")
        body_lines.append(f"        raise TypeError(")
        body_lines.append(f"            f\"Failed to map single row result to dataclass {final_dataclass_name}. \"")
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
    current_imports = set()
    return_type_hint = "None"  # Default
    final_dataclass_name = None
    base_type_hint = "None" # Initialize base_type_hint

    if func.returns_table:
        # This path is taken if the parser identified the return as TABLE(...), SETOF table, or [SETOF] custom_type

        # Case 1: Explicit SETOF table_name or SETOF custom_type_name
        if func.setof_table_name:
            # Parser provides the original SQL name (e.g., 'users', 'public.users', 'user_identity')
            final_dataclass_name = _to_singular_camel_case(func.setof_table_name)
            base_type_hint = final_dataclass_name

        # Case 2: Non-SETOF return of a known table or custom type
        elif func.returns_sql_type_name and func.returns_sql_type_name in custom_types:
             # Parser identified a specific known type (table or composite)
             final_dataclass_name = _to_singular_camel_case(func.returns_sql_type_name)
             base_type_hint = final_dataclass_name

        # Case 3: Explicit RETURNS TABLE(...) or fallback for unknown named type
        elif func.return_columns: # Check if columns were parsed (indicates RETURNS TABLE)
             # This covers RETURNS TABLE(...)
             final_dataclass_name = inflection.camelize(func.python_name) + "Result"
             base_type_hint = final_dataclass_name
             # Need to ensure this ad-hoc dataclass is generated later
             # Add imports needed for the columns of this ad-hoc dataclass
             for col in func.return_columns:
                 # Extract base type and add imports
                 col_base_type = col.python_type.replace("Optional[", "").replace("List[", "").replace("]", "")
                 if col_base_type in PYTHON_IMPORTS:
                     current_imports.add(PYTHON_IMPORTS[col_base_type])
                 if col.python_type.startswith("Optional["): current_imports.add(PYTHON_IMPORTS["Optional"])
                 if col.python_type.startswith("List["): current_imports.add(PYTHON_IMPORTS["List"])

        # Case 4: Error/Inconsistency
        else:
            logging.error(f"Inconsistent state in _determine_return_type for {func.sql_name}: returns_table=True but no specific type identified and no columns found.")
            base_type_hint = "Any"
            current_imports.add(PYTHON_IMPORTS["Any"])

        # Wrap the determined base type hint (dataclass name or Any) with List/Optional
        if base_type_hint != "Any" and base_type_hint != "None":
            # Add dataclass import if we determined a dataclass name
            current_imports.add(PYTHON_IMPORTS["dataclass"])
            if func.returns_setof:
                return_type_hint = f"List[{base_type_hint}]"
                current_imports.add(PYTHON_IMPORTS["List"])
            else:
                # Should be Optional for single composite/table return
                return_type_hint = f"Optional[{base_type_hint}]"
                current_imports.add(PYTHON_IMPORTS["Optional"])
        else:
             # If base_type_hint is Any or None, use it directly
             return_type_hint = base_type_hint


    elif func.return_type != "None":
        # The parser provides the type hint string (e.g., 'int', 'Optional[str]', 'List[UUID]', 'Optional[List[int]]')
        parser_type_hint = func.return_type
        final_type_hint = parser_type_hint # Start with the parser's hint

        # Determine the base type within the hint for import purposes
        base_type_in_hint = parser_type_hint.replace("Optional[", "").replace("List[", "").replace("]", "")

        # Add necessary imports based on the final hint structure
        if parser_type_hint.startswith("List["):
            current_imports.add(PYTHON_IMPORTS.get("List"))
        if parser_type_hint.startswith("Optional["):
            current_imports.add(PYTHON_IMPORTS.get("Optional"))
            
        # Add import for the base type itself (int, str, UUID, etc.)
        import_stmt = PYTHON_IMPORTS.get(base_type_in_hint)
        if import_stmt:
            current_imports.add(import_stmt)

        # Also add any other imports the parser deemed necessary (e.g., for custom enums mapped to str)
        for imp_name in func.required_imports:
             if imp_name in PYTHON_IMPORTS:
                 current_imports.add(PYTHON_IMPORTS[imp_name])

    # return_type_hint remains "None" for VOID functions

    # --- Determine final type hint string --- 
    # This section is now simplified as we mostly rely on the parser_type_hint
    # The logic here was complex and potentially conflicting with the import logic above.
    # Let's recalculate the final hint based on parser input and SETOF flag.
    
    if func.returns_table:
        # This logic remains the same: Always List[Dataclass] for now
        final_type_hint = f"List[{final_dataclass_name}]"
    elif func.returns_record:
        # This logic remains the same: Optional[Tuple] or List[Tuple]
        final_type_hint = "Tuple"
        if func.returns_setof:
             final_type_hint = f"List[{final_type_hint}]"
        else:
             final_type_hint = f"Optional[{final_type_hint}]"
    elif func.return_type != "None": # Scalar/Array return
        # Trust the parser's output for Optional/List structure
        final_type_hint = func.return_type 
        # Safety check: Non-SETOF scalar/array should probably still be Optional?
        # Because fetchone() can return None.
        if not func.returns_setof and not final_type_hint.startswith("Optional["):
             final_type_hint = f"Optional[{final_type_hint}]"
             current_imports.add(PYTHON_IMPORTS.get("Optional")) # Ensure Optional is imported
    else: # Void return
        final_type_hint = "None"

    # Clean up None entries from the set of collected imports
    required_imports = {imp for imp in current_imports if imp}
    
    return final_type_hint, final_dataclass_name, required_imports


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

    # Determine return type hint and dataclass name
    # Retrieve the pre-calculated hint and the potential dataclass name
    return_type_hint = func.return_type_hint
    final_dataclass_name = func.dataclass_name # Get name stored on func object

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
) -> str:
    """
    Generates the full Python module code as a string.
    
    This is the main entry point for code generation. It processes all parsed SQL functions
    and generates a complete Python module with imports, dataclass definitions, and
    async function implementations.
    
    Args:
        functions (List[ParsedFunction]): List of parsed SQL function definitions
        table_schema_imports (Dict[str, set]): Imports needed for table schemas
        parsed_composite_types (Dict[str, List[ReturnColumn]]): Parsed composite type definitions
        source_sql_file (str, optional): Name of the source SQL file for header comment
    
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

        # >> Determine return type and collect imports in this first pass <<
        return_type_hint, determined_dataclass_name, type_imports = _determine_return_type(func, current_custom_types)
        func.return_type_hint = return_type_hint # Store the calculated hint
        func.dataclass_name = determined_dataclass_name # Store the determined dataclass name
        current_imports.update(type_imports) # Add imports specific to the return type

        # If _determine_return_type determined a name for an ad-hoc RETURNS TABLE dataclass,
        # ensure its definition exists in current_custom_types.
        if determined_dataclass_name and determined_dataclass_name.endswith("Result") and determined_dataclass_name not in current_custom_types:
            if func.return_columns:
                # Generate internal name matching the parser's potential ad-hoc creation logic
                # Use the name determined above
                ad_hoc_class_name = determined_dataclass_name
                if ad_hoc_class_name: # Ensure the name was actually mapped
                    # Create placeholder entry if missing. The actual generation happens later.
                    if ad_hoc_class_name not in current_custom_types:
                        logging.debug(f"Creating placeholder for ad-hoc dataclass: {ad_hoc_class_name}")
                        # Store the columns needed to generate it later
                        current_custom_types[ad_hoc_class_name] = func.return_columns
                        # Add dataclass import generally if any ad-hoc is needed
                        current_imports.add(PYTHON_IMPORTS["dataclass"])
            else:
                logging.warning(f"Function {func.sql_name} seems to need ad-hoc dataclass {determined_dataclass_name} but has no return columns.")
        
        # *** NEW FIX ***
        # If a dataclass name was determined (not ad-hoc) but its definition is missing 
        # (e.g., SETOF table_name where table schema wasn't found),
        # add a placeholder entry so the generator creates a TODO comment.
        elif determined_dataclass_name and determined_dataclass_name not in current_custom_types:
            logging.warning(f"Schema for type '{determined_dataclass_name}' (likely from function '{func.sql_name}') not found. Generating placeholder dataclass.")
            # Add entry with empty columns list to trigger placeholder generation
            # Use the determined dataclass name as the key
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
    # Iterate through the custom types collected (from CREATE TYPE and ad-hoc RETURNS TABLE)
    for type_name, columns in current_custom_types.items():
         # Determine the final class name (handle potential internal names for ad-hoc)
         if type_name.endswith("Result"):
              class_name = type_name # Use the name directly (e.g., GetUserDataResult)
         else:
              class_name = _to_singular_camel_case(type_name) # Convert SQL name (e.g., user_identity -> UserIdentity)

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

    # --- Restore Import Calculation Logic ---
    # ... existing code ...

    # --- Assemble code ---
    current_imports.discard(None)
    # DEBUG: Print the final set of all collected imports before formatting
    # print(f"[GENERATOR DEBUG] Final current_imports before formatting: {current_imports}")
    logging.debug(f"[DEBUG] Imports BEFORE consolidation: {current_imports}") # Added debug log

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

    # Filter the standard imports based on what's actually in current_imports
    present_standard_imports = []
    temp_current_imports = current_imports.copy()

    for std_imp in standard_imports_order:
        # Check if any part of the standard import line matches an import in the set
        # Example: Check if 'from typing import List' is needed if 'from typing import Optional' is also standard
        import_parts = std_imp.split('import')
        module = import_parts[0].replace('from', '').strip()
        names = [name.strip() for name in import_parts[1].split(',')] if len(import_parts) > 1 else []

        needed = False
        imports_to_remove = set()
        for imp_in_set in temp_current_imports:
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
            temp_current_imports -= imports_to_remove # Imperfect removal, might leave unused specific imports

    # Collect any remaining non-standard imports (e.g., custom types)
    # This is still imperfect; we might add standard imports unnecessarily if a part matches.
    # A better approach would be to parse all required names (List, Optional, UUID, etc.)
    # and then build the standard import lines based ONLY on the names present.
    # For now, prioritizing inclusion over perfect minimalism.
    other_imports = sorted(list(temp_current_imports))

    # Combine standard and other imports
    import_statements = present_standard_imports + other_imports

    logging.debug(f"[DEBUG] Imports AFTER consolidation: {import_statements}") # Added debug log

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

    # --- Assemble code ---
    final_parts = [header]
    if import_statements:
        final_parts.append("\n".join(import_statements))

    # Prepare dataclass and function blocks
    dataclass_block = ""
    function_block = ""
    non_empty_dataclasses = [dc for dc in [dataclasses_section.strip()] if dc]
    non_empty_functions = [func.strip() for func in generated_functions if func.strip()]

    if non_empty_dataclasses:
        dataclass_block = "\n\n".join(non_empty_dataclasses)
    if non_empty_functions:
        function_block = "\n\n".join(non_empty_functions)

    # Join Header and Imports
    output = "\n\n".join(final_parts)

    # Add Dataclasses (if any) with correct separator
    if dataclass_block:
        output += "\n\n\n" + dataclass_block # Revert to 3 newlines

    # Add Functions (if any) with correct separator
    if function_block:
        # Use 3 newlines after imports or dataclasses
        separator = "\n\n\n" # Revert to 3 newlines
        output += separator + function_block

    return output.strip() + "\n"
