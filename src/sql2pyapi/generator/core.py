# ===== SECTION: IMPORTS AND SETUP =====
# Standard library and third-party imports
from typing import List, Dict, Tuple, Optional
import textwrap
import inflection  # Using inflection library for plural->singular
from pathlib import Path
import os
import logging

# Local imports
from ..sql_models import ParsedFunction, ReturnColumn, SQLParameter
from ..constants import *
from ..errors import MissingSchemaError # Added import
from .function_generator import _generate_function
from .enum_generator import _generate_enum_class
from .dataclass_generator import _generate_dataclass
from .utils import _to_singular_camel_case
from .return_handlers import _determine_return_type

# ===== SECTION: CONSTANTS AND CONFIGURATION =====
# REMOVED local definition of PYTHON_IMPORTS
# PYTHON_IMPORTS = {
#     "Any": "from typing import Any",
#     "List": "from typing import List",
# ... etc ...
# }


def generate_python_code(
    functions: List[ParsedFunction],
    table_schema_imports: Dict[str, set],  # Accept the schema imports
    parsed_composite_types: Dict[str, List[ReturnColumn]], # Accept composite types
    parsed_enum_types: Dict[str, List[str]] = None, # Accept enum types
    source_sql_file: str = "",
    omit_helpers: bool = False,
    fail_on_missing_schema: bool = True, # Added parameter
) -> str:
    """
    Generates the full Python module code as a string.
    
    Args:
        functions (List[ParsedFunction]): List of parsed SQL function definitions
        table_schema_imports (Dict[str, set]): Imports needed for table schemas
        parsed_composite_types (Dict[str, List[ReturnColumn]]): Parsed composite type definitions
        parsed_enum_types (Dict[str, List[str]], optional): Definitions for ENUM types. Defaults to None.
        source_sql_file (str, optional): Name of the source SQL file for header comment
        omit_helpers (bool, optional): Whether to omit helper functions from the final output
        fail_on_missing_schema (bool, optional): If True (default), raise an error if a required schema is missing.
                                               If False, log a warning and generate a placeholder.
    
    Returns:
        str: Complete Python module code as a string
    
    Raises:
        MissingSchemaError: If `fail_on_missing_schema` is True and a required schema is not found.
    
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
        # Attach enum_types to each function for downstream use
        func.enum_types = parsed_enum_types or {}
        # Get the return type hint, dataclass name, and imports from the function
        # This uses the information already determined by the parser
        return_type_hint, determined_dataclass_name, type_imports = _determine_return_type(func, current_custom_types)
        current_imports.update(type_imports) # Add imports specific to the return type

        # If we have a dataclass name, ensure its definition exists in current_custom_types
        if determined_dataclass_name:
            # For ad-hoc dataclasses (those ending with 'Result') - These are defined by the function itself, so no schema lookup needed
            if determined_dataclass_name.endswith("Result") and determined_dataclass_name not in current_custom_types:
                if func.return_columns:
                    # Create placeholder entry if missing. The actual generation happens later.
                    logging.debug(f"Creating placeholder for ad-hoc dataclass: {determined_dataclass_name}")
                    # Store the columns needed to generate it later
                    current_custom_types[determined_dataclass_name] = func.return_columns
                    # Add dataclass import generally if any ad-hoc is needed
                    current_imports.add(PYTHON_IMPORTS["dataclass"])
                else:
                    # This case might indicate a parser issue, but we don't fail here.
                    logging.warning(f"Function {func.sql_name} needs ad-hoc dataclass {determined_dataclass_name} but has no return columns.")

            # For non-ad-hoc dataclasses (based on existing tables/types)
            else:
                # Determine the original SQL name (could be SETOF table, single table, or custom type)
                original_sql_type_name = func.setof_table_name or func.returns_sql_type_name

                # Check if the *original* SQL type name exists in the parsed types
                if original_sql_type_name and original_sql_type_name not in current_custom_types:
                    # Schema is missing!
                    error_message = f"Schema for type '{determined_dataclass_name}' (SQL: '{original_sql_type_name}', likely from function '{func.sql_name}') not found."
                    if fail_on_missing_schema:
                        raise MissingSchemaError(type_name=original_sql_type_name, function_name=func.sql_name)
                    else:
                        # Original behavior: Warn and create placeholder
                        logging.warning(f"{error_message} Generating placeholder dataclass.")
                        # Add entry with empty columns list to trigger placeholder generation
                        # Use the *Python class name* as the key here, because the dataclass generation loop later expects it
                        current_custom_types[determined_dataclass_name] = []

        # Update current_imports with requirements from function parameters
        # (Parser should have added base type imports like UUID, Decimal to func.required_imports)
        for imp_name in func.required_imports:
            if imp_name in PYTHON_IMPORTS:
                current_imports.add(PYTHON_IMPORTS[imp_name])
            else:
                # If we don't have a mapping, just add the name as-is (for debugging)
                current_imports.add(imp_name)

    # --- Generate Enum classes section ---
    enum_classes_section_list = []
    processed_enum_names = set()
    
    # Generate Enum classes if we have parsed enum types
    if parsed_enum_types:
        for enum_name, enum_values in parsed_enum_types.items():
            # Convert enum_name to PascalCase for Python Enum class name
            class_name = ''.join(word.capitalize() for word in enum_name.split('_'))
            
            if class_name in processed_enum_names:
                continue  # Avoid duplicates
            processed_enum_names.add(class_name)
            
            # Generate the Enum class code
            enum_class_code = _generate_enum_class(enum_name, enum_values)
            enum_classes_section_list.append(enum_class_code)
            
            # Ensure Enum is imported
            current_imports.add(PYTHON_IMPORTS['Enum'])
    
    enum_classes_section = "\n\n".join(enum_classes_section_list)
    
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
        "from enum import Enum",
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
    helper_functions_code = HELPER_FUNCTIONS_CODE if not omit_helpers else ""

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

    # --- Generate Enum classes, Dataclasses and Functions (without section headers) ---
    # Filter out empty strings before joining
    non_empty_enums = [enum_class for enum_class in enum_classes_section_list if enum_class.strip()]
    non_empty_dataclasses = [dc for dc in [dataclasses_section.strip()] if dc]
    non_empty_functions = [func.strip() for func in generated_functions if func.strip()]
    
    # Combine definitions with minimal spacing, handling empty sections
    code_parts = non_empty_enums + non_empty_dataclasses + non_empty_functions
    code_body = "\n\n".join(code_parts)

    # --- Assemble final code --- REVISED
    final_parts = [header]
    if import_statements:
        # Keep only the correct logic using the pre-calculated import_statements:
        final_imports = sorted(list(import_statements))
        final_parts.append("\n".join(final_imports))
    
    # Add code body (enums, dataclasses, and functions)
    if code_body:
        final_parts.append(code_body)
    
    # Add Helpers (only if not omitted and code is non-empty)
    if helper_functions_code:
        final_parts.append(helper_functions_code)

    # Join parts with two newlines, add trailing newline
    # Ensure empty parts don't create extra newlines by filtering them out
    final_code = "\n\n".join(part for part in final_parts if part) + "\n"

    return final_code

# ===== SECTION: FILE WRITING =====
# Function to write the generated code to a file
