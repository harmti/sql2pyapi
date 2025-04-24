from typing import List, Dict
from .parser import ParsedFunction, ReturnColumn, SQLParameter
import textwrap
import logging
import inflection # Using inflection library for plural->singular

# Define PYTHON_IMPORTS locally as well for fallback cases
PYTHON_IMPORTS = {
    "Any": "from typing import Any",
    # Add others if needed directly by generator logic, but prefer parser-provided imports
}

def _to_singular_camel_case(name: str) -> str:
    """Converts snake_case plural to SingularCamelCase."""
    if not name:
        return "ResultRow"
    # Simple pluralization check (can be improved)
    # Use inflection library for better singularization
    singular_snake = inflection.singularize(name)
    # Convert snake_case to CamelCase
    return inflection.camelize(singular_snake)

def _generate_dataclass(class_name: str, columns: List[ReturnColumn]) -> str:
    """Generates a dataclass string given a name and columns."""
    if not columns or (len(columns) == 1 and columns[0].name == "unknown"):
        # Handle case where schema wasn't found or columns couldn't be parsed
        sql_table_name = columns[0].sql_type if columns else "unknown_table"
        return f"""# TODO: Define dataclass for table '{sql_table_name}'
# @dataclass
# class {class_name}:
#     pass"""

    fields = [f"    {col.name}: {col.python_type}" for col in columns]
    fields_str = "\n".join(fields)
    return f"""@dataclass
class {class_name}:
{fields_str}
"""

def _generate_function(func: ParsedFunction, class_name_map: Dict[str, str]) -> str:
    """Generates the Python async function string."""
    params_list_py = ["conn: AsyncConnection"]
    for p in func.params:
        params_list_py.append(f"{p.python_name}: {p.python_type}{' = None' if p.is_optional else ''}")
    params_str_py = ", ".join(params_list_py)

    # --- Determine return type hint --- 
    return_type_hint = "None" # Default
    final_dataclass_name = None

    if func.returns_table:
        if func.setof_table_name:
            # Case: RETURNS SETOF table_name (schema potentially found)
            # Get the standardized class name from the map
            final_dataclass_name = class_name_map.get(func.setof_table_name, _to_singular_camel_case(func.setof_table_name))
        else:
            # Case: Explicit RETURNS TABLE(...)
            # Generate a unique name based on function name
            final_dataclass_name = _to_singular_camel_case(func.python_name.replace("get_", "").replace("list_", ""))
            if not final_dataclass_name or final_dataclass_name == "ResultRow": # Avoid generic name if possible
                 final_dataclass_name = inflection.camelize(func.python_name) + "Result"
                 
        return_type_hint = final_dataclass_name
        if func.returns_setof:
            return_type_hint = f"List[{final_dataclass_name}]"
        else:
            return_type_hint = f"Optional[{final_dataclass_name}]"
    elif func.return_type != "None":
        # Parser already determined Optional/List wrapping for scalar/record types
        return_type_hint = func.return_type 

    sql_args_placeholders = ", ".join(["%s"] * len(func.params))
    python_args_list = "[" + ", ".join([p.python_name for p in func.params]) + "]"
    
    # Generate the docstring
    if func.sql_comment:
        # Use the cleaned comment directly from the parser
        docstring = f'"""{func.sql_comment}"""'
    else:
        # Default docstring if no comment was found
        docstring = f'"""Call PostgreSQL function {func.sql_name}()."""'

    # --- Generate Function Body --- 
    body_lines = []
    body_lines.append("async with conn.cursor() as cur:")
    body_lines.append(f'    await cur.execute("SELECT * FROM {func.sql_name}({sql_args_placeholders})", {python_args_list})')

    if not func.returns_table and func.return_type == "None":
        body_lines.append("    return None") # Void function
    elif func.returns_setof:
        body_lines.append("    rows = await cur.fetchall()")
        if func.returns_table:
             body_lines.append(f"    # Ensure dataclass '{final_dataclass_name}' is defined above.")
             body_lines.append(f"    return [{final_dataclass_name}(*row) for row in rows] if rows else []")
        elif func.returns_record:
             body_lines.append("    # Return list of tuples for SETOF record")
             body_lines.append("    return rows")
        else: # SETOF scalar 
             body_lines.append("    # Assuming SETOF returns list of single-element tuples for scalars")
             body_lines.append("    return [row[0] for row in rows if row]" )
    else: # Single row expected
        body_lines.append("    row = await cur.fetchone()")
        body_lines.append("    if row is None:")
        body_lines.append("        return None")
        if func.returns_table:
             body_lines.append(f"    # Ensure dataclass '{final_dataclass_name}' is defined above.")
             body_lines.append(f"    return {final_dataclass_name}(*row)")
        elif func.returns_record:
             body_lines.append("    # Return tuple for record type")
             body_lines.append("    return row")
        else: # Single scalar
             body_lines.append("    # Return first element for scalar")
             body_lines.append("    return row[0]") # Assumes scalar is first element

    indented_body = textwrap.indent("\n".join(body_lines), prefix="    ")

    # --- Assemble the function --- 
    func_def = f"""
async def {func.python_name}({params_str_py}) -> {return_type_hint}:
    {docstring}
{indented_body}
"""
    return func_def

def generate_python_code(
    functions: List[ParsedFunction], 
    table_schema_imports: Dict[str, set], # Accept the schema imports
    source_sql_file: str = ""
) -> str:
    """Generates the full Python module code as a string."""
    
    all_imports = set()
    dataclass_defs = {}
    generated_functions = []

    all_imports.add("from psycopg import AsyncConnection")
    # Base typing imports like Optional, List etc are added by the parser as needed

    table_to_class_name_map: Dict[str, str] = {}
    # Keep track of imports needed *directly* by the generated dataclasses
    # These imports come from the types used within the dataclass fields.
    dataclass_field_imports: Dict[str, set] = {}

    # --- First pass: Identify and prepare unique dataclasses --- 
    processed_tables = set()
    for func in functions:
        # Update all_imports with requirements from this function FIRST
        all_imports.update(func.required_imports) 
        
        target_dataclass_name = None
        table_key = None # Key for tracking processed tables (normalized name)
        
        if func.returns_table:
            all_imports.add("from dataclasses import dataclass") # Needed if any table is returned
            if func.setof_table_name:
                table_key = func.setof_table_name # Already normalized by parser
                target_dataclass_name = _to_singular_camel_case(table_key)
                table_to_class_name_map[table_key] = target_dataclass_name
                
                if table_key not in processed_tables:
                    processed_tables.add(table_key)
                    # Get imports FOR THE FIELDS using the PASSED dictionary
                    schema_imports = table_schema_imports.get(table_key, set())
                    dataclass_field_imports[target_dataclass_name] = schema_imports # Store imports per class name
                    
                    if func.return_columns and func.return_columns[0].name != "unknown":
                        dataclass_defs[target_dataclass_name] = _generate_dataclass(target_dataclass_name, func.return_columns)
                    else: # Schema not found by parser
                         placeholder_cols = [ReturnColumn(name="unknown", sql_type=table_key, python_type="Any")] 
                         dataclass_defs[target_dataclass_name] = _generate_dataclass(target_dataclass_name, placeholder_cols)
                         # Add Any import if placeholder used
                         any_import = PYTHON_IMPORTS.get("Any") # Use local PYTHON_IMPORTS
                         if any_import: dataclass_field_imports[target_dataclass_name].add(any_import)

            else: # Explicit RETURNS TABLE(...)
                 target_dataclass_name = _to_singular_camel_case(func.python_name.replace("get_", "").replace("list_", ""))
                 if not target_dataclass_name or target_dataclass_name == "ResultRow":
                     target_dataclass_name = inflection.camelize(func.python_name) + "Result"
                 
                 if target_dataclass_name not in dataclass_defs:
                      dataclass_defs[target_dataclass_name] = _generate_dataclass(target_dataclass_name, func.return_columns)
                      # Imports for explicit table fields are already in func.required_imports
                      # Store them associated with the class name for potential later use/consistency
                      dataclass_field_imports[target_dataclass_name] = func.required_imports
                      
    # Add all necessary field type imports from generated dataclasses
    for imports in dataclass_field_imports.values():
        all_imports.update(imports)

    # --- Second pass: Generate functions --- 
    for func in functions:
        generated_functions.append(_generate_function(func, table_to_class_name_map))
        # Ensure imports from this function are added *after* the dataclass pass
        # but *before* assembling the final import list
        # all_imports.update(func.required_imports) # MOVED to first pass

    # --- Assemble code --- 
    all_imports.discard(None) 
    
    # Consolidate typing imports for better readability
    typing_imports = set()
    other_imports = set()
    
    consolidated_typings = {
        "Optional": "from typing import Optional",
        "List": "from typing import List",
        "Any": "from typing import Any",
        "Tuple": "from typing import Tuple",
        "Dict": "from typing import Dict",
    }
    typing_needed = set()
    datetime_needed = False
    date_needed = False

    for imp in all_imports:
        if imp == "from datetime import datetime":
            datetime_needed = True
            continue # Handled below
        if imp == "from datetime import date":
            date_needed = True
            continue # Handled below
            
        is_typing = False
        for type_name, type_import in consolidated_typings.items():
            if imp == type_import or type_name in imp: # Check if it's the import or used within another (e.g., List[str])
                 typing_needed.add(type_name)
                 is_typing = True
                 # Don't break, might need multiple (e.g., Optional[List[...]])
        
        if not is_typing:
            # Keep non-typing imports as they are (psycopg, uuid, datetime, dataclasses etc.)
            other_imports.add(imp)

    # Build the consolidated typing import line if needed
    if typing_needed:
        sorted_typings = sorted(list(typing_needed))
        typing_line = f"from typing import {', '.join(sorted_typings)}"
        other_imports.add(typing_line) # Add the single consolidated line

    # Build the consolidated datetime/date import line if needed
    if datetime_needed and date_needed:
        other_imports.add("from datetime import datetime, date")
    elif datetime_needed:
        other_imports.add("from datetime import datetime")
    elif date_needed:
        other_imports.add("from datetime import date")

    # Sort all resulting imports
    from_imports = sorted([imp for imp in other_imports if imp.startswith("from")])
    direct_imports = sorted([imp for imp in other_imports if imp.startswith("import")])

    header = f"# Generated by sql-to-python-api from {source_sql_file}\n# DO NOT EDIT MANUALLY"
    
    import_section = "\n".join(direct_imports + from_imports)
    import_section += "\n\n" if dataclass_defs or generated_functions else ""
    
    dataclass_section = "\n\n".join(dataclass_defs[name] for name in sorted(dataclass_defs.keys()))
    dataclass_section += "\n\n" if generated_functions else ""
    
    function_section = "\n\n".join(generated_functions)

    code_parts = [header, import_section]
    if dataclass_section.strip():
        code_parts.append(dataclass_section)
    if function_section.strip():
        code_parts.append(function_section)

    return "\n".join(code_parts).strip() + "\n"
