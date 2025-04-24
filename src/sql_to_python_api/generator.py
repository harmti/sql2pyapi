from typing import List
from .parser import ParsedFunction, ReturnColumn, SQLParameter
import textwrap
import logging

def _generate_dataclass(func: ParsedFunction) -> str:
    """Generates a dataclass string for a function returning TABLE."""
    # Attempt to generate a sensible class name from the SQL function name
    # e.g., get_user_details -> UserDetails, list_all_items -> AllItems
    class_name_base = func.python_name
    if class_name_base.startswith("get_"):
        class_name_base = class_name_base[4:]
    elif class_name_base.startswith("list_"):
        class_name_base = class_name_base[5:]
        
    class_name = class_name_base.replace("_", " ").title().replace(" ", "")
    if not class_name:
        class_name = "ResultRow" # Fallback
    
    # Handle potential SETOF table_name case where columns might be placeholder
    if func.return_columns and func.return_columns[0].name == "unknown":
         table_name_guess = func.return_columns[0].sql_type.capitalize()
         logging.warning(f"Using assumed table name '{table_name_guess}' for dataclass name due to 'RETURNS SETOF {func.return_columns[0].sql_type}'.")
         class_name = table_name_guess
         # Return a placeholder comment using triple quotes for multi-line f-string
         return f"""# TODO: Define dataclass for table '{func.return_columns[0].sql_type}' returned by {func.sql_name}
# @dataclass
# class {class_name}:
#     pass"""

    fields = []
    for col in func.return_columns:
        fields.append(f"    {col.name}: {col.python_type}")

    fields_str = "\n".join(fields)
    return f"""@dataclass
class {class_name}:
{fields_str}
"""

def _generate_function(func: ParsedFunction) -> str:
    """Generates the Python async function string."""
    # Generate Python parameter string with Optional[...] = None for defaults
    params_list_py = ["conn: AsyncConnection"]
    for p in func.params:
        if p.is_optional:
            # Type hint already includes Optional[...] from parser
            params_list_py.append(f"{p.name}: {p.python_type} = None") 
        else:
            params_list_py.append(f"{p.name}: {p.python_type}")
    params_str_py = ", ".join(params_list_py)

    # Determine return type hint
    return_type_hint = "None"
    class_name_for_table = "ResultRow" # Default
    # Derive class name based on function name or specified table name
    if func.returns_table:
        if func.return_columns and func.return_columns[0].name != "unknown":
            # RETURNS TABLE(...) case - use derived name
            class_name_base = func.python_name
            if class_name_base.startswith("get_"):
                 class_name_base = class_name_base[4:]
            elif class_name_base.startswith("list_"):
                 class_name_base = class_name_base[5:]
            class_name_for_table = class_name_base.replace("_", " ").title().replace(" ", "") or "ResultRow"
        elif func.return_columns: 
             # RETURNS SETOF table_name case - use capitalized table name
             class_name_for_table = func.return_columns[0].sql_type.capitalize()
        
        return_type_hint = class_name_for_table
        if func.returns_setof:
            return_type_hint = f"List[{class_name_for_table}]"
        else:
            # Single table row result should be Optional
            return_type_hint = f"Optional[{class_name_for_table}]"
    
    # Use the return_type calculated by the parser for non-table cases
    elif func.return_type != "None":
        return_type_hint = func.return_type # Already includes Optional/List wrapping from parser

    sql_args_placeholders = ", ".join(["%s"] * len(func.params))
    python_args_list = "[" + ", ".join([p.name for p in func.params]) + "]"
    docstring = f'""Call PostgreSQL function {func.sql_name}().""'

    # --- Generate Function Body --- 
    body_lines = []
    body_lines.append("async with conn.cursor() as cur:")
    body_lines.append(f'    await cur.execute("SELECT * FROM {func.sql_name}({sql_args_placeholders})", {python_args_list})')

    if func.return_type == "None" and not func.returns_table and not func.returns_record:
        body_lines.append("    return None")
    elif func.returns_setof:
        body_lines.append("    rows = await cur.fetchall()")
        if func.returns_table:
             # Assumes class_name_for_table is correct (either derived or table name)
             body_lines.append(f"    # TODO: Ensure dataclass '{class_name_for_table}' is defined correctly above.")
             body_lines.append(f"    return [{class_name_for_table}(*row) for row in rows] if rows else []")
        else: # SETOF scalar or record
             body_lines.append("    return [row[0] for row in rows if row]" )
    else: # Single row expected
        body_lines.append("    row = await cur.fetchone()")
        body_lines.append("    if row is None:")
        body_lines.append("        return None")
        if func.returns_table:
            # Assumes class_name_for_table is correct
             body_lines.append(f"    # TODO: Ensure dataclass '{class_name_for_table}' is defined correctly above.")
             body_lines.append(f"    return {class_name_for_table}(*row)")
        else: # Single scalar or record
             body_lines.append("    return row[0] if len(row) == 1 else row")

    indented_body = textwrap.indent("\n".join(body_lines), prefix="    ")

    # --- Assemble the function --- 
    func_def = f"""
async def {func.python_name}({params_str_py}) -> {return_type_hint}:
    {docstring}
{indented_body}
"""
    return func_def

def generate_python_code(functions: List[ParsedFunction], source_sql_file: str = "") -> str:
    """Generates the full Python module code as a string."""
    if not functions:
        return "# No functions found in the source SQL file.\n"

    all_imports = set()
    dataclasses = []
    generated_functions = []

    all_imports.add("from psycopg import AsyncConnection")
    all_imports.add("from typing import Optional, List, Any, Tuple, Dict")

    for func in functions:
        all_imports.update(func.required_imports)
        if func.returns_table:
            # Ensure dataclass import is added *if* a table is returned
            all_imports.add("from dataclasses import dataclass") 
            dataclasses.append(_generate_dataclass(func))
        generated_functions.append(_generate_function(func))

    # Filter out potential None values from imports, just in case
    all_imports = {imp for imp in all_imports if imp}

    from_imports = sorted([imp for imp in all_imports if imp.startswith("from")])
    direct_imports = sorted([imp for imp in all_imports if imp.startswith("import")])

    header = f"# Generated by sql-to-python-api from {source_sql_file}\n# DO NOT EDIT MANUALLY"
    
    import_section = "\n".join(direct_imports + from_imports)
    # Add an extra newline after imports if there are subsequent sections
    import_section += "\n" if dataclasses or generated_functions else ""
    
    dataclass_section = "\n\n".join(dataclasses)
     # Add an extra newline after dataclasses if there are functions
    dataclass_section += "\n" if generated_functions else ""
    
    function_section = "\n\n".join(generated_functions)

    # Assemble the final code
    code_parts = [header, import_section]
    if dataclass_section.strip(): # Only add if there's content
        code_parts.append(dataclass_section)
    if function_section.strip(): # Only add if there's content
        code_parts.append(function_section)

    return "\n".join(code_parts).strip() + "\n" # Ensure single trailing newline 