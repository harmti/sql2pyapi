# ===== SECTION: IMPORTS AND SETUP =====
# Standard library and third-party imports
from typing import List, Tuple, Optional, Dict
import textwrap

# Local imports
from ..sql_models import ParsedFunction, ReturnColumn, SQLParameter
from ..constants import *
from .utils import _to_singular_camel_case
from .return_handlers import _determine_return_type


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
    
    # For ENUM type returns, use a simpler query format and fetchval
    if func.returns_enum_type:
        body_lines = []
        sql_query = f"SELECT {func.sql_name}({sql_args_placeholders});"
        
        # Use proper indentation and string formatting for the SQL query
        body_lines.append(f'    async with conn.cursor() as cur:')
        body_lines.append(f'        await cur.execute("SELECT * FROM {func.sql_name}({sql_args_placeholders})", {python_args_list})')
        body_lines.append(f'        row = await cur.fetchone()')
        body_lines.append(f'        if row is None:')
        body_lines.append(f'            return None')
        body_lines.append(f'        return {func.return_type}(row[0])')
        return body_lines
        
    # Check if there are any enum parameters by checking if 'Enum' is in required imports
    # and if any parameter types match enum class names
    is_enum_import = 'Enum' in func.required_imports
    has_enum_params = False
    
    if is_enum_import:
        # Check for parameters with types that could be enums
        has_enum_params = any(not p.python_type.startswith(('Optional[', 'List[')) and 
                              not p.python_type in ('str', 'int', 'float', 'bool', 'UUID', 'datetime', 'date', 'Decimal', 'Any', 'dict', 'Dict[str, Any]')
                              for p in func.params)
    
    # If we have enum parameters, we need to extract the .value attribute
    if has_enum_params:
        body_lines.append("# Extract .value from enum parameters")
        for p in func.params:
            if not p.python_type.startswith(('Optional[', 'List[')) and not p.python_type in ('str', 'int', 'float', 'bool', 'UUID', 'datetime', 'date', 'Decimal', 'Any', 'dict', 'Dict[str, Any]'):
                body_lines.append(f"    {p.python_name}_value = {p.python_name}.value if {p.python_name} is not None else None")
        
        # Modify the python_args_list to use the *_value variables for enum parameters
        enum_args_list = []
        for p in func.params:
            if not p.python_type.startswith(('Optional[', 'List[')) and not p.python_type in ('str', 'int', 'float', 'bool', 'UUID', 'datetime', 'date', 'Decimal', 'Any', 'dict', 'Dict[str, Any]'):
                enum_args_list.append(f"{p.python_name}_value")
            else:
                enum_args_list.append(p.python_name)
        python_args_list = "[" + ", ".join(enum_args_list) + "]"
    
    # Common setup for all other function types
    body_lines.append("async with conn.cursor() as cur:")
    body_lines.append(
        f'    await cur.execute("SELECT * FROM {func.sql_name}({sql_args_placeholders})", {python_args_list})'
    )
    
    # For void returns, no need to fetch any results
    if func.return_type == 'None':
        body_lines.append("    # Function returns void, no results to fetch")
        body_lines.append("    return None")
        return body_lines
        
    # For scalar returns (int, str, bool, etc.), use fetchone
    if not func.returns_table and not func.returns_record and not func.returns_enum_type:
        body_lines.append("    row = await cur.fetchone()")
        body_lines.append("    if row is None:")
        body_lines.append("        return None")
        body_lines.append("    return row[0]")
        return body_lines
    
    # Handle different return types
    if func.returns_setof:
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
    
    # Handle SETOF ENUM type
    if func.returns_enum_type:
        body_lines.append("    if not rows:")
        body_lines.append("        return []")
        body_lines.append(f"    return [{func.return_type}(row[0]) for row in rows]")
        return body_lines
    
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
            
        # Check if any columns are ENUM types by checking if 'Enum' is in required imports
        is_enum_import = 'Enum' in func.required_imports
        has_enum_columns = False
        
        if is_enum_import:
            # Check for columns with types that could be enums
            has_enum_columns = any(not col.python_type.startswith(('Optional[', 'List[')) and 
                                  not col.python_type in ('str', 'int', 'float', 'bool', 'UUID', 'datetime', 'date', 'Decimal', 'Any', 'dict', 'Dict[str, Any]')
                                  for col in func.return_columns)
        
        if has_enum_columns:
            # Generate an inner helper function to efficiently convert enum values during object creation
            body_lines.append(f"    # Inner helper function for efficient conversion")
            body_lines.append(f"    def create_{singular_class_name.lower()}(row):")
            
            # Generate field assignments with ENUM conversions
            field_assignments = []
            for i, col in enumerate(func.return_columns):
                if not col.python_type.startswith(('Optional[', 'List[')) and not col.python_type in ('str', 'int', 'float', 'bool', 'UUID', 'datetime', 'date', 'Decimal', 'Any', 'dict', 'Dict[str, Any]'):
                    field_assignments.append(f"{col.name}={col.python_type}(row[{i}]) if row[{i}] is not None else None")
                else:
                    field_assignments.append(f"{col.name}=row[{i}]")
            field_assignments_str = ",\n                ".join(field_assignments)
            
            body_lines.append(f"        return {singular_class_name}(\n                {field_assignments_str}\n            )")
            body_lines.append(f"")
            body_lines.append(f"    try:")
            body_lines.append(f"        return [create_{singular_class_name.lower()}(row) for row in rows]")
        else:
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

    # Handle ENUM type returns
    if func.returns_enum_type:
        body_lines.append(f"    return {func.return_type}(row[0])")
        return body_lines

    if func.returns_table:
        # Handle single row table/composite type returns -> Hint is Optional[Dataclass]
        # Ensure we use the singular form of the class name
        singular_class_name = final_dataclass_name
        # If it's a table name, make sure it's in singular form
        if func.returns_table and func.returns_sql_type_name:
            singular_class_name = _to_singular_camel_case(func.returns_sql_type_name)
            
        body_lines.append(f"    # Ensure dataclass '{singular_class_name}' is defined above.")
        body_lines.append(f"    # Expecting simple tuple return for composite type {singular_class_name}")
        
        # Check if any columns are ENUM types by checking if 'Enum' is in required imports
        is_enum_import = 'Enum' in func.required_imports
        has_enum_columns = False
        
        if is_enum_import:
            # Check for columns with types that could be enums
            has_enum_columns = any(not col.python_type.startswith(('Optional[', 'List[')) and 
                                  not col.python_type in ('str', 'int', 'float', 'bool', 'UUID', 'datetime', 'date', 'Decimal', 'Any', 'dict', 'Dict[str, Any]')
                                  for col in func.return_columns)
        
        if has_enum_columns:
            # Generate field assignments with ENUM conversions
            field_assignments = []
            for i, col in enumerate(func.return_columns):
                if not col.python_type.startswith(('Optional[', 'List[')) and not col.python_type in ('str', 'int', 'float', 'bool', 'UUID', 'datetime', 'date', 'Decimal', 'Any', 'dict', 'Dict[str, Any]'):
                    field_assignments.append(f"{col.name}=row[{i}]")
                else:
                    field_assignments.append(f"{col.name}=row[{i}]")
            field_assignments_str = ",\n                ".join(field_assignments)
            
            body_lines.append(f"    try:")
            body_lines.append(f"        instance = {singular_class_name}(\n                {field_assignments_str}\n            )")
            body_lines.append(f"        # Check for 'empty' composite rows (all values are None) returned as a single tuple")
            body_lines.append(f"        # Note: This check might be DB-driver specific for NULL composites")
            body_lines.append(f"        if all(v is None for v in row):")
            # Return None if the single row represents a NULL composite (consistency with Optional hint)
            body_lines.append(f"             return None") 
            
            # Convert string values to enum objects after creating the instance
            for i, col in enumerate(func.return_columns):
                if not col.python_type.startswith(('Optional[', 'List[')) and not col.python_type in ('str', 'int', 'float', 'bool', 'UUID', 'datetime', 'date', 'Decimal', 'Any', 'dict', 'Dict[str, Any]'):
                    body_lines.append(f"        if instance.{col.name} is not None:")
                    body_lines.append(f"            instance.{col.name} = {col.python_type}(instance.{col.name})")
            
            body_lines.append(f"        return instance") # Return the single instance, not a list
        else:
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


