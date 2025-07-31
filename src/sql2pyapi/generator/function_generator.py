# ===== SECTION: IMPORTS AND SETUP =====
# Standard library and third-party imports
import textwrap

from ..constants import *
from ..parser.utils import _to_singular_camel_case

# Local imports
from ..sql_models import ParsedFunction
from ..sql_models import ReturnColumn
from ..sql_models import SQLParameter
from .composite_unpacker import generate_composite_unpacking_code
from .composite_unpacker import needs_nested_unpacking
from .return_handlers import _determine_return_type


def _python_type_to_sql_type(python_type: str) -> str:
    """
    Maps Python types back to SQL types for AS clause generation.

    Args:
        python_type: The Python type string

    Returns:
        Corresponding SQL type string
    """
    # Basic type mappings (reverse of TYPE_MAP)
    python_to_sql = {
        "str": "TEXT",
        "int": "INTEGER",
        "bool": "BOOLEAN",
        "float": "DOUBLE PRECISION",
        "UUID": "UUID",
        "datetime": "TIMESTAMP",
        "date": "DATE",
        "Decimal": "NUMERIC",
        "dict": "JSON",
        "Mood": "mood",  # Custom enum type
        "Any": "TEXT",  # Fallback
    }

    # Handle Optional types
    if python_type.startswith("Optional[") and python_type.endswith("]"):
        inner_type = python_type[9:-1]
        return python_to_sql.get(inner_type, "TEXT")

    return python_to_sql.get(python_type, "TEXT")


def _generate_parameter_list(func_params: list[SQLParameter]) -> tuple[list[SQLParameter], str]:
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


def _generate_function_body(
    func: ParsedFunction,
    final_dataclass_name: str | None,
    sorted_params: list[SQLParameter],
    composite_types: dict[str, list[ReturnColumn]],
) -> list[str]:
    """
    Generates the body of a Python async function based on the SQL function's return type.

    Args:
        func (ParsedFunction): The parsed SQL function definition
        final_dataclass_name (Optional[str]): The name of the dataclass for table returns
        sorted_params (List[SQLParameter]): Sorted parameters for the function
        composite_types (Dict[str, List[ReturnColumn]]): Dictionary of all known composite types

    Returns:
        List[str]: Lines of code for the function body

    Notes:
        - Handles different return types: void, scalar, record, table, setof
        - Implements proper NULL handling for both None rows and composite NULL rows
        - Uses constants from constants.py for consistent code generation
    """
    body_lines = []
    param_preparation_lines = []

    # --- Prepare arguments for SQL call (handles enums and special types) ---
    enum_types = getattr(func, "enum_types", {}) if hasattr(func, "enum_types") else {}

    def is_enum_param(param_sql_type: str) -> bool:
        if param_sql_type in enum_types:
            return True
        return bool("." in param_sql_type and param_sql_type.split(".")[-1] in enum_types)

    def is_json_param(param_sql_type: str) -> bool:
        return param_sql_type.lower() in ("json", "jsonb")

    # Check if we need special parameter handling
    has_any_enum_params = any(is_enum_param(p.sql_type) for p in sorted_params)
    has_any_json_params = any(is_json_param(p.sql_type) for p in sorted_params)

    # Only add parameter preparation code if needed
    if has_any_enum_params:
        param_preparation_lines.append("# Extract .value from enum parameters")
        for p in sorted_params:
            if is_enum_param(p.sql_type):
                # Ensure None check for the enum object itself before accessing .value
                param_preparation_lines.append(
                    f"{p.python_name}_value = {p.python_name}.value if {p.python_name} is not None else None"
                )

    # Handle JSON parameters separately to avoid breaking existing tests
    if has_any_json_params:
        param_preparation_lines.append("# Handle JSON parameters with custom encoder for UUID support")
        param_preparation_lines.append("import json")
        param_preparation_lines.append("from uuid import UUID")
        param_preparation_lines.append("from datetime import datetime")
        param_preparation_lines.append("")
        param_preparation_lines.append("class DatabaseJSONEncoder(json.JSONEncoder):")
        param_preparation_lines.append("    def default(self, obj):")
        param_preparation_lines.append("        if isinstance(obj, UUID):")
        param_preparation_lines.append("            return str(obj)")
        param_preparation_lines.append("        elif isinstance(obj, datetime):")
        param_preparation_lines.append("            return obj.isoformat()")
        param_preparation_lines.append("        return super().default(obj)")
        param_preparation_lines.append("")
        for p in sorted_params:
            if is_json_param(p.sql_type):
                # Convert Python dict to JSON string for JSON parameters using custom encoder
                param_preparation_lines.append(
                    f"{p.python_name}_json = json.dumps({p.python_name}, cls=DatabaseJSONEncoder) if {p.python_name} is not None else None"
                )

    body_lines.extend(param_preparation_lines)

    # --- Dynamically build SQL named arguments and parameter dictionary ---
    body_lines.append("_sql_named_args_parts = []")
    body_lines.append("_call_params_dict = {}")
    body_lines.append("")  # Add a blank line for readability

    for p in sorted_params:
        param_key_for_dict = p.python_name

        # Determine the actual value variable to use based on parameter type
        if is_enum_param(p.sql_type):
            actual_value_var = f"{p.python_name}_value"
        elif is_json_param(p.sql_type) and has_any_json_params:  # Only use _json suffix if we have JSON params
            # For JSON parameters, we use the JSON-converted value but keep the original parameter name
            actual_value_var = f"{p.python_name}_json"
        else:
            actual_value_var = p.python_name

        if p.is_optional:
            body_lines.append(
                f"if {p.python_name} is not None: # User provided a value, or it's an explicit None for a DEFAULT NULL param"
            )
            body_lines.append(f"    _sql_named_args_parts.append(f'{p.name} := %({param_key_for_dict})s')")
            body_lines.append(f"    _call_params_dict['{param_key_for_dict}'] = {actual_value_var}")
            # If {p.python_name} is None:
            #   - and p.has_sql_default (non-NULL DEFAULT): we omit it, SQL uses its default.
            #   - and not p.has_sql_default (SQL DEFAULT is NULL): we *could* pass it explicitly if needed,
            #     but omitting it also works for DEFAULT NULL. The current logic omits for simplicity.
            #     If explicit NULL passing for DEFAULT NULL cases is desired when Python arg is None,
            #     an `else` block here would be needed for `if {p.python_name} is not None:`.
            #     For now, omitting is fine for both `DEFAULT <value>` and `DEFAULT NULL` when Python arg is None.
        else:  # Parameter is not optional, it must be included.
            body_lines.append(f"_sql_named_args_parts.append(f'{p.name} := %({param_key_for_dict})s')")
            body_lines.append(f"_call_params_dict['{param_key_for_dict}'] = {actual_value_var}")
        body_lines.append("")  # Add a blank line for readability after each param logic

    body_lines.append("_sql_query_named_args = ', '.join(_sql_named_args_parts)")

    # Determine the base SQL query structure using func.sql_name (which is schema-qualified)
    # For RECORD functions, add AS clause with column definitions
    if func.returns_record and func.return_columns:
        # Generate column definitions for AS clause
        as_columns = []
        for col in func.return_columns:
            # Map Python types back to SQL types for AS clause
            sql_type = _python_type_to_sql_type(col.python_type)
            as_columns.append(f"{col.name} {sql_type}")
        as_clause = f" AS ({', '.join(as_columns)})"
        query_template = f"SELECT * FROM {func.sql_name}({{_sql_query_named_args}}){as_clause}"
    else:
        query_template = f"SELECT * FROM {func.sql_name}({{_sql_query_named_args}})"

    body_lines.append(f'_full_sql_query = f"{query_template}"')
    body_lines.append("")  # Blank line

    # --- Execute SQL query ---
    body_lines.append("async with conn.cursor() as cur:")
    body_lines.append("    await cur.execute(_full_sql_query, _call_params_dict)")  # Use the dictionary

    # --- Process results (largely existing logic) ---
    # For void returns, no need to fetch any results
    if func.return_type == "None":
        body_lines.append("    # Function returns void, no results to fetch")
        body_lines.append("    return None")
        return body_lines

    # For scalar returns (int, str, bool, etc.), use fetchone - ONLY if NOT SETOF
    if not func.returns_table and not func.returns_record and not func.returns_enum_type and not func.returns_setof:
        body_lines.append("    row = await cur.fetchone()")
        body_lines.append("    if row is None:")
        body_lines.append("        return None")
        body_lines.append("    return row[0]")
        return body_lines

    # Handle different return types
    if func.returns_setof:
        # Handle SETOF returns (multiple rows)
        body_lines.extend(_generate_setof_return_body(func, final_dataclass_name, composite_types))
    else:
        # Handle single row returns (scalar, record, or single table row)
        body_lines.extend(_generate_single_row_return_body(func, final_dataclass_name, composite_types))

    return body_lines


def _generate_setof_return_body(
    func: ParsedFunction, final_dataclass_name: str | None, composite_types: dict[str, list[ReturnColumn]]
) -> list[str]:
    """
    Generates code for handling SETOF returns (multiple rows).

    Args:
        func (ParsedFunction): The parsed SQL function definition
        final_dataclass_name (Optional[str]): The name of the dataclass for table returns
        composite_types (Dict[str, List[ReturnColumn]]): Dictionary of all known composite types

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

    if func.returns_record and func.returns_setof:
        # Handle SETOF RECORD specially - return list of tuples
        body_lines.append("    # Return list of tuples for SETOF record")
        body_lines.append("    return rows")
        return body_lines
    elif func.returns_table:
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

        # Debug logging for troubleshooting
        # Removed debug logging - implementation is stable

        # Check if any columns are ENUM types by checking if 'Enum' is in required imports
        is_enum_import = "Enum" in func.required_imports
        has_enum_columns = False

        if is_enum_import:
            # Check for columns with types that could be enums
            has_enum_columns = any(
                not col.python_type.startswith(("Optional[", "List["))
                and col.python_type
                not in (
                    "str",
                    "int",
                    "float",
                    "bool",
                    "UUID",
                    "datetime",
                    "date",
                    "Decimal",
                    "Any",
                    "dict",
                    "Dict[str, Any]",
                )
                for col in func.return_columns
            )

        if has_enum_columns:
            # Generate an inner helper function to efficiently convert enum values during object creation
            body_lines.append("    # Inner helper function for efficient conversion")
            # Defensive check for None singular_class_name
            function_name_suffix = singular_class_name.lower() if singular_class_name else "item"
            body_lines.append(f"    def create_{function_name_suffix}(row):")

            # Generate field assignments with ENUM conversions
            field_assignments = []
            for i, col in enumerate(func.return_columns):
                if not col.python_type.startswith(("Optional[", "List[")) and col.python_type not in (
                    "str",
                    "int",
                    "float",
                    "bool",
                    "UUID",
                    "datetime",
                    "date",
                    "Decimal",
                    "Any",
                    "dict",
                    "Dict[str, Any]",
                ):
                    field_assignments.append(
                        f"{col.name}={col.python_type}(row[{i}]) if row[{i}] is not None else None"
                    )
                else:
                    field_assignments.append(f"{col.name}=row[{i}]")
            field_assignments_str = ",\n                ".join(field_assignments)

            body_lines.append("        try:")
            body_lines.append(
                "            # First try with tuple unpacking to handle both tuple and dict row factories"
            )
            body_lines.append(f"            instance = {singular_class_name}(*row)")

            # Convert string values to enum objects after creating the instance
            for i, col in enumerate(func.return_columns):
                if not col.python_type.startswith(("Optional[", "List[")) and col.python_type not in (
                    "str",
                    "int",
                    "float",
                    "bool",
                    "UUID",
                    "datetime",
                    "date",
                    "Decimal",
                    "Any",
                    "dict",
                    "Dict[str, Any]",
                ):
                    body_lines.append(f"            if instance.{col.name} is not None:")
                    body_lines.append(f"                instance.{col.name} = {col.python_type}(instance.{col.name})")

            body_lines.append("            return instance")
            body_lines.append("        except (TypeError, KeyError) as e:")
            body_lines.append("            # Fallback to explicit construction if tuple unpacking fails")
            body_lines.append("            try:")
            body_lines.append(
                f"                return {singular_class_name}(\n                    {field_assignments_str}\n                )"
            )
            body_lines.append("            except Exception as inner_e:")
            body_lines.append("                # Re-raise the original error if the fallback also fails")
            body_lines.append(
                f'                raise TypeError(f"Failed to map row to {singular_class_name}. Original error: {{e}}, Fallback error: {{inner_e}}") from e'
            )

            body_lines.append("")

            # Main try block for the function
            body_lines.append("    try:")
            body_lines.append(f"        return [create_{singular_class_name.lower()}(row) for row in rows]")
        else:
            # No enum columns case - just use tuple unpacking directly
            body_lines.append("    try:")
            body_lines.append(f"        return [{singular_class_name}(*r) for r in rows]")

        body_lines.append("    except TypeError as e:")
        body_lines.append("        # Tuple unpacking failed. This often happens if the DB connection")
        body_lines.append("        # is configured with a dict-like row factory (e.g., DictRow).")
        body_lines.append("        # This generated code expects the default tuple row factory.")
        body_lines.append("        raise TypeError(")
        body_lines.append(f'            f"Failed to map SETOF results to dataclass list for {singular_class_name}. "')
        body_lines.append('            f"Check DB connection: Default tuple row_factory expected. Error: {e}"')
        body_lines.append("        )")

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


def _generate_single_row_return_body(
    func: ParsedFunction, final_dataclass_name: str | None, composite_types: dict[str, list[ReturnColumn]]
) -> list[str]:
    """
    Generates code for handling single-row returns (scalar, record, or table).

    Args:
        func (ParsedFunction): The parsed SQL function definition
        final_dataclass_name (Optional[str]): The name of the dataclass for table returns
        composite_types (Dict[str, List[ReturnColumn]]): Dictionary of all known composite types

    Returns:
        List[str]: Lines of code for handling single-row returns
    """
    body_lines = []
    body_lines.append("    row = await cur.fetchone()")
    body_lines.append("    if row is None:")
    # If returns_table is true BUT returns_setof is false, the hint is Optional[Dataclass],
    # so we should return None here, not [].
    body_lines.append("        return None")

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

        # Check if we need special handling for nested composites
        if func.return_columns and needs_nested_unpacking(func.return_columns, composite_types):
            # Use the nested composite unpacking helper
            body_lines.append("    try:")
            unpacking_lines = generate_composite_unpacking_code(
                singular_class_name, func.return_columns, composite_types, indent="        "
            )
            body_lines.extend(unpacking_lines)
            body_lines.append("    except TypeError as e:")
            body_lines.append("        # Tuple unpacking failed. This often happens if the DB connection")
            body_lines.append("        # is configured with a dict-like row factory (e.g., DictRow).")
            body_lines.append("        # This generated code expects the default tuple row factory.")
            body_lines.append("        raise TypeError(")
            body_lines.append(f'            f"Failed to map single row result to dataclass {singular_class_name}. "')
            body_lines.append(
                '            f"Check DB connection: Default tuple row_factory expected. Row: {row!r}. Error: {e}"'
            )
            body_lines.append("        )")
        else:
            # Original logic for non-nested composites
            # Check if any columns are ENUM types by checking if 'Enum' is in required imports
            is_enum_import = "Enum" in func.required_imports
            has_enum_columns = False

            if is_enum_import:
                # Check for columns with types that could be enums
                has_enum_columns = any(
                    not col.python_type.startswith(("Optional[", "List["))
                    and col.python_type
                    not in (
                        "str",
                        "int",
                        "float",
                        "bool",
                        "UUID",
                        "datetime",
                        "date",
                        "Decimal",
                        "Any",
                        "dict",
                        "Dict[str, Any]",
                    )
                    for col in func.return_columns
                )

            if has_enum_columns:
                # Generate field assignments with ENUM conversions
                field_assignments = []
                for i, col in enumerate(func.return_columns):
                    if not col.python_type.startswith(("Optional[", "List[")) and col.python_type not in (
                        "str",
                        "int",
                        "float",
                        "bool",
                        "UUID",
                        "datetime",
                        "date",
                        "Decimal",
                        "Any",
                        "dict",
                        "Dict[str, Any]",
                    ):
                        field_assignments.append(f"{col.name}=row[{i}]")
                    else:
                        field_assignments.append(f"{col.name}=row[{i}]")
                field_assignments_str = ",\n                    ".join(field_assignments)

                body_lines.append("    try:")
                body_lines.append(
                    "        # First try with tuple unpacking to handle both tuple and dict row factories"
                )
                body_lines.append(f"        instance = {singular_class_name}(*row)")
                body_lines.append(
                    "        # Check for 'empty' composite rows (all values are None) returned as a single tuple"
                )
                body_lines.append("        # Note: This check might be DB-driver specific for NULL composites")
                body_lines.append("        if all(v is None for v in row):")
                # Return None if the single row represents a NULL composite (consistency with Optional hint)
                body_lines.append("             return None")

                # Convert string values to enum objects after creating the instance
                for i, col in enumerate(func.return_columns):
                    if not col.python_type.startswith(("Optional[", "List[")) and col.python_type not in (
                        "str",
                        "int",
                        "float",
                        "bool",
                        "UUID",
                        "datetime",
                        "date",
                        "Decimal",
                        "Any",
                        "dict",
                        "Dict[str, Any]",
                    ):
                        body_lines.append(f"        if instance.{col.name} is not None:")
                        body_lines.append(f"            instance.{col.name} = {col.python_type}(instance.{col.name})")

                body_lines.append("        return instance")  # Return the single instance, not a list

                body_lines.append("    except (TypeError, KeyError) as e:")
                body_lines.append("        # If tuple unpacking fails, try explicit construction")
                body_lines.append("        try:")
                body_lines.append(
                    f"            instance = {singular_class_name}(\n                    {field_assignments_str}\n                )"
                )

                # Convert string values to enum objects after creating the instance
                for i, col in enumerate(func.return_columns):
                    if not col.python_type.startswith(("Optional[", "List[")) and col.python_type not in (
                        "str",
                        "int",
                        "float",
                        "bool",
                        "UUID",
                        "datetime",
                        "date",
                        "Decimal",
                        "Any",
                        "dict",
                        "Dict[str, Any]",
                    ):
                        body_lines.append(f"            if instance.{col.name} is not None:")
                        body_lines.append(
                            f"                instance.{col.name} = {col.python_type}(instance.{col.name})"
                        )

                body_lines.append("            return instance")
                body_lines.append("        except Exception as inner_e:")
                body_lines.append("            # Re-raise the original error if the fallback also fails")
                body_lines.append(
                    f'            raise TypeError(f"Failed to map row to {singular_class_name}. Original error: {{e}}, Fallback error: {{inner_e}}") from e'
                )
            else:
                body_lines.append("    try:")
                body_lines.append(f"        instance = {singular_class_name}(*row)")
                body_lines.append(
                    "        # Check for 'empty' composite rows (all values are None) returned as a single tuple"
                )
                body_lines.append("        # Note: This check might be DB-driver specific for NULL composites")
                body_lines.append("        if all(v is None for v in row):")
                # Return None if the single row represents a NULL composite (consistency with Optional hint)
                body_lines.append("             return None")
                body_lines.append("        return instance")  # Return the single instance, not a list
                body_lines.append("    except TypeError as e:")
                body_lines.append("        # Tuple unpacking failed. This often happens if the DB connection")
                body_lines.append("        # is configured with a dict-like row factory (e.g., DictRow).")
                body_lines.append("        # This generated code expects the default tuple row factory.")
                body_lines.append("        raise TypeError(")
                body_lines.append(
                    f'            f"Failed to map single row result to dataclass {singular_class_name}. "'
                )
                body_lines.append(
                    '            f"Check DB connection: Default tuple row_factory expected. Row: {row!r}. Error: {e}"'
                )
                body_lines.append("        )")

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


def _generate_function(func: ParsedFunction, composite_types: dict[str, list[ReturnColumn]] | None = None) -> str:
    """
    Generates a Python async function string from a parsed SQL function definition.

    This is the core code generation function that creates Python wrapper functions
    for PostgreSQL functions. It handles different return types (scalar, record, table),
    parameter ordering, docstring generation, and proper NULL handling.

    Args:
        func (ParsedFunction): The parsed SQL function definition
        composite_types (Dict[str, List[ReturnColumn]], optional): Dictionary of all known composite types

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

    # sql_args_placeholders and python_args_list are now generated dynamically in _generate_function_body
    # So, we remove their old static generation here.

    # Generate the docstring
    docstring = _generate_docstring(func)

    # Generate the function body based on the return type
    # Pass sorted_params and composite_types to _generate_function_body
    body_lines = _generate_function_body(func, final_dataclass_name, sorted_params, composite_types or {})

    indented_body = textwrap.indent("\n".join(body_lines), prefix="    ")

    # Ensure we use the correct class name in the return type hint for both
    # schema-qualified and non-schema-qualified table names.
    # However, if the type was overridden to 'Any' (e.g., missing schema and allow_missing_schemas=True),
    # then the return_type_hint from _determine_return_type should be preserved.

    # Check if _determine_return_type already set the hint to involve 'Any'
    is_already_any_hint = (
        return_type_hint in {"Any", "Optional[Any]", "List[Any]"}
    )

    if not is_already_any_hint:  # Only proceed with this refinement if not already an 'Any' hint
        if func.returns_table:
            # Handle SETOF table returns
            if func.returns_setof and func.setof_table_name:
                # Convert the table name to a singular class name
                singular_name = _to_singular_camel_case(func.setof_table_name)
                return_type_hint = f"List[{singular_name}]"
            # Handle single table returns
            elif not func.returns_setof and func.returns_sql_type_name:  # Added not func.returns_setof for clarity
                # Convert the table name to a singular class name
                singular_name = _to_singular_camel_case(func.returns_sql_type_name)
                return_type_hint = f"Optional[{singular_name}]"
            # Handle ad-hoc RETURNS TABLE
            elif (
                final_dataclass_name and not func.setof_table_name and not func.returns_sql_type_name
            ):  # Check it's not a named table already handled
                if func.returns_setof:
                    return_type_hint = f"List[{final_dataclass_name}]"
                else:
                    return_type_hint = f"Optional[{final_dataclass_name}]"

    # Assemble the function string
    sql_name_parts = func.sql_name.split(".")
    python_func_name_base = sql_name_parts[-1]  # Get part after schema if present

    # Sanitize the base name to be a valid Python identifier
    # Replace non-alphanumeric (excluding underscore) with underscore
    sanitized_base_name = "".join(c if c.isalnum() or c == "_" else "_" for c in python_func_name_base)

    # Ensure it doesn't start with a digit (prefix with underscore if it does)
    if sanitized_base_name and sanitized_base_name[0].isdigit():
        python_func_name = "_" + sanitized_base_name
    elif not sanitized_base_name:  # Handle empty base name (e.g. from "schema.")
        python_func_name = "_unnamed_function"  # Or raise error
    else:
        python_func_name = sanitized_base_name

    func_signature = f"async def {python_func_name}({params_str_py}) -> {return_type_hint}:"

    # Combine signature, docstring, and body
    func_def = f"{func_signature}\n{docstring}\n{indented_body}\n"
    return func_def
