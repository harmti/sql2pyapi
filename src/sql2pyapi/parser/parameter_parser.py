# ===== SECTION: IMPORTS =====
import logging
import re

# Import custom error classes
# Import the models
from ..sql_models import SQLParameter

# Import type mapper
from .type_mapper import map_sql_to_python_type


# ===== SECTION: REGEX DEFINITIONS =====
# Regex for parsing parameters in _parse_params
PARAM_REGEX = re.compile(
    r"""
    \s*                         # Leading whitespace
    (?:(IN|OUT|INOUT)\s+)?      # Optional mode (Group 1)
    ([a-zA-Z0-9_]+)             # Parameter name (Group 2)
    \s+                         # Whitespace after name
    (.*?)                       # Parameter type (Group 3) - Non-greedy
    # Optional Default clause (Group 4 is "DEFAULT", Group 5 is the value) or end of string
    (?:\s+(DEFAULT)\s+(.+?))?   # DEFAULT (Group 4) and its value (Group 5)
    \s*$                        # Trailing whitespace and end of string
    """,
    re.IGNORECASE | re.VERBOSE,
)

# ===== SECTION: FUNCTIONS =====


def parse_single_param_definition(
    param_def: str,
    context: str,
    enum_types: dict[str, list[str]] | None = None,
    table_schemas: dict[str, list] | None = None,
    composite_types: dict[str, list] | None = None,
) -> tuple[SQLParameter, set[str]] | None:
    """
    Parses a single parameter definition string. Returns SQLParameter and its imports, or None.

    Args:
        param_def (str): The parameter definition string
        context (str): Context for error reporting
        enum_types (Dict[str, List[str]], optional): Dictionary of enum types
        table_schemas (Dict[str, List], optional): Dictionary of table schemas
        composite_types (Dict[str, List], optional): Dictionary of composite types

    Returns:
        Optional[Tuple[SQLParameter, Set[str]]]: The parsed parameter and its imports, or None if parsing failed
    """
    match = PARAM_REGEX.match(param_def)

    if not match:
        # Cannot parse this fragment as a standalone parameter
        # Recovery for split types (like numeric(10,2)) is handled in the caller
        return None

    sql_name = match.group(2).strip()
    sql_type = match.group(3).strip()

    default_keyword = match.group(4)  # "DEFAULT" or None
    default_value_str = match.group(5)  # The actual default value string or None

    is_optional = bool(default_keyword)
    has_sql_default = False

    if default_keyword and default_value_str:
        normalized_default_value = default_value_str.strip().lower()
        # Check if it's a non-NULL SQL default
        if normalized_default_value != "null":
            has_sql_default = True
    # Note: is_optional remains True for any DEFAULT, including DEFAULT NULL.
    # has_sql_default is True only for DEFAULT <non-NULL value>.

    # Generate Pythonic name
    python_name = sql_name
    if python_name.startswith("p_") and len(python_name) > 2:
        python_name = python_name[2:]
    elif python_name.startswith("_") and len(python_name) > 1:
        python_name = python_name[1:]

    # Map SQL type to Python type
    param_context = f"parameter '{sql_name}'" + (f" in {context}" if context else "")
    try:
        py_type, imports = map_sql_to_python_type(
            sql_type, is_optional, param_context, enum_types, table_schemas, composite_types
        )
    except Exception as e:
        logging.warning(str(e))
        py_type = "Any" if not is_optional else "Optional[Any]"
        imports = {"Any", "Optional"} if is_optional else {"Any"}

    param = SQLParameter(
        name=sql_name,
        python_name=python_name,
        sql_type=sql_type,
        python_type=py_type,
        is_optional=is_optional,
        has_sql_default=has_sql_default,
    )
    return param, imports


def parse_params(
    param_str: str,
    context: str | None = None,
    enum_types: dict[str, list[str]] | None = None,
    table_schemas: dict[str, list] | None = None,
    composite_types: dict[str, list] | None = None,
) -> tuple[list[SQLParameter], set[str]]:
    """
    Parses parameter string including optional DEFAULT values.
    Uses a helper to parse individual definitions.

    Args:
        param_str (str): The parameter string
        context (str, optional): Context for error reporting
        enum_types (Dict[str, List[str]], optional): Dictionary of enum types
        table_schemas (Dict[str, List], optional): Dictionary of table schemas
        composite_types (Dict[str, List], optional): Dictionary of composite types

    Returns:
        Tuple[List[SQLParameter], Set[str]]: The parsed parameters and their imports
    """
    params = []
    required_imports = set()
    if not param_str:
        return params, required_imports

    # Split by comma first
    param_defs = param_str.split(",")

    current_context = f"function '{context}'" if context else "unknown function"

    for param_def in param_defs:
        param_def = param_def.strip()
        if not param_def:
            continue

        # Attempt to parse the fragment using the helper
        parse_result = parse_single_param_definition(
            param_def, current_context, enum_types, table_schemas, composite_types
        )

        if parse_result:
            param, imports = parse_result
            params.append(param)
            required_imports.update(imports)
        # If helper failed, check for recovery case (split type)
        elif params and ")" not in params[-1].sql_type and ")" in param_def:
            param_context_recovery = f"parameter '{params[-1].name}' in {current_context}"
            logging.debug(
                f"Attempting recovery for split inside type: appending '{param_def}' to {param_context_recovery}"
            )
            params[-1].sql_type += "," + param_def
            # Re-run type mapping for the corrected type
            try:
                py_type, imports = map_sql_to_python_type(
                    params[-1].sql_type,
                    params[-1].is_optional,
                    param_context_recovery,
                    enum_types,
                    table_schemas,
                    composite_types,
                )
                params[-1].python_type = py_type
                required_imports.update(imports)
            except Exception as e:
                logging.warning(str(e))
            # Continue to next fragment after recovery attempt
        else:
            # If not a recovery case, log warning for unparseable fragment
            error_msg = f"Could not parse parameter definition fragment: {param_def}"
            logging.warning(f"{error_msg} in {current_context}")
            # Optionally, could add a placeholder parameter or raise error

    return params, required_imports
