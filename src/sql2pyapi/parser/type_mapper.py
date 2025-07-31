# ===== SECTION: IMPORTS =====
import logging
import re

# Import custom error classes
# Import the type mapping constants
from ..sql_models import TYPE_MAP


# ===== SECTION: FUNCTIONS =====


def map_sql_to_python_type(
    sql_type: str,
    is_optional: bool = False,
    context: str | None = None,
    enum_types: dict[str, list[str]] | None = None,
    table_schemas: dict[str, list] | None = None,
    composite_types: dict[str, list] | None = None,
) -> tuple[str, set[str]]:
    """
    Maps a SQL type to its corresponding Python type and required imports.
    Refined logic to handle types with precision/qualifiers.

    Args:
        sql_type (str): The PostgreSQL type to map
        is_optional (bool): Whether the type should be wrapped in Optional
        context (str, optional): Context information for error reporting
        enum_types (Dict[str, List[str]], optional): Dictionary of enum types
        table_schemas (Dict[str, List], optional): Dictionary of table schemas
        composite_types (Dict[str, List], optional): Dictionary of composite types

    Returns:
        Tuple[str, Set[str]]: The Python type and a set of required imports

    Raises:
        TypeMappingError: If the SQL type cannot be mapped to a Python type
    """
    # Default empty dictionaries if not provided
    enum_types = enum_types or {}
    table_schemas = table_schemas or {}
    composite_types = composite_types or {}

    # --- Check for ENUM Type ---
    if sql_type in enum_types:
        # Convert enum_name to PascalCase for Python Enum class name
        enum_name = "".join(word.capitalize() for word in sql_type.split("_"))
        imports = {"Enum"}

        # Add Optional wrapper if explicitly requested
        if is_optional:
            py_type = f"Optional[{enum_name}]"
            imports.add("Optional")
        else:
            py_type = enum_name

        return py_type, imports

    # --- Initial Check: Table Schema Reference ---
    if sql_type in table_schemas or ("." not in sql_type and sql_type.split(".")[-1] in table_schemas):
        # Import the conversion function
        from .utils import _to_singular_camel_case

        # Convert table name to dataclass name
        dataclass_name = _to_singular_camel_case(sql_type)

        # Add Optional wrapper if explicitly requested
        if is_optional:
            py_type = f"Optional[{dataclass_name}]"
            imports = {"Optional"}
        else:
            py_type = dataclass_name
            imports = set()

        return py_type, imports

    # --- Initialize imports set ---
    imports = set()

    # --- Normalization and Array Handling ---
    sql_type_normal = sql_type.lower().strip()
    is_array = False
    if sql_type_normal.endswith("[]"):
        is_array = True
        sql_type_no_array = sql_type_normal[:-2].strip()
    else:
        sql_type_no_array = sql_type_normal

    # --- Specific Handling for Timestamps with Precision ---
    # Remove `(N)` before looking up complex timestamp types
    if sql_type_no_array.startswith("timestamp("):
        sql_type_no_array = re.sub(r"^timestamp\(\d+\)", "timestamp", sql_type_no_array)

    # --- Type Lookup Strategy ---
    py_type = None

    # 1. Try exact match on the normalized type (potentially without precision for timestamps)
    py_type = TYPE_MAP.get(sql_type_no_array)

    # 2. If no exact match, try stripping general precision/length specifiers `(...)`
    if not py_type:
        base_type_no_precision = re.sub(r"\(.*\)", "", sql_type_no_array).strip()
        if base_type_no_precision != sql_type_no_array:
            py_type = TYPE_MAP.get(base_type_no_precision)

    # 3. If still no match, try splitting on the *first* space or parenthesis
    if not py_type:
        lookup_type_for_split = (
            base_type_no_precision
            if "base_type_no_precision" in locals() and base_type_no_precision != sql_type_no_array
            else sql_type_no_array
        )
        potential_base_type_split = re.split(r"[\s(]", lookup_type_for_split, maxsplit=1)[0]
        if potential_base_type_split != lookup_type_for_split:
            py_type = TYPE_MAP.get(potential_base_type_split)

    # --- Check Custom Types (ENUM, Table, Composite) Before Fallback ---
    if not py_type:
        # Check for ENUM type (using the base type without array suffix)
        if sql_type_no_array in enum_types:
            # Convert enum_name to PascalCase for Python Enum class name
            py_type = "".join(word.capitalize() for word in sql_type_no_array.split("_"))
            imports.add("Enum")

        # Check for Composite Type Reference (using the base type without array suffix)
        elif sql_type_no_array in composite_types or (
            "." not in sql_type_no_array and sql_type_no_array.split(".")[-1] in composite_types
        ):
            # Import the conversion function
            from .utils import _to_singular_camel_case

            # Convert composite type name to dataclass name
            py_type = _to_singular_camel_case(sql_type_no_array)

        # Check for Table Schema Reference (using the base type without array suffix)
        elif sql_type_no_array in table_schemas or (
            "." not in sql_type_no_array and sql_type_no_array.split(".")[-1] in table_schemas
        ):
            # Import the conversion function
            from .utils import _to_singular_camel_case

            # Convert table name to dataclass name
            py_type = _to_singular_camel_case(sql_type_no_array)

    # --- Fallback and Logging ---
    if not py_type:
        error_msg = f"Unknown SQL type: {sql_type}"
        if context:
            error_msg += f" in {context}"
        logging.warning(f"{error_msg}. Using 'Any' as fallback.")
        py_type = "Any"

    # --- Import Handling ---
    if py_type == "UUID":
        imports.add("UUID")
    elif py_type == "datetime":
        imports.add("datetime")
    elif py_type == "date":
        imports.add("date")
    elif py_type == "timedelta":
        imports.add("timedelta")
    elif py_type == "Decimal":
        imports.add("Decimal")
    elif py_type == "Any":
        imports.add("Any")
    elif py_type in {"dict", "Dict[str, Any]"}:
        imports.add("Dict")
        imports.add("Any")
        py_type = "Dict[str, Any]"

    # --- Array Wrapping ---
    if is_array:
        py_type = f"List[{py_type}]"
        imports.add("List")

    # --- Optional Wrapping ---
    if is_optional and py_type != "Any" and not py_type.startswith("Optional["):
        py_type = f"Optional[{py_type}]"
        imports.add("Optional")

    return py_type, imports
