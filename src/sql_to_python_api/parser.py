import re
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Tuple, Any
import logging

# Basic PostgreSQL to Python type mapping
TYPE_MAP = {
    "uuid": "UUID",
    "text": "str",
    "varchar": "str",
    "integer": "int",
    "int": "int",
    "bigint": "int",
    "smallint": "int",
    "boolean": "bool",
    "bool": "bool",
    "timestamp": "datetime",
    "timestamptz": "datetime", # Often preferred
    "date": "date",
    "numeric": "Decimal",
    "decimal": "Decimal",
    "json": "dict", # Or Any, depending on usage
    "jsonb": "dict", # Or Any
    # Add more mappings as needed
}

PYTHON_IMPORTS = {
    "UUID": "from uuid import UUID",
    "datetime": "from datetime import datetime, date",
    "date": "from datetime import datetime, date", # Ensure datetime is also imported if date is used
    "Decimal": "from decimal import Decimal",
    "Any": "from typing import Any", # Import for Any
    "List": "from typing import List", # Import for List
    "Dict": "from typing import Dict", # Import for Dict
    "Tuple": "from typing import Tuple", # Import for Tuple
}

class SQLParsingError(Exception):
    """Custom exception for parsing errors."""
    pass

@dataclass
class SQLParameter:
    name: str
    sql_type: str
    python_type: str

@dataclass
class ReturnColumn:
    name: str
    sql_type: str
    python_type: str

@dataclass
class ParsedFunction:
    sql_name: str
    python_name: str
    params: List[SQLParameter] = field(default_factory=list)
    return_type: str = "None"  # Python type hint for scalar return
    return_columns: List[ReturnColumn] = field(default_factory=list) # For RETURNS TABLE
    returns_table: bool = False
    returns_record: bool = False # Simple record, treat as tuple or dict
    returns_setof: bool = False # For SETOF scalar types
    required_imports: set = field(default_factory=set)

def _map_sql_to_python_type(sql_type: str) -> Tuple[str, Optional[str]]:
    """Maps SQL type to Python type and returns required import."""
    sql_type_lower = sql_type.lower().strip().split('(')[0] # Handle varchar(n), numeric(p,s) etc.
    is_array = sql_type_lower.endswith('[]')
    if is_array:
        sql_type_lower = sql_type_lower[:-2] # Remove [] for mapping

    py_type = TYPE_MAP.get(sql_type_lower, "Any") # Default to Any if unknown
    import_stmt = PYTHON_IMPORTS.get(py_type)

    if is_array:
        py_type = f"List[{py_type}]"
        # Ensure List import is added if an array is detected
        list_import = PYTHON_IMPORTS.get("List")
        # Return both the base type import and the List import if needed
        combined_import = f"{import_stmt}\n{list_import}" if import_stmt and list_import else list_import
        return py_type, combined_import
    else:
        return py_type, import_stmt


def _parse_params(param_str: str) -> Tuple[List[SQLParameter], set]:
    """Parses parameter string like 'user_id UUID, name TEXT'."""
    params = []
    required_imports = set()
    if not param_str:
        return params, required_imports

    # Regex to capture 'param_name type' pairs, handling potential whitespace and commas
    # Handles optional parameter prefixes like IN, OUT, INOUT
    param_regex = re.compile(r"(?:IN|OUT|INOUT)?\s*([a-zA-Z0-9_]+)\s+([a-zA-Z0-9_.()]+(?:\s*\[\])?)\s*,?")

    for match in param_regex.finditer(param_str):
        name = match.group(1).strip()
        sql_type = match.group(2).strip()
        py_type, import_stmt = _map_sql_to_python_type(sql_type)

        if import_stmt:
            # Handle potential combined imports (like for arrays)
            for imp in import_stmt.split('\n'):
                if imp:
                    required_imports.add(imp)

        params.append(SQLParameter(name=name, sql_type=sql_type, python_type=py_type))

    return params, required_imports

def _parse_return_columns(columns_str: str) -> Tuple[List[ReturnColumn], set]:
    """Parses return columns string like 'id UUID, name TEXT'."""
    columns = []
    required_imports = set()
    if not columns_str:
        return columns, required_imports

    # Regex similar to parameters
    col_regex = re.compile(r"([a-zA-Z0-9_]+)\s+([a-zA-Z0-9_.()]+(?:\s*\[\])?)\s*,?")
    for match in col_regex.finditer(columns_str):
        name = match.group(1).strip()
        sql_type = match.group(2).strip()
        py_type, import_stmt = _map_sql_to_python_type(sql_type)

        if import_stmt:
            for imp in import_stmt.split('\n'):
                if imp:
                    required_imports.add(imp)

        columns.append(ReturnColumn(name=name, sql_type=sql_type, python_type=py_type))
    return columns, required_imports


def parse_sql(sql_content: str) -> List[ParsedFunction]:
    """
    Parses a string containing one or more SQL CREATE FUNCTION statements.
    Returns a list of ParsedFunction objects.
    """
    functions = []
    # Remove block comments /* ... */
    sql_content = re.sub(r"/\*.*?\*/", "", sql_content, flags=re.DOTALL)
    # Remove single line comments -- ...
    sql_content = re.sub(r"--.*?\n", "", sql_content)

    # Regex to find CREATE FUNCTION blocks
    # Captures: 1=function_name, 2=parameters, 3=SETOF, 4=TABLE clause, 5=TABLE columns, 6=Scalar Type
    function_regex = re.compile(
        r"CREATE(?:\s+OR\s+REPLACE)?\s+FUNCTION\s+([a-zA-Z0-9_.]+)"  # 1: Function name
        r"\s*\(([^)]*)\)"  # 2: Parameters (optional content)
        r"\s+RETURNS\s+(SETOF\s+)?(?:(TABLE\s*\((.*?)\))|([a-zA-Z0-9_.()\[\]]+))" # 3: SETOF?, 4: TABLE(..), 5: TABLE cols, 6: Scalar
        r".*?" # Non-greedy match for intermediate keywords (LANGUAGE, AS etc.)
        r"AS\s+(?:\$\$.*?\$\$|'.*?')", # Match body defined by $$ or '
        re.IGNORECASE | re.DOTALL | re.MULTILINE
    )

    for match in function_regex.finditer(sql_content):
        sql_name = match.group(1).strip()
        param_str = match.group(2).strip()
        returns_setof = bool(match.group(3)) # Check if SETOF was present
        table_clause = match.group(4) # Full "TABLE (...)"
        table_columns_str = match.group(5) # Content inside TABLE (...)
        scalar_return_type = match.group(6) # Scalar type if not TABLE

        logging.info(f"Parsing function: {sql_name}")

        try:
            params, param_imports = _parse_params(param_str)
            required_imports = param_imports.copy()
            # Add base imports needed for any generated function
            required_imports.add("from psycopg import AsyncConnection")
            required_imports.add("from typing import Optional, List, Any, Tuple, Dict")


            func = ParsedFunction(sql_name=sql_name, python_name=sql_name) # Simple name mapping for now
            func.params = params
            func.returns_setof = returns_setof

            if table_clause and table_columns_str is not None:
                logging.debug(f"  -> Returns TABLE: {table_columns_str}")
                return_cols, col_imports = _parse_return_columns(table_columns_str)
                func.return_columns = return_cols
                func.returns_table = True
                required_imports.update(col_imports)
                required_imports.add("from dataclasses import dataclass")
            elif scalar_return_type:
                scalar_return_type_clean = scalar_return_type.strip()
                if scalar_return_type_clean.lower() == 'record':
                    logging.debug(f"  -> Returns RECORD")
                    func.returns_record = True
                    func.return_type = "Tuple" # Represent nameless record as Tuple
                    # Tuple import added by default now
                elif scalar_return_type_clean.lower() == 'void':
                    logging.debug(f"  -> Returns VOID")
                    func.return_type = "None"
                else:
                    logging.debug(f"  -> Returns SCALAR: {scalar_return_type_clean}")
                    py_return_type, ret_import = _map_sql_to_python_type(scalar_return_type_clean)
                    func.return_type = py_return_type
                    if ret_import:
                        for imp in ret_import.split('\n'):
                            if imp:
                                required_imports.add(imp)
            else:
                 logging.warning(f"Could not determine return type for function {sql_name}. Assuming None.")
                 func.return_type = "None"

            if returns_setof:
                # List import added by default now
                 if not func.returns_table and not func.return_type.startswith("List["):
                     # Wrap scalar return types in List for SETOF
                     if func.return_type != "None": # Don't wrap None
                        func.return_type = f"List[{func.return_type}]"
                 # If it returns TABLE, the generator handles List[Dataclass]

            # Filter out None from imports before assigning
            func.required_imports = {imp for imp in required_imports if imp}
            functions.append(func)

        except Exception as e:
            logging.exception(f"Failed to parse function '{sql_name}'. Skipping.") # Use logging.exception for traceback
            # raise SQLParsingError(f"Failed to parse function '{sql_name}': {e}") from e
            continue # Skip this function and try the next

    if not functions and sql_content.strip():
         logging.warning("No CREATE FUNCTION statements found in the provided SQL content.")
         # Consider if an error should be raised if no functions are found
         # raise SQLParsingError("No CREATE FUNCTION statements found.")

    return functions 