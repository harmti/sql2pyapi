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
    is_optional: bool = False # Added flag for default values

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

def _map_sql_to_python_type(sql_type: str, is_optional: bool = False) -> Tuple[str, Optional[str]]:
    """Maps SQL type to Python type and returns required import. Wraps with Optional if needed."""
    sql_type_lower = sql_type.lower().strip().split('(')[0] # Handle varchar(n), numeric(p,s) etc.
    is_array = sql_type_lower.endswith('[]')
    if is_array:
        sql_type_lower = sql_type_lower[:-2] # Remove [] for mapping

    py_type = TYPE_MAP.get(sql_type_lower, "Any") # Default to Any if unknown
    import_stmt = PYTHON_IMPORTS.get(py_type)
    combined_imports = {import_stmt} if import_stmt else set()

    if is_array:
        py_type = f"List[{py_type}]"
        list_import = PYTHON_IMPORTS.get("List")
        if list_import: combined_imports.add(list_import)
    
    if is_optional and py_type != "Any":
        py_type = f"Optional[{py_type}]"
        combined_imports.add("from typing import Optional")

    # Return combined imports as a newline-separated string or None
    final_imports_str = "\n".join(filter(None, sorted(list(combined_imports))))
    return py_type, final_imports_str if final_imports_str else None

def _parse_params(param_str: str) -> Tuple[List[SQLParameter], set]:
    """Parses parameter string including optional DEFAULT values."""
    params = []
    required_imports = set()
    if not param_str:
        return params, required_imports

    # Regex to capture: 
    # 1: Optional IN/OUT/INOUT prefix
    # 2: Parameter name
    # 3: Parameter type (including arrays like TEXT[])
    # 4: Optional DEFAULT clause (non-capturing)
    param_regex = re.compile(
        r"(?:\s*(IN|OUT|INOUT)\s+)?"  # 1: Optional mode
        r"([a-zA-Z0-9_]+)"           # 2: Parameter name
        r"\s+([a-zA-Z0-9_.()\[\]]+)"  # 3: Parameter type
        r"(?:\s+DEFAULT\s+.*?)?"    # 4: Optional DEFAULT clause (non-capturing the value)
        r"(?:\s*,|\s*$)",            # Match comma or end of string
        re.IGNORECASE
    )
    
    # Need to track position to detect DEFAULT correctly for subsequent matches
    last_pos = 0
    for match in param_regex.finditer(param_str):
        # Check if the match starts where the last one ended (or at the beginning)
        # This helps avoid matching parts *within* a DEFAULT clause if the regex is too loose
        if match.start() < last_pos:
             continue
        last_pos = match.end()

        name = match.group(2).strip()
        sql_type = match.group(3).strip()
        
        # Check if the original segment contained 'DEFAULT' to mark as optional
        original_segment = param_str[match.start():match.end()]
        is_optional = "default" in original_segment.lower()
        
        py_type, import_stmts = _map_sql_to_python_type(sql_type, is_optional)

        if import_stmts:
            for imp in import_stmts.split('\n'):
                if imp:
                    required_imports.add(imp)

        params.append(SQLParameter(name=name, sql_type=sql_type, python_type=py_type, is_optional=is_optional))

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
        # Return columns are never optional in this context
        py_type, import_stmt = _map_sql_to_python_type(sql_type, is_optional=False)

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
    # Captures: 1=function_name, 2=parameters, 3=SETOF, 4=TABLE clause, 5=TABLE columns, 6=Scalar Type/table_name
    function_regex = re.compile(
        r"CREATE(?:\s+OR\s+REPLACE)?\s+FUNCTION\s+([a-zA-Z0-9_.]+)"  # 1: Function name
        r"\s*\(([^)]*)\)"  # 2: Parameters (optional content)
        # Match RETURNS SETOF table_name OR RETURNS TABLE(...) OR RETURNS scalar_type
        r"\s+RETURNS\s+(?:(SETOF)\s+)?(?:(TABLE\s*\((.*?)\))|([a-zA-Z0-9_.()\[\]]+))" # 3: SETOF?, 4: TABLE(..), 5: TABLE cols, 6: Scalar/table_name
        r".*?" # Non-greedy match for intermediate keywords (LANGUAGE, AS etc.)
        r"(?:AS|LANGUAGE)\s+(?:\$\$.*?\$\$|'.*?')", # Match body defined by $$ or ' (stop before AS/LANGUAGE)
        re.IGNORECASE | re.DOTALL | re.MULTILINE
    )

    for match in function_regex.finditer(sql_content):
        sql_name = match.group(1).strip()
        param_str = match.group(2).strip()
        returns_setof = bool(match.group(3)) # Check if SETOF was present
        table_clause = match.group(4) # Full "TABLE (...)"
        table_columns_str = match.group(5) # Content inside TABLE (...)
        scalar_or_table_name_return = match.group(6) # Scalar type OR table name if SETOF

        logging.info(f"Parsing function: {sql_name}")

        try:
            params, param_imports = _parse_params(param_str)
            required_imports = param_imports.copy()
            # Add base imports needed for any generated function
            required_imports.add("from psycopg import AsyncConnection")
            # Base typing imports added conditionally later or via type mapping

            func = ParsedFunction(sql_name=sql_name, python_name=sql_name)
            func.params = params
            func.returns_setof = returns_setof

            if table_clause and table_columns_str is not None:
                # Case: RETURNS TABLE(...) or RETURNS SETOF TABLE(...)
                logging.debug(f"  -> Returns TABLE definition: {table_columns_str}")
                return_cols, col_imports = _parse_return_columns(table_columns_str)
                func.return_columns = return_cols
                func.returns_table = True # Mark that it uses an explicit TABLE definition
                required_imports.update(col_imports)
                required_imports.add("from dataclasses import dataclass")
            elif scalar_or_table_name_return:
                return_type_str = scalar_or_table_name_return.strip()
                # Check common specific types first
                if return_type_str.lower() == 'record':
                    logging.debug(f"  -> Returns RECORD")
                    func.returns_record = True
                    func.return_type = "Tuple" # Represent nameless record as Tuple
                    required_imports.add(PYTHON_IMPORTS["Tuple"])
                elif return_type_str.lower() == 'void':
                    logging.debug(f"  -> Returns VOID")
                    func.return_type = "None"
                else:
                    # Case: RETURNS scalar OR RETURNS SETOF scalar OR RETURNS SETOF table_name
                    # Try mapping as a scalar type first
                    py_return_type, ret_import = _map_sql_to_python_type(return_type_str)
                    
                    if py_return_type != "Any" or not returns_setof:
                        # Treat as scalar if mapping is known, OR if it's not SETOF
                        logging.debug(f"  -> Returns SCALAR: {return_type_str}")
                        func.return_type = py_return_type
                        if ret_import:
                            for imp in ret_import.split('\n'):
                                if imp: required_imports.add(imp)
                    else:
                        # Case: RETURNS SETOF unknown_type (assume table name)
                        # This is where we'd ideally look up the table structure.
                        # For now, represent as SETOF Any, requiring a dataclass named after the type.
                        logging.warning(f"  -> Returns SETOF {return_type_str}. Assuming it's a table name. Mapping to List[Any]. Define dataclass '{return_type_str.capitalize()}' manually or enhance parser.")
                        func.return_type = "Any" # Base type is Any
                        func.returns_table = True # Treat like table return for generation (needs manual dataclass)
                        func.return_columns = [ReturnColumn(name="unknown", sql_type=return_type_str, python_type="Any")] # Placeholder
                        required_imports.add(PYTHON_IMPORTS["Any"])
                        required_imports.add("from dataclasses import dataclass") # Assume dataclass needed
            else:
                 logging.warning(f"Could not determine return type for function {sql_name}. Assuming None.")
                 func.return_type = "None"

            # Add List/Optional imports based on final determination
            if returns_setof:
                 required_imports.add(PYTHON_IMPORTS["List"])
                 # Wrap scalar return types in List for SETOF (if not already List)
                 if not func.returns_table and func.return_type not in ["None", "Any"] and not func.return_type.startswith("List"):
                     func.return_type = f"List[{func.return_type}]"
                 # For SETOF TABLE or SETOF table_name, generator handles List[DataclassName]
            elif func.return_type != "None":
                 # Non-SETOF functions returning something should be Optional
                 required_imports.add("from typing import Optional")
                 if not func.return_type.startswith("Optional"):
                      func.return_type = f"Optional[{func.return_type}]"
            
            # Add base typing imports if any complex types were used
            if any(t in func.return_type for t in ["Optional", "List", "Tuple", "Dict", "Any"]):
                 required_imports.add("from typing import Optional, List, Any, Tuple, Dict")
                 
            # Filter out None from imports before assigning
            func.required_imports = {imp for imp in required_imports if imp}
            functions.append(func)

        except Exception as e:
            logging.exception(f"Failed to parse function '{sql_name}'. Skipping.")
            continue

    if not functions and sql_content.strip():
         logging.warning("No CREATE FUNCTION statements found in the provided SQL content.")

    return functions 