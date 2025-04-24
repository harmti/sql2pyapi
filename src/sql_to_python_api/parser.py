import re
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Tuple, Any
import logging

# --- Type Maps and Imports --- (Keep these first)
# Basic PostgreSQL to Python type mapping
TYPE_MAP = {
    "uuid": "UUID",
    "text": "str",
    "varchar": "str",
    "character varying": "str", # Add alias
    "integer": "int",
    "int": "int",
    "bigint": "int", # Consider using int in Python 3, as it has arbitrary precision
    "smallint": "int",
    "boolean": "bool",
    "bool": "bool",
    "timestamp": "datetime",
    "timestamp without time zone": "datetime",
    "timestamptz": "datetime", # Often preferred
    "timestamp with time zone": "datetime",
    "date": "date",
    "numeric": "Decimal",
    "decimal": "Decimal",
    "json": "dict", # Or Any, depending on usage
    "jsonb": "dict", # Or Any
    "bytea": "bytes",
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

# --- Data Structures --- (Define these BEFORE globals)
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
    # New field to store the base table name for SETOF table_name returns
    setof_table_name: Optional[str] = None 

# --- Global Schema Storage --- (Define globals AFTER structures)
TABLE_SCHEMAS: Dict[str, List[ReturnColumn]] = {}
TABLE_SCHEMA_IMPORTS: Dict[str, set] = {}

def _map_sql_to_python_type(sql_type: str, is_optional: bool = False) -> Tuple[str, Optional[str]]:
    """Maps SQL type to Python type and returns required import. Wraps with Optional if needed."""
    sql_type_normal = sql_type.lower().strip()
    # Split on whitespace, parenthesis, or square brackets to get the base type
    sql_type_base = re.split(r'[\s(\[]', sql_type_normal, 1)[0]
    is_array = sql_type_normal.endswith('[]')

    py_type = TYPE_MAP.get(sql_type_base, "Any")
    import_stmt = PYTHON_IMPORTS.get(py_type)
    combined_imports = {import_stmt} if import_stmt else set()

    if is_array:
        py_type = f"List[{py_type}]"
        list_import = PYTHON_IMPORTS.get("List")
        if list_import: combined_imports.add(list_import)
    
    if is_optional and py_type != "Any":
        # Only wrap non-array types with Optional here; array types are handled by [] or List[]
        # Or assume default NULL means optional even for arrays? For now, only non-arrays.
        if not is_array:
             py_type = f"Optional[{py_type}]"
             combined_imports.add("from typing import Optional")

    final_imports_str = "\n".join(filter(None, sorted(list(combined_imports))))
    return py_type, final_imports_str if final_imports_str else None

def _parse_column_definitions(col_defs_str: str) -> Tuple[List[ReturnColumn], set]:
    """Parses column definitions from CREATE TABLE or RETURNS TABLE."""
    columns = []
    required_imports = set()
    if not col_defs_str:
        return columns, required_imports

    # Split definition block into individual lines, easier to process
    lines = col_defs_str.splitlines()
    
    terminating_keywords = {
        'primary', 'unique', 'not', 'null', 
        'references', 'check', 'collate', 
        'default', 'generated', 'constraint'
    }

    for line in lines:
        line = line.strip()
        if not line or line.lower().startswith(('constraint', 'primary key', 'foreign key', 'unique', 'check', 'like')):
            continue # Skip constraint definitions or LIKE clauses
        
        # Remove trailing comma if present
        if line.endswith(','):
            line = line[:-1].strip()
            
        parts = line.split() # Split by whitespace
        if len(parts) < 2:
            continue 
            
        col_name = parts[0].strip('"')
        
        # Accumulate type parts, stopping at keywords
        type_parts = []
        for i in range(1, len(parts)):
            part = parts[i]
            part_lower = part.lower()
            
            is_terminator = False
            for keyword in terminating_keywords:
                # Check if the part *is* or *starts with* a keyword
                # (Handle cases like `DEFAULT` vs `DEFAULT 'value'`)
                if part_lower == keyword or part_lower.startswith(keyword + '('): 
                    is_terminator = True
                    break
            
            if is_terminator:
                break 
            type_parts.append(part) 
        
        if not type_parts:
             # This might happen for lines defining constraints inline but not starting with keyword
             # e.g., `col INT PRIMARY KEY` - we only want name/type here
             if len(parts) >= 2:
                  # Assume second part might be the type if first isn't constraint
                  if parts[0].lower() not in terminating_keywords and parts[1].lower() not in terminating_keywords: 
                      sql_type = parts[1]
                  else: continue # Skip likely constraint line
             else: continue # Skip short lines
             logging.debug(f"Trying fallback type parsing for column '{col_name}' in definition: '{line}'")
        else:
            sql_type = " ".join(type_parts)
        
        # Clean up type string (e.g., remove trailing precision info if not needed for mapping)
        sql_type_cleaned = sql_type.split('(')[0].strip() 

        py_type, import_stmt = _map_sql_to_python_type(sql_type, is_optional=False) # is_optional not relevant here

        if import_stmt:
            for imp in import_stmt.split('\n'):
                if imp:
                    required_imports.add(imp)
        columns.append(ReturnColumn(name=col_name, sql_type=sql_type, python_type=py_type))

    if not columns and col_defs_str.strip():
        logging.warning(f"Could not parse any columns from CREATE TABLE block: '{col_defs_str[:100]}...'")
        
    return columns, required_imports

def _parse_create_table(sql_content: str):
    """Finds and parses CREATE TABLE statements, storing schemas globally."""
    # Simpler regex: Find CREATE TABLE name (...) ; - less prone to backtracking
    # Might capture slightly too much in complex cases, relies on _parse_column_definitions robustness
    table_regex = re.compile(
        r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([a-zA-Z0-9_.]+)" # 1: Table name
        r"\s*\(" # Opening parenthesis
        r"(.*?)"   # 2: Everything inside parenthesis (non-greedy)
        r"\)\s*(?:INHERITS|WITH|TABLESPACE|;)", # Stop at known clauses after ) or semicolon
        re.IGNORECASE | re.DOTALL | re.MULTILINE
    )

    for match in table_regex.finditer(sql_content):
        table_name = match.group(1).strip()
        col_defs_str = match.group(2).strip()
        
        # Further clean column defs: remove comments that might span lines within the block
        col_defs_str_cleaned = re.sub(r"--.*?$", "", col_defs_str, flags=re.MULTILINE)
        col_defs_str_cleaned = re.sub(r"/\*.*?\*/", "", col_defs_str_cleaned, flags=re.DOTALL)
        col_defs_str_cleaned = "\n".join(line.strip() for line in col_defs_str_cleaned.splitlines() if line.strip())

        logging.info(f"Found CREATE TABLE for: {table_name}")
        
        try:
            columns, required_imports = _parse_column_definitions(col_defs_str_cleaned)
            if columns:
                 # Use normalized name (remove schema if present) for storage key?
                 normalized_table_name = table_name.split('.')[-1]
                 TABLE_SCHEMAS[normalized_table_name] = columns 
                 TABLE_SCHEMA_IMPORTS[normalized_table_name] = required_imports
                 logging.debug(f"  -> Parsed {len(columns)} columns for table {normalized_table_name} (from {table_name})")
            else:
                 logging.warning(f"  -> No columns parsed for table {table_name} from definition: '{col_defs_str_cleaned[:100]}...'")
        except Exception as e:
             logging.exception(f"Failed to parse columns for table '{table_name}'. Skipping.")
             continue

def _parse_params(param_str: str) -> Tuple[List[SQLParameter], set]:
    """Parses parameter string including optional DEFAULT values."""
    params = []
    required_imports = set()
    if not param_str:
        return params, required_imports

    param_regex = re.compile(
        r"\s*(?:(?:IN|OUT|INOUT)\s+)?([a-zA-Z0-9_]+)\s+([a-zA-Z0-9_.()\[\]]+(?:(?:\s*\(.*?\))?))" # Name and Type (incl precision/array)
        r"(.*)", # Capture the rest (potential default clause)
        re.IGNORECASE
    )
    
    # Split parameters by comma first
    param_defs = param_str.split(',')
    
    for param_def in param_defs:
        param_def = param_def.strip()
        if not param_def: continue

        match = param_regex.match(param_def)
        if not match:
             logging.warning(f"Could not parse parameter definition: {param_def}")
             continue
             
        name = match.group(1).strip()
        sql_type = match.group(2).strip()
        remainder = match.group(3).strip()
        
        is_optional = "default" in remainder.lower()
        
        py_type, import_stmts = _map_sql_to_python_type(sql_type, is_optional)

        if import_stmts:
            for imp in import_stmts.split('\n'):
                if imp:
                    required_imports.add(imp)

        params.append(SQLParameter(name=name, sql_type=sql_type, python_type=py_type, is_optional=is_optional))

    return params, required_imports

def parse_sql(sql_content: str, schema_content: Optional[str] = None) -> Tuple[List[ParsedFunction], Dict[str, set]]:
    """
    Parses SQL content, optionally using a separate schema file.
    
    Args:
        sql_content: String containing CREATE FUNCTION statements (and potentially CREATE TABLE).
        schema_content: Optional string containing CREATE TABLE statements.

    Returns:
        A tuple containing:
          - list of ParsedFunction objects.
          - dictionary mapping table names to required imports for their schemas.
    """
    global TABLE_SCHEMAS, TABLE_SCHEMA_IMPORTS
    TABLE_SCHEMAS = {} 
    TABLE_SCHEMA_IMPORTS = {} 
    
    functions = []
    
    # --- Pre-process both inputs --- 
    def preprocess_sql(content: Optional[str]) -> str:
        if not content: return ""
        processed = re.sub(r"/\*.*?\*/", "", content, flags=re.DOTALL)
        processed = re.sub(r"--.*?$", "", processed, flags=re.MULTILINE)
        return "\n".join(line for line in processed.splitlines() if line.strip())

    processed_schema_sql = preprocess_sql(schema_content)
    processed_function_sql = preprocess_sql(sql_content)

    # --- First Pass: Parse CREATE TABLE (from schema file first, then function file) --- 
    if processed_schema_sql:
        logging.info("Parsing CREATE TABLE statements from schema file...")
        _parse_create_table(processed_schema_sql)
    # Also parse tables from the main file in case they are defined there
    logging.info("Parsing CREATE TABLE statements from main functions file...")
    _parse_create_table(processed_function_sql)
    
    # --- Second Pass: Parse CREATE FUNCTION statements (only from main file) --- 
    logging.info("Parsing CREATE FUNCTION statements...")
    function_regex = re.compile(
        r"CREATE(?:\s+OR\s+REPLACE)?\s+FUNCTION\s+([a-zA-Z0-9_.]+)" 
        r"\s*\(([^)]*)\)" 
        r"\s+RETURNS\s+(?:(SETOF)\s+)?(?:(TABLE)\s*\((.*?)\)|([a-zA-Z0-9_.()\[\]]+))" 
        r"(.*?)(?:AS\s+\$\$|AS\s+\'|LANGUAGE\s+\w+)", 
        re.IGNORECASE | re.DOTALL | re.MULTILINE
    )

    # Find function bodies separately (this part might be less reliable)
    # func_bodies = {m.group(1).strip(): m.group(0) 
    #                for m in re.finditer(r"(FUNCTION\s+[a-zA-Z0-9_.]+\s*\([^)]*\)).*?(AS\s+(?:\$\$.*?\$\$|'.*?'))", processed_function_sql, re.IGNORECASE | re.DOTALL)}

    for match in function_regex.finditer(processed_function_sql): # Use processed_function_sql here
        sql_name = match.group(1).strip()
        param_str = match.group(2).strip() if match.group(2) else ""
        returns_setof = bool(match.group(3))
        is_returns_table = bool(match.group(4))
        table_columns_str = match.group(5).strip() if is_returns_table and match.group(5) else ""
        scalar_or_table_name_return = match.group(6).strip() if match.group(6) else ""
        
        logging.info(f"Parsing function signature: {sql_name}")

        try:
            params, param_imports = _parse_params(param_str)
            required_imports = param_imports.copy()
            required_imports.add("from psycopg import AsyncConnection")

            func = ParsedFunction(sql_name=sql_name, python_name=sql_name)
            func.params = params
            func.returns_setof = returns_setof

            table_name_return = None 

            if is_returns_table:
                logging.debug(f"  -> Returns explicit TABLE definition")
                return_cols, col_imports = _parse_column_definitions(table_columns_str)
                if not return_cols:
                     logging.warning(f"    No columns parsed from explicit TABLE definition for {sql_name}. Content: '{table_columns_str[:100]}...'")
                     func.return_type = "Any" # Fallback
                     required_imports.add(PYTHON_IMPORTS["Any"])
                else:
                     func.return_columns = return_cols
                     func.returns_table = True
                     required_imports.update(col_imports)
                     required_imports.add("from dataclasses import dataclass")
                     
            elif scalar_or_table_name_return:
                return_type_str = scalar_or_table_name_return
                if return_type_str.lower() == 'void':
                    logging.debug(f"  -> Returns VOID")
                    func.return_type = "None"
                elif return_type_str.lower() == 'record':
                    logging.debug(f"  -> Returns RECORD")
                    func.returns_record = True
                    func.return_type = "Tuple"
                    required_imports.add(PYTHON_IMPORTS["Tuple"])
                else:
                    py_return_type, ret_import = _map_sql_to_python_type(return_type_str)
                    if py_return_type != "Any" or not returns_setof:
                        logging.debug(f"  -> Returns SCALAR: {return_type_str} -> {py_return_type}")
                        func.return_type = py_return_type
                        if ret_import:
                            for imp in ret_import.split('\n'):
                                if imp: required_imports.add(imp)
                    else: # SETOF unknown type -> assume table name
                        table_name_return = return_type_str 
                        normalized_table_name = table_name_return.split('.')[-1]
                        logging.debug(f"  -> Returns SETOF {table_name_return}. Looking for table schema '{normalized_table_name}'.")
                        
                        # Store the table name for the generator
                        func.setof_table_name = normalized_table_name 
                        
                        if normalized_table_name in TABLE_SCHEMAS:
                             logging.info(f"    Found schema for table '{normalized_table_name}'")
                             func.return_columns = TABLE_SCHEMAS[normalized_table_name]
                             func.returns_table = True 
                             required_imports.update(TABLE_SCHEMA_IMPORTS[normalized_table_name])
                             required_imports.add("from dataclasses import dataclass")
                        else:
                             # Fallback as before
                             logging.warning(f"    Schema not found for table '{normalized_table_name}'. Mapping to List[Any]. Define dataclass '{normalized_table_name.capitalize()}' manually or ensure CREATE TABLE is parsed.")
                             func.return_type = "Any" 
                             func.returns_table = True # Still treat as table for generator naming
                             func.return_columns = [ReturnColumn(name="unknown", sql_type=table_name_return, python_type="Any")]
                             required_imports.add(PYTHON_IMPORTS["Any"])
                             required_imports.add("from dataclasses import dataclass")
            else:
                 logging.warning(f"Could not determine return type for function {sql_name}. Assuming None.")
                 func.return_type = "None"

            # --- Final type adjustments (List/Optional) --- 
            is_optional_wrapper_needed = False
            is_list_wrapper_needed = False

            if returns_setof:
                 is_list_wrapper_needed = True
            elif not func.returns_table and func.return_type != "None": 
                 is_optional_wrapper_needed = True
            elif func.returns_table and not returns_setof:
                 is_optional_wrapper_needed = True
                 
            if is_list_wrapper_needed: required_imports.add(PYTHON_IMPORTS["List"])
            if is_optional_wrapper_needed: required_imports.add("from typing import Optional")

            if is_list_wrapper_needed or is_optional_wrapper_needed or \
               any(t in func.return_type for t in ["Tuple", "Dict", "Any"]):
                 required_imports.add("from typing import Optional, List, Any, Tuple, Dict")
                 
            func.required_imports = {imp for imp in required_imports if imp}
            functions.append(func)

        except Exception as e:
            logging.exception(f"Failed to parse function '{sql_name}'. Skipping.")
            continue

    logging.info(f"Parsed {len(TABLE_SCHEMAS)} CREATE TABLE statements.")
    logging.info(f"Parsed {len(functions)} CREATE FUNCTION statements.")
    if not functions and processed_function_sql:
         logging.warning("No CREATE FUNCTION statements found or parsed successfully in main file.")

    # Return both functions and the collected schema imports
    return functions, TABLE_SCHEMA_IMPORTS 