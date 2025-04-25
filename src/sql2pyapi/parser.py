import re
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Tuple
import logging
import textwrap

# --- Type Maps and Imports --- (Keep these first)
# Basic PostgreSQL to Python type mapping
TYPE_MAP = {
    "uuid": "UUID",
    "text": "str",
    "varchar": "str",
    "character varying": "str",  # Explicitly map this
    "character": "str",         # Add mapping for 'character' base type
    "integer": "int",
    "int": "int",
    "bigint": "int",  # Consider using int in Python 3, as it has arbitrary precision
    "smallint": "int",
    "serial": "int",  # Add serial mapping
    "bigserial": "int",  # Add bigserial mapping
    "boolean": "bool",
    "bool": "bool",
    "timestamp": "datetime",
    "timestamp without time zone": "datetime",
    "timestamptz": "datetime",  # Often preferred
    "timestamp with time zone": "datetime",
    "date": "date",
    "numeric": "Decimal",
    "decimal": "Decimal",
    "json": "dict",  # Or Any, depending on usage
    "jsonb": "dict",  # Or Any
    "bytea": "bytes",
    # Add more mappings as needed
}

PYTHON_IMPORTS = {
    "UUID": "from uuid import UUID",
    "datetime": "from datetime import datetime",  # Import only datetime
    "date": "from datetime import date",  # Import only date
    "Decimal": "from decimal import Decimal",
    "Any": "from typing import Any",  # Import for Any
    "List": "from typing import List",  # Import for List
    "Dict": "from typing import Dict",  # Import for Dict
    "Tuple": "from typing import Tuple",  # Import for Tuple
}


# --- Data Structures --- (Define these BEFORE globals)
class SQLParsingError(Exception):
    """Custom exception for parsing errors."""

    pass


@dataclass
class SQLParameter:
    name: str  # Original SQL name (e.g., p_email)
    python_name: str  # Pythonic name (e.g., email)
    sql_type: str
    python_type: str
    is_optional: bool = False


@dataclass
class ReturnColumn:
    name: str
    sql_type: str
    python_type: str
    is_optional: bool = True # Default to optional unless NOT NULL is found


@dataclass
class ParsedFunction:
    sql_name: str
    python_name: str
    params: List[SQLParameter] = field(default_factory=list)
    return_type: str = "None"  # Python type hint for scalar return
    return_columns: List[ReturnColumn] = field(default_factory=list)  # For RETURNS TABLE
    returns_table: bool = False
    returns_record: bool = False  # Simple record, treat as tuple or dict
    returns_setof: bool = False  # For SETOF scalar types
    required_imports: set = field(default_factory=set)
    # New field to store the base table name for SETOF table_name returns
    setof_table_name: Optional[str] = None
    sql_comment: Optional[str] = None  # Store the cleaned SQL comment


# --- Global Schema Storage --- (Define globals AFTER structures)
TABLE_SCHEMAS: Dict[str, List[ReturnColumn]] = {}
TABLE_SCHEMA_IMPORTS: Dict[str, set] = {}


def _map_sql_to_python_type(sql_type: str, is_optional: bool = False) -> Tuple[str, Optional[str]]:
    """Maps SQL type to Python type and returns required import. Wraps with Optional if needed."""
    sql_type_normal = sql_type.lower().strip()
    is_array = sql_type_normal.endswith("[]")
    if is_array:
        # Remove the [] suffix for base type lookup
        sql_type_no_array = sql_type_normal[:-2].strip()
    else:
        sql_type_no_array = sql_type_normal

    # Split on whitespace, parenthesis, or square brackets to get the *potential* base type
    # This handles cases like "character varying(N)" but might fail for "timestamp with time zone"
    potential_base_type = re.split(r"[\s(\[]", sql_type_no_array, maxsplit=1)[0]

    # --- Refined Type Lookup ---
    # First, check for an exact match (e.g., "timestamp with time zone")
    py_type = TYPE_MAP.get(sql_type_no_array)
    if not py_type:
        # If no exact match, check using the split potential base type (e.g., "character")
        py_type = TYPE_MAP.get(potential_base_type, "Any")

    # --- Import Handling ---
    import_stmt = PYTHON_IMPORTS.get(py_type)
    combined_imports = {import_stmt} if import_stmt else set()
    if py_type == "Any":
        # Ensure Any is imported if type maps to Any
        any_import = PYTHON_IMPORTS.get("Any")
        if any_import:
            combined_imports.add(any_import)

    # --- Array Handling ---
    if is_array:
        py_type = f"List[{py_type}]"
        list_import = PYTHON_IMPORTS.get("List")
        if list_import:
            combined_imports.add(list_import)

    # --- Special handling for dict/json types (BEFORE Optional wrapping, AFTER array wrapping) ---
    # Use 'in' check to handle both 'dict' and 'List[dict]'
    if "dict" in py_type:
        py_type = py_type.replace("dict", "Dict[str, Any]") # Replace dict part
        # Ensure Dict and Any imports are added
        dict_import = PYTHON_IMPORTS.get("Dict")
        any_import = PYTHON_IMPORTS.get("Any")
        if dict_import:
            combined_imports.add(dict_import)
        if any_import:
            combined_imports.add(any_import)

    # --- Optional Handling ---
    if is_optional and py_type != "Any" and not py_type.startswith("Optional["):
        # Only wrap non-Any types that aren't already Optional
        # We apply Optional even to List types if the base SQL type could be NULL
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

    # Split by comma or newline to get potential definition parts
    # Clean fragments: remove comments and strip whitespace
    fragments = []
    for part in re.split(r'[,\n]', col_defs_str):
        cleaned_part = re.sub(r"--.*?$", "", part).strip()
        if cleaned_part:
            fragments.append(cleaned_part)

    if not fragments:
        return columns, required_imports

    # Regex to capture column name (quoted or unquoted) and the rest
    name_regex = re.compile(r'^\s*(?:("[^"\n]+")|([a-zA-Z0-9_]+))\s+(.*)$')

    # Process fragments, attempting to form complete definitions
    i = 0
    while i < len(fragments):
        current_def = fragments[i]
        i += 1

        # --- Attempt to merge fragments split inside parentheses (e.g., numeric(p, s)) ---
        if columns and re.match(r"^\d+\s*\)?", current_def): # Looks like scale part e.g., "2)"
            last_col = columns[-1]
            if last_col.sql_type.lower().startswith(("numeric(", "decimal(")) and ',' not in last_col.sql_type:
                 logging.debug(f"Attempting merge for numeric/decimal scale: '{current_def}'")
                 merged_type = last_col.sql_type + ", " + current_def.rstrip(")") + ")"
                 # Update last column's type
                 last_col.sql_type = merged_type
                 # Re-map python type and update imports if necessary (nullability unlikely to change)
                 py_type, import_stmt = _map_sql_to_python_type(merged_type, last_col.is_optional)
                 if py_type != last_col.python_type:
                     last_col.python_type = py_type
                     if import_stmt:
                        for imp in import_stmt.split("\n"):
                             if imp:
                                 required_imports.add(imp)
                 continue # Skip processing this fragment further
        # --- End merge attempt ---

        # Skip constraint definitions
        if current_def.lower().startswith(("constraint", "primary key", "foreign key", "unique", "check", "like")):
            continue

        # Match name and the rest of the definition
        name_match = name_regex.match(current_def)
        if not name_match:
            logging.warning(f"Could not extract column name from definition fragment: '{current_def}'")
            continue

        col_name = (name_match.group(1) or name_match.group(2)).strip('"')
        rest_of_def = name_match.group(3).strip()

        # Extract type - find the first terminating keyword
        terminating_keywords = {
            "primary", "unique", "not", "null", "references",
            "check", "collate", "default", "generated"
        }
        type_parts = []
        words = rest_of_def.split()
        sql_type_extracted = None
        constraint_part_start_index = len(words)

        for j, word in enumerate(words):
            word_lower = word.lower()
            is_terminator = False
            for keyword in terminating_keywords:
                # Handle NOT NULL as a single keyword
                if keyword == "not" and j + 1 < len(words) and words[j+1].lower() == "null":
                    is_terminator = True
                    break
                # Skip 'null' if it follows 'not'
                if keyword == "null" and j > 0 and words[j-1].lower() == "not":
                    continue
                
                if word_lower == keyword or word_lower.startswith(keyword + "("):
                    is_terminator = True
                    break
            if is_terminator:
                constraint_part_start_index = j
                break
            type_parts.append(word)
        
        if not type_parts:
             logging.warning(f"Could not extract column type from definition: '{current_def}'")
             continue
        
        sql_type_extracted = " ".join(type_parts)
        constraint_part = " ".join(words[constraint_part_start_index:]).lower()

        # Determine if the column is optional (nullable)
        is_optional = "not null" not in constraint_part and "primary key" not in constraint_part

        # Pass the determined optionality to the type mapping function
        py_type, import_stmt = _map_sql_to_python_type(sql_type_extracted, is_optional=is_optional)

        if import_stmt:
            for imp in import_stmt.split("\n"):
                if imp:
                    required_imports.add(imp)
        columns.append(ReturnColumn(name=col_name, sql_type=sql_type_extracted, python_type=py_type, is_optional=is_optional))

    if not columns and col_defs_str.strip():
        logging.warning(f"Could not parse any columns from CREATE TABLE block: '{col_defs_str[:100]}...'")

    return columns, required_imports


def _parse_create_table(sql_content: str):
    """Finds and parses CREATE TABLE statements, storing schemas globally."""
    # Simpler regex: Find CREATE TABLE name (...) ; - less prone to backtracking
    # Might capture slightly too much in complex cases, relies on _parse_column_definitions robustness
    table_regex = re.compile(
        r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([a-zA-Z0-9_.]+)"  # 1: Table name
        r"\s*\("  # Opening parenthesis
        r"(.*?)"  # 2: Everything inside parenthesis (non-greedy)
        r"\)\s*(?:INHERITS|WITH|TABLESPACE|;)",  # Stop at known clauses after ) or semicolon
        re.IGNORECASE | re.DOTALL | re.MULTILINE,
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
                normalized_table_name = table_name.split(".")[-1]
                TABLE_SCHEMAS[normalized_table_name] = columns
                TABLE_SCHEMA_IMPORTS[normalized_table_name] = required_imports
                logging.debug(
                    f"  -> Parsed {len(columns)} columns for table {normalized_table_name} (from {table_name})"
                )
            else:
                logging.warning(
                    f"  -> No columns parsed for table {table_name} from definition: '{col_defs_str_cleaned[:100]}...'"
                )
        except Exception as e:
            logging.exception(f"Failed to parse columns for table '{table_name}'.")
            # Re-raise as a specific parsing error instead of continuing
            raise SQLParsingError(f"Failed to parse columns for table '{table_name}'") from e


def _parse_params(param_str: str) -> Tuple[List[SQLParameter], set]:
    """Parses parameter string including optional DEFAULT values."""
    params = []
    required_imports = set()
    if not param_str:
        return params, required_imports

    # Regex revised: simpler, less greedy type capture
    # 1: Optional mode (IN/OUT/INOUT)
    # 2: Parameter name
    # 3: Parameter type (non-greedy, stop before DEFAULT or end)
    # 4: Optional DEFAULT clause (and anything after)
    param_regex = re.compile(
        r"""
        \s*                         # Leading whitespace
        (?:(IN|OUT|INOUT)\s+)?      # Optional mode (Group 1)
        ([a-zA-Z0-9_]+)             # Parameter name (Group 2)
        \s+                         # Whitespace after name
        (.*?)                       # Parameter type (Group 3) - Non-greedy
        (?:\s+(DEFAULT\s+.*)|$)    # Optional Default clause or end of string (Group 4 for DEFAULT part)
        """,
        re.IGNORECASE | re.VERBOSE,
    )

    # Split by comma, but handle potential commas inside DEFAULT strings or type definitions
    # Basic split works for now, complexity increases if defaults contain commas
    param_defs = param_str.split(",")

    for param_def in param_defs:
        param_def = param_def.strip()
        if not param_def:
            continue

        match = param_regex.match(param_def)
        if not match:
            # Handle the case where split might happen inside parens like numeric(10, 2)
            # This is a basic recovery attempt
            if params and ')' not in params[-1].sql_type and ')' in param_def:
                 logging.debug(f"Attempting recovery for split inside type: appending '{param_def}'")
                 params[-1].sql_type += "," + param_def
                 # Re-run type mapping for the corrected type
                 py_type, import_stmts = _map_sql_to_python_type(params[-1].sql_type, params[-1].is_optional)
                 params[-1].python_type = py_type
                 if import_stmts:
                    for imp in import_stmts.split("\n"):
                         if imp:
                             required_imports.add(imp)
                 continue # Skip rest of processing for this fragment
            else:
                 logging.warning(f"Could not parse parameter definition: {param_def}")
                 continue

        # mode = match.group(1) # Currently unused
        sql_name = match.group(2).strip()
        sql_type = match.group(3).strip()
        remainder = match.group(4) # Includes 'DEFAULT ...'
        remainder = remainder.strip() if remainder else ""

        is_optional = remainder.lower().startswith("default")

        # Generate Pythonic name
        python_name = sql_name
        if python_name.startswith("p_") and len(python_name) > 2:
            python_name = python_name[2:]
        elif python_name.startswith("_") and len(python_name) > 1:
            python_name = python_name[1:]

        py_type, import_stmts = _map_sql_to_python_type(sql_type, is_optional)

        if import_stmts:
            for imp in import_stmts.split("\n"):
                if imp:
                    required_imports.add(imp)

        params.append(
            SQLParameter(
                name=sql_name,
                python_name=python_name,
                sql_type=sql_type,
                python_type=py_type,
                is_optional=is_optional,
            )
        )

    return params, required_imports


def _clean_comment_block(comment_block: str) -> str:
    """Cleans a block of SQL comments (either -- or /* */ style) for use as a docstring."""
    cleaned_comment = None
    lines = comment_block.strip().splitlines()
    if not lines:
        return ""

    if lines[0].strip().startswith("--"):
        cleaned_lines = []
        for line in lines:
            dash_pos = line.find("--")
            if dash_pos != -1:
                content_start_pos = dash_pos + 2
                if content_start_pos < len(line) and line[content_start_pos] == " ":
                    content_start_pos += 1
                cleaned_lines.append(line[content_start_pos:])
            else:
                cleaned_lines.append(line)
        raw_comment = "\n".join(cleaned_lines)
        cleaned_comment = textwrap.dedent(raw_comment).strip("\n")

    elif lines[0].strip().startswith("/*"):
        start_block_idx = comment_block.find("/*")
        end_block_idx = comment_block.rfind("*/")
        if start_block_idx != -1 and end_block_idx != -1 and end_block_idx > start_block_idx:
            comment_content = comment_block[start_block_idx + 2 : end_block_idx]
            content_lines = comment_content.splitlines()
            consistent_star = all(line.strip().startswith("*") or not line.strip() for line in content_lines[1:])

            processed_lines = []
            if consistent_star:
                for line in content_lines:
                    lstripped_line = line.lstrip(" ")
                    if lstripped_line.startswith("*"):
                        star_pos = line.find("*")
                        content_start = star_pos + 1
                        if content_start < len(line) and line[content_start] == " ":
                            content_start += 1
                        processed_lines.append(line[content_start:])
                    else:
                        processed_lines.append(line)
                comment_content = "\n".join(processed_lines)

            dedented_content = textwrap.dedent(comment_content)
            cleaned_comment = dedented_content.strip("\n")
        else:
            cleaned_comment = comment_block.strip()
    else:
        # Should not happen if called with valid comment block, but handle defensively
        cleaned_comment = comment_block.strip()

    return cleaned_comment if cleaned_comment is not None else ""


def _find_preceding_comment(sql_content: str, function_start_pos: int, all_comments: List[Dict]) -> Optional[str]:
    """Finds and cleans the comment block immediately preceding a function definition."""
    best_comment_block_lines = []
    # Start anchor position at the beginning of the function itself
    last_anchor_pos = function_start_pos 

    for i in range(len(all_comments) - 1, -1, -1):  # Search backwards
        comment = all_comments[i]
        
        # Skip comments that start at or after the current anchor position
        if comment["start"] >= last_anchor_pos:
            continue

        # Check the text between the end of this comment and the last anchor position
        intervening_text = sql_content[comment["end"] : last_anchor_pos]

        if intervening_text.strip() == "":
            # This comment is contiguous to the block or function start
            # Add its text to the beginning of our list
            best_comment_block_lines.insert(0, comment["text"])
            # Update the anchor to the start of this comment, looking for more comments before it
            last_anchor_pos = comment["start"]
        else:
            # Found non-whitespace text between this comment and the anchor
            # This means the contiguous block (if any) has ended, so stop searching
            break

    if not best_comment_block_lines:
        return None

    full_comment_block = "\n".join(best_comment_block_lines)
    return _clean_comment_block(full_comment_block)


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

    # --- First Pass: Parse CREATE TABLE (from schema file first, then function file) ---
    if schema_content:  # Use original schema_content
        logging.info("Parsing CREATE TABLE statements from schema file...")
        _parse_create_table(schema_content)
    # Also parse tables from the main file in case they are defined there
    logging.info("Parsing CREATE TABLE statements from main functions file...")
    _parse_create_table(sql_content)  # Use original sql_content

    # --- Second Pass: Parse CREATE FUNCTION statements (only from main file) ---
    logging.info("Parsing CREATE FUNCTION statements...")

    # Pre-parse all comments once
    comment_regex = re.compile(r"(--.*?$)|(/\*.*?\*/)", re.MULTILINE | re.DOTALL)
    all_comments = [
        {"start": m.start(), "end": m.end(), "text": m.group(0)}
        for m in comment_regex.finditer(sql_content)
    ]

    function_regex = re.compile(
        r"CREATE(?:\s+OR\s+REPLACE)?\s+FUNCTION\s+([a-zA-Z0-9_.]+)"
        r"\s*\(([^)]*)\)"
        r"\s+RETURNS\s+(?:(SETOF)\s+)?(?:(TABLE)\s*\((.*?)\)|([a-zA-Z0-9_.()\[\]]+))"
        r"(.*?)(?:AS\s+\$\$|AS\s+\'|LANGUAGE\s+\w+)",
        re.IGNORECASE | re.DOTALL | re.MULTILINE,
    )

    for match in function_regex.finditer(sql_content):
        sql_name = match.group(1).strip()
        param_str = match.group(2).strip() if match.group(2) else ""
        returns_setof = bool(match.group(3))
        is_returns_table = bool(match.group(4))
        table_columns_str = match.group(5).strip() if is_returns_table and match.group(5) else ""
        scalar_or_table_name_return = match.group(6).strip() if match.group(6) else ""

        function_start_pos = match.start()

        # Find and clean the preceding comment using the helper function
        cleaned_comment = _find_preceding_comment(sql_content, function_start_pos, all_comments)

        logging.info(f"Parsing function signature: {sql_name}")

        try:
            # Clean comments from param_str before parsing
            param_str_cleaned = re.sub(r"--.*?$", "", param_str, flags=re.MULTILINE)
            param_str_cleaned = re.sub(r"/\*.*?\*/", "", param_str_cleaned, flags=re.DOTALL)
            param_str_cleaned = " ".join(param_str_cleaned.split()) # Normalize whitespace
            params, param_imports = _parse_params(param_str_cleaned)
            required_imports = param_imports.copy()
            required_imports.add("from psycopg import AsyncConnection")

            func = ParsedFunction(sql_name=sql_name, python_name=sql_name)
            func.sql_comment = cleaned_comment
            func.params = params
            func.returns_setof = returns_setof

            table_name_return = None

            if is_returns_table:
                logging.debug("  -> Returns explicit TABLE definition")
                # Clean comments from table_columns_str before parsing
                cleaned_table_columns_str = re.sub(r"--.*?$", "", table_columns_str, flags=re.MULTILINE)
                cleaned_table_columns_str = re.sub(r"/\\*.*?\\*/", "", cleaned_table_columns_str, flags=re.DOTALL)
                cleaned_table_columns_str = "\n".join(line.strip() for line in cleaned_table_columns_str.splitlines() if line.strip())
                return_cols, col_imports = _parse_column_definitions(cleaned_table_columns_str) # Use cleaned string
                if not return_cols:
                    logging.warning(
                        f"    No columns parsed from explicit TABLE definition for {sql_name}. Content: '{cleaned_table_columns_str[:100]}...'")
                    func.return_type = "Any"  # Fallback
                    required_imports.add(PYTHON_IMPORTS["Any"])
                else:
                    func.return_columns = return_cols
                    func.returns_table = True
                    required_imports.update(col_imports)
                    required_imports.add("from dataclasses import dataclass")

            elif scalar_or_table_name_return:
                return_type_str = scalar_or_table_name_return
                if return_type_str.lower() == "void":
                    logging.debug("  -> Returns VOID")
                    func.return_type = "None"
                elif return_type_str.lower() == "record":
                    logging.debug("  -> Returns RECORD")
                    func.returns_record = True
                    func.return_type = "Tuple"
                    required_imports.add(PYTHON_IMPORTS["Tuple"])
                else:
                    py_return_type, ret_import = _map_sql_to_python_type(return_type_str)
                    if py_return_type != "Any" or not returns_setof:
                        logging.debug(f"  -> Returns SCALAR: {return_type_str} -> {py_return_type}")
                        func.return_type = py_return_type
                        if ret_import:
                            for imp in ret_import.split("\n"):
                                if imp:
                                    required_imports.add(imp)
                    else:  # SETOF unknown type -> assume table name
                        table_name_return = return_type_str
                        normalized_table_name = table_name_return.split(".")[-1]
                        logging.debug(
                            f"  -> Returns SETOF {table_name_return}. Looking for table schema '{normalized_table_name}'.")

                        func.setof_table_name = normalized_table_name

                        if normalized_table_name in TABLE_SCHEMAS:
                            logging.info(f"    Found schema for table '{normalized_table_name}'")
                            func.return_columns = TABLE_SCHEMAS[normalized_table_name]
                            func.returns_table = True
                            required_imports.update(TABLE_SCHEMA_IMPORTS[normalized_table_name])
                            required_imports.add("from dataclasses import dataclass")
                        else:
                            logging.warning(
                                f"    Schema not found for table '{normalized_table_name}'. Generating placeholder dataclass. Define the corresponding dataclass manually or ensure CREATE TABLE is parsed.")
                            func.returns_table = True
                            func.return_columns = [
                                ReturnColumn(
                                    name="unknown",
                                    sql_type=table_name_return,
                                    python_type="Any",
                                )
                            ]
                            required_imports.add(PYTHON_IMPORTS["Any"])
                            required_imports.add("from dataclasses import dataclass")
            else:
                logging.warning(f"Could not determine return type for function {sql_name}. Assuming None.")
                func.return_type = "None"

            base_return_type = "None"
            if func.returns_table:
                base_return_type = "DataclassPlaceholder"
            elif func.returns_record:
                base_return_type = "Tuple"
            elif func.return_type != "None":
                base_return_type = func.return_type

            final_return_type = base_return_type
            # is_complex_type was here, removed as unused

            if returns_setof:
                if base_return_type != "None":
                    final_return_type = f"List[{base_return_type}]"
                    required_imports.add(PYTHON_IMPORTS["List"])
                    # is_complex_type assignment removed
                elif base_return_type == "DataclassPlaceholder":
                    final_return_type = "List[DataclassPlaceholder]"
                    required_imports.add(PYTHON_IMPORTS["List"])
                    # is_complex_type assignment removed
            elif base_return_type != "None":
                required_imports.add("from typing import Optional")
                # is_complex_type assignment removed
                if base_return_type != "DataclassPlaceholder":
                    final_return_type = f"Optional[{base_return_type}]"
                else:
                    final_return_type = "Optional[DataclassPlaceholder]"

            func.return_type = final_return_type

            typing_imports_to_add = set()
            if "Optional[" in final_return_type:
                typing_imports_to_add.add("from typing import Optional")
            if "List[" in final_return_type:
                typing_imports_to_add.add(PYTHON_IMPORTS["List"])
            if "Tuple" in final_return_type:
                typing_imports_to_add.add(PYTHON_IMPORTS["Tuple"])
            if "Dict" in final_return_type:
                typing_imports_to_add.add(PYTHON_IMPORTS["Dict"])
            if "Any" in final_return_type:
                typing_imports_to_add.add(PYTHON_IMPORTS["Any"])

            required_imports.update(typing_imports_to_add)

            func.required_imports = {imp for imp in required_imports if imp}
            functions.append(func)

        except SQLParsingError as spe: # Catch specific parsing errors
            logging.error(f"Failed to parse function '{sql_name}' due to: {spe}")
            raise # Re-raise the parsing error to halt execution

        except Exception as e:
            logging.error(
                f"Unexpected error parsing function '{sql_name}': {e}. Skipping this function.", exc_info=True
            )
            # Continue to the next function for non-parsing errors

    logging.info(f"Finished parsing. Found {len(functions)} functions and {len(TABLE_SCHEMAS)} table schemas.")
    return functions, TABLE_SCHEMA_IMPORTS
