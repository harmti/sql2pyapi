import re
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Tuple
import logging
import textwrap
import math

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

# --- Regex Definitions (Module Level) ---
FUNCTION_REGEX = re.compile(
    r"CREATE(?:\s+OR\s+REPLACE)?\s+FUNCTION\s+([a-zA-Z0-9_.]+)"
    r"\s*\(([^)]*)\)"
    r"\s+RETURNS\s+(?:(SETOF)\s+)?(?:(TABLE)\s*\((.*?)\)|([a-zA-Z0-9_.()\[\]]+))" # Groups 3,4,5,6 relate to returns
    r"(.*?)(?:AS\s+\$\$|AS\s+\'|LANGUAGE\s+\w+)",
    re.IGNORECASE | re.DOTALL | re.MULTILINE,
)

# Regex to find comments (both -- and /* */)
COMMENT_REGEX = re.compile(r"(--.*?$)|(/\*.*?\*/)", re.MULTILINE | re.DOTALL)


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
        py_type, import_stmt = _map_sql_to_python_type(sql_type_extracted, is_optional)

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


def _clean_comment_block(comment_lines: List[str]) -> str:
    """Cleans a list of raw SQL comment lines for use as a docstring."""
    if not comment_lines:
        return ""

    cleaned_lines = []
    for line in comment_lines: # Raw line from list passed by _find_preceding_comment
        stripped_line = line.strip() # Whitespace removed from ends
        cleaned_line = None # Default to None, set if we successfully clean

        # --- Determine line type ---
        is_line_comment = stripped_line.startswith("--")
        is_block_start = stripped_line.startswith("/*")
        is_block_end = stripped_line.endswith("*/")
        is_block_single = is_block_start and is_block_end
        # Check for leading star *only if* it's not the start/end of the block itself
        is_leading_star = stripped_line.startswith("*") and not is_block_start and not is_block_end

        # --- Process based on type ---
        if is_line_comment:
            cleaned_line = stripped_line[2:]
            if cleaned_line.startswith(" "): cleaned_line = cleaned_line[1:] # Remove one leading space AFTER --
        elif is_block_single:
            cleaned_line = stripped_line[2:-2].strip() if len(stripped_line) > 4 else "" # Strip inside /* */
        elif is_block_start: # Start of multi-line block (might be the only content, e.g., "/*")
             cleaned_line = stripped_line[2:].lstrip() # Remove '/*' and leading space AFTER it
        elif is_block_end: # End of multi-line block (might be the only content, e.g., "*/")
             # This case now correctly handles lines like ' * ... */' because is_leading_star is false
             cleaned_line = stripped_line[:-2].rstrip() # Remove '*/' and trailing space BEFORE it
        elif is_leading_star: # Middle line of multi-line block starting with *
             cleaned_line = stripped_line[1:] # Remove '*'
             if cleaned_line.startswith(" "): cleaned_line = cleaned_line[1:] # Remove only one leading space after *
        else: # Line inside block without marker, or some other unexpected line
             # Keep original behavior: append stripped line
             cleaned_line = stripped_line

        # Append the processed line (reverting: remove individual strip)
        if cleaned_line is not None:
             cleaned_lines.append(cleaned_line) # Reverted: No .strip() here

    # Join the cleaned lines, dedent, and final strip
    valid_lines = cleaned_lines
    if not valid_lines:
         return ""

    raw_comment = "\n".join(valid_lines) # Join the pieces
    # Use textwrap.dedent for final indentation cleanup
    try:
        # Dedent assumes consistent indentation, might mess up mixed comments
        # Apply strip() after dedent
        dedented_comment = textwrap.dedent(raw_comment).strip()
    except Exception as e:
        logging.warning(f"textwrap.dedent failed during comment cleaning: {e}. Using raw comment.")
        # Strip raw comment if dedent fails
        dedented_comment = raw_comment.strip()

    # Return the final result (already stripped)
    return dedented_comment


def _find_preceding_comment(lines: list[str], func_start_line_idx: int) -> str | None:
    """
    Finds the comment block immediately preceding a function definition.

    Args:
        lines: The list of lines in the SQL file.
        func_start_line_idx: The 0-based index of the line where the function definition starts.

    Returns:
        The cleaned comment block as a string, or None if no preceding comment is found.
    """
    comment_lines = []
    in_block_comment = False
    last_comment_end_line_idx = -1
    found_first_comment_line = False # New flag

    for i in range(func_start_line_idx - 1, -1, -1):
        line_content = lines[i] # Keep original line with leading whitespace
        stripped_line = line_content.strip()

        # If we already found a comment, and this line is blank, stop searching
        if found_first_comment_line and not stripped_line:
             break # Stop searching backwards if we hit a blank line after a comment
        elif not stripped_line:
             continue # Skip blank lines before finding the first comment

        # --- Block comment end `*/` --- 
        is_block_end = stripped_line.endswith("*/")
        if is_block_end:
            if in_block_comment:
                # Nested block comments aren't handled simply; stop searching
                return None # Or maybe just break?
            in_block_comment = True
            found_first_comment_line = True # Mark that we found a comment
            # ... rest of existing /* ... */ and /* ... */ check ...
            start_block_idx = stripped_line.rfind("/*")
            if start_block_idx != -1 and start_block_idx < stripped_line.rfind("*/"): # Single line block
                comment_lines.insert(0, line_content) # Insert original line
                in_block_comment = False
                last_comment_end_line_idx = i
                continue
            else: # End of a multi-line block
                comment_lines.insert(0, line_content) # Insert original line
                last_comment_end_line_idx = i
                continue

        # --- Block comment start `/*` --- 
        is_block_start = stripped_line.startswith("/*")
        if is_block_start:
            if not in_block_comment:
                # Started a block comment without seeing the end - malformed or not preceding comment
                # If we haven't found any comment line yet, this isn't the preceding comment
                if not found_first_comment_line: return None
                # Otherwise, could be a block above line comments, stop here
                break
            # This is the start of a block we are tracking
            found_first_comment_line = True
            comment_lines.insert(0, line_content) # Insert original line
            in_block_comment = False
            last_comment_end_line_idx = i
            continue

        # --- Inside a block comment --- 
        if in_block_comment:
            found_first_comment_line = True
            comment_lines.insert(0, line_content) # Insert original line
            last_comment_end_line_idx = i
            continue

        # --- Line comment `--` --- 
        is_line_comment = stripped_line.startswith("--")
        if is_line_comment:
            found_first_comment_line = True
            comment_lines.insert(0, line_content) # Insert original line
            last_comment_end_line_idx = i
            continue

        # --- Non-comment, non-empty line --- 
        # If we encounter a non-empty, non-comment line, stop searching
        # This will also handle the gap check implicitly
        break

    if not comment_lines:
        return None

    # No final gap check needed as the loop logic handles it.

    # Pass the list of original comment lines (with whitespace)
    return _clean_comment_block(comment_lines)


def _parse_return_clause(match: re.Match, initial_imports: set) -> Tuple[dict, set]:
    """Parses the RETURNS clause details from the function regex match.

    Args:
        match: The regex match object from function_regex.
        initial_imports: The set of imports gathered so far (e.g., from params).

    Returns:
        A tuple containing:
        - A dictionary with parsed return properties:
            {
                'return_type': str,      # Base Python type ('None', 'int', 'Tuple', 'DataclassPlaceholder')
                'returns_table': bool,
                'return_columns': List[ReturnColumn],
                'returns_record': bool,
                'setof_table_name': Optional[str]
            }
        - The updated set of required imports.
    """
    returns_setof = bool(match.group(3))
    is_returns_table = bool(match.group(4))
    table_columns_str = match.group(5).strip() if is_returns_table and match.group(5) else ""
    scalar_or_table_name_return = match.group(6).strip() if match.group(6) else ""
    sql_name = match.group(1).strip() # For logging

    # Initialize return properties
    return_props = {
        'return_type': "None",
        'returns_table': False,
        'return_columns': [],
        'returns_record': False,
        'setof_table_name': None
    }
    required_imports = initial_imports.copy()

    if is_returns_table:
        logging.debug(f"  -> Function '{sql_name}' returns explicit TABLE definition")
        cleaned_table_columns_str = re.sub(r"--.*?$", "", table_columns_str, flags=re.MULTILINE)
        cleaned_table_columns_str = re.sub(r"/\\*.*?\\*/", "", cleaned_table_columns_str, flags=re.DOTALL)
        cleaned_table_columns_str = "\n".join(line.strip() for line in cleaned_table_columns_str.splitlines() if line.strip())
        return_cols, col_imports = _parse_column_definitions(cleaned_table_columns_str)
        if not return_cols:
            logging.warning(
                f"    No columns parsed from explicit TABLE definition for {sql_name}. Content: '{cleaned_table_columns_str[:100]}...'")
            return_props['return_type'] = "Any"  # Fallback
            required_imports.add(PYTHON_IMPORTS["Any"])
        else:
            return_props['return_columns'] = return_cols
            return_props['returns_table'] = True
            return_props['return_type'] = "DataclassPlaceholder" # Placeholder for later List/Optional wrapping
            required_imports.update(col_imports)
            required_imports.add("from dataclasses import dataclass")

    elif scalar_or_table_name_return:
        return_type_str = scalar_or_table_name_return
        if return_type_str.lower() == "void":
            logging.debug(f"  -> Function '{sql_name}' returns VOID")
            return_props['return_type'] = "None"
        elif return_type_str.lower() == "record":
            logging.debug(f"  -> Function '{sql_name}' returns RECORD")
            return_props['returns_record'] = True
            return_props['return_type'] = "Tuple"
            required_imports.add(PYTHON_IMPORTS["Tuple"])
        else:
            # Could be scalar, table_name, or SETOF table_name
            normalized_table_name = return_type_str.split(".")[-1]

            # --- NEW: Check if it's a known table name (even if not SETOF) ---
            if normalized_table_name in TABLE_SCHEMAS:
                logging.debug(
                    f"  -> Function '{sql_name}' returns TABLE TYPE: {return_type_str}. Found schema '{normalized_table_name}'."
                )
                return_props['return_columns'] = TABLE_SCHEMAS[normalized_table_name]
                # Use .get() for imports dict
                found_imports = TABLE_SCHEMA_IMPORTS.get(normalized_table_name, set())
                return_props['returns_table'] = True
                return_props['return_type'] = "DataclassPlaceholder" # Placeholder for later List/Optional wrapping
                required_imports.update(found_imports)
                required_imports.add("from dataclasses import dataclass")
                # If it was also SETOF, record the table name for generator logic
                if returns_setof:
                    return_props['setof_table_name'] = normalized_table_name
            # --- END NEW ---
            else:
                # Could be scalar or SETOF unknown_table_name
                py_return_type, ret_import = _map_sql_to_python_type(return_type_str)
                if py_return_type != "Any" or not returns_setof:
                    # Treat as scalar only if not found in TABLE_SCHEMAS
                    logging.debug(
                        f"  -> Function '{sql_name}' returns SCALAR or unknown type: {return_type_str} -> {py_return_type}"
                    )
                    return_props['return_type'] = py_return_type
                    if ret_import:
                        for imp in ret_import.split("\n"):
                            if imp:
                                required_imports.add(imp)
                else:
                    # Assume SETOF unknown type is SETOF table_name (schema not found)
                    # Keep existing logic for SETOF <unknown_table>
                    table_name_return = return_type_str # Original name
                    normalized_table_name = table_name_return.split(".")[-1] # Normalized name
                    logging.debug(
                        f"  -> Function '{sql_name}' returns SETOF {table_name_return}. Schema NOT FOUND for '{normalized_table_name}'."
                    )
                    return_props['setof_table_name'] = normalized_table_name # Still record the name

                    logging.warning(
                        f"    Schema not found for table '{normalized_table_name}'. Generating placeholder dataclass."
                    )
                    return_props['returns_table'] = True # Treat as table for placeholder generation
                    return_props['return_type'] = "DataclassPlaceholder"
                    # Generate minimal placeholder column info
                    return_props['return_columns'] = [
                        ReturnColumn(
                            name="unknown",
                            sql_type=table_name_return, # Use original name in placeholder
                            python_type="Any"
                        )
                    ]
                    required_imports.add(PYTHON_IMPORTS["Any"])
                    required_imports.add("from dataclasses import dataclass")
    else:
        # No explicit RETURNS TABLE or scalar/table name - should be rare
        logging.warning(f"Could not determine base return type for function '{sql_name}'. Assuming None.")
        return_props['return_type'] = "None"

    return return_props, required_imports


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

    # Split content into lines once for comment finding
    lines = sql_content.splitlines()

    # --- First Pass: Parse CREATE TABLE (from schema file first, then function file) ---
    if schema_content:  # Use original schema_content
        logging.info("Parsing CREATE TABLE statements from schema file...")
        _parse_create_table(schema_content)
    # Also parse tables from the main file in case they are defined there
    logging.info("Parsing CREATE TABLE statements from main functions file...")
    _parse_create_table(sql_content)  # Use original sql_content

    # --- Second Pass: Parse CREATE FUNCTION statements (only from main file) ---
    logging.info("Parsing CREATE FUNCTION statements...")

    # Use module-level regex here
    for match in FUNCTION_REGEX.finditer(sql_content):
        sql_name = match.group(1).strip()
        param_str = match.group(2).strip() if match.group(2) else ""
        returns_setof = bool(match.group(3))
        is_returns_table = bool(match.group(4))
        table_columns_str = match.group(5).strip() if is_returns_table and match.group(5) else ""
        scalar_or_table_name_return = match.group(6).strip() if match.group(6) else ""

        function_start_pos = match.start()
        # Calculate the line index corresponding to the start position
        function_start_line_idx = sql_content.count('\n', 0, function_start_pos)

        # Find and clean the preceding comment using the new helper function
        cleaned_comment = _find_preceding_comment(lines, function_start_line_idx)

        logging.info(f"Parsing function signature: {sql_name}")

        try:
            # Clean comments from param_str before parsing
            param_str_cleaned = re.sub(r"--.*?$", "", param_str, flags=re.MULTILINE)
            param_str_cleaned = re.sub(r"/\*.*?\*/", "", param_str_cleaned, flags=re.DOTALL)
            param_str_cleaned = " ".join(param_str_cleaned.split()) # Normalize whitespace
            params, param_imports = _parse_params(param_str_cleaned)
            required_imports = param_imports.copy()
            required_imports.add("from psycopg import AsyncConnection")

            # --- Parse RETURNS clause using helper --- 
            return_props, updated_imports = _parse_return_clause(match, required_imports)
            current_imports = updated_imports
            # --- End RETURNS clause parsing ---
            
            # Create ParsedFunction object and assign parsed properties
            func = ParsedFunction(
                sql_name=sql_name, 
                python_name=sql_name, # TODO: Pythonic name conversion for func?
                sql_comment=cleaned_comment,
                params=params,
                returns_setof=returns_setof, # Use original bool flag
                returns_table=return_props['returns_table'],
                return_columns=return_props['return_columns'],
                returns_record=return_props['returns_record'],
                setof_table_name=return_props['setof_table_name'],
                return_type="" # Will be set below
            )

            # --- Determine final Python return type hint --- 
            base_return_type = return_props['return_type'] # Get base type from helper

            final_return_type = base_return_type
            if returns_setof:
                if base_return_type != "None":
                    final_return_type = f"List[{base_return_type}]"
                    current_imports.add(PYTHON_IMPORTS["List"])
                # No else needed, SETOF None remains None (or should it be List[None]? No.)
            elif base_return_type != "None": 
                # Non-SETOF, non-None return types are Optional
                final_return_type = f"Optional[{base_return_type}]"
                current_imports.add("from typing import Optional")

            func.return_type = final_return_type
            # --- End final return type determination ---

            # --- Final import aggregation (ensure List/Optional/Tuple etc. are present if used) ---
            typing_imports_to_add = set()
            if "Optional[" in final_return_type:
                typing_imports_to_add.add("from typing import Optional")
            if "List[" in final_return_type:
                typing_imports_to_add.add(PYTHON_IMPORTS["List"])
            if "Tuple" in final_return_type: # Check base or final? Base seems safer.
                typing_imports_to_add.add(PYTHON_IMPORTS["Tuple"])
            if "Dict" in final_return_type:
                typing_imports_to_add.add(PYTHON_IMPORTS["Dict"])
            if "Any" in final_return_type:
                typing_imports_to_add.add(PYTHON_IMPORTS["Any"])
            current_imports.update(typing_imports_to_add)
            # --- End final import aggregation ---

            func.required_imports = {imp for imp in current_imports if imp}
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
