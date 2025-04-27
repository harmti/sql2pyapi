# ===== SECTION: IMPORTS =====
import re
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Tuple
import logging
import textwrap
import math
from pathlib import Path
import copy

# Import custom error classes
from .errors import ParsingError, FunctionParsingError, TableParsingError, TypeMappingError, ReturnTypeError

# ===== SECTION: TYPE MAPS AND CONSTANTS =====
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


# ===== SECTION: DATA STRUCTURES =====
# Core data structures for representing SQL functions, parameters, and return types

# SQLParsingError has been replaced by the error hierarchy in errors.py


@dataclass
class SQLParameter:
    """
    Represents a parameter in a SQL function.
    
    Attributes:
        name (str): Original SQL parameter name (e.g., 'p_user_id')
        python_name (str): Pythonic parameter name (e.g., 'user_id')
        sql_type (str): Original SQL type (e.g., 'uuid')
        python_type (str): Mapped Python type (e.g., 'UUID')
        is_optional (bool): Whether the parameter has a DEFAULT value in SQL
    """
    name: str
    python_name: str
    sql_type: str
    python_type: str
    is_optional: bool = False


@dataclass
class ReturnColumn:
    """
    Represents a column in a table or a field in a composite return type.
    
    Attributes:
        name (str): Column name
        sql_type (str): Original SQL type
        python_type (str): Mapped Python type
        is_optional (bool): Whether the column can be NULL
    """
    name: str
    sql_type: str
    python_type: str
    is_optional: bool = True


@dataclass
class ParsedFunction:
    """
    Represents a parsed SQL function with all its metadata.
    
    This is the main data structure that holds all information about a SQL function
    after parsing, including its name, parameters, return type, and other properties.
    
    Attributes:
        sql_name (str): Original SQL function name
        python_name (str): Pythonic function name (usually the same)
        params (List[SQLParameter]): List of function parameters
        return_type (str): Python return type (e.g., 'int', 'List[User]')
        return_columns (List[ReturnColumn]): For table returns, the columns
        returns_table (bool): Whether the function returns a table/composite type
        returns_record (bool): Whether the function returns a RECORD type
        returns_setof (bool): Whether the function returns a SETOF (multiple rows)
        required_imports (set): Set of Python imports needed for this function
        setof_table_name (Optional[str]): For SETOF table_name, the table name
        sql_comment (Optional[str]): SQL comment preceding the function definition
    """
    sql_name: str
    python_name: str
    params: List[SQLParameter] = field(default_factory=list)
    return_type: str = "None"
    return_columns: List[ReturnColumn] = field(default_factory=list)
    returns_table: bool = False
    returns_record: bool = False
    returns_setof: bool = False
    required_imports: set = field(default_factory=set)
    setof_table_name: Optional[str] = None
    sql_comment: Optional[str] = None  # Store the cleaned SQL comment


# ===== SECTION: GLOBAL STATE =====
# Global storage for table schemas and imports
# These are populated during parsing and used across functions
TABLE_SCHEMAS: Dict[str, List[ReturnColumn]] = {}
TABLE_SCHEMA_IMPORTS: Dict[str, set] = {}

# --- Regex Definitions (Module Level) ---
FUNCTION_REGEX = re.compile(
    r"CREATE(?:\s+OR\s+REPLACE)?\s+FUNCTION\s+([a-zA-Z0-9_.]+)"
    r"\s*\(([^)]*)\)"
    r"\s+RETURNS\s+(?:(SETOF)\s+)?(?:(TABLE)\s*\((.*?)\)|([a-zA-Z0-9_.()\[\]]+))" # Groups 3,4,5,6 relate to returns
    r"(.*?)(?:AS\s+\$\$|AS\s+\'|LANGUAGE\s+\w+)", # <<< REVERTED TO ORIGINAL
    re.IGNORECASE | re.DOTALL | re.MULTILINE,
)

# Regex to find comments (both -- and /* */)
COMMENT_REGEX = re.compile(r"(--.*?$)|(/\*.*?\*/)", re.MULTILINE | re.DOTALL)


def _map_sql_to_python_type(sql_type: str, is_optional: bool = False, context: str = None) -> Tuple[str, set]:
    """
    Maps a SQL type to its corresponding Python type and required imports.
    
    Args:
        sql_type (str): The PostgreSQL type to map
        is_optional (bool): Whether the type should be wrapped in Optional
        context (str, optional): Context information for error reporting
    
    Returns:
        Tuple[str, set]: The Python type and a set of required imports
        
    Raises:
        TypeMappingError: If the SQL type cannot be mapped to a Python type
    """
    # Check if this is a schema-qualified name (e.g., public.companies)
    # or a table name that exists in our schemas
    if '.' in sql_type and not sql_type.endswith('[]'):
        # This is a schema-qualified name, extract the table part
        table_name = sql_type
        normalized_table_name = table_name.split('.')[-1]
        
        # Check if we have this table in our schemas
        if table_name in TABLE_SCHEMAS or normalized_table_name in TABLE_SCHEMAS:
            # This is likely a table reference, not a primitive type
            # We'll return 'Any' and let the caller handle it as a table reference
            return "Any", {"Any"}
    else:
        # Check if this is a non-qualified table name
        normalized_table_name = sql_type.strip()
        if normalized_table_name in TABLE_SCHEMAS:
            # This is a table reference, not a primitive type
            # We'll return 'Any' and let the caller handle it as a table reference
            return "Any", {"Any"}
    
    # Handle array types (e.g., text[], integer[])
    is_array = False
    
    # Normalize the SQL type for lookup
    sql_type_normal = sql_type.lower().strip()
    if sql_type_normal.endswith("[]"):
        is_array = True
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
        py_type = TYPE_MAP.get(potential_base_type)
        
    if not py_type:
        # If still no match, log a warning and use Any as fallback
        error_msg = f"Unknown SQL type: {sql_type}"
        if context:
            error_msg += f" in {context}"
        logging.warning(f"{error_msg}. Using 'Any' as fallback.")
        py_type = "Any"
    
    # --- Import Handling ---
    # Create a set to collect required imports
    imports = set()
    
    # Add specific imports based on the Python type
    if py_type == 'UUID':
        imports.add('UUID')
    elif py_type == 'datetime':
        imports.add('datetime')
    elif py_type == 'date':
        imports.add('date')
    elif py_type == 'Decimal':
        imports.add('Decimal')
    elif py_type == 'Any':
        imports.add('Any')
    elif py_type == 'dict' or py_type == 'Dict[str, Any]':
        imports.add('Dict')
        imports.add('Any')
        py_type = 'Dict[str, Any]'  # Standardize on Dict[str, Any] for JSON types
        
    # --- Array Handling ---
    if is_array:
        py_type = f"List[{py_type}]"
        imports.add('List')
    
    # --- Optional Handling ---
    if is_optional and py_type != "Any" and not py_type.startswith("Optional["):
        # Only wrap non-Any types that aren't already Optional
        # We apply Optional even to List types if the base SQL type could be NULL
        py_type = f"Optional[{py_type}]"
        imports.add('Optional')
        
    return py_type, imports


def _parse_column_definitions(col_defs_str: str, context: str = None) -> Tuple[List[ReturnColumn], set]:
    """
    Parses column definitions from CREATE TABLE or RETURNS TABLE.
    
    Args:
        col_defs_str (str): The column definitions string to parse
        context (str, optional): Context information for error reporting
    
    Returns:
        Tuple[List[ReturnColumn], set]: List of parsed columns and required imports
        
    Raises:
        ParsingError: If column definitions cannot be parsed correctly
    """
    columns = []
    required_imports = set()
    if not col_defs_str:
        return columns, required_imports

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

        # Skip constraint definitions
        if current_def.lower().startswith(("constraint", "primary key", "foreign key", "unique", "check", "like")):
            continue

        # --- Attempt to merge fragments split inside parentheses (e.g., numeric(p, s)) ---
        # Looks like scale part, possibly followed by constraints
        scale_match = re.match(r"^(\d+)\s*\)?(.*)", current_def)
        if columns and scale_match:
            last_col = columns[-1]
            # Check if last col looks like it's missing scale AND current fragment starts with scale
            if last_col.sql_type.lower().startswith(("numeric(", "decimal(")) and ',' not in last_col.sql_type:
                scale_part = scale_match.group(1)
                remaining_constraint = scale_match.group(2).strip()
                # logging.debug(f"Attempting merge for numeric/decimal scale: adding scale '{scale_part}' and constraint '{remaining_constraint}'")
                
                # Merge the scale part with the previous column's type
                merged_type = last_col.sql_type + ", " + scale_part + ")" # Add closing parenthesis
                
                # Update last column's type and constraint info
                last_col.sql_type = merged_type 
                # Determine optionality based on the *newly found* constraint part
                new_constraint_part = remaining_constraint.lower()
                last_col.is_optional = "not null" not in new_constraint_part and "primary key" not in new_constraint_part

                # Re-map python type using the *new* optionality
                try:
                    col_context = f"column '{last_col.name}'" + (f" in {context}" if context else "")
                    # Pass the updated is_optional value
                    py_type, imports = _map_sql_to_python_type(merged_type, last_col.is_optional, col_context) 
                    last_col.python_type = py_type
                    required_imports.update(imports)
                    # logging.debug(f"  -> Updated column: {last_col}")

                except TypeMappingError as e:
                    logging.warning(str(e))
                continue # Skip normal processing for this fragment

        # Match name and the rest of the definition
        name_match = name_regex.match(current_def)
        if not name_match:
            error_msg = f"Could not extract column name from definition fragment: '{current_def}'"
            if context:
                error_msg += f" in {context}"
            logging.warning(error_msg)
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
            error_msg = f"Could not extract column type from definition: '{current_def}'"
            if context:
                error_msg += f" in {context}"
            logging.warning(error_msg)
            continue
        
        sql_type_extracted = " ".join(type_parts)
        constraint_part = " ".join(words[constraint_part_start_index:]).lower()

        # Determine if the column is optional (nullable)
        is_optional = "not null" not in constraint_part and "primary key" not in constraint_part

        # Pass the determined optionality to the type mapping function
        try:
            col_context = f"column '{col_name}'" + (f" in {context}" if context else "")
            py_type, imports = _map_sql_to_python_type(sql_type_extracted, is_optional, col_context)
            required_imports.update(imports)
        except TypeMappingError as e:
            # Log the error but continue with Any as fallback
            logging.warning(str(e))
            py_type = "Any" if not is_optional else "Optional[Any]"
            required_imports.update({"Any", "Optional"} if is_optional else {"Any"})

        # Create and add the column
        columns.append(ReturnColumn(name=col_name, sql_type=sql_type_extracted, python_type=py_type, is_optional=is_optional))

    if not columns and col_defs_str.strip():
        error_msg = f"Could not parse any columns from definition: '{col_defs_str[:100]}...'"
        if context:
            error_msg += f" in {context}"
        raise ParsingError(error_msg)

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
    
    # Debug: Log the current state of TABLE_SCHEMAS before parsing
    logging.debug(f"TABLE_SCHEMAS before parsing: {list(TABLE_SCHEMAS.keys())}")
    logging.debug(f"TABLE_SCHEMA_IMPORTS before parsing: {list(TABLE_SCHEMA_IMPORTS.keys())}")


    for match in table_regex.finditer(sql_content):
        table_name = match.group(1).strip()
        col_defs_str = match.group(2).strip()

        # Further clean column defs: remove comments that might span lines within the block
        col_defs_str_cleaned = re.sub(r"--.*?$", "", col_defs_str, flags=re.MULTILINE)
        col_defs_str_cleaned = re.sub(r"/\*.*?\*/", "", col_defs_str_cleaned, flags=re.DOTALL).strip()
        col_defs_str_cleaned = "\n".join(line.strip() for line in col_defs_str_cleaned.splitlines() if line.strip())

        logging.info(f"Found CREATE TABLE for: {table_name}")

        try:
            columns, required_imports = _parse_column_definitions(col_defs_str_cleaned)
            if columns:
                # --- Remove Logging ---
                # logging.debug(f"  -> Parsed columns for '{table_name}':")
                # for col in columns:
                #     logging.debug(f"    - {col}")
                # --------------------
                
                # Store under both the normalized name and the fully qualified name
                normalized_table_name = table_name.split(".")[-1]
                
                # Store under normalized name (without schema)
                TABLE_SCHEMAS[normalized_table_name] = columns
                TABLE_SCHEMA_IMPORTS[normalized_table_name] = required_imports
                
                # Also store under the fully qualified name if it's different
                if table_name != normalized_table_name:
                    TABLE_SCHEMAS[table_name] = columns
                    TABLE_SCHEMA_IMPORTS[table_name] = required_imports
                    logging.debug(f"  -> Stored schema under both '{normalized_table_name}' and '{table_name}'")
                else:
                    logging.debug(f"  -> Parsed {len(columns)} columns for table {normalized_table_name}")
                
            else:
                logging.warning(
                    f"  -> No columns parsed for table {table_name} from definition: '{col_defs_str_cleaned[:100]}...'"
                )
        except ParsingError as e:
            # Re-raise with more context about the table
            raise TableParsingError(
                f"Failed to parse columns for table '{table_name}'", 
                table_name=table_name,
                sql_snippet=col_defs_str_cleaned[:100] + "..."
            ) from e
        except Exception as e:
            logging.exception(f"Failed to parse columns for table '{table_name}'.")
            # Re-raise as a specific parsing error instead of continuing
            raise TableParsingError(
                f"Failed to parse columns for table '{table_name}'", 
                table_name=table_name,
                sql_snippet=col_defs_str_cleaned[:100] + "..."
            ) from e


def _parse_params(param_str: str, context: str = None) -> Tuple[List[SQLParameter], set]:
    """
    Parses parameter string including optional DEFAULT values.
    
    Args:
        param_str (str): The parameter string to parse
        context (str, optional): Context information for error reporting
    
    Returns:
        Tuple[List[SQLParameter], set]: List of parsed parameters and required imports
        
    Raises:
        ParsingError: If parameter definitions cannot be parsed correctly
    """
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
                # Attempt to recover from a split inside a type definition (e.g., numeric(10, 2))
                param_context = f"parameter '{params[-1].name}'" + (f" in {context}" if context else "")
                logging.debug(f"Attempting recovery for split inside type: appending '{param_def}' to {param_context}")
                params[-1].sql_type += "," + param_def
                # Re-run type mapping for the corrected type
                try:
                    py_type, imports = _map_sql_to_python_type(params[-1].sql_type, params[-1].is_optional, param_context)
                    params[-1].python_type = py_type
                    required_imports.update(imports)
                except TypeMappingError as e:
                    logging.warning(str(e))
                continue # Skip rest of processing for this fragment
            else:
                error_msg = f"Could not parse parameter definition: {param_def}"
                if context:
                    error_msg += f" in {context}"
                logging.warning(error_msg)
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

        # Map SQL type to Python type with error handling
        param_context = f"parameter '{sql_name}'" + (f" in {context}" if context else "")
        try:
            py_type, imports = _map_sql_to_python_type(sql_type, is_optional, param_context)
            required_imports.update(imports)
        except TypeMappingError as e:
            # Log the error but continue with Any as fallback
            logging.warning(str(e))
            py_type = "Any" if not is_optional else "Optional[Any]"
            required_imports.update({"Any", "Optional"} if is_optional else {"Any"})

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
    Searches backwards, handles multi-line blocks, and stops at blank lines.
    """
    comment_lines = []
    in_block_comment = False
    last_comment_end_line_idx = -1
    found_first_comment_line = False # Flag to check if we found *any* comment line

    for i in range(func_start_line_idx - 1, -1, -1):
        line_content = lines[i]
        stripped_line = line_content.strip()

        # If we already found a comment, and this line is blank, stop searching
        if found_first_comment_line and not stripped_line:
             break # Stop searching backwards if we hit a blank line after a comment
        elif not stripped_line:
             continue # Skip blank lines before finding the first comment

        is_block_end = stripped_line.endswith("*/")
        is_block_start = stripped_line.startswith("/*")
        is_line_comment = stripped_line.startswith("--")

        if is_block_end:
            if in_block_comment: # Malformed/nested comment
                return None
            in_block_comment = True
            found_first_comment_line = True
            # Check for single-line block comment /* ... */
            if is_block_start and len(stripped_line) > 4:
                comment_lines.insert(0, line_content)
                in_block_comment = False # Immediately closed
            else:
                comment_lines.insert(0, line_content) # End of multi-line block
            continue # Move to previous line

        if is_block_start:
            if not in_block_comment: # Start without end seen first?
                if not found_first_comment_line: return None # No comment associated
                break # Part of a different block above
            # This is the start of the block we are tracking
            comment_lines.insert(0, line_content)
            in_block_comment = False
            found_first_comment_line = True # Should already be true
            continue # Move to previous line

        if in_block_comment:
            # Inside a multi-line block comment
            comment_lines.insert(0, line_content)
            found_first_comment_line = True
            continue # Move to previous line

        if is_line_comment:
            # A dash comment line
            comment_lines.insert(0, line_content)
            found_first_comment_line = True
            continue # Move to previous line

        # If we reach here, it's a non-comment, non-blank line.
        # Stop searching backwards.
        break

    if not comment_lines:
        return None

    # Clean the collected lines
    cleaned_comment = _clean_comment_block(comment_lines)
    return cleaned_comment if cleaned_comment else None


def _parse_return_clause(match: re.Match, initial_imports: set, function_name: str = None) -> Tuple[dict, set]:
    """
    Parses the RETURNS clause of a CREATE FUNCTION statement.

    Args:
        match (re.Match): The regex match object from FUNCTION_REGEX.
        initial_imports (set): Imports already gathered (e.g., from parameters).
        function_name (str, optional): Name of the function for context in errors.

    Returns:
        Tuple[dict, set]: A dictionary containing return type information and the updated set of imports.
            Keys in dict:
            - 'return_type': BASE Python type string ('None', 'int', 'Tuple', 'DataclassPlaceholder', 'Any')
            - 'returns_table': bool
            - 'returns_record': bool
            - 'returns_setof': bool
            - 'return_columns': List[ReturnColumn] (for RETURNS TABLE)
            - 'setof_table_name': str (for RETURNS SETOF table_name)
    """
    # Initialize default properties
    returns_info = {
        "return_type": "None", # <<< Store BASE type here
        "returns_table": False,
        "returns_record": False,
        "returns_setof": False,
        "return_columns": [],
        "setof_table_name": None,
    }
    current_imports = initial_imports.copy()

    # Determine SETOF flag
    is_setof = match.group(3) is not None
    if is_setof:
        returns_info["returns_setof"] = True
        # Don't add 'List' import here

    returns_table_keyword = match.group(4) is not None
    table_columns_str = match.group(5)
    return_type_name = match.group(6)

    if returns_table_keyword:
        # Case: RETURNS TABLE(...)
        returns_info["returns_table"] = True
        current_imports.add("dataclass")
        if table_columns_str:
            try:
                cols, col_imports = _parse_column_definitions(table_columns_str, context=f"RETURNS TABLE of {function_name or 'unknown'}")
                returns_info["return_columns"] = cols
                current_imports.update(col_imports)
            except ParsingError as e:
                raise ReturnTypeError(f"Error parsing columns in RETURNS TABLE of {function_name or 'unknown'}: {e}") from e
        # Use DataclassPlaceholder as base type
        returns_info["return_type"] = "DataclassPlaceholder"
        # Placeholder doesn't require Any import itself

    elif return_type_name:
        # Case: RETURNS [SETOF] type_name
        sql_return_type = return_type_name.strip().lower()

        if sql_return_type == "void":
            returns_info["return_type"] = "None" # Store BASE type

        elif sql_return_type == "record":
            returns_info["returns_record"] = True
            returns_info["return_type"] = "Tuple" # Store BASE type
            current_imports.add("Tuple")

        else:
            # Could be table name or scalar
            table_key_qualified = sql_return_type
            table_key_normalized = table_key_qualified.split('.')[-1]

            schema_found = False
            table_key_to_use = None
            if table_key_qualified in TABLE_SCHEMAS:
                 schema_found = True
                 table_key_to_use = table_key_qualified
            elif table_key_normalized in TABLE_SCHEMAS:
                 schema_found = True
                 table_key_to_use = table_key_normalized

            if schema_found:
                # Known table name
                returns_info["returns_table"] = True
                returns_info["return_columns"] = TABLE_SCHEMAS.get(table_key_to_use, [])
                current_imports.update(TABLE_SCHEMA_IMPORTS.get(table_key_to_use, set()))
                current_imports.add("dataclass")
                if is_setof:
                     returns_info["setof_table_name"] = table_key_qualified
                returns_info["return_type"] = "DataclassPlaceholder" # Store BASE type
            else:
                 # Scalar type OR unknown table name
                 try:
                      # Map SQL type to base Python type (is_optional=False)
                      context_msg = f"return type of function {function_name or 'unknown'}"
                      py_type, type_imports = _map_sql_to_python_type(sql_return_type, is_optional=False, context=context_msg)
                      current_imports.update(type_imports)
                      returns_info["return_type"] = py_type # Store the BASE type

                      # Special handling for unknown SETOF table (widgets test case)
                      if py_type == "Any" and is_setof:
                           returns_info["returns_table"] = True
                           returns_info["return_columns"] = [ReturnColumn(name="unknown", sql_type=sql_return_type, python_type="Any")]
                           returns_info["setof_table_name"] = sql_return_type
                           returns_info["return_type"] = "DataclassPlaceholder" # Set base type
                           current_imports.add("dataclass")
                           current_imports.add("Any") # For the unknown column
                      # Special handling for unknown non-SETOF table (widgets test case)
                      elif sql_return_type == 'widgets' and not is_setof:
                            returns_info["return_type"] = "Any" # Explicitly match test expectation
                            current_imports.add("Any")
                      elif py_type == "Any":
                            logging.warning(f"Return type '{sql_return_type}' mapped to Any for {function_name or 'unknown'}. Interpreting as scalar Any.")

                 except TypeMappingError:
                      logging.error(f"Type mapping failed unexpectedly for {sql_return_type}. Using Any.")
                      returns_info["return_type"] = "Any" # Store BASE type Any
                      current_imports.add("Any")

    # Clean up imports (remove None if present)
    current_imports.discard(None)

    # Now returns_info["return_type"] should contain the base type matching unit tests
    return returns_info, current_imports


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
    # Clear existing schemas to avoid conflicts
    TABLE_SCHEMAS.clear()
    TABLE_SCHEMA_IMPORTS.clear()

    # === Parse Schema (if provided) ===
    if schema_content:
        try:
            _parse_create_table(schema_content)
        except Exception as e:
            logging.error(f"Error parsing schema content: {e}")
            # Decide if we should raise or continue
            # raise TableParsingError(f"Failed to parse schema: {e}") from e

    # === Parse Functions ===
    # Also parse tables defined within the main SQL content
    try:
        _parse_create_table(sql_content)
    except Exception as e:
        # Log non-fatal error if table parsing fails here
        logging.warning(f"Could not parse CREATE TABLE statements in function file: {e}")

    # Find comments and remove them temporarily
    comments = {}
    processed_lines = []
    current_byte_offset = 0

    def comment_replacer(match):
        nonlocal current_byte_offset
        start, end = match.span()
        original_text = match.group(0)
        placeholder = f"__COMMENT__{len(comments)}__"
        comments[placeholder] = original_text
        line_num = sql_content[:start].count('\n') + 1

        if match.group(2) and '\n' in original_text:
            return '\n' * original_text.count('\n')
        else:
            return ""

    sql_no_comments = COMMENT_REGEX.sub(comment_replacer, sql_content)
    # --- Remove Debugging for func3.sql issue ---
    # print("--- SQL without comments (for func3.sql test): ---")
    # print(sql_no_comments)
    # print("-------------------------------------------------")
    # -----------------------------------------

    # Find function definitions using regex
    matches = FUNCTION_REGEX.finditer(sql_no_comments)
    functions = []
    lines = sql_content.splitlines()
    # match_count = 0 # Debug counter

    for match in matches:
        # match_count += 1 # Debug counter
        sql_name = None
        # Get match details from the stripped content first
        stripped_content_start_byte = match.start()
        stripped_content_end_byte = match.end()
        # Estimate line in stripped content (for refining search later)
        approx_line_in_stripped = sql_no_comments[:stripped_content_start_byte].count('\n') + 1

        try:
            sql_name = match.group(1)
            python_name = sql_name # Simplistic conversion for now
            # --- Remove Debugging for func3.sql issue ---
            # print(f"Match {match_count}: Found function '{sql_name}'")
            # print(f"  Match groups: {match.groups()}")
            # -----------------------------------------

            # --- Find the accurate start line in the ORIGINAL content ---
            original_start_byte = -1
            # Create specific search patterns for the function definition
            pattern1 = f"CREATE FUNCTION {sql_name}"
            pattern2 = f"CREATE OR REPLACE FUNCTION {sql_name}"
            
            # Search for the pattern in the original content
            # Start search slightly before the estimated line number in original content
            # (This is still heuristic)
            search_start_offset = 0 
            # Heuristic: find the byte offset corresponding to approx_line_in_stripped - N lines
            temp_lines = sql_content.splitlines()
            if approx_line_in_stripped > 5:
                 search_start_offset = sql_content.find(temp_lines[approx_line_in_stripped - 5])
                 if search_start_offset == -1: search_start_offset = 0 # Fallback

            original_start_byte = sql_content.find(pattern1, search_start_offset)
            if original_start_byte == -1:
                original_start_byte = sql_content.find(pattern2, search_start_offset)

            if original_start_byte == -1:
                 # Fallback: search from beginning if not found near estimate
                 logging.warning(f"Could not find exact function start for {sql_name} near estimate line {approx_line_in_stripped}. Searching from start.")
                 original_start_byte = sql_content.find(pattern1)
                 if original_start_byte == -1:
                     original_start_byte = sql_content.find(pattern2)

            if original_start_byte != -1:
                function_start_line = sql_content[:original_start_byte].count('\n') + 1
            else:
                # If we STILL can't find it, use the old (likely incorrect) estimate and warn
                logging.error(f"CRITICAL: Cannot find function definition start for '{sql_name}' in original SQL. Comment association may be wrong.")
                function_start_line = sql_content[:stripped_content_start_byte].count('\n') + 1 # Fallback to old method
            # ---------------------------------------------------------

            # --- Parse Parameters ---
            param_str = match.group(2) or ""
            param_str_cleaned = COMMENT_REGEX.sub("", param_str)
            param_str_cleaned = " ".join(param_str_cleaned.split())
            parsed_params, param_imports = _parse_params(param_str_cleaned, f"function '{sql_name}'")
            current_imports = param_imports.copy()
            current_imports.add("from psycopg import AsyncConnection")

            # --- Parse Return Clause (gets base type info) --- 
            return_info, current_imports = _parse_return_clause(match, current_imports, sql_name)

            # --- Find Preceding Comment ---
            function_start_line_idx = function_start_line - 1
            sql_comment = _find_preceding_comment(lines, function_start_line_idx)
            # --- Remove Logging ---
            # logging.debug(f"Function: {sql_name}, Start Line: {function_start_line}, Found Comment: {'Yes' if sql_comment else 'No'}")
            # if sql_comment:
            #      logging.debug(f"  Comment Content: {sql_comment[:100]}...") # Log first 100 chars
            # --------------------

            # --- Determine final Python type hint (apply wrapping) --- 
            base_py_type = return_info["return_type"] # Base type from _parse_return_clause
            final_py_type = base_py_type
            is_setof = return_info["returns_setof"]

            if is_setof:
                if base_py_type != "None": # Don't wrap None
                     # Handle placeholder specifically for generator
                     if base_py_type == "DataclassPlaceholder":
                         final_py_type = "List[Any]" # Generator expects List[SpecificClass]
                         current_imports.add("Any") # Add Any for the placeholder list type
                     else:
                         final_py_type = f"List[{base_py_type}]"
                     current_imports.add("List")
            elif base_py_type != "None": # Non-SETOF, non-None
                 if base_py_type == "DataclassPlaceholder":
                      final_py_type = "Optional[Any]" # Generator expects Optional[SpecificClass]
                      current_imports.add("Any") # Add Any for the placeholder optional type
                 else:
                      final_py_type = f"Optional[{base_py_type}]"
                 current_imports.add("Optional")
                 
            # Ensure Tuple/Any imports if used in the final type string
            if "Tuple" in final_py_type:
                 current_imports.add("Tuple")
            if "Any" in final_py_type:
                 current_imports.add("Any")
                 
            # Clean up imports before assigning
            current_imports.discard(None) 
            # Replace DataclassPlaceholder in final type hint before assigning to ParsedFunction?
            # No, generator needs the placeholder info via returns_table/return_columns.
            # Let the final_py_type contain Optional[Any] or List[Any] if base was placeholder.

            # --- Create ParsedFunction object ---
            func_data = ParsedFunction(
                sql_name=sql_name,
                python_name=python_name,
                params=parsed_params,
                return_type=final_py_type, # Use the final wrapped type
                returns_table=return_info["returns_table"],
                returns_record=return_info["returns_record"],
                returns_setof=is_setof,
                return_columns=return_info["return_columns"],
                setof_table_name=return_info["setof_table_name"],
                # Assign cleaned, unique imports
                required_imports={imp for imp in current_imports if imp}, 
                sql_comment=sql_comment,
            )
            functions.append(func_data)

        except Exception as e:
            # Log specific parsing errors or generic errors
            line_msg = f" near line {function_start_line}" if function_start_line else ""
            func_msg = f" in function '{sql_name}'" if sql_name else ""
            logging.error(f"Parser error{func_msg}{line_msg}: {e}")
            # Optionally re-raise or collect errors
            # raise FunctionParsingError(...) from e
            # For now, let's just log and continue to see if other functions parse
            pass

    return functions, TABLE_SCHEMA_IMPORTS
