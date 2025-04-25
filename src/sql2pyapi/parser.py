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

    # Handle both comma-separated (from RETURNS TABLE) and newline-separated (from CREATE TABLE)
    processed_defs = col_defs_str.replace(",", "\n")

    # Split definition block into individual lines, easier to process
    lines = processed_defs.splitlines()

    terminating_keywords = {
        "primary",
        "unique",
        "not",
        "null",
        "references",
        "check",
        "collate",
        "default",
        "generated",
        "constraint",
    }

    for line in lines:
        # Strip -- comments first, then strip whitespace
        line = re.sub(r"--.*?$", "", line).strip()
        if not line or line.lower().startswith(("constraint", "primary key", "foreign key", "unique", "check", "like")):
            continue  # Skip constraint definitions or LIKE clauses

        # Remove trailing comma if present (after comment removal)
        if line.endswith(","):
            line = line[:-1].strip()

        parts = line.split()  # Split by whitespace
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
                if part_lower == keyword or part_lower.startswith(keyword + "("):
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
                else:
                    continue  # Skip likely constraint line
            else:
                continue  # Skip short lines
            logging.debug(f"Trying fallback type parsing for column '{col_name}' in definition: '{line}'")
        else:
            sql_type = " ".join(type_parts)

        # Determine if the column is optional (nullable)
        # A column is considered optional unless 'NOT NULL' is explicitly present in its definition line.
        # We also consider PRIMARY KEY columns as not optional.
        line_lower = line.lower()
        is_optional = "not null" not in line_lower and "primary key" not in line_lower

        # Pass the determined optionality to the type mapping function
        py_type, import_stmt = _map_sql_to_python_type(sql_type, is_optional=is_optional)

        if import_stmt:
            for imp in import_stmt.split("\n"):
                if imp:
                    required_imports.add(imp)
        columns.append(ReturnColumn(name=col_name, sql_type=sql_type, python_type=py_type, is_optional=is_optional))

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

    param_regex = re.compile(
        r"\s*(?:(?:IN|OUT|INOUT)\s+)?([a-zA-Z0-9_]+)\s+([a-zA-Z0-9_.()\[\]]+(?:(?:\s*\(.*?\))?))"  # Name and Type (incl precision/array)
        r"(.*)",  # Capture the rest (potential default clause)
        re.IGNORECASE,
    )

    param_defs = param_str.split(",")

    for param_def in param_defs:
        param_def = param_def.strip()
        if not param_def:
            continue

        match = param_regex.match(param_def)
        if not match:
            logging.warning(f"Could not parse parameter definition: {param_def}")
            continue

        sql_name = match.group(1).strip()
        sql_type = match.group(2).strip()
        remainder = match.group(3).strip()

        is_optional = "default" in remainder.lower()

        # Generate Pythonic name
        python_name = sql_name
        if python_name.startswith("p_") and len(python_name) > 2:
            python_name = python_name[2:]
        elif python_name.startswith("_") and len(python_name) > 1:
            python_name = python_name[1:]
        # Add more prefix handling if needed

        py_type, import_stmts = _map_sql_to_python_type(sql_type, is_optional)

        if import_stmts:
            for imp in import_stmts.split("\n"):
                if imp:
                    required_imports.add(imp)

        params.append(
            SQLParameter(
                name=sql_name,
                python_name=python_name,  # Store pythonic name
                sql_type=sql_type,
                python_type=py_type,
                is_optional=is_optional,
            )
        )

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

    # --- First Pass: Parse CREATE TABLE (from schema file first, then function file) ---
    if schema_content:  # Use original schema_content
        logging.info("Parsing CREATE TABLE statements from schema file...")
        _parse_create_table(schema_content)
    # Also parse tables from the main file in case they are defined there
    logging.info("Parsing CREATE TABLE statements from main functions file...")
    _parse_create_table(sql_content)  # Use original sql_content

    # --- Second Pass: Parse CREATE FUNCTION statements (only from main file) ---
    logging.info("Parsing CREATE FUNCTION statements...")

    # Regex to find comments (both -- and /* */)
    comment_regex = re.compile(r"(--.*?$)|(/\\*.*?\\*/)", re.MULTILINE | re.DOTALL)
    # Find all comments and store their positions and content
    comments = []
    for comment_match in comment_regex.finditer(sql_content):
        start, end = comment_match.span()
        comment_text = comment_match.group(0)
        comments.append({"start": start, "end": end, "text": comment_text})

    function_regex = re.compile(
        r"CREATE(?:\s+OR\s+REPLACE)?\s+FUNCTION\s+([a-zA-Z0-9_.]+)"
        r"\s*\(([^)]*)\)"
        r"\s+RETURNS\s+(?:(SETOF)\s+)?(?:(TABLE)\s*\((.*?)\)|([a-zA-Z0-9_.()\[\]]+))"
        r"(.*?)(?:AS\s+\$\$|AS\s+\'|LANGUAGE\s+\w+)",
        re.IGNORECASE | re.DOTALL | re.MULTILINE,
    )

    # last_match_end removed from here, wasn't used before the loop
    for match in function_regex.finditer(sql_content):  # Use original sql_content here
        sql_name = match.group(1).strip()
        param_str = match.group(2).strip() if match.group(2) else ""
        returns_setof = bool(match.group(3))
        is_returns_table = bool(match.group(4))
        table_columns_str = match.group(5).strip() if is_returns_table and match.group(5) else ""
        scalar_or_table_name_return = match.group(6).strip() if match.group(6) else ""

        function_start_pos = match.start()

        # Find the comment block immediately preceding this function
        best_comment_block = []
        last_comment_end = -1

        for i in range(len(comments) - 1, -1, -1):  # Search backwards
            comment = comments[i]
            if comment["end"] <= function_start_pos:
                intervening_text = sql_content[
                    comment["end"] : function_start_pos if last_comment_end == -1 else last_comment_end
                ]
                if intervening_text.strip() == "":
                    best_comment_block.insert(0, comment["text"])
                    last_comment_end = comment["start"]
                else:
                    break
            elif comment["start"] >= function_start_pos:
                continue
            else:
                break

        best_comment = "\n".join(best_comment_block) if best_comment_block else None

        cleaned_comment = None
        if best_comment:
            lines = best_comment.strip().splitlines()
            if lines[0].strip().startswith("--"):
                cleaned_lines = []
                for line in lines:  # Process original lines
                    # Find the start of the actual comment text
                    dash_pos = line.find("--")
                    if dash_pos != -1:
                        content_start_pos = dash_pos + 2
                        # Remove one optional space after '--'
                        if content_start_pos < len(line) and line[content_start_pos] == " ":
                            content_start_pos += 1
                        # Append the rest of the line, preserving its original form
                        cleaned_lines.append(line[content_start_pos:])
                    else:
                        cleaned_lines.append(line)  # Keep lines not starting with -- as is?
                raw_comment = "\n".join(cleaned_lines)
                # Dedent the result to align with standard docstring formatting
                cleaned_comment = textwrap.dedent(raw_comment).strip("\n")

            elif lines[0].strip().startswith("/*"):
                # Extract content, dedent based on first line of content, keep internal formatting
                start_block_idx = best_comment.find("/*")
                end_block_idx = best_comment.rfind("*/")
                if start_block_idx != -1 and end_block_idx != -1 and end_block_idx > start_block_idx:
                    comment_content = best_comment[start_block_idx + 2 : end_block_idx]

                    # Check if lines consistently start with * (common block comment style)
                    content_lines = comment_content.splitlines()
                    consistent_star = True
                    if len(content_lines) > 1:
                        for line in content_lines[1:]:
                            stripped_line = line.strip()
                            if stripped_line and not stripped_line.startswith("*"):
                                consistent_star = False
                                break
                    else:
                        # Single line block comment, check if it starts with *
                        if content_lines and content_lines[0].strip().startswith("*"):
                            pass  # It's consistent for a single line
                        else:
                            consistent_star = False

                    processed_lines = []
                    if consistent_star:
                        # Strip leading * and optional space
                        for i, line in enumerate(content_lines):
                            lstripped_line = line.lstrip(" ")
                            if lstripped_line.startswith("*"):
                                star_pos = line.find("*")
                                content_start = star_pos + 1
                                if content_start < len(line) and line[content_start] == " ":
                                    content_start += 1
                                processed_lines.append(line[content_start:])
                            else:
                                processed_lines.append(line)  # Keep lines without star (e.g. first line?)
                        comment_content = "\n".join(processed_lines)
                    # else: keep original comment_content

                    # Dedent, but don't strip leading/trailing newlines from the block yet
                    dedented_content = textwrap.dedent(comment_content)
                    # Remove leading/trailing empty lines that might result from dedent/original formatting
                    cleaned_comment = dedented_content.strip("\n")
                else:
                    # Fallback: simple strip if block markers are weird
                    cleaned_comment = best_comment.strip()

        logging.info(f"Parsing function signature: {sql_name}")

        try:
            # Clean comments from param_str before parsing
            param_str_cleaned = re.sub(r"--.*?$", "", param_str, flags=re.MULTILINE)
            param_str_cleaned = re.sub(r"/\\*.*?\\*/", "", param_str_cleaned, flags=re.DOTALL)
            param_str_cleaned = " ".join(param_str_cleaned.split()) # Normalize whitespace
            params, param_imports = _parse_params(param_str_cleaned) # Use cleaned string
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
