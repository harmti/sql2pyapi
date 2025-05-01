# ===== SECTION: IMPORTS =====
import re
import logging
from typing import Dict, List, Tuple, Set, Optional

# Import custom error classes
from ..errors import ParsingError

# Import the models
from ..sql_models import ReturnColumn

# Import type mapper
from .type_mapper import map_sql_to_python_type

# Import comment parser
from ..comment_parser import COMMENT_REGEX

# ===== SECTION: REGEX DEFINITIONS =====
# Regex for parsing column names in parse_column_definitions
COLUMN_NAME_REGEX = re.compile(r'^\s*(?:("[^"\n]+")|([a-zA-Z0-9_]+))\s*(.*)$')

# ===== SECTION: FUNCTIONS =====

def clean_and_split_column_fragments(col_defs_str: str) -> List[str]:
    """Cleans comments and splits column definition string into fragments."""
    if not col_defs_str:
        return []

    # 1. Split the ORIGINAL string by comma OR newline first
    # Corrected regex: Split on comma or escaped newline
    raw_fragments = re.split(r'[,\n]', col_defs_str)
    
    cleaned_fragments = []
    # 2. Clean comments from each fragment individually
    for fragment in raw_fragments:
        cleaned_part = COMMENT_REGEX.sub("", fragment).strip()
        if cleaned_part:
            cleaned_fragments.append(cleaned_part)
            
    return cleaned_fragments


def parse_single_column_fragment(current_def: str, columns: List[ReturnColumn], required_imports: Set[str], 
                                context: str, enum_types: Dict[str, List[str]] = None,
                                table_schemas: Dict[str, List] = None) -> Optional[ReturnColumn]:
    """Parses a single column definition fragment. Returns ReturnColumn or None if skipped."""
    
    # Default empty dictionaries if not provided
    enum_types = enum_types or {}
    table_schemas = table_schemas or {}
    
    # Skip constraint definitions
    if current_def.lower().startswith((
        "constraint", "primary key", "foreign key", 
        "unique", "check", "like", "index", "exclude"
    )):
        return None # Skipped

    # --- Attempt to merge fragments split inside parentheses (e.g., numeric(p, s)) ---
    scale_match = re.match(r"^(\d+)\s*\)?(.*)", current_def)
    if columns and scale_match:
        last_col = columns[-1]
        if last_col.sql_type.lower().startswith(("numeric(", "decimal(")) and ',' not in last_col.sql_type:
            scale_part = scale_match.group(1)
            remaining_constraint = scale_match.group(2).strip()
            merged_type = last_col.sql_type + ", " + scale_part + ")"
            last_col.sql_type = merged_type
            new_constraint_part = remaining_constraint.lower()
            last_col.is_optional = "not null" not in new_constraint_part and "primary key" not in new_constraint_part
            try:
                col_context = f"column '{last_col.name}'" + (f" in {context}" if context else "")
                py_type, imports = map_sql_to_python_type(merged_type, last_col.is_optional, col_context, enum_types, table_schemas)
                last_col.python_type = py_type # Update the existing column object
                required_imports.update(imports) # Update the main import set
            except Exception as e:
                logging.warning(str(e))
            return None # Fragment processed by merging, skip normal parsing

    # --- Match column name and the rest --- 
    name_regex = COLUMN_NAME_REGEX
    name_match = name_regex.match(current_def)
    if not name_match:
        error_msg = f"Could not extract column name from definition fragment: '{current_def}'"
        if context: error_msg += f" in {context}"
        logging.warning(error_msg)
        return None # Cannot parse name

    # Get the column name from either the quoted group (1) or the unquoted group (2)
    col_name = (name_match.group(1) or name_match.group(2)).strip('"')
    rest_of_def = name_match.group(3).strip()

    # --- Extract type and constraints --- 
    terminating_keywords = {
        "primary", "unique", "not", "null", "references",
        "check", "collate", "default", "generated"
    }
    type_parts = []
    words = rest_of_def.split()
    constraint_part_start_index = len(words)
    for j, word in enumerate(words):
        # Stop if a comment marker is found
        if word.startswith("--") or word.startswith("/*"):
            constraint_part_start_index = j
            break 
        word_lower = word.lower()
        is_terminator = False
        for keyword in terminating_keywords:
            if keyword == "not" and j + 1 < len(words) and words[j+1].lower() == "null":
                is_terminator = True; break
            if keyword == "null" and j > 0 and words[j-1].lower() == "not":
                continue # Handled by 'not null'
            if word_lower == keyword or word_lower.startswith(keyword + "("):
                is_terminator = True; break
        if is_terminator:
            constraint_part_start_index = j; break
        type_parts.append(word)

    if not type_parts:
        error_msg = f"Could not extract column type from definition: '{current_def}'"
        if context: error_msg += f" in {context}"
        logging.warning(error_msg)
        return None # Cannot parse type

    sql_type_extracted = " ".join(type_parts)
    constraint_part = " ".join(words[constraint_part_start_index:]).lower()

    # --- Determine optionality and map type --- 
    is_optional = "not null" not in constraint_part and "primary key" not in constraint_part
    
    # Special handling for ENUM types in table columns
    if sql_type_extracted in enum_types:
        # Convert enum_name to PascalCase for Python Enum class name
        enum_name = ''.join(word.capitalize() for word in sql_type_extracted.split('_'))
        py_type = enum_name
        required_imports.add('Enum')
    else:
        try:
            col_context = f"column '{col_name}'" + (f" in {context}" if context else "")
            py_type, imports = map_sql_to_python_type(sql_type_extracted, is_optional, col_context, enum_types, table_schemas)
            required_imports.update(imports) # Update main import set
        except Exception as e:
            logging.warning(str(e))
            py_type = "Any" if not is_optional else "Optional[Any]"
            required_imports.update({"Any", "Optional"} if is_optional else {"Any"})

    # --- Create and return column --- 
    return ReturnColumn(name=col_name, sql_type=sql_type_extracted, python_type=py_type, is_optional=is_optional)


def parse_column_definitions(col_defs_str: str, context: str = None,
                            enum_types: Dict[str, List[str]] = None,
                            table_schemas: Dict[str, List] = None,
                            composite_types: Dict[str, List] = None) -> Tuple[List[ReturnColumn], Set[str]]:
    """
    Parses column definitions from CREATE TABLE or RETURNS TABLE.
    Uses helper methods for cleaning/splitting and parsing fragments.
    
    Args:
        col_defs_str (str): The column definitions string
        context (str, optional): Context for error reporting
        enum_types (Dict[str, List[str]], optional): Dictionary of enum types
        table_schemas (Dict[str, List], optional): Dictionary of table schemas
        composite_types (Dict[str, List], optional): Dictionary of composite types
        
    Returns:
        Tuple[List[ReturnColumn], Set[str]]: The parsed columns and their imports
    """
    columns = []
    required_imports = set()
    
    fragments = clean_and_split_column_fragments(col_defs_str)
    
    if not fragments:
         return columns, required_imports

    # --- Parse Fragments using helper --- 
    for fragment in fragments:
        # Pass current columns list for potential modification (numeric scale merge)
        parsed_col = parse_single_column_fragment(fragment, columns, required_imports, context, enum_types, table_schemas)
        if parsed_col:
            columns.append(parsed_col)

    # --- Final check --- 
    col_defs_cleaned_check = COMMENT_REGEX.sub("", col_defs_str).strip() # Need a cleaned version for this check
    if not columns and col_defs_str.strip() and not col_defs_cleaned_check:
         pass 
    elif not columns and col_defs_cleaned_check:
         error_msg = f"Could not parse any columns from definition: '{col_defs_str[:100]}...' (Cleaned content: '{col_defs_cleaned_check.strip()[:100]}...')"
         if context: error_msg += f" in {context}"
         raise ParsingError(error_msg)

    return columns, required_imports
