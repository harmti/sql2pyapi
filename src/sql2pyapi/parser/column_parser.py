# ===== SECTION: IMPORTS =====
import logging
import re

# Import comment parser
from ..comment_parser import COMMENT_REGEX

# Import custom error classes
from ..errors import ParsingError

# Import the models
from ..sql_models import ReturnColumn

# Import type mapper
from .type_mapper import map_sql_to_python_type


# ===== SECTION: REGEX DEFINITIONS =====
# Regex for parsing column names in parse_column_definitions
COLUMN_NAME_REGEX = re.compile(r'^\s*(?:("[^"\n]+")|([a-zA-Z0-9_]+))\s*(.*)$')

# ===== SECTION: FUNCTIONS =====


def clean_and_split_column_fragments(col_defs_str: str) -> list[str]:
    """Cleans comments and splits column definition string into fragments."""
    if not col_defs_str:
        return []

    logging.debug(f"Original column definitions string:\n{col_defs_str}")

    # Split by lines first to handle each line separately
    lines = col_defs_str.splitlines()
    logging.debug(f"Split into {len(lines)} lines: {lines}")

    # Process each line to extract column definitions without comments
    processed_lines = []
    for i, line in enumerate(lines):
        # Skip empty lines
        if not line.strip():
            continue

        # Extract the column definition part before any comment
        parts = line.split("--", 1)[0].strip()
        if parts:
            processed_lines.append(parts)
            logging.debug(f"Line {i}: '{line}' -> '{parts}'")
        else:
            logging.debug(f"Line {i}: '{line}' -> EMPTY after comment removal")

    # Now split by commas, but only if they're not inside parentheses
    fragments = []

    # First join all processed lines with commas
    combined = ",".join(processed_lines)
    logging.debug(f"Combined processed lines: '{combined}'")

    # Then parse character by character to handle parentheses, quotes, and braces correctly
    current_fragment = ""
    paren_depth = 0
    in_single_quote = False
    in_double_quote = False
    brace_depth = 0

    i = 0
    while i < len(combined):
        char = combined[i]

        # Handle escapes in quotes
        if (in_single_quote or in_double_quote) and char == "\\" and i + 1 < len(combined):
            current_fragment += char + combined[i + 1]  # Add both escape and escaped char
            i += 2
            continue

        # Handle single quotes
        if char == "'" and not in_double_quote:
            in_single_quote = not in_single_quote
            current_fragment += char
        # Handle double quotes
        elif char == '"' and not in_single_quote:
            in_double_quote = not in_double_quote
            current_fragment += char
        # Handle parentheses (only when not in quotes)
        elif char == "(" and not in_single_quote and not in_double_quote:
            paren_depth += 1
            current_fragment += char
        elif char == ")" and not in_single_quote and not in_double_quote:
            paren_depth -= 1
            current_fragment += char
        # Handle braces (only when not in quotes)
        elif char == "{" and not in_single_quote and not in_double_quote:
            brace_depth += 1
            current_fragment += char
        elif char == "}" and not in_single_quote and not in_double_quote:
            brace_depth -= 1
            current_fragment += char
        # Handle commas - only split when not inside any nesting
        elif char == "," and paren_depth == 0 and brace_depth == 0 and not in_single_quote and not in_double_quote:
            if current_fragment.strip():
                fragments.append(current_fragment.strip())
                logging.debug(f"Found fragment: '{current_fragment.strip()}'")
            current_fragment = ""
        else:
            current_fragment += char

        i += 1

    # Don't forget the last fragment
    if current_fragment.strip():
        fragments.append(current_fragment.strip())
        logging.debug(f"Added final fragment: '{current_fragment.strip()}'")

    logging.debug(f"Final fragments: {fragments}")
    return fragments


def parse_single_column_fragment(
    current_def: str,
    columns: list[ReturnColumn],
    required_imports: set[str],
    context: str,
    enum_types: dict[str, list[str]] | None = None,
    table_schemas: dict[str, list] | None = None,
    composite_types: dict[str, list] | None = None,
) -> ReturnColumn | None:
    """Parses a single column definition fragment. Returns ReturnColumn or None if skipped."""

    # Default empty dictionaries if not provided
    enum_types = enum_types or {}
    table_schemas = table_schemas or {}
    composite_types = composite_types or {}

    # Skip constraint definitions
    if current_def.lower().startswith(
        ("constraint", "primary key", "foreign key", "unique", "check", "like", "index", "exclude")
    ):
        return None  # Skipped

    # --- Attempt to merge fragments split inside parentheses (e.g., numeric(p, s)) ---
    scale_match = re.match(r"^(\d+)\s*\)?(.*)", current_def)
    if columns and scale_match:
        last_col = columns[-1]
        if last_col.sql_type.lower().startswith(("numeric(", "decimal(")) and "," not in last_col.sql_type:
            scale_part = scale_match.group(1)
            remaining_constraint = scale_match.group(2).strip()
            merged_type = last_col.sql_type + ", " + scale_part + ")"
            last_col.sql_type = merged_type
            new_constraint_part = remaining_constraint.lower()
            last_col.is_optional = "not null" not in new_constraint_part and "primary key" not in new_constraint_part
            try:
                col_context = f"column '{last_col.name}'" + (f" in {context}" if context else "")
                # For composite types, don't make columns optional by default
                is_composite_type = context and "type " in context
                use_optional = last_col.is_optional and not is_composite_type
                py_type, imports = map_sql_to_python_type(
                    merged_type, use_optional, col_context, enum_types, table_schemas, composite_types
                )
                last_col.python_type = py_type  # Update the existing column object
                required_imports.update(imports)  # Update the main import set
            except Exception as e:
                logging.warning(str(e))
            return None  # Fragment processed by merging, skip normal parsing

    # --- Match column name and the rest ---
    name_regex = COLUMN_NAME_REGEX
    name_match = name_regex.match(current_def)
    if not name_match:
        error_msg = f"Could not extract column name from definition fragment: '{current_def}'"
        if context:
            error_msg += f" in {context}"
        logging.warning(error_msg)
        return None  # Cannot parse name

    # Get the column name from either the quoted group (1) or the unquoted group (2)
    col_name = (name_match.group(1) or name_match.group(2)).strip('"')
    rest_of_def = name_match.group(3).strip()

    # --- Extract type and constraints ---
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
            if keyword == "not" and j + 1 < len(words) and words[j + 1].lower() == "null":
                is_terminator = True
                break
            if keyword == "null" and j > 0 and words[j - 1].lower() == "not":
                continue  # Handled by 'not null'
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
        return None  # Cannot parse type

    sql_type_extracted = " ".join(type_parts)
    constraint_part = " ".join(words[constraint_part_start_index:]).lower()

    # --- Determine optionality and map type ---
    is_optional = "not null" not in constraint_part and "primary key" not in constraint_part

    # Check if this is a column in a composite type definition
    is_composite_type = context and "type " in context

    # For composite types, don't make columns optional by default (unless explicitly NULL)
    use_optional = is_optional and not is_composite_type

    # Special handling for ENUM types in table columns
    if sql_type_extracted in enum_types:
        # Convert enum_name to PascalCase for Python Enum class name
        enum_name = "".join(word.capitalize() for word in sql_type_extracted.split("_"))
        py_type = enum_name
        required_imports.add("Enum")
    else:
        try:
            col_context = f"column '{col_name}'" + (f" in {context}" if context else "")
            py_type, imports = map_sql_to_python_type(
                sql_type_extracted, use_optional, col_context, enum_types, table_schemas, composite_types
            )
            required_imports.update(imports)  # Update main import set
        except Exception as e:
            logging.warning(str(e))
            py_type = "Any" if not use_optional else "Optional[Any]"
            required_imports.update({"Any", "Optional"} if use_optional else {"Any"})

    # --- Create and return column ---
    return ReturnColumn(name=col_name, sql_type=sql_type_extracted, python_type=py_type, is_optional=is_optional)


def parse_column_definitions(
    col_defs_str: str,
    context: str | None = None,
    enum_types: dict[str, list[str]] | None = None,
    table_schemas: dict[str, list] | None = None,
    composite_types: dict[str, list] | None = None,
) -> tuple[list[ReturnColumn], set[str]]:
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
    # Initialize parameters to prevent None errors
    enum_types = enum_types or {}
    table_schemas = table_schemas or {}
    composite_types = composite_types or {}

    columns = []
    required_imports = set()

    fragments = clean_and_split_column_fragments(col_defs_str)

    if not fragments:
        return columns, required_imports

    # --- Parse Fragments using helper ---
    for fragment in fragments:
        # Pass current columns list for potential modification (numeric scale merge)
        parsed_col = parse_single_column_fragment(
            fragment, columns, required_imports, context, enum_types, table_schemas, composite_types
        )
        if parsed_col:
            columns.append(parsed_col)

    # --- Final check ---
    col_defs_cleaned_check = COMMENT_REGEX.sub("", col_defs_str).strip()  # Need a cleaned version for this check
    if not columns and col_defs_str.strip() and not col_defs_cleaned_check:
        pass
    elif not columns and col_defs_cleaned_check:
        error_msg = f"Could not parse any columns from definition: '{col_defs_str[:100]}...' (Cleaned content: '{col_defs_cleaned_check.strip()[:100]}...')"
        if context:
            error_msg += f" in {context}"
        raise ParsingError(error_msg)

    return columns, required_imports
