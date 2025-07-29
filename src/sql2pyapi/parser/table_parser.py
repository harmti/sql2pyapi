# ===== SECTION: IMPORTS =====
import re
import logging
from typing import Dict, List, Tuple, Set

# Import custom error classes
from ..errors import ParsingError, TableParsingError

# Import the models
from ..sql_models import ReturnColumn

# Import column parser
from .column_parser import parse_column_definitions

# Import comment parser
from ..comment_parser import COMMENT_REGEX

# ===== SECTION: REGEX DEFINITIONS =====
# Regex for CREATE TABLE
TABLE_REGEX = re.compile(
    r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([a-zA-Z0-9_.]+)"  # 1: Table name
    r"\s*\("  # Opening parenthesis
    r"(.*?)"  # 2: Everything inside parenthesis (non-greedy)
    r"\)\s*(?:INHERITS|WITH|TABLESPACE|;)",  # Stop at known clauses after ) or semicolon
    re.IGNORECASE | re.DOTALL | re.MULTILINE,
)

# ===== SECTION: FUNCTIONS =====

def parse_create_table(sql_content: str, 
                      existing_table_schemas: Dict[str, List[ReturnColumn]] = None,
                      existing_table_schema_imports: Dict[str, Set[str]] = None,
                      enum_types: Dict[str, List[str]] = None,
                      composite_types: Dict[str, List[ReturnColumn]] = None) -> Tuple[Dict[str, List[ReturnColumn]], Dict[str, Set[str]]]:
    """
    Finds and parses CREATE TABLE statements, storing schemas in instance variables.
    
    Args:
        sql_content (str): SQL content to parse
        existing_table_schemas (Dict[str, List[ReturnColumn]], optional): Existing table schemas to update
        existing_table_schema_imports (Dict[str, Set[str]], optional): Existing table schema imports to update
        enum_types (Dict[str, List[str]], optional): Dictionary of enum types
        
    Returns:
        Tuple[Dict[str, List[ReturnColumn]], Dict[str, Set[str]]]: Updated table schemas and their imports
    """
    # Initialize or use existing dictionaries
    table_schemas = existing_table_schemas or {}
    table_schema_imports = existing_table_schema_imports or {}
    enum_types = enum_types or {}
    composite_types = composite_types or {}
    
    # Debug: Log the current state of table_schemas before parsing
    logging.debug(f"TABLE_SCHEMAS before parsing: {list(table_schemas.keys())}")
    logging.debug(f"TABLE_SCHEMA_IMPORTS before parsing: {list(table_schema_imports.keys())}")

    for match in TABLE_REGEX.finditer(sql_content):
        table_name = match.group(1).strip()
        col_defs_str = match.group(2).strip()

        # Further clean column defs: remove comments using COMMENT_REGEX
        col_defs_str_cleaned = COMMENT_REGEX.sub("", col_defs_str).strip()
        col_defs_str_cleaned = "\n".join(line.strip() for line in col_defs_str_cleaned.splitlines() if line.strip())

        logging.info(f"Found CREATE TABLE for: {table_name}")

        try:
            # Use method for parsing columns *within* the table definition
            # Pass the cleaned definition string
            columns, required_imports = parse_column_definitions(col_defs_str_cleaned, 
                                                              context=f"table {table_name}",
                                                              enum_types=enum_types,
                                                              table_schemas=table_schemas,
                                                              composite_types=composite_types) 
            if columns:
                # Store under both the normalized name and the fully qualified name
                normalized_table_name = table_name.split(".")[-1]

                # Store under normalized name (without schema)
                table_schemas[normalized_table_name] = columns
                table_schema_imports[normalized_table_name] = required_imports

                # Also store under the fully qualified name if it's different
                if table_name != normalized_table_name:
                    table_schemas[table_name] = columns
                    table_schema_imports[table_name] = required_imports
                    logging.debug(f"  -> Stored schema under both '{normalized_table_name}' and '{table_name}'")
                else:
                    logging.debug(f"  -> Parsed {len(columns)} columns for table {normalized_table_name}")

            else:
                # If parse_column_definitions returned empty list but input wasn't just comments, log warning
                if col_defs_str_cleaned:
                     logging.warning(
                         f"  -> No columns parsed for table {table_name} from definition: '{col_defs_str_cleaned[:100]}...'")
                else:
                     logging.debug(f"  -> Table {table_name} definition contained only comments or was empty.")
                    
        except ParsingError as e:
            # Re-raise with more context about the table
            raise TableParsingError(
                f"Failed to parse columns for table '{table_name}'",
                sql_snippet=col_defs_str_cleaned[:100] + "...",
                line_number=None  # We don't have line number information here
            ) from e
        except Exception as e:
            logging.exception(f"Failed to parse columns for table '{table_name}'.")
            # Re-raise as a specific parsing error instead of continuing
            raise TableParsingError(
                f"Failed to parse columns for table '{table_name}'",
                sql_snippet=col_defs_str_cleaned[:100] + "...",
                line_number=None  # We don't have line number information here
            ) from e
            
    return table_schemas, table_schema_imports
