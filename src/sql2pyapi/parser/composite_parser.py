# ===== SECTION: IMPORTS =====
import re
import logging
from typing import Dict, List, Tuple, Set, Optional

# Import custom error classes
from ..errors import ParsingError, TypeMappingError

# Import the models
from ..sql_models import ReturnColumn

# Import column parser
from .column_parser import parse_column_definitions

# Import comment parser
from ..comment_parser import COMMENT_REGEX

# ===== SECTION: REGEX DEFINITIONS =====
# Regex for CREATE TYPE name AS (...)
TYPE_REGEX = re.compile(
    r"CREATE\s+TYPE\s+([a-zA-Z0-9_.]+)"  # 1: Type name
    r"\s+AS\s*\("  # AS (
    r"(.*?)" # 2: Everything inside parenthesis (non-greedy)
    r"\)"        # Closing parenthesis
    , re.IGNORECASE | re.DOTALL | re.MULTILINE
)

# ===== SECTION: FUNCTIONS =====

def parse_create_type(sql_content: str, 
                     existing_composite_types: Dict[str, List[ReturnColumn]] = None,
                     existing_composite_type_imports: Dict[str, Set[str]] = None,
                     enum_types: Dict[str, List[str]] = None,
                     table_schemas: Dict[str, List[ReturnColumn]] = None) -> Tuple[Dict[str, List[ReturnColumn]], Dict[str, Set[str]]]:
    """
    Finds and parses CREATE TYPE name AS (...) statements.
    
    Args:
        sql_content (str): SQL content to parse
        existing_composite_types (Dict[str, List[ReturnColumn]], optional): Existing composite types to update
        existing_composite_type_imports (Dict[str, Set[str]], optional): Existing composite type imports to update
        enum_types (Dict[str, List[str]], optional): Dictionary of enum types
        table_schemas (Dict[str, List[ReturnColumn]], optional): Dictionary of table schemas
        
    Returns:
        Tuple[Dict[str, List[ReturnColumn]], Dict[str, Set[str]]]: Updated composite types and their imports
    """
    # Initialize or use existing dictionaries
    composite_types = existing_composite_types or {}
    composite_type_imports = existing_composite_type_imports or {}
    
    # Use regex to find all CREATE TYPE statements
    type_regex = TYPE_REGEX
    logging.debug(f"COMPOSITE_TYPES before parsing: {list(composite_types.keys())}")

    for match in type_regex.finditer(sql_content):
        logging.debug(f"TYPE_REGEX matched: {match.groups()}") # Log captured groups
        type_name = match.group(1).strip()
        field_defs_str = match.group(2).strip()
        
        # Clean up the field definitions string
        # Use the already imported COMMENT_REGEX from the parent package
        field_defs_str_cleaned = COMMENT_REGEX.sub("", field_defs_str).strip()
        field_defs_str_cleaned = "\n".join(line.strip() for line in field_defs_str_cleaned.splitlines() if line.strip())

        logging.debug(f"  Processing CREATE TYPE: {type_name}") # Log type name
        logging.debug(f"  Cleaned field defs:\n{field_defs_str_cleaned}") # Log cleaned fields
        logging.info(f"Found CREATE TYPE for: {type_name}")

        try:
            # Use parse_column_definitions to parse the fields inside the type
            fields, required_imports = parse_column_definitions(field_defs_str_cleaned, 
                                                              context=f"type {type_name}",
                                                              enum_types=enum_types,
                                                              table_schemas=table_schemas)
            if fields:
                normalized_type_name = type_name.split(".")[-1]
                # Store under normalized name
                composite_types[normalized_type_name] = fields
                composite_type_imports[normalized_type_name] = required_imports
                # Also store under qualified name if different
                if type_name != normalized_type_name:
                    composite_types[type_name] = fields
                    composite_type_imports[type_name] = required_imports
                    logging.debug(f"  -> Stored type under both '{normalized_type_name}' and '{type_name}'")
                else:
                     logging.debug(f"  -> Parsed {len(fields)} fields for type {normalized_type_name}")
            elif field_defs_str_cleaned:
                logging.warning(f"  -> No fields parsed for type {type_name} from definition: '{field_defs_str_cleaned[:100]}...'")
            else:
                logging.debug(f"  -> Type {type_name} definition was empty or only comments.")
        except ParsingError as e:
            raise TypeParsingError(f"Failed to parse fields for type '{type_name}'", type_name=type_name) from e
        except Exception as e:
            logging.exception(f"Unexpected error parsing type '{type_name}'.")
            raise TypeParsingError(f"Failed to parse fields for type '{type_name}'", type_name=type_name) from e

    return composite_types, composite_type_imports
