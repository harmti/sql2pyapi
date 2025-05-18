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
    r"([\s\S]*?)" # 2: Everything inside parenthesis (non-greedy, including newlines and all characters)
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
    
    logging.debug(f"COMPOSITE_TYPES before parsing: {list(composite_types.keys())}")
    
    # Find all CREATE TYPE statements using a simpler regex pattern
    create_type_pattern = re.compile(r"CREATE\s+TYPE\s+([a-zA-Z0-9_.]+)\s+AS\s*\(", re.IGNORECASE)
    
    for match in create_type_pattern.finditer(sql_content):
        type_name = match.group(1).strip()
        start_pos = match.end()  # Position right after the opening parenthesis
        
        # Find the matching closing parenthesis using a parenthesis counter
        paren_depth = 1
        end_pos = start_pos
        
        for i in range(start_pos, len(sql_content)):
            if sql_content[i] == '(':
                paren_depth += 1
            elif sql_content[i] == ')':
                paren_depth -= 1
                if paren_depth == 0:
                    end_pos = i
                    break
        
        # Extract the field definitions string including all comments
        field_defs_str = sql_content[start_pos:end_pos].strip()
        
        logging.debug(f"  Processing CREATE TYPE: {type_name}")
        logging.debug(f"  Original field defs with comments:\n{field_defs_str}")
        logging.info(f"Found CREATE TYPE for: {type_name}")
        
        try:
            # Use parse_column_definitions to parse the fields inside the type
            fields, required_imports = parse_column_definitions(field_defs_str, 
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
