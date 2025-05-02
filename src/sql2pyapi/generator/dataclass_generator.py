# ===== SECTION: IMPORTS AND SETUP =====
# Standard library and third-party imports
from typing import List, Dict, Tuple, Optional
import textwrap
import inflection  # Using inflection library for plural->singular
from pathlib import Path
import os
import logging

# Local imports
from ..sql_models import ParsedFunction, ReturnColumn, SQLParameter
# from ..constants import * # Constants likely not needed directly here


def _generate_dataclass(class_name: str, columns: List[ReturnColumn], make_fields_optional: bool = False) -> str:
    """
    Generates a Python dataclass definition string based on SQL column definitions.
    
    Args:
        class_name (str): Name for the generated dataclass
        columns (List[ReturnColumn]): Column definitions extracted from SQL
        make_fields_optional (bool): Whether to make all fields Optional, regardless of
                                    their nullability in the database schema
    
    Returns:
        str: Python code for the dataclass definition as a string
    
    Notes:
        - If columns list is empty or only contains an 'unknown' column, a TODO comment
          will be generated instead of a complete dataclass
        - Column types are mapped from SQL to Python types by the parser
    """
    if not columns or (len(columns) == 1 and columns[0].name == "unknown"):
        # Handle case where schema wasn't found or columns couldn't be parsed
        # If columns exist (parser couldn't map), use the SQL type from the dummy column.
        # If columns is empty (generator added placeholder), try to guess SQL name from class name.
        if columns:
             sql_table_name_guess = columns[0].sql_type
        else:
             # Attempt to convert CamelCase class_name back to snake_case for the comment
             # REVISED: Pluralize the snake_case name for the comment to match original table likely name
             singular_snake = inflection.underscore(class_name) 
             sql_table_name_guess = inflection.pluralize(singular_snake) # Convert 'item' back to 'items'
             # If it was an ad-hoc Result class, remove _result suffix (apply before pluralizing? No, class_name is Item)
             # if sql_table_name_guess.endswith("_result"):
             #      sql_table_name_guess = sql_table_name_guess[:-7] 
        
        # Ensure the guessed name is not empty, fallback if needed
        if not sql_table_name_guess:
             sql_table_name_guess = "unknown_table"
             
        return f"""# TODO: Define dataclass for table '{sql_table_name_guess}'
# @dataclass
# class {class_name}:
#     pass"""

    fields = []
    for col in columns:
        field_type = col.python_type
        # Wrap with Optional if needed based on column's optionality OR if forced for RETURNS TABLE
        if make_fields_optional and not field_type.startswith("Optional["):
            field_type = f"Optional[{field_type}]"
        elif col.is_optional and not field_type.startswith("Optional["):
            # This case should already be handled by _map_sql_to_python_type, but double-check
            # We might have removed Optional in mapping if is_optional was False initially
            # Re-add Optional if the parser determined it should be optional now
            field_type = f"Optional[{field_type}]"

        fields.append(f"    {col.name}: {field_type}")

    fields_str = "\n".join(fields)
    return f"""@dataclass
class {class_name}:
{fields_str}
"""

