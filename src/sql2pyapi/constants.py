# ===== SECTION: CONSTANTS =====
# Constants used throughout the sql2pyapi codebase

# Mapping from SQL base types to Python types and their required imports
PYTHON_IMPORTS = {
    "Any": "from typing import Any",
    "List": "from typing import List",
    "Optional": "from typing import Optional",
    "Dict": "from typing import Dict",
    "Tuple": "from typing import Tuple",
    "UUID": "from uuid import UUID",
    "datetime": "from datetime import datetime",
    "date": "from datetime import date",
    "timedelta": "from datetime import timedelta",
    "Decimal": "from decimal import Decimal",
    "dataclass": "from dataclasses import dataclass",
    "Enum": "from enum import Enum",
    # Add specific array types if needed, e.g.:
    # "List[int]": "from typing import List",
    # "List[str]": "from typing import List",
    # Or rely on the parser to add 'List' and the base type separately.
}

# Default docstring template
DEFAULT_DOCSTRING_TEMPLATE = '"""Call PostgreSQL function {sql_name}()."""'

# SQL query template
SQL_QUERY_TEMPLATE = 'SELECT * FROM {sql_name}({placeholders})'

# NULL handling code snippets
NULL_ROW_CHECK = "if row is None:\n    return None"
EMPTY_ROWS_CHECK = "if not rows:\n    return []"
COMPOSITE_NULL_CHECK = "if all(value is None for value in row_dict.values()):\n    return None"

# Row processing code snippets
COLUMN_NAMES_EXTRACTION = "colnames = [desc[0] for desc in cur.description]"
SINGLE_ROW_DICT_CONVERSION = "row_dict = dict(zip(colnames, row)) if not isinstance(row, dict) else row"
MULTIPLE_ROWS_DICT_CONVERSION = """processed_rows = [
    dict(zip(colnames, r)) if not isinstance(r, dict) else r
    for r in rows
]"""

# Return type handling comments
VOID_FUNCTION_COMMENT = "# Void function"
SETOF_RECORD_COMMENT = "# Return list of tuples for SETOF record"
SETOF_SCALAR_COMMENT = "# Assuming SETOF returns list of single-element tuples for scalars"
SINGLE_SCALAR_COMMENT = "# Fallback for tuple-like rows (index 0)"
DATACLASS_COMMENT = "# Ensure dataclass '{class_name}' is defined above."
COMPOSITE_NULL_COMMENT = "# Check for 'empty' composite rows (all values are None)"

# Helper functions for generator
HELPER_FUNCTIONS_CODE = """
# ===== SECTION: RESULT HELPERS =====
# REMOVED redundant import line

T = TypeVar('T')

def get_optional(result: Optional[List[T]] | Optional[T]) -> Optional[T]:
    \"\"\"\\
    Safely retrieves an optional single result.

    Handles cases where the input is:
    - None
    - An empty list
    - A list with one item
    - A single item (non-list, non-None)

    Returns the item if exactly one is found, otherwise None.
    \"\"\"
    if result is None:
        return None
    # Check if it's a list/tuple but not string/bytes
    if isinstance(result, Sequence) and not isinstance(result, (str, bytes)):
        if len(result) == 1:
            return result[0]
        else: # Empty list or list with more than one item
            return None
    else: # It's already a single item
        return result

def get_required(result: Optional[List[T]] | Optional[T]) -> T:
    \"\"\"\\
    Retrieves a required single result, raising an error if none or multiple are found.

    Handles cases where the input is:
    - None
    - An empty list
    - A list with one item
    - A single item (non-list, non-None)

    Returns the item if exactly one is found.
    Raises ValueError otherwise.
    \"\"\"
    item = get_optional(result)
    if item is None:
         # Improved error message
         input_repr = repr(result)
         if len(input_repr) > 80: # Truncate long inputs
             input_repr = input_repr[:77] + '...'
         raise ValueError(f"Expected exactly one result, but got none or multiple. Input was: {input_repr}")
    return item
"""
