# ===== SECTION: CONSTANTS =====
# Constants used throughout the sql2pyapi codebase

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
