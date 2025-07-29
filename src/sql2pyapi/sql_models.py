# ===== SECTION: IMPORTS =====
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Tuple, Any, Set, Union  # Added Union
from uuid import UUID
from datetime import datetime, date
from decimal import Decimal

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
    "double precision": "float", # Map double precision to float
    "interval": "timedelta",  # Map PostgreSQL INTERVAL to Python timedelta
    # Add more mappings as needed
}

PYTHON_IMPORTS = {
    "UUID": "from uuid import UUID",
    "datetime": "from datetime import datetime",  # Import only datetime
    "date": "from datetime import date",  # Import only date
    "timedelta": "from datetime import timedelta",  # Import for timedelta
    "Decimal": "from decimal import Decimal",
    "Any": "from typing import Any",  # Import for Any
    "List": "from typing import List",  # Import for List
    "Dict": "from typing import Dict",  # Import for Dict
    "Tuple": "from typing import Tuple",  # Import for Tuple
    "Optional": "from typing import Optional", # Added Optional
    "dataclass": "from dataclasses import dataclass", # Added dataclass
    "Enum": "from enum import Enum", # Added Enum for SQL ENUM types
}


# ===== SECTION: DATA STRUCTURES =====
# Core data structures for representing SQL functions, parameters, and return types

@dataclass
class SQLType:
    name: str  # Original SQL type name (e.g., 'TEXT', 'my_schema.my_type', 'users')
    python_type: str  # Corresponding Python type (e.g., 'str', 'MyType', 'User')
    is_array: bool = False
    is_setof: bool = False  # If the type is part of a SETOF construct
    is_table_type: bool = False # True if 'name' refers to a known table
    is_composite_type: bool = False # True if 'name' refers to a known composite type (CREATE TYPE)
    is_enum_type: bool = False # True if 'name' refers to a known ENUM type
    columns: List['ReturnColumn'] = field(default_factory=list) # For composite types / tables; Forward reference for ReturnColumn
    type_name_override: Optional[str] = None # e.g., to 'Any' if schema missing
    array_dimensions: int = 0 # Number of array dimensions if is_array is True


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
        has_sql_default (bool): Whether the parameter has a SQL DEFAULT value
    """
    name: str
    python_name: str
    sql_type: str
    python_type: str
    is_optional: bool = False
    has_sql_default: bool = False


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
        return_type_hint (Optional[str]): Placeholder for generator
        returns_table (bool): Whether the function returns a table/composite type
        dataclass_name (Optional[str]): Store determined dataclass name
        returns_record (bool): Whether the function returns a RECORD type
        returns_setof (bool): Whether the function returns a SETOF (multiple rows)
        required_imports (set): Set of Python imports needed for this function
        setof_table_name (Optional[str]): For SETOF table_name, the table name
        sql_comment (Optional[str]): SQL comment preceding the function definition
        returns_sql_type_name (Optional[str]): Store original SQL name for RETURNS named_type
        returns_enum_type (bool): Whether the function returns an ENUM type
    """
    sql_name: str
    python_name: str
    params: List[SQLParameter] = field(default_factory=list)
    return_type: Union[SQLType, str] = "None"
    return_columns: List[ReturnColumn] = field(default_factory=list)
    return_type_hint: Optional[str] = None # Add placeholder for generator
    returns_table: bool = False
    dataclass_name: Optional[str] = None # Store determined dataclass name
    returns_record: bool = False
    returns_setof: bool = False
    required_imports: Set[str] = field(default_factory=set) # Changed to Set[str]
    setof_table_name: Optional[str] = None
    returns_sql_type_name: Optional[str] = None # Store original SQL name for RETURNS named_type
    sql_comment: Optional[str] = None  # Store the cleaned SQL comment
    returns_enum_type: bool = False  # Whether the function returns an ENUM type