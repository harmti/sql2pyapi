"""sql2pyapi - Convert SQL functions to a Python API"""

__version__ = "0.1.0" # Example version

# Attempt to make sub-modules more easily accessible, though not strictly necessary for relative imports to work
from . import parser
from . import generator
from . import sql_models
from . import constants

# Optionally, re-export key components for easier top-level import by users of the library
# For example:
# from .parser import parse_sql
from .generator import generate_python_code
# from .sql_models import ParsedFunction

# Expose the main parse_sql function from the parser package
from .parser import parse_sql

__all__ = ['parse_sql', 'generate_python_code']