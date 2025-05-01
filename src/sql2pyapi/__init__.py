# Expose the main parse_sql function from the parser package
from .parser import parse_sql

__all__ = ['parse_sql']