# This file makes the parser directory a Python package
from .parser import parse_sql, SQLParser

# Re-export necessary models for backward compatibility
from ..sql_models import ParsedFunction, ReturnColumn, SQLParameter

__all__ = ['parse_sql', 'SQLParser', 'ParsedFunction', 'ReturnColumn', 'SQLParameter']
