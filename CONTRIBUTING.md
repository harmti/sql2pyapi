# Contributing Guide for LLMs

This document provides guidance specifically for Large Language Models (LLMs) working with the SQL2PyAPI codebase. It highlights key files, patterns, and edge cases to be aware of.

## Key Files and Their Purposes

### Core Functionality

- `src/sql2pyapi/parser.py`: Parses SQL to extract function definitions and table schemas
- `src/sql2pyapi/generator.py`: Generates Python code from parsed definitions
- `src/sql2pyapi/cli.py`: Command-line interface for the tool

### Tests

- `tests/unit/test_parser.py`: Unit tests for the parser
- `tests/integration/test_generator.py`: Integration tests for the generator
- `tests/integration/test_runtime.py`: Tests for the runtime behavior of generated code
- `tests/integration/test_none_row_handling.py`: Tests for handling None rows
- `tests/integration/test_composite_null_handling.py`: Tests for handling composite NULL rows

## Code Navigation

The codebase uses section markers to help with navigation. Look for comments like:

```python
# ===== SECTION: IMPORTS AND SETUP =====
# ===== SECTION: TYPE MAPS AND CONSTANTS =====
# ===== SECTION: DATA STRUCTURES =====
```

## Common Patterns

### 1. Type Mapping

SQL types are mapped to Python types in `_map_sql_to_python_type()` in parser.py. The mapping is defined in the `TYPE_MAP` dictionary.

```python
TYPE_MAP = {
    "uuid": "UUID",
    "text": "str",
    # ... more mappings
}
```

### 2. NULL Handling

The codebase handles NULL values in two important ways:

1. **None Row Handling**: When a SQL function returns no rows

```python
row = await cur.fetchone()
if row is None:
    return None
# Process row here
```

2. **Composite NULL Handling**: When a PostgreSQL function returns a composite type with all NULL values

```python
# Check for 'empty' composite rows (all values are None)
if all(value is None for value in row_dict.values()):
    return None
```

### 3. Return Type Handling

The generator handles different return types with specific code patterns:

```python
if func.returns_setof:  # SETOF handling
    # ...
elif func.returns_table:  # TABLE handling
    # ...
elif func.returns_record:  # RECORD handling
    # ...
else:  # Scalar handling
    # ...
```

## Known Edge Cases

1. **PostgreSQL Composite Types**: PostgreSQL functions returning composite types (e.g., `RETURNS table_name`) always return a row with all NULLs instead of NULL when no matching data is found

2. **Array Types**: Array types need special handling in both parsing and code generation

3. **Optional Parameters**: Parameters with DEFAULT values in SQL are made optional in Python

4. **Table Schema Inference**: When a function returns `SETOF table_name`, the code attempts to find the table schema to generate a proper dataclass

## Common Modifications

When modifying the codebase, be aware of these common tasks:

### 1. Adding Support for a New SQL Type

To add support for a new SQL type:
1. Add it to the `TYPE_MAP` in parser.py
2. Ensure proper imports are added in `_map_sql_to_python_type()`

### 2. Enhancing NULL Handling

NULL handling is critical for correctness. Any changes should be tested with:
- SQL functions that return no rows
- SQL functions that return composite types with all NULL values
- SQL functions that return rows with some NULL values

### 3. Adding New Return Type Support

If adding support for a new return type pattern:
1. Update `_parse_return_clause()` in parser.py to recognize the pattern
2. Update `_generate_function()` in generator.py to generate appropriate code
3. Add tests in the integration test suite

## Testing Approach

When making changes, ensure that:

1. All existing tests pass
2. New tests are added for new functionality or bug fixes
3. Edge cases are covered, especially around NULL handling

Use the existing test files as templates for new tests.
