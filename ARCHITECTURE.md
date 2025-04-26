# SQL2PyAPI Architecture

This document provides an overview of the SQL2PyAPI architecture, explaining the core components, data flow, and design decisions. It's intended to help developers (both human and AI) understand how the system works.

## Core Components

### 1. Parser (`parser.py`)

The parser is responsible for analyzing SQL files and extracting function definitions and table schemas. It handles:

- Parsing `CREATE FUNCTION` statements
- Parsing `CREATE TABLE` statements
- Extracting function parameters, return types, and comments
- Mapping SQL types to Python types

Key functions:
- `parse_sql()`: Main entry point for parsing SQL files
- `_parse_create_table()`: Extracts table schemas
- `_parse_return_clause()`: Analyzes function return types
- `_map_sql_to_python_type()`: Maps SQL types to Python types

### 2. Generator (`generator.py`)

The generator takes the parsed SQL definitions and generates Python code. It handles:

- Generating Python async functions that wrap SQL functions
- Creating dataclasses for complex return types
- Managing imports and dependencies
- Handling different return types (scalar, record, table, setof)

Key functions:
- `generate_python_code()`: Main entry point for code generation
- `_generate_function()`: Creates a Python function from a parsed SQL function
- `_generate_dataclass()`: Creates dataclasses for table returns

### 3. CLI (`cli.py`)

The command-line interface provides a user-friendly way to use the tool. It handles:

- Processing command-line arguments
- Reading input files
- Writing output files

## Data Flow

1. **Input**: SQL files containing function definitions and optionally table schemas
2. **Parsing**: SQL files → Parser → `ParsedFunction` objects
3. **Generation**: `ParsedFunction` objects → Generator → Python API code
4. **Output**: Generated Python module with async functions and dataclasses

## Key Data Structures

### `ParsedFunction`

Represents a SQL function after parsing, containing:
- Function name (SQL and Python versions)
- Parameters list
- Return type information
- Required imports
- SQL comments

### `SQLParameter`

Represents a parameter in a SQL function, containing:
- Parameter name (SQL and Python versions)
- SQL and Python types
- Optional flag

### `ReturnColumn`

Represents a column in a table or a field in a composite return type, containing:
- Column name
- SQL and Python types
- Optional flag

## Special Handling Cases

### NULL Handling

The system carefully handles NULL values in different contexts:

1. **None Row Handling**: When a SQL function returns no rows, the generated code checks for `None` before processing the row
2. **Composite NULL Handling**: When a PostgreSQL function returns a composite type with all NULL values (which happens when no matching data is found), the code detects this and returns `None`

### Return Type Handling

The system supports various PostgreSQL return styles:

1. **Scalar Types**: Simple values like integers, strings, etc.
2. **RECORD**: Returns a tuple in Python
3. **TABLE**: Generates a dataclass and returns an instance
4. **SETOF scalar**: Returns a list of scalar values
5. **SETOF table**: Returns a list of dataclass instances

## Design Decisions

### Type Mapping

SQL types are mapped to appropriate Python types with proper imports. For example:
- `uuid` → `UUID` (with `from uuid import UUID`)
- `timestamp` → `datetime` (with `from datetime import datetime`)

### Naming Conventions

- Dataclass names are generated using singular CamelCase from table names
- Function names generally preserve the SQL function name
- Parameter names are cleaned to be more Pythonic (e.g., removing `p_` prefixes)

### Optional Parameters

Parameters with DEFAULT values in SQL are made optional in Python with a default value of `None`.

### Docstrings

SQL comments preceding function definitions are preserved as Python docstrings.
