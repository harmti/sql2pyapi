# SQL to Python Async API Generator

This tool automatically generates Python asynchronous API wrappers for PostgreSQL functions defined in `.sql` files. It parses SQL function definitions, maps types, handles various return structures, and produces clean, type-hinted Python code using `psycopg` for database interaction.

## Use Case

Modern web applications often interact with databases through stored procedures or functions for complex logic, data validation, or security. Maintaining separate database logic and application-level API code can be tedious and error-prone.

This tool aims to bridge that gap by automatically creating Python functions that directly call your PostgreSQL functions, ensuring consistency and reducing boilerplate code. It's particularly useful when:

*   You have a significant number of PostgreSQL functions exposed to your application layer.
*   You want to leverage `asyncio` and `psycopg` for non-blocking database operations.
*   You need type safety and clear API definitions in your Python code based on your SQL schema.
*   You want to automatically generate data transfer objects (DTOs) or data classes based on `RETURNS TABLE` definitions or `SETOF table_name` returns.

## Features Supported

*   **SQL Parsing:**
    *   Parses PostgreSQL `CREATE FUNCTION` statements.
    *   Parses `CREATE TABLE` statements (from the same file or a separate schema file) to infer return types for `SETOF table_name`.
    *   Extracts function name, parameters (including `IN`/`OUT`/`INOUT`, name, type, and `DEFAULT` for optional parameters).
    *   Extracts return types:
        *   `VOID`
        *   Scalar types (e.g., `INTEGER`, `TEXT`, `UUID`, `TIMESTAMP`, `BOOLEAN`, `NUMERIC`, `JSONB`, etc.)
        *   `RECORD` (returns a `Tuple`)
        *   `TABLE (...)` (generates a specific `@dataclass`)
        *   `SETOF scalar` (returns a `List[scalar_type]`)
        *   `SETOF table_name` (returns a `List[Dataclass]` based on the `CREATE TABLE` definition or a placeholder if the table definition is not found).
    *   Parses and includes SQL comments (`--` style and `/* ... */` style) preceding the function definition as the Python function's docstring. Handles multi-line comments.
*   **Type Mapping:**
    *   Maps common PostgreSQL types to Python types (e.g., `uuid` -> `UUID`, `text` -> `str`, `integer` -> `int`, `timestamp` -> `datetime`, `numeric` -> `Decimal`, `jsonb` -> `Dict[str, Any]`, `boolean` -> `bool`, `bytea` -> `bytes`).
    *   Handles array types (e.g., `integer[]` -> `List[int]`).
    *   Automatically adds necessary imports (`UUID`, `datetime`, `date`, `Decimal`, `Optional`, `List`, `Any`, `Dict`, `Tuple`).
*   **Code Generation:**
    *   Generates Python `async` functions using `psycopg` (v3+).
    *   Includes type hints for parameters and return values (`Optional` for single returns, `List` for `SETOF`).
    *   Generates `@dataclass` definitions for `RETURNS TABLE` and `SETOF table_name` structures. Uses `inflection` library to generate singular CamelCase class names from snake_case table names.
    *   Generates placeholder dataclasses with TODO comments if a `SETOF table_name` is encountered but the corresponding `CREATE TABLE` statement wasn't found.
    *   Handles optional parameters by assigning `None` as the default value in the Python function signature.
    *   Uses Pythonic parameter names (e.g., removes `p_` or `_` prefixes).

## Setup

We recommend using `uv` for managing dependencies and virtual environments.

1.  **Create and activate a virtual environment:**
    ```bash
    uv venv
    source .venv/bin/activate # Or .venv\Scripts\activate on Windows
    ```

2.  **Install dependencies:**
    ```bash
    uv pip install -e . # Installs the package in editable mode with its dependencies
    ```
    *(Optional: To install from a locked file)*
    ```bash
    # Generate requirements.lock (or requirements.txt) if needed
    uv pip compile pyproject.toml -o requirements.lock
    # Install exact versions
    uv pip sync requirements.lock
    ```

## Usage

The tool provides a command-line interface `sql2pyapi`.

### Basic Usage

```bash
sql2pyapi <input_sql_file> <output_python_file>
```

Replace `<input_sql_file>` with the path to your SQL file containing function definitions and `<output_python_file>` with the desired path for the generated Python API module.

Example:

```bash
sql2pyapi path/to/your/functions.sql path/to/your/generated_api.py
```

### Using a Schema File

If your functions depend on custom types or tables defined in separate files, you can provide a schema file using the `--schema-file` option:

```bash
sql2pyapi functions.sql generated_api.py --schema-file schema.sql
```

## Limitations and Future Work

*   **Complex SQL:** Does not handle very complex SQL syntax within the function body or advanced `CREATE FUNCTION` options (like `VOLATILE`, `STABLE`, security attributes etc., although they are ignored gracefully).
*   **Error Handling:** Generated code assumes the SQL function executes successfully. Robust error handling within the generated Python wrappers might need manual addition.
*   **Type Mapping:** While common types are covered, less common or custom PostgreSQL types might map to `Any`. The `TYPE_MAP` in `parser.py` can be extended.
*   **Dependencies:** Relies on `psycopg` (v3 async interface) and `inflection`.
*   **Testing:** While unit tests cover various scenarios, more complex integration testing might be beneficial.

## Development

*   Install development dependencies: `uv pip install -e ".[dev]"`
*   Run tests: `pytest`
*   Linting/Formatting: Uses `ruff`. Check with `ruff check .` and format with `ruff format .` 
