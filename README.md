# SQL2PyAPI: SQL to Python Type-Safe API Generator

`sql2pyapi` generates type-safe async Python wrappers for PostgreSQL functions, creating a bridge between SQL and Python with proper type mapping.

```
┌───────────────┐
│ PostgreSQL    │
│ Functions     │───┐
└───────────────┘   │   ┌──────────────┐     ┌────────────────┐
                    ├──▶│ sql2pyapi    │────▶│ Python API     │
┌───────────────┐   │   │ Generator    │     │ Async Wrappers │
│ SQL Schema    │───┘   └──────────────┘     └────────────────┘
│ Definitions   │
└───────────────┘
```

## Quick Example

**Write SQL functions (your_functions.sql):**
```sql
CREATE FUNCTION get_user_by_id(p_user_id UUID)
RETURNS TABLE (
    id UUID,
    username TEXT,
    email TEXT,
    created_at TIMESTAMP
)
AS $$
    SELECT id, username, email, created_at 
    FROM users 
    WHERE id = p_user_id;
$$ LANGUAGE SQL;
```

**Generate Python API:**
```bash
sql2pyapi your_functions.sql user_api.py
```

**Use in your Python code:**
```python
import asyncio
from user_api import get_user_by_id, User

async def main():
    user = await get_user_by_id("123e4567-e89b-12d3-a456-426614174000")
    print(f"Found user: {user.username}")

asyncio.run(main())
```

## Why SQL2PyAPI?

### SQL Advantages for Complex Applications

SQL offers significant benefits for data-intensive applications. SQL2PyAPI helps you:

- Write queries in SQL where it excels - aggregations, joins, window functions
- Use database-native features like triggers, constraints, and stored procedures
- Control query optimization with direct SQL access
- See exactly what SQL is executed, with clear error messages

### Benefits for Larger Applications

- Provides precise control over database queries
- Helps prevent N+1 query problems through optimized SQL
- Enables separation of data access from business logic
- Supports layered architecture design

### Technical Features

- Type-safe interfaces with Python type hints
- Async implementation with psycopg3
- Compatible with database migration workflows
- Works well with SQL generation tools and AI assistants

## Usage

The tool provides a command-line interface:

```bash
sql2pyapi <input_sql_file> <output_python_file>
```

Example:
```bash
sql2pyapi functions.sql generated_api.py
```

With a schema file:
```bash
sql2pyapi functions.sql generated_api.py --schema-file schema.sql
```

## Setup

We recommend using `uv` for managing dependencies:

1. **Create and activate a virtual environment:**
   ```bash
   uv venv
   source .venv/bin/activate # Or .venv\Scripts\activate on Windows
   ```

2. **Install dependencies:**
   ```bash
   uv pip install -e . # Installs the package in editable mode
   ```

## Features

- **Complete Type Mapping** from PostgreSQL to Python types
- **Dataclass Generation** for table returns
- **Async Functions** using psycopg3
- **Support for Complex Returns** (scalar, SETOF, TABLE, RECORD)
- **Docstring Generation** from SQL comments
- **Pythonic Parameter Names** (removes prefixes like p_)

## Detailed Type Support

* **SQL Parsing:**
  * Parses `CREATE FUNCTION` and `CREATE TABLE` statements
  * Extracts parameters, return types, and comments
  * Handles various return structures (scalar, TABLE, SETOF, RECORD)

* **Type Mapping:**
  * Maps PostgreSQL types to Python equivalents (uuid → UUID, text → str, etc.)
  * Handles array types (e.g., integer[] → List[int])
  * Automatically adds necessary imports

## Limitations and Future Work

* **Complex SQL:** Does not handle very complex SQL syntax within function bodies
* **Error Handling:** Generated code assumes successful execution
* **Dependencies:** Relies on psycopg3 and inflection
* **Custom Types:** Less common PostgreSQL types might map to Any

## Development

* Install development dependencies: `uv pip install -e ".[dev]"`
* Run tests: `pytest`
* Linting/Formatting: `ruff check .` and `ruff format .`

## Return Type Handling (`List` vs. `Optional`)

`sql2pyapi` aims for a predictable mapping from SQL function return types to Python type hints. A key aspect is how it determines whether a Python function should return a `List[...]` or an `Optional[...]`.

**The Rule:**

The **sole determinant** for using `List` versus `Optional` as the *outer* wrapper in the Python return type hint is the presence or absence of the `SETOF` keyword in the SQL function's `RETURNS` clause.

1.  **`RETURNS SETOF <type>`:**
    *   If your SQL function includes `SETOF` (e.g., `RETURNS SETOF integer`, `RETURNS SETOF users`, `RETURNS SETOF user_identity`), the generated Python function will **always** have a return type hint of `List[MappedType]` (e.g., `List[int]`, `List[User]`, `List[UserIdentity]`)).
    *   If the SQL function returns zero rows, the Python function will return an empty list (`[]`).

2.  **`RETURNS <type>` (No `SETOF`):**
    *   If your SQL function returns a single value, row, or composite type *without* `SETOF` (e.g., `RETURNS integer`, `RETURNS users`, `RETURNS user_identity`), the generated Python function will **always** have a return type hint of `Optional[MappedType]` (e.g., `Optional[int]`, `Optional[User]`, `Optional[UserIdentity]`)).
    *   This handles the common database pattern where a function designed to return a single row might return zero rows (e.g., `SELECT ... FROM users WHERE id = p_id LIMIT 1`). In such cases, the Python function will return `None`. If a row *is* found, it returns the mapped object/value.

3.  **`RETURNS VOID` or `PROCEDURE`:**
    *   Functions returning `VOID` or defined as `PROCEDURE` will result in a Python function with a return type hint of `None`.

**Examples:**

| SQL Function Signature                  | Generated Python Return Hint | Notes                                      |
| :-------------------------------------- | :--------------------------- | :----------------------------------------- |
| `RETURNS integer`                       | `Optional[int]`              | Returns `None` if no row                   |
| `RETURNS SETOF integer`                 | `List[int]`                  | Returns `[]` if no rows                    |
| `RETURNS users`                         | `Optional[User]`             | `User` is generated dataclass              |
| `RETURNS SETOF users`                   | `List[User]`                 | Returns `[]` if no rows                    |
| `RETURNS user_identity` (custom type) | `Optional[UserIdentity]`     | `UserIdentity` is generated dataclass      |
| `RETURNS SETOF user_identity`           | `List[UserIdentity]`         | Returns `[]` if no rows                    |
| `RETURNS TABLE(id int, name text)`    | `List[FunctionNameResult]`   | `RETURNS TABLE` implies potentially > 1 row |
| `RETURNS record`                        | `Optional[Tuple]`            | Returns `None` if no row                   |
| `RETURNS SETOF record`                  | `List[Tuple]`                | Returns `[]` if no rows                    |
| `RETURNS void`                          | `None`                       |                                            |

**SQL Conventions for API Design:**

Understanding this behavior allows you to design your SQL functions to produce the desired Python API:

*   **If your function logically returns a collection of items (even if sometimes zero or one), use `SETOF` in your SQL `RETURNS` clause.** This guarantees the Python function returns a `List`.
*   **If your function logically returns a single item or potentially nothing, define the return type *without* `SETOF`.** The Python function will return an `Optional`, correctly handling cases where no data is found. You don't need to explicitly handle `NULL` returns in SQL solely to get an `Optional` wrapper; the tool does this based on the lack of `SETOF`.

**Note on Dataclass Fields:** Currently, fields within generated dataclasses (like `User` or `UserIdentity`) often default to `Optional[...]` for flexibility in handling potentially missing columns or `NULL` values returned by the database, even if the original `CREATE TABLE` or `CREATE TYPE` specified `NOT NULL`. This behavior might be refined in future versions to offer stricter typing based on schema nullability.
