# SQL2PyAPI: SQL to Python Type-Safe API Generator

`sql2pyapi` generates type-safe async Python wrappers for PostgreSQL functions, creating a bridge between SQL and Python with proper type mapping.

```
┌───────────────┐
│ PostgreSQL    │
│ Functions     │──┐
└───────────────┘  │    ┌──────────────┐     ┌────────────────┐
                   ├───▶│ sql2pyapi    │────▶│ Python API     │
┌───────────────┐  │    │ Generator    │     │ Async Wrappers │
│ SQL Schema    │──┘    └──────────────┘     └────────────────┘
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
