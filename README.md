# SQL to Python API Generator

This tool generates Python async API wrappers for PostgreSQL functions defined in a `.sql` file.

## Setup with UV

We recommend using [uv](https://github.com/astral-sh/uv) for managing dependencies and virtual environments.

1.  **Create and activate a virtual environment:**
    ```bash
    uv venv
    source .venv/bin/activate # Or .venv\Scripts\activate on Windows
    ```

2.  **Install dependencies:**
    ```bash
    uv pip install -e . # Installs the package in editable mode
    ```

    *(Optional) For locked dependencies, generate requirements files:*
    ```bash
    # Generate requirements.txt from pyproject.toml
    uv pip compile pyproject.toml -o requirements.txt
    
    # Install from the lock file
    uv pip sync requirements.txt 
    ```

## Usage

Once installed, you can run the tool directly:

```bash
sql-to-pyapi examples/users.sql generated/users_api.py
```

This will read function definitions from `examples/users.sql` and write the generated Python code to `generated/users_api.py`.

## Features

*   Parses PostgreSQL `CREATE FUNCTION` statements.
*   Extracts function name, parameters (name and type), and return type (`SCALAR`, `TABLE`, `RECORD`).
*   Maps common PostgreSQL types to Python types (`UUID`, `TEXT`, `VARCHAR`, `INTEGER`, `TIMESTAMP`, `BOOLEAN`, `NUMERIC`, `DECIMAL`).
*   Generates `@dataclass` for functions returning `TABLE`.
*   Generates `async` Python functions using `psycopg` for database interaction.
*   Includes type hints and basic docstrings. 