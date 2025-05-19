# SQL2PyAPI System Tests

This directory contains system tests for the SQL2PyAPI tool. The tests verify that the tool correctly generates Python code from SQL schema and function definitions.

## Test Setup

The system tests use a PostgreSQL database running in a Docker container. The database is populated with test data from SQL files in the `sql` directory.

### SQL File Organization

SQL files are organized into two categories:
1. **Schema files**: Define database tables, types, and other schema objects
2. **Function files**: Define SQL functions that operate on the schema objects

Files are named with a numeric prefix to control the order of execution:
- `01_schema_*.sql` and `02_schema_*.sql`: Schema definitions (executed first)
- `01_functions_*.sql` and `02_functions_*.sql`: Function definitions (executed after schema files)

### Directory Structure

```
tests/system/
├── sql/                    # Source SQL files
│   ├── 01_schema_*.sql     # Schema definitions
│   ├── 02_schema_*.sql     # More schema definitions
│   ├── 01_functions_*.sql  # Function definitions
│   ├── 02_functions_*.sql  # More function definitions
│   └── dist/               # Generated combined files
│       ├── combined_schema.sql    # Combined schema file
│       └── combined_functions.sql # Combined function file
├── combine_sql_files.py    # Script to combine SQL files
├── conftest.py             # Test fixtures and setup
└── test_system.py          # Test cases
```

### File Combination

Since SQL2PyAPI requires a single schema file and a single function file, we use a script to combine the individual SQL files:

- `combine_sql_files.py`: Combines all schema files into `dist/combined_schema.sql` and all function files into `dist/combined_functions.sql`

This script is automatically run before the tests start, so you don't need to run it manually.

## Adding New Tests

To add new tests:

1. Create schema definitions in a new file named `01_schema_*.sql` or `02_schema_*.sql`
2. Create function definitions in a new file named `01_functions_*.sql` or `02_functions_*.sql`
3. Add test cases to `test_system.py`

The file combiner will automatically include your new files in the combined files, and the Docker container will execute them in the correct order.

## Running Tests

Run the tests with:

```bash
cd tests/system
python -m pytest test_system.py -v
```

This will:
1. Combine the SQL files (automatically)
2. Start a PostgreSQL container
3. Generate Python code using SQL2PyAPI
4. Run the tests against the generated code
