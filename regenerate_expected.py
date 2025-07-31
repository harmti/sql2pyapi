import os
import subprocess
import sys


fixtures_dir = "tests/fixtures"
expected_dir = "tests/expected"
project_root = "."  # Assuming we run from the project root

# Ensure expected directory exists
os.makedirs(expected_dir, exist_ok=True)

sql_files = [f for f in os.listdir(fixtures_dir) if f.endswith(".sql")]

print(f"Found {len(sql_files)} SQL files in {fixtures_dir}. Regenerating expected outputs...")

success_count = 0
error_count = 0

# Files that need special handling with allow-missing-schemas flag
files_with_missing_schemas = [
    "setof_missing_table_function.sql",
    "optional_params_function.sql",
]

for sql_file in sql_files:
    base_name = os.path.splitext(sql_file)[0]
    # Determine expected .py file name
    expected_py_file = f"{base_name}_api.py"

    sql_path = os.path.join(fixtures_dir, sql_file)
    expected_path = os.path.join(expected_dir, expected_py_file)

    # Check if schema file exists
    schema_file_path = None
    if base_name == "example_func1":
        schema_file_path = "tests/fixtures/example_schema1.sql"
    if base_name == "example_schema1":
        schema_file_path = "tests/fixtures/example_schema1.sql"
    if base_name == "schema_qualified_example":
        schema_file_path = "tests/fixtures/schema_qualified_schema.sql"  # Add check for this test
    # Add more specific schema checks if needed for other tests

    command = ["sql2pyapi", sql_path, expected_path]
    # Add schema file argument if it exists
    if schema_file_path and os.path.exists(schema_file_path):
        command.extend(["--schema-file", schema_file_path])

    # Add allow-missing-schemas flag for files that need it
    if sql_file in files_with_missing_schemas:
        command.append("--allow-missing-schemas")

    # print(f"Running: {' '.join(command)}") # Reduced verbosity

    try:
        result = subprocess.run(command, capture_output=True, text=True, cwd=project_root, check=False)
        if result.returncode != 0:
            error_count += 1
            print(f"  Error generating {expected_py_file} from {sql_file} (RC: {result.returncode}):")
            print(f"  STDERR: {result.stderr.strip()}")
        else:
            success_count += 1
            # print(f"  Successfully generated {expected_py_file}") # Reduced verbosity

    except Exception as e:
        error_count += 1
        print(f"  Failed to run command for {sql_file}: {e}")

print(f"Finished regenerating expected files. Success: {success_count}, Errors: {error_count}.")

# Exit with non-zero code if there were errors
if error_count > 0:
    sys.exit(1)
