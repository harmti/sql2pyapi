from pathlib import Path
import subprocess
import sys
import ast
from dataclasses import is_dataclass

# Define paths relative to the main tests/ directory
TESTS_ROOT_DIR = Path(__file__).parent.parent # Go up one level to tests/
FIXTURES_DIR = TESTS_ROOT_DIR / "fixtures"
EXPECTED_DIR = TESTS_ROOT_DIR / "expected"
PROJECT_ROOT = TESTS_ROOT_DIR.parent # Go up one level from tests/ to project root


def run_cli_tool(functions_sql: Path, output_py: Path, schema_sql: Path = None):
    """Helper function to run the CLI tool as a subprocess."""
    cmd = [
        sys.executable,  # Use the current Python executable
        "-m",
        "sql2pyapi.cli",  # Invoke the module's entry point
        str(functions_sql),
        str(output_py),
    ]
    if schema_sql:
        cmd.extend(["--schema-file", str(schema_sql)])

    # Run from the project root directory
    result = subprocess.run(cmd, capture_output=True, text=True, cwd=PROJECT_ROOT, check=False)

    if result.returncode != 0:
        print("CLI Error STDOUT:", result.stdout)
        print("CLI Error STDERR:", result.stderr)
        result.check_returncode()  # Raise CalledProcessError

    return result


def test_func2_generation_with_schema(tmp_path):
    """Test generating the func2 API with a separate schema file using AST checks."""
    functions_sql_path = FIXTURES_DIR / "example_func1.sql"
    schema_sql_path = FIXTURES_DIR / "example_schema1.sql"
    expected_output_path = EXPECTED_DIR / "example_func1_api.py"
    actual_output_path = tmp_path / "example_func1_api.py"

    # Run the generator tool
    run_cli_tool(functions_sql_path, actual_output_path, schema_sql_path)

    # --- AST Based Assertions ---
    assert actual_output_path.is_file(), "Generated file was not created."
    actual_content = actual_output_path.read_text()
    tree = ast.parse(actual_content)

    # 1. Check Imports
    expected_imports_from_typing = {"List", "Optional", "Tuple", "Dict", "Any"}
    expected_imports_other = {
        ("uuid", "UUID"),
        ("datetime", "date"),
        ("datetime", "datetime"),
        ("psycopg", "AsyncConnection"),
        ("dataclasses", "dataclass")
    }
    
    found_imports_from_typing = set()
    found_imports_other = set()

    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            if node.module == 'typing':
                for alias in node.names:
                    found_imports_from_typing.add(alias.name)
            elif node.module in ['uuid', 'datetime', 'psycopg', 'dataclasses']:
                 for alias in node.names:
                     found_imports_other.add((node.module, alias.name))

    assert found_imports_from_typing == expected_imports_from_typing, f"Missing/unexpected imports from typing: {expected_imports_from_typing.symmetric_difference(found_imports_from_typing)}"
    assert found_imports_other == expected_imports_other, f"Missing/unexpected other imports: {expected_imports_other.symmetric_difference(found_imports_other)}"


    # 2. Check Company Dataclass
    company_class_node = None
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef) and node.name == 'Company':
            company_class_node = node
            break
    
    assert company_class_node is not None, "Class definition 'Company' not found"
    assert any(isinstance(d, ast.Name) and d.id == 'dataclass' for d in company_class_node.decorator_list), "'Company' class is not decorated with @dataclass"
    
    expected_fields = {
        'id': 'UUID',
        'name': 'str',
        'industry': 'Optional[str]',
        'size': 'Optional[str]',
        'primary_address': 'Optional[str]',
        'created_at': 'datetime',
        'updated_at': 'datetime',
        'created_by_user_id': 'UUID'
    }
    
    actual_fields = {}
    for stmt in company_class_node.body:
        if isinstance(stmt, ast.AnnAssign):
            field_name = stmt.target.id
            # Handle simple Name annotations and Optional[type]
            if isinstance(stmt.annotation, ast.Name):
                type_name = stmt.annotation.id
            elif isinstance(stmt.annotation, ast.Subscript) and isinstance(stmt.annotation.value, ast.Name) and stmt.annotation.value.id == 'Optional':
                 # Extract the inner type name for Optional
                 if isinstance(stmt.annotation.slice, ast.Name):
                     inner_type = stmt.annotation.slice.id
                     type_name = f"Optional[{inner_type}]"
                 else:
                     # Handle potential complex types within Optional if needed later
                     type_name = ast.unparse(stmt.annotation) # Fallback
            else:
                 type_name = ast.unparse(stmt.annotation) # Fallback for other complex types
            actual_fields[field_name] = type_name

    assert actual_fields == expected_fields, f"Mismatch in Company fields/types. Expected {expected_fields}, Got {actual_fields}"

    # 3. Check list_user_companies Function
    list_func_node = None
    for node in ast.walk(tree):
        if isinstance(node, ast.AsyncFunctionDef) and node.name == 'list_user_companies':
            list_func_node = node
            break
            
    assert list_func_node is not None, "Async function definition 'list_user_companies' not found"

    # Check parameters
    expected_params = {'conn': 'AsyncConnection', 'user_id': 'UUID'}
    actual_params = {arg.arg: ast.unparse(arg.annotation) for arg in list_func_node.args.args}
    assert actual_params == expected_params, f"Mismatch in list_user_companies parameters. Expected {expected_params}, Got {actual_params}"

    # Check return annotation
    expected_return_type = 'List[Company]'
    actual_return_type = ast.unparse(list_func_node.returns)
    assert actual_return_type == expected_return_type, f"Mismatch in list_user_companies return type. Expected {expected_return_type}, Got {actual_return_type}"
    
    # Check docstring presence and content
    docstring = ast.get_docstring(list_func_node)
    assert docstring is not None, "list_user_companies is missing a docstring"
    assert docstring == "Function to list companies created by a specific user", "Docstring content mismatch for list_user_companies"

    # 4. Check Function Body (Simplified Checks)
    sql_query = None
    execute_call = None
    fetchall_call = None
    list_comp = None

    for node in ast.walk(list_func_node):
        # Find the SQL query string
        if isinstance(node, ast.Await) and isinstance(node.value, ast.Call):
            call = node.value
            if isinstance(call.func, ast.Attribute) and call.func.attr == 'execute':
                 execute_call = call
                 # Assume first arg is the SQL query string literal or variable name
                 if len(call.args) > 0 and isinstance(call.args[0], ast.Constant):
                     sql_query = call.args[0].value
                 elif len(call.args) > 0 and isinstance(call.args[0], ast.Name):
                      # If it's a variable, we might need to trace it back, simpler check for now
                      pass # Placeholder - complex trace not implemented yet

            elif isinstance(call.func, ast.Attribute) and call.func.attr == 'fetchall':
                fetchall_call = call
                
        # Find the return list comprehension
        if isinstance(node, ast.Return) and isinstance(node.value, ast.ListComp):
             list_comp = node.value
             # Check if the comprehension calls the 'Company' constructor
             assert isinstance(list_comp.elt, ast.Call) and isinstance(list_comp.elt.func, ast.Name) and list_comp.elt.func.id == 'Company', "List comprehension does not call Company()"


    assert sql_query == "SELECT * FROM list_user_companies(%s)", f"SQL query mismatch. Found: '{sql_query}'"
    assert execute_call is not None, "cur.execute call not found"
    # Check execute arguments
    assert len(execute_call.args) == 2, "execute call should have 2 arguments"
    assert isinstance(execute_call.args[1], ast.List), "Second argument to execute should be a list"
    assert len(execute_call.args[1].elts) == 1 and isinstance(execute_call.args[1].elts[0], ast.Name) and execute_call.args[1].elts[0].id == 'user_id', "Execute parameters mismatch"

    assert fetchall_call is not None, "cur.fetchall call not found"
    assert list_comp is not None, "Return list comprehension not found"


def test_void_function_generation(tmp_path):
    """Test generating a function that returns void."""
    functions_sql_path = FIXTURES_DIR / "void_function.sql"
    expected_output_path = EXPECTED_DIR / "void_function_api.py"
    actual_output_path = tmp_path / "void_function_api.py"

    # Run the generator tool (no schema file needed)
    run_cli_tool(functions_sql_path, actual_output_path)

    # Compare the generated file with the expected file
    assert actual_output_path.is_file(), "Generated file was not created."

    expected_content = expected_output_path.read_text()
    actual_content = actual_output_path.read_text()

    assert actual_content == expected_content, (
        f"Generated file content does not match expected content.\n"
        f"Expected ({expected_output_path}):\n{expected_content}\n"
        f"Actual ({actual_output_path}):\n{actual_content}"
    )


def test_scalar_function_generation(tmp_path):
    """Test generating functions that return simple scalar types."""
    functions_sql_path = FIXTURES_DIR / "scalar_function.sql"
    expected_output_path = EXPECTED_DIR / "scalar_function_api.py"
    actual_output_path = tmp_path / "scalar_function_api.py"

    # Run the generator tool
    run_cli_tool(functions_sql_path, actual_output_path)

    # Compare the generated file with the expected file
    assert actual_output_path.is_file(), "Generated file was not created."

    expected_content = expected_output_path.read_text()
    actual_content = actual_output_path.read_text()

    assert actual_content == expected_content, (
        f"Generated file content does not match expected content.\n"
        f"Expected ({expected_output_path}):\n{expected_content}\n"
        f"Actual ({actual_output_path}):\n{actual_content}"
    )


def test_setof_scalar_function_generation(tmp_path):
    """Test generating a function that returns SETOF scalar."""
    functions_sql_path = FIXTURES_DIR / "setof_scalar_function.sql"
    expected_output_path = EXPECTED_DIR / "setof_scalar_function_api.py"
    actual_output_path = tmp_path / "setof_scalar_function_api.py"

    # Run the generator tool
    run_cli_tool(functions_sql_path, actual_output_path)

    # Compare the generated file with the expected file
    assert actual_output_path.is_file(), "Generated file was not created."

    expected_content = expected_output_path.read_text()
    actual_content = actual_output_path.read_text()

    assert actual_content == expected_content, (
        f"Generated file content does not match expected content.\n"
        f"Expected ({expected_output_path}):\n{expected_content}\n"
        f"Actual ({actual_output_path}):\n{actual_content}"
    )


def test_returns_table_function_generation(tmp_path):
    """Test generating a function that returns TABLE(...)."""
    functions_sql_path = FIXTURES_DIR / "returns_table_function.sql"
    expected_output_path = EXPECTED_DIR / "returns_table_function_api.py"
    actual_output_path = tmp_path / "returns_table_function_api.py"

    # Run the generator tool
    run_cli_tool(functions_sql_path, actual_output_path)

    # Compare the generated file with the expected file
    assert actual_output_path.is_file(), "Generated file was not created."

    expected_content = expected_output_path.read_text()
    actual_content = actual_output_path.read_text()

    assert actual_content == expected_content, (
        f"Generated file content does not match expected content.\n"
        f"Expected ({expected_output_path}):\n{expected_content}\n"
        f"Actual ({actual_output_path}):\n{actual_content}"
    )


def test_setof_missing_table_function_generation(tmp_path):
    """Test generating a function that returns SETOF table_name where the schema is missing."""
    functions_sql_path = FIXTURES_DIR / "setof_missing_table_function.sql"
    expected_output_path = EXPECTED_DIR / "setof_missing_table_function_api.py"
    actual_output_path = tmp_path / "setof_missing_table_function_api.py"

    # Run the generator tool (no schema file needed)
    run_cli_tool(functions_sql_path, actual_output_path)

    # Compare the generated file with the expected file
    assert actual_output_path.is_file(), "Generated file was not created."

    expected_content = expected_output_path.read_text()
    actual_content = actual_output_path.read_text()

    assert actual_content == expected_content, (
        f"Generated file content does not match expected content.\n"
        f"Expected ({expected_output_path}):\n{expected_content}\n"
        f"Actual ({actual_output_path}):\n{actual_content}"
    )


def test_returns_record_function_generation(tmp_path):
    """Test generating functions that return record or SETOF record."""
    functions_sql_path = FIXTURES_DIR / "returns_record_function.sql"
    expected_output_path = EXPECTED_DIR / "returns_record_function_api.py"
    actual_output_path = tmp_path / "returns_record_function_api.py"

    # Run the generator tool
    run_cli_tool(functions_sql_path, actual_output_path)

    # Compare the generated file with the expected file
    assert actual_output_path.is_file(), "Generated file was not created."

    expected_content = expected_output_path.read_text()
    actual_content = actual_output_path.read_text()

    assert actual_content == expected_content, (
        f"Generated file content does not match expected content.\n"
        f"Expected ({expected_output_path}):\n{expected_content}\n"
        f"Actual ({actual_output_path}):\n{actual_content}"
    )


def test_array_types_function_generation(tmp_path):
    """Test generating functions that take/return array types."""
    functions_sql_path = FIXTURES_DIR / "array_types_function.sql"
    expected_output_path = EXPECTED_DIR / "array_types_function_api.py"
    actual_output_path = tmp_path / "array_types_function_api.py"

    # Run the generator tool
    run_cli_tool(functions_sql_path, actual_output_path)

    # Compare the generated file with the expected file
    assert actual_output_path.is_file(), "Generated file was not created."

    expected_content = expected_output_path.read_text()
    actual_content = actual_output_path.read_text()

    assert actual_content == expected_content, (
        f"Generated file content does not match expected content.\n"
        f"Expected ({expected_output_path}):\n{expected_content}\n"
        f"Actual ({actual_output_path}):\n{actual_content}"
    )


def test_no_params_function_generation(tmp_path):
    """Test generating a function that takes no parameters."""
    functions_sql_path = FIXTURES_DIR / "no_params_function.sql"
    expected_output_path = EXPECTED_DIR / "no_params_function_api.py"
    actual_output_path = tmp_path / "no_params_function_api.py"

    # Run the generator tool
    run_cli_tool(functions_sql_path, actual_output_path)

    # Compare the generated file with the expected file
    assert actual_output_path.is_file(), "Generated file was not created."

    expected_content = expected_output_path.read_text()
    actual_content = actual_output_path.read_text()

    assert actual_content == expected_content, (
        f"Generated file content does not match expected content.\n"
        f"Expected ({expected_output_path}):\n{expected_content}\n"
        f"Actual ({actual_output_path}):\n{actual_content}"
    )


def test_multi_params_function_generation(tmp_path):
    """Test generating a function that takes multiple parameters."""
    functions_sql_path = FIXTURES_DIR / "multi_params_function.sql"
    expected_output_path = EXPECTED_DIR / "multi_params_function_api.py"
    actual_output_path = tmp_path / "multi_params_function_api.py"

    # Run the generator tool
    run_cli_tool(functions_sql_path, actual_output_path)

    # Compare the generated file with the expected file
    assert actual_output_path.is_file(), "Generated file was not created."

    expected_content = expected_output_path.read_text()
    actual_content = actual_output_path.read_text()

    assert actual_content == expected_content, (
        f"Generated file content does not match expected content.\n"
        f"Expected ({expected_output_path}):\n{expected_content}\n"
        f"Actual ({actual_output_path}):\n{actual_content}"
    )


def test_optional_params_function_generation(tmp_path):
    """Test generating a function with optional parameters (DEFAULT)."""
    functions_sql_path = FIXTURES_DIR / "optional_params_function.sql"
    expected_output_path = EXPECTED_DIR / "optional_params_function_api.py"
    actual_output_path = tmp_path / "optional_params_function_api.py"

    # Run the generator tool
    run_cli_tool(functions_sql_path, actual_output_path)

    # Compare the generated file with the expected file
    assert actual_output_path.is_file(), "Generated file was not created."

    expected_content = expected_output_path.read_text()
    actual_content = actual_output_path.read_text()

    assert actual_content == expected_content, (
        f"Generated file content does not match expected content.\n"
        f"Expected ({expected_output_path}):\n{expected_content}\n"
        f"Actual ({actual_output_path}):\n{actual_content}"
    )


def test_inline_schema_function_generation(tmp_path):
    """Test generating code when CREATE TABLE is in the same file."""
    functions_sql_path = FIXTURES_DIR / "inline_schema_function.sql"
    expected_output_path = EXPECTED_DIR / "inline_schema_function_api.py"
    actual_output_path = tmp_path / "inline_schema_function_api.py"

    # Run the generator tool (NO schema file argument)
    run_cli_tool(functions_sql_path, actual_output_path)

    # Compare the generated file with the expected file
    assert actual_output_path.is_file(), "Generated file was not created."

    expected_content = expected_output_path.read_text()
    actual_content = actual_output_path.read_text()

    assert actual_content == expected_content, (
        f"Generated file content does not match expected content.\n"
        f"Expected ({expected_output_path}):\n{expected_content}\n"
        f"Actual ({actual_output_path}):\n{actual_content}"
    )


def test_comment_formats_generation(tmp_path):
    """Test handling of various SQL comment formats for docstrings."""
    functions_sql_path = FIXTURES_DIR / "comment_formats.sql"
    expected_output_path = EXPECTED_DIR / "comment_formats_api.py"
    actual_output_path = tmp_path / "comment_formats_api.py"

    # Run the generator tool
    run_cli_tool(functions_sql_path, actual_output_path)

    # Compare the generated file with the expected file
    assert actual_output_path.is_file(), "Generated file was not created."

    expected_content = expected_output_path.read_text()
    actual_content = actual_output_path.read_text()

    assert actual_content == expected_content, (
        f"Generated file content does not match expected content.\n"
        f"Expected ({expected_output_path}):\n{expected_content}\n"
        f"Actual ({actual_output_path}):\n{actual_content}"
    )


def test_param_comments_function_generation(tmp_path):
    """Test generating API for function with comments in parameters."""
    functions_sql_path = FIXTURES_DIR / "param_comments_function.sql"
    expected_output_path = EXPECTED_DIR / "param_comments_function_api.py"
    actual_output_path = tmp_path / "param_comments_function_api.py"

    run_cli_tool(functions_sql_path, actual_output_path)

    assert actual_output_path.is_file(), "Generated file was not created."
    expected_content = expected_output_path.read_text()
    actual_content = actual_output_path.read_text()
    assert actual_content == expected_content, \
        f"Generated file content does not match expected.\nExpected:\n{expected_content}\nActual:\n{actual_content}"


def test_table_col_comments_generation(tmp_path):
    """Test generating API for function returning table with column comments."""
    functions_sql_path = FIXTURES_DIR / "table_col_comments.sql"
    expected_output_path = EXPECTED_DIR / "table_col_comments_api.py"
    actual_output_path = tmp_path / "table_col_comments_api.py"

    # This fixture includes CREATE TABLE, so the tool should parse it directly
    run_cli_tool(functions_sql_path, actual_output_path)

    assert actual_output_path.is_file(), "Generated file was not created."
    expected_content = expected_output_path.read_text()
    actual_content = actual_output_path.read_text()
    assert actual_content == expected_content, \
        f"Generated file content does not match expected.\nExpected:\n{expected_content}\nActual:\n{actual_content}"


def test_returns_table_comments_function_generation(tmp_path):
    """Test generating API for function with comments in RETURNS TABLE columns."""
    functions_sql_path = FIXTURES_DIR / "returns_table_comments_function.sql"
    expected_output_path = EXPECTED_DIR / "returns_table_comments_function_api.py"
    actual_output_path = tmp_path / "returns_table_comments_function_api.py"

    run_cli_tool(functions_sql_path, actual_output_path)

    assert actual_output_path.is_file(), "Generated file was not created."
    expected_content = expected_output_path.read_text()
    actual_content = actual_output_path.read_text()
    assert actual_content == expected_content, \
        f"Generated file content does not match expected.\nExpected:\n{expected_content}\nActual:\n{actual_content}"
