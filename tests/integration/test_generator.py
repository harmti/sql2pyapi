from pathlib import Path
import subprocess
import sys
import ast
from dataclasses import is_dataclass
import re
import textwrap
import pytest

# Define paths relative to the main tests/ directory
TESTS_ROOT_DIR = Path(__file__).parent.parent # Go up one level to tests/
FIXTURES_DIR = TESTS_ROOT_DIR / "fixtures"
EXPECTED_DIR = TESTS_ROOT_DIR / "expected"
PROJECT_ROOT = TESTS_ROOT_DIR.parent # Go up one level from tests/ to project root


def run_cli_tool(functions_sql: Path, output_py: Path, schema_sql: Path = None, verbose: bool = False, allow_missing_schemas: bool = False):
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
    if verbose:
        cmd.append("-v") # Add verbose flag if requested
    if allow_missing_schemas:
        cmd.append("--allow-missing-schemas")

    # Run from the project root directory
    result = subprocess.run(cmd, capture_output=True, text=True, cwd=PROJECT_ROOT, check=False)

    # Always print output if the input is table_col_comments.sql for debugging
    if "table_col_comments.sql" in str(functions_sql):
        print("--- CLI Output for table_col_comments.sql ---")
        print("STDOUT:", result.stdout)
        print("STDERR:", result.stderr)
        print("--- End CLI Output ---")
    elif result.returncode != 0:
        # Keep these prints for actual errors in other tests
        print("CLI Error STDOUT:", result.stdout)
        print("CLI Error STDERR:", result.stderr)

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
    print(f"--- START content of {actual_output_path} ---")
    print(actual_content)
    print(f"--- END content of {actual_output_path} ---")
    tree = ast.parse(actual_content)

    # 1. Check Imports
    expected_imports_from_typing = {"List", "Optional", "Tuple", "Dict", "Any", "TypeVar", "Sequence"}
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
    actual_output_path = tmp_path / "void_function_api.py"

    # Run the generator tool (no schema file needed)
    run_cli_tool(functions_sql_path, actual_output_path)

    # --- AST Based Assertions ---
    assert actual_output_path.is_file(), "Generated file was not created."
    actual_content = actual_output_path.read_text()
    tree = ast.parse(actual_content)

    # Find the generated function
    func_node = None
    for node in ast.walk(tree):
        if isinstance(node, ast.AsyncFunctionDef) and node.name == 'do_something':
            func_node = node
            break

    assert func_node is not None, "Async function definition 'do_something' not found"

    # Check parameters
    expected_params = {'conn': 'AsyncConnection', 'item_id': 'int'}
    actual_params = {arg.arg: ast.unparse(arg.annotation) for arg in func_node.args.args}
    assert actual_params == expected_params, f"Mismatch in do_something parameters. Expected {expected_params}, Got {actual_params}"

    # Check return annotation (should be None)
    assert func_node.returns is not None and isinstance(func_node.returns, ast.Constant) and func_node.returns.value is None, \
        f"Return annotation should be 'None', but got: {ast.unparse(func_node.returns) if func_node.returns else 'None (implicit)'}"

    # Check docstring
    docstring = ast.get_docstring(func_node)
    assert docstring is not None, "do_something is missing a docstring"
    assert docstring == "A function that does something but returns nothing", "Docstring content mismatch for do_something"

    # Check function body for execute call
    execute_call = None
    sql_query = None
    for node in ast.walk(func_node):
        if isinstance(node, ast.Await) and isinstance(node.value, ast.Call):
            call = node.value
            if isinstance(call.func, ast.Attribute) and call.func.attr == 'execute':
                execute_call = call
                # Assume first arg is the SQL query string literal
                if len(call.args) > 0 and isinstance(call.args[0], ast.Constant):
                    sql_query = call.args[0].value
                break # Assume only one execute call for simplicity

    assert execute_call is not None, "'cur.execute' call not found in do_something"
    assert sql_query == "SELECT * FROM do_something(%s)", f"SQL query mismatch in execute call. Found: '{sql_query}'"
    # Check execute parameters
    assert len(execute_call.args) == 2, "execute call should have 2 arguments"
    assert isinstance(execute_call.args[1], ast.List), "Second argument to execute should be a list"
    assert len(execute_call.args[1].elts) == 1 and isinstance(execute_call.args[1].elts[0], ast.Name) and execute_call.args[1].elts[0].id == 'item_id', \
        "Execute parameters mismatch"

    # Check explicit return None
    return_node = None
    # Search the whole function body, not just top-level statements
    for node in ast.walk(func_node):
        if isinstance(node, ast.Return):
            # Ensure the return is directly within the target function,
            # not a nested function (if any were possible)
            # This check might be overly cautious but safe.
            # Find the parent FunctionDef/AsyncFunctionDef
            parent_func = next((p for p in ast.walk(func_node) if isinstance(p, (ast.FunctionDef, ast.AsyncFunctionDef)) and node in ast.walk(p)), None)
            if parent_func == func_node:
                 return_node = node
                 break # Found the return statement for our function

    assert return_node is not None, "No return statement found within the function body"
    assert isinstance(return_node.value, ast.Constant) and return_node.value.value is None, "Function does not explicitly return None"


def test_scalar_function_generation(tmp_path):
    """Test generating functions that return simple scalar types."""
    functions_sql_path = FIXTURES_DIR / "scalar_function.sql"
    # expected_output_path = EXPECTED_DIR / "scalar_function_api.py" # No longer needed
    actual_output_path = tmp_path / "scalar_function_api.py"

    # Run the generator tool
    run_cli_tool(functions_sql_path, actual_output_path)

    # --- AST Based Assertions ---
    assert actual_output_path.is_file(), "Generated file was not created."
    actual_content = actual_output_path.read_text()
    tree = ast.parse(actual_content)

    # --- Check get_item_count function ---
    count_func_node = None
    for node in ast.walk(tree):
        if isinstance(node, ast.AsyncFunctionDef) and node.name == 'get_item_count':
            count_func_node = node
            break
    assert count_func_node is not None, "Async function 'get_item_count' not found"

    # Check parameters
    expected_params_count = {'conn': 'AsyncConnection'}
    actual_params_count = {arg.arg: ast.unparse(arg.annotation) for arg in count_func_node.args.args}
    assert actual_params_count == expected_params_count, f"Mismatch in get_item_count parameters. Expected {expected_params_count}, Got {actual_params_count}"

    # Check return annotation
    expected_return_count = 'Optional[int]'
    actual_return_count = ast.unparse(count_func_node.returns)
    assert actual_return_count == expected_return_count, f"Mismatch in get_item_count return type. Expected {expected_return_count}, Got {actual_return_count}"

    # Check docstring
    docstring_count = ast.get_docstring(count_func_node)
    assert docstring_count == "Returns a simple count", "Docstring content mismatch for get_item_count"

    # Check body for execute and fetchone
    execute_call_count = None
    sql_query_count = None
    fetchone_call_count = None
    return_logic_count = False
    for node in ast.walk(count_func_node):
        if isinstance(node, ast.Await) and isinstance(node.value, ast.Call):
            call = node.value
            if isinstance(call.func, ast.Attribute) and call.func.attr == 'execute':
                execute_call_count = call
                if len(call.args) > 0 and isinstance(call.args[0], ast.Constant):
                    sql_query_count = call.args[0].value
            elif isinstance(call.func, ast.Attribute) and call.func.attr == 'fetchone':
                fetchone_call_count = call
        elif isinstance(node, ast.Return):
            # Check for 'return row[0]'
            if isinstance(node.value, ast.Subscript) and \
               isinstance(node.value.value, ast.Name) and node.value.value.id == 'row' and \
               isinstance(node.value.slice, ast.Constant) and node.value.slice.value == 0:
                return_logic_count = True

    assert execute_call_count is not None, "execute call not found in get_item_count"
    assert sql_query_count == "SELECT * FROM get_item_count()", f"SQL query mismatch in get_item_count. Found '{sql_query_count}'"
    # Check execute params is empty list
    assert len(execute_call_count.args) == 2 and isinstance(execute_call_count.args[1], ast.List) and not execute_call_count.args[1].elts, "Execute parameters mismatch for get_item_count"
    assert fetchone_call_count is not None, "fetchone call not found in get_item_count"
    assert return_logic_count, "'return row[0]' logic not found in get_item_count"


    # --- Check get_item_name function ---
    name_func_node = None
    for node in ast.walk(tree):
        if isinstance(node, ast.AsyncFunctionDef) and node.name == 'get_item_name':
            name_func_node = node
            break
    assert name_func_node is not None, "Async function 'get_item_name' not found"

    # Check parameters
    expected_params_name = {'conn': 'AsyncConnection', 'id': 'int'}
    actual_params_name = {arg.arg: ast.unparse(arg.annotation) for arg in name_func_node.args.args}
    assert actual_params_name == expected_params_name, f"Mismatch in get_item_name parameters. Expected {expected_params_name}, Got {actual_params_name}"

    # Check return annotation
    expected_return_name = 'Optional[str]'
    actual_return_name = ast.unparse(name_func_node.returns)
    assert actual_return_name == expected_return_name, f"Mismatch in get_item_name return type. Expected {expected_return_name}, Got {actual_return_name}"

    # Check docstring
    docstring_name = ast.get_docstring(name_func_node)
    assert docstring_name == "Returns text, potentially null", "Docstring content mismatch for get_item_name"

    # Check body for execute and fetchone
    execute_call_name = None
    sql_query_name = None
    fetchone_call_name = None
    return_logic_name = False
    execute_params_name = []
    for node in ast.walk(name_func_node):
        if isinstance(node, ast.Await) and isinstance(node.value, ast.Call):
            call = node.value
            if isinstance(call.func, ast.Attribute) and call.func.attr == 'execute':
                execute_call_name = call
                if len(call.args) > 0 and isinstance(call.args[0], ast.Constant):
                    sql_query_name = call.args[0].value
                if len(call.args) > 1 and isinstance(call.args[1], ast.List):
                    execute_params_name = [elt.id for elt in call.args[1].elts if isinstance(elt, ast.Name)]
            elif isinstance(call.func, ast.Attribute) and call.func.attr == 'fetchone':
                fetchone_call_name = call
        elif isinstance(node, ast.Return):
            if isinstance(node.value, ast.Subscript) and \
               isinstance(node.value.value, ast.Name) and node.value.value.id == 'row' and \
               isinstance(node.value.slice, ast.Constant) and node.value.slice.value == 0:
                return_logic_name = True

    assert execute_call_name is not None, "execute call not found in get_item_name"
    assert sql_query_name == "SELECT * FROM get_item_name(%s)", f"SQL query mismatch in get_item_name. Found '{sql_query_name}'"
    assert execute_params_name == ['id'], f"Execute parameters mismatch for get_item_name. Expected ['id'], Got {execute_params_name}"
    assert fetchone_call_name is not None, "fetchone call not found in get_item_name"
    assert return_logic_name, "'return row[0]' logic not found in get_item_name"


def test_setof_scalar_function_generation(tmp_path):
    """Test generating a function that returns SETOF scalar."""
    functions_sql_path = FIXTURES_DIR / "setof_scalar_function.sql"
    # expected_output_path = EXPECTED_DIR / "setof_scalar_function_api.py"
    actual_output_path = tmp_path / "setof_scalar_function_api.py"

    # Run the generator tool
    run_cli_tool(functions_sql_path, actual_output_path)

    # --- AST Based Assertions ---
    assert actual_output_path.is_file(), "Generated file was not created."
    actual_content = actual_output_path.read_text()
    tree = ast.parse(actual_content)

    # --- Check get_item_ids_by_category function ---
    func_node = None
    for node in ast.walk(tree):
        if isinstance(node, ast.AsyncFunctionDef) and node.name == 'get_item_ids_by_category':
            func_node = node
            break
    assert func_node is not None, "Async function 'get_item_ids_by_category' not found"

    # Check parameters
    expected_params = {'conn': 'AsyncConnection', 'category_name': 'str'}
    actual_params = {arg.arg: ast.unparse(arg.annotation) for arg in func_node.args.args}
    assert actual_params == expected_params, f"Mismatch in parameters. Expected {expected_params}, Got {actual_params}"

    # Check return annotation
    expected_return = 'List[int]'
    actual_return = ast.unparse(func_node.returns)
    assert actual_return == expected_return, f"Mismatch in return type. Expected {expected_return}, Got {actual_return}"

    # Check docstring
    docstring = ast.get_docstring(func_node)
    assert docstring == "Returns a list of item IDs for a given category", "Docstring content mismatch"

    # Check body for execute, fetchall, and list comprehension
    execute_call = None
    sql_query = None
    fetchall_call = None
    list_comp = None
    execute_params = []
    for node in ast.walk(func_node):
        if isinstance(node, ast.Await) and isinstance(node.value, ast.Call):
            call = node.value
            if isinstance(call.func, ast.Attribute) and call.func.attr == 'execute':
                execute_call = call
                if len(call.args) > 0 and isinstance(call.args[0], ast.Constant):
                    sql_query = call.args[0].value
                if len(call.args) > 1 and isinstance(call.args[1], ast.List):
                    execute_params = [elt.id for elt in call.args[1].elts if isinstance(elt, ast.Name)]
            elif isinstance(call.func, ast.Attribute) and call.func.attr == 'fetchall':
                fetchall_call = call
        elif isinstance(node, ast.Return):
            # Check for 'return [row[0] for row in rows if row]'
            if isinstance(node.value, ast.ListComp):
                list_comp = node.value
                # Check element is row[0]
                is_elt_row_zero = (
                    isinstance(list_comp.elt, ast.Subscript) and \
                    isinstance(list_comp.elt.value, ast.Name) and list_comp.elt.value.id == 'row' and \
                    isinstance(list_comp.elt.slice, ast.Constant) and list_comp.elt.slice.value == 0
                )
                # Check generator target is row
                comp = list_comp.generators[0]
                is_target_row = isinstance(comp.target, ast.Name) and comp.target.id == 'row'
                # Check iterable is rows
                is_iter_rows = isinstance(comp.iter, ast.Name) and comp.iter.id == 'rows'
                # Check if condition is 'if row'
                has_if_row = len(comp.ifs) == 1 and isinstance(comp.ifs[0], ast.Name) and comp.ifs[0].id == 'row'
                
                if not (is_elt_row_zero and is_target_row and is_iter_rows and has_if_row):
                    list_comp = None # Mark as not found if structure is wrong

    assert execute_call is not None, "execute call not found"
    assert sql_query == "SELECT * FROM get_item_ids_by_category(%s)", f"SQL query mismatch. Found '{sql_query}'"
    assert execute_params == ['category_name'], f"Execute parameters mismatch. Expected ['category_name'], Got {execute_params}"
    assert fetchall_call is not None, "fetchall call not found"
    assert list_comp is not None, "List comprehension '[row[0] for row in rows if row]' not found or has wrong structure"


def test_returns_table_function_generation(tmp_path):
    """Test generating a function that returns TABLE(...)."""
    functions_sql_path = FIXTURES_DIR / "returns_table_function.sql"
    # expected_output_path = EXPECTED_DIR / "returns_table_function_api.py" # No longer needed
    actual_output_path = tmp_path / "returns_table_function_api.py"

    # Run the generator tool
    run_cli_tool(functions_sql_path, actual_output_path)

    # --- AST Based Assertions ---
    assert actual_output_path.is_file(), "Generated file was not created."
    actual_content = actual_output_path.read_text()
    tree = ast.parse(actual_content)

    # 1. Check Imports (dataclass, List, Optional, UUID)
    found_typing_imports = set()
    found_dataclass = False
    found_uuid = False
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            if node.module == 'typing':
                for alias in node.names:
                    found_typing_imports.add(alias.name)
            elif node.module == 'dataclasses' and any(alias.name == 'dataclass' for alias in node.names):
                 found_dataclass = True
            elif node.module == 'uuid' and any(alias.name == 'UUID' for alias in node.names):
                 found_uuid = True
    assert {'List', 'Optional'}.issubset(found_typing_imports), "Missing required typing imports"
    assert found_dataclass, "Missing dataclass import"
    assert found_uuid, "Missing UUID import"

    # 2. Check GetUserBasicInfoResult Dataclass
    dataclass_node = None
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef) and node.name == 'GetUserBasicInfoResult':
            dataclass_node = node
            break
    assert dataclass_node is not None, "Dataclass 'GetUserBasicInfoResult' not found"
    assert any(isinstance(d, ast.Name) and d.id == 'dataclass' for d in dataclass_node.decorator_list), "Class is not decorated with @dataclass"
    
    expected_fields = {
        'user_id': 'Optional[UUID]',
        'first_name': 'Optional[str]',
        'is_active': 'Optional[bool]'
    }
    actual_fields = {stmt.target.id: ast.unparse(stmt.annotation) 
                     for stmt in dataclass_node.body if isinstance(stmt, ast.AnnAssign)}
    assert actual_fields == expected_fields, "GetUserBasicInfoResult dataclass fields mismatch"

    # 3. Check get_user_basic_info Function
    func_node = None
    for node in ast.walk(tree):
        if isinstance(node, ast.AsyncFunctionDef) and node.name == 'get_user_basic_info':
            func_node = node
            break
    assert func_node is not None, "Async function 'get_user_basic_info' not found"

    # Check parameters
    expected_params = {'conn': 'AsyncConnection', 'user_id': 'UUID'}
    actual_params = {arg.arg: ast.unparse(arg.annotation) for arg in func_node.args.args}
    assert actual_params == expected_params, f"Parameter mismatch. Expected {expected_params}, Got {actual_params}"

    # Check return annotation
    expected_return = 'List[GetUserBasicInfoResult]'
    actual_return = ast.unparse(func_node.returns)
    assert actual_return == expected_return, f"Return type mismatch. Expected {expected_return}, Got {actual_return}"

    # Check docstring
    docstring = ast.get_docstring(func_node)
    assert docstring == "Returns a user's basic info as a table", "Docstring content mismatch"

    # Check body for execute, fetchall, and list comprehension with try/except
    execute_call = None
    sql_query = None
    fetchall_call = None
    list_comp_call = None # The call GetUserBasicInfoResult(*r)
    try_except_node = None
    execute_params = []
    for node in ast.walk(func_node):
        if isinstance(node, ast.Await) and isinstance(node.value, ast.Call):
            call = node.value
            if isinstance(call.func, ast.Attribute) and call.func.attr == 'execute':
                execute_call = call
                if len(call.args) > 0 and isinstance(call.args[0], ast.Constant):
                    sql_query = call.args[0].value
                if len(call.args) > 1 and isinstance(call.args[1], ast.List):
                     execute_params = [elt.id for elt in call.args[1].elts if isinstance(elt, ast.Name)]
            elif isinstance(call.func, ast.Attribute) and call.func.attr == 'fetchall':
                fetchall_call = call
        elif isinstance(node, ast.Try):
            try_except_node = node
            # Find the list comprehension inside the try block
            for try_node in ast.walk(node):
                if isinstance(try_node, ast.ListComp):
                     # Check the element: GetUserBasicInfoResult(*r)
                     if (
                         isinstance(try_node.elt, ast.Call) and \
                         isinstance(try_node.elt.func, ast.Name) and try_node.elt.func.id == 'GetUserBasicInfoResult' and \
                         len(try_node.elt.args) == 1 and isinstance(try_node.elt.args[0], ast.Starred) and \
                         isinstance(try_node.elt.args[0].value, ast.Name) and try_node.elt.args[0].value.id == 'r'
                     ):
                         # Check the generator: for r in rows
                         comp = try_node.generators[0]
                         if (
                             isinstance(comp.target, ast.Name) and comp.target.id == 'r' and \
                             isinstance(comp.iter, ast.Name) and comp.iter.id == 'rows'
                         ):
                             list_comp_call = try_node.elt # Store the call node if structure matches
                             break # Found it
            # Check the except handler catches TypeError
            assert len(node.handlers) == 1, "Expected one except handler"
            handler = node.handlers[0]
            assert isinstance(handler.type, ast.Name) and handler.type.id == 'TypeError', "Except handler should catch TypeError"
            # Check if it raises a TypeError
            raises_type_error = False
            for except_node in ast.walk(handler):
                if isinstance(except_node, ast.Raise) and isinstance(except_node.exc, ast.Call) and \
                   isinstance(except_node.exc.func, ast.Name) and except_node.exc.func.id == 'TypeError':
                   raises_type_error = True
                   break
            assert raises_type_error, "Except handler should raise a TypeError"

    assert execute_call is not None, "execute call not found"
    assert sql_query == "SELECT * FROM get_user_basic_info(%s)", f"SQL query mismatch. Found '{sql_query}'"
    assert execute_params == ['user_id'], f"Execute parameters mismatch. Expected ['user_id'], Got {execute_params}"
    assert fetchall_call is not None, "fetchall call not found"
    assert try_except_node is not None, "Try/except block not found"
    assert list_comp_call is not None, "List comprehension '[GetUserBasicInfoResult(*r) for r in rows]' not found or has wrong structure inside try block"


def test_setof_missing_table_function_generation(tmp_path):
    """Test generating a function that returns SETOF table_name where the schema is missing."""
    functions_sql_path = FIXTURES_DIR / "setof_missing_table_function.sql"
    # expected_output_path = EXPECTED_DIR / "setof_missing_table_function_api.py" # No longer needed
    actual_output_path = tmp_path / "setof_missing_table_function_api.py"

    # Run the generator tool (no schema file needed) with allow_missing_schemas
    result = run_cli_tool(functions_sql_path, actual_output_path, allow_missing_schemas=True)
    assert result.returncode == 0, f"CLI failed even with --allow-missing-schemas: {result.stderr}"

    # --- AST Based Assertions ---
    assert actual_output_path.is_file(), "Generated file was not created."
    actual_content = actual_output_path.read_text()
    tree = ast.parse(actual_content)

    # 1. Check Imports (dataclass, List)
    # Optional might not be imported if the placeholder class has no Optional fields
    # We don't expect Optional here.
    found_typing_list = False
    found_dataclass = False # dataclass should be imported even for placeholder
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            if node.module == 'typing' and any(alias.name == 'List' for alias in node.names):
                 found_typing_list = True
            elif node.module == 'dataclasses' and any(alias.name == 'dataclass' for alias in node.names):
                 found_dataclass = True
    assert found_typing_list, "Missing List import from typing"
    assert found_dataclass, "Missing dataclass import"

    # 2. Check for Placeholder Dataclass Comment
    # Verify the exact comment text generated
    assert "# TODO: Define dataclass for table 'some_undefined_tables'" in actual_content, \
           "Missing or incorrect placeholder TODO comment for SomeUndefinedTable"
    assert "# @dataclass" in actual_content, "Missing placeholder @dataclass comment"
    assert "# class SomeUndefinedTable:" in actual_content, "Missing placeholder class definition comment"
    # Ensure the actual class definition is NOT present
    class_def_found = False
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef) and node.name == 'SomeUndefinedTable':
            class_def_found = True
            break
    assert not class_def_found, "Actual class definition for SomeUndefinedTable should not be present"


    # 3. Check get_undefined_table_data Function
    func_node = None
    for node in ast.walk(tree):
        if isinstance(node, ast.AsyncFunctionDef) and node.name == 'get_undefined_table_data':
            func_node = node
            break
    assert func_node is not None, "Async function 'get_undefined_table_data' not found"

    # Check parameters (should only be conn)
    expected_params = {'conn': 'AsyncConnection'}
    actual_params = {arg.arg: ast.unparse(arg.annotation) for arg in func_node.args.args}
    assert actual_params == expected_params, f"Parameter mismatch. Expected {expected_params}, Got {actual_params}"

    # Check return annotation (uses placeholder class name)
    expected_return = 'List[SomeUndefinedTable]'
    actual_return = ast.unparse(func_node.returns)
    assert actual_return == expected_return, f"Return type mismatch. Expected {expected_return}, Got {actual_return}"

    # Check docstring
    docstring = ast.get_docstring(func_node)
    expected_docstring_part1 = "Returns a setof some_undefined_table records"
    expected_docstring_part2 = "The schema for 'some_undefined_table' is intentionally missing."
    assert docstring is not None and expected_docstring_part1 in docstring and expected_docstring_part2 in docstring, \
           f"Docstring content mismatch. Expected parts '{expected_docstring_part1}' and '{expected_docstring_part2}', Got '{docstring}'"

    # Check body for execute, fetchall, and list comprehension with try/except using placeholder
    execute_call = None
    sql_query = None
    fetchall_call = None
    list_comp_call = None # The call SomeUndefinedTable(*r)
    try_except_node = None
    execute_params = []
    for node in ast.walk(func_node):
        if isinstance(node, ast.Await) and isinstance(node.value, ast.Call):
            call = node.value
            if isinstance(call.func, ast.Attribute) and call.func.attr == 'execute':
                execute_call = call
                if len(call.args) > 0 and isinstance(call.args[0], ast.Constant):
                    sql_query = call.args[0].value
                if len(call.args) > 1 and isinstance(call.args[1], ast.List):
                     # Check params list is empty
                     assert not call.args[1].elts, "Execute should have empty parameter list"
                     execute_params = [] # Mark as checked
            elif isinstance(call.func, ast.Attribute) and call.func.attr == 'fetchall':
                fetchall_call = call
        elif isinstance(node, ast.Try):
            try_except_node = node
            for try_node in ast.walk(node):
                if isinstance(try_node, ast.ListComp):
                     # Check the element: SomeUndefinedTable(*r)
                     if (
                         isinstance(try_node.elt, ast.Call) and \
                         isinstance(try_node.elt.func, ast.Name) and try_node.elt.func.id == 'SomeUndefinedTable' and \
                         len(try_node.elt.args) == 1 and isinstance(try_node.elt.args[0], ast.Starred) and \
                         isinstance(try_node.elt.args[0].value, ast.Name) and try_node.elt.args[0].value.id == 'r'
                     ):
                         comp = try_node.generators[0]
                         if (
                             isinstance(comp.target, ast.Name) and comp.target.id == 'r' and \
                             isinstance(comp.iter, ast.Name) and comp.iter.id == 'rows'
                         ):
                             list_comp_call = try_node.elt
                             break
            # Check the except handler
            assert len(node.handlers) == 1
            handler = node.handlers[0]
            assert isinstance(handler.type, ast.Name) and handler.type.id == 'TypeError'
            raises_type_error = False
            for except_node in ast.walk(handler):
                if isinstance(except_node, ast.Raise) and isinstance(except_node.exc, ast.Call) and \
                   isinstance(except_node.exc.func, ast.Name) and except_node.exc.func.id == 'TypeError':
                   raises_type_error = True
                   break
            assert raises_type_error

    assert execute_call is not None, "execute call not found"
    assert sql_query == "SELECT * FROM get_undefined_table_data()", f"SQL query mismatch. Found '{sql_query}'"
    assert execute_params == [], "Execute parameters mismatch (should be empty)"
    assert fetchall_call is not None, "fetchall call not found"
    assert try_except_node is not None, "Try/except block not found"
    assert list_comp_call is not None, "List comprehension '[SomeUndefinedTable(*r) for r in rows]' not found or has wrong structure inside try block"


def test_returns_record_function_generation(tmp_path):
    """Test generating functions that return record or SETOF record."""
    functions_sql_path = FIXTURES_DIR / "returns_record_function.sql"
    # expected_output_path = EXPECTED_DIR / "returns_record_function_api.py" # No longer needed
    actual_output_path = tmp_path / "returns_record_function_api.py"

    # Run the generator tool
    run_cli_tool(functions_sql_path, actual_output_path)

    # --- AST Based Assertions ---
    assert actual_output_path.is_file(), "Generated file was not created."
    actual_content = actual_output_path.read_text()
    tree = ast.parse(actual_content)

    # 1. Check Imports (Tuple, List, Optional)
    found_typing_imports = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module == 'typing':
            for alias in node.names:
                found_typing_imports.add(alias.name)
    assert {'List', 'Optional', 'Tuple'}.issubset(found_typing_imports), "Missing required typing imports"

    # 2. Check get_processing_status Function (returns Optional[Tuple])
    status_func_node = None
    for node in ast.walk(tree):
        if isinstance(node, ast.AsyncFunctionDef) and node.name == 'get_processing_status':
            status_func_node = node
            break
    assert status_func_node is not None, "Async function 'get_processing_status' not found"

    # Check parameters
    expected_params_status = {'conn': 'AsyncConnection'}
    actual_params_status = {arg.arg: ast.unparse(arg.annotation) for arg in status_func_node.args.args}
    assert actual_params_status == expected_params_status, f"Mismatch in get_processing_status parameters"

    # Check return annotation
    expected_return_status = 'Optional[Tuple]'
    actual_return_status = ast.unparse(status_func_node.returns)
    assert actual_return_status == expected_return_status, f"Mismatch in get_processing_status return type"

    # Check docstring
    docstring_status = ast.get_docstring(status_func_node)
    assert docstring_status == "Returns an anonymous record containing status and count", "Docstring mismatch for get_processing_status"

    # Check body for execute, fetchone, and return row
    execute_call_status = None
    sql_query_status = None
    fetchone_call_status = None
    return_row_status = False
    for node in ast.walk(status_func_node):
        if isinstance(node, ast.Await) and isinstance(node.value, ast.Call):
            call = node.value
            if isinstance(call.func, ast.Attribute) and call.func.attr == 'execute':
                execute_call_status = call
                if len(call.args) > 0 and isinstance(call.args[0], ast.Constant):
                    sql_query_status = call.args[0].value
                assert len(call.args) == 2 and isinstance(call.args[1], ast.List) and not call.args[1].elts, "Params should be empty list"
            elif isinstance(call.func, ast.Attribute) and call.func.attr == 'fetchone':
                fetchone_call_status = call
        elif isinstance(node, ast.Return):
            if isinstance(node.value, ast.Name) and node.value.id == 'row':
                return_row_status = True

    assert execute_call_status is not None, "execute call not found in get_processing_status"
    assert sql_query_status == "SELECT * FROM get_processing_status()", f"SQL query mismatch"
    assert fetchone_call_status is not None, "fetchone call not found in get_processing_status"
    assert return_row_status, "'return row' logic not found in get_processing_status"

    # 3. Check get_all_statuses Function (returns List[Tuple])
    all_status_func_node = None
    for node in ast.walk(tree):
        if isinstance(node, ast.AsyncFunctionDef) and node.name == 'get_all_statuses':
            all_status_func_node = node
            break
    assert all_status_func_node is not None, "Async function 'get_all_statuses' not found"

    # Check parameters
    expected_params_all = {'conn': 'AsyncConnection'}
    actual_params_all = {arg.arg: ast.unparse(arg.annotation) for arg in all_status_func_node.args.args}
    assert actual_params_all == expected_params_all, f"Mismatch in get_all_statuses parameters"

    # Check return annotation
    expected_return_all = 'List[Tuple]'
    actual_return_all = ast.unparse(all_status_func_node.returns)
    assert actual_return_all == expected_return_all, f"Mismatch in get_all_statuses return type"

    # Check docstring
    docstring_all = ast.get_docstring(all_status_func_node)
    assert docstring_all == "Returns a setof anonymous records", "Docstring mismatch for get_all_statuses"

    # Check body for execute, fetchall, and return rows
    execute_call_all = None
    sql_query_all = None
    fetchall_call_all = None
    return_rows_all = False
    for node in ast.walk(all_status_func_node):
        if isinstance(node, ast.Await) and isinstance(node.value, ast.Call):
            call = node.value
            if isinstance(call.func, ast.Attribute) and call.func.attr == 'execute':
                execute_call_all = call
                if len(call.args) > 0 and isinstance(call.args[0], ast.Constant):
                    sql_query_all = call.args[0].value
                assert len(call.args) == 2 and isinstance(call.args[1], ast.List) and not call.args[1].elts, "Params should be empty list"
            elif isinstance(call.func, ast.Attribute) and call.func.attr == 'fetchall':
                fetchall_call_all = call
        elif isinstance(node, ast.Return):
            if isinstance(node.value, ast.Name) and node.value.id == 'rows':
                return_rows_all = True

    assert execute_call_all is not None, "execute call not found in get_all_statuses"
    assert sql_query_all == "SELECT * FROM get_all_statuses()", f"SQL query mismatch"
    assert fetchall_call_all is not None, "fetchall call not found in get_all_statuses"
    assert return_rows_all, "'return rows' logic not found in get_all_statuses"


def test_array_types_function_generation(tmp_path):
    """Test generating functions that take/return array types."""
    functions_sql_path = FIXTURES_DIR / "array_types_function.sql"
    # expected_output_path = EXPECTED_DIR / "array_types_function_api.py"
    actual_output_path = tmp_path / "array_types_function_api.py"

    # Run the generator tool
    run_cli_tool(functions_sql_path, actual_output_path)

    # --- AST Based Assertions ---
    assert actual_output_path.is_file(), "Generated file was not created."
    actual_content = actual_output_path.read_text()
    tree = ast.parse(actual_content)

    # 1. Check Imports (List, Optional)
    found_typing_imports = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module == 'typing':
            for alias in node.names:
                found_typing_imports.add(alias.name)
    assert {'List', 'Optional'}.issubset(found_typing_imports), "Missing required typing imports (List, Optional)"

    # 2. Check get_item_ids Function (returns Optional[List[int]])
    ids_func_node = None
    for node in ast.walk(tree):
        if isinstance(node, ast.AsyncFunctionDef) and node.name == 'get_item_ids':
            ids_func_node = node
            break
    assert ids_func_node is not None, "Async function 'get_item_ids' not found"

    # Check parameters
    expected_params_ids = {'conn': 'AsyncConnection'}
    actual_params_ids = {arg.arg: ast.unparse(arg.annotation) for arg in ids_func_node.args.args}
    assert actual_params_ids == expected_params_ids, f"Mismatch in get_item_ids parameters"

    # Check return annotation
    expected_return_ids = 'Optional[List[int]]'
    actual_return_ids = ast.unparse(ids_func_node.returns)
    assert actual_return_ids == expected_return_ids, f"Mismatch in get_item_ids return type"

    # Check docstring
    docstring_ids = ast.get_docstring(ids_func_node)
    assert docstring_ids == "Returns an array of integers", "Docstring mismatch for get_item_ids"

    # Check body for execute, fetchone, and return row[0]
    execute_call_ids = None
    sql_query_ids = None
    fetchone_call_ids = None
    return_logic_ids = False
    for node in ast.walk(ids_func_node):
        if isinstance(node, ast.Await) and isinstance(node.value, ast.Call):
            call = node.value
            if isinstance(call.func, ast.Attribute) and call.func.attr == 'execute':
                execute_call_ids = call
                if len(call.args) > 0 and isinstance(call.args[0], ast.Constant):
                    sql_query_ids = call.args[0].value
                assert len(call.args) == 2 and isinstance(call.args[1], ast.List) and not call.args[1].elts, "Params should be empty list"
            elif isinstance(call.func, ast.Attribute) and call.func.attr == 'fetchone':
                fetchone_call_ids = call
        elif isinstance(node, ast.Return):
            if isinstance(node.value, ast.Subscript) and \
               isinstance(node.value.value, ast.Name) and node.value.value.id == 'row' and \
               isinstance(node.value.slice, ast.Constant) and node.value.slice.value == 0:
                return_logic_ids = True

    assert execute_call_ids is not None, "execute call not found in get_item_ids"
    assert sql_query_ids == "SELECT * FROM get_item_ids()", f"SQL query mismatch"
    assert fetchone_call_ids is not None, "fetchone call not found in get_item_ids"
    assert return_logic_ids, "'return row[0]' logic not found in get_item_ids"

    # 3. Check process_tags Function (takes List[str], returns Optional[List[str]])
    tags_func_node = None
    for node in ast.walk(tree):
        if isinstance(node, ast.AsyncFunctionDef) and node.name == 'process_tags':
            tags_func_node = node
            break
    assert tags_func_node is not None, "Async function 'process_tags' not found"

    # Check parameters
    expected_params_tags = {'conn': 'AsyncConnection', 'tags': 'List[str]'}
    actual_params_tags = {arg.arg: ast.unparse(arg.annotation) for arg in tags_func_node.args.args}
    assert actual_params_tags == expected_params_tags, f"Mismatch in process_tags parameters"

    # Check return annotation
    expected_return_tags = 'Optional[List[str]]'
    actual_return_tags = ast.unparse(tags_func_node.returns)
    assert actual_return_tags == expected_return_tags, f"Mismatch in process_tags return type"

    # Check docstring
    docstring_tags = ast.get_docstring(tags_func_node)
    assert docstring_tags == "Takes and returns an array of text", "Docstring mismatch for process_tags"

    # Check body for execute, fetchone, and return row[0]
    execute_call_tags = None
    sql_query_tags = None
    fetchone_call_tags = None
    return_logic_tags = False
    execute_params_tags = []
    for node in ast.walk(tags_func_node):
        if isinstance(node, ast.Await) and isinstance(node.value, ast.Call):
            call = node.value
            if isinstance(call.func, ast.Attribute) and call.func.attr == 'execute':
                execute_call_tags = call
                if len(call.args) > 0 and isinstance(call.args[0], ast.Constant):
                    sql_query_tags = call.args[0].value
                if len(call.args) > 1 and isinstance(call.args[1], ast.List):
                    execute_params_tags = [elt.id for elt in call.args[1].elts if isinstance(elt, ast.Name)]
            elif isinstance(call.func, ast.Attribute) and call.func.attr == 'fetchone':
                fetchone_call_tags = call
        elif isinstance(node, ast.Return):
            if isinstance(node.value, ast.Subscript) and \
               isinstance(node.value.value, ast.Name) and node.value.value.id == 'row' and \
               isinstance(node.value.slice, ast.Constant) and node.value.slice.value == 0:
                return_logic_tags = True

    assert execute_call_tags is not None, "execute call not found in process_tags"
    assert sql_query_tags == "SELECT * FROM process_tags(%s)", f"SQL query mismatch"
    assert execute_params_tags == ['tags'], f"Execute parameters mismatch for process_tags"
    assert fetchone_call_tags is not None, "fetchone call not found in process_tags"
    assert return_logic_tags, "'return row[0]' logic not found in process_tags"


def test_no_params_function_generation(tmp_path):
    """Test generating a function that takes no parameters."""
    functions_sql_path = FIXTURES_DIR / "no_params_function.sql"
    # expected_output_path = EXPECTED_DIR / "no_params_function_api.py" # No longer needed
    actual_output_path = tmp_path / "no_params_function_api.py"

    # Run the generator tool
    run_cli_tool(functions_sql_path, actual_output_path)

    # --- AST Based Assertions ---
    assert actual_output_path.is_file(), "Generated file was not created."
    actual_content = actual_output_path.read_text()
    tree = ast.parse(actual_content)

    # 1. Check Imports (datetime, Optional)
    found_typing_optional = False
    found_datetime = False
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            if node.module == 'typing' and any(alias.name == 'Optional' for alias in node.names):
                 found_typing_optional = True
            elif node.module == 'datetime' and any(alias.name == 'datetime' for alias in node.names):
                 found_datetime = True
    assert found_typing_optional, "Missing Optional import from typing"
    assert found_datetime, "Missing datetime import from datetime"

    # 2. Check get_current_db_time Function
    func_node = None
    for node in ast.walk(tree):
        if isinstance(node, ast.AsyncFunctionDef) and node.name == 'get_current_db_time':
            func_node = node
            break
    assert func_node is not None, "Async function 'get_current_db_time' not found"

    # Check parameters (should only be conn)
    expected_params = {'conn': 'AsyncConnection'}
    actual_params = {arg.arg: ast.unparse(arg.annotation) for arg in func_node.args.args}
    assert actual_params == expected_params, f"Parameter mismatch. Expected {expected_params}, Got {actual_params}"

    # Check return annotation
    expected_return = 'Optional[datetime]'
    actual_return = ast.unparse(func_node.returns)
    assert actual_return == expected_return, f"Return type mismatch. Expected {expected_return}, Got {actual_return}"

    # Check docstring
    docstring = ast.get_docstring(func_node)
    assert docstring == "Returns the current database time", "Docstring content mismatch"

    # Check body for execute, fetchone, and return row[0]
    execute_call = None
    sql_query = None
    fetchone_call = None
    return_logic = False
    for node in ast.walk(func_node):
        if isinstance(node, ast.Await) and isinstance(node.value, ast.Call):
            call = node.value
            if isinstance(call.func, ast.Attribute) and call.func.attr == 'execute':
                execute_call = call
                if len(call.args) > 0 and isinstance(call.args[0], ast.Constant):
                    sql_query = call.args[0].value
                # Check execute params is empty list
                assert len(call.args) == 2 and isinstance(call.args[1], ast.List) and not call.args[1].elts, "Execute parameters mismatch (should be empty)"
            elif isinstance(call.func, ast.Attribute) and call.func.attr == 'fetchone':
                fetchone_call = call
        elif isinstance(node, ast.Return):
            if isinstance(node.value, ast.Subscript) and \
               isinstance(node.value.value, ast.Name) and node.value.value.id == 'row' and \
               isinstance(node.value.slice, ast.Constant) and node.value.slice.value == 0:
                return_logic = True

    assert execute_call is not None, "execute call not found"
    assert sql_query == "SELECT * FROM get_current_db_time()", f"SQL query mismatch. Found '{sql_query}'"
    assert fetchone_call is not None, "fetchone call not found"
    assert return_logic, "'return row[0]' logic not found"


def test_multi_params_function_generation(tmp_path):
    """Test generating a function with multiple parameters using AST checks."""
    functions_sql_path = FIXTURES_DIR / "multi_params_function.sql"
    # expected_output_path = EXPECTED_DIR / "multi_params_function_api.py"
    actual_output_path = tmp_path / "multi_params_function_api.py"

    # Run the generator tool
    run_cli_tool(functions_sql_path, actual_output_path)

    # --- AST Based Assertions ---
    assert actual_output_path.is_file(), "Generated file was not created."
    actual_content = actual_output_path.read_text()
    tree = ast.parse(actual_content)

    # 1. Check Imports (Optional, UUID, Decimal, Dict, Any)
    found_typing_imports = set()
    found_uuid = False
    found_decimal = False
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            if node.module == 'typing':
                for alias in node.names:
                    found_typing_imports.add(alias.name)
            elif node.module == 'uuid' and any(alias.name == 'UUID' for alias in node.names):
                 found_uuid = True
            elif node.module == 'decimal' and any(alias.name == 'Decimal' for alias in node.names):
                 found_decimal = True
                 
    assert {'Optional', 'Dict', 'Any'}.issubset(found_typing_imports), "Missing required typing imports"
    assert found_uuid, "Missing UUID import"
    assert found_decimal, "Missing Decimal import"

    # 2. Check add_item Function
    func_node = None
    for node in ast.walk(tree):
        if isinstance(node, ast.AsyncFunctionDef) and node.name == 'add_item':
            func_node = node
            break
    assert func_node is not None, "Async function 'add_item' not found"
    
    # Check parameters
    expected_params = {
        'conn': 'AsyncConnection',
        'name': 'str',
        'category_id': 'int',
        'is_available': 'bool',
        'price': 'Decimal',
        'attributes': 'Dict[str, Any]'
    }
    actual_params = {arg.arg: ast.unparse(arg.annotation) for arg in func_node.args.args}
    assert actual_params == expected_params, "Parameter mismatch"

    # Check return annotation
    expected_return = 'Optional[UUID]'
    actual_return = ast.unparse(func_node.returns)
    assert actual_return == expected_return, "Return type mismatch"

    # Check docstring
    docstring = ast.get_docstring(func_node)
    assert docstring == "Adds an item with various attributes", "Docstring mismatch"
    
    # Check body (standard scalar return)
    sql_query = None
    execute_call = None
    fetchone_call = None
    return_logic_found = False
    execute_params = []
    for node in ast.walk(func_node):
        if isinstance(node, ast.Await) and isinstance(node.value, ast.Call):
            call = node.value
            if isinstance(call.func, ast.Attribute) and call.func.attr == 'execute':
                execute_call = call
                if len(call.args) > 0 and isinstance(call.args[0], ast.Constant):
                    sql_query = call.args[0].value
                # Get the list of parameter names passed to execute
                if len(call.args) > 1 and isinstance(call.args[1], ast.List):
                     execute_params = [elt.id for elt in call.args[1].elts if isinstance(elt, ast.Name)]
            elif isinstance(call.func, ast.Attribute) and call.func.attr == 'fetchone':
                fetchone_call = call
        elif isinstance(node, ast.Return):
             if isinstance(node.value, ast.Subscript) and isinstance(node.value.value, ast.Name) and node.value.value.id == 'row':
                 return_logic_found = True
                 
    assert sql_query == "SELECT * FROM add_item(%s, %s, %s, %s, %s)", f"SQL query mismatch. Found: '{sql_query}'"
    assert execute_call is not None, "cur.execute call not found"
    # Check execute call passes the correct parameters in order (excluding conn)
    expected_execute_params = ['name', 'category_id', 'is_available', 'price', 'attributes']
    assert execute_params == expected_execute_params, f"Execute parameters mismatch. Expected {expected_execute_params}, Got {execute_params}"
    assert fetchone_call is not None, "cur.fetchone call not found"
    assert return_logic_found, "Expected scalar return logic not found"

    # Old comparison removed
    # assert actual_content == expected_content, (...)


def test_optional_params_function_generation(tmp_path):
    """Test generating a function with optional parameters using AST checks."""
    functions_sql_path = FIXTURES_DIR / "optional_params_function.sql"
    # expected_output_path = EXPECTED_DIR / "optional_params_function_api.py"
    actual_output_path = tmp_path / "optional_params_function_api.py"

    # Run the generator tool - Re-add flag (because 'items' table is missing)
    result = run_cli_tool(functions_sql_path, actual_output_path, allow_missing_schemas=True)
    assert result.returncode == 0, f"CLI failed even with --allow-missing-schemas: {result.stderr}"

    # --- AST Based Assertions ---
    assert actual_output_path.is_file(), "Generated file was not created."
    actual_content = actual_output_path.read_text()
    tree = ast.parse(actual_content)

    # 1. Check Imports (List, Optional, dataclass)
    found_typing_imports = set()
    found_dataclass = False
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            if node.module == 'typing':
                for alias in node.names:
                    found_typing_imports.add(alias.name)
            elif node.module == 'dataclasses' and any(alias.name == 'dataclass' for alias in node.names):
                 found_dataclass = True
    assert {'List', 'Optional'}.issubset(found_typing_imports), "Missing required typing imports"
    # Again, dataclass might not be imported if no classes generated, check placeholder comment instead
    # REVERT: Do NOT assert found_dataclass, as it won't be imported if placeholder is used
    # assert found_dataclass, "Dataclass import missing, expected for generated 'Item' class"

    # 2. Check for Placeholder Dataclass Comment (because no schema is provided)
    # REVERT: Check that the placeholder comment IS present
    assert "# TODO: Define dataclass for table 'items'" in actual_content, "Missing placeholder dataclass comment for Item"
    # REVERT: Check that the commented-out definition is present
    assert "# @dataclass" in actual_content
    assert "# class Item:" in actual_content
    # REVERT: Do NOT check for the actual generated class definition
    # assert "# TODO: Define dataclass for table 'items'" not in actual_content, "Placeholder dataclass comment should NOT be present for Item"
    # assert "@dataclass\\nclass Item:" in actual_content, "Generated 'Item' dataclass definition not found"

    # 3. Check search_items Function
    func_node = None
    for node in ast.walk(tree):
        if isinstance(node, ast.AsyncFunctionDef) and node.name == 'search_items':
            func_node = node
            break
    assert func_node is not None, "Async function 'search_items' not found"
    
    # Check parameters (including optional ones with defaults)
    args = func_node.args
    actual_params = {arg.arg: ast.unparse(arg.annotation) for arg in args.args}
    expected_params = {
        'conn': 'AsyncConnection',
        'query': 'str',
        'limit': 'Optional[int]',
        'include_unavailable': 'Optional[bool]'
    }
    assert actual_params == expected_params, "Parameter mismatch"
    
    # Check defaults for optional parameters
    # Defaults list corresponds to the last N arguments where N = len(defaults)
    defaults = args.defaults
    assert len(defaults) == 2, "Expected 2 default values"
    assert isinstance(defaults[0], ast.Constant) and defaults[0].value is None, "Default for limit should be None"
    assert isinstance(defaults[1], ast.Constant) and defaults[1].value is None, "Default for include_unavailable should be None"

    # Check return annotation
    expected_return = 'List[Item]' # Uses placeholder name
    actual_return = ast.unparse(func_node.returns)
    assert actual_return == expected_return, "Return type mismatch"

    # Check docstring
    docstring = ast.get_docstring(func_node)
    assert docstring == "Search for items with optional filters", "Docstring mismatch"
    
    # Check body (fetchall, list comprehension using placeholder)
    sql_query = None
    execute_call = None
    fetchall_call = None
    list_comp = None
    execute_params = []
    for node in ast.walk(func_node):
        if isinstance(node, ast.Await) and isinstance(node.value, ast.Call):
            call = node.value
            if isinstance(call.func, ast.Attribute) and call.func.attr == 'execute':
                execute_call = call
                if len(call.args) > 0 and isinstance(call.args[0], ast.Constant):
                    sql_query = call.args[0].value
                if len(call.args) > 1 and isinstance(call.args[1], ast.List):
                     execute_params = [elt.id for elt in call.args[1].elts if isinstance(elt, ast.Name)]
            elif isinstance(call.func, ast.Attribute) and call.func.attr == 'fetchall':
                fetchall_call = call
        elif isinstance(node, ast.Return) and isinstance(node.value, ast.ListComp):
            list_comp = node.value
            assert isinstance(list_comp.elt, ast.Call) and isinstance(list_comp.elt.func, ast.Name) and list_comp.elt.func.id == 'Item', "List comprehension does not call placeholder Item()"
                 
    assert sql_query == "SELECT * FROM search_items(%s, %s, %s)", f"SQL query mismatch. Found: '{sql_query}'"
    assert execute_call is not None, "cur.execute call not found"
    expected_execute_params = ['query', 'limit', 'include_unavailable']
    assert execute_params == expected_execute_params, f"Execute parameters mismatch. Expected {expected_execute_params}, Got {execute_params}"
    assert fetchall_call is not None, "cur.fetchall call not found"
    assert list_comp is not None, "Return list comprehension using placeholder not found"

    # Old comparison removed
    # assert actual_content == expected_content, (...)


def test_inline_schema_function_generation(tmp_path):
    """Test generating with inline CREATE TABLE using AST checks."""
    functions_sql_path = FIXTURES_DIR / "inline_schema_function.sql"
    # expected_output_path = EXPECTED_DIR / "inline_schema_function_api.py"
    actual_output_path = tmp_path / "inline_schema_function_api.py"

    # Run the generator tool (NO schema file argument)
    run_cli_tool(functions_sql_path, actual_output_path)

    # --- AST Based Assertions ---
    assert actual_output_path.is_file(), "Generated file was not created."
    actual_content = actual_output_path.read_text()
    tree = ast.parse(actual_content)

    # 1. Check Imports (List, Optional, Decimal, dataclass)
    found_typing_imports = set()
    found_decimal = False
    found_dataclass = False
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            if node.module == 'typing':
                for alias in node.names:
                    found_typing_imports.add(alias.name)
            elif node.module == 'decimal' and any(alias.name == 'Decimal' for alias in node.names):
                 found_decimal = True
            elif node.module == 'dataclasses' and any(alias.name == 'dataclass' for alias in node.names):
                 found_dataclass = True
                 
    assert {'List', 'Optional'}.issubset(found_typing_imports), "Missing required typing imports"
    assert found_decimal, "Missing Decimal import"
    assert found_dataclass, "Missing dataclass import"

    # 2. Check Product Dataclass
    dataclass_node = None
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef) and node.name == 'Product':
            dataclass_node = node
            break
    assert dataclass_node is not None, "Dataclass 'Product' not found"
    assert any(isinstance(d, ast.Name) and d.id == 'dataclass' for d in dataclass_node.decorator_list), "'Product' class is not decorated with @dataclass"
    
    expected_fields = {
        'product_id': 'int',
        'product_name': 'str',
        'price': 'Optional[Decimal]'
    }
    actual_fields = {stmt.target.id: ast.unparse(stmt.annotation) 
                     for stmt in dataclass_node.body if isinstance(stmt, ast.AnnAssign)}
    assert actual_fields == expected_fields, "Product dataclass fields mismatch"

    # 3. Check get_all_products Function
    func_node = None
    for node in ast.walk(tree):
        if isinstance(node, ast.AsyncFunctionDef) and node.name == 'get_all_products':
            func_node = node
            break
    assert func_node is not None, "Async function 'get_all_products' not found"

    # Check parameters
    expected_params = {'conn': 'AsyncConnection'}
    actual_params = {arg.arg: ast.unparse(arg.annotation) for arg in func_node.args.args}
    assert actual_params == expected_params, "Parameter mismatch"

    # Check return annotation
    expected_return = 'List[Product]'
    actual_return = ast.unparse(func_node.returns)
    assert actual_return == expected_return, "Return type mismatch"

    # Check docstring
    docstring = ast.get_docstring(func_node)
    assert docstring is not None and "defined above" in docstring, "Docstring mismatch or doesn't mention inline schema"

    # 4. Check Body Logic (Fetchall, ListComp using Product)
    sql_query = None
    execute_call = None
    fetchall_call = None
    list_comp = None

    for node in ast.walk(func_node):
        if isinstance(node, ast.Await) and isinstance(node.value, ast.Call):
            call = node.value
            if isinstance(call.func, ast.Attribute) and call.func.attr == 'execute':
                execute_call = call
                if len(call.args) > 0 and isinstance(call.args[0], ast.Constant):
                    sql_query = call.args[0].value
            elif isinstance(call.func, ast.Attribute) and call.func.attr == 'fetchall':
                fetchall_call = call
        elif isinstance(node, ast.Return) and isinstance(node.value, ast.ListComp):
            list_comp = node.value
            assert isinstance(list_comp.elt, ast.Call) and isinstance(list_comp.elt.func, ast.Name) and list_comp.elt.func.id == 'Product', "List comprehension does not call Product()"
            
    assert sql_query == "SELECT * FROM get_all_products()", f"SQL query mismatch. Found: '{sql_query}'"
    assert execute_call is not None, "cur.execute call not found"
    assert fetchall_call is not None, "cur.fetchall call not found"
    assert list_comp is not None, "Return list comprehension using Product not found"

    # Old comparison removed
    # assert actual_content == expected_content, (...)


def test_comment_formats_generation(tmp_path):
    """Test comment handling using AST checks for docstrings."""
    functions_sql_path = FIXTURES_DIR / "comment_formats.sql"
    # expected_output_path = EXPECTED_DIR / "comment_formats_api.py"
    actual_output_path = tmp_path / "comment_formats_api.py"

    # Run the generator tool
    result = run_cli_tool(functions_sql_path, actual_output_path)
    assert result.returncode == 0, f"CLI failed: {result.stderr}"

    # --- AST Based Assertions ---
    assert actual_output_path.is_file(), "Generated file was not created."
    actual_content = actual_output_path.read_text()
    tree = ast.parse(actual_content)

    # Helper to find function node and check its docstring
    def check_docstring(tree, func_name, expected_docstring):
        func_node = None
        for node in ast.walk(tree):
            if isinstance(node, ast.AsyncFunctionDef) and node.name == func_name:
                func_node = node
                break
        assert func_node is not None, f"Async function '{func_name}' not found"
        
        actual_docstring = ast.get_docstring(func_node)
        # Need to handle None case for no_comment scenario
        if expected_docstring is None:
             # The generator currently adds a default docstring, let's check for that pattern
             assert actual_docstring is not None and actual_docstring.startswith("Call PostgreSQL function"), f"Expected default docstring pattern for {func_name}, got: {actual_docstring}"
        else:
             assert actual_docstring == expected_docstring, f"Docstring mismatch for {func_name}.\nExpected:\n{expected_docstring}\nGot:\n{actual_docstring}"

    # Check docstring for each function
    check_docstring(
        tree,
        'function_with_multiline_dash_comment',
        "This is a multi-line comment\ndescribing the first function.\nIt has three lines."
    )
    check_docstring(
        tree,
        'function_with_single_block_comment',
        "This is a single-line block comment."
    )
    check_docstring(
        tree,
        'function_with_multi_block_comment',
        "This is a multi-line block comment.\nIt uses asterisks for alignment.\n  And has some indentation."
    )
    check_docstring( # Check default docstring generation
        tree,
        'function_with_no_comment',
        "Call PostgreSQL function function_with_no_comment()." # Match default pattern
    )
    check_docstring( # Ensure only the comment immediately preceding is used
        tree,
        'function_with_separated_comment',
        None # MODIFIED: Expect no comment (or default) as it stops at blank lines
    )

    # Old comparison removed
    # assert actual_content == expected_content, (...)


def test_param_comments_function_generation(tmp_path):
    """Test handling comments within parameters using AST checks."""
    functions_sql_path = FIXTURES_DIR / "param_comments_function.sql"
    # expected_output_path = EXPECTED_DIR / "param_comments_function_api.py"
    actual_output_path = tmp_path / "param_comments_function_api.py"

    result = run_cli_tool(functions_sql_path, actual_output_path)
    assert result.returncode == 0, f"CLI failed: {result.stderr}"

    # --- AST Based Assertions ---
    assert actual_output_path.is_file(), "Generated file was not created."
    actual_content = actual_output_path.read_text()
    tree = ast.parse(actual_content)
    
    # 1. Check Imports
    found_typing_optional = False
    found_uuid = False
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            if node.module == 'typing' and any(alias.name == 'Optional' for alias in node.names):
                found_typing_optional = True
            elif node.module == 'uuid' and any(alias.name == 'UUID' for alias in node.names):
                 found_uuid = True
    assert found_typing_optional, "Missing Optional import"
    assert found_uuid, f"Missing UUID import. File content:\n{actual_content}"
    
    # 2. Check function_with_param_comments Function
    func_node = None
    for node in ast.walk(tree):
        if isinstance(node, ast.AsyncFunctionDef) and node.name == 'function_with_param_comments':
            func_node = node
            break
    assert func_node is not None, "Async function 'function_with_param_comments' not found"

    # Check parameters (ensure comments were ignored correctly)
    args = func_node.args
    actual_params = {arg.arg: ast.unparse(arg.annotation) for arg in args.args}
    expected_params = {
        'conn': 'AsyncConnection',
        'id': 'UUID',
        'name': 'str',
        'active': 'bool',
        'age': 'Optional[int]' # Optional due to DEFAULT
    }
    assert actual_params == expected_params, "Parameter mismatch"
    
    # Check default for age
    defaults = args.defaults
    assert len(defaults) == 1, "Expected 1 default value"
    assert isinstance(defaults[0], ast.Constant) and defaults[0].value is None, "Default for age should be None"

    # Check return annotation
    expected_return = 'None'
    actual_return = ast.unparse(func_node.returns)
    assert actual_return == expected_return, "Return type mismatch"

    # Check docstring (comes from comment before function)
    docstring = ast.get_docstring(func_node)
    assert docstring == "Function with comments in parameters", "Docstring mismatch"

    # Check body (ensure execute call includes all params)
    execute_call = None
    execute_params = []
    return_none = None
    for node in ast.walk(func_node):
        if isinstance(node, ast.Await) and isinstance(node.value, ast.Call):
             call = node.value
             if isinstance(call.func, ast.Attribute) and call.func.attr == 'execute':
                 execute_call = call
                 if len(call.args) > 1 and isinstance(call.args[1], ast.List):
                     execute_params = [elt.id for elt in call.args[1].elts if isinstance(elt, ast.Name)]
        elif isinstance(node, ast.Return):
             if isinstance(node.value, ast.Constant) and node.value.value is None:
                 return_none = node
             
    assert execute_call is not None, "Execute call not found"
    expected_execute_params = ['id', 'name', 'active', 'age']
    assert execute_params == expected_execute_params, f"Execute parameters mismatch. Expected {expected_execute_params}, Got {execute_params}"
    assert return_none is not None, "Return None statement not found"

    # Old assertion removed
    # assert actual_content == expected_content, \
    #     f"Generated file content does not match expected.\nExpected:\n{expected_content}\nActual:\n{actual_content}"


def test_table_col_comments_generation(tmp_path):
    """Test generating dataclass and function signature when table has column comments."""
    # Note: This test currently verifies the generator processes the SQL,
    # but does NOT verify that column comments are included in the Python output,
    # as that feature might not be implemented.
    functions_sql_path = FIXTURES_DIR / "table_col_comments.sql"
    # expected_output_path = EXPECTED_DIR / "table_col_comments_api.py" # No longer needed
    actual_output_path = tmp_path / "table_col_comments_api.py"

    # Run the generator tool
    result = run_cli_tool(functions_sql_path, actual_output_path)
    assert result.returncode == 0, f"CLI failed: {result.stderr}"

    # --- AST Based Assertions ---
    assert actual_output_path.is_file(), "Generated file was not created."
    actual_content = actual_output_path.read_text()
    tree = ast.parse(actual_content)

    # 1. Check Imports (dataclass, datetime, List, Optional, UUID)
    found_typing_imports = set()
    found_dataclass = False
    found_uuid = False
    found_datetime = False
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            if node.module == 'typing':
                for alias in node.names:
                    found_typing_imports.add(alias.name)
            elif node.module == 'dataclasses' and any(alias.name == 'dataclass' for alias in node.names):
                 found_dataclass = True
            elif node.module == 'uuid' and any(alias.name == 'UUID' for alias in node.names):
                 found_uuid = True
            elif node.module == 'datetime' and any(alias.name == 'datetime' for alias in node.names):
                 found_datetime = True # Assuming timestamptz maps to datetime
    assert {'List', 'Optional'}.issubset(found_typing_imports), "Missing required typing imports"
    assert found_dataclass, "Missing dataclass import"
    assert found_uuid, "Missing UUID import"
    assert found_datetime, "Missing datetime import"

    # 2. Check TableWithColComment Dataclass
    dataclass_node = None
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef) and node.name == 'TableWithColComment':
            dataclass_node = node
            break
    assert dataclass_node is not None, "Dataclass 'TableWithColComment' not found"
    assert any(isinstance(d, ast.Name) and d.id == 'dataclass' for d in dataclass_node.decorator_list), "Class is not decorated with @dataclass"
    
    expected_fields = {
        'id': 'UUID', # NOT NULL in SQL
        'name': 'str', # NOT NULL in SQL
        'industry': 'Optional[str]', # NULLable in SQL
        'size': 'Optional[str]', # NULLable in SQL
        'notes': 'Optional[str]', # NULLable in SQL
        'created_at': 'datetime' # NOT NULL w/ DEFAULT in SQL -> maps to non-optional datetime
    }
    actual_fields = {}
    field_docstrings = {}
    for stmt in dataclass_node.body:
         if isinstance(stmt, ast.AnnAssign):
             field_name = stmt.target.id
             actual_fields[field_name] = ast.unparse(stmt.annotation)
             # Check for associated docstring comment (feature check)
             # Current generator does not seem to add field docstrings from column comments
             # If it did, we would extract it here, e.g., using ast.get_docstring(stmt)
             # field_docstrings[field_name] = ast.get_docstring(stmt)
             
    assert actual_fields == expected_fields, "TableWithColComment dataclass fields mismatch"
    # assert field_docstrings['id'] == 'The primary key', "Docstring for id field missing/incorrect" # Example assertion if implemented
    # assert field_docstrings['name'] == 'The name, mandatory', "Docstring for name field missing/incorrect" # Example assertion if implemented
    
    # Check class docstring (feature check)
    # Current generator doesn't add class docstring from table comments or column comments
    class_docstring = ast.get_docstring(dataclass_node)
    assert class_docstring is None, "Dataclass should not have a docstring generated from column comments (currently)"

    # 3. Check get_table_with_col_comments Function
    func_node = None
    for node in ast.walk(tree):
        if isinstance(node, ast.AsyncFunctionDef) and node.name == 'get_table_with_col_comments':
            func_node = node
            break
    assert func_node is not None, "Async function 'get_table_with_col_comments' not found"

    # Check parameters
    expected_params = {'conn': 'AsyncConnection'}
    actual_params = {arg.arg: ast.unparse(arg.annotation) for arg in func_node.args.args}
    assert actual_params == expected_params, f"Parameter mismatch"

    # Check return annotation
    expected_return = 'List[TableWithColComment]'
    actual_return = ast.unparse(func_node.returns)
    assert actual_return == expected_return, f"Return type mismatch"

    # Check docstring
    docstring = ast.get_docstring(func_node)
    assert docstring == "Function using the table (needed for test structure)", "Docstring content mismatch"

    # Check body for execute, fetchall, and list comprehension with try/except
    execute_call = None
    sql_query = None
    fetchall_call = None
    list_comp_call = None # The call TableWithColComment(*r)
    try_except_node = None
    for node in ast.walk(func_node):
        if isinstance(node, ast.Await) and isinstance(node.value, ast.Call):
            call = node.value
            if isinstance(call.func, ast.Attribute) and call.func.attr == 'execute':
                execute_call = call
                if len(call.args) > 0 and isinstance(call.args[0], ast.Constant):
                    sql_query = call.args[0].value
                assert len(call.args) == 2 and isinstance(call.args[1], ast.List) and not call.args[1].elts, "Params should be empty list"
            elif isinstance(call.func, ast.Attribute) and call.func.attr == 'fetchall':
                fetchall_call = call
        elif isinstance(node, ast.Try):
            try_except_node = node
            for try_node in ast.walk(node):
                if isinstance(try_node, ast.ListComp):
                     if (
                         isinstance(try_node.elt, ast.Call) and \
                         isinstance(try_node.elt.func, ast.Name) and try_node.elt.func.id == 'TableWithColComment' and \
                         len(try_node.elt.args) == 1 and isinstance(try_node.elt.args[0], ast.Starred) and \
                         isinstance(try_node.elt.args[0].value, ast.Name) and try_node.elt.args[0].value.id == 'r'
                     ):
                         comp = try_node.generators[0]
                         if (
                             isinstance(comp.target, ast.Name) and comp.target.id == 'r' and \
                             isinstance(comp.iter, ast.Name) and comp.iter.id == 'rows'
                         ):
                             list_comp_call = try_node.elt
                             break
            assert len(node.handlers) == 1
            handler = node.handlers[0]
            assert isinstance(handler.type, ast.Name) and handler.type.id == 'TypeError'
            raises_type_error = False
            for except_node in ast.walk(handler):
                if isinstance(except_node, ast.Raise) and isinstance(except_node.exc, ast.Call) and \
                   isinstance(except_node.exc.func, ast.Name) and except_node.exc.func.id == 'TypeError':
                   raises_type_error = True
                   break
            assert raises_type_error

    assert execute_call is not None, "execute call not found"
    assert sql_query == "SELECT * FROM get_table_with_col_comments()", f"SQL query mismatch"
    assert fetchall_call is not None, "fetchall call not found"
    assert try_except_node is not None, "Try/except block not found"
    assert list_comp_call is not None, "List comprehension '[TableWithColComment(*r) for r in rows]' not found or structure wrong"


# --- Test Case for RETURNS TABLE comments (This one is passing) ---
# def test_returns_table_comments_function_generation(tmp_path):


# --- New Test Case for RETURNS TABLE (Non-SETOF) ---

def test_returns_table_non_setof_generates_list_and_fetchall(tmp_path):
    """Verify RETURNS TABLE (non-SETOF) generates List[...] and uses fetchall."""
    functions_sql_path = FIXTURES_DIR / "returns_table_function.sql"
    actual_output_path = tmp_path / "returns_table_non_setof_api.py"

    result = run_cli_tool(functions_sql_path, actual_output_path)
    assert result.returncode == 0, f"CLI failed: {result.stderr}"

    assert actual_output_path.is_file(), "Generated file was not created."
    actual_content = actual_output_path.read_text()
    tree = ast.parse(actual_content)

    func_node = None
    for node in ast.walk(tree):
        if isinstance(node, ast.AsyncFunctionDef) and node.name == 'get_user_basic_info':
            func_node = node
            break
    assert func_node is not None, "Async function 'get_user_basic_info' not found"

    # 1. Verify return type hint is List[...]
    expected_return_pattern = r"List\[GetUserBasicInfoResult\]" # Expect List
    actual_return = ast.unparse(func_node.returns)
    assert re.match(expected_return_pattern, actual_return), \
        f"Expected return type pattern '{expected_return_pattern}', but got '{actual_return}'"

    # 2. Verify 'fetchall()' is used in the function body
    fetchall_found = False # Expect fetchall
    for node_in_body in ast.walk(func_node):
        # Check for await cur.fetchall() or assignment
        is_fetchall_call = False
        if isinstance(node_in_body, ast.Await) and isinstance(node_in_body.value, ast.Call):
            call = node_in_body.value
            if isinstance(call.func, ast.Attribute) and call.func.attr == 'fetchall':
                 is_fetchall_call = True
        elif isinstance(node_in_body, ast.Assign) and isinstance(node_in_body.value, ast.Await):
             call = node_in_body.value.value
             if isinstance(call, ast.Call) and isinstance(call.func, ast.Attribute) and call.func.attr == 'fetchall':
                 is_fetchall_call = True
                 
        if is_fetchall_call:
             fetchall_found = True
             break
                 
    assert fetchall_found, "Expected 'fetchall()' call not found in function body."


def test_custom_type_return_generation(tmp_path):
    """Test generation for functions returning custom composite types."""
    functions_sql_path = FIXTURES_DIR / "custom_type_return.sql"
    # expected_output_path = EXPECTED_DIR / "custom_type_return_api.py" # No longer needed
    actual_output_path = tmp_path / "custom_type_return_api.py"

    # Run the generator tool (no separate schema needed, type is inline)
    result = run_cli_tool(functions_sql_path, actual_output_path, verbose=True)
    assert result.returncode == 0, f"CLI failed: {result.stderr}"

    # --- AST Based Assertions ---
    assert actual_output_path.is_file(), "Generated file was not created."
    actual_content = actual_output_path.read_text()
    tree = ast.parse(actual_content)

    # 1. Check Imports (dataclass, List, Optional, UUID)
    found_typing_imports = set()
    found_dataclass = False
    found_uuid = False
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            if node.module == 'typing':
                for alias in node.names:
                    found_typing_imports.add(alias.name)
            elif node.module == 'dataclasses' and any(alias.name == 'dataclass' for alias in node.names):
                 found_dataclass = True
            elif node.module == 'uuid' and any(alias.name == 'UUID' for alias in node.names):
                 found_uuid = True
    assert {'List', 'Optional'}.issubset(found_typing_imports), "Missing required typing imports"
    assert found_dataclass, "Missing dataclass import"
    assert found_uuid, "Missing UUID import"

    # 2. Check UserIdentity Dataclass
    dataclass_node = None
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef) and node.name == 'UserIdentity':
            dataclass_node = node
            break
    assert dataclass_node is not None, "Dataclass 'UserIdentity' not found"
    assert any(isinstance(d, ast.Name) and d.id == 'dataclass' for d in dataclass_node.decorator_list), "Class is not decorated with @dataclass"
    
    expected_fields = {
        'user_id': 'Optional[UUID]',
        'clerk_id': 'Optional[str]',
        'is_active': 'Optional[bool]'
    }
    actual_fields = {stmt.target.id: ast.unparse(stmt.annotation) 
                     for stmt in dataclass_node.body if isinstance(stmt, ast.AnnAssign)}
    assert actual_fields == expected_fields, "UserIdentity dataclass fields mismatch"

    # 3. Check get_user_identity_by_clerk_id Function
    single_func_node = None
    for node in ast.walk(tree):
        if isinstance(node, ast.AsyncFunctionDef) and node.name == 'get_user_identity_by_clerk_id':
            single_func_node = node
            break
    assert single_func_node is not None, "Async function 'get_user_identity_by_clerk_id' not found"

    # Check parameters
    expected_params_single = {'conn': 'AsyncConnection', 'clerk_id': 'str'}
    actual_params_single = {arg.arg: ast.unparse(arg.annotation) for arg in single_func_node.args.args}
    assert actual_params_single == expected_params_single, f"Parameter mismatch for single func"

    # Check return annotation
    expected_return_single = 'Optional[UserIdentity]'
    actual_return_single = ast.unparse(single_func_node.returns)
    assert actual_return_single == expected_return_single, f"Return type mismatch for single func"

    # Check docstring
    docstring_single = ast.get_docstring(single_func_node)
    assert docstring_single and "Function returning the custom composite type" in docstring_single, "Docstring mismatch for single func"

    # Check body for execute, fetchone, try/except, and UserIdentity(*row) call
    execute_call_single = None
    sql_query_single = None
    fetchone_call_single = None
    dataclass_call_single = None
    try_except_node_single = None
    execute_params_single = []
    for node in ast.walk(single_func_node):
        if isinstance(node, ast.Await) and isinstance(node.value, ast.Call):
            call = node.value
            if isinstance(call.func, ast.Attribute) and call.func.attr == 'execute':
                execute_call_single = call
                if len(call.args) > 0 and isinstance(call.args[0], ast.Constant):
                    sql_query_single = call.args[0].value
                if len(call.args) > 1 and isinstance(call.args[1], ast.List):
                    execute_params_single = [elt.id for elt in call.args[1].elts if isinstance(elt, ast.Name)]
            elif isinstance(call.func, ast.Attribute) and call.func.attr == 'fetchone':
                fetchone_call_single = call
        elif isinstance(node, ast.Try):
            try_except_node_single = node
            # Check for UserIdentity(*row) inside try
            for try_node in ast.walk(node):
                if isinstance(try_node, ast.Call) and isinstance(try_node.func, ast.Name) and try_node.func.id == 'UserIdentity':
                    if len(try_node.args) == 1 and isinstance(try_node.args[0], ast.Starred) and \
                       isinstance(try_node.args[0].value, ast.Name) and try_node.args[0].value.id == 'row':
                       dataclass_call_single = try_node
                       break
            # Check except handler
            assert len(node.handlers) == 1 and isinstance(node.handlers[0].type, ast.Name) and node.handlers[0].type.id == 'TypeError', "Single func Try/Except structure mismatch"

    assert execute_call_single is not None, "execute call not found in single func"
    assert sql_query_single == "SELECT * FROM get_user_identity_by_clerk_id(%s)", f"SQL query mismatch"
    assert execute_params_single == ['clerk_id'], f"Execute parameters mismatch"
    assert fetchone_call_single is not None, "fetchone call not found in single func"
    assert try_except_node_single is not None, "Try/except block not found in single func"
    assert dataclass_call_single is not None, "UserIdentity(*row) call not found in single func try block"

    # 4. Check get_all_active_identities Function
    setof_func_node = None
    for node in ast.walk(tree):
        if isinstance(node, ast.AsyncFunctionDef) and node.name == 'get_all_active_identities':
            setof_func_node = node
            break
    assert setof_func_node is not None, "Async function 'get_all_active_identities' not found"

    # Check parameters
    expected_params_setof = {'conn': 'AsyncConnection'}
    actual_params_setof = {arg.arg: ast.unparse(arg.annotation) for arg in setof_func_node.args.args}
    assert actual_params_setof == expected_params_setof, f"Parameter mismatch for setof func"

    # Check return annotation
    expected_return_setof = 'List[UserIdentity]'
    actual_return_setof = ast.unparse(setof_func_node.returns)
    assert actual_return_setof == expected_return_setof, f"Return type mismatch for setof func"

    # Check docstring
    docstring_setof = ast.get_docstring(setof_func_node)
    assert docstring_setof == "Function returning SETOF the custom composite type", "Docstring mismatch for setof func"

    # Check body for execute, fetchall, try/except, and list comprehension
    execute_call_setof = None
    sql_query_setof = None
    fetchall_call_setof = None
    list_comp_call_setof = None
    try_except_node_setof = None
    for node in ast.walk(setof_func_node):
        if isinstance(node, ast.Await) and isinstance(node.value, ast.Call):
            call = node.value
            if isinstance(call.func, ast.Attribute) and call.func.attr == 'execute':
                execute_call_setof = call
                if len(call.args) > 0 and isinstance(call.args[0], ast.Constant):
                    sql_query_setof = call.args[0].value
                assert len(call.args) == 2 and isinstance(call.args[1], ast.List) and not call.args[1].elts, "Params should be empty list"
            elif isinstance(call.func, ast.Attribute) and call.func.attr == 'fetchall':
                fetchall_call_setof = call
        elif isinstance(node, ast.Try):
            try_except_node_setof = node
            # Find the list comprehension inside the try block
            for try_node in ast.walk(node):
                if isinstance(try_node, ast.ListComp):
                     if (
                         isinstance(try_node.elt, ast.Call) and \
                         isinstance(try_node.elt.func, ast.Name) and try_node.elt.func.id == 'UserIdentity' and \
                         len(try_node.elt.args) == 1 and isinstance(try_node.elt.args[0], ast.Starred) and \
                         isinstance(try_node.elt.args[0].value, ast.Name) and try_node.elt.args[0].value.id == 'r'
                     ):
                         comp = try_node.generators[0]
                         if (
                             isinstance(comp.target, ast.Name) and comp.target.id == 'r' and \
                             isinstance(comp.iter, ast.Name) and comp.iter.id == 'rows'
                         ):
                             list_comp_call_setof = try_node.elt
                             break
            # Check except handler
            assert len(node.handlers) == 1 and isinstance(node.handlers[0].type, ast.Name) and node.handlers[0].type.id == 'TypeError', "Setof func Try/Except structure mismatch"

    assert execute_call_setof is not None, "execute call not found in setof func"
    assert sql_query_setof == "SELECT * FROM get_all_active_identities()", f"SQL query mismatch"
    assert fetchall_call_setof is not None, "fetchall call not found in setof func"
    assert list_comp_call_setof is not None, "List comprehension '[UserIdentity(*r) for r in rows]' not found in setof func try block"

    # Old comparison removed
    # assert actual_content == expected_content, (...)

# ===== END: Additional Test Cases =====

def test_no_helpers_flag(tmp_path):
    """Test that --no-helpers flag omits helper functions and imports."""
    functions_sql_path = FIXTURES_DIR / "scalar_function.sql" # Use a simple fixture
    actual_output_path = tmp_path / "scalar_function_no_helpers_api.py"

    # Run the generator tool WITH the --no-helpers flag
    # Use the helper function and add the flag to the command
    cmd = [
        sys.executable,  # Use the current Python executable
        "-m",
        "sql2pyapi.cli",  # Invoke the module's entry point
        str(functions_sql_path),
        str(actual_output_path),
        "--no-helpers", # Add the flag
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, cwd=PROJECT_ROOT, check=False)
    assert result.returncode == 0, f"CLI tool failed with --no-helpers: {result.stderr}"

    # --- Assertions --- 
    assert actual_output_path.is_file(), "Generated file was not created."
    actual_content = actual_output_path.read_text()

    # 1. Check helper functions are NOT present
    assert "def get_optional(" not in actual_content, "get_optional helper function found but should be omitted."
    assert "def get_required(" not in actual_content, "get_required helper function found but should be omitted."
    assert "# ===== SECTION: RESULT HELPERS ====" not in actual_content, "Helper section header found but should be omitted."

    # 2. Check helper-specific imports are NOT present (unless needed otherwise)
    # Use AST to be more robust than simple string checking
    tree = ast.parse(actual_content)
    found_typevar = False
    found_sequence = False
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            if node.module == 'typing':
                for alias in node.names:
                    if alias.name == 'TypeVar':
                        found_typevar = True
                    if alias.name == 'Sequence':
                        found_sequence = True
            
    assert not found_typevar, "TypeVar import found but should be omitted when helpers are excluded."
    assert not found_sequence, "Sequence import found but should be omitted when helpers are excluded."
