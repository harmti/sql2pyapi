from pathlib import Path
import ast
from dataclasses import is_dataclass
import re
import textwrap
import pytest

# Define paths relative to the main tests/ directory
TESTS_ROOT_DIR = Path(__file__).parent.parent # Go up one level to tests/
FIXTURES_DIR = TESTS_ROOT_DIR / "fixtures"
EXPECTED_DIR = TESTS_ROOT_DIR / "expected"


def test_func2_generation_with_schema(tmp_path, run_cli_tool):
    """Test generating the func2 API with a separate schema file using AST checks."""
    functions_sql_path = FIXTURES_DIR / "example_func1.sql"
    schema_sql_path = FIXTURES_DIR / "example_schema1.sql"
    expected_output_path = EXPECTED_DIR / "example_func1_api.py"
    actual_output_path = tmp_path / "example_func1_api.py"

    # Run the generator tool
    run_cli_tool(functions_sql_path, actual_output_path, schema_sql=schema_sql_path)

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


def test_inline_schema_function_generation(tmp_path, run_cli_tool):
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


def test_setof_missing_table_function_generation(tmp_path, run_cli_tool):
    """Test SETOF Table function where table definition is missing (requires flag)."""
    functions_sql_path = FIXTURES_DIR / "setof_missing_table_function.sql"
    # expected_output_path = EXPECTED_DIR / "setof_missing_table_function_api.py"
    actual_output_path = tmp_path / "setof_missing_table_function_api.py"

    # Run WITHOUT the flag first - Expect failure
    result = run_cli_tool(functions_sql_path, actual_output_path)
    assert result.returncode != 0, "CLI should fail when schema is missing and flag is not provided"
    assert "Required schema definition for table/type 'some_undefined_table' was not found" in result.stderr, "Expected error message missing in STDERR"

    # Run WITH the flag - Expect success (with placeholder comment)
    result_allowed = run_cli_tool(functions_sql_path, actual_output_path, allow_missing_schemas=True)
    assert result_allowed.returncode == 0, f"CLI failed even with --allow-missing-schemas: {result_allowed.stderr}"

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


@pytest.mark.skip(reason="Fixture file scalar_missing_schema.sql is missing")
def test_scalar_missing_schema_generation(tmp_path, run_cli_tool):
    """Test generating a function returning a missing complex type (requires flag)."""
    functions_sql_path = FIXTURES_DIR / "scalar_missing_schema.sql"
    # expected_output_path = EXPECTED_DIR / "scalar_missing_schema_api.py"
    actual_output_path = tmp_path / "scalar_missing_schema_api.py"

    # Run WITHOUT the flag - Expect failure
    result = run_cli_tool(functions_sql_path, actual_output_path)
    assert result.returncode != 0, "CLI should fail for missing type when flag is not provided"
    assert "Could not map SQL type 'public.some_other_type' to Python type" in result.stderr

    # Run WITH the flag - Expect success (with Any)
    result_allowed = run_cli_tool(functions_sql_path, actual_output_path, allow_missing_schemas=True)
    assert result_allowed.returncode == 0, f"CLI failed even with --allow-missing-schemas: {result_allowed.stderr}"

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
    func_docstring = "Function that returns a SETOF a table not defined in schema"
    assert docstring is not None and func_docstring in docstring, "Docstring mismatch"

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
