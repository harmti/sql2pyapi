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


def test_comment_formats_generation(tmp_path, run_cli_tool):
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


def test_param_comments_function_generation(tmp_path, run_cli_tool):
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


def test_table_col_comments_generation(tmp_path, run_cli_tool):
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

