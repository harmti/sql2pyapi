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


def test_no_params_function_generation(tmp_path, run_cli_tool):
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


def test_multi_params_function_generation(tmp_path, run_cli_tool):
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


def test_optional_params_function_generation(tmp_path, run_cli_tool):
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


def test_array_types_function_generation(tmp_path, run_cli_tool):
    """Test functions with array parameters and return types."""
    functions_sql_path = FIXTURES_DIR / "array_types_function.sql"
    # expected_output_path = EXPECTED_DIR / "array_types_function_api.py"
    actual_output_path = tmp_path / "array_types_function_api.py"

    # Run the generator tool - Need allow_missing_schemas for the custom type
    result = run_cli_tool(functions_sql_path, actual_output_path, allow_missing_schemas=True)
    assert result.returncode == 0, f"CLI failed: {result.stderr}"

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
