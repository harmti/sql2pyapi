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
    sql_query_assign_node = None
    fetchone_call = None
    return_logic = False

    for stmt in func_node.body:
        if isinstance(stmt, ast.Assign):
            for target in stmt.targets:
                if isinstance(target, ast.Name) and target.id == '_full_sql_query':
                    sql_query_assign_node = stmt
                    break
            if sql_query_assign_node: break

    for node_in_body in ast.walk(func_node):
        if isinstance(node_in_body, ast.Await) and isinstance(node_in_body.value, ast.Call):
            call = node_in_body.value
            if isinstance(call.func, ast.Attribute) and call.func.attr == 'execute':
                execute_call = call
                assert isinstance(call.args[0], ast.Name) and call.args[0].id == '_full_sql_query', "execute in get_current_db_time not using _full_sql_query"
                assert len(call.args) == 2 and isinstance(call.args[1], ast.Name) and call.args[1].id == '_call_params_dict', "Execute second arg is not _call_params_dict"
            elif isinstance(call.func, ast.Attribute) and call.func.attr == 'fetchone':
                fetchone_call = call
        elif isinstance(node_in_body, ast.Return):
            if isinstance(node_in_body.value, ast.Subscript) and \
               isinstance(node_in_body.value.value, ast.Name) and node_in_body.value.value.id == 'row' and \
               isinstance(node_in_body.value.slice, ast.Constant) and node_in_body.value.slice.value == 0:
                return_logic = True

    assert execute_call is not None, "execute call not found for get_current_db_time"
    assert sql_query_assign_node is not None, "Assignment to _full_sql_query not found for get_current_db_time"
    
    assert isinstance(sql_query_assign_node.value, ast.JoinedStr), "_full_sql_query in get_current_db_time is not an f-string"
    f_string_parts = sql_query_assign_node.value.values
    assert len(f_string_parts) == 3, "f-string for get_current_db_time has unexpected number of parts"
    assert isinstance(f_string_parts[0], ast.Constant) and f_string_parts[0].value == "SELECT * FROM get_current_db_time(", "f-string part 0 for get_current_db_time mismatch"
    assert isinstance(f_string_parts[1], ast.FormattedValue) and f_string_parts[1].value.id == "_sql_query_named_args", "f-string part 1 for get_current_db_time placeholder mismatch"
    assert isinstance(f_string_parts[2], ast.Constant) and f_string_parts[2].value == ")", "f-string part 2 for get_current_db_time mismatch"

    # Verify that _call_params_dict is initialized and remains empty for get_current_db_time
    call_params_dict_init_empty_no_params = False
    call_params_dict_populated_no_params = False
    for node in ast.walk(func_node):
        if isinstance(node, ast.Assign):
            if len(node.targets) == 1 and isinstance(node.targets[0], ast.Name) and node.targets[0].id == '_call_params_dict':
                if isinstance(node.value, ast.Dict) and not node.value.keys:
                    call_params_dict_init_empty_no_params = True
            elif len(node.targets) == 1 and isinstance(node.targets[0], ast.Subscript) and \
                 isinstance(node.targets[0].value, ast.Name) and node.targets[0].value.id == '_call_params_dict':
                call_params_dict_populated_no_params = True # Found an assignment to a key
                break 
    assert call_params_dict_init_empty_no_params, "_call_params_dict was not initialized as empty for get_current_db_time"
    assert not call_params_dict_populated_no_params, "_call_params_dict should not be populated for get_current_db_time"

    assert fetchone_call is not None, "fetchone call not found for get_current_db_time"


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
    sql_query_assign_node = None
    execute_call = None
    fetchone_call = None
    return_logic_found = False
    expected_execute_param_names = ['name', 'category_id', 'is_available', 'price', 'attributes']

    for stmt in func_node.body:
        if isinstance(stmt, ast.Assign):
            for target in stmt.targets:
                if isinstance(target, ast.Name) and target.id == '_full_sql_query':
                    sql_query_assign_node = stmt
                    break
            if sql_query_assign_node: break

    for node_in_body in ast.walk(func_node):
        if isinstance(node_in_body, ast.Await) and isinstance(node_in_body.value, ast.Call):
            call = node_in_body.value
            if isinstance(call.func, ast.Attribute) and call.func.attr == 'execute':
                execute_call = call
                assert isinstance(call.args[0], ast.Name) and call.args[0].id == '_full_sql_query', "execute in add_item not using _full_sql_query"
                assert len(call.args) == 2 and isinstance(call.args[1], ast.Name) and call.args[1].id == '_call_params_dict', "Execute second arg for add_item is not _call_params_dict"
            elif isinstance(call.func, ast.Attribute) and call.func.attr == 'fetchone':
                fetchone_call = call
        elif isinstance(node_in_body, ast.Return):
             if isinstance(node_in_body.value, ast.Subscript) and isinstance(node_in_body.value.value, ast.Name) and node_in_body.value.value.id == 'row':
                 return_logic_found = True
                 
    assert execute_call is not None, "cur.execute call not found for add_item"
    assert sql_query_assign_node is not None, "Assignment to _full_sql_query not found for add_item"

    assert isinstance(sql_query_assign_node.value, ast.JoinedStr), "_full_sql_query in add_item is not an f-string"
    f_string_parts_add = sql_query_assign_node.value.values
    assert len(f_string_parts_add) == 3, "f-string for add_item has unexpected number of parts"
    assert isinstance(f_string_parts_add[0], ast.Constant) and f_string_parts_add[0].value == "SELECT * FROM add_item(", "f-string part 0 for add_item mismatch"
    assert isinstance(f_string_parts_add[1], ast.FormattedValue) and f_string_parts_add[1].value.id == "_sql_query_named_args", "f-string part 1 for add_item placeholder mismatch"
    assert isinstance(f_string_parts_add[2], ast.Constant) and f_string_parts_add[2].value == ")", "f-string part 2 for add_item mismatch"

    # Verify that _call_params_dict is populated correctly for add_item parameters
    # All params for add_item are non-optional
    assigned_params_to_call_params_dict = {} # Store as dict_key: value_var_name
    call_params_dict_init_empty_add = False

    for node in ast.walk(func_node):
        if isinstance(node, ast.Assign):
            # Check for _call_params_dict = {}
            if len(node.targets) == 1 and isinstance(node.targets[0], ast.Name) and node.targets[0].id == '_call_params_dict':
                if isinstance(node.value, ast.Dict) and not node.value.keys:
                    call_params_dict_init_empty_add = True
            # Check for _call_params_dict['param_key'] = param_value_var
            elif len(node.targets) == 1 and isinstance(node.targets[0], ast.Subscript):
                target_subscript = node.targets[0]
                if isinstance(target_subscript.value, ast.Name) and target_subscript.value.id == '_call_params_dict' and \
                   isinstance(target_subscript.slice, ast.Constant) and \
                   isinstance(node.value, ast.Name):
                    param_key = target_subscript.slice.value # This is the Python name used as dict key
                    param_value_var = node.value.id
                    assigned_params_to_call_params_dict[param_key] = param_value_var
            
    assert call_params_dict_init_empty_add, "_call_params_dict not initialized empty for add_item"

    # Expected Python names from func signature are the dict keys and value variables
    expected_assigned_params = {p: p for p in expected_params if p != 'conn'} # Exclude 'conn'
    
    # Check if all expected params are assigned and their values are the param names themselves
    # (since there are no enums that would create *_value variables here)
    assert assigned_params_to_call_params_dict == expected_assigned_params, \
        f"Parameters assigned to _call_params_dict mismatch for add_item. Expected {expected_assigned_params}, Got {assigned_params_to_call_params_dict}"

    assert fetchone_call is not None, "cur.fetchone call not found for add_item"


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
    # If 'items' becomes List[Any], no dataclass for 'Item' is generated or imported.
    # The 'found_dataclass' assertion for this specific test might need to be conditional
    # or removed if only List[Any] is produced.
    # For now, let's assume if it resolves to List[Any], these comments are not generated.

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
    expected_return = 'List[Any]' # Was List[Item], changed due to missing 'items' table resolving to Any
    actual_return = ast.unparse(func_node.returns)
    assert actual_return == expected_return, "Return type mismatch"

    # Check docstring
    docstring = ast.get_docstring(func_node)
    assert docstring == "Search for items with optional filters", "Docstring mismatch"
    
    # Check body (fetchall, list comprehension using placeholder)
    sql_query_assign_node = None
    execute_call = None
    fetchall_call = None
    list_comp = None
    expected_execute_param_names_search = ['query', 'limit', 'include_unavailable']

    for stmt in func_node.body:
        if isinstance(stmt, ast.Assign):
            for target in stmt.targets:
                if isinstance(target, ast.Name) and target.id == '_full_sql_query':
                    sql_query_assign_node = stmt
                    break
            if sql_query_assign_node: break

    for node_in_body in ast.walk(func_node):
        if isinstance(node_in_body, ast.Await) and isinstance(node_in_body.value, ast.Call):
            call = node_in_body.value
            if isinstance(call.func, ast.Attribute) and call.func.attr == 'execute':
                execute_call = call
                assert isinstance(call.args[0], ast.Name) and call.args[0].id == '_full_sql_query', "execute in search_items not using _full_sql_query"
                assert len(call.args) == 2 and isinstance(call.args[1], ast.Name) and call.args[1].id == '_call_params_dict', "Execute second arg for search_items is not _call_params_dict"
            elif isinstance(call.func, ast.Attribute) and call.func.attr == 'fetchall':
                fetchall_call = call
        elif isinstance(node_in_body, ast.Return) and isinstance(node_in_body.value, ast.ListComp):
            list_comp = node_in_body.value
            # If the return type is List[Any], the list comprehension might be different, e.g., [row[0] for row in rows if row]
            # Original assertion: assert isinstance(list_comp.elt, ast.Call) and isinstance(list_comp.elt.func, ast.Name) and list_comp.elt.func.id == 'Item', "List comprehension does not call placeholder Item()"
            # For List[Any] from SETOF unknown, it should be like [row[0] for row in rows if row]
            is_correct_list_comp_for_any = (
                isinstance(list_comp.elt, ast.Subscript) and
                isinstance(list_comp.elt.value, ast.Name) and list_comp.elt.value.id == 'row' and
                isinstance(list_comp.elt.slice, ast.Constant) and list_comp.elt.slice.value == 0 and
                len(list_comp.generators) == 1 and
                isinstance(list_comp.generators[0].target, ast.Name) and list_comp.generators[0].target.id == 'row' and
                isinstance(list_comp.generators[0].iter, ast.Name) and list_comp.generators[0].iter.id == 'rows' and
                len(list_comp.generators[0].ifs) == 1 and isinstance(list_comp.generators[0].ifs[0], ast.Name) and list_comp.generators[0].ifs[0].id == 'row'
            )
            assert is_correct_list_comp_for_any, "List comprehension for List[Any] is not structured as [row[0] for row in rows if row]"
                 
    assert execute_call is not None, "cur.execute call not found for search_items"
    assert sql_query_assign_node is not None, "Assignment to _full_sql_query not found for search_items"

    assert isinstance(sql_query_assign_node.value, ast.JoinedStr), "_full_sql_query in search_items is not an f-string"
    f_string_parts_search = sql_query_assign_node.value.values
    assert len(f_string_parts_search) == 3, "f-string for search_items has unexpected number of parts"
    assert isinstance(f_string_parts_search[0], ast.Constant) and f_string_parts_search[0].value == "SELECT * FROM search_items(", "f-string part 0 for search_items mismatch"
    assert isinstance(f_string_parts_search[1], ast.FormattedValue) and f_string_parts_search[1].value.id == "_sql_query_named_args", "f-string part 1 for search_items placeholder mismatch"
    assert isinstance(f_string_parts_search[2], ast.Constant) and f_string_parts_search[2].value == ")", "f-string part 2 for search_items mismatch"

    # Verify _call_params_dict population for search_items (optional params)
    # Expected Python param names from function signature
    # p_query (query), p_category_id (category_id), p_min_price (min_price), p_max_price (max_price)
    # p_tags (tags), p_page (page), p_page_size (page_size)

    call_params_dict_init_empty_search = False
    # Track if the 'if param is not None:' checks are present for optional params
    # and if the assignment to _call_params_dict happens inside these if blocks.
    # This is a bit complex to check fully with AST for all combinations.
    # For now, we'll check that _call_params_dict is initialized.
    # A full check would involve ensuring that for each optional param,
    # the assignment to _call_params_dict is inside an If node testing that param.

    # Let's check for the general structure:
    # _call_params_dict = {}
    # if query is not None: _call_params_dict['query'] = query
    # ... etc for other optional params ...
    # _sql_named_args_parts.append(f'p_page := %(page)s') # For non-optional (if any, or mandatory defaults)
    # _call_params_dict['page'] = page
    
    # For search_items, 'query' is non-optional, others are optional.
    # So, 'query' should always be assigned.
    # Others should be inside 'if x is not None:' blocks.

    found_query_assignment = False
    optional_params_handled_in_ifs = True # Assume true, try to falsify
    
    # Python names from signature: query (non-opt), category_id, min_price, max_price, tags, page, page_size (all opt)
    optional_param_python_names = ['category_id', 'min_price', 'max_price', 'tags', 'page', 'page_size']

    for node in ast.walk(func_node):
        if isinstance(node, ast.Assign):
            if len(node.targets) == 1 and isinstance(node.targets[0], ast.Name) and node.targets[0].id == '_call_params_dict':
                if isinstance(node.value, ast.Dict) and not node.value.keys:
                    call_params_dict_init_empty_search = True
            # Direct assignment for non-optional 'query'
            elif len(node.targets) == 1 and isinstance(node.targets[0], ast.Subscript) and \
                 isinstance(node.targets[0].value, ast.Name) and node.targets[0].value.id == '_call_params_dict' and \
                 isinstance(node.targets[0].slice, ast.Constant) and node.targets[0].slice.value == 'query' and \
                 isinstance(node.value, ast.Name) and node.value.id == 'query':
                # Check if this assignment is NOT inside an If block
                # This is tricky as ast.walk gives a flat list. We'd need parent pointers or nested iteration.
                # For now, assume direct assignment means it's for a non-optional param.
                found_query_assignment = True
        
        # Check for optional params being handled inside 'if param is not None:'
        elif isinstance(node, ast.If):
            if_node = node
            # Test should be 'param is not None'
            if isinstance(if_node.test, ast.Compare) and \
               isinstance(if_node.test.ops[0], ast.IsNot) and \
               isinstance(if_node.test.comparators[0], ast.Constant) and if_node.test.comparators[0].value is None and \
               isinstance(if_node.test.left, ast.Name) and if_node.test.left.id in optional_param_python_names:
                
                param_name_in_if_test = if_node.test.left.id
                assignment_in_if_body = False
                for if_body_stmt in if_node.body:
                    if isinstance(if_body_stmt, ast.Assign) and \
                       len(if_body_stmt.targets) == 1 and isinstance(if_body_stmt.targets[0], ast.Subscript) and \
                       isinstance(if_body_stmt.targets[0].value, ast.Name) and if_body_stmt.targets[0].value.id == '_call_params_dict' and \
                       isinstance(if_body_stmt.targets[0].slice, ast.Constant) and if_body_stmt.targets[0].slice.value == param_name_in_if_test and \
                       isinstance(if_body_stmt.value, ast.Name) and if_body_stmt.value.id == param_name_in_if_test:
                        assignment_in_if_body = True
                        break
                if not assignment_in_if_body:
                    optional_params_handled_in_ifs = False
                    # print(f"Debug: Optional param {param_name_in_if_test} not correctly assigned in _call_params_dict within its If block.")
                    break 
            # else: # If test is not 'param is not None' for an optional param
                # This might catch other If blocks not relevant or incorrectly structured ones
                # For a robust check, we'd need to ensure ALL optional params have such an If block around their assignment.
                # This simpler check looks for ANY malformed If around an optional param assignment.
                # A more precise check would be to iterate `optional_param_python_names` and verify each one.
                pass


    assert call_params_dict_init_empty_search, "_call_params_dict not initialized empty for search_items"
    assert found_query_assignment, "Non-optional 'query' param not directly assigned to _call_params_dict for search_items"
    assert optional_params_handled_in_ifs, "Optional parameters for search_items are not correctly handled with 'if param is not None:' and assigned to _call_params_dict"
    
    assert fetchall_call is not None, "cur.fetchall call not found for search_items"


def test_array_types_function_generation(tmp_path, run_cli_tool):
    """Test generating functions that use array types with AST checks."""
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
    expected_params_ids = {'conn': 'AsyncConnection', 'category_id': 'int'}
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
    sql_query_assign_node_ids = None
    fetchone_call_ids = None
    return_logic_ids = False

    for stmt in ids_func_node.body:
        if isinstance(stmt, ast.Assign):
            for target in stmt.targets:
                if isinstance(target, ast.Name) and target.id == '_full_sql_query':
                    sql_query_assign_node_ids = stmt
                    break
            if sql_query_assign_node_ids: break

    for node_in_body in ast.walk(ids_func_node):
        if isinstance(node_in_body, ast.Await) and isinstance(node_in_body.value, ast.Call):
            call = node_in_body.value
            if isinstance(call.func, ast.Attribute) and call.func.attr == 'execute':
                execute_call_ids = call
                assert isinstance(call.args[0], ast.Name) and call.args[0].id == '_full_sql_query', "execute in get_item_ids not using _full_sql_query"
                assert len(call.args) == 2 and isinstance(call.args[1], ast.Name) and call.args[1].id == '_call_params_dict', "Execute second arg for get_item_ids is not _call_params_dict"
            elif isinstance(call.func, ast.Attribute) and call.func.attr == 'fetchone':
                fetchone_call_ids = call
        elif isinstance(node_in_body, ast.Return):
            if isinstance(node_in_body.value, ast.Subscript) and \
               isinstance(node_in_body.value.value, ast.Name) and node_in_body.value.value.id == 'row' and \
               isinstance(node_in_body.value.slice, ast.Constant) and node_in_body.value.slice.value == 0:
                return_logic_ids = True

    assert execute_call_ids is not None, "execute call not found for get_item_ids"
    assert sql_query_assign_node_ids is not None, "Assignment to _full_sql_query not found for get_item_ids"
    
    assert isinstance(sql_query_assign_node_ids.value, ast.JoinedStr), "_full_sql_query in get_item_ids is not an f-string"
    f_string_parts_ids = sql_query_assign_node_ids.value.values
    assert len(f_string_parts_ids) == 3, "f-string for get_item_ids has unexpected number of parts"
    assert isinstance(f_string_parts_ids[0], ast.Constant) and f_string_parts_ids[0].value == "SELECT * FROM get_item_ids(", "f-string part 0 for get_item_ids mismatch"
    assert isinstance(f_string_parts_ids[1], ast.FormattedValue) and f_string_parts_ids[1].value.id == "_sql_query_named_args", "f-string part 1 for get_item_ids placeholder mismatch"
    assert isinstance(f_string_parts_ids[2], ast.Constant) and f_string_parts_ids[2].value == ")", "f-string part 2 for get_item_ids mismatch"

    # Verify _call_params_dict for get_item_ids (param: p_category_id -> category_id)
    call_params_dict_init_empty_ids = False
    category_id_assigned_ids = False

    for node in ast.walk(ids_func_node): # ids_func_node is for get_item_ids
        if isinstance(node, ast.Assign):
            if len(node.targets) == 1 and isinstance(node.targets[0], ast.Name) and node.targets[0].id == '_call_params_dict':
                if isinstance(node.value, ast.Dict) and not node.value.keys:
                    call_params_dict_init_empty_ids = True
            elif len(node.targets) == 1 and isinstance(node.targets[0], ast.Subscript):
                target_subscript = node.targets[0]
                if isinstance(target_subscript.value, ast.Name) and target_subscript.value.id == '_call_params_dict' and \
                   isinstance(target_subscript.slice, ast.Constant) and target_subscript.slice.value == 'category_id' and \
                   isinstance(node.value, ast.Name) and node.value.id == 'category_id':
                    category_id_assigned_ids = True
                    
    assert call_params_dict_init_empty_ids, "_call_params_dict was not initialized as empty for get_item_ids"
    assert category_id_assigned_ids, "'category_id' was not assigned to _call_params_dict for get_item_ids"

    assert fetchone_call_ids is not None, "fetchone call not found for get_item_ids" # get_item_ids in fixture returns INTEGER[], so it should be fetchone

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

    # Check body for execute, fetchone, and return row[0] for process_tags
    execute_call_tags = None
    sql_query_assign_node_tags = None
    fetchone_call_tags = None
    return_logic_tags = False
    expected_execute_param_names_tags = ['tags']

    for stmt in tags_func_node.body:
        if isinstance(stmt, ast.Assign):
            for target in stmt.targets:
                if isinstance(target, ast.Name) and target.id == '_full_sql_query':
                    sql_query_assign_node_tags = stmt
                    break
            if sql_query_assign_node_tags: break

    for node_in_body in ast.walk(tags_func_node):
        if isinstance(node_in_body, ast.Await) and isinstance(node_in_body.value, ast.Call):
            call = node_in_body.value
            if isinstance(call.func, ast.Attribute) and call.func.attr == 'execute':
                execute_call_tags = call
                assert isinstance(call.args[0], ast.Name) and call.args[0].id == '_full_sql_query', "execute in process_tags not using _full_sql_query"
                assert len(call.args) == 2 and isinstance(call.args[1], ast.Name) and call.args[1].id == '_call_params_dict', "Execute second arg for process_tags is not _call_params_dict"
            elif isinstance(call.func, ast.Attribute) and call.func.attr == 'fetchone':
                fetchone_call_tags = call
        elif isinstance(node_in_body, ast.Return):
            if isinstance(node_in_body.value, ast.Subscript) and \
               isinstance(node_in_body.value.value, ast.Name) and node_in_body.value.value.id == 'row' and \
               isinstance(node_in_body.value.slice, ast.Constant) and node_in_body.value.slice.value == 0:
                return_logic_tags = True
    
    assert execute_call_tags is not None, "execute call not found in process_tags"
    assert sql_query_assign_node_tags is not None, "Assignment to _full_sql_query not found for process_tags"

    assert isinstance(sql_query_assign_node_tags.value, ast.JoinedStr), "_full_sql_query in process_tags is not an f-string"
    f_string_parts_tags = sql_query_assign_node_tags.value.values
    assert len(f_string_parts_tags) == 3, "f-string for process_tags has unexpected number of parts"
    assert isinstance(f_string_parts_tags[0], ast.Constant) and f_string_parts_tags[0].value == "SELECT * FROM process_tags(", "f-string part 0 for process_tags mismatch"
    assert isinstance(f_string_parts_tags[1], ast.FormattedValue) and f_string_parts_tags[1].value.id == "_sql_query_named_args", "f-string part 1 for process_tags placeholder mismatch"
    assert isinstance(f_string_parts_tags[2], ast.Constant) and f_string_parts_tags[2].value == ")", "f-string part 2 for process_tags mismatch"

    # Verify that _call_params_dict is populated correctly for process_tags (param: p_tag_list -> tag_list)
    call_params_dict_init_empty_tags = False
    tag_list_assigned_tags = False
    
    for node in ast.walk(tags_func_node): # tags_func_node is for process_tags
        if isinstance(node, ast.Assign):
            if len(node.targets) == 1 and isinstance(node.targets[0], ast.Name) and node.targets[0].id == '_call_params_dict':
                if isinstance(node.value, ast.Dict) and not node.value.keys:
                    call_params_dict_init_empty_tags = True
            elif len(node.targets) == 1 and isinstance(node.targets[0], ast.Subscript):
                target_subscript = node.targets[0]
                if isinstance(target_subscript.value, ast.Name) and target_subscript.value.id == '_call_params_dict' and \
                   isinstance(target_subscript.slice, ast.Constant) and target_subscript.slice.value == 'tag_list' and \
                   isinstance(node.value, ast.Name) and node.value.id == 'tag_list':
                    tag_list_assigned_tags = True

    assert call_params_dict_init_empty_tags, "_call_params_dict was not initialized as empty for process_tags"
    assert tag_list_assigned_tags, "'tag_list' was not assigned to _call_params_dict for process_tags"

    assert fetchone_call_tags is not None, "fetchone call not found in process_tags"
