import ast
import subprocess
import sys
from pathlib import Path


# Define paths relative to the main tests/ directory
TESTS_ROOT_DIR = Path(__file__).parent.parent  # Go up one level to tests/
FIXTURES_DIR = TESTS_ROOT_DIR / "fixtures"
EXPECTED_DIR = TESTS_ROOT_DIR / "expected"
PROJECT_ROOT = TESTS_ROOT_DIR.parent  # Go up one level from tests/ to project root


def run_cli_tool(
    functions_sql: Path,
    output_py: Path,
    schema_sql: Path | None = None,
    verbose: bool = False,
    allow_missing_schemas: bool = False,
):
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
        cmd.append("-v")  # Add verbose flag if requested
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
        if isinstance(node, ast.AsyncFunctionDef) and node.name == "do_something":
            func_node = node
            break

    assert func_node is not None, "Async function definition 'do_something' not found"

    # Check parameters
    expected_params = {"conn": "AsyncConnection", "item_id": "int"}
    actual_params = {arg.arg: ast.unparse(arg.annotation) for arg in func_node.args.args}
    assert actual_params == expected_params, (
        f"Mismatch in do_something parameters. Expected {expected_params}, Got {actual_params}"
    )

    # Check return annotation (should be None)
    assert (
        func_node.returns is not None
        and isinstance(func_node.returns, ast.Constant)
        and func_node.returns.value is None
    ), (
        f"Return annotation should be 'None', but got: {ast.unparse(func_node.returns) if func_node.returns else 'None (implicit)'}"
    )

    # Check docstring
    docstring = ast.get_docstring(func_node)
    assert docstring is not None, "do_something is missing a docstring"
    assert docstring == "A function that does something but returns nothing", (
        "Docstring content mismatch for do_something"
    )

    # Check function body for _full_sql_query assignment and execute call
    execute_call = None
    sql_query_assign_node = None

    for stmt in func_node.body:
        if isinstance(stmt, ast.Assign):
            for target in stmt.targets:
                if isinstance(target, ast.Name) and target.id == "_full_sql_query":
                    sql_query_assign_node = stmt
                    break
            if sql_query_assign_node:  # Found assignment, no need to check further top-level statements for this
                break

    for node_in_body in ast.walk(func_node):
        if isinstance(node_in_body, ast.Await) and isinstance(node_in_body.value, ast.Call):
            call = node_in_body.value
            if isinstance(call.func, ast.Attribute) and call.func.attr == "execute":
                execute_call = call
                assert isinstance(call.args[0], ast.Name) and call.args[0].id == "_full_sql_query", (
                    "execute call in 'do_something' does not use _full_sql_query variable"
                )
                break  # Assume only one execute call

    assert execute_call is not None, "'cur.execute' call not found in do_something"
    assert sql_query_assign_node is not None, "Assignment to _full_sql_query not found in do_something"

    assert isinstance(sql_query_assign_node.value, ast.JoinedStr), "_full_sql_query in do_something is not an f-string"
    f_string_parts = sql_query_assign_node.value.values
    assert len(f_string_parts) == 3, "f-string for _full_sql_query in do_something has unexpected number of parts"
    assert isinstance(f_string_parts[0], ast.Constant) and f_string_parts[0].value == "SELECT * FROM do_something(", (
        "f-string part 0 for do_something mismatch"
    )
    assert (
        isinstance(f_string_parts[1], ast.FormattedValue)
        and isinstance(f_string_parts[1].value, ast.Name)
        and f_string_parts[1].value.id == "_sql_query_named_args"
    ), "f-string part 1 for do_something (placeholder var) mismatch"
    assert isinstance(f_string_parts[2], ast.Constant) and f_string_parts[2].value == ")", (
        "f-string part 2 for do_something mismatch"
    )

    # Check execute parameters: second arg to execute is _call_params_dict variable
    assert len(execute_call.args) == 2, "execute call should have 2 arguments for do_something"
    assert isinstance(execute_call.args[1], ast.Name) and execute_call.args[1].id == "_call_params_dict", (
        "Second arg to execute in do_something should be _call_params_dict variable"
    )

    # Verify that _call_params_dict is populated correctly for 'item_id'
    item_id_assigned_to_call_params_dict = False
    for node in ast.walk(func_node):
        if (
            isinstance(node, ast.Assign)
            and len(node.targets) == 1
            and isinstance(node.targets[0], ast.Subscript)
            and isinstance(node.targets[0].value, ast.Name)
            and node.targets[0].value.id == "_call_params_dict"
            and isinstance(node.targets[0].slice, ast.Constant)
            and node.targets[0].slice.value == "item_id"
            and isinstance(node.value, ast.Name)
            and node.value.id == "item_id"
        ):
            item_id_assigned_to_call_params_dict = True
            break
    assert item_id_assigned_to_call_params_dict, (
        "'item_id' was not correctly assigned to _call_params_dict for do_something"
    )

    # Check explicit return None
    return_node = None
    # Search the whole function body, not just top-level statements
    for node in ast.walk(func_node):
        if isinstance(node, ast.Return):
            # Ensure the return is directly within the target function,
            # not a nested function (if any were possible)
            # This check might be overly cautious but safe.
            # Find the parent FunctionDef/AsyncFunctionDef
            parent_func = next(
                (
                    p
                    for p in ast.walk(func_node)
                    if isinstance(p, ast.FunctionDef | ast.AsyncFunctionDef) and node in ast.walk(p)
                ),
                None,
            )
            if parent_func == func_node:
                return_node = node
                break  # Found the return statement for our function

    assert return_node is not None, "No return statement found within the function body"
    assert isinstance(return_node.value, ast.Constant) and return_node.value.value is None, (
        "Function does not explicitly return None"
    )


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
        if isinstance(node, ast.AsyncFunctionDef) and node.name == "get_item_count":
            count_func_node = node
            break
    assert count_func_node is not None, "Async function 'get_item_count' not found"

    # Check parameters
    expected_params_count = {"conn": "AsyncConnection"}
    actual_params_count = {arg.arg: ast.unparse(arg.annotation) for arg in count_func_node.args.args}
    assert actual_params_count == expected_params_count, (
        f"Mismatch in get_item_count parameters. Expected {expected_params_count}, Got {actual_params_count}"
    )

    # Check return annotation
    expected_return_count = "Optional[int]"
    actual_return_count = ast.unparse(count_func_node.returns)
    assert actual_return_count == expected_return_count, (
        f"Mismatch in get_item_count return type. Expected {expected_return_count}, Got {actual_return_count}"
    )

    # Check docstring
    docstring_count = ast.get_docstring(count_func_node)
    assert docstring_count == "Returns a simple count", "Docstring content mismatch for get_item_count"

    # Check body for execute and fetchone for get_item_count
    execute_call_count = None
    sql_query_assign_node_count = None
    fetchone_call_count = None
    return_logic_count = False

    for stmt in count_func_node.body:
        if isinstance(stmt, ast.Assign):
            for target in stmt.targets:
                if isinstance(target, ast.Name) and target.id == "_full_sql_query":
                    sql_query_assign_node_count = stmt
                    break
            if sql_query_assign_node_count:
                break

    for node_in_body in ast.walk(count_func_node):
        if isinstance(node_in_body, ast.Await) and isinstance(node_in_body.value, ast.Call):
            call = node_in_body.value
            if isinstance(call.func, ast.Attribute) and call.func.attr == "execute":
                execute_call_count = call
                assert isinstance(call.args[0], ast.Name) and call.args[0].id == "_full_sql_query", (
                    "execute in get_item_count not using _full_sql_query"
                )
            elif isinstance(call.func, ast.Attribute) and call.func.attr == "fetchone":
                fetchone_call_count = call
        elif isinstance(node_in_body, ast.Return):
            if (
                isinstance(node_in_body.value, ast.Subscript)
                and isinstance(node_in_body.value.value, ast.Name)
                and node_in_body.value.value.id == "row"
                and isinstance(node_in_body.value.slice, ast.Constant)
                and node_in_body.value.slice.value == 0
            ):
                return_logic_count = True

    assert execute_call_count is not None, "execute call not found in get_item_count"
    assert sql_query_assign_node_count is not None, "Assignment to _full_sql_query not found in get_item_count"

    assert isinstance(sql_query_assign_node_count.value, ast.JoinedStr), (
        "_full_sql_query in get_item_count is not an f-string"
    )
    f_string_parts_count = sql_query_assign_node_count.value.values
    assert len(f_string_parts_count) == 3, "f-string for get_item_count has unexpected number of parts"
    assert (
        isinstance(f_string_parts_count[0], ast.Constant)
        and f_string_parts_count[0].value == "SELECT * FROM get_item_count("
    ), "f-string part 0 for get_item_count mismatch"
    assert (
        isinstance(f_string_parts_count[1], ast.FormattedValue)
        and isinstance(f_string_parts_count[1].value, ast.Name)
        and f_string_parts_count[1].value.id == "_sql_query_named_args"
    ), "f-string part 1 for get_item_count placeholder mismatch"
    assert isinstance(f_string_parts_count[2], ast.Constant) and f_string_parts_count[2].value == ")", (
        "f-string part 2 for get_item_count mismatch"
    )

    # Check execute parameters: second arg is _call_params_dict
    assert len(execute_call_count.args) == 2, "execute call should have 2 arguments for get_item_count"
    assert isinstance(execute_call_count.args[1], ast.Name) and execute_call_count.args[1].id == "_call_params_dict", (
        "Second arg to execute in get_item_count should be _call_params_dict variable"
    )

    # Verify that _call_params_dict is not populated for get_item_count
    call_params_dict_populated = False
    for node in ast.walk(count_func_node):
        if (
            isinstance(node, ast.Assign)
            and len(node.targets) == 1
            and isinstance(node.targets[0], ast.Subscript)
            and isinstance(node.targets[0].value, ast.Name)
            and node.targets[0].value.id == "_call_params_dict"
        ):
            call_params_dict_populated = True  # Found an assignment to _call_params_dict
            break
    assert not call_params_dict_populated, (
        "_call_params_dict should be empty for get_item_count (a no-parameter function)"
    )

    assert fetchone_call_count is not None, "fetchone call not found in get_item_count"
    assert return_logic_count, "'return row[0]' logic not found in get_item_count"

    # --- Check get_item_name function ---
    name_func_node = None
    for node in ast.walk(tree):
        if isinstance(node, ast.AsyncFunctionDef) and node.name == "get_item_name":
            name_func_node = node
            break
    assert name_func_node is not None, "Async function 'get_item_name' not found"

    # Check parameters
    expected_params_name = {"conn": "AsyncConnection", "id": "int"}
    actual_params_name = {arg.arg: ast.unparse(arg.annotation) for arg in name_func_node.args.args}
    assert actual_params_name == expected_params_name, (
        f"Mismatch in get_item_name parameters. Expected {expected_params_name}, Got {actual_params_name}"
    )

    # Check return annotation
    expected_return_name = "Optional[str]"
    actual_return_name = ast.unparse(name_func_node.returns)
    assert actual_return_name == expected_return_name, (
        f"Mismatch in get_item_name return type. Expected {expected_return_name}, Got {actual_return_name}"
    )

    # Check docstring
    docstring_name = ast.get_docstring(name_func_node)
    assert docstring_name == "Returns text, potentially null", "Docstring content mismatch for get_item_name"

    # Check body for execute and fetchone for get_item_name
    execute_call_name = None
    sql_query_assign_node_name = None
    fetchone_call_name = None
    return_logic_name = False

    for stmt in name_func_node.body:
        if isinstance(stmt, ast.Assign):
            for target in stmt.targets:
                if isinstance(target, ast.Name) and target.id == "_full_sql_query":
                    sql_query_assign_node_name = stmt
                    break
            if sql_query_assign_node_name:
                break

    for node_in_body in ast.walk(name_func_node):
        if isinstance(node_in_body, ast.Await) and isinstance(node_in_body.value, ast.Call):
            call = node_in_body.value
            if isinstance(call.func, ast.Attribute) and call.func.attr == "execute":
                execute_call_name = call
                assert isinstance(call.args[0], ast.Name) and call.args[0].id == "_full_sql_query", (
                    "execute in get_item_name not using _full_sql_query"
                )
                if len(call.args) > 1 and isinstance(call.args[1], ast.List):
                    [elt.id for elt in call.args[1].elts if isinstance(elt, ast.Name)]
            elif isinstance(call.func, ast.Attribute) and call.func.attr == "fetchone":
                fetchone_call_name = call
        elif isinstance(node_in_body, ast.Return):
            if (
                isinstance(node_in_body.value, ast.Subscript)
                and isinstance(node_in_body.value.value, ast.Name)
                and node_in_body.value.value.id == "row"
                and isinstance(node_in_body.value.slice, ast.Constant)
                and node_in_body.value.slice.value == 0
            ):
                return_logic_name = True

    assert execute_call_name is not None, "execute call not found in get_item_name"
    assert sql_query_assign_node_name is not None, "Assignment to _full_sql_query not found in get_item_name"

    assert isinstance(sql_query_assign_node_name.value, ast.JoinedStr), (
        "_full_sql_query in get_item_name is not an f-string"
    )
    f_string_parts_name = sql_query_assign_node_name.value.values
    assert len(f_string_parts_name) == 3, "f-string for get_item_name has unexpected number of parts"
    assert (
        isinstance(f_string_parts_name[0], ast.Constant)
        and f_string_parts_name[0].value == "SELECT * FROM get_item_name("
    ), "f-string part 0 for get_item_name mismatch"
    assert (
        isinstance(f_string_parts_name[1], ast.FormattedValue)
        and isinstance(f_string_parts_name[1].value, ast.Name)
        and f_string_parts_name[1].value.id == "_sql_query_named_args"
    ), "f-string part 1 for get_item_name placeholder mismatch"
    assert isinstance(f_string_parts_name[2], ast.Constant) and f_string_parts_name[2].value == ")", (
        "f-string part 2 for get_item_name mismatch"
    )

    # Check execute parameters
    assert len(execute_call_name.args) == 2, "execute call for get_item_name should have 2 arguments"
    assert isinstance(execute_call_name.args[1], ast.Name) and execute_call_name.args[1].id == "_call_params_dict", (
        "Second arg to execute in get_item_name should be _call_params_dict variable"
    )

    # Verify _call_params_dict for get_item_name
    item_id_assigned_to_dict_for_name = False
    for node in ast.walk(name_func_node):
        if (
            isinstance(node, ast.Assign)
            and len(node.targets) == 1
            and isinstance(node.targets[0], ast.Subscript)
            and isinstance(node.targets[0].value, ast.Name)
            and node.targets[0].value.id == "_call_params_dict"
            and isinstance(node.targets[0].slice, ast.Constant)
            and node.targets[0].slice.value == "id"
            and isinstance(node.value, ast.Name)
            and node.value.id == "id"
        ):  # Python var is 'id' for SQL param 'p_id' -> python_name: id
            item_id_assigned_to_dict_for_name = True
            break
    assert item_id_assigned_to_dict_for_name, "'id' was not correctly assigned to _call_params_dict for get_item_name"

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
        if isinstance(node, ast.AsyncFunctionDef) and node.name == "get_item_ids_by_category":
            func_node = node
            break
    assert func_node is not None, "Async function 'get_item_ids_by_category' not found"

    # Check parameters
    expected_params = {"conn": "AsyncConnection", "category_name": "str"}
    actual_params = {arg.arg: ast.unparse(arg.annotation) for arg in func_node.args.args}
    assert actual_params == expected_params, f"Mismatch in parameters. Expected {expected_params}, Got {actual_params}"

    # Check return annotation
    expected_return = "List[int]"
    actual_return = ast.unparse(func_node.returns)
    assert actual_return == expected_return, f"Mismatch in return type. Expected {expected_return}, Got {actual_return}"

    # Check docstring
    docstring = ast.get_docstring(func_node)
    assert docstring == "Returns a list of item IDs for a given category", "Docstring content mismatch"

    # Check body for execute, fetchall, and list comprehension
    execute_call = None
    sql_query_assign_node = None
    fetchall_call = None
    list_comp = None

    for stmt in func_node.body:
        if isinstance(stmt, ast.Assign):
            for target in stmt.targets:
                if isinstance(target, ast.Name) and target.id == "_full_sql_query":
                    sql_query_assign_node = stmt
                    break
            if sql_query_assign_node:
                break

    for node_in_body in ast.walk(func_node):
        if isinstance(node_in_body, ast.Await) and isinstance(node_in_body.value, ast.Call):
            call = node_in_body.value
            if isinstance(call.func, ast.Attribute) and call.func.attr == "execute":
                execute_call = call
                assert isinstance(call.args[0], ast.Name) and call.args[0].id == "_full_sql_query", (
                    "execute in get_item_ids_by_category not using _full_sql_query"
                )
                if len(call.args) > 1 and isinstance(call.args[1], ast.List):
                    [elt.id for elt in call.args[1].elts if isinstance(elt, ast.Name)]
            elif isinstance(call.func, ast.Attribute) and call.func.attr == "fetchall":
                fetchall_call = call
        elif isinstance(node_in_body, ast.Return):
            if isinstance(node_in_body.value, ast.ListComp):
                list_comp = node_in_body.value
                # Check element is row[0]
                is_elt_row_zero = (
                    isinstance(list_comp.elt, ast.Subscript)
                    and isinstance(list_comp.elt.value, ast.Name)
                    and list_comp.elt.value.id == "row"
                    and isinstance(list_comp.elt.slice, ast.Constant)
                    and list_comp.elt.slice.value == 0
                )
                # Check generator target is row
                comp = list_comp.generators[0]
                is_target_row = isinstance(comp.target, ast.Name) and comp.target.id == "row"
                # Check iterable is rows
                is_iter_rows = isinstance(comp.iter, ast.Name) and comp.iter.id == "rows"
                # Check if condition is 'if row'
                has_if_row = len(comp.ifs) == 1 and isinstance(comp.ifs[0], ast.Name) and comp.ifs[0].id == "row"

                if not (is_elt_row_zero and is_target_row and is_iter_rows and has_if_row):
                    list_comp = None  # Mark as not found if structure is wrong

    assert execute_call is not None, "execute call not found in get_item_ids_by_category"
    assert sql_query_assign_node is not None, "Assignment to _full_sql_query not found in get_item_ids_by_category"

    assert isinstance(sql_query_assign_node.value, ast.JoinedStr), (
        "_full_sql_query in get_item_ids_by_category is not an f-string"
    )
    f_string_parts_ids = sql_query_assign_node.value.values
    assert len(f_string_parts_ids) == 3, "f-string for get_item_ids_by_category has unexpected number of parts"
    assert (
        isinstance(f_string_parts_ids[0], ast.Constant)
        and f_string_parts_ids[0].value == "SELECT * FROM get_item_ids_by_category("
    ), "f-string part 0 for get_item_ids_by_category mismatch"
    assert (
        isinstance(f_string_parts_ids[1], ast.FormattedValue)
        and isinstance(f_string_parts_ids[1].value, ast.Name)
        and f_string_parts_ids[1].value.id == "_sql_query_named_args"
    ), "f-string part 1 for get_item_ids_by_category placeholder mismatch"
    assert isinstance(f_string_parts_ids[2], ast.Constant) and f_string_parts_ids[2].value == ")", (
        "f-string part 2 for get_item_ids_by_category mismatch"
    )

    # Check execute parameters for get_item_ids_by_category
    assert len(execute_call.args) == 2, "execute call should have 2 arguments for get_item_ids_by_category"
    assert isinstance(execute_call.args[1], ast.Name) and execute_call.args[1].id == "_call_params_dict", (
        "Second arg to execute in get_item_ids_by_category should be _call_params_dict variable"
    )

    # Verify that _call_params_dict is populated correctly for 'category_name'
    category_name_assigned_to_call_params_dict = False
    for node in ast.walk(func_node):  # func_node is for get_item_ids_by_category here
        if (
            isinstance(node, ast.Assign)
            and len(node.targets) == 1
            and isinstance(node.targets[0], ast.Subscript)
            and isinstance(node.targets[0].value, ast.Name)
            and node.targets[0].value.id == "_call_params_dict"
            and isinstance(node.targets[0].slice, ast.Constant)
            and node.targets[0].slice.value == "category_name"
            and isinstance(node.value, ast.Name)
            and node.value.id == "category_name"
        ):
            category_name_assigned_to_call_params_dict = True
            break
    assert category_name_assigned_to_call_params_dict, (
        "'category_name' was not correctly assigned to _call_params_dict for get_item_ids_by_category"
    )

    assert fetchall_call is not None, "fetchall call not found in get_item_ids_by_category"
    assert list_comp is not None, (
        "List comprehension '[row[0] for row in rows if row]' not found or has wrong structure"
    )


def test_setof_async_table_function_generation(tmp_path):
    """Test SETOF with table names starting with 'as' and 'lang' (regression test for word boundary bug)."""
    functions_sql_path = FIXTURES_DIR / "setof_async_table_function.sql"
    actual_output_path = tmp_path / "setof_async_table_function_api.py"

    # Run the generator tool
    result = run_cli_tool(functions_sql_path, actual_output_path)

    # Ensure the CLI tool succeeded
    assert result.returncode == 0, f"CLI tool failed with stderr: {result.stderr}"

    # Check that the file was generated
    assert actual_output_path.is_file(), "Generated file was not created."
    actual_content = actual_output_path.read_text()

    # Parse the generated code
    tree = ast.parse(actual_content)

    # Check that both dataclasses were generated
    async_process_class = None
    language_setting_class = None
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef):
            if node.name == "AsyncProcess":
                async_process_class = node
            elif node.name == "LanguageSetting":
                language_setting_class = node

    assert async_process_class is not None, "AsyncProcess dataclass not found"
    assert language_setting_class is not None, "LanguageSetting dataclass not found"

    # Check both functions have correct signatures
    async_func_node = None
    lang_func_node = None
    for node in ast.walk(tree):
        if isinstance(node, ast.AsyncFunctionDef):
            if node.name == "list_async_processes":
                async_func_node = node
            elif node.name == "get_language_settings":
                lang_func_node = node

    assert async_func_node is not None, "Async function 'list_async_processes' not found"
    assert lang_func_node is not None, "Async function 'get_language_settings' not found"

    # Check return annotations are correct, not Optional[Any]
    async_return = ast.unparse(async_func_node.returns)
    lang_return = ast.unparse(lang_func_node.returns)
    assert async_return == "List[AsyncProcess]", f"Expected List[AsyncProcess], got {async_return}"
    assert lang_return == "List[LanguageSetting]", f"Expected List[LanguageSetting], got {lang_return}"

    # Check that both functions use fetchall(), not fetchone()
    for func_node, func_name in [(async_func_node, "list_async_processes"), (lang_func_node, "get_language_settings")]:
        fetchall_found = False
        fetchone_found = False
        for node in ast.walk(func_node):
            if isinstance(node, ast.Attribute) and node.attr == "fetchall":
                fetchall_found = True
            elif isinstance(node, ast.Attribute) and node.attr == "fetchone":
                fetchone_found = True

        assert fetchall_found, f"Function {func_name} should use fetchall() for SETOF return type"
        assert not fetchone_found, f"Function {func_name} should not use fetchone() for SETOF return type"

    # Check that the content includes the proper enum and dataclasses
    assert "AsyncStatus" in actual_content, "AsyncStatus enum should be generated"
    assert "class AsyncProcess:" in actual_content, "AsyncProcess dataclass should be generated"
    assert "class LanguageSetting:" in actual_content, "LanguageSetting dataclass should be generated"
    assert "List[AsyncProcess]" in actual_content, "Return type should be List[AsyncProcess]"
    assert "List[LanguageSetting]" in actual_content, "Return type should be List[LanguageSetting]"


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
            if node.module == "typing":
                for alias in node.names:
                    found_typing_imports.add(alias.name)
            elif node.module == "dataclasses" and any(alias.name == "dataclass" for alias in node.names):
                found_dataclass = True
            elif node.module == "uuid" and any(alias.name == "UUID" for alias in node.names):
                found_uuid = True
    assert {"List", "Optional"}.issubset(found_typing_imports), "Missing required typing imports"
    assert found_dataclass, "Missing dataclass import"
    assert found_uuid, "Missing UUID import"

    # 2. Check GetUserBasicInfoResult Dataclass
    dataclass_node = None
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef) and node.name == "GetUserBasicInfoResult":
            dataclass_node = node
            break
    assert dataclass_node is not None, "Dataclass 'GetUserBasicInfoResult' not found"
    assert any(isinstance(d, ast.Name) and d.id == "dataclass" for d in dataclass_node.decorator_list), (
        "Class is not decorated with @dataclass"
    )

    expected_fields = {"user_id": "Optional[UUID]", "first_name": "Optional[str]", "is_active": "Optional[bool]"}
    actual_fields = {
        stmt.target.id: ast.unparse(stmt.annotation) for stmt in dataclass_node.body if isinstance(stmt, ast.AnnAssign)
    }
    assert actual_fields == expected_fields, "GetUserBasicInfoResult dataclass fields mismatch"

    # 3. Check get_user_basic_info Function
    func_node = None
    for node in ast.walk(tree):
        if isinstance(node, ast.AsyncFunctionDef) and node.name == "get_user_basic_info":
            func_node = node
            break
    assert func_node is not None, "Async function 'get_user_basic_info' not found"

    # Check parameters
    expected_params = {"conn": "AsyncConnection", "user_id": "UUID"}
    actual_params = {arg.arg: ast.unparse(arg.annotation) for arg in func_node.args.args}
    assert actual_params == expected_params, f"Parameter mismatch. Expected {expected_params}, Got {actual_params}"

    # Check return annotation
    expected_return = "List[GetUserBasicInfoResult]"
    actual_return = ast.unparse(func_node.returns)
    assert actual_return == expected_return, f"Return type mismatch. Expected {expected_return}, Got {actual_return}"

    # Check docstring
    docstring = ast.get_docstring(func_node)
    assert docstring == "Returns a user's basic info as a table", "Docstring content mismatch"

    # Check body for execute, fetchall, and list comprehension with try/except
    execute_call = None
    sql_query_assign_node = None
    fetchall_call = None
    list_comp_call = None
    try_except_node = None

    for stmt in func_node.body:
        if isinstance(stmt, ast.Assign):
            for target in stmt.targets:
                if isinstance(target, ast.Name) and target.id == "_full_sql_query":
                    sql_query_assign_node = stmt
                    break
            if sql_query_assign_node:
                break

    for node_in_body in ast.walk(func_node):
        if isinstance(node_in_body, ast.Await) and isinstance(node_in_body.value, ast.Call):
            call = node_in_body.value
            if isinstance(call.func, ast.Attribute) and call.func.attr == "execute":
                execute_call = call
                assert isinstance(call.args[0], ast.Name) and call.args[0].id == "_full_sql_query", (
                    "execute in get_user_basic_info not using _full_sql_query"
                )
                if len(call.args) > 1 and isinstance(call.args[1], ast.List):
                    [elt.id for elt in call.args[1].elts if isinstance(elt, ast.Name)]
            elif isinstance(call.func, ast.Attribute) and call.func.attr == "fetchall":
                fetchall_call = call
        elif isinstance(node_in_body, ast.Try):
            try_except_node = node_in_body
            # Find the list comprehension inside the try block
            for try_node_wewnętrzny in ast.walk(try_except_node):
                if isinstance(try_node_wewnętrzny, ast.ListComp):
                    # Check the element: GetUserBasicInfoResult(*r)
                    if (
                        isinstance(try_node_wewnętrzny.elt, ast.Call)
                        and isinstance(try_node_wewnętrzny.elt.func, ast.Name)
                        and try_node_wewnętrzny.elt.func.id == "GetUserBasicInfoResult"
                        and len(try_node_wewnętrzny.elt.args) == 1
                        and isinstance(try_node_wewnętrzny.elt.args[0], ast.Starred)
                        and isinstance(try_node_wewnętrzny.elt.args[0].value, ast.Name)
                        and try_node_wewnętrzny.elt.args[0].value.id == "r"
                    ):
                        # Check the generator: for r in rows
                        comp = try_node_wewnętrzny.generators[0]
                        if (
                            isinstance(comp.target, ast.Name)
                            and comp.target.id == "r"
                            and isinstance(comp.iter, ast.Name)
                            and comp.iter.id == "rows"
                        ):
                            list_comp_call = try_node_wewnętrzny.elt  # Store the call node if structure matches
                            break  # Found it
            # Check the except handler catches TypeError
            assert len(try_except_node.handlers) == 1, "Expected one except handler"
            assert isinstance(try_except_node.handlers[0], ast.ExceptHandler), "Handler is not ExceptHandler type"
            assert (
                isinstance(try_except_node.handlers[0].type, ast.Name)
                and try_except_node.handlers[0].type.id == "TypeError"
            ), "Handler does not catch TypeError"
            # Check for a raise statement in the except block
            raises_type_error = any(isinstance(item, ast.Raise) for item in try_except_node.handlers[0].body)
            assert raises_type_error, "Except handler should raise a TypeError"

    assert execute_call is not None, "execute call not found in get_user_basic_info"
    assert sql_query_assign_node is not None, "Assignment to _full_sql_query not found in get_user_basic_info"

    assert isinstance(sql_query_assign_node.value, ast.JoinedStr), (
        "_full_sql_query in get_user_basic_info is not an f-string"
    )
    f_string_parts_basic = sql_query_assign_node.value.values
    assert len(f_string_parts_basic) == 3, "f-string for get_user_basic_info has unexpected number of parts"
    assert (
        isinstance(f_string_parts_basic[0], ast.Constant)
        and f_string_parts_basic[0].value == "SELECT * FROM get_user_basic_info("
    ), "f-string part 0 for get_user_basic_info mismatch"
    assert (
        isinstance(f_string_parts_basic[1], ast.FormattedValue)
        and isinstance(f_string_parts_basic[1].value, ast.Name)
        and f_string_parts_basic[1].value.id == "_sql_query_named_args"
    ), "f-string part 1 for get_user_basic_info placeholder mismatch"
    assert isinstance(f_string_parts_basic[2], ast.Constant) and f_string_parts_basic[2].value == ")", (
        "f-string part 2 for get_user_basic_info mismatch"
    )

    # Check execute parameters for get_user_basic_info
    assert len(execute_call.args) == 2, "execute call should have 2 arguments for get_user_basic_info"
    assert isinstance(execute_call.args[1], ast.Name) and execute_call.args[1].id == "_call_params_dict", (
        "Second arg to execute in get_user_basic_info should be _call_params_dict variable"
    )

    # Verify _call_params_dict for get_user_basic_info
    user_id_assigned_to_call_params_dict_basic = False
    for node in ast.walk(func_node):  # func_node is for get_user_basic_info here
        if (
            isinstance(node, ast.Assign)
            and len(node.targets) == 1
            and isinstance(node.targets[0], ast.Subscript)
            and isinstance(node.targets[0].value, ast.Name)
            and node.targets[0].value.id == "_call_params_dict"
            and isinstance(node.targets[0].slice, ast.Constant)
            and node.targets[0].slice.value == "user_id"
            and isinstance(node.value, ast.Name)
            and node.value.id == "user_id"
        ):
            user_id_assigned_to_call_params_dict_basic = True
            break
    assert user_id_assigned_to_call_params_dict_basic, (
        "'user_id' was not correctly assigned to _call_params_dict for get_user_basic_info"
    )

    assert fetchall_call is not None, "fetchall call not found in get_user_basic_info"
    assert try_except_node is not None, "Try/except block not found in get_user_basic_info"
    assert list_comp_call is not None, (
        "List comprehension '[GetUserBasicInfoResult(*r) for r in rows]' not found or has wrong structure inside try block"
    )


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
        if isinstance(node, ast.ImportFrom) and node.module == "typing":
            for alias in node.names:
                found_typing_imports.add(alias.name)
    assert {"List", "Optional", "Tuple"}.issubset(found_typing_imports), "Missing required typing imports"

    # 2. Check get_processing_status Function (returns RECORD)
    status_func_node = None
    for node in ast.walk(tree):
        if isinstance(node, ast.AsyncFunctionDef) and node.name == "get_processing_status":
            status_func_node = node
            break
    assert status_func_node is not None, "Async function 'get_processing_status' not found"

    # Check parameters
    expected_params_status = {"conn": "AsyncConnection"}
    actual_params_status = {arg.arg: ast.unparse(arg.annotation) for arg in status_func_node.args.args}
    assert actual_params_status == expected_params_status, "Mismatch in get_processing_status parameters"

    # Check return annotation
    expected_return_status = "Optional[Tuple]"
    actual_return_status = ast.unparse(status_func_node.returns)
    assert actual_return_status == expected_return_status, "Mismatch in get_processing_status return type"

    # Check docstring
    docstring_status = ast.get_docstring(status_func_node)
    assert docstring_status == "Returns an anonymous record containing status and count", (
        "Docstring mismatch for get_processing_status"
    )

    # Check body for execute, fetchone, and return row
    execute_call_status = None
    sql_query_assign_node_status = None
    fetchone_call_status = None
    return_row_status = False

    for stmt in status_func_node.body:
        if isinstance(stmt, ast.Assign):
            for target in stmt.targets:
                if isinstance(target, ast.Name) and target.id == "_full_sql_query":
                    sql_query_assign_node_status = stmt
                    break
            if sql_query_assign_node_status:
                break

    for node_in_body in ast.walk(status_func_node):
        if isinstance(node_in_body, ast.Await) and isinstance(node_in_body.value, ast.Call):
            call = node_in_body.value
            if isinstance(call.func, ast.Attribute) and call.func.attr == "execute":
                execute_call_status = call
                assert isinstance(call.args[0], ast.Name) and call.args[0].id == "_full_sql_query", (
                    "execute in get_processing_status not using _full_sql_query"
                )
            elif isinstance(call.func, ast.Attribute) and call.func.attr == "fetchone":
                fetchone_call_status = call
        elif isinstance(node_in_body, ast.Return):
            if isinstance(node_in_body.value, ast.Name) and node_in_body.value.id == "row":
                return_row_status = True

    assert execute_call_status is not None, "execute call not found in get_processing_status"
    assert sql_query_assign_node_status is not None, "Assignment to _full_sql_query not found in get_processing_status"

    assert isinstance(sql_query_assign_node_status.value, ast.JoinedStr), (
        "_full_sql_query in get_processing_status is not an f-string"
    )
    f_string_parts_status = sql_query_assign_node_status.value.values
    assert len(f_string_parts_status) == 3, "f-string for get_processing_status has unexpected number of parts"
    assert (
        isinstance(f_string_parts_status[0], ast.Constant)
        and f_string_parts_status[0].value == "SELECT * FROM get_processing_status("
    ), "f-string part 0 for get_processing_status mismatch"
    assert (
        isinstance(f_string_parts_status[1], ast.FormattedValue)
        and isinstance(f_string_parts_status[1].value, ast.Name)
        and f_string_parts_status[1].value.id == "_sql_query_named_args"
    ), "f-string part 1 for get_processing_status placeholder mismatch"
    assert isinstance(f_string_parts_status[2], ast.Constant) and f_string_parts_status[2].value == ")", (
        "f-string part 2 for get_processing_status mismatch"
    )

    # Check execute parameters for get_processing_status (no params)
    assert len(execute_call_status.args) == 2, "execute call should have 2 arguments for get_processing_status"
    assert (
        isinstance(execute_call_status.args[1], ast.Name) and execute_call_status.args[1].id == "_call_params_dict"
    ), "Second arg to execute in get_processing_status should be _call_params_dict variable"

    # Verify _call_params_dict for get_processing_status (has param job_id)
    job_id_assigned_to_dict_for_record = False
    for node in ast.walk(status_func_node):
        if (
            isinstance(node, ast.Assign)
            and len(node.targets) == 1
            and isinstance(node.targets[0], ast.Subscript)
            and isinstance(node.targets[0].value, ast.Name)
            and node.targets[0].value.id == "_call_params_dict"
            and isinstance(node.targets[0].slice, ast.Constant)
            and node.targets[0].slice.value == "job_id"
            and isinstance(node.value, ast.Name)
            and node.value.id == "job_id"
        ):
            job_id_assigned_to_dict_for_record = True
            break
    assert not job_id_assigned_to_dict_for_record, (
        "'job_id' should NOT be assigned to _call_params_dict for get_processing_status as it is not a parameter"
    )

    assert fetchone_call_status is not None, "fetchone call not found in get_processing_status"
    assert return_row_status, "'return row' logic not found in get_processing_status"

    # 3. Check get_all_statuses Function (returns SETOF RECORD)
    all_status_func_node = None
    for node in ast.walk(tree):
        if isinstance(node, ast.AsyncFunctionDef) and node.name == "get_all_statuses":
            all_status_func_node = node
            break
    assert all_status_func_node is not None, "Async function 'get_all_statuses' not found"

    # Check parameters
    expected_params_all = {"conn": "AsyncConnection"}
    actual_params_all = {arg.arg: ast.unparse(arg.annotation) for arg in all_status_func_node.args.args}
    assert actual_params_all == expected_params_all, "Mismatch in get_all_statuses parameters"

    # Check return annotation
    expected_return_all = "List[Tuple]"
    actual_return_all = ast.unparse(all_status_func_node.returns)
    assert actual_return_all == expected_return_all, "Mismatch in get_all_statuses return type"

    # Check docstring
    docstring_all = ast.get_docstring(all_status_func_node)
    assert docstring_all == "Returns a setof anonymous records", "Docstring mismatch for get_all_statuses"

    # Check body for execute, fetchall, and return rows
    execute_call_all = None
    sql_query_assign_node_all = None
    fetchall_call_all = None
    return_rows_all = False

    for stmt in all_status_func_node.body:
        if isinstance(stmt, ast.Assign):
            for target in stmt.targets:
                if isinstance(target, ast.Name) and target.id == "_full_sql_query":
                    sql_query_assign_node_all = stmt
                    break
            if sql_query_assign_node_all:
                break

    for node_in_body in ast.walk(all_status_func_node):
        if isinstance(node_in_body, ast.Await) and isinstance(node_in_body.value, ast.Call):
            call = node_in_body.value
            if isinstance(call.func, ast.Attribute) and call.func.attr == "execute":
                execute_call_all = call
                assert isinstance(call.args[0], ast.Name) and call.args[0].id == "_full_sql_query", (
                    "execute in get_all_statuses not using _full_sql_query"
                )
                if len(call.args) > 1 and isinstance(call.args[1], ast.List):  # Check params list
                    [elt.id for elt in call.args[1].elts if isinstance(elt, ast.Name)]
            elif isinstance(call.func, ast.Attribute) and call.func.attr == "fetchall":
                fetchall_call_all = call
        elif isinstance(node_in_body, ast.Return):
            if isinstance(node_in_body.value, ast.Name) and node_in_body.value.id == "rows":
                return_rows_all = True

    assert execute_call_all is not None, "execute call not found in get_all_statuses"
    assert sql_query_assign_node_all is not None, "Assignment to _full_sql_query not found in get_all_statuses"

    assert isinstance(sql_query_assign_node_all.value, ast.JoinedStr), (
        "_full_sql_query in get_all_statuses is not an f-string"
    )
    f_string_parts_all = sql_query_assign_node_all.value.values
    assert len(f_string_parts_all) == 3, "f-string for get_all_statuses has unexpected number of parts"
    assert (
        isinstance(f_string_parts_all[0], ast.Constant)
        and f_string_parts_all[0].value == "SELECT * FROM get_all_statuses("
    ), "f-string part 0 for get_all_statuses mismatch"
    assert (
        isinstance(f_string_parts_all[1], ast.FormattedValue)
        and isinstance(f_string_parts_all[1].value, ast.Name)
        and f_string_parts_all[1].value.id == "_sql_query_named_args"
    ), "f-string part 1 for get_all_statuses placeholder mismatch"
    assert isinstance(f_string_parts_all[2], ast.Constant) and f_string_parts_all[2].value == ")", (
        "f-string part 2 for get_all_statuses mismatch"
    )

    # Check execute parameters for get_all_statuses (no params)
    assert len(execute_call_all.args) == 2, "execute call should have 2 arguments for get_all_statuses"
    assert isinstance(execute_call_all.args[1], ast.Name) and execute_call_all.args[1].id == "_call_params_dict", (
        "Second arg to execute in get_all_statuses should be _call_params_dict variable"
    )

    call_params_dict_populated_all_status = False
    for node in ast.walk(all_status_func_node):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id == "_call_params_dict"
            and node.func.attr == "append"
        ):
            call_params_dict_populated_all_status = True
            break
    assert not call_params_dict_populated_all_status, (
        "_call_params_dict should not have params appended for get_all_statuses"
    )

    assert fetchall_call_all is not None, "fetchall call not found in setof func"
    assert return_rows_all, "'return rows' logic not found in get_all_statuses"


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
            if node.module == "typing":
                for alias in node.names:
                    found_typing_imports.add(alias.name)
            elif node.module == "dataclasses" and any(alias.name == "dataclass" for alias in node.names):
                found_dataclass = True
            elif node.module == "uuid" and any(alias.name == "UUID" for alias in node.names):
                found_uuid = True
    assert {"List", "Optional"}.issubset(found_typing_imports), "Missing required typing imports"
    assert found_dataclass, "Missing dataclass import"
    assert found_uuid, "Missing UUID import"

    # 2. Check UserIdentity Dataclass
    dataclass_node = None
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef) and node.name == "UserIdentity":
            dataclass_node = node
            break
    assert dataclass_node is not None, "Dataclass 'UserIdentity' not found"
    assert any(isinstance(d, ast.Name) and d.id == "dataclass" for d in dataclass_node.decorator_list), (
        "Class is not decorated with @dataclass"
    )

    expected_fields = {"user_id": "Optional[UUID]", "clerk_id": "Optional[str]", "is_active": "Optional[bool]"}
    actual_fields = {
        stmt.target.id: ast.unparse(stmt.annotation) for stmt in dataclass_node.body if isinstance(stmt, ast.AnnAssign)
    }
    assert actual_fields == expected_fields, "UserIdentity dataclass fields mismatch"

    # 3. Check get_user_identity_by_clerk_id Function
    single_func_node = None
    for node in ast.walk(tree):
        if isinstance(node, ast.AsyncFunctionDef) and node.name == "get_user_identity_by_clerk_id":
            single_func_node = node
            break
    assert single_func_node is not None, "Async function 'get_user_identity_by_clerk_id' not found"

    # Check parameters
    expected_params_single = {"conn": "AsyncConnection", "clerk_id": "str"}
    actual_params_single = {arg.arg: ast.unparse(arg.annotation) for arg in single_func_node.args.args}
    assert actual_params_single == expected_params_single, "Parameter mismatch for single func"

    # Check return annotation
    expected_return_single = "Optional[UserIdentity]"
    actual_return_single = ast.unparse(single_func_node.returns)
    assert actual_return_single == expected_return_single, "Return type mismatch for single func"

    # Check docstring
    docstring_single = ast.get_docstring(single_func_node)
    assert docstring_single and "Function returning the custom composite type" in docstring_single, (
        "Docstring mismatch for single func"
    )

    # Check body for execute, fetchone, try/except, and UserIdentity(*row) call
    execute_call_single = None
    sql_query_assign_node_single = None
    fetchone_call_single = None
    dataclass_call_single = None
    try_except_node_single = None

    for stmt in single_func_node.body:
        if isinstance(stmt, ast.Assign):
            for target in stmt.targets:
                if isinstance(target, ast.Name) and target.id == "_full_sql_query":
                    sql_query_assign_node_single = stmt
                    break
            if sql_query_assign_node_single:
                break

    for node_in_body in ast.walk(single_func_node):
        if isinstance(node_in_body, ast.Await) and isinstance(node_in_body.value, ast.Call):
            call = node_in_body.value
            if isinstance(call.func, ast.Attribute) and call.func.attr == "execute":
                execute_call_single = call
                assert isinstance(call.args[0], ast.Name) and call.args[0].id == "_full_sql_query", (
                    "execute in get_user_identity_by_clerk_id not using _full_sql_query"
                )
                if len(call.args) > 1 and isinstance(call.args[1], ast.List):
                    [elt.id for elt in call.args[1].elts if isinstance(elt, ast.Name)]
            elif isinstance(call.func, ast.Attribute) and call.func.attr == "fetchone":
                fetchone_call_single = call
        elif isinstance(node_in_body, ast.Try):
            try_except_node_single = node_in_body
            # Check for UserIdentity(*row) inside try for get_user_identity_by_clerk_id
            for try_node_wewnętrzny in ast.walk(try_except_node_single):
                if (
                    isinstance(try_node_wewnętrzny, ast.Call)
                    and isinstance(try_node_wewnętrzny.func, ast.Name)
                    and try_node_wewnętrzny.func.id == "UserIdentity"
                ) and (
                    len(try_node_wewnętrzny.args) == 1
                    and isinstance(try_node_wewnętrzny.args[0], ast.Starred)
                    and isinstance(try_node_wewnętrzny.args[0].value, ast.Name)
                    and try_node_wewnętrzny.args[0].value.id == "row"
                ):
                    dataclass_call_single = try_node_wewnętrzny
                    break
            # Check except handler
            assert len(try_except_node_single.handlers) == 1 and isinstance(
                try_except_node_single.handlers[0], ast.ExceptHandler
            ), "Single func Try/Except structure mismatch"
            assert (
                isinstance(try_except_node_single.handlers[0].type, ast.Name)
                and try_except_node_single.handlers[0].type.id == "TypeError"
            ), "Single func Try/Except structure mismatch"
            # Check for a raise statement in the except block
            raises_type_error = any(isinstance(item, ast.Raise) for item in try_except_node_single.handlers[0].body)
            assert raises_type_error, "Except handler should raise a TypeError"

    assert execute_call_single is not None, "execute call not found in single func"
    assert sql_query_assign_node_single is not None, (
        "Assignment to _full_sql_query not found in get_user_identity_by_clerk_id"
    )

    assert isinstance(sql_query_assign_node_single.value, ast.JoinedStr), (
        "_full_sql_query in get_user_identity_by_clerk_id is not an f-string"
    )
    f_string_parts_single = sql_query_assign_node_single.value.values
    assert len(f_string_parts_single) == 3, "f-string for get_user_identity_by_clerk_id has unexpected number of parts"
    assert (
        isinstance(f_string_parts_single[0], ast.Constant)
        and f_string_parts_single[0].value == "SELECT * FROM get_user_identity_by_clerk_id("
    ), "f-string part 0 for get_user_identity_by_clerk_id mismatch"
    assert (
        isinstance(f_string_parts_single[1], ast.FormattedValue)
        and isinstance(f_string_parts_single[1].value, ast.Name)
        and f_string_parts_single[1].value.id == "_sql_query_named_args"
    ), "f-string part 1 for get_user_identity_by_clerk_id placeholder mismatch"
    assert isinstance(f_string_parts_single[2], ast.Constant) and f_string_parts_single[2].value == ")", (
        "f-string part 2 for get_user_identity_by_clerk_id mismatch"
    )

    # Check execute parameters for get_user_identity_by_clerk_id
    assert len(execute_call_single.args) == 2, "execute call should have 2 arguments for get_user_identity_by_clerk_id"
    assert (
        isinstance(execute_call_single.args[1], ast.Name) and execute_call_single.args[1].id == "_call_params_dict"
    ), "Second arg to execute in get_user_identity_by_clerk_id should be _call_params_dict variable"

    # Verify _call_params_dict for get_user_identity_by_clerk_id
    clerk_id_assigned_to_call_params_dict_single = False
    for node in ast.walk(single_func_node):
        if (
            isinstance(node, ast.Assign)
            and len(node.targets) == 1
            and isinstance(node.targets[0], ast.Subscript)
            and isinstance(node.targets[0].value, ast.Name)
            and node.targets[0].value.id == "_call_params_dict"
            and isinstance(node.targets[0].slice, ast.Constant)
            and node.targets[0].slice.value == "clerk_id"
            and isinstance(node.value, ast.Name)
            and node.value.id == "clerk_id"
        ):
            clerk_id_assigned_to_call_params_dict_single = True
            break
    assert clerk_id_assigned_to_call_params_dict_single, (
        "'clerk_id' was not correctly assigned to _call_params_dict for get_user_identity_by_clerk_id"
    )

    assert fetchone_call_single is not None, "fetchone call not found in single func"
    assert try_except_node_single is not None, "Try/except block not found in single func"
    assert dataclass_call_single is not None, "UserIdentity(*row) call not found in single func try block"

    # 4. Check get_all_active_identities Function
    setof_func_node = None
    for node in ast.walk(tree):
        if isinstance(node, ast.AsyncFunctionDef) and node.name == "get_all_active_identities":
            setof_func_node = node
            break
    assert setof_func_node is not None, "Async function 'get_all_active_identities' not found"

    # Check parameters
    expected_params_setof = {"conn": "AsyncConnection"}
    actual_params_setof = {arg.arg: ast.unparse(arg.annotation) for arg in setof_func_node.args.args}
    assert actual_params_setof == expected_params_setof, "Parameter mismatch for setof func"

    # Check return annotation
    expected_return_setof = "List[UserIdentity]"
    actual_return_setof = ast.unparse(setof_func_node.returns)
    assert actual_return_setof == expected_return_setof, "Return type mismatch for setof func"

    # Check docstring
    docstring_setof = ast.get_docstring(setof_func_node)
    assert docstring_setof == "Function returning SETOF the custom composite type", "Docstring mismatch for setof func"

    # Check body for execute, fetchall, try/except, and list comprehension
    execute_call_setof = None
    sql_query_assign_node_setof = None
    fetchall_call_setof = None
    list_comp_call_setof = None
    try_except_node_setof = None
    for stmt in setof_func_node.body:
        if isinstance(stmt, ast.Assign):
            for target in stmt.targets:
                if isinstance(target, ast.Name) and target.id == "_full_sql_query":
                    sql_query_assign_node_setof = stmt
                    break
            if sql_query_assign_node_setof:
                break

    for node_in_body in ast.walk(setof_func_node):
        if isinstance(node_in_body, ast.Await) and isinstance(node_in_body.value, ast.Call):
            call = node_in_body.value
            if isinstance(call.func, ast.Attribute) and call.func.attr == "execute":
                execute_call_setof = call
                assert isinstance(call.args[0], ast.Name) and call.args[0].id == "_full_sql_query", (
                    "execute in get_all_active_identities not using _full_sql_query"
                )
                if len(call.args) > 1 and isinstance(call.args[1], ast.List):
                    [elt.id for elt in call.args[1].elts if isinstance(elt, ast.Name)]
            elif isinstance(call.func, ast.Attribute) and call.func.attr == "fetchall":
                fetchall_call_setof = call
        elif isinstance(node_in_body, ast.Try):
            try_except_node_setof = node_in_body
            # Find the list comprehension inside the try block: [UserIdentity(*r) for r in rows]
            for try_node_wewnętrzny in ast.walk(try_except_node_setof):
                if isinstance(try_node_wewnętrzny, ast.ListComp) and (
                    isinstance(try_node_wewnętrzny.elt, ast.Call)
                    and isinstance(try_node_wewnętrzny.elt.func, ast.Name)
                    and try_node_wewnętrzny.elt.func.id == "UserIdentity"
                    and len(try_node_wewnętrzny.elt.args) == 1
                    and isinstance(try_node_wewnętrzny.elt.args[0], ast.Starred)
                    and isinstance(try_node_wewnętrzny.elt.args[0].value, ast.Name)
                    and try_node_wewnętrzny.elt.args[0].value.id == "r"
                ):
                    comp = try_node_wewnętrzny.generators[0]
                    if (
                        isinstance(comp.target, ast.Name)
                        and comp.target.id == "r"
                        and isinstance(comp.iter, ast.Name)
                        and comp.iter.id == "rows"
                    ):
                        list_comp_call_setof = try_node_wewnętrzny.elt
                        break
            # Check except handler
            assert len(try_except_node_setof.handlers) == 1 and isinstance(
                try_except_node_setof.handlers[0], ast.ExceptHandler
            ), "Setof func Try/Except structure mismatch"
            assert (
                isinstance(try_except_node_setof.handlers[0].type, ast.Name)
                and try_except_node_setof.handlers[0].type.id == "TypeError"
            ), "Setof func Try/Except structure mismatch"
            # Check for a raise statement in the except block
            raises_type_error = any(isinstance(item, ast.Raise) for item in try_except_node_setof.handlers[0].body)
            assert raises_type_error, "Except handler should raise a TypeError"

    assert execute_call_setof is not None, "execute call not found in setof func"
    assert sql_query_assign_node_setof is not None, (
        "Assignment to _full_sql_query not found in get_all_active_identities"
    )

    assert isinstance(sql_query_assign_node_setof.value, ast.JoinedStr), (
        "_full_sql_query in get_all_active_identities is not an f-string"
    )
    f_string_parts_setof = sql_query_assign_node_setof.value.values
    assert len(f_string_parts_setof) == 3, "f-string for get_all_active_identities has unexpected number of parts"
    assert (
        isinstance(f_string_parts_setof[0], ast.Constant)
        and f_string_parts_setof[0].value == "SELECT * FROM get_all_active_identities("
    ), "f-string part 0 for get_all_active_identities mismatch"
    assert (
        isinstance(f_string_parts_setof[1], ast.FormattedValue)
        and isinstance(f_string_parts_setof[1].value, ast.Name)
        and f_string_parts_setof[1].value.id == "_sql_query_named_args"
    ), "f-string part 1 for get_all_active_identities placeholder mismatch"
    assert isinstance(f_string_parts_setof[2], ast.Constant) and f_string_parts_setof[2].value == ")", (
        "f-string part 2 for get_all_active_identities mismatch"
    )

    # Check execute parameters for get_all_active_identities (no params)
    assert len(execute_call_setof.args) == 2, "execute call should have 2 arguments for get_all_active_identities"
    assert isinstance(execute_call_setof.args[1], ast.Name) and execute_call_setof.args[1].id == "_call_params_dict", (
        "Second arg to execute in get_all_active_identities should be _call_params_dict variable"
    )

    call_params_dict_populated_setof = False
    for node in ast.walk(setof_func_node):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id == "_call_params_dict"
            and node.func.attr == "append"
        ):
            call_params_dict_populated_setof = True  # Found an append
            break
    assert not call_params_dict_populated_setof, (
        "_call_params_dict should not have params appended for get_all_active_identities"
    )

    assert fetchall_call_setof is not None, "fetchall call not found in setof func"
    assert list_comp_call_setof is not None, (
        "List comprehension '[UserIdentity(*r) for r in rows]' not found in setof func try block"
    )

    # Old comparison removed
    # assert actual_content == expected_content, (...)
