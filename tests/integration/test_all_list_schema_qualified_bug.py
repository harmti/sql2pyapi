"""Test for __all__ list bug with schema-qualified function names.

This test reproduces the bug reported where schema-qualified function names
like 'public.create_contract' appear in the __all__ list instead of just
the function name 'create_contract'.
"""

import ast

from sql2pyapi.generator import generate_python_code
from sql2pyapi.parser import parse_sql


def extract_all_list_from_ast(tree: ast.AST) -> list:
    """Extract the __all__ list from an AST, returning its contents as a list of strings."""
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Assign)
            and len(node.targets) == 1
            and isinstance(node.targets[0], ast.Name)
            and node.targets[0].id == "__all__"
        ):
            # Extract the list contents
            if isinstance(node.value, ast.List):
                all_items = []
                for elt in node.value.elts:
                    if isinstance(elt, ast.Constant) and isinstance(elt.value, str):
                        all_items.append(elt.value)
                return all_items

    return []  # __all__ not found


def test_all_list_with_schema_qualified_function_names():
    """Test that __all__ list contains valid Python identifiers (no schema prefixes)."""

    # SQL with schema-qualified function names
    sql_content = """
-- Schema-qualified table
CREATE TABLE public.contracts (
    id uuid PRIMARY KEY,
    name text NOT NULL,
    status text
);

CREATE TABLE public.market_participants (
    id uuid PRIMARY KEY,
    name text NOT NULL,
    type text
);

-- Schema-qualified function names (this is the bug trigger)
CREATE FUNCTION public.create_contract(p_name text)
RETURNS public.contracts
LANGUAGE sql
AS $$
    INSERT INTO public.contracts (id, name, status)
    VALUES (gen_random_uuid(), p_name, 'draft')
    RETURNING *;
$$;

CREATE FUNCTION public.create_market_participant(p_name text, p_type text)
RETURNS public.market_participants
LANGUAGE sql
AS $$
    INSERT INTO public.market_participants (id, name, type)
    VALUES (gen_random_uuid(), p_name, p_type)
    RETURNING *;
$$;

CREATE FUNCTION public.get_contract_count()
RETURNS integer
LANGUAGE sql
AS $$
    SELECT COUNT(*)::integer FROM public.contracts;
$$;
"""

    # Parse the SQL
    parsed_functions, table_imports, composite_types, enum_types = parse_sql(sql_content)

    # Generate Python code
    python_code = generate_python_code(parsed_functions, table_imports, composite_types, enum_types)

    # Parse the generated Python code to extract __all__
    tree = ast.parse(python_code)
    all_list = extract_all_list_from_ast(tree)

    # Verify __all__ list exists
    assert all_list, "No __all__ list found in generated code"

    # The BUG: Check for invalid identifiers with dots
    invalid_identifiers = [name for name in all_list if "." in name]

    # This assertion should FAIL initially (reproducing the bug)
    assert not invalid_identifiers, f"Found invalid identifiers in __all__: {invalid_identifiers}"

    # Expected valid identifiers (without schema prefixes)
    expected_function_names = {"create_contract", "create_market_participant", "get_contract_count"}

    expected_class_names = {
        "Contract",  # from public.contracts table
        "MarketParticipant",  # from public.market_participants table
    }

    # Verify all expected valid names are in __all__
    all_set = set(all_list)
    assert expected_function_names.issubset(all_set), (
        f"Missing function names in __all__. Expected: {expected_function_names}, Got: {all_set}"
    )
    assert expected_class_names.issubset(all_set), (
        f"Missing class names in __all__. Expected: {expected_class_names}, Got: {all_set}"
    )

    # Verify all items in __all__ are valid Python identifiers
    for name in all_list:
        assert name.isidentifier(), f"'{name}' is not a valid Python identifier"
        assert "." not in name, f"'{name}' contains a dot, making it invalid for __all__"


def test_function_python_name_vs_sql_name():
    """Test that Function.python_name strips schema qualification while sql_name preserves it."""

    sql_content = """
CREATE TABLE public.users (id uuid, name text);

CREATE FUNCTION public.get_user(p_id uuid)
RETURNS public.users
LANGUAGE sql
AS $$
    SELECT * FROM public.users WHERE id = p_id;
$$;
"""

    # Parse the SQL
    parsed_functions, _, _, _ = parse_sql(sql_content)

    # Find our function
    get_user_func = next((f for f in parsed_functions if "get_user" in f.sql_name), None)
    assert get_user_func is not None, "get_user function not found"

    # The BUG: Both sql_name and python_name are currently the same (schema-qualified)
    # After fix: sql_name should be schema-qualified, python_name should not be

    # This assertion should FAIL initially (reproducing the bug)
    assert get_user_func.python_name == "get_user", (
        f"python_name should be 'get_user', got '{get_user_func.python_name}'"
    )

    # sql_name should preserve the schema qualification for database operations
    assert get_user_func.sql_name == "public.get_user", (
        f"sql_name should be 'public.get_user', got '{get_user_func.sql_name}'"
    )
