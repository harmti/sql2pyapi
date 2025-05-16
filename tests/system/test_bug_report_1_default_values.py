import pytest
from psycopg import AsyncConnection
from typing import Optional, List

from sql2pyapi import parse_sql, generate_python_code
from sql2pyapi.sql_models import SQLParameter, ParsedFunction

from tests.test_utils import ( # Reverted to absolute-like import from project root
    create_test_function,
    create_test_table,
    setup_db_and_load_api, 
    TEST_DB_CONN_STRING 
)

# Test for Bug Report 1: SQL DEFAULT <non-NULL value> being overridden

FUNC_SQL_DEFAULT_NON_NULL = """
CREATE OR REPLACE FUNCTION public.test_default_value(
    p_name TEXT,
    p_quantity INTEGER DEFAULT 10,
    p_category_id INTEGER DEFAULT 1
)
RETURNS TEXT
LANGUAGE plpgsql
AS $$
BEGIN
    -- For testing, we'll just return a concatenation of the parameters
    -- to observe what values were actually used by the database.
    RETURN 'Name=' || p_name || ', Qty=' || p_quantity::TEXT || ', CatID=' || p_category_id::TEXT;
END;
$$;
"""

@pytest.mark.asyncio
async def test_sql_default_non_null_value_used_when_python_arg_is_none(tmp_path):
    """
    Tests that when a Python optional argument (SQL DEFAULT non-NULL) is None (omitted),
    the database's own non-NULL DEFAULT value is used.
    (Corresponds to Bug Report 1)
    """
    # 1. Parse and Generate the API
    functions, _, _, _ = parse_sql(FUNC_SQL_DEFAULT_NON_NULL)
    assert len(functions) == 1
    func = functions[0]

    assert func.sql_name == "public.test_default_value" 
    assert len(func.params) == 3
    
    qty_param = next(p for p in func.params if p.name == 'p_quantity')
    cat_param = next(p for p in func.params if p.name == 'p_category_id')

    assert qty_param.is_optional
    assert qty_param.has_sql_default  # DEFAULT 10 is non-NULL
    assert qty_param.python_type == 'Optional[int]'

    assert cat_param.is_optional
    assert cat_param.has_sql_default  # DEFAULT 1 is non-NULL
    assert cat_param.python_type == 'Optional[int]'

    python_code = generate_python_code(functions, {}, {}, {})
    
    # Removed outdated assertions that checked for the old code generation pattern
    # The end-to-end database tests below (section #2) will verify the correct behavior
    # regarding SQL DEFAULT values.

    # 2. Execute and Test against a live DB
    db_api = await setup_db_and_load_api( 
        tmp_path, 
        FUNC_SQL_DEFAULT_NON_NULL, 
        sql_schema_content=None, 
        module_name="test_default_value_api"
    )

    async with await AsyncConnection.connect(TEST_DB_CONN_STRING) as conn:
        # Call 1: Omit p_quantity, provide p_category_id
        result1 = await db_api.test_default_value(conn, name="TestItem1", category_id=5)
        assert result1 == "Name=TestItem1, Qty=10, CatID=5" 

        # Call 2: Provide p_quantity, omit p_category_id
        result2 = await db_api.test_default_value(conn, name="TestItem2", quantity=25)
        assert result2 == "Name=TestItem2, Qty=25, CatID=1" 

        # Call 3: Omit both p_quantity and p_category_id
        result3 = await db_api.test_default_value(conn, name="TestItem3")
        assert result3 == "Name=TestItem3, Qty=10, CatID=1" 

        # Call 4: Provide all
        result4 = await db_api.test_default_value(conn, name="TestItem4", quantity=50, category_id=7)
        assert result4 == "Name=TestItem4, Qty=50, CatID=7"
        
        # Call 5: Explicitly pass None for an optional arg with non-NULL default
        result5 = await db_api.test_default_value(conn, name="TestItem5", quantity=None, category_id=None)
        assert result5 == "Name=TestItem5, Qty=10, CatID=1" 