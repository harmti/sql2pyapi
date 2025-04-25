import pytest
import psycopg
from dataclasses import dataclass
from unittest.mock import MagicMock, AsyncMock  # Use AsyncMock for async methods
from typing import Optional, List

# 1. Define a sample dataclass similar to what sql2py might generate
@dataclass
class SimpleResult:
    id: int
    name: str
    is_active: bool

# 2. Define a representative function structure mirroring the generated code
#    This function includes the core logic we want to test:
#    - fetching column names from description
#    - converting tuple to dict if necessary (though we'll test the dict case)
#    - using **row_dict for initialization
async def get_simple_result_runtime(conn: psycopg.AsyncConnection, item_id: int) -> Optional[SimpleResult]:
    """Mimics a generated function fetching a single row returning a dataclass."""
    async with conn.cursor() as cur:
        await cur.execute("SELECT id, name, is_active FROM simple_table WHERE id = %s", [item_id])
        row = await cur.fetchone() # This will be a dict in our test case
        if row is None:
            return None
        # The core logic under test:
        # Although fetchone returns a dict here, the generated code needs this logic
        # to handle both tuple and dict rows universally.
        colnames = [desc[0] for desc in cur.description]
        row_dict = dict(zip(colnames, row)) if not isinstance(row, dict) else row
        return SimpleResult(**row_dict)

async def get_simple_results_runtime(conn: psycopg.AsyncConnection) -> List[SimpleResult]:
    """Mimics a generated function fetching multiple rows returning a list of dataclasses."""
    async with conn.cursor() as cur:
        await cur.execute("SELECT id, name, is_active FROM simple_table")
        rows = await cur.fetchall() # This will be a list of dicts in our test case
        if not rows:
            return []
        # The core logic under test:
        colnames = [desc[0] for desc in cur.description]
        processed_rows = [
            dict(zip(colnames, r)) if not isinstance(r, dict) else r
            for r in rows
        ]
        return [SimpleResult(**row_dict) for row_dict in processed_rows]


# 3. Test case using pytest-asyncio and unittest.mock
@pytest.mark.asyncio
async def test_generated_code_handles_dict_rows_fetchone():
    """Verify the generated code structure works with dict rows from fetchone."""
    mock_conn = AsyncMock(spec=psycopg.AsyncConnection)
    mock_cursor = AsyncMock(spec=psycopg.AsyncCursor)

    # Configure mock cursor
    # description needs to match the keys in the returned dict
    mock_cursor.description = [("id",), ("name",), ("is_active",)]
    # Simulate fetchone returning a dictionary
    mock_dict_row = {"id": 123, "name": "Test Item", "is_active": True}
    mock_cursor.fetchone.return_value = mock_dict_row

    # Setup context manager behavior
    mock_conn.cursor.return_value.__aenter__.return_value = mock_cursor
    mock_conn.cursor.return_value.__aexit__.return_value = None # Or mock appropriately if needed

    # Call the function that mimics generated code
    result = await get_simple_result_runtime(mock_conn, 123)

    # Assertions
    mock_cursor.execute.assert_called_once_with(
        "SELECT id, name, is_active FROM simple_table WHERE id = %s", [123]
    )
    mock_cursor.fetchone.assert_awaited_once()
    assert result is not None
    assert isinstance(result, SimpleResult)
    assert result.id == 123
    assert result.name == "Test Item"
    assert result.is_active is True

@pytest.mark.asyncio
async def test_generated_code_handles_dict_rows_fetchall():
    """Verify the generated code structure works with list of dict rows from fetchall."""
    mock_conn = AsyncMock(spec=psycopg.AsyncConnection)
    mock_cursor = AsyncMock(spec=psycopg.AsyncCursor)

    # Configure mock cursor
    mock_cursor.description = [("id",), ("name",), ("is_active",)]
    # Simulate fetchall returning a list of dictionaries
    mock_dict_rows = [
        {"id": 10, "name": "Alpha", "is_active": True},
        {"id": 20, "name": "Beta", "is_active": False},
    ]
    mock_cursor.fetchall.return_value = mock_dict_rows

    # Setup context manager behavior
    mock_conn.cursor.return_value.__aenter__.return_value = mock_cursor
    mock_conn.cursor.return_value.__aexit__.return_value = None

    # Call the function that mimics generated code
    results = await get_simple_results_runtime(mock_conn)

    # Assertions
    mock_cursor.execute.assert_called_once_with("SELECT id, name, is_active FROM simple_table")
    mock_cursor.fetchall.assert_awaited_once()
    assert isinstance(results, list)
    assert len(results) == 2
    assert isinstance(results[0], SimpleResult)
    assert results[0].id == 10
    assert results[0].name == "Alpha"
    assert results[0].is_active is True
    assert isinstance(results[1], SimpleResult)
    assert results[1].id == 20
    assert results[1].name == "Beta"
    assert results[1].is_active is False 