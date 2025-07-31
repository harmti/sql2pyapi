from dataclasses import dataclass
from unittest.mock import AsyncMock

import psycopg
import pytest


# Define a sample dataclass similar to what sql2py might generate
@dataclass
class EntityTableResult:
    id: int
    name: str
    is_active: bool


# Function that mimics the generated code for a single-row return function
async def get_entity(conn: psycopg.AsyncConnection, id: int) -> EntityTableResult | None:
    """Mimics a generated function that returns a single row or None."""
    async with conn.cursor() as cur:
        await cur.execute("SELECT * FROM get_entity(%s)", [id])
        row = await cur.fetchone()
        if row is None:
            return None
        # The row processing happens after the None check
        colnames = [desc[0] for desc in cur.description]
        row_dict = dict(zip(colnames, row, strict=False)) if not isinstance(row, dict) else row
        return EntityTableResult(**row_dict)


@pytest.mark.asyncio
async def test_none_row_handling():
    """Test that the function properly returns None when no rows are found."""
    mock_conn = AsyncMock(spec=psycopg.AsyncConnection)
    mock_cursor = AsyncMock(spec=psycopg.AsyncCursor)

    # Configure mock cursor to return None (no rows found)
    mock_cursor.description = [("id",), ("name",), ("is_active",)]
    mock_cursor.fetchone.return_value = None

    # Setup context manager behavior
    mock_conn.cursor.return_value.__aenter__.return_value = mock_cursor
    mock_conn.cursor.return_value.__aexit__.return_value = None

    # Call the function that mimics generated code
    result = await get_entity(mock_conn, 999)  # Non-existent ID

    # Assertions
    mock_cursor.execute.assert_called_once_with("SELECT * FROM get_entity(%s)", [999])
    mock_cursor.fetchone.assert_awaited_once()
    assert result is None  # Should return None when no rows are found


@pytest.mark.asyncio
async def test_none_row_handling_with_actual_row():
    """Test that the function properly processes a row when one is found."""
    mock_conn = AsyncMock(spec=psycopg.AsyncConnection)
    mock_cursor = AsyncMock(spec=psycopg.AsyncCursor)

    # Configure mock cursor to return a row
    mock_cursor.description = [("id",), ("name",), ("is_active",)]
    mock_cursor.fetchone.return_value = {"id": 123, "name": "Test Entity", "is_active": True}

    # Setup context manager behavior
    mock_conn.cursor.return_value.__aenter__.return_value = mock_cursor
    mock_conn.cursor.return_value.__aexit__.return_value = None

    # Call the function that mimics generated code
    result = await get_entity(mock_conn, 123)  # Existing ID

    # Assertions
    mock_cursor.execute.assert_called_once_with("SELECT * FROM get_entity(%s)", [123])
    mock_cursor.fetchone.assert_awaited_once()
    assert result is not None
    assert isinstance(result, EntityTableResult)
    assert result.id == 123
    assert result.name == "Test Entity"
    assert result.is_active is True
