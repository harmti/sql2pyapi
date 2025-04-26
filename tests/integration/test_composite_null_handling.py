import pytest
import psycopg
from dataclasses import dataclass
from unittest.mock import AsyncMock
from typing import Optional

# Define a sample dataclass similar to what sql2py might generate
@dataclass
class CompanyResult:
    id: Optional[str]
    name: Optional[str]
    industry: Optional[str]
    size: Optional[int]
    created_at: Optional[str]

# Function that mimics the generated code with our enhancement
async def get_company_by_id(conn: psycopg.AsyncConnection, company_id: str) -> Optional[CompanyResult]:
    """Mimics a generated function that returns a composite type."""
    async with conn.cursor() as cur:
        await cur.execute("SELECT * FROM get_company_by_id(%s)", [company_id])
        row = await cur.fetchone()
        if row is None:
            return None
        # Process the row
        colnames = [desc[0] for desc in cur.description]
        row_dict = dict(zip(colnames, row)) if not isinstance(row, dict) else row
        # Check for 'empty' composite rows (all values are None)
        if all(value is None for value in row_dict.values()):
            return None
        return CompanyResult(**row_dict)

@pytest.mark.asyncio
async def test_composite_type_all_nulls_handling():
    """Test that the function properly returns None when a composite row has all NULL values."""
    mock_conn = AsyncMock(spec=psycopg.AsyncConnection)
    mock_cursor = AsyncMock(spec=psycopg.AsyncCursor)
    
    # Configure mock cursor to return a row with all NULL values
    # This simulates PostgreSQL's behavior with composite types when no matching row is found
    mock_cursor.description = [("id",), ("name",), ("industry",), ("size",), ("created_at",)]
    mock_cursor.fetchone.return_value = {"id": None, "name": None, "industry": None, "size": None, "created_at": None}
    
    # Setup context manager behavior
    mock_conn.cursor.return_value.__aenter__.return_value = mock_cursor
    mock_conn.cursor.return_value.__aexit__.return_value = None
    
    # Call the function that mimics generated code
    result = await get_company_by_id(mock_conn, "non-existent-id")  # Non-existent ID
    
    # Assertions
    mock_cursor.execute.assert_called_once_with(
        "SELECT * FROM get_company_by_id(%s)", ["non-existent-id"]
    )
    mock_cursor.fetchone.assert_awaited_once()
    assert result is None  # Should return None for all-NULL composite rows

@pytest.mark.asyncio
async def test_composite_type_with_valid_row():
    """Test that the function properly processes a valid composite row."""
    mock_conn = AsyncMock(spec=psycopg.AsyncConnection)
    mock_cursor = AsyncMock(spec=psycopg.AsyncCursor)
    
    # Configure mock cursor to return a valid row
    mock_cursor.description = [("id",), ("name",), ("industry",), ("size",), ("created_at",)]
    mock_cursor.fetchone.return_value = {
        "id": "123", 
        "name": "Acme Corp", 
        "industry": "Technology", 
        "size": 500, 
        "created_at": "2023-01-01"
    }
    
    # Setup context manager behavior
    mock_conn.cursor.return_value.__aenter__.return_value = mock_cursor
    mock_conn.cursor.return_value.__aexit__.return_value = None
    
    # Call the function that mimics generated code
    result = await get_company_by_id(mock_conn, "123")  # Existing ID
    
    # Assertions
    mock_cursor.execute.assert_called_once_with(
        "SELECT * FROM get_company_by_id(%s)", ["123"]
    )
    mock_cursor.fetchone.assert_awaited_once()
    assert result is not None
    assert isinstance(result, CompanyResult)
    assert result.id == "123"
    assert result.name == "Acme Corp"
    assert result.industry == "Technology"
    assert result.size == 500
    assert result.created_at == "2023-01-01"

@pytest.mark.asyncio
async def test_composite_type_with_partial_nulls():
    """Test that the function properly processes a row with some NULL values."""
    mock_conn = AsyncMock(spec=psycopg.AsyncConnection)
    mock_cursor = AsyncMock(spec=psycopg.AsyncCursor)
    
    # Configure mock cursor to return a row with some NULL values
    mock_cursor.description = [("id",), ("name",), ("industry",), ("size",), ("created_at",)]
    mock_cursor.fetchone.return_value = {
        "id": "456", 
        "name": "Beta Inc", 
        "industry": None,  # Some fields are NULL
        "size": None, 
        "created_at": "2023-02-01"
    }
    
    # Setup context manager behavior
    mock_conn.cursor.return_value.__aenter__.return_value = mock_cursor
    mock_conn.cursor.return_value.__aexit__.return_value = None
    
    # Call the function that mimics generated code
    result = await get_company_by_id(mock_conn, "456")  # Existing ID with some NULL fields
    
    # Assertions
    mock_cursor.execute.assert_called_once_with(
        "SELECT * FROM get_company_by_id(%s)", ["456"]
    )
    mock_cursor.fetchone.assert_awaited_once()
    assert result is not None
    assert isinstance(result, CompanyResult)
    assert result.id == "456"
    assert result.name == "Beta Inc"
    assert result.industry is None
    assert result.size is None
    assert result.created_at == "2023-02-01"
