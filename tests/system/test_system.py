# tests/system/test_system.py
import pytest
import pytest_asyncio
import psycopg # type: ignore
import subprocess
import time
import os
import sys
import importlib.util
from pathlib import Path
import uuid
from decimal import Decimal
from datetime import datetime, timezone, date
from typing import Any, List, Optional, Tuple
import types

# --- Constants ---
# Paths relative to project root (where pytest is run)
PROJECT_ROOT = Path(__file__).parent.parent.parent
SYSTEM_TEST_DIR = Path("tests/system")
SQL_DIR = SYSTEM_TEST_DIR / "sql"
SCHEMA_FILE = SQL_DIR / "00_schema.sql"
FUNCTIONS_FILE = SQL_DIR / "01_functions.sql"
GENERATED_API_FILENAME = "generated_db_api.py"
GENERATED_API_PATH = SYSTEM_TEST_DIR / GENERATED_API_FILENAME # Generate inside tests/system

DB_USER = "testuser"
DB_PASSWORD = "testpass"
DB_NAME = "testdb"
DB_HOST = "localhost"
DB_PORT = 5433 # Exposed host port
DSN = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"


# --- Fixtures ---

# The manage_db_container fixture has been moved to tests/system/conftest.py

@pytest.fixture(scope="session")
def generated_api_module():
    """Runs sql2pyapi and returns the dynamically imported module."""
    print(f"\nGenerating API code to {GENERATED_API_PATH}...")
    # Ensure the target directory exists
    (PROJECT_ROOT / SYSTEM_TEST_DIR).mkdir(parents=True, exist_ok=True)

    generated_file_abs_path = PROJECT_ROOT / GENERATED_API_PATH

    try:
        # Run sql2pyapi from the project root
        # Use the command directly if it's installed as an entry point
        cmd = [
            "sql2pyapi", # Assuming this is in PATH after installation
            str(FUNCTIONS_FILE), # Path relative to project root
            str(GENERATED_API_PATH), # Path relative to project root
            "--schema-file",
            str(SCHEMA_FILE), # Path relative to project root
        ]
        print(f"Running command: {' '.join(cmd)} from {PROJECT_ROOT}")
        result = subprocess.run(cmd, check=True, capture_output=True, text=True, cwd=PROJECT_ROOT)
        print("sql2pyapi stdout:", result.stdout)
        print("sql2pyapi stderr:", result.stderr)

        if not generated_file_abs_path.exists():
            raise FileNotFoundError(f"Generated file not found at {generated_file_abs_path}")

        # Read the generated code
        print(f"Reading generated code from {generated_file_abs_path}")
        with open(generated_file_abs_path, 'r') as f:
            generated_code = f.read()

        # Dynamically create and execute the corrected module code
        module_name = GENERATED_API_PATH.stem
        generated_module = types.ModuleType(module_name)

        # Add the directory of the generated file to sys.path temporarily
        # This helps if the generated code imports other things relative to itself
        sys.path.insert(0, str(generated_file_abs_path.parent))

        print(f"Executing corrected code for module: {module_name}")
        exec(generated_code, generated_module.__dict__)

        print(f"Successfully loaded generated module: {module_name}")
        yield generated_module
    finally:
        # Clean up sys.path
        if str(generated_file_abs_path.parent) in sys.path:
             sys.path.pop(0)
        # File cleanup is disabled for now during debugging

@pytest_asyncio.fixture(scope="function")
async def db_conn():
    """Provides an async psycopg connection with tuple row factory."""
    try:
        async with await psycopg.AsyncConnection.connect(DSN, autocommit=True) as aconn:
             print(f"\nDB connection established ({id(aconn)})")
             yield aconn
             print(f"DB connection closed ({id(aconn)})")
    except psycopg.OperationalError as e:
        pytest.fail(f"Failed to connect to database at {DSN}: {e}")

@pytest_asyncio.fixture(scope="function")
async def db_conn_dict_row():
    """Provides an async psycopg connection with DictRow row factory."""
    try:
        # Connect using the same DSN, but specify row_factory
        async with await psycopg.AsyncConnection.connect(
            DSN,
            row_factory=psycopg.rows.dict_row, # Use dict_row factory
            autocommit=True
        ) as aconn:
            print(f"\nDB dict_row connection established ({id(aconn)})\n")
            yield aconn
            print(f"\nDB dict_row connection closed ({id(aconn)})\n")
    except psycopg.OperationalError as e:
        pytest.fail(f"Failed to connect to database (dict_row) at {DSN}: {e}")


# --- Test Data Setup ---

@pytest_asyncio.fixture(scope="function", autouse=True)
async def setup_initial_data(db_conn):
    """Cleans relevant tables and inserts some base data before each test."""
    async with db_conn.cursor() as cur:
        # Clean up tables in reverse order of dependencies
        await cur.execute("DELETE FROM related_items;")
        await cur.execute("DELETE FROM items;")
        # Reset sequence for SERIAL PK
        await cur.execute("ALTER SEQUENCE items_id_seq RESTART WITH 1;")

        # Insert some initial data
        await cur.execute(
            """
            INSERT INTO items (name, description, quantity, price, is_active, metadata, tags, related_ids, current_mood)
            VALUES
                ('Apple', 'A crisp red apple', 10, 0.50, true, '{"color": "red", "origin": "local"}', ARRAY['fruit', 'healthy'], ARRAY[101, 102], 'happy'),
                ('Banana', 'A ripe yellow banana', 25, 0.30, true, '{"color": "yellow"}', ARRAY['fruit'], NULL, 'ok'),
                ('inactive Chair', 'A wooden chair', 2, 55.99, false, NULL, ARRAY['furniture'], ARRAY[201], 'sad');
            """
        )
        print("Initial data setup complete.")
    yield # Test runs here
    # No specific teardown needed here as tables are cleaned at the start of next test


# --- Test Functions ---

@pytest.mark.asyncio
async def test_get_item_count(db_conn, generated_api_module):
    """Test counting items after initial setup."""
    count = await generated_api_module.get_item_count(db_conn)
    assert count == 3 # Based on setup_initial_data

@pytest.mark.asyncio
async def test_get_item_by_id_found(db_conn, generated_api_module):
    """Test retrieving an existing item by ID."""
    # Assuming the first item inserted has id=1
    item_id = 1
    retrieved_item = await generated_api_module.get_item_by_id(db_conn, item_id=item_id)

    # Dynamically get the expected class name (e.g., 'Items') if needed
    # For now, assume it's 'Items' based on table name convention
    assert retrieved_item is not None
    assert retrieved_item.id == item_id
    assert retrieved_item.name == "Apple"
    assert retrieved_item.price == Decimal("0.50")
    assert retrieved_item.is_active is True
    assert retrieved_item.current_mood == generated_api_module.Mood.HAPPY
    assert retrieved_item.tags == ["fruit", "healthy"]
    assert retrieved_item.related_ids == [101, 102]
    assert retrieved_item.metadata == {"color": "red", "origin": "local"}
    assert isinstance(retrieved_item.created_at, datetime)
    assert retrieved_item.updated_at is None # Not set initially

@pytest.mark.asyncio
async def test_get_item_by_id_not_found(db_conn, generated_api_module):
    """Test retrieving a non-existent item."""
    item = await generated_api_module.get_item_by_id(db_conn, item_id=999)
    assert item is None

@pytest.mark.asyncio
# @pytest.mark.xfail(reason="Generator currently uses fetchone() for SETOF scalar.") # Removed xfail
async def test_get_all_item_names(db_conn, generated_api_module):
    """Test getting all item names using SETOF TEXT."""
    names = await generated_api_module.get_all_item_names(db_conn)
    assert sorted(names) == sorted(["Apple", "Banana", "inactive Chair"])

@pytest.mark.asyncio
async def test_get_items_with_mood(db_conn, generated_api_module):
    """Test filtering items by enum mood."""
    happy_items = await generated_api_module.get_items_with_mood(db_conn, mood=generated_api_module.Mood.HAPPY)
    ok_items = await generated_api_module.get_items_with_mood(db_conn, mood=generated_api_module.Mood.OK)
    sad_items = await generated_api_module.get_items_with_mood(db_conn, mood=generated_api_module.Mood.SAD)

    assert len(happy_items) == 1
    assert happy_items[0].name == "Apple"
    assert happy_items[0].current_mood == generated_api_module.Mood.HAPPY

    assert len(ok_items) == 1
    assert ok_items[0].name == "Banana"
    assert ok_items[0].current_mood == generated_api_module.Mood.OK

    assert len(sad_items) == 1
    assert sad_items[0].name == "inactive Chair"
    assert sad_items[0].current_mood == generated_api_module.Mood.SAD

@pytest.mark.asyncio
async def test_search_items(db_conn, generated_api_module):
    """Test the function returning a TABLE definition."""
    # Search term matching 'Apple' name and 'Chair' description
    results = await generated_api_module.search_items(db_conn, search_term="app")
    assert len(results) == 1
    # Assuming the TABLE return generates a dataclass/Pydantic model 'SearchItemsResult' or similar
    # Let's assume field names match the TABLE definition for now
    assert results[0].item_id == 1
    assert results[0].item_name == "Apple"
    assert isinstance(results[0].creation_date, date)

    results = await generated_api_module.search_items(db_conn, search_term="chair")
    assert len(results) == 1
    assert results[0].item_id == 3
    assert results[0].item_name == "inactive Chair"

    results = await generated_api_module.search_items(db_conn, search_term="fruit") # Matches tags, not name/desc
    # The SQL ILIKE is only on name/description, so this should be empty
    # Wait, the SQL function only searches name/description. Let's test that.
    results_fruit = await generated_api_module.search_items(db_conn, search_term="fruit")
    assert len(results_fruit) == 0 # Correct, as 'fruit' is only in tags

    results_an = await generated_api_module.search_items(db_conn, search_term="an") # Matches Banana
    assert len(results_an) == 1
    assert results_an[0].item_name == "Banana"

# @pytest.mark.asyncio
# async def test_add_related_item(db_conn, generated_api_module):
#     """Test adding a related item and checking the returned UUID."""
#     # TODO: Enable this test once sql2pyapi can parse PL/pgSQL functions like add_related_item
#     item_id = 1 # Apple
#     notes = "Related note for apple"
#     config = {"setting": "value", "enabled": True}
#     specific_uuid = uuid.uuid4()
#
#     # Call with specific UUID
#     returned_uuid = await generated_api_module.add_related_item(
#         db_conn,
#         item_id=item_id,
#         notes=notes,
#         config=config, # Pass dict directly, assuming json conversion
#         uuid=specific_uuid
#     )
#     assert returned_uuid == specific_uuid
#
#     # Verify in DB
#     async with db_conn.cursor() as cur:
#         await cur.execute("SELECT item_id, notes, config FROM related_items WHERE uuid_key = %s", (specific_uuid,))
#         row = await cur.fetchone()
#         assert row is not None
#         assert row[0] == item_id
#         assert row[1] == notes
#         assert row[2] == config # Check if JSON comes back correctly
#
#     # Call with default UUID
#     returned_uuid_default = await generated_api_module.add_related_item(
#         db_conn, item_id=2, notes="Another note" # config defaults to {}
#     )
#     assert isinstance(returned_uuid_default, uuid.UUID)
#     assert returned_uuid_default != specific_uuid

@pytest.mark.asyncio
async def test_update_item_timestamp(db_conn, generated_api_module):
    """Test the VOID function updating a timestamp."""
    item_id = 2 # Banana
    # Get current timestamp (should be NULL)
    async with db_conn.cursor() as cur:
        await cur.execute("SELECT updated_at FROM items WHERE id = %s", (item_id,))
        initial_ts = (await cur.fetchone())[0]
        assert initial_ts is None

    # Call the VOID function
    result = await generated_api_module.update_item_timestamp(db_conn, item_id=item_id)
    assert result is None # VOID functions should return None

    # Verify timestamp was updated
    async with db_conn.cursor() as cur:
        await cur.execute("SELECT updated_at FROM items WHERE id = %s", (item_id,))
        updated_ts = (await cur.fetchone())[0]
        assert updated_ts is not None
        assert isinstance(updated_ts, datetime)
        # Ensure it's timezone-aware if TIMESTAMPTZ, or naive if TIMESTAMP
        # Our schema uses TIMESTAMP (nullable, no TZ)
        assert updated_ts.tzinfo is None


@pytest.mark.asyncio
async def test_get_item_description_nullable(db_conn, generated_api_module):
    """Test retrieving a nullable text field."""
    # Item 1 has description
    desc1 = await generated_api_module.get_item_description(db_conn, item_id=1)
    assert desc1 == "A crisp red apple"

    # Item 3 has description
    desc3 = await generated_api_module.get_item_description(db_conn, item_id=3)
    assert desc3 == "A wooden chair"

    # Item 2 has no description (NULL in DB)
    # We need to insert an item without a description first.
    # Let's modify the setup fixture or add one here.
    async with db_conn.cursor() as cur:
        await cur.execute("UPDATE items SET description = NULL WHERE id = 2;") # Ensure Banana has null desc

    desc2 = await generated_api_module.get_item_description(db_conn, item_id=2)
    assert desc2 is None

# Tests for composite types (item_summary) and anonymous records might need adjustments
# depending on how sql2pyapi generates code for them. Let's add placeholders.

@pytest.mark.asyncio
async def test_get_item_summaries_composite(db_conn, generated_api_module):
    """Test function returning SETOF a composite type."""
    summaries = await generated_api_module.get_item_summaries(db_conn)
    # Based on initial data:
    # Apple: 10 * 0.50 = 5.00
    # Banana: 25 * 0.30 = 7.50
    # Chair: 2 * 55.99 = 111.98
    assert len(summaries) == 3
    # Assuming a dataclass/Pydantic model 'ItemSummary' with fields 'item_name' and 'total_value'
    summary_map = {s.item_name: s.total_value for s in summaries}
    assert summary_map["Apple"] == Decimal("5.00")
    assert summary_map["Banana"] == Decimal("7.50")
    assert summary_map["inactive Chair"] == Decimal("111.98") # Ensure NUMERIC precision handled

@pytest.mark.asyncio
@pytest.mark.xfail(reason="Handling of anonymous RECORD returns needs verification in generated code.")
async def test_get_item_name_and_mood_record(db_conn, generated_api_module):
    """Test function returning an anonymous RECORD."""
    record = await generated_api_module.get_item_name_and_mood(db_conn, item_id=1)
    # How is RECORD represented? Tuple? Generic object? NamedTuple?
    # This assertion will likely need adjustment based on sql2pyapi output.
    assert record is not None
    # Example: Assuming it returns a tuple
    # assert record == ("Apple", "happy")
    # Example: Assuming a generated NamedTuple or object with attributes
    # assert record.name == "Apple"
    # assert record.mood == "happy"
    pytest.fail("Need to inspect generated code for anonymous RECORD handling.")

@pytest.mark.asyncio
@pytest.mark.xfail(reason="Handling of SETOF anonymous RECORD returns needs verification.")
async def test_get_all_names_and_moods_setof_record(db_conn, generated_api_module):
    """Test function returning SETOF anonymous RECORD."""
    records = await generated_api_module.get_all_names_and_moods(db_conn)
    assert len(records) == 3
    # Similar to the single RECORD test, assertions depend on the generated structure.
    # Example: Assuming list of tuples
    # assert ("Apple", "happy") in records
    # assert ("Banana", "ok") in records
    # assert ("inactive Chair", "sad") in records
    pytest.fail("Need to inspect generated code for SETOF anonymous RECORD handling.")

@pytest.mark.asyncio
async def test_filter_items_by_optional_mood_with_value(db_conn, generated_api_module):
    """Test filtering items with an optional enum parameter when a value is provided."""
    # Use 'happy' mood to filter
    items = await generated_api_module.filter_items_by_optional_mood(db_conn, mood=generated_api_module.Mood.HAPPY)
    
    # Should only get the 'Apple' item which has 'happy' mood
    assert len(items) == 1
    assert items[0].name == 'Apple'
    assert items[0].current_mood == generated_api_module.Mood.HAPPY

@pytest.mark.asyncio
async def test_filter_items_by_optional_mood_with_none(db_conn, generated_api_module):
    """Test filtering items with an optional enum parameter when None is provided."""
    # Pass None to get all items
    items = await generated_api_module.filter_items_by_optional_mood(db_conn, mood=None)
    
    # Should get all items (3 from setup)
    assert len(items) == 3
    
    # Verify we got all the different moods
    moods = [item.current_mood for item in items]
    assert generated_api_module.Mood.HAPPY in moods
    assert generated_api_module.Mood.OK in moods
    assert generated_api_module.Mood.SAD in moods

@pytest.mark.asyncio
async def test_get_default_mood_enum_return(db_conn, generated_api_module):
    """Test function that returns an enum value."""
    # Get the default mood
    mood = await generated_api_module.get_default_mood(db_conn)
    
    # Should be 'happy' as defined in the function
    assert mood == generated_api_module.Mood.HAPPY
    assert isinstance(mood, generated_api_module.Mood)

# End of file marker if necessary 