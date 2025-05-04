-- tests/system/sql/01_functions.sql

-- Function returning a single scalar value
CREATE FUNCTION get_item_count()
RETURNS BIGINT
AS $$
    SELECT count(*) FROM items;
$$ LANGUAGE SQL STABLE;

-- Function returning a single row matching a table structure
-- Note: The generated Python should use the 'items' table definition
-- to create a Pydantic/dataclass model.
CREATE FUNCTION get_item_by_id(p_item_id INTEGER)
RETURNS items -- Returns a single row matching the 'items' table
AS $$
    SELECT * FROM items WHERE id = p_item_id;
$$ LANGUAGE SQL STABLE;

-- Function returning SETOF scalar
CREATE FUNCTION get_all_item_names()
RETURNS SETOF TEXT
AS $$
    SELECT name FROM items ORDER BY name;
$$ LANGUAGE SQL STABLE;

-- Function returning SETOF rows matching a table structure
CREATE FUNCTION get_items_with_mood(p_mood mood)
RETURNS SETOF items
AS $$
    SELECT * FROM items WHERE current_mood = p_mood ORDER BY id;
$$ LANGUAGE SQL STABLE;

-- Function returning a TABLE definition
CREATE FUNCTION search_items(p_search_term TEXT)
RETURNS TABLE (
    item_id INTEGER,
    item_name TEXT,
    creation_date DATE -- Different date/time type
)
AS $$
    SELECT id, name, created_at::DATE
    FROM items
    WHERE name ILIKE '%' || p_search_term || '%' OR description ILIKE '%' || p_search_term || '%';
$$ LANGUAGE SQL STABLE;

-- Function returning SETOF a composite type
CREATE FUNCTION get_item_summaries()
RETURNS SETOF item_summary
AS $$
    SELECT name, quantity * price FROM items WHERE quantity IS NOT NULL AND price IS NOT NULL;
$$ LANGUAGE SQL STABLE;

-- Function with various parameter types
CREATE FUNCTION add_related_item(
    p_item_id INTEGER,
    p_notes TEXT,
    p_config JSON DEFAULT '{}',
    p_uuid UUID DEFAULT gen_random_uuid()
)
RETURNS UUID -- Return the UUID of the newly created related item
AS $$
DECLARE
    v_uuid UUID;
BEGIN
    INSERT INTO related_items (item_id, uuid_key, notes, config)
    VALUES (p_item_id, p_uuid, p_notes, p_config)
    RETURNING uuid_key INTO v_uuid;
    RETURN v_uuid;
END;
$$ LANGUAGE plpgsql;

-- Function returning VOID (procedure-like)
CREATE FUNCTION update_item_timestamp(p_item_id INTEGER)
RETURNS VOID
AS $$
    UPDATE items SET updated_at = CURRENT_TIMESTAMP WHERE id = p_item_id;
$$ LANGUAGE SQL;

-- Function returning nullable scalar
CREATE FUNCTION get_item_description(p_item_id INTEGER)
RETURNS TEXT -- Description is nullable
AS $$
    SELECT description FROM items WHERE id = p_item_id;
$$ LANGUAGE SQL STABLE;

-- Function returning anonymous RECORD (potentially harder to handle)
-- Let's see how sql2pyapi handles this. It might require explicit type hints
-- or might not be fully supported without a TABLE return.
CREATE FUNCTION get_item_name_and_mood(p_item_id INTEGER)
RETURNS RECORD
AS $$
    SELECT name, current_mood FROM items WHERE id = p_item_id;
$$ LANGUAGE SQL STABLE;

-- Function returning SETOF anonymous RECORD
CREATE FUNCTION get_all_names_and_moods()
RETURNS SETOF RECORD
AS $$
    SELECT name, current_mood FROM items ORDER BY id;
$$ LANGUAGE SQL STABLE; 