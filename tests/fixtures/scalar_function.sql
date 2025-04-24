-- Returns a simple count
CREATE OR REPLACE FUNCTION get_item_count()
RETURNS integer
LANGUAGE sql
AS $$
    SELECT count(*)::integer FROM items;
$$;

-- Returns text, potentially null
CREATE OR REPLACE FUNCTION get_item_name(p_id int)
RETURNS text
LANGUAGE sql
AS $$
    SELECT name FROM items WHERE id = p_id;
$$; 