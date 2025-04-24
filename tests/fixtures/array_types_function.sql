-- Returns an array of integers
CREATE OR REPLACE FUNCTION get_item_ids()
RETURNS integer[]
LANGUAGE sql
AS $$
    SELECT array_agg(id) FROM items;
$$;

-- Takes and returns an array of text
CREATE OR REPLACE FUNCTION process_tags(p_tags text[])
RETURNS text[]
LANGUAGE sql
AS $$
    SELECT array_append(p_tags, 'processed');
$$; 