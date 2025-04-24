-- Returns a list of item IDs for a given category
CREATE OR REPLACE FUNCTION get_item_ids_by_category(p_category_name text)
RETURNS SETOF integer
LANGUAGE sql
AS $$
    SELECT id FROM items WHERE category = p_category_name;
$$; 