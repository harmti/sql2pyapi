-- Adds an item with various attributes
CREATE OR REPLACE FUNCTION add_item(
    p_name text,
    p_category_id integer,
    p_is_available boolean,
    p_price numeric,
    p_attributes jsonb
)
RETURNS uuid
LANGUAGE sql
AS $$
    INSERT INTO items (name, category_id, is_available, price, attributes)
    VALUES (p_name, p_category_id, p_is_available, p_price, p_attributes)
    RETURNING id;
$$; 