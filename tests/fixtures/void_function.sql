-- A function that does something but returns nothing
CREATE OR REPLACE FUNCTION do_something(p_item_id integer)
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    -- Some action here, maybe logging or updating
    UPDATE items SET processed = true WHERE id = p_item_id;
END;
$$; 