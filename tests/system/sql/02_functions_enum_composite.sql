
-- Function returning a single composite type with an enum field
CREATE FUNCTION get_item_with_mood(p_item_id INTEGER)
RETURNS item_with_mood
AS $$
    SELECT id, name, current_mood FROM items WHERE id = p_item_id;
$$ LANGUAGE SQL STABLE;

-- Function returning SETOF a composite type with an enum field
CREATE FUNCTION get_all_items_with_mood()
RETURNS SETOF item_with_mood
AS $$
   SELECT id, name, current_mood FROM items ORDER BY id;
$$ LANGUAGE SQL STABLE;
