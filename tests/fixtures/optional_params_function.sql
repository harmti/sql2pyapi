-- Search for items with optional filters
CREATE OR REPLACE FUNCTION search_items(
    p_query text,
    p_limit integer DEFAULT 10,
    p_include_unavailable boolean DEFAULT false
)
RETURNS SETOF items -- Assuming 'items' table schema is available/parsed elsewhere
LANGUAGE sql
AS $$
    SELECT * FROM items
    WHERE name ILIKE ('%' || p_query || '%')
      AND (p_include_unavailable OR is_available = true)
    LIMIT p_limit;
$$; 