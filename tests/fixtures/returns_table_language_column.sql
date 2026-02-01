-- Returns table with a column named 'language' which conflicts with LANGUAGE keyword
CREATE OR REPLACE FUNCTION get_collections(p_user_id uuid)
RETURNS TABLE(id uuid, name text, language text, status text)
LANGUAGE sql
AS $$
    SELECT id, name, language, status
    FROM collections
    WHERE user_id = p_user_id;
$$;
