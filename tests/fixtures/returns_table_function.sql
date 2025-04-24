-- Returns a user's basic info as a table
CREATE OR REPLACE FUNCTION get_user_basic_info(p_user_id uuid)
RETURNS TABLE(user_id uuid, first_name text, is_active boolean)
LANGUAGE sql
AS $$
    SELECT id, first_name, not is_deleted
    FROM users
    WHERE id = p_user_id;
$$; 