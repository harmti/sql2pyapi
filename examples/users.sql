-- Example SQL functions for testing

CREATE FUNCTION get_user_by_id(user_id UUID)
RETURNS TABLE (id UUID, name TEXT, email TEXT, created_at TIMESTAMP)
LANGUAGE SQL
STABLE -- Good practice to mark functions appropriately
AS $$
  SELECT id, name, email, created_at
  FROM users -- Assumes a 'users' table exists
  WHERE id = $1;
$$;

CREATE FUNCTION insert_user(p_name TEXT, p_email TEXT)
RETURNS UUID
LANGUAGE SQL
VOLATILE -- Good practice
AS $$
  INSERT INTO users (name, email)
  VALUES (p_name, p_email)
  RETURNING id;
$$;

-- Example function returning a scalar list (might not be directly supported yet)
-- CREATE FUNCTION get_all_user_ids()
-- RETURNS SETOF UUID
-- LANGUAGE SQL
-- STABLE
-- AS $$
--   SELECT id FROM users;
-- $$;

CREATE FUNCTION get_users_with_name_like(name_pattern TEXT)
RETURNS TABLE (id UUID, name TEXT, email TEXT)
LANGUAGE SQL
STABLE
AS $$
    SELECT id, name, email
    FROM users
    WHERE name LIKE $1;
$$; 