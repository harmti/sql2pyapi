-- Function to create a new user or return existing if clerk_id matches
CREATE OR REPLACE FUNCTION create_user(
    p_clerk_id TEXT,
    p_email TEXT DEFAULT NULL,
    p_email_verified BOOLEAN DEFAULT FALSE,
    p_first_name TEXT DEFAULT NULL,
    p_last_name TEXT DEFAULT NULL
)
RETURNS TABLE(id UUID, clerk_id TEXT) AS $$
BEGIN
    RETURN QUERY
    INSERT INTO users (clerk_id, email, email_verified, first_name, last_name, updated_at)
    VALUES (p_clerk_id, p_email, p_email_verified, p_first_name, p_last_name, now())
    ON CONFLICT (clerk_id) DO NOTHING -- Or potentially DO UPDATE if desired
    RETURNING users.id, users.clerk_id;

    -- If INSERT did not happen due to conflict, return the existing user
    IF NOT FOUND THEN
        RETURN QUERY SELECT u.id, u.clerk_id FROM users u WHERE u.clerk_id = p_clerk_id;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Function to get a user by their clerk_id
CREATE OR REPLACE FUNCTION get_user_by_clerk_id(p_clerk_id TEXT)
RETURNS SETOF users AS $$
BEGIN
    RETURN QUERY
    SELECT * FROM users WHERE clerk_id = p_clerk_id AND is_deleted = FALSE;
END;
$$ LANGUAGE plpgsql;

-- Function to get a user by their internal id
CREATE OR REPLACE FUNCTION get_user_by_id(p_id UUID)
RETURNS SETOF users AS $$
BEGIN
    RETURN QUERY
    SELECT * FROM users WHERE id = p_id AND is_deleted = FALSE;
END;
$$ LANGUAGE plpgsql;

-- Function to get all non-deleted users
CREATE OR REPLACE FUNCTION get_users()
RETURNS SETOF users AS $$
BEGIN
    RETURN QUERY
    SELECT * FROM users WHERE is_deleted = FALSE ORDER BY created_at DESC;
END;
$$ LANGUAGE plpgsql;

-- Function to update user details
CREATE OR REPLACE FUNCTION update_user(
    p_id UUID,
    p_email TEXT DEFAULT NULL,
    p_email_verified BOOLEAN DEFAULT NULL,
    p_first_name TEXT DEFAULT NULL,
    p_last_name TEXT DEFAULT NULL,
    p_last_sign_in_at TIMESTAMPTZ DEFAULT NULL
)
RETURNS SETOF users AS $$
BEGIN
    RETURN QUERY
    UPDATE users
    SET
        email = COALESCE(p_email, email),
        email_verified = COALESCE(p_email_verified, email_verified),
        first_name = COALESCE(p_first_name, first_name),
        last_name = COALESCE(p_last_name, last_name),
        last_sign_in_at = COALESCE(p_last_sign_in_at, last_sign_in_at),
        updated_at = now()
    WHERE id = p_id AND is_deleted = FALSE
    RETURNING *;
END;
$$ LANGUAGE plpgsql;

-- Function to mark a user as deleted
CREATE OR REPLACE FUNCTION delete_user(p_id UUID)
RETURNS UUID AS $$
DECLARE
    deleted_id UUID;
BEGIN
    UPDATE users
    SET is_deleted = TRUE, updated_at = now()
    WHERE id = p_id AND is_deleted = FALSE
    RETURNING users.id INTO deleted_id;
    RETURN deleted_id;
END;
$$ LANGUAGE plpgsql;