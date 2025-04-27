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
    -- Specify the constraint name explicitly to avoid ambiguity
    ON CONFLICT ON CONSTRAINT users_clerk_id_key DO NOTHING
    -- Cast clerk_id to TEXT to match the function's RETURN TABLE definition
    RETURNING users.id, users.clerk_id::TEXT;

    -- If INSERT did not happen due to conflict, return the existing user
    IF NOT FOUND THEN
        -- Also cast here for consistency, although it might not be strictly necessary
        -- depending on how PostgreSQL handles variable assignment vs. RETURN QUERY
        RETURN QUERY SELECT u.id, u.clerk_id::TEXT FROM users u WHERE u.clerk_id = p_clerk_id;
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
$$ LANGUAGE plpgsql;-- Functions for managing companies

-- Function to create a new company
CREATE OR REPLACE FUNCTION create_company(
    p_name TEXT,
    p_industry TEXT,
    p_size TEXT,
    p_primary_address TEXT,
    p_user_id UUID
)
RETURNS companies
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
    new_company companies;
BEGIN
    INSERT INTO companies (name, industry, size, primary_address, created_by_user_id)
    VALUES (p_name, p_industry, p_size, p_primary_address, p_user_id)
    RETURNING * INTO new_company;

    RETURN new_company;
END;
$$;

COMMENT ON FUNCTION create_company(TEXT, TEXT, TEXT, TEXT, UUID) IS 'Creates a new company record associated with the provided user ID.';

-- Function to get a specific company by ID
-- Note: This basic version doesn't enforce specific user access rules beyond existence.
-- You might want to add checks here later (e.g., user is creator, user is part of an org associated with the company).
CREATE OR REPLACE FUNCTION get_company_by_id(
    p_company_id UUID
)
RETURNS companies
LANGUAGE sql
STABLE
SECURITY DEFINER
AS $$
    SELECT *
    FROM companies
    WHERE id = p_company_id;
$$;

COMMENT ON FUNCTION get_company_by_id(UUID) IS 'Retrieves a company by its unique ID.';

-- Function to list companies created by a specific user
CREATE OR REPLACE FUNCTION list_user_companies(
    p_user_id UUID
)
RETURNS SETOF companies
LANGUAGE sql
STABLE
SECURITY DEFINER
AS $$
    SELECT *
    FROM companies
    WHERE created_by_user_id = p_user_id
    ORDER BY created_at DESC;
$$;

COMMENT ON FUNCTION list_user_companies(UUID) IS 'Lists all companies created by a specific user, ordered by creation date.';

-- Function to update an existing company
CREATE OR REPLACE FUNCTION update_company(
    p_company_id UUID,
    -- The ID of the user attempting the update (currently unused for auth)
    p_user_id UUID,
    p_name TEXT DEFAULT NULL,
    p_industry TEXT DEFAULT NULL,
    p_size TEXT DEFAULT NULL,
    p_primary_address TEXT DEFAULT NULL
)
RETURNS companies
LANGUAGE plpgsql
-- Removed SECURITY DEFINER as it's less critical without specific auth checks
AS $$
DECLARE
    updated_company companies;
BEGIN
    -- Check if the user is the creator (basic authorization) - REMOVED
    -- IF NOT EXISTS (
    --     SELECT 1
    --     FROM companies
    --     WHERE id = p_company_id AND created_by_user_id = p_user_id
    -- ) THEN
    --     RAISE EXCEPTION 'User % does not have permission to update company % or company does not exist.', p_user_id, p_company_id;
    -- END IF;

    UPDATE companies
    SET
        name = COALESCE(p_name, name),
        industry = COALESCE(p_industry, industry),
        size = COALESCE(p_size, size),
        primary_address = COALESCE(p_primary_address, primary_address),
        updated_at = now() -- Ensure updated_at is set
    WHERE id = p_company_id
    -- Note: We might want to add a check here to ensure the company exists 
    -- before updating, otherwise it silently does nothing if id doesn't match.
    -- Example: Add `AND EXISTS (SELECT 1 FROM companies WHERE id = p_company_id)`
    -- or check affected rows after UPDATE.
    RETURNING * INTO updated_company;

    -- Check if the update actually happened (if RETURNING didn't return rows)
    IF updated_company.id IS NULL THEN
         RAISE EXCEPTION 'Company with ID % not found, update failed.', p_company_id;
    END IF;

    RETURN updated_company;
END;
$$;

COMMENT ON FUNCTION update_company(UUID, UUID, TEXT, TEXT, TEXT, TEXT) IS 'Updates a company record. User ID currently unused for auth. Fields not provided are left unchanged.';


-- Function to delete a company
CREATE OR REPLACE FUNCTION delete_company(
    p_company_id UUID,
    p_user_id UUID -- The ID of the user attempting the deletion (currently unused for auth)
)
RETURNS VOID
LANGUAGE plpgsql
-- Removed SECURITY DEFINER as it's less critical without specific auth checks
AS $$
DECLARE
    deleted_count INTEGER;
BEGIN
    -- Delete and check if a row was actually deleted
    WITH rows AS (
        DELETE FROM companies
        WHERE id = p_company_id
        RETURNING 1
    )
    SELECT count(*) INTO deleted_count FROM rows;

    -- Raise an error if the company didn't exist to be deleted
    IF deleted_count = 0 THEN
        RAISE EXCEPTION 'Company with ID % not found, deletion failed.', p_company_id;
    END IF;

END;
$$;

COMMENT ON FUNCTION delete_company(UUID, UUID) IS 'Deletes a company record. User ID currently unused for auth.';
