-- Define a custom composite type
CREATE TYPE user_identity AS (
    user_id UUID,
    clerk_id TEXT,
    is_active BOOLEAN
);

-- Function returning the custom composite type
-- Assume this gets data from somewhere, exact logic doesn't matter for parsing
CREATE OR REPLACE FUNCTION get_user_identity_by_clerk_id(p_clerk_id TEXT)
RETURNS user_identity -- Returns the single composite type instance
LANGUAGE plpgsql
AS $$
DECLARE
    result_identity user_identity;
BEGIN
    -- Simulate fetching or creating the data
    SELECT gen_random_uuid(), p_clerk_id, true 
    INTO result_identity.user_id, result_identity.clerk_id, result_identity.is_active;
    
    -- Return the populated composite type variable
    RETURN result_identity; 
END;
$$;

-- Function returning SETOF the custom composite type
CREATE OR REPLACE FUNCTION get_all_active_identities()
RETURNS SETOF user_identity -- Returns multiple composite type instances
LANGUAGE sql
AS $$
    SELECT id, clerk_id, true FROM users WHERE NOT is_deleted LIMIT 5; -- Example query
$$; 