-- Function to create a new user or return existing if clerk_id matches
CREATE OR REPLACE FUNCTION create_user(
    p_clerk_id TEXT,
    p_email TEXT DEFAULT NULL,
    p_email_verified BOOLEAN DEFAULT FALSE,
    p_first_name TEXT DEFAULT NULL,
    p_last_name TEXT DEFAULT NULL
)
RETURNS user_identity AS $$
DECLARE
    result_identity user_identity; -- Variable to hold the result
BEGIN
    INSERT INTO users (clerk_id, email, email_verified, first_name, last_name, updated_at)
    VALUES (p_clerk_id, p_email, p_email_verified, p_first_name, p_last_name, now())
    -- Specify the constraint name explicitly to avoid ambiguity
    ON CONFLICT ON CONSTRAINT users_clerk_id_key DO NOTHING
    -- Return the relevant columns into the variable
    RETURNING users.id, users.clerk_id::TEXT INTO result_identity;

    -- If INSERT did not happen due to conflict (result_identity is NULL),
    -- select the existing user's info into the variable
    IF result_identity IS NULL THEN
        SELECT u.id, u.clerk_id::TEXT
        INTO result_identity
        FROM users u WHERE u.clerk_id = p_clerk_id;
    END IF;

    -- Return the single result record
    RETURN result_identity;
END;
$$ LANGUAGE plpgsql;

-- Function to get a user by their clerk_id
CREATE OR REPLACE FUNCTION get_user_by_clerk_id(p_clerk_id TEXT)
RETURNS users AS $$
DECLARE
    result users;
BEGIN
    SELECT * INTO result FROM users WHERE clerk_id = p_clerk_id AND is_deleted = FALSE LIMIT 1;
    RETURN result;
END;
$$ LANGUAGE plpgsql;

-- Function to get a user by their internal id
CREATE OR REPLACE FUNCTION get_user_by_id(p_id UUID)
RETURNS users AS $$
DECLARE
    result users;
BEGIN
    SELECT * INTO result FROM users WHERE id = p_id AND is_deleted = FALSE LIMIT 1;
    RETURN result;
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
RETURNS users AS $$
DECLARE
    updated_user users;
BEGIN
    -- First update and store the result
    UPDATE users
    SET
        email = COALESCE(p_email, email),
        email_verified = COALESCE(p_email_verified, email_verified),
        first_name = COALESCE(p_first_name, first_name),
        last_name = COALESCE(p_last_name, last_name),
        last_sign_in_at = COALESCE(p_last_sign_in_at, last_sign_in_at),
        updated_at = now()
    WHERE id = p_id AND is_deleted = FALSE
    RETURNING * INTO updated_user;
    
    -- Check if a row was updated
    IF updated_user.id IS NULL THEN
        RAISE EXCEPTION 'User with ID % not found or already deleted.', p_id;
    END IF;
    
    -- Return the updated user
    RETURN updated_user;
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
    
    -- Raise an exception if no user was deleted
    IF deleted_id IS NULL THEN
        RAISE EXCEPTION 'User with ID % not found or already deleted.', p_id;
    END IF;
    
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
    WHERE id = p_company_id
    LIMIT 1;
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
-- Functions for managing company locations

-- Function to create a new location for a company
CREATE OR REPLACE FUNCTION create_location(
    p_company_id UUID,
    p_name TEXT,
    p_user_id UUID, -- User creating the location
    p_address TEXT DEFAULT NULL,
    p_city TEXT DEFAULT NULL,
    p_state TEXT DEFAULT NULL,
    p_country TEXT DEFAULT NULL,
    p_zip_code TEXT DEFAULT NULL,
    p_latitude DOUBLE PRECISION DEFAULT NULL,
    p_longitude DOUBLE PRECISION DEFAULT NULL,
    p_location_type TEXT DEFAULT NULL
)
RETURNS locations
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
    new_location locations;
BEGIN
    -- Check if the company exists
    IF NOT EXISTS (SELECT 1 FROM companies WHERE id = p_company_id) THEN
        RAISE EXCEPTION 'Company with ID % not found.', p_company_id;
    END IF;

    INSERT INTO locations (
        company_id,
        name,
        created_by_user_id,
        address,
        city,
        state,
        country,
        zip_code,
        latitude,
        longitude,
        location_type
    )
    VALUES (
        p_company_id,
        p_name,
        p_user_id,
        p_address,
        p_city,
        p_state,
        p_country,
        p_zip_code,
        p_latitude,
        p_longitude,
        p_location_type
    )
    RETURNING * INTO new_location;

    RETURN new_location;
END;
$$;

COMMENT ON FUNCTION create_location(UUID, TEXT, UUID, TEXT, TEXT, TEXT, TEXT, TEXT, DOUBLE PRECISION, DOUBLE PRECISION, TEXT) IS 'Creates a new location for a company.';

-- Function to get a specific location by ID
CREATE OR REPLACE FUNCTION get_location_by_id(
    p_location_id UUID
)
RETURNS locations
LANGUAGE sql
STABLE
SECURITY DEFINER
AS $$
    SELECT *
    FROM locations
    WHERE id = p_location_id
    LIMIT 1;
$$;

COMMENT ON FUNCTION get_location_by_id(UUID) IS 'Retrieves a location by its unique ID.';

-- Function to list all locations for a specific company
CREATE OR REPLACE FUNCTION list_company_locations(
    p_company_id UUID
)
RETURNS SETOF locations
LANGUAGE sql
STABLE
SECURITY DEFINER
AS $$
    SELECT *
    FROM locations
    WHERE company_id = p_company_id
    ORDER BY name ASC;
$$;

COMMENT ON FUNCTION list_company_locations(UUID) IS 'Lists all locations for a specific company, ordered by name.';

-- Function to update an existing location
CREATE OR REPLACE FUNCTION update_location(
    p_location_id UUID,
    p_user_id UUID, -- The ID of the user attempting the update
    p_name TEXT DEFAULT NULL,
    p_address TEXT DEFAULT NULL,
    p_city TEXT DEFAULT NULL,
    p_state TEXT DEFAULT NULL,
    p_country TEXT DEFAULT NULL,
    p_zip_code TEXT DEFAULT NULL,
    p_latitude DOUBLE PRECISION DEFAULT NULL,
    p_longitude DOUBLE PRECISION DEFAULT NULL,
    p_location_type TEXT DEFAULT NULL
)
RETURNS locations
LANGUAGE plpgsql
AS $$
DECLARE
    updated_location locations;
BEGIN
    -- Update the location and store the result
    UPDATE locations
    SET
        name = COALESCE(p_name, name),
        address = COALESCE(p_address, address),
        city = COALESCE(p_city, city),
        state = COALESCE(p_state, state),
        country = COALESCE(p_country, country),
        zip_code = COALESCE(p_zip_code, zip_code),
        latitude = COALESCE(p_latitude, latitude),
        longitude = COALESCE(p_longitude, longitude),
        location_type = COALESCE(p_location_type, location_type),
        updated_at = now()
    WHERE id = p_location_id
    RETURNING * INTO updated_location;

    -- Check if the update actually happened (if RETURNING didn't return rows)
    IF updated_location.id IS NULL THEN
        RAISE EXCEPTION 'Location with ID % not found, update failed.', p_location_id;
    END IF;

    RETURN updated_location;
END;
$$;

COMMENT ON FUNCTION update_location(UUID, UUID, TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, DOUBLE PRECISION, DOUBLE PRECISION, TEXT) IS 'Updates a location record. Fields not provided are left unchanged.';

-- Function to delete a location
CREATE OR REPLACE FUNCTION delete_location(
    p_location_id UUID,
    p_user_id UUID -- The ID of the user attempting the deletion
)
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
    deleted_count INTEGER;
BEGIN
    -- Delete and check if a row was actually deleted
    WITH rows AS (
        DELETE FROM locations
        WHERE id = p_location_id
        RETURNING 1
    )
    SELECT count(*) INTO deleted_count FROM rows;

    -- Raise an error if the location didn't exist to be deleted
    IF deleted_count = 0 THEN
        RAISE EXCEPTION 'Location with ID % not found, deletion failed.', p_location_id;
    END IF;
END;
$$;

COMMENT ON FUNCTION delete_location(UUID, UUID) IS 'Deletes a location record.';
-- Functions for managing company members

-- Function to add a member to a company
CREATE OR REPLACE FUNCTION add_company_member(
    p_company_id UUID,
    p_user_id UUID,
    p_invited_by_user_id UUID,
    p_role company_role DEFAULT 'member',
    p_status TEXT DEFAULT 'pending'
)
RETURNS company_members
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
    new_member company_members;
    existing_member company_members;
BEGIN
    -- Check if the company exists
    IF NOT EXISTS (SELECT 1 FROM companies WHERE id = p_company_id) THEN
        RAISE EXCEPTION 'Company with ID % not found.', p_company_id;
    END IF;

    -- Check if the user exists
    IF NOT EXISTS (SELECT 1 FROM users WHERE id = p_user_id AND is_deleted = FALSE) THEN
        RAISE EXCEPTION 'User with ID % not found or is deleted.', p_user_id;
    END IF;

    -- Check if the user is already a member of the company
    IF EXISTS (
        SELECT 1 FROM company_members 
        WHERE company_id = p_company_id 
        AND user_id = p_user_id
        AND status = 'active'
    ) THEN
        RAISE EXCEPTION 'User is already a member of this company.';
    END IF;
    
    -- Add the user as a member
    INSERT INTO company_members (
        company_id, 
        user_id, 
        role, 
        status, 
        invited_by_user_id,
        invited_at,
        joined_at
    )
    VALUES (
        p_company_id, 
        p_user_id, 
        p_role, 
        p_status,
        p_invited_by_user_id,
        now(),
        CASE WHEN p_status = 'active' THEN now() ELSE NULL END
    )
    RETURNING * INTO new_member;

    RETURN new_member;
END;
$$;

COMMENT ON FUNCTION add_company_member(UUID, UUID, UUID, company_role, TEXT) IS 'Adds a user as a direct member to a company with the specified role and status.';

-- Function to get a specific membership by ID
CREATE OR REPLACE FUNCTION get_company_member_by_id(
    p_member_id UUID
)
RETURNS company_members
LANGUAGE sql
STABLE
SECURITY DEFINER
AS $$
    SELECT *
    FROM company_members
    WHERE id = p_member_id
    LIMIT 1;
$$;

COMMENT ON FUNCTION get_company_member_by_id(UUID) IS 'Retrieves a company membership by its unique ID.';

-- Function to get a specific user's membership in a company
CREATE OR REPLACE FUNCTION get_company_membership(
    p_company_id UUID,
    p_user_id UUID
)
RETURNS company_members
LANGUAGE sql
STABLE
SECURITY DEFINER
AS $$
    SELECT *
    FROM company_members
    WHERE company_id = p_company_id AND user_id = p_user_id
    LIMIT 1;
$$;

COMMENT ON FUNCTION get_company_membership(UUID, UUID) IS 'Retrieves a user''s membership in a specific company.';

-- Function to list all members of a company
CREATE OR REPLACE FUNCTION list_company_members(
    p_company_id UUID,
    p_status TEXT DEFAULT NULL
)
RETURNS SETOF company_members
LANGUAGE sql
STABLE
SECURITY DEFINER
AS $$
    SELECT cm.*
    FROM company_members cm
    WHERE cm.company_id = p_company_id
    AND (p_status IS NULL OR cm.status = p_status)
    ORDER BY 
        CASE WHEN cm.role = 'owner' THEN 0
             WHEN cm.role = 'admin' THEN 1
             ELSE 2
        END,
        cm.joined_at;
$$;

COMMENT ON FUNCTION list_company_members(UUID, TEXT) IS 'Lists all members of a company, optionally filtered by status and ordered by role and join date.';

-- Function to list all companies a user is a member of
CREATE OR REPLACE FUNCTION list_user_memberships(
    p_user_id UUID,
    p_status TEXT DEFAULT NULL
)
RETURNS TABLE (
    membership_id UUID,
    company_id UUID,
    company_name TEXT,
    role TEXT,
    status TEXT,
    joined_at TIMESTAMP WITH TIME ZONE
)
LANGUAGE sql
STABLE
SECURITY DEFINER
AS $$
    SELECT 
        cm.id AS membership_id,
        c.id AS company_id,
        c.name AS company_name,
        cm.role,
        cm.status,
        cm.joined_at
    FROM company_members cm
    JOIN companies c ON cm.company_id = c.id
    WHERE cm.user_id = p_user_id
    AND (p_status IS NULL OR cm.status = p_status)
    ORDER BY cm.joined_at DESC;
$$;

COMMENT ON FUNCTION list_user_memberships(UUID, TEXT) IS 'Lists all companies a user is a member of, optionally filtered by status.';

-- Function to update a company membership
CREATE OR REPLACE FUNCTION update_company_membership(
    p_member_id UUID,
    p_role company_role DEFAULT NULL,
    p_status TEXT DEFAULT NULL,
    p_joined_at TIMESTAMP WITH TIME ZONE DEFAULT NULL
)
RETURNS company_members
LANGUAGE plpgsql
AS $$
DECLARE
    updated_member company_members;
BEGIN
    -- Update the membership and store the result
    UPDATE company_members
    SET
        role = COALESCE(p_role, role),
        status = COALESCE(p_status, status),
        joined_at = COALESCE(p_joined_at, joined_at),
        updated_at = now()
    WHERE id = p_member_id
    RETURNING * INTO updated_member;

    -- Check if the update actually happened
    IF updated_member.id IS NULL THEN
        RAISE EXCEPTION 'Membership with ID % not found, update failed.', p_member_id;
    END IF;

    RETURN updated_member;
END;
$$;

COMMENT ON FUNCTION update_company_membership(UUID, company_role, TEXT, TIMESTAMP WITH TIME ZONE) IS 'Updates a company membership record. Fields not provided are left unchanged.';

-- Function to remove a member from a company
CREATE OR REPLACE FUNCTION remove_company_member(
    p_member_id UUID
)
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
    deleted_count INTEGER;
BEGIN
    -- Set the membership to inactive instead of deleting
    WITH rows AS (
        UPDATE company_members
        SET status = 'inactive', updated_at = now()
        WHERE id = p_member_id AND status != 'inactive'
        RETURNING 1
    )
    SELECT count(*) INTO deleted_count FROM rows;

    -- Raise an error if the membership didn't exist or was already inactive
    IF deleted_count = 0 THEN
        -- Check if the membership exists but is already inactive
        IF EXISTS (SELECT 1 FROM company_members WHERE id = p_member_id AND status = 'inactive') THEN
            RAISE EXCEPTION 'Membership with ID % is already inactive.', p_member_id;
        ELSE
            -- Membership doesn't exist
            RAISE EXCEPTION 'Membership with ID % not found.', p_member_id;
        END IF;
    END IF;
END;
$$;

COMMENT ON FUNCTION remove_company_member(UUID) IS 'Removes a member from a company by setting their status to inactive.';
-- func/350_company_invitations.sql

-- Function to create a company invitation
CREATE OR REPLACE FUNCTION create_company_invitation(
    p_company_id UUID,
    p_email TEXT,
    p_invited_by_user_id UUID,
    p_role company_role DEFAULT 'member',
    p_expires_in_days INTEGER DEFAULT 7
)
RETURNS company_invitations
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
    new_invitation company_invitations;
    new_token TEXT;
    expiration_date TIMESTAMP WITH TIME ZONE;
BEGIN
    -- Generate a secure random token
    new_token := encode(gen_random_bytes(24), 'hex');
    
    -- Calculate expiration date
    expiration_date := now() + (p_expires_in_days || ' days')::INTERVAL;
    
    -- Check if there's already a pending invitation for this email in this company
    IF EXISTS (
        SELECT 1 FROM company_invitations 
        WHERE company_id = p_company_id 
        AND email = p_email 
        AND status = 'pending'
    ) THEN
        RAISE EXCEPTION 'An invitation for % to join this company already exists.', p_email;
    END IF;
    
    -- Create the invitation
    INSERT INTO company_invitations (
        company_id, 
        email, 
        role, 
        token, 
        expires_at, 
        invited_by_user_id
    )
    VALUES (
        p_company_id, 
        p_email, 
        p_role, 
        new_token, 
        expiration_date, 
        p_invited_by_user_id
    )
    RETURNING * INTO new_invitation;

    RETURN new_invitation;
END;
$$;

COMMENT ON FUNCTION create_company_invitation(UUID, TEXT, UUID, company_role, INTEGER) IS 'Creates an invitation for a user to join a company with the specified role.';

-- Function to get a company invitation by ID
CREATE OR REPLACE FUNCTION get_company_invitation_by_id(
    p_invitation_id UUID
)
RETURNS company_invitations
LANGUAGE sql
STABLE
SECURITY DEFINER
AS $$
    SELECT * FROM company_invitations WHERE id = p_invitation_id LIMIT 1;
$$;

COMMENT ON FUNCTION get_company_invitation_by_id(UUID) IS 'Gets a company invitation by its ID.';

-- Function to get a company invitation by token
CREATE OR REPLACE FUNCTION get_company_invitation_by_token(
    p_token TEXT
)
RETURNS company_invitations
LANGUAGE sql
STABLE
SECURITY DEFINER
AS $$
    SELECT * FROM company_invitations WHERE token = p_token LIMIT 1;
$$;

COMMENT ON FUNCTION get_company_invitation_by_token(TEXT) IS 'Gets a company invitation by its token.';

-- Function to list all invitations for a company
CREATE OR REPLACE FUNCTION list_company_invitations(
    p_company_id UUID,
    p_status TEXT DEFAULT NULL
)
RETURNS SETOF company_invitations
LANGUAGE sql
STABLE
SECURITY DEFINER
AS $$
    SELECT * 
    FROM company_invitations 
    WHERE company_id = p_company_id
    AND (p_status IS NULL OR status = p_status)
    ORDER BY created_at DESC;
$$;

COMMENT ON FUNCTION list_company_invitations(UUID, TEXT) IS 'Lists all invitations for a company, optionally filtered by status.';

-- Function to list all invitations for an email address
CREATE OR REPLACE FUNCTION list_email_invitations(
    p_email TEXT,
    p_status TEXT DEFAULT 'pending'
)
RETURNS TABLE (
    invitation_id UUID,
    company_id UUID,
    company_name TEXT,
    role TEXT,
    token TEXT,
    expires_at TIMESTAMP WITH TIME ZONE,
    invited_by_user_id UUID,
    invited_by_name TEXT,
    created_at TIMESTAMP WITH TIME ZONE
)
LANGUAGE sql
STABLE
SECURITY DEFINER
AS $$
    SELECT 
        i.id AS invitation_id,
        i.company_id,
        c.name AS company_name,
        i.role,
        i.token,
        i.expires_at,
        i.invited_by_user_id,
        CONCAT(u.first_name, ' ', u.last_name) AS invited_by_name,
        i.created_at
    FROM company_invitations i
    JOIN companies c ON i.company_id = c.id
    JOIN users u ON i.invited_by_user_id = u.id
    WHERE i.email = p_email
    AND i.status = p_status
    AND i.expires_at > now()
    ORDER BY i.created_at DESC;
$$;

COMMENT ON FUNCTION list_email_invitations(TEXT, TEXT) IS 'Lists all invitations for an email address, optionally filtered by status.';

-- Function to accept a company invitation
CREATE OR REPLACE FUNCTION accept_company_invitation(
    p_token TEXT,
    p_user_id UUID
)
RETURNS company_members
LANGUAGE plpgsql
AS $$
DECLARE
    invitation company_invitations;
    new_member company_members;
BEGIN
    -- Find the invitation
    SELECT * INTO invitation 
    FROM company_invitations 
    WHERE token = p_token 
    AND status = 'pending' 
    AND expires_at > now();
    
    -- Check if invitation exists and is valid
    IF invitation.id IS NULL THEN
        RAISE EXCEPTION 'Invalid, expired, or already used invitation token.';
    END IF;
    
    -- Begin transaction to ensure atomicity
    BEGIN
        -- Create the company membership
        INSERT INTO company_members (
            company_id,
            user_id,
            role,
            status,
            invited_by_user_id,
            invited_at,
            joined_at
        )
        VALUES (
            invitation.company_id,
            p_user_id,
            invitation.role,
            'active',
            invitation.invited_by_user_id,
            invitation.created_at,
            now()
        )
        RETURNING * INTO new_member;
        
        -- Update the invitation status
        UPDATE company_invitations
        SET 
            status = 'accepted',
            updated_at = now()
        WHERE id = invitation.id;
        
        -- Return the new membership
        RETURN new_member;
    EXCEPTION WHEN OTHERS THEN
        -- If anything goes wrong, rollback and re-raise the exception
        RAISE;
    END;
END;
$$;

COMMENT ON FUNCTION accept_company_invitation(TEXT, UUID) IS 'Accepts a company invitation using its token and creates a company membership for the user.';

-- Function to reject a company invitation
CREATE OR REPLACE FUNCTION reject_company_invitation(
    p_token TEXT
)
RETURNS company_invitations
LANGUAGE plpgsql
AS $$
DECLARE
    updated_invitation company_invitations;
BEGIN
    -- Update the invitation status
    UPDATE company_invitations
    SET 
        status = 'rejected',
        updated_at = now()
    WHERE token = p_token
    AND status = 'pending'
    RETURNING * INTO updated_invitation;
    
    -- Check if invitation was found and updated
    IF updated_invitation.id IS NULL THEN
        RAISE EXCEPTION 'Invalid, expired, or already used invitation token.';
    END IF;
    
    RETURN updated_invitation;
END;
$$;

COMMENT ON FUNCTION reject_company_invitation(TEXT) IS 'Rejects a company invitation using its token.';

-- Function to cancel a company invitation
CREATE OR REPLACE FUNCTION cancel_company_invitation(
    p_invitation_id UUID
)
RETURNS company_invitations
LANGUAGE plpgsql
AS $$
DECLARE
    updated_invitation company_invitations;
BEGIN
    -- Update the invitation status
    UPDATE company_invitations
    SET 
        status = 'cancelled',
        updated_at = now()
    WHERE id = p_invitation_id
    AND status = 'pending'
    RETURNING * INTO updated_invitation;
    
    -- Check if invitation was found and updated
    IF updated_invitation.id IS NULL THEN
        -- Check if the invitation exists at all
        IF EXISTS (SELECT 1 FROM company_invitations WHERE id = p_invitation_id) THEN
            RAISE EXCEPTION 'Invitation cannot be cancelled because it is not in pending status.';
        ELSE
            RAISE EXCEPTION 'Invitation with ID % not found.', p_invitation_id;
        END IF;
    END IF;
    
    RETURN updated_invitation;
END;
$$;

COMMENT ON FUNCTION cancel_company_invitation(UUID) IS 'Cancels a pending company invitation.';

-- Function to resend a company invitation with a new token and expiration
CREATE OR REPLACE FUNCTION resend_company_invitation(
    p_invitation_id UUID,
    p_expires_in_days INTEGER DEFAULT 7
)
RETURNS company_invitations
LANGUAGE plpgsql
AS $$
DECLARE
    updated_invitation company_invitations;
    new_token TEXT;
    expiration_date TIMESTAMP WITH TIME ZONE;
BEGIN
    -- Generate a new secure random token
    new_token := encode(gen_random_bytes(24), 'hex');
    
    -- Calculate new expiration date
    expiration_date := now() + (p_expires_in_days || ' days')::INTERVAL;
    
    -- Update the invitation
    UPDATE company_invitations
    SET 
        token = new_token,
        expires_at = expiration_date,
        status = 'pending',
        updated_at = now()
    WHERE id = p_invitation_id
    AND (status = 'pending' OR status = 'expired')
    RETURNING * INTO updated_invitation;
    
    -- Check if invitation was found and updated
    IF updated_invitation.id IS NULL THEN
        -- Check if the invitation exists at all
        IF EXISTS (SELECT 1 FROM company_invitations WHERE id = p_invitation_id) THEN
            RAISE EXCEPTION 'Invitation cannot be resent because it is not in pending or expired status.';
        ELSE
            RAISE EXCEPTION 'Invitation with ID % not found.', p_invitation_id;
        END IF;
    END IF;
    
    RETURN updated_invitation;
END;
$$;

COMMENT ON FUNCTION resend_company_invitation(UUID, INTEGER) IS 'Resends a company invitation with a new token and expiration date.';

-- Function to expire outdated invitations
CREATE OR REPLACE FUNCTION expire_outdated_invitations()
RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE
    expired_count INTEGER;
BEGIN
    -- Update all pending invitations that have expired
    UPDATE company_invitations
    SET 
        status = 'expired',
        updated_at = now()
    WHERE status = 'pending'
    AND expires_at < now();
    
    -- Get the count of expired invitations
    GET DIAGNOSTICS expired_count = ROW_COUNT;
    
    RETURN expired_count;
END;
$$;

COMMENT ON FUNCTION expire_outdated_invitations() IS 'Marks all pending invitations that have passed their expiration date as expired.';
