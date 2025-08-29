-- Functions for enum regression test

-- Main test case: function that returns enum (the bug case)
CREATE OR REPLACE FUNCTION get_user_role(p_user_id UUID)
RETURNS user_role_enum
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN (SELECT role FROM enum_users WHERE id = p_user_id);
END;
$$;

-- THE REAL BUG: Function returning composite type (table row) with enum field
CREATE OR REPLACE FUNCTION get_user_by_id(p_user_id UUID)
RETURNS enum_users
LANGUAGE plpgsql
AS $$
DECLARE
    result enum_users;
BEGIN
    SELECT * INTO result FROM enum_users WHERE id = p_user_id;
    RETURN result;
END;
$$;

-- Function returning SETOF composite type with enum field
CREATE OR REPLACE FUNCTION get_all_users()
RETURNS SETOF enum_users
LANGUAGE sql
AS $$
    SELECT * FROM enum_users ORDER BY name;
$$;

-- Simple function returning constant enum for testing
CREATE OR REPLACE FUNCTION get_admin_role()
RETURNS user_role_enum
LANGUAGE sql
AS $$
    SELECT 'admin'::user_role_enum;
$$;

-- Function returning enum from parameter
CREATE OR REPLACE FUNCTION echo_user_role(p_role user_role_enum)
RETURNS user_role_enum
LANGUAGE sql
AS $$
    SELECT p_role;
$$;


-- Function to create a company invitation
CREATE OR REPLACE FUNCTION create_company_invitation(
    p_company_id UUID,
    p_email TEXT,
    p_role company_role
)
RETURNS company_invitations
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
    new_invitation company_invitations;
BEGIN
    -- Create the invitation
    INSERT INTO company_invitations (
        company_id,
        email,
        role
    )
    VALUES (
        p_company_id,
        p_email,
        p_role
    )
    RETURNING * INTO new_invitation;

    RETURN new_invitation;
END;
$$;
