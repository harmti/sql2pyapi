-- Define an enum type
CREATE TYPE company_role AS ENUM (
    'member',
    'admin',
    'owner'
);

-- Function taking an enum parameter
CREATE OR REPLACE FUNCTION add_company_member(
    p_company_id UUID,
    p_user_id UUID,
    p_role company_role
) RETURNS UUID AS $$
DECLARE
    v_member_id UUID;
BEGIN
    -- Insert logic would go here
    v_member_id := gen_random_uuid();
    RETURN v_member_id;
END;
$$ LANGUAGE plpgsql;

-- Function returning an enum
CREATE OR REPLACE FUNCTION get_user_role(
    p_company_id UUID,
    p_user_id UUID
) RETURNS company_role AS $$
BEGIN
    -- Query logic would go here
    RETURN 'member'::company_role;
END;
$$ LANGUAGE plpgsql;

-- Function with a table result containing an enum
CREATE OR REPLACE FUNCTION get_company_member(
    p_company_id UUID,
    p_user_id UUID
) RETURNS TABLE (
    id UUID,
    user_id UUID,
    company_id UUID,
    role company_role
) AS $$
BEGIN
    -- Query logic would go here
    RETURN QUERY SELECT 
        gen_random_uuid() as id,
        p_user_id as user_id,
        p_company_id as company_id,
        'member'::company_role as role;
END;
$$ LANGUAGE plpgsql;

-- Function returning a list of table results containing an enum
CREATE OR REPLACE FUNCTION list_company_members(
    p_company_id UUID
) RETURNS SETOF TABLE (
    id UUID,
    user_id UUID,
    company_id UUID,
    role company_role
) AS $$
BEGIN
    -- Query logic would go here
    RETURN QUERY SELECT 
        gen_random_uuid() as id,
        gen_random_uuid() as user_id,
        p_company_id as company_id,
        'member'::company_role as role;
    
    RETURN QUERY SELECT 
        gen_random_uuid() as id,
        gen_random_uuid() as user_id,
        p_company_id as company_id,
        'admin'::company_role as role;
END;
$$ LANGUAGE plpgsql; 