-- Create an ENUM type for company roles for better type safety
CREATE TYPE company_role AS ENUM ('owner', 'admin', 'member');

-- Function that uses the enum type as a parameter
CREATE OR REPLACE FUNCTION get_role_description(role company_role)
RETURNS text
LANGUAGE sql
AS $$
    SELECT CASE
        WHEN role = 'owner' THEN 'Company owner with full access'
        WHEN role = 'admin' THEN 'Administrator with management rights'
        WHEN role = 'member' THEN 'Regular team member'
        ELSE 'Unknown role'
    END;
$$;

-- Function that returns the enum type
CREATE OR REPLACE FUNCTION get_default_role()
RETURNS company_role
LANGUAGE sql
AS $$
    SELECT 'member'::company_role;
$$;

-- Function that returns a table with an enum column
CREATE OR REPLACE FUNCTION get_user_roles()
RETURNS TABLE(user_id int, role company_role)
LANGUAGE sql
AS $$
    SELECT 1, 'owner'::company_role
    UNION ALL
    SELECT 2, 'admin'::company_role
    UNION ALL
    SELECT 3, 'member'::company_role;
$$;
