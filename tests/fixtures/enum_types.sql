-- SQL file with ENUM type definitions and functions using them

-- Create an ENUM type for status
CREATE TYPE status_type AS ENUM (
    'pending',
    'active',
    'inactive',
    'deleted'
);

-- Create an ENUM type for user roles
CREATE TYPE user_role AS ENUM (
    'admin',
    'moderator',
    'user',
    'guest'
);

-- Function that returns an ENUM type
CREATE OR REPLACE FUNCTION get_default_status()
RETURNS status_type
LANGUAGE sql
AS $$
    SELECT 'active'::status_type;
$$;

-- Function that takes an ENUM parameter
CREATE OR REPLACE FUNCTION is_active_role(p_role user_role)
RETURNS boolean
LANGUAGE sql
AS $$
    SELECT p_role IN ('admin', 'moderator');
$$;

-- Function that returns a table with ENUM columns
CREATE OR REPLACE FUNCTION get_users_by_status(p_status status_type)
RETURNS TABLE (
    user_id integer,
    username text,
    status status_type,
    role user_role
)
LANGUAGE sql
AS $$
    SELECT 1, 'admin_user', 'active'::status_type, 'admin'::user_role
    UNION ALL
    SELECT 2, 'mod_user', 'active'::status_type, 'moderator'::user_role
    UNION ALL
    SELECT 3, 'regular_user', p_status, 'user'::user_role;
$$;
