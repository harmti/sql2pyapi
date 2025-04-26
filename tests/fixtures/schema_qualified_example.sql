-- Schema-qualified table definition
CREATE TABLE public.users (
    id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
    clerk_id VARCHAR(255) UNIQUE NOT NULL,
    email TEXT UNIQUE,
    email_verified BOOLEAN DEFAULT FALSE,
    first_name TEXT,
    last_name TEXT,
    last_sign_in_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT now() NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT now() NOT NULL,
    is_deleted BOOLEAN DEFAULT FALSE
);

-- Function returning a schema-qualified table
CREATE OR REPLACE FUNCTION get_user_by_clerk_id(p_clerk_id TEXT)
RETURNS SETOF public.users AS $$
BEGIN
    RETURN QUERY
    SELECT * FROM public.users WHERE clerk_id = p_clerk_id AND is_deleted = FALSE;
END;
$$ LANGUAGE plpgsql;
