-- Function with comments in parameters
CREATE OR REPLACE FUNCTION function_with_param_comments(
    p_id UUID, -- The unique identifier
    p_name TEXT, -- The name value
    -- Some description for the next parameter
    p_age INTEGER DEFAULT 30, -- The age, defaults to 30
    p_active BOOLEAN -- Active status
)
RETURNS void
LANGUAGE sql
AS $$ SELECT; $$; 