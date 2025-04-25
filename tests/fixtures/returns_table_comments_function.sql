CREATE OR REPLACE FUNCTION function_with_returns_table_comments(p_filter TEXT)
RETURNS TABLE (
    col_id UUID, -- The result ID
    col_value TEXT, -- The result value
    -- Another comment
    col_status BOOLEAN -- The status flag
)
LANGUAGE sql
AS $$
    SELECT gen_random_uuid(), 'test', true WHERE p_filter IS NOT NULL;
$$; 