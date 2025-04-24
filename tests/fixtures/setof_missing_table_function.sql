-- Returns a setof some_undefined_table records
-- The schema for 'some_undefined_table' is intentionally missing.
CREATE OR REPLACE FUNCTION get_undefined_table_data()
RETURNS SETOF some_undefined_table
LANGUAGE sql
AS $$
    -- This is just an example, the actual query doesn't matter
    -- as long as the return type signature is parsed.
    SELECT id, name FROM some_table_that_might_exist;
$$; 