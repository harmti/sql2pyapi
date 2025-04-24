-- Returns the current database time
CREATE OR REPLACE FUNCTION get_current_db_time()
RETURNS timestamptz
LANGUAGE sql
AS $$
    SELECT now();
$$; 