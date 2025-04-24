-- Returns an anonymous record containing status and count
CREATE OR REPLACE FUNCTION get_processing_status()
RETURNS record
LANGUAGE plpgsql
AS $$
DECLARE
    status_record record;
BEGIN
    SELECT 'processed'::text, count(*)::bigint INTO status_record
    FROM items WHERE processed = true;
    RETURN status_record;
END;
$$;

-- Returns a setof anonymous records
CREATE OR REPLACE FUNCTION get_all_statuses()
RETURNS SETOF record
LANGUAGE plpgsql
AS $$
DECLARE
    status_record record;
BEGIN
    FOR status_record IN
        SELECT status::text, count(*)::bigint
        FROM processing_log GROUP BY status
    LOOP
        RETURN NEXT status_record;
    END LOOP;
    RETURN;
END;
$$;
-- End of file 