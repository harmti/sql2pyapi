-- Type with inline comments
CREATE TYPE daily_consumption_summary AS (
    day TEXT, -- The day of consumption
    location_id UUID, -- The location ID
    location_name TEXT, -- The name of the location
    quantity NUMERIC, -- The quantity consumed (SUM of NUMERIC(12,6))
    unittype TEXT -- The unit of measurement
);

-- Function that uses the type with comments
CREATE OR REPLACE FUNCTION get_daily_consumption(p_location_id UUID, p_day DATE)
RETURNS SETOF daily_consumption_summary
LANGUAGE sql
AS $$
    SELECT 
        p_day::TEXT as day,
        p_location_id as location_id,
        'Test Location' as location_name,
        100.50 as quantity,
        'kWh' as unittype
    WHERE p_location_id IS NOT NULL;
$$;
