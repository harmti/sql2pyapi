-- Test case for table names starting with "as" (regression test for word boundary bug)
CREATE TYPE async_status AS ENUM ('pending', 'running', 'completed');

CREATE TABLE async_processes (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    entity_id UUID NOT NULL,
    status async_status NOT NULL DEFAULT 'pending'
);

-- Additional test table with name starting with "lang"
CREATE TABLE language_settings (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    code TEXT NOT NULL
);

-- Function returning SETOF table with name starting with 'as'
CREATE OR REPLACE FUNCTION list_async_processes(p_entity_id UUID)
RETURNS SETOF async_processes
LANGUAGE sql
AS $$
    SELECT * FROM async_processes WHERE entity_id = p_entity_id;
$$;

-- Function returning SETOF table with name starting with 'lang'
CREATE OR REPLACE FUNCTION get_language_settings()
RETURNS SETOF language_settings
LANGUAGE sql
AS $$
    SELECT * FROM language_settings;
$$;