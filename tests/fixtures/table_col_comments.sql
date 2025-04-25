CREATE TABLE table_with_col_comments (
    id uuid DEFAULT gen_random_uuid() NOT NULL PRIMARY KEY, -- The primary key
    name text NOT NULL, -- The name, mandatory
    industry text,   -- industry type
    size text, -- company size
    notes text, -- Some notes here
    created_at timestamp with time zone DEFAULT now() NOT NULL -- Creation timestamp
);

-- Function using the table (needed for test structure)
CREATE OR REPLACE FUNCTION get_table_with_col_comments()
RETURNS SETOF table_with_col_comments
LANGUAGE sql
AS $$ SELECT * FROM table_with_col_comments; $$; 