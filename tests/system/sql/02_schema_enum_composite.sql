-- tests/system/sql/02_enum_composite_type.sql

-- Create a composite type that includes the mood enum
CREATE TYPE item_with_mood AS (
    id INTEGER,
    name TEXT,
    current_mood mood
);

