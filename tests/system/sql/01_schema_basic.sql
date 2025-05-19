-- tests/system/sql/00_schema.sql

-- Custom ENUM type
CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');

-- Basic table covering various data types
CREATE TABLE items (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    description TEXT,
    quantity INTEGER DEFAULT 0,
    price NUMERIC(10, 2),
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP, -- Nullable timestamp without timezone
    metadata JSONB,
    tags TEXT[], -- Array of text
    related_ids INT[], -- Array of integers
    current_mood mood -- Enum type
);

-- Another table for relations and different key types
CREATE TABLE related_items (
    item_id INTEGER REFERENCES items(id) ON DELETE CASCADE,
    uuid_key UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    notes TEXT,
    config JSON -- Plain JSON type
);

-- A simple view
CREATE VIEW active_items AS
SELECT id, name, quantity, price
FROM items
WHERE is_active = true;

-- Composite type (similar to table structure but not a table itself)
CREATE TYPE item_summary AS (
    item_name TEXT,
    total_value NUMERIC
); 