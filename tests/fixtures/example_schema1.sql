CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE TABLE IF NOT EXISTS users (
    id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
    clerk_id VARCHAR(255) UNIQUE NOT NULL,
    email TEXT UNIQUE,
    email_verified BOOLEAN DEFAULT FALSE,
    first_name TEXT,
    last_name TEXT,
    last_sign_in_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT now() NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT now() NOT NULL,
    is_deleted BOOLEAN DEFAULT FALSE
);

-- Trigger function to set updated_at timestamp
CREATE OR REPLACE FUNCTION trigger_set_timestamp()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger to automatically update updated_at on users table changes
DROP TRIGGER IF EXISTS set_timestamp ON users;
CREATE TRIGGER set_timestamp
BEFORE UPDATE ON users
FOR EACH ROW
EXECUTE FUNCTION trigger_set_timestamp();
-- Table definition for companies
CREATE TABLE companies (
    id uuid DEFAULT gen_random_uuid() NOT NULL PRIMARY KEY,
    name text NOT NULL,
    industry text,
    size text,
    primary_address text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    created_by_user_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE
);

-- Add indexes for common lookups
CREATE INDEX idx_companies_created_by_user_id ON companies(created_by_user_id);

-- Optional: Trigger to update 'updated_at' timestamp
-- This assumes a function like 'trigger_set_timestamp' exists, common in many setups.
-- CREATE TRIGGER set_timestamp
-- BEFORE UPDATE ON companies
-- FOR EACH ROW
-- EXECUTE PROCEDURE trigger_set_timestamp();

COMMENT ON TABLE companies IS 'Stores company information.';
COMMENT ON COLUMN companies.id IS 'Unique identifier for the company.';
COMMENT ON COLUMN companies.name IS 'Name of the company.';
COMMENT ON COLUMN companies.industry IS 'Industry the company operates in.';
COMMENT ON COLUMN companies.size IS 'Approximate size of the company (e.g., number of employees).';
COMMENT ON COLUMN companies.primary_address IS 'Primary physical address of the company.';
COMMENT ON COLUMN companies.created_at IS 'Timestamp when the company was created.';
COMMENT ON COLUMN companies.updated_at IS 'Timestamp when the company was last updated.';
COMMENT ON COLUMN companies.created_by_user_id IS 'User ID of the user who created the company record.';
