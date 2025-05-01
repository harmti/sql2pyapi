CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";
-- Custom type for returning basic user identification
CREATE TYPE user_identity AS (
    id UUID,
    clerk_id TEXT
);

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
    id uuid DEFAULT uuid_generate_v4() NOT NULL PRIMARY KEY,
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
-- schema/320_locations.sql

-- Table definition for company locations
CREATE TABLE locations (
    -- Unique identifier for the location
    id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),

    -- Foreign key referencing the company this location belongs to
    company_id uuid NOT NULL REFERENCES companies(id) ON DELETE CASCADE,

    -- Name of the location (e.g., "Headquarters", "Warehouse A")
    name text NOT NULL,

    -- Full street address
    address text,

    -- City
    city text,

    -- State or province
    state text,

    -- Country
    country text,

    -- Postal or ZIP code
    zip_code text,

    -- Geographic latitude
    latitude double precision,

    -- Geographic longitude
    longitude double precision,

    -- Type of location (e.g., 'office', 'warehouse', 'retail') - Consider ENUM type later if applicable
    location_type text,

    -- User who created this location record
    created_by_user_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,

    -- Timestamp when the record was created
    created_at timestamp with time zone DEFAULT now() NOT NULL,

    -- Timestamp when the record was last updated
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);

-- Indexes for faster lookups
CREATE INDEX idx_locations_company_id ON locations(company_id);
CREATE INDEX idx_locations_created_by_user_id ON locations(created_by_user_id);

-- Add comments on table and columns for better documentation
COMMENT ON TABLE locations IS 'Stores location information for companies.';
COMMENT ON COLUMN locations.id IS 'Unique identifier for the location';
COMMENT ON COLUMN locations.company_id IS 'Foreign key referencing the company this location belongs to';
COMMENT ON COLUMN locations.name IS 'Name of the location (e.g., "Headquarters", "Warehouse A")';
COMMENT ON COLUMN locations.address IS 'Full street address';
COMMENT ON COLUMN locations.city IS 'City';
COMMENT ON COLUMN locations.state IS 'State or province';
COMMENT ON COLUMN locations.country IS 'Country';
COMMENT ON COLUMN locations.zip_code IS 'Postal or ZIP code';
COMMENT ON COLUMN locations.latitude IS 'Geographic latitude';
COMMENT ON COLUMN locations.longitude IS 'Geographic longitude';
COMMENT ON COLUMN locations.location_type IS 'Type of location (e.g., ''office'', ''warehouse'', ''retail'')';
COMMENT ON COLUMN locations.created_by_user_id IS 'User ID of the user who created the location record';
COMMENT ON COLUMN locations.created_at IS 'Timestamp when the record was created';
COMMENT ON COLUMN locations.updated_at IS 'Timestamp when the record was last updated';-- schema/340_company_members.sql

-- Create an ENUM type for company roles for better type safety
CREATE TYPE company_role AS ENUM ('owner', 'admin', 'member');

-- Table definition for company members
CREATE TABLE company_members (
    -- Unique identifier for the membership record
    id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),

    -- Foreign key referencing the user
    user_id uuid NOT NULL REFERENCES users(id) ON DELETE CASCADE,

    -- Foreign key referencing the company
    company_id uuid NOT NULL REFERENCES companies(id) ON DELETE CASCADE,

    -- Role of the user in the company (owner, admin, member)
    role company_role NOT NULL DEFAULT 'member',

    -- Status of the membership (active, inactive)
    status text NOT NULL DEFAULT 'active',

    -- When the user was invited to join the company
    invited_at timestamp with time zone DEFAULT now(),

    -- When the user joined the company (accepted invitation)
    joined_at timestamp with time zone DEFAULT now(),

    -- User who invited this member
    invited_by_user_id uuid REFERENCES users(id) ON DELETE SET NULL,

    -- Optional reference to the invitation that created this membership
    invitation_id uuid, -- Will be updated with REFERENCES constraint after company_invitations table is created

    -- Timestamp when the record was created
    created_at timestamp with time zone DEFAULT now() NOT NULL,

    -- Timestamp when the record was last updated
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);

-- Indexes for faster lookups
CREATE INDEX idx_company_members_user_id ON company_members(user_id);
CREATE INDEX idx_company_members_company_id ON company_members(company_id);
CREATE INDEX idx_company_members_invited_by_user_id ON company_members(invited_by_user_id);

-- Unique constraint to prevent duplicate memberships
CREATE UNIQUE INDEX idx_company_members_unique_membership 
    ON company_members(user_id, company_id) 
    WHERE status != 'inactive';

-- Add comments on table and columns for better documentation
COMMENT ON TABLE company_members IS 'Stores membership information for users in companies.';
COMMENT ON COLUMN company_members.id IS 'Unique identifier for the membership record';
COMMENT ON COLUMN company_members.user_id IS 'Foreign key referencing the user';
COMMENT ON COLUMN company_members.company_id IS 'Foreign key referencing the company';
COMMENT ON COLUMN company_members.role IS 'Role of the user in the company (owner, admin, member) using company_role enum';
COMMENT ON COLUMN company_members.status IS 'Status of the membership (active, pending, inactive)';
COMMENT ON COLUMN company_members.invited_at IS 'When the user was invited to the company';
COMMENT ON COLUMN company_members.joined_at IS 'When the user accepted the invitation and joined the company';
COMMENT ON COLUMN company_members.invited_by_user_id IS 'User who created this membership/sent the invitation';
COMMENT ON COLUMN company_members.created_at IS 'Timestamp when the record was created';
COMMENT ON COLUMN company_members.updated_at IS 'Timestamp when the record was last updated';
-- schema/350_company_invitations.sql

-- Table definition for company invitations
CREATE TABLE company_invitations (
    -- Unique identifier for the invitation
    id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
    
    -- Foreign key referencing the company sending the invitation
    company_id uuid NOT NULL REFERENCES companies(id) ON DELETE CASCADE,
    
    -- Email of the user being invited
    email text NOT NULL,
    
    -- Role the user will have when they accept the invitation
    role company_role NOT NULL DEFAULT 'member',
    
    -- Secure token for accepting the invitation
    token text NOT NULL,
    
    -- When the invitation expires
    expires_at timestamp with time zone NOT NULL,
    
    -- Status of the invitation (pending, accepted, rejected, expired)
    status text NOT NULL DEFAULT 'pending',
    
    -- User who sent the invitation
    invited_by_user_id uuid NOT NULL REFERENCES users(id) ON DELETE SET NULL,
    
    -- Timestamps
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);

-- Add indexes for common lookups
CREATE INDEX idx_company_invitations_company_id ON company_invitations(company_id);
CREATE INDEX idx_company_invitations_email ON company_invitations(email);
CREATE INDEX idx_company_invitations_token ON company_invitations(token);

-- Add unique constraint to prevent duplicate active invitations
CREATE UNIQUE INDEX idx_unique_active_invitation ON company_invitations(company_id, email) 
    WHERE status = 'pending';

-- Add comments for documentation
COMMENT ON TABLE company_invitations IS 'Stores invitations for users to join companies';
COMMENT ON COLUMN company_invitations.id IS 'Unique identifier for the invitation';
COMMENT ON COLUMN company_invitations.company_id IS 'ID of the company sending the invitation';
COMMENT ON COLUMN company_invitations.email IS 'Email address of the invited user';
COMMENT ON COLUMN company_invitations.role IS 'Role the user will have when they accept the invitation using company_role enum';
COMMENT ON COLUMN company_invitations.token IS 'Secure token used for accepting the invitation';
COMMENT ON COLUMN company_invitations.expires_at IS 'When the invitation expires';
COMMENT ON COLUMN company_invitations.status IS 'Status of the invitation (pending, accepted, rejected, expired)';
COMMENT ON COLUMN company_invitations.invited_by_user_id IS 'ID of the user who sent the invitation';
COMMENT ON COLUMN company_invitations.created_at IS 'When the invitation was created';
COMMENT ON COLUMN company_invitations.updated_at IS 'When the invitation was last updated';

-- Update the company_members table to add the foreign key constraint to company_invitations
ALTER TABLE company_members 
    ADD CONSTRAINT fk_company_members_invitation_id 
    FOREIGN KEY (invitation_id) 
    REFERENCES company_invitations(id) 
    ON DELETE SET NULL;
