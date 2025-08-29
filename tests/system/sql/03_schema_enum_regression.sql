-- Schema for enum regression test
-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Create enum type
CREATE TYPE user_role_enum AS ENUM ('owner', 'admin', 'member');

-- Create simple table
CREATE TABLE enum_users (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT NOT NULL,
    role user_role_enum NOT NULL
);

-- Insert test data
INSERT INTO enum_users (name, role) VALUES 
    ('Test Admin', 'admin'),
    ('Test Owner', 'owner'),
    ('Test Member', 'member');


--- ENUM type for invitation status
CREATE TYPE invitation_status AS ENUM (
    'pending',
    'accepted',
    'rejected',
    'expired',
    'cancelled'
);

CREATE TYPE company_role AS ENUM ('owner', 'admin', 'member', 'system');

-- Table definition for company invitations
CREATE TABLE IF NOT EXISTS company_invitations (
    -- Unique identifier for the invitation
    id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),

    company_id uuid NOT NULL,

    -- Email of the user being invited
    email text NOT NULL,

    -- Role the user will have when they accept the invitation
    role company_role NOT NULL DEFAULT 'member'::company_role,

    -- Status of the invitation (pending, accepted, rejected, expired)
    status invitation_status NOT NULL DEFAULT 'pending',

    -- Timestamps
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);

-- Insert test data for company invitations
INSERT INTO company_invitations (company_id, email, role, status) VALUES 
    (uuid_generate_v4(), 'admin@test.com', 'admin', 'pending'),
    (uuid_generate_v4(), 'owner@test.com', 'owner', 'accepted'),
    (uuid_generate_v4(), 'member@test.com', 'member', 'pending');
