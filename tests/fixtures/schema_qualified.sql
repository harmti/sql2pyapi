-- Schema-qualified table definition
CREATE TABLE public.companies (
    id uuid PRIMARY KEY,
    name text NOT NULL,
    description text,
    created_at timestamp with time zone NOT NULL DEFAULT now(),
    updated_at timestamp with time zone NOT NULL DEFAULT now()
);

-- Function returning a schema-qualified table
CREATE FUNCTION get_company_by_id(company_id uuid) RETURNS public.companies AS $$
BEGIN
    RETURN (SELECT * FROM public.companies WHERE id = company_id);
END;
$$ LANGUAGE plpgsql;

-- Function returning a SETOF schema-qualified table
CREATE FUNCTION list_companies() RETURNS SETOF public.companies AS $$
BEGIN
    RETURN QUERY SELECT * FROM public.companies;
END;
$$ LANGUAGE plpgsql;
