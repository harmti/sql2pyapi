"""Tests for schema-qualified table name handling using the public API.

These tests focus on the specific issue of handling schema-qualified table names
in various contexts, especially in RETURNS SETOF clauses.
"""


# Import the public API

# Import test utilities
from tests.test_utils import find_function
from tests.test_utils import find_return_column
from tests.test_utils import parse_test_sql


def test_schema_qualified_table_returns():
    """Test that schema-qualified table names work correctly in RETURNS clauses."""
    # Create test tables with schema qualification
    schema_sql = """
    CREATE TABLE public.companies (
        company_id serial PRIMARY KEY,
        name text NOT NULL,
        founded_date date
    );

    CREATE TABLE analytics.metrics (
        metric_id uuid PRIMARY KEY,
        name text NOT NULL,
        value numeric NOT NULL,
        recorded_at timestamp DEFAULT now()
    );
    """

    # Create functions that return these tables with different qualification patterns
    functions_sql = """
    -- Function returning fully qualified table
    CREATE FUNCTION get_company(p_id integer)
    RETURNS public.companies
    LANGUAGE sql AS $$
        SELECT * FROM public.companies WHERE company_id = p_id;
    $$;

    -- Function returning SETOF fully qualified table
    CREATE FUNCTION list_companies()
    RETURNS SETOF public.companies
    LANGUAGE sql AS $$
        SELECT * FROM public.companies;
    $$;

    -- Function returning non-qualified table that exists with schema qualification
    CREATE FUNCTION find_company(p_name text)
    RETURNS companies
    LANGUAGE sql AS $$
        SELECT * FROM public.companies WHERE name ILIKE '%' || p_name || '%';
    $$;

    -- Function returning SETOF non-qualified table that exists with schema qualification
    CREATE FUNCTION search_companies(p_term text)
    RETURNS SETOF companies
    LANGUAGE sql AS $$
        SELECT * FROM public.companies WHERE name ILIKE '%' || p_term || '%';
    $$;

    -- Function returning table from a different schema
    CREATE FUNCTION get_metric(p_id uuid)
    RETURNS analytics.metrics
    LANGUAGE sql AS $$
        SELECT * FROM analytics.metrics WHERE metric_id = p_id;
    $$;

    -- Function returning SETOF table from a different schema
    CREATE FUNCTION list_metrics()
    RETURNS SETOF analytics.metrics
    LANGUAGE sql AS $$
        SELECT * FROM analytics.metrics;
    $$;

    -- Function returning non-qualified table from a different schema
    CREATE FUNCTION find_metric(p_name text)
    RETURNS metrics
    LANGUAGE sql AS $$
        SELECT * FROM analytics.metrics WHERE name ILIKE '%' || p_name || '%';
    $$;
    """

    # Parse the SQL
    functions, table_imports, _, _ = parse_test_sql(functions_sql, schema_sql)

    # Verify we parsed all 7 functions
    assert len(functions) == 7, f"Expected 7 functions, got {len(functions)}"

    # Test function returning fully qualified table
    get_company = find_function(functions, "get_company")
    assert get_company.returns_table
    assert not get_company.returns_setof
    assert len(get_company.return_columns) == 3

    # Test function returning SETOF fully qualified table
    list_companies = find_function(functions, "list_companies")
    assert list_companies.returns_table
    assert list_companies.returns_setof
    assert list_companies.setof_table_name == "public.companies"
    assert len(list_companies.return_columns) == 3

    # Test function returning non-qualified table that exists with schema qualification
    find_company = find_function(functions, "find_company")
    assert find_company.returns_table
    assert not find_company.returns_setof
    assert len(find_company.return_columns) == 3

    # Test function returning SETOF non-qualified table that exists with schema qualification
    search_companies = find_function(functions, "search_companies")
    assert search_companies.returns_table
    assert search_companies.returns_setof
    assert search_companies.setof_table_name == "companies"
    assert len(search_companies.return_columns) == 3

    # Test function returning table from a different schema
    get_metric = find_function(functions, "get_metric")
    assert get_metric.returns_table
    assert not get_metric.returns_setof
    assert len(get_metric.return_columns) == 4

    # Test function returning SETOF table from a different schema
    list_metrics = find_function(functions, "list_metrics")
    assert list_metrics.returns_table
    assert list_metrics.returns_setof
    assert list_metrics.setof_table_name == "analytics.metrics"
    assert len(list_metrics.return_columns) == 4

    # Test function returning non-qualified table from a different schema
    find_metric = find_function(functions, "find_metric")
    assert find_metric.returns_table
    assert not find_metric.returns_setof
    assert len(find_metric.return_columns) == 4

    # Verify that all functions returning the same table have the same column structure
    company_funcs = [get_company, list_companies, find_company, search_companies]
    for func in company_funcs:
        company_id = find_return_column(func, "company_id")
        assert company_id.sql_type == "serial"
        assert company_id.python_type == "int"
        assert not company_id.is_optional

        name = find_return_column(func, "name")
        assert name.sql_type == "text"
        assert name.python_type == "str"
        assert not name.is_optional

        founded_date = find_return_column(func, "founded_date")
        assert founded_date.sql_type == "date"
        assert founded_date.python_type == "Optional[date]"
        assert founded_date.is_optional

    # Verify that all functions returning metrics have the same column structure
    metric_funcs = [get_metric, list_metrics, find_metric]
    for func in metric_funcs:
        metric_id = find_return_column(func, "metric_id")
        assert metric_id.sql_type == "uuid"
        assert metric_id.python_type == "UUID"
        assert not metric_id.is_optional

        name = find_return_column(func, "name")
        assert name.sql_type == "text"
        assert name.python_type == "str"
        assert not name.is_optional

        value = find_return_column(func, "value")
        assert value.sql_type == "numeric"
        assert value.python_type == "Decimal"
        assert not value.is_optional

        recorded_at = find_return_column(func, "recorded_at")
        assert recorded_at.sql_type == "timestamp"
        assert recorded_at.python_type == "Optional[datetime]"
        assert recorded_at.is_optional


def test_schema_qualified_table_in_returns_setof():
    """Test the specific issue with RETURNS SETOF for schema-qualified tables."""
    # Create a test table with schema qualification
    schema_sql = """
    CREATE TABLE public.companies (
        company_id serial PRIMARY KEY,
        name text NOT NULL
    );
    """

    # Create a function that returns SETOF the table
    function_sql = """
    CREATE FUNCTION list_all_companies()
    RETURNS SETOF public.companies
    LANGUAGE sql AS $$
        SELECT * FROM public.companies;
    $$;
    """

    # Parse the SQL
    functions, _, _, _ = parse_test_sql(function_sql, schema_sql)

    # Find the function
    func = find_function(functions, "list_all_companies")

    # Verify the function properties
    assert func.returns_table
    assert func.returns_setof
    assert func.setof_table_name == "public.companies"
    assert len(func.return_columns) == 2

    # Verify the return columns
    company_id = find_return_column(func, "company_id")
    assert company_id.sql_type == "serial"
    assert company_id.python_type == "int"
    assert not company_id.is_optional

    name = find_return_column(func, "name")
    assert name.sql_type == "text"
    assert name.python_type == "str"
    assert not name.is_optional


def test_schema_qualified_table_with_missing_schema():
    """Test handling of schema-qualified tables when the schema qualifier is missing in one reference."""
    # Create a test table with schema qualification
    schema_sql = """
    CREATE TABLE public.products (
        product_id serial PRIMARY KEY,
        name text NOT NULL,
        price numeric(10, 2) NOT NULL
    );
    """

    # Create functions that reference the table with and without schema qualification
    functions_sql = """
    -- Function returning fully qualified table
    CREATE FUNCTION get_product(p_id integer)
    RETURNS public.products
    LANGUAGE sql AS $$
        SELECT * FROM public.products WHERE product_id = p_id;
    $$;

    -- Function returning SETOF non-qualified table
    CREATE FUNCTION list_products()
    RETURNS SETOF products
    LANGUAGE sql AS $$
        SELECT * FROM public.products;
    $$;
    """

    # Parse the SQL
    functions, _, _, _ = parse_test_sql(functions_sql, schema_sql)

    # Find the functions
    get_product = find_function(functions, "get_product")
    list_products = find_function(functions, "list_products")

    # Verify both functions have the same return structure
    assert get_product.returns_table
    assert list_products.returns_table
    assert not get_product.returns_setof
    assert list_products.returns_setof
    assert list_products.setof_table_name == "products"

    # Both should have the same columns
    assert len(get_product.return_columns) == 3
    assert len(list_products.return_columns) == 3

    # Verify columns in both functions
    for func in [get_product, list_products]:
        product_id = find_return_column(func, "product_id")
        assert product_id.sql_type == "serial"
        assert product_id.python_type == "int"
        assert not product_id.is_optional

        name = find_return_column(func, "name")
        assert name.sql_type == "text"
        assert name.python_type == "str"
        assert not name.is_optional

        price = find_return_column(func, "price")
        assert price.sql_type == "numeric(10, 2)"
        assert price.python_type == "Decimal"
        assert not price.is_optional
