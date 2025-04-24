-- Define a simple table
CREATE TABLE products (
    product_id serial PRIMARY KEY,
    product_name varchar(255) NOT NULL,
    price numeric(10, 2)
);

-- Function that returns rows from the products table
-- Should use the schema defined above
CREATE OR REPLACE FUNCTION get_all_products()
RETURNS SETOF products
LANGUAGE sql
AS $$
    SELECT * FROM products;
$$; 