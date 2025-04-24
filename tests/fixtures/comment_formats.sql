-- This is a multi-line comment
-- describing the first function.
-- It has three lines.
CREATE OR REPLACE FUNCTION function_with_multiline_dash_comment()
RETURNS integer
LANGUAGE sql AS $$ SELECT 1; $$;

/* This is a single-line block comment. */
CREATE OR REPLACE FUNCTION function_with_single_block_comment()
RETURNS text
LANGUAGE sql AS $$ SELECT 'single block'; $$;

/*
 * This is a multi-line block comment.
 * It uses asterisks for alignment.
 *   And has some indentation.
 */
CREATE OR REPLACE FUNCTION function_with_multi_block_comment()
RETURNS boolean
LANGUAGE sql AS $$ SELECT true; $$;

-- This comment is for the table, not the function below
CREATE TABLE some_other_table (id int);

CREATE OR REPLACE FUNCTION function_with_no_comment()
RETURNS void
LANGUAGE sql AS $$ SELECT; $$;

/* This comment is separated by a blank line */

CREATE OR REPLACE FUNCTION function_with_separated_comment()
RETURNS integer
LANGUAGE sql AS $$ SELECT 2; $$; 