import typer
from pathlib import Path
import logging
from typing import Optional

from .parser import parse_sql
from .errors import SQL2PyAPIError
from .generator import generate_python_code

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

app = typer.Typer()


@app.command()
def main(
    sql_file: Path = typer.Argument(
        ...,
        exists=True,
        file_okay=True,
        dir_okay=False,
        readable=True,
        help="Path to the input .sql file containing functions.",
    ),
    output_file: Path = typer.Argument(
        ...,
        file_okay=True,
        dir_okay=False,
        writable=True,
        help="Path for the generated Python output file.",
    ),
    schema_file: Optional[Path] = typer.Option(
        None,
        "--schema-file",
        "-s",
        exists=True,
        file_okay=True,
        dir_okay=False,
        readable=True,
        help="Optional path to a .sql file containing table schema (CREATE TABLE statements).",
    ),
    verbose: bool = typer.Option(
        False,
        "--verbose",
        "-v",
        help="Enable verbose (DEBUG) logging.",
    ),
    no_helpers: bool = typer.Option(
        False,
        "--no-helpers",
        help="Do not include helper functions (get_optional, get_required) in the output.",
    ),
    allow_missing_schemas: bool = typer.Option(
        False,
        "--allow-missing-schemas",
        help="Warn and generate placeholders instead of failing if a function's return table/type schema is not found. "
             "Warning: This may produce code that fails at runtime.",
    ),
):
    """Generates Python async API wrappers from PostgreSQL function definitions."""
    # Configure logging level based on verbose flag
    log_level = logging.DEBUG if verbose else logging.INFO
    # If verbose, reconfigure basicConfig to set the level to DEBUG
    # Force=True is needed if basicConfig was already called at the module level
    if verbose:
        logging.basicConfig(level=log_level, format="%(levelname)s: %(message)s", force=True)
    else:
        # Ensure INFO level if not verbose (might already be set by module level call)
        logging.basicConfig(level=log_level, format="%(levelname)s: %(message)s", force=True)

    logging.info(f"Reading functions SQL from: {sql_file}")
    if schema_file:
        logging.info(f"Reading schema SQL from: {schema_file}")
    logging.info(f"Writing Python code to: {output_file}")

    try:
        sql_content = sql_file.read_text()
        schema_content: Optional[str] = None
        if schema_file:
            schema_content = schema_file.read_text()

    except Exception as e:
        logging.error(f"Failed to read SQL file(s): {e}")
        raise typer.Exit(code=1)

    try:
        # Get functions, schema imports, composite types, AND enum types from parser
        functions, table_schema_imports, composite_types, enum_types = parse_sql(sql_content, schema_content=schema_content)

        if not functions:
            logging.warning("No functions found or parsed successfully. Output file will reflect this.")
            # Ensure output directory exists even for empty/warning output
            output_file.parent.mkdir(parents=True, exist_ok=True)
            output_file.write_text(f"# No functions parsed successfully from {sql_file.name}.\n")
            # Decide if exiting here is desired, or generating an empty file is ok.
            # return # Optionally exit if no functions are found

        # Pass schema imports, composite types, AND enum types to the generator
        python_code = generate_python_code(
            functions,
            table_schema_imports,
            composite_types,
            enum_types,
            source_sql_file=sql_file.name,
            omit_helpers=no_helpers,
            fail_on_missing_schema=not allow_missing_schemas,
        )

    except SQL2PyAPIError as e:
        logging.error(f"Failed to parse SQL: {e}")
        raise typer.Exit(code=1)
    except Exception as e:
        logging.error(f"An unexpected error occurred during processing: {e}")
        # Optionally add traceback here for debugging
        # import traceback
        # logging.error(traceback.format_exc())
        raise typer.Exit(code=1)

    try:
        output_file.parent.mkdir(parents=True, exist_ok=True)
        output_file.write_text(python_code)
        logging.info(f"Successfully generated Python code to {output_file}")
    except Exception as e:
        logging.error(f"Failed to write Python file: {e}")
        raise typer.Exit(code=1)


if __name__ == "__main__":
    app()
