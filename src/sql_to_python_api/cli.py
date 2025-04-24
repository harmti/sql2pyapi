import typer
from pathlib import Path
import logging
import sys

from .parser import parse_sql, SQLParsingError
from .generator import generate_python_code

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

app = typer.Typer()

@app.command()
def main(
    sql_file: Path = typer.Argument(..., exists=True, file_okay=True, dir_okay=False, readable=True, help="Path to the input .sql file."),
    output_file: Path = typer.Argument(..., file_okay=True, dir_okay=False, writable=True, help="Path for the generated Python output file."),
):
    """Generates Python async API wrappers from PostgreSQL function definitions."""
    logging.info(f"Reading SQL from: {sql_file}")
    logging.info(f"Writing Python code to: {output_file}")

    try:
        sql_content = sql_file.read_text()
    except Exception as e:
        logging.error(f"Failed to read SQL file: {e}")
        raise typer.Exit(code=1)

    try:
        functions = parse_sql(sql_content)
        if not functions:
            logging.warning("No functions found in the SQL file. Output file will be empty.")
            # Ensure output directory exists even for empty output
            output_file.parent.mkdir(parents=True, exist_ok=True)
            output_file.write_text("# No functions found in the input SQL file.\n")
            return
        
        python_code = generate_python_code(functions, source_sql_file=sql_file.name)

    except SQLParsingError as e:
        logging.error(f"Failed to parse SQL: {e}")
        raise typer.Exit(code=1)
    except Exception as e:
        logging.error(f"An unexpected error occurred during processing: {e}")
        # Optionally add traceback here for debugging
        # import traceback
        # logging.error(traceback.format_exc())
        raise typer.Exit(code=1)

    try:
        # Ensure output directory exists
        output_file.parent.mkdir(parents=True, exist_ok=True)
        output_file.write_text(python_code)
        logging.info(f"Successfully generated Python code to {output_file}")
    except Exception as e:
        logging.error(f"Failed to write Python file: {e}")
        raise typer.Exit(code=1)


if __name__ == "__main__":
    app() 