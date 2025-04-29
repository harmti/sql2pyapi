import sys
from pathlib import Path
import logging

# Add project root to sys.path to allow importing sql2pyapi
PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.sql2pyapi.parser import parse_sql
# from src.sql2pyapi.generator import generate_python_code

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(levelname)s: %(message)s')

# Define paths
FIXTURES_DIR = PROJECT_ROOT / "tests" / "fixtures"
SQL_FILE_PATH = FIXTURES_DIR / "table_col_comments.sql"

def main():
    if not SQL_FILE_PATH.is_file():
        logging.error(f"SQL file not found: {SQL_FILE_PATH}")
        return

    logging.info(f"Reading SQL from: {SQL_FILE_PATH}")
    sql_content = SQL_FILE_PATH.read_text()

    try:
        logging.info("--- Starting Parser ---")
        parsed_functions, all_imports, composite_types = parse_sql(sql_content)
        logging.info("--- Parser Finished ---")

        if not parsed_functions:
            logging.warning("Parser did not find any functions.")
        else:
            logging.info(f"Found {len(parsed_functions)} function(s):")
            for func in parsed_functions:
                logging.info(f"  - {func.sql_name}")
                # Optionally print more details
                # print(f"    Params: {func.params}")
                # print(f"    Return: {func.return_type}")
                # print(f"    Columns: {func.return_columns}")

        # Uncomment to test generator as well
        # logging.info("--- Starting Generator ---")
        # Pass the correct variables to the generator
        # generated_code = generate_python_code(parsed_functions, all_imports, composite_types, str(SQL_FILE_PATH))
        # logging.info("--- Generator Finished ---")
        # print("\n--- Generated Code: ---")
        # print(generated_code)
        # print("--- End Generated Code ---")

    except Exception as e:
        logging.exception(f"An error occurred during processing: {e}")

if __name__ == "__main__":
    main() 