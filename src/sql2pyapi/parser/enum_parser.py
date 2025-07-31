# ===== SECTION: IMPORTS =====
import logging
import re


# ===== SECTION: REGEX DEFINITIONS =====
# Regex for CREATE TYPE name AS ENUM (...)
ENUM_TYPE_REGEX = re.compile(
    r"CREATE\s+TYPE\s+([a-zA-Z0-9_.]+)"  # 1: Type name
    r"\s+AS\s+ENUM\s*\("  # AS ENUM (
    r"(.*?)"  # 2: Everything inside parenthesis (non-greedy)
    r"\)",  # Closing parenthesis
    re.IGNORECASE | re.DOTALL | re.MULTILINE,
)

# ===== SECTION: FUNCTIONS =====


def parse_enum_types(sql_content: str, existing_enum_types: dict[str, list[str]] | None = None) -> dict[str, list[str]]:
    """
    Parse SQL ENUM type definitions from the SQL content.

    Args:
        sql_content (str): SQL content to parse
        existing_enum_types (Dict[str, List[str]], optional): Existing enum types to update

    Returns:
        Dict[str, List[str]]: Dictionary mapping enum type names to their values
    """
    # Initialize or use existing dictionary
    enum_types = existing_enum_types or {}

    # Find all ENUM type definitions
    enum_matches = ENUM_TYPE_REGEX.finditer(sql_content)

    for match in enum_matches:
        enum_name = match.group(1)  # Type name
        enum_values_str = match.group(2)  # Values inside parentheses

        # Parse the enum values (they are quoted strings separated by commas)
        # Use a regex to extract quoted strings
        values_regex = re.compile(r"'([^']*)'")
        enum_values = values_regex.findall(enum_values_str)

        # Store the enum type and its values
        enum_types[enum_name] = enum_values
        logging.debug(f"Parsed ENUM type '{enum_name}' with values: {enum_values}")

    return enum_types
