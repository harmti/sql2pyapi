# ===== SECTION: IMPORTS AND SETUP =====
# Standard library and third-party imports


# Local imports - Removed as unused
# from ..sql_models import ParsedFunction, ReturnColumn, SQLParameter
# from ..constants import *


def _generate_enum_class(enum_name: str, enum_values: list[str]) -> str:
    """
    Generates a Python Enum class definition string based on SQL ENUM type.

    Args:
        enum_name (str): Name of the SQL ENUM type
        enum_values (List[str]): List of values for the ENUM

    Returns:
        str: Python code for the Enum class definition as a string
    """
    # Convert enum_name to PascalCase for Python Enum class name
    class_name = "".join(word.capitalize() for word in enum_name.split("_"))

    # Generate enum members (convert values to UPPER_SNAKE_CASE)
    members = []
    for value in enum_values:
        # Convert value to UPPER_SNAKE_CASE for Python enum member
        member_name = value.upper()
        members.append(f"    {member_name} = '{value}'")

    members_str = "\n".join(members)

    return f"class {class_name}(Enum):\n{members_str}\n"
