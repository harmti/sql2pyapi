"""Unit tests for the sql2pyapi parser module."""

import pytest
from typing import Optional, Tuple, Set

# Import the function under test (even if private)
from sql2pyapi.parser import _map_sql_to_python_type

# Parameterized test cases: (sql_type, is_optional, expected_py_type, expected_imports)
map_type_test_cases = [
    # Basic types
    ("integer", False, "int", set()),
    ("int", False, "int", set()),
    ("bigint", False, "int", set()),
    ("smallint", False, "int", set()),
    ("serial", False, "int", set()),
    ("bigserial", False, "int", set()),
    ("text", False, "str", set()),
    ("varchar", False, "str", set()),
    ("character varying", False, "str", set()),
    ("boolean", False, "bool", set()),
    ("bool", False, "bool", set()),
    ("bytea", False, "bytes", set()),
    # Types requiring imports
    ("uuid", False, "UUID", {"from uuid import UUID"}),
    ("timestamp", False, "datetime", {"from datetime import datetime"}),
    ("timestamp without time zone", False, "datetime", {"from datetime import datetime"}),
    ("timestamptz", False, "datetime", {"from datetime import datetime"}),
    ("timestamp with time zone", False, "datetime", {"from datetime import datetime"}),
    ("date", False, "date", {"from datetime import date"}),
    ("numeric", False, "Decimal", {"from decimal import Decimal"}),
    ("decimal", False, "Decimal", {"from decimal import Decimal"}),
    # JSON types
    ("json", False, "Dict[str, Any]", {"from typing import Dict", "from typing import Any"}),
    ("jsonb", False, "Dict[str, Any]", {"from typing import Dict", "from typing import Any"}),
    # Unknown type
    ("some_unknown_type", False, "Any", {"from typing import Any"}),
    # Case insensitivity and whitespace
    (" INTEGER ", False, "int", set()),
    (" VARCHAR ", False, "str", set()),
    # Optional types (basic)
    ("integer", True, "Optional[int]", {"from typing import Optional"}),
    ("text", True, "Optional[str]", {"from typing import Optional"}),
    ("boolean", True, "Optional[bool]", {"from typing import Optional"}),
    # Optional types (requiring imports)
    ("uuid", True, "Optional[UUID]", {"from typing import Optional", "from uuid import UUID"}),
    ("date", True, "Optional[date]", {"from typing import Optional", "from datetime import date"}),
    ("numeric", True, "Optional[Decimal]", {"from typing import Optional", "from decimal import Decimal"}),
    ("jsonb", True, "Optional[Dict[str, Any]]", {"from typing import Optional", "from typing import Dict", "from typing import Any"}),
    # Optional unknown type maps to Any (not Optional[Any])
    ("some_unknown_type", True, "Any", {"from typing import Any"}),
    # Array types (basic)
    ("integer[]", False, "List[int]", {"from typing import List"}),
    ("text[]", False, "List[str]", {"from typing import List"}),
    ("varchar[]", False, "List[str]", {"from typing import List"}),
    # Array types (requiring imports)
    ("uuid[]", False, "List[UUID]", {"from typing import List", "from uuid import UUID"}),
    ("date[]", False, "List[date]", {"from typing import List", "from datetime import date"}),
    ("numeric[]", False, "List[Decimal]", {"from typing import List", "from decimal import Decimal"}),
    ("jsonb[]", False, "List[Dict[str, Any]]", {"from typing import List", "from typing import Dict", "from typing import Any"}),
    # Optional array types (Now expecting Optional[List[T]])
    ("integer[]", True, "Optional[List[int]]", {"from typing import List", "from typing import Optional"}),
    ("uuid[]", True, "Optional[List[UUID]]", {"from typing import List", "from uuid import UUID", "from typing import Optional"}),
    # Complex type names (like varchar(N))
    ("character varying(255)", False, "str", set()),
    ("varchar(100)", False, "str", set()),
    ("numeric(10, 2)", False, "Decimal", {"from decimal import Decimal"}),
    ("decimal(5, 0)", False, "Decimal", {"from decimal import Decimal"}),
    ("timestamp(0) without time zone", False, "datetime", {"from datetime import datetime"}),
    ("timestamp(6) with time zone", False, "datetime", {"from datetime import datetime"}),
    # Optional complex types
    ("character varying(50)", True, "Optional[str]", {"from typing import Optional"}),
    ("numeric(8, 4)", True, "Optional[Decimal]", {"from typing import Optional", "from decimal import Decimal"}),
]


@pytest.mark.parametrize("sql_type, is_optional, expected_py_type, expected_imports", map_type_test_cases)
def test_map_sql_to_python_type(sql_type: str, is_optional: bool, expected_py_type: str, expected_imports: Set[str]):
    """Tests the _map_sql_to_python_type function with various inputs."""
    py_type, imports_str = _map_sql_to_python_type(sql_type, is_optional)

    # Check the Python type
    assert py_type == expected_py_type

    # Check the imports
    # Convert the returned string of imports (or None) into a set for comparison
    returned_imports = set(imports_str.split('\n')) if imports_str else set()
    # Remove empty strings that might result from splitting None or empty string
    returned_imports.discard('')

    assert returned_imports == expected_imports


# Remove the dummy test now that we have real tests
# def test_dummy():
#     """Dummy test to ensure discovery."""
#     assert True 