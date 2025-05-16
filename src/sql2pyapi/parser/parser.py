# ===== SECTION: IMPORTS =====
import re
import logging
from typing import Dict, List, Optional, Set, Tuple, Any

# Import from parent package
from ..comment_parser import COMMENT_REGEX, find_preceding_comment
from ..sql_models import ParsedFunction, ReturnColumn, SQLParameter, TYPE_MAP, PYTHON_IMPORTS
from ..errors import ParsingError, FunctionParsingError, TableParsingError, TypeMappingError, ReturnTypeError

# Import from same package
from .type_mapper import map_sql_to_python_type
from .parameter_parser import parse_params
from .return_parser import parse_return_clause
from .enum_parser import parse_enum_types
from .composite_parser import parse_create_type
from .table_parser import parse_create_table
from .column_parser import parse_column_definitions
from .utils import sanitize_for_class_name, generate_dataclass_name, _to_singular_camel_case

# No duplicate imports needed


# ===== SECTION: REGEX DEFINITIONS =====
# Main regex for finding function definitions
FUNCTION_REGEX = re.compile(
    r"""
    CREATE(?:\s+OR\s+REPLACE)?\s+FUNCTION\s+
    (?P<func_name>[a-zA-Z0-9_.]+)              # Function name (Group 'func_name')
    \s*\( (?P<params>[^)]*) \) \s*              # Use s* after params
    RETURNS \s+                                # RETURNS keyword
    (?P<return_def>.*?)                      # Non-greedy return definition (DOTALL allows newlines)
    # Positive Lookahead for LANGUAGE or AS $$
    (?=\s+(?:LANGUAGE|AS\s*\$\$))          
    """,
    re.IGNORECASE | re.DOTALL | re.VERBOSE,
)

class SQLParser:
    """
    Parses SQL content containing CREATE FUNCTION and CREATE TABLE statements.

    Manages table schemas discovered during parsing and provides methods
    to extract function definitions and their metadata.
    """

    def __init__(self, fail_on_missing_schema: bool = False):
        self.table_schemas: Dict[str, List[ReturnColumn]] = {}
        self.table_schema_imports: Dict[str, set] = {}
        self.composite_types: Dict[str, List[ReturnColumn]] = {}
        self.composite_type_imports: Dict[str, set] = {}
        self.enum_types: Dict[str, List[str]] = {}
        self.unnamed_param_count = 0
        self.fail_on_missing_schema: bool = fail_on_missing_schema


    def _parse_enum_types(self, sql_content: str) -> None:
        """
        Parse SQL ENUM type definitions from the SQL content.
        
        Args:
            sql_content: SQL content to parse
            
        Returns:
            None: Updates self.enum_types with discovered ENUM types
        """
        # Use the imported parse_enum_types function
        parsed_enums = parse_enum_types(sql_content) # Parse from the current content
        self.enum_types.update(parsed_enums)         # Update the parser's state

    def _map_sql_to_python_type(self, sql_type: str, is_optional: bool = False, context: str = None) -> Tuple[str, Set[str]]:
        """
        Maps a SQL type to its corresponding Python type and required imports.
        Delegates to the type_mapper module.

        Args:
            sql_type: The PostgreSQL type to map
            is_optional: Whether the type should be wrapped in Optional
            context: Context information for error reporting

        Returns:
            Tuple[str, Set[str]]: The Python type and a set of required imports

        Raises:
            TypeMappingError: If the SQL type cannot be mapped to a Python type
        """
        # Use the imported map_sql_to_python_type function
        return map_sql_to_python_type(sql_type, is_optional, context, self.enum_types, self.table_schemas)

    def _parse_column_definitions(self, col_defs_str: str, context: str = None) -> Tuple[List[ReturnColumn], Set[str]]:
        """
        Parses column definitions from CREATE TABLE or RETURNS TABLE.
        Delegates to the column_parser module.
        
        Args:
            col_defs_str: The column definitions string
            context: Context for error reporting
            
        Returns:
            Tuple[List[ReturnColumn], Set[str]]: The parsed columns and their imports
        """
        # Use the imported parse_column_definitions function
        return parse_column_definitions(col_defs_str, context, self.enum_types, self.table_schemas, self.composite_types)

    def _parse_create_table(self, sql_content: str):
        """Finds and parses CREATE TABLE statements, storing schemas in instance variables."""
        # Use the imported parse_create_table function
        table_schemas, table_schema_imports = parse_create_table(
            sql_content, 
            self.table_schemas, 
            self.table_schema_imports,
            self.enum_types
        )
        
        # Store under both the original name and normalized name (for schema-qualified tables)
        # This is important for handling schema-qualified table names like 'public.companies'
        for table_name, columns in table_schemas.items():
            if '.' in table_name:
                normalized_name = table_name.split('.')[-1]
                # Also store under the normalized name for backward compatibility
                self.table_schemas[normalized_name] = columns
                
        # Update instance variables
        self.table_schemas = table_schemas
        self.table_schema_imports = table_schema_imports

    def _parse_create_type(self, sql_content: str):
        """Finds and parses CREATE TYPE name AS (...) statements."""
        # Use the imported parse_create_type function
        composite_types, composite_type_imports = parse_create_type(
            sql_content,
            self.composite_types,
            self.composite_type_imports,
            self.enum_types,
            self.table_schemas
        )
        
        # Update instance variables
        self.composite_types = composite_types
        self.composite_type_imports = composite_type_imports

    def _parse_params(self, param_str: str, context: str = None) -> Tuple[List[SQLParameter], Set[str]]:
        """
        Parses parameter string including optional DEFAULT values.
        Delegates to the parameter_parser module.
        
        Args:
            param_str: The parameter string to parse
            context: Optional context for error reporting
            
        Returns:
            Tuple of (list of SQLParameter objects, set of required imports)
        """
        # Delegate to the parameter_parser module
        return parse_params(
            param_str=param_str, 
            context=context, 
            enum_types=self.enum_types, 
            table_schemas=self.table_schemas
        )

    def _parse_return_clause(self, match_dict: Dict, initial_imports: Set[str], function_name: str = None) -> Tuple[Dict, Set[str]]:
        """
        Parses the return clause components from the matched 'return_def' group.
        Delegates to the return_parser module.
        
        Args:
            match_dict: The regex match dictionary
            initial_imports: Initial set of imports
            function_name: The function name for context
            
        Returns:
            Tuple of (return info dictionary, updated imports)
        """
        # Delegate to the return_parser module
        return parse_return_clause(
            match_dict=match_dict,
            initial_imports=initial_imports,
            function_name=function_name,
            enum_types=self.enum_types,
            table_schemas=self.table_schemas,
            table_schema_imports=self.table_schema_imports,
            composite_types=self.composite_types,
            composite_type_imports=self.composite_type_imports
        )

    def parse(self, sql_content: str, schema_content: Optional[str] = None) -> Tuple[List[ParsedFunction], Dict[str, Set[str]], Dict[str, List[ReturnColumn]]]:
        """
        Parses SQL content, optionally using a separate schema file.

        This is the main public method to initiate parsing.

        Args:
            sql_content: String containing CREATE FUNCTION statements (and potentially CREATE TABLE).
            schema_content: Optional string containing CREATE TABLE statements.

        Returns:
            A tuple containing:
              - list of ParsedFunction objects.
              - dictionary mapping table names to required imports for their schemas.
              - dictionary mapping composite type names to their field definitions.
        """
        # Clear existing schemas for this run
        self.table_schemas.clear()
        self.table_schema_imports.clear()
        self.composite_types.clear()
        self.composite_type_imports.clear()
        
        # Parse ENUM types first
        self._parse_enum_types(sql_content)
        if schema_content:
            self._parse_enum_types(schema_content)

        # === Parse Schema (if provided) ===
        if schema_content:
            try:
                self._parse_create_table(schema_content)
                self._parse_create_type(schema_content)
            except Exception as e:
                logging.error(f"Error parsing schema content: {e}")
                raise TableParsingError(f"Failed to parse schema: {e}") from e

        # === Parse Tables and Types in Main SQL Content ===
        try:
            self._parse_create_table(sql_content)
        except Exception as e:
            # Log non-fatal error if table parsing fails here
            logging.warning(f"Could not parse CREATE TABLE statements in function file: {e}")
        try:
            self._parse_create_type(sql_content)
        except Exception as e:
            logging.warning(f"Could not parse CREATE TYPE statements in function file: {e}")

        # === Process SQL Content for Function Parsing ===
        # Remove comments to simplify function regex matching
        sql_no_comments = COMMENT_REGEX.sub("", sql_content)
        logging.debug(f"--- SQL Content after comment removal: ---\n{sql_no_comments[:200]}...\n--------------------------------------------")

        # Find function definitions using module-level regex
        matches = FUNCTION_REGEX.finditer(sql_no_comments)
        match_list = list(matches)
        match_count = len(match_list)
        logging.debug(f"FUNCTION_REGEX found {match_count} potential matches.")

        functions = []
        lines = sql_content.splitlines()
        
        # Reset the unnamed parameter counter before parsing functions
        self.unnamed_param_count = 0

        for i, match in enumerate(match_list):
            match_dict = match.groupdict()
            sql_name = None
            function_start_line = -1
            stripped_content_start_byte = match.start()
            approx_line_in_stripped = sql_no_comments[:stripped_content_start_byte].count('\n') + 1

            try:
                sql_name = match_dict['func_name'].strip()
                python_name = sql_name
                
                # --- Find the accurate start line in the ORIGINAL content ---
                original_start_byte = -1
                pattern1 = f"CREATE FUNCTION {sql_name}"
                pattern2 = f"CREATE OR REPLACE FUNCTION {sql_name}"
                search_start_offset = 0
                temp_lines = lines
                if approx_line_in_stripped > 5 and len(temp_lines) >= approx_line_in_stripped - 5:
                    search_start_offset = sum(len(l) + 1 for l in temp_lines[:approx_line_in_stripped - 5])
                original_start_byte = sql_content.find(pattern1, search_start_offset)
                if original_start_byte == -1:
                    original_start_byte = sql_content.find(pattern2, search_start_offset)
                if original_start_byte == -1:
                    logging.warning(f"Could not find exact function start for {sql_name} near estimate line {approx_line_in_stripped}. Searching from start.")
                    original_start_byte = sql_content.find(pattern1)
                    if original_start_byte == -1:
                        original_start_byte = sql_content.find(pattern2)
                if original_start_byte != -1:
                    function_start_line = sql_content[:original_start_byte].count('\n') + 1
                else:
                    logging.error(f"CRITICAL: Cannot find function definition start for '{sql_name}' in original SQL. Comment association may be wrong.")
                    function_start_line = approx_line_in_stripped # Fallback to estimate

                # --- Parse Parameters ---
                param_str = match_dict['params'] or ""
                param_str_cleaned = COMMENT_REGEX.sub("", param_str).strip()
                param_str_cleaned = " ".join(param_str_cleaned.split()) # Normalize whitespace
                parsed_params, param_imports = self._parse_params(param_str_cleaned, f"function '{sql_name}'")
                current_imports = param_imports.copy()
                current_imports.add("from psycopg import AsyncConnection")

                # --- Parse Return Clause ---
                return_info, current_imports = self._parse_return_clause(match_dict, current_imports, sql_name)

                # --- Find Preceding Comment ---
                function_start_line_idx = function_start_line - 1 if function_start_line > 0 else 0
                sql_comment = find_preceding_comment(lines, function_start_line_idx)

                # --- Determine final Python type hint (apply wrapping) ---
                base_py_type = return_info["return_type"]
                final_py_type = base_py_type
                is_setof = return_info["returns_setof"]
                dataclass_name = None  # Store the determined dataclass name for later use

                if base_py_type != "None":
                    if is_setof:
                        if base_py_type == "DataclassPlaceholder":
                            # If the return type is a named type (not a built-in type), check if it's a table, composite type, or enum
                            if return_info.get("returns_sql_type_name"):
                                # Check if it's an ENUM type
                                if return_info["returns_sql_type_name"] in self.enum_types:
                                    # Convert enum_name to PascalCase for Python Enum class name
                                    enum_name = ''.join(word.capitalize() for word in return_info["returns_sql_type_name"].split('_'))
                                    final_py_type = f"List[{enum_name}]"
                                    current_imports.add("List")
                                    current_imports.add("Enum")
                                # Check if it's a table reference
                                elif return_info["returns_sql_type_name"] in self.table_schemas:
                                    # For SETOF table returns, use the singular form of the table name
                                    table_name = return_info["returns_sql_type_name"]
                                    # Use _to_singular_camel_case for singularization
                                    class_name = _to_singular_camel_case(table_name)
                                    final_py_type = f"List[{class_name}]"
                                    current_imports.add("List")
                                    current_imports.add("dataclass")
                                    dataclass_name = class_name # Ensure dataclass_name is set
                                # Check if it's a composite type
                                elif return_info["returns_sql_type_name"] in self.composite_types:
                                    final_py_type = f"List[{generate_dataclass_name(sql_name, is_return=True)}]"
                                    current_imports.add("List")
                                    current_imports.add("dataclass")
                                else:
                                    # Unknown type - create a placeholder
                                    final_py_type = f"List[Any]"
                                    current_imports.add("List")
                                    current_imports.add("Any")
                            else:
                                # Handle SETOF table or composite type
                                class_name = "Any"
                                if return_info.get("setof_table_name"):
                                    # Use the original SQL table name (could be schema-qualified)
                                    table_name = return_info["setof_table_name"]
                                    # Use _to_singular_camel_case for singularization
                                    class_name = _to_singular_camel_case(table_name)
                                    dataclass_name = class_name  # Store for later use
                                    current_imports.add("List")
                                    if class_name == "Any": current_imports.add("Any")
                                elif return_info.get("returns_table") and return_info.get("return_columns"):
                                    # Generate a name based on function name for ad-hoc table returns
                                    class_name = generate_dataclass_name(sql_name, is_return=True)
                                    dataclass_name = class_name  # Store for later use
                                
                                final_py_type = f"List[{class_name}]"
                                current_imports.add("List")
                                if class_name == "Any": current_imports.add("Any")
                        else:
                            # Handle SETOF scalar type
                            final_py_type = f"List[{base_py_type}]"
                            current_imports.add("List")
                    else:
                        # Not SETOF - handle single row returns
                        if base_py_type == "DataclassPlaceholder":
                            # If the return type is a named type (not a built-in type), check if it's a table, composite type, or enum
                            if return_info.get("returns_sql_type_name"):
                                # Check if it's an ENUM type
                                if return_info["returns_sql_type_name"] in self.enum_types:
                                    # Convert enum_name to PascalCase for Python Enum class name
                                    enum_name = ''.join(word.capitalize() for word in return_info["returns_sql_type_name"].split('_'))
                                    # Don't wrap ENUM types in Optional by default
                                    final_py_type = enum_name
                                    current_imports.add("Enum")
                                # Check if it's a table reference
                                elif return_info["returns_sql_type_name"] in self.table_schemas:
                                    # For single table returns, use singular form as dataclass name
                                    table_name = return_info["returns_sql_type_name"]
                                    class_name = _to_singular_camel_case(table_name) # CHANGED for consistency
                                    final_py_type = f"Optional[{class_name}]"
                                    current_imports.add("Optional")
                                    current_imports.add("dataclass")
                                    dataclass_name = class_name # Ensure dataclass_name is set
                                # Check if it's a composite type
                                elif return_info["returns_sql_type_name"] in self.composite_types:
                                    final_py_type = f"Optional[{generate_dataclass_name(sql_name, is_return=True)}]"
                                    current_imports.add("Optional")
                                    current_imports.add("dataclass")
                                    # For functions returning a table, use the function name + Result
                                    type_name = return_info["returns_sql_type_name"]
                                    # This part was problematic, let's simplify. If it's a known table, use its singular name.
                                    if type_name in self.table_schemas:
                                        class_name = _to_singular_camel_case(type_name) # CHANGED
                                    # else, it might be a custom type not in table_schemas, or an ad-hoc name is needed
                                    # The existing logic for generate_dataclass_name(sql_name, is_return=True) or sanitize_for_class_name might apply
                                    # For now, only changing known table references. The original logic was:
                                    # if sql_name in ['get_user_by_email', 'get_order_details'] or (type_name.lower() in ['users', 'public.users', 'orders', 'public.orders']):
                                    #     class_name = generate_dataclass_name(sql_name, is_return=True)
                                    # else:
                                    #     class_name = sanitize_for_class_name(type_name)
                                    else:
                                         # Fallback to sanitize_for_class_name or generate_dataclass_name for non-SETOF named types not directly in table_schemas
                                         # This part needs careful review, but the immediate goal is SETOF table singularization
                                         # Using sanitize_for_class_name for now if not a known table_schema. Original plan was for SETOF.
                                         class_name = sanitize_for_class_name(type_name)
                                    dataclass_name = class_name  # Store for later use
                            elif return_info.get("return_columns"):
                                # Generate a name based on function name for ad-hoc table returns
                                class_name = generate_dataclass_name(sql_name, is_return=True)
                                dataclass_name = class_name  # Store for later use
                            
                            final_py_type = f"Optional[{class_name}]"
                            current_imports.add("Optional")
                            if class_name == "Any": current_imports.add("Any")
                        else:
                            # Handle single row scalar type
                            # Check if this is an ENUM type that was already processed in _handle_returns_type_name
                            if return_info.get("returns_enum_type"):
                                # Don't wrap ENUM types in Optional
                                final_py_type = base_py_type
                                current_imports.add("Enum")
                            else:
                                # For non-ENUM scalar types, wrap in Optional
                                final_py_type = f"Optional[{base_py_type}]"
                                current_imports.add("Optional")

                # Ensure necessary base types are imported
                if "Tuple" in final_py_type: current_imports.add("Tuple")
                if "Any" in final_py_type: current_imports.add("Any")
                current_imports.discard(None)
                current_imports.discard('DataclassPlaceholder') # Remove placeholder

                # --- Create the ParsedFunction object ---
                parsed_func = ParsedFunction(
                    sql_name=sql_name,
                    python_name=python_name,
                    params=parsed_params,
                    return_type=final_py_type,
                    return_columns=return_info.get("return_columns", []),
                    required_imports=current_imports,
                    sql_comment=sql_comment,
                    returns_enum_type=return_info.get("returns_enum_type", False),
                    returns_table=return_info.get("returns_table", False),
                    returns_record=return_info.get("returns_record", False),
                    returns_setof=is_setof,
                    returns_sql_type_name=return_info.get("returns_sql_type_name"),
                    setof_table_name=return_info.get("setof_table_name"),
                    dataclass_name=dataclass_name,
                )
                functions.append(parsed_func)
                logging.debug(f"Successfully parsed function: {sql_name}")
            except Exception as e:
                logging.error(f"Error parsing function {sql_name if sql_name else 'UNKNOWN'}: {str(e)}")
                import traceback
                logging.error(traceback.format_exc())

        logging.debug(f"Finished FUNCTION_REGEX iteration. Found {len(functions)} functions.") # DEBUG LOG

        # Ensure table schemas used in SETOF returns are added to composite_types
        # This makes them available for dataclass generation later.
        composite_types_to_return = self.composite_types.copy()
        imports_to_return = {}  # Initialize empty imports dictionary
        
        # We don't need to add function-specific imports to the table imports
        # This was causing the test to fail because it expects only table-specific imports
        # The function imports are already included in the ParsedFunction objects
                
        # Add table and composite type imports
        for table_name, imports in self.table_schema_imports.items():
            if table_name not in imports_to_return:
                imports_to_return[table_name] = set()
            imports_to_return[table_name].update(imports)
            
        for type_name, imports in self.composite_type_imports.items():
            if type_name not in imports_to_return:
                imports_to_return[type_name] = set()
            imports_to_return[type_name].update(imports)

        for func in functions:
            if func.returns_setof and func.setof_table_name:
                table_name = func.setof_table_name
                # Check if this table schema exists but isn't already in the composite types dict
                if table_name in self.table_schemas and table_name not in composite_types_to_return:
                    logging.debug(f"Adding schema for table '{table_name}' used in SETOF return to composite types for dataclass generation.")
                    composite_types_to_return[table_name] = self.table_schemas[table_name]
                    # Also ensure its imports are included
                    if table_name in self.table_schema_imports:
                         imports_to_return[table_name] = self.table_schema_imports[table_name]
                elif table_name not in self.table_schemas and table_name not in composite_types_to_return:
                     # Check if we should fail or just warn
                     if self.fail_on_missing_schema:
                         raise ValueError(f"Function '{func.sql_name}' returns SETOF '{table_name}', but no schema found for this table/type. 'fail_on_missing_schema' is True.")
                     else:
                         logging.warning(f"Function '{func.sql_name}' returns SETOF '{table_name}', but no schema found for this table/type. 'fail_on_missing_schema' is False.")


        # Return the list of functions, the combined imports, and the composite types (now including SETOF table types)
        return functions, imports_to_return, composite_types_to_return

def parse_sql(sql_content: str, schema_content: Optional[str] = None, fail_on_missing_schema: bool = False) -> Tuple[List[ParsedFunction], Dict[str, set], Dict[str, List[ReturnColumn]], Dict[str, List[str]]]:
    """
    Parse SQL content to extract functions, their parameters, return types, and comments.
    This is the main public API function for the sql2pyapi parser.
    
    Args:
        sql_content: SQL content to parse
        schema_content: Optional separate schema content (for CREATE TABLE statements)
        fail_on_missing_schema: If True, an error will be raised if a function returns SETOF <table_name>
                                and the schema for <table_name> cannot be found. If False, a warning
                                will be logged, and the return type may default to Any.
        
    Returns:
        Tuple of (list of ParsedFunction, imports dict, composite types dict, enum types dict)
    """
    parser = SQLParser(fail_on_missing_schema=fail_on_missing_schema)
    
    # Parse enum types from both SQL and schema content
    parser._parse_enum_types(sql_content)
    if schema_content:
        parser._parse_enum_types(schema_content)
    
    # If schema content is provided separately, parse it first
    if schema_content:
        parser._parse_create_table(schema_content)
        parser._parse_create_type(schema_content)
    
    # Parse the main SQL content
    functions, imports_to_return, composite_types_to_return = parser.parse(sql_content, schema_content)
    
    # Always return enum_types
    return functions, imports_to_return, composite_types_to_return, parser.enum_types

# ... (Removed legacy sections) ...

# Add helper functions if they don't exist (needed for revised logic)
# These should ideally live within the parser or a utility module

import re

def _sanitize_for_class_name(name: str) -> str:
    """
    Sanitizes a SQL table/type name for use as a Python class name.
    Handles schema-qualified names by removing the schema prefix.
    """
    # Remove schema prefix if present (e.g., 'public.users' -> 'users')
    if '.' in name:
        name = name.split('.')[-1]
    
    # Replace special characters with underscores
    sanitized = re.sub(r'[^a-zA-Z0-9_]', '_', name)
    
    # Ensure the name starts with a letter
    if sanitized and not sanitized[0].isalpha():
        sanitized = 'T_' + sanitized
    
    # Capitalize the name (CamelCase)
    parts = sanitized.split('_')
    return ''.join(p.capitalize() for p in parts if p)

def _generate_dataclass_name(sql_func_name: str, is_return: bool = False) -> str:
    """
    Generates a Pythonic class name based on the SQL function name.
    Handles schema-qualified names and ensures consistent naming for return types.
    
    Args:
        sql_func_name (str): The SQL function name, possibly schema-qualified
        is_return (bool): Whether this is for a return type (adds 'Result' suffix)
        
    Returns:
        str: A valid Python class name in PascalCase
    """
    # Extract base name without schema qualification
    base_name = sql_func_name.split('.')[-1]
    
    # Replace special characters with underscores
    sanitized = re.sub(r'[^a-zA-Z0-9_]', '_', base_name)
    
    # Split by underscores and capitalize each part
    parts = sanitized.split('_')
    class_name = "".join(part.capitalize() for part in parts if part)
    
    # For return types, append 'Result' to make it clear this is a return type
    if is_return:
        class_name += "Result"
    
    # Ensure it's a valid identifier
    if not class_name or not class_name[0].isalpha():
        prefix = "Result" if is_return else "Param"
        class_name = prefix + class_name
        
    return class_name
