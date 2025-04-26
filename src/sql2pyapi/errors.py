# ===== SECTION: ERROR CLASSES =====
# Custom exception classes for sql2pyapi

class SQL2PyAPIError(Exception):
    """Base class for all sql2pyapi errors."""
    pass


class ParsingError(SQL2PyAPIError):
    """Error during SQL parsing."""
    def __init__(self, message: str, sql_snippet: str = None, line_number: int = None, file_name: str = None):
        self.sql_snippet = sql_snippet
        self.line_number = line_number
        self.file_name = file_name
        
        details = ""
        if file_name:
            details += f" in file '{file_name}'"
        if line_number is not None:
            details += f" at line {line_number}"
        if sql_snippet:
            # Truncate very long SQL snippets
            if len(sql_snippet) > 100:
                sql_snippet = sql_snippet[:97] + "..."
            details += f": {sql_snippet}"
            
        super().__init__(f"{message}{details}")


class FunctionParsingError(ParsingError):
    """Error parsing a SQL function definition."""
    pass


class TableParsingError(ParsingError):
    """Error parsing a SQL table definition."""
    pass


class TypeMappingError(SQL2PyAPIError):
    """Error mapping SQL type to Python type."""
    def __init__(self, sql_type: str, context: str = None):
        self.sql_type = sql_type
        message = f"Unable to map SQL type '{sql_type}' to Python type"
        if context:
            message += f" in {context}"
        super().__init__(message)


class CodeGenerationError(SQL2PyAPIError):
    """Error during Python code generation."""
    def __init__(self, message: str, function_name: str = None, return_type: str = None):
        self.function_name = function_name
        self.return_type = return_type
        
        details = ""
        if function_name:
            details += f" for function '{function_name}'"
        if return_type:
            details += f" with return type '{return_type}'"
            
        super().__init__(f"{message}{details}")


class ParameterError(SQL2PyAPIError):
    """Error related to function parameters."""
    def __init__(self, message: str, param_name: str = None, param_type: str = None, function_name: str = None):
        self.param_name = param_name
        self.param_type = param_type
        self.function_name = function_name
        
        details = ""
        if function_name:
            details += f" in function '{function_name}'"
        if param_name:
            details += f" for parameter '{param_name}'"
        if param_type:
            details += f" of type '{param_type}'"
            
        super().__init__(f"{message}{details}")


class ReturnTypeError(SQL2PyAPIError):
    """Error related to function return types."""
    def __init__(self, message: str, return_type: str = None, function_name: str = None):
        self.return_type = return_type
        self.function_name = function_name
        
        details = ""
        if function_name:
            details += f" in function '{function_name}'"
        if return_type:
            details += f" for return type '{return_type}'"
            
        super().__init__(f"{message}{details}")
