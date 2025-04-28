# Contributing to SQL2PyAPI

Thank you for your interest in contributing to SQL2PyAPI! This guide provides high-level information for contributors.

## Project Overview

SQL2PyAPI is a tool that generates Python async wrappers around PostgreSQL functions. It was developed with the assistance of LLMs (Large Language Models) and continues to welcome contributions from both human developers and LLM-assisted development.

The project consists of these main components:

- **SQL Parser**: Extracts function definitions and table schemas from SQL files
- **Python Code Generator**: Creates typed async Python wrappers from parsed SQL
- **CLI Tool**: Provides command-line interface for the generator

## Development Principles

1. **Type Safety**: Maintain strong typing throughout the codebase and generated code
2. **Maintainability**: Write clear, documented code with sensible abstractions
3. **Testing**: Ensure all features and edge cases are well-tested
4. **Compatibility**: Support standard PostgreSQL syntax and features

## Contributing Code

1. **Fork the repository** and create a feature branch
2. **Write tests** that demonstrate your change works as expected
3. **Submit a pull request** with a clear description of the changes
4. **Participate in code review** to refine the implementation

## LLM Contributions

This project was developed with significant assistance from LLMs and welcomes LLM-assisted contributions. When submitting LLM-generated code:

1. **Review the output** for correctness and quality before submitting
2. **Ensure proper testing** as LLMs may miss edge cases
3. **Mention LLM assistance** in your PR description for transparency
4. **Maintain high standards** - LLM contributions should meet the same quality bar as human-written code

## Key Concepts

Understanding these core concepts will help you contribute effectively:

### SQL Type Mapping

SQL2PyAPI maps PostgreSQL types to Python types. The mapping is extensible to support additional types.

### Return Type Handling

The generator handles various PostgreSQL return types:
- Scalar values (`INTEGER`, `TEXT`, etc.)
- Records (`RETURNS RECORD`)
- Tables (`RETURNS TABLE(...)`)
- Sets (`RETURNS SETOF ...`) 

### NULL Handling

Proper NULL handling is critical for correctness in database applications. The codebase handles:
- Functions that return no rows
- Functions that return rows with NULL values
- Composite types with NULL values

## Getting Started

1. **Set up your environment**:
   ```bash
   # Clone the repository
   git clone https://github.com/yourusername/sql2pyapi.git
   cd sql2pyapi
   
   # Create a virtual environment
   uv venv
   source .venv/bin/activate  # Or .venv\Scripts\activate on Windows
   
   # Install development dependencies
   uv pip install -e ".[dev]"
   ```

2. **Run the tests**:
   ```bash
   pytest
   ```

3. **Lint your code**:
   ```bash
   ruff check .
   ruff format .
   ```

## Finding Things to Work On

Check the GitHub issues to find bugs to fix or features to implement. If you have ideas for improvements, feel free to open a new issue to discuss them before implementation.

## Code of Conduct

Please be respectful and constructive in all interactions related to this project. We aim to maintain a welcoming and inclusive community for all contributors.
