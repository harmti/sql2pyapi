"""
Dependency resolver for ordering dataclass generation.

This module analyzes dependencies between dataclasses and provides
a topologically sorted order for generation to avoid forward references.
"""

import re
from typing import Dict, List, Set, Tuple, Optional
from collections import defaultdict, deque
import logging

from ..sql_models import ReturnColumn
from ..parser.utils import _to_singular_camel_case


def extract_class_references(python_type: str) -> Set[str]:
    """
    Extract class names referenced in a Python type annotation.
    
    Args:
        python_type: Type annotation string (e.g., "Optional[User]", "List[Company]")
        
    Returns:
        Set of referenced class names
    """
    # Pattern to match class names in type annotations
    # This handles Optional[ClassName], List[ClassName], Dict[str, ClassName], etc.
    # We look for capitalized identifiers that are not Python built-ins
    
    # Remove generic type wrappers to find the actual class names
    # First, handle nested generics by repeatedly removing outer wrappers
    type_str = python_type
    
    # Common built-in types that should not be considered as class references
    builtin_types = {
        'str', 'int', 'float', 'bool', 'bytes', 'None', 
        'Any', 'Dict', 'List', 'Tuple', 'Set', 'Optional', 'Union',
        'UUID', 'Decimal', 'datetime', 'date', 'time', 'timedelta',
        'JSONDict', 'JSONList', 'JSONValue'
    }
    
    # Pattern to find potential class names (PascalCase identifiers)
    class_pattern = r'[A-Z][a-zA-Z0-9]*'
    
    # Find all potential class names
    potential_classes = re.findall(class_pattern, type_str)
    
    # Filter out built-in types and generic type constructors
    class_refs = {
        cls for cls in potential_classes 
        if cls not in builtin_types
    }
    
    return class_refs


def analyze_dataclass_dependencies(
    dataclasses: Dict[str, List[ReturnColumn]]
) -> Dict[str, Set[str]]:
    """
    Analyze dependencies between dataclasses based on their field types.
    
    Args:
        dataclasses: Dict mapping dataclass names to their column definitions
        
    Returns:
        Dict mapping each dataclass name to set of dataclass names it depends on
    """
    dependencies = defaultdict(set)
    
    # For each dataclass, analyze its fields
    for class_name, columns in dataclasses.items():
        for column in columns:
            # Extract class references from the column's Python type
            refs = extract_class_references(column.python_type)
            
            # Only include references that are actually other dataclasses
            # and not self-references
            for ref in refs:
                if ref in dataclasses and ref != class_name:
                    dependencies[class_name].add(ref)
                    logging.debug(f"Dependency found: {class_name} -> {ref} (field: {column.name}: {column.python_type})")
    
    return dict(dependencies)


def topological_sort(
    nodes: Set[str],
    dependencies: Dict[str, Set[str]]
) -> Tuple[List[str], Optional[List[str]]]:
    """
    Perform topological sort on the dependency graph.
    
    Uses Kahn's algorithm for topological sorting.
    
    Args:
        nodes: Set of all node names (dataclass names)
        dependencies: Dict mapping node to set of nodes it depends on
        
    Returns:
        Tuple of (sorted_list, cycle) where:
        - sorted_list: Topologically sorted list of nodes (dependencies first)
        - cycle: List of nodes involved in a cycle (if any), None otherwise
    """
    # Create a copy of dependencies to avoid modifying the original
    deps = {node: set(dependencies.get(node, set())) for node in nodes}
    
    # Calculate in-degrees (how many nodes depend on this node)
    # If A depends on B, then B must come before A (B has no dependencies on A)
    in_degree = defaultdict(int)
    for node in nodes:
        in_degree[node] = len(deps.get(node, set()))
    
    # Queue of nodes with no dependencies (in-degree 0)
    queue = deque([node for node in nodes if in_degree[node] == 0])
    sorted_list = []
    
    while queue:
        # Remove node with no dependencies
        current = queue.popleft()
        sorted_list.append(current)
        
        # For all nodes that depend on current, reduce their in-degree
        for node, node_deps in deps.items():
            if current in node_deps:
                in_degree[node] -= 1
                if in_degree[node] == 0:
                    queue.append(node)
    
    # Check if all nodes were processed (no cycles)
    if len(sorted_list) != len(nodes):
        # Find nodes involved in cycle
        remaining = [node for node in nodes if node not in sorted_list]
        logging.error(f"Circular dependency detected involving: {remaining}")
        return sorted_list, remaining
    
    return sorted_list, None


def collect_referenced_table_types(
    custom_types: Dict[str, List[ReturnColumn]],
    table_schemas: Dict[str, List[ReturnColumn]]
) -> Dict[str, List[ReturnColumn]]:
    """
    Collect all table types that are referenced by composite types.
    
    This ensures that if a composite type has a field of a table type,
    that table type will be included in the generated dataclasses.
    
    Args:
        custom_types: Currently known custom types (composite types, etc.)
        table_schemas: Available table schemas from parsing
        
    Returns:
        Dict with all custom types plus any referenced table types
    """
    result = custom_types.copy()
    
    # Keep track of what we've already processed to avoid infinite loops
    processed = set()
    to_process = list(custom_types.keys())
    
    while to_process:
        current_type = to_process.pop()
        if current_type in processed:
            continue
        processed.add(current_type)
        
        # Check if this type has columns that reference other types
        if current_type in result:
            columns = result[current_type]
            for column in columns:
                # Check if the SQL type is a table reference
                sql_type = column.sql_type
                if sql_type in table_schemas and sql_type not in result:
                    # Add the referenced table to our result
                    logging.debug(f"Adding referenced table type: {sql_type}")
                    result[sql_type] = table_schemas[sql_type]
                    # Add it to the processing queue
                    to_process.append(sql_type)
    
    return result


def resolve_dataclass_order(
    custom_types: Dict[str, List[ReturnColumn]],
    table_schemas: Optional[Dict[str, List[ReturnColumn]]] = None
) -> List[Tuple[str, List[ReturnColumn]]]:
    """
    Resolve the correct order for dataclass generation based on dependencies.
    
    Args:
        custom_types: Dict mapping SQL type names to their column definitions
        table_schemas: Optional dict of available table schemas
        
    Returns:
        List of (type_name, columns) tuples in dependency order
    """
    # First, collect all referenced table types
    if table_schemas:
        all_types = collect_referenced_table_types(custom_types, table_schemas)
    else:
        all_types = custom_types
    
    # Create a mapping of Python class names to their data
    # This is because dependencies are between Python class names, not SQL names
    
    class_to_sql_name = {}
    class_to_columns = {}
    
    for sql_type_name, columns in all_types.items():
        # Determine Python class name
        if sql_type_name.endswith("Result"):
            # Ad-hoc result classes keep their name
            class_name = sql_type_name
        else:
            # Convert SQL name to Python class name
            class_name = _to_singular_camel_case(sql_type_name)
        
        class_to_sql_name[class_name] = sql_type_name
        class_to_columns[class_name] = columns
    
    # Analyze dependencies between Python classes
    dependencies = analyze_dataclass_dependencies(class_to_columns)
    
    # Get all class names
    all_classes = set(class_to_columns.keys())
    
    # Perform topological sort
    sorted_classes, cycle = topological_sort(all_classes, dependencies)
    
    if cycle:
        # Still return a valid order, but with cycle warning
        logging.warning(f"Circular dependency detected in dataclasses: {cycle}")
        # Add remaining classes at the end
        for cls in cycle:
            if cls not in sorted_classes:
                sorted_classes.append(cls)
    
    # Convert back to (sql_type_name, columns) tuples in sorted order
    result = []
    for class_name in sorted_classes:
        sql_name = class_to_sql_name[class_name]
        columns = class_to_columns[class_name]
        result.append((sql_name, columns))
        logging.debug(f"Dataclass order: {class_name} (SQL: {sql_name})")
    
    return result