"""
Utility functions for file and import handling in Python code generation.

This module provides helpers for normalizing import statements, resolving references, 
globbing files, and indenting code blocks.
"""

import ast
import re
from pathlib import Path

def normalize_import(stmt: str) -> str:
    """ Normalize whitespace in an import statement. """
    return re.sub(r"\s+", " ", stmt.strip())

def is_helper_import(stmt: str) -> bool:
    """
    Check if an import statement is a helper import from 'import_contents'.
    """
    try:
        tree = ast.parse(stmt)
    except SyntaxError:
        return False
    if not tree.body:
        return False
    node = tree.body[0]
    if isinstance(node, ast.ImportFrom):
        module = node.module or ""
        if module.endswith("import_contents"):
            return True
    return False

def resolve_ref(root_dir: Path, current_file: Path, ref: str) -> Path:
    """ Resolve a reference path relative to root or current file. """
    candidate = root_dir / ref
    if candidate.exists():
        return candidate.resolve()
    candidate = (current_file.parent / ref).resolve()
    if candidate.exists():
        return candidate
    raise FileNotFoundError(
        f"Could not resolve import_contents path '{ref}' from '{current_file}'"
    )

def glob_files(root_dir: Path, pattern: str) -> tuple:
    """ Return a sorted tuple of files matching a glob pattern under root_dir. """
    return tuple(sorted(path for path in root_dir.glob(pattern) if path.is_file()))

def indent_block(block: str, indent: str) -> str:
    """ Indent each non-empty line in a block of text. """
    lines = block.splitlines(keepends=True)
    output = []
    for line in lines:
        if line.strip():
            output.append(f"{indent}{line}")
        else:
            output.append(line)
    return "".join(output)

def deduplicate_and_merge_imports(imports: list[str]) -> list[str]:
    """
    Deduplicate and merge imports from the same module.
    
    Converts multiple imports from the same module into a single merged import:
        from airflow.sdk import task
        from airflow.sdk import get_current_context
        
    Into:
        from airflow.sdk import get_current_context, task
    
    Returns:
        A list of import statements with merged imports from the same module,
        sorted by module name, with imported names sorted alphabetically.
    """
    from_imports: dict[str, set[str]] = {}  # module -> set of names
    other_imports: list[str] = []
    
    for stmt in imports:
        try:
            tree = ast.parse(stmt)
        except SyntaxError:
            # If we can't parse, keep it as-is
            other_imports.append(stmt)
            continue
            
        if not tree.body:
            continue
            
        node = tree.body[0]
        if isinstance(node, ast.ImportFrom):
            module = node.module or ""
            if module not in from_imports:
                from_imports[module] = set()
            
            for alias in node.names:
                if alias.name == "*":
                    # Can't merge star imports, add separately
                    other_imports.append(stmt)
                else:
                    from_imports[module].add(alias.name)
        else:
            # Regular imports (e.g., import os, import sys)
            other_imports.append(stmt)
    
    result = []
    
    # Add merged from imports in sorted order
    for module in sorted(from_imports.keys()):
        names = sorted(from_imports[module])
        result.append(f"from {module} import {', '.join(names)}")
    
    # Add other imports (preserving their original order)
    result.extend(other_imports)
    
    return result
