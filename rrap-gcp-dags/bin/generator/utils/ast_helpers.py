"""
Helpers for analyzing Python source code using the AST (Abstract Syntax Tree).

Provides utilities for extracting top-level symbol names, function names, and 
ask group call names, as well as calculating line spans for AST nodes.
"""

import ast
from typing import Set, List

def line_span(source: str, node: ast.AST) -> tuple[int, int]:
    """ Calculate the character offset span for an AST node in the source code. """
    if not hasattr(node, "lineno") or not hasattr(node, "end_lineno"):
        raise ValueError("AST node does not expose line information")
    
    lines = source.splitlines(keepends=True)
    start_offset = sum(len(line) for line in lines[: node.lineno - 1])
    end_offset = sum(len(line) for line in lines[: node.end_lineno])

    return start_offset, end_offset

def top_level_symbol_names(source: str) -> Set[str]:
    """ Extract all top-level symbol names (functions, classes, and variables) from source code. """
    tree = ast.parse(source)
    symbols: Set[str] = set()

    for stmt in tree.body:
        if isinstance(stmt, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            symbols.add(stmt.name)
        elif isinstance(stmt, ast.Assign):
            for target in stmt.targets:
                if isinstance(target, ast.Name):
                    symbols.add(target.id)
        elif isinstance(stmt, ast.AnnAssign) and isinstance(stmt.target, ast.Name):
            symbols.add(stmt.target.id)
    
    return symbols

def top_level_function_names(source: str) -> Set[str]:
    """ Extract all top-level function names from source code. """
    tree = ast.parse(source)
    functions: Set[str] = set()

    for stmt in tree.body:
        if isinstance(stmt, (ast.FunctionDef, ast.AsyncFunctionDef)):
            functions.add(stmt.name)
    
    return functions

def top_level_task_group_call_names(source: str) -> List[str]:
    """ Extract names of top-level calls to functions decorated with @task_group. """
    tree = ast.parse(source)
    task_group_defs: Set[str] = set()

    for stmt in tree.body:
        if not isinstance(stmt, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        for deco in stmt.decorator_list:
            if isinstance(deco, ast.Name) and deco.id == "task_group":
                task_group_defs.add(stmt.name)
                break
            if isinstance(deco, ast.Call):
                func = deco.func
                if isinstance(func, ast.Name) and func.id == "task_group":
                    task_group_defs.add(stmt.name)
                    break
    
    call_names: List[str] = []
    
    for stmt in tree.body:
        if not isinstance(stmt, ast.Expr) or not isinstance(stmt.value, ast.Call):
            continue
        call = stmt.value
        if call.args or call.keywords:
            continue
        if isinstance(call.func, ast.Name) and call.func.id in task_group_defs:
            call_names.append(call.func.id)
    
    return call_names
