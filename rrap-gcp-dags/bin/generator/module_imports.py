"""Helpers for extracting and removing module-level imports."""

from __future__ import annotations

import ast

from .utils.ast_helpers import line_span


def extract_module_imports(source: str) -> tuple[list[str], str]:
    """Return top-level imports and source body with those imports removed."""
    tree = ast.parse(source)
    imports: list[tuple[int, int, str]] = []
    for stmt in tree.body:
        if isinstance(stmt, (ast.Import, ast.ImportFrom)):
            start, end = line_span(source, stmt)
            imports.append((start, end, source[start:end].rstrip("\n")))

    if not imports:
        return [], source

    chunks: list[str] = []
    cursor = 0
    for start, end, _ in sorted(imports, key=lambda item: item[0]):
        chunks.append(source[cursor:start])
        cursor = end
    chunks.append(source[cursor:])

    body = "".join(chunks).lstrip("\n")
    return [item[2] for item in imports], body
