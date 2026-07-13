"""Rewrite helpers to expand config-driven feature-group loops into explicit calls.

This module intentionally uses text-preserving rewrites (regex + AST lookups)
instead of full-source AST unparse to keep original SQL string formatting.
"""

from __future__ import annotations

import ast
import re


_FEATURE_LOOP_RE = re.compile(
    r"(?P<indent>^[ \t]*)features\s*=\s*(?P<getter>\w+)\(\)\s*\n"
    r"(?:^[ \t]*(?:#.*)?\n)*"
    r"(?P=indent)feature_groups\s*=\s*\[(?P<factory>\w+)\(feature_config\)\s+for\s+feature_config\s+in\s+features\]\s*\n"
    r"(?:^[ \t]*(?:#.*)?\n)*"
    r"(?P=indent)for\s+upstream,\s*downstream\s+in\s+zip\(feature_groups,\s*feature_groups\[1:\]\):\s*\n"
    r"(?P=indent)[ \t]+upstream\s*>>\s*downstream\s*$",
    re.MULTILINE,
)


def _getter_to_list_map(tree: ast.Module) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for node in tree.body:
        if not isinstance(node, ast.FunctionDef):
            continue
        for stmt in node.body:
            if not isinstance(stmt, ast.If):
                continue
            if not isinstance(stmt.test, ast.Compare):
                continue
            if len(stmt.test.ops) != 1 or not isinstance(stmt.test.ops[0], ast.Is):
                continue
            if len(stmt.test.comparators) != 1:
                continue
            comp = stmt.test.comparators[0]
            if not isinstance(comp, ast.Constant) or comp.value is not None:
                continue
            if not isinstance(stmt.test.left, ast.Name):
                continue
            for nested in stmt.body:
                if isinstance(nested, ast.Return) and isinstance(nested.value, ast.Name):
                    mapping[node.name] = nested.value.id
    return mapping


def _list_symbol_elements(tree: ast.Module) -> dict[str, list[str]]:
    mapping: dict[str, list[str]] = {}
    for node in tree.body:
        if not isinstance(node, ast.Assign) or len(node.targets) != 1:
            continue
        target = node.targets[0]
        if not isinstance(target, ast.Name):
            continue
        if not isinstance(node.value, ast.List):
            continue
        names: list[str] = []
        for elt in node.value.elts:
            if not isinstance(elt, ast.Name):
                names = []
                break
            names.append(elt.id)
        if names:
            mapping[target.id] = names
    return mapping


def expand_feature_group_factory_loops(source: str) -> str:
    """Expand dynamic feature factory loops into explicit statements.

    Example transform:
      features = get_at94_config()
      feature_groups = [create_chunked_feature_group(feature_config) for feature_config in features]
      for upstream, downstream in zip(feature_groups, feature_groups[1:]):
          upstream >> downstream

    Becomes:
      feature_group_1 = create_chunked_feature_group(AT94_CONFIG)
      feature_group_2 = create_chunked_feature_group(AT94_MIN24M_CONFIG)
      feature_group_1 >> feature_group_2
    """
    tree = ast.parse(source)
    getter_map = _getter_to_list_map(tree)
    list_map = _list_symbol_elements(tree)

    if not getter_map or not list_map:
        return source

    def repl(match: re.Match[str]) -> str:
        indent = match.group("indent")
        getter = match.group("getter")
        factory = match.group("factory")

        list_symbol = getter_map.get(getter)
        if not list_symbol:
            return match.group(0)

        configs = list_map.get(list_symbol)
        if not configs:
            return match.group(0)

        lines: list[str] = []
        for idx, config_name in enumerate(configs, start=1):
            lines.append(
                f"{indent}feature_group_{idx} = {factory}({config_name})"
            )
        for idx in range(1, len(configs)):
            lines.append(
                f"{indent}feature_group_{idx} >> feature_group_{idx + 1}"
            )
        return "\n".join(lines)

    return _FEATURE_LOOP_RE.sub(repl, source)
