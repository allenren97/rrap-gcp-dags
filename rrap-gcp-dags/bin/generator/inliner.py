"""Recursive inliner for DAG template snippets."""

from __future__ import annotations

from pathlib import Path
import re

import click

from .module_imports import extract_module_imports
from .patterns import CHAIN_RE, CHAIN_SEQUENCE_RE, DEDUPED_RE, PLACEHOLDER_RE
from .utils.feature_group_expansion import expand_feature_group_factory_loops
from .taskgroup_rewrites import rewrite_taskgroup_invocations
from .utils.ast_helpers import top_level_symbol_names, top_level_task_group_call_names
from .utils.chain_utils import parse_chain_sequence_args
from .utils.dataclasses import InlinedSnippet
from .utils.file_utils import glob_files, indent_block, is_helper_import, normalize_import, resolve_ref, deduplicate_and_merge_imports
from .utils.rename_helpers import build_rename_map, load_naming_config, rename_symbols


def build_inlined_snippet(
    ref_path: Path,
    root_dir: Path,
    strict: bool,
    stack: set[Path],
    cache: dict[Path, InlinedSnippet],
) -> InlinedSnippet:
    """Build one snippet, recursively expanding import_contents markers."""
    ref_path = ref_path.resolve()
    if ref_path in cache:
        return cache[ref_path]

    if ref_path in stack:
        cycle = " -> ".join(str(path) for path in [*stack, ref_path])
        raise ValueError(f"Cyclic import_contents reference detected: {cycle}")

    stack.add(ref_path)
    try:
        source = ref_path.read_text(encoding="utf-8")
        nested_imports: list[str] = []

        def resolve_or_warn(nested_ref: str) -> Path | None:
            try:
                return resolve_ref(root_dir, ref_path, nested_ref)
            except FileNotFoundError as exc:
                if strict:
                    raise
                click.echo(f"WARN: {exc}", err=True)
                return None

        def replace_deduped(match: "re.Match[str]") -> str:
            pattern = match.group("pattern")
            strict_local = match.group("strict") == "True"
            matches = glob_files(root_dir, pattern)
            if strict_local and not matches:
                raise FileNotFoundError(
                    f"No files matched pattern '{pattern}' from '{ref_path}'"
                )
            for match_path in matches:
                if match_path.resolve() == ref_path:
                    continue
                snippet = build_inlined_snippet(
                    match_path.resolve(),
                    root_dir,
                    strict,
                    stack,
                    cache,
                )
                nested_imports.extend(snippet.imports)
            return ""

        source = DEDUPED_RE.sub(replace_deduped, source)

        def replace_chain(match: "re.Match[str]") -> str:
            indent = match.group("indent")
            left_path = resolve_or_warn(match.group("left"))
            right_path = resolve_or_warn(match.group("right"))
            if left_path is None or right_path is None:
                return match.group(0)

            left_snippet = build_inlined_snippet(left_path, root_dir, strict, stack, cache)
            right_snippet = build_inlined_snippet(right_path, root_dir, strict, stack, cache)
            nested_imports.extend(left_snippet.imports)
            nested_imports.extend(right_snippet.imports)

            parts: list[str] = []
            left_body = left_snippet.body.strip("\n")
            right_body = right_snippet.body.strip("\n")
            if left_body:
                parts.append(indent_block(left_body + "\n", indent).rstrip("\n"))
            if right_body:
                parts.append(indent_block(right_body + "\n", indent).rstrip("\n"))

            if left_snippet.last_task_symbol and right_snippet.first_task_symbol:
                parts.append(
                    f"{indent}{left_snippet.last_task_symbol} >> {right_snippet.first_task_symbol}"
                )
            else:
                msg = (
                    "chain_import_contents requires BOTH files to define top-level "
                    "FIRST_TASK and LAST_TASK variables."
                )
                if strict:
                    raise ValueError(msg)
                click.echo(f"WARN: {msg}", err=True)

            return "\n".join(parts)

        source = CHAIN_RE.sub(replace_chain, source)

        def replace_chain_sequence(match: "re.Match[str]") -> str:
            indent = match.group("indent")
            args_text = match.group("args")

            try:
                stages = parse_chain_sequence_args(args_text)
            except ValueError as exc:
                if strict:
                    raise
                click.echo(f"WARN: {exc}", err=True)
                return match.group(0)

            stage_paths: list[list[Path]] = []
            all_unique_paths: list[Path] = []
            seen_paths: set[Path] = set()
            for refs in stages:
                stage: list[Path] = []
                stage_seen: set[Path] = set()
                for ref in refs:
                    path = resolve_or_warn(ref)
                    if path is None:
                        continue
                    path = path.resolve()
                    if path in stage_seen:
                        continue
                    stage_seen.add(path)
                    stage.append(path)
                    if path not in seen_paths:
                        seen_paths.add(path)
                        all_unique_paths.append(path)
                if stage:
                    stage_paths.append(stage)

            if len(stage_paths) < 2:
                msg = "chain_import_sequence requires at least two resolvable stages"
                if strict:
                    raise ValueError(msg)
                click.echo(f"WARN: {msg}", err=True)
                return match.group(0)

            snippet_by_path: dict[Path, InlinedSnippet] = {}
            for path in all_unique_paths:
                snippet = build_inlined_snippet(path, root_dir, strict, stack, cache)
                snippet_by_path[path] = snippet
                nested_imports.extend(snippet.imports)

            parts: list[str] = []
            for path in all_unique_paths:
                block = snippet_by_path[path].body.strip("\n")
                if block:
                    parts.append(indent_block(block + "\n", indent).rstrip("\n"))

            def add_link(left: InlinedSnippet, right: InlinedSnippet) -> None:
                if left.last_task_symbol and right.first_task_symbol:
                    parts.append(f"{indent}{left.last_task_symbol} >> {right.first_task_symbol}")
                    return
                msg = (
                    "chain_import_sequence requires each file to define top-level "
                    "FIRST_TASK/LAST_TASK (or *_FIRST_TASK/*_LAST_TASK)."
                )
                if strict:
                    raise ValueError(msg)
                click.echo(f"WARN: {msg}", err=True)

            for left_stage, right_stage in zip(stage_paths, stage_paths[1:]):
                for left_path in left_stage:
                    for right_path in right_stage:
                        add_link(snippet_by_path[left_path], snippet_by_path[right_path])

            return "\n".join(parts)

        source = CHAIN_SEQUENCE_RE.sub(replace_chain_sequence, source)

        def replace_nested(match: "re.Match[str]") -> str:
            indent = match.group("indent")
            nested_path = resolve_or_warn(match.group("path"))
            if nested_path is None:
                return match.group(0)

            nested_snippet = build_inlined_snippet(nested_path, root_dir, strict, stack, cache)
            nested_imports.extend(nested_snippet.imports)
            body = nested_snippet.body.strip("\n")

            parts: list[str] = []
            if body:
                parts.append(indent_block(body + "\n", indent).rstrip("\n"))

            if nested_snippet.taskgroup_instance_vars:
                for tg_var in nested_snippet.taskgroup_instance_vars:
                    parts.append(f"{indent}handle_month_context >> {tg_var}")

            return "\n".join(parts)

        expanded = PLACEHOLDER_RE.sub(replace_nested, source)
        own_imports, body = extract_module_imports(expanded)
        body = expand_feature_group_factory_loops(body)

        naming = load_naming_config(ref_path, root_dir)
        rename_map = build_rename_map(body, naming)
        renamed_body = rename_symbols(body, rename_map)

        task_group_call_names = top_level_task_group_call_names(body)
        renamed_task_group_call_names = [rename_map.get(name, name) for name in task_group_call_names]
        renamed_body, taskgroup_instance_vars = rewrite_taskgroup_invocations(
            renamed_body,
            renamed_task_group_call_names,
        )

        cleaned_imports = [
            stmt for stmt in [*nested_imports, *own_imports] if not is_helper_import(stmt)
        ]
        top_symbols = top_level_symbol_names(body)

        def resolve_endpoint(explicit: str, suffix: str) -> str | None:
            if explicit in top_symbols:
                return rename_map.get(explicit, explicit)
            matches = sorted(symbol for symbol in top_symbols if symbol.endswith(suffix))
            if len(matches) == 1:
                symbol = matches[0]
                return rename_map.get(symbol, symbol)
            return None

        snippet = InlinedSnippet(
            imports=cleaned_imports,
            body=renamed_body,
            first_task_symbol=resolve_endpoint("FIRST_TASK", "_FIRST_TASK"),
            last_task_symbol=resolve_endpoint("LAST_TASK", "_LAST_TASK"),
            taskgroup_instance_vars=taskgroup_instance_vars,
        )
        cache[ref_path] = snippet
        return snippet
    finally:
        stack.discard(ref_path)


def build_expanded_file(entry_file: Path, source_root: Path, strict: bool) -> str:
    """Render one entry DAG template into a fully expanded DAG file."""
    snippet = build_inlined_snippet(
        entry_file.resolve(),
        source_root.resolve(),
        strict,
        set(),
        {},
    )

    seen: set[str] = set()
    ordered_imports: list[str] = []
    for stmt in snippet.imports:
        norm = normalize_import(stmt)
        if norm in seen:
            continue
        seen.add(norm)
        ordered_imports.append(stmt)

    # Merge imports from the same module
    merged_imports = deduplicate_and_merge_imports(ordered_imports)
    imports_block = "\n".join(merged_imports).strip()
    body_block = snippet.body.strip("\n")

    if imports_block and body_block:
        return imports_block + "\n\n\n" + body_block + "\n"
    if imports_block:
        return imports_block + "\n"
    return body_block + "\n"
