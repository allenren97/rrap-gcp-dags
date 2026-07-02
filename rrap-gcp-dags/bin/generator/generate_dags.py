#!/usr/bin/env python3
"""Generate fully-expanded DAG files from template DAG definitions."""

from __future__ import annotations

import shutil
from pathlib import Path

from .inliner import build_expanded_file


def generate_dags(
    source_dir: Path,
    output_dir: Path,
    entry_glob: str,
    strict: bool,
    clean: bool,
) -> list[Path]:
    """Generate expanded DAG files from source templates into output directory."""
    source_dir = source_dir.resolve()
    output_dir = output_dir.resolve()

    if not source_dir.exists():
        raise FileNotFoundError(f"Source DAG template directory does not exist: {source_dir}")

    entry_files = sorted(path for path in source_dir.glob(entry_glob) if path.is_file())
    if not entry_files:
        raise FileNotFoundError(
            f"No entry DAG templates matched '{entry_glob}' under {source_dir}"
        )

    output_dir.mkdir(parents=True, exist_ok=True)

    if clean:
        for child in output_dir.iterdir():
            if child.name == ".gitkeep":
                continue
            if child.is_dir():
                shutil.rmtree(child)
            else:
                child.unlink()

    written: list[Path] = []
    for entry_file in entry_files:
        expanded = build_expanded_file(entry_file, source_dir, strict)
        rel = entry_file.relative_to(source_dir)
        target = output_dir / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(expanded, encoding="utf-8")
        written.append(target)

    return written
