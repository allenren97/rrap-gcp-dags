"""
Utilities for renaming top-level Python symbols in source files based on naming 
conventions and configuration files.

This module provides functions to build rename maps, load naming configurations, 
and apply renaming to Python source code using tokenization.
"""
import io
import json
import tokenize
from pathlib import Path
from .dataclasses import NamingConfig
from .ast_helpers import top_level_symbol_names, top_level_function_names


def default_prefix_for(source_path: Path, root_dir: Path) -> str:
    """ Generate a default prefix for renaming based on the source file name. """
    # Keep symbols stable by default in materialized DAG output.
    # Explicit renames can still be provided via naming.json.
    return ""

def load_naming_config(source_path: Path, root_dir: Path) -> NamingConfig:
    """ Load naming configuration for a source file, including prefix and rename map. """
    rel_parts = source_path.relative_to(root_dir).parts
    rename = {}
    prefix = default_prefix_for(source_path, root_dir)
    if "taskgroups" in rel_parts:
        idx = rel_parts.index("taskgroups")
        if idx + 1 < len(rel_parts):
            group_dir = root_dir / "taskgroups" / rel_parts[idx + 1]
            cfg_path = group_dir / "naming.json"
            if cfg_path.exists():
                data = json.loads(cfg_path.read_text(encoding="utf-8"))
                if isinstance(data.get("rename"), dict):
                    rename = {
                        str(key): str(value)
                        for key, value in data["rename"].items()
                        if str(key).strip() and str(value).strip()
                    }
    return NamingConfig(prefix=prefix, rename=rename)

def build_rename_map(source: str, naming: NamingConfig) -> dict:
    """ Build a rename map for top-level symbols in the source code based on naming config. """
    symbols = top_level_symbol_names(source)
    function_symbols = top_level_function_names(source)
    rename_map = {}
    if naming.prefix:
        lower_prefix = naming.prefix.lower()
        upper_prefix = naming.prefix.upper()
        for symbol in symbols:
            if symbol.startswith("__") and symbol.endswith("__"):
                continue
            prefix = lower_prefix if symbol in function_symbols else upper_prefix
            rename_map[symbol] = f"{prefix}_{symbol}"
    for old_name, new_name in naming.rename.items():
        if not new_name.isidentifier():
            raise ValueError(
                f"Invalid naming.json rename target '{new_name}' for symbol '{old_name}'. Rename values must be valid Python identifiers."
            )
        rename_map[old_name] = new_name
    return rename_map

def rename_symbols(source: str, rename_map: dict) -> str:
    """ Rename symbols in the source code using the provided rename map. """
    if not rename_map:
        return source
    reader = io.StringIO(source).readline
    tokens = list(tokenize.generate_tokens(reader))
    updated = []
    for token in tokens:
        if token.type == tokenize.NAME and token.string in rename_map:
            updated.append(
                tokenize.TokenInfo(
                    token.type,
                    rename_map[token.string],
                    token.start,
                    token.end,
                    token.line,
                )
            )
        else:
            updated.append(token)
    return tokenize.untokenize(updated)
