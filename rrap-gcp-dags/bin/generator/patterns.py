"""Regex patterns used by DAG template expansion."""

from __future__ import annotations

import re

PLACEHOLDER_RE = re.compile(
    r"^(?P<indent>[ \t]*)import_contents\([\"'](?P<path>[^\"']+)[\"']\)\s*$",
    re.MULTILINE,
)

DEDUPED_RE = re.compile(
    r"^(?P<indent>[ \t]*)deduped_imports\("
    r"[\"'](?P<pattern>[^\"']+)[\"']"
    r"(?:\s*,\s*strict\s*=\s*(?P<strict>True|False))?"
    r"\)\s*$",
    re.MULTILINE,
)

CHAIN_RE = re.compile(
    r"^(?P<indent>[ \t]*)chain_import_contents\("
    r"[\"'](?P<left>[^\"']+)[\"']\s*,\s*[\"'](?P<right>[^\"']+)[\"']"
    r"\)\s*$",
    re.MULTILINE,
)

CHAIN_SEQUENCE_RE = re.compile(
    r"^(?P<indent>[ \t]*)chain_import_sequence\((?P<args>.*?)\)\s*$",
    re.MULTILINE | re.DOTALL,
)
