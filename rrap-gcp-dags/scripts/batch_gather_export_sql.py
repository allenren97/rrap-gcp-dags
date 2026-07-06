#!/usr/bin/env python3
"""Rewrite gather export SQL into MATERIALIZED CTE join batches (spill control)."""

from __future__ import annotations

import re
import sys
from pathlib import Path

JOIN_BATCH_SIZE = 21
JOIN_RE = re.compile(
    r"LEFT JOIN (?P<table>\S+) (?P<alias>\S+)\s+"
    r"ON sp\.BASEL_ACCT_ID = (?P=alias)\.BASEL_ACCT_ID\s+"
    r"AND sp\.OBSN_DT = (?P=alias)\.OBSN_DT\s+"
    r"AND sp\.STREAM = (?P=alias)\.STREAM",
    re.MULTILINE,
)
FEATURE_JOIN_RE = re.compile(
    r"LEFT JOIN (?P<table>\S+) (?P<alias>\S+)\s+"
    r"ON sp\.BASEL_ACCT_ID = (?P=alias)\.BASEL_ACCT_ID\s+"
    r"AND sp\.OBSN_DT = (?P=alias)\.OBSN_DT(?:\s+AND sp\.SRC_SYS_CD = (?P=alias)\.SRC_SYS_CD)?",
    re.MULTILINE,
)
COL_RE = re.compile(r"^\s+(?P<expr>.+?) AS (?P<out>[^,\s]+),?\s*$")


def _indent_join(join_block: str, prev_alias: str) -> str:
    lines = join_block.strip().splitlines()
    return "\n".join(
        f"        {line.strip().replace('sp.', f'{prev_alias}.')}" for line in lines
    )


def transform(content: str) -> str:
    spine_m = re.search(r"(WITH spine AS \([\s\S]*?\))\nSELECT\n", content)
    if not spine_m:
        raise ValueError("Could not parse spine CTE")

    spine = spine_m.group(1)
    rest = content[spine_m.end() :]

    select_end = rest.index("FROM spine sp")
    select_body = rest[:select_end]
    joins_body = rest[select_end + len("FROM spine sp") :].strip()

    cols = []
    for line in select_body.splitlines():
        m = COL_RE.match(line)
        if not m:
            continue
        expr = m.group("expr")
        alias = expr.split(".", 1)[0]
        col = expr.split(".", 1)[1] if "." in expr else expr
        cols.append({"alias": alias, "col": col, "out": m.group("out")})

    instrument_joins = []
    feature_joins = []
    pos = 0
    while pos < len(joins_body):
        m = JOIN_RE.match(joins_body, pos)
        if m:
            instrument_joins.append(joins_body[m.start() : m.end()])
            pos = m.end()
            continue
        m = FEATURE_JOIN_RE.match(joins_body, pos)
        if m:
            feature_joins.append(joins_body[m.start() : m.end()])
            pos = m.end()
            continue
        if joins_body[pos : pos + 1].isspace():
            pos += 1
            continue
        raise ValueError(f"Unparsed join at offset {pos}: {joins_body[pos:pos+80]!r}")

    alias_to_join = {}
    for block in instrument_joins + feature_joins:
        alias = re.search(r"LEFT JOIN \S+ (\S+)", block).group(1)
        alias_to_join[alias] = block

    instrument_cols = [c for c in cols if c["alias"].startswith("j")]
    feature_cols = [c for c in cols if c["alias"].startswith("fe_")]

    batches: list[list[dict]] = []
    current: list[dict] = []
    for col in instrument_cols:
        current.append(col)
        if len(current) >= JOIN_BATCH_SIZE:
            batches.append(current)
            current = []
    if current:
        batches.append(current)
    if feature_cols:
        batches.append(feature_cols)

    parts: list[str] = []
    prev_alias = "sp"
    prev_part = "spine"

    for idx, batch in enumerate(batches, start=1):
        part_name = f"part{idx}"
        if prev_alias == "sp":
            select_lines = [
                "        sp.OBSN_DT",
                "        sp.STREAM",
                "        sp.BASEL_ACCT_ID",
                "        sp.SRC_SYS_CD",
                "        sp.PIT_STAT_CD",
            ]
        else:
            select_lines = [f"        {prev_alias}.*"]

        for col in batch:
            select_lines.append(f"        {col['alias']}.{col['col']} AS {col['out']}")

        join_blocks = [
            _indent_join(alias_to_join[col["alias"]], prev_alias) for col in batch
        ]

        parts.append(
            f",\n{part_name} AS MATERIALIZED (\n"
            f"    SELECT\n"
            + ",\n".join(select_lines)
            + f"\n    FROM {prev_part} {prev_alias}\n"
            + "\n".join(join_blocks)
            + "\n)"
        )
        prev_alias = part_name
        prev_part = part_name

    final_select = []
    for line in select_body.splitlines():
        stripped = line.strip().rstrip(",")
        if not stripped:
            continue
        if stripped.startswith("sp."):
            final_select.append("    " + stripped.replace("sp.", f"{prev_alias}.", 1))
        elif "{{ task_instance" in stripped:
            final_select.append("    " + stripped)
        elif stripped.startswith("CURRENT_TIMESTAMP"):
            final_select.append("    " + stripped)
        else:
            m = COL_RE.match(line)
            if m:
                final_select.append(f"    {prev_alias}.{m.group('out')}")

    return (
        spine
        + "".join(parts)
        + "\nSELECT\n"
        + ",\n".join(final_select)
        + f"\nFROM {prev_part} {prev_alias}\n"
    )


def main(paths: list[str]) -> int:
    for raw in paths:
        path = Path(raw)
        transformed = transform(path.read_text())
        path.write_text(transformed)
        print(f"rewrote {path}")
    return 0


if __name__ == "__main__":
    root = Path(__file__).resolve().parents[1]
    targets = sys.argv[1:] or [
        str(p)
        for p in sorted(
            root.glob("conf/*/instruments/fact/basel_analytcl_bl_instrmnt_fact.export_*.sql")
        )
    ]
    raise SystemExit(main(targets))
