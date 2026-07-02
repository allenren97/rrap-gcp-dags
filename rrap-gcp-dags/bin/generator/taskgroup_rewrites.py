"""TaskGroup invocation rewriting helpers."""

from __future__ import annotations

import re


def rewrite_taskgroup_invocations(
    body: str,
    renamed_call_names: list[str],
) -> tuple[str, list[str]]:
    """Capture top-level TaskGroup calls into instance vars for later wiring."""
    rewritten = body
    instance_vars: list[str] = []

    for idx, call_name in enumerate(renamed_call_names, start=1):
        var_name = f"__imported_taskgroup_{idx}"
        pattern = re.compile(
            rf"^(?P<indent>[ \t]*){re.escape(call_name)}\(\)\s*$",
            re.MULTILINE,
        )

        def repl(match: re.Match[str]) -> str:
            return f"{match.group('indent')}{var_name} = {call_name}()"

        rewritten, count = pattern.subn(repl, rewritten, count=1)
        if count:
            instance_vars.append(var_name)

    return rewritten, instance_vars

