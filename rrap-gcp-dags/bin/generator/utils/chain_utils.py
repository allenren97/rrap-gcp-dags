"""
Utilities for parsing and validating chain import sequence arguments for code generation.
"""

import ast

def parse_chain_sequence_args(args_text: str) -> list[list[str]]:
    """
    Parse and validate arguments for a chain import sequence.

    Args:
        args_text (str): The argument text to parse, e.g., '"a", ["b", "c"]'.
    Returns:
        list[list[str]]: A list of stages, each stage is a list of string references.
    Raises:
        ValueError: If the arguments are invalid or do not meet requirements.
    """
    call = ast.parse(f"f({args_text})", mode="eval").body
    if not isinstance(call, ast.Call):
        raise ValueError("Invalid chain_import_sequence call")
    if call.keywords:
        raise ValueError(
            "chain_import_sequence does not accept keyword args; use staged string/list arguments instead"
        )
    stages = []
    for arg in call.args:
        if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
            stages.append([arg.value])
            continue
        if isinstance(arg, (ast.List, ast.Tuple)):
            refs = []
            for elt in arg.elts:
                if not (isinstance(elt, ast.Constant) and isinstance(elt.value, str)):
                    raise ValueError(
                        "chain_import_sequence list/tuple values must be string literals"
                    )
                refs.append(elt.value)
            if not refs:
                raise ValueError("chain_import_sequence list/tuple arguments cannot be empty")
            stages.append(refs)
            continue
        raise ValueError(
            "chain_import_sequence arguments must be either a string literal or a list/tuple of string literals"
        )
    if len(stages) < 2:
        raise ValueError("chain_import_sequence requires at least two stages")
    return stages
