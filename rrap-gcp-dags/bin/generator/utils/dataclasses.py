"""
Data structures for naming configuration and inlined code snippets used in code generation utilities.
"""
from dataclasses import dataclass
from typing import Optional, List, Dict


@dataclass(frozen=True)
class NamingConfig:
    """
    Configuration for symbol renaming, including a prefix and a rename mapping.
    Attributes:
        prefix (str): The prefix to use for renamed symbols.
        rename (Dict[str, str]): Mapping of original symbol names to new names.
    """
    prefix: str
    rename: Dict[str, str]

@dataclass(frozen=True)
class InlinedSnippet:
    """
    Represents an inlined code snippet with associated imports and metadata for task groups.
    Attributes:
        imports (List[str]): List of import statements required by the snippet.
        body (str): The main code body of the snippet.
        first_task_symbol (Optional[str]): Symbol name of the first task, if any.
        last_task_symbol (Optional[str]): Symbol name of the last task, if any.
        taskgroup_instance_vars (Optional[List[str]]): Instance variables for task groups, if any.
    """
    imports: List[str]
    body: str
    first_task_symbol: Optional[str] = None
    last_task_symbol: Optional[str] = None
    taskgroup_instance_vars: Optional[List[str]] = None
