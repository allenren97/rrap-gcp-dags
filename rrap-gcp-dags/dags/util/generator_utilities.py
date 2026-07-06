"""
All required helper functions for dynamic generation of dags
"""

from __future__ import annotations
import ast
import importlib.util
import inspect
import os

from collections import OrderedDict
from typing import Dict, List, Tuple, Optional
from configparser import ConfigParser
from functools import lru_cache

from airflow.sdk import task, TaskGroup, get_current_context

_MODULE_CACHE = {}
_SIG_CACHE = {}


def _dag_dir(dag_file: str) -> str:
    """Directory where this DAG file resides."""
    return os.path.dirname(os.path.abspath(dag_file))


def _conf_dir(dag_file: str) -> str:
    """
    Conf root is <dags>/../conf as requested.
    If the DAG file is at ./dags/stream_dag_generator.py, then conf is ../conf.
    """
    return os.path.normpath(os.path.join(_dag_dir(dag_file), os.pardir, "conf"))


def _get_tags(key: str, stream: Optional[str]) -> List[str]:
    """
    Return tags for the dag based on stream and key
    """
    tags = []
    tags.append(key)
    if stream:
        tags.append(stream)
    return tags


def get_ordered_params(callable_obj):
    key = callable_obj  # identity
    if key in _SIG_CACHE:
        return _SIG_CACHE[key]
    sig = inspect.signature(callable_obj)
    od = OrderedDict({p.name: p.default for p in sig.parameters.values()})
    _SIG_CACHE[key] = od
    return od


def has_python_functions(file_path: str) -> bool:
    """Removed AST usage for a cheaper scan"""
    try:
        with open(file_path, "rb") as f:
            for line in f:
                s = line.lstrip()
                if s.startswith(b"def ") or s.startswith(b"async def "):
                    return True
        return False
    except Exception:   
        return False


def _prefix_asset(stream: Optional[str], asset: str) -> Optional[str]:
    """Prefix assets with stream to avoid collisions."""
    if not stream:
        return f"{asset}"
    return f"{stream}.{asset}"


def _prefix_asset_list(assets, stream: Optional[str]) -> List[str]:
    """
    Take a list or string of assets and optionally a stream
    Return a list of Assets prefixed with stream if provided
    """
    if assets is None:
        return []
    if isinstance(assets, str):
        return [_prefix_asset(asset=assets, stream=stream)]
    if isinstance(assets, list):
        return [_prefix_asset(asset=a, stream=stream) for a in assets]
    else:
        raise Exception("Wrong type..")


def _load_module(key: str, subdir: str, module_file: str, functions_dir: str):
    full_path = os.path.join(functions_dir, key, subdir, module_file)
    if not (module_file.endswith(".py") and has_python_functions(full_path)):
        return None, None

    st = os.stat(full_path)
    cache_key = (full_path, st.st_mtime)
    if cache_key in _MODULE_CACHE:
        return _MODULE_CACHE[cache_key]

    mod_base = module_file[:-3]
    module_name = f"{key}_{subdir}_{mod_base}" if subdir else f"{key}__{mod_base}"
    spec = importlib.util.spec_from_file_location(module_name, full_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    meta = {
        "DAG_PARAMS": getattr(module, "DAG_PARAMS", {}),
        "DEPENDENCIES": getattr(module, "DEPENDENCIES", {}),
        "UPSTREAM_ASSET": getattr(module, "UPSTREAM_ASSET", None),
        "DOWNSTREAM_ASSET": getattr(module, "DOWNSTREAM_ASSET", None),
        "SCHEDULE": getattr(module, "SCHEDULE", None),
        "SCHEMA": getattr(module, "SCHEMA", {}),
        "PRODUCT_TYPE": getattr(module, "PRODUCT_TYPE", None),
        "MODEL_NAME": getattr(module, "MODEL_NAME", None),
    }
    _MODULE_CACHE[cache_key] = (module, meta)
    return module, meta


def _create_task_group_from_module(
    key: str, subdir: str, module_file: str, dag_obj, stream: Optional[str], functions_dir: str
) -> Tuple[
    Optional[TaskGroup],
    Optional[str],
    Optional[str],
    List[str],
    Dict,
    List[str],
    List[str],
]:
    """
    Build a TaskGroup from a module, returning:
    (task_group, module_key, tags, dag_params, produces, consumes)
    """
    module, meta = _load_module(key, subdir, module_file, functions_dir)
    if not module:
        return None, None, None, [], {}, [], []

    mod_base = module_file[:-3]
    group_id = f"{(subdir or '').replace('/', '_')}__{mod_base}"
    module_key = f"{key}:{subdir or ''}:{mod_base}"

    # Stream-prefixed assets
    produces = _prefix_asset_list(assets=meta["DOWNSTREAM_ASSET"], stream=stream)
    consumes = _prefix_asset_list(assets=meta["UPSTREAM_ASSET"], stream=stream)

    collected_tags = _get_tags(key=key, stream=stream)

    with TaskGroup(
        group_id=group_id,
        dag=dag_obj,
    ) as tg:
        task_refs = {}

        for attr_name in dir(module):
            attr = getattr(module, attr_name)
            if callable(attr) and getattr(attr, "__module__", None) == module.__name__:
                task_args = {"task_id": attr_name}
                task_args.update(get_ordered_params(attr))

                # Attach outlets only if the function name implies an outlet and we have a produced asset
                if "outlet" in attr_name and produces:
                    task_args["outlets"] = produces

                # Pooling rule for duckdb
                if "duckdb" in attr_name or "export" in attr_name:
                    if "pool" not in task_args.keys():
                           task_args["pool"] = "duckdb_pool"
                    if "pool_slots" not in task_args.keys():
                            task_args["pool_slots"] = 16

                # Choose decorator variant based on function name
                if "duckdb" in attr_name:
                    @task.duckdb(**task_args)
                    def dynamic_task(attr=attr, task_args=task_args):
                        return attr()
                elif "sensor" in attr_name:
                    @task.sensor(**task_args)
                    def dynamic_task(attr=attr, task_args=task_args):
                        return attr()
                elif "export" in attr_name:
                    @task.export(**task_args)
                    def dynamic_task(attr=attr, task_args=task_args):
                        return attr()
                elif "beeline" in attr_name:
                    @task.beeline(**task_args)
                    def dynamic_task(attr=attr, task_args=task_args):
                        return attr()
                elif "branch" in attr_name:
                    @task.branch(**task_args)
                    def dynamic_task(attr=attr, task_args=task_args):
                        return attr()
                elif "gcs_scopy" in attr_name:
                    @task.gcs_scopy(**task_args)
                    def dynamic_task(attr=attr, task_args=task_args):
                        return attr()
                else:
                    @task(**task_args)
                    def dynamic_task(attr=attr, task_args=task_args):
                        return attr()

                task_refs[attr_name] = dynamic_task()

        # Wire function-level dependencies inside this module
        for upstream, downstream_list in meta["DEPENDENCIES"].items():
            for downstream in downstream_list:
                task_refs[upstream] >> task_refs[downstream]

    return (
        tg,
        module_key,
        collected_tags,
        meta["DAG_PARAMS"],
        produces,
        consumes,
    )


def _normalize_to_list(string: str) -> List[str]:
    """Convert string to list, split on commas, trimming whitespace around each element"""
    result = [p.strip() for p in string.strip().split(",")]
    return result


@lru_cache(maxsize=8)
def _read_config(config_file: str, mtime: float) -> ConfigParser:
    parser = ConfigParser()
    with open(config_file, encoding="utf-8") as conf:
        parser.read_file(conf)
    return parser


def _get_parser(config_file: str) -> ConfigParser:
    # mtime makes cache invalidate automatically on file changes
    st = os.stat(config_file)
    return _read_config(config_file, st.st_mtime)


@lru_cache(maxsize=128)
def _section_to_dict_cached(config_file: str, section: str, mtime: float):
    parser = _get_parser(config_file)
    if not parser.has_section(section):
        raise KeyError(f"Missing section: [{section}]")
    result = {}
    for key, value in parser.items(section):
        result[key] = _normalize_to_list(value)
    return result


def _section_to_dict(config_file: str, section: str):
    st = os.stat(config_file)
    return _section_to_dict_cached(config_file, section, st.st_mtime)


@lru_cache(maxsize=32)
def _get_functions_dir_cached(config_file: str, mtime: float) -> str:
    parser = _get_parser(config_file)
    if not parser.has_section("global"):
        raise KeyError("Missing section: [global]")
    if not parser.has_option("global", "functions_dir"):
        raise KeyError("Missing option: functions_dir")
    return parser.get("global", "functions_dir")


def _get_functions_dir(config_file: str) -> str:
    st = os.stat(config_file)
    return _get_functions_dir_cached(config_file, st.st_mtime)