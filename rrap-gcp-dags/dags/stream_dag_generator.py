"""
Generate one DAG per (stream, key) from STRUCTURE_MAP using dag_id "<stream>_<key>".

- Streams are discovered by scanning ../conf/* relative to the DAGs folder.
- For each (stream, key) DAG:
  - Create one TaskGroup per discovered Python file under FUNCTIONS_DIR/<key>/<subdir>/**
  - For 'model', subdirs are 'scored' and 'segmented'; for 'instrument' and 'reporting', subdir is '' only.
  - Wire task-level function dependencies using DEPENDENCIES in each module.
  - Wire group-level dependencies by matching Assets (UPSTREAM_ASSET/DOWNSTREAM_ASSET).
    * Assets are automatically prefixed with the stream: f"{stream}.{ASSET}" to avoid collisions.
- Tags include the stream and key (not the segment), and we preserve your duckdb pool/slots rule for tasks
  whose function name contains "duckdb".
"""
from __future__ import annotations
import os
import glob
from collections import defaultdict
from typing import Dict, List

import pendulum

from airflow.sdk import dag, get_parsing_context, TaskGroup, get_current_context, task
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor

from util.generator_utilities import (
    _create_task_group_from_module,
    _conf_dir,
    _section_to_dict,
    _get_functions_dir,
)

from bns.rrap.hooks.duckdb import DuckLakeHook


CONFIG_FILE = '/bns/rrap/apps/rebuild-airflow/conf/generators.ini'

FUNCTIONS_DIR = _get_functions_dir(CONFIG_FILE)

STRUCTURE_MAP = _section_to_dict(CONFIG_FILE, 'stream_map')

DAG_DEPENDENCIES = _section_to_dict(CONFIG_FILE, 'stream_dependencies')

current_dag_id = get_parsing_context().dag_id


def discover_streams(conf_dir: str) -> List[str]:
    """Enumerate stream names by listing immediate subdirectories under conf/* """
    if not os.path.isdir(conf_dir):
        return []
    names = []
    for entry in os.listdir(conf_dir):
        full = os.path.join(conf_dir, entry)
        if os.path.isdir(full) and not entry.startswith("."):
            names.append(entry)
    names.sort()
    return names


def build_dag_for_stream_and_key(*, stream: str, key: str, conf_dir: str):
    """
    Create one DAG named "<stream>_<key>" and add a TaskGroup per file discovered.
    Auto-wire TaskGroup dependencies when produced asset of one group appears
    in the consumed assets of another group within the same DAG.
    """
    dag_id = f"{stream}_{key}"
    if current_dag_id is not None and current_dag_id != dag_id:
        # Airflow is parsing another DAG — skip building this one.
        return

    all_tags = {stream, key}
    merged_params: Dict = { "stream": stream }  # make stream visible as a DAG param
    groups_by_key: Dict[str, TaskGroup] = {}
    consumes_by_key: Dict[str, List[str]] = {}
    produces_by_asset: Dict[str, List[str]] = defaultdict(list)

    @dag(
        dag_id=dag_id,
        start_date=pendulum.datetime(2020, 1, 1, tz="America/Toronto"),
        schedule="@monthly",
        catchup=False,
        tags=[stream, key],
        params={},
    )
    def domain_dag():
        pass

    dag_obj = domain_dag()

    with dag_obj:
        for subdir in STRUCTURE_MAP.get(key, []):
            path = os.path.join(FUNCTIONS_DIR, key, subdir)
            if not os.path.exists(path):
                continue

            for f in os.listdir(path):
                if f.endswith(".py"):
                    if key == "model" and not os.path.exists(os.path.join(conf_dir,stream,"models",subdir, f"{f[:-3]}_{"scoring" if subdir == "scored" else "segmentation"}_config.csv")):
                        continue
                    if key == "reporting" and not f.startswith(stream):
                        continue
                    (
                        tg,
                        module_key,
                        tags,
                        dag_params,
                        produces,
                        consumes,
                    ) = _create_task_group_from_module(
                        key=key, 
                        subdir=subdir, 
                        module_file=f, 
                        dag_obj=dag_obj, 
                        stream=stream, 
                        functions_dir=FUNCTIONS_DIR
                    )

                    if tg is None or module_key is None:
                        continue

                    groups_by_key[module_key] = tg
                    consumes_by_key[module_key] = consumes

                    for a in produces:
                        produces_by_asset[a].append(module_key)
                    
                    all_tags |= set(tags)  # union instead of reset
                    merged_params.update(dag_params)

        # Auto-wire TaskGroup dependencies by matching stream-prefixed assets
        for consumer_key, asset_list in consumes_by_key.items():
            for asset in asset_list:
                producers = produces_by_asset.get(asset, [])
                for producer_key in producers:
                    if producer_key == consumer_key:
                        continue
                    upstream_tg = groups_by_key.get(producer_key)
                    downstream_tg = groups_by_key.get(consumer_key)
                    if upstream_tg and downstream_tg:
                        upstream_tg >> downstream_tg

        # handle_month_context at dag-level
        @task()
        def handle_month_context():
            context = get_current_context()
            
            rundate = context['logical_date'].subtract(months=1).end_of('month').strftime('%Y-%m-%d')
            
            hook = DuckLakeHook(duckdb_conn_id='duckdb-conn')
            mth_tm_id = hook.duckdb.sql(f"""
                SELECT TM_ID FROM ingestion.TM_DIM
                WHERE TM_LVL = 'Month' AND TM_LVL_END_DT = '{ rundate }'
            """).fetchone()[0]

            prev_mth_tm_id = mth_tm_id - 40
            yyyymm = context['logical_date'].subtract(months=1).end_of('month').strftime('%Y%m')
            popn_dt = context['logical_date'].strftime("%Y-%m-15")

            print(f"rundate: {rundate} - mth_tm_id: {mth_tm_id} - prev_mth_tm_id: {prev_mth_tm_id} - yyyymm: {yyyymm} - popn_dt: {popn_dt}")

            context['ti'].xcom_push(key="rundate", value=rundate)
            context['ti'].xcom_push(key="mth_tm_id", value=mth_tm_id)
            context['ti'].xcom_push(key="prev_mth_tm_id", value=prev_mth_tm_id)
            context['ti'].xcom_push(key="yyyymm", value=yyyymm)
            context['ti'].xcom_push(key="popn_dt", value=popn_dt)
            context['ti'].xcom_push(key="stream", value=stream.upper())

        handler = handle_month_context()

        # For streamed reference
        if "reference" in dag_id:
            for group_id in groups_by_key.keys():
                taskgroup = groups_by_key.get(group_id)
                if not taskgroup.upstream_list:
                    handler >> taskgroup
        else:
            # Create ExternalTaskSensor for DAG to DAG dependencies
            for downstream, upstream in DAG_DEPENDENCIES.items():
                for up in upstream:
                    if f"{stream}_{downstream}" == dag_id:
                        task_id = ""
                        external_dag_id = ""
                        if "features" in upstream:
                            task_id = f"{up}"
                            external_dag_id = f"{up}"
                        else:
                            task_id = f"{stream}_{up}"
                            external_dag_id = f"{stream}_{up}"

                        upstream_dag_sensor = ExternalTaskSensor(
                            task_id=f"{task_id}",
                            external_dag_id=f"{external_dag_id}",
                            poll_interval=60*5
                        )
                        for group_id in groups_by_key.keys():
                            taskgroup = groups_by_key.get(group_id)
                            if not taskgroup.upstream_list:
                                handler >> upstream_dag_sensor >> taskgroup


    dag_obj.tags = sorted(all_tags)
    dag_obj.params.update(merged_params)

    # Register in globals for Airflow to pick up
    globals()[dag_id] = dag_obj


def generate_stream_key_dags():
    """Enumerate streams from conf/* and build one DAG per (stream, key)."""
    conf_dir = _conf_dir(__file__)

    if current_dag_id:
        # Find which key this dag_id corresponds to by suffix match: "<stream>_<key>"
        for key in STRUCTURE_MAP.keys():
            suffix = f"_{key}"
            if current_dag_id.endswith(suffix):
                stream = current_dag_id[:-len(suffix)]
                build_dag_for_stream_and_key(stream=stream, key=key, conf_dir=conf_dir)
                return
        # If it doesn't match the pattern, fall through to full discovery.

    streams = discover_streams(conf_dir)
    for stream in streams:
        for key in STRUCTURE_MAP.keys():
            build_dag_for_stream_and_key(stream=stream, key=key, conf_dir=conf_dir)


# Entrypoint
generate_stream_key_dags()