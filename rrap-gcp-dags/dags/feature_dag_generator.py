"""
 Generate one dag per top-level key in STRUCTURE_MAP using the id "<key>".
 Inside each DAG:
  - create one TaskGroup per discovered Python file
  - wire task-level function dependencies using DEPENDENCIES in module
  - wire group-level dependencies using Assets
  if upstream group's DOWNSTREAM_ASSET equals any asset in downstream group's UPSTREAM_ASSET,
  then upstream TaskGroup >> downstream TaskGroup.
 """

import os
from collections import defaultdict
from typing import Dict, List

import pendulum

from airflow.sdk import dag, task, get_parsing_context, TaskGroup, get_current_context
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor

from util.generator_utilities import (
    _create_task_group_from_module,
    _section_to_dict,
    _get_functions_dir,
)

from bns.rrap.hooks.duckdb import DuckLakeHook



CONFIG_FILE = '/bns/rrap/apps/rebuild-airflow/conf/generators.ini'

FUNCTIONS_DIR = _get_functions_dir(CONFIG_FILE)

STRUCTURE_MAP = _section_to_dict(CONFIG_FILE, 'feature_map')

DAG_DEPENDENCIES = _section_to_dict(CONFIG_FILE, 'feature_dependencies')

current_dag_id = get_parsing_context().dag_id 


def build_single_dag_per_key(key: str):
    """
    Create one DAG named "<key>" and add a TaskGroup per file.
    Auto-wire TaskGroup dependencies when produced asset of one group appears
    in the consumed assets of another group.
    """
    dag_id = f"{key}"

    if current_dag_id is not None and current_dag_id != dag_id:
        return

    all_tags = {key}
    merged_params: Dict = {}

    groups_by_key: Dict[str, TaskGroup] = {}
    consumes_by_key: Dict[str, List[str]] = {}
    produces_by_asset: Dict[str, List[str]] = defaultdict(list) 

    @dag(
        dag_id=dag_id,
        start_date=pendulum.datetime(2020, 1, 1, tz="America/Toronto"),
        schedule="@monthly",
        catchup=False,
        tags=[key],
        params={},
    )
    def domain_dag():
        pass

    dag_obj = domain_dag()

    with dag_obj:
        # Discover files for this top-level and create TaskGroups
        for subdir in STRUCTURE_MAP.get(key, []):
            path = os.path.join(FUNCTIONS_DIR, key, subdir)
            if not os.path.exists(path):
                continue
            
            for f in os.listdir(path):
                if f.endswith(".py"):
                    ( 
                        tg, 
                        module_key,
                        tags, 
                        dag_params, 
                        produces,
                        consumes
                     ) = _create_task_group_from_module(
                            key=key,
                            subdir=subdir,
                            module_file=f,
                            dag_obj=dag_obj,
                            stream=None,
                            functions_dir=FUNCTIONS_DIR,
                        )
                    
                    if tg is None:
                        continue

                    groups_by_key[module_key] = tg
                    consumes_by_key[module_key] = consumes

                    for a in (produces or []):
                        produces_by_asset[a].append(module_key)

                    all_tags |= set(tags)
                    merged_params.update(dag_params)

        # If a group's DOWNSTREAM_ASSET (produces) equals another group's UPSTREAM_ASSET (consumes),
        # connect producing TaskGroup >> consuming TaskGroup.
        for consumer_key, asset_list in consumes_by_key.items():
            if asset_list:
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

        handler = handle_month_context()

        sensors = []
        # Create ExternalTaskSensor for DAG to DAG dependencies
        for downstream, upstream in DAG_DEPENDENCIES.items():
            for up in upstream:
                if downstream == dag_id:
                    upstream_dag_sensor = ExternalTaskSensor(
                        task_id=f"wait_for_{up}",
                        external_dag_id=f"{up}",
                        poll_interval=60*5
                    )
                    sensors.append(upstream_dag_sensor)

        if "ingestion" == dag_id or "reference" == dag_id:
            for group_id in groups_by_key.keys():
                taskgroup = groups_by_key.get(group_id)
                if not taskgroup.upstream_list:
                    handler >> taskgroup
        else:
            for group_id in groups_by_key.keys():
                taskgroup = groups_by_key.get(group_id)
                if not taskgroup.upstream_list:
                    sensors >> handler >> taskgroup

    dag_obj.tags = sorted(all_tags)
    dag_obj.params.update(merged_params)

    globals()[dag_id] = dag_obj


def generate_layer_dags():
    if current_dag_id:
        # Find which key this dag_id corresponds to by suffix match: "<stream>_<key>"
        for key in STRUCTURE_MAP.keys():
            if current_dag_id == key:
                build_single_dag_per_key(key=key)
                return
        # If it doesn't match the pattern, fall through to full discovery.

    for key in STRUCTURE_MAP.keys():
        build_single_dag_per_key(key=key)


generate_layer_dags()