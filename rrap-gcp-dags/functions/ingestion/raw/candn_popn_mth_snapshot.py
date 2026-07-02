import os
from datetime import timedelta
import pendulum
from airflow.sdk import get_current_context
from airflow.sdk.bases.sensor import PokeReturnValue
from bns.rrap.helpers.asset_event import _pull_asset_event_extras, _push_asset_event_extras

DUCKLAKE_SCHEMA = "ingestion"
UPSTREAM_ASSET = [ f"{DUCKLAKE_SCHEMA}.TM_DIM" ]
DOWNSTREAM_ASSET = f"{DUCKLAKE_SCHEMA}.CANDN_POPN_MTH_SNAPSHOT"
DEPENDENCIES = {
    'sensor_wait_for_table': ['duckdb_update'],
    'duckdb_update' : ['duckdb_delete'],
    'duckdb_delete' : ['duckdb_insert'],
}
INPUT_PATH = 'jb0441_AIRB_CANDN_POPN.parquet'


def sensor_wait_for_table(poke_interval=300, timeout=(60 * 60 * 24 * 8), mode='reschedule'):
    context = get_current_context()
    rundate, mth_tm_id = _pull_asset_event_extras(context, UPSTREAM_ASSET[0])
    path = os.path.join("/bns/rrap/data", f"{ rundate }", INPUT_PATH)

    if os.path.exists(path):
        context['ti'].xcom_push(key="parquet", value=path)
        return PokeReturnValue(is_done=True)
    else:
        return PokeReturnValue(is_done=False)

def duckdb_update(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    UPDATE {DOWNSTREAM_ASSET} SET
        CANDN_POPN_THSNDTH_VAL = b.CANDN_POPN_THSNDTH_VAL,
        UPDT_PROCESS_TMSTMP = CURRENT_TIMESTAMP
    FROM read_parquet('{{{{ task_instance.xcom_pull(task_ids="raw__candn_popn_mth_snapshot.sensor_wait_for_table", key="parquet") }}}}') b
    WHERE
        {DOWNSTREAM_ASSET}.MTH_TM_ID = b.MTH_TM_ID
        AND {DOWNSTREAM_ASSET}.CANDN_POPN_THSNDTH_VAL != b.CANDN_POPN_THSNDTH_VAL
    """
):
    pass

def duckdb_delete(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    DELETE FROM {DOWNSTREAM_ASSET}
    WHERE MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    """
):
    pass

def duckdb_insert(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} BY NAME
    FROM (
        SELECT
            MTH_TM_ID,
            CANDN_POPN_THSNDTH_VAL,
            INSRT_PROCESS_TMSTMP,
            NULL as UPDT_PROCESS_TMSTMP
        FROM read_parquet('{{{{ task_instance.xcom_pull(task_ids="raw__candn_popn_mth_snapshot.sensor_wait_for_table", key="parquet") }}}}')
        WHERE MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    )
    """
):
    pass

