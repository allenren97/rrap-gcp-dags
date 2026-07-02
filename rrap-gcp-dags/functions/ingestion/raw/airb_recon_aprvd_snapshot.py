import os
from datetime import timedelta
import pendulum
from airflow.sdk import get_current_context
from airflow.sdk.bases.sensor import PokeReturnValue
from bns.rrap.helpers.asset_event import _pull_asset_event_extras, _push_asset_event_extras


DUCKLAKE_SCHEMA = "ingestion"
UPSTREAM_ASSET = [ f"{DUCKLAKE_SCHEMA}.TM_DIM" ]
DOWNSTREAM_ASSET = f"{DUCKLAKE_SCHEMA}.AIRB_RECON_APRVD_SNAPSHOT"
DEPENDENCIES = {
    'sensor_wait_for_table': ['duckdb_delete'],
    'duckdb_delete' : ['duckdb_load'],
}
INPUT_PATH = 'AIRB_RECON_APRVD_SNAPSHOT.parquet'


def sensor_wait_for_table(poke_interval=300, timeout=(60 * 60 * 24 * 8), mode='reschedule'):
    context = get_current_context()
    rundate, mth_tm_id = _pull_asset_event_extras(context, UPSTREAM_ASSET[0])
    path = os.path.join("/bns/rrap/data", f"{ rundate }", INPUT_PATH)

    if os.path.exists(path):
        context['ti'].xcom_push(key="parquet", value=path)
        return PokeReturnValue(is_done=True)
    else:
        return PokeReturnValue(is_done=False)


def duckdb_delete(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    DELETE FROM {DOWNSTREAM_ASSET} WHERE MTH_END_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass


def duckdb_load(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} BY NAME
    FROM (
        SELECT * FROM read_parquet('{{{{ task_instance.xcom_pull(task_ids="raw__airb_recon_aprvd_snapshot.sensor_wait_for_table", key="parquet") }}}}')
    )
    """
):
    pass


