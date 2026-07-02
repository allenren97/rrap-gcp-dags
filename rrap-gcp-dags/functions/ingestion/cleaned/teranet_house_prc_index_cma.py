import os
from datetime import timedelta
import pendulum
import duckdb

from airflow.sdk import get_current_context
from airflow.sdk.bases.sensor import PokeReturnValue

from bns.rrap.helpers.asset_event import _push_asset_event_extras,_pull_asset_event_extras
from bns.rrap.hooks.duckdb import DuckLakeHook


UPSTREAM_ASSET = ['ingestion.TM_DIM']
DOWNSTREAM_ASSET = "ingestion.TERANET_HOUSE_PRC_INDEX_CMA"
DEPENDENCIES = {
    'sensor_wait_for_table': ['duckdb_delete_ducklake'],
    'duckdb_delete_ducklake': ['duckdb_load_into_ducklake'],
}


def sensor_wait_for_table(poke_interval=300, timeout=(60 * 60 * 24 * 8), mode='reschedule'):
    context = get_current_context()
    rundate, mth_tm_id = _pull_asset_event_extras(context, UPSTREAM_ASSET[0])
    prev_mth_tm_id = mth_tm_id - 40
    path = os.path.join("/bns/rrap/data", f"{ rundate }", 'CMA32_DATA.parquet')

    if os.path.exists(path):
        context['ti'].xcom_push(key="parquet", value=path)
        return PokeReturnValue(is_done=True)
    else:
        return PokeReturnValue(is_done=False)

def duckdb_delete_ducklake(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    DELETE FROM {DOWNSTREAM_ASSET}
    WHERE MTH_TM_ID IN ({{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}},{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="prev_mth_tm_id") }}}})
    """
):pass

def duckdb_load_into_ducklake(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    INSERT INTO { DOWNSTREAM_ASSET }
    FROM (
    SELECT * 
    FROM '{{{{ task_instance.xcom_pull(task_ids="cleaned__teranet_house_prc_index_cma.sensor_wait_for_table", key="parquet") }}}}'
    )
    """
):
    pass


