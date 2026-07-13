import os
from datetime import timedelta
import pendulum

from airflow.sdk import get_current_context
from airflow.sdk.bases.sensor import PokeReturnValue

from bns.rrap.helpers.asset_event import _pull_asset_event_extras, _push_asset_event_extras


UPSTREAM_ASSET = [ 'ingestion.TM_DIM' ]
DOWNSTREAM_ASSET = "ingestion.BASEL_PSNL_LOAN_MTH_SNAPSHOT_PRE_SL"
DEPENDENCIES = {
    'sensor_wait_for_table': ['duckdb_clear_basel_psnl_loan_mth_snapshot_pre_sl'],
    'duckdb_clear_basel_psnl_loan_mth_snapshot_pre_sl': ['duckdb_load_into_ducklake'],
}


def sensor_wait_for_table(poke_interval=300, timeout=(60 * 60 * 24 * 8), mode='reschedule'):
    context = get_current_context()
    rundate, mth_tm_id = _pull_asset_event_extras(context, UPSTREAM_ASSET[0])
    path = os.path.join("/bns/rrap/data", f"{ rundate }", 'jb0062_BASEL_PSNL_LOAN_MTH_SNAPSHOT.parquet')

    if os.path.exists(path):
        context['ti'].xcom_push(key="parquet", value=path)
        context['ti'].xcom_push(key="rundate", value=rundate)
        context['ti'].xcom_push(key='mth_tm_id', value=mth_tm_id)
        return PokeReturnValue(is_done=True)
    else:
        return PokeReturnValue(is_done=False)


def duckdb_clear_basel_psnl_loan_mth_snapshot_pre_sl(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
        DELETE FROM { DOWNSTREAM_ASSET } WHERE MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    """
):
    pass


def duckdb_load_into_ducklake(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
    INSERT INTO { DOWNSTREAM_ASSET }
    BY NAME FROM (
    SELECT * FROM '{{{{ task_instance.xcom_pull(task_ids="cleaned__basel_psnl_loan_mth_snapshot_pre_sl.sensor_wait_for_table", key="parquet") }}}}'
    )
    """
):
    pass


