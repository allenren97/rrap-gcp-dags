import os
from datetime import timedelta
import pendulum
from airflow.sdk import get_current_context
from airflow.sdk.bases.sensor import PokeReturnValue
from bns.rrap.helpers.asset_event import _pull_asset_event_extras, _push_asset_event_extras

DUCKLAKE_SCHEMA = "ingestion"
UPSTREAM_ASSET = [
    f"{DUCKLAKE_SCHEMA}.TM_DIM",
    f"{DUCKLAKE_SCHEMA}.IWF_CUST_ACCT"
]
DOWNSTREAM_ASSET = f"{DUCKLAKE_SCHEMA}.CUST_MODEL"
DEPENDENCIES = {
    'duckdb_delete' : ['duckdb_load'],
}



def duckdb_delete(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    DELETE FROM {DOWNSTREAM_ASSET} WHERE TIME_KEY = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}'
    """
):
    pass


def duckdb_load(
    duckdb_conn_id='duckdb-conn',
    sql = f"""
    INSERT INTO {DOWNSTREAM_ASSET} (TIME_KEY, CUST_BASE_KEY, CUST_KEY)
    FROM (
        SELECT DISTINCT TIME_KEY, CUST_BASE_KEY, CUST_KEY
        FROM ingestion.IWF_CUST_ACCT
        WHERE TIME_KEY = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}'
    )
    """
):
    pass


