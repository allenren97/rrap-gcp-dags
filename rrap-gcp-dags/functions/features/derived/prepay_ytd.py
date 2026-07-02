import pyarrow as pa
from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

UPSTREAM_ASSET = [ 'ingestion.MORT_MTH_SNAPSHOT' ]
DOWNSTREAM_ASSET = 'features.PREPAY_YTD'
DEPENDENCIES = {
    'duckdb_clear_prepay_ytd': ['duckdb_derive_prepay_ytd'],
}


def duckdb_clear_prepay_ytd(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
        DELETE FROM { DOWNSTREAM_ASSET } 
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass


def duckdb_derive_prepay_ytd(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    INSERT INTO { DOWNSTREAM_ASSET }
    BY NAME FROM (
        SELECT
            BASEL_ACCT_ID,
            YTD_PRPY_AMT AS PREPAY_YTD,
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' as OBSN_DT
        FROM { UPSTREAM_ASSET[0] }
        WHERE MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    )
    """
):
    pass

