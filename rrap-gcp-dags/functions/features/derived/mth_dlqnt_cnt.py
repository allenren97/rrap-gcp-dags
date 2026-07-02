import pyarrow as pa
from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

UPSTREAM_ASSET = [ 'ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT' ]
DOWNSTREAM_ASSET = 'features.MTH_DLQNT_CNT'
DEPENDENCIES = {
    'duckdb_clear_mth_dlqnt_cnt': ['duckdb_derive_mth_dlqnt_cnt'],
}


def duckdb_clear_mth_dlqnt_cnt(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
        DELETE FROM { DOWNSTREAM_ASSET } 
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass


def duckdb_derive_mth_dlqnt_cnt(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    INSERT INTO { DOWNSTREAM_ASSET }
    BY NAME FROM (
                SELECT
                '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
                BASEL_ACCT_ID,
                MTH_DLQNT_CNT
                FROM
                    {UPSTREAM_ASSET[0]}
                WHERE
                    MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
    )
    """
):
    pass

