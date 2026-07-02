import pyarrow as pa
from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

UPSTREAM_ASSET = [ 'features.UTIL_KSA' ]
DOWNSTREAM_ASSET = 'features.UTIL_KSA_AVG6M'
DEPENDENCIES = {
    'duckdb_clear_util_ksa_avg6m': ['duckdb_derive_util_ksa_avg6m'],
}


def duckdb_clear_util_ksa_avg6m(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
        DELETE FROM { DOWNSTREAM_ASSET } 
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass


def duckdb_derive_util_ksa_avg6m(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    INSERT INTO { DOWNSTREAM_ASSET }
    WITH base AS (
        select
        BASEL_ACCT_ID,
        AVG(UTIL_KSA) as UTIL_KSA_AVG6M
        from { UPSTREAM_ASSET[0] }
        WHERE OBSN_DT BETWEEN
            DATE_ADD(DATE '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}', -INTERVAL 5 MONTH) AND
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
        group by BASEL_ACCT_ID
    )
    SELECT 
    BASEL_ACCT_ID, 
    '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
    util_ksa_avg6m
    FROM base
    """
):
    pass


