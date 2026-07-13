import pyarrow as pa
from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

UPSTREAM_ASSET = [ 'features.GO06' ]
DOWNSTREAM_ASSET = 'features.GO06MAX12M'
DEPENDENCIES = {
    'duckdb_clear_go06max12m': ['duckdb_derive_go06max12m'],
}


def duckdb_clear_go06max12m(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
        DELETE FROM { DOWNSTREAM_ASSET } 
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass


def duckdb_derive_go06max12m(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    INSERT INTO { DOWNSTREAM_ASSET }
    WITH base AS (
        select
        BASEL_CUST_ID,
        MAX(GO06) as GO06MAX12M
        from { UPSTREAM_ASSET[0] }
        WHERE OBSN_DT BETWEEN
            DATE_ADD(DATE '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}', -INTERVAL 11 MONTH) AND
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
        group by basel_cust_id
    )
    SELECT 
    BASEL_CUST_ID, 
    '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
    GO06MAX12M
    FROM base
    """
):
    pass

