import pyarrow as pa
from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

UPSTREAM_ASSET = [ 'features.AT34' ]
DOWNSTREAM_ASSET = 'features.AT34_MAX3M'
DEPENDENCIES = {
    'duckdb_clear_at34_max3m': ['duckdb_derive_at34_max3m'],
}


def duckdb_clear_at34_max3m(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
        DELETE FROM { DOWNSTREAM_ASSET } 
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass


def duckdb_derive_at34_max3m(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
    INSERT INTO { DOWNSTREAM_ASSET }
    WITH base AS (
        SELECT 
            BASEL_CUST_ID,
            MAX(AT34) AS AT34_MAX3M
        FROM 
            {UPSTREAM_ASSET[0]}
        WHERE 
            OBSN_DT BETWEEN 
                DATE_ADD(DATE '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}', - INTERVAL 2 MONTH)
                AND
                '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
        GROUP BY 
            BASEL_CUST_ID
    )
    SELECT 
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate")  }}}}' AS OBSN_DT,
        BASEL_CUST_ID, 
        AT34_MAX3M 
    FROM base
    """
):
    pass

