import os
from datetime import timedelta
import pendulum

from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import _pull_asset_event_extras, _push_asset_event_extras


UPSTREAM_ASSET = [ 'features.AT29']
DOWNSTREAM_ASSET = "features.AT29_AVG6M"
DEPENDENCIES = {
    'duckdb_clear_derive_at29_avg6m': ['duckdb_derive_at29_avg6m'],
}


def duckdb_clear_derive_at29_avg6m(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    DELETE FROM { DOWNSTREAM_ASSET } 
    WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass

def duckdb_derive_at29_avg6m(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} (
        SELECT 
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate")  }}}}' AS OBSN_DT,
        BASEL_CUST_ID,
        AVG(at29) AS AT29_AVG6M
        FROM {UPSTREAM_ASSET[0]}
        WHERE OBSN_DT BETWEEN 
            DATE_ADD(DATE '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}', -INTERVAL 5 MONTH) AND 
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
        GROUP BY BASEL_CUST_ID
    )
    """

):
    pass

