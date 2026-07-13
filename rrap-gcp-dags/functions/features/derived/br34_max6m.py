import pyarrow as pa
from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

UPSTREAM_ASSET = [ 'features.BR34' ]
DOWNSTREAM_ASSET = 'features.BR34_MAX6M'
DEPENDENCIES = {
    'duckdb_clear_br34_max6m': ['duckdb_derive_br34_max6m'],
}


def duckdb_clear_br34_max6m(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
        DELETE FROM { DOWNSTREAM_ASSET } 
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass


def duckdb_derive_br34_max6m(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
    INSERT INTO { DOWNSTREAM_ASSET }
    BY NAME FROM (
        select
            max(br34) as BR34_MAX6M,
            BASEL_CUST_ID,
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT
        from { UPSTREAM_ASSET[0] }
        WHERE OBSN_DT BETWEEN
            DATE_ADD(DATE '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}', -INTERVAL 5 MONTH) AND
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
         group by basel_cust_id
    )
    """
):
    pass

