import pyarrow as pa
from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

UPSTREAM_ASSET = [ 'features.D2D_BAL_AMT' ]
DOWNSTREAM_ASSET = 'features.D2D_BAL_AMT_MAX3M'
DEPENDENCIES = {
    'duckdb_clear_d2d_bal_amt_max3m': ['duckdb_derive_d2d_bal_amt_max3m'],
}


def duckdb_clear_d2d_bal_amt_max3m(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
        DELETE FROM { DOWNSTREAM_ASSET } 
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass


def duckdb_derive_d2d_bal_amt_max3m(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    INSERT INTO { DOWNSTREAM_ASSET }
    BY NAME FROM (
        SELECT 
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            BASEL_CUST_ID,
            MAX(D2D_BAL_AMT) AS D2D_BAL_AMT_MAX3M
        FROM
            {UPSTREAM_ASSET[0]}
        WHERE OBSN_DT BETWEEN
            DATE_ADD(DATE '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}', -INTERVAL 2 MONTH) 
            AND
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
        GROUP BY
            BASEL_CUST_ID
    )
    """
):
    pass

