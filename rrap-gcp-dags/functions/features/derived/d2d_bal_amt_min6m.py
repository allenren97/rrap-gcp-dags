import os
from datetime import timedelta
import pendulum

from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import _pull_asset_event_extras, _push_asset_event_extras


UPSTREAM_ASSET = [ 'features.D2D_BAL_AMT_WITHNULL']
DOWNSTREAM_ASSET = "features.D2D_BAL_AMT_MIN6M"
DEPENDENCIES = {
    'duckdb_clear_derive_d2d_bal_amt_min6m': ['duckdb_derive_d2d_bal_amt_min6m'],
}


def duckdb_clear_derive_d2d_bal_amt_min6m(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    DELETE FROM { DOWNSTREAM_ASSET } 
    WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass

def duckdb_derive_d2d_bal_amt_min6m(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} 
    WITH base as (
        SELECT 
            BASEL_CUST_ID, 
            MIN(D2D_BAL_AMT_WITHNULL) as D2D_BAL_AMT_MIN6M
        FROM {UPSTREAM_ASSET[0]}
        WHERE
            OBSN_DT BETWEEN 
            DATE_ADD(DATE '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}', - INTERVAL 5 MONTH)
            AND
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
        GROUP BY 
            BASEL_CUST_ID
        )
    SELECT 
        BASEL_CUST_ID, 
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate")  }}}}' AS OBSN_DT,
        D2D_BAL_AMT_MIN6M 
    FROM base
    """
):
    pass

