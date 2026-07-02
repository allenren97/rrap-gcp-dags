import os
from datetime import timedelta
import pendulum

from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import _pull_asset_event_extras, _push_asset_event_extras


UPSTREAM_ASSET = [ 'features.PRCH_CRNT_C_BAL_KSC']
DOWNSTREAM_ASSET = "features.PRCH_CRNT_C_BAL_KSC_MAX3M"
DEPENDENCIES = {
    'duckdb_clear_derive_prch_crnt_c_bal_kscmax3m': ['duckdb_derive_prch_crnt_c_bal_kscmax3m'],
}


def duckdb_clear_derive_prch_crnt_c_bal_kscmax3m(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    DELETE FROM { DOWNSTREAM_ASSET } 
    WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass

def duckdb_derive_prch_crnt_c_bal_kscmax3m(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} 
    with base as (
        SELECT 
        PRIM_BASEL_CUST_ID,
        SUM(PRCH_CRNT_C_BAL_KSC) AS PRCH_CRNT_C_BAL_KSCMAX3M
        FROM 
            {UPSTREAM_ASSET[0]}
        WHERE 
            OBSN_DT BETWEEN 
                DATE_ADD(DATE '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}', - INTERVAL 2 MONTH)
                AND
                '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
        GROUP BY 
            PRIM_BASEL_CUST_ID, OBSN_DT
    ),max as (
    SELECT 
    PRIM_BASEL_CUST_ID, 
    MAX(PRCH_CRNT_C_BAL_KSCMAX3M) AS PRCH_CRNT_C_BAL_KSC_MAX3M
    FROM base
    GROUP BY PRIM_BASEL_CUST_ID
    )
    SELECT 
    PRIM_BASEL_CUST_ID AS BASEL_CUST_ID,
    '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate")  }}}}' AS OBSN_DT,
    PRCH_CRNT_C_BAL_KSC_MAX3M
    from max

    """

):
    pass

