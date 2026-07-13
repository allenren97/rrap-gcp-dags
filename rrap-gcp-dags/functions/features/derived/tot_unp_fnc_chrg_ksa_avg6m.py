import os
from datetime import timedelta
import pendulum

from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import _pull_asset_event_extras, _push_asset_event_extras


UPSTREAM_ASSET = ['features.TOT_UNP_FNC_CHRG_KSA']

DOWNSTREAM_ASSET = "features.TOT_UNP_FNC_CHRG_KSA_AVG6M"
DEPENDENCIES = {
    'duckdb_clear_derive_tot_unpaid_fncl_chrg_ksaavg6m': ['duckdb_derive_tot_unpaid_fncl_chrg_ksaavg6m'],
}


def duckdb_clear_derive_tot_unpaid_fncl_chrg_ksaavg6m(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    DELETE FROM { DOWNSTREAM_ASSET } 
    WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass

def duckdb_derive_tot_unpaid_fncl_chrg_ksaavg6m(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} 
        SELECT 
        BASEL_ACCT_ID,
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate")  }}}}' AS OBSN_DT,
        AVG(TOT_UNP_FNC_CHRG_KSA) as TOT_UNP_FNC_CHRG_KSA_AVG6M
        FROM 
            {UPSTREAM_ASSET[0]}
        WHERE 
            OBSN_DT BETWEEN 
            DATE_ADD(DATE '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}', - INTERVAL 5 MONTH)
            AND
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
        GROUP BY BASEL_ACCT_ID    
    
    """

):
    pass

