import os
from datetime import timedelta
import pendulum

from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import _pull_asset_event_extras, _push_asset_event_extras


UPSTREAM_ASSET = [ 'features.TOT_C_DT_FNC_CHRG_AMT']
DOWNSTREAM_ASSET = "features.TOT_C_DT_FNC_CHRG_KSA_AVG24M"
DEPENDENCIES = {
    'duckdb_clear_derive_tot_c_dt_fnc_chrg_ksa_avg24m': ['duckdb_derive_tot_c_dt_fnc_chrg_ksa_avg24m'],
}


def duckdb_clear_derive_tot_c_dt_fnc_chrg_ksa_avg24m(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    DELETE FROM { DOWNSTREAM_ASSET } 
    WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass

def duckdb_derive_tot_c_dt_fnc_chrg_ksa_avg24m(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} (
        SELECT 
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate")  }}}}' AS OBSN_DT,
        BASEL_ACCT_ID,
        AVG(TOT_C_DT_FNC_CHRG_AMT) AS TOT_C_DT_FNC_CHRG_KSA_AVG24M
        FROM {UPSTREAM_ASSET[0]}
        WHERE OBSN_DT BETWEEN 
            DATE_ADD(DATE '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}', -INTERVAL 23 MONTH) AND 
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
        GROUP BY BASEL_ACCT_ID
    )
    """

):
    pass

