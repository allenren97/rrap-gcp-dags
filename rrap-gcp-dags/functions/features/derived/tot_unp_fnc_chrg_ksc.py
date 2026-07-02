import os
from datetime import timedelta
import pendulum

from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import _pull_asset_event_extras, _push_asset_event_extras


UPSTREAM_ASSET = ['ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT']

DOWNSTREAM_ASSET = "features.TOT_UNP_FNC_CHRG_KSC"
DEPENDENCIES = {
    'duckdb_clear_derive_tot_unp_fnc_chrg_ksc': ['duckdb_derive_tot_unp_fnc_chrg_ksc'],
}


def duckdb_clear_derive_tot_unp_fnc_chrg_ksc(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    DELETE FROM { DOWNSTREAM_ASSET } 
    WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass

def duckdb_derive_tot_unp_fnc_chrg_ksc(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    INSERT INTO {DOWNSTREAM_ASSET} 
        SELECT 
        PRIM_BASEL_CUST_ID AS BASEL_CUST_ID,
        '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate")  }}}}' AS OBSN_DT,
        SUM(TOT_UNPAID_FNCL_CHRG_AMT) as TOT_UNP_FNC_AMT_KSC
        FROM 
            {UPSTREAM_ASSET[0]}
        WHERE 
            MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id")  }}}} 
        GROUP BY PRIM_BASEL_CUST_ID    
    
    """

):
    pass

