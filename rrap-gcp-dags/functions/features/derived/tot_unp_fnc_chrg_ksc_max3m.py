import pyarrow as pa
from airflow.sdk import get_current_context

from bns.rrap.helpers.asset_event import (
    _pull_asset_event_extras,
    _push_asset_event_extras,
)

UPSTREAM_ASSET = [ 'ingestion.BASEL_REVLVNG_CR_MTH_SNAPSHOT' ]
DOWNSTREAM_ASSET = 'features.TOT_UNP_FNC_CHRG_KSC_MAX3M'
DEPENDENCIES = {
    'duckdb_clear_tot_unp_fnc_chrg_ksc_max3m': ['duckdb_derive_tot_unp_fnc_chrg_ksc_max3m'],
}


def duckdb_clear_tot_unp_fnc_chrg_ksc_max3m(
    duckdb_conn_id='duckdb-conn',
    sql=rf"""
        DELETE FROM { DOWNSTREAM_ASSET } 
        WHERE OBSN_DT = '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}'
    """
):
    pass


def duckdb_derive_tot_unp_fnc_chrg_ksc_max3m(
    duckdb_conn_id='duckdb-conn',
    sql=f"""
    INSERT INTO { DOWNSTREAM_ASSET }
    BY NAME FROM (
        WITH CURR_ACCOUNTS AS (
            SELECT DISTINCT BASEL_ACCT_ID 
            FROM {UPSTREAM_ASSET[0]}
            WHERE MTH_TM_ID = {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
            ),
            
        FIRST_ORDER AS (
            SELECT 
                A.PRIM_BASEL_CUST_ID,
                A.MTH_TM_ID,
                SUM(A.TOT_UNPAID_FNCL_CHRG_AMT) AS TOT_UNP_FNC_AMT_KSC
            FROM 
                {UPSTREAM_ASSET[0]} A
            INNER JOIN 
                CURR_ACCOUNTS B
            ON 
                A.BASEL_ACCT_ID = B.BASEL_ACCT_ID
            WHERE 
                MTH_TM_ID BETWEEN {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}-80 
                AND {{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="mth_tm_id") }}}}
            GROUP BY 
                A.PRIM_BASEL_CUST_ID, A.MTH_TM_ID
            )
            
                
        SELECT
            '{{{{ task_instance.xcom_pull(task_ids="handle_month_context", key="rundate") }}}}' AS OBSN_DT,
            PRIM_BASEL_CUST_ID AS BASEL_CUST_ID,
            MAX(TOT_UNP_FNC_AMT_KSC) AS TOT_UNP_FNC_CHRG_KSC_MAX3M
            FROM 
                FIRST_ORDER
            GROUP BY
                PRIM_BASEL_CUST_ID
    )
    """
):
    pass

